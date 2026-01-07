use std::collections::HashSet;

use promql_parser::label::{METRIC_NAME, MatchOp};
use promql_parser::parser::VectorSelector;

use crate::index::{ForwardIndex, ForwardIndexLookup, InvertedIndex, InvertedIndexLookup};
use crate::model::Label;
use crate::model::SeriesId;
use crate::query::QueryReader;
use crate::util::Result;

/// Evaluates a PromQL vector selector using a QueryReader.
/// This is the core implementation that can be tested independently.
pub(crate) async fn evaluate_selector_with_reader<R: QueryReader>(
    reader: &R,
    selector: &VectorSelector,
) -> Result<HashSet<SeriesId>> {
    let terms = extract_equality_terms(selector);
    if terms.is_empty() {
        return Ok(HashSet::new());
    }

    // Find all series matching the equality terms from all tiers
    let inverted_index_view = reader.inverted_index(&terms).await?;
    let candidates: HashSet<SeriesId> = inverted_index_view.intersect(terms).iter().collect();

    // If there are not-equal matchers, we need to filter using forward index
    if candidates.is_empty() || !has_not_equal_matchers(selector) {
        return Ok(candidates);
    }

    // Get forward index view for candidates to apply not-equal filtering
    // This avoids cloning from head/frozen tiers upfront
    let candidates_vec: Vec<SeriesId> = candidates.into_iter().collect();
    let forward_index_view = reader.forward_index(&candidates_vec).await?;
    let filtered = apply_not_equal_matchers(&forward_index_view, candidates_vec, selector);

    Ok(filtered.into_iter().collect())
}

/// Evaluate selector on in-memory indexes.
fn evaluate_on_indexes(
    forward_index: &ForwardIndex,
    inverted_index: &InvertedIndex,
    selector: &VectorSelector,
) -> Vec<SeriesId> {
    let terms = extract_equality_terms(selector);
    if terms.is_empty() {
        return Vec::new();
    }

    let candidates: Vec<SeriesId> = inverted_index.intersect(terms).iter().collect();
    if candidates.is_empty() || !has_not_equal_matchers(selector) {
        return candidates;
    }

    apply_not_equal_matchers(forward_index, candidates, selector)
}

/// Extract equality terms from the selector.
fn extract_equality_terms(selector: &VectorSelector) -> Vec<Label> {
    let mut terms = Vec::new();
    if let Some(ref name) = selector.name {
        terms.push(Label {
            name: METRIC_NAME.to_string(),
            value: name.clone(),
        });
    }
    for matcher in &selector.matchers.matchers {
        if matches!(matcher.op, MatchOp::Equal) {
            terms.push(Label {
                name: matcher.name.clone(),
                value: matcher.value.clone(),
            });
        }
    }
    terms
}

fn has_not_equal_matchers(selector: &VectorSelector) -> bool {
    selector
        .matchers
        .matchers
        .iter()
        .any(|m| matches!(m.op, MatchOp::NotEqual))
}

/// Apply not-equal matchers using any ForwardIndexLookup implementation.
fn apply_not_equal_matchers(
    index: &impl ForwardIndexLookup,
    candidates: Vec<SeriesId>,
    selector: &VectorSelector,
) -> Vec<SeriesId> {
    let mut result = candidates;
    for matcher in selector
        .matchers
        .matchers
        .iter()
        .filter(|m| matches!(m.op, MatchOp::NotEqual))
    {
        result.retain(|id| {
            index
                .get_spec(id)
                .map(|spec| !has_label(&spec.labels, &matcher.name, &matcher.value))
                .unwrap_or(false)
        });
    }
    result
}

fn has_label(labels: &[Label], name: &str, value: &str) -> bool {
    labels
        .iter()
        .any(|label| label.name == name && label.value == value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::SeriesSpec;
    use crate::model::{MetricType, Sample};
    use promql_parser::label::{Matcher, Matchers};

    fn empty_matchers() -> Matchers {
        Matchers {
            matchers: vec![],
            or_matchers: vec![],
        }
    }

    fn create_test_indexes() -> (ForwardIndex, InvertedIndex) {
        let forward = ForwardIndex::default();
        let inverted = InvertedIndex::default();

        let series = vec![
            (1, "http_requests_total", "GET", "prod"),
            (2, "http_requests_total", "POST", "prod"),
            (3, "http_requests_total", "GET", "staging"),
        ];

        for (id, metric, method, env) in series {
            let attrs = vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: metric.to_string(),
                },
                Label {
                    name: "method".to_string(),
                    value: method.to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: env.to_string(),
                },
            ];
            forward.series.insert(
                id,
                SeriesSpec {
                    unit: None,
                    metric_type: Some(MetricType::Gauge),
                    labels: attrs.clone(),
                },
            );
            for attr in attrs {
                inverted
                    .postings
                    .entry(attr)
                    .or_default()
                    .value_mut()
                    .insert(id);
            }
        }
        (forward, inverted)
    }

    #[test]
    fn should_match_by_metric_name() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: empty_matchers(),
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert_eq!(result.len(), 3);
    }

    #[test]
    fn should_match_by_equality_matcher() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![Matcher::new(MatchOp::Equal, "method", "GET")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert_eq!(result.len(), 2);
        assert!(result.contains(&1));
        assert!(result.contains(&3));
    }

    #[test]
    fn should_exclude_by_not_equal_matcher() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![Matcher::new(MatchOp::NotEqual, "method", "GET")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert_eq!(result.len(), 1);
        assert!(result.contains(&2));
    }

    #[test]
    fn should_combine_equal_and_not_equal() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![
                    Matcher::new(MatchOp::Equal, "method", "GET"),
                    Matcher::new(MatchOp::NotEqual, "env", "staging"),
                ],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert_eq!(result.len(), 1);
        assert!(result.contains(&1));
    }

    #[test]
    fn should_return_empty_for_unknown_metric() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: Some("unknown".to_string()),
            matchers: empty_matchers(),
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert!(result.is_empty());
    }

    #[test]
    fn should_return_empty_for_no_equality_matchers() {
        let (forward, inverted) = create_test_indexes();
        let selector = VectorSelector {
            name: None,
            matchers: empty_matchers(),
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn should_merge_results_from_head_and_storage() {
        use crate::model::TimeBucket;
        use crate::query::test_utils::MockQueryReaderBuilder;

        // given: create a mock reader with 3 series
        let bucket = TimeBucket::hour(1000);
        let mut builder = MockQueryReaderBuilder::new(bucket);

        // Add series with env=prod, method=GET
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests_total".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
                Label {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 1000,
                value: 10.0,
            },
        );

        // Add series with env=prod, method=POST
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests_total".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
                Label {
                    name: "method".to_string(),
                    value: "POST".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 1001,
                value: 20.0,
            },
        );

        // Add series with env=staging, method=GET
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests_total".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "staging".to_string(),
                },
                Label {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 2000,
                value: 30.0,
            },
        );

        let reader = builder.build();

        // when: query for all http_requests_total series
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: empty_matchers(),
            offset: None,
            at: None,
        };
        let result = evaluate_selector_with_reader(&reader, &selector)
            .await
            .unwrap();

        // then: should find all 3 series
        assert_eq!(result.len(), 3, "Should find 3 series total");
    }
}

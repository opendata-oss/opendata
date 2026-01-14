use std::collections::HashSet;

use crate::index::{ForwardIndex, ForwardIndexLookup, InvertedIndex, InvertedIndexLookup};
use crate::model::Label;
use crate::model::SeriesId;
use crate::promql::evaluator::CachedQueryReader;
use crate::query::QueryReader;
use crate::util::Result;
use promql_parser::label::{METRIC_NAME, MatchOp};
use promql_parser::parser::VectorSelector;
use regex_syntax::hir::{Hir, HirKind};

fn parse_literal(hir: &Hir, pattern: &str) -> std::result::Result<String, String> {
    match hir.kind() {
        HirKind::Empty => Err(format!("empty alternative in pattern: {}", pattern)),
        HirKind::Literal(l) => String::from_utf8(l.0.to_vec())
            .map_err(|_| format!("Non-UTF-8 literal in regex: {:?}", l)),
        HirKind::Concat(hirs) => {
            let mut value = String::new();
            for hir in hirs {
                value.push_str(&parse_literal(hir, pattern)?);
            }
            Ok(value)
        }
        _ => Err(format!(
            "Regex pattern '{}' is not supported. Only alternations of literal strings allowed (e.g., 'value1|value2|value3').",
            pattern
        )),
    }
}

/// Parse a limited regex pattern of the form "value1|value2|...|valueN" into individual values.
/// Returns an error if the regex is not of the expected simple pipe-separated form.
/// Uses regex-syntax to properly parse and validate the regex structure.
fn parse_limited_regex(pattern: &str) -> std::result::Result<Vec<String>, String> {
    use regex_syntax::Parser;
    use regex_syntax::hir::HirKind;

    let hir = Parser::new()
        .parse(pattern)
        .map_err(|e| format!("Invalid regex pattern '{}': {}", pattern, e))?;

    match hir.kind() {
        HirKind::Alternation(alternatives) => {
            let mut values = Vec::new();
            // Each alternative must be a literal string or a simple concatenation
            for alt in alternatives {
                values.push(parse_literal(alt, pattern)?);
            }
            Ok(values)
        }
        HirKind::Literal(_) => Ok(vec![parse_literal(&hir, pattern)?]),
        HirKind::Concat(_) => Ok(vec![parse_literal(&hir, pattern)?]),
        _ => Err(format!(
            "Regex pattern '{}' not supported. Only alternations of literal strings allowed (e.g., 'value1|value2|value3').",
            pattern
        )),
    }
}

/// Find candidate series IDs using a QueryReader
async fn find_candidates_with_reader<'reader, R: QueryReader>(
    reader: &mut CachedQueryReader<'reader, R>,
    bucket: &crate::model::TimeBucket,
    selector: &VectorSelector,
) -> Result<Vec<SeriesId>> {
    use std::collections::HashSet;

    let mut and_terms = Vec::new(); // Terms that must ALL match (AND)
    let mut or_groups = Vec::new(); // Groups of terms where ANY can match (OR)

    // Add metric name if specified
    if let Some(ref name) = selector.name {
        and_terms.push(Label {
            name: METRIC_NAME.to_string(),
            value: name.clone(),
        });
    }

    for matcher in &selector.matchers.matchers {
        match &matcher.op {
            MatchOp::Equal => {
                and_terms.push(Label {
                    name: matcher.name.clone(),
                    value: matcher.value.clone(),
                });
            }
            MatchOp::Re(_) => {
                let values = parse_limited_regex(&matcher.value)
                    .map_err(crate::error::Error::InvalidInput)?;
                let or_terms: Vec<Label> = values
                    .into_iter()
                    .map(|value| Label {
                        name: matcher.name.clone(),
                        value,
                    })
                    .collect();
                or_groups.push(or_terms);
            }
            _ => {
                // Other match operations are handled in negative matchers
            }
        }
    }

    if and_terms.is_empty() && or_groups.is_empty() {
        return Ok(Vec::new());
    }

    let all_terms = or_groups
        .iter()
        .flat_map(|terms| terms.iter().cloned())
        .chain(and_terms.iter().cloned())
        .collect::<Vec<_>>();
    let inverted_index_view = reader.inverted_index(bucket, &all_terms).await?;

    // Start with AND terms intersection
    let mut result_set: HashSet<SeriesId> = if !and_terms.is_empty() {
        inverted_index_view
            .intersect(and_terms.clone())
            .iter()
            .collect()
    } else {
        HashSet::new()
    };

    // Apply OR groups
    for or_terms in or_groups {
        let mut or_result = HashSet::new();
        // Get union of all terms in this OR group
        // Since we can't use a union method on the trait, collect individual intersections
        for term in or_terms {
            let term_result = inverted_index_view.intersect(vec![term]);
            or_result.extend(term_result.iter());
        }

        if and_terms.is_empty() && result_set.is_empty() {
            result_set = or_result;
        } else {
            result_set = result_set.intersection(&or_result).cloned().collect();
        }
    }

    Ok(result_set.into_iter().collect())
}

/// Evaluates a PromQL vector selector using a QueryReader.
/// This is the core implementation that can be tested independently.
pub(crate) async fn evaluate_selector_with_reader<'reader, R: QueryReader>(
    reader: &mut CachedQueryReader<'reader, R>,
    bucket: crate::model::TimeBucket,
    selector: &VectorSelector,
) -> Result<HashSet<SeriesId>> {
    let candidates = find_candidates_with_reader(reader, &bucket, selector).await?;

    // If there are not-equal or regex-not matchers, we need to filter using forward index
    if candidates.is_empty() || !has_negative_matchers(selector) {
        return Ok(candidates.into_iter().collect());
    }

    // Get forward index view for candidates to apply negative filtering
    let forward_index_view = reader.forward_index(&bucket, &candidates).await?;
    let filtered = apply_negative_matchers(forward_index_view.as_ref(), candidates, selector)
        .map_err(crate::error::Error::InvalidInput)?;

    Ok(filtered.into_iter().collect())
}

/// Evaluate selector on in-memory indexes.
fn evaluate_on_indexes(
    forward_index: &ForwardIndex,
    inverted_index: &InvertedIndex,
    selector: &VectorSelector,
) -> std::result::Result<Vec<SeriesId>, String> {
    // Handle regex and equality matchers separately to support OR logic for regex
    let candidates = find_candidates_with_regex_support(inverted_index, selector)?;
    if candidates.is_empty() || !has_negative_matchers(selector) {
        return Ok(candidates);
    }

    let filtered = apply_negative_matchers(forward_index, candidates, selector)?;
    Ok(filtered)
}

/// Find candidate series IDs with support for regex OR logic.
/// Handles equality and regex matchers properly by building unions for regex terms.
fn find_candidates_with_regex_support(
    inverted_index: &InvertedIndex,
    selector: &VectorSelector,
) -> std::result::Result<Vec<SeriesId>, String> {
    use std::collections::HashSet;

    let mut and_terms = Vec::new(); // Terms that must ALL match (AND)
    let mut or_groups = Vec::new(); // Groups of terms where ANY can match (OR)

    // Add metric name if specified
    if let Some(ref name) = selector.name {
        and_terms.push(Label {
            name: METRIC_NAME.to_string(),
            value: name.clone(),
        });
    }

    // Process matchers
    for matcher in &selector.matchers.matchers {
        match &matcher.op {
            MatchOp::Equal => {
                and_terms.push(Label {
                    name: matcher.name.clone(),
                    value: matcher.value.clone(),
                });
            }
            MatchOp::Re(_) => {
                // Validate and expand regex pattern
                let values = parse_limited_regex(&matcher.value)?;
                let or_terms: Vec<Label> = values
                    .into_iter()
                    .map(|value| Label {
                        name: matcher.name.clone(),
                        value,
                    })
                    .collect();
                or_groups.push(or_terms);
            }
            _ => {
                // Other match operations are handled in negative matchers
            }
        }
    }

    if and_terms.is_empty() && or_groups.is_empty() {
        return Ok(Vec::new());
    }

    // Start with AND terms intersection
    let mut result_set: HashSet<SeriesId> = if !and_terms.is_empty() {
        inverted_index.intersect(and_terms.clone()).iter().collect()
    } else {
        HashSet::new()
    };

    // Apply OR groups
    for or_terms in or_groups {
        // Use the union method to get OR logic for this group
        let or_result: HashSet<SeriesId> = inverted_index.union(or_terms).iter().collect();

        if and_terms.is_empty() && result_set.is_empty() {
            // First OR group becomes the base set
            result_set = or_result;
        } else {
            // Intersect with existing results (AND relationship between groups)
            result_set = result_set.intersection(&or_result).cloned().collect();
        }
    }

    Ok(result_set.into_iter().collect())
}

/// Extract equality terms from the selector (simplified version for compatibility).
fn extract_equality_terms(selector: &VectorSelector) -> std::result::Result<Vec<Label>, String> {
    let mut terms = Vec::new();
    if let Some(ref name) = selector.name {
        terms.push(Label {
            name: METRIC_NAME.to_string(),
            value: name.clone(),
        });
    }

    for matcher in &selector.matchers.matchers {
        match &matcher.op {
            MatchOp::Equal => {
                terms.push(Label {
                    name: matcher.name.clone(),
                    value: matcher.value.clone(),
                });
            }
            MatchOp::Re(_) => {
                // Regex validation only - actual handling done in find_candidates_with_regex_support
                let _values = parse_limited_regex(&matcher.value)?;
            }
            _ => {
                // Other match operations handled elsewhere
            }
        }
    }

    Ok(terms)
}

fn has_negative_matchers(selector: &VectorSelector) -> bool {
    selector
        .matchers
        .matchers
        .iter()
        .any(|m| matches!(m.op, MatchOp::NotEqual | MatchOp::NotRe(_)))
}

/// Apply negative matchers (not-equal, not-regex) using any ForwardIndexLookup implementation.
fn apply_negative_matchers(
    index: &dyn ForwardIndexLookup,
    candidates: Vec<SeriesId>,
    selector: &VectorSelector,
) -> std::result::Result<Vec<SeriesId>, String> {
    let mut result = candidates;

    for matcher in &selector.matchers.matchers {
        match &matcher.op {
            MatchOp::NotEqual => {
                result.retain(|id| {
                    index
                        .get_spec(id)
                        .map(|spec| !has_label(&spec.labels, &matcher.name, &matcher.value))
                        .unwrap_or(false)
                });
            }
            MatchOp::NotRe(_) => {
                // Parse limited regex and exclude series matching any of the values
                let values = parse_limited_regex(&matcher.value)?;
                result.retain(|id| {
                    index
                        .get_spec(id)
                        .map(|spec| {
                            !values
                                .iter()
                                .any(|value| has_label(&spec.labels, &matcher.name, value))
                        })
                        .unwrap_or(false)
                });
            }
            _ => {
                // Other match operations are handled separately
            }
        }
    }

    Ok(result)
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
    use rstest::rstest;

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

        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

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

        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

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

        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

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

        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

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

        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

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

        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

        assert!(result.is_empty());
    }

    #[rstest]
    #[case("host-40", Ok(vec!["host-40".to_string()]))]
    #[case("host-40|host-39|host-38", Ok(vec!["host-40".to_string(), "host-39".to_string(), "host-38".to_string()]))]
    #[case("prod|staging|dev", Ok(vec!["prod".to_string(), "staging".to_string(), "dev".to_string()]))]
    #[case("host-[0-9]+", Err("regex constructs not allowed"))]
    #[case("host.*", Err("regex constructs not allowed"))]
    #[case("(host-40|host-39)", Err("regex constructs not allowed"))]
    #[case("host-40+", Err("regex constructs not allowed"))]
    #[case("^host-40$", Err("regex constructs not allowed"))]
    #[case("host-40?", Err("regex constructs not allowed"))]
    #[case("host-4[0-9]", Err("regex constructs not allowed"))]
    #[case("", Err("empty pattern"))]
    #[case("host-40||host-39", Err("empty alternative"))]
    #[case("|host-40", Err("empty alternative"))]
    #[case("host-40|", Err("empty alternative"))]
    fn should_parse_limited_regex_patterns(
        #[case] pattern: &str,
        #[case] expected: std::result::Result<Vec<String>, &str>,
    ) {
        let result = parse_limited_regex(pattern);
        match expected {
            Ok(expected_values) => {
                assert_eq!(result, Ok(expected_values));
            }
            Err(_) => {
                assert!(
                    result.is_err(),
                    "Pattern '{}' should be rejected but was accepted: {:?}",
                    pattern,
                    result
                );
            }
        }
    }

    #[rstest]
    #[case(r"host\d+", "Digit escape")]
    #[case(r"host\w+", "Word character escape")]
    #[case(r"host\s*", "Whitespace escape")]
    #[case(r"host.*\.com", "Dot-star pattern")]
    #[case(r"(prod|staging)", "Grouped alternation")]
    #[case(r"host-\d{2}", "Digit with quantifier")]
    #[case(r"^host-40$", "Start/end anchors")]
    #[case(r"host-40|host-.*", "Mixed literal and pattern")]
    #[case(r"(?i)host-40", "Case-insensitive flag")]
    #[case(r"host-40{1,3}", "Counted repetition")]
    #[case(r"GE[Tt]", "Character class")]
    #[case(r"GET.*", "Dot-star")]
    fn should_fail_complex_regex_patterns(#[case] pattern: &str, #[case] description: &str) {
        let result = parse_limited_regex(pattern);
        assert!(
            result.is_err(),
            "Pattern '{}' ({}) should be rejected but was accepted: {:?}",
            pattern,
            description,
            result
        );
    }

    fn create_regex_matcher(name: &str, pattern: &str) -> std::result::Result<Matcher, String> {
        // Create a regex from the pattern to validate it
        use regex::Regex;
        let regex = Regex::new(pattern).map_err(|e| format!("Invalid regex: {}", e))?;

        Ok(Matcher {
            op: MatchOp::Re(regex),
            name: name.to_string(),
            value: pattern.to_string(),
        })
    }

    fn create_not_regex_matcher(name: &str, pattern: &str) -> std::result::Result<Matcher, String> {
        // Create a regex from the pattern to validate it
        use regex::Regex;
        let regex = Regex::new(pattern).map_err(|e| format!("Invalid regex: {}", e))?;

        Ok(Matcher {
            op: MatchOp::NotRe(regex),
            name: name.to_string(),
            value: pattern.to_string(),
        })
    }

    #[test]
    fn should_match_by_regex_matcher() {
        // given:
        let (forward, inverted) = create_test_indexes();
        let matcher = create_regex_matcher("method", "GET|POST").unwrap();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![matcher],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        // when:
        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

        // then: should match all three series since all have method=GET or method=POST
        assert_eq!(result.len(), 3);
        assert!(result.contains(&1)); // method=GET
        assert!(result.contains(&2)); // method=POST
        assert!(result.contains(&3)); // method=GET
    }

    #[test]
    fn should_match_by_single_value_regex() {
        // given:
        let (forward, inverted) = create_test_indexes();
        let matcher = create_regex_matcher("method", "GET").unwrap();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![matcher],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        // when:
        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

        // then:
        assert_eq!(result.len(), 2);
        assert!(result.contains(&1)); // method=GET
        assert!(result.contains(&3)); // method=GET
    }

    #[test]
    fn should_exclude_by_not_regex_matcher() {
        // given:
        let (forward, inverted) = create_test_indexes();
        let matcher = create_not_regex_matcher("env", "prod|staging").unwrap();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![matcher],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        // when:
        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

        // then: should return empty since all series have env=prod or env=staging
        assert!(result.is_empty());
    }

    #[test]
    fn should_combine_equal_and_regex() {
        // given:
        let (forward, inverted) = create_test_indexes();
        let regex_matcher = create_regex_matcher("method", "GET|POST").unwrap();
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![Matcher::new(MatchOp::Equal, "env", "prod"), regex_matcher],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        // when:
        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

        // then:
        assert_eq!(result.len(), 2);
        assert!(result.contains(&1)); // env=prod, method=GET
        assert!(result.contains(&2)); // env=prod, method=POST
    }

    #[test]
    fn should_fail_on_invalid_regex_pattern() {
        // given:
        let (forward, inverted) = create_test_indexes();
        let matcher = create_regex_matcher("method", "GET.*").unwrap(); // This will create the regex but fail during limited pattern validation
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![matcher],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        // when:
        let result = evaluate_on_indexes(&forward, &inverted, &selector);

        // then:
        assert!(result.is_err());
        let error_msg = result.unwrap_err();
        assert!(
            error_msg.contains("contains regex metacharacters")
                || error_msg.contains("contains unsupported constructs")
                || error_msg.contains("is not supported"),
            "Unexpected error message: {}",
            error_msg
        );
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
        let mut cached_reader = CachedQueryReader::new(&reader);
        let result = evaluate_selector_with_reader(&mut cached_reader, bucket, &selector)
            .await
            .unwrap();

        // then: should find all 3 series
        assert_eq!(result.len(), 3, "Should find 3 series total");
    }

    #[tokio::test]
    async fn should_support_exact_user_example() {
        use crate::model::TimeBucket;
        use crate::query::test_utils::MockQueryReaderBuilder;

        // Test the exact example from the user: 'node_netstat_Icmp6_OutMsgs{instance=~"host-40|host-39|host-38"}'

        let bucket = TimeBucket::hour(1000);
        let mut builder = MockQueryReaderBuilder::new(bucket);

        // Add series with instance=host-40
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "node_netstat_Icmp6_OutMsgs".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host-40".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 1000,
                value: 10.0,
            },
        );

        // Add series with instance=host-39
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "node_netstat_Icmp6_OutMsgs".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host-39".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 1001,
                value: 20.0,
            },
        );

        // Add series with instance=host-38
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "node_netstat_Icmp6_OutMsgs".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host-38".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 2000,
                value: 30.0,
            },
        );

        // Add series with instance=host-50 (should not match)
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "node_netstat_Icmp6_OutMsgs".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host-50".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 3000,
                value: 40.0,
            },
        );

        let reader = builder.build();

        // when: query with regex matcher instance=~"host-40|host-39|host-38"
        let matcher = create_regex_matcher("instance", "host-40|host-39|host-38").unwrap();
        let selector = VectorSelector {
            name: Some("node_netstat_Icmp6_OutMsgs".to_string()),
            matchers: Matchers {
                matchers: vec![matcher],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };
        let mut cached_reader = CachedQueryReader::new(&reader);
        let result = evaluate_selector_with_reader(&mut cached_reader, bucket, &selector)
            .await
            .unwrap();

        // then: should find exactly 3 series (excluding host-50)
        assert_eq!(result.len(), 3, "Should find 3 matching series");
    }

    #[tokio::test]
    async fn should_support_regex_matchers_with_query_reader() {
        use crate::model::TimeBucket;
        use crate::query::test_utils::MockQueryReaderBuilder;

        // given: create a mock reader with 3 series with different instance values
        let bucket = TimeBucket::hour(1000);
        let mut builder = MockQueryReaderBuilder::new(bucket);

        // Add series with instance=host-40
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "node_netstat_Icmp6_OutMsgs".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host-40".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 1000,
                value: 10.0,
            },
        );

        // Add series with instance=host-39
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "node_netstat_Icmp6_OutMsgs".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host-39".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 1001,
                value: 20.0,
            },
        );

        // Add series with instance=host-38
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "node_netstat_Icmp6_OutMsgs".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host-38".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 2000,
                value: 30.0,
            },
        );

        // Add series with instance=host-50 (should not match)
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "node_netstat_Icmp6_OutMsgs".to_string(),
                },
                Label {
                    name: "instance".to_string(),
                    value: "host-50".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 3000,
                value: 40.0,
            },
        );

        let reader = builder.build();

        // when: query with regex matcher instance=~"host-40|host-39|host-38"
        let matcher = create_regex_matcher("instance", "host-40|host-39|host-38").unwrap();
        let selector = VectorSelector {
            name: Some("node_netstat_Icmp6_OutMsgs".to_string()),
            matchers: Matchers {
                matchers: vec![matcher],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };
        let mut cached_reader = CachedQueryReader::new(&reader);
        let result = evaluate_selector_with_reader(&mut cached_reader, bucket, &selector)
            .await
            .unwrap();

        // then: should find exactly 3 series (excluding host-50)
        assert_eq!(result.len(), 3, "Should find 3 matching series");
    }
}

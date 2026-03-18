use std::collections::HashSet;

use crate::index::{ForwardIndex, ForwardIndexLookup, InvertedIndex, InvertedIndexLookup};
use crate::load_coordinator::ReadLoadCoordinator;
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

/// (and_terms, or_groups, needs_metric_fallback)
type SelectorTerms = (Vec<Label>, Vec<Vec<Label>>, bool);

/// Selector term decomposition: (and_terms, or_groups, needs_metric_fallback).
/// `needs_metric_fallback` is true when there are no positive matchers but empty-string matchers
/// exist, requiring a metric-name-based fallback lookup.
fn build_selector_terms(
    selector: &VectorSelector,
) -> std::result::Result<SelectorTerms, crate::error::Error> {
    let mut and_terms = Vec::new();
    let mut or_groups = Vec::new();

    if let Some(ref name) = selector.name {
        and_terms.push(Label {
            name: METRIC_NAME.to_string(),
            value: name.clone(),
        });
    }

    for matcher in &selector.matchers.matchers {
        match &matcher.op {
            MatchOp::Equal => {
                if !matcher.value.is_empty() {
                    and_terms.push(Label {
                        name: matcher.name.clone(),
                        value: matcher.value.clone(),
                    });
                }
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
            _ => {}
        }
    }

    let needs_metric_fallback =
        and_terms.is_empty() && or_groups.is_empty() && has_empty_string_matchers(selector);

    Ok((and_terms, or_groups, needs_metric_fallback))
}

/// Resolve candidates from an inverted index view using pre-built terms.
fn resolve_candidates_from_index(
    inverted_index_view: &dyn InvertedIndexLookup,
    and_terms: &[Label],
    or_groups: &[Vec<Label>],
) -> HashSet<SeriesId> {
    let mut result_set: HashSet<SeriesId> = if !and_terms.is_empty() {
        inverted_index_view
            .intersect(and_terms.to_vec())
            .iter()
            .collect()
    } else {
        HashSet::new()
    };

    for or_terms in or_groups {
        let mut or_result = HashSet::new();
        for term in or_terms {
            let term_result = inverted_index_view.intersect(vec![term.clone()]);
            or_result.extend(term_result.iter());
        }

        if and_terms.is_empty() && result_set.is_empty() {
            result_set = or_result;
        } else {
            result_set = result_set.intersection(&or_result).cloned().collect();
        }
    }

    result_set
}

/// Find candidate series IDs using a QueryReader
async fn find_candidates_with_reader<'reader, R: QueryReader>(
    reader: &mut CachedQueryReader<'reader, R>,
    bucket: &crate::model::TimeBucket,
    selector: &VectorSelector,
) -> Result<Vec<SeriesId>> {
    let (and_terms, or_groups, needs_metric_fallback) = build_selector_terms(selector)?;

    if and_terms.is_empty() && or_groups.is_empty() {
        if !needs_metric_fallback {
            return Ok(Vec::new());
        }
        if let Some(ref name) = selector.name {
            let metric_term = Label {
                name: METRIC_NAME.to_string(),
                value: name.clone(),
            };
            let inverted_index_view = reader
                .inverted_index(bucket, std::slice::from_ref(&metric_term))
                .await?;
            let result_set: Vec<SeriesId> = inverted_index_view
                .intersect(vec![metric_term])
                .iter()
                .collect();
            return Ok(result_set);
        } else {
            return Err(crate::error::Error::InvalidInput(
                "must specify a metric name when using empty label matcher".to_string(),
            ));
        }
    }

    let all_terms: Vec<Label> = or_groups
        .iter()
        .flat_map(|terms| terms.iter().cloned())
        .chain(and_terms.iter().cloned())
        .collect();
    let inverted_index_view = reader.inverted_index(bucket, &all_terms).await?;

    let result_set =
        resolve_candidates_from_index(inverted_index_view.as_ref(), &and_terms, &or_groups);

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

    // If there are negative matchers or empty string matchers, we need to filter using forward index
    if candidates.is_empty()
        || (!has_negative_matchers(selector) && !has_empty_string_matchers(selector))
    {
        return Ok(candidates.into_iter().collect());
    }

    // Get forward index view for candidates to apply filtering
    let forward_index_view = reader.forward_index(&bucket, &candidates).await?;
    let mut filtered = candidates;

    // Apply negative matchers
    if has_negative_matchers(selector) {
        filtered = apply_negative_matchers(forward_index_view.as_ref(), filtered, selector)
            .map_err(crate::error::Error::InvalidInput)?;
    }

    // Apply empty string matchers
    if has_empty_string_matchers(selector) {
        filtered = apply_empty_string_matchers(forward_index_view.as_ref(), filtered, selector)
            .map_err(crate::error::Error::InvalidInput)?;
    }

    Ok(filtered.into_iter().collect())
}

#[derive(Debug, Default)]
pub(crate) struct SelectorRawStats {
    pub(crate) metadata_queue_wait_ms: u64,
    pub(crate) metadata_load_ms: u64,
    pub(crate) metadata_permit_acquires: u64,
    // Batched forward index stats (from negative/empty-string matcher filtering)
    pub(crate) fi_series_loaded: u64,
    pub(crate) fi_batch_ops: u64,
    pub(crate) fi_point_lookups: u64,
    pub(crate) fi_range_scans: u64,
    pub(crate) fi_range_scan_series: u64,
}

/// Selector evaluation using raw QueryReader — no caching layer.
/// Acquires metadata permits around each individual raw I/O call.
pub(crate) async fn evaluate_selector_raw<R: QueryReader>(
    reader: &R,
    bucket: &crate::model::TimeBucket,
    selector: &VectorSelector,
    coordinator: &Option<ReadLoadCoordinator>,
) -> std::result::Result<(HashSet<SeriesId>, SelectorRawStats), crate::error::Error> {
    let mut stats = SelectorRawStats::default();
    let (and_terms, or_groups, needs_metric_fallback) = build_selector_terms(selector)?;

    if and_terms.is_empty() && or_groups.is_empty() {
        if !needs_metric_fallback {
            return Ok((HashSet::new(), stats));
        }
        if let Some(ref name) = selector.name {
            let metric_term = Label {
                name: METRIC_NAME.to_string(),
                value: name.clone(),
            };
            let inverted_index_view = acquire_and_load_inverted_index(
                reader,
                bucket,
                std::slice::from_ref(&metric_term),
                coordinator,
                &mut stats,
            )
            .await?;
            let result_set: HashSet<SeriesId> = inverted_index_view
                .intersect(vec![metric_term])
                .iter()
                .collect();
            return Ok((result_set, stats));
        } else {
            return Err(crate::error::Error::InvalidInput(
                "must specify a metric name when using empty label matcher".to_string(),
            ));
        }
    }

    // Load inverted index with permit gating
    let all_terms: Vec<Label> = or_groups
        .iter()
        .flat_map(|terms| terms.iter().cloned())
        .chain(and_terms.iter().cloned())
        .collect();
    let inverted_index_view =
        acquire_and_load_inverted_index(reader, bucket, &all_terms, coordinator, &mut stats)
            .await?;

    let candidates =
        resolve_candidates_from_index(inverted_index_view.as_ref(), &and_terms, &or_groups);

    if candidates.is_empty()
        || (!has_negative_matchers(selector) && !has_empty_string_matchers(selector))
    {
        return Ok((candidates, stats));
    }

    // Need forward index for negative/empty-string filtering
    let candidates_vec: Vec<_> = candidates.iter().copied().collect();
    let forward_index_view =
        acquire_and_load_forward_index(reader, bucket, &candidates_vec, coordinator, &mut stats)
            .await?;

    let mut filtered: Vec<_> = candidates.into_iter().collect();

    if has_negative_matchers(selector) {
        filtered = apply_negative_matchers(forward_index_view.as_ref(), filtered, selector)
            .map_err(crate::error::Error::InvalidInput)?;
    }

    if has_empty_string_matchers(selector) {
        filtered = apply_empty_string_matchers(forward_index_view.as_ref(), filtered, selector)
            .map_err(crate::error::Error::InvalidInput)?;
    }

    Ok((filtered.into_iter().collect(), stats))
}

/// Acquire metadata permit and load inverted index from raw reader.
async fn acquire_and_load_inverted_index<R: QueryReader>(
    reader: &R,
    bucket: &crate::model::TimeBucket,
    terms: &[Label],
    coordinator: &Option<ReadLoadCoordinator>,
    stats: &mut SelectorRawStats,
) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
    if let Some(coord) = coordinator {
        let (permit, wait) = coord.acquire_metadata().await;
        stats.metadata_permit_acquires += 1;
        stats.metadata_queue_wait_ms += wait.as_millis() as u64;
        let t0 = std::time::Instant::now();
        let result = reader.inverted_index(bucket, terms).await?;
        stats.metadata_load_ms += t0.elapsed().as_millis() as u64;
        drop(permit);
        Ok(result)
    } else {
        let t0 = std::time::Instant::now();
        let result = reader.inverted_index(bucket, terms).await?;
        stats.metadata_load_ms += t0.elapsed().as_millis() as u64;
        Ok(result)
    }
}

/// Acquire metadata permit and load forward index from raw reader.
async fn acquire_and_load_forward_index<R: QueryReader>(
    reader: &R,
    bucket: &crate::model::TimeBucket,
    series_ids: &[SeriesId],
    coordinator: &Option<ReadLoadCoordinator>,
    stats: &mut SelectorRawStats,
) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
    let result = if let Some(coord) = coordinator {
        let (permit, wait) = coord.acquire_metadata().await;
        stats.metadata_permit_acquires += 1;
        stats.metadata_queue_wait_ms += wait.as_millis() as u64;
        let t0 = std::time::Instant::now();
        let result = reader.forward_index(bucket, series_ids).await?;
        stats.metadata_load_ms += t0.elapsed().as_millis() as u64;
        drop(permit);
        result
    } else {
        let t0 = std::time::Instant::now();
        let result = reader.forward_index(bucket, series_ids).await?;
        stats.metadata_load_ms += t0.elapsed().as_millis() as u64;
        result
    };
    let bs = result.batch_stats();
    stats.fi_series_loaded += bs.unique_series as u64;
    stats.fi_batch_ops += bs.batch_ops as u64;
    stats.fi_point_lookups += bs.point_lookups as u64;
    stats.fi_range_scans += bs.range_scans as u64;
    stats.fi_range_scan_series += bs.range_scan_series as u64;
    Ok(result)
}

/// Evaluate selector on in-memory indexes.
fn evaluate_on_indexes(
    forward_index: &ForwardIndex,
    inverted_index: &InvertedIndex,
    selector: &VectorSelector,
) -> std::result::Result<Vec<SeriesId>, String> {
    // Handle regex and equality matchers separately to support OR logic for regex
    let candidates = find_candidates_with_regex_support(inverted_index, selector)?;
    if candidates.is_empty()
        || (!has_negative_matchers(selector) && !has_empty_string_matchers(selector))
    {
        return Ok(candidates);
    }

    let mut filtered = candidates;

    // Apply negative matchers
    if has_negative_matchers(selector) {
        filtered = apply_negative_matchers(forward_index, filtered, selector)?;
    }

    // Apply empty string matchers
    if has_empty_string_matchers(selector) {
        filtered = apply_empty_string_matchers(forward_index, filtered, selector)?;
    }

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
                // For empty string matchers, we skip adding them to and_terms
                // and handle them later with post-filtering
                if !matcher.value.is_empty() {
                    and_terms.push(Label {
                        name: matcher.name.clone(),
                        value: matcher.value.clone(),
                    });
                }
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

    // If we have no positive matchers but have empty string matchers, we need to get all series
    // Otherwise, if we have no positive matchers and no empty string matchers, return empty
    if and_terms.is_empty() && or_groups.is_empty() {
        if !has_empty_string_matchers(selector) {
            return Ok(Vec::new());
        }
        // For empty string matchers only, we need to get all series to filter later
        // We'll get all series by getting the metric name (if specified)
        if let Some(ref name) = selector.name {
            let metric_term = Label {
                name: METRIC_NAME.to_string(),
                value: name.clone(),
            };
            let result_set: Vec<SeriesId> =
                inverted_index.intersect(vec![metric_term]).iter().collect();
            return Ok(result_set);
        } else {
            // No metric name specified - this would match all series, but that's expensive
            // For now, return empty as this case should be rare
            return Ok(Vec::new());
        }
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
                // For empty string matchers, we skip adding them to terms
                // and handle them later with post-filtering
                if !matcher.value.is_empty() {
                    terms.push(Label {
                        name: matcher.name.clone(),
                        value: matcher.value.clone(),
                    });
                }
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

fn has_empty_string_matchers(selector: &VectorSelector) -> bool {
    selector
        .matchers
        .matchers
        .iter()
        .any(|m| matches!(m.op, MatchOp::Equal) && m.value.is_empty())
}

/// Apply empty string matchers using any ForwardIndexLookup implementation.
/// For empty string matchers (label=""), include series that either:
/// - Have the label set to an empty string
/// - Don't have the label at all
fn apply_empty_string_matchers(
    index: &dyn ForwardIndexLookup,
    candidates: Vec<SeriesId>,
    selector: &VectorSelector,
) -> std::result::Result<Vec<SeriesId>, String> {
    let mut result = candidates;

    for matcher in &selector.matchers.matchers {
        if matches!(matcher.op, MatchOp::Equal) && matcher.value.is_empty() {
            result.retain(|id| {
                index
                    .get_spec(id)
                    .map(|spec| {
                        // Include if the label is missing OR set to empty string
                        !has_label_with_non_empty_value(&spec.labels, &matcher.name)
                    })
                    .unwrap_or(false)
            });
        }
    }

    Ok(result)
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

fn has_label_with_non_empty_value(labels: &[Label], name: &str) -> bool {
    labels
        .iter()
        .any(|label| label.name == name && !label.value.is_empty())
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
    async fn should_handle_empty_label_selector() {
        use crate::model::TimeBucket;
        use crate::query::test_utils::MockQueryReaderBuilder;
        // given:
        let bucket = TimeBucket::hour(1000);
        let mut builder = MockQueryReaderBuilder::new(bucket);
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests_total".to_string(),
                },
                Label {
                    name: "foo".to_string(),
                    value: "bar".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 1000,
                value: 10.0,
            },
        );
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests_total".to_string(),
                },
                Label {
                    name: "foo".to_string(),
                    value: "".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 2000,
                value: 20.0,
            },
        );
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests_total".to_string(),
                },
                Label {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 3000,
                value: 30.0,
            },
        );
        let reader = builder.build();

        // when: query with foo=""
        let selector = VectorSelector {
            name: Some("http_requests_total".to_string()),
            matchers: Matchers {
                matchers: vec![Matcher::new(MatchOp::Equal, "foo", "")],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };
        let mut cached_reader = CachedQueryReader::new(&reader);
        let result = evaluate_selector_with_reader(&mut cached_reader, bucket, &selector)
            .await
            .unwrap();

        // then: should find 2 series (foo="" and no foo label), but NOT foo="bar"
        assert_eq!(
            result.len(),
            2,
            "Should find 2 series: foo='' and no foo label"
        );
    }

    #[test]
    fn should_handle_empty_label_with_mixed_matchers() {
        // Test empty label selector combined with regular matchers
        let forward = ForwardIndex::default();
        let inverted = InvertedIndex::default();

        // Series with foo="", env="prod" (should match)
        let series1_labels = vec![
            Label {
                name: METRIC_NAME.to_string(),
                value: "test_metric".to_string(),
            },
            Label {
                name: "foo".to_string(),
                value: "".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "prod".to_string(),
            },
        ];
        forward.series.insert(
            1,
            SeriesSpec {
                unit: None,
                metric_type: Some(MetricType::Gauge),
                labels: series1_labels.clone(),
            },
        );
        for label in series1_labels {
            inverted
                .postings
                .entry(label)
                .or_default()
                .value_mut()
                .insert(1);
        }

        // Series with no foo, env="prod" (should match)
        let series2_labels = vec![
            Label {
                name: METRIC_NAME.to_string(),
                value: "test_metric".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "prod".to_string(),
            },
        ];
        forward.series.insert(
            2,
            SeriesSpec {
                unit: None,
                metric_type: Some(MetricType::Gauge),
                labels: series2_labels.clone(),
            },
        );
        for label in series2_labels {
            inverted
                .postings
                .entry(label)
                .or_default()
                .value_mut()
                .insert(2);
        }

        // Series with foo="bar", env="prod" (should NOT match)
        let series3_labels = vec![
            Label {
                name: METRIC_NAME.to_string(),
                value: "test_metric".to_string(),
            },
            Label {
                name: "foo".to_string(),
                value: "bar".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "prod".to_string(),
            },
        ];
        forward.series.insert(
            3,
            SeriesSpec {
                unit: None,
                metric_type: Some(MetricType::Gauge),
                labels: series3_labels.clone(),
            },
        );
        for label in series3_labels {
            inverted
                .postings
                .entry(label)
                .or_default()
                .value_mut()
                .insert(3);
        }

        // Series with foo="", env="staging" (should NOT match)
        let series4_labels = vec![
            Label {
                name: METRIC_NAME.to_string(),
                value: "test_metric".to_string(),
            },
            Label {
                name: "foo".to_string(),
                value: "".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "staging".to_string(),
            },
        ];
        forward.series.insert(
            4,
            SeriesSpec {
                unit: None,
                metric_type: Some(MetricType::Gauge),
                labels: series4_labels.clone(),
            },
        );
        for label in series4_labels {
            inverted
                .postings
                .entry(label)
                .or_default()
                .value_mut()
                .insert(4);
        }

        // when: query with foo="" AND env="prod"
        let selector = VectorSelector {
            name: Some("test_metric".to_string()),
            matchers: Matchers {
                matchers: vec![
                    Matcher::new(MatchOp::Equal, "foo", ""),
                    Matcher::new(MatchOp::Equal, "env", "prod"),
                ],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluate_on_indexes(&forward, &inverted, &selector).unwrap();

        // then: should find only 2 series that match both conditions
        assert_eq!(
            result.len(),
            2,
            "Should find 2 series matching both foo='' and env='prod'"
        );
        assert!(result.contains(&1)); // foo="", env="prod"
        assert!(result.contains(&2)); // no foo label, env="prod"
        assert!(!result.contains(&3)); // foo="bar", env="prod" - fails foo=""
        assert!(!result.contains(&4)); // foo="", env="staging" - fails env="prod"
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

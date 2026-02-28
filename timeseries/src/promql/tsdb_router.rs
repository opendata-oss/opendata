use std::collections::{HashMap, HashSet};

use super::evaluator::{CachedQueryReader, compute_preload_ranges};
use super::selector::evaluate_selector_with_reader;
use crate::model::SeriesId;
use crate::model::TimeBucket;
use crate::query::QueryReader;
use promql_parser::parser::{EvalStmt, Expr, VectorSelector};

/// Compute the disjoint preload ranges for bucket discovery.
///
/// Uses `compute_preload_ranges` to account for `offset`/`@` modifiers.
/// Falls back to a single `[(default_start, default_end)]` for
/// selector-free expressions.
pub(crate) fn preload_ranges(
    stmt: &EvalStmt,
    default_start: i64,
    default_end: i64,
) -> Vec<(i64, i64)> {
    let ranges = compute_preload_ranges(&stmt.expr, stmt.start, stmt.end, stmt.lookback_delta);
    if ranges.is_empty() {
        vec![(default_start, default_end)]
    } else {
        ranges
    }
}

/// Parse a match[] selector string into a VectorSelector
pub(crate) fn parse_selector(selector: &str) -> Result<VectorSelector, String> {
    let expr = promql_parser::parser::parse(selector).map_err(|e| e.to_string())?;
    match expr {
        Expr::VectorSelector(vs) => Ok(vs),
        _ => Err("Expected a vector selector".to_string()),
    }
}

/// Get all series IDs matching any of the given selectors (UNION)
pub(crate) async fn get_matching_series<R: QueryReader>(
    reader: &R,
    bucket: TimeBucket,
    matches: &[String],
) -> Result<HashSet<SeriesId>, String> {
    let mut all_series = HashSet::new();

    let mut cached_reader = CachedQueryReader::new(reader);
    for selector_str in matches {
        let selector = parse_selector(selector_str)?;
        let series = evaluate_selector_with_reader(&mut cached_reader, bucket, &selector)
            .await
            .map_err(|e| e.to_string())?;
        all_series.extend(series);
    }

    Ok(all_series)
}

/// Get all series across multiple buckets, with fingerprint-based deduplication
pub(crate) async fn get_matching_series_multi_bucket<R: QueryReader>(
    reader: &R,
    buckets: &[TimeBucket],
    matches: &[String],
) -> Result<HashMap<TimeBucket, HashSet<SeriesId>>, String> {
    let mut bucket_series_map = HashMap::new();

    for &bucket in buckets {
        let series = get_matching_series(reader, bucket, matches).await?;
        if !series.is_empty() {
            bucket_series_map.insert(bucket, series);
        }
    }

    Ok(bucket_series_map)
}

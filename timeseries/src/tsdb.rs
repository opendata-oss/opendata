use std::collections::{HashMap, HashSet};
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use common::Storage;
use futures::TryStreamExt;
use moka::future::Cache;
use promql_parser::parser::{EvalStmt, Expr, VectorSelector};
use tokio::sync::RwLock;
use tracing::{Instrument, error};

use crate::config::{SampleStorageLayout, TsdbRuntimeConfig};
use crate::error::QueryError;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::load_coordinator::ReadLoadCoordinator;
use crate::minitsdb::{MiniQueryReader, MiniTsdb};
use crate::model::{
    InstantSample, Label, Labels, MetricMetadata, QueryOptions, QueryValue, RangeSample, Sample,
    Series, SeriesId, TimeBucket,
};
use crate::promql::evaluator::{CachedQueryReader, Evaluator, ExprResult, compute_preload_ranges};
use crate::promql::selector::evaluate_selector_with_reader;
use crate::query::{BucketQueryReader, QueryReader, SampleLocator};
use crate::query_io::{self, QueryIoCollector};
use crate::storage::OpenTsdbStorageReadExt;
use crate::util::Result;

#[cfg(feature = "http-server")]
use crate::server::metrics::{
    Metrics, QueryLabels, QueryOperation, QueryOperationLabels, QueryPhase, QueryPhaseLabels,
    QueryStatus, QueryWorkKind, QueryWorkLabels,
};

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
fn parse_selector(selector: &str) -> std::result::Result<VectorSelector, String> {
    let expr = promql_parser::parser::parse(selector).map_err(|e| e.to_string())?;
    match expr {
        Expr::VectorSelector(vs) => Ok(vs),
        _ => Err("Expected a vector selector".to_string()),
    }
}

/// Get all series IDs matching any of the given selectors (UNION)
async fn get_matching_series<R: QueryReader>(
    reader: &R,
    bucket: TimeBucket,
    matches: &[String],
) -> std::result::Result<HashSet<SeriesId>, String> {
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
async fn get_matching_series_multi_bucket<R: QueryReader>(
    reader: &R,
    buckets: &[TimeBucket],
    matches: &[String],
) -> std::result::Result<HashMap<TimeBucket, HashSet<SeriesId>>, String> {
    let mut bucket_series_map = HashMap::new();

    for &bucket in buckets {
        let series = get_matching_series(reader, bucket, matches).await?;
        if !series.is_empty() {
            bucket_series_map.insert(bucket, series);
        }
    }

    Ok(bucket_series_map)
}

// ── TsdbReadEngine trait ─────────────────────────────────────────────────
//
// Factors out the 5 duplicated eval/find methods shared between `Tsdb`
// (writer) and `TimeSeriesDbReader` (reader). Each implementor provides
// its own `QueryReader` construction; the query logic lives once in the
// default methods.

#[async_trait]
pub(crate) trait TsdbReadEngine: Send + Sync {
    type QR: QueryReader + Send + Sync;

    /// Build a query reader spanning `[start, end]` (seconds).
    async fn make_query_reader(&self, start: i64, end: i64) -> Result<Self::QR>;

    /// Build a query reader spanning a set of disjoint ranges (seconds).
    async fn make_query_reader_for_ranges(&self, ranges: &[(i64, i64)]) -> Result<Self::QR>;

    /// Optional load coordinator for I/O budgeting.
    fn load_coordinator(&self) -> Option<&ReadLoadCoordinator> {
        None
    }

    // ── Provided: 5 default methods written once ──

    /// Evaluate an instant PromQL query, returning typed `InstantSample`s.
    async fn eval_query(
        &self,
        query: &str,
        time: Option<std::time::SystemTime>,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError> {
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

        let query_time = time.unwrap_or_else(std::time::SystemTime::now);
        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start: query_time,
            end: query_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };

        let query_time_secs = query_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let lookback_start_secs = query_time
            .checked_sub(lookback_delta)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs() as i64;

        let ranges = preload_ranges(&stmt, lookback_start_secs, query_time_secs);
        let reader = self.make_query_reader_for_ranges(&ranges).await?;

        evaluate_instant(&reader, stmt, query_time, self.load_coordinator()).await
    }

    /// Evaluate a range PromQL query, returning typed `RangeSample`s.
    async fn eval_query_range(
        &self,
        query: &str,
        start: std::time::SystemTime,
        end: std::time::SystemTime,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start,
            end,
            interval: step,
            lookback_delta,
        };

        let default_start_secs = start
            .checked_sub(lookback_delta)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs() as i64;
        let default_end_secs = end.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        let ranges = preload_ranges(&stmt, default_start_secs, default_end_secs);

        // Install I/O collector scope around reader construction + evaluation
        // so bucket-list reads during reader construction are captured.
        let io_collector = QueryIoCollector::new();
        let result = query_io::run_with_collector(&io_collector, async {
            let reader = self.make_query_reader_for_ranges(&ranges).await?;
            let (result, _stats) = evaluate_range(&reader, stmt, self.load_coordinator()).await?;
            Ok::<_, QueryError>(result)
        })
        .await?;
        Ok(result)
    }

    /// Discover series matching any of the given selectors.
    async fn find_series(
        &self,
        matchers: &[&str],
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_series(&reader, matchers).await
    }

    /// Discover label names, optionally filtered by matchers.
    async fn find_labels(
        &self,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_labels(&reader, matchers).await
    }

    /// Discover values for a specific label, optionally filtered by matchers.
    async fn find_label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_label_values(&reader, label_name, matchers).await
    }
}

/// Evaluate a range query using Rust range bounds, converting to
/// `(start, end)` exactly once before dispatching to `TsdbReadEngine`.
pub(crate) async fn eval_query_range_bounds<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    query: &str,
    range: impl RangeBounds<SystemTime>,
    step: Duration,
    opts: &QueryOptions,
) -> std::result::Result<Vec<RangeSample>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_system_time(range);
    E::eval_query_range(engine, query, start, end, step, opts).await
}

/// Discover series over a time range specified as Rust range bounds.
pub(crate) async fn find_series_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    matchers: &[&str],
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<Labels>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_series(engine, matchers, start, end).await
}

/// Discover label names over a time range specified as Rust range bounds.
pub(crate) async fn find_labels_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    matchers: Option<&[&str]>,
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<String>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_labels(engine, matchers, start, end).await
}

/// Discover label values over a time range specified as Rust range bounds.
pub(crate) async fn find_label_values_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    label_name: &str,
    matchers: Option<&[&str]>,
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<String>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_label_values(engine, label_name, matchers, start, end).await
}

// ── Shared evaluation free functions ────────────────────────────────

/// Evaluate an instant PromQL query against the given reader.
pub(crate) async fn evaluate_instant(
    reader: &impl QueryReader,
    stmt: EvalStmt,
    query_time: std::time::SystemTime,
    coordinator: Option<&ReadLoadCoordinator>,
) -> std::result::Result<QueryValue, QueryError> {
    let mut evaluator = if let Some(coord) = coordinator {
        Evaluator::with_coordinator(reader, coord.clone())
    } else {
        Evaluator::new(reader)
    };
    let result = evaluator.evaluate(stmt).await?;

    match result {
        ExprResult::Scalar(value) => {
            let timestamp_ms = query_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
            Ok(QueryValue::Scalar {
                timestamp_ms,
                value,
            })
        }
        ExprResult::InstantVector(samples) => Ok(QueryValue::Vector(
            samples
                .into_iter()
                .map(|s| InstantSample {
                    labels: s.labels.into_labels(),
                    timestamp_ms: s.timestamp_ms,
                    value: s.value,
                })
                .collect(),
        )),
        ExprResult::RangeVector(samples) => Ok(QueryValue::Matrix(
            samples
                .into_iter()
                .map(|s| RangeSample {
                    labels: s.labels.into_labels(),
                    samples: s
                        .values
                        .into_iter()
                        .map(|v| (v.timestamp_ms, v.value))
                        .collect(),
                })
                .collect(),
        )),
    }
}

/// Evaluate a range PromQL query against the given reader.
/// Returns the result and the EvalStats for metrics publishing.
pub(crate) async fn evaluate_range(
    reader: &impl QueryReader,
    stmt: EvalStmt,
    coordinator: Option<&ReadLoadCoordinator>,
) -> std::result::Result<(Vec<RangeSample>, crate::promql::evaluator::EvalStats), QueryError> {
    let total_start = std::time::Instant::now();
    let start = stmt.start;
    let end = stmt.end;
    let step = stmt.interval;
    let lookback_delta = stmt.lookback_delta;

    if step.is_zero() {
        return Err(QueryError::InvalidQuery(
            "step must be greater than zero".to_string(),
        ));
    }

    let mut series_map: HashMap<Labels, Vec<(i64, f64)>> = HashMap::new();
    let mut evaluator = if let Some(coord) = coordinator {
        Evaluator::with_coordinator(reader, coord.clone())
    } else {
        Evaluator::new(reader)
    };

    // Preload VectorSelector data for all steps
    let start_ms = start.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let end_ms = end.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let step_ms = step.as_millis() as i64;
    let lookback_delta_ms = lookback_delta.as_millis() as i64;

    let preload_span = tracing::info_span!("preload_for_range");
    evaluator
        .preload_for_range(&stmt.expr, start_ms, end_ms, step_ms, lookback_delta_ms)
        .instrument(preload_span)
        .await?;

    let step_loop_start = std::time::Instant::now();
    let mut current_time = start;
    let mut step_count: u64 = 0;

    while current_time <= end {
        let instant_stmt = EvalStmt {
            expr: stmt.expr.clone(),
            start: current_time,
            end: current_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };

        let result = evaluator.evaluate(instant_stmt).await?;
        let timestamp_ms = current_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

        match result {
            ExprResult::InstantVector(samples) => {
                for sample in samples {
                    let labels = sample.labels.into_labels();
                    series_map
                        .entry(labels)
                        .or_default()
                        .push((sample.timestamp_ms, sample.value));
                }
            }
            ExprResult::Scalar(value) => {
                let labels = Labels::empty();
                series_map
                    .entry(labels)
                    .or_default()
                    .push((timestamp_ms, value));
            }
            ExprResult::RangeVector(_) => {
                return Err(QueryError::Execution(
                    "range vectors not supported in range query evaluation".to_string(),
                ));
            }
        }

        step_count += 1;
        current_time += step;
    }

    let step_loop_ms = step_loop_start.elapsed().as_millis() as u64;
    let mut stats = evaluator.into_stats();
    stats.step_loop_ms = step_loop_ms;

    // Snapshot physical I/O stats from the task-local collector (if active).
    // The caller is responsible for installing the collector scope via
    // query_io::run_with_collector() so that reader construction and
    // evaluation are both captured.
    stats.io = query_io::snapshot_current();
    let io = &stats.io;

    tracing::info!(
        step_count,
        total_ms = total_start.elapsed().as_millis() as u64,
        // Phase timing
        preload_ms = stats.preload_ms,
        step_loop_ms = stats.step_loop_ms,
        // Existing counters
        list_buckets_calls = stats.list_buckets_calls,
        list_buckets_misses = stats.list_buckets_misses,
        selector_hits = stats.selector_cache_hits,
        selector_misses = stats.selector_misses,
        forward_index_hits = stats.forward_index_cache_hits,
        forward_index_misses = stats.forward_index_misses,
        sample_hits = stats.sample_cache_hits,
        sample_misses = stats.sample_misses,
        samples_loaded = stats.samples_loaded,
        series_meta_hits = stats.series_meta_cache_hits,
        series_meta_misses = stats.series_meta_misses,
        preload_selectors = stats.preload_selectors,
        preload_series = stats.preload_series,
        preload_hits = stats.preload_hits,
        unique_series = series_map.len(),
        // Miss latency totals
        list_buckets_miss_ms = stats.list_buckets_miss_ms,
        forward_index_miss_ms = stats.forward_index_miss_ms,
        sample_miss_ms = stats.sample_miss_ms,
        selector_miss_ms = stats.selector_miss_ms,
        // Per-bucket preload aggregates
        preload_bucket_ms_max = stats.preload_bucket_ms_max,
        preload_bucket_ms_sum = stats.preload_bucket_ms_sum,
        // Load coordinator stats
        sample_queue_wait_ms = stats.sample_queue_wait_ms,
        sample_load_ms = stats.sample_load_ms,
        sample_permit_acquires = stats.sample_permit_acquires,
        metadata_queue_wait_ms = stats.metadata_queue_wait_ms,
        metadata_load_ms = stats.metadata_load_ms,
        metadata_permit_acquires = stats.metadata_permit_acquires,
        parallel_sample_loads = stats.parallel_sample_loads,
        // Phase B1: parallel selector + metadata resolution
        parallel_selector_wall_ms = stats.parallel_selector_wall_ms,
        parallel_selector_sum_ms = stats.parallel_selector_sum_ms,
        parallel_selector_count = stats.parallel_selector_count,
        // Phase B3: parallel sample loading
        parallel_sample_wall_ms = stats.parallel_sample_wall_ms,
        parallel_sample_sum_ms = stats.parallel_sample_sum_ms,
        parallel_sample_bucket_count = stats.parallel_sample_bucket_count,
        // Metric-prefixed layout experiment
        sample_distinct_metrics = stats.sample_distinct_metrics,
        sample_series_per_metric_max = stats.sample_series_per_metric_max,
        // Logical metadata entry counts
        bucket_list_entries_loaded = stats.bucket_list_entries_loaded,
        forward_index_entries_loaded = stats.forward_index_entries_loaded,
        sample_series_loaded = stats.sample_series_loaded,
        sample_logical_bytes = stats.samples_loaded * 16,
        // Physical I/O stats
        io_bucket_list_bytes = io.bucket_list_bytes,
        io_inverted_index_bytes = io.inverted_index_bytes,
        io_inverted_index_records = io.inverted_index_records,
        io_forward_index_bytes = io.forward_index_bytes,
        io_forward_index_records = io.forward_index_records,
        io_sample_get_bytes = io.sample_get_bytes,
        io_sample_get_records = io.sample_get_records,
        io_sample_metric_get_bytes = io.sample_metric_get_bytes,
        io_sample_metric_get_records = io.sample_metric_get_records,
        io_metadata_bytes = io.metadata_bytes(),
        io_sample_bytes = io.sample_bytes(),
        io_physical_bytes_total = io.physical_bytes_total(),
        "evaluate_range complete"
    );

    let result: Vec<RangeSample> = series_map
        .into_iter()
        .map(|(labels, samples)| RangeSample { labels, samples })
        .collect();
    Ok((result, stats))
}

/// Publish query metrics into the Prometheus registry.
#[cfg(feature = "http-server")]
fn publish_query_metrics(metrics: &Metrics, total_duration_secs: f64, is_ok: bool) {
    let op = QueryOperation::QueryRange;
    let status = if is_ok {
        QueryStatus::Ok
    } else {
        QueryStatus::Error
    };

    metrics
        .query
        .requests_total
        .get_or_create(&QueryLabels {
            operation: op.clone(),
            status: status.clone(),
        })
        .inc();
    metrics
        .query
        .duration_seconds
        .get_or_create(&QueryLabels {
            operation: op,
            status,
        })
        .observe(total_duration_secs);
}

/// Publish detailed phase-level query metrics from EvalStats.
#[cfg(feature = "http-server")]
pub(crate) fn publish_query_phase_metrics(
    metrics: &Metrics,
    stats: &crate::promql::evaluator::EvalStats,
) {
    let op = QueryOperation::QueryRange;

    // Phase wall durations
    metrics
        .query
        .phase_wall_duration_seconds
        .get_or_create(&QueryPhaseLabels {
            operation: op.clone(),
            phase: QueryPhase::Preload,
        })
        .observe(stats.preload_ms as f64 / 1000.0);
    metrics
        .query
        .phase_wall_duration_seconds
        .get_or_create(&QueryPhaseLabels {
            operation: op.clone(),
            phase: QueryPhase::Selector,
        })
        .observe(stats.parallel_selector_wall_ms as f64 / 1000.0);
    metrics
        .query
        .phase_wall_duration_seconds
        .get_or_create(&QueryPhaseLabels {
            operation: op.clone(),
            phase: QueryPhase::Sample,
        })
        .observe(stats.parallel_sample_wall_ms as f64 / 1000.0);
    metrics
        .query
        .phase_wall_duration_seconds
        .get_or_create(&QueryPhaseLabels {
            operation: op.clone(),
            phase: QueryPhase::StepLoop,
        })
        .observe(stats.step_loop_ms as f64 / 1000.0);

    // Phase work durations
    metrics
        .query
        .phase_work_duration_seconds
        .get_or_create(&QueryWorkLabels {
            operation: op.clone(),
            kind: QueryWorkKind::MetadataQueueWait,
        })
        .observe(stats.metadata_queue_wait_ms as f64 / 1000.0);
    metrics
        .query
        .phase_work_duration_seconds
        .get_or_create(&QueryWorkLabels {
            operation: op.clone(),
            kind: QueryWorkKind::MetadataLoad,
        })
        .observe(stats.metadata_load_ms as f64 / 1000.0);
    metrics
        .query
        .phase_work_duration_seconds
        .get_or_create(&QueryWorkLabels {
            operation: op.clone(),
            kind: QueryWorkKind::SampleQueueWait,
        })
        .observe(stats.sample_queue_wait_ms as f64 / 1000.0);
    metrics
        .query
        .phase_work_duration_seconds
        .get_or_create(&QueryWorkLabels {
            operation: op.clone(),
            kind: QueryWorkKind::SampleLoad,
        })
        .observe(stats.sample_load_ms as f64 / 1000.0);

    // Volume counters
    metrics
        .query
        .samples_loaded_total
        .get_or_create(&QueryOperationLabels {
            operation: op.clone(),
        })
        .inc_by(stats.samples_loaded);
    metrics
        .query
        .forward_index_series_loaded_total
        .get_or_create(&QueryOperationLabels { operation: op })
        .inc_by(stats.preload_series);
}

/// Discover series matching any of the given selectors.
pub(crate) async fn discover_series(
    reader: &impl QueryReader,
    matchers: &[&str],
) -> std::result::Result<Vec<Labels>, QueryError> {
    if matchers.is_empty() {
        return Err(QueryError::InvalidQuery(
            "at least one match[] required".to_string(),
        ));
    }

    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let matches: Vec<String> = matchers.iter().map(|s| s.to_string()).collect();
    let bucket_series_map = get_matching_series_multi_bucket(reader, &buckets, &matches)
        .await
        .map_err(QueryError::InvalidQuery)?;

    let mut unique_series: HashSet<Labels> = HashSet::new();

    for (bucket, series_ids) in bucket_series_map {
        let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
        if series_ids_vec.is_empty() {
            continue;
        }
        let forward_index = reader.forward_index(&bucket, &series_ids_vec).await?;
        for (_id, spec) in forward_index.all_series() {
            let mut sorted_labels = spec.labels.clone();
            sorted_labels.sort();
            unique_series.insert(Labels::new(sorted_labels));
        }
    }

    let mut result: Vec<Labels> = unique_series.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Discover label names, optionally filtered by matchers.
pub(crate) async fn discover_labels(
    reader: &impl QueryReader,
    matchers: Option<&[&str]>,
) -> std::result::Result<Vec<String>, QueryError> {
    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let mut label_names: HashSet<String> = HashSet::new();

    match matchers {
        Some(matches) if !matches.is_empty() => {
            let matches: Vec<String> = matches.iter().map(|s| s.to_string()).collect();
            let bucket_series_map = get_matching_series_multi_bucket(reader, &buckets, &matches)
                .await
                .map_err(QueryError::InvalidQuery)?;

            for (bucket, series_ids) in bucket_series_map {
                let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
                if series_ids_vec.is_empty() {
                    continue;
                }
                let forward_index = reader.forward_index(&bucket, &series_ids_vec).await?;
                for (_id, spec) in forward_index.all_series() {
                    for attr in &spec.labels {
                        label_names.insert(attr.name.clone());
                    }
                }
            }
        }
        _ => {
            for bucket in buckets {
                let inverted_index = reader.all_inverted_index(&bucket).await?;
                for attr in inverted_index.all_keys() {
                    label_names.insert(attr.name);
                }
            }
        }
    }

    let mut result: Vec<String> = label_names.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Discover values for a specific label, optionally filtered by matchers.
pub(crate) async fn discover_label_values(
    reader: &impl QueryReader,
    label_name: &str,
    matchers: Option<&[&str]>,
) -> std::result::Result<Vec<String>, QueryError> {
    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let mut values: HashSet<String> = HashSet::new();

    match matchers {
        Some(matches) if !matches.is_empty() => {
            let matches: Vec<String> = matches.iter().map(|s| s.to_string()).collect();
            let bucket_series_map = get_matching_series_multi_bucket(reader, &buckets, &matches)
                .await
                .map_err(QueryError::InvalidQuery)?;

            for (bucket, series_ids) in bucket_series_map {
                let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
                if series_ids_vec.is_empty() {
                    continue;
                }
                let forward_index = reader.forward_index(&bucket, &series_ids_vec).await?;
                for (_id, spec) in forward_index.all_series() {
                    for attr in &spec.labels {
                        if attr.name == label_name {
                            values.insert(attr.value.clone());
                        }
                    }
                }
            }
        }
        _ => {
            for bucket in buckets {
                let label_vals = reader.label_values(&bucket, label_name).await?;
                values.extend(label_vals);
            }
        }
    }

    let mut result: Vec<String> = values.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Multi-bucket time series database.
///
/// Tsdb manages multiple MiniTsdb instances (one per time bucket) and provides
/// a unified QueryReader interface that merges results across buckets.
pub(crate) struct Tsdb {
    storage: Arc<dyn Storage>,

    // TODO(rohan): weird things can happen if these get out of sync
    //  (e.g. ingest cache purged while query cache is present)
    /// TTI cache (15 min idle) for buckets being actively ingested into.
    /// Also used during queries - checked first before query_cache.
    ingest_cache: Cache<TimeBucket, Arc<MiniTsdb>>,

    /// LRU cache (50 max) for read-only query buckets.
    /// Only populated for buckets NOT in ingest_cache.
    query_cache: Cache<TimeBucket, Arc<MiniTsdb>>,

    // Metadata catalog (keyed by metric name)
    pub(crate) metadata_catalog: RwLock<HashMap<String, Vec<MetricMetadata>>>,

    load_coordinator: ReadLoadCoordinator,
    pub(crate) runtime_config: TsdbRuntimeConfig,
    #[cfg(feature = "http-server")]
    metrics: std::sync::OnceLock<Arc<Metrics>>,
    warmer_handle: tokio::sync::Mutex<Option<crate::metadata_warmer::MetadataWarmerHandle>>,
}

impl Tsdb {
    pub(crate) fn new(storage: Arc<dyn Storage>) -> Self {
        Self::with_runtime_config(storage, TsdbRuntimeConfig::from_env())
    }

    pub(crate) fn with_runtime_config(
        storage: Arc<dyn Storage>,
        config: TsdbRuntimeConfig,
    ) -> Self {
        // TTI cache: 15 minute idle timeout for ingest buckets
        let ingest_cache = Cache::builder()
            .time_to_idle(Duration::from_secs(15 * 60))
            .build();

        // LRU cache: max 50 buckets for query
        let query_cache = Cache::builder().max_capacity(50).build();

        let load_coordinator = ReadLoadCoordinator::from_config(&config.read_load);

        Self {
            storage,
            ingest_cache,
            query_cache,
            metadata_catalog: RwLock::new(HashMap::new()),
            load_coordinator,
            runtime_config: config,
            #[cfg(feature = "http-server")]
            metrics: std::sync::OnceLock::new(),
            warmer_handle: tokio::sync::Mutex::new(None),
        }
    }

    #[cfg(feature = "http-server")]
    pub(crate) fn attach_metrics(&self, metrics: Arc<Metrics>) {
        let _ = self.metrics.set(metrics);
    }

    #[cfg(feature = "http-server")]
    pub(crate) fn metrics(&self) -> Option<&Arc<Metrics>> {
        self.metrics.get()
    }

    pub(crate) fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Start background tasks (metadata warmer).
    /// Should be called after attach_metrics() and before serving queries.
    #[cfg(feature = "http-server")]
    pub(crate) async fn start_background_tasks(&self) {
        let handle = crate::metadata_warmer::start_metadata_warmer(
            self.runtime_config.metadata_warm.clone(),
            self.storage.clone(),
            self.load_coordinator.clone(),
            self.metrics.get().cloned(),
        );
        *self.warmer_handle.lock().await = handle;
    }

    /// Stop background tasks before shutdown.
    pub(crate) async fn stop_background_tasks(&self) {
        if let Some(handle) = self.warmer_handle.lock().await.take() {
            handle.stop().await;
        }
    }

    /// Get or create a MiniTsdb for ingestion into a specific bucket.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn get_or_create_for_ingest(
        &self,
        bucket: TimeBucket,
    ) -> Result<Arc<MiniTsdb>> {
        // Try ingest cache first
        if let Some(mini) = self.ingest_cache.get(&bucket).await {
            return Ok(mini);
        }

        // Load from storage and put in ingest cache
        let mini = Arc::new(
            MiniTsdb::load(
                bucket,
                self.storage.clone(),
                self.runtime_config.sample_storage_layout,
            )
            .await?,
        );
        self.ingest_cache.insert(bucket, mini.clone()).await;
        Ok(mini)
    }

    /// Get a MiniTsdb for a bucket, checking ingest cache first, then query cache.
    async fn get_bucket(&self, bucket: TimeBucket) -> Result<Arc<MiniTsdb>> {
        // 1. Check ingest cache first (has freshest data)
        if let Some(mini) = self.ingest_cache.get(&bucket).await {
            return Ok(mini);
        }

        // 2. Check query cache
        if let Some(mini) = self.query_cache.get(&bucket).await {
            return Ok(mini);
        }

        // 3. Load from storage into query cache (NOT ingest cache)
        let mini = Arc::new(
            MiniTsdb::load(
                bucket,
                self.storage.clone(),
                self.runtime_config.sample_storage_layout,
            )
            .await?,
        );
        self.query_cache.insert(bucket, mini.clone()).await;
        Ok(mini)
    }

    /// Create a QueryReader for a time range.
    /// This discovers all buckets covering the range and returns a TsdbQueryReader
    /// that properly handles bucket-scoped series IDs.
    pub(crate) async fn query_reader(
        &self,
        start_secs: i64,
        end_secs: i64,
    ) -> Result<TsdbQueryReader> {
        // TODO(rohan): its weird that we use a snapshot here and the minitsdbs have a different snapshot
        let snapshot = self.storage.snapshot().await?;

        // Discover buckets that cover the query range
        let buckets = snapshot
            .get_buckets_in_range(Some(start_secs), Some(end_secs))
            .await?;

        // Load MiniTsdbs for each bucket (from cache or storage)
        let mut readers = Vec::new();
        for bucket in buckets {
            let mini = self.get_bucket(bucket).await?;
            let reader = mini.query_reader();
            readers.push((bucket, reader));
        }

        Ok(TsdbQueryReader::new(
            readers,
            self.runtime_config.sample_storage_layout,
        ))
    }

    /// Create a QueryReader for a set of disjoint time ranges.
    /// Discovers all buckets overlapping any range and returns a TsdbQueryReader.
    pub(crate) async fn query_reader_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> Result<TsdbQueryReader> {
        let snapshot = self.storage.snapshot().await?;
        let buckets = snapshot.get_buckets_for_ranges(ranges).await?;

        let mut readers = Vec::new();
        for bucket in buckets {
            let mini = self.get_bucket(bucket).await?;
            let reader = mini.query_reader();
            readers.push((bucket, reader));
        }

        Ok(TsdbQueryReader::new(
            readers,
            self.runtime_config.sample_storage_layout,
        ))
    }

    /// Flush all dirty buckets to durable storage.
    ///
    /// First flushes each bucket's delta to the storage memtable in parallel,
    /// then issues a single `storage.flush()` to persist everything durably.
    pub(crate) async fn flush(&self) -> Result<()> {
        // Note: moka's iter() returns a clone of the current entries
        let futs: futures::stream::FuturesUnordered<_> = self
            .ingest_cache
            .iter()
            .map(|(_, mini)| async move { mini.flush_written().await })
            .collect();
        futs.try_collect::<Vec<_>>().await?;

        self.storage.flush().await?;
        Ok(())
    }

    pub(crate) async fn close(self) -> Result<()> {
        self.stop_background_tasks().await;
        self.flush().await?;
        self.storage.close().await?;
        Ok(())
    }

    /// Ingest series into the TSDB.
    /// Each series is split by time bucket based on sample timestamps.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            series_count = series_list.len(),
            total_samples = tracing::field::Empty,
            buckets_touched = tracing::field::Empty
        )
    )]
    pub(crate) async fn ingest_samples(&self, series_list: Vec<Series>) -> Result<()> {
        let mut bucket_series_map: HashMap<TimeBucket, Vec<Series>> = HashMap::new();
        let mut total_samples = 0;

        // First pass: group all series by bucket
        for series in series_list {
            let series_sample_count = series.samples.len();
            total_samples += series_sample_count;

            if let Some(metric_name) = series
                .labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.clone())
            {
                let entry = MetricMetadata {
                    metric_name: metric_name.clone(),
                    metric_type: series.metric_type,
                    description: series.description.clone(),
                    unit: series.unit.clone(),
                };
                let mut catalog = self.metadata_catalog.write().await;
                let entries = catalog.entry(metric_name).or_default();
                if !entries.contains(&entry) {
                    entries.push(entry);
                }
            }

            // Group samples by bucket for this series
            let mut bucket_samples: HashMap<TimeBucket, Vec<Sample>> = HashMap::new();

            for sample in series.samples {
                let bucket = TimeBucket::round_to_hour(
                    std::time::UNIX_EPOCH
                        + std::time::Duration::from_millis(sample.timestamp_ms as u64),
                )?;
                bucket_samples.entry(bucket).or_default().push(sample);
            }

            // Create a series for each bucket and add to bucket_series_map
            for (bucket, samples) in bucket_samples {
                let bucket_series = Series {
                    labels: series.labels.clone(),
                    metric_type: series.metric_type,
                    unit: series.unit.clone(),
                    description: series.description.clone(),
                    samples,
                };
                bucket_series_map
                    .entry(bucket)
                    .or_default()
                    .push(bucket_series);
            }
        }

        let buckets_touched = bucket_series_map.len();

        // Second pass: ingest all series for each bucket in a single batch
        for (bucket, series_list) in bucket_series_map {
            let series_count = series_list.len();
            let samples_count: usize = series_list.iter().map(|s| s.samples.len()).sum();

            tracing::debug!(
                bucket = ?bucket,
                series_count = series_count,
                samples_count = samples_count,
                "Ingesting batch into bucket"
            );

            let mini = match self.get_or_create_for_ingest(bucket).await {
                Ok(mini) => mini,
                Err(err) => {
                    error!("failed to load minitsdb: {:?}: {:?}", bucket, err);
                    return Err(err);
                }
            };
            mini.ingest_batch(&series_list).await?;

            tracing::debug!(
                bucket = ?bucket,
                series_count = series_count,
                samples_count = samples_count,
                "Bucket batch ingestion completed"
            );
        }

        // Record final metrics on the main span
        tracing::Span::current().record("total_samples", total_samples);
        tracing::Span::current().record("buckets_touched", buckets_touched);

        tracing::debug!(
            total_samples = total_samples,
            buckets_touched = buckets_touched,
            "Completed ingesting all samples"
        );

        Ok(())
    }

    /// Evaluate a range PromQL query, returning typed `RangeSample`s.
    ///
    /// This inherent method converts `impl RangeBounds<SystemTime>` to the
    /// `(start, end)` pair expected by `TsdbReadEngine`. It captures EvalStats
    /// for metrics publishing when the http-server feature is enabled.
    pub(crate) async fn eval_query_range(
        &self,
        query: &str,
        range: impl std::ops::RangeBounds<std::time::SystemTime>,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        let t0 = std::time::Instant::now();
        let (start, end) = crate::util::range_bounds_to_system_time(range);

        let eval_result = self
            .eval_query_range_with_stats(query, start, end, step, opts)
            .await;

        #[cfg(feature = "http-server")]
        if let Some(metrics) = self.metrics() {
            let total_secs = t0.elapsed().as_secs_f64();
            let is_ok = eval_result.is_ok();
            publish_query_metrics(metrics, total_secs, is_ok);
            if let Ok((ref _result, ref stats)) = eval_result {
                publish_query_phase_metrics(metrics, stats);
            }
        }
        let _ = t0;

        eval_result.map(|(result, _stats)| result)
    }

    /// Inner implementation: runs the full eval pipeline and returns stats.
    ///
    /// NOTE: This duplicates the parse/stmt/reader logic from
    /// `TsdbReadEngine::eval_query_range` so that `evaluate_range` can be
    /// called directly and its `EvalStats` captured for Prometheus metrics.
    /// The trait default discards stats, and changing the trait return type
    /// would propagate to all implementors (`TimeSeriesDb`, `TimeSeriesDbReader`).
    async fn eval_query_range_with_stats(
        &self,
        query: &str,
        start: std::time::SystemTime,
        end: std::time::SystemTime,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<(Vec<RangeSample>, crate::promql::evaluator::EvalStats), QueryError>
    {
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start,
            end,
            interval: step,
            lookback_delta,
        };

        let default_start_secs = start
            .checked_sub(lookback_delta)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs() as i64;
        let default_end_secs = end.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        let ranges = preload_ranges(&stmt, default_start_secs, default_end_secs);

        // Install I/O collector scope around reader construction + evaluation
        // so bucket-list reads during reader construction are captured.
        let io_collector = QueryIoCollector::new();
        query_io::run_with_collector(&io_collector, async {
            let reader = self.make_query_reader_for_ranges(&ranges).await?;
            evaluate_range(&reader, stmt, self.load_coordinator()).await
        })
        .await
    }

    /// Return metadata for all (or a specific) metric.
    pub(crate) async fn find_metadata(
        &self,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        let catalog = self.metadata_catalog.read().await;

        let entries: Vec<MetricMetadata> = match metric {
            Some(name) => catalog.get(name).cloned().unwrap_or_default(),
            None => catalog.values().flatten().cloned().collect(),
        };

        Ok(entries)
    }
}

#[async_trait]
impl TsdbReadEngine for Tsdb {
    type QR = TsdbQueryReader;

    async fn make_query_reader(&self, start: i64, end: i64) -> Result<TsdbQueryReader> {
        self.query_reader(start, end).await
    }

    async fn make_query_reader_for_ranges(&self, ranges: &[(i64, i64)]) -> Result<TsdbQueryReader> {
        self.query_reader_for_ranges(ranges).await
    }

    fn load_coordinator(&self) -> Option<&ReadLoadCoordinator> {
        Some(&self.load_coordinator)
    }
}

/// QueryReader implementation that properly handles bucket-scoped series IDs.
pub(crate) struct TsdbQueryReader {
    /// Map from bucket to MiniTsdb for efficient bucket queries
    mini_readers: HashMap<TimeBucket, MiniQueryReader>,
    sample_storage_layout: SampleStorageLayout,
}

impl TsdbQueryReader {
    pub fn new(
        mini_tsdbs: Vec<(TimeBucket, MiniQueryReader)>,
        sample_storage_layout: SampleStorageLayout,
    ) -> Self {
        let bucket_minis = mini_tsdbs.into_iter().collect();
        Self {
            mini_readers: bucket_minis,
            sample_storage_layout,
        }
    }
}

#[async_trait]
impl QueryReader for TsdbQueryReader {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        Ok(self.mini_readers.keys().cloned().collect())
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index(series_ids).await
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.inverted_index(terms).await
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        // TODO: this should be internal error
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.all_inverted_index().await
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.label_values(label_name).await
    }

    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.samples(series_id, start_ms, end_ms).await
    }

    async fn samples_by_locator(
        &self,
        locator: &SampleLocator,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        match self.sample_storage_layout {
            SampleStorageLayout::LegacySeriesId => {
                self.samples(&locator.bucket, locator.series_id, start_ms, end_ms)
                    .await
            }
            SampleStorageLayout::MetricPrefixed => {
                let mini = self.mini_readers.get(&locator.bucket).ok_or_else(|| {
                    crate::error::Error::Internal(format!("Bucket {:?} not found", locator.bucket))
                })?;
                mini.snapshot()
                    .get_metric_samples(
                        &locator.bucket,
                        &locator.metric_name,
                        locator.series_id,
                        start_ms,
                        end_ms,
                    )
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MetricType;
    use crate::promql::evaluator::{EvalLabels, EvalSample};
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::storage::in_memory::InMemoryStorage;
    use opendata_macros::storage_test;

    fn create_sample(
        metric_name: &str,
        label_pairs: Vec<(&str, &str)>,
        timestamp: i64,
        value: f64,
    ) -> Series {
        let mut labels = vec![Label {
            name: "__name__".to_string(),
            value: metric_name.to_string(),
        }];
        for (key, val) in label_pairs {
            labels.push(Label {
                name: key.to_string(),
                value: val.to_string(),
            });
        }
        Series {
            labels,
            unit: None,
            metric_type: Some(MetricType::Gauge),
            description: None,
            samples: vec![Sample {
                timestamp_ms: timestamp,
                value,
            }],
        }
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_create_tsdb_with_caches(storage: Arc<dyn Storage>) {
        // when
        let tsdb = Tsdb::new(storage);

        // then: tsdb is created successfully
        // Sync caches to ensure counts are accurate
        tsdb.ingest_cache.run_pending_tasks().await;
        tsdb.query_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 0);
        assert_eq!(tsdb.query_cache.entry_count(), 0);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_get_or_create_bucket_for_ingest(storage: Arc<dyn Storage>) {
        // given
        let tsdb = Tsdb::new(storage);
        let bucket = TimeBucket::hour(1000);

        // when
        let mini1 = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // then: same Arc is returned (cached)
        assert!(Arc::ptr_eq(&mini1, &mini2));
        tsdb.ingest_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 1);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_use_ingest_cache_during_queries(storage: Arc<dyn Storage>) {
        // given: a bucket in the ingest cache
        let tsdb = Tsdb::new(storage);
        let bucket = TimeBucket::hour(1000);

        // Put bucket in ingest cache
        let mini_ingest = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // when: getting the same bucket for query
        let mini_query = tsdb.get_bucket(bucket).await.unwrap();

        // then: should return the same instance from ingest cache
        assert!(Arc::ptr_eq(&mini_ingest, &mini_query));
        // Query cache should still be empty
        tsdb.query_cache.run_pending_tasks().await;
        assert_eq!(tsdb.query_cache.entry_count(), 0);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_use_query_cache_for_non_ingest_buckets(storage: Arc<dyn Storage>) {
        // given
        let tsdb = Tsdb::new(storage);
        let bucket = TimeBucket::hour(1000);

        // when: getting a bucket not in ingest cache
        let mini1 = tsdb.get_bucket(bucket).await.unwrap();
        let mini2 = tsdb.get_bucket(bucket).await.unwrap();

        // then: same Arc is returned (cached in query cache)
        assert!(Arc::ptr_eq(&mini1, &mini2));
        tsdb.query_cache.run_pending_tasks().await;
        tsdb.ingest_cache.run_pending_tasks().await;
        assert_eq!(tsdb.query_cache.entry_count(), 1);
        assert_eq!(tsdb.ingest_cache.entry_count(), 0);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_ingest_and_query_single_bucket(storage: Arc<dyn Storage>) {
        // given
        let tsdb = Tsdb::new(storage);

        // Use hour-aligned bucket (60 minutes = 1 hour)
        // Bucket at minute 60 covers minutes 60-119, i.e., seconds 3600-7199
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // Ingest a sample with timestamp in the bucket range (seconds 3600-7199)
        // Using 4000 seconds = 4000000 ms
        let sample = create_sample("http_requests", vec![("env", "prod")], 4000000, 42.0);
        mini.ingest(&sample).await.unwrap();

        // Flush to make data visible
        tsdb.flush().await.unwrap();

        // when: query the data with range covering the bucket (seconds 3600-7200)
        let reader = tsdb.query_reader(3600, 7200).await.unwrap();
        let terms = vec![Label {
            name: "__name__".to_string(),
            value: "http_requests".to_string(),
        }];
        let bucket = TimeBucket::hour(60);
        let index = reader.inverted_index(&bucket, &terms).await.unwrap();
        let series_ids: Vec<_> = index.intersect(terms).iter().collect();

        // then
        assert_eq!(series_ids.len(), 1);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_query_across_multiple_buckets_with_evaluator(storage: Arc<dyn Storage>) {
        use crate::promql::evaluator::Evaluator;
        use crate::test_utils::assertions::assert_approx_eq;
        use promql_parser::parser::EvalStmt;
        use std::time::{Duration, UNIX_EPOCH};

        // given: 4 hour-aligned buckets
        // Bucket layout (each bucket is 1 hour = 60 minutes):
        //   Bucket 60:  minutes 60-119,  seconds 3600-7199,   ms 3,600,000-7,199,999
        //   Bucket 120: minutes 120-179, seconds 7200-10799,  ms 7,200,000-10,799,999
        //   Bucket 180: minutes 180-239, seconds 10800-14399, ms 10,800,000-14,399,999
        //   Bucket 240: minutes 240-299, seconds 14400-17999, ms 14,400,000-17,999,999
        let tsdb = Tsdb::new(storage);

        // Buckets 1 & 2: will end up in query cache (ingest, flush, then invalidate from ingest cache)
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);

        // Buckets 3 & 4: will stay in ingest cache
        let bucket3 = TimeBucket::hour(180);
        let bucket4 = TimeBucket::hour(240);

        // Ingest data into buckets 1 & 2
        // Sample timestamps should be well within the bucket and reachable by lookback
        // Bucket 60: covers 3,600,000-7,199,999 ms -> sample at 3,900,000 ms (3900s)
        // Bucket 120: covers 7,200,000-10,799,999 ms -> sample at 7,900,000 ms (7900s)
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        mini1
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                3_900_000,
                10.0,
            ))
            .await
            .unwrap();
        mini1
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                3_900_001,
                15.0,
            ))
            .await
            .unwrap();

        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini2
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                7_900_000,
                20.0,
            ))
            .await
            .unwrap();
        mini2
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                7_900_001,
                25.0,
            ))
            .await
            .unwrap();

        // Flush buckets 1 & 2 to storage
        tsdb.flush().await.unwrap();

        // Invalidate buckets 1 & 2 from ingest cache so they'll be loaded from query cache
        tsdb.ingest_cache.invalidate(&bucket1).await;
        tsdb.ingest_cache.invalidate(&bucket2).await;
        tsdb.ingest_cache.run_pending_tasks().await;

        // Ingest data into buckets 3 & 4 (these stay in ingest cache)
        // Bucket 180: covers 10,800,000-14,399,999 ms -> sample at 11,900,000 ms (11900s)
        // Bucket 240: covers 14,400,000-17,999,999 ms -> sample at 15,900,000 ms (15900s)
        let mini3 = tsdb.get_or_create_for_ingest(bucket3).await.unwrap();
        mini3
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                11_900_000,
                30.0,
            ))
            .await
            .unwrap();
        mini3
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                11_900_001,
                35.0,
            ))
            .await
            .unwrap();

        let mini4 = tsdb.get_or_create_for_ingest(bucket4).await.unwrap();
        mini4
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                15_900_000,
                40.0,
            ))
            .await
            .unwrap();
        mini4
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                15_900_001,
                45.0,
            ))
            .await
            .unwrap();

        // Flush buckets 3 & 4 to storage (data is now visible for queries)
        tsdb.flush().await.unwrap();

        // Verify cache state: 2 in ingest cache, 0 in query cache (query cache populated on read)
        tsdb.ingest_cache.run_pending_tasks().await;
        tsdb.query_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 2);
        assert_eq!(tsdb.query_cache.entry_count(), 0);

        // when: query across all 4 buckets using the evaluator
        // Query range: seconds 3600-18000 covers all 4 buckets
        let reader = tsdb.query_reader(3600, 18000).await.unwrap();

        // Verify cache state after query: 2 in ingest cache, 2 in query cache
        tsdb.ingest_cache.run_pending_tasks().await;
        tsdb.query_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 2);
        assert_eq!(tsdb.query_cache.entry_count(), 2);

        // Use the evaluator to run 4 separate instant queries, one per bucket
        let mut evaluator = Evaluator::new(&reader);
        let query = r#"http_requests"#;
        let lookback = Duration::from_secs(1000);

        // Query times: one point in each bucket where we expect to find data
        // Bucket 60:  sample at 3,900,000 ms (3900s) -> query at 4000s
        // Bucket 120: sample at 7,900,000 ms (7900s) -> query at 8000s
        // Bucket 180: sample at 11,900,000 ms (11900s) -> query at 12000s
        // Bucket 240: sample at 15,900,000 ms (15900s) -> query at 16000s
        // Lookback of 1000s ensures samples are within the window
        let query_times_secs = [4000u64, 8000, 12000, 16000];
        let expected_prod_values = [10.0, 20.0, 30.0, 40.0];
        let expected_staging_values = [15.0, 25.0, 35.0, 45.0];

        for (i, &query_time_secs) in query_times_secs.iter().enumerate() {
            let expr = promql_parser::parser::parse(query).unwrap();
            let query_time = UNIX_EPOCH + Duration::from_secs(query_time_secs);
            let stmt = EvalStmt {
                expr,
                start: query_time,
                end: query_time,
                interval: Duration::from_secs(0),
                lookback_delta: lookback,
            };

            let mut results = evaluator
                .evaluate(stmt)
                .await
                .unwrap()
                .expect_instant_vector("Expected instant vector result");
            results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

            assert_eq!(
                results.len(),
                2,
                "query {} at {}s: expected 2 results",
                i,
                query_time_secs
            );

            // env=prod
            assert_eq!(results[0].labels.get("env"), Some("prod"));
            assert_approx_eq(results[0].value, expected_prod_values[i]);

            // env=staging
            assert_eq!(results[1].labels.get("env"), Some("staging"));
            assert_approx_eq(results[1].value, expected_staging_values[i]);
        }
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_query_across_multiple_buckets_with_different_series_id_mappings(
        storage: Arc<dyn Storage>,
    ) {
        use crate::promql::evaluator::Evaluator;
        use promql_parser::parser::EvalStmt;
        use std::time::{Duration, UNIX_EPOCH};

        // given: Two time buckets with overlapping series but different series IDs
        let tsdb = Tsdb::new(storage);
        // Bucket 1: hour 60 (covers 3,600,000-7,199,999 ms)
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        // Add series in bucket 1: foo{a="b",x="y"} and foo{a="c",x="z"}
        mini1
            .ingest(&create_sample(
                "foo",
                vec![("a", "b"), ("x", "y")],
                3_900_000,
                1.0,
            ))
            .await
            .unwrap();
        mini1
            .ingest(&create_sample(
                "foo",
                vec![("a", "c"), ("x", "z")],
                3_900_001,
                2.0,
            ))
            .await
            .unwrap();
        // Bucket 2: hour 120 (covers 7,200,000-10,799,999 ms)
        let bucket2 = TimeBucket::hour(120);
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        // Add series in bucket 2: foo{a="c",x="z"} and foo{a="d",x="w"}
        mini2
            .ingest(&create_sample(
                "foo",
                vec![("a", "c"), ("x", "z")],
                7_900_000,
                3.0,
            ))
            .await
            .unwrap();
        mini2
            .ingest(&create_sample(
                "foo",
                vec![("a", "d"), ("x", "w")],
                7_900_001,
                4.0,
            ))
            .await
            .unwrap();
        // Flush to storage
        tsdb.flush().await.unwrap();

        // when: Execute a PromQL query that filters by a="c"
        let reader = tsdb.query_reader(3000, 8000).await.unwrap();
        let mut evaluator = Evaluator::new(&reader);
        // Query for foo{a="c"} at time 8000 seconds (in bucket 2)
        let query = r#"foo{a="c"}"#;
        let query_time = UNIX_EPOCH + Duration::from_secs(8000);
        let lookback = Duration::from_secs(5000);
        let expr = promql_parser::parser::parse(query).unwrap();
        let stmt = EvalStmt {
            expr,
            start: query_time,
            end: query_time,
            interval: Duration::from_secs(0),
            lookback_delta: lookback,
        };
        let results = evaluator
            .evaluate(stmt)
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        // then: we should only get the series foo{a="c",x="z"} with value 3.0
        let expected = vec![EvalSample {
            timestamp_ms: 7_900_000,
            value: 3.0,
            labels: EvalLabels::from_pairs(&[("a", "c"), ("x", "z"), ("__name__", "foo")]),
            drop_name: false,
        }];
        assert_eq!(results, expected);
    }

    // ── Native read method tests ─────────────────────────────────────

    async fn create_tsdb_with_data() -> Tsdb {
        let tsdb = Tsdb::new(Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        ))));

        // Ingest two series into bucket at minute 60 (covers 3,600,000–7,199,999 ms)
        let series = vec![
            create_sample("http_requests", vec![("env", "prod")], 4_000_000, 42.0),
            create_sample("http_requests", vec![("env", "staging")], 4_000_000, 10.0),
        ];
        tsdb.ingest_samples(series).await.unwrap();
        tsdb.flush().await.unwrap();
        tsdb
    }

    #[tokio::test]
    async fn eval_query_should_return_instant_vector() {
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("http_requests", Some(query_time), &opts)
            .await
            .unwrap();
        let mut samples = match result {
            QueryValue::Vector(samples) => samples,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| {
            a.labels
                .metric_name()
                .cmp(b.labels.metric_name())
                .then_with(|| a.labels.get("env").cmp(&b.labels.get("env")))
        });

        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("env"), Some("prod"));
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[1].labels.get("env"), Some("staging"));
        assert_eq!(samples[1].value, 10.0);
    }

    #[tokio::test]
    async fn eval_query_should_respect_lookback_delta() {
        // Sample at t=4000s, query at t=4100s (100s later).
        // Default 5m lookback finds it; 10s lookback should not.
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);

        let wide = QueryOptions::default(); // 5m
        let results = tsdb
            .eval_query("http_requests", Some(query_time), &wide)
            .await
            .unwrap()
            .into_matrix();
        assert_eq!(results.len(), 2);

        let narrow = QueryOptions {
            lookback_delta: std::time::Duration::from_secs(10),
        };
        let results = tsdb
            .eval_query("http_requests", Some(query_time), &narrow)
            .await
            .unwrap()
            .into_matrix();
        assert_eq!(
            results.len(),
            0,
            "10s lookback should miss samples 100s ago"
        );
    }

    #[tokio::test]
    async fn eval_query_range_should_respect_lookback_delta() {
        // Same idea but for range queries: narrow lookback → no results.
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);
        let end = start;
        let step = std::time::Duration::from_secs(60);

        let wide = QueryOptions::default();
        let results = tsdb
            .eval_query_range("http_requests", start..=end, step, &wide)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        let narrow = QueryOptions {
            lookback_delta: std::time::Duration::from_secs(10),
        };
        let results = tsdb
            .eval_query_range("http_requests", start..=end, step, &narrow)
            .await
            .unwrap();
        assert!(
            results.is_empty(),
            "10s lookback should miss samples 100s ago"
        );
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_should_return_scalar(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("1+1", Some(query_time), &opts)
            .await
            .unwrap();

        match result {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => {
                assert_eq!(value, 2.0);
                assert_eq!(
                    timestamp_ms,
                    query_time
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64
                );
            }
            other => panic!("expected Scalar, got {:?}", other),
        }
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_should_return_error_for_invalid_query(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let opts = QueryOptions::default();
        let result = tsdb.eval_query("invalid{", None, &opts).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[tokio::test]
    async fn eval_query_range_should_return_range_samples() {
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4000);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4000);
        let step = std::time::Duration::from_secs(60);

        let opts = QueryOptions::default();
        let mut results = tsdb
            .eval_query_range("http_requests", start..=end, step, &opts)
            .await
            .unwrap();
        results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].labels.get("env"), Some("prod"));
        assert!(!results[0].samples.is_empty());
        assert_eq!(results[1].labels.get("env"), Some("staging"));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_range_should_return_scalar(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(160);
        let step = std::time::Duration::from_secs(60);

        let opts = QueryOptions::default();
        let results = tsdb
            .eval_query_range("1+1", start..=end, step, &opts)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].labels.metric_name(), "");
        assert_eq!(results[0].samples.len(), 2); // two steps: 100s and 160s
        assert_eq!(results[0].samples[0].1, 2.0);
        assert_eq!(results[0].samples[1].1, 2.0);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_range_should_return_error_for_invalid_query(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(200);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query_range(
                "invalid{",
                start..=end,
                std::time::Duration::from_secs(60),
                &opts,
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[tokio::test]
    async fn find_series_should_return_matching_series() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_series(&["http_requests"], 3600, 7200)
            .await
            .unwrap();
        results.sort_by(|a, b| a.get("env").cmp(&b.get("env")));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("env"), Some("prod"));
        assert_eq!(results[0].metric_name(), "http_requests");
        assert_eq!(results[1].get("env"), Some("staging"));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_series_should_error_on_empty_matchers(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let result = tsdb.find_series(&[], 0, i64::MAX).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_series_should_dedup_across_buckets(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2]).await.unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb.find_series(&["cpu"], 0, i64::MAX).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name(), "cpu");
    }

    #[tokio::test]
    async fn find_labels_should_return_all_label_names() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb.find_labels(None, 3600, 7200).await.unwrap();
        results.sort();

        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"env".to_string()));
    }

    #[tokio::test]
    async fn find_labels_should_filter_by_matcher() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_labels(Some(&[r#"http_requests{env="prod"}"#]), 3600, 7200)
            .await
            .unwrap();
        results.sort();

        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"env".to_string()));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_labels_should_return_empty_for_no_data(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let results = tsdb.find_labels(None, 0, 100).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn find_label_values_should_return_values() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_label_values("env", None, 3600, 7200)
            .await
            .unwrap();
        results.sort();

        assert_eq!(results, vec!["prod", "staging"]);
    }

    #[tokio::test]
    async fn find_label_values_should_filter_by_matcher() {
        let tsdb = create_tsdb_with_data().await;

        let results = tsdb
            .find_label_values("env", Some(&[r#"http_requests{env="prod"}"#]), 3600, 7200)
            .await
            .unwrap();

        assert_eq!(results, vec!["prod"]);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_label_values_should_return_empty_for_no_data(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let results = tsdb.find_label_values("env", None, 0, 100).await.unwrap();
        assert!(results.is_empty());
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_metadata_should_return_all(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let mut series = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        series.description = Some("CPU usage".to_string());
        series.unit = Some("percent".to_string());
        tsdb.ingest_samples(vec![series]).await.unwrap();

        let results = tsdb.find_metadata(None).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name, "cpu");
        assert_eq!(results[0].metric_type, Some(MetricType::Gauge));
        assert_eq!(results[0].description, Some("CPU usage".to_string()));
        assert_eq!(results[0].unit, Some("percent".to_string()));
    }

    // -----------------------------------------------------------------------
    // Offset / @ modifier tests (bucket preloading)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn eval_query_with_offset_should_load_correct_bucket() {
        // Data at 4000s (bucket hour-60: 3600–7199s).
        // Query at 7600s with offset 1h → effective time = 4000s.
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7600);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query("http_requests offset 1h", Some(query_time), &opts)
            .await
            .unwrap();
        let samples = result.into_matrix();
        assert_eq!(samples.len(), 2, "offset 1h should find data at 4000s");
    }

    #[tokio::test]
    async fn eval_query_range_with_offset_crossing_bucket() {
        // Data at 4000s. Range [7600,7660] with offset 1h → effective [4000,4060].
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7600);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7660);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("http_requests offset 1h", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(!results.is_empty(), "offset range query should find data");
        for rs in &results {
            assert!(!rs.samples.is_empty());
        }
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_with_offset_before_epoch_should_not_error(storage: Arc<dyn Storage>) {
        // Query at 100s with offset 1h → effective time = -3500s (before epoch).
        let tsdb = Tsdb::new(storage);
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query("up offset 1h", Some(query_time), &opts)
            .await
            .unwrap();
        let samples = result.into_matrix();
        assert!(
            samples.is_empty(),
            "before-epoch offset should return empty"
        );
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_range_with_at_before_epoch_should_not_error(storage: Arc<dyn Storage>) {
        // `@ 0 offset 1h` pins evaluation to t=0, then offset pushes to -3600.
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1000);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2000);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("up @ 0 offset 1h", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(
            results.is_empty() || results.iter().all(|rs| rs.samples.is_empty()),
            "@ 0 offset 1h should return empty matrix"
        );
    }

    #[tokio::test]
    async fn eval_query_range_with_at_end_should_load_correct_bucket() {
        // Data at 4000s. Range [4000,8000]. `@ end()` pins to 8000s.
        // With default 5m lookback, sample at 4000s is within range from 8000.
        // Actually, @ end() pins evaluation to t=8000 for each step, but
        // lookback only covers 5min=300s. Data at 4000s is 4000s before 8000s,
        // so it won't be found by lookback. Let's use a range where @ end()
        // helps: range [3900,4100], data at 4000s, `@ end()` pins to 4100s,
        // lookback 5min covers it.
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(3900);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("http_requests @ end()", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(!results.is_empty(), "@ end() should find data");
        // All steps should see the same sample (pinned to end)
        for rs in &results {
            assert!(!rs.samples.is_empty());
        }
    }

    // -----------------------------------------------------------------------
    // Multi-bucket dedup for labels and label_values
    // -----------------------------------------------------------------------

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_labels_should_dedup_across_buckets(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2]).await.unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb.find_labels(None, 0, i64::MAX).await.unwrap();

        // Label names should not be duplicated
        let mut sorted = results.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            results.len(),
            sorted.len(),
            "label names should be deduplicated across buckets"
        );
        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"host".to_string()));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_label_values_should_dedup_across_buckets(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2]).await.unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb
            .find_label_values("host", None, 0, i64::MAX)
            .await
            .unwrap();

        assert_eq!(
            results,
            vec!["a"],
            "label values should be deduplicated across buckets"
        );
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_metadata_should_filter_by_metric(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        tsdb.ingest_samples(vec![
            create_sample("cpu", vec![], 4_000_000, 1.0),
            create_sample("mem", vec![], 4_000_000, 2.0),
        ])
        .await
        .unwrap();

        let results = tsdb.find_metadata(Some("cpu")).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name, "cpu");

        let results = tsdb.find_metadata(Some("nonexistent")).await.unwrap();
        assert!(results.is_empty());
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_ingest_and_query_with_metric_prefixed_layout(storage: Arc<dyn Storage>) {
        use crate::promql::evaluator::Evaluator;
        use promql_parser::parser::EvalStmt;
        use std::time::{Duration, UNIX_EPOCH};

        // given: Tsdb configured with MetricPrefixed sample storage layout
        let config = TsdbRuntimeConfig {
            sample_storage_layout: SampleStorageLayout::MetricPrefixed,
            ..Default::default()
        };
        let tsdb = Tsdb::with_runtime_config(storage, config);

        // Ingest two different metrics into the same bucket
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            4_000_000,
            42.0,
        ))
        .await
        .unwrap();
        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "staging")],
            4_000_000,
            10.0,
        ))
        .await
        .unwrap();
        mini.ingest(&create_sample(
            "cpu_usage",
            vec![("host", "web1")],
            4_000_000,
            0.75,
        ))
        .await
        .unwrap();

        tsdb.flush().await.unwrap();

        // when: query via evaluator (exercises samples_by_locator dispatch)
        let reader = tsdb.query_reader(3600, 7200).await.unwrap();
        let mut evaluator = Evaluator::new(&reader);

        let query_time = UNIX_EPOCH + Duration::from_secs(4100);
        let expr = promql_parser::parser::parse("http_requests").unwrap();
        let stmt = EvalStmt {
            expr,
            start: query_time,
            end: query_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let mut results = evaluator
            .evaluate(stmt)
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");
        results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

        // then: should return both http_requests series, not cpu_usage
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].labels.get("env"), Some("prod"));
        assert_eq!(results[0].value, 42.0);
        assert_eq!(results[1].labels.get("env"), Some("staging"));
        assert_eq!(results[1].value, 10.0);

        // also query cpu_usage
        let mut evaluator2 = Evaluator::new(&reader);
        let expr2 = promql_parser::parser::parse("cpu_usage").unwrap();
        let stmt2 = EvalStmt {
            expr: expr2,
            start: query_time,
            end: query_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let results2 = evaluator2
            .evaluate(stmt2)
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        assert_eq!(results2.len(), 1);
        assert_eq!(results2[0].labels.get("host"), Some("web1"));
        assert_eq!(results2[0].value, 0.75);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_range_query_with_metric_prefixed_layout_via_b3(storage: Arc<dyn Storage>) {
        use crate::test_utils::assertions::assert_approx_eq;
        use std::time::{Duration, UNIX_EPOCH};

        // given: two buckets with metric-prefixed layout
        let config = TsdbRuntimeConfig {
            sample_storage_layout: SampleStorageLayout::MetricPrefixed,
            ..Default::default()
        };
        let tsdb = Tsdb::with_runtime_config(storage, config);

        // Bucket 60: minutes 60-119, ms 3,600,000-7,199,999
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        mini1
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                3_900_000,
                10.0,
            ))
            .await
            .unwrap();
        mini1
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                3_900_000,
                20.0,
            ))
            .await
            .unwrap();

        // Bucket 120: minutes 120-179, ms 7,200,000-10,799,999
        let bucket2 = TimeBucket::hour(120);
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini2
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                7_500_000,
                30.0,
            ))
            .await
            .unwrap();
        mini2
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                7_500_000,
                40.0,
            ))
            .await
            .unwrap();

        tsdb.flush().await.unwrap();

        // when: range query spanning both buckets (exercises B3 parallel sample loading)
        let start = UNIX_EPOCH + Duration::from_secs(3900);
        let end = UNIX_EPOCH + Duration::from_secs(7600);
        let step = Duration::from_secs(3600);
        let opts = QueryOptions::default();

        let mut results = tsdb
            .eval_query_range("http_requests", start..=end, step, &opts)
            .await
            .unwrap();
        results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

        // then: both series returned with samples from both buckets
        assert_eq!(results.len(), 2, "expected 2 series");

        // env=prod
        assert_eq!(results[0].labels.get("env"), Some("prod"));
        assert!(!results[0].samples.is_empty(), "prod should have samples");
        // The latest sample for prod should be 30.0 from bucket2
        let prod_last = results[0].samples.last().unwrap();
        assert_approx_eq(prod_last.1, 30.0);

        // env=staging
        assert_eq!(results[1].labels.get("env"), Some("staging"));
        assert!(
            !results[1].samples.is_empty(),
            "staging should have samples"
        );
        let staging_last = results[1].samples.last().unwrap();
        assert_approx_eq(staging_last.1, 40.0);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn evaluate_range_should_capture_physical_io_stats(storage: Arc<dyn Storage>) {
        use crate::query_io::{self, QueryIoCollector};
        use std::time::{Duration, UNIX_EPOCH};

        // given: metric-prefixed layout with data in one bucket
        let config = TsdbRuntimeConfig {
            sample_storage_layout: SampleStorageLayout::MetricPrefixed,
            ..Default::default()
        };
        let tsdb = Tsdb::with_runtime_config(storage, config);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
        for i in 0..5 {
            mini.ingest(&create_sample(
                "cpu",
                vec![("host", &format!("h{i}"))],
                4_000_000,
                i as f64,
            ))
            .await
            .unwrap();
        }
        tsdb.flush().await.unwrap();

        // when: run evaluate_range with collector scope covering reader construction
        // so bucket-list bytes are captured
        let io_collector = QueryIoCollector::new();
        let (_result, stats) = query_io::run_with_collector(&io_collector, async {
            let reader = tsdb.query_reader(3600, 7200).await.unwrap();
            let expr = promql_parser::parser::parse("cpu").unwrap();
            let stmt = EvalStmt {
                expr,
                start: UNIX_EPOCH + Duration::from_secs(4000),
                end: UNIX_EPOCH + Duration::from_secs(4060),
                interval: Duration::from_secs(15),
                lookback_delta: Duration::from_secs(300),
            };
            evaluate_range(&reader, stmt, None).await
        })
        .await
        .unwrap();

        // then: IO stats should be populated
        let io = &stats.io;

        // Bucket list: captured because collector scope includes reader construction
        assert!(
            io.bucket_list_bytes > 0,
            "expected bucket_list_bytes > 0 (scope includes reader construction)"
        );
        assert_eq!(io.bucket_list_gets, 1, "single bucket list get");
        assert_eq!(io.bucket_list_records, 1, "single bucket list record");

        // Inverted index: should have loaded terms for __name__=cpu
        assert!(
            io.inverted_index_records > 0,
            "expected inverted_index_records > 0"
        );
        assert!(
            io.inverted_index_bytes > 0,
            "expected inverted_index_bytes > 0"
        );

        // Forward index: should have loaded 5 series entries
        assert!(
            io.forward_index_records >= 5,
            "expected forward_index_records >= 5, got {}",
            io.forward_index_records
        );
        assert!(
            io.forward_index_bytes > 0,
            "expected forward_index_bytes > 0"
        );

        // Sample metric-prefixed gets: 5 series
        assert!(
            io.sample_metric_get_records >= 5,
            "expected sample_metric_get_records >= 5, got {}",
            io.sample_metric_get_records
        );
        assert!(
            io.sample_metric_get_bytes > 0,
            "expected sample_metric_get_bytes > 0"
        );

        // Derived totals: physical_bytes_total == metadata + samples
        assert!(io.physical_bytes_total() > 0);
        assert!(io.metadata_bytes() > 0);
        assert!(io.sample_bytes() > 0);
        assert_eq!(
            io.physical_bytes_total(),
            io.metadata_bytes() + io.sample_bytes()
        );

        // Reconciliation: total == all parts
        assert_eq!(
            io.physical_bytes_total(),
            io.bucket_list_bytes
                + io.inverted_index_bytes
                + io.forward_index_bytes
                + io.sample_get_bytes
                + io.sample_metric_get_bytes
        );

        // MetricPrefixed layout: no legacy sample gets
        assert_eq!(io.sample_get_bytes, 0);
        assert!(io.sample_metric_get_bytes > 0);

        // Logical vs physical: sample_logical_bytes should be less than physical
        let sample_logical_bytes = stats.samples_loaded * 16;
        assert!(sample_logical_bytes > 0, "expected samples_loaded > 0");
        assert!(
            io.sample_bytes() > sample_logical_bytes,
            "physical sample bytes ({}) should exceed logical ({})",
            io.sample_bytes(),
            sample_logical_bytes,
        );
    }

    /// Deterministic test: exact record counts and byte reconciliation for a
    /// small known dataset. Validates the full I/O instrumentation pipeline
    /// from storage through to the evaluate_range stats.
    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn io_stats_deterministic_record_counts(storage: Arc<dyn Storage>) {
        use crate::query_io::{self, QueryIoCollector};
        use std::time::{Duration, UNIX_EPOCH};

        // given: MetricPrefixed layout, 1 metric, 2 series, 1 sample each
        let config = TsdbRuntimeConfig {
            sample_storage_layout: SampleStorageLayout::MetricPrefixed,
            ..Default::default()
        };
        let tsdb = Tsdb::with_runtime_config(storage.clone(), config);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
        mini.ingest(&create_sample("temp", vec![("loc", "a")], 4_000_000, 1.0))
            .await
            .unwrap();
        mini.ingest(&create_sample("temp", vec![("loc", "b")], 4_000_000, 2.0))
            .await
            .unwrap();
        tsdb.flush().await.unwrap();

        // Compute expected physical bytes by reading records directly from storage.
        // This gives us exact byte counts to compare against the collector.
        let snapshot = storage.snapshot().await.unwrap();
        use crate::serde::TimeBucketScoped;
        use crate::serde::key::{
            BucketListKey, ForwardIndexKey, InvertedIndexKey, MetricTimeSeriesKey,
        };

        // Bucket list: single key
        let bl_record = snapshot.get(BucketListKey.encode()).await.unwrap().unwrap();
        let expected_bucket_list_bytes = (bl_record.key.len() + bl_record.value.len()) as u64;

        // Inverted index: terms for __name__=temp, loc=a, loc=b
        let mut expected_inverted_bytes = 0u64;
        for (name, value) in [("__name__", "temp"), ("loc", "a"), ("loc", "b")] {
            let key = InvertedIndexKey {
                time_bucket: bucket.start,
                bucket_size: bucket.size,
                attribute: name.to_string(),
                value: value.to_string(),
            }
            .encode();
            if let Some(record) = snapshot.get(key).await.unwrap() {
                expected_inverted_bytes += (record.key.len() + record.value.len()) as u64;
            }
        }

        // Forward index: 2 series entries
        let fi_range = ForwardIndexKey::bucket_range(&bucket);
        let fi_records = snapshot.scan(fi_range).await.unwrap();
        let expected_forward_bytes: u64 = fi_records
            .iter()
            .map(|r| (r.key.len() + r.value.len()) as u64)
            .sum();
        let expected_forward_records = fi_records.len() as u64;

        // Sample (MetricPrefixed): 2 series blobs
        // Note: both series share the metric "temp" but have different series IDs.
        // We read all records under the metric-prefixed key range.
        let sample_range = MetricTimeSeriesKey::metric_range(&bucket, "temp");
        let sample_records = snapshot.scan(sample_range).await.unwrap();
        let expected_sample_bytes: u64 = sample_records
            .iter()
            .map(|r| (r.key.len() + r.value.len()) as u64)
            .sum();
        let expected_sample_records = sample_records.len() as u64;

        // when: run query with collector scope around everything
        let io_collector = QueryIoCollector::new();
        let (_result, stats) = query_io::run_with_collector(&io_collector, async {
            let reader = tsdb.query_reader(3600, 7200).await.unwrap();
            let expr = promql_parser::parser::parse("temp").unwrap();
            let stmt = EvalStmt {
                expr,
                start: UNIX_EPOCH + Duration::from_secs(4000),
                end: UNIX_EPOCH + Duration::from_secs(4000),
                interval: Duration::from_secs(15),
                lookback_delta: Duration::from_secs(300),
            };
            evaluate_range(&reader, stmt, None).await
        })
        .await
        .unwrap();

        let io = &stats.io;

        // then: exact record counts
        assert_eq!(io.bucket_list_gets, 1, "one bucket list get");
        assert_eq!(io.bucket_list_records, 1, "one bucket list record");
        assert_eq!(
            io.forward_index_records, expected_forward_records,
            "forward index records"
        );
        assert_eq!(
            io.sample_metric_get_records, expected_sample_records,
            "sample metric get records"
        );
        assert_eq!(io.sample_get_records, 0, "no legacy sample gets");

        // Exact byte counts matching direct storage reads
        assert_eq!(
            io.bucket_list_bytes, expected_bucket_list_bytes,
            "bucket list bytes: collector={} vs direct={}",
            io.bucket_list_bytes, expected_bucket_list_bytes
        );
        assert_eq!(
            io.forward_index_bytes, expected_forward_bytes,
            "forward index bytes: collector={} vs direct={}",
            io.forward_index_bytes, expected_forward_bytes
        );
        assert_eq!(
            io.sample_metric_get_bytes, expected_sample_bytes,
            "sample metric bytes: collector={} vs direct={}",
            io.sample_metric_get_bytes, expected_sample_bytes
        );

        // Inverted index bytes: the query only loads terms it needs (__name__=temp),
        // which may differ from all 3 terms. Assert consistency, not exact match.
        assert!(io.inverted_index_bytes > 0, "inverted index bytes > 0");
        assert!(
            io.inverted_index_bytes <= expected_inverted_bytes,
            "inverted index bytes ({}) should not exceed all-terms total ({})",
            io.inverted_index_bytes,
            expected_inverted_bytes
        );

        // Full reconciliation
        assert_eq!(
            io.physical_bytes_total(),
            io.bucket_list_bytes
                + io.inverted_index_bytes
                + io.forward_index_bytes
                + io.sample_get_bytes
                + io.sample_metric_get_bytes
        );

        // Logical metadata entry counts
        assert_eq!(stats.bucket_list_entries_loaded, 1, "1 bucket");
        assert_eq!(
            stats.forward_index_entries_loaded, 2,
            "2 forward index entries"
        );
        assert_eq!(stats.sample_series_loaded, 2, "2 sample series loaded");
    }

    /// Verify that the legacy (LegacySeriesId) layout path records
    /// sample_get_bytes and NOT sample_metric_get_bytes.
    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn io_stats_legacy_layout_uses_sample_get(storage: Arc<dyn Storage>) {
        use crate::query_io::{self, QueryIoCollector};
        use std::time::{Duration, UNIX_EPOCH};

        let config = TsdbRuntimeConfig {
            sample_storage_layout: SampleStorageLayout::LegacySeriesId,
            ..Default::default()
        };
        let tsdb = Tsdb::with_runtime_config(storage, config);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
        mini.ingest(&create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0))
            .await
            .unwrap();
        tsdb.flush().await.unwrap();

        let io_collector = QueryIoCollector::new();
        let (_result, stats) = query_io::run_with_collector(&io_collector, async {
            let reader = tsdb.query_reader(3600, 7200).await.unwrap();
            let expr = promql_parser::parser::parse("cpu").unwrap();
            let stmt = EvalStmt {
                expr,
                start: UNIX_EPOCH + Duration::from_secs(4000),
                end: UNIX_EPOCH + Duration::from_secs(4000),
                interval: Duration::from_secs(15),
                lookback_delta: Duration::from_secs(300),
            };
            evaluate_range(&reader, stmt, None).await
        })
        .await
        .unwrap();

        let io = &stats.io;

        // Legacy layout: sample_get path used, not metric-prefixed
        assert!(
            io.sample_get_bytes > 0,
            "expected sample_get_bytes > 0 for legacy layout"
        );
        assert_eq!(
            io.sample_get_records, 1,
            "expected 1 legacy sample get record"
        );
        assert_eq!(
            io.sample_metric_get_bytes, 0,
            "no metric-prefixed gets for legacy layout"
        );
        assert_eq!(
            io.sample_metric_get_records, 0,
            "no metric-prefixed records for legacy layout"
        );

        // Bucket list captured
        assert!(io.bucket_list_bytes > 0);

        // Full reconciliation
        assert_eq!(
            io.physical_bytes_total(),
            io.bucket_list_bytes
                + io.inverted_index_bytes
                + io.forward_index_bytes
                + io.sample_get_bytes
                + io.sample_metric_get_bytes
        );
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_range_rejects_zero_step(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        tsdb.ingest_samples(vec![create_sample("cpu", vec![], 1_000_000, 1.0)])
            .await
            .unwrap();
        tsdb.flush().await.unwrap();

        let start = UNIX_EPOCH + Duration::from_secs(1000);
        let end = UNIX_EPOCH + Duration::from_secs(1060);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query_range("cpu", start..=end, Duration::ZERO, &opts)
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::InvalidQuery(_)));
    }
}

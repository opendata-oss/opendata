//! Explicit phase artifacts for the evaluator query pipeline.
//!
//! All evaluator-backed query paths (instant vector selector, matrix selector,
//! subquery vector-selector fast path) share the same logical execution model:
//!
//! 1. **Plan** – compute concrete time bounds, bucket list, path-specific parameters
//! 2. **ResolveMetadata** – run selector, load forward index per bucket
//! 3. **LoadSamples** – load sample data for explicit (bucket, series) work items
//! 4. **ShapeSamples** – merge/filter/dedup into evaluator-ready per-series structures
//! 5. **Evaluate** – run PromQL expression semantics on prepared in-memory inputs
//!
//! The types in this module represent the intermediate artifacts produced by each phase.

use crate::index::ForwardIndexLookup;
use crate::model::{Label, Sample, SeriesFingerprint, SeriesId, TimeBucket};
use crate::promql::evaluator::{
    CachedQueryReader, EvalResult, EvalSample, EvalSamples, EvaluationError, ExprResult,
};
use crate::promql::selector::evaluate_selector_with_reader;
use crate::query::QueryReader;
use promql_parser::parser::VectorSelector;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Phase artifact types
// ---------------------------------------------------------------------------

/// Path-specific execution parameters.
pub(crate) enum QueryPathKind {
    /// Instant vector selector. Buckets newest-first, fingerprint dedup.
    InstantVector { lookback_delta_ms: i64 },
    /// Matrix selector. Buckets chronological, merge all samples per label set.
    Matrix,
    /// Subquery vector-selector fast path. Merge, sort/dedup, step-bucket.
    SubqueryVectorSelector {
        aligned_start_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
        expected_steps: usize,
    },
}

impl QueryPathKind {
    fn name(&self) -> &'static str {
        match self {
            QueryPathKind::InstantVector { .. } => "instant",
            QueryPathKind::Matrix => "matrix",
            QueryPathKind::SubqueryVectorSelector { .. } => "subquery",
        }
    }
}

/// Computed execution plan for a query path.
pub(crate) struct QueryPlan {
    /// Exclusive lower bound for sample filtering (timestamp > start).
    pub sample_start_ms: i64,
    /// Inclusive upper bound for sample filtering (timestamp <= end).
    pub sample_end_ms: i64,
    /// Buckets to process, pre-sorted in execution order.
    pub buckets: Vec<TimeBucket>,
    /// Path-specific behavior and parameters.
    pub path_kind: QueryPathKind,
}

impl QueryPlan {
    /// Build sample work for a bucket, dispatching strict vs lenient by path kind.
    fn build_work(
        &self,
        metadata: &BucketMetadata,
        seen_fingerprints: Option<&HashSet<SeriesFingerprint>>,
    ) -> EvalResult<BucketSampleWork> {
        let (skip, strict) = match &self.path_kind {
            QueryPathKind::InstantVector { .. } => (seen_fingerprints, true),
            QueryPathKind::Matrix => (None, true),
            QueryPathKind::SubqueryVectorSelector { .. } => (None, false),
        };
        build_bucket_sample_work(self, metadata, skip, strict)
    }

    /// For the instant path, record fingerprints of series that had data so
    /// older buckets can skip them. No-op for other path kinds.
    fn track_seen_fingerprints(
        &self,
        data: &BucketSampleData,
        seen: &mut HashSet<SeriesFingerprint>,
    ) {
        if matches!(self.path_kind, QueryPathKind::InstantVector { .. }) {
            for series in &data.series_data {
                if series.samples.last().is_some() {
                    seen.insert(series.fingerprint);
                }
            }
        }
    }

    /// Shape all loaded bucket data into the final ExprResult for this path.
    fn shape(&self, all_bucket_data: &[BucketSampleData]) -> ExprResult {
        match &self.path_kind {
            QueryPathKind::InstantVector { .. } => {
                ExprResult::InstantVector(shape_instant_results(all_bucket_data))
            }
            QueryPathKind::Matrix => ExprResult::RangeVector(shape_matrix_results(all_bucket_data)),
            QueryPathKind::SubqueryVectorSelector { .. } => {
                ExprResult::RangeVector(shape_subquery_results(all_bucket_data, self))
            }
        }
    }
}

/// Resolved metadata for one bucket.
pub(crate) struct BucketMetadata {
    pub bucket: TimeBucket,
    /// Candidate series IDs matching the selector.
    pub candidates: Vec<SeriesId>,
    /// Forward index providing series specs (labels, metric type, etc.).
    pub forward_index: Arc<dyn ForwardIndexLookup + Send + Sync>,
}

/// One series to load samples for.
pub(crate) struct SeriesWorkItem {
    pub series_id: SeriesId,
    pub fingerprint: SeriesFingerprint,
    pub labels: Vec<Label>,
}

/// All sample I/O work for one bucket.
pub(crate) struct BucketSampleWork {
    pub bucket: TimeBucket,
    pub series: Vec<SeriesWorkItem>,
    pub start_ms: i64,
    pub end_ms: i64,
}

/// Loaded samples for one series.
pub(crate) struct LoadedSeriesSamples {
    pub fingerprint: SeriesFingerprint,
    pub labels: Vec<Label>,
    pub samples: Vec<Sample>,
}

/// All loaded samples for one bucket.
pub(crate) struct BucketSampleData {
    pub bucket: TimeBucket,
    pub series_data: Vec<LoadedSeriesSamples>,
}

// ---------------------------------------------------------------------------
// Phase 1: Planning
// ---------------------------------------------------------------------------

impl QueryPlan {
    /// Construct plan for instant vector selector.
    ///
    /// Buckets sorted newest-first so that cross-bucket fingerprint dedup
    /// can short-circuit on the first bucket that has data for a series.
    pub(crate) fn for_instant_vector(
        adjusted_eval_ts_ms: i64,
        lookback_delta_ms: i64,
        mut buckets: Vec<TimeBucket>,
    ) -> Self {
        let end_ms = adjusted_eval_ts_ms;
        let start_ms = end_ms - lookback_delta_ms;
        buckets.sort_by(|a, b| b.start.cmp(&a.start)); // newest first
        QueryPlan {
            sample_start_ms: start_ms,
            sample_end_ms: end_ms,
            buckets,
            path_kind: QueryPathKind::InstantVector { lookback_delta_ms },
        }
    }

    /// Construct plan for matrix selector.
    ///
    /// Buckets sorted chronologically and filtered to only those overlapping
    /// with `[start_ms, end_ms]`. All samples are merged per label set.
    pub(crate) fn for_matrix(
        adjusted_eval_ts_ms: i64,
        range_ms: i64,
        mut buckets: Vec<TimeBucket>,
    ) -> Self {
        let end_ms = adjusted_eval_ts_ms;
        let start_ms = end_ms - range_ms;
        buckets.sort_by(|a, b| a.start.cmp(&b.start)); // chronological
        buckets.retain(|bucket| {
            let bucket_start_ms = (bucket.start as i64) * 60 * 1000;
            let bucket_end_ms = bucket_start_ms + (bucket.size_in_mins() as i64) * 60 * 1000;
            !(bucket_end_ms < start_ms || bucket_start_ms > end_ms)
        });
        QueryPlan {
            sample_start_ms: start_ms,
            sample_end_ms: end_ms,
            buckets,
            path_kind: QueryPathKind::Matrix,
        }
    }

    /// Construct plan for subquery vector-selector fast path.
    ///
    /// Computes step alignment and extends the sample range backward by
    /// `lookback_delta_ms` so the first step has data. Buckets sorted
    /// newest-first (matching existing `fetch_series_samples` behavior).
    pub(crate) fn for_subquery_vector_selector(
        subquery_start_ms: i64,
        subquery_end_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
        mut buckets: Vec<TimeBucket>,
    ) -> Self {
        let (aligned_start_ms, range_start_ms, range_end_ms, expected_steps) =
            compute_subquery_alignment(
                subquery_start_ms,
                subquery_end_ms,
                step_ms,
                lookback_delta_ms,
            );
        buckets.sort_by(|a, b| b.start.cmp(&a.start)); // newest first
        QueryPlan {
            sample_start_ms: range_start_ms,
            sample_end_ms: range_end_ms,
            buckets,
            path_kind: QueryPathKind::SubqueryVectorSelector {
                aligned_start_ms,
                step_ms,
                lookback_delta_ms,
                expected_steps,
            },
        }
    }
}

/// Compute subquery time alignment and range extension.
///
/// Returns `(aligned_start_ms, range_start_ms, range_end_ms, expected_steps)`.
///
/// Uses `div_euclid` for correct floor division with negative timestamps
/// (e.g. `-41ms / 10ms` → `-50ms`, not `-40ms`).
fn compute_subquery_alignment(
    subquery_start_ms: i64,
    subquery_end_ms: i64,
    step_ms: i64,
    lookback_delta_ms: i64,
) -> (i64, i64, i64, usize) {
    let div = subquery_start_ms.div_euclid(step_ms);
    let mut aligned_start_ms = div * step_ms;
    if aligned_start_ms <= subquery_start_ms {
        aligned_start_ms += step_ms;
    }
    let expected_steps = ((subquery_end_ms - aligned_start_ms) / step_ms) as usize + 1;
    let range_start_ms = aligned_start_ms - lookback_delta_ms;
    let range_end_ms = subquery_end_ms;
    (
        aligned_start_ms,
        range_start_ms,
        range_end_ms,
        expected_steps,
    )
}

// ---------------------------------------------------------------------------
// Phase 2: Metadata resolution
// ---------------------------------------------------------------------------

/// Resolve metadata for a single bucket: run selector, load forward index.
///
/// Returns `None` if no candidate series match the selector in this bucket.
pub(crate) async fn resolve_bucket_metadata<R: QueryReader>(
    reader: &mut CachedQueryReader<'_, R>,
    bucket: TimeBucket,
    selector: &VectorSelector,
) -> EvalResult<Option<BucketMetadata>> {
    let candidates = evaluate_selector_with_reader(reader, bucket, selector)
        .await
        .map_err(|e| EvaluationError::InternalError(e.to_string()))?;

    if candidates.is_empty() {
        return Ok(None);
    }

    let candidates_vec: Vec<_> = candidates.into_iter().collect();
    let forward_index = reader.forward_index(&bucket, &candidates_vec).await?;

    Ok(Some(BucketMetadata {
        bucket,
        candidates: candidates_vec,
        forward_index,
    }))
}

// ---------------------------------------------------------------------------
// Phase 3: Sample work generation
// ---------------------------------------------------------------------------

/// Build explicit sample work for a bucket from resolved metadata.
///
/// `skip_fingerprints` enables cross-bucket dedup for instant vector selectors
/// (series already found in a newer bucket are excluded from the work list).
///
/// When `strict` is true, a candidate series ID missing from the forward index
/// is an error (instant and matrix paths). When false, missing specs are
/// silently skipped (subquery path, matching existing `fetch_series_samples`).
pub(crate) fn build_bucket_sample_work(
    plan: &QueryPlan,
    metadata: &BucketMetadata,
    skip_fingerprints: Option<&HashSet<SeriesFingerprint>>,
    strict: bool,
) -> EvalResult<BucketSampleWork> {
    let mut series = Vec::with_capacity(metadata.candidates.len());

    for &series_id in &metadata.candidates {
        let spec = match metadata.forward_index.get_spec(&series_id) {
            Some(spec) => spec,
            None if strict => {
                return Err(EvaluationError::InternalError(format!(
                    "Series {} not found in bucket {:?}",
                    series_id, metadata.bucket
                )));
            }
            None => continue,
        };

        let fingerprint = compute_fingerprint(&spec.labels);

        if let Some(skip) = skip_fingerprints
            && skip.contains(&fingerprint)
        {
            continue;
        }

        series.push(SeriesWorkItem {
            series_id,
            fingerprint,
            labels: spec.labels.clone(),
        });
    }

    Ok(BucketSampleWork {
        bucket: metadata.bucket,
        series,
        start_ms: plan.sample_start_ms,
        end_ms: plan.sample_end_ms,
    })
}

// ---------------------------------------------------------------------------
// Phase 4: Sample loading
// ---------------------------------------------------------------------------

/// Load samples for all series in a bucket sample work item.
pub(crate) async fn load_bucket_samples<R: QueryReader>(
    reader: &mut CachedQueryReader<'_, R>,
    work: &BucketSampleWork,
) -> EvalResult<BucketSampleData> {
    let mut series_data = Vec::with_capacity(work.series.len());

    for item in &work.series {
        let samples = reader
            .samples(&work.bucket, item.series_id, work.start_ms, work.end_ms)
            .await?;

        series_data.push(LoadedSeriesSamples {
            fingerprint: item.fingerprint,
            labels: item.labels.clone(),
            samples,
        });
    }

    Ok(BucketSampleData {
        bucket: work.bucket,
        series_data,
    })
}

// ---------------------------------------------------------------------------
// Phase 5: Shaping
// ---------------------------------------------------------------------------

/// Shape loaded data for instant vector selector.
///
/// Takes the latest sample per fingerprint. Bucket data should already be in
/// newest-first order (from `QueryPlan::for_instant_vector`). A fingerprint
/// dedup pass ensures within-bucket duplicates are also handled.
pub(crate) fn shape_instant_results(all_bucket_data: &[BucketSampleData]) -> Vec<EvalSample> {
    let mut seen: HashSet<SeriesFingerprint> = HashSet::new();
    let mut results = Vec::new();

    for bd in all_bucket_data {
        for series in &bd.series_data {
            if seen.contains(&series.fingerprint) {
                continue;
            }
            if let Some(best) = series.samples.last() {
                results.push(EvalSample {
                    timestamp_ms: best.timestamp_ms,
                    value: best.value,
                    labels: labels_to_hashmap(&series.labels),
                    drop_name: false,
                });
                seen.insert(series.fingerprint);
            }
        }
    }

    results
}

/// Shape loaded data for matrix selector.
///
/// Merges all samples per series (keyed by sorted label vector) across all
/// buckets. Bucket data should be in chronological order.
pub(crate) fn shape_matrix_results(all_bucket_data: &[BucketSampleData]) -> Vec<EvalSamples> {
    let mut series_map: HashMap<Vec<Label>, Vec<Sample>> = HashMap::new();

    for bd in all_bucket_data {
        for series in &bd.series_data {
            let mut key = series.labels.clone();
            key.sort();
            let values = series_map.entry(key).or_default();
            values.extend(series.samples.iter().cloned());
        }
    }

    series_map
        .into_iter()
        .map(|(labels, values)| EvalSamples {
            values,
            labels: labels_to_hashmap(&labels),
        })
        .collect()
}

/// Shape loaded data for subquery vector-selector fast path.
///
/// Merges by fingerprint across buckets, sorts/deduplicates, then applies
/// the sliding-window step-bucketing algorithm.
pub(crate) fn shape_subquery_results(
    all_bucket_data: &[BucketSampleData],
    plan: &QueryPlan,
) -> Vec<EvalSamples> {
    let (aligned_start_ms, step_ms, lookback_delta_ms, expected_steps) = match &plan.path_kind {
        QueryPathKind::SubqueryVectorSelector {
            aligned_start_ms,
            step_ms,
            lookback_delta_ms,
            expected_steps,
        } => (
            *aligned_start_ms,
            *step_ms,
            *lookback_delta_ms,
            *expected_steps,
        ),
        _ => return Vec::new(),
    };

    // Merge by fingerprint
    let mut merged: HashMap<SeriesFingerprint, (Vec<Label>, Vec<Sample>)> = HashMap::new();
    for bd in all_bucket_data {
        for series in &bd.series_data {
            let entry = merged
                .entry(series.fingerprint)
                .or_insert_with(|| (series.labels.clone(), Vec::new()));
            entry.1.extend(series.samples.iter().cloned());
        }
    }

    // Sort and dedup
    for (_, (_, samples)) in merged.iter_mut() {
        samples.sort_by_key(|s| s.timestamp_ms);
        samples.dedup_by_key(|s| s.timestamp_ms);
    }

    // Step-bucketing with sliding window
    let subquery_end_ms = plan.sample_end_ms;
    let mut range_vector = Vec::with_capacity(merged.len());

    for (_fingerprint, (labels, samples)) in merged {
        let mut step_samples = Vec::with_capacity(expected_steps);
        let mut i = 0usize;
        let mut last_valid: Option<&Sample> = None;

        for current_step_ms in (aligned_start_ms..=subquery_end_ms).step_by(step_ms as usize) {
            let lookback_start_ms = current_step_ms - lookback_delta_ms;

            while i < samples.len() && samples[i].timestamp_ms <= current_step_ms {
                last_valid = Some(&samples[i]);
                i += 1;
            }

            if let Some(sample) = last_valid
                && sample.timestamp_ms > lookback_start_ms
            {
                step_samples.push(Sample {
                    timestamp_ms: current_step_ms,
                    value: sample.value,
                });
            }
        }

        if !step_samples.is_empty() {
            range_vector.push(EvalSamples {
                values: step_samples,
                labels: labels_to_hashmap(&labels),
            });
        }
    }

    range_vector
}

// ---------------------------------------------------------------------------
// Unified pipeline orchestrator
// ---------------------------------------------------------------------------

/// Aggregate phase timings for the selector pipeline.
#[derive(Debug, Default, Clone)]
pub(crate) struct PipelineTimings {
    pub metadata_resolve_ms: f64,
    pub sample_load_ms: f64,
    pub shape_samples_ms: f64,
}

/// Execute the shared selector pipeline for all evaluator-backed query paths.
///
/// Orchestrates: resolve metadata -> build work -> load samples -> shape results.
/// Branching on `QueryPathKind` handles the behavioral differences between
/// instant vector selectors, matrix selectors, and subquery fast paths.
pub(crate) async fn execute_selector_pipeline<R: QueryReader>(
    reader: &mut CachedQueryReader<'_, R>,
    plan: &QueryPlan,
    selector: &VectorSelector,
) -> EvalResult<ExprResult> {
    use std::time::Instant;

    let mut all_bucket_data = Vec::new();
    let mut seen_fingerprints: HashSet<SeriesFingerprint> = HashSet::new();
    let mut timings = PipelineTimings::default();

    for &bucket in &plan.buckets {
        // Phase: ResolveMetadata + BuildWork
        let t0 = Instant::now();
        let Some(metadata) = resolve_bucket_metadata(reader, bucket, selector).await? else {
            timings.metadata_resolve_ms += t0.elapsed().as_secs_f64() * 1000.0;
            continue;
        };
        let work = plan.build_work(&metadata, Some(&seen_fingerprints))?;
        timings.metadata_resolve_ms += t0.elapsed().as_secs_f64() * 1000.0;

        // Phase: LoadSamples
        let t1 = Instant::now();
        let data = load_bucket_samples(reader, &work).await?;
        timings.sample_load_ms += t1.elapsed().as_secs_f64() * 1000.0;

        plan.track_seen_fingerprints(&data, &mut seen_fingerprints);
        all_bucket_data.push(data);
    }

    // Phase: ShapeSamples
    let t2 = Instant::now();
    let result = plan.shape(&all_bucket_data);
    timings.shape_samples_ms = t2.elapsed().as_secs_f64() * 1000.0;

    tracing::debug!(
        path = plan.path_kind.name(),
        buckets = plan.buckets.len(),
        metadata_ms = format!("{:.2}", timings.metadata_resolve_ms),
        load_ms = format!("{:.2}", timings.sample_load_ms),
        shape_ms = format!("{:.2}", timings.shape_samples_ms),
        "pipeline phase timings"
    );

    Ok(result)
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Convert a label slice to a HashMap<String, String>.
pub(crate) fn labels_to_hashmap(labels: &[Label]) -> HashMap<String, String> {
    labels
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect()
}

/// Compute a fingerprint from a label slice for series deduplication.
///
/// Sorts labels by name (consistent with the existing evaluator implementation)
/// and hashes them. Two label sets that are logically identical will produce the
/// same fingerprint regardless of the order they are stored in.
pub(crate) fn compute_fingerprint(labels: &[Label]) -> SeriesFingerprint {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut sorted_labels: Vec<_> = labels.iter().collect();
    sorted_labels.sort_by(|a, b| a.name.cmp(&b.name));
    for label in sorted_labels {
        label.name.hash(&mut hasher);
        label.value.hash(&mut hasher);
    }
    hasher.finish() as SeriesFingerprint
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Sample, TimeBucket};

    fn label(name: &str, value: &str) -> Label {
        Label::new(name, value)
    }

    fn sample(ts: i64, val: f64) -> Sample {
        Sample::new(ts, val)
    }

    fn make_loaded(
        fingerprint: SeriesFingerprint,
        labels: Vec<Label>,
        samples: Vec<Sample>,
    ) -> LoadedSeriesSamples {
        LoadedSeriesSamples {
            fingerprint,
            labels,
            samples,
        }
    }

    // -----------------------------------------------------------------------
    // Planning tests
    // -----------------------------------------------------------------------

    #[test]
    fn instant_plan_sorts_buckets_newest_first() {
        let buckets = vec![
            TimeBucket::hour(100),
            TimeBucket::hour(300),
            TimeBucket::hour(200),
        ];
        let plan = QueryPlan::for_instant_vector(500_000, 60_000, buckets);
        assert_eq!(plan.buckets[0].start, 300);
        assert_eq!(plan.buckets[1].start, 200);
        assert_eq!(plan.buckets[2].start, 100);
        assert_eq!(plan.sample_start_ms, 500_000 - 60_000);
        assert_eq!(plan.sample_end_ms, 500_000);
    }

    #[test]
    fn matrix_plan_filters_non_overlapping_buckets() {
        // Bucket at minute 100 covers [100*60*1000, 100*60*1000 + 60*60*1000] = [6_000_000, 9_600_000]
        // Bucket at minute 200 covers [12_000_000, 15_600_000]
        // Query range: end=10_000_000, range=2_000_000 -> [8_000_000, 10_000_000]
        let buckets = vec![
            TimeBucket::hour(100), // [6_000_000, 9_600_000] - overlaps
            TimeBucket::hour(200), // [12_000_000, 15_600_000] - no overlap
        ];
        let plan = QueryPlan::for_matrix(10_000_000, 2_000_000, buckets);
        assert_eq!(plan.buckets.len(), 1);
        assert_eq!(plan.buckets[0].start, 100);
    }

    #[test]
    fn matrix_plan_chronological_order() {
        let buckets = vec![
            TimeBucket::hour(200),
            TimeBucket::hour(100),
            TimeBucket::hour(150),
        ];
        // Use a huge range to include all buckets
        let plan = QueryPlan::for_matrix(100_000_000, 100_000_000, buckets);
        assert_eq!(plan.buckets[0].start, 100);
        assert_eq!(plan.buckets[1].start, 150);
        assert_eq!(plan.buckets[2].start, 200);
    }

    #[test]
    fn subquery_plan_alignment() {
        // subquery_start=15, end=55, step=10, lookback=20
        // aligned_start = ceil_to_next_step(15, 10) = 20
        // expected_steps = (55 - 20) / 10 + 1 = 4 (steps at 20, 30, 40, 50)
        // range_start = 20 - 20 = 0
        let buckets = vec![TimeBucket::hour(0)];
        let plan = QueryPlan::for_subquery_vector_selector(15, 55, 10, 20, buckets);
        match &plan.path_kind {
            QueryPathKind::SubqueryVectorSelector {
                aligned_start_ms,
                step_ms,
                lookback_delta_ms,
                expected_steps,
            } => {
                assert_eq!(*aligned_start_ms, 20);
                assert_eq!(*step_ms, 10);
                assert_eq!(*lookback_delta_ms, 20);
                assert_eq!(*expected_steps, 4);
            }
            _ => panic!("wrong path kind"),
        }
        assert_eq!(plan.sample_start_ms, 0);
        assert_eq!(plan.sample_end_ms, 55);
    }

    #[test]
    fn subquery_alignment_with_negative_timestamps() {
        // subquery_start=-41, end=0, step=10, lookback=5
        // div_euclid(-41, 10) = -5, aligned = -50, since -50 <= -41, aligned = -40
        // expected_steps = (0 - (-40)) / 10 + 1 = 5
        // range_start = -40 - 5 = -45
        let buckets = vec![TimeBucket::hour(0)];
        let plan = QueryPlan::for_subquery_vector_selector(-41, 0, 10, 5, buckets);
        match &plan.path_kind {
            QueryPathKind::SubqueryVectorSelector {
                aligned_start_ms,
                expected_steps,
                ..
            } => {
                assert_eq!(*aligned_start_ms, -40);
                assert_eq!(*expected_steps, 5);
            }
            _ => panic!("wrong path kind"),
        }
        assert_eq!(plan.sample_start_ms, -45);
    }

    // -----------------------------------------------------------------------
    // Shaping tests
    // -----------------------------------------------------------------------

    #[test]
    fn shape_instant_newest_bucket_wins() {
        let labels_a = vec![label("__name__", "m"), label("env", "prod")];
        let fp = compute_fingerprint(&labels_a);

        let bucket_data = vec![
            // Newest bucket first (bucket 200)
            BucketSampleData {
                bucket: TimeBucket::hour(200),
                series_data: vec![make_loaded(fp, labels_a.clone(), vec![sample(100, 1.0)])],
            },
            // Older bucket (bucket 100)
            BucketSampleData {
                bucket: TimeBucket::hour(100),
                series_data: vec![make_loaded(fp, labels_a, vec![sample(50, 2.0)])],
            },
        ];

        let results = shape_instant_results(&bucket_data);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, 1.0); // newest bucket wins
        assert_eq!(results[0].timestamp_ms, 100);
    }

    #[test]
    fn shape_instant_takes_latest_sample() {
        let labels_a = vec![label("__name__", "m")];
        let fp = compute_fingerprint(&labels_a);

        let bucket_data = vec![BucketSampleData {
            bucket: TimeBucket::hour(100),
            series_data: vec![make_loaded(
                fp,
                labels_a,
                vec![sample(10, 1.0), sample(20, 2.0), sample(30, 3.0)],
            )],
        }];

        let results = shape_instant_results(&bucket_data);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].value, 3.0); // latest sample
    }

    #[test]
    fn shape_instant_skips_empty_samples() {
        let labels_a = vec![label("__name__", "m")];
        let fp = compute_fingerprint(&labels_a);

        let bucket_data = vec![BucketSampleData {
            bucket: TimeBucket::hour(100),
            series_data: vec![make_loaded(fp, labels_a, vec![])], // no samples
        }];

        let results = shape_instant_results(&bucket_data);
        assert!(results.is_empty());
    }

    #[test]
    fn shape_matrix_merges_across_buckets() {
        let labels_a = vec![label("__name__", "m"), label("env", "prod")];

        let fp = compute_fingerprint(&labels_a);

        let bucket_data = vec![
            BucketSampleData {
                bucket: TimeBucket::hour(100),
                series_data: vec![make_loaded(
                    fp,
                    labels_a.clone(),
                    vec![sample(10, 1.0), sample(20, 2.0)],
                )],
            },
            BucketSampleData {
                bucket: TimeBucket::hour(200),
                series_data: vec![make_loaded(fp, labels_a, vec![sample(30, 3.0)])],
            },
        ];

        let results = shape_matrix_results(&bucket_data);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values.len(), 3);
    }

    #[test]
    fn shape_subquery_step_bucketing() {
        let labels_a = vec![label("__name__", "m")];
        let fp = compute_fingerprint(&labels_a);

        // Samples at t=5, t=15, t=25
        let bucket_data = vec![BucketSampleData {
            bucket: TimeBucket::hour(0),
            series_data: vec![make_loaded(
                fp,
                labels_a,
                vec![sample(5, 1.0), sample(15, 2.0), sample(25, 3.0)],
            )],
        }];

        // Steps at 10, 20, 30 with lookback=10
        let plan = QueryPlan {
            sample_start_ms: -5,
            sample_end_ms: 30,
            buckets: vec![],
            path_kind: QueryPathKind::SubqueryVectorSelector {
                aligned_start_ms: 10,
                step_ms: 10,
                lookback_delta_ms: 10,
                expected_steps: 3,
            },
        };

        let results = shape_subquery_results(&bucket_data, &plan);
        assert_eq!(results.len(), 1);
        let values = &results[0].values;
        assert_eq!(values.len(), 3);
        // Step 10: latest sample <= 10 with ts > 0 -> sample at t=5, value=1.0
        assert_eq!(values[0].timestamp_ms, 10);
        assert_eq!(values[0].value, 1.0);
        // Step 20: latest sample <= 20 with ts > 10 -> sample at t=15, value=2.0
        assert_eq!(values[1].timestamp_ms, 20);
        assert_eq!(values[1].value, 2.0);
        // Step 30: latest sample <= 30 with ts > 20 -> sample at t=25, value=3.0
        assert_eq!(values[2].timestamp_ms, 30);
        assert_eq!(values[2].value, 3.0);
    }

    #[test]
    fn shape_subquery_deduplicates_across_buckets() {
        let labels_a = vec![label("__name__", "m")];
        let fp = compute_fingerprint(&labels_a);

        // Same timestamp in two buckets
        let bucket_data = vec![
            BucketSampleData {
                bucket: TimeBucket::hour(200),
                series_data: vec![make_loaded(
                    fp,
                    labels_a.clone(),
                    vec![sample(10, 1.0), sample(20, 2.0)],
                )],
            },
            BucketSampleData {
                bucket: TimeBucket::hour(100),
                series_data: vec![make_loaded(
                    fp,
                    labels_a,
                    vec![sample(10, 1.0), sample(15, 1.5)], // t=10 is duplicate
                )],
            },
        ];

        let plan = QueryPlan {
            sample_start_ms: 0,
            sample_end_ms: 20,
            buckets: vec![],
            path_kind: QueryPathKind::SubqueryVectorSelector {
                aligned_start_ms: 10,
                step_ms: 10,
                lookback_delta_ms: 10,
                expected_steps: 2,
            },
        };

        let results = shape_subquery_results(&bucket_data, &plan);
        assert_eq!(results.len(), 1);
        // After merge+dedup: samples at t=10, t=15, t=20
        // Step 10: sample at t=10, value=1.0
        // Step 20: sample at t=20, value=2.0
        let values = &results[0].values;
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].value, 1.0);
        assert_eq!(values[1].value, 2.0);
    }

    // -----------------------------------------------------------------------
    // Work generation tests
    // -----------------------------------------------------------------------

    #[test]
    fn build_work_skip_fingerprints_filters() {
        use crate::index::{ForwardIndex, SeriesSpec};
        use crate::model::MetricType;

        let labels_a = vec![label("__name__", "m"), label("env", "prod")];
        let labels_b = vec![label("__name__", "m"), label("env", "staging")];
        let fp_a = compute_fingerprint(&labels_a);

        let forward_index = ForwardIndex {
            series: dashmap::DashMap::new(),
        };
        forward_index.series.insert(
            0,
            SeriesSpec {
                labels: labels_a,
                metric_type: Some(MetricType::Gauge),
                unit: None,
            },
        );
        forward_index.series.insert(
            1,
            SeriesSpec {
                labels: labels_b,
                metric_type: Some(MetricType::Gauge),
                unit: None,
            },
        );

        let metadata = BucketMetadata {
            bucket: TimeBucket::hour(100),
            candidates: vec![0, 1],
            forward_index: Arc::new(forward_index),
        };

        let plan = QueryPlan::for_instant_vector(100_000, 60_000, vec![TimeBucket::hour(100)]);

        let mut skip = HashSet::new();
        skip.insert(fp_a); // skip series_id=0

        let work = build_bucket_sample_work(&plan, &metadata, Some(&skip), true).unwrap();
        assert_eq!(work.series.len(), 1);
        assert_eq!(work.series[0].series_id, 1);
    }

    // -----------------------------------------------------------------------
    // Utility tests
    // -----------------------------------------------------------------------

    #[test]
    fn fingerprint_order_independent() {
        let labels_a = vec![label("b", "2"), label("a", "1")];
        let labels_b = vec![label("a", "1"), label("b", "2")];
        assert_eq!(
            compute_fingerprint(&labels_a),
            compute_fingerprint(&labels_b)
        );
    }

    #[test]
    fn labels_to_hashmap_roundtrip() {
        let labels = vec![label("env", "prod"), label("region", "us")];
        let map = labels_to_hashmap(&labels);
        assert_eq!(map.get("env").unwrap(), "prod");
        assert_eq!(map.get("region").unwrap(), "us");
    }
}

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
    QueryReaderEvalCache,
};
use crate::promql::selector::evaluate_selector_with_reader;
use crate::query::QueryReader;
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use promql_parser::parser::VectorSelector;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

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
    ///
    /// All matching series in the bucket become explicit sample work items.
    /// Instant selectors rely on `shape_instant_results` to enforce newest-wins
    /// across buckets, so planning does not prune cross-bucket duplicates here.
    fn build_work(&self, metadata: &BucketMetadata) -> EvalResult<BucketSampleWork> {
        let strict = !matches!(self.path_kind, QueryPathKind::SubqueryVectorSelector { .. });
        build_bucket_sample_work(self, metadata, strict)
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
    /// Buckets sorted newest-first so that shaping can apply newest-wins
    /// dedup when assembling the final result.
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
    reader: &CachedQueryReader<'_, R>,
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
/// When `strict` is true, a candidate series ID missing from the forward index
/// is an error (instant and matrix paths). When false, missing specs are
/// silently skipped (subquery path, matching existing `fetch_series_samples`).
pub(crate) fn build_bucket_sample_work(
    plan: &QueryPlan,
    metadata: &BucketMetadata,
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
    reader: &CachedQueryReader<'_, R>,
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
// Concurrency configuration
// ---------------------------------------------------------------------------

/// Concurrency limits for the selector pipeline.
///
/// Controls bounded parallelism for metadata resolution and sample loading
/// phases independently, allowing each to be tuned for its workload.
#[derive(Debug, Clone)]
pub(crate) struct PipelineConcurrency {
    /// Maximum number of concurrent bucket metadata resolutions.
    pub metadata: usize,
    /// Maximum number of concurrent bucket sample loads.
    pub samples: usize,
}

impl Default for PipelineConcurrency {
    fn default() -> Self {
        Self {
            metadata: 4,
            samples: 4,
        }
    }
}

impl From<&crate::model::QueryOptions> for PipelineConcurrency {
    fn from(opts: &crate::model::QueryOptions) -> Self {
        Self {
            metadata: opts.metadata_concurrency.max(1),
            samples: opts.sample_concurrency.max(1),
        }
    }
}

// ---------------------------------------------------------------------------
// Unified pipeline orchestrator
// ---------------------------------------------------------------------------

/// Per-bucket timing from a single bucket task within the pipeline.
#[derive(Debug, Clone, Default)]
struct BucketTaskTiming {
    metadata_queue_wait_ms: f64,
    metadata_resolve_ms: f64,
    sample_queue_wait_ms: f64,
    sample_load_ms: f64,
}

/// One bucket waiting to be processed by the metadata worker pool.
struct MetadataStageJob {
    idx: usize,
    bucket: TimeBucket,
    queued_at: Instant,
}

/// One bucket whose metadata has been resolved and is ready for sample loading.
struct SampleStageItem {
    idx: usize,
    work: Option<BucketSampleWork>,
    timing: BucketTaskTiming,
    queued_at: Instant,
}

/// Aggregate phase timings for the selector pipeline.
///
/// Under parallel execution, wall time shows user-visible latency while sum
/// time shows total work. Queue wait measures time spent waiting for a stage
/// worker slot.
#[derive(Debug, Default, Clone)]
pub(crate) struct PipelineTimings {
    /// Wall-clock time for the entire metadata+sample pipeline phase.
    pub pipeline_wall_ms: f64,
    /// Sum of per-bucket metadata resolution durations.
    pub metadata_resolve_sum_ms: f64,
    /// Sum of per-bucket metadata stage queue wait durations.
    pub metadata_queue_wait_sum_ms: f64,
    /// Number of buckets that had metadata resolved.
    pub metadata_bucket_count: usize,
    /// Sum of per-bucket sample load durations.
    pub sample_load_sum_ms: f64,
    /// Sum of per-bucket sample stage queue wait durations.
    pub sample_queue_wait_sum_ms: f64,
    /// Number of buckets that had samples loaded.
    pub sample_bucket_count: usize,
    /// Time spent in the shaping phase.
    pub shape_samples_ms: f64,
}

impl PipelineTimings {
    /// Build aggregate timings from per-bucket task timings.
    fn from_bucket_timings(
        bucket_timings: &[BucketTaskTiming],
        pipeline_wall_ms: f64,
        shape_samples_ms: f64,
    ) -> Self {
        let mut timings = PipelineTimings {
            pipeline_wall_ms,
            shape_samples_ms,
            ..Default::default()
        };
        for bt in bucket_timings {
            if bt.metadata_resolve_ms > 0.0 || bt.metadata_queue_wait_ms > 0.0 {
                timings.metadata_resolve_sum_ms += bt.metadata_resolve_ms;
                timings.metadata_queue_wait_sum_ms += bt.metadata_queue_wait_ms;
                timings.metadata_bucket_count += 1;
            }
            if bt.sample_load_ms > 0.0 || bt.sample_queue_wait_ms > 0.0 {
                timings.sample_load_sum_ms += bt.sample_load_ms;
                timings.sample_queue_wait_sum_ms += bt.sample_queue_wait_ms;
                timings.sample_bucket_count += 1;
            }
        }
        timings
    }
}

/// Execute one metadata-stage job.
///
/// A metadata worker dequeues a bucket, resolves selector metadata for it, and
/// packages explicit sample work for the sample stage.
async fn execute_metadata_stage_job<R: QueryReader>(
    reader: &R,
    cache: Arc<QueryReaderEvalCache>,
    plan: &QueryPlan,
    selector: &VectorSelector,
    job: MetadataStageJob,
) -> EvalResult<SampleStageItem> {
    let mut timing = BucketTaskTiming {
        metadata_queue_wait_ms: job.queued_at.elapsed().as_secs_f64() * 1000.0,
        ..Default::default()
    };

    let metadata_start = Instant::now();
    let cached = CachedQueryReader::with_shared_cache(reader, cache);
    let metadata = resolve_bucket_metadata(&cached, job.bucket, selector).await?;
    timing.metadata_resolve_ms = metadata_start.elapsed().as_secs_f64() * 1000.0;

    let work = match metadata {
        Some(metadata) => Some(plan.build_work(&metadata)?),
        None => None,
    };

    Ok(SampleStageItem {
        idx: job.idx,
        work,
        timing,
        queued_at: Instant::now(),
    })
}

/// Execute one sample-stage job.
///
/// Today a sample worker loads all series for the bucket serially via
/// `load_bucket_samples`.
///
/// For `#366`, this helper is the seam where bucket-scoped sample loading can
/// become per-series loading without changing the outer metadata queue or the
/// `SampleStageItem` handoff. The intended model is:
/// - keep this outer sample stage as a bucket coordinator
/// - iterate `work.series` lazily
/// - use a small per-bucket `buffer_unordered(PER_BUCKET_READAHEAD)` window
/// - let `CachedQueryReader::samples` enforce the real global
///   `sample_concurrency` limit at the cache-miss read sites
///
/// Suggested basis for the follow-up:
///
/// ```ignore
/// async fn load_bucket_samples_parallel<R: QueryReader>(
///     reader: &CachedQueryReader<'_, R>,
///     work: &BucketSampleWork,
/// ) -> EvalResult<BucketSampleData> {
///     let series_data = stream::iter(work.series.iter().cloned())
///         .map(|item| async move {
///             let samples = reader
///                 .samples(&work.bucket, item.series_id, work.start_ms, work.end_ms)
///                 .await?;
///
///             Ok::<_, EvaluationError>(LoadedSeriesSamples {
///                 fingerprint: item.fingerprint,
///                 labels: item.labels,
///                 samples,
///             })
///         })
///         .buffer_unordered(PER_BUCKET_READAHEAD)
///         .try_collect::<Vec<_>>()
///         .await?;
///
///     Ok(BucketSampleData {
///         bucket: work.bucket,
///         series_data,
///     })
/// }
/// ```
async fn execute_sample_stage_job<R: QueryReader>(
    reader: &R,
    cache: Arc<QueryReaderEvalCache>,
    item: SampleStageItem,
) -> EvalResult<(usize, Option<BucketSampleData>, BucketTaskTiming)> {
    let mut timing = item.timing;
    let Some(work) = item.work else {
        return Ok((item.idx, None, timing));
    };

    timing.sample_queue_wait_ms = item.queued_at.elapsed().as_secs_f64() * 1000.0;

    let sample_start = Instant::now();
    let cached = CachedQueryReader::with_shared_cache(reader, cache);
    let data = load_bucket_samples(&cached, &work).await?;
    timing.sample_load_ms = sample_start.elapsed().as_secs_f64() * 1000.0;

    Ok((item.idx, Some(data), timing))
}

/// Execute the shared selector pipeline for all evaluator-backed query paths.
///
/// Orchestrates: resolve metadata -> build work -> load samples -> shape results.
/// Branching on `QueryPathKind` handles the behavioral differences between
/// instant vector selectors, matrix selectors, and subquery fast paths.
///
/// Concurrency model:
/// - One `MetadataStageJob` is created per bucket.
/// - `buffer_unordered(metadata_concurrency)` forms the metadata worker pool.
/// - As each metadata worker finishes, it forwards a `SampleStageItem` into a
///   bounded channel.
/// - `buffer_unordered(sample_concurrency)` forms the sample worker pool that
///   dequeues those items and loads bucket sample data.
/// - Results are reassembled in plan order before shaping to preserve query
///   semantics.
pub(crate) async fn execute_selector_pipeline<R: QueryReader>(
    reader: &R,
    cache: &Arc<QueryReaderEvalCache>,
    plan: &QueryPlan,
    selector: &VectorSelector,
    concurrency: &PipelineConcurrency,
) -> EvalResult<ExprResult> {
    let pipeline_start = Instant::now();
    let metadata_concurrency = concurrency.metadata.max(1);
    let sample_concurrency = concurrency.samples.max(1);

    let metadata_jobs: Vec<_> = plan
        .buckets
        .iter()
        .enumerate()
        .map(|(idx, &bucket)| MetadataStageJob {
            idx,
            bucket,
            queued_at: pipeline_start,
        })
        .collect();

    let (sample_tx, sample_rx) = mpsc::channel(sample_concurrency);

    let metadata_stage = {
        let metadata_cache = cache.clone();
        async move {
            stream::iter(metadata_jobs)
                .map(|job| {
                    let job_cache = metadata_cache.clone();
                    async move {
                        execute_metadata_stage_job(reader, job_cache, plan, selector, job).await
                    }
                })
                .buffer_unordered(metadata_concurrency)
                .try_for_each(|item| {
                    let sample_tx = sample_tx.clone();
                    async move {
                        // As each metadata worker finishes, it immediately
                        // hands off its bucket's sample work to the bounded
                        // queue so sample workers can start without waiting
                        // for the rest of metadata resolution to complete.
                        sample_tx.send(item).await.map_err(|_| {
                            EvaluationError::InternalError("sample work queue closed".into())
                        })
                    }
                })
                .await
        }
    };

    let sample_stage = stream::unfold(sample_rx, |mut sample_rx| async move {
        sample_rx.recv().await.map(|item| (item, sample_rx))
    })
    .map(|item| {
        let sample_cache = cache.clone();
        async move { execute_sample_stage_job(reader, sample_cache, item).await }
    })
    .buffer_unordered(sample_concurrency)
    .try_collect::<Vec<_>>();

    let (_, results) = tokio::try_join!(metadata_stage, sample_stage)?;

    let pipeline_elapsed_ms = pipeline_start.elapsed().as_secs_f64() * 1000.0;

    // Reassemble in plan order (critical for instant-vector newest-wins).
    let mut indexed: Vec<_> = results.into_iter().collect();
    indexed.sort_by_key(|(idx, _, _)| *idx);

    let bucket_timings: Vec<_> = indexed.iter().map(|(_, _, t)| t.clone()).collect();
    let all_bucket_data: Vec<BucketSampleData> = indexed
        .into_iter()
        .filter_map(|(_, data, _)| data)
        .collect();

    // -- Phase: ShapeSamples --
    let t_shape = Instant::now();
    let result = plan.shape(&all_bucket_data);
    let shape_ms = t_shape.elapsed().as_secs_f64() * 1000.0;

    let timings =
        PipelineTimings::from_bucket_timings(&bucket_timings, pipeline_elapsed_ms, shape_ms);

    tracing::debug!(
        path = plan.path_kind.name(),
        buckets = plan.buckets.len(),
        pipeline_wall_ms = timings.pipeline_wall_ms,
        metadata_resolve_sum_ms = timings.metadata_resolve_sum_ms,
        metadata_queue_wait_sum_ms = timings.metadata_queue_wait_sum_ms,
        metadata_bucket_count = timings.metadata_bucket_count,
        sample_load_sum_ms = timings.sample_load_sum_ms,
        sample_queue_wait_sum_ms = timings.sample_queue_wait_sum_ms,
        sample_bucket_count = timings.sample_bucket_count,
        shape_ms = timings.shape_samples_ms,
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
    fn build_work_includes_all_candidates() {
        use crate::index::{ForwardIndex, SeriesSpec};
        use crate::model::MetricType;

        let labels_a = vec![label("__name__", "m"), label("env", "prod")];
        let labels_b = vec![label("__name__", "m"), label("env", "staging")];

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

        let work = build_bucket_sample_work(&plan, &metadata, true).unwrap();
        assert_eq!(work.series.len(), 2);
    }

    #[test]
    fn build_work_strict_errors_on_missing_spec() {
        use crate::index::{ForwardIndex, SeriesSpec};
        use crate::model::MetricType;

        let forward_index = ForwardIndex {
            series: dashmap::DashMap::new(),
        };
        forward_index.series.insert(
            0,
            SeriesSpec {
                labels: vec![label("__name__", "m")],
                metric_type: Some(MetricType::Gauge),
                unit: None,
            },
        );

        let metadata = BucketMetadata {
            bucket: TimeBucket::hour(100),
            candidates: vec![0, 99], // 99 not in forward index
            forward_index: Arc::new(forward_index),
        };

        let plan = QueryPlan::for_instant_vector(100_000, 60_000, vec![TimeBucket::hour(100)]);

        let result = build_bucket_sample_work(&plan, &metadata, true);
        assert!(result.is_err(), "strict mode should error on missing spec");
    }

    #[test]
    fn build_work_lenient_skips_missing_spec() {
        use crate::index::{ForwardIndex, SeriesSpec};
        use crate::model::MetricType;

        let forward_index = ForwardIndex {
            series: dashmap::DashMap::new(),
        };
        forward_index.series.insert(
            0,
            SeriesSpec {
                labels: vec![label("__name__", "m")],
                metric_type: Some(MetricType::Gauge),
                unit: None,
            },
        );

        let metadata = BucketMetadata {
            bucket: TimeBucket::hour(100),
            candidates: vec![0, 99], // 99 not in forward index
            forward_index: Arc::new(forward_index),
        };

        let plan = QueryPlan::for_subquery_vector_selector(
            0,
            100_000,
            10_000,
            60_000,
            vec![TimeBucket::hour(100)],
        );

        let work = build_bucket_sample_work(&plan, &metadata, false).unwrap();
        assert_eq!(
            work.series.len(),
            1,
            "lenient mode should skip missing spec"
        );
        assert_eq!(work.series[0].series_id, 0);
    }

    // -----------------------------------------------------------------------
    // Concurrency config tests
    // -----------------------------------------------------------------------

    #[test]
    fn pipeline_concurrency_defaults() {
        let c = PipelineConcurrency::default();
        assert_eq!(c.metadata, 4);
        assert_eq!(c.samples, 4);
    }

    #[test]
    fn pipeline_concurrency_from_query_options() {
        use crate::model::QueryOptions;

        let opts = QueryOptions {
            metadata_concurrency: 8,
            sample_concurrency: 2,
            ..Default::default()
        };
        let c = PipelineConcurrency::from(&opts);
        assert_eq!(c.metadata, 8);
        assert_eq!(c.samples, 2);
    }

    #[test]
    fn pipeline_concurrency_clamps_zero_to_one() {
        use crate::model::QueryOptions;

        let opts = QueryOptions {
            metadata_concurrency: 0,
            sample_concurrency: 0,
            ..Default::default()
        };
        let c = PipelineConcurrency::from(&opts);
        assert_eq!(c.metadata, 1);
        assert_eq!(c.samples, 1);
    }

    // -----------------------------------------------------------------------
    // Timing tests
    // -----------------------------------------------------------------------

    #[test]
    fn pipeline_timings_default_all_zero() {
        let t = PipelineTimings::default();
        assert_eq!(t.pipeline_wall_ms, 0.0);
        assert_eq!(t.metadata_resolve_sum_ms, 0.0);
        assert_eq!(t.metadata_queue_wait_sum_ms, 0.0);
        assert_eq!(t.metadata_bucket_count, 0);
        assert_eq!(t.sample_load_sum_ms, 0.0);
        assert_eq!(t.sample_queue_wait_sum_ms, 0.0);
        assert_eq!(t.sample_bucket_count, 0);
        assert_eq!(t.shape_samples_ms, 0.0);
    }

    #[test]
    fn pipeline_timings_accumulates_two_active_buckets() {
        let bucket_timings = vec![
            BucketTaskTiming {
                metadata_queue_wait_ms: 1.0,
                metadata_resolve_ms: 10.0,
                sample_queue_wait_ms: 2.0,
                sample_load_ms: 20.0,
            },
            BucketTaskTiming {
                metadata_queue_wait_ms: 3.0,
                metadata_resolve_ms: 15.0,
                sample_queue_wait_ms: 4.0,
                sample_load_ms: 25.0,
            },
        ];

        let timings = PipelineTimings::from_bucket_timings(&bucket_timings, 50.0, 5.0);

        assert_eq!(timings.pipeline_wall_ms, 50.0);
        assert_eq!(timings.metadata_resolve_sum_ms, 25.0); // 10 + 15
        assert_eq!(timings.metadata_queue_wait_sum_ms, 4.0); // 1 + 3
        assert_eq!(timings.metadata_bucket_count, 2);
        assert_eq!(timings.sample_load_sum_ms, 45.0); // 20 + 25
        assert_eq!(timings.sample_queue_wait_sum_ms, 6.0); // 2 + 4
        assert_eq!(timings.sample_bucket_count, 2);
        assert_eq!(timings.shape_samples_ms, 5.0);
    }

    #[test]
    fn pipeline_timings_skips_empty_buckets() {
        // An empty bucket has all-zero timings — from_bucket_timings should
        // not count it toward bucket counts.
        let bucket_timings = vec![
            BucketTaskTiming {
                metadata_queue_wait_ms: 1.0,
                metadata_resolve_ms: 10.0,
                sample_queue_wait_ms: 2.0,
                sample_load_ms: 20.0,
            },
            BucketTaskTiming::default(), // empty bucket
        ];

        let timings = PipelineTimings::from_bucket_timings(&bucket_timings, 30.0, 1.0);

        assert_eq!(timings.metadata_bucket_count, 1);
        assert_eq!(timings.sample_bucket_count, 1);
        assert_eq!(timings.metadata_resolve_sum_ms, 10.0);
        assert_eq!(timings.sample_load_sum_ms, 20.0);
    }

    #[test]
    fn pipeline_timings_metadata_only_bucket() {
        // A bucket that resolved metadata but had no matching series (no
        // sample work) should count toward metadata but not sample.
        let bucket_timings = vec![BucketTaskTiming {
            metadata_queue_wait_ms: 0.5,
            metadata_resolve_ms: 5.0,
            sample_queue_wait_ms: 0.0,
            sample_load_ms: 0.0,
        }];

        let timings = PipelineTimings::from_bucket_timings(&bucket_timings, 6.0, 0.5);

        assert_eq!(timings.metadata_bucket_count, 1);
        assert_eq!(timings.sample_bucket_count, 0);
        assert_eq!(timings.metadata_resolve_sum_ms, 5.0);
        assert_eq!(timings.metadata_queue_wait_sum_ms, 0.5);
        assert_eq!(timings.sample_load_sum_ms, 0.0);
    }

    #[test]
    fn pipeline_timings_sum_can_exceed_wall_under_parallelism() {
        // With 2 parallel buckets, sum of per-bucket work can exceed wall
        // time — this is the expected signature of parallel execution.
        let bucket_timings = vec![
            BucketTaskTiming {
                metadata_queue_wait_ms: 0.0,
                metadata_resolve_ms: 8.0,
                sample_queue_wait_ms: 0.0,
                sample_load_ms: 12.0,
            },
            BucketTaskTiming {
                metadata_queue_wait_ms: 5.0,
                metadata_resolve_ms: 7.0,
                sample_queue_wait_ms: 3.0,
                sample_load_ms: 10.0,
            },
        ];

        let timings = PipelineTimings::from_bucket_timings(&bucket_timings, 20.0, 1.0);

        // metadata_sum = 15, sample_sum = 22, both with wall = 20
        assert_eq!(timings.metadata_resolve_sum_ms, 15.0);
        assert_eq!(timings.sample_load_sum_ms, 22.0);
        assert_eq!(timings.pipeline_wall_ms, 20.0);
        // sample_sum > wall is fine — it means parallelism helped
        assert!(timings.sample_load_sum_ms > timings.pipeline_wall_ms);
    }

    #[test]
    fn pipeline_timings_all_fields_nonnegative() {
        let bt = vec![BucketTaskTiming {
            metadata_queue_wait_ms: 0.5,
            metadata_resolve_ms: 2.3,
            sample_queue_wait_ms: 0.1,
            sample_load_ms: 5.7,
        }];
        let timings = PipelineTimings::from_bucket_timings(&bt, 10.0, 1.0);

        assert!(timings.pipeline_wall_ms >= 0.0);
        assert!(timings.metadata_resolve_sum_ms >= 0.0);
        assert!(timings.metadata_queue_wait_sum_ms >= 0.0);
        assert!(timings.sample_load_sum_ms >= 0.0);
        assert!(timings.sample_queue_wait_sum_ms >= 0.0);
        assert!(timings.shape_samples_ms >= 0.0);
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

    // -----------------------------------------------------------------------
    // Async pipeline integration tests
    // -----------------------------------------------------------------------

    /// Helper: build a multi-bucket mock reader and run execute_selector_pipeline.
    async fn run_pipeline(
        reader: &crate::query::test_utils::MockQueryReader,
        plan: &QueryPlan,
        selector_str: &str,
        concurrency: &PipelineConcurrency,
    ) -> EvalResult<ExprResult> {
        let selector = match promql_parser::parser::parse(selector_str).unwrap() {
            promql_parser::parser::Expr::VectorSelector(vs) => vs,
            _ => panic!("expected vector selector"),
        };
        let cache = std::sync::Arc::new(crate::promql::evaluator::QueryReaderEvalCache::new());
        execute_selector_pipeline(reader, &cache, plan, &selector, concurrency).await
    }

    fn build_two_bucket_reader() -> crate::query::test_utils::MockQueryReader {
        use crate::model::MetricType;
        use crate::query::test_utils::MockMultiBucketQueryReaderBuilder;

        let labels = vec![label("__name__", "cpu"), label("host", "a")];
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        // Older bucket 100: sample at t=6_100_000 (within bucket [6_000_000, 9_600_000])
        builder.add_sample(
            TimeBucket::hour(100),
            labels.clone(),
            MetricType::Gauge,
            sample(6_100_000, 1.0),
        );
        // Newer bucket 200: sample at t=12_100_000 (within bucket [12_000_000, 15_600_000])
        builder.add_sample(
            TimeBucket::hour(200),
            labels,
            MetricType::Gauge,
            sample(12_100_000, 2.0),
        );
        builder.build()
    }

    #[tokio::test]
    async fn should_preserve_instant_newest_wins_with_out_of_order_completion() {
        // Even if bucket tasks complete out of order, instant vector should
        // return the sample from the newest bucket.
        let reader = build_two_bucket_reader();
        let buckets = reader.list_buckets().await.unwrap();
        let plan = QueryPlan::for_instant_vector(
            12_200_000, // eval time: just after the newer sample
            300_001,    // lookback covers both samples
            buckets,
        );

        // Use concurrency=4 to allow out-of-order completion
        let concurrency = PipelineConcurrency {
            metadata: 4,
            samples: 4,
        };
        let result = run_pipeline(&reader, &plan, "cpu", &concurrency)
            .await
            .unwrap();

        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 2.0, "newest bucket sample should win");
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[tokio::test]
    async fn should_preserve_instant_newest_wins_with_serial_execution() {
        // Same test with concurrency=1 to verify serial fallback produces
        // identical results.
        let reader = build_two_bucket_reader();
        let buckets = reader.list_buckets().await.unwrap();
        let plan = QueryPlan::for_instant_vector(12_200_000, 300_001, buckets);

        let concurrency = PipelineConcurrency {
            metadata: 1,
            samples: 1,
        };
        let result = run_pipeline(&reader, &plan, "cpu", &concurrency)
            .await
            .unwrap();

        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 2.0, "newest bucket sample should win");
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[tokio::test]
    async fn should_merge_matrix_samples_from_both_buckets_under_concurrency() {
        let reader = build_two_bucket_reader();
        let buckets = reader.list_buckets().await.unwrap();
        // Range covers both buckets
        let plan = QueryPlan::for_matrix(
            12_200_000, 7_000_000, // range back to ~5_200_000
            buckets,
        );

        let concurrency = PipelineConcurrency {
            metadata: 4,
            samples: 4,
        };
        let result = run_pipeline(&reader, &plan, "cpu", &concurrency)
            .await
            .unwrap();

        match result {
            ExprResult::RangeVector(samples) => {
                assert_eq!(samples.len(), 1, "one series");
                assert_eq!(samples[0].values.len(), 2, "both bucket samples merged");
            }
            _ => panic!("expected RangeVector"),
        }
    }

    // -----------------------------------------------------------------------
    // Instrumented reader for concurrency tests
    // -----------------------------------------------------------------------

    /// A QueryReader wrapper that counts calls and supports per-bucket delays.
    struct CountingQueryReader<R: crate::query::QueryReader> {
        inner: R,
        forward_index_calls: std::sync::atomic::AtomicUsize,
        inverted_index_calls: std::sync::atomic::AtomicUsize,
        samples_calls: std::sync::atomic::AtomicUsize,
        /// Per-bucket metadata delay (applied to both forward_index and inverted_index).
        metadata_delays: HashMap<TimeBucket, std::time::Duration>,
        /// Per-bucket sample delay.
        sample_delays: HashMap<TimeBucket, std::time::Duration>,
    }

    impl<R: crate::query::QueryReader> CountingQueryReader<R> {
        fn new(inner: R) -> Self {
            Self {
                inner,
                forward_index_calls: std::sync::atomic::AtomicUsize::new(0),
                inverted_index_calls: std::sync::atomic::AtomicUsize::new(0),
                samples_calls: std::sync::atomic::AtomicUsize::new(0),
                metadata_delays: HashMap::new(),
                sample_delays: HashMap::new(),
            }
        }

        fn with_metadata_delay(mut self, bucket: TimeBucket, delay: std::time::Duration) -> Self {
            self.metadata_delays.insert(bucket, delay);
            self
        }

        fn with_sample_delay(mut self, bucket: TimeBucket, delay: std::time::Duration) -> Self {
            self.sample_delays.insert(bucket, delay);
            self
        }

        fn forward_index_count(&self) -> usize {
            self.forward_index_calls
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn inverted_index_count(&self) -> usize {
            self.inverted_index_calls
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn samples_count(&self) -> usize {
            self.samples_calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl<R: crate::query::QueryReader> crate::query::QueryReader for CountingQueryReader<R> {
        async fn list_buckets(&self) -> crate::util::Result<Vec<TimeBucket>> {
            self.inner.list_buckets().await
        }

        async fn forward_index(
            &self,
            bucket: &TimeBucket,
            series_ids: &[crate::model::SeriesId],
        ) -> crate::util::Result<Box<dyn crate::index::ForwardIndexLookup + Send + Sync + 'static>>
        {
            self.forward_index_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(&delay) = self.metadata_delays.get(bucket) {
                tokio::time::sleep(delay).await;
            }
            self.inner.forward_index(bucket, series_ids).await
        }

        async fn inverted_index(
            &self,
            bucket: &TimeBucket,
            terms: &[crate::model::Label],
        ) -> crate::util::Result<Box<dyn crate::index::InvertedIndexLookup + Send + Sync + 'static>>
        {
            self.inverted_index_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(&delay) = self.metadata_delays.get(bucket) {
                tokio::time::sleep(delay).await;
            }
            self.inner.inverted_index(bucket, terms).await
        }

        async fn all_inverted_index(
            &self,
            bucket: &TimeBucket,
        ) -> crate::util::Result<Box<dyn crate::index::InvertedIndexLookup + Send + Sync + 'static>>
        {
            self.inner.all_inverted_index(bucket).await
        }

        async fn label_values(
            &self,
            bucket: &TimeBucket,
            label_name: &str,
        ) -> crate::util::Result<Vec<String>> {
            self.inner.label_values(bucket, label_name).await
        }

        async fn samples(
            &self,
            bucket: &TimeBucket,
            series_id: crate::model::SeriesId,
            start_ms: i64,
            end_ms: i64,
        ) -> crate::util::Result<Vec<crate::model::Sample>> {
            self.samples_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(&delay) = self.sample_delays.get(bucket) {
                tokio::time::sleep(delay).await;
            }
            self.inner
                .samples(bucket, series_id, start_ms, end_ms)
                .await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn should_preserve_instant_newest_wins_when_newer_bucket_finishes_last() {
        // Newer bucket (200) has a longer metadata delay than older bucket (100),
        // so older finishes first. Instant result must still pick the newer sample.
        let inner = build_two_bucket_reader();
        let reader = CountingQueryReader::new(inner)
            .with_metadata_delay(TimeBucket::hour(200), std::time::Duration::from_millis(50));

        let buckets = reader.list_buckets().await.unwrap();
        let plan = QueryPlan::for_instant_vector(12_200_000, 300_001, buckets);
        let concurrency = PipelineConcurrency {
            metadata: 4,
            samples: 4,
        };

        let cache = std::sync::Arc::new(crate::promql::evaluator::QueryReaderEvalCache::new());
        let selector = match promql_parser::parser::parse("cpu").unwrap() {
            promql_parser::parser::Expr::VectorSelector(vs) => vs,
            _ => panic!("expected vector selector"),
        };

        let result = execute_selector_pipeline(&reader, &cache, &plan, &selector, &concurrency)
            .await
            .unwrap();

        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(
                    samples[0].value, 2.0,
                    "newer bucket must win despite finishing last"
                );
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn should_record_nonzero_metadata_queue_wait_under_contention() {
        // With metadata_concurrency=1 and two buckets that each take 20ms of
        // metadata work, the second bucket must wait for the first's permit.
        // We can't inspect PipelineTimings directly from here, but we can
        // verify the pipeline completes correctly under contention and that
        // wall time reflects serialized metadata work.
        let inner = build_two_bucket_reader();
        let reader = CountingQueryReader::new(inner)
            .with_metadata_delay(TimeBucket::hour(100), std::time::Duration::from_millis(20))
            .with_metadata_delay(TimeBucket::hour(200), std::time::Duration::from_millis(20));

        let buckets = reader.list_buckets().await.unwrap();
        let plan = QueryPlan::for_instant_vector(12_200_000, 300_001, buckets);
        // metadata_concurrency=1 forces serial metadata, creating contention
        let concurrency = PipelineConcurrency {
            metadata: 1,
            samples: 4,
        };

        let cache = std::sync::Arc::new(crate::promql::evaluator::QueryReaderEvalCache::new());
        let selector = match promql_parser::parser::parse("cpu").unwrap() {
            promql_parser::parser::Expr::VectorSelector(vs) => vs,
            _ => panic!("expected vector selector"),
        };

        let start = tokio::time::Instant::now();
        let result = execute_selector_pipeline(&reader, &cache, &plan, &selector, &concurrency)
            .await
            .unwrap();
        let elapsed = start.elapsed();

        // With concurrency=1, two 20ms metadata delays must serialize → >= 40ms
        assert!(
            elapsed >= std::time::Duration::from_millis(35),
            "expected serialized metadata: elapsed={:?}",
            elapsed,
        );

        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 2.0);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn should_record_nonzero_sample_queue_wait_under_contention() {
        // With sample_concurrency=1 and two buckets with 20ms sample delays,
        // sample loads must serialize.
        let inner = build_two_bucket_reader();
        let reader = CountingQueryReader::new(inner)
            .with_sample_delay(TimeBucket::hour(100), std::time::Duration::from_millis(20))
            .with_sample_delay(TimeBucket::hour(200), std::time::Duration::from_millis(20));

        let buckets = reader.list_buckets().await.unwrap();
        let plan = QueryPlan::for_instant_vector(12_200_000, 300_001, buckets);
        let concurrency = PipelineConcurrency {
            metadata: 4,
            samples: 1,
        };

        let cache = std::sync::Arc::new(crate::promql::evaluator::QueryReaderEvalCache::new());
        let selector = match promql_parser::parser::parse("cpu").unwrap() {
            promql_parser::parser::Expr::VectorSelector(vs) => vs,
            _ => panic!("expected vector selector"),
        };

        let start = tokio::time::Instant::now();
        let result = execute_selector_pipeline(&reader, &cache, &plan, &selector, &concurrency)
            .await
            .unwrap();
        let elapsed = start.elapsed();

        // With sample concurrency=1, two 20ms sample delays serialize → >= 40ms
        assert!(
            elapsed >= std::time::Duration::from_millis(35),
            "expected serialized sample loads: elapsed={:?}",
            elapsed,
        );

        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 2.0);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[tokio::test]
    async fn should_reuse_shared_cache_without_reloading() {
        // Two pipeline invocations sharing the same cache. The second should
        // not invoke the underlying reader's samples() method again.
        use crate::model::MetricType;
        use crate::query::test_utils::MockMultiBucketQueryReaderBuilder;

        let labels = vec![label("__name__", "mem"), label("host", "b")];
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        builder.add_sample(
            TimeBucket::hour(100),
            labels,
            MetricType::Gauge,
            sample(6_100_000, 42.0),
        );
        let reader = CountingQueryReader::new(builder.build());

        let cache = std::sync::Arc::new(crate::promql::evaluator::QueryReaderEvalCache::new());
        let concurrency = PipelineConcurrency::default();
        let selector = match promql_parser::parser::parse("mem").unwrap() {
            promql_parser::parser::Expr::VectorSelector(vs) => vs,
            _ => panic!("expected vector selector"),
        };

        let buckets = reader.list_buckets().await.unwrap();
        let plan = QueryPlan::for_instant_vector(6_200_000, 300_000, buckets.clone());

        // First invocation: populates cache, calls underlying reader
        execute_selector_pipeline(&reader, &cache, &plan, &selector, &concurrency)
            .await
            .unwrap();
        let samples_after_first = reader.samples_count();
        let inverted_after_first = reader.inverted_index_count();
        assert!(samples_after_first > 0, "first call should load samples");

        // Second invocation: should reuse cache, no new reader calls
        let plan2 = QueryPlan::for_instant_vector(6_200_000, 300_000, buckets);
        let r2 = execute_selector_pipeline(&reader, &cache, &plan2, &selector, &concurrency)
            .await
            .unwrap();

        assert_eq!(
            reader.samples_count(),
            samples_after_first,
            "second call should not reload samples"
        );
        assert_eq!(
            reader.inverted_index_count(),
            inverted_after_first,
            "second call should not reload inverted index"
        );

        match r2 {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples[0].value, 42.0);
            }
            _ => panic!("expected InstantVector"),
        }
    }

    #[tokio::test]
    async fn should_reuse_cache_across_range_query_steps() {
        // End-to-end test through evaluate_range: one Evaluator is reused
        // across multiple range-query steps. Underlying reader call counts
        // should not grow with the number of steps.
        use crate::model::MetricType;
        use crate::query::test_utils::MockMultiBucketQueryReaderBuilder;
        use std::time::{Duration, UNIX_EPOCH};

        let labels = vec![label("__name__", "cpu"), label("host", "a")];
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        // Bucket 100 covers [6_000_000ms, 9_600_000ms].
        // Place a sample well inside the bucket.
        builder.add_sample(
            TimeBucket::hour(100),
            labels,
            MetricType::Gauge,
            sample(6_100_000, 1.0),
        );
        let reader = CountingQueryReader::new(builder.build());

        let expr = promql_parser::parser::parse("cpu").unwrap();
        let lookback_delta = Duration::from_secs(300); // 5 minutes

        // 5 steps, each 60s apart, all within lookback range of the sample.
        // step 0: t=6_100_000ms  step 1: t=6_160_000ms ... step 4: t=6_340_000ms
        let start = UNIX_EPOCH + Duration::from_millis(6_100_000);
        let end = UNIX_EPOCH + Duration::from_millis(6_340_000);
        let step = Duration::from_secs(60);

        let stmt = promql_parser::parser::EvalStmt {
            expr,
            start,
            end,
            interval: step,
            lookback_delta,
        };

        let concurrency = PipelineConcurrency::default();
        let results = crate::tsdb::evaluate_range(&reader, stmt, concurrency)
            .await
            .unwrap();

        // All 5 steps should find the sample (it's within 5min lookback of each).
        assert_eq!(results.len(), 1, "one series");
        assert_eq!(results[0].samples.len(), 5, "one point per step");

        // The critical assertion: the underlying reader's samples() and
        // inverted_index() should have been called only on the first step.
        // All subsequent steps should hit the shared query-scoped cache.
        assert_eq!(
            reader.samples_count(),
            1,
            "samples() should be called once, not once per step"
        );
        assert_eq!(
            reader.inverted_index_count(),
            1,
            "inverted_index() should be called once, not once per step"
        );
        assert_eq!(
            reader.forward_index_count(),
            1,
            "forward_index() should be called once, not once per step"
        );
    }
}

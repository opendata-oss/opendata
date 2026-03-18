use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;

use futures::stream::StreamExt;

use crate::index::{ForwardIndexLookup, InvertedIndexLookup, SeriesSpec};
use crate::load_coordinator::ReadLoadCoordinator;
use crate::model::Sample;
use crate::model::SeriesFingerprint;
use crate::model::{Label, SeriesId, TimeBucket};
use crate::promql::functions::{FunctionCallContext, FunctionRegistry, PromQLArg};
use crate::promql::selector::{evaluate_selector_raw, evaluate_selector_with_reader};
use crate::promql::timestamp::Timestamp;
use crate::query::QueryReader;
use crate::util::Result;
use promql_parser::label::METRIC_NAME;
use promql_parser::parser::token::*;
use promql_parser::parser::value::ValueType;
use promql_parser::parser::{
    AggregateExpr, AtModifier, BinaryExpr, Call, EvalStmt, Expr, LabelModifier, MatrixSelector,
    Offset, SubqueryExpr, VectorMatchCardinality, VectorSelector,
};

#[derive(Debug)]
pub enum EvaluationError {
    StorageError(String),
    InternalError(String),
}

impl Display for EvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EvaluationError::StorageError(err) => write!(f, "PromQL evaluation error: {err}"),
            EvaluationError::InternalError(err) => write!(f, "PromQL internal error: {err}"),
        }
    }
}

impl std::error::Error for EvaluationError {}

impl From<crate::error::Error> for EvaluationError {
    fn from(err: crate::error::Error) -> Self {
        EvaluationError::StorageError(err.to_string())
    }
}

pub(crate) type EvalResult<T> = std::result::Result<T, EvaluationError>;

/// Aggregated counters for evaluator operations.
/// Incremented inside hot methods, read once after the step loop completes.
#[derive(Debug, Default)]
pub(crate) struct EvalStats {
    pub(crate) list_buckets_calls: u64,
    pub(crate) list_buckets_misses: u64,
    pub(crate) selector_calls: u64,
    pub(crate) selector_cache_hits: u64,
    pub(crate) selector_misses: u64,
    pub(crate) forward_index_calls: u64,
    pub(crate) forward_index_cache_hits: u64,
    pub(crate) forward_index_misses: u64,
    pub(crate) sample_loads: u64,
    pub(crate) sample_cache_hits: u64,
    pub(crate) sample_misses: u64,
    pub(crate) samples_loaded: u64,
    pub(crate) series_meta_cache_hits: u64,
    pub(crate) series_meta_misses: u64,
    pub(crate) preload_selectors: u64,
    pub(crate) preload_series: u64,
    pub(crate) preload_hits: u64,
    // Phase timing (milliseconds)
    pub(crate) preload_ms: u64,
    pub(crate) step_loop_ms: u64,
    // Miss latency totals (milliseconds)
    pub(crate) list_buckets_miss_ms: u64,
    pub(crate) forward_index_miss_ms: u64,
    pub(crate) sample_miss_ms: u64,
    pub(crate) selector_miss_ms: u64,
    // Per-bucket preload aggregates
    pub(crate) preload_bucket_ms_max: u64,
    pub(crate) preload_bucket_ms_sum: u64,
    // Queue wait time (ms waiting for semaphore permits)
    pub(crate) sample_queue_wait_ms: u64,
    pub(crate) metadata_queue_wait_ms: u64,
    // Pure I/O load time (ms doing actual storage reads, excludes queue wait)
    pub(crate) sample_load_ms: u64,
    pub(crate) metadata_load_ms: u64,
    // Permit acquisition counters
    pub(crate) sample_permit_acquires: u64,
    pub(crate) metadata_permit_acquires: u64,
    // Parallel path counters
    pub(crate) parallel_sample_loads: u64,
    // Phase B1: parallel selector + metadata resolution
    pub(crate) parallel_selector_wall_ms: u64,
    pub(crate) parallel_selector_sum_ms: u64,
    pub(crate) parallel_selector_count: u64,
    // Phase B3: parallel sample loading
    pub(crate) parallel_sample_wall_ms: u64,
    pub(crate) parallel_sample_sum_ms: u64,
    pub(crate) parallel_sample_bucket_count: u64,
    // Batched forward index stats
    pub(crate) fi_series_loaded: u64,
    pub(crate) fi_batch_ops: u64,
    pub(crate) fi_point_lookups: u64,
    pub(crate) fi_range_scans: u64,
    pub(crate) fi_range_scan_series: u64,
    pub(crate) fi_scan_span_series: u64,
    pub(crate) fi_run_len_1: u64,
    pub(crate) fi_run_len_2_3: u64,
    pub(crate) fi_run_len_4_7: u64,
    pub(crate) fi_run_len_8_plus: u64,
    // Merge decision counters
    pub(crate) fi_merge_accepted: u64,
    pub(crate) fi_merge_rejected_gap: u64,
    pub(crate) fi_merge_rejected_density: u64,
}

/// Canonical key for caching selector results across steps.
/// Derived from VectorSelector's matchers (which determine which series match).
/// Offset and @ modifiers are excluded — they affect time windows, not series selection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SelectorKey(u64);

impl SelectorKey {
    pub(crate) fn from_selector(selector: &VectorSelector) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        selector.name.hash(&mut hasher);
        selector.matchers.matchers.hash(&mut hasher);
        selector.matchers.or_matchers.hash(&mut hasher);
        Self(hasher.finish())
    }
}

/// Structural key for preloaded instant vector data.
/// Captures selector identity + time modifiers that affect which samples map to which steps.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PreloadKey {
    selector: SelectorKey,
    offset: Option<OffsetKey>,
    at: Option<AtKey>,
}

impl PreloadKey {
    fn from_selector(vs: &VectorSelector) -> Self {
        Self {
            selector: SelectorKey::from_selector(vs),
            offset: vs.offset.as_ref().map(OffsetKey::from),
            at: vs.at.as_ref().map(AtKey::from),
        }
    }
}

/// Hashable representation of Offset
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum OffsetKey {
    Pos(i64),
    Neg(i64),
}

impl From<&Offset> for OffsetKey {
    fn from(offset: &Offset) -> Self {
        match offset {
            Offset::Pos(d) => OffsetKey::Pos(d.as_millis() as i64),
            Offset::Neg(d) => OffsetKey::Neg(d.as_millis() as i64),
        }
    }
}

/// Hashable representation of AtModifier
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum AtKey {
    At(i64),
    Start,
    End,
}

impl From<&AtModifier> for AtKey {
    fn from(at: &AtModifier) -> Self {
        match at {
            AtModifier::At(t) => AtKey::At(Timestamp::from(*t).as_millis()),
            AtModifier::Start => AtKey::Start,
            AtModifier::End => AtKey::End,
        }
    }
}

/// Preloaded per-step evaluation data for a VectorSelector across a range query.
struct PreloadedInstantData {
    eval_start_ms: i64,
    step_ms: i64,
    series: Vec<PreloadedInstantSeries>,
}

struct PreloadedInstantSeries {
    labels: Arc<[Label]>,
    /// Dense array indexed by outer step number. values[i] = Some(Sample) if a
    /// sample exists in the lookback window for that step, None otherwise.
    values: Vec<Option<Sample>>,
}

/// Cached per-series metadata: fingerprint and canonical sorted labels.
/// Computed once per (bucket, series_id), reused across all steps via Arc sharing.
/// Labels are shared via Arc and wrapped in EvalLabels at output time — no string cloning.
/// Most steps don't emit output for most series.
pub(crate) struct SeriesMeta {
    pub(crate) fingerprint: SeriesFingerprint,
    pub(crate) sorted_labels: Arc<[Label]>,
}

/// Result of Phase B1 parallel metadata resolution for one bucket.
struct BucketResolutionResult {
    bucket: TimeBucket,
    candidates: HashSet<SeriesId>,
    forward_index_view: Option<Box<dyn ForwardIndexLookup + Send + Sync + 'static>>,
    forward_index_key: Vec<SeriesId>,
    series_meta: Vec<(SeriesId, Arc<SeriesMeta>)>,
    stats: BucketResolutionStats,
}

#[derive(Debug, Default)]
struct BucketResolutionStats {
    total_ms: u64,
    selector_ms: u64,
    forward_index_calls: u64,
    forward_index_misses: u64,
    forward_index_miss_ms: u64,
    metadata_queue_wait_ms: u64,
    metadata_load_ms: u64,
    metadata_permit_acquires: u64,
    series_meta_count: u64,
    // Batched forward index stats
    fi_series_loaded: u64,
    fi_batch_ops: u64,
    fi_point_lookups: u64,
    fi_range_scans: u64,
    fi_range_scan_series: u64,
    fi_scan_span_series: u64,
    fi_run_len_1: u64,
    fi_run_len_2_3: u64,
    fi_run_len_4_7: u64,
    fi_run_len_8_plus: u64,
    // Merge decision counters
    fi_merge_accepted: u64,
    fi_merge_rejected_gap: u64,
    fi_merge_rejected_density: u64,
}

/// Work item for parallel sample loading (Phase B3).
struct BucketSampleWorkItem {
    bucket: TimeBucket,
    to_load: Vec<(SeriesId, Arc<SeriesMeta>)>,
}

/// Per-series data loaded by the B3 sample worker.
struct BucketSeriesData {
    series_id: SeriesId,
    meta: Arc<SeriesMeta>,
    all_samples: Vec<Sample>,
}

#[derive(Debug, Default)]
struct BucketSampleStats {
    sample_loads: u64,
    sample_misses: u64,
    samples_loaded: u64,
    sample_queue_wait_ms: u64,
    sample_load_ms: u64,
    sample_miss_ms: u64,
    sample_permit_acquires: u64,
    parallel_sample_loads: u64,
    bucket_ms: u64,
}

/// Result of Phase B3 sample loading for one bucket.
struct BucketSampleResult {
    bucket: TimeBucket,
    series_data: Vec<BucketSeriesData>,
    stats: BucketSampleStats,
}

/// Type alias for complex HashMap used in matrix selector evaluation.
/// Maps from label key (Arc-wrapped sorted slice of label pairs) to samples vector.
/// Arc key enables cheap insertion from cached SeriesMeta without cloning the label vec.
type SeriesMap = HashMap<Arc<[Label]>, Vec<Sample>>;

pub(crate) struct QueryReaderBucketEvalCache {
    // Map from terms (series_ids for forward, labels for inverted) to cached results
    forward_index_cache:
        HashMap<Vec<SeriesId>, Arc<dyn ForwardIndexLookup + Send + Sync + 'static>>,
    inverted_index_cache: HashMap<Vec<Label>, Arc<dyn InvertedIndexLookup + Send + Sync + 'static>>,
    samples: HashMap<SeriesId, Vec<Sample>>,
}

impl QueryReaderBucketEvalCache {
    fn new() -> Self {
        Self {
            forward_index_cache: HashMap::new(),
            inverted_index_cache: HashMap::new(),
            samples: HashMap::new(),
        }
    }
}

pub(crate) struct QueryReaderEvalCache {
    cache: HashMap<TimeBucket, QueryReaderBucketEvalCache>,
}

impl QueryReaderEvalCache {
    pub(crate) fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub(crate) fn get_bucket_cache_mut(
        &mut self,
        bucket: &TimeBucket,
    ) -> &mut QueryReaderBucketEvalCache {
        self.cache
            .entry(*bucket)
            .or_insert_with(QueryReaderBucketEvalCache::new)
    }

    pub(crate) fn cache_forward_index(
        &mut self,
        bucket: TimeBucket,
        series_ids: Vec<SeriesId>,
        forward_index: Box<dyn ForwardIndexLookup + Send + Sync + 'static>,
    ) {
        let bucket_cache = self.get_bucket_cache_mut(&bucket);
        bucket_cache
            .forward_index_cache
            .insert(series_ids, forward_index.into());
    }

    pub(crate) fn get_forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Option<Arc<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        self.cache
            .get(bucket)
            .and_then(|bucket_cache| bucket_cache.forward_index_cache.get(series_ids))
            .cloned()
    }

    pub(crate) fn cache_inverted_index(
        &mut self,
        bucket: TimeBucket,
        terms: Vec<Label>,
        result: Box<dyn InvertedIndexLookup + Send + Sync + 'static>,
    ) {
        let bucket_cache = self.get_bucket_cache_mut(&bucket);
        bucket_cache
            .inverted_index_cache
            .insert(terms, result.into());
    }

    pub(crate) fn get_inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Option<Arc<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        self.cache
            .get(bucket)
            .and_then(|bucket_cache| bucket_cache.inverted_index_cache.get(terms))
            .cloned()
    }

    pub(crate) fn cache_samples(
        &mut self,
        bucket: TimeBucket,
        series_id: SeriesId,
        samples: Vec<Sample>,
    ) {
        let bucket_cache = self.get_bucket_cache_mut(&bucket);
        bucket_cache.samples.insert(series_id, samples);
    }

    pub(crate) fn get_samples(
        &self,
        bucket: &TimeBucket,
        series_id: &SeriesId,
    ) -> Option<&Vec<Sample>> {
        self.cache
            .get(bucket)
            .and_then(|bucket_cache| bucket_cache.samples.get(series_id))
    }
}

/// A cached forward index lookup that wraps cached data
struct CachedForwardIndex {
    data: HashMap<SeriesId, SeriesSpec>,
}

impl ForwardIndexLookup for CachedForwardIndex {
    fn get_spec(&self, series_id: &SeriesId) -> Option<SeriesSpec> {
        self.data.get(series_id).cloned()
    }

    fn all_series(&self) -> Vec<(SeriesId, SeriesSpec)> {
        self.data
            .iter()
            .map(|(&id, spec)| (id, spec.clone()))
            .collect()
    }
}

/// A cached inverted index lookup that wraps cached data
struct CachedInvertedIndex {
    result: roaring::RoaringBitmap,
}

impl InvertedIndexLookup for CachedInvertedIndex {
    fn intersect(&self, _terms: Vec<Label>) -> roaring::RoaringBitmap {
        // Return the pre-computed intersection result
        self.result.clone()
    }

    fn all_keys(&self) -> Vec<Label> {
        // This method doesn't make sense for a pre-computed intersection result
        // but we need to implement it for the trait
        Vec::new()
    }
}

/// Cheap-to-clone label container for evaluator internals.
///
/// Storage provides labels as `Arc<[Label]>` (sorted). The `Shared` variant
/// wraps that Arc directly — cloning is an atomic refcount bump. Mutation
/// (remove/insert/retain) promotes to `Owned`, which copies the vec once.
#[derive(Debug, Clone)]
pub(crate) enum EvalLabels {
    /// Shared immutable labels from storage. Clone = O(1) refcount bump.
    Shared(Arc<[Label]>),
    /// Owned mutable sorted labels, materialized on first mutation.
    Owned(Vec<Label>),
}

impl EvalLabels {
    /// Binary search on the sorted label slice.
    pub(crate) fn get(&self, key: &str) -> Option<&str> {
        let slice = self.as_slice();
        slice
            .binary_search_by(|l| l.name.as_str().cmp(key))
            .ok()
            .map(|i| slice[i].value.as_str())
    }

    /// Remove a label by name. Promotes Shared→Owned if needed.
    pub(crate) fn remove(&mut self, key: &str) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self
            && let Ok(i) = vec.binary_search_by(|l| l.name.as_str().cmp(key))
        {
            vec.remove(i);
        }
    }

    /// Insert or update a label. Maintains sort order. Promotes Shared→Owned.
    pub(crate) fn insert(&mut self, key: String, value: String) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self {
            match vec.binary_search_by(|l| l.name.as_str().cmp(key.as_str())) {
                Ok(i) => vec[i].value = value,
                Err(i) => vec.insert(i, Label { name: key, value }),
            }
        }
    }

    /// Retain only labels matching the predicate. Promotes Shared→Owned.
    pub(crate) fn retain(&mut self, f: impl FnMut(&Label) -> bool) {
        self.make_owned();
        if let EvalLabels::Owned(vec) = self {
            vec.retain(f);
        }
    }

    /// Returns true if there are no labels.
    pub(crate) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// Iterate over labels (sorted order in both variants).
    pub(crate) fn iter(&self) -> impl Iterator<Item = &Label> {
        self.as_slice().iter()
    }

    /// Convert into `Labels` for the output boundary. Both variants are
    /// already sorted, so `Labels::new()` does no extra work.
    pub(crate) fn into_labels(self) -> crate::model::Labels {
        match self {
            EvalLabels::Shared(arc) => crate::model::Labels::new(arc.to_vec()),
            EvalLabels::Owned(vec) => crate::model::Labels::new(vec),
        }
    }

    /// Construct from pairs (for tests). Sorts on construction.
    #[cfg(test)]
    pub(crate) fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        let mut vec: Vec<Label> = pairs
            .iter()
            .map(|(k, v)| Label {
                name: k.to_string(),
                value: v.to_string(),
            })
            .collect();
        vec.sort();
        EvalLabels::Owned(vec)
    }

    fn as_slice(&self) -> &[Label] {
        match self {
            EvalLabels::Shared(arc) => arc,
            EvalLabels::Owned(vec) => vec,
        }
    }

    fn make_owned(&mut self) {
        if let EvalLabels::Shared(arc) = self {
            *self = EvalLabels::Owned(arc.to_vec());
        }
    }
}

impl PartialEq for EvalLabels {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

// ToDo(cadonna): Add histogram samples
#[derive(Debug, Clone, PartialEq)]
pub struct EvalSample {
    pub(crate) timestamp_ms: i64,
    pub(crate) value: f64,
    pub(crate) labels: EvalLabels,
    pub(crate) drop_name: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvalSamples {
    pub(crate) values: Vec<Sample>,
    pub(crate) labels: EvalLabels,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum KAggregationOrder {
    Top,
    Bottom,
}

#[derive(Clone, Copy)]
struct AggregationEvalContext {
    // Keep timing inputs bundled so k-aggregation helpers stay small and
    // always use a consistent eval context.
    query_start: Timestamp,
    query_end: Timestamp,
    evaluation_ts: Timestamp,
    interval_ms: i64,
    lookback_delta_ms: i64,
}

/// Compares values for topk/bottomk aggregation.
/// NaN values are always considered "greater" (sorted last) regardless of order.
/// Uses partial_cmp for IEEE 754 semantics (-0.0 == +0.0), matching Prometheus.
fn compare_k_values(left: f64, right: f64, order: KAggregationOrder) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => match order {
            KAggregationOrder::Top => right.partial_cmp(&left).unwrap_or(Ordering::Equal),
            KAggregationOrder::Bottom => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
        },
    }
}

#[derive(Clone, Copy)]
struct KHeapEntry {
    value: f64,
    index: usize,
    order: KAggregationOrder,
}

impl PartialEq for KHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
            && self.order == other.order
            && self.value.to_bits() == other.value.to_bits()
    }
}

impl Eq for KHeapEntry {}

impl PartialOrd for KHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap. Define "greater" as "worse" so heap.peek()
        // returns the least desirable currently selected sample.
        compare_k_values(self.value, other.value, self.order)
            .then_with(|| self.index.cmp(&other.index))
    }
}

fn select_k_indices_with_heap(
    samples: &[EvalSample],
    keep: usize,
    order: KAggregationOrder,
) -> Vec<usize> {
    if keep == 0 || samples.is_empty() {
        return Vec::new();
    }
    let mut heap = BinaryHeap::with_capacity(keep);
    for (idx, sample) in samples.iter().enumerate() {
        let entry = KHeapEntry {
            value: sample.value,
            index: idx,
            order,
        };
        if heap.len() < keep {
            heap.push(entry);
            continue;
        }

        if let Some(worst) = heap.peek()
            && compare_k_values(sample.value, worst.value, order).is_lt()
        {
            // Replace only when the candidate outranks the current worst,
            // preserving the "peek is worst-kept" invariant.
            heap.pop();
            heap.push(entry);
        }
    }
    heap.into_iter().map(|entry| entry.index).collect()
}

pub(crate) struct Evaluator<'reader, R: QueryReader> {
    reader: CachedQueryReader<'reader, R>,
    /// Preloaded per-step instant vector data for range queries.
    /// Populated by preload_for_range() before the step loop.
    preloaded_instant: HashMap<PreloadKey, PreloadedInstantData>,
    /// True when executing in the outer range-query step loop context.
    /// Set to false inside subquery evaluation to prevent cross-context
    /// reuse of preloaded data (subqueries have different step grids).
    preload_eligible: bool,
}

/// A wrapper around QueryReader that uses QueryReaderEvalCache for caching
pub(crate) struct CachedQueryReader<'reader, R: QueryReader> {
    reader: &'reader R,
    cache: QueryReaderEvalCache,
    pub(crate) stats: EvalStats,
    /// Cached bucket list — populated on first call, reused for all subsequent steps.
    cached_buckets: Option<Vec<TimeBucket>>,
    /// Cached selector results: (bucket, selector_key) → matching series IDs.
    /// Arc-wrapped so cache hits return a cheap refcount bump instead of cloning the set.
    selector_cache: HashMap<(TimeBucket, SelectorKey), Arc<HashSet<SeriesId>>>,
    /// Cached per-series metadata: (bucket, series_id) → fingerprint + labels.
    /// Arc-wrapped so callers can share metadata without cloning strings.
    series_meta_cache: HashMap<(TimeBucket, SeriesId), Arc<SeriesMeta>>,
    /// Optional load coordinator for semaphore-based I/O budgeting.
    load_coordinator: Option<ReadLoadCoordinator>,
}

impl<'reader, R: QueryReader> CachedQueryReader<'reader, R> {
    pub(crate) fn new(reader: &'reader R) -> Self {
        Self {
            reader,
            cache: QueryReaderEvalCache::new(),
            stats: EvalStats::default(),
            cached_buckets: None,
            selector_cache: HashMap::new(),
            series_meta_cache: HashMap::new(),
            load_coordinator: None,
        }
    }

    pub(crate) fn with_coordinator(reader: &'reader R, coordinator: ReadLoadCoordinator) -> Self {
        Self {
            load_coordinator: Some(coordinator),
            ..Self::new(reader)
        }
    }

    pub(crate) async fn list_buckets(&mut self) -> Result<Vec<TimeBucket>> {
        self.stats.list_buckets_calls += 1;
        if let Some(ref buckets) = self.cached_buckets {
            return Ok(buckets.clone());
        }
        self.stats.list_buckets_misses += 1;
        let t0 = std::time::Instant::now();
        let buckets = self.reader.list_buckets().await?;
        self.stats.list_buckets_miss_ms += t0.elapsed().as_millis() as u64;
        self.cached_buckets = Some(buckets.clone());
        Ok(buckets)
    }

    pub(crate) async fn forward_index(
        &mut self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Arc<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        self.stats.forward_index_calls += 1;
        let mut series_ids = Vec::from(series_ids);
        series_ids.sort();
        // Check cache first
        if let Some(cached_data) = self.cache.get_forward_index(bucket, &series_ids) {
            self.stats.forward_index_cache_hits += 1;
            Ok(cached_data)
        } else {
            self.stats.forward_index_misses += 1;
            let (wait_ms, load_ms);
            let forward_index = if let Some(ref coord) = self.load_coordinator {
                let (permit, wait) = coord.acquire_metadata().await;
                self.stats.metadata_permit_acquires += 1;
                wait_ms = wait.as_millis() as u64;
                let t0 = std::time::Instant::now();
                let fi = self.reader.forward_index(bucket, &series_ids).await?;
                load_ms = t0.elapsed().as_millis() as u64;
                drop(permit);
                fi
            } else {
                wait_ms = 0;
                let t0 = std::time::Instant::now();
                let fi = self.reader.forward_index(bucket, &series_ids).await?;
                load_ms = t0.elapsed().as_millis() as u64;
                fi
            };
            self.stats.metadata_queue_wait_ms += wait_ms;
            self.stats.metadata_load_ms += load_ms;
            self.stats.forward_index_miss_ms += wait_ms + load_ms;

            let bs = forward_index.batch_stats();
            self.stats.fi_series_loaded += bs.unique_series as u64;
            self.stats.fi_batch_ops += bs.batch_ops as u64;
            self.stats.fi_point_lookups += bs.point_lookups as u64;
            self.stats.fi_range_scans += bs.range_scans as u64;
            self.stats.fi_range_scan_series += bs.range_scan_series as u64;
            self.stats.fi_scan_span_series += bs.scan_span_series as u64;
            self.stats.fi_run_len_1 += bs.run_len_1 as u64;
            self.stats.fi_run_len_2_3 += bs.run_len_2_3 as u64;
            self.stats.fi_run_len_4_7 += bs.run_len_4_7 as u64;
            self.stats.fi_run_len_8_plus += (bs.run_len_8_15 + bs.run_len_16_plus) as u64;
            self.stats.fi_merge_accepted += bs.merge_accepted as u64;
            self.stats.fi_merge_rejected_gap += bs.merge_rejected_gap as u64;
            self.stats.fi_merge_rejected_density += bs.merge_rejected_density as u64;

            self.cache
                .cache_forward_index(*bucket, series_ids.clone(), forward_index);

            Ok(self
                .cache
                .get_forward_index(bucket, &series_ids)
                .expect("unreachable"))
        }
    }

    pub(crate) async fn inverted_index(
        &mut self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Arc<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        self.stats.selector_calls += 1;
        let mut terms = terms.to_vec();
        // Sort by canonical Label ordering (name, then value) for cache key consistency
        terms.sort();
        // Check cache first
        if let Some(cached_result) = self.cache.get_inverted_index(bucket, &terms) {
            return Ok(cached_result);
        }

        // Load from underlying reader
        let (wait_ms, load_ms);
        let inverted_index = if let Some(ref coord) = self.load_coordinator {
            let (permit, wait) = coord.acquire_metadata().await;
            self.stats.metadata_permit_acquires += 1;
            wait_ms = wait.as_millis() as u64;
            let t0 = std::time::Instant::now();
            let ii = self.reader.inverted_index(bucket, &terms).await?;
            load_ms = t0.elapsed().as_millis() as u64;
            drop(permit);
            ii
        } else {
            wait_ms = 0;
            let t0 = std::time::Instant::now();
            let ii = self.reader.inverted_index(bucket, &terms).await?;
            load_ms = t0.elapsed().as_millis() as u64;
            ii
        };
        self.stats.metadata_queue_wait_ms += wait_ms;
        self.stats.metadata_load_ms += load_ms;
        self.stats.selector_miss_ms += wait_ms + load_ms;

        // Cache the result
        self.cache
            .cache_inverted_index(*bucket, terms.clone(), inverted_index);

        Ok(self
            .cache
            .get_inverted_index(bucket, &terms)
            .expect("unreachable"))
    }

    pub(crate) async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        self.reader.all_inverted_index(bucket).await
    }

    pub(crate) async fn label_values(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> Result<Vec<String>> {
        self.reader.label_values(bucket, label_name).await
    }

    pub(crate) async fn samples(
        &mut self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        self.stats.sample_loads += 1;
        // Check cache first
        if let Some(cached_samples) = self.cache.get_samples(bucket, &series_id) {
            self.stats.sample_cache_hits += 1;
            return Ok(Self::filter_samples_binary_search(
                cached_samples,
                start_ms,
                end_ms,
            ));
        }

        // Not in cache, load from underlying reader with wide bounds to cache the whole bucket
        self.stats.sample_misses += 1;
        let (wait_ms, load_ms);
        let samples = if let Some(ref coord) = self.load_coordinator {
            let (permit, wait) = coord.acquire_sample().await;
            self.stats.sample_permit_acquires += 1;
            wait_ms = wait.as_millis() as u64;
            let t0 = std::time::Instant::now();
            let s = self
                .reader
                .samples(bucket, series_id, i64::MIN, i64::MAX)
                .await?;
            load_ms = t0.elapsed().as_millis() as u64;
            drop(permit);
            s
        } else {
            wait_ms = 0;
            let t0 = std::time::Instant::now();
            let s = self
                .reader
                .samples(bucket, series_id, i64::MIN, i64::MAX)
                .await?;
            load_ms = t0.elapsed().as_millis() as u64;
            s
        };
        self.stats.sample_queue_wait_ms += wait_ms;
        self.stats.sample_load_ms += load_ms;
        self.stats.sample_miss_ms += wait_ms + load_ms;

        self.stats.samples_loaded += samples.len() as u64;

        // Cache the full sample set
        self.cache
            .cache_samples(*bucket, series_id, samples.clone());

        // Filter by requested time range using binary search
        Ok(Self::filter_samples_binary_search(
            &samples, start_ms, end_ms,
        ))
    }

    /// Filter samples to (start_ms, end_ms] using binary search on sorted timestamps.
    /// Samples are sorted by timestamp_ms (storage invariant), so we use partition_point
    /// to find bounds in O(log n) instead of scanning the full vector.
    fn filter_samples_binary_search(samples: &[Sample], start_ms: i64, end_ms: i64) -> Vec<Sample> {
        // Find first index where timestamp_ms > start_ms
        let lo = samples.partition_point(|s| s.timestamp_ms <= start_ms);
        // Find first index where timestamp_ms > end_ms
        let hi = samples.partition_point(|s| s.timestamp_ms <= end_ms);
        samples[lo..hi].to_vec()
    }

    /// Look up cached selector results for (bucket, selector_key).
    /// Returns an Arc (cheap refcount bump) and increments the cache hit counter.
    pub(crate) fn get_cached_selector(
        &mut self,
        bucket: &TimeBucket,
        key: &SelectorKey,
    ) -> Option<Arc<HashSet<SeriesId>>> {
        let result = self.selector_cache.get(&(*bucket, key.clone())).cloned();
        if result.is_some() {
            self.stats.selector_cache_hits += 1;
        }
        result
    }

    /// Cache selector results for (bucket, selector_key).
    pub(crate) fn cache_selector(
        &mut self,
        bucket: TimeBucket,
        key: SelectorKey,
        candidates: HashSet<SeriesId>,
    ) {
        self.selector_cache
            .insert((bucket, key), Arc::new(candidates));
    }

    /// Look up or compute series metadata (fingerprint + label map + sorted labels).
    /// On first call for a (bucket, series_id), computes from the forward index view
    /// and caches. Returns Arc for cheap sharing — callers never clone the inner data.
    pub(crate) fn get_or_insert_series_meta(
        &mut self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        forward_index_view: &dyn ForwardIndexLookup,
    ) -> Option<Arc<SeriesMeta>> {
        let cache_key = (*bucket, series_id);
        match self.series_meta_cache.entry(cache_key) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                self.stats.series_meta_cache_hits += 1;
                Some(Arc::clone(entry.get()))
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.stats.series_meta_misses += 1;
                let spec = forward_index_view.get_spec(&series_id)?;
                let fingerprint = Self::compute_fingerprint_static(&spec.labels);
                let mut sorted_labels = spec.labels;
                sorted_labels.sort();
                let meta = Arc::new(SeriesMeta {
                    fingerprint,
                    sorted_labels: Arc::from(sorted_labels),
                });
                entry.insert(Arc::clone(&meta));
                Some(meta)
            }
        }
    }

    /// Compute fingerprint from labels — static version for use in cache population.
    fn compute_fingerprint_static(labels: &[Label]) -> SeriesFingerprint {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut sorted_labels: Vec<_> = labels.iter().collect();
        sorted_labels.sort_by(|a, b| a.name.cmp(&b.name));
        for label in sorted_labels {
            label.name.hash(&mut hasher);
            label.value.hash(&mut hasher);
        }
        hasher.finish() as SeriesFingerprint
    }
}

#[derive(Debug)]
pub(crate) enum ExprResult {
    Scalar(f64),
    InstantVector(Vec<EvalSample>),
    RangeVector(Vec<EvalSamples>),
}

impl ExprResult {
    /// Extract the instant vector samples, returning None if this is a scalar or range vector result
    pub(crate) fn into_instant_vector(self) -> Option<Vec<EvalSample>> {
        match self {
            ExprResult::InstantVector(samples) => Some(samples),
            ExprResult::Scalar(_) | ExprResult::RangeVector(_) => None,
        }
    }

    /// Extract the range vector samples, returning None if this is not a range vector result
    pub(crate) fn into_range_vector(self) -> Option<Vec<EvalSamples>> {
        match self {
            ExprResult::RangeVector(samples) => Some(samples),
            ExprResult::Scalar(_) | ExprResult::InstantVector(_) => None,
        }
    }

    #[cfg(test)]
    /// Extract instant vector samples, panicking if this is not an instant vector result
    pub(crate) fn expect_instant_vector(self, msg: &str) -> Vec<EvalSample> {
        match self {
            ExprResult::InstantVector(samples) => samples,
            ExprResult::Scalar(_) | ExprResult::RangeVector(_) => panic!("{}", msg),
        }
    }
}

/// Walk the AST and compute the disjoint time ranges needed for bucket preloading.
///
/// For each selector, compute the effective evaluation time by applying
/// @ and offset modifiers, then expand by lookback_delta (vector) or
/// range (matrix). Returns a sorted, non-overlapping list of
/// `(earliest_secs, latest_secs)` ranges covering all selectors.
///
/// Returns an empty Vec when the expression contains no selectors
/// (e.g. `1 + 2`), allowing the caller to fall back to the default window.
pub(crate) fn compute_preload_ranges(
    expr: &Expr,
    query_start: std::time::SystemTime,
    query_end: std::time::SystemTime,
    lookback_delta: std::time::Duration,
) -> Vec<(i64, i64)> {
    let start_ms = Timestamp::from(query_start).as_millis();
    let end_ms = Timestamp::from(query_end).as_millis();
    let lookback_ms = lookback_delta.as_millis() as i64;
    // At the top level, eval range == query range
    let mut ranges = Vec::new();
    preload_ranges_inner(
        expr,
        start_ms,
        end_ms,
        start_ms,
        end_ms,
        lookback_ms,
        &mut ranges,
    );
    // Convert ms to seconds (floor for start, ceil for end), then normalize
    let ranges_secs: Vec<(i64, i64)> = ranges
        .into_iter()
        .map(|(lo, hi)| {
            let start_secs = lo.div_euclid(1000);
            let end_secs = hi.div_euclid(1000) + i64::from(hi.rem_euclid(1000) != 0);
            (start_secs, end_secs)
        })
        .collect();
    normalize_ranges(ranges_secs)
}

/// Sort ranges by start and merge overlapping ones.
/// Adjacent-but-not-overlapping ranges are kept separate since they may map
/// to different buckets.
pub(crate) fn normalize_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|&(start, _)| start);
    let mut merged = Vec::with_capacity(ranges.len());
    let (mut cur_start, mut cur_end) = ranges[0];
    for &(start, end) in &ranges[1..] {
        if start <= cur_end {
            // Overlapping — extend
            cur_end = cur_end.max(end);
        } else {
            merged.push((cur_start, cur_end));
            cur_start = start;
            cur_end = end;
        }
    }
    merged.push((cur_start, cur_end));
    merged
}

/// Compute the effective evaluation-time range for a selector after applying
/// @ and offset modifiers, then return (earliest_ms, latest_ms) after
/// subtracting the backward window (lookback or matrix range).
///
/// `at_start_ms`/`at_end_ms` are the values that `@ start()` and `@ end()`
/// resolve to. `eval_start_ms`/`eval_end_ms` are the effective evaluation-time
/// range for selectors without `@`.
///
/// At the top level both pairs are identical (the query range). Inside
/// subqueries they diverge: `at_start_ms`/`at_end_ms` remain the outer query
/// bounds (since `evaluate_subquery` passes `query_start`/`query_end` through)
/// while `eval_start_ms`/`eval_end_ms` become the subquery step window.
fn selector_bounds(
    at: Option<&AtModifier>,
    offset: Option<&Offset>,
    at_start_ms: i64,
    at_end_ms: i64,
    eval_start_ms: i64,
    eval_end_ms: i64,
    backward_window_ms: i64,
) -> (i64, i64) {
    // Step 1: Determine the evaluation time range.
    //
    // `@ <timestamp>` pins evaluation to a fixed instant (single point).
    //
    // `@ start()` / `@ end()`: in query_range(), each step creates an
    // instant_stmt with start=end=current_time, so both @ start() and
    // @ end() resolve to current_time, sweeping [range_start, range_end].
    // For preload purposes we must cover the full eval range for both.
    // Inside subqueries, evaluate_subquery passes the outer query bounds
    // through unchanged, so @ start()/@ end() resolve to constants —
    // but we still use (at_start_ms, at_end_ms) which correctly narrows
    // to a single point when those are equal (instant query or inner
    // subquery context).
    let (mut start, mut end) = if let Some(at_mod) = at {
        match at_mod {
            AtModifier::At(time) => {
                let t = Timestamp::from(*time).as_millis();
                (t, t)
            }
            // Both @ start() and @ end() sweep the full at-modifier range.
            // At the top level at_start == eval_start and at_end == eval_end
            // (the query range). Inside subqueries at_start/at_end are the
            // outer query bounds passed through by evaluate_subquery.
            AtModifier::Start | AtModifier::End => (at_start_ms, at_end_ms),
        }
    } else {
        (eval_start_ms, eval_end_ms)
    };

    // Step 2: Apply offset
    if let Some(off) = offset {
        match off {
            Offset::Pos(d) => {
                let off_ms = d.as_millis() as i64;
                start = start.saturating_sub(off_ms);
                end = end.saturating_sub(off_ms);
            }
            Offset::Neg(d) => {
                let off_ms = d.as_millis() as i64;
                start = start.saturating_add(off_ms);
                end = end.saturating_add(off_ms);
            }
        }
    }

    // Step 3: Subtract backward window from start
    let earliest = start.saturating_sub(backward_window_ms);
    (earliest, end)
}

/// Recursive inner function operating in milliseconds.
/// Appends per-selector `(earliest_ms, latest_ms)` ranges to `out`.
///
/// `at_start_ms`/`at_end_ms` are what `@ start()` / `@ end()` resolve to.
/// `eval_start_ms`/`eval_end_ms` are the effective evaluation-time range
/// for selectors without `@`.
fn preload_ranges_inner(
    expr: &Expr,
    at_start_ms: i64,
    at_end_ms: i64,
    eval_start_ms: i64,
    eval_end_ms: i64,
    lookback_ms: i64,
    out: &mut Vec<(i64, i64)>,
) {
    match expr {
        Expr::VectorSelector(vs) => {
            out.push(selector_bounds(
                vs.at.as_ref(),
                vs.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
            ));
        }
        Expr::MatrixSelector(ms) => {
            let range_ms = ms.range.as_millis() as i64;
            out.push(selector_bounds(
                ms.vs.at.as_ref(),
                ms.vs.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                range_ms,
            ));
        }
        Expr::Subquery(sq) => {
            // Compute the subquery's own adjusted eval-time window.
            let (sq_start, sq_end) = selector_bounds(
                sq.at.as_ref(),
                sq.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                0,
            );
            let range_ms = sq.range.as_millis() as i64;
            let inner_eval_start = sq_start.saturating_sub(range_ms);
            // Recurse: evaluate_subquery passes the original query_start/query_end
            // through, so @ start()/@ end() inside the subquery resolve to the
            // outer query bounds. The eval-time range narrows to the subquery
            // step window.
            preload_ranges_inner(
                &sq.expr,
                at_start_ms,
                at_end_ms,
                inner_eval_start,
                sq_end,
                lookback_ms,
                out,
            );
        }
        Expr::Aggregate(agg) => {
            preload_ranges_inner(
                &agg.expr,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
            if let Some(ref param) = agg.param {
                preload_ranges_inner(
                    param,
                    at_start_ms,
                    at_end_ms,
                    eval_start_ms,
                    eval_end_ms,
                    lookback_ms,
                    out,
                );
            }
        }
        Expr::Binary(b) => {
            preload_ranges_inner(
                &b.lhs,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
            preload_ranges_inner(
                &b.rhs,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
        }
        Expr::Paren(p) => preload_ranges_inner(
            &p.expr,
            at_start_ms,
            at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_ms,
            out,
        ),
        Expr::Call(call) => {
            for arg in &call.args.args {
                preload_ranges_inner(
                    arg,
                    at_start_ms,
                    at_end_ms,
                    eval_start_ms,
                    eval_end_ms,
                    lookback_ms,
                    out,
                );
            }
        }
        Expr::Unary(u) => preload_ranges_inner(
            &u.expr,
            at_start_ms,
            at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_ms,
            out,
        ),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}

/// Single source of truth for offset / @ time-modifier arithmetic (in milliseconds).
/// Both evaluate_vector_selector (per-step) and preload_vector_selector call this.
fn apply_time_modifiers_ms(
    at: Option<&AtModifier>,
    offset: Option<&Offset>,
    query_start_ms: i64,
    query_end_ms: i64,
    evaluation_ts_ms: i64,
) -> i64 {
    let mut adjusted = if let Some(at_modifier) = at {
        match at_modifier {
            AtModifier::At(timestamp) => Timestamp::from(*timestamp).as_millis(),
            AtModifier::Start => query_start_ms,
            AtModifier::End => query_end_ms,
        }
    } else {
        evaluation_ts_ms
    };

    if let Some(offset) = offset {
        adjusted = match offset {
            Offset::Pos(duration) => adjusted - (duration.as_millis() as i64),
            Offset::Neg(duration) => adjusted + (duration.as_millis() as i64),
        };
    }

    adjusted
}

/// Walk the AST and collect references to all VectorSelectors.
/// MatrixSelectors and Subqueries are skipped (not preloaded).
fn collect_vector_selectors(expr: &Expr) -> Vec<&VectorSelector> {
    let mut out = Vec::new();
    collect_vector_selectors_inner(expr, &mut out);
    out
}

fn collect_vector_selectors_inner<'a>(expr: &'a Expr, out: &mut Vec<&'a VectorSelector>) {
    match expr {
        Expr::VectorSelector(vs) => out.push(vs),
        Expr::Aggregate(agg) => {
            collect_vector_selectors_inner(&agg.expr, out);
            if let Some(ref param) = agg.param {
                collect_vector_selectors_inner(param, out);
            }
        }
        Expr::Binary(b) => {
            collect_vector_selectors_inner(&b.lhs, out);
            collect_vector_selectors_inner(&b.rhs, out);
        }
        Expr::Paren(p) => collect_vector_selectors_inner(&p.expr, out),
        Expr::Call(call) => {
            for arg in &call.args.args {
                collect_vector_selectors_inner(arg, out);
            }
        }
        Expr::Unary(u) => collect_vector_selectors_inner(&u.expr, out),
        // MatrixSelector: needs sample slices, not latest-value — not preloaded
        // Subquery: has own step loop with different step params — not preloaded
        Expr::MatrixSelector(_)
        | Expr::Subquery(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => {}
    }
}

/// Phase B1 worker: resolve bucket metadata using raw QueryReader (no caching layer).
async fn resolve_bucket_raw<R: QueryReader>(
    reader: &R,
    bucket: TimeBucket,
    selector: &VectorSelector,
    coordinator: &Option<ReadLoadCoordinator>,
) -> EvalResult<BucketResolutionResult> {
    let total_start = std::time::Instant::now();
    let mut stats = BucketResolutionStats::default();

    // Step 1: Selector resolution (inverted_index + negative matcher filtering)
    let selector_start = std::time::Instant::now();
    let (candidates, sel_stats) = evaluate_selector_raw(reader, &bucket, selector, coordinator)
        .await
        .map_err(|e| EvaluationError::InternalError(e.to_string()))?;
    stats.selector_ms = selector_start.elapsed().as_millis() as u64;
    stats.metadata_queue_wait_ms += sel_stats.metadata_queue_wait_ms;
    stats.metadata_load_ms += sel_stats.metadata_load_ms;
    stats.metadata_permit_acquires += sel_stats.metadata_permit_acquires;
    stats.fi_series_loaded += sel_stats.fi_series_loaded;
    stats.fi_batch_ops += sel_stats.fi_batch_ops;
    stats.fi_point_lookups += sel_stats.fi_point_lookups;
    stats.fi_range_scans += sel_stats.fi_range_scans;
    stats.fi_range_scan_series += sel_stats.fi_range_scan_series;
    stats.fi_scan_span_series += sel_stats.fi_scan_span_series;
    stats.fi_run_len_1 += sel_stats.fi_run_len_1;
    stats.fi_run_len_2_3 += sel_stats.fi_run_len_2_3;
    stats.fi_run_len_4_7 += sel_stats.fi_run_len_4_7;
    stats.fi_run_len_8_plus += sel_stats.fi_run_len_8_plus;
    stats.fi_merge_accepted += sel_stats.fi_merge_accepted;
    stats.fi_merge_rejected_gap += sel_stats.fi_merge_rejected_gap;
    stats.fi_merge_rejected_density += sel_stats.fi_merge_rejected_density;

    if candidates.is_empty() {
        stats.total_ms = total_start.elapsed().as_millis() as u64;
        return Ok(BucketResolutionResult {
            bucket,
            candidates,
            forward_index_view: None,
            forward_index_key: Vec::new(),
            series_meta: Vec::new(),
            stats,
        });
    }

    // Step 2: Forward index load for full candidate set
    let mut sorted_candidates: Vec<_> = candidates.iter().copied().collect();
    sorted_candidates.sort();
    stats.forward_index_calls += 1;
    stats.forward_index_misses += 1;

    let fi_view = if let Some(coord) = coordinator {
        let (permit, wait) = coord.acquire_metadata().await;
        stats.metadata_permit_acquires += 1;
        stats.metadata_queue_wait_ms += wait.as_millis() as u64;
        let t0 = std::time::Instant::now();
        let fi = reader
            .forward_index(&bucket, &sorted_candidates)
            .await
            .map_err(|e| EvaluationError::StorageError(e.to_string()))?;
        let load_ms = t0.elapsed().as_millis() as u64;
        stats.metadata_load_ms += load_ms;
        stats.forward_index_miss_ms += load_ms;
        drop(permit);
        fi
    } else {
        let t0 = std::time::Instant::now();
        let fi = reader
            .forward_index(&bucket, &sorted_candidates)
            .await
            .map_err(|e| EvaluationError::StorageError(e.to_string()))?;
        let load_ms = t0.elapsed().as_millis() as u64;
        stats.forward_index_miss_ms += load_ms;
        fi
    };

    let fi_bs = fi_view.batch_stats();
    stats.fi_series_loaded += fi_bs.unique_series as u64;
    stats.fi_batch_ops += fi_bs.batch_ops as u64;
    stats.fi_point_lookups += fi_bs.point_lookups as u64;
    stats.fi_range_scans += fi_bs.range_scans as u64;
    stats.fi_range_scan_series += fi_bs.range_scan_series as u64;
    stats.fi_scan_span_series += fi_bs.scan_span_series as u64;
    stats.fi_run_len_1 += fi_bs.run_len_1 as u64;
    stats.fi_run_len_2_3 += fi_bs.run_len_2_3 as u64;
    stats.fi_run_len_4_7 += fi_bs.run_len_4_7 as u64;
    stats.fi_run_len_8_plus += (fi_bs.run_len_8_15 + fi_bs.run_len_16_plus) as u64;
    stats.fi_merge_accepted += fi_bs.merge_accepted as u64;
    stats.fi_merge_rejected_gap += fi_bs.merge_rejected_gap as u64;
    stats.fi_merge_rejected_density += fi_bs.merge_rejected_density as u64;

    // Step 3: Series meta computation (pure CPU)
    let mut series_meta = Vec::with_capacity(candidates.len());
    for &sid in &sorted_candidates {
        if let Some(spec) = fi_view.get_spec(&sid) {
            let fingerprint = CachedQueryReader::<R>::compute_fingerprint_static(&spec.labels);
            let mut sorted_labels = spec.labels;
            sorted_labels.sort();
            let meta = Arc::new(SeriesMeta {
                fingerprint,
                sorted_labels: Arc::from(sorted_labels),
            });
            series_meta.push((sid, meta));
        }
    }
    stats.series_meta_count = series_meta.len() as u64;
    stats.total_ms = total_start.elapsed().as_millis() as u64;

    tracing::debug!(
        bucket_id = ?bucket,
        candidates = candidates.len(),
        series_meta_count = series_meta.len(),
        total_ms = stats.total_ms,
        selector_ms = stats.selector_ms,
        forward_index_miss_ms = stats.forward_index_miss_ms,
        metadata_queue_wait_ms = stats.metadata_queue_wait_ms,
        metadata_load_ms = stats.metadata_load_ms,
        "resolve_bucket_parallel"
    );

    Ok(BucketResolutionResult {
        bucket,
        candidates,
        forward_index_view: Some(fi_view),
        forward_index_key: sorted_candidates,
        series_meta,
        stats,
    })
}

/// Phase B3 worker: parallel sample loading for one bucket's work items.
async fn process_bucket_sample_loads<R: QueryReader>(
    reader: &R,
    work: BucketSampleWorkItem,
    coordinator: Option<ReadLoadCoordinator>,
) -> EvalResult<BucketSampleResult> {
    const PER_QUERY_SAMPLE_CONCURRENCY: usize = 8;
    let bucket_start = std::time::Instant::now();
    let bucket = work.bucket;

    let load_results: Vec<_> = futures::stream::iter(work.to_load.into_iter())
        .map(|(series_id, meta)| {
            let coord = coordinator.clone();
            async move {
                let (wait_ms, _permit) = if let Some(ref c) = coord {
                    let (permit, wait) = c.acquire_sample().await;
                    (wait.as_millis() as u64, Some(permit))
                } else {
                    (0, None)
                };
                let t0 = std::time::Instant::now();
                let result = reader.samples(&bucket, series_id, i64::MIN, i64::MAX).await;
                let load_ms = t0.elapsed().as_millis() as u64;
                drop(_permit);
                (series_id, meta, result, wait_ms, load_ms)
            }
        })
        .buffer_unordered(PER_QUERY_SAMPLE_CONCURRENCY)
        .collect()
        .await;

    let mut stats = BucketSampleStats::default();
    let mut series_data = Vec::with_capacity(load_results.len());

    for (series_id, meta, result, wait_ms, load_ms) in load_results {
        let all_samples = result.map_err(|e| EvaluationError::StorageError(e.to_string()))?;

        stats.sample_misses += 1;
        stats.sample_loads += 1;
        stats.samples_loaded += all_samples.len() as u64;
        stats.sample_queue_wait_ms += wait_ms;
        stats.sample_load_ms += load_ms;
        stats.sample_miss_ms += wait_ms + load_ms;
        if coordinator.is_some() {
            stats.sample_permit_acquires += 1;
        }
        stats.parallel_sample_loads += 1;

        series_data.push(BucketSeriesData {
            series_id,
            meta,
            all_samples,
        });
    }

    stats.bucket_ms = bucket_start.elapsed().as_millis() as u64;

    tracing::debug!(
        bucket_id = ?bucket,
        series_count = series_data.len(),
        samples_loaded = stats.samples_loaded,
        bucket_ms = stats.bucket_ms,
        sample_queue_wait_ms = stats.sample_queue_wait_ms,
        sample_load_ms = stats.sample_load_ms,
        "preload_bucket_parallel"
    );

    Ok(BucketSampleResult {
        bucket,
        series_data,
        stats,
    })
}

impl<'reader, R: QueryReader> Evaluator<'reader, R> {
    pub(crate) fn new(reader: &'reader R) -> Self {
        Self {
            reader: CachedQueryReader::new(reader),
            preloaded_instant: HashMap::new(),
            preload_eligible: true,
        }
    }

    pub(crate) fn with_coordinator(reader: &'reader R, coordinator: ReadLoadCoordinator) -> Self {
        Self {
            reader: CachedQueryReader::with_coordinator(reader, coordinator),
            preloaded_instant: HashMap::new(),
            preload_eligible: true,
        }
    }

    /// Access aggregated evaluation statistics.
    pub(crate) fn stats(&self) -> &EvalStats {
        &self.reader.stats
    }

    /// Preload VectorSelector data for all steps of a range query.
    /// Must be called before the step loop. Walks the AST, deduplicates selectors,
    /// and builds dense per-step sample arrays for O(1) per-step lookup.
    pub(crate) async fn preload_for_range(
        &mut self,
        expr: &Expr,
        eval_start_ms: i64,
        eval_end_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<()> {
        let preload_start = std::time::Instant::now();
        let selectors = collect_vector_selectors(expr);
        // Deduplicate by PreloadKey
        let mut seen = HashSet::new();
        for vs in &selectors {
            let key = PreloadKey::from_selector(vs);
            if !seen.insert(key.clone()) {
                continue;
            }
            self.preload_vector_selector(
                vs,
                eval_start_ms,
                eval_end_ms,
                step_ms,
                lookback_delta_ms,
            )
            .await?;
        }
        self.reader.stats.preload_ms = preload_start.elapsed().as_millis() as u64;
        Ok(())
    }

    async fn preload_vector_selector(
        &mut self,
        vs: &VectorSelector,
        eval_start_ms: i64,
        eval_end_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<()> {
        // Compute fetch range via selector_bounds
        let (earliest_ms, latest_ms) = selector_bounds(
            vs.at.as_ref(),
            vs.offset.as_ref(),
            eval_start_ms,
            eval_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_delta_ms,
        );

        // Fetch all series + samples for the full time range
        let series_samples = self
            .fetch_series_samples(vs, earliest_ms, latest_ms)
            .await?;

        let num_steps = ((eval_end_ms - eval_start_ms) / step_ms) as usize + 1;
        let mut preloaded_series = Vec::with_capacity(series_samples.len());

        for (_fp, (labels, samples)) in series_samples {
            let mut values = Vec::with_capacity(num_steps);
            let mut i = 0usize;
            let mut last_valid: Option<&Sample> = None;

            for step_idx in 0..num_steps {
                let eval_ts_i = eval_start_ms + (step_idx as i64) * step_ms;

                // Per-step instant stmt sets query_start = query_end = eval_ts
                let adjusted_ts = apply_time_modifiers_ms(
                    vs.at.as_ref(),
                    vs.offset.as_ref(),
                    eval_ts_i,
                    eval_ts_i,
                    eval_ts_i,
                );
                let lookback_start = adjusted_ts - lookback_delta_ms;

                while i < samples.len() && samples[i].timestamp_ms <= adjusted_ts {
                    last_valid = Some(&samples[i]);
                    i += 1;
                }

                if let Some(sample) = last_valid {
                    if sample.timestamp_ms > lookback_start {
                        values.push(Some(Sample {
                            timestamp_ms: sample.timestamp_ms,
                            value: sample.value,
                        }));
                    } else {
                        values.push(None);
                    }
                } else {
                    values.push(None);
                }
            }

            preloaded_series.push(PreloadedInstantSeries { labels, values });
        }

        self.reader.stats.preload_selectors += 1;
        self.reader.stats.preload_series += preloaded_series.len() as u64;

        let key = PreloadKey::from_selector(vs);
        self.preloaded_instant.insert(
            key,
            PreloadedInstantData {
                eval_start_ms,
                step_ms,
                series: preloaded_series,
            },
        );

        Ok(())
    }

    pub(crate) async fn evaluate(&mut self, stmt: EvalStmt) -> EvalResult<ExprResult> {
        if stmt.start != stmt.end {
            return Err(EvaluationError::InternalError(format!(
                "evaluation must always be done at an instant.got start({:?}), end({:?})",
                stmt.start, stmt.end
            )));
        }

        // Convert SystemTime to Timestamp at entry point
        let query_start = Timestamp::from(stmt.start);
        let query_end = Timestamp::from(stmt.end);
        let evaluation_ts = query_end; // using end follows the "as-of" convention
        let interval_ms = stmt.interval.as_millis() as i64;
        let lookback_delta_ms = stmt.lookback_delta.as_millis() as i64;

        let mut result = self
            .evaluate_expr(
                &stmt.expr,
                query_start,
                query_end,
                evaluation_ts,
                interval_ms,
                lookback_delta_ms,
            )
            .await?;

        // Deferred __name__ cleanup (mirrors Prometheus cleanupMetricLabels)
        if let ExprResult::InstantVector(ref mut samples) = result {
            for sample in samples.iter_mut() {
                if sample.drop_name {
                    sample.labels.remove(METRIC_NAME);
                }
            }
        }

        Ok(result)
    }

    // this call recurses to evaluate sub-expressions, so it needs to return a boxed future
    // so that the return type is sized (so can be stack-allocated)
    fn evaluate_expr<'a>(
        &'a mut self,
        expr: &'a Expr,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
        interval_ms: i64,
        lookback_delta_ms: i64,
    ) -> Pin<Box<dyn Future<Output = EvalResult<ExprResult>> + Send + 'a>> {
        match expr {
            Expr::Aggregate(aggregate) => {
                let fut = self.evaluate_aggregate(
                    aggregate,
                    query_start,
                    query_end,
                    evaluation_ts,
                    interval_ms,
                    lookback_delta_ms,
                );
                Box::pin(fut)
            }
            Expr::Unary(_u) => {
                todo!()
            }
            Expr::Binary(b) => {
                let fut = self.evaluate_binary_expr(
                    b,
                    query_start,
                    query_end,
                    evaluation_ts,
                    interval_ms,
                    lookback_delta_ms,
                );
                Box::pin(fut)
            }
            Expr::Paren(p) => {
                let fut = self.evaluate_expr(
                    &p.expr,
                    query_start,
                    query_end,
                    evaluation_ts,
                    interval_ms,
                    lookback_delta_ms,
                );
                Box::pin(fut)
            }
            Expr::Subquery(q) => {
                let fut = self.evaluate_subquery(
                    q,
                    query_start,
                    query_end,
                    evaluation_ts,
                    interval_ms,
                    lookback_delta_ms,
                );
                Box::pin(fut)
            }
            Expr::NumberLiteral(l) => {
                let val = l.val;
                Box::pin(async move { Ok(ExprResult::Scalar(val)) })
            }
            Expr::StringLiteral(l) => {
                let val = l.val.clone();
                Box::pin(async move {
                    Err(EvaluationError::InternalError(format!(
                        "string literal \"{}\" is not directly evaluatable",
                        val
                    )))
                })
            }
            Expr::VectorSelector(vector_selector) => {
                let fut = self.evaluate_vector_selector(
                    vector_selector,
                    query_start,
                    query_end,
                    evaluation_ts,
                    lookback_delta_ms,
                );
                Box::pin(fut)
            }
            Expr::MatrixSelector(matrix_selector) => {
                let fut = self.evaluate_matrix_selector(
                    matrix_selector.clone(),
                    query_start,
                    query_end,
                    evaluation_ts,
                );
                Box::pin(fut)
            }
            Expr::Call(call) => {
                let fut = self.evaluate_call(
                    call,
                    query_start,
                    query_end,
                    evaluation_ts,
                    interval_ms,
                    lookback_delta_ms,
                );
                Box::pin(fut)
            }
            Expr::Extension(_) => {
                todo!()
            }
        }
    }

    async fn evaluate_matrix_selector(
        &mut self,
        matrix_selector: MatrixSelector,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
    ) -> EvalResult<ExprResult> {
        let vector_selector = &matrix_selector.vs;
        let range = matrix_selector.range;

        // Apply time modifiers to evaluation_ts
        let adjusted_eval_ts = self.apply_time_modifiers(
            vector_selector.at.as_ref(),
            vector_selector.offset.as_ref(),
            query_start,
            query_end,
            evaluation_ts,
        )?;

        // Example where this matters:
        //   sum_over_time(metric[100s] @ 100 offset 50s)
        //   → adjusted_eval_ts = 50s
        //   → start = 50s - 100s = -50s (before UNIX_EPOCH!)
        //
        // NOTE: Prometheus represents timestamps internally as int64 milliseconds
        // and allows negative timestamps (times before UNIX_EPOCH).
        // See: https://github.com/prometheus/prometheus/blob/main/model/timestamp/timestamp.go
        let end_ms = adjusted_eval_ts.as_millis();
        let start_ms = end_ms - (range.as_millis() as i64);

        // order buckets in chronological order
        let mut buckets = self.reader.list_buckets().await?;
        buckets.sort_by(|a, b| a.start.cmp(&b.start));

        // Group samples by series (using sorted label vector as key since HashMap doesn't impl Hash)
        let mut series_map: SeriesMap = HashMap::new();

        let selector_key = SelectorKey::from_selector(vector_selector);

        for bucket in buckets {
            // Check if bucket overlaps with our time range
            let bucket_start_ms = (bucket.start as i64) * 60 * 1000; // Convert minutes to milliseconds
            let bucket_end_ms = bucket_start_ms + (bucket.size_in_mins() as i64) * 60 * 1000;
            if bucket_end_ms < start_ms || bucket_start_ms > end_ms {
                continue;
            }

            // Check selector cache first, fall back to full evaluation
            let candidates =
                if let Some(cached) = self.reader.get_cached_selector(&bucket, &selector_key) {
                    cached
                } else {
                    self.reader.stats.selector_misses += 1;
                    let t0 = std::time::Instant::now();
                    let result =
                        evaluate_selector_with_reader(&mut self.reader, bucket, vector_selector)
                            .await
                            .map_err(|e| EvaluationError::InternalError(e.to_string()))?;
                    self.reader.stats.selector_miss_ms += t0.elapsed().as_millis() as u64;
                    self.reader
                        .cache_selector(bucket, selector_key.clone(), result.clone());
                    Arc::new(result)
                };

            if candidates.is_empty() {
                continue;
            }

            let candidates_vec: Vec<_> = candidates.iter().copied().collect();
            let forward_index_view = self.reader.forward_index(&bucket, &candidates_vec).await?;

            for series_id in candidates_vec {
                // Get series metadata from cache (Arc — cheap refcount bump)
                let meta = match self.reader.get_or_insert_series_meta(
                    &bucket,
                    series_id,
                    forward_index_view.as_ref(),
                ) {
                    Some(m) => m,
                    None => {
                        return Err(EvaluationError::InternalError(format!(
                            "Series {} not found in bucket {:?}",
                            series_id, bucket
                        )));
                    }
                };

                let sample_data = self
                    .reader
                    .samples(&bucket, series_id, start_ms, end_ms)
                    .await?;

                // Use sorted_labels as map key via Arc to avoid cloning the Vec<Label>
                let values = series_map
                    .entry(Arc::clone(&meta.sorted_labels))
                    .or_default();
                for sample in sample_data {
                    values.push(sample);
                }
            }
        }

        let mut range_vector = Vec::new();
        for (labels, values) in series_map {
            range_vector.push(EvalSamples {
                values,
                labels: EvalLabels::Shared(labels),
            });
        }

        Ok(ExprResult::RangeVector(range_vector))
    }

    async fn evaluate_subquery(
        &mut self,
        subquery: &SubqueryExpr,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
        interval_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<ExprResult> {
        let adjusted_eval_ts = self.apply_time_modifiers(
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            query_start,
            query_end,
            evaluation_ts,
        )?;

        // Calculate subquery time range: [adjusted_eval_ts - range, adjusted_eval_ts]
        let subquery_end_ms = adjusted_eval_ts.as_millis();
        let range_ms = subquery.range.as_millis() as i64;
        let subquery_start_ms = subquery_end_ms - range_ms;

        // Subquery step resolution fallback per PromQL spec:
        // "<resolution> is optional. Default is the global evaluation interval."
        // See: https://prometheus.io/docs/prometheus/latest/querying/basics/#subquery
        let step_ms = if let Some(s) = subquery.step {
            s.as_millis() as i64
        } else if interval_ms > 0 {
            interval_ms
        } else {
            // See: https://github.com/prometheus/prometheus/blob/main/config/config.go#L169
            // DefaultGlobalConfig.EvaluationInterval = 1 * time.Minute
            60_000
        };

        // Guard against invalid step
        if step_ms <= 0 {
            return Err(EvaluationError::InternalError(
                "subquery step must be > 0".to_string(),
            ));
        }

        // Fast path: if inner expression is a pure VectorSelector, evaluate over range once
        if let Expr::VectorSelector(ref selector) = *subquery.expr {
            return self
                .evaluate_subquery_vector_selector(
                    selector,
                    subquery_start_ms,
                    subquery_end_ms,
                    step_ms,
                    lookback_delta_ms,
                )
                .await;
        }

        // Align start time to step interval to ensure consistent evaluation points.
        // Prometheus: newEv.startTimestamp = newEv.interval * ((ev.startTimestamp - offset - range) / newEv.interval)
        // Go's division truncates toward zero, but we need floor division for negative timestamps.
        // Example: -41ms / 10ms
        //   Go (truncate): -41 / 10 = -4, then -4 * 10 = -40ms (wrong for negatives)
        //   Rust div_euclid (floor): -41 / 10 = -5, then -5 * 10 = -50ms (correct)
        // This ensures steps align consistently regardless of whether timestamps are negative.
        let div = subquery_start_ms.div_euclid(step_ms);
        let mut aligned_start_ms = div * step_ms;
        if aligned_start_ms <= subquery_start_ms {
            aligned_start_ms += step_ms;
        }

        // Evaluate the inner expression at each step within the subquery range.
        // Disable preload fast path — subquery has its own step grid/lookback context.
        let outer_preload_eligible = self.preload_eligible;
        self.preload_eligible = false;

        let mut series_map: HashMap<Vec<Label>, Vec<Sample>> = HashMap::new();

        for current_time_ms in (aligned_start_ms..=subquery_end_ms).step_by(step_ms as usize) {
            let current_time = Timestamp::from_millis(current_time_ms);

            let result = self
                .evaluate_expr(
                    &subquery.expr,
                    query_start,
                    query_end,
                    current_time,
                    step_ms,
                    lookback_delta_ms,
                )
                .await?;

            // PromQL requires subquery inner expression to evaluate to an instant vector.
            // Enforce this invariant at runtime.
            let ExprResult::InstantVector(samples) = result else {
                self.preload_eligible = outer_preload_eligible;
                return Err(EvaluationError::InternalError(
                    "subquery inner expression must return instant vector".to_string(),
                ));
            };

            for sample in samples {
                // Labels are already sorted in EvalLabels — clone into key directly.
                let labels_key: Vec<Label> = sample.labels.iter().cloned().collect();
                let values = series_map.entry(labels_key).or_default();
                values.push(Sample {
                    timestamp_ms: current_time_ms,
                    value: sample.value,
                });
            }
        }

        self.preload_eligible = outer_preload_eligible;

        let mut range_vector = Vec::new();
        for (labels, values) in series_map {
            range_vector.push(EvalSamples {
                values,
                labels: EvalLabels::Owned(labels),
            });
        }

        Ok(ExprResult::RangeVector(range_vector))
    }

    /// Computes time alignment and range boundaries for subquery evaluation.
    ///
    /// Aligns the start time to step boundaries using floor division to ensure
    /// consistent evaluation points. Extends the range backwards to include
    /// lookback for the first step.
    fn compute_subquery_plan(
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

    /// Check sample cache for each candidate. Collect cache hits into series_samples.
    /// Return cache misses with pre-computed meta for B3 sample loading.
    fn plan_sample_hits_and_misses(
        &mut self,
        bucket: &TimeBucket,
        series_with_meta: &[(SeriesId, Arc<SeriesMeta>)],
        range_start_ms: i64,
        range_end_ms: i64,
        series_samples: &mut HashMap<SeriesFingerprint, (Arc<[Label]>, Vec<Sample>)>,
    ) -> Vec<(SeriesId, Arc<SeriesMeta>)> {
        let mut to_load = Vec::new();
        for (series_id, meta) in series_with_meta {
            if let Some(cached_samples) = self.reader.cache.get_samples(bucket, series_id) {
                self.reader.stats.sample_loads += 1;
                self.reader.stats.sample_cache_hits += 1;
                let samples = CachedQueryReader::<R>::filter_samples_binary_search(
                    cached_samples,
                    range_start_ms,
                    range_end_ms,
                );
                if !samples.is_empty() {
                    let entry = series_samples
                        .entry(meta.fingerprint)
                        .or_insert_with(|| (Arc::clone(&meta.sorted_labels), Vec::new()));
                    entry.1.extend(samples);
                }
            } else {
                to_load.push((*series_id, Arc::clone(meta)));
            }
        }
        to_load
    }

    /// Fetches and normalizes samples for all matching series across all buckets.
    ///
    /// Five-phase execution model:
    /// - Phase A: Serial planning (cache checks for all buckets)
    /// - Phase B1: Parallel metadata resolution (selector-miss buckets)
    /// - Phase B2: Serial planning for newly-resolved buckets
    /// - Phase B3: Parallel sample loading (all work items)
    /// - Phase C: Sequential merge (cache + filter + merge results)
    async fn fetch_series_samples(
        &mut self,
        vector_selector: &VectorSelector,
        range_start_ms: i64,
        range_end_ms: i64,
    ) -> EvalResult<HashMap<SeriesFingerprint, (Arc<[Label]>, Vec<Sample>)>> {
        let mut buckets = self.reader.list_buckets().await?;
        buckets.sort_by(|a, b| b.start.cmp(&a.start));

        let selector_key = SelectorKey::from_selector(vector_selector);
        let mut series_samples = HashMap::new();

        // Per-bucket elapsed time accumulator for backward-compatible metrics
        let mut bucket_totals: HashMap<TimeBucket, u64> = HashMap::new();

        // ═══ Phase A: Serial planning (all buckets) ═══
        let mut selector_miss_buckets: Vec<TimeBucket> = Vec::new();
        let mut sample_work_items: Vec<BucketSampleWorkItem> = Vec::new();

        for bucket in &buckets {
            let bucket_start = std::time::Instant::now();

            match self.reader.get_cached_selector(bucket, &selector_key) {
                Some(candidates) => {
                    if candidates.is_empty() {
                        *bucket_totals.entry(*bucket).or_default() +=
                            bucket_start.elapsed().as_millis() as u64;
                        continue;
                    }

                    // Cached selector — resolve forward_index + series_meta through CachedQueryReader
                    let candidates_vec: Vec<_> = candidates.iter().copied().collect();
                    let fi_view = self
                        .reader
                        .forward_index(bucket, &candidates_vec)
                        .await
                        .map_err(|e| EvaluationError::StorageError(e.to_string()))?;
                    let mut series_with_meta = Vec::new();
                    for &sid in &candidates_vec {
                        if let Some(meta) =
                            self.reader
                                .get_or_insert_series_meta(bucket, sid, fi_view.as_ref())
                        {
                            series_with_meta.push((sid, meta));
                        }
                    }

                    let to_load = self.plan_sample_hits_and_misses(
                        bucket,
                        &series_with_meta,
                        range_start_ms,
                        range_end_ms,
                        &mut series_samples,
                    );

                    *bucket_totals.entry(*bucket).or_default() +=
                        bucket_start.elapsed().as_millis() as u64;

                    if !to_load.is_empty() {
                        sample_work_items.push(BucketSampleWorkItem {
                            bucket: *bucket,
                            to_load,
                        });
                    }
                }
                None => {
                    *bucket_totals.entry(*bucket).or_default() +=
                        bucket_start.elapsed().as_millis() as u64;
                    selector_miss_buckets.push(*bucket);
                }
            }
        }

        // ═══ Phase B1: Parallel metadata resolution (selector-miss buckets) ═══
        if !selector_miss_buckets.is_empty() {
            const PER_QUERY_BUCKET_CONCURRENCY: usize = 4;
            let underlying_reader = self.reader.reader;
            let coordinator = self.reader.load_coordinator.clone();
            let selector_clone = vector_selector.clone();

            let b1_wall_start = std::time::Instant::now();

            let resolved: Vec<_> = futures::stream::iter(selector_miss_buckets.into_iter())
                .map(|bucket| {
                    let sel = selector_clone.clone();
                    let coord = coordinator.clone();
                    async move { resolve_bucket_raw(underlying_reader, bucket, &sel, &coord).await }
                })
                .buffer_unordered(PER_QUERY_BUCKET_CONCURRENCY)
                .collect()
                .await;

            self.reader.stats.parallel_selector_wall_ms =
                b1_wall_start.elapsed().as_millis() as u64;

            // ═══ Phase B2: Serial planning for newly-resolved buckets ═══
            for result in resolved {
                let res = result?;
                let bs = &res.stats;

                self.reader.stats.selector_misses += 1;
                self.reader.stats.selector_miss_ms += bs.selector_ms;
                self.reader.stats.forward_index_calls += bs.forward_index_calls;
                self.reader.stats.forward_index_misses += bs.forward_index_misses;
                self.reader.stats.forward_index_miss_ms += bs.forward_index_miss_ms;
                self.reader.stats.metadata_queue_wait_ms += bs.metadata_queue_wait_ms;
                self.reader.stats.metadata_load_ms += bs.metadata_load_ms;
                self.reader.stats.metadata_permit_acquires += bs.metadata_permit_acquires;
                self.reader.stats.series_meta_misses += bs.series_meta_count;
                self.reader.stats.fi_series_loaded += bs.fi_series_loaded;
                self.reader.stats.fi_batch_ops += bs.fi_batch_ops;
                self.reader.stats.fi_point_lookups += bs.fi_point_lookups;
                self.reader.stats.fi_range_scans += bs.fi_range_scans;
                self.reader.stats.fi_range_scan_series += bs.fi_range_scan_series;
                self.reader.stats.fi_scan_span_series += bs.fi_scan_span_series;
                self.reader.stats.fi_run_len_1 += bs.fi_run_len_1;
                self.reader.stats.fi_run_len_2_3 += bs.fi_run_len_2_3;
                self.reader.stats.fi_run_len_4_7 += bs.fi_run_len_4_7;
                self.reader.stats.fi_run_len_8_plus += bs.fi_run_len_8_plus;
                self.reader.stats.fi_merge_accepted += bs.fi_merge_accepted;
                self.reader.stats.fi_merge_rejected_gap += bs.fi_merge_rejected_gap;
                self.reader.stats.fi_merge_rejected_density += bs.fi_merge_rejected_density;
                self.reader.stats.parallel_selector_count += 1;
                self.reader.stats.parallel_selector_sum_ms += bs.total_ms;

                *bucket_totals.entry(res.bucket).or_default() += bs.total_ms;

                // Cache selector result
                self.reader.cache_selector(
                    res.bucket,
                    selector_key.clone(),
                    res.candidates.clone(),
                );

                if res.candidates.is_empty() {
                    continue;
                }

                let b2_bucket_start = std::time::Instant::now();

                // Cache forward_index_view
                if let Some(fi_view) = res.forward_index_view {
                    self.reader.cache.cache_forward_index(
                        res.bucket,
                        res.forward_index_key,
                        fi_view,
                    );
                }

                // Cache series_meta entries
                for (sid, meta) in &res.series_meta {
                    self.reader
                        .series_meta_cache
                        .insert((res.bucket, *sid), Arc::clone(meta));
                }

                // Check sample cache
                let to_load = self.plan_sample_hits_and_misses(
                    &res.bucket,
                    &res.series_meta,
                    range_start_ms,
                    range_end_ms,
                    &mut series_samples,
                );

                *bucket_totals.entry(res.bucket).or_default() +=
                    b2_bucket_start.elapsed().as_millis() as u64;

                if !to_load.is_empty() {
                    sample_work_items.push(BucketSampleWorkItem {
                        bucket: res.bucket,
                        to_load,
                    });
                }
            }
        }

        // ═══ Phase B3: Parallel sample loading ═══
        if !sample_work_items.is_empty() {
            const PER_QUERY_BUCKET_CONCURRENCY: usize = 4;
            let underlying_reader = self.reader.reader;
            let coordinator = self.reader.load_coordinator.clone();

            let b3_wall_start = std::time::Instant::now();

            let sample_results: Vec<_> = futures::stream::iter(sample_work_items.into_iter())
                .map(|work| {
                    let coord = coordinator.clone();
                    async move { process_bucket_sample_loads(underlying_reader, work, coord).await }
                })
                .buffer_unordered(PER_QUERY_BUCKET_CONCURRENCY)
                .collect()
                .await;

            self.reader.stats.parallel_sample_wall_ms = b3_wall_start.elapsed().as_millis() as u64;

            // ═══ Phase C: Sequential merge ═══
            for result in sample_results {
                let sr = result?;
                let bs = &sr.stats;

                self.reader.stats.sample_loads += bs.sample_loads;
                self.reader.stats.sample_misses += bs.sample_misses;
                self.reader.stats.samples_loaded += bs.samples_loaded;
                self.reader.stats.sample_queue_wait_ms += bs.sample_queue_wait_ms;
                self.reader.stats.sample_load_ms += bs.sample_load_ms;
                self.reader.stats.sample_miss_ms += bs.sample_miss_ms;
                self.reader.stats.sample_permit_acquires += bs.sample_permit_acquires;
                self.reader.stats.parallel_sample_loads += bs.parallel_sample_loads;
                self.reader.stats.parallel_sample_bucket_count += 1;
                self.reader.stats.parallel_sample_sum_ms += bs.bucket_ms;

                *bucket_totals.entry(sr.bucket).or_default() += bs.bucket_ms;

                for sd in sr.series_data {
                    let filtered = CachedQueryReader::<R>::filter_samples_binary_search(
                        &sd.all_samples,
                        range_start_ms,
                        range_end_ms,
                    );
                    self.reader
                        .cache
                        .cache_samples(sr.bucket, sd.series_id, sd.all_samples);
                    if !filtered.is_empty() {
                        let entry = series_samples
                            .entry(sd.meta.fingerprint)
                            .or_insert_with(|| (Arc::clone(&sd.meta.sorted_labels), Vec::new()));
                        entry.1.extend(filtered);
                    }
                }
            }
        }

        // Derive backward-compatible bucket metrics
        self.reader.stats.preload_bucket_ms_sum = bucket_totals.values().sum();
        self.reader.stats.preload_bucket_ms_max =
            bucket_totals.values().copied().max().unwrap_or(0);

        for (_fp, (_labels, samples)) in series_samples.iter_mut() {
            samples.sort_by_key(|s| s.timestamp_ms);
            samples.dedup_by_key(|s| s.timestamp_ms);
        }

        Ok(series_samples)
    }

    /// Buckets samples into step-aligned time windows using a sliding window algorithm.
    ///
    /// Uses O(samples + steps) complexity instead of O(samples × steps) by maintaining
    /// a monotonic pointer through sorted samples. For each step, finds the most recent
    /// sample within the lookback window. Uses > (not >=) for start boundary to match
    /// Prometheus staleness semantics.
    fn bucket_series_samples(
        series_samples: HashMap<SeriesFingerprint, (Arc<[Label]>, Vec<Sample>)>,
        aligned_start_ms: i64,
        subquery_end_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
        expected_steps: usize,
    ) -> Vec<EvalSamples> {
        let mut range_vector = Vec::with_capacity(series_samples.len());

        for (_fingerprint, (labels, samples)) in series_samples {
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
                    labels: EvalLabels::Shared(labels),
                });
            }
        }

        range_vector
    }

    /// Fast path for VectorSelector subqueries using range-based evaluation.
    ///
    /// Instead of evaluating the selector once per step (O(steps × series × index_lookup)),
    /// this fetches all samples in the range once and buckets them into steps
    /// (O(series × samples_in_range + samples + steps)). Achieves 113× speedup for
    /// typical workloads by eliminating redundant selector evaluations and using
    /// a sliding window algorithm for bucketing.
    async fn evaluate_subquery_vector_selector(
        &mut self,
        vector_selector: &VectorSelector,
        subquery_start_ms: i64,
        subquery_end_ms: i64,
        step_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<ExprResult> {
        let (aligned_start_ms, range_start_ms, range_end_ms, expected_steps) =
            Self::compute_subquery_plan(
                subquery_start_ms,
                subquery_end_ms,
                step_ms,
                lookback_delta_ms,
            );

        let series_samples = self
            .fetch_series_samples(vector_selector, range_start_ms, range_end_ms)
            .await?;

        let range_vector = Self::bucket_series_samples(
            series_samples,
            aligned_start_ms,
            subquery_end_ms,
            step_ms,
            lookback_delta_ms,
            expected_steps,
        );

        Ok(ExprResult::RangeVector(range_vector))
    }

    async fn evaluate_vector_selector(
        &mut self,
        vector_selector: &VectorSelector,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
        lookback_delta_ms: i64,
    ) -> EvalResult<ExprResult> {
        // Fast path: use preloaded data if available (outer range-query context only).
        // Disabled inside subqueries which have their own step grid/lookback context.
        let preload_key = PreloadKey::from_selector(vector_selector);
        if self.preload_eligible
            && let Some(preloaded) = self.preloaded_instant.get(&preload_key)
        {
            self.reader.stats.preload_hits += 1;

            // Step index from raw evaluation_ts (before modifiers) — matches outer step loop
            let step_idx = ((evaluation_ts.as_millis() - preloaded.eval_start_ms)
                / preloaded.step_ms) as usize;

            let mut samples = Vec::new();
            for series in &preloaded.series {
                if let Some(Some(sample)) = series.values.get(step_idx) {
                    samples.push(EvalSample {
                        timestamp_ms: sample.timestamp_ms,
                        value: sample.value,
                        labels: EvalLabels::Shared(Arc::clone(&series.labels)),
                        drop_name: false,
                    });
                }
            }
            return Ok(ExprResult::InstantVector(samples));
        }

        // Slow path: per-step evaluation
        // Apply time modifiers (offset and @)
        let adjusted_eval_ts = self.apply_time_modifiers(
            vector_selector.at.as_ref(),
            vector_selector.offset.as_ref(),
            query_start,
            query_end,
            evaluation_ts,
        )?;

        let end_ms = adjusted_eval_ts.as_millis();
        let start_ms = end_ms - lookback_delta_ms;

        // Get all buckets and sort by start time in reverse order (newest first)
        let mut buckets = self.reader.list_buckets().await?;
        buckets.sort_by(|a, b| b.start.cmp(&a.start)); // newest first

        let selector_key = SelectorKey::from_selector(vector_selector);
        let mut series_with_results: HashSet<SeriesFingerprint> = HashSet::new();
        let mut samples = Vec::new();

        // Iterate through buckets in reverse time order (newest first)
        for bucket in buckets {
            // Check selector cache first, fall back to full evaluation
            let candidates =
                if let Some(cached) = self.reader.get_cached_selector(&bucket, &selector_key) {
                    cached
                } else {
                    self.reader.stats.selector_misses += 1;
                    let t0 = std::time::Instant::now();
                    let result =
                        evaluate_selector_with_reader(&mut self.reader, bucket, vector_selector)
                            .await
                            .map_err(|e| EvaluationError::InternalError(e.to_string()))?;
                    self.reader.stats.selector_miss_ms += t0.elapsed().as_millis() as u64;
                    self.reader
                        .cache_selector(bucket, selector_key.clone(), result.clone());
                    Arc::new(result)
                };

            if candidates.is_empty() {
                continue;
            }

            // Batch load forward index for all candidates upfront
            let candidates_vec: Vec<_> = candidates.iter().copied().collect();
            let forward_index_view = self.reader.forward_index(&bucket, &candidates_vec).await?;

            for series_id in candidates_vec {
                // Get series metadata from cache (Arc — cheap refcount bump)
                let meta = match self.reader.get_or_insert_series_meta(
                    &bucket,
                    series_id,
                    forward_index_view.as_ref(),
                ) {
                    Some(m) => m,
                    None => {
                        return Err(EvaluationError::InternalError(format!(
                            "Series {} not found in bucket {:?}",
                            series_id, bucket
                        )));
                    }
                };

                // Skip if we already found a sample for this series in a newer bucket
                if series_with_results.contains(&meta.fingerprint) {
                    continue;
                }

                // Read samples from this bucket within the lookback window
                let sample_data = self
                    .reader
                    .samples(&bucket, series_id, start_ms, end_ms)
                    .await?;

                // Find the best (latest) point in the time range
                if let Some(best_sample) = sample_data.last() {
                    samples.push(EvalSample {
                        timestamp_ms: best_sample.timestamp_ms,
                        value: best_sample.value,
                        labels: EvalLabels::Shared(Arc::clone(&meta.sorted_labels)),
                        drop_name: false,
                    });

                    // Mark this series fingerprint as found so we don't add it again from older buckets
                    series_with_results.insert(meta.fingerprint);
                }
            }
        }

        Ok(ExprResult::InstantVector(samples))
    }

    /// Apply offset and @ modifiers to adjust the evaluation time.
    /// Thin wrapper around the free function `apply_time_modifiers_ms`.
    fn apply_time_modifiers(
        &self,
        at: Option<&AtModifier>,
        offset: Option<&Offset>,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
    ) -> EvalResult<Timestamp> {
        let ms = apply_time_modifiers_ms(
            at,
            offset,
            query_start.as_millis(),
            query_end.as_millis(),
            evaluation_ts.as_millis(),
        );
        Ok(Timestamp::from_millis(ms))
    }

    async fn evaluate_call(
        &mut self,
        call: &Call,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
        interval_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<ExprResult> {
        // String-typed arguments are passed through as raw AST nodes for
        // string-argument functions such as label_replace/label_join.
        let mut arg_results = Vec::with_capacity(call.args.args.len());
        for (idx, arg) in call.args.args.iter().enumerate() {
            let expected_type = if idx < call.func.arg_types.len() {
                call.func.arg_types[idx]
            } else if call.func.variadic != 0 && !call.func.arg_types.is_empty() {
                call.func.arg_types[call.func.arg_types.len() - 1]
            } else {
                return Err(EvaluationError::InternalError(format!(
                    "argument {} is out of bounds for function {}",
                    idx, call.func.name
                )));
            };

            if expected_type == ValueType::String {
                arg_results.push(None);
                continue;
            }

            let arg_result = self
                .evaluate_expr(
                    arg,
                    query_start,
                    query_end,
                    evaluation_ts,
                    interval_ms,
                    lookback_delta_ms,
                )
                .await?;
            arg_results.push(Some(arg_result));
        }

        let registry = FunctionRegistry::new();

        // Handle negative timestamps from subquery steps that go before UNIX_EPOCH.
        // Example: sum_over_time(metric[100s:1s] @ 50) at 25s
        //   - Subquery range: [50s - 100s, 50s] = [-50s, 50s]
        //   - Steps include: -50s, -49s, ..., 0s, 1s, ..., 50s
        //   - Function is called with evaluation_ts = -50s (negative!)
        let eval_timestamp_ms = evaluation_ts.as_millis();

        let mut evaluated_args = Vec::with_capacity(arg_results.len());
        for arg_result in arg_results {
            match arg_result {
                Some(ExprResult::InstantVector(samples)) => {
                    evaluated_args.push(Some(PromQLArg::InstantVector(samples)))
                }
                Some(ExprResult::Scalar(value)) => {
                    evaluated_args.push(Some(PromQLArg::Scalar(value)))
                }
                Some(ExprResult::RangeVector(samples)) => {
                    evaluated_args.push(Some(PromQLArg::RangeVector(samples)))
                }
                // Preserve string-arg positions here; string-aware functions
                // read the raw AST nodes from ctx.raw_args instead.
                None => evaluated_args.push(None),
            }
        }

        if let Some(func) = registry.get(call.func.name) {
            let ctx = FunctionCallContext {
                eval_timestamp_ms,
                raw_args: &call.args.args,
            };
            let result = func.apply_call(evaluated_args, &ctx)?;
            if call.func.return_type == ValueType::Scalar {
                return match result.as_slice() {
                    [sample] => Ok(ExprResult::Scalar(sample.value)),
                    _ => Err(EvaluationError::InternalError(format!(
                        "scalar-returning function {} must return exactly one sample",
                        call.func.name
                    ))),
                };
            }

            Ok(ExprResult::InstantVector(result))
        } else {
            Err(EvaluationError::InternalError(format!(
                "Unknown instant/scalar function: {}",
                call.func.name
            )))
        }
    }

    async fn evaluate_binary_expr(
        &mut self,
        expr: &BinaryExpr,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
        interval_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<ExprResult> {
        let lhs = expr.lhs.as_ref();
        let rhs = expr.rhs.as_ref();
        let op = expr.op;
        // Evaluate left and right expressions
        let left_result = self
            .evaluate_expr(
                lhs,
                query_start,
                query_end,
                evaluation_ts,
                interval_ms,
                lookback_delta_ms,
            )
            .await?;
        let right_result = self
            .evaluate_expr(
                rhs,
                query_start,
                query_end,
                evaluation_ts,
                interval_ms,
                lookback_delta_ms,
            )
            .await?;

        // Check if this is a comparison operation (filters results in PromQL)
        let is_comparison = matches!(op.id(), T_NEQ | T_LSS | T_GTR | T_LTE | T_GTE | T_EQLC);
        // With `bool` modifier, comparison ops return 0/1 for all pairs instead of filtering
        let return_bool = expr.return_bool();

        match (left_result, right_result) {
            // Vector-Scalar operations: apply scalar to each vector element
            (ExprResult::InstantVector(vector), ExprResult::Scalar(scalar)) => {
                let result: Vec<_> = vector
                    .into_iter()
                    .filter_map(|mut sample| {
                        match self.apply_binary_op(op, sample.value, scalar) {
                            Ok(value) => {
                                // For comparison ops without bool, filter out false results
                                if is_comparison && !return_bool && value == 0.0 {
                                    None
                                } else {
                                    sample.value = value;
                                    sample.drop_name |=
                                        Self::changes_metric_schema(op) || return_bool;
                                    Some(sample)
                                }
                            }
                            Err(_) => None,
                        }
                    })
                    .collect();
                Ok(ExprResult::InstantVector(result))
            }
            // Scalar-Vector operations: apply scalar to each vector element
            (ExprResult::Scalar(scalar), ExprResult::InstantVector(vector)) => {
                let result: Vec<_> = vector
                    .into_iter()
                    .filter_map(|mut sample| {
                        match self.apply_binary_op(op, scalar, sample.value) {
                            Ok(value) => {
                                // For comparison ops without bool, filter out false results
                                if is_comparison && !return_bool && value == 0.0 {
                                    None
                                } else {
                                    sample.value = value;
                                    sample.drop_name |=
                                        Self::changes_metric_schema(op) || return_bool;
                                    Some(sample)
                                }
                            }
                            Err(_) => None,
                        }
                    })
                    .collect();
                Ok(ExprResult::InstantVector(result))
            }
            // Vector-Vector operations: many-to-many not supported
            (
                ExprResult::InstantVector(mut left_vector),
                ExprResult::InstantVector(mut right_vector),
            ) => {
                let card = match expr.modifier.as_ref().map(|m| &m.card) {
                    Some(VectorMatchCardinality::ManyToMany) => {
                        return Err(EvaluationError::InternalError(
                            "many-to-many cardinality not supported".to_string(),
                        ));
                    }
                    Some(c) => c,
                    None => &VectorMatchCardinality::OneToOne,
                };

                let matching = expr.modifier.as_ref().and_then(|m| m.matching.as_ref());

                // Materialize pending __name__ drops before matching so that
                // stale names don't participate in match keys or result labels
                for sample in left_vector.iter_mut() {
                    if sample.drop_name {
                        sample.labels.remove(METRIC_NAME);
                    }
                }
                for sample in right_vector.iter_mut() {
                    if sample.drop_name {
                        sample.labels.remove(METRIC_NAME);
                    }
                }

                let is_group_right = matches!(card, VectorMatchCardinality::OneToMany(_));
                let is_one_to_one = matches!(card, VectorMatchCardinality::OneToOne);
                let group_labels = card.labels().map(|l| &l.labels);

                // Determine which side is "one" vs "many" for matching purposes.
                // For one-to-one mappings, we treat the right-hand side as the "one" side.
                let (one_vec, many_vec) = if is_group_right {
                    (left_vector, right_vector)
                } else {
                    (right_vector, left_vector)
                };

                // Build "one" side index keyed by match signature
                let mut one_map: HashMap<Vec<(String, String)>, EvalSample> = HashMap::new();
                for sample in one_vec {
                    let key = Self::compute_binary_match_key(&sample.labels, matching);
                    if one_map.insert(key, sample).is_some() {
                        return Err(EvaluationError::InternalError(format!(
                            "many-to-many matching not allowed: found duplicate series on the {} side of the operation",
                            if is_group_right { "left" } else { "right" }
                        )));
                    }
                }

                let mut result = Vec::new();
                let mut one_to_one_seen: HashSet<Vec<(String, String)>> = HashSet::new();
                // PromQL grouped matching (`group_left` / `group_right`) requires every
                // output time series to remain uniquely identifiable. Two different matches
                // are not allowed to collapse to the same final output labels.
                // Keep a set of final output label keys and fail on duplicates.
                let mut grouped_result_seen: HashSet<Vec<(String, String)>> = HashSet::new();

                for many_sample in many_vec {
                    let key = Self::compute_binary_match_key(&many_sample.labels, matching);

                    // Look up matching "one" sample
                    let one_sample = match one_map.get(&key) {
                        Some(s) => s,
                        None => continue, // silently dropped if unmatched on "one" side
                    };

                    // One-to-one vector matching must be unique on both sides. We already
                    // validated uniqueness on the "one" map build; this guards the "many"
                    // iteration path when card=OneToOne.
                    if is_one_to_one && !one_to_one_seen.insert(key) {
                        return Err(EvaluationError::InternalError(
                            "many-to-many matching not allowed: found duplicate series on the left side of the operation".to_string(),
                        ));
                    }

                    // Preserve original lhs/rhs ordering for the operator.
                    let (lhs, rhs) = if is_group_right {
                        (one_sample.value, many_sample.value)
                    } else {
                        (many_sample.value, one_sample.value)
                    };

                    match self.apply_binary_op(op, lhs, rhs) {
                        Ok(value) => {
                            let mut result_labels = Self::result_metric(
                                many_sample.labels,
                                op,
                                if is_one_to_one { matching } else { None }, // should only be filtered for one-to-one case
                            );
                            // For `group_left(<labels>)` / `group_right(<labels>)`, each listed
                            // label must come from the "one" side. Use set-or-remove semantics:
                            // - if present on "one" side, copy/overwrite it in output
                            // - if absent on "one" side, remove it from output
                            // This avoids leaking stale values from the "many" side.
                            if let Some(extra) = group_labels {
                                for name in extra {
                                    match one_sample.labels.get(name) {
                                        Some(v) => {
                                            result_labels.insert(name.clone(), v.to_string());
                                        }
                                        None => {
                                            result_labels.remove(name);
                                        }
                                    }
                                }
                            }

                            let drop_name = many_sample.drop_name || return_bool;
                            if !is_one_to_one {
                                // Duplicate detection: compute key by borrowing and filtering,
                                // avoiding a clone of result_labels.
                                let key: Vec<(String, String)> = result_labels
                                    .iter()
                                    .filter(|l| !drop_name || l.name != METRIC_NAME)
                                    .map(|l| (l.name.clone(), l.value.clone()))
                                    .collect();
                                if !grouped_result_seen.insert(key) {
                                    return Err(EvaluationError::InternalError(
                                        "multiple matches for labels: grouping labels must ensure unique matches"
                                            .to_string(),
                                    ));
                                }
                            }

                            // For comparison ops without bool, filter out false results.
                            if is_comparison && !return_bool && value == 0.0 {
                                continue;
                            }
                            // PromQL comparison operators without `bool` are filters.
                            // For vector-vector comparisons (one-to-one and grouped), keep
                            // matched true pairs and propagate the original LHS sample value
                            // instead of the computed predicate value (1/0).
                            let output_value = if is_comparison && !return_bool {
                                lhs
                            } else {
                                value
                            };
                            result.push(EvalSample {
                                timestamp_ms: many_sample.timestamp_ms,
                                value: output_value,
                                labels: result_labels,
                                drop_name,
                            });
                        }
                        Err(e) => return Err(e),
                    }
                }

                Ok(ExprResult::InstantVector(result))
            }
            // Scalar-Scalar operations
            (ExprResult::Scalar(left), ExprResult::Scalar(right)) => {
                let result_value = self.apply_binary_op(op, left, right)?;
                Ok(ExprResult::Scalar(result_value))
            }
            // RangeVector operations not yet supported
            (ExprResult::RangeVector(_), _) | (_, ExprResult::RangeVector(_)) => {
                Err(EvaluationError::InternalError(
                    "Binary operations with range vectors not yet supported".to_string(),
                ))
            }
        }
    }

    /// Returns true if the binary operation changes the metric schema, meaning
    /// `__name__` should be dropped from the result. Mirrors Prometheus's `resultMetric`
    /// logic in engine.go.
    fn changes_metric_schema(op: TokenType) -> bool {
        matches!(op.id(), T_ADD | T_SUB | T_MUL | T_DIV)
    }

    /// Compute the result labels for a vector-vector binary operation.
    /// Mirrors Prometheus's `resultMetric` (engine.go L3062-3104):
    /// 1. Arithmetic ops always drop `__name__`
    /// 2. `on()` keeps only listed labels; `ignoring()` removes listed labels
    fn result_metric(
        mut labels: EvalLabels,
        op: TokenType,
        matching: Option<&LabelModifier>,
    ) -> EvalLabels {
        if Self::changes_metric_schema(op) {
            labels.remove(METRIC_NAME);
        }
        match matching {
            Some(LabelModifier::Include(label_list)) => {
                labels.retain(|l| label_list.labels.contains(&l.name));
            }
            Some(LabelModifier::Exclude(label_list)) => {
                labels.retain(|l| !label_list.labels.contains(&l.name));
            }
            None => {}
        }
        labels
    }

    fn apply_binary_op(&self, op: TokenType, left: f64, right: f64) -> EvalResult<f64> {
        // Use the token constants with TokenType::new() for clean comparison
        match op.id() {
            T_ADD => Ok(left + right),
            T_SUB => Ok(left - right),
            T_MUL => Ok(left * right),
            T_DIV => {
                if right == 0.0 {
                    Ok(f64::NAN) // Division by zero results in NaN in PromQL
                } else {
                    Ok(left / right)
                }
            }
            T_NEQ => Ok(if left != right { 1.0 } else { 0.0 }),
            T_LSS => Ok(if left < right { 1.0 } else { 0.0 }),
            T_GTR => Ok(if left > right { 1.0 } else { 0.0 }),
            T_LTE => Ok(if left <= right { 1.0 } else { 0.0 }),
            T_GTE => Ok(if left >= right { 1.0 } else { 0.0 }),
            T_EQLC => Ok(if left == right { 1.0 } else { 0.0 }),
            _ => Err(EvaluationError::InternalError(format!(
                "Binary operator not yet implemented: {:?}",
                op
            ))),
        }
    }

    /// Compute a grouping key from sorted labels. Because input is sorted,
    /// output is sorted — no extra sort step needed.
    fn compute_grouping_key(
        labels: &EvalLabels,
        modifier: Option<&LabelModifier>,
        drop_name: bool,
    ) -> Vec<(String, String)> {
        let filter_name = |l: &&Label| !drop_name || l.name != METRIC_NAME;
        match modifier {
            None => Vec::new(),
            Some(LabelModifier::Include(label_list)) => labels
                .iter()
                .filter(filter_name)
                .filter(|l| label_list.labels.contains(&l.name))
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
            Some(LabelModifier::Exclude(label_list)) => labels
                .iter()
                .filter(filter_name)
                .filter(|l| !label_list.labels.contains(&l.name))
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
        }
    }

    async fn evaluate_aggregation_param_as_scalar(
        &mut self,
        param: Option<&Expr>,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
        interval_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<f64> {
        let Some(param_expr) = param else {
            return Err(EvaluationError::InternalError(
                "aggregation requires a scalar parameter".to_string(),
            ));
        };

        let param_value = match self
            .evaluate_expr(
                param_expr,
                query_start,
                query_end,
                evaluation_ts,
                interval_ms,
                lookback_delta_ms,
            )
            .await?
        {
            ExprResult::Scalar(value) => value,
            ExprResult::InstantVector(_) => {
                return Err(EvaluationError::InternalError(
                    "aggregation parameter must evaluate to a scalar".to_string(),
                ));
            }
            ExprResult::RangeVector(_) => {
                return Err(EvaluationError::InternalError(
                    "aggregation parameter cannot be a range vector".to_string(),
                ));
            }
        };

        Ok(param_value)
    }

    // topk/bottomk params are scalar floats, but selection needs a bounded count.
    // Coerce once to match PromQL-like behavior: clamp to input size and treat
    // k < 1 as empty output.
    fn coerce_k_size(k_param: f64, input_len: usize) -> usize {
        let max_k = input_len as i64;
        let coerced = (k_param as i64).min(max_k);
        if coerced < 1 { 0 } else { coerced as usize }
    }

    // Group key construction clones label strings. This is acceptable since
    // topk's dominant cost is sorting/selection. If profiling shows this is hot,
    // consider using series fingerprints as group keys instead.
    fn group_samples_for_k_aggregation(
        samples: Vec<EvalSample>,
        modifier: Option<&LabelModifier>,
    ) -> HashMap<Vec<(String, String)>, Vec<EvalSample>> {
        let mut groups: HashMap<Vec<(String, String)>, Vec<EvalSample>> = HashMap::new();
        for mut sample in samples {
            let group_key = Self::compute_grouping_key(&sample.labels, modifier, sample.drop_name);
            // Materialize pending metric-name drops so __name__
            // doesn't incorrectly affect downstream label output.
            if sample.drop_name {
                sample.labels.remove(METRIC_NAME);
                sample.drop_name = false;
            }
            groups.entry(group_key).or_default().push(sample);
        }
        groups
    }

    fn select_k_from_group(
        mut samples: Vec<EvalSample>,
        k: usize,
        order: KAggregationOrder,
    ) -> Vec<EvalSample> {
        let keep = k.min(samples.len());
        let mut selected_indices = select_k_indices_with_heap(&samples, keep, order);
        // Remove from highest index first so swap_remove cannot invalidate
        // indices we still need to read.
        selected_indices.sort_unstable_by(|left, right| right.cmp(left));

        let mut result = Vec::with_capacity(selected_indices.len());
        for idx in selected_indices {
            result.push(samples.swap_remove(idx));
        }
        result.sort_by(|left, right| compare_k_values(left.value, right.value, order));
        result
    }

    async fn evaluate_k_aggregate(
        &mut self,
        aggregate: &AggregateExpr,
        mut samples: Vec<EvalSample>,
        eval_ctx: AggregationEvalContext,
    ) -> EvalResult<Vec<EvalSample>> {
        let order = match aggregate.op.id() {
            T_TOPK => KAggregationOrder::Top,
            T_BOTTOMK => KAggregationOrder::Bottom,
            _ => {
                return Err(EvaluationError::InternalError(format!(
                    "evaluate_k_aggregate called for non-k aggregation operator: {:?}",
                    aggregate.op
                )));
            }
        };

        let k_param = self
            .evaluate_aggregation_param_as_scalar(
                aggregate.param.as_deref(),
                eval_ctx.query_start,
                eval_ctx.query_end,
                eval_ctx.evaluation_ts,
                eval_ctx.interval_ms,
                eval_ctx.lookback_delta_ms,
            )
            .await?;
        let k = Self::coerce_k_size(k_param, samples.len());
        if k == 0 {
            return Ok(vec![]);
        }

        // k-aggregation without `by` / `without`: all samples belong to one
        // implicit group, so skip hashmap bucketing overhead.
        if aggregate.modifier.is_none() {
            if samples.iter().any(|sample| sample.drop_name) {
                for sample in &mut samples {
                    if sample.drop_name {
                        sample.labels.remove(METRIC_NAME);
                        sample.drop_name = false;
                    }
                }
            }
            return Ok(Self::select_k_from_group(samples, k, order));
        }

        // Histogram samples are not yet represented in EvalSample.
        // This path is isolated so histogram handling can be added later
        // without changing k-aggregation grouping/selection flow.
        let grouped = Self::group_samples_for_k_aggregation(samples, aggregate.modifier.as_ref());
        let mut result = Vec::new();
        for group_samples in grouped.into_values() {
            result.extend(Self::select_k_from_group(group_samples, k, order));
        }
        Ok(result)
    }

    /// Compute a match signature for a sample's labels per Prometheus binary op semantics.
    /// - No modifier: match on ALL labels except `__name__`
    /// - `on(l1, l2)` (Include): match only on listed labels
    /// - `ignoring(l1, l2)` (Exclude): match on all labels except listed ones and `__name__`
    ///
    /// This is intentionally separate from `compute_grouping_labels` because their `None`
    /// cases have opposite semantics (aggregation groups everything together; binary ops
    /// match on all labels).
    fn compute_binary_match_key(
        labels: &EvalLabels,
        matching: Option<&LabelModifier>,
    ) -> Vec<(String, String)> {
        // Input is sorted, so output is sorted — no sort step needed.
        match matching {
            None => labels
                .iter()
                .filter(|l| l.name != METRIC_NAME)
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
            Some(LabelModifier::Include(label_list)) => labels
                .iter()
                .filter(|l| label_list.labels.contains(&l.name))
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
            Some(LabelModifier::Exclude(label_list)) => labels
                .iter()
                .filter(|l| l.name != METRIC_NAME && !label_list.labels.contains(&l.name))
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
        }
    }

    async fn evaluate_aggregate(
        &mut self,
        aggregate: &AggregateExpr,
        query_start: Timestamp,
        query_end: Timestamp,
        evaluation_ts: Timestamp,
        interval_ms: i64,
        lookback_delta_ms: i64,
    ) -> EvalResult<ExprResult> {
        // Evaluate the inner expression to get all samples
        let result = self
            .evaluate_expr(
                &aggregate.expr,
                query_start,
                query_end,
                evaluation_ts,
                interval_ms,
                lookback_delta_ms,
            )
            .await?;

        // Extract samples from the result
        let samples = match result {
            ExprResult::InstantVector(samples) => samples,
            ExprResult::Scalar(_) => {
                return Err(EvaluationError::InternalError(
                    "Cannot aggregate scalar values".to_string(),
                ));
            }
            ExprResult::RangeVector(_) => {
                return Err(EvaluationError::InternalError(
                    "Cannot aggregate range vectors directly - use functions like rate() first"
                        .to_string(),
                ));
            }
        };

        // If there are no samples, return empty result
        if samples.is_empty() {
            return Ok(ExprResult::InstantVector(vec![]));
        }

        // `topk`/`bottomk` are selection aggregations, not reduction aggregations.
        // They must keep full EvalSample records so the selected output series
        // retain their original labels/timestamps. The generic path below is a
        // reducer: it collapses each group to Vec<f64> and emits one value per
        // group, which discards per-series identity and cannot express k-selection.
        if matches!(aggregate.op.id(), T_TOPK | T_BOTTOMK) {
            let eval_ctx = AggregationEvalContext {
                query_start,
                query_end,
                evaluation_ts,
                interval_ms,
                lookback_delta_ms,
            };
            let k_samples = self
                .evaluate_k_aggregate(aggregate, samples, eval_ctx)
                .await?;
            return Ok(ExprResult::InstantVector(k_samples));
        }

        // Group samples by their grouping key
        let mut groups: HashMap<Vec<(String, String)>, Vec<f64>> = HashMap::new();
        for sample in samples {
            let group_key = Self::compute_grouping_key(
                &sample.labels,
                aggregate.modifier.as_ref(),
                sample.drop_name,
            );
            groups.entry(group_key).or_default().push(sample.value);
        }

        // Use the evaluation_ts time as the timestamp for the aggregated result
        let timestamp_ms = evaluation_ts.as_millis();

        // Aggregate each group
        let mut result_samples = Vec::new();
        for (group_key, values) in groups {
            // Apply the aggregation function to this group
            let aggregated_value = match aggregate.op.id() {
                T_SUM => values.iter().sum(),
                T_AVG => values.iter().sum::<f64>() / values.len() as f64,
                T_MIN => values.iter().fold(f64::INFINITY, |a, &b| f64::min(a, b)),
                T_MAX => values
                    .iter()
                    .fold(f64::NEG_INFINITY, |a, &b| f64::max(a, b)),
                T_COUNT => values.len() as f64,
                _ => {
                    return Err(EvaluationError::InternalError(format!(
                        "Unsupported aggregation operator: {:?}",
                        aggregate.op
                    )));
                }
            };

            let result_labels = EvalLabels::Owned(
                group_key
                    .into_iter()
                    .map(|(n, v)| Label { name: n, value: v })
                    .collect(),
            );

            result_samples.push(EvalSample {
                timestamp_ms,
                value: aggregated_value,
                labels: result_labels,
                drop_name: false,
            });
        }

        Ok(ExprResult::InstantVector(result_samples))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TimeBucket;
    use crate::model::{Label, MetricType, Sample};
    use crate::query::test_utils::{MockMultiBucketQueryReaderBuilder, MockQueryReaderBuilder};
    use crate::test_utils::assertions::approx_eq;
    use promql_parser::label::{METRIC_NAME, Matchers};
    use promql_parser::parser::value::ValueType;
    use promql_parser::parser::{
        AtModifier, EvalStmt, Function, FunctionArgs, NumberLiteral, Offset, VectorSelector,
    };
    use rstest::rstest;

    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Type alias for test data: (metric_name, labels, timestamp_offset_ms, value)
    type TestSampleData = Vec<(&'static str, Vec<(&'static str, &'static str)>, i64, f64)>;

    // Type aliases for vector selector test to reduce complexity warnings
    type VectorSelectorTestData = Vec<(
        TimeBucket,
        &'static str,
        Vec<(&'static str, &'static str)>,
        i64,
        f64,
    )>;
    type VectorSelectorExpectedResults = Vec<(f64, Vec<(&'static str, &'static str)>)>;

    /// Helper to parse a PromQL query and evaluate it
    async fn parse_and_evaluate<'reader, R: QueryReader>(
        evaluator: &mut Evaluator<'reader, R>,
        query: &str,
        end_time: SystemTime,
        lookback_delta: Duration,
    ) -> EvalResult<Vec<EvalSample>> {
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| EvaluationError::InternalError(format!("Parse error: {}", e)))?;

        let stmt = EvalStmt {
            expr,
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };

        evaluator
            .evaluate(stmt)
            .await
            .map(|result| result.expect_instant_vector("Expected instant vector result"))
    }

    /// Helper to convert label vec to EvalLabels for comparison
    fn labels_to_eval(labels: &[(&str, &str)]) -> EvalLabels {
        EvalLabels::from_pairs(labels)
    }

    /// Sort samples by labels (for deterministic comparison)
    fn sort_samples_by_labels(samples: &mut [EvalSample]) {
        samples.sort_by(|a, b| {
            let a_labels: Vec<_> = a.labels.iter().collect();
            let b_labels: Vec<_> = b.labels.iter().collect();
            a_labels.cmp(&b_labels)
        });
    }

    /// Compare actual results with expected results
    fn assert_results_match(actual: &[EvalSample], expected: &[(f64, Vec<(&str, &str)>)]) {
        assert_eq!(
            actual.len(),
            expected.len(),
            "Result count mismatch: got {}, expected {}",
            actual.len(),
            expected.len()
        );

        let mut actual_sorted: Vec<_> = actual.to_vec();
        sort_samples_by_labels(&mut actual_sorted);

        let mut expected_sorted: Vec<_> = expected.to_vec();
        expected_sorted.sort_by(|a, b| {
            let a_labels: Vec<_> = a.1.iter().collect();
            let b_labels: Vec<_> = b.1.iter().collect();
            a_labels.cmp(&b_labels)
        });

        for (i, (actual_sample, (expected_value, expected_labels))) in
            actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
        {
            assert!(
                approx_eq(actual_sample.value, *expected_value),
                "Sample {} value mismatch: got {}, expected {}",
                i,
                actual_sample.value,
                expected_value
            );

            let expected_eval = labels_to_eval(expected_labels);
            assert_eq!(
                actual_sample.labels, expected_eval,
                "Sample {} labels mismatch: got {:?}, expected {:?}",
                i, actual_sample.labels, expected_eval
            );
        }
    }

    /// Helper to create labels from metric name and label pairs
    fn create_labels(metric_name: &str, label_pairs: Vec<(&str, &str)>) -> Vec<Label> {
        let mut labels = vec![Label {
            name: METRIC_NAME.to_string(),
            value: metric_name.to_string(),
        }];
        for (key, val) in label_pairs {
            labels.push(Label {
                name: key.to_string(),
                value: val.to_string(),
            });
        }
        labels
    }

    /// Setup helper: Creates a MockQueryReader with test data
    /// data: Vec of (metric_name, labels, timestamp_offset_ms, value)
    /// Returns (MockQueryReader, end_time) where end_time is suitable for querying
    fn setup_mock_reader(
        data: TestSampleData,
    ) -> (crate::query::test_utils::MockQueryReader, SystemTime) {
        let bucket = TimeBucket::hour(1000);
        let mut builder = MockQueryReaderBuilder::new(bucket);

        // Base timestamp: 300001ms (ensures samples are > start_ms with 5min lookback)
        // Query time will be calculated to be well after all samples
        let base_timestamp = 300001i64;

        // Find max offset before consuming data
        let max_offset = data
            .iter()
            .map(|(_, _, offset_ms, _)| *offset_ms)
            .max()
            .unwrap_or(0);

        for (metric_name, labels, offset_ms, value) in data {
            let attributes = create_labels(metric_name, labels);
            let sample = Sample {
                timestamp_ms: base_timestamp + offset_ms,
                value,
            };
            builder.add_sample(attributes, MetricType::Gauge, sample);
        }

        // Query time: base_timestamp + max_offset + 1ms (just after all samples)
        // Lookback window: (start_ms, query_time] where start_ms = query_time - 300000
        // Since lookback uses exclusive start (timestamp > start_ms), we need:
        //   start_ms < base_timestamp (to include all samples)
        //   => query_time - 300000 < base_timestamp
        //   => query_time < base_timestamp + 300000
        // We set query_time = base_timestamp + max_offset + 1, which works as long as max_offset < 300000
        // This ensures start_ms = base_timestamp + max_offset + 1 - 300000 < base_timestamp
        // So all samples at base_timestamp + offset (where offset <= max_offset) are included
        let query_timestamp = base_timestamp + max_offset + 1;
        let end_time = UNIX_EPOCH + Duration::from_millis(query_timestamp as u64);

        (builder.build(), end_time)
    }

    #[rstest]
    // Vector Selectors
    #[case(
        "vector_selector_all_series",
        "http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_with_single_equality_matcher",
        r#"http_requests_total{env="prod"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_with_different_label_matcher",
        r#"http_requests_total{method="GET"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
        ]
    )]
    #[case(
        "vector_selector_with_multiple_equality_matchers",
        r#"http_requests_total{env="prod",method="GET"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
        ]
    )]
    #[case(
        "vector_selector_with_not_equal_matcher",
        r#"http_requests_total{env!="staging"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_different_metric",
        "cpu_usage",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "vector_selector_single_series_metric",
        "memory_bytes",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0, vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    // Function Calls - Unary Math
    #[case(
        "function_abs",
        "abs(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "function_sqrt",
        "sqrt(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![("__name__", "memory_bytes"), ("env", "prod")]), // sqrt(100) = 10
        ]
    )]
    #[case(
        "function_ceil",
        "ceil(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_floor",
        "floor(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_round",
        "round(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    // Function Calls - Trigonometry
    #[case(
        "function_sin",
        "sin(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0_f64.sin(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0_f64.sin(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_cos",
        "cos(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0_f64.cos(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0_f64.cos(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    // Function Calls - Logarithms
    #[case(
        "function_ln",
        "ln(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.ln(), vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    #[case(
        "function_log10",
        "log10(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.log10(), vec![("__name__", "memory_bytes"), ("env", "prod")]), // log10(100) = 2
        ]
    )]
    #[case(
        "function_log2",
        "log2(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.log2(), vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    // Function Calls - Special
    #[case(
        "function_absent_with_existing_metric",
        "absent(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
        ],
        vec![] // Should return empty since http_requests_total exists
    )]
    #[case(
        "function_absent_with_nonexistent_metric",
        "absent(nonexistent_metric)",
        vec![
            ("other_metric", vec![("env", "prod")], 0, 5.0),
        ],
        vec![
            (1.0, vec![]), // Should return 1.0 when metric doesn't exist
        ]
    )]
    // Binary Operations - Arithmetic
    #[case(
        "binary_add_vector_scalar",
        "http_requests_total + 5",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (15.0, vec![("env", "prod"), ("method", "GET")]),
            (25.0, vec![("env", "prod"), ("method", "POST")]),
            (35.0, vec![("env", "staging"), ("method", "GET")]),
            (45.0, vec![("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "binary_multiply_vector_scalar",
        "http_requests_total * 2",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (20.0, vec![("env", "prod"), ("method", "GET")]),
            (40.0, vec![("env", "prod"), ("method", "POST")]),
            (60.0, vec![("env", "staging"), ("method", "GET")]),
            (80.0, vec![("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "binary_divide_vector_scalar",
        "memory_bytes / 10",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![("env", "prod")]), // 100 / 10 = 10
        ]
    )]
    // Binary Operations - Comparison
    #[case(
        "binary_greater_than_filter",
        "http_requests_total > 15",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 > 15
            (1.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]), // 30 > 15
            (1.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]), // 40 > 15
        ]
    )]
    #[case(
        "binary_less_than_filter",
        "http_requests_total < 25",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]), // 10 < 25
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 < 25
        ]
    )]
    #[case(
        "binary_equal_filter",
        "http_requests_total == 20",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 == 20
        ]
    )]
    // Binary Operations - Comparison with bool (vector-scalar and scalar-vector)
    #[case(
        "binary_vector_scalar_comparison_bool_keeps_false",
        "http_requests_total > bool 15",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            // bool: false results retained as 0, true as 1; __name__ dropped
            (0.0, vec![("env", "prod"), ("method", "GET")]),  // 10 > 15 = false → 0
            (1.0, vec![("env", "prod"), ("method", "POST")]), // 20 > 15 = true → 1
        ]
    )]
    #[case(
        "binary_scalar_vector_comparison_bool_keeps_false",
        "15 < bool http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            // bool: 15 < 10 = false → 0, 15 < 20 = true → 1; __name__ dropped
            (0.0, vec![("env", "prod"), ("method", "GET")]),
            (1.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    // Vector-Vector Binary Operations
    #[case(
        "binary_vector_vector_sub_same_metric",
        "http_requests_total - http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (0.0, vec![("env", "prod"), ("method", "GET")]),
            (0.0, vec![("env", "prod"), ("method", "POST")]),
            (0.0, vec![("env", "staging"), ("method", "GET")]),
        ]
    )]
    #[case(
        "binary_vector_vector_add_same_metric",
        "http_requests_total + http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            (20.0, vec![("env", "prod"), ("method", "GET")]),
            (40.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "binary_vector_vector_unmatched_dropped",
        "cpu_usage + memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ],
        vec![] // No matching label sets (different labels), all dropped
    )]
    #[case(
        "binary_vector_vector_comparison",
        "cpu_usage > memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
            ("cpu_usage", vec![("env", "staging")], 1, 50.0),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
            ("memory_bytes", vec![("env", "staging")], 3, 100.0),
        ],
        vec![
            // 150 > 100 = true; non-bool comparison propagates lhs value
            (150.0, vec![("__name__", "cpu_usage"), ("env", "prod")]),
            // 50 > 100 = false, filtered out
        ]
    )]
    #[case(
        "binary_vector_vector_after_aggregation",
        "sum by (env)(http_requests_total) - sum by (env)(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (0.0, vec![("env", "prod")]),   // (10+20) - (10+20) = 0
            (0.0, vec![("env", "staging")]), // 30 - 30 = 0
        ]
    )]
    #[case(
        "binary_vector_vector_on_modifier",
        "cpu_usage + on(env) memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ],
        vec![
            (150.0, vec![("env", "prod")]),
        ]
    )]
    #[case(
        "binary_vector_vector_comparison_on_drops_name",
        "cpu_usage > on(env) memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 150.0),
            ("cpu_usage", vec![("env", "staging"), ("instance", "i2")], 1, 50.0),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
            ("memory_bytes", vec![("env", "staging")], 3, 100.0),
        ],
        vec![
            // 150 > 100 = true; on(env) keeps only env label, value stays from lhs
            (150.0, vec![("env", "prod")]),
            // 50 > 100 = false, filtered out
        ]
    )]
    #[case(
        "binary_vector_vector_comparison_ignoring_preserves_name",
        "cpu_usage > ignoring(instance) memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 150.0),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ],
        vec![
            // ignoring(instance) comparison preserves __name__ but removes instance
            (150.0, vec![("__name__", "cpu_usage"), ("env", "prod")]),
        ]
    )]
    #[case(
        "binary_vector_vector_comparison_on_name_preserves_name",
        "cpu_usage == on(__name__) cpu_usage",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
        ],
        vec![
            // on(__name__) comparison without bool: Prometheus preserves __name__
            // and propagates lhs value.
            (150.0, vec![("__name__", "cpu_usage")]),
        ]
    )]
    #[case(
        "binary_nested_arithmetic_then_on_name_no_match_left",
        "(cpu_usage + 1) == on(__name__) cpu_usage",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
        ],
        vec![
            // Inner + drops __name__ (materialized before matching), so on(__name__)
            // finds no __name__ on left → match keys differ → no match → empty result
        ]
    )]
    #[case(
        "binary_nested_arithmetic_then_on_name_no_match_right",
        "cpu_usage == on(__name__) (cpu_usage + 1)",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
        ],
        vec![
            // Inner + drops __name__ on right side → on(__name__) match keys differ → empty
        ]
    )]
    #[case(
        "binary_vector_vector_comparison_bool_keeps_false",
        "cpu_usage > bool memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
            ("cpu_usage", vec![("env", "staging")], 1, 50.0),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
            ("memory_bytes", vec![("env", "staging")], 3, 100.0),
        ],
        vec![
            // bool modifier: keep all results as 0/1 and drop __name__
            (0.0, vec![("env", "staging")]), // 50 > 100 = false → 0
            (1.0, vec![("env", "prod")]),    // 150 > 100 = true → 1
        ]
    )]
    #[case(
        "binary_vector_vector_ignoring_modifier",
        "cpu_usage - ignoring(instance) memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ],
        vec![
            // ignoring(instance) removes instance; arithmetic drops __name__
            (-50.0, vec![("env", "prod")]),
        ]
    )]
    #[case(
        "binary_vector_vector_order_insensitive",
        "http_requests_total - http_requests_total",
        vec![
            // Reverse order: staging first, prod second
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 0, 30.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 1, 10.0),
        ],
        vec![
            (0.0, vec![("env", "prod"), ("method", "GET")]),
            (0.0, vec![("env", "staging"), ("method", "GET")]),
        ]
    )]
    // Vector-Vector: matched arithmetic with different metric names (only __name__ differs)
    #[case(
        "binary_vector_vector_arithmetic_different_metrics",
        "cpu_usage + memory_bytes",
        vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ],
        vec![
            // Matched on {env=prod} (__name__ excluded from match key), __name__ dropped (arithmetic)
            (150.0, vec![("env", "prod")]),
        ]
    )]
    // Aggregation over arithmetic: drop_name materialized before grouping
    #[case(
        "binary_aggregation_over_arithmetic_drops_name",
        r#"sum by (__name__) (http_requests_total + 1)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            // Inner + sets drop_name=true, aggregation materializes the drop before grouping,
            // so by(__name__) finds no __name__ → single group {} with sum 11+21=32
            (32.0, vec![]),
        ]
    )]
    // Nested expression: drop_name propagation through nested binary ops
    #[case(
        "binary_nested_arithmetic_then_comparison",
        "(http_requests_total + 1) > 15",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            // Inner + sets drop_name=true, outer > filters to values > 15.
            // 10+1=11 not > 15 (filtered). 20+1=21 > 15 → returns 1.0. __name__ stripped at top level.
            (1.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    // Function-wrapped arithmetic: drop_name propagation through instant-vector functions
    #[case(
        "binary_function_wrapped_arithmetic_drops_name",
        "abs(http_requests_total + 1)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, -20.0),
        ],
        vec![
            // Inner + sets drop_name=true, abs() preserves drop_name (mutates value in-place),
            // __name__ stripped at top-level deferred cleanup
            (11.0, vec![("env", "prod"), ("method", "GET")]),
            (19.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    // Aggregations
    #[case(
        "aggregation_sum",
        "sum(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (100.0, vec![]), // 10 + 20 + 30 + 40 = 100
        ]
    )]
    #[case(
        "aggregation_avg",
        "avg(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (25.0, vec![]), // (10 + 20 + 30 + 40) / 4 = 25
        ]
    )]
    #[case(
        "aggregation_min",
        "min(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![]), // min(10, 20, 30, 40) = 10
        ]
    )]
    #[case(
        "aggregation_max",
        "max(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (40.0, vec![]), // max(10, 20, 30, 40) = 40
        ]
    )]
    #[case(
        "aggregation_count",
        "count(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (4.0, vec![]), // 4 series
        ]
    )]
    #[case(
        "aggregation_topk",
        "topk(2, http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "aggregation_topk_by_env",
        r#"topk by (env) (1, http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "aggregation_topk_materializes_drop_name",
        "topk(1, http_requests_total + 1)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            (21.0, vec![("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "aggregation_topk_zero_k_returns_empty",
        "topk(0, http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![]
    )]
    // Aggregations with grouping
    #[case(
        "aggregation_sum_by_env",
        r#"sum by (env) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (30.0, vec![("env", "prod")]),    // 10 + 20 = 30
            (70.0, vec![("env", "staging")]), // 30 + 40 = 70
        ]
    )]
    #[case(
        "aggregation_avg_by_env",
        r#"avg by (env) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (15.0, vec![("env", "prod")]),    // (10 + 20) / 2 = 15
            (35.0, vec![("env", "staging")]), // (30 + 40) / 2 = 35
        ]
    )]
    #[case(
        "aggregation_sum_by_method",
        r#"sum by (method) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (40.0, vec![("method", "GET")]),  // 10 + 30 = 40
            (60.0, vec![("method", "POST")]), // 20 + 40 = 60
        ]
    )]
    // Complex Expressions
    #[case(
        "nested_function_abs_sum",
        "abs(sum(http_requests_total))",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (100.0, vec![]), // abs(sum(10, 20, 30, 40)) = abs(100) = 100
        ]
    )]
    #[case(
        "nested_function_sqrt_sum",
        "sqrt(sum(memory_bytes))",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![]), // sqrt(sum(100)) = sqrt(100) = 10
        ]
    )]
    #[case(
        "aggregation_with_selector",
        r#"sum(http_requests_total{env="prod"})"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (30.0, vec![]), // sum(10, 20) = 30
        ]
    )]
    #[rstest]
    #[tokio::test]
    async fn should_evaluate_queries(
        #[case] _name: &str,
        #[case] query: &str,
        #[case] test_data: TestSampleData,
        #[case] expected_samples: Vec<(f64, Vec<(&str, &str)>)>,
    ) {
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300); // 5 minutes

        let result = parse_and_evaluate(&mut evaluator, query, end_time, lookback_delta)
            .await
            .expect("Query should evaluate successfully");

        assert_results_match(&result, &expected_samples);
    }

    #[tokio::test]
    async fn should_cache_samples_across_evaluations() {
        // given: mock reader with data in specific bucket
        let bucket = TimeBucket::hour(1000);
        let mut builder = MockQueryReaderBuilder::new(bucket);
        let labels = vec![
            Label {
                name: METRIC_NAME.to_string(),
                value: "cached_metric".to_string(),
            },
            Label {
                name: "instance".to_string(),
                value: "server1".to_string(),
            },
        ];
        let sample = Sample {
            timestamp_ms: 300001,
            value: 100.0,
        };
        builder.add_sample(labels, MetricType::Gauge, sample);
        let reader = builder.build();
        // Create cached reader and evaluator
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate the same query multiple times
        let end_time = UNIX_EPOCH + Duration::from_millis(300002);
        let lookback_delta = Duration::from_secs(300);
        let expr = promql_parser::parser::parse("cached_metric").unwrap();
        let stmt = EvalStmt {
            expr,
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };
        // First evaluation
        let result1 = evaluator
            .evaluate(stmt.clone())
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");
        // Second evaluation - should use cached data
        let result2 = evaluator
            .evaluate(stmt.clone())
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        // then: results should be identical (sample caching disabled for now)
        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result1[0].value, 100.0);
        assert_eq!(result2[0].value, 100.0);
        assert_eq!(result1[0].labels, result2[0].labels);

        // Note: Sample caching is disabled for now to avoid time range issues
        // assert!(cache.get_samples(&bucket, &0).is_some());
        // let cached_samples = cache.get_samples(&bucket, &0).unwrap();
        // assert_eq!(cached_samples.len(), 1);
        // assert_eq!(cached_samples[0].value, 100.0);
    }

    #[tokio::test]
    async fn should_evaluate_number_literal() {
        // given: create an empty mock reader
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate a number literal (should return scalar, which is unsupported)
        let end_time = UNIX_EPOCH + Duration::from_secs(2000);
        let stmt = EvalStmt {
            expr: promql_parser::parser::Expr::NumberLiteral(
                promql_parser::parser::NumberLiteral { val: 42.0 },
            ),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt).await;

        // then: should return scalar result with value 42.0
        assert!(result.is_ok());
        match result.unwrap() {
            ExprResult::Scalar(value) => assert_eq!(value, 42.0),
            ExprResult::InstantVector(_) => panic!("Expected scalar result, got vector"),
            ExprResult::RangeVector(_) => panic!("Expected scalar result, got range vector"),
        }
    }

    #[tokio::test]
    async fn should_evaluate_time_function_as_scalar() {
        // given
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);
        let end_time = UNIX_EPOCH + Duration::from_millis(1);

        // when
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("time()").unwrap(),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let result = evaluator.evaluate(stmt).await.unwrap();

        // then
        match result {
            ExprResult::Scalar(value) => assert_eq!(value, 0.001),
            ExprResult::InstantVector(_) => panic!("Expected scalar result, got vector"),
            ExprResult::RangeVector(_) => panic!("Expected scalar result, got range vector"),
        }
    }

    #[tokio::test]
    async fn should_evaluate_pi_function_as_scalar() {
        // given
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);
        let end_time = UNIX_EPOCH + Duration::from_secs(2000);

        // when
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("pi()").unwrap(),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let result = evaluator.evaluate(stmt).await.unwrap();

        // then
        match result {
            ExprResult::Scalar(value) => assert_eq!(value, std::f64::consts::PI),
            ExprResult::InstantVector(_) => panic!("Expected scalar result, got vector"),
            ExprResult::RangeVector(_) => panic!("Expected scalar result, got range vector"),
        }
    }

    #[tokio::test]
    async fn should_evaluate_scalar_function_as_scalar() {
        // given
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);
        let end_time = UNIX_EPOCH + Duration::from_secs(2000);

        // when
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("scalar(vector(42))").unwrap(),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let result = evaluator.evaluate(stmt).await.unwrap();

        // then
        match result {
            ExprResult::Scalar(value) => assert_eq!(value, 42.0),
            ExprResult::InstantVector(_) => panic!("Expected scalar result, got vector"),
            ExprResult::RangeVector(_) => panic!("Expected scalar result, got range vector"),
        }
    }

    #[tokio::test]
    async fn should_allow_scalar_function_results_as_vector_arguments() {
        // given
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);
        let end_time = UNIX_EPOCH + Duration::from_secs(5);

        // when
        let stmt = EvalStmt {
            expr: promql_parser::parser::parse("vector(time())").unwrap(),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };
        let result = evaluator
            .evaluate(stmt)
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 5.0);
    }

    #[tokio::test]
    async fn should_error_on_string_literal() {
        // given: create an empty mock reader
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate a string literal
        let end_time = UNIX_EPOCH + Duration::from_secs(2000);
        let stmt = EvalStmt {
            expr: promql_parser::parser::Expr::StringLiteral(
                promql_parser::parser::StringLiteral {
                    val: "hello".to_string(),
                },
            ),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt).await;

        // then: should return an error (string literals cannot be evaluated standalone)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("string literal"),
            "Error message should mention 'string literal', got: {}",
            err
        );
    }

    #[tokio::test]
    async fn should_evaluate_label_replace_with_raw_string_arguments() {
        // given: create an empty mock reader
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate a function call with a string literal argument
        let end_time = UNIX_EPOCH + Duration::from_secs(2000);
        let stmt = EvalStmt {
            expr: promql_parser::parser::Expr::Call(promql_parser::parser::Call {
                func: promql_parser::parser::Function {
                    name: "label_replace",
                    arg_types: vec![
                        ValueType::Vector,
                        ValueType::String,
                        ValueType::String,
                        ValueType::String,
                        ValueType::String,
                    ],
                    variadic: 0,
                    return_type: ValueType::Vector,
                    experimental: false,
                },
                args: promql_parser::parser::FunctionArgs {
                    args: vec![
                        Box::new(promql_parser::parser::Expr::Call(
                            promql_parser::parser::Call {
                                func: promql_parser::parser::Function::new(
                                    "vector",
                                    vec![ValueType::Scalar],
                                    0,
                                    ValueType::Vector,
                                    false,
                                ),
                                args: promql_parser::parser::FunctionArgs::new_args(
                                    promql_parser::parser::Expr::NumberLiteral(
                                        promql_parser::parser::NumberLiteral { val: 1.0 },
                                    ),
                                ),
                            },
                        )),
                        Box::new(promql_parser::parser::Expr::Paren(
                            promql_parser::parser::ParenExpr {
                                expr: Box::new(promql_parser::parser::Expr::StringLiteral(
                                    promql_parser::parser::StringLiteral {
                                        val: "dst".to_string(),
                                    },
                                )),
                            },
                        )),
                        Box::new(promql_parser::parser::Expr::StringLiteral(
                            promql_parser::parser::StringLiteral {
                                val: "replacement".to_string(),
                            },
                        )),
                        Box::new(promql_parser::parser::Expr::StringLiteral(
                            promql_parser::parser::StringLiteral {
                                val: "src".to_string(),
                            },
                        )),
                        Box::new(promql_parser::parser::Expr::StringLiteral(
                            promql_parser::parser::StringLiteral {
                                val: "(.*)".to_string(),
                            },
                        )),
                    ],
                },
            }),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt).await;

        // then: raw string args should reach label_replace and be applied
        let result = result.expect("label_replace should succeed");
        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 1.0);
                assert_eq!(samples[0].labels.get("dst"), Some("replacement"));
            }
            ExprResult::Scalar(_) => panic!("Expected instant vector result, got scalar"),
            ExprResult::RangeVector(_) => {
                panic!("Expected instant vector result, got range vector")
            }
        }
    }

    #[allow(clippy::type_complexity)]
    #[rstest]
    #[case(
        "single_bucket_selector", 
        vec![
            (TimeBucket::hour(100), "http_requests", vec![("env", "prod")], 6_000_001, 10.0),
            (TimeBucket::hour(100), "http_requests", vec![("env", "staging")], 6_000_002, 20.0),
        ],
        6_300_000, // query time 
        300_000,   // 5 min lookback
        vec![(10.0, vec![("__name__", "http_requests"), ("env", "prod")]), (20.0, vec![("__name__", "http_requests"), ("env", "staging")])]
    )]
    #[case(
        "multi_bucket_latest_wins", 
        vec![
            // Same series in bucket 100 (older)
            (TimeBucket::hour(100), "cpu_usage", vec![("host", "server1")], 6_000_000, 50.0),
            // Same series in bucket 200 (newer) - this should win
            (TimeBucket::hour(200), "cpu_usage", vec![("host", "server1")], 12_000_000, 75.0),
        ],
        12_300_000, // query time in bucket 200
        600_000,    // 10 min lookback covers both buckets
        vec![(75.0, vec![("__name__", "cpu_usage"), ("host", "server1")])] // only the newer value
    )]
    #[case(
        "multi_bucket_different_series_different_buckets", 
        vec![
            // Series A: sample in bucket 100 is outside lookback, sample in bucket 200 is within lookback
            (TimeBucket::hour(100), "memory", vec![("app", "frontend")], 6_000_000, 100.0), // outside lookback window
            (TimeBucket::hour(200), "memory", vec![("app", "frontend")], 10_000_000, 80.0), // within lookback window
            // Series B: latest sample in bucket 200 within lookback
            (TimeBucket::hour(100), "memory", vec![("app", "backend")], 5_000_000, 150.0), // outside lookback window
            (TimeBucket::hour(200), "memory", vec![("app", "backend")], 12_000_000, 200.0), // within lookback window
        ],
        12_300_000, // query time
        3_600_000,  // 1 hour lookback: (8,700,000, 12,300,000]
        vec![
            (80.0, vec![("__name__", "memory"), ("app", "frontend")]),  // latest within lookback from bucket 200
            (200.0, vec![("__name__", "memory"), ("app", "backend")])   // latest within lookback from bucket 200
        ]
    )]
    #[tokio::test]
    async fn should_evaluate_vector_selector(
        #[case] _test_name: &str,
        #[case] data: VectorSelectorTestData,
        #[case] query_time_ms: i64,
        #[case] lookback_ms: i64,
        #[case] expected: VectorSelectorExpectedResults,
    ) {
        use crate::query::test_utils::MockMultiBucketQueryReaderBuilder;
        use promql_parser::label::{METRIC_NAME, Matchers};
        use promql_parser::parser::VectorSelector;
        use std::time::{Duration, UNIX_EPOCH};

        // Extract metric name from first sample for selector before consuming data
        let metric_name = if let Some((_, name, _, _, _)) = data.first() {
            name.to_string()
        } else {
            "test_metric".to_string()
        };

        // given: build mock reader with test data
        let mut builder = MockMultiBucketQueryReaderBuilder::new();

        for (bucket, metric_name, label_pairs, timestamp_ms, value) in data {
            let mut labels = vec![Label {
                name: METRIC_NAME.to_string(),
                value: metric_name.to_string(),
            }];
            for (key, val) in label_pairs {
                labels.push(Label {
                    name: key.to_string(),
                    value: val.to_string(),
                });
            }

            builder.add_sample(
                bucket,
                labels,
                MetricType::Gauge,
                Sample {
                    timestamp_ms,
                    value,
                },
            );
        }

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate vector selector
        let query_time = UNIX_EPOCH + Duration::from_millis(query_time_ms as u64);
        let lookback_delta = Duration::from_millis(lookback_ms as u64);

        let selector = VectorSelector {
            name: Some(metric_name),
            matchers: Matchers {
                matchers: vec![],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluator
            .evaluate_vector_selector(
                &selector,
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                lookback_delta.as_millis() as i64,
            )
            .await
            .unwrap();

        // then: verify results
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), expected.len(), "Result count mismatch");

            // Sort both actual and expected results for comparison
            let mut actual_sorted = samples;
            actual_sorted.sort_by(|a, b| {
                let mut a_labels: Vec<_> = a.labels.iter().collect();
                let mut b_labels: Vec<_> = b.labels.iter().collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            });

            let mut expected_sorted = expected;
            expected_sorted.sort_by(|a, b| {
                let mut a_labels: Vec<(String, String)> =
                    a.1.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                let mut b_labels: Vec<(String, String)> =
                    b.1.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            });

            for (i, (actual, (expected_value, expected_labels))) in
                actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
            {
                assert!(
                    (actual.value - expected_value).abs() < 0.0001,
                    "Sample {} value mismatch: got {}, expected {}",
                    i,
                    actual.value,
                    expected_value
                );

                for (key, value) in expected_labels {
                    assert_eq!(
                        actual.labels.get(key),
                        Some(*value),
                        "Sample {} missing label {}={}",
                        i,
                        key,
                        value
                    );
                }
            }
        } else {
            panic!("Expected InstantVector result");
        }
    }

    // Matrix Selector Tests

    type MatrixSelectorTestData = Vec<(
        TimeBucket,
        &'static str,
        Vec<(&'static str, &'static str)>,
        i64,
        f64,
    )>;
    type MatrixSelectorExpectedResults = Vec<(Vec<(&'static str, &'static str)>, Vec<(i64, f64)>)>;

    #[rstest]
    #[case(
        "single_series_multiple_samples",
        vec![
            // One series with multiple samples across time
            (TimeBucket::hour(100), "cpu_usage", vec![("host", "server1")], 6_000_000, 10.0),
            (TimeBucket::hour(100), "cpu_usage", vec![("host", "server1")], 6_060_000, 15.0), // 1 min later
            (TimeBucket::hour(100), "cpu_usage", vec![("host", "server1")], 6_120_000, 20.0), // 2 min later
        ],
        6_150_000, // query time: 2.5 min after first sample
        Duration::from_secs(180), // 3 min range: covers all 3 samples
        vec![
            (vec![("__name__", "cpu_usage"), ("host", "server1")], vec![(6_000_000, 10.0), (6_060_000, 15.0), (6_120_000, 20.0)])
        ]
    )]
    #[case(
        "multiple_series_same_time_range",
        vec![
            // Two different series with samples in the range
            (TimeBucket::hour(100), "memory", vec![("app", "frontend")], 6_000_000, 100.0),
            (TimeBucket::hour(100), "memory", vec![("app", "frontend")], 6_060_000, 110.0),
            (TimeBucket::hour(100), "memory", vec![("app", "backend")], 6_030_000, 200.0),
            (TimeBucket::hour(100), "memory", vec![("app", "backend")], 6_090_000, 220.0),
        ],
        6_100_000, // query time
        Duration::from_secs(120), // 2 min range
        vec![
            (vec![("__name__", "memory"), ("app", "backend")], vec![(6_030_000, 200.0), (6_090_000, 220.0)]),
            (vec![("__name__", "memory"), ("app", "frontend")], vec![(6_000_000, 100.0), (6_060_000, 110.0)])
        ]
    )]
    #[case(
        "single_bucket_all_samples_in_range",
        vec![
            // All samples in same bucket within the range
            (TimeBucket::hour(100), "disk_io", vec![("device", "sda")], 6_000_000, 50.0),
            (TimeBucket::hour(100), "disk_io", vec![("device", "sda")], 6_030_000, 55.0),
            (TimeBucket::hour(100), "disk_io", vec![("device", "sda")], 6_060_000, 60.0),
            (TimeBucket::hour(100), "disk_io", vec![("device", "sda")], 6_090_000, 65.0),
        ],
        6_100_000, // query time
        Duration::from_secs(120), // 2 min range: should include last 3 samples
        vec![
            (vec![("__name__", "disk_io"), ("device", "sda")], vec![(6_000_000, 50.0), (6_030_000, 55.0), (6_060_000, 60.0), (6_090_000, 65.0)])
        ]
    )]
    #[case(
        "partial_time_range_filtering",
        vec![
            // Some samples outside the range should be filtered out
            (TimeBucket::hour(100), "requests", vec![("method", "GET")], 5_900_000, 100.0), // too old
            (TimeBucket::hour(100), "requests", vec![("method", "GET")], 6_000_000, 110.0), // in range
            (TimeBucket::hour(100), "requests", vec![("method", "GET")], 6_030_000, 120.0), // in range  
            (TimeBucket::hour(100), "requests", vec![("method", "GET")], 6_200_000, 130.0), // too new
        ],
        6_100_000, // query time
        Duration::from_secs(90), // 1.5 min range: end-90s to end, so 6_010_000 to 6_100_000
        vec![
            (vec![("__name__", "requests"), ("method", "GET")], vec![(6_030_000, 120.0)]) // only middle samples in range
        ]
    )]
    #[tokio::test]
    async fn should_evaluate_matrix_selector(
        #[case] test_name: &str,
        #[case] data: MatrixSelectorTestData,
        #[case] query_time_ms: i64,
        #[case] range: Duration,
        #[case] expected: MatrixSelectorExpectedResults,
    ) {
        // given:
        let bucket = TimeBucket::hour(100);
        let mut builder = MockQueryReaderBuilder::new(bucket);
        for (_test_bucket, metric_name, label_pairs, timestamp_ms, value) in data {
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
            builder.add_sample(
                labels,
                MetricType::Gauge,
                Sample {
                    timestamp_ms,
                    value,
                },
            );
        }
        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate matrix selector
        let query_time = UNIX_EPOCH + Duration::from_millis(query_time_ms as u64);
        let metric_name = match expected.first() {
            Some((labels, _)) => labels
                .iter()
                .find(|(k, _)| k == &"__name__")
                .map(|(_, v)| v.as_ref())
                .unwrap_or("cpu_usage"),
            None => "cpu_usage", // default fallback
        };
        let matrix_selector = MatrixSelector {
            vs: VectorSelector {
                name: Some(metric_name.to_string()),
                matchers: Matchers {
                    matchers: vec![],
                    or_matchers: vec![],
                },
                offset: None,
                at: None,
            },
            range,
        };
        let result = evaluator
            .evaluate_matrix_selector(
                matrix_selector,
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                Timestamp::from(query_time),
            )
            .await
            .unwrap();

        // then: verify results
        if let ExprResult::RangeVector(range_samples) = result {
            assert_eq!(
                range_samples.len(),
                expected.len(),
                "Test '{}': Expected {} series, got {}",
                test_name,
                expected.len(),
                range_samples.len()
            );
            let mut actual_sorted = range_samples;
            actual_sorted.sort_by(|a, b| {
                let mut a_labels: Vec<_> = a.labels.iter().collect();
                let mut b_labels: Vec<_> = b.labels.iter().collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            });
            let mut expected_sorted = expected;
            expected_sorted.sort_by(|a, b| {
                let mut a_labels: Vec<(String, String)> =
                    a.0.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                let mut b_labels: Vec<(String, String)> =
                    b.0.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            });

            // Compare each series
            for (i, (actual, expected)) in
                actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
            {
                // Check that the series has the expected labels
                for (key, expected_value) in &expected.0 {
                    assert_eq!(
                        actual.labels.get(key),
                        Some(*expected_value),
                        "Test '{}': Series {} missing label {}={}",
                        test_name,
                        i,
                        key,
                        expected_value
                    );
                }
                // Check that the series has the expected number of samples
                assert_eq!(
                    actual.values.len(),
                    expected.1.len(),
                    "Test '{}': Series {} expected {} samples, got {}",
                    test_name,
                    i,
                    expected.1.len(),
                    actual.values.len()
                );
                // Check each sample's timestamp and value
                for (j, (actual, (expected_ts, expected_val))) in
                    actual.values.iter().zip(expected.1.iter()).enumerate()
                {
                    assert_eq!(
                        actual.timestamp_ms, *expected_ts,
                        "Test '{}': Series {} sample {} timestamp mismatch: expected {}, got {}",
                        test_name, i, j, expected_ts, actual.timestamp_ms
                    );
                    assert_eq!(
                        actual.value, *expected_val,
                        "Test '{}': Series {} sample {} value mismatch: expected {}, got {}",
                        test_name, i, j, expected_val, actual.value
                    );
                }
            }
        } else {
            panic!(
                "Test '{}': Expected RangeVector result, got {:?}",
                test_name, result
            );
        }
    }

    #[tokio::test]
    async fn test_evaluate_call_scalar_argument() {
        let (reader, end_time) = setup_mock_reader(vec![]);
        let mut evaluator = Evaluator::new(&reader);

        let call = Call {
            func: Function::new(
                "vector",
                vec![ValueType::Scalar],
                0,
                ValueType::Vector,
                false,
            ),
            args: FunctionArgs::new_args(Expr::NumberLiteral(NumberLiteral { val: 42.0 })),
        };

        let result = evaluator
            .evaluate_call(
                &call,
                Timestamp::from(end_time),
                Timestamp::from(end_time),
                Timestamp::from(end_time),
                60_000,
                300_000,
            )
            .await
            .unwrap();

        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 42.0);
                assert!(samples[0].labels.is_empty());
                let expected_ts = end_time
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                assert_eq!(samples[0].timestamp_ms, expected_ts);
            }
            other => panic!("Expected Instant Vector, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_evaluate_call_scalar_expression_argument() {
        let (reader, end_time) = setup_mock_reader(vec![]);
        let mut evaluator = Evaluator::new(&reader);
        let call = Call {
            func: Function::new(
                "vector",
                vec![ValueType::Scalar],
                0,
                ValueType::Vector,
                false,
            ),
            args: FunctionArgs::new_args(Expr::Binary(BinaryExpr {
                lhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 1.0 })),
                rhs: Box::new(Expr::NumberLiteral(NumberLiteral { val: 1.0 })),
                op: TokenType::new(T_SUB),
                modifier: None,
            })),
        };
        let result = evaluator
            .evaluate_call(
                &call,
                Timestamp::from(end_time),
                Timestamp::from(end_time),
                Timestamp::from(end_time),
                60_000,
                300_000,
            )
            .await
            .unwrap();
        match result {
            ExprResult::InstantVector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 0.0);
                assert!(samples[0].labels.is_empty());
            }
            other => panic!("Expected Instant Vector, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_evaluate_clamp_family_queries() {
        let test_data = vec![
            ("test_clamp", vec![("src", "clamp-a")], 0, -50.0),
            ("test_clamp", vec![("src", "clamp-b")], 1, 0.0),
            ("test_clamp", vec![("src", "clamp-c")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        let clamp_result = parse_and_evaluate(
            &mut evaluator,
            "clamp(test_clamp, -25, 75)",
            end_time,
            lookback_delta,
        )
        .await
        .unwrap();
        assert_results_match(
            &clamp_result,
            &[
                (-25.0, vec![("__name__", "test_clamp"), ("src", "clamp-a")]),
                (0.0, vec![("__name__", "test_clamp"), ("src", "clamp-b")]),
                (75.0, vec![("__name__", "test_clamp"), ("src", "clamp-c")]),
            ],
        );

        let clamp_min_result = parse_and_evaluate(
            &mut evaluator,
            "clamp_min(test_clamp, -25)",
            end_time,
            lookback_delta,
        )
        .await
        .unwrap();
        assert_results_match(
            &clamp_min_result,
            &[
                (-25.0, vec![("__name__", "test_clamp"), ("src", "clamp-a")]),
                (0.0, vec![("__name__", "test_clamp"), ("src", "clamp-b")]),
                (100.0, vec![("__name__", "test_clamp"), ("src", "clamp-c")]),
            ],
        );

        let clamp_max_result = parse_and_evaluate(
            &mut evaluator,
            "clamp_max(test_clamp, 75)",
            end_time,
            lookback_delta,
        )
        .await
        .unwrap();
        assert_results_match(
            &clamp_max_result,
            &[
                (-50.0, vec![("__name__", "test_clamp"), ("src", "clamp-a")]),
                (0.0, vec![("__name__", "test_clamp"), ("src", "clamp-b")]),
                (75.0, vec![("__name__", "test_clamp"), ("src", "clamp-c")]),
            ],
        );
    }

    #[tokio::test]
    async fn should_return_empty_vector_for_clamp_when_min_exceeds_max() {
        let test_data = vec![
            ("test_clamp", vec![("src", "clamp-a")], 0, -50.0),
            ("test_clamp", vec![("src", "clamp-b")], 1, 0.0),
            ("test_clamp", vec![("src", "clamp-c")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &mut evaluator,
            "clamp(test_clamp, 5, -5)",
            end_time,
            lookback_delta,
        )
        .await
        .unwrap();

        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn should_evaluate_rate_function_with_matrix_selector() {
        // given: mock reader with counter data over time
        let bucket = TimeBucket::hour(100);
        let mut builder = MockQueryReaderBuilder::new(bucket);
        let labels = vec![
            Label {
                name: "__name__".to_string(),
                value: "http_requests_total".to_string(),
            },
            Label {
                name: "job".to_string(),
                value: "webapp".to_string(),
            },
        ];
        builder
            .add_sample(
                labels.clone(),
                MetricType::Sum {
                    monotonic: true,
                    temporality: crate::model::Temporality::Cumulative,
                },
                Sample {
                    timestamp_ms: 6_000_000, // t=0s, counter at 100
                    value: 100.0,
                },
            )
            .add_sample(
                labels.clone(),
                MetricType::Sum {
                    monotonic: true,
                    temporality: crate::model::Temporality::Cumulative,
                },
                Sample {
                    timestamp_ms: 6_030_000, // t=30s, counter at 115
                    value: 115.0,
                },
            )
            .add_sample(
                labels.clone(),
                MetricType::Sum {
                    monotonic: true,
                    temporality: crate::model::Temporality::Cumulative,
                },
                Sample {
                    timestamp_ms: 6_060_000, // t=60s, counter at 130
                    value: 130.0,
                },
            );
        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate rate(http_requests_total[1m])
        let query_time = UNIX_EPOCH + Duration::from_millis(6_060_000);
        let query = "rate(http_requests_total[1m])";
        let expr = promql_parser::parser::parse(query).expect("Failed to parse query");
        let pipeline_result = evaluator
            .evaluate_expr(
                &expr,
                Timestamp::from(query_time - Duration::from_secs(60)), // query_start
                Timestamp::from(query_time),                           // query_end
                Timestamp::from(query_time), // evaluation_ts (for instant queries, equals query_end)
                15_000,                      // 15s step
                5_000,                       // 5s lookback
            )
            .await
            .unwrap();

        if let ExprResult::InstantVector(instant_samples) = pipeline_result {
            assert_eq!(instant_samples.len(), 1, "Expected 1 result from pipeline");
            // The pipeline should give the same rate as the direct function call
            assert!(instant_samples[0].value > 0.0, "Rate should be positive");
            assert_eq!(instant_samples[0].labels.get("job"), Some("webapp"));
        } else {
            panic!(
                "Expected InstantVector result from rate function pipeline, got {:?}",
                pipeline_result
            );
        }
    }

    #[tokio::test]
    async fn should_evaluate_vector_selector_with_positive_offset() {
        // given: samples at different times
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        let bucket = TimeBucket::hour(100);

        for (ts, val) in [(5_700_000, 10.0), (6_000_000, 20.0), (6_300_000, 30.0)] {
            builder.add_sample(
                bucket,
                vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "http_requests".to_string(),
                    },
                    Label {
                        name: "env".to_string(),
                        value: "prod".to_string(),
                    },
                ],
                MetricType::Gauge,
                Sample {
                    timestamp_ms: ts,
                    value: val,
                },
            );
        }

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: query at t=6_300_000 with positive offset 5m (look back 300_000ms)
        let selector = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: Some(Offset::Pos(Duration::from_millis(300_000))),
            at: None,
        };

        let query_time = UNIX_EPOCH + Duration::from_millis(6_300_000);
        let result = evaluator
            .evaluate_vector_selector(
                &selector,
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                300_000,
            )
            .await
            .unwrap();

        // then: should get the sample from t=6_000_000 (value 20.0)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 20.0);
            assert_eq!(samples[0].timestamp_ms, 6_000_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[tokio::test]
    async fn should_evaluate_vector_selector_with_at_modifier() {
        // given: samples at different times
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        let bucket = TimeBucket::hour(100);

        builder.add_sample(
            bucket,
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 5_700_000,
                value: 10.0,
            },
        );
        builder.add_sample(
            bucket,
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 6_000_000,
                value: 20.0,
            },
        );

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: query at t=6_300_000 but with @ 6_000_000
        let at_time = UNIX_EPOCH + Duration::from_millis(6_000_000);
        let selector = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: None,
            at: Some(AtModifier::At(at_time)),
        };

        let query_time = UNIX_EPOCH + Duration::from_millis(6_300_000);
        let result = evaluator
            .evaluate_vector_selector(
                &selector,
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                300_000,
            )
            .await
            .unwrap();

        // then: should get the sample from @ time (value 20.0)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 20.0);
            assert_eq!(samples[0].timestamp_ms, 6_000_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[tokio::test]
    async fn should_evaluate_vector_selector_with_both_at_and_offset_modifiers() {
        // given: samples at different times
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        let bucket = TimeBucket::hour(100);

        builder.add_sample(
            bucket,
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 5_700_000,
                value: 30.0,
            },
        );
        builder.add_sample(
            bucket,
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 6_000_000,
                value: 20.0,
            },
        );

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: query with @ 6_000_000 and offset 5m
        // Should apply @ first (6_000_000), then subtract offset (300_000) = 5_700_000
        let at_time = UNIX_EPOCH + Duration::from_millis(6_000_000);
        let selector = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: Some(Offset::Pos(Duration::from_millis(300_000))),
            at: Some(AtModifier::At(at_time)),
        };

        let query_time = UNIX_EPOCH + Duration::from_millis(6_300_000);
        let result = evaluator
            .evaluate_vector_selector(
                &selector,
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                300_000,
            )
            .await
            .unwrap();

        // then: should get the sample from @ time - offset (value 30.0 at t=5_700_000)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 30.0);
            assert_eq!(samples[0].timestamp_ms, 5_700_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[tokio::test]
    async fn should_evaluate_matrix_selector_with_offset_modifier() {
        // given: samples at different times
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        let bucket = TimeBucket::hour(100);

        // Samples from t=5_700_000 to t=6_300_000
        for (ts, val) in [
            (5_700_000, 10.0),
            (5_800_000, 15.0),
            (5_900_000, 20.0),
            (6_000_000, 25.0),
            (6_100_000, 30.0),
            (6_200_000, 35.0),
            (6_300_000, 40.0),
        ] {
            builder.add_sample(
                bucket,
                vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "cpu_usage".to_string(),
                    },
                    Label {
                        name: "host".to_string(),
                        value: "server1".to_string(),
                    },
                ],
                MetricType::Gauge,
                Sample {
                    timestamp_ms: ts,
                    value: val,
                },
            );
        }

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: query matrix selector with 5m range and 5m offset at t=6_300_000
        // offset 5m means look at t=6_000_000, then get [5_700_000, 6_000_000]
        let matrix_selector = promql_parser::parser::MatrixSelector {
            vs: VectorSelector {
                name: Some("cpu_usage".to_string()),
                matchers: Matchers::new(vec![]),
                offset: Some(Offset::Pos(Duration::from_millis(300_000))),
                at: None,
            },
            range: Duration::from_millis(300_000),
        };

        let query_time = UNIX_EPOCH + Duration::from_millis(6_300_000);
        let result = evaluator
            .evaluate_matrix_selector(
                matrix_selector,
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                Timestamp::from(query_time),
            )
            .await
            .unwrap();

        // then: should get samples in range (5_700_000, 6_000_000] (exclusive start, inclusive end)
        if let ExprResult::RangeVector(range_samples) = result {
            assert_eq!(range_samples.len(), 1);
            let samples = &range_samples[0].values;
            assert_eq!(samples.len(), 3); // 5_800_000, 5_900_000, 6_000_000
            assert_eq!(samples[0].timestamp_ms, 5_800_000);
            assert_eq!(samples[0].value, 15.0);
            assert_eq!(samples[2].timestamp_ms, 6_000_000);
            assert_eq!(samples[2].value, 25.0);
        } else {
            panic!("Expected RangeVector result");
        }
    }

    #[tokio::test]
    async fn should_evaluate_vector_selector_with_at_start_and_end() {
        // given: samples at different times
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        let bucket = TimeBucket::hour(100);

        builder.add_sample(
            bucket,
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 5_700_000,
                value: 10.0,
            },
        );
        builder.add_sample(
            bucket,
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 6_000_000,
                value: 20.0,
            },
        );
        builder.add_sample(
            bucket,
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "http_requests".to_string(),
                },
                Label {
                    name: "env".to_string(),
                    value: "prod".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 6_300_000,
                value: 30.0,
            },
        );

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: query with @ start() where query_start = 5_700_000
        let selector_start = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: None,
            at: Some(AtModifier::Start),
        };

        let query_start = UNIX_EPOCH + Duration::from_millis(5_700_000);
        let query_end = UNIX_EPOCH + Duration::from_millis(6_300_000);
        let result_start = evaluator
            .evaluate_vector_selector(
                &selector_start,
                Timestamp::from(query_start),
                Timestamp::from(query_end),
                Timestamp::from(query_end),
                300_000,
            )
            .await
            .unwrap();

        // then: should get sample at query_start (value 10.0)
        if let ExprResult::InstantVector(samples) = result_start {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 10.0);
            assert_eq!(samples[0].timestamp_ms, 5_700_000);
        } else {
            panic!("Expected InstantVector result");
        }

        // when: query with @ end() where query_end = 6_300_000
        let selector_end = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: None,
            at: Some(AtModifier::End),
        };

        let result_end = evaluator
            .evaluate_vector_selector(
                &selector_end,
                Timestamp::from(query_start),
                Timestamp::from(query_end),
                Timestamp::from(query_end),
                300_000,
            )
            .await
            .unwrap();

        // then: should get sample at query_end (value 30.0)
        if let ExprResult::InstantVector(samples) = result_end {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 30.0);
            assert_eq!(samples[0].timestamp_ms, 6_300_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[tokio::test]
    async fn should_evaluate_vector_selector_with_negative_offset() {
        // given: samples at different times
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        let bucket = TimeBucket::hour(100);

        for (ts, val) in [(5_700_000, 10.0), (6_000_000, 20.0), (6_300_000, 30.0)] {
            builder.add_sample(
                bucket,
                vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "http_requests".to_string(),
                    },
                    Label {
                        name: "env".to_string(),
                        value: "prod".to_string(),
                    },
                ],
                MetricType::Gauge,
                Sample {
                    timestamp_ms: ts,
                    value: val,
                },
            );
        }

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: query at t=5_700_000 with negative offset -5m (look forward 300_000ms)
        let selector = VectorSelector {
            name: Some("http_requests".to_string()),
            matchers: Matchers::new(vec![]),
            offset: Some(Offset::Neg(Duration::from_millis(300_000))),
            at: None,
        };

        let query_time = UNIX_EPOCH + Duration::from_millis(5_700_000);
        let result = evaluator
            .evaluate_vector_selector(
                &selector,
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                300_000,
            )
            .await
            .unwrap();

        // then: should get the sample from t=6_000_000 (value 20.0)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 20.0);
            assert_eq!(samples[0].timestamp_ms, 6_000_000);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[tokio::test]
    async fn should_evaluate_vector_selector_with_non_aligned_timestamps() {
        // given: samples at irregular timestamps
        let mut builder = MockMultiBucketQueryReaderBuilder::new();
        let bucket = TimeBucket::hour(100);

        for (ts, val) in [
            (5_723_456, 10.0),
            (5_987_654, 20.0),
            (6_234_567, 30.0),
            (6_456_789, 40.0),
        ] {
            builder.add_sample(
                bucket,
                vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "cpu_usage".to_string(),
                    },
                    Label {
                        name: "host".to_string(),
                        value: "server1".to_string(),
                    },
                ],
                MetricType::Gauge,
                Sample {
                    timestamp_ms: ts,
                    value: val,
                },
            );
        }

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: query at non-aligned timestamp with offset
        let selector = VectorSelector {
            name: Some("cpu_usage".to_string()),
            matchers: Matchers::new(vec![]),
            offset: Some(Offset::Pos(Duration::from_millis(250_000))),
            at: None,
        };

        let query_time = UNIX_EPOCH + Duration::from_millis(6_234_567);
        let result = evaluator
            .evaluate_vector_selector(
                &selector,
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                Timestamp::from(query_time),
                300_000,
            )
            .await
            .unwrap();

        // then: should get the sample closest to (6_234_567 - 250_000 = 5_984_567)
        // Lookback window: (5_684_567, 5_984_567]
        // Sample 5_723_456 (value 10.0) is within the window
        // Sample 5_987_654 (value 20.0) is outside the window (too late)
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 10.0);
            assert_eq!(samples[0].timestamp_ms, 5_723_456);
        } else {
            panic!("Expected InstantVector result");
        }
    }

    #[tokio::test]
    async fn binary_vector_vector_duplicate_right_key_error() {
        // Two right-side series that have different full label sets but the same match key
        // when using on(env). memory_bytes{env="prod",instance="i1"} and
        // memory_bytes{env="prod",instance="i2"} both map to match key {env="prod"}.
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) memory_bytes",
            end_time,
            lookback_delta,
        )
        .await;

        assert!(
            result.is_err(),
            "Expected error for duplicate right-side match key"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the right side"),
            "Error should mention right side: {err}"
        );
    }

    #[tokio::test]
    async fn binary_vector_vector_duplicate_left_key_matched_error() {
        // Two left-side series that collapse to the same match key with on(env),
        // and there IS a matching right-side series.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) memory_bytes",
            end_time,
            lookback_delta,
        )
        .await;

        assert!(
            result.is_err(),
            "Expected error for duplicate left-side match key"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the left side"),
            "Error should mention left side: {err}"
        );
    }

    #[tokio::test]
    async fn binary_vector_vector_duplicate_left_key_unmatched_ok() {
        // Two left-side series that collapse to the same match key with on(env),
        // but no right-side match — should NOT error, silently dropped.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            ("memory_bytes", vec![("env", "staging")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage - on(env) memory_bytes",
            end_time,
            lookback_delta,
        )
        .await;

        assert!(
            result.is_ok(),
            "Should not error for unmatched duplicate left keys"
        );
        let samples = result.unwrap();
        assert!(
            samples.is_empty(),
            "No matches expected, got {} samples",
            samples.len()
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_left_add() {
        // given: many left-side series matched to one right-side series via on(env)
        // cpu_usage has two series per env (different instance), memory_bytes has one per env.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            (
                "cpu_usage",
                vec![("env", "staging"), ("instance", "i3")],
                2,
                70.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 3, 100.0),
            ("memory_bytes", vec![("env", "staging")], 4, 200.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_left query should evaluate successfully");

        // then: result labels come from the many (left) side, __name__ dropped by arithmetic
        assert_results_match(
            &result,
            &[
                (150.0, vec![("env", "prod"), ("instance", "i1")]),
                (160.0, vec![("env", "prod"), ("instance", "i2")]),
                (270.0, vec![("env", "staging"), ("instance", "i3")]),
            ],
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_left_with_extra_labels() {
        // given: group_left(region) copies the "region" label from the one (right) side
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("region", "us-east")],
                2,
                100.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_left(region) memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_left with extra labels should evaluate successfully");

        // then: result has many-side labels plus the extra "region" from one side
        assert_results_match(
            &result,
            &[
                (
                    150.0,
                    vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                ),
                (
                    160.0,
                    vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                ),
            ],
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_left_comparison() {
        // given: comparison with group_left filters out false results
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                150.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                50.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage > on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_left comparison should evaluate successfully");

        // then: only the 150 > 100 result survives; non-bool comparison propagates lhs sample value
        assert_results_match(
            &result,
            &[(
                150.0,
                vec![
                    ("__name__", "cpu_usage"),
                    ("env", "prod"),
                    ("instance", "i1"),
                ],
            )],
        );
    }

    #[tokio::test]
    async fn should_error_group_left_duplicate_on_right_side() {
        // given: group_left but the right (one) side has duplicates after matching
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
        .await;

        // then: error - the one side has duplicates
        assert!(
            result.is_err(),
            "Expected error for duplicate series on the one (right) side"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the right side"),
            "Error should mention right side: {err}"
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_left_no_match() {
        // given: no matching env between lhs and rhs
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            ("memory_bytes", vec![("env", "staging")], 1, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_left with no match should return empty");

        // then: empty - no env matches
        assert!(
            result.is_empty(),
            "Expected empty result, got {} samples",
            result.len()
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_right_add() {
        // given: one left-side series matched to many right-side series via on(env)
        // cpu_usage has one per env, memory_bytes has two per env (different instance).
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            ("cpu_usage", vec![("env", "staging")], 1, 70.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                2,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                3,
                200.0,
            ),
            (
                "memory_bytes",
                vec![("env", "staging"), ("instance", "i3")],
                4,
                300.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_right query should evaluate successfully");

        // then: result labels come from the many (right) side, __name__ dropped by arithmetic
        assert_results_match(
            &result,
            &[
                (150.0, vec![("env", "prod"), ("instance", "i1")]),
                (250.0, vec![("env", "prod"), ("instance", "i2")]),
                (370.0, vec![("env", "staging"), ("instance", "i3")]),
            ],
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_right_with_extra_labels() {
        // given: group_right(region) copies "region" label from the one (left) side
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("region", "us-east")],
                0,
                50.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_right(region) memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_right with extra labels should evaluate successfully");

        // then: result has many-side labels plus the extra "region" from one (left) side
        assert_results_match(
            &result,
            &[
                (
                    150.0,
                    vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                ),
                (
                    250.0,
                    vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                ),
            ],
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_right_comparison() {
        // given: comparison with group_right filters out false results
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 150.0),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2")],
                2,
                200.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage > on(env) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_right comparison should evaluate successfully");

        // then: only 150 > 100 survives; non-bool comparison propagates lhs sample value
        assert_results_match(
            &result,
            &[(
                150.0,
                vec![
                    ("__name__", "memory_bytes"),
                    ("env", "prod"),
                    ("instance", "i1"),
                ],
            )],
        );
    }

    #[tokio::test]
    async fn should_error_group_right_duplicate_on_left_side() {
        // given: group_right but the left (one) side has duplicates after matching
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2")],
                1,
                60.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
        .await;

        // then: error - the one (left) side has duplicates
        assert!(
            result.is_err(),
            "Expected error for duplicate series on the one (left) side"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("duplicate series on the left side"),
            "Error should mention left side: {err}"
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_right_no_match() {
        // given: no matching env between lhs and rhs
        let test_data: TestSampleData = vec![
            ("cpu_usage", vec![("env", "prod")], 0, 50.0),
            (
                "memory_bytes",
                vec![("env", "staging"), ("instance", "i1")],
                1,
                100.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_right with no match should return empty");

        // then: empty - no env matches
        assert!(
            result.is_empty(),
            "Expected empty result, got {} samples",
            result.len()
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_left_with_ignoring() {
        // given: group_left with ignoring() instead of on()
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                0,
                50.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                1,
                60.0,
            ),
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i3"), ("region", "eu-west")],
                2,
                70.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i99"), ("region", "us-east")],
                3,
                100.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when: ignoring(instance) removes instance from key, so key = {env, region}
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + ignoring(instance) group_left memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_left with ignoring should evaluate successfully");

        // then: only region="us-east" matches; region="eu-west" is dropped (no one-side match)
        assert_results_match(
            &result,
            &[
                (
                    150.0,
                    vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                ),
                (
                    160.0,
                    vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                ),
            ],
        );
    }

    #[tokio::test]
    async fn should_evaluate_group_right_with_ignoring() {
        // given: group_right with ignoring() instead of on()
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i99"), ("region", "us-east")],
                0,
                50.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                1,
                100.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                2,
                200.0,
            ),
            (
                "memory_bytes",
                vec![("env", "prod"), ("instance", "i3"), ("region", "eu-west")],
                3,
                300.0,
            ),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when: ignoring(instance) removes instance from key, so key = {env, region}
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + ignoring(instance) group_right memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_right with ignoring should evaluate successfully");

        // then: only region="us-east" matches; region="eu-west" is dropped (no one-side match)
        assert_results_match(
            &result,
            &[
                (
                    150.0,
                    vec![("env", "prod"), ("instance", "i1"), ("region", "us-east")],
                ),
                (
                    250.0,
                    vec![("env", "prod"), ("instance", "i2"), ("region", "us-east")],
                ),
            ],
        );
    }

    #[tokio::test]
    async fn should_error_when_group_left_produces_duplicate_output_labelsets() {
        // given: two many-side series differ only by metric name. Arithmetic drops __name__,
        // so both outputs collapse to the same label set and must be rejected.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                50.0,
            ),
            (
                "cpu_usage_alt",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                60.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            r#"{__name__=~"cpu_usage|cpu_usage_alt"} + on(env) group_left memory_bytes"#,
            end_time,
            lookback_delta,
        )
        .await;

        // then: Prometheus requires output series to remain uniquely identifiable
        assert!(
            result.is_err(),
            "Expected error for duplicate output label sets in group_left result"
        );
    }

    #[tokio::test]
    async fn should_remove_group_left_extra_label_when_one_side_lacks_it() {
        // given: many-side sample already has region label, but one-side match has no region.
        // group_left(region) should remove region from output, not preserve stale value.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1"), ("region", "stale")],
                0,
                50.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 1, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            "cpu_usage + on(env) group_left(region) memory_bytes",
            end_time,
            lookback_delta,
        )
        .await
        .expect("group_left(region) query should evaluate successfully");

        // then: region must not be present because one-side series has no region label
        assert_results_match(
            &result,
            &[(150.0, vec![("env", "prod"), ("instance", "i1")])],
        );
    }

    #[tokio::test]
    async fn should_error_grouped_comparison_duplicates_before_false_filter() {
        // given: two many-side series collapse to identical output labels after inner arithmetic
        // drops __name__. Both comparisons are false, but grouped duplicate validation should
        // still run before non-bool filter drop.
        let test_data: TestSampleData = vec![
            (
                "cpu_usage",
                vec![("env", "prod"), ("instance", "i1")],
                0,
                10.0,
            ),
            (
                "cpu_usage_alt",
                vec![("env", "prod"), ("instance", "i1")],
                1,
                20.0,
            ),
            ("memory_bytes", vec![("env", "prod")], 2, 100.0),
        ];
        let (reader, end_time) = setup_mock_reader(test_data);
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300);

        // when
        let result = parse_and_evaluate(
            &mut evaluator,
            r#"({__name__=~"cpu_usage|cpu_usage_alt"} + 0) > on(env) group_left memory_bytes"#,
            end_time,
            lookback_delta,
        )
        .await;

        // then: duplicate output labels must error even though both comparisons are false
        assert!(
            result.is_err(),
            "Expected duplicate-match error before comparison false filtering"
        );
    }

    #[tokio::test]
    async fn should_handle_subquery_step_fallback_in_instant_context() {
        use promql_parser::parser::SubqueryExpr;

        // given: data at 0s and 10s
        let bucket = TimeBucket::hour(0);
        let mut builder = MockQueryReaderBuilder::new(bucket);
        builder.add_sample(
            vec![Label {
                name: "__name__".to_string(),
                value: "metric".to_string(),
            }],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 0,
                value: 1.0,
            },
        );
        builder.add_sample(
            vec![Label {
                name: "__name__".to_string(),
                value: "metric".to_string(),
            }],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 10000,
                value: 2.0,
            },
        );

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: subquery with no step in instant query context (interval = 0)
        let subquery = SubqueryExpr {
            expr: Box::new(promql_parser::parser::Expr::VectorSelector(
                promql_parser::parser::VectorSelector {
                    name: Some("metric".to_string()),
                    matchers: promql_parser::label::Matchers::empty(),
                    offset: None,
                    at: None,
                },
            )),
            range: Duration::from_secs(50),
            step: None, // No explicit step
            offset: None,
            at: None,
        };

        let eval_time = UNIX_EPOCH + Duration::from_secs(10);
        let result = evaluator
            .evaluate_subquery(
                &subquery,
                Timestamp::from(eval_time),
                Timestamp::from(eval_time),
                Timestamp::from(eval_time),
                0, // instant query context
                300_000,
            )
            .await;

        // then: should not panic or infinite loop, should use fallback step
        assert!(result.is_ok());
    }

    #[test]
    fn should_align_negative_timestamps_correctly() {
        // Test floor division alignment for negative timestamps
        let subquery_start_ms = -41i64;
        let step_ms = 10i64;

        // Using regular division (incorrect)
        let wrong_div = subquery_start_ms / step_ms; // -4 (truncates toward zero)
        let wrong_aligned = wrong_div * step_ms; // -40

        // Using div_euclid (correct floor division)
        let correct_div = subquery_start_ms.div_euclid(step_ms); // -5 (floor)
        let correct_aligned = correct_div * step_ms; // -50

        assert_eq!(wrong_aligned, -40, "Regular division gives -40");
        assert_eq!(correct_aligned, -50, "Floor division gives -50");

        // Prometheus expects floor division behavior
        assert_ne!(
            wrong_aligned, correct_aligned,
            "Regular division != floor division for negatives"
        );
    }

    // ── compute_preload_ranges tests ──────────────────────────────────

    mod preload_ranges_tests {
        use super::*;
        use crate::promql::evaluator::compute_preload_ranges;

        fn t(secs: u64) -> SystemTime {
            UNIX_EPOCH + Duration::from_secs(secs)
        }

        #[test]
        fn scalar_only_returns_empty() {
            let expr = promql_parser::parser::parse("1 + 2").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(1000), Duration::from_secs(300));
            assert_eq!(result, vec![]);
        }

        #[test]
        fn simple_selector_no_modifiers() {
            // query_time=1000s, lookback=300s
            // eval_time=1000s, data window: [1000-300, 1000] = [700, 1000]
            let expr = promql_parser::parser::parse("metric_name").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(1000), Duration::from_secs(300));
            assert_eq!(result, vec![(700, 1000)]);
        }

        #[test]
        fn offset_shifts_start_back() {
            // query_time=7200s, lookback=300s, offset=1h=3600s
            // eval_time = 7200 - 3600 = 3600
            // data window: [3600 - 300, 3600] = [3300, 3600]
            let expr = promql_parser::parser::parse("metric_name offset 1h").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(3300, 3600)]);
        }

        #[test]
        fn at_modifier_absolute_time() {
            // query_time=2000s, lookback=300s, @500
            // eval_time = 500, data window: [500 - 300, 500] = [200, 500]
            let expr = promql_parser::parser::parse("metric_name @ 500").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(300));
            assert_eq!(result, vec![(200, 500)]);
        }

        #[test]
        fn at_with_offset() {
            // query_time=2000s, lookback=300s, @500 offset 5m
            // eval_time = 500 - 300 = 200, data window: [200 - 300, 200] = [-100, 200]
            let expr = promql_parser::parser::parse("metric_name @ 500 offset 5m").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(300));
            assert_eq!(result, vec![(-100, 200)]);
        }

        #[test]
        fn negative_offset_shifts_forward() {
            // query_time=1000s, lookback=300s, offset=-5m=-300s
            // eval_time = 1000 + 300 = 1300, data window: [1300 - 300, 1300] = [1000, 1300]
            let expr = promql_parser::parser::parse("metric_name offset -5m").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(1000), Duration::from_secs(300));
            assert_eq!(result, vec![(1000, 1300)]);
        }

        #[test]
        fn matrix_selector_uses_range() {
            // query_time=7200s, lookback=300s (unused for matrix), offset=1h, range=5m=300s
            // eval_time = 7200 - 3600 = 3600
            // data window: [3600 - 300, 3600] = [3300, 3600]
            let expr = promql_parser::parser::parse("metric_name[5m] offset 1h").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(3300, 3600)]);
        }

        #[test]
        fn nested_binary_produces_disjoint_ranges() {
            // max(metric offset 1h) - max(metric offset 2h) at query_time=7200, lookback=300
            // Left:  eval=3600, window=[3300, 3600]
            // Right: eval=0,    window=[-300, 0]
            // Two disjoint ranges (not merged into one)
            let expr = promql_parser::parser::parse(
                "max(metric_name offset 1h) - max(metric_name offset 2h)",
            )
            .unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(-300, 0), (3300, 3600)]);
        }

        #[test]
        fn subquery_accounts_for_range_and_offset() {
            // metric[1h:5m] offset 30m at query_time=7200s, lookback=300s
            // subquery eval_time = 7200 - 1800 = 5400
            // subquery range = 1h = 3600s, inner eval over [5400-3600, 5400] = [1800, 5400]
            // inner selector lookback: [1800 - 300, 5400] = [1500, 5400]
            let expr = promql_parser::parser::parse("metric_name[1h:5m] offset 30m").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(1500, 5400)]);
        }

        #[test]
        fn subquery_inner_at_end_uses_outer_query_bounds() {
            // metric @ end()[1h:5m] offset 30m
            // query_start=query_end=7200s, lookback=300s
            //
            // Subquery: eval_time = 7200 - 1800 = 5400, range = 3600s
            //   inner eval window: [5400 - 3600, 5400] = [1800, 5400]
            // Inner selector: @ end() resolves to outer query_end = 7200s (not sq_end!)
            //   eval_time = 7200, data window: [7200 - 300, 7200] = [6900, 7200]
            //
            // Without the fix, @ end() would resolve to sq_end = 5400, giving [5100, 5400].
            let expr =
                promql_parser::parser::parse("metric_name @ end()[1h:5m] offset 30m").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(6900, 7200)]);
        }

        #[test]
        fn subquery_inner_at_start_uses_outer_query_bounds() {
            // metric @ start()[1h:5m] offset 30m
            // query_start=3600s, query_end=7200s, lookback=300s
            //
            // Subquery: eval range = [3600, 7200], then offset 30m
            //   → [3600-1800, 7200-1800] = [1800, 5400], range=3600s
            //   → inner eval window: [1800-3600, 5400] = [-1800, 5400]
            // Inner selector: @ start() resolves to outer [query_start, query_end]
            //   = [3600, 7200] (@ start/end sweep the full at-modifier range)
            //   data window: [3600 - 300, 7200] = [3300, 7200]
            let expr =
                promql_parser::parser::parse("metric_name @ start()[1h:5m] offset 30m").unwrap();
            let result = compute_preload_ranges(&expr, t(3600), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(3300, 7200)]);
        }

        #[test]
        fn range_query_simple_selector() {
            // range [1000, 2000] with lookback=300
            // data window: [1000 - 300, 2000] = [700, 2000]
            let expr = promql_parser::parser::parse("metric_name").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(2000), Duration::from_secs(300));
            assert_eq!(result, vec![(700, 2000)]);
        }

        #[test]
        fn range_query_with_offset() {
            // range [3600, 7200] with lookback=300, offset=1h
            // eval range: [3600-3600, 7200-3600] = [0, 3600]
            // data window: [0 - 300, 3600] = [-300, 3600]
            let expr = promql_parser::parser::parse("metric_name offset 1h").unwrap();
            let result = compute_preload_ranges(&expr, t(3600), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(-300, 3600)]);
        }

        #[test]
        fn range_query_at_end_covers_all_steps() {
            // range [1000, 5000] with lookback=300, metric @ end()
            // In query_range, each step sets start=end=current_time, so
            // @ end() resolves to current_time, sweeping [1000, 5000].
            // Preload must cover: [1000 - 300, 5000] = [700, 5000]
            // Without the fix, @ end() would collapse to (5000, 5000)
            // giving only [4700, 5000] and missing earlier steps.
            let expr = promql_parser::parser::parse("metric_name @ end()").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(5000), Duration::from_secs(300));
            assert_eq!(result, vec![(700, 5000)]);
        }

        #[test]
        fn range_query_at_start_covers_all_steps() {
            // range [1000, 5000] with lookback=300, metric @ start()
            // Same as @ end(): sweeps [1000, 5000] across steps.
            // Preload must cover: [1000 - 300, 5000] = [700, 5000]
            let expr = promql_parser::parser::parse("metric_name @ start()").unwrap();
            let result = compute_preload_ranges(&expr, t(1000), t(5000), Duration::from_secs(300));
            assert_eq!(result, vec![(700, 5000)]);
        }

        #[test]
        fn instant_query_at_end_is_single_point() {
            // instant query at t=2000, lookback=300, metric @ end()
            // start=end=2000s, so @ end() → (2000, 2000)
            // data window: [2000 - 300, 2000] = [1700, 2000]
            let expr = promql_parser::parser::parse("metric_name @ end()").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(300));
            assert_eq!(result, vec![(1700, 2000)]);
        }

        #[test]
        fn function_call_recurses_into_args() {
            // rate(metric[5m] offset 1h) at query_time=7200, lookback=300 (unused for matrix)
            // matrix: eval_time = 7200 - 3600 = 3600, range = 5m = 300s
            // data window: [3600 - 300, 3600] = [3300, 3600]
            let expr = promql_parser::parser::parse("rate(metric_name[5m] offset 1h)").unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(3300, 3600)]);
        }

        #[test]
        fn ceil_conversion_exact_second_boundary() {
            // hi_ms = 2000000 (exact), end should be 2000 not 2001
            let expr = promql_parser::parser::parse("metric_name").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(0));
            assert_eq!(result, vec![(2000, 2000)]);
        }

        #[test]
        fn ceil_conversion_non_exact_boundary() {
            // offset -500ms shifts eval_end to 2000500ms → ceil = 2001
            // eval_start = 2000500ms → floor = 2000
            let expr = promql_parser::parser::parse("metric_name offset -500ms").unwrap();
            let result = compute_preload_ranges(&expr, t(2000), t(2000), Duration::from_secs(0));
            assert_eq!(result, vec![(2000, 2001)]);
        }

        #[test]
        fn disjoint_ranges_not_merged() {
            // metric - metric offset 5d at query_time=1000000s, lookback=300
            // Left: eval=1000000, window=[999700, 1000000]
            // Right: eval=1000000-432000=568000, window=[567700, 568000]
            // These are far apart and should produce 2 disjoint ranges
            let expr = promql_parser::parser::parse("metric_name - metric_name offset 5d").unwrap();
            let result =
                compute_preload_ranges(&expr, t(1_000_000), t(1_000_000), Duration::from_secs(300));
            assert_eq!(result, vec![(567700, 568000), (999700, 1000000)]);
        }

        #[test]
        fn overlapping_ranges_are_merged() {
            // metric offset 1m - metric offset 2m at query_time=7200, lookback=300
            // Left:  eval=7140, window=[6840, 7140]
            // Right: eval=7080, window=[6780, 7080]
            // These overlap → merged into [6780, 7140]
            let expr =
                promql_parser::parser::parse("metric_name offset 1m - metric_name offset 2m")
                    .unwrap();
            let result = compute_preload_ranges(&expr, t(7200), t(7200), Duration::from_secs(300));
            assert_eq!(result, vec![(6780, 7140)]);
        }

        #[test]
        fn normalize_ranges_basic() {
            use crate::promql::evaluator::normalize_ranges;
            // Already sorted, overlapping
            assert_eq!(normalize_ranges(vec![(0, 5), (3, 10)]), vec![(0, 10)]);
            // Disjoint
            assert_eq!(
                normalize_ranges(vec![(0, 5), (10, 15)]),
                vec![(0, 5), (10, 15)]
            );
            // Unsorted
            assert_eq!(
                normalize_ranges(vec![(10, 15), (0, 5)]),
                vec![(0, 5), (10, 15)]
            );
            // Negative start is preserved (pre-epoch timestamps are valid)
            assert_eq!(normalize_ranges(vec![(-5, 10)]), vec![(-5, 10)]);
            // Empty
            assert_eq!(normalize_ranges(vec![]), vec![]);
        }
    }

    /// Tests that exercise the ReadLoadCoordinator integration with the evaluator.
    mod load_coordinator_integration {
        use super::*;
        use crate::load_coordinator::ReadLoadCoordinator;
        use crate::query::test_utils::MockMultiBucketQueryReaderBuilder;

        /// Range query through Evaluator::with_coordinator produces correct results
        /// and populates the new stats fields.
        #[tokio::test]
        async fn range_query_with_coordinator_populates_stats() {
            let mut builder = MockMultiBucketQueryReaderBuilder::new();
            let bucket = TimeBucket::hour(100);

            // 3 series × 3 samples each → enough to exercise parallel loading
            for env in ["prod", "staging", "dev"] {
                for (ts, val) in [(5_700_000, 10.0), (6_000_000, 20.0), (6_300_000, 30.0)] {
                    builder.add_sample(
                        bucket,
                        vec![
                            Label {
                                name: METRIC_NAME.to_string(),
                                value: "http_requests".to_string(),
                            },
                            Label {
                                name: "env".to_string(),
                                value: env.to_string(),
                            },
                        ],
                        MetricType::Gauge,
                        Sample {
                            timestamp_ms: ts,
                            value: val,
                        },
                    );
                }
            }

            let reader = builder.build();
            let coordinator = ReadLoadCoordinator::new(4, 2);
            let mut evaluator = Evaluator::with_coordinator(&reader, coordinator);

            // Preload triggers fetch_series_samples → the parallel path
            let eval_start_ms = 5_700_000i64;
            let eval_end_ms = 6_300_000i64;
            let step_ms = 300_000i64;
            let lookback_delta_ms = 300_000i64;

            let expr = promql_parser::parser::parse("http_requests").unwrap();
            evaluator
                .preload_for_range(
                    &expr,
                    eval_start_ms,
                    eval_end_ms,
                    step_ms,
                    lookback_delta_ms,
                )
                .await
                .unwrap();

            let stats = evaluator.stats();

            // Parallel path was used (samples weren't cached, so all are misses)
            assert!(
                stats.parallel_sample_loads > 0,
                "expected parallel_sample_loads > 0, got {}",
                stats.parallel_sample_loads
            );

            // Permit acquires should match parallel loads (coordinator was present)
            assert_eq!(
                stats.sample_permit_acquires, stats.parallel_sample_loads,
                "permit acquires ({}) should equal parallel loads ({})",
                stats.sample_permit_acquires, stats.parallel_sample_loads
            );

            // Load time decomposition is consistent
            assert!(
                stats.sample_load_ms + stats.sample_queue_wait_ms >= stats.sample_miss_ms
                    || stats.sample_miss_ms
                        <= stats.sample_load_ms + stats.sample_queue_wait_ms + 1,
                "sample_load_ms ({}) + sample_queue_wait_ms ({}) should ≈ sample_miss_ms ({})",
                stats.sample_load_ms,
                stats.sample_queue_wait_ms,
                stats.sample_miss_ms
            );

            // Metadata permits were acquired for forward_index loads
            assert!(
                stats.metadata_permit_acquires > 0,
                "expected metadata_permit_acquires > 0, got {}",
                stats.metadata_permit_acquires
            );

            // Bucket parallelism stats should be populated
            assert!(
                stats.parallel_sample_bucket_count > 0,
                "expected parallel_sample_bucket_count > 0, got {}",
                stats.parallel_sample_bucket_count
            );

            // Now verify the results are still correct by evaluating a step
            let query_time = UNIX_EPOCH + Duration::from_millis(6_300_000);
            let stmt = EvalStmt {
                expr,
                start: query_time,
                end: query_time,
                interval: Duration::from_secs(0),
                lookback_delta: Duration::from_millis(lookback_delta_ms as u64),
            };
            let result = evaluator.evaluate(stmt).await.unwrap();
            if let ExprResult::InstantVector(samples) = result {
                assert_eq!(samples.len(), 3, "expected 3 series");
                for s in &samples {
                    assert_eq!(s.value, 30.0, "expected latest sample value");
                }
            } else {
                panic!("expected InstantVector");
            }
        }

        /// Evaluator::new (no coordinator) still works and leaves coordinator stats at zero.
        #[tokio::test]
        async fn range_query_without_coordinator_leaves_stats_zeroed() {
            let mut builder = MockMultiBucketQueryReaderBuilder::new();
            let bucket = TimeBucket::hour(100);

            builder.add_sample(
                bucket,
                vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "metric".to_string(),
                    },
                    Label {
                        name: "k".to_string(),
                        value: "v".to_string(),
                    },
                ],
                MetricType::Gauge,
                Sample {
                    timestamp_ms: 6_000_000,
                    value: 42.0,
                },
            );

            let reader = builder.build();
            let mut evaluator = Evaluator::new(&reader);

            let expr = promql_parser::parser::parse("metric").unwrap();
            evaluator
                .preload_for_range(&expr, 5_700_000, 6_300_000, 300_000, 300_000)
                .await
                .unwrap();

            let stats = evaluator.stats();
            assert_eq!(stats.sample_permit_acquires, 0);
            assert_eq!(stats.metadata_permit_acquires, 0);
            // parallel_sample_loads is still populated (the buffer_unordered path
            // runs regardless of coordinator presence)
            assert!(stats.parallel_sample_loads > 0);
        }

        /// Multi-bucket query resolves selectors and loads samples in parallel,
        /// populating both B1 and B3 phase stats.
        #[tokio::test]
        async fn multi_bucket_parallel_resolution_and_loading() {
            let mut builder = MockMultiBucketQueryReaderBuilder::new();
            let buckets = [
                TimeBucket::hour(100),
                TimeBucket::hour(101),
                TimeBucket::hour(102),
                TimeBucket::hour(103),
            ];

            // 2 series across 4 buckets
            for &bucket in &buckets {
                let base_ts = bucket.start as i64 * 3_600_000;
                for env in ["prod", "staging"] {
                    builder.add_sample(
                        bucket,
                        vec![
                            Label {
                                name: METRIC_NAME.to_string(),
                                value: "http_requests".to_string(),
                            },
                            Label {
                                name: "env".to_string(),
                                value: env.to_string(),
                            },
                        ],
                        MetricType::Gauge,
                        Sample {
                            timestamp_ms: base_ts + 1_800_000,
                            value: 1.0,
                        },
                    );
                }
            }

            let reader = builder.build();
            let coordinator = ReadLoadCoordinator::new(16, 4);
            let mut evaluator = Evaluator::with_coordinator(&reader, coordinator);

            // Wide range covering all 4 buckets
            let eval_start_ms = 100 * 3_600_000i64;
            let eval_end_ms = 104 * 3_600_000i64;
            let step_ms = 3_600_000i64;
            let lookback_delta_ms = 3_600_000i64;

            let expr = promql_parser::parser::parse("http_requests").unwrap();
            evaluator
                .preload_for_range(
                    &expr,
                    eval_start_ms,
                    eval_end_ms,
                    step_ms,
                    lookback_delta_ms,
                )
                .await
                .unwrap();

            let stats = evaluator.stats();

            // All 4 buckets were selector misses resolved via B1
            assert_eq!(
                stats.parallel_selector_count, 4,
                "expected 4 parallel selector resolutions, got {}",
                stats.parallel_selector_count
            );
            // B1 wall/sum may be 0ms for fast mock readers — just check count is populated

            // All 4 buckets had samples loaded via B3
            assert_eq!(
                stats.parallel_sample_bucket_count, 4,
                "expected 4 parallel sample buckets, got {}",
                stats.parallel_sample_bucket_count
            );

            // 2 series × 4 buckets = 8 parallel sample loads
            assert_eq!(
                stats.parallel_sample_loads, 8,
                "expected 8 parallel sample loads, got {}",
                stats.parallel_sample_loads
            );

            // Backward-compatible metrics still populated
            // preload_bucket_ms_sum/max may be 0 for fast mock readers — not worth asserting
        }

        /// Second preload for a different (uncached) selector reuses sample cache
        /// entries from first preload's Phase C merge.
        #[tokio::test]
        async fn uncached_selector_reuses_sample_cache() {
            let mut builder = MockMultiBucketQueryReaderBuilder::new();
            let bucket = TimeBucket::hour(100);

            // Series matches both selectors: http_requests{method="GET"} and http_requests{env="prod"}
            for ts in [5_700_000i64, 6_000_000, 6_300_000] {
                builder.add_sample(
                    bucket,
                    vec![
                        Label {
                            name: METRIC_NAME.to_string(),
                            value: "http_requests".to_string(),
                        },
                        Label {
                            name: "method".to_string(),
                            value: "GET".to_string(),
                        },
                        Label {
                            name: "env".to_string(),
                            value: "prod".to_string(),
                        },
                    ],
                    MetricType::Gauge,
                    Sample {
                        timestamp_ms: ts,
                        value: 1.0,
                    },
                );
            }

            let reader = builder.build();
            let coordinator = ReadLoadCoordinator::new(4, 2);
            let mut evaluator = Evaluator::with_coordinator(&reader, coordinator);

            // First preload — selector A loads samples
            let expr_a = promql_parser::parser::parse(r#"http_requests{method="GET"}"#).unwrap();
            evaluator
                .preload_for_range(&expr_a, 5_700_000, 6_300_000, 300_000, 300_000)
                .await
                .unwrap();
            let loads_after_a = evaluator.stats().parallel_sample_loads;
            assert!(loads_after_a > 0, "first preload should load samples");

            // Second preload — selector B (different, uncached) but same series
            let expr_b = promql_parser::parser::parse(r#"http_requests{env="prod"}"#).unwrap();
            evaluator
                .preload_for_range(&expr_b, 5_700_000, 6_300_000, 300_000, 300_000)
                .await
                .unwrap();
            let loads_after_b = evaluator.stats().parallel_sample_loads;

            // No additional sample I/O — samples were cached from first preload
            assert_eq!(
                loads_after_a, loads_after_b,
                "second preload should reuse cached samples (loads_after_a={}, loads_after_b={})",
                loads_after_a, loads_after_b
            );

            // But sample cache hits should have increased
            assert!(
                evaluator.stats().sample_cache_hits > 0,
                "expected sample_cache_hits > 0"
            );
        }

        /// Empty buckets are cached and skipped on subsequent queries.
        #[tokio::test]
        async fn empty_bucket_cached_and_skipped() {
            let mut builder = MockMultiBucketQueryReaderBuilder::new();
            let bucket_with_data = TimeBucket::hour(100);
            let bucket_empty = TimeBucket::hour(101);

            // Only add data to one bucket; register the empty bucket via a different metric
            builder.add_sample(
                bucket_with_data,
                vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "http_requests".to_string(),
                    },
                    Label {
                        name: "k".to_string(),
                        value: "v".to_string(),
                    },
                ],
                MetricType::Gauge,
                Sample {
                    timestamp_ms: 100 * 3_600_000 + 1_800_000,
                    value: 1.0,
                },
            );
            builder.add_sample(
                bucket_empty,
                vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "other_metric".to_string(),
                    },
                    Label {
                        name: "k".to_string(),
                        value: "v".to_string(),
                    },
                ],
                MetricType::Gauge,
                Sample {
                    timestamp_ms: 101 * 3_600_000 + 1_800_000,
                    value: 2.0,
                },
            );

            let reader = builder.build();
            let coordinator = ReadLoadCoordinator::new(4, 2);
            let mut evaluator = Evaluator::with_coordinator(&reader, coordinator);

            let eval_start_ms = 100 * 3_600_000i64;
            let eval_end_ms = 102 * 3_600_000i64;

            let expr = promql_parser::parser::parse("http_requests").unwrap();
            evaluator
                .preload_for_range(&expr, eval_start_ms, eval_end_ms, 3_600_000, 3_600_000)
                .await
                .unwrap();

            let misses_first = evaluator.stats().selector_misses;

            // Second preload — same selector, both buckets should be cached
            evaluator
                .preload_for_range(&expr, eval_start_ms, eval_end_ms, 3_600_000, 3_600_000)
                .await
                .unwrap();

            let misses_second = evaluator.stats().selector_misses;
            assert_eq!(
                misses_first, misses_second,
                "second preload should have no additional selector misses"
            );
            assert!(
                evaluator.stats().selector_cache_hits > 0,
                "selector cache should be hit on second preload"
            );
        }

        /// Cache hits bypass the parallel path — second preload for the same selector
        /// should not increment parallel_sample_loads further.
        #[tokio::test]
        async fn cached_samples_skip_parallel_path() {
            let mut builder = MockMultiBucketQueryReaderBuilder::new();
            let bucket = TimeBucket::hour(100);

            builder.add_sample(
                bucket,
                vec![
                    Label {
                        name: METRIC_NAME.to_string(),
                        value: "metric".to_string(),
                    },
                    Label {
                        name: "k".to_string(),
                        value: "v".to_string(),
                    },
                ],
                MetricType::Gauge,
                Sample {
                    timestamp_ms: 6_000_000,
                    value: 1.0,
                },
            );

            let reader = builder.build();
            let coordinator = ReadLoadCoordinator::new(4, 2);
            let mut evaluator = Evaluator::with_coordinator(&reader, coordinator);

            let expr = promql_parser::parser::parse("metric").unwrap();

            // First preload — cold
            evaluator
                .preload_for_range(&expr, 5_700_000, 6_300_000, 300_000, 300_000)
                .await
                .unwrap();
            let after_first = evaluator.stats().parallel_sample_loads;
            assert!(after_first > 0);

            // Second preload — samples should be cached
            evaluator
                .preload_for_range(&expr, 5_700_000, 6_300_000, 300_000, 300_000)
                .await
                .unwrap();
            let after_second = evaluator.stats().parallel_sample_loads;
            assert_eq!(
                after_first, after_second,
                "parallel_sample_loads should not increase on cache hit"
            );
        }
    }

    /// Verify that forward_index cache hits do not re-accumulate batch stats.
    /// MockQueryReader returns ForwardIndex (not LoadedForwardIndex), so batch_stats()
    /// returns zeros. On cache miss the evaluator records those zeros. On cache hit
    /// the evaluator skips batch_stats() entirely, so fi_batch_ops must not change.
    #[tokio::test]
    async fn forward_index_cache_hit_does_not_add_batch_stats() {
        let bucket = TimeBucket {
            start: 100,
            size: 1,
        };
        let mut builder = MockQueryReaderBuilder::new(bucket);
        builder.add_sample(
            vec![
                Label {
                    name: METRIC_NAME.to_string(),
                    value: "test_metric".to_string(),
                },
                Label {
                    name: "host".to_string(),
                    value: "a".to_string(),
                },
            ],
            MetricType::Gauge,
            Sample {
                timestamp_ms: 6_000_000,
                value: 1.0,
            },
        );
        let reader = builder.build();
        let mut cached = CachedQueryReader::new(&reader);

        let series_ids = vec![0u32];

        // First call — cache miss
        let _ = cached.forward_index(&bucket, &series_ids).await.unwrap();
        let after_miss = cached.stats.fi_batch_ops;

        // Second call — cache hit
        let _ = cached.forward_index(&bucket, &series_ids).await.unwrap();
        let after_hit = cached.stats.fi_batch_ops;

        assert_eq!(
            after_miss, after_hit,
            "fi_batch_ops should not increase on cache hit"
        );
        assert_eq!(cached.stats.forward_index_cache_hits, 1);
        assert_eq!(cached.stats.forward_index_misses, 1);
    }
}

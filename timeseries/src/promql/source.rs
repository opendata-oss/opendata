//! The contract between the engine and whatever backend actually stores
//! samples. Built around the [`SeriesSource`] trait plus the request and
//! response types it operates on.
//!
//! The source is deliberately PromQL-unaware: it turns `(matcher,
//! time_range)` into resolved series and `(series, time_range)` into raw
//! samples, and that's it. PromQL semantics — `lookback_delta`, `@`,
//! `offset`, step alignment, rollup windows — all live in the operators.
//! We looked at pushing them into the storage layer and rejected it:
//! subquery semantics like `@ start()` / `@ end()` would force every
//! backend to re-implement evaluator state.

use std::ops::Range;
use std::sync::Arc;

use futures::Stream;
use promql_parser::parser::VectorSelector;

use crate::model::Labels;

use super::memory::QueryError;

/// Absolute ms window `[start_ms, end_ms_exclusive)` passed to the source
/// for both metadata and sample fetches.
///
/// The caller has already folded in max lookback, `offset`, and any
/// subquery shift; the source must **not** further widen or shift it. This
/// keeps PromQL semantics on the engine side and lets the source treat the
/// range as an opaque absolute window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeRange {
    pub start_ms: i64,
    pub end_ms_exclusive: i64,
}

impl TimeRange {
    pub fn new(start_ms: i64, end_ms_exclusive: i64) -> Self {
        Self {
            start_ms,
            end_ms_exclusive,
        }
    }

    /// Saturates to `0` on empty or inverted ranges. Sources must treat an
    /// empty range as "no samples," not as an error — it can arise from
    /// operator-side lookback arithmetic at query start.
    pub fn duration_ms(&self) -> i64 {
        self.end_ms_exclusive.saturating_sub(self.start_ms).max(0)
    }

    pub fn is_empty(&self) -> bool {
        self.end_ms_exclusive <= self.start_ms
    }
}

/// Opaque handle minted by [`SeriesSource::resolve`] and threaded back into
/// [`SamplesRequest::series`]. Callers treat the contents as opaque.
///
/// `metric_name` is carried here so the samples path can issue
/// `reader.samples(..)` without a forward-index re-lookup — the planner's
/// resolve pass already has the `SeriesSpec` in hand.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedSeriesRef {
    pub bucket_id: u64,
    /// Bucket-scoped series identifier (matches [`crate::model::SeriesId`]).
    pub series_id: u32,
    /// `__name__` label value for this series. Populated by the resolver from
    /// the forward index and threaded through to `SeriesSource::samples` so
    /// the adapter needn't re-fetch the `SeriesSpec`.
    pub metric_name: Arc<str>,
}

impl ResolvedSeriesRef {
    pub fn new(bucket_id: u64, series_id: u32, metric_name: Arc<str>) -> Self {
        Self {
            bucket_id,
            series_id,
            metric_name,
        }
    }
}

/// One contiguous slice of resolved-series metadata streamed from
/// [`SeriesSource::resolve`]. A selector typically spans multiple storage
/// buckets, and the source returns one chunk per bucket — the caller
/// concatenates them into the plan-time series roster.
///
/// Each chunk is self-describing: `labels` and `series` are parallel arrays
/// indexed together, and every handle in `series` points into this chunk's
/// `bucket_id`.
#[derive(Debug, Clone)]
pub struct ResolvedSeriesChunk {
    pub bucket_id: u64,
    pub labels: Arc<[Labels]>,
    pub series: Arc<[ResolvedSeriesRef]>,
}

/// Request from operator to source for raw samples covering a specific
/// series list over a specific window.
///
/// The source must not widen `series` and must not shift `time_range` —
/// these are the exact inputs the caller has already planned around.
/// Batches returned carry `series_range` indices into `series` so the
/// caller can re-assemble them in roster order.
#[derive(Debug, Clone)]
pub struct SamplesRequest {
    pub series: Arc<[ResolvedSeriesRef]>,
    pub time_range: TimeRange,
}

impl SamplesRequest {
    pub fn new(series: Arc<[ResolvedSeriesRef]>, time_range: TimeRange) -> Self {
        Self { series, time_range }
    }
}

/// Per-series raw samples: length-matched `timestamps` / `values` columns per
/// series, in timestamp order. Stale markers are preserved as
/// [`STALE_NAN`](crate::model::STALE_NAN); callers distinguish them via
/// [`crate::model::is_stale_nan`].
///
/// Block index `j` corresponds to `request.series[series_range.start + j]`.
#[derive(Debug, Clone)]
pub struct SampleBlock {
    pub timestamps: Vec<Vec<i64>>,
    pub values: Vec<Vec<f64>>,
}

impl SampleBlock {
    pub fn with_series_count(series_count: usize) -> Self {
        Self {
            timestamps: (0..series_count).map(|_| Vec::new()).collect(),
            values: (0..series_count).map(|_| Vec::new()).collect(),
        }
    }

    pub fn series_count(&self) -> usize {
        self.timestamps.len()
    }
}

/// Raw-sample batch from [`SeriesSource::samples`]. Covers a contiguous slice
/// of the caller's series list, per-series in timestamp order with exact
/// source timestamps. Batch sizing is the source's choice.
#[derive(Debug, Clone)]
pub struct SampleBatch {
    pub series_range: Range<usize>,
    pub samples: SampleBlock,
}

/// The engine's storage-facing trait. Implementations turn a selector
/// into resolved series and resolved series into raw samples, and do
/// nothing else PromQL-flavoured.
///
/// Not `dyn`-safe (the methods use RPITIT); the planner binds a generic
/// `S: SeriesSource` and carries it through `Arc<S>`.
pub trait SeriesSource: Send + Sync {
    /// Stream series metadata for `selector` over `time_range`. Each chunk
    /// is a contiguous slice the caller appends to the plan-time roster.
    fn resolve(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
    ) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send;

    /// Stream raw samples for a pre-resolved series roster. No step, lookback,
    /// or rollup — per-series timestamp order, exact source timestamps, stale
    /// markers preserved as [`STALE_NAN`](crate::model::STALE_NAN).
    fn samples(
        &self,
        request: SamplesRequest,
    ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_time_range_with_inclusive_exclusive_bounds() {
        // given: a caller-computed absolute window
        let start = 1_000i64;
        let end = 5_000i64;

        // when: wrap it in a TimeRange
        let tr = TimeRange::new(start, end);

        // then: fields and derived width match the caller's intent
        assert_eq!(tr.start_ms, start);
        assert_eq!(tr.end_ms_exclusive, end);
        assert_eq!(tr.duration_ms(), 4_000);
        assert!(!tr.is_empty());
    }

    #[test]
    fn should_treat_empty_and_inverted_time_ranges_as_empty() {
        // given: an empty range and an inverted range
        let empty = TimeRange::new(10, 10);
        let inverted = TimeRange::new(20, 5);

        // when / then: both report empty and zero-width
        assert!(empty.is_empty());
        assert_eq!(empty.duration_ms(), 0);
        assert!(inverted.is_empty());
        assert_eq!(inverted.duration_ms(), 0);
    }

    #[test]
    fn should_construct_resolved_series_ref_as_opaque_handle() {
        // given: a bucket id, a bucket-scoped series id, and a metric name
        let bucket = 42u64;
        let sid = 7u32;
        let name: Arc<str> = Arc::from("http_requests_total");

        // when: mint a handle
        let handle = ResolvedSeriesRef::new(bucket, sid, name.clone());

        // then: the struct preserves all fields and is hashable / clone
        assert_eq!(handle.bucket_id, bucket);
        assert_eq!(handle.series_id, sid);
        assert_eq!(&*handle.metric_name, "http_requests_total");
        let cloned = handle.clone();
        assert_eq!(cloned, handle);
    }

    #[test]
    fn should_build_sample_request_from_resolved_series_and_time_range() {
        // given: a caller-resolved series slice and an absolute window
        let name: Arc<str> = Arc::from("m");
        let series: Arc<[ResolvedSeriesRef]> = Arc::from(vec![
            ResolvedSeriesRef::new(1, 10, name.clone()),
            ResolvedSeriesRef::new(1, 11, name.clone()),
        ]);
        let tr = TimeRange::new(0, 60_000);

        // when: build the request
        let request = SamplesRequest::new(series.clone(), tr);

        // then: series slice and time range round-trip; request is clone
        assert_eq!(request.series.len(), 2);
        assert_eq!(request.time_range, tr);
        let cloned = request.clone();
        assert_eq!(cloned.series.len(), request.series.len());
    }

    #[test]
    fn should_build_empty_sample_block_sized_per_series_count() {
        // given: a batch covering 3 series
        // when: allocate an empty sample block
        let block = SampleBlock::with_series_count(3);

        // then: per-series columns exist but are empty
        assert_eq!(block.series_count(), 3);
        assert!(block.timestamps.iter().all(|v| v.is_empty()));
        assert!(block.values.iter().all(|v| v.is_empty()));
    }
}

//! Storage contract for the v2 execution engine.
//!
//! Defines [`SeriesSource`] — the single trait the planner and leaf
//! operators use to reach storage — plus the supporting value types the
//! trait's methods exchange with callers:
//!
//! - [`TimeRange`]: inclusive-exclusive absolute ms window.
//! - [`ResolvedSeriesRef`]: opaque handle minted by [`SeriesSource::resolve`]
//!   and later threaded back into [`SampleHint::series`].
//! - [`ResolvedSeriesChunk`]: a contiguous slice of resolved series
//!   metadata, streamed as selector resolution progresses.
//! - [`CardinalityEstimate`]: the pre-resolve gate the planner uses to
//!   reject oversized queries before materialising sample state.
//! - [`SampleHint`] / [`SampleBatch`] / [`SampleBlock`]: the raw-sample
//!   fetch pair.
//!
//! The contract is deliberately PromQL-unaware. Lookback delta, `@`,
//! `offset`, step alignment, and rollup semantics all live in the
//! operators — the source's job is bytes → samples. See RFC 0007
//! §"Storage Contract" and §5 Decisions Log entry 2 (pushdown rejected
//! for v1).
//!
//! Unit 2.1 defines the trait and types only. The adapter over the
//! existing per-bucket `QueryReader` fan-out lives in unit 2.2, and its
//! integration tests in unit 2.3.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use futures::Stream;
use promql_parser::parser::VectorSelector;

use crate::model::Labels;

use super::memory::QueryError;

/// Absolute time window in milliseconds, **inclusive-exclusive**
/// (`[start_ms, end_ms_exclusive)`).
///
/// The caller has already folded in the maximum lookback, `offset`, and
/// any subquery shift the downstream operator needs — the source must
/// **not** further widen or shift this window (RFC 0007 §"Storage Contract",
/// caller → source contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeRange {
    /// First millisecond covered by the window, inclusive.
    pub start_ms: i64,
    /// First millisecond **not** covered by the window, exclusive.
    pub end_ms_exclusive: i64,
}

impl TimeRange {
    /// Build a range from an inclusive start and an exclusive end.
    pub fn new(start_ms: i64, end_ms_exclusive: i64) -> Self {
        Self {
            start_ms,
            end_ms_exclusive,
        }
    }

    /// Width of the window in milliseconds. Saturates to `0` if the range
    /// is empty or inverted; the source must treat that case as "no
    /// samples to return" rather than an error (it is a plausible
    /// byproduct of operator-side lookback arithmetic at the start of a
    /// query).
    pub fn duration_ms(&self) -> i64 {
        self.end_ms_exclusive.saturating_sub(self.start_ms).max(0)
    }

    /// `true` when the window covers zero milliseconds.
    pub fn is_empty(&self) -> bool {
        self.end_ms_exclusive <= self.start_ms
    }
}

/// Opaque handle the source hands out via [`SeriesSource::resolve`] and
/// the caller later threads into [`SampleHint::series`].
///
/// Conceptually a `(bucket, series_id)` pair — kept as a struct so the
/// source can extend it (e.g. with a cross-bucket fingerprint cache
/// token) without churning the trait. Callers treat the contents as
/// opaque: pass them back in, do not interpret them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResolvedSeriesRef {
    /// Bucket the series lives in. Bucket identifiers are hourly in the
    /// storage layer; the source chooses the representation.
    pub bucket_id: u64,
    /// Bucket-scoped series identifier (matches
    /// [`crate::model::SeriesId`] in the existing storage layer, kept as
    /// `u32` here to avoid leaking the crate-private alias).
    pub series_id: u32,
}

impl ResolvedSeriesRef {
    /// Build a handle from its two parts.
    pub fn new(bucket_id: u64, series_id: u32) -> Self {
        Self {
            bucket_id,
            series_id,
        }
    }
}

/// Contiguous slice of resolved-series metadata streamed from
/// [`SeriesSource::resolve`].
///
/// Each chunk describes **some** of the series that matched the
/// selector; a selector may produce many chunks (e.g. one per bucket).
/// Chunks are self-describing: the caller does not need to remember
/// previous chunks' handles to make use of a later one.
///
/// Invariants:
/// - `labels.len() == series.len()`. The `i`th series' labelset lives at
///   `labels[i]` and its handle at `series[i]`.
/// - `bucket_id` is the single bucket every handle in this chunk
///   belongs to (`series[i].bucket_id == bucket_id`). Kept as an
///   explicit field so the caller does not have to re-derive it when
///   grouping samples per bucket.
///
/// The adapter in unit 2.2 extends this shape if it needs to carry
/// additional per-series plan hints (e.g. fingerprints); keep the
/// minimum here.
#[derive(Debug, Clone)]
pub struct ResolvedSeriesChunk {
    /// Bucket every handle in this chunk belongs to.
    pub bucket_id: u64,
    /// Labelsets, dense-indexed alongside [`Self::series`].
    pub labels: Arc<[Labels]>,
    /// Opaque handles the caller threads back into [`SampleHint::series`].
    pub series: Arc<[ResolvedSeriesRef]>,
}

/// Cardinality estimate returned by [`SeriesSource::estimate_cardinality`].
///
/// The planner multiplies `series_count_estimate() * steps` against the
/// configured cell-limit and rejects with
/// [`QueryError::TooLarge`](super::memory::QueryError::TooLarge) before
/// any sample state is materialised (RFC 0007 §"Execution Model"
/// "Fail-fast cardinality gate").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CardinalityEstimate {
    /// Lower bound on the number of series the selector will resolve
    /// to. `0` means "possibly none".
    pub series_lower_bound: u64,
    /// Upper bound on the number of series the selector will resolve
    /// to. Used as the representative count unless the source flagged
    /// the estimate as approximate and the lower bound is tighter.
    pub series_upper_bound: u64,
    /// `true` when the estimate is best-effort (e.g. derived from
    /// per-bucket summary statistics rather than a precise matcher
    /// evaluation). Callers that care about an upper bound should still
    /// use [`Self::series_upper_bound`]; the flag tells observability
    /// that the number is not authoritative.
    pub approx: bool,
}

impl CardinalityEstimate {
    /// Build an exact estimate where lower and upper bound match and the
    /// result is not flagged approximate.
    pub fn exact(series_count: u64) -> Self {
        Self {
            series_lower_bound: series_count,
            series_upper_bound: series_count,
            approx: false,
        }
    }

    /// Representative series count for the planner's cardinality gate.
    ///
    /// Returns the upper bound (the conservative choice — rejecting a
    /// query that would have fit is preferable to accepting a query
    /// that will OOM). When the source flagged the estimate as
    /// approximate the planner is still free to consult both bounds
    /// directly.
    pub fn series_count_estimate(&self) -> u64 {
        self.series_upper_bound
    }
}

/// Raw-sample fetch request.
///
/// Carries the post-resolution series set and the absolute time window
/// the caller needs. The source must **not** widen `series` and must
/// **not** shift `time_range`. The hint intentionally does not carry
/// step, lookback, `@`, offset, or rollup-function hints: pushdown was
/// rejected for v1 because `@ start()`/`@ end()` inside subqueries
/// would force every storage backend to re-implement evaluator
/// semantics (RFC 0007 §"Storage Contract" and §5 Decisions Log).
#[derive(Debug, Clone)]
pub struct SampleHint {
    /// Complete post-resolution series set, matching handles the source
    /// previously emitted from [`SeriesSource::resolve`]. The caller
    /// owns the ordering; batches the source returns carry
    /// `series_range` indices into this slice.
    pub series: Arc<[ResolvedSeriesRef]>,
    /// Absolute window the caller wants samples for. Already folded in
    /// maximum lookback / offset / subquery shift at the operator
    /// level.
    pub time_range: TimeRange,
}

impl SampleHint {
    /// Build a hint from a pre-resolved series slice and a time range.
    pub fn new(series: Arc<[ResolvedSeriesRef]>, time_range: TimeRange) -> Self {
        Self { series, time_range }
    }
}

/// Per-series column of raw samples returned inside a [`SampleBatch`].
///
/// Columnar layout: one `timestamps` and one `values` vector per
/// series, length-matched, in timestamp order.
/// Stale markers are preserved as
/// [`STALE_NAN`](crate::model::STALE_NAN); the caller distinguishes
/// them via [`crate::model::is_stale_nan`] (RFC 0007 §"Storage Contract",
/// source → caller contract).
#[derive(Debug, Clone)]
pub struct SampleBlock {
    /// Per-series timestamp columns. `timestamps[i]` and `values[i]`
    /// share a length. Indexed alongside [`SampleBatch::series_range`]:
    /// block index `j` corresponds to `hint.series[series_range.start + j]`.
    pub timestamps: Vec<Vec<i64>>,
    /// Per-series value columns. Aligned with [`Self::timestamps`].
    pub values: Vec<Vec<f64>>,
}

impl SampleBlock {
    /// Build an empty block sized for `series_count` series, ready for
    /// the source to push per-series samples into.
    pub fn with_series_count(series_count: usize) -> Self {
        Self {
            timestamps: (0..series_count).map(|_| Vec::new()).collect(),
            values: (0..series_count).map(|_| Vec::new()).collect(),
        }
    }

    /// Number of series columns the block carries.
    pub fn series_count(&self) -> usize {
        self.timestamps.len()
    }
}

/// Raw-sample batch returned by [`SeriesSource::samples`].
///
/// One batch covers a contiguous slice of the caller's series list and
/// (implicitly) a contiguous sub-range of the hint's `time_range`.
/// Samples inside the batch are ordered per series in timestamp order,
/// with exact source timestamps (no re-alignment). The batch size is
/// the source's choice: the planner's allocator uses [`SampleBlock`]
/// shape to size operator-side buffers.
#[derive(Debug, Clone)]
pub struct SampleBatch {
    /// Slice into the original `SampleHint::series` covered by this
    /// batch. `series_range.end <= hint.series.len()`.
    pub series_range: Range<usize>,
    /// Dense, per-series columnar samples (timestamps in ms, values as
    /// `f64`; stale markers preserved as `STALE_NAN`).
    pub samples: SampleBlock,
}

/// Storage contract for the v2 execution engine.
///
/// See RFC 0007 §"Storage Contract" for the full caller/source
/// invariants. Summary:
///
/// - `resolve` streams series metadata; `series` in each chunk is a
///   contiguous slice the caller can append to a global roster.
/// - `estimate_cardinality` is a cheap pre-resolve gate (RFC
///   §"Execution Model" "Fail-fast cardinality gate").
/// - `samples` streams raw samples for a post-resolution series set
///   and an absolute time window. The source must not widen the
///   series set and must not shift the window. Samples come back in
///   timestamp order per series with exact timestamps. Stale markers
///   are preserved as [`STALE_NAN`](crate::model::STALE_NAN).
///
/// The trait uses return-position `impl Future` / `impl Stream` (stable
/// RPITIT) so concrete adapters do not box on the hot path. This makes
/// the trait **not** dyn-safe; the planner binds a generic
/// `S: SeriesSource` rather than a trait object. See §5 Decisions Log
/// (unit 2.1) for the tradeoff.
pub trait SeriesSource: Send + Sync {
    /// Resolve a selector to series metadata, streamed per bucket (or
    /// finer). The planner consumes the stream into a plan-time series
    /// roster; each [`ResolvedSeriesChunk`] carries the handles the
    /// caller later threads back into [`SampleHint::series`].
    fn resolve(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
    ) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send;

    /// Estimate the series cardinality of a selector over a time range.
    /// Must be cheap (sub-millisecond target) — the planner calls this
    /// before `resolve` to fail-fast on oversized queries.
    fn estimate_cardinality(
        &self,
        selector: &VectorSelector,
        time_range: TimeRange,
    ) -> impl Future<Output = Result<CardinalityEstimate, QueryError>> + Send;

    /// Stream raw samples for a pre-resolved series set over an
    /// absolute time window. No step, no lookback, no rollup. Samples
    /// per series are emitted in timestamp order with exact source
    /// timestamps; stale markers are preserved as
    /// [`STALE_NAN`](crate::model::STALE_NAN).
    fn samples(
        &self,
        hint: SampleHint,
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
        // given: a bucket id and a bucket-scoped series id
        let bucket = 42u64;
        let sid = 7u32;

        // when: mint a handle
        let handle = ResolvedSeriesRef::new(bucket, sid);

        // then: the struct preserves both fields and is hashable / copy
        assert_eq!(handle.bucket_id, bucket);
        assert_eq!(handle.series_id, sid);
        let copy = handle;
        assert_eq!(copy, handle);
    }

    #[test]
    fn should_treat_cardinality_estimate_upper_bound_as_representative() {
        // given: a range estimate
        let est = CardinalityEstimate {
            series_lower_bound: 100,
            series_upper_bound: 250,
            approx: true,
        };

        // when: ask for the representative count
        let n = est.series_count_estimate();

        // then: planner-facing count is the upper bound (conservative)
        assert_eq!(n, 250);
    }

    #[test]
    fn should_build_exact_cardinality_estimate_with_matching_bounds() {
        // given: a known-precise count
        // when: build an exact estimate
        let est = CardinalityEstimate::exact(17);

        // then: bounds collapse and the estimate is not flagged approx
        assert_eq!(est.series_lower_bound, 17);
        assert_eq!(est.series_upper_bound, 17);
        assert!(!est.approx);
        assert_eq!(est.series_count_estimate(), 17);
    }

    #[test]
    fn should_build_sample_hint_from_resolved_series_and_time_range() {
        // given: a caller-resolved series slice and an absolute window
        let series: Arc<[ResolvedSeriesRef]> = Arc::from(vec![
            ResolvedSeriesRef::new(1, 10),
            ResolvedSeriesRef::new(1, 11),
        ]);
        let tr = TimeRange::new(0, 60_000);

        // when: build the hint
        let hint = SampleHint::new(series.clone(), tr);

        // then: series slice and time range round-trip; hint is clone
        assert_eq!(hint.series.len(), 2);
        assert_eq!(hint.time_range, tr);
        let cloned = hint.clone();
        assert_eq!(cloned.series.len(), hint.series.len());
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

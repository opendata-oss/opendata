//! `MatrixSelectorOp` — the storage leaf for PromQL range vectors
//! (`metric[5m]`). It fetches raw samples and repackages them into
//! per-step windows for [`RollupOp`](super::rollup::RollupOp) (`rate`, `*_over_time`, ...) and for
//! [`SubqueryOp`](super::subquery::SubqueryOp) to consume.
//!
//! `metric[range]` produces a *vector of samples* per step, which doesn't
//! fit [`StepBatch`]'s one-float-per-cell shape. So this operator doesn't
//! emit `StepBatch`es on its main `Operator::next` loop (that path is a
//! degenerate "immediate EOS"); the real output is
//! [`MatrixSelectorOp::windows`], which emits [`MatrixWindowBatch`]es.
//! [`RollupOp`](super::rollup::RollupOp) drives this via the [`super::rollup::WindowStream`]
//! trait, which [`super::rollup::MatrixWindowSource`] implements over a
//! `MatrixSelectorOp`.
//!
//! Per-step semantics (no `lookback_delta` — the bracketed `[range]`
//! replaces it):
//! ```text
//!   pin         = @ value when set, else t
//!   effective   = pin - offset
//!   window      = (effective - range, effective]
//!   samples     = source samples in window, ascending, STALE_NAN dropped
//! ```
//!
//! Tiling: one [`MatrixWindowBatch`] per `(series_chunk × step_chunk)` —
//! the same two-level tiling scheme as
//! [`VectorSelectorOp`](super::vector_selector::VectorSelectorOp).
//!
//! [`MatrixWindowBatch`] layout: flat `timestamps` / `values` buffers plus
//! a row-major-by-step `cells: Vec<CellIndex>` where
//! `cells[t * series_count + s] = { offset, len }` indexes into the flat
//! buffers. Adjacent-step cells for the same series advance monotonically,
//! so `RollupOp`'s two-pointer driver reuses state across steps.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::StreamExt;
use promql_parser::parser::{AtModifier, Offset};

use crate::model::is_stale_nan;
use crate::promql::timestamp::Timestamp;

use super::super::batch::{SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::source::{
    ResolvedSeriesRef, SampleBatch, SamplesRequest, SeriesSource, TimeRange,
};

// ---------------------------------------------------------------------------
// Defaults & tile shape (mirrors 3a.1)
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_STEP_CHUNK: usize = 64;
pub(crate) const DEFAULT_SERIES_CHUNK: usize = 512;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchShape {
    pub(crate) step_chunk: usize,
    pub(crate) series_chunk: usize,
}

impl BatchShape {
    pub(crate) fn new(step_chunk: usize, series_chunk: usize) -> Self {
        assert!(step_chunk > 0, "step_chunk must be > 0");
        assert!(series_chunk > 0, "series_chunk must be > 0");
        Self {
            step_chunk,
            series_chunk,
        }
    }
}

impl Default for BatchShape {
    fn default() -> Self {
        Self::new(DEFAULT_STEP_CHUNK, DEFAULT_SERIES_CHUNK)
    }
}

// ---------------------------------------------------------------------------
// Window batch type — the "secondary API" shape
// ---------------------------------------------------------------------------

/// Index into a [`MatrixWindowBatch`]'s flat sample buffer for a single
/// `(step, series)` cell.
///
/// `offset` and `len` are `u32` because a single window cell is bounded
/// by the range's sample count at scrape resolution; billions of samples
/// in one cell would not fit the operator's memory reservation anyway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellIndex {
    /// Start index into [`MatrixWindowBatch::timestamps`] / [`MatrixWindowBatch::values`].
    pub offset: u32,
    /// Number of samples in the cell's window (`0` for an empty window).
    pub len: u32,
}

impl CellIndex {
    /// Empty cell (no samples in the window).
    pub const EMPTY: Self = Self { offset: 0, len: 0 };

    /// `(start, end)` bounds for slicing the flat sample buffer.
    #[inline]
    pub fn range(&self) -> std::ops::Range<usize> {
        let start = self.offset as usize;
        let end = start + self.len as usize;
        start..end
    }
}

/// The window-batch counterpart to [`StepBatch`]: a rectangle of *sample
/// lists* (one list of `(ts, val)` pairs per `(step, series)` cell)
/// instead of single values. This is what flows between range-vector
/// producers ([`MatrixSelectorOp`], [`SubqueryOp`]) and
/// [`RollupOp`](super::rollup::RollupOp).
///
/// Covers a contiguous `(series_range × step_range)` tile. Samples for
/// each cell are packed into the flat `timestamps` / `values` columns and
/// indexed per cell via [`Self::cells`] (row-major by step, matching
/// [`StepBatch`]'s layout).
///
/// Consumers read per-cell slices via [`Self::cell_samples`]. `STALE_NAN`
/// values are already filtered out by the producer — consumers see only
/// valid numeric samples in ascending timestamp order.
///
/// [`SubqueryOp`]: super::subquery::SubqueryOp
#[derive(Debug, Clone)]
pub struct MatrixWindowBatch {
    /// Absolute step timestamps (ms), shared with the rest of the query
    /// via `Arc` just like [`StepBatch::step_timestamps`].
    pub step_timestamps: Arc<[i64]>,
    /// Slice of [`Self::step_timestamps`] covered by this batch.
    pub step_range: std::ops::Range<usize>,

    /// Series roster the planner built.
    pub series: SchemaRef,
    /// Slice of the series roster covered by this batch.
    pub series_range: std::ops::Range<usize>,

    /// Flat per-cell sample timestamps (ms), packed in row-major step ×
    /// series order. A cell's slice lives at [`CellIndex::range`].
    pub timestamps: Vec<i64>,
    /// Flat per-cell sample values, parallel to [`Self::timestamps`].
    pub values: Vec<f64>,
    /// Per-cell index. Length is `step_count * series_count`, row-major
    /// by step (cell `(t_off, s_off)` lives at `t_off * series_count +
    /// s_off`).
    pub cells: Vec<CellIndex>,

    /// Optional per-step **effective** timestamps — the window-end each
    /// step's samples actually live under after folding `@` / `offset`.
    /// `None` means the effective time equals the step timestamp (the
    /// common no-modifier case). When present, shares layout with
    /// [`Self::step_timestamps`] (absolute, indexed by global step idx)
    /// so consumers slice via `step_range` the same way.
    ///
    /// [`RollupOp`](super::rollup::RollupOp) prefers this over `step_timestamps` when computing
    /// `(window_start, window_end)` for rate-family extrapolation; without
    /// it, `rate(metric[100s] @ 100)` at outer step `t=25s` would compute
    /// rate over the window `(-75s, 25s]` while the packed samples
    /// actually cover `(0, 100s]`, producing a negative rate disjoint
    /// from the data.
    pub effective_times: Option<Arc<[i64]>>,
}

impl MatrixWindowBatch {
    /// Steps covered by this batch.
    #[inline]
    pub fn step_count(&self) -> usize {
        self.step_range.end - self.step_range.start
    }

    /// Series covered by this batch.
    #[inline]
    pub fn series_count(&self) -> usize {
        self.series_range.end - self.series_range.start
    }

    /// Total cell count (`step_count * series_count`).
    #[inline]
    pub fn len(&self) -> usize {
        self.step_count() * self.series_count()
    }

    /// `true` if the batch covers zero cells.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Samples for the `(step_off, series_off)` cell (offsets are
    /// batch-local, not grid-global).
    pub fn cell_samples(&self, step_off: usize, series_off: usize) -> (&[i64], &[f64]) {
        debug_assert!(step_off < self.step_count());
        debug_assert!(series_off < self.series_count());
        let idx = step_off * self.series_count() + series_off;
        let r = self.cells[idx].range();
        (&self.timestamps[r.clone()], &self.values[r])
    }
}

// ---------------------------------------------------------------------------
// Memory-guarded window-batch buffers
// ---------------------------------------------------------------------------

/// Conservative byte estimate for a per-cell index array + flat sample
/// buffer with `cells` cells and `samples` total samples.
#[inline]
fn window_bytes(cells: usize, samples: usize) -> usize {
    let cell_bytes = cells.saturating_mul(std::mem::size_of::<CellIndex>());
    let ts_bytes = samples.saturating_mul(std::mem::size_of::<i64>());
    let val_bytes = samples.saturating_mul(std::mem::size_of::<f64>());
    cell_bytes
        .saturating_add(ts_bytes)
        .saturating_add(val_bytes)
}

/// Per-series sample column byte estimate.
#[inline]
fn samples_bytes(n: usize) -> usize {
    n.saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>())
}

/// RAII reservation wrapper around an in-flight window batch's buffers.
///
/// Reserves on construction (cells-only, up front), grows as samples are
/// pushed, and releases the entire reservation on [`Drop`]. Callers move
/// the inner vectors out via [`Self::finish`], which transfers ownership
/// of the bytes and releases the reservation slice (downstream
/// re-reserves if it holds the batch).
struct WindowBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    cells: Vec<CellIndex>,
}

impl WindowBuffers {
    /// Reserve space for `cell_count` cell indices (samples accrue via
    /// [`Self::grow_samples`] as the driver walks the window).
    fn allocate(reservation: &MemoryReservation, cell_count: usize) -> Result<Self, QueryError> {
        let bytes = window_bytes(cell_count, 0);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            timestamps: Vec::new(),
            values: Vec::new(),
            cells: vec![CellIndex::EMPTY; cell_count],
        })
    }

    /// Reserve incremental bytes for `extra` additional samples before
    /// pushing. Returns `MemoryLimit` without pushing on reject.
    fn grow_samples(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        let bytes = extra.saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>());
        self.reservation.try_grow(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

    /// Consume the buffers into an owned window batch body.
    fn finish(mut self) -> (Vec<i64>, Vec<f64>, Vec<CellIndex>) {
        let ts = std::mem::take(&mut self.timestamps);
        let vs = std::mem::take(&mut self.values);
        let cells = std::mem::take(&mut self.cells);
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (ts, vs, cells)
    }
}

impl Drop for WindowBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Effective time computation (mirrors 3a.1's `EffectiveTimes` but does
// not subtract lookback — the explicit `[range]` replaces it).
// ---------------------------------------------------------------------------

/// Pre-computed per-step evaluation times after applying `@` then
/// `offset`. Range is **not** folded in here; the operator subtracts it
/// per-step when sliding the window.
#[derive(Debug, Clone)]
struct EffectiveTimes {
    times: Arc<[i64]>,
}

impl EffectiveTimes {
    fn compute(
        step_timestamps: &[i64],
        grid: &StepGrid,
        at: Option<&AtModifier>,
        offset: Option<&Offset>,
    ) -> Self {
        // Phase 1: pin.
        let times: Vec<i64> = match at {
            Some(AtModifier::At(t)) => {
                let pin = Timestamp::from(*t).as_millis();
                vec![pin; step_timestamps.len()]
            }
            Some(AtModifier::Start) => vec![grid.start_ms; step_timestamps.len()],
            Some(AtModifier::End) => vec![grid.end_ms; step_timestamps.len()],
            None => step_timestamps.to_vec(),
        };
        // Phase 2: apply offset (matches evaluator.rs:1749-1764).
        let offset_ms = match offset {
            Some(Offset::Pos(d)) => -(d.as_millis() as i64),
            Some(Offset::Neg(d)) => d.as_millis() as i64,
            None => 0,
        };
        let shifted: Vec<i64> = times
            .into_iter()
            .map(|t| t.saturating_add(offset_ms))
            .collect();
        Self {
            times: Arc::from(shifted),
        }
    }

    #[inline]
    fn get(&self, step_idx: usize) -> i64 {
        self.times[step_idx]
    }

    /// Time window the source must cover so the operator has samples for
    /// every step's `(effective - range, effective]` slot.
    fn time_range_with_range(&self, range_ms: i64) -> TimeRange {
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for &t in self.times.iter() {
            if t < min {
                min = t;
            }
            if t > max {
                max = t;
            }
        }
        if min == i64::MAX {
            return TimeRange::new(0, 0);
        }
        // Window is `(t - range, t]`; source needs samples in
        // `[min - range + 1, max + 1)` (inclusive-exclusive TimeRange
        // convention).
        let start = min.saturating_sub(range_ms).saturating_add(1);
        let end = max.saturating_add(1);
        TimeRange::new(start, end)
    }
}

// ---------------------------------------------------------------------------
// Per-chunk sample state (same shape as 3a.1's ChunkSamples)
// ---------------------------------------------------------------------------

/// Per-series sample columns for the current series chunk.
struct ChunkSamples {
    reservation: MemoryReservation,
    bytes: usize,
    timestamps: Vec<Vec<i64>>,
    values: Vec<Vec<f64>>,
}

impl ChunkSamples {
    fn new(reservation: MemoryReservation, chunk_len: usize) -> Self {
        Self {
            reservation,
            bytes: 0,
            timestamps: (0..chunk_len).map(|_| Vec::new()).collect(),
            values: (0..chunk_len).map(|_| Vec::new()).collect(),
        }
    }

    fn absorb(
        &mut self,
        batch: SampleBatch,
        request_to_series: &[usize],
    ) -> Result<(), QueryError> {
        let mut total_new = 0usize;
        for col in batch.samples.timestamps.iter() {
            total_new = total_new.saturating_add(col.len());
        }
        let bytes = samples_bytes(total_new);
        self.reservation.try_grow(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);

        for (block_idx, (mut ts_col, mut val_col)) in batch
            .samples
            .timestamps
            .into_iter()
            .zip(batch.samples.values)
            .enumerate()
        {
            let request_idx = batch.series_range.start + block_idx;
            let local_idx = request_to_series[request_idx];
            self.timestamps[local_idx].append(&mut ts_col);
            self.values[local_idx].append(&mut val_col);
        }
        Ok(())
    }
}

impl Drop for ChunkSamples {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// Sliding-walk driver — the correctness-sensitive bit
// ---------------------------------------------------------------------------

/// Two-pointer cursor for a single series' sample column.
///
/// Invariant: `lo` is the index of the first sample whose timestamp is
/// `> window_lo_exclusive` (exclusive lower bound). `hi` is one past the
/// index of the last sample whose timestamp is `<= window_hi_inclusive`.
/// Callers advance both monotonically as the window slides forward.
///
/// Because `effective` times are monotonic in the step index **only when
/// `@` is not set and offset is constant** — which is the common case —
/// but the operator must also handle `@`-pinned cases (all windows
/// identical) and other non-monotonic configurations, the driver
/// *resets* the cursors when it detects the window moved backward. This
/// keeps the code correct at the cost of a single `O(samples_per_series)`
/// scan per reset; in the common monotonic case the two-pointer sweep is
/// amortised O(samples_per_series) across all steps.
struct SeriesCursor {
    lo: usize,
    hi: usize,
    /// Last window `(lo_excl, hi_incl]` processed, used to detect
    /// backward jumps that require a reset.
    last_window_hi: i64,
}

impl SeriesCursor {
    fn new() -> Self {
        Self {
            lo: 0,
            hi: 0,
            last_window_hi: i64::MIN,
        }
    }

    /// Advance the cursor to cover `(window_lo_exclusive, window_hi_inclusive]`
    /// in `timestamps`. Returns the `[lo, hi)` range of samples in-window.
    ///
    /// `timestamps` is assumed ascending. `STALE_NAN` exclusion is *not*
    /// performed here — the caller filters while packing into the window
    /// batch, so the cursor can stay simple and integer-only.
    fn advance(
        &mut self,
        timestamps: &[i64],
        window_lo_exclusive: i64,
        window_hi_inclusive: i64,
    ) -> std::ops::Range<usize> {
        // Backward jump (e.g. `@` pinning all steps to a fixed time, then
        // a forward-shifted offset): reset.
        if window_hi_inclusive < self.last_window_hi {
            self.lo = 0;
            self.hi = 0;
        }
        self.last_window_hi = window_hi_inclusive;

        // Advance `lo` past samples <= window_lo_exclusive.
        while self.lo < timestamps.len() && timestamps[self.lo] <= window_lo_exclusive {
            self.lo += 1;
        }
        // `hi` may be behind `lo` after a reset — pull it up.
        if self.hi < self.lo {
            self.hi = self.lo;
        }
        // Advance `hi` past samples <= window_hi_inclusive.
        while self.hi < timestamps.len() && timestamps[self.hi] <= window_hi_inclusive {
            self.hi += 1;
        }
        // If `lo` moved past `hi` (window slid entirely past earlier
        // samples), normalise.
        if self.lo > self.hi {
            self.hi = self.lo;
        }
        self.lo..self.hi
    }
}

// ---------------------------------------------------------------------------
// Operator state machine
// ---------------------------------------------------------------------------

type SampleStream<'a> = Pin<Box<dyn Stream<Item = Result<SampleBatch, QueryError>> + Send + 'a>>;

enum State<'a> {
    Init,
    LoadingChunk {
        chunk_start: usize,
        chunk_len: usize,
        #[allow(clippy::type_complexity)]
        future: Pin<Box<dyn Future<Output = Result<ChunkSamples, QueryError>> + Send + 'a>>,
    },
    Emitting {
        chunk_start: usize,
        chunk_len: usize,
        samples: Box<ChunkSamples>,
        /// Per-series cursor for the current series chunk. `cursors[i]`
        /// tracks chunk-local series `i`.
        cursors: Vec<SeriesCursor>,
        /// Next step chunk's starting index within the full grid.
        next_step_chunk_start: usize,
    },
    Done,
    Errored,
    Transitioning,
}

// ---------------------------------------------------------------------------
// Operator struct
// ---------------------------------------------------------------------------

/// Storage leaf for PromQL range vectors (`metric[range]`). Fetches raw
/// samples from a [`SeriesSource`] and packs them into per-step windows
/// for a downstream [`RollupOp`](super::rollup::RollupOp) to reduce.
///
/// See module docs for the two-API arrangement (degenerate
/// `Operator::next`, plus the useful [`Self::windows`] secondary API) and
/// for the sliding-window semantics.
pub(crate) struct MatrixSelectorOp<'a, S: SeriesSource + 'a> {
    // Plan-time inputs ------------------------------------------------------
    source: Arc<S>,
    request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
    schema: OperatorSchema,
    step_timestamps: Arc<[i64]>,
    effective_times: EffectiveTimes,
    /// `true` when `@` or `offset` shifts the per-step effective time away
    /// from `step_timestamps`. When set, [`MatrixWindowBatch`] carries the
    /// `effective_times` array so the enclosing `RollupOp` can compute
    /// rate-family extrapolation over the window the samples actually
    /// live in.
    has_effective_shift: bool,
    range_ms: i64,
    shape: BatchShape,
    reservation: MemoryReservation,

    // Runtime state ---------------------------------------------------------
    state: State<'a>,
}

impl<'a, S: SeriesSource + Send + Sync + 'a> MatrixSelectorOp<'a, S> {
    /// Build an operator from resolved inputs.
    ///
    /// * `source` — storage handle.
    /// * `series` — post-resolve series roster.
    /// * `request_series` — parallel per-logical-series opaque source handle
    ///   groups. A single logical series may map to several bucket-local refs
    ///   after planner-side roster deduplication.
    /// * `grid` — **outer** step grid the query runs on. Downstream
    ///   operators that fuse (`Rollup`, `Subquery`) still see this grid
    ///   via [`Operator::schema`].
    /// * `at` / `offset` — selector modifiers.
    /// * `range_ms` — explicit bracketed window in ms (e.g. `[5m]` →
    ///   `300_000`). Must be `> 0`.
    /// * `reservation` — per-query reservation (cloned).
    /// * `shape` — tile dimensions; [`BatchShape::default`] outside tests.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: Arc<S>,
        series: Arc<SeriesSchema>,
        request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
        grid: StepGrid,
        at: Option<AtModifier>,
        offset: Option<Offset>,
        range_ms: i64,
        reservation: MemoryReservation,
        shape: BatchShape,
    ) -> Self {
        assert_eq!(
            series.len(),
            request_series.len(),
            "series roster and request_series must be length-aligned",
        );
        assert!(range_ms > 0, "matrix range must be > 0 ms");
        let step_timestamps: Arc<[i64]> = Arc::from(
            (0..grid.step_count)
                .map(|k| grid.start_ms + (k as i64) * grid.step_ms)
                .collect::<Vec<_>>(),
        );
        let effective_times =
            EffectiveTimes::compute(&step_timestamps, &grid, at.as_ref(), offset.as_ref());
        let has_effective_shift = at.is_some()
            || matches!(
                offset,
                Some(Offset::Pos(d)) | Some(Offset::Neg(d)) if d.as_millis() > 0
            );
        let schema = OperatorSchema::new(SchemaRef::Static(series), grid);
        Self {
            source,
            request_series,
            schema,
            step_timestamps,
            effective_times,
            has_effective_shift,
            range_ms,
            shape,
            reservation,
            state: State::Init,
        }
    }

    fn total_series(&self) -> usize {
        self.request_series.len()
    }

    fn chunk_request(&self, chunk_start: usize, chunk_end: usize) -> (SamplesRequest, Vec<usize>) {
        let mut flat: Vec<ResolvedSeriesRef> = Vec::new();
        let mut request_to_series: Vec<usize> = Vec::new();
        for (series_off, series_refs) in self.request_series[chunk_start..chunk_end]
            .iter()
            .enumerate()
        {
            debug_assert!(
                !series_refs.is_empty(),
                "every logical series must have at least one source handle",
            );
            for sref in series_refs.iter() {
                flat.push(sref.clone());
                request_to_series.push(series_off);
            }
        }
        let window = self.effective_times.time_range_with_range(self.range_ms);
        (
            SamplesRequest::new(Arc::from(flat), window),
            request_to_series,
        )
    }

    fn start_chunk_load(&mut self, chunk_start: usize) -> State<'a>
    where
        S: 'a,
    {
        let chunk_end = (chunk_start + self.shape.series_chunk).min(self.total_series());
        let chunk_len = chunk_end - chunk_start;
        let (request, request_to_series) = self.chunk_request(chunk_start, chunk_end);
        let source = self.source.clone();
        let reservation = self.reservation.clone();

        let future = Box::pin(async move {
            let mut samples = ChunkSamples::new(reservation, chunk_len);
            let stream = source.samples(request);
            let mut stream: SampleStream<'_> = Box::pin(stream);
            while let Some(item) = stream.next().await {
                let batch = item?;
                samples.absorb(batch, &request_to_series)?;
            }
            let _ = chunk_start;
            Ok(samples)
        });

        State::LoadingChunk {
            chunk_start,
            chunk_len,
            future,
        }
    }

    fn build_window_batch(
        &self,
        chunk_start: usize,
        chunk_len: usize,
        samples: &ChunkSamples,
        cursors: &mut [SeriesCursor],
        step_chunk_start: usize,
    ) -> Result<MatrixWindowBatch, QueryError> {
        let grid = &self.schema.step_grid;
        let step_chunk_end = (step_chunk_start + self.shape.step_chunk).min(grid.step_count);
        let step_count = step_chunk_end - step_chunk_start;
        let cell_count = step_count * chunk_len;

        let mut buffers = WindowBuffers::allocate(&self.reservation, cell_count)?;

        for step_off in 0..step_count {
            let step_idx = step_chunk_start + step_off;
            let effective = self.effective_times.get(step_idx);
            let window_lo = effective.saturating_sub(self.range_ms); // exclusive
            let window_hi = effective; // inclusive

            for (series_off, cursor) in cursors.iter_mut().enumerate().take(chunk_len) {
                let ts = &samples.timestamps[series_off];
                let vs = &samples.values[series_off];

                // Integer-only cursor advance first — STALE_NAN filter
                // happens while packing.
                let window_range = cursor.advance(ts, window_lo, window_hi);

                // Count non-stale samples up front so we can grow the
                // reservation once per cell rather than once per sample.
                let mut in_cell: usize = 0;
                for i in window_range.clone() {
                    if !is_stale_nan(vs[i]) {
                        in_cell += 1;
                    }
                }
                buffers.grow_samples(in_cell)?;

                let cell_offset = buffers.timestamps.len() as u32;
                for i in window_range {
                    let v = vs[i];
                    if is_stale_nan(v) {
                        continue;
                    }
                    buffers.timestamps.push(ts[i]);
                    buffers.values.push(v);
                }
                let cell_idx = step_off * chunk_len + series_off;
                buffers.cells[cell_idx] = CellIndex {
                    offset: cell_offset,
                    len: in_cell as u32,
                };
            }
        }

        let (timestamps, values, cells) = buffers.finish();
        let series_range = chunk_start..(chunk_start + chunk_len);
        let step_range = step_chunk_start..step_chunk_end;
        let effective_times = if self.has_effective_shift {
            Some(self.effective_times.times.clone())
        } else {
            None
        };
        Ok(MatrixWindowBatch {
            step_timestamps: self.step_timestamps.clone(),
            step_range,
            series: self.schema.series.clone(),
            series_range,
            timestamps,
            values,
            cells,
            effective_times,
        })
    }

    /// Secondary API — the useful one.
    ///
    /// Polls for the next window batch. Consumers (`Rollup`, `Subquery`)
    /// drive the operator through this method rather than
    /// [`Operator::next`], which is a degenerate end-of-stream for this
    /// operator.
    pub(crate) fn windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>>
    where
        S: 'a,
    {
        loop {
            let state = std::mem::replace(&mut self.state, State::Transitioning);
            match state {
                State::Init => {
                    if self.total_series() == 0 || self.schema.step_grid.step_count == 0 {
                        self.state = State::Done;
                        return Poll::Ready(None);
                    }
                    self.state = self.start_chunk_load(0);
                }
                State::LoadingChunk {
                    chunk_start,
                    chunk_len,
                    mut future,
                } => match future.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = State::LoadingChunk {
                            chunk_start,
                            chunk_len,
                            future,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(samples)) => {
                        let cursors = (0..chunk_len).map(|_| SeriesCursor::new()).collect();
                        self.state = State::Emitting {
                            chunk_start,
                            chunk_len,
                            samples: Box::new(samples),
                            cursors,
                            next_step_chunk_start: 0,
                        };
                    }
                    Poll::Ready(Err(err)) => {
                        self.state = State::Errored;
                        return Poll::Ready(Some(Err(err)));
                    }
                },
                State::Emitting {
                    chunk_start,
                    chunk_len,
                    samples,
                    mut cursors,
                    next_step_chunk_start,
                } => {
                    let grid = &self.schema.step_grid;
                    if next_step_chunk_start >= grid.step_count {
                        let next_chunk_start = chunk_start + chunk_len;
                        drop(samples);
                        if next_chunk_start >= self.total_series() {
                            self.state = State::Done;
                            return Poll::Ready(None);
                        }
                        self.state = self.start_chunk_load(next_chunk_start);
                        continue;
                    }
                    match self.build_window_batch(
                        chunk_start,
                        chunk_len,
                        &samples,
                        &mut cursors,
                        next_step_chunk_start,
                    ) {
                        Ok(batch) => {
                            let step_advance = batch.step_count();
                            self.state = State::Emitting {
                                chunk_start,
                                chunk_len,
                                samples,
                                cursors,
                                next_step_chunk_start: next_step_chunk_start + step_advance,
                            };
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Err(err) => {
                            self.state = State::Errored;
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
                State::Done | State::Errored => {
                    self.state = State::Done;
                    return Poll::Ready(None);
                }
                State::Transitioning => {
                    unreachable!("transient state observed in windows()");
                }
            }
        }
    }
}

// Operator trait compliance — the `Operator::next` surface is degenerate
// for this operator (matrix output cannot fit `StepBatch`'s single-float
// cell shape). Consumers use `MatrixSelectorOp::windows` instead.
impl<S: SeriesSource + Send + Sync + 'static> Operator for MatrixSelectorOp<'static, S> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    /// Degenerate: immediately returns end-of-stream. See the module
    /// docs and [`MatrixSelectorOp::windows`].
    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        Poll::Ready(None)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use promql_parser::parser::VectorSelector;
    use std::future::ready;
    use std::task::{RawWaker, RawWakerVTable, Waker};
    use std::time::Duration;

    use crate::model::{Label, Labels, STALE_NAN};
    use crate::promql::source::{ResolvedSeriesChunk, SampleBlock};

    // ---- mock source (same shape 3a.1 uses) -----------------------------

    struct MockSource {
        data: Vec<(Vec<i64>, Vec<f64>)>,
    }

    impl MockSource {
        fn new(data: Vec<(Vec<i64>, Vec<f64>)>) -> Self {
            Self { data }
        }
    }

    impl SeriesSource for MockSource {
        fn resolve(
            &self,
            _selector: &VectorSelector,
            _time_range: TimeRange,
        ) -> impl Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
            stream::empty()
        }

        fn samples(
            &self,
            request: SamplesRequest,
        ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
            let mut block = SampleBlock::with_series_count(request.series.len());
            for (col_idx, sref) in request.series.iter().enumerate() {
                let col = &self.data[sref.series_id as usize];
                for (t, v) in col.0.iter().zip(col.1.iter()) {
                    if *t >= request.time_range.start_ms && *t < request.time_range.end_ms_exclusive
                    {
                        block.timestamps[col_idx].push(*t);
                        block.values[col_idx].push(*v);
                    }
                }
            }
            stream::once(ready(Ok(SampleBatch {
                series_range: 0..request.series.len(),
                samples: block,
            })))
        }
    }

    // ---- fixture builders -----------------------------------------------

    fn mk_label(metric: &str, suffix: usize) -> Labels {
        Labels::new(vec![
            Label {
                name: "__name__".to_string(),
                value: metric.to_string(),
            },
            Label {
                name: "i".to_string(),
                value: suffix.to_string(),
            },
        ])
    }

    fn mk_schema(n: usize) -> Arc<SeriesSchema> {
        let labels: Vec<Labels> = (0..n).map(|i| mk_label("m", i)).collect();
        let fps: Vec<u128> = (0..n as u128).collect();
        Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
    }

    fn mk_request_series(n: usize) -> Arc<[Arc<[ResolvedSeriesRef]>]> {
        let name: Arc<str> = Arc::from("m");
        Arc::from(
            (0..n)
                .map(|i| {
                    Arc::<[ResolvedSeriesRef]>::from(vec![ResolvedSeriesRef::new(
                        1,
                        i as u32,
                        name.clone(),
                    )])
                })
                .collect::<Vec<_>>(),
        )
    }

    fn mk_grid(start_ms: i64, step_ms: i64, step_count: usize) -> StepGrid {
        let end_ms = start_ms + (step_count as i64 - 1) * step_ms;
        StepGrid {
            start_ms,
            end_ms,
            step_ms,
            step_count,
        }
    }

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    fn drain_windows<S: SeriesSource + Send + Sync + 'static>(
        op: &mut MatrixSelectorOp<'static, S>,
    ) -> Vec<Result<MatrixWindowBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.windows(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(result)) => out.push(result),
                Poll::Pending => panic!("unexpected Pending from sync MockSource"),
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn make_op(
        source: MockSource,
        grid: StepGrid,
        at: Option<AtModifier>,
        offset: Option<Offset>,
        range_ms: i64,
        reservation: MemoryReservation,
        shape: BatchShape,
    ) -> MatrixSelectorOp<'static, MockSource> {
        let n = source.data.len();
        let schema = mk_schema(n);
        let request = mk_request_series(n);
        MatrixSelectorOp::new(
            Arc::new(source),
            schema,
            request,
            grid,
            at,
            offset,
            range_ms,
            reservation,
            shape,
        )
    }

    /// Collect all `(t, v)` pairs for a specific `(step_global, series_global)`
    /// cell from a full drain of window batches.
    fn cell_samples(
        batches: &[MatrixWindowBatch],
        step_global: usize,
        series_global: usize,
    ) -> Vec<(i64, f64)> {
        for batch in batches {
            if !batch.step_range.contains(&step_global)
                || !batch.series_range.contains(&series_global)
            {
                continue;
            }
            let step_off = step_global - batch.step_range.start;
            let series_off = series_global - batch.series_range.start;
            let (ts, vs) = batch.cell_samples(step_off, series_off);
            return ts.iter().zip(vs.iter()).map(|(t, v)| (*t, *v)).collect();
        }
        Vec::new()
    }

    // ====================================================================
    // required tests
    // ====================================================================

    #[test]
    fn should_include_samples_within_range_window() {
        // given: one series with samples 10..=40 at step 10; range=20ms,
        // step_ms=10, a single step t=30 → window (10, 30] → {20, 30}.
        let source = MockSource::new(vec![(
            vec![10, 20, 30, 40, 50],
            vec![1.0, 2.0, 3.0, 4.0, 5.0],
        )]);
        let grid = StepGrid {
            start_ms: 30,
            end_ms: 30,
            step_ms: 10,
            step_count: 1,
        };
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            20,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: cell (step 0, series 0) = [(20, 2.0), (30, 3.0)]
        let cell = cell_samples(&batches, 0, 0);
        assert_eq!(cell, vec![(20, 2.0), (30, 3.0)]);
    }

    #[test]
    fn should_merge_cross_bucket_refs_into_one_window_series() {
        // given: one logical output series backed by two bucket-local refs.
        let source = Arc::new(MockSource::new(vec![
            (vec![10], vec![1.0]),
            (vec![20], vec![2.0]),
        ]));
        let schema = mk_schema(1);
        let name: Arc<str> = Arc::from("m");
        let request_series: Arc<[Arc<[ResolvedSeriesRef]>]> = Arc::from(vec![Arc::from(vec![
            ResolvedSeriesRef::new(1, 0, name.clone()),
            ResolvedSeriesRef::new(2, 1, name.clone()),
        ])]);
        let grid = StepGrid {
            start_ms: 20,
            end_ms: 20,
            step_ms: 10,
            step_count: 1,
        };
        let reservation = MemoryReservation::new(1 << 20);
        let mut op = MatrixSelectorOp::new(
            source,
            schema,
            request_series,
            grid,
            None,
            None,
            15,
            reservation,
            BatchShape::new(1, 1),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then
        assert_eq!(cell_samples(&batches, 0, 0), vec![(10, 1.0), (20, 2.0)]);
    }

    #[test]
    fn should_exclude_samples_at_exclusive_window_start() {
        // given: sample at exactly `t - range` (the exclusive lower
        // bound) must NOT appear in the cell.
        let source = MockSource::new(vec![(vec![10, 15, 30], vec![1.0, 2.0, 3.0])]);
        // step t=30, range=20 → window (10, 30] — sample at t=10 excluded
        let grid = StepGrid {
            start_ms: 30,
            end_ms: 30,
            step_ms: 10,
            step_count: 1,
        };
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            20,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then
        let cell = cell_samples(&batches, 0, 0);
        assert_eq!(cell, vec![(15, 2.0), (30, 3.0)]);
    }

    #[test]
    fn should_include_sample_at_inclusive_window_end() {
        // given: sample at exactly `t` (the inclusive upper bound) must
        // appear in the cell.
        let source = MockSource::new(vec![(vec![25, 30], vec![1.0, 2.0])]);
        let grid = StepGrid {
            start_ms: 30,
            end_ms: 30,
            step_ms: 10,
            step_count: 1,
        };
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            10,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: window (20, 30] → {25, 30}
        let cell = cell_samples(&batches, 0, 0);
        assert_eq!(cell, vec![(25, 1.0), (30, 2.0)]);
    }

    #[test]
    fn should_apply_offset_shifting_range_window() {
        // given: offset=10ms positive — the window pins at (t - range -
        // 10, t - 10]. Sample at t=20 must appear for step=30 with
        // range=15 and offset=10 (window (5, 20]).
        let source = MockSource::new(vec![(vec![5, 20, 30], vec![1.0, 2.0, 3.0])]);
        let grid = StepGrid {
            start_ms: 30,
            end_ms: 30,
            step_ms: 10,
            step_count: 1,
        };
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            Some(Offset::Pos(Duration::from_millis(10))),
            15,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: effective = 30 - 10 = 20, window (5, 20] → {20}
        let cell = cell_samples(&batches, 0, 0);
        assert_eq!(cell, vec![(20, 2.0)]);
    }

    #[test]
    fn should_apply_at_timestamp_pinning() {
        // given: @ 100 — all steps pin to 100; range=50 → window (50,
        // 100] for every step.
        let source = MockSource::new(vec![(vec![30, 60, 90, 120], vec![1.0, 2.0, 3.0, 4.0])]);
        let grid = mk_grid(0, 10, 5); // steps 0,10,20,30,40
        let at_time = std::time::SystemTime::UNIX_EPOCH + Duration::from_millis(100);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            Some(AtModifier::At(at_time)),
            None,
            50,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: every step's cell = {60, 90} (sample at 30 excluded
        // by exclusive start; 120 excluded by inclusive end).
        for step in 0..5 {
            let cell = cell_samples(&batches, step, 0);
            assert_eq!(
                cell,
                vec![(60, 2.0), (90, 3.0)],
                "step {step} must see the @-pinned window"
            );
        }
    }

    #[test]
    fn should_yield_empty_window_when_no_samples_in_range() {
        // given: samples far outside the window — hole in data for the
        // step we're asking about.
        let source = MockSource::new(vec![(vec![1000, 2000], vec![1.0, 2.0])]);
        let grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 10,
            step_count: 1,
        };
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            50,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: cell empty; batch shape still reports the step & series
        assert_eq!(batches.len(), 1);
        let cell = cell_samples(&batches, 0, 0);
        assert!(cell.is_empty());
        assert_eq!(batches[0].cells[0], CellIndex::EMPTY);
    }

    #[test]
    fn should_treat_stale_nan_as_absence() {
        // given: mix of good and STALE_NAN samples — STALE_NAN must not
        // appear in the window. Matches 3a.1's stricter policy.
        let stale = f64::from_bits(STALE_NAN);
        let source = MockSource::new(vec![(vec![10, 20, 30, 40], vec![1.0, stale, 3.0, stale])]);
        // step t=40, range=40 → window (0, 40] → all four source
        // samples, but filter drops the STALE_NANs.
        let grid = StepGrid {
            start_ms: 40,
            end_ms: 40,
            step_ms: 10,
            step_count: 1,
        };
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            40,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: only the two good samples make it through
        let cell = cell_samples(&batches, 0, 0);
        assert_eq!(cell, vec![(10, 1.0), (30, 3.0)]);
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: a tiny cap that cannot fit even the cell-index array.
        let source = MockSource::new(vec![(vec![10, 20, 30], vec![1.0, 2.0, 3.0]); 4]);
        let grid = mk_grid(10, 10, 8);
        let tiny = MemoryReservation::new(16);
        let mut op = make_op(source, grid, None, None, 20, tiny, BatchShape::new(8, 4));

        // when
        let results = drain_windows(&mut op);

        // then: at least one result is a MemoryLimit error.
        let err = results
            .into_iter()
            .find_map(|r| r.err())
            .expect("expected a MemoryLimit error");
        assert!(matches!(err, QueryError::MemoryLimit { .. }));
    }

    #[test]
    fn should_return_static_schema() {
        // given
        let source = MockSource::new(vec![(vec![10], vec![1.0])]);
        let grid = mk_grid(10, 10, 1);
        let reservation = MemoryReservation::new(1_000_000);
        let op = make_op(
            source,
            grid,
            None,
            None,
            10,
            reservation,
            BatchShape::default(),
        );

        // when
        let schema = op.schema();

        // then
        assert!(!schema.series.is_deferred());
        assert!(schema.series.as_static().is_some());
    }

    #[test]
    fn should_yield_end_of_windows_stream() {
        // given: a small operator — drive to exhaustion then re-poll.
        let source = MockSource::new(vec![(vec![10, 20], vec![1.0, 2.0])]);
        let grid = mk_grid(20, 10, 2);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            20,
            reservation,
            BatchShape::default(),
        );

        // when
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut count = 0usize;
        loop {
            match op.windows(&mut cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(_))) => count += 1,
                Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
                Poll::Pending => panic!("unexpected Pending"),
            }
        }
        // then
        assert!(count >= 1);
        match op.windows(&mut cx) {
            Poll::Ready(None) => {}
            other => panic!("expected Ready(None) after exhaustion, got {other:?}"),
        }
    }

    // ---- extra coverage ------------------------------------------------

    #[test]
    fn should_report_degenerate_next_as_end_of_stream() {
        // given: the `Operator::next` entry point is degenerate — matrix
        // cells cannot fit `StepBatch`'s single-float layout.
        let source = MockSource::new(vec![(vec![10, 20], vec![1.0, 2.0])]);
        let grid = mk_grid(20, 10, 2);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            20,
            reservation,
            BatchShape::default(),
        );

        // when
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let polled = <MatrixSelectorOp<'static, MockSource> as Operator>::next(&mut op, &mut cx);

        // then: immediate Ready(None) — consumers use `windows()` instead
        match polled {
            Poll::Ready(None) => {}
            other => panic!("expected Ready(None) from Operator::next, got {other:?}"),
        }
    }

    #[test]
    fn should_slide_window_monotonically_across_steps() {
        // given: three steps walk a window of 20ms forward — two-pointer
        // cursor must advance monotonically without dropping samples that
        // slide in or re-emitting samples that slide out.
        let source = MockSource::new(vec![(
            vec![0, 5, 15, 25, 35, 45, 55],
            vec![0.0, 5.0, 15.0, 25.0, 35.0, 45.0, 55.0],
        )]);
        let grid = mk_grid(20, 10, 3); // steps 20, 30, 40
        let reservation = MemoryReservation::new(1_000_000);
        // One-step-chunk forces a fresh `build_window_batch` call per step,
        // exercising the cursor across multiple emit cycles.
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            20,
            reservation,
            BatchShape::new(1, 1),
        );

        // when
        let batches: Vec<MatrixWindowBatch> = drain_windows(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: step 20 → (0, 20] = {5, 15}; step 30 → (10, 30] = {15, 25};
        // step 40 → (20, 40] = {25, 35}
        assert_eq!(cell_samples(&batches, 0, 0), vec![(5, 5.0), (15, 15.0)]);
        assert_eq!(cell_samples(&batches, 1, 0), vec![(15, 15.0), (25, 25.0)]);
        assert_eq!(cell_samples(&batches, 2, 0), vec![(25, 25.0), (35, 35.0)]);
    }
}

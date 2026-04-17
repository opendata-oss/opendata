//! `VectorSelector` operator — unit 3a.1.
//!
//! Instant-vector leaf. Emits one sample per series per step of the query's
//! step grid, where each sample is the latest raw sample in the operator's
//! lookback window. Owns PromQL semantics for `lookback_delta`,
//! `@ timestamp`/`@ start()`/`@ end()`, and `offset`. The upstream
//! [`SeriesSource`](crate::promql::v2::source::SeriesSource) is PromQL-unaware
//! — it returns raw samples in an absolute time range; this operator applies
//! lookback, `@`, `offset`, and stale-marker handling (RFC 0007
//! §"Operator Taxonomy", §"Storage Contract").
//!
//! # Semantics
//!
//! For each step `t = start_ms + k * step_ms` (k in `0..step_count`):
//!
//! ```text
//!   pin         = @ value when @ is set, else t
//!   effective   = pin - offset               (Offset::Pos subtracts; Neg adds)
//!   window      = (effective - lookback, effective]
//!   sample      = latest non-STALE_NAN sample with timestamp in window
//!   validity    = 1 when sample exists, 0 otherwise
//!   value[cell] = sample.value              (undefined when validity = 0)
//! ```
//!
//! Composition order verified against the existing evaluator at
//! `timeseries/src/promql/evaluator.rs:1126-1160`. Lookback window uses the
//! same `(start, end]` convention as `timeseries/src/promql/pipeline.rs:673-688`.
//!
//! `STALE_NAN` samples are treated as absence here (task spec for 3a.1) —
//! stricter than the current `pipeline.rs` path, which does not filter them.
//! Matches Prometheus lookback semantics where an explicit staleness marker
//! terminates a series.
//!
//! # Batching
//!
//! Emits one [`StepBatch`] per `(series_chunk, step_chunk)` tile. Default
//! tile dimensions are `N ≈ 64 steps × K ≈ 512 series` per RFC
//! §"Core Data Model"; callers may override via [`BatchShape::new`]. Samples
//! for a given series chunk are drained from the source stream on-demand:
//! the first poll produces the batch `(0, 0)`, subsequent polls advance
//! steps within the current series chunk, then move to the next series
//! chunk once step chunks are exhausted.
//!
//! # Memory accounting
//!
//! Every allocation that scales with `series_count × step_count` routes
//! through [`MemoryReservation::try_grow`]. The [`BatchBuffers`] guard
//! releases the reservation in its `Drop` impl once a batch has been
//! emitted — the values/validity vectors are moved into the batch, and
//! the reservation is only released when the operator is ready to build
//! the next buffer (`BatchBuffers` therefore re-reserves per batch).
//!
//! The per-series sample columns materialised from the source also route
//! through the reservation, proportional to the number of samples in the
//! series-chunk window.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::StreamExt;
use promql_parser::parser::{AtModifier, Offset};

use crate::model::{Labels, is_stale_nan};
use crate::promql::timestamp::Timestamp;

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::source::{ResolvedSeriesRef, SampleBatch, SampleHint, SeriesSource, TimeRange};

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Default step chunk — roughly `N ≈ 64` per RFC §"Core Data Model".
pub(crate) const DEFAULT_STEP_CHUNK: usize = 64;
/// Default series chunk — roughly `K ≈ 512` per RFC §"Core Data Model".
pub(crate) const DEFAULT_SERIES_CHUNK: usize = 512;
/// Default Prometheus lookback delta (5 minutes) — matches
/// `crate::model::QueryOptions` default and `promqltest` expectations.
pub(crate) const DEFAULT_LOOKBACK_MS: i64 = 5 * 60 * 1_000;

// ---------------------------------------------------------------------------
// Batch shape
// ---------------------------------------------------------------------------

/// Tile dimensions emitted by the operator. Chosen to fit an `N × K` tile
/// into the L2-sized rectangle the RFC targets; callers may override the
/// defaults for tests / tuning.
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
// Memory-guarded per-batch buffers
// ---------------------------------------------------------------------------

/// RAII wrapper around a `StepBatch`'s value + validity allocations.
///
/// Reserves bytes against a [`MemoryReservation`] on [`Self::allocate`] and
/// releases them on [`Drop`]. Callers move the inner `values` / `validity`
/// into a freshly-built [`StepBatch`] via [`Self::finish`] — the move
/// transfers ownership of the bytes to the caller, so the `Drop` release
/// must happen *before* the batch is emitted. In practice we release the
/// reservation the moment `finish()` is called; the caller accounts for
/// the batch's memory separately (i.e. in an outer reservation scope, the
/// operator is not the sole owner of the emitted batch).
///
/// v1 simplification: operator releases its reservation slice as soon as
/// the batch leaves the operator. Downstream operators re-reserve if they
/// need to hold onto the batch. Matches the per-batch accounting model in
/// RFC §"Execution Model".
struct BatchBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
    validity: BitSet,
}

impl BatchBuffers {
    /// Attempt to allocate `len` cells of `(f64, validity bit)` storage,
    /// reserving `len * (8 + 1)` bytes plus a rounded-up `Vec<u64>` for the
    /// bitset. Returns `QueryError::MemoryLimit` without allocating if the
    /// reservation rejects the grow.
    fn allocate(reservation: &MemoryReservation, len: usize) -> Result<Self, QueryError> {
        let bytes = cell_bytes(len);
        reservation.try_grow(bytes)?;
        Self::fill(reservation.clone(), bytes, len)
    }

    fn fill(reservation: MemoryReservation, bytes: usize, len: usize) -> Result<Self, QueryError> {
        // NaN-fill so accidental reads of invalid cells surface as NaN
        // rather than stale stack data. Callers must consult validity
        // before reading `values`.
        let values = vec![f64::NAN; len];
        let validity = BitSet::with_len(len);
        Ok(Self {
            reservation,
            bytes,
            values,
            validity,
        })
    }

    /// Consume the buffer, returning `(values, validity)` and releasing
    /// the reservation.
    fn finish(mut self) -> (Vec<f64>, BitSet) {
        let values = std::mem::take(&mut self.values);
        let validity = std::mem::take(&mut self.validity);
        // Release now: the caller owns the bytes via the returned vectors.
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (values, validity)
    }
}

impl Drop for BatchBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

/// Conservative byte estimate for a `len`-cell buffer: `f64` column +
/// bit-per-cell validity (rounded up to whole `u64` words).
#[inline]
fn cell_bytes(len: usize) -> usize {
    let values = len.saturating_mul(std::mem::size_of::<f64>());
    let words = len.div_ceil(64);
    let validity = words.saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

/// Conservative byte estimate for a pre-materialised per-series sample
/// column holding `n` samples (timestamps + values).
#[inline]
fn samples_bytes(n: usize) -> usize {
    n.saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>())
}

// ---------------------------------------------------------------------------
// Lookback / @ / offset resolution
// ---------------------------------------------------------------------------

/// Pre-computed per-step lookup times. `times[k]` is the pinned evaluation
/// time for step `k` **before** subtracting the lookback window (i.e.
/// `pin - offset`); the operator then looks for the latest sample in
/// `(times[k] - lookback, times[k]]`.
///
/// When `@` is set, all entries are identical (all steps pin to the same
/// evaluation time). When `@ start()`/`@ end()` is used, all entries pin
/// to the grid's `start_ms`/`end_ms` respectively.
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
        // Phase 2: apply offset (matches evaluator.rs:1143-1156).
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

    /// Minimum and maximum effective time across all steps. Used to size
    /// the `SampleHint` time window so the source returns enough samples
    /// for every step's lookback window.
    fn time_range_with_lookback(&self, lookback_ms: i64) -> TimeRange {
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
            // Empty grid — return a zero window. The adapter treats
            // `end <= start` as empty and short-circuits.
            return TimeRange::new(0, 0);
        }
        // Lookback window is (t - lookback, t]; the source needs samples
        // in `[min - lookback + 1, max + 1)`. We use `+ 1` on the exclusive
        // end so that samples at exactly `max` are included (TimeRange is
        // inclusive-exclusive).
        let start = min.saturating_sub(lookback_ms).saturating_add(1);
        let end = max.saturating_add(1);
        TimeRange::new(start, end)
    }
}

// ---------------------------------------------------------------------------
// Per-chunk sample state
// ---------------------------------------------------------------------------

/// Samples for every series in the current series chunk, pre-materialised
/// as two parallel columns. Uses the operator's reservation for all
/// per-chunk allocations; released in `Drop`.
struct ChunkSamples {
    reservation: MemoryReservation,
    bytes: usize,
    /// Indexed by chunk-local series offset (0..chunk_len).
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

    /// Absorb a `SampleBatch` whose `series_range` indexes into the flattened
    /// chunk-local `SampleHint::series`. `hint_to_series[hint_idx]` resolves
    /// each sample column back onto the logical output series it belongs to.
    fn absorb(&mut self, batch: SampleBatch, hint_to_series: &[usize]) -> Result<(), QueryError> {
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
            let hint_idx = batch.series_range.start + block_idx;
            let local_idx = hint_to_series[hint_idx];
            // Append rather than replace — a series may span several
            // SampleBatches (one per bucket for cross-bucket series).
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
// Operator state machine
// ---------------------------------------------------------------------------

type SampleStream<'a> = Pin<Box<dyn Stream<Item = Result<SampleBatch, QueryError>> + Send + 'a>>;

enum State<'a> {
    /// Pending first poll — need to build effective times and start the
    /// first series chunk.
    Init,
    /// A series chunk is being hydrated from the source stream. The
    /// future drives the stream to completion and collects all batches
    /// into `chunk`.
    LoadingChunk {
        chunk_start: usize,
        chunk_len: usize,
        #[allow(clippy::type_complexity)]
        future: Pin<Box<dyn Future<Output = Result<ChunkSamples, QueryError>> + Send + 'a>>,
    },
    /// Chunk is loaded; iterate step chunks emitting one batch per poll.
    Emitting {
        chunk_start: usize,
        chunk_len: usize,
        samples: Box<ChunkSamples>,
        /// Next step chunk index within the full grid (absolute, 0-based).
        next_step_chunk_start: usize,
    },
    /// All chunks / steps exhausted.
    Done,
    /// Terminal error already reported — subsequent polls return
    /// `Ready(None)`.
    Errored,
    /// Transient placeholder used while swapping state inside `next()`.
    Transitioning,
    #[allow(dead_code)]
    _Phantom(std::marker::PhantomData<&'a ()>),
}

// ---------------------------------------------------------------------------
// Operator struct
// ---------------------------------------------------------------------------

/// PromQL instant-vector selector operator.
///
/// See module docs for semantics and batching strategy.
pub(crate) struct VectorSelectorOp<'a, S: SeriesSource + 'a> {
    // Plan-time inputs ------------------------------------------------------
    source: Arc<S>,
    hint_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
    schema: OperatorSchema,
    step_timestamps: Arc<[i64]>,
    effective_times: EffectiveTimes,
    lookback_ms: i64,
    shape: BatchShape,
    reservation: MemoryReservation,

    // Runtime state ---------------------------------------------------------
    state: State<'a>,
}

impl<'a, S: SeriesSource + Send + Sync + 'a> VectorSelectorOp<'a, S> {
    /// Build an operator from resolved inputs.
    ///
    /// * `source` — storage handle. The operator clones `Arc<S>` into the
    ///   sample-load future so the stream can outlive the operator's
    ///   call stack inside `next()`.
    /// * `series` — post-resolve series roster in the order the planner
    ///   wants samples to land. `schema.series` is a
    ///   [`SchemaRef::Static`] handle over the same roster.
    /// * `hint_series` — parallel slice of per-logical-series opaque source
    ///   handle groups aligned with `schema.series`. A single logical series
    ///   may carry several bucket-local refs when the planner deduplicates the
    ///   resolved roster by fingerprint.
    /// * `grid` — step grid the query runs on.
    /// * `at` / `offset` — selector modifiers; may both be `None`.
    /// * `lookback_ms` — Prometheus lookback delta in ms. Use
    ///   [`DEFAULT_LOOKBACK_MS`] for the 5-minute default.
    /// * `reservation` — per-query reservation. Cloned for internal use.
    /// * `shape` — tile dimensions; use [`BatchShape::default`] outside tests.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: Arc<S>,
        series: Arc<SeriesSchema>,
        hint_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
        grid: StepGrid,
        at: Option<AtModifier>,
        offset: Option<Offset>,
        lookback_ms: i64,
        reservation: MemoryReservation,
        shape: BatchShape,
    ) -> Self {
        assert_eq!(
            series.len(),
            hint_series.len(),
            "series roster and hint_series must be length-aligned",
        );
        let step_timestamps: Arc<[i64]> = Arc::from(
            (0..grid.step_count)
                .map(|k| grid.start_ms + (k as i64) * grid.step_ms)
                .collect::<Vec<_>>(),
        );
        let effective_times =
            EffectiveTimes::compute(&step_timestamps, &grid, at.as_ref(), offset.as_ref());
        let schema = OperatorSchema::new(SchemaRef::Static(series), grid);
        Self {
            source,
            hint_series,
            schema,
            step_timestamps,
            effective_times,
            lookback_ms,
            shape,
            reservation,
            state: State::Init,
        }
    }

    fn total_series(&self) -> usize {
        self.hint_series.len()
    }

    fn chunk_hint(&self, chunk_start: usize, chunk_end: usize) -> (SampleHint, Vec<usize>) {
        let mut flat: Vec<ResolvedSeriesRef> = Vec::new();
        let mut hint_to_series: Vec<usize> = Vec::new();
        for (series_off, series_refs) in self.hint_series[chunk_start..chunk_end].iter().enumerate()
        {
            debug_assert!(
                !series_refs.is_empty(),
                "every logical series must have at least one source handle",
            );
            for sref in series_refs.iter() {
                flat.push(*sref);
                hint_to_series.push(series_off);
            }
        }
        let window = self
            .effective_times
            .time_range_with_lookback(self.lookback_ms);
        (SampleHint::new(Arc::from(flat), window), hint_to_series)
    }

    fn start_chunk_load(&mut self, chunk_start: usize) -> State<'a>
    where
        S: 'a,
    {
        let chunk_end = (chunk_start + self.shape.series_chunk).min(self.total_series());
        let chunk_len = chunk_end - chunk_start;
        let (hint, hint_to_series) = self.chunk_hint(chunk_start, chunk_end);
        let source = self.source.clone();
        let reservation = self.reservation.clone();

        let future = Box::pin(async move {
            let mut samples = ChunkSamples::new(reservation, chunk_len);
            let stream = source.samples(hint);
            let mut stream: SampleStream<'_> = Box::pin(stream);
            while let Some(item) = stream.next().await {
                let batch = item?;
                samples.absorb(batch, &hint_to_series)?;
            }
            // Silence "unused variable" when chunk_start isn't read
            // by absorb — it's threaded into the state machine as an
            // identifier for the series slice we're hydrating.
            let _ = chunk_start;
            Ok(samples)
        });

        State::LoadingChunk {
            chunk_start,
            chunk_len,
            future,
        }
    }

    fn build_batch(
        &self,
        chunk_start: usize,
        chunk_len: usize,
        samples: &ChunkSamples,
        step_chunk_start: usize,
    ) -> Result<StepBatch, QueryError> {
        let grid = &self.schema.step_grid;
        let step_chunk_end = (step_chunk_start + self.shape.step_chunk).min(grid.step_count);
        let step_count = step_chunk_end - step_chunk_start;
        let cell_count = step_count * chunk_len;

        let mut buffers = BatchBuffers::allocate(&self.reservation, cell_count)?;
        // Per-cell source-sample timestamp (0 is a safe placeholder when
        // the cell is absent — callers must consult validity before
        // reading). RFC 0007 §6.3.7 / at_modifier.test `timestamp()`
        // semantics require the matching sample's actual timestamp, not
        // the step timestamp.
        let mut source_timestamps = vec![0i64; cell_count];

        for step_off in 0..step_count {
            let step_idx = step_chunk_start + step_off;
            let effective = self.effective_times.get(step_idx);
            let window_lo = effective.saturating_sub(self.lookback_ms); // exclusive
            let window_hi = effective; // inclusive

            for series_off in 0..chunk_len {
                let ts = &samples.timestamps[series_off];
                let vs = &samples.values[series_off];
                // Walk from the newest sample backward; caller already
                // sorted per-series samples ascending by timestamp, so we
                // scan from the end and stop at the first in-window
                // non-stale sample.
                let mut selected: Option<(f64, i64)> = None;
                for i in (0..ts.len()).rev() {
                    let t = ts[i];
                    if t > window_hi {
                        continue;
                    }
                    if t <= window_lo {
                        break;
                    }
                    let v = vs[i];
                    if is_stale_nan(v) {
                        // Per task spec: STALE_NAN terminates the lookback;
                        // treat this cell as absent.
                        break;
                    }
                    selected = Some((v, t));
                    break;
                }

                let cell = step_off * chunk_len + series_off;
                if let Some((v, t)) = selected {
                    buffers.values[cell] = v;
                    buffers.validity.set(cell);
                    source_timestamps[cell] = t;
                }
                // else: values[cell] stays NaN, validity bit stays clear.
            }
        }

        let (values, validity) = buffers.finish();
        let series_range = chunk_start..(chunk_start + chunk_len);
        let step_range = step_chunk_start..step_chunk_end;
        Ok(StepBatch::new(
            self.step_timestamps.clone(),
            step_range,
            self.schema.series.clone(),
            series_range,
            values,
            validity,
        )
        .with_source_timestamps(Arc::from(source_timestamps)))
    }
}

impl<S: SeriesSource + Send + Sync + 'static> Operator for VectorSelectorOp<'static, S> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        loop {
            // Swap out the current state so we can own it across arms.
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
                        self.state = State::Emitting {
                            chunk_start,
                            chunk_len,
                            samples: Box::new(samples),
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
                    next_step_chunk_start,
                } => {
                    let grid = &self.schema.step_grid;
                    if next_step_chunk_start >= grid.step_count {
                        // All steps for this chunk emitted — advance to
                        // the next series chunk or finish.
                        let next_chunk_start = chunk_start + chunk_len;
                        // Drop `samples` here, releasing its reservation.
                        drop(samples);
                        if next_chunk_start >= self.total_series() {
                            self.state = State::Done;
                            return Poll::Ready(None);
                        }
                        self.state = self.start_chunk_load(next_chunk_start);
                        continue;
                    }
                    match self.build_batch(chunk_start, chunk_len, &samples, next_step_chunk_start)
                    {
                        Ok(batch) => {
                            let step_advance = batch.step_count();
                            self.state = State::Emitting {
                                chunk_start,
                                chunk_len,
                                samples,
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
                State::Transitioning | State::_Phantom(_) => {
                    unreachable!("transient state observed in next()");
                }
            }
        }
    }
}

// Stand-in for `SeriesSchema` constructor from parallel roster data.
// Not exported — operator callers build the schema out-of-band.
#[allow(dead_code)]
pub(crate) fn build_schema_from_labels(
    labels: Vec<Labels>,
    fingerprints: Vec<u128>,
) -> SeriesSchema {
    SeriesSchema::new(Arc::from(labels), Arc::from(fingerprints))
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
    use crate::promql::v2::source::{CardinalityEstimate, ResolvedSeriesChunk, SampleBlock};

    // ---- mock source ----------------------------------------------------

    /// In-memory [`SeriesSource`] stub backed by per-series sample vectors.
    /// Bucket/series bookkeeping is deliberately trivial — the operator
    /// doesn't care about bucket IDs, it just threads them back from the
    /// hint. All test series share `bucket_id = 1`.
    struct MockSource {
        /// Indexed by `series_id` (equal to the series' position in the
        /// roster). Each column is `(timestamps, values)`.
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
            // Operator tests pre-resolve series externally; resolve is
            // exercised in 2.3. Return an empty stream to satisfy the
            // trait.
            stream::empty()
        }

        fn estimate_cardinality(
            &self,
            _selector: &VectorSelector,
            _time_range: TimeRange,
        ) -> impl Future<Output = Result<CardinalityEstimate, QueryError>> + Send {
            ready(Ok(CardinalityEstimate::exact(self.data.len() as u64)))
        }

        fn samples(
            &self,
            hint: SampleHint,
        ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
            let mut block = SampleBlock::with_series_count(hint.series.len());
            for (col_idx, sref) in hint.series.iter().enumerate() {
                let col = &self.data[sref.series_id as usize];
                // Honour the inclusive-exclusive time_range. The source
                // contract says samples in `[start, end)` are returned
                // in timestamp order.
                for (t, v) in col.0.iter().zip(col.1.iter()) {
                    if *t >= hint.time_range.start_ms && *t < hint.time_range.end_ms_exclusive {
                        block.timestamps[col_idx].push(*t);
                        block.values[col_idx].push(*v);
                    }
                }
            }
            stream::once(ready(Ok(SampleBatch {
                series_range: 0..hint.series.len(),
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

    fn mk_hint_series(n: usize) -> Arc<[Arc<[ResolvedSeriesRef]>]> {
        Arc::from(
            (0..n)
                .map(|i| {
                    Arc::<[ResolvedSeriesRef]>::from(vec![ResolvedSeriesRef::new(1, i as u32)])
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

    /// Drain the operator to exhaustion, collecting all batches. Since
    /// `MockSource` futures are ready-immediately, `Poll::Pending` is a
    /// test failure.
    fn drain<S: SeriesSource + Send + Sync + 'static>(
        op: &mut VectorSelectorOp<'static, S>,
    ) -> Vec<Result<StepBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(result)) => out.push(result),
                Poll::Pending => panic!("unexpected Pending from sync MockSource"),
            }
        }
    }

    fn make_op(
        source: MockSource,
        grid: StepGrid,
        at: Option<AtModifier>,
        offset: Option<Offset>,
        lookback_ms: i64,
        reservation: MemoryReservation,
        shape: BatchShape,
    ) -> VectorSelectorOp<'static, MockSource> {
        let n = source.data.len();
        let schema = mk_schema(n);
        let hint = mk_hint_series(n);
        VectorSelectorOp::new(
            Arc::new(source),
            schema,
            hint,
            grid,
            at,
            offset,
            lookback_ms,
            reservation,
            shape,
        )
    }

    // ---- assertions over full drains ------------------------------------

    /// Materialise the full [step x series] result grid from a stream of
    /// batches. `None` cells mean "validity bit clear". The resulting
    /// vector is indexed `[step][series]`.
    fn materialise(
        batches: &[StepBatch],
        step_count: usize,
        series_count: usize,
    ) -> Vec<Vec<Option<f64>>> {
        let mut out = vec![vec![None; series_count]; step_count];
        for batch in batches {
            for step_off in 0..batch.step_count() {
                let step_global = batch.step_range.start + step_off;
                for series_off in 0..batch.series_count() {
                    let series_global = batch.series_range.start + series_off;
                    out[step_global][series_global] = batch.get(step_off, series_off);
                }
            }
        }
        out
    }

    // ====================================================================
    // required tests (task spec)
    // ====================================================================

    #[test]
    fn should_emit_last_sample_within_lookback_window() {
        // given: one series with samples at t=10,20,30, step_ms=10,
        // step_count=4 starting at t=10, lookback=5.
        let source = MockSource::new(vec![(vec![10, 20, 30], vec![1.0, 2.0, 3.0])]);
        let grid = mk_grid(10, 10, 4);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            5,
            reservation,
            BatchShape::new(4, 1),
        );

        // when
        let results = drain(&mut op);
        let batches: Vec<StepBatch> = results.into_iter().map(|r| r.unwrap()).collect();
        let cells = materialise(&batches, 4, 1);

        // then: step 10 → 1.0; step 20 → 2.0; step 30 → 3.0; step 40 → none
        assert_eq!(cells[0][0], Some(1.0));
        assert_eq!(cells[1][0], Some(2.0));
        assert_eq!(cells[2][0], Some(3.0));
        assert_eq!(cells[3][0], None, "no sample in (35, 40]");
    }

    #[test]
    fn should_merge_cross_bucket_refs_for_one_logical_series() {
        // given: one logical output series backed by two bucket-local refs.
        // The older bucket contributes the earlier sample and the newer bucket
        // contributes the later one.
        let source = Arc::new(MockSource::new(vec![
            (vec![10], vec![1.0]),
            (vec![20], vec![2.0]),
        ]));
        let schema = mk_schema(1);
        let hint_series: Arc<[Arc<[ResolvedSeriesRef]>]> = Arc::from(vec![Arc::from(vec![
            ResolvedSeriesRef::new(1, 0),
            ResolvedSeriesRef::new(2, 1),
        ])]);
        let grid = mk_grid(10, 10, 2);
        let reservation = MemoryReservation::new(1 << 20);
        let mut op = VectorSelectorOp::new(
            source,
            schema,
            hint_series,
            grid,
            None,
            None,
            15,
            reservation,
            BatchShape::new(2, 1),
        );

        // when
        let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let cells = materialise(&batches, 2, 1);

        // then
        assert_eq!(cells[0][0], Some(1.0));
        assert_eq!(cells[1][0], Some(2.0));
    }

    #[test]
    fn should_set_validity_zero_when_no_sample_in_lookback() {
        // given: series with a hole between t=10 and t=100
        let source = MockSource::new(vec![(vec![10, 100], vec![1.0, 2.0])]);
        // 4 steps: 20, 40, 60, 80 — all inside the hole with lookback=15
        let grid = mk_grid(20, 20, 4);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            15,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let cells = materialise(&batches, 4, 1);

        // then: step 20 → 1.0 (in (5,20]); step 40 → none (hole);
        // step 60/80 → none (hole)
        assert_eq!(cells[0][0], Some(1.0));
        assert_eq!(cells[1][0], None);
        assert_eq!(cells[2][0], None);
        assert_eq!(cells[3][0], None);
    }

    #[test]
    fn should_treat_stale_nan_as_absence() {
        // given: sample at t=10 (good), t=20 (STALE_NAN). Step at t=25
        // with lookback=30 would reach back to t=10 — but the intervening
        // STALE_NAN terminates the lookback.
        let stale = f64::from_bits(STALE_NAN);
        let source = MockSource::new(vec![(vec![10, 20], vec![1.0, stale])]);
        let grid = mk_grid(25, 10, 1);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            30,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let cells = materialise(&batches, 1, 1);

        // then: cell absent — the STALE_NAN at t=20 is the most recent
        // in-window sample and is treated as a staleness marker.
        assert_eq!(cells[0][0], None);
    }

    #[test]
    fn should_apply_offset_shifting_lookup_window() {
        // given: samples at t=10,20,30 and offset=10ms. For step t,
        // window = (t - 10 - lookback, t - 10].
        let source = MockSource::new(vec![(vec![10, 20, 30], vec![1.0, 2.0, 3.0])]);
        // step=20 → window = (15, 20] → pick 2.0 without offset, or
        // (5, 10] with offset=10 → pick 1.0
        let grid = mk_grid(20, 10, 3);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            Some(Offset::Pos(Duration::from_millis(10))),
            5,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let cells = materialise(&batches, 3, 1);

        // then:
        //   step 20, effective=10, window=(5,10] → 1.0
        //   step 30, effective=20, window=(15,20] → 2.0
        //   step 40, effective=30, window=(25,30] → 3.0
        assert_eq!(cells[0][0], Some(1.0));
        assert_eq!(cells[1][0], Some(2.0));
        assert_eq!(cells[2][0], Some(3.0));
    }

    #[test]
    fn should_apply_at_timestamp_pinning() {
        // given: samples at t=50, t=100, t=150. `@ 100` pins every step
        // to eval-time 100; lookback=60 → window (40, 100] → latest is 100.
        let source = MockSource::new(vec![(vec![50, 100, 150], vec![1.0, 2.0, 3.0])]);
        let grid = mk_grid(0, 10, 5); // steps 0, 10, 20, 30, 40
        let at_time = std::time::SystemTime::UNIX_EPOCH + Duration::from_millis(100);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            Some(AtModifier::At(at_time)),
            None,
            60,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let cells = materialise(&batches, 5, 1);

        // then: every step emits the same value (the sample at t=100)
        for (k, row) in cells.iter().enumerate().take(5) {
            assert_eq!(row[0], Some(2.0), "step {k} must match @-pin");
        }
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: a tiny reservation cap that cannot fit a batch.
        let source = MockSource::new(vec![(vec![10], vec![1.0]); 4]);
        let grid = mk_grid(10, 10, 8);
        // cell_bytes for 8 steps × 4 series = 32 cells * (8 + bit) ≈ 264
        // bytes. Cap at 16 → try_grow rejects.
        let tiny = MemoryReservation::new(16);
        let mut op = make_op(source, grid, None, None, 5, tiny, BatchShape::new(8, 4));

        // when
        let results = drain(&mut op);

        // then: at least one result is a MemoryLimit error.
        let err = results
            .into_iter()
            .find_map(|r| r.err())
            .expect("expected a MemoryLimit error");
        assert!(matches!(err, QueryError::MemoryLimit { .. }));

        // and-given: a reasonable cap lets it emit
        let source = MockSource::new(vec![(vec![10], vec![1.0]); 4]);
        let reservation = MemoryReservation::new(1_000_000);
        let grid = mk_grid(10, 10, 8);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            5,
            reservation,
            BatchShape::new(8, 4),
        );
        let results = drain(&mut op);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn should_emit_batches_covering_full_step_grid() {
        // given: 3 series × 5 steps, batch shape 2 × 2 → tiles:
        // [0..2, 0..2], [0..2, 2..3], [2..4, 0..2], [2..4, 2..3],
        // [4..5, 0..2], [4..5, 2..3]
        let source = MockSource::new(vec![
            (vec![0, 10, 20, 30, 40], vec![1.0, 2.0, 3.0, 4.0, 5.0]),
            (vec![0, 10, 20, 30, 40], vec![10.0, 20.0, 30.0, 40.0, 50.0]),
            (
                vec![0, 10, 20, 30, 40],
                vec![100.0, 200.0, 300.0, 400.0, 500.0],
            ),
        ]);
        let grid = mk_grid(0, 10, 5);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            5,
            reservation,
            BatchShape::new(2, 2),
        );

        // when
        let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: every (step, series) cell is accounted for in exactly one
        // batch. Materialising yields a full 5×3 grid.
        let cells = materialise(&batches, 5, 3);
        for (k, row) in cells.iter().enumerate().take(5) {
            assert_eq!(row[0], Some(k as f64 + 1.0));
            assert_eq!(row[1], Some((k as f64 + 1.0) * 10.0));
            assert_eq!(row[2], Some((k as f64 + 1.0) * 100.0));
        }
        // total cells covered equals step_count × series_count
        let total: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total, 5 * 3);
    }

    #[test]
    fn should_yield_end_of_stream_when_grid_exhausted() {
        // given: a two-step, one-series operator
        let source = MockSource::new(vec![(vec![0, 10], vec![1.0, 2.0])]);
        let grid = mk_grid(0, 10, 2);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            5,
            reservation,
            BatchShape::default(),
        );

        // when: drain, then poll one more time
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut count = 0usize;
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(_))) => count += 1,
                Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
                Poll::Pending => panic!("unexpected Pending"),
            }
        }
        // then: at least one batch was emitted, and the next poll after
        // end-of-stream is still Ready(None).
        assert!(count >= 1);
        match op.next(&mut cx) {
            Poll::Ready(None) => {}
            other => panic!("expected Ready(None) after exhaustion, got {other:?}"),
        }
    }

    #[test]
    fn should_return_static_schema() {
        // given
        let source = MockSource::new(vec![(vec![0], vec![1.0])]);
        let grid = mk_grid(0, 10, 1);
        let reservation = MemoryReservation::new(1_000_000);
        let op = make_op(
            source,
            grid,
            None,
            None,
            5,
            reservation,
            BatchShape::default(),
        );

        // when
        let schema = op.schema();

        // then
        assert!(!schema.series.is_deferred());
        assert!(schema.series.as_static().is_some());
        assert_eq!(schema.step_grid.step_count, 1);
    }

    // ---- extra coverage ------------------------------------------------

    #[test]
    fn should_repeat_at_start_across_all_steps() {
        // given: @ start() pins every step to grid.start_ms
        let source = MockSource::new(vec![(vec![0, 10, 20], vec![10.0, 20.0, 30.0])]);
        let grid = mk_grid(10, 10, 3);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            Some(AtModifier::Start),
            None,
            5,
            reservation,
            BatchShape::default(),
        );

        // when
        let batches: Vec<StepBatch> = drain(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let cells = materialise(&batches, 3, 1);

        // then: every step picks the sample at t=10 (grid.start_ms)
        for row in cells.iter().take(3) {
            assert_eq!(row[0], Some(20.0));
        }
    }

    #[test]
    fn should_handle_empty_series_roster() {
        // given: zero series
        let source = MockSource::new(vec![]);
        let grid = mk_grid(0, 10, 4);
        let reservation = MemoryReservation::new(1_000_000);
        let mut op = make_op(
            source,
            grid,
            None,
            None,
            5,
            reservation,
            BatchShape::default(),
        );

        // when / then: end-of-stream on first poll
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        match op.next(&mut cx) {
            Poll::Ready(None) => {}
            other => panic!("expected Ready(None), got {other:?}"),
        }
    }
}

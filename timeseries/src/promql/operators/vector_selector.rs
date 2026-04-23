//! `VectorSelectorOp` — the storage leaf for PromQL instant vectors (the
//! plain `metric{labels=...}` selector, no brackets). It turns raw
//! samples from a [`SeriesSource`] into [`StepBatch`]es ready for
//! downstream operators to consume.
//!
//! This is where all the PromQL semantics for picking "the sample at
//! step `t`" live: `lookback_delta`, the `@` modifier, `offset`, and
//! `STALE_NAN` handling. The underlying [`SeriesSource`] is deliberately
//! PromQL-unaware and just returns raw samples in an absolute time
//! window; everything else happens here.
//!
//! Per-step semantics:
//! ```text
//!   pin         = @ value when @ is set, else t
//!   effective   = pin - offset               (Offset::Pos subtracts; Neg adds)
//!   window      = (effective - lookback, effective]
//!   sample      = latest non-STALE_NAN sample in window
//! ```
//!
//! `STALE_NAN` is treated as absence — stricter than v1's pipeline.rs
//! but matches Prometheus' "stale marker terminates a series" rule.
//!
//! Output is tiled: one [`StepBatch`] per `(series_chunk, step_chunk)`
//! rectangle (default ~64 steps × 512 series) so a single batch's
//! allocation stays bounded regardless of query width. Samples are
//! drained from the source stream on demand; everything
//! `series_count × step_count`-scaled routes through
//! [`MemoryReservation::try_grow`].
//!
//! [`SeriesSource`]: crate::promql::source::SeriesSource

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use futures::stream::StreamExt;
use promql_parser::parser::{AtModifier, Offset};

use crate::model::is_stale_nan;
use crate::promql::timestamp::Timestamp;

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::source::{
    ResolvedSeriesRef, SampleBatch, SamplesRequest, SeriesSource, TimeRange,
};
use super::super::trace;

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

pub(crate) const DEFAULT_STEP_CHUNK: usize = 64;
pub(crate) const DEFAULT_SERIES_CHUNK: usize = 512;
/// Prometheus default.
pub(crate) const DEFAULT_LOOKBACK_MS: i64 = 5 * 60 * 1_000;

// ---------------------------------------------------------------------------
// Batch shape
// ---------------------------------------------------------------------------

/// Tile dimensions; defaults target an L2-sized rectangle.
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

/// RAII wrapper around a batch's value / validity allocations. Reserves on
/// [`Self::allocate`] and releases on drop or [`Self::finish`]. Downstream
/// operators re-reserve if they need to hold onto the emitted batch.
struct BatchBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
    validity: BitSet,
}

impl BatchBuffers {
    /// Returns `QueryError::MemoryLimit` without allocating if the reservation
    /// rejects the grow.
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

#[inline]
fn cell_bytes(len: usize) -> usize {
    let values = len.saturating_mul(std::mem::size_of::<f64>());
    let words = len.div_ceil(64);
    let validity = words.saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

#[inline]
fn samples_bytes(n: usize) -> usize {
    n.saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>())
}

// ---------------------------------------------------------------------------
// Lookback / @ / offset resolution
// ---------------------------------------------------------------------------

/// Per-step `pin - offset` times. The operator looks up
/// `(times[k] - lookback, times[k]]`. `@` / `@ start()` / `@ end()` collapse
/// all entries to the pinned value.
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
    /// the `SamplesRequest` time window so the source returns enough samples
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

/// Pre-materialised per-series-chunk sample columns.
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

    /// `request_to_series[request_idx]` maps each sample column back to its
    /// logical output series (a logical series may span several request refs).
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
    Init,
    /// Series chunk being hydrated from the source stream.
    LoadingChunk {
        chunk_start: usize,
        chunk_len: usize,
        #[allow(clippy::type_complexity)]
        future: Pin<Box<dyn Future<Output = Result<ChunkSamples, QueryError>> + Send + 'a>>,
    },
    /// Chunk loaded; step chunks emit one batch per poll.
    Emitting {
        chunk_start: usize,
        chunk_len: usize,
        samples: Box<ChunkSamples>,
        next_step_chunk_start: usize,
    },
    Done,
    /// Terminal error; subsequent polls return `Ready(None)`.
    Errored,
    /// Transient placeholder used while swapping state inside `next()`.
    Transitioning,
}

// ---------------------------------------------------------------------------
// Operator struct
// ---------------------------------------------------------------------------

/// Storage leaf for PromQL instant vectors (`metric{labels=...}`). Pulls
/// raw samples from a [`SeriesSource`] and applies lookback / `@` /
/// `offset` / stale-marker semantics to emit tiled [`StepBatch`]es.
pub(crate) struct VectorSelectorOp<'a, S: SeriesSource + 'a> {
    // Plan-time inputs ------------------------------------------------------
    source: Arc<S>,
    request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
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
    /// `request_series[i]` is the group of bucket-local source handles for
    /// logical series `i` (deduplicated by fingerprint).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: Arc<S>,
        series: Arc<SeriesSchema>,
        request_series: Arc<[Arc<[ResolvedSeriesRef]>]>,
        grid: StepGrid,
        at: Option<AtModifier>,
        offset: Option<Offset>,
        lookback_ms: i64,
        reservation: MemoryReservation,
        shape: BatchShape,
    ) -> Self {
        assert_eq!(
            series.len(),
            request_series.len(),
            "series roster and request_series must be length-aligned",
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
            request_series,
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
        let window = self
            .effective_times
            .time_range_with_lookback(self.lookback_ms);
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
                // `absorb` copies per-series (ts, value) pairs into this
                // chunk's columnar staging vectors. Pure CPU, measured
                // separately from the source's I/O wait (which is already
                // attributed to `object_storage_fetch` / `deserialize`).
                trace::record_subphase_sync("absorb", || {
                    samples.absorb(batch, &request_to_series)
                })?;
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

        // Forward two-pointer per series: `effective_times` is non-decreasing
        // across steps (monotonic grid, or constant when `@` is set), and
        // each series' timestamps are ascending, so a cursor advancing past
        // samples with `ts <= window_hi` is correct across all steps. The
        // candidate is always `cursor - 1` (the newest in-window sample).
        // This turns the old O(step_count × series × ts.len()) reverse scan
        // into O(step_count × series + Σ ts.len()).
        let mut cursors = vec![0usize; chunk_len];

        for step_off in 0..step_count {
            let step_idx = step_chunk_start + step_off;
            let effective = self.effective_times.get(step_idx);
            let window_lo = effective.saturating_sub(self.lookback_ms); // exclusive
            let window_hi = effective; // inclusive

            for (series_off, cursor) in cursors.iter_mut().enumerate() {
                let ts = &samples.timestamps[series_off];
                let vs = &samples.values[series_off];
                while *cursor < ts.len() && ts[*cursor] <= window_hi {
                    *cursor += 1;
                }
                if *cursor == 0 {
                    continue;
                }
                let idx = *cursor - 1;
                let t = ts[idx];
                if t <= window_lo {
                    continue;
                }
                let v = vs[idx];
                if is_stale_nan(v) {
                    // STALE_NAN terminates the lookback; treat the cell as
                    // absent. Per-step; the cursor stays put so later steps
                    // with wider `window_hi` may select a newer sample.
                    continue;
                }

                let cell = step_off * chunk_len + series_off;
                buffers.values[cell] = v;
                buffers.validity.set(cell);
                source_timestamps[cell] = t;
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
                    // `request_series` is indexed by logical (fingerprint-
                    // deduped) series; each entry holds one ref per bucket
                    // the series lives in. The outer length is the distinct
                    // fingerprint count; the sum of inner lengths is the
                    // total number of bucket-local series handles fetched.
                    let unique_fingerprints = self.request_series.len() as u64;
                    let series_selected: u64 = self
                        .request_series
                        .iter()
                        .map(|refs| refs.len() as u64)
                        .sum();
                    trace::record_counter("series_selected", series_selected);
                    trace::record_counter("unique_fingerprints", unique_fingerprints);
                    let _g = trace::Scope::enter("start_chunk_load");
                    self.state = self.start_chunk_load(0);
                }
                State::LoadingChunk {
                    chunk_start,
                    chunk_len,
                    mut future,
                } => {
                    // Time each poll of the hydration future — covers the
                    // `samples_stream.next().await` + `absorb` loop. Only
                    // Ready polls record (Pending polls don't contribute
                    // CPU, just the wake-up cost).
                    let _g = trace::Scope::enter("hydrate");
                    match future.as_mut().poll(cx) {
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
                    }
                }
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
                        let _g = trace::Scope::enter("start_chunk_load");
                        self.state = self.start_chunk_load(next_chunk_start);
                        continue;
                    }
                    let build_result = {
                        let _g = trace::Scope::enter("build_batch");
                        self.build_batch(chunk_start, chunk_len, &samples, next_step_chunk_start)
                    };
                    match build_result {
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
                State::Transitioning => {
                    unreachable!("transient state observed in next()");
                }
            }
        }
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

    // ---- mock source ----------------------------------------------------

    /// In-memory [`SeriesSource`] stub backed by per-series sample vectors.
    /// Bucket/series bookkeeping is deliberately trivial — the operator
    /// doesn't care about bucket IDs, it just threads them back from the
    /// request. All test series share `bucket_id = 1`.
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

        fn samples(
            &self,
            request: SamplesRequest,
        ) -> impl Stream<Item = Result<SampleBatch, QueryError>> + Send {
            let mut block = SampleBlock::with_series_count(request.series.len());
            for (col_idx, sref) in request.series.iter().enumerate() {
                let col = &self.data[sref.series_id as usize];
                // Honour the inclusive-exclusive time_range. The source
                // contract says samples in `[start, end)` are returned
                // in timestamp order.
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
        let request = mk_request_series(n);
        VectorSelectorOp::new(
            Arc::new(source),
            schema,
            request,
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
    // required tests
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
        let name: Arc<str> = Arc::from("m");
        let request_series: Arc<[Arc<[ResolvedSeriesRef]>]> = Arc::from(vec![Arc::from(vec![
            ResolvedSeriesRef::new(1, 0, name.clone()),
            ResolvedSeriesRef::new(2, 1, name.clone()),
        ])]);
        let grid = mk_grid(10, 10, 2);
        let reservation = MemoryReservation::new(1 << 20);
        let mut op = VectorSelectorOp::new(
            source,
            schema,
            request_series,
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

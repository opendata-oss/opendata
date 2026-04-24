//! `RollupOp` implements every PromQL range function — `rate`,
//! `increase`, `delta`, `*_over_time`, `quantile_over_time`, `resets`,
//! `changes`, and the rest. One operator type, with the specific
//! reduction selected by a [`RollupKind`] enum.
//!
//! The division of labour with the upstream selector is deliberate:
//! [`MatrixSelectorOp`] does the "per-step window" work (fetch samples,
//! slice them per cell, emit [`MatrixWindowBatch`]es); `RollupOp` does
//! the "window → scalar" work (pick a reducer, apply it per cell, emit
//! [`StepBatch`]es). They talk through the [`WindowStream`] trait so
//! [`SubqueryOp`](super::subquery::SubqueryOp) can plug in as an
//! alternative producer without subclassing the selector.
//!
//! Dispatch is a [`RollupKind`] enum-match on a no-alloc
//! `(window_start, window_end, &[ts], &[val]) → Option<f64>` reducer —
//! `None` flips the output cell's validity to 0.
//!
//! Scope: the legacy engine's range functions plus `increase`, `delta`,
//! `irate`, `idelta`, `resets`, `changes`, `last_over_time`,
//! `quantile_over_time`, `present_over_time`.
//!
//! Extrapolation for `rate` / `increase` / `delta` is ported verbatim
//! from v1's `counter_increase_correction` + `extrapolated_rate`.
//!
//! Only the output `StepBatch` buffers route through
//! [`MemoryReservation::try_grow`]; the input window batch is already
//! accounted for by `MatrixSelectorOp`.

use std::task::{Context, Poll};

use super::super::batch::{BitSet, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};
use super::super::source::SeriesSource;
use super::matrix_selector::{MatrixSelectorOp, MatrixWindowBatch};

// ---------------------------------------------------------------------------
// RollupKind — one operator, function selection as data
// ---------------------------------------------------------------------------

/// Per-window scalar reducer. Adding a rollup is a new variant + arms in
/// [`RollupKind::min_samples`] and [`RollupKind::compute`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RollupKind {
    Rate,
    Increase,
    Delta,
    Irate,
    Idelta,
    Resets,
    Changes,
    SumOverTime,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    CountOverTime,
    LastOverTime,
    StddevOverTime,
    StdvarOverTime,
    /// `quantile_over_time(q, v[range])`. `q` is plan-time constant per
    /// Prometheus semantics; range-vector quantiles with per-series `q`
    /// are not a PromQL construct.
    QuantileOverTime(f64),
    PresentOverTime,
}

impl RollupKind {
    /// Minimum number of in-window samples for the kind to produce a
    /// value. Below this threshold the cell is emitted with validity=0.
    ///
    /// Mirrors Prometheus: extrapolated counters/gauges need 2+; the
    /// `*_over_time` family needs 1+.
    #[inline]
    pub fn min_samples(&self) -> usize {
        match self {
            Self::Rate | Self::Increase | Self::Delta | Self::Irate | Self::Idelta => 2,
            _ => 1,
        }
    }

    /// Reduce an in-window sample set to a single scalar, or `None` when
    /// the cell should be emitted with `validity = 0`.
    ///
    /// `window_start_ms` is the exclusive lower bound, `window_end_ms`
    /// the inclusive upper bound (matching [`MatrixSelectorOp`]'s
    /// emission contract). `ts` / `vs` are parallel, ascending, and
    /// already filtered of `STALE_NAN` by the matrix operator.
    pub fn compute(
        &self,
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
    ) -> Option<f64> {
        if ts.len() < self.min_samples() {
            // Present-over-time still produces a validity-0 cell when empty;
            // callers handle the distinction via the validity bit.
            return None;
        }
        match self {
            Self::Rate => rollup_fns::extrapolated_rate(window_start_ms, window_end_ms, ts, vs),
            Self::Increase => {
                rollup_fns::extrapolated_increase(window_start_ms, window_end_ms, ts, vs)
            }
            Self::Delta => rollup_fns::extrapolated_delta(window_start_ms, window_end_ms, ts, vs),
            Self::Irate => rollup_fns::irate(ts, vs),
            Self::Idelta => rollup_fns::idelta(vs),
            Self::Resets => Some(rollup_fns::resets(vs) as f64),
            Self::Changes => Some(rollup_fns::changes(vs) as f64),
            Self::SumOverTime => Some(rollup_fns::sum_over_time(vs)),
            Self::AvgOverTime => Some(rollup_fns::avg_over_time(vs)),
            Self::MinOverTime => Some(rollup_fns::min_over_time(vs)),
            Self::MaxOverTime => Some(rollup_fns::max_over_time(vs)),
            Self::CountOverTime => Some(vs.len() as f64),
            Self::LastOverTime => vs.last().copied(),
            Self::StddevOverTime => Some(rollup_fns::variance(vs).sqrt()),
            Self::StdvarOverTime => Some(rollup_fns::variance(vs)),
            Self::QuantileOverTime(q) => Some(rollup_fns::quantile(*q, vs)),
            Self::PresentOverTime => {
                if vs.is_empty() {
                    None
                } else {
                    Some(1.0)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// WindowStream trait — the injection point for production vs test
// ---------------------------------------------------------------------------

/// The "producer of [`MatrixWindowBatch`]es" abstraction that
/// [`RollupOp`] pulls from — narrower than [`Operator`] (only `schema` +
/// window-poll), so any producer of windowed samples can slot in.
///
/// Production: [`MatrixWindowSource`] wraps a [`MatrixSelectorOp`].
/// Subqueries: [`SubqueryOp`] implements it directly. Tests: hand-built
/// mocks.
///
/// There's no blanket impl for [`MatrixSelectorOp`] because its
/// `Operator::schema` is only exposed for `'static`, but `Rollup::schema`
/// must work for any `'a` before any poll; the wrapper snapshots the
/// schema at construction time to bridge the gap.
///
/// [`SubqueryOp`]: super::subquery::SubqueryOp
pub trait WindowStream: Send {
    fn schema(&self) -> &OperatorSchema;

    fn poll_windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>>;
}

// Production wiring lives on [`MatrixWindowSource`] below: it captures
// the upstream schema at construction (which is plan-time-stable) and
// exposes it via `WindowStream::schema`. There is no direct
// `WindowStream for MatrixSelectorOp<'a, S>` impl because the latter's
// `Operator::schema` accessor is only available for `'static` in 3a.2,
// and `Rollup::schema` must be callable for any `'a` before any poll.

// ---------------------------------------------------------------------------
// Memory-accounted output buffers
// ---------------------------------------------------------------------------

#[inline]
fn out_bytes(cells: usize) -> usize {
    // Values + validity bitset (bytes ≈ cells / 8 + one word slack).
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

struct OutBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
    validity: BitSet,
}

impl OutBuffers {
    fn allocate(reservation: &MemoryReservation, cells: usize) -> Result<Self, QueryError> {
        let bytes = out_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            values: vec![0.0; cells],
            validity: BitSet::with_len(cells),
        })
    }

    fn finish(mut self) -> (Vec<f64>, BitSet) {
        let values = std::mem::take(&mut self.values);
        let validity = std::mem::replace(&mut self.validity, BitSet::with_len(0));
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (values, validity)
    }
}

impl Drop for OutBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// RollupOp — the operator
// ---------------------------------------------------------------------------

/// Implements every PromQL range function (`rate`, `*_over_time`, ...).
/// Pulls [`MatrixWindowBatch`]es from a [`WindowStream`] and reduces each
/// `(step, series)` window to one scalar via the reducer named by
/// [`RollupKind`], emitting a [`StepBatch`].
///
/// See module docs for the function taxonomy and extrapolation citation.
pub struct RollupOp<W: WindowStream> {
    child: W,
    kind: RollupKind,
    range_ms: i64,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    done: bool,
    errored: bool,
}

impl<W: WindowStream> RollupOp<W> {
    /// Construct a rollup over an arbitrary [`WindowStream`].
    ///
    /// * `child` — any window-stream producer; production = `MatrixSelectorOp`,
    ///   tests = hand-built mock.
    /// * `kind` — plan-time-selected rollup function.
    /// * `range_ms` — bracketed window in ms (same value passed to
    ///   `MatrixSelectorOp`). Used by [`RollupKind::Rate`] et al. for
    ///   extrapolation; `*_over_time` ignore it.
    /// * `reservation` — per-query reservation; cloned into the output
    ///   buffers.
    pub fn new(child: W, kind: RollupKind, range_ms: i64, reservation: MemoryReservation) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            kind,
            range_ms,
            reservation,
            schema,
            done: false,
            errored: false,
        }
    }

    fn reduce_batch(&self, window: &MatrixWindowBatch) -> Result<StepBatch, QueryError> {
        let step_count = window.step_count();
        let series_count = window.series_count();
        let cell_count = step_count * series_count;

        let mut out = OutBuffers::allocate(&self.reservation, cell_count)?;

        // Pre-slice the step timestamps covered by this window for
        // per-cell (window_start, window_end) math. Prefer the
        // `effective_times` column when present — the upstream operator
        // has folded `@` / offset into those, so the samples actually
        // live in `(effective - range, effective]` rather than the
        // `(step_t - range, step_t]` window that `step_timestamps`
        // would imply (RFC 0007 §6.3.8; at_modifier #21).
        let window_end_ts: &[i64] = match &window.effective_times {
            Some(eff) => &eff[window.step_range.clone()],
            None => &window.step_timestamps[window.step_range.clone()],
        };

        for (step_off, &window_end) in window_end_ts.iter().enumerate().take(step_count) {
            let window_start = window_end.saturating_sub(self.range_ms);
            for series_off in 0..series_count {
                let (ts, vs) = window.cell_samples(step_off, series_off);
                let out_idx = step_off * series_count + series_off;
                if let Some(v) = self.kind.compute(window_start, window_end, ts, vs) {
                    out.values[out_idx] = v;
                    out.validity.set(out_idx);
                }
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            window.step_timestamps.clone(),
            window.step_range.clone(),
            window.series.clone(),
            window.series_range.clone(),
            values,
            validity,
        ))
    }
}

impl<W: WindowStream> Operator for RollupOp<W> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }
        match self.child.poll_windows(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(err))) => {
                self.errored = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Ok(window))) => match self.reduce_batch(&window) {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Integration helper: wrap a MatrixSelectorOp without the schema trait gap
// ---------------------------------------------------------------------------

/// The production implementation of [`WindowStream`] — wraps a
/// [`MatrixSelectorOp`] so [`RollupOp`] can drive it for any lifetime
/// `'a`.
///
/// This wrapper exists because `MatrixSelectorOp::schema` is only
/// exposed through `Operator::schema` for the `'static` impl, but
/// `WindowStream::schema` has to be callable for any `'a` before any
/// poll. We resolve the gap by snapshotting the schema at construction
/// and owning it here.
pub struct MatrixWindowSource<'a, S: SeriesSource + Send + Sync + 'a> {
    inner: MatrixSelectorOp<'a, S>,
    schema_snapshot: OperatorSchema,
}

impl<'a, S: SeriesSource + Send + Sync + 'a> MatrixWindowSource<'a, S> {
    /// Wrap a matrix selector, snapshotting its schema now.
    pub fn new(inner: MatrixSelectorOp<'a, S>, schema_snapshot: OperatorSchema) -> Self {
        Self {
            inner,
            schema_snapshot,
        }
    }
}

impl<'a, S: SeriesSource + Send + Sync + 'a> WindowStream for MatrixWindowSource<'a, S> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema_snapshot
    }

    fn poll_windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
        self.inner.windows(cx)
    }
}

// ---------------------------------------------------------------------------
// Per-function reducers
// ---------------------------------------------------------------------------

mod rollup_fns {
    /// Kahan-Neumaier compensated summation step. `#[inline(never)]`
    /// matches the existing implementation to guard against compiler
    /// reordering that would change IEEE-754 output (cf. Prometheus
    /// #16714).
    #[inline(never)]
    fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
        let t = sum + inc;
        let new_c = if t.is_infinite() {
            0.0
        } else if sum.abs() >= inc.abs() {
            c + ((sum - t) + inc)
        } else {
            c + ((inc - t) + sum)
        };
        (t, new_c)
    }

    fn counter_increase_correction(vs: &[f64]) -> f64 {
        let mut correction = 0.0;
        let mut prev = vs[0];
        for &v in &vs[1..] {
            if v < prev {
                correction += prev;
            }
            prev = v;
        }
        correction
    }

    /// Shared core of rate/increase/delta. Returns the extrapolation
    /// factor, the (possibly reset-corrected) raw delta, and the time
    /// difference in seconds between first and last sample.
    ///
    /// Kind == `IsCounter` toggles counter-reset correction.
    enum DeltaKind {
        Counter, // rate / increase
        Gauge,   // delta
    }

    fn compute_delta(
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
        kind: DeltaKind,
    ) -> Option<(f64, f64)> {
        // caller guarantees ts.len() >= 2 via RollupKind::min_samples
        let n = ts.len();
        let range_seconds = (window_end_ms - window_start_ms) as f64 / 1000.0;
        let first_t = ts[0];
        let last_t = ts[n - 1];
        let first_v = vs[0];
        let last_v = vs[n - 1];

        let time_diff_seconds = (last_t - first_t) as f64 / 1000.0;
        if time_diff_seconds <= 0.0 {
            return None;
        }

        let mut result = last_v - first_v;
        if matches!(kind, DeltaKind::Counter) {
            result += counter_increase_correction(vs);
        }

        let num_minus_one = (n - 1) as f64;
        let avg_interval = time_diff_seconds / num_minus_one;
        let extrapolation_threshold = avg_interval * 1.1;

        let mut duration_to_start = (first_t - window_start_ms) as f64 / 1000.0;
        let mut duration_to_end = (window_end_ms - last_t) as f64 / 1000.0;

        if duration_to_start >= extrapolation_threshold {
            duration_to_start = avg_interval / 2.0;
        }

        // Counter-only zero-bound clip (identical to functions.rs:1060-1066).
        if matches!(kind, DeltaKind::Counter) {
            let mut duration_to_zero = duration_to_start;
            if result > 0.0 && first_v >= 0.0 {
                duration_to_zero = first_v * (time_diff_seconds / result);
            }
            if duration_to_zero < duration_to_start {
                duration_to_start = duration_to_zero;
            }
        }

        if duration_to_end >= extrapolation_threshold {
            duration_to_end = avg_interval / 2.0;
        }

        // `rate`: factor divides by range_seconds → result is per-second.
        // `increase`/`delta`: no division by range_seconds.
        let factor_unit =
            (time_diff_seconds + duration_to_start + duration_to_end) / time_diff_seconds;
        Some((result * factor_unit, range_seconds))
    }

    pub(super) fn extrapolated_rate(
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
    ) -> Option<f64> {
        let (scaled, range_seconds) =
            compute_delta(window_start_ms, window_end_ms, ts, vs, DeltaKind::Counter)?;
        if range_seconds <= 0.0 {
            return None;
        }
        Some(scaled / range_seconds)
    }

    pub(super) fn extrapolated_increase(
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
    ) -> Option<f64> {
        // increase = rate * range_seconds — i.e. the `scaled` result
        // before dividing by range.
        compute_delta(window_start_ms, window_end_ms, ts, vs, DeltaKind::Counter)
            .map(|(scaled, _)| scaled)
    }

    pub(super) fn extrapolated_delta(
        window_start_ms: i64,
        window_end_ms: i64,
        ts: &[i64],
        vs: &[f64],
    ) -> Option<f64> {
        compute_delta(window_start_ms, window_end_ms, ts, vs, DeltaKind::Gauge)
            .map(|(scaled, _)| scaled)
    }

    pub(super) fn irate(ts: &[i64], vs: &[f64]) -> Option<f64> {
        let n = ts.len();
        // n >= 2 by min_samples
        let prev_t = ts[n - 2];
        let last_t = ts[n - 1];
        let prev_v = vs[n - 2];
        let last_v = vs[n - 1];
        let dt_seconds = (last_t - prev_t) as f64 / 1000.0;
        if dt_seconds <= 0.0 {
            return None;
        }
        // Counter-reset: if last < prev, the diff is `last` itself (as
        // if the series jumped to 0 and counted up to `last`).
        let diff = if last_v < prev_v {
            last_v
        } else {
            last_v - prev_v
        };
        Some(diff / dt_seconds)
    }

    pub(super) fn idelta(vs: &[f64]) -> Option<f64> {
        let n = vs.len();
        Some(vs[n - 1] - vs[n - 2])
    }

    pub(super) fn resets(vs: &[f64]) -> usize {
        let mut count = 0;
        for w in vs.windows(2) {
            if w[1] < w[0] {
                count += 1;
            }
        }
        count
    }

    pub(super) fn changes(vs: &[f64]) -> usize {
        let mut count = 0;
        for w in vs.windows(2) {
            let (a, b) = (w[0], w[1]);
            // NaN != NaN under `!=` — but PromQL treats NaN→NaN as not
            // a change. Follow Prometheus: both NaN ⇒ not a change.
            if a.is_nan() && b.is_nan() {
                continue;
            }
            if a != b {
                count += 1;
            }
        }
        count
    }

    pub(super) fn sum_over_time(vs: &[f64]) -> f64 {
        let mut sum = 0.0;
        let mut c = 0.0;
        for &v in vs {
            (sum, c) = kahan_inc(v, sum, c);
        }
        if sum.is_infinite() { sum } else { sum + c }
    }

    pub(super) fn avg_over_time(vs: &[f64]) -> f64 {
        // caller guarantees vs.len() >= 1
        if vs.len() == 1 {
            return vs[0];
        }
        let mut sum = vs[0];
        let mut c = 0.0;
        let mut mean = 0.0;
        let mut incremental = false;
        for (i, &v) in vs.iter().enumerate().skip(1) {
            let count = (i + 1) as f64;
            if !incremental {
                let (new_sum, new_c) = kahan_inc(v, sum, c);
                if !new_sum.is_infinite() {
                    sum = new_sum;
                    c = new_c;
                    continue;
                }
                incremental = true;
                mean = sum / (count - 1.0);
                c /= count - 1.0;
            }
            let q = (count - 1.0) / count;
            (mean, c) = kahan_inc(v / count, q * mean, q * c);
        }
        if incremental {
            mean + c
        } else {
            let count = vs.len() as f64;
            sum / count + c / count
        }
    }

    /// Prometheus min_over_time semantics: NaN is replaced by any real
    /// value; all-NaN stays NaN.
    pub(super) fn min_over_time(vs: &[f64]) -> f64 {
        let mut min = vs[0];
        for &v in &vs[1..] {
            if v < min || min.is_nan() {
                min = v;
            }
        }
        min
    }

    pub(super) fn max_over_time(vs: &[f64]) -> f64 {
        let mut max = vs[0];
        for &v in &vs[1..] {
            if v > max || max.is_nan() {
                max = v;
            }
        }
        max
    }

    /// Population variance via Welford with Kahan compensation on mean
    /// and M2 accumulators.
    pub(super) fn variance(vs: &[f64]) -> f64 {
        if vs.is_empty() {
            return f64::NAN;
        }
        let mut count = 0.0;
        let mut mean = 0.0;
        let mut c_mean = 0.0;
        let mut m2 = 0.0;
        let mut c_m2 = 0.0;
        for &v in vs {
            count += 1.0;
            let delta = v - (mean + c_mean);
            (mean, c_mean) = kahan_inc(delta / count, mean, c_mean);
            let new_delta = v - (mean + c_mean);
            (m2, c_m2) = kahan_inc(delta * new_delta, m2, c_m2);
        }
        (m2 + c_m2) / count
    }

    /// Prometheus quantile_over_time: linear interpolation between ranks.
    ///
    /// - `q < 0` ⇒ `-inf`
    /// - `q > 1` ⇒ `+inf`
    /// - empty ⇒ caller filtered via `min_samples`; returns `NaN` here
    ///   as a guard but is unreachable in normal flow.
    pub(super) fn quantile(q: f64, vs: &[f64]) -> f64 {
        if vs.is_empty() {
            return f64::NAN;
        }
        if q.is_nan() {
            return f64::NAN;
        }
        if q < 0.0 {
            return f64::NEG_INFINITY;
        }
        if q > 1.0 {
            return f64::INFINITY;
        }
        let mut sorted: Vec<f64> = vs.to_vec();
        // `total_cmp` handles NaN deterministically, sorting NaN to the
        // end. Prometheus does the same via Go's sort.Float64s.
        sorted.sort_by(|a, b| a.total_cmp(b));
        let n = sorted.len();
        if n == 1 {
            return sorted[0];
        }
        let rank = q * (n - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        if lo == hi {
            return sorted[lo];
        }
        let weight = rank - lo as f64;
        sorted[lo] * (1.0 - weight) + sorted[hi] * weight
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels};
    use crate::promql::batch::{SchemaRef, SeriesSchema};
    use crate::promql::operator::StepGrid;
    use crate::promql::operators::matrix_selector::CellIndex;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    // ---- mock WindowStream --------------------------------------------------

    struct MockWindows {
        schema: OperatorSchema,
        queue: Vec<Result<MatrixWindowBatch, QueryError>>,
    }

    impl MockWindows {
        fn new(schema: OperatorSchema, batches: Vec<MatrixWindowBatch>) -> Self {
            Self {
                schema,
                queue: batches.into_iter().map(Ok).collect(),
            }
        }
    }

    impl WindowStream for MockWindows {
        fn schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn poll_windows(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
            if self.queue.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(self.queue.remove(0)))
            }
        }
    }

    // ---- fixtures -----------------------------------------------------------

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    fn mk_schema(n: usize) -> Arc<SeriesSchema> {
        let labels: Vec<Labels> = (0..n)
            .map(|i| {
                Labels::new(vec![
                    Label {
                        name: "__name__".to_string(),
                        value: "m".to_string(),
                    },
                    Label {
                        name: "i".to_string(),
                        value: i.to_string(),
                    },
                ])
            })
            .collect();
        let fps: Vec<u128> = (0..n as u128).collect();
        Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
    }

    /// Build a single-tile `MatrixWindowBatch` from scratch.
    ///
    /// `step_timestamps` — absolute step timestamps (ms).
    /// `cells` — row-major by step (outer = step, inner = series). Each
    /// inner vec is the packed `(ts, v)` sample list for that cell.
    fn build_window(
        step_timestamps: Vec<i64>,
        series_count: usize,
        cells: Vec<Vec<(i64, f64)>>,
    ) -> MatrixWindowBatch {
        let step_count = step_timestamps.len();
        assert_eq!(cells.len(), step_count * series_count);
        let schema = mk_schema(series_count);

        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        let mut cell_idx = Vec::with_capacity(cells.len());
        for samples in cells {
            let offset = timestamps.len() as u32;
            let len = samples.len() as u32;
            for (t, v) in samples {
                timestamps.push(t);
                values.push(v);
            }
            cell_idx.push(CellIndex { offset, len });
        }

        let ts_arc: Arc<[i64]> = Arc::from(step_timestamps);
        MatrixWindowBatch {
            step_timestamps: ts_arc.clone(),
            step_range: 0..step_count,
            series: SchemaRef::Static(schema),
            series_range: 0..series_count,
            timestamps,
            values,
            cells: cell_idx,
            effective_times: None,
        }
    }

    fn build_schema(
        step_timestamps: Vec<i64>,
        step_ms: i64,
        series_count: usize,
    ) -> OperatorSchema {
        let step_count = step_timestamps.len();
        let start_ms = step_timestamps.first().copied().unwrap_or(0);
        let end_ms = step_timestamps.last().copied().unwrap_or(0);
        let series = SchemaRef::Static(mk_schema(series_count));
        OperatorSchema::new(
            series,
            StepGrid {
                start_ms,
                end_ms,
                step_ms,
                step_count,
            },
        )
    }

    fn drive<W: WindowStream>(op: &mut RollupOp<W>) -> Vec<Result<StepBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(result)) => out.push(result),
                Poll::Pending => panic!("unexpected Pending from sync mock"),
            }
        }
    }

    fn approx_eq(a: f64, b: f64, eps: f64) -> bool {
        if a.is_nan() && b.is_nan() {
            return true;
        }
        if a.is_infinite() || b.is_infinite() {
            return a == b;
        }
        (a - b).abs() <= eps * (1.0 + a.abs().max(b.abs()))
    }

    // ========================================================================
    // required tests
    // ========================================================================

    #[test]
    fn should_compute_rate_over_simple_counter() {
        // given: single step t=40, range=40 → window (0, 40]; samples
        // 10, 20, 30, 40 at timestamps 10, 20, 30, 40. Result should be
        // extrapolated_rate, matching the v1 reference in functions.rs.
        let step_ts = vec![40];
        let series = 1;
        let cells = vec![vec![(10, 10.0), (20, 20.0), (30, 30.0), (40, 40.0)]];
        let window = build_window(step_ts.clone(), series, cells);
        let schema = build_schema(step_ts, 10, series);

        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(mock, RollupKind::Rate, 40, MemoryReservation::new(1 << 20));

        // when
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one cell, validity=1.
        // Prometheus extrapolated_rate math:
        //   last-first = 30; no resets; time_diff = 30ms = 0.03s
        //   avg_interval = 0.01s; extrapolation_threshold = 0.011s
        //   duration_to_start = 10ms = 0.01s (< threshold → keep)
        //   duration_to_end = 0 (< threshold → keep)
        //   factor_unit = (0.03 + 0.01 + 0) / 0.03 = 4/3
        //   scaled = 30 * 4/3 = 40
        //   rate = 40 / 0.04 = 1000/s
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        let v = b.get(0, 0).expect("cell should be valid");
        assert!(approx_eq(v, 1000.0, 1e-9), "rate = {v}");
    }

    #[test]
    fn should_handle_counter_reset_in_rate() {
        // given: counter values 10,20,5,15 at ts 10,20,30,40 — one reset
        // (20 → 5). `counter_increase_correction` returns 20.
        //   last-first = 15-10 = 5; +correction 20 → 25
        //   time_diff = 30ms = 0.03s; avg_interval = 0.01s
        //   duration_to_start = 10ms = 0.01s; duration_to_end = 0
        //   rate = 25 * (0.04/0.03) / 0.04 = 25/0.03 = 833.333.../s
        let step_ts = vec![40];
        let window = build_window(
            step_ts.clone(),
            1,
            vec![vec![(10, 10.0), (20, 20.0), (30, 5.0), (40, 15.0)]],
        );
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(mock, RollupKind::Rate, 40, MemoryReservation::new(1 << 20));

        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let v = batches[0].get(0, 0).unwrap();
        assert!(approx_eq(v, 25.0 / 0.03, 1e-9), "rate = {v}");
    }

    #[test]
    fn should_compute_increase_matching_pipeline() {
        // given: strict counter 0,10,20,30 at ts 10,20,30,40.
        // Same math as rate but without the per-second divide.
        //   last-first = 30; time_diff 30ms = 0.03s; avg_interval 0.01s
        //   duration_to_start = 10ms (< threshold → keep)
        //     BUT: counter-zero clip: first_v=0 ≥ 0 and result>0, so
        //          duration_to_zero = 0 * 30/30 = 0; duration_to_start = 0.
        //   duration_to_end = 0.
        //   factor = 0.03/0.03 = 1.0
        //   increase = 30 * 1 = 30
        let step_ts = vec![40];
        let window = build_window(
            step_ts.clone(),
            1,
            vec![vec![(10, 0.0), (20, 10.0), (30, 20.0), (40, 30.0)]],
        );
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::Increase,
            40,
            MemoryReservation::new(1 << 20),
        );

        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let v = batches[0].get(0, 0).unwrap();
        assert!(approx_eq(v, 30.0, 1e-9), "increase = {v}");
    }

    #[test]
    fn should_compute_avg_over_time() {
        // given: 2,4,6,8 → avg = 5.0
        let step_ts = vec![40];
        let window = build_window(
            step_ts.clone(),
            1,
            vec![vec![(10, 2.0), (20, 4.0), (30, 6.0), (40, 8.0)]],
        );
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::AvgOverTime,
            40,
            MemoryReservation::new(1 << 20),
        );

        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert!(approx_eq(batches[0].get(0, 0).unwrap(), 5.0, 1e-9));
    }

    #[test]
    fn should_compute_sum_min_max_count_over_time() {
        // given: values 3,1,4,1,5 — sum=14, min=1, max=5, count=5
        let step_ts = vec![50];
        let cells = || vec![vec![(10, 3.0), (20, 1.0), (30, 4.0), (40, 1.0), (50, 5.0)]];

        let schema = build_schema(step_ts.clone(), 10, 1);

        for (kind, expected) in [
            (RollupKind::SumOverTime, 14.0),
            (RollupKind::MinOverTime, 1.0),
            (RollupKind::MaxOverTime, 5.0),
            (RollupKind::CountOverTime, 5.0),
        ] {
            let window = build_window(step_ts.clone(), 1, cells());
            let mock = MockWindows::new(schema.clone(), vec![window]);
            let mut op = RollupOp::new(mock, kind, 50, MemoryReservation::new(1 << 20));
            let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
            let v = batches[0].get(0, 0).unwrap();
            assert!(
                approx_eq(v, expected, 1e-9),
                "{kind:?}: got {v}, want {expected}"
            );
        }
    }

    #[test]
    fn should_compute_irate_from_last_two_samples() {
        // given: 5 samples; irate uses only the last two.
        //   last two: (30, 100), (40, 160) → (160-100)/0.01s = 6000/s
        let step_ts = vec![40];
        let window = build_window(
            step_ts.clone(),
            1,
            vec![vec![
                (0, 1.0),
                (10, 10.0),
                (20, 50.0),
                (30, 100.0),
                (40, 160.0),
            ]],
        );
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(mock, RollupKind::Irate, 40, MemoryReservation::new(1 << 20));
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert!(approx_eq(batches[0].get(0, 0).unwrap(), 6000.0, 1e-9));
    }

    #[test]
    fn should_set_validity_zero_when_too_few_samples_for_rate() {
        // given: one sample — rate requires 2.
        let step_ts = vec![40];
        let window = build_window(step_ts.clone(), 1, vec![vec![(30, 5.0)]]);
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(mock, RollupKind::Rate, 40, MemoryReservation::new(1 << 20));
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(batches[0].get(0, 0), None);
    }

    #[test]
    fn should_set_validity_zero_when_window_empty() {
        // given: empty window — every rollup returns None (except
        // count_over_time which returns 0? Prometheus actually returns
        // "no result" for empty — absent cell. Our min_samples=1 enforces it.)
        let step_ts = vec![40];
        let window = build_window(step_ts.clone(), 1, vec![vec![]]);
        let schema = build_schema(step_ts.clone(), 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::SumOverTime,
            40,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(batches[0].get(0, 0), None);

        // Also verify for present_over_time specifically (which emits
        // 1 if any sample, else absent).
        let window = build_window(step_ts.clone(), 1, vec![vec![]]);
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::PresentOverTime,
            40,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(batches[0].get(0, 0), None);
    }

    #[test]
    fn should_compute_quantile_over_time() {
        // given: 1..=5. quantile(0.5) = 3; quantile(0.75) = 4.
        let step_ts = vec![50];
        let cells = || vec![vec![(10, 1.0), (20, 2.0), (30, 3.0), (40, 4.0), (50, 5.0)]];
        let schema = build_schema(step_ts.clone(), 10, 1);

        for (q, expected) in [(0.5_f64, 3.0), (0.75_f64, 4.0), (0.0, 1.0), (1.0, 5.0)] {
            let window = build_window(step_ts.clone(), 1, cells());
            let mock = MockWindows::new(schema.clone(), vec![window]);
            let mut op = RollupOp::new(
                mock,
                RollupKind::QuantileOverTime(q),
                50,
                MemoryReservation::new(1 << 20),
            );
            let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
            let v = batches[0].get(0, 0).unwrap();
            assert!(
                approx_eq(v, expected, 1e-9),
                "q={q}: got {v}, want {expected}"
            );
        }
    }

    #[test]
    fn should_compute_stddev_and_stdvar_over_time() {
        // given: 2,4,4,4,5,5,7,9 — population mean = 5, variance = 4, stddev = 2.
        let step_ts = vec![80];
        let cells = || {
            vec![vec![
                (10, 2.0),
                (20, 4.0),
                (30, 4.0),
                (40, 4.0),
                (50, 5.0),
                (60, 5.0),
                (70, 7.0),
                (80, 9.0),
            ]]
        };
        let schema = build_schema(step_ts.clone(), 10, 1);

        let window = build_window(step_ts.clone(), 1, cells());
        let mock = MockWindows::new(schema.clone(), vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::StdvarOverTime,
            80,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert!(approx_eq(batches[0].get(0, 0).unwrap(), 4.0, 1e-9));

        let window = build_window(step_ts.clone(), 1, cells());
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::StddevOverTime,
            80,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert!(approx_eq(batches[0].get(0, 0).unwrap(), 2.0, 1e-9));
    }

    #[test]
    fn should_compute_changes_and_resets() {
        // given: 1,1,2,3,3,2,5 — changes=5 (1→2, 2→3, 3→2, 2→5, also 3→3 not a change),
        //        let's recount: pairs (1,1),(1,2),(2,3),(3,3),(3,2),(2,5)
        //         changes at positions 1→2 (yes), 2→3 (yes), 3→2 (yes), 2→5 (yes) = 4
        //         resets: only decreases: 3→2 = 1
        let step_ts = vec![70];
        let values = vec![
            (10, 1.0),
            (20, 1.0),
            (30, 2.0),
            (40, 3.0),
            (50, 3.0),
            (60, 2.0),
            (70, 5.0),
        ];
        let schema = build_schema(step_ts.clone(), 10, 1);

        let window = build_window(step_ts.clone(), 1, vec![values.clone()]);
        let mock = MockWindows::new(schema.clone(), vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::Changes,
            70,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(batches[0].get(0, 0), Some(4.0));

        let window = build_window(step_ts.clone(), 1, vec![values]);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::Resets,
            70,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(batches[0].get(0, 0), Some(1.0));
    }

    #[test]
    fn should_return_static_schema() {
        // given: mock with a Static schema.
        let step_ts = vec![10];
        let window = build_window(step_ts.clone(), 1, vec![vec![(10, 1.0)]]);
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let op = RollupOp::new(
            mock,
            RollupKind::SumOverTime,
            10,
            MemoryReservation::new(1 << 20),
        );
        // when/then
        assert!(!op.schema().series.is_deferred());
        assert!(op.schema().series.as_static().is_some());
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: cap too small for even a single-cell output.
        let step_ts = vec![10];
        let window = build_window(step_ts.clone(), 1, vec![vec![(10, 1.0)]]);
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(mock, RollupKind::SumOverTime, 10, MemoryReservation::new(1));

        let results = drive(&mut op);
        let err = results
            .into_iter()
            .find_map(|r| r.err())
            .expect("expected MemoryLimit");
        assert!(matches!(err, QueryError::MemoryLimit { .. }));
    }

    #[test]
    fn should_pass_through_step_grid() {
        // given: upstream schema with a specific grid
        let step_ts = vec![100, 110, 120];
        let window = build_window(
            step_ts.clone(),
            1,
            vec![vec![(100, 1.0)], vec![(110, 2.0)], vec![(120, 3.0)]],
        );
        let schema = build_schema(step_ts.clone(), 10, 1);

        let expected_grid = schema.step_grid;
        let mock = MockWindows::new(schema, vec![window]);
        let op = RollupOp::new(
            mock,
            RollupKind::LastOverTime,
            10,
            MemoryReservation::new(1 << 20),
        );
        assert_eq!(op.schema().step_grid, expected_grid);
    }

    #[test]
    fn should_drive_two_pointer_walk_across_many_steps() {
        // given: 16 steps, one series per step with a single sample at
        // the step timestamp. last_over_time of a window covering just
        // that step = the step value.
        let step_ms = 10;
        let n = 16;
        let step_ts: Vec<i64> = (0..n).map(|k| 10 + k as i64 * step_ms).collect();
        let cells: Vec<Vec<(i64, f64)>> = step_ts
            .iter()
            .map(|&t| vec![(t, (t / 10) as f64)])
            .collect();
        let window = build_window(step_ts.clone(), 1, cells);
        let schema = build_schema(step_ts.clone(), step_ms, 1);

        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::LastOverTime,
            step_ms,
            MemoryReservation::new(1 << 20),
        );

        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        // then: one batch with n cells, each holding value = step_ts[i]/10.
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.step_count(), n);
        for (i, &t) in step_ts.iter().enumerate().take(n) {
            let v = b.get(i, 0).expect("valid cell");
            assert!(approx_eq(v, (t / 10) as f64, 1e-9));
        }
    }

    // ---- extra coverage (not strictly required but trivially cheap) --------

    #[test]
    fn should_compute_idelta_from_last_two_samples() {
        let step_ts = vec![40];
        let window = build_window(
            step_ts.clone(),
            1,
            vec![vec![(10, 1.0), (20, 5.0), (30, 7.0), (40, 12.0)]],
        );
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::Idelta,
            40,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert!(approx_eq(batches[0].get(0, 0).unwrap(), 5.0, 1e-9));
    }

    #[test]
    fn should_compute_delta_for_gauge() {
        // given: gauge values — delta does not correct for resets.
        //   10, 20, 5, 15 at ts 10,20,30,40
        //   last-first = 15-10 = 5 (NO correction)
        //   time_diff = 30ms; avg_interval = 0.01s
        //   duration_to_start = 10ms (< threshold → keep)
        //   NO counter-zero clip (Gauge path)
        //   duration_to_end = 0
        //   factor_unit = (0.03 + 0.01 + 0)/0.03 = 4/3
        //   delta = 5 * 4/3 ≈ 6.6667
        let step_ts = vec![40];
        let window = build_window(
            step_ts.clone(),
            1,
            vec![vec![(10, 10.0), (20, 20.0), (30, 5.0), (40, 15.0)]],
        );
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(mock, RollupKind::Delta, 40, MemoryReservation::new(1 << 20));
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert!(approx_eq(
            batches[0].get(0, 0).unwrap(),
            5.0 * 4.0 / 3.0,
            1e-9
        ));
    }

    #[test]
    fn should_emit_present_over_time_as_one_when_any_sample() {
        let step_ts = vec![40];
        let window = build_window(step_ts.clone(), 1, vec![vec![(30, 42.0)]]);
        let schema = build_schema(step_ts, 10, 1);
        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(
            mock,
            RollupKind::PresentOverTime,
            40,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(batches[0].get(0, 0), Some(1.0));
    }

    #[test]
    fn should_use_effective_times_for_window_math_when_present() {
        // given: outer step is t=25 (= `step_timestamps[0]`) but samples
        // live in the `(0, 100]` window (folded `@ 100`) — same shape as
        // `rate(metric[100s] @ 100)` at outer t=25s. With
        // `effective_times = Some([100])` the rollup must compute over
        // `(0, 100]`, not `(-75, 25]`.
        let step_ts = vec![25i64];
        let effective = vec![100i64];
        let mut window = build_window(
            step_ts.clone(),
            1,
            vec![vec![
                (10, 1.0),
                (20, 2.0),
                (30, 3.0),
                (40, 4.0),
                (50, 5.0),
                (60, 6.0),
                (70, 7.0),
                (80, 8.0),
                (90, 9.0),
                (100, 10.0),
            ]],
        );
        window.effective_times = Some(Arc::from(effective));
        let schema = build_schema(step_ts, 10, 1);

        let mock = MockWindows::new(schema, vec![window]);
        let mut op = RollupOp::new(mock, RollupKind::Rate, 100, MemoryReservation::new(1 << 20));

        // when
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: Prometheus extrapolated rate over (0, 100] with samples
        // at 10..100 (avg interval 10ms):
        //   last - first = 9; time_diff = 90ms = 0.09s
        //   dur_to_start = 10ms = 0.01s (< 1.1 * avg = 0.011 → keep)
        //   dur_to_end = 0
        //   factor = (0.09 + 0.01 + 0) / 0.09 = 10/9
        //   scaled = 9 * 10/9 = 10
        //   rate = 10 / 0.1 = 100/s  (legacy v1 evaluator matches)
        let v = batches[0].get(0, 0).expect("rate valid");
        assert!(
            approx_eq(v, 100.0, 1e-9),
            "rate over effective window (0, 100]: {v}"
        );
    }

    #[test]
    fn should_propagate_error_from_upstream() {
        // given: upstream yields an error — Rollup should pass it through
        // and refuse further polls.
        let schema = build_schema(vec![10], 10, 1);
        let mock = MockWindows {
            schema,
            queue: vec![Err(QueryError::MemoryLimit {
                requested: 10,
                cap: 0,
                already_reserved: 0,
            })],
        };
        let mut op = RollupOp::new(
            mock,
            RollupKind::SumOverTime,
            10,
            MemoryReservation::new(1 << 20),
        );

        let results = drive(&mut op);
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(QueryError::MemoryLimit { .. })));
    }

    /// Build a tile [`MatrixWindowBatch`] over `series_range` in a roster
    /// of `total_series`. `cells` is row-major across the tile.
    fn build_window_tile(
        step_timestamps: Vec<i64>,
        total_series: usize,
        series_range: std::ops::Range<usize>,
        cells: Vec<Vec<(i64, f64)>>,
    ) -> MatrixWindowBatch {
        let step_count = step_timestamps.len();
        let tile_sc = series_range.len();
        assert_eq!(cells.len(), step_count * tile_sc);
        let schema = mk_schema(total_series);

        let mut timestamps = Vec::new();
        let mut values = Vec::new();
        let mut cell_idx = Vec::with_capacity(cells.len());
        for samples in cells {
            let offset = timestamps.len() as u32;
            let len = samples.len() as u32;
            for (t, v) in samples {
                timestamps.push(t);
                values.push(v);
            }
            cell_idx.push(CellIndex { offset, len });
        }

        let ts_arc: Arc<[i64]> = Arc::from(step_timestamps);
        MatrixWindowBatch {
            step_timestamps: ts_arc.clone(),
            step_range: 0..step_count,
            series: SchemaRef::Static(schema),
            series_range,
            timestamps,
            values,
            cells: cell_idx,
            effective_times: None,
        }
    }

    #[test]
    fn should_reduce_multi_series_tile_window_batches_over_512_series() {
        // given: `RollupOp` consumes `MatrixWindowBatch`es from its child
        // `MatrixSelectorOp`, which tiles the same step range into
        // per-512-series batches for rosters >512 series. Each input tile
        // should round-trip to one output `StepBatch` tile with the same
        // series_range — the operator is per-batch stateless.
        const SERIES: usize = 1024;
        const TILE: usize = 512;
        let step_ts = vec![10_i64];
        let schema = build_schema(step_ts.clone(), 10, SERIES);

        // Each cell: one sample at t=10 with value = global series idx.
        let mut cells_a: Vec<Vec<(i64, f64)>> = Vec::with_capacity(TILE);
        for s in 0..TILE {
            cells_a.push(vec![(10, s as f64)]);
        }
        let mut cells_b: Vec<Vec<(i64, f64)>> = Vec::with_capacity(TILE);
        for s in TILE..SERIES {
            cells_b.push(vec![(10, s as f64)]);
        }
        let tile_a = build_window_tile(step_ts.clone(), SERIES, 0..TILE, cells_a);
        let tile_b = build_window_tile(step_ts, SERIES, TILE..SERIES, cells_b);

        let mock = MockWindows::new(schema, vec![tile_a, tile_b]);

        // when: sum_over_time emits the value as-is (one sample per cell).
        let mut op = RollupOp::new(
            mock,
            RollupKind::SumOverTime,
            10,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: two output batches preserving the child's tile shape,
        // each cell carrying the global series index it covers.
        assert_eq!(outs.len(), 2);
        assert_eq!(outs[0].series_range, 0..TILE);
        assert_eq!(outs[1].series_range, TILE..SERIES);
        for s in 0..TILE {
            assert_eq!(outs[0].get(0, s), Some(s as f64));
        }
        for s in 0..(SERIES - TILE) {
            let global = TILE + s;
            assert_eq!(outs[1].get(0, s), Some(global as f64));
        }
    }
}

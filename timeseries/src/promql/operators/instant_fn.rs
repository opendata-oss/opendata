//! `InstantFnOp` implements PromQL's pointwise scalar functions (`abs`,
//! `ln`, `round`, `clamp`, ...) — every function that takes one sample
//! and produces one sample at the same `(step, series)` position. One
//! operator type, with the specific function selected by an
//! [`InstantFnKind`] enum; dispatch is a single `match` per cell.
//!
//! Because the transformation is cell-for-cell, the child's series schema
//! and step grid pass through unchanged. The operator never rearranges
//! series and never buffers across steps.
//!
//! Scope: every pointwise float→float function the legacy engine ships,
//! plus `sgn`. Excluded: `absent` / `absent_over_time` (derive their own
//! schema), `histogram_quantile` (native histograms, out of scope),
//! `scalar` / `vector` / `pi` / `time` (planner-level coercions),
//! `label_replace` / `label_join` (label mutation — see [`super::label_manip`]).
//!
//! `timestamp()` returns the value of
//! [`StepBatch::source_timestamps`](super::super::batch::StepBatch::source_timestamps)
//! for the cell when present, else falls back to the step timestamp (in
//! seconds). [`VectorSelectorOp`](super::vector_selector::VectorSelectorOp)
//! is the only operator that populates `source_timestamps`; any operator
//! that derives a new value drops the column.
//!
//! Validity: `InstantFnOp` never flips bits — a clear input cell stays
//! clear, a valid input cell stays valid even when the result is `NaN` /
//! `±Inf`. The output batch's `validity` is pointer-cloned from the input;
//! only `values` is charged through [`MemoryReservation::try_grow`].

use std::task::{Context, Poll};

use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};

use super::super::batch::{BitSet, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};

// ---------------------------------------------------------------------------
// InstantFnKind — one operator, function selection as data
// ---------------------------------------------------------------------------

/// Per-cell reducer. `Round` / `Clamp*` carry their plan-time scalar args
/// inline (constant across steps and series per PromQL).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum InstantFnKind {
    // --- unary math / transcendentals ---
    Abs,
    Ceil,
    Floor,
    Exp,
    Ln,
    Log2,
    Log10,
    Sqrt,

    // --- trig ---
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,

    // --- hyperbolic ---
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,

    // --- angular conversion ---
    Deg,
    Rad,

    // --- sign ---
    Sgn,

    // --- round with plan-time `to_nearest` (defaults to 1.0 if unset) ---
    /// `round(v, to_nearest)` — rounds to the nearest multiple of
    /// `to_nearest`, tie-breaking toward `+∞` (matching the legacy engine
    /// at `timeseries/src/promql/functions.rs:216-223`). `to_nearest == 0`
    /// is a no-op per that impl; legal values are any real.
    Round {
        to_nearest: f64,
    },

    // --- clamp family with plan-time scalar bounds ---
    /// `clamp(v, min, max)` — NaN-propagating; `min > max` is caller's
    /// concern (legacy returns an empty vector in that case; this
    /// operator is not in charge of shaping — the planner emits an empty
    /// schema when it detects the case).
    Clamp {
        min: f64,
        max: f64,
    },
    ClampMin {
        min: f64,
    },
    ClampMax {
        max: f64,
    },

    /// `timestamp(v)` — returns the matching **source sample's**
    /// timestamp in seconds when the input cell carries one (the common
    /// case: `timestamp(metric @ t)` and similar bare-selector inputs,
    /// populated by `VectorSelectorOp` into
    /// `StepBatch::source_timestamps`). Falls back to the output step
    /// timestamp when the input is derived (e.g. `timestamp(rate(…))` —
    /// any operator between the source and `timestamp()` drops the
    /// source-timestamp column, matching Prometheus semantics at
    /// `at_modifier.test:193–201`).
    Timestamp,

    /// Calendar/date extraction functions operating on seconds-since-epoch
    /// values (or `vector(time())` for the zero-arg forms).
    Year,
    Month,
    DayOfMonth,
    DayOfYear,
    DayOfWeek,
    Hour,
    Minute,
    DaysInMonth,
}

impl InstantFnKind {
    /// Reduce a single cell's `(value, step_timestamp_ms)` to a scalar.
    ///
    /// `step_timestamp_ms` is the output step timestamp for the cell (in
    /// absolute UTC ms); only [`Self::Timestamp`] reads it. Called only
    /// for cells whose input validity bit is set.
    #[inline]
    pub fn compute(&self, v: f64, step_timestamp_ms: i64) -> f64 {
        match *self {
            Self::Abs => v.abs(),
            Self::Ceil => v.ceil(),
            Self::Floor => v.floor(),
            Self::Exp => v.exp(),
            Self::Ln => v.ln(),
            Self::Log2 => v.log2(),
            Self::Log10 => v.log10(),
            Self::Sqrt => v.sqrt(),
            Self::Sin => v.sin(),
            Self::Cos => v.cos(),
            Self::Tan => v.tan(),
            Self::Asin => v.asin(),
            Self::Acos => v.acos(),
            Self::Atan => v.atan(),
            Self::Sinh => v.sinh(),
            Self::Cosh => v.cosh(),
            Self::Tanh => v.tanh(),
            Self::Asinh => v.asinh(),
            Self::Acosh => v.acosh(),
            Self::Atanh => v.atanh(),
            Self::Deg => v.to_degrees(),
            Self::Rad => v.to_radians(),
            Self::Sgn => {
                // Prometheus semantics: -1, 0, +1; NaN passes through.
                if v.is_nan() {
                    f64::NAN
                } else if v > 0.0 {
                    1.0
                } else if v < 0.0 {
                    -1.0
                } else {
                    0.0
                }
            }
            Self::Round { to_nearest } => round_to_nearest(v, to_nearest),
            Self::Clamp { min, max } => clamp_nan_aware(v, min, max),
            Self::ClampMin { min } => max_nan_aware(v, min),
            Self::ClampMax { max } => min_nan_aware(v, max),
            Self::Timestamp => step_timestamp_ms as f64 / 1000.0,
            Self::Year => datetime_from_seconds(v)
                .map(|dt| dt.year() as f64)
                .unwrap_or(f64::NAN),
            Self::Month => datetime_from_seconds(v)
                .map(|dt| dt.month() as f64)
                .unwrap_or(f64::NAN),
            Self::DayOfMonth => datetime_from_seconds(v)
                .map(|dt| dt.day() as f64)
                .unwrap_or(f64::NAN),
            Self::DayOfYear => datetime_from_seconds(v)
                .map(|dt| dt.ordinal() as f64)
                .unwrap_or(f64::NAN),
            Self::DayOfWeek => datetime_from_seconds(v)
                .map(|dt| dt.weekday().num_days_from_sunday() as f64)
                .unwrap_or(f64::NAN),
            Self::Hour => datetime_from_seconds(v)
                .map(|dt| dt.hour() as f64)
                .unwrap_or(f64::NAN),
            Self::Minute => datetime_from_seconds(v)
                .map(|dt| dt.minute() as f64)
                .unwrap_or(f64::NAN),
            Self::DaysInMonth => datetime_from_seconds(v)
                .map(|dt| days_in_month(dt) as f64)
                .unwrap_or(f64::NAN),
        }
    }
}

#[inline]
fn datetime_from_seconds(value: f64) -> Option<DateTime<Utc>> {
    if !value.is_finite() {
        return None;
    }

    let seconds = value.trunc();
    if !(i64::MIN as f64..=i64::MAX as f64).contains(&seconds) {
        return None;
    }

    DateTime::from_timestamp(seconds as i64, 0)
}

#[inline]
fn days_in_month(dt: DateTime<Utc>) -> u32 {
    let start_of_month =
        NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).expect("valid start of month");
    let start_of_next_month = if dt.month() == 12 {
        NaiveDate::from_ymd_opt(dt.year() + 1, 1, 1).expect("valid start of next month")
    } else {
        NaiveDate::from_ymd_opt(dt.year(), dt.month() + 1, 1).expect("valid start of next month")
    };

    start_of_next_month
        .signed_duration_since(start_of_month)
        .num_days() as u32
}

/// `round(v, to_nearest)` — ported from
/// `timeseries/src/promql/functions.rs:216-223`. Matches Prometheus'
/// half-up rounding: `(v * inv + 0.5).floor() / inv`.
#[inline]
fn round_to_nearest(value: f64, to_nearest: f64) -> f64 {
    if to_nearest == 0.0 {
        return value;
    }
    let inv = 1.0 / to_nearest;
    (value * inv + 0.5).floor() / inv
}

/// NaN-propagating `min`. Matches `timeseries/src/promql/functions.rs:269-277`.
#[inline]
fn min_nan_aware(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left < right {
        left
    } else {
        right
    }
}

/// NaN-propagating `max`. Matches `timeseries/src/promql/functions.rs:279-287`.
#[inline]
fn max_nan_aware(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else if left > right {
        left
    } else {
        right
    }
}

/// `clamp(v, min, max)` — NaN-propagating, matches the legacy engine's
/// `max(min(v, max), min)` composition at
/// `timeseries/src/promql/functions.rs:441`.
#[inline]
fn clamp_nan_aware(v: f64, min: f64, max: f64) -> f64 {
    max_nan_aware(min_nan_aware(v, max), min)
}

// ---------------------------------------------------------------------------
// Memory-accounted output buffer (values only — validity is pointer-cloned)
// ---------------------------------------------------------------------------

#[inline]
fn values_bytes(cells: usize) -> usize {
    cells.saturating_mul(std::mem::size_of::<f64>())
}

/// RAII wrapper around a fresh `Vec<f64>` output column. Reserves on
/// construction; releases on `Drop`. `finish()` transfers the `Vec` to
/// the caller and releases the reservation. Downstream re-reserves if
/// it retains the batch.
struct OutValues {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
}

impl OutValues {
    fn allocate(reservation: &MemoryReservation, cells: usize) -> Result<Self, QueryError> {
        let bytes = values_bytes(cells);
        reservation.try_grow(bytes)?;
        // NaN-fill so accidental reads of invalid cells surface as NaN
        // rather than zero — matches the 3a.1 convention.
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            values: vec![f64::NAN; cells],
        })
    }

    fn finish(mut self) -> Vec<f64> {
        let values = std::mem::take(&mut self.values);
        self.reservation.release(self.bytes);
        self.bytes = 0;
        values
    }
}

impl Drop for OutValues {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// InstantFnOp — the operator
// ---------------------------------------------------------------------------

/// Implements PromQL's pointwise functions (`abs`, `ln`, `round`, ...).
/// Applies the reducer named by [`InstantFnKind`] to each cell in every
/// [`StepBatch`] pulled from `child`, preserving the child's series
/// schema and step grid.
///
/// See module docs for function taxonomy, timestamp semantics, and the
/// validity / memory-accounting policy.
pub struct InstantFnOp<C: Operator> {
    child: C,
    kind: InstantFnKind,
    reservation: MemoryReservation,
    /// Passthrough of the child's schema — identical series roster, step
    /// grid, and deferred-ness. Captured at construction so the operator
    /// can hand out `&OperatorSchema` without re-querying the child.
    schema: OperatorSchema,
    errored: bool,
    done: bool,
}

impl<C: Operator> InstantFnOp<C> {
    /// Wrap `child` with a pointwise reducer.
    ///
    /// * `child` — any upstream operator producing `StepBatch`es.
    /// * `kind` — plan-time-selected pointwise function (with any plan-time
    ///   scalar arguments baked in; see [`InstantFnKind`]).
    /// * `reservation` — per-query reservation the output values column
    ///   is charged against.
    pub fn new(child: C, kind: InstantFnKind, reservation: MemoryReservation) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            kind,
            reservation,
            schema,
            errored: false,
            done: false,
        }
    }

    fn apply_batch(&self, batch: StepBatch) -> Result<StepBatch, QueryError> {
        let cell_count = batch.len();
        let mut out = OutValues::allocate(&self.reservation, cell_count)?;

        if let InstantFnKind::Clamp { min, max } = self.kind
            && min > max
        {
            let values = out.finish();
            return Ok(StepBatch::new(
                batch.step_timestamps.clone(),
                batch.step_range.clone(),
                batch.series.clone(),
                batch.series_range.clone(),
                values,
                BitSet::with_len(cell_count),
            ));
        }

        let series_count = batch.series_count();
        let step_ts = batch.step_timestamps_slice();

        // Iterate cells in row-major (step, series) order — matches
        // `StepBatch`'s layout. Only touch valid cells; invalid cells
        // keep the NaN fill and the pointer-cloned validity bit clear.
        // `timestamp()` prefers the source-sample timestamp when the
        // input batch carries one (RFC 0007 §6.3.7); all other kinds
        // ignore the column.
        let source_ts: Option<&[i64]> = batch.source_timestamps.as_deref();
        for (step_off, &step_ms) in step_ts.iter().enumerate().take(batch.step_count()) {
            for series_off in 0..series_count {
                let idx = step_off * series_count + series_off;
                if batch.validity.get(idx) {
                    let per_cell_ts = match (self.kind, source_ts) {
                        (InstantFnKind::Timestamp, Some(ts)) => ts[idx],
                        _ => step_ms,
                    };
                    out.values[idx] = self.kind.compute(batch.values[idx], per_cell_ts);
                }
            }
        }

        let values = out.finish();
        Ok(StepBatch::new(
            batch.step_timestamps.clone(),
            batch.step_range.clone(),
            batch.series.clone(),
            batch.series_range.clone(),
            values,
            // Pointer-clone: the function never produces new absences, so
            // the output validity matches the input bit-for-bit. See the
            // module-level "Validity policy" note.
            batch.validity.clone(),
        ))
    }
}

impl<C: Operator> Operator for InstantFnOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }
        match self.child.next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(err))) => {
                self.errored = true;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(Some(Ok(batch))) => match self.apply_batch(batch) {
                Ok(out) => Poll::Ready(Some(Ok(out))),
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels};
    use crate::promql::batch::{BitSet, SchemaRef, SeriesSchema};
    use crate::promql::operator::StepGrid;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    // ---- test fixtures ------------------------------------------------------

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

    fn mk_operator_schema(step_timestamps: &[i64], step_ms: i64, series: usize) -> OperatorSchema {
        let start_ms = step_timestamps.first().copied().unwrap_or(0);
        let end_ms = step_timestamps.last().copied().unwrap_or(0);
        OperatorSchema::new(
            SchemaRef::Static(mk_schema(series)),
            StepGrid {
                start_ms,
                end_ms,
                step_ms,
                step_count: step_timestamps.len(),
            },
        )
    }

    /// Build a single `StepBatch` from a row-major `values` vector and a
    /// parallel `validity` vector of `bool`s.
    fn mk_batch(
        step_timestamps: Vec<i64>,
        series_count: usize,
        values: Vec<f64>,
        validity: Vec<bool>,
    ) -> StepBatch {
        let step_count = step_timestamps.len();
        assert_eq!(values.len(), step_count * series_count);
        assert_eq!(validity.len(), values.len());
        let schema = mk_schema(series_count);
        let mut bits = BitSet::with_len(validity.len());
        for (i, &b) in validity.iter().enumerate() {
            if b {
                bits.set(i);
            }
        }
        StepBatch::new(
            Arc::from(step_timestamps),
            0..step_count,
            SchemaRef::Static(schema),
            0..series_count,
            values,
            bits,
        )
    }

    /// Mock upstream operator. Yields a pre-built queue of batches (or
    /// errors) in order, then `Ready(None)` forever.
    struct MockChild {
        schema: OperatorSchema,
        queue: Vec<Result<StepBatch, QueryError>>,
    }

    impl MockChild {
        fn new(schema: OperatorSchema, batches: Vec<StepBatch>) -> Self {
            Self {
                schema,
                queue: batches.into_iter().map(Ok).collect(),
            }
        }

        fn with_queue(schema: OperatorSchema, queue: Vec<Result<StepBatch, QueryError>>) -> Self {
            Self { schema, queue }
        }
    }

    impl Operator for MockChild {
        fn schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
            if self.queue.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(self.queue.remove(0)))
            }
        }
    }

    fn drive<C: Operator>(op: &mut InstantFnOp<C>) -> Vec<Result<StepBatch, QueryError>> {
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
    fn should_apply_abs_to_each_cell() {
        // given: two steps × two series, mixed signs, all valid
        let ts = vec![1_000, 2_000];
        let values = vec![-1.0, 2.0, -3.5, 4.5];
        let validity = vec![true; 4];
        let batch = mk_batch(ts.clone(), 2, values, validity);
        let schema = mk_operator_schema(&ts, 1_000, 2);
        let child = MockChild::new(schema, vec![batch]);

        // when
        let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.get(0, 0), Some(1.0));
        assert_eq!(b.get(0, 1), Some(2.0));
        assert_eq!(b.get(1, 0), Some(3.5));
        assert_eq!(b.get(1, 1), Some(4.5));
    }

    #[test]
    fn should_preserve_validity_bits_from_child() {
        // given: 1 step × 3 series, middle cell invalid
        let ts = vec![1_000];
        let values = vec![-1.0, 999.0, 3.0];
        let validity = vec![true, false, true];
        let batch = mk_batch(ts.clone(), 3, values, validity);
        let schema = mk_operator_schema(&ts, 1_000, 3);
        let child = MockChild::new(schema, vec![batch]);

        // when
        let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: invalid cell stays invalid; valid cells computed
        let b = &batches[0];
        assert_eq!(b.get(0, 0), Some(1.0));
        assert_eq!(b.get(0, 1), None);
        assert_eq!(b.get(0, 2), Some(3.0));
    }

    #[test]
    fn should_apply_clamp_with_plan_time_bounds() {
        // given: values span below / within / above [2.0, 5.0]
        let ts = vec![1_000];
        let values = vec![1.0, 3.0, 6.0, f64::NAN];
        let validity = vec![true; 4];
        let batch = mk_batch(ts.clone(), 4, values, validity);
        let schema = mk_operator_schema(&ts, 1_000, 4);
        let child = MockChild::new(schema, vec![batch]);

        // when
        let mut op = InstantFnOp::new(
            child,
            InstantFnKind::Clamp { min: 2.0, max: 5.0 },
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let b = &batches[0];
        assert_eq!(b.get(0, 0), Some(2.0));
        assert_eq!(b.get(0, 1), Some(3.0));
        assert_eq!(b.get(0, 2), Some(5.0));
        assert!(b.get(0, 3).unwrap().is_nan(), "NaN should propagate");
    }

    #[test]
    fn should_emit_no_samples_for_clamp_when_min_exceeds_max() {
        // given
        let ts = vec![1_000];
        let values = vec![1.0, 3.0, 6.0];
        let validity = vec![true; 3];
        let batch = mk_batch(ts.clone(), 3, values, validity);
        let schema = mk_operator_schema(&ts, 1_000, 3);
        let child = MockChild::new(schema, vec![batch]);

        // when
        let mut op = InstantFnOp::new(
            child,
            InstantFnKind::Clamp {
                min: 5.0,
                max: -5.0,
            },
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let batch = &batches[0];
        assert_eq!(batch.get(0, 0), None);
        assert_eq!(batch.get(0, 1), None);
        assert_eq!(batch.get(0, 2), None);
    }

    #[test]
    fn should_apply_clamp_min_and_clamp_max() {
        // given: 4 values exercising both tails
        let ts = vec![1_000];
        let values = vec![-1.0, 0.0, 1.0, 2.0];
        let validity = vec![true; 4];
        let schema = mk_operator_schema(&ts, 1_000, 4);

        let child = MockChild::new(
            schema.clone(),
            vec![mk_batch(ts.clone(), 4, values.clone(), validity.clone())],
        );
        let mut op = InstantFnOp::new(
            child,
            InstantFnKind::ClampMin { min: 0.0 },
            MemoryReservation::new(1 << 20),
        );
        let b = &drive(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(0.0));
        assert_eq!(b.get(0, 1), Some(0.0));
        assert_eq!(b.get(0, 2), Some(1.0));
        assert_eq!(b.get(0, 3), Some(2.0));

        let child = MockChild::new(schema, vec![mk_batch(ts, 4, values, validity)]);
        let mut op = InstantFnOp::new(
            child,
            InstantFnKind::ClampMax { max: 1.0 },
            MemoryReservation::new(1 << 20),
        );
        let b = &drive(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(-1.0));
        assert_eq!(b.get(0, 1), Some(0.0));
        assert_eq!(b.get(0, 2), Some(1.0));
        assert_eq!(b.get(0, 3), Some(1.0));
    }

    #[test]
    fn should_handle_ln_of_nonpositive() {
        // given: ln(0) → -inf; ln(-1) → NaN (per Prometheus UnaryFunction
        // at timeseries/src/promql/functions.rs:203-211 which delegates to
        // f64::ln directly). Validity stays set in both cases — the legacy
        // engine writes `sample.value = self.op(...)` without flipping the
        // sample's presence.
        let ts = vec![1_000];
        let values = vec![0.0_f64, -1.0, 1.0, std::f64::consts::E];
        let validity = vec![true; 4];
        let batch = mk_batch(ts.clone(), 4, values, validity);
        let schema = mk_operator_schema(&ts, 1_000, 4);
        let child = MockChild::new(schema, vec![batch]);

        // when
        let mut op = InstantFnOp::new(child, InstantFnKind::Ln, MemoryReservation::new(1 << 20));
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let b = &batches[0];
        let ln_zero = b.get(0, 0).expect("validity preserved across ln(0)");
        assert!(ln_zero.is_infinite() && ln_zero.is_sign_negative());
        let ln_neg = b.get(0, 1).expect("validity preserved across ln(-1)");
        assert!(ln_neg.is_nan());
        assert!(approx_eq(b.get(0, 2).unwrap(), 0.0, 1e-9));
        assert!(approx_eq(b.get(0, 3).unwrap(), 1.0, 1e-9));
    }

    #[test]
    fn should_apply_timestamp_falling_back_to_step_time_without_source_column() {
        // given: two steps at t=1_500 ms and 3_000 ms; the input batch
        // carries no `source_timestamps` column (the producer was a
        // derived operator, e.g. `rate` or a binary op). `timestamp()`
        // therefore returns the step timestamp in seconds, matching
        // Prometheus' behaviour for derived inputs.
        let ts = vec![1_500, 3_000];
        let values = vec![7.0, -42.0];
        let validity = vec![true, true];
        let batch = mk_batch(ts.clone(), 1, values, validity);
        let schema = mk_operator_schema(&ts, 1_500, 1);
        let child = MockChild::new(schema, vec![batch]);

        // when
        let mut op = InstantFnOp::new(
            child,
            InstantFnKind::Timestamp,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let b = &batches[0];
        assert!(approx_eq(b.get(0, 0).unwrap(), 1.5, 1e-9));
        assert!(approx_eq(b.get(1, 0).unwrap(), 3.0, 1e-9));
    }

    #[test]
    fn should_apply_timestamp_returning_source_sample_timestamp_when_available() {
        // given: two steps at step_ms 1_500 / 3_000, but the input
        // batch was produced by a bare vector selector which stamped
        // per-cell source timestamps (the actual matching-sample times,
        // e.g. from an `@ t` pin). `timestamp()` must prefer those over
        // the step timestamp.
        let ts = vec![1_500, 3_000];
        let values = vec![7.0, -42.0];
        let validity = vec![true, true];
        let batch = mk_batch(ts.clone(), 1, values, validity)
            .with_source_timestamps(Arc::from(vec![10_000i64, 10_000]));
        let schema = mk_operator_schema(&ts, 1_500, 1);
        let child = MockChild::new(schema, vec![batch]);

        // when
        let mut op = InstantFnOp::new(
            child,
            InstantFnKind::Timestamp,
            MemoryReservation::new(1 << 20),
        );
        let batches: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: both cells return the source sample timestamp (10s),
        // not the step time.
        let b = &batches[0];
        assert!(approx_eq(b.get(0, 0).unwrap(), 10.0, 1e-9));
        assert!(approx_eq(b.get(1, 0).unwrap(), 10.0, 1e-9));
        // and: the operator's own output does NOT carry source
        // timestamps — derived values have no source-timestamp concept,
        // so a wrapping `timestamp()` falls back to step time.
        assert!(b.source_timestamps.is_none());
    }

    #[test]
    fn should_apply_calendar_extractions_from_epoch_seconds() {
        // given: 2006-01-02 22:04:05 UTC, the canonical timestamp used by the
        // legacy function tests.
        let ts = vec![0];
        let values = vec![1_136_239_445.0];
        let validity = vec![true];
        let schema = mk_operator_schema(&ts, 0, 1);

        let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
        let child = MockChild::new(schema.clone(), vec![batch]);
        let mut year =
            InstantFnOp::new(child, InstantFnKind::Year, MemoryReservation::new(1 << 20));
        let b = &drive(&mut year)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(2006.0));

        let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
        let child = MockChild::new(schema.clone(), vec![batch]);
        let mut month =
            InstantFnOp::new(child, InstantFnKind::Month, MemoryReservation::new(1 << 20));
        let b = &drive(&mut month)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(1.0));

        let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
        let child = MockChild::new(schema.clone(), vec![batch]);
        let mut dom = InstantFnOp::new(
            child,
            InstantFnKind::DayOfMonth,
            MemoryReservation::new(1 << 20),
        );
        let b = &drive(&mut dom)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(2.0));

        let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
        let child = MockChild::new(schema.clone(), vec![batch]);
        let mut dow = InstantFnOp::new(
            child,
            InstantFnKind::DayOfWeek,
            MemoryReservation::new(1 << 20),
        );
        let b = &drive(&mut dow)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(1.0));

        let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
        let child = MockChild::new(schema.clone(), vec![batch]);
        let mut hour =
            InstantFnOp::new(child, InstantFnKind::Hour, MemoryReservation::new(1 << 20));
        let b = &drive(&mut hour)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(22.0));

        let batch = mk_batch(ts.clone(), 1, values.clone(), validity.clone());
        let child = MockChild::new(schema.clone(), vec![batch]);
        let mut minute = InstantFnOp::new(
            child,
            InstantFnKind::Minute,
            MemoryReservation::new(1 << 20),
        );
        let b = &drive(&mut minute)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(4.0));

        let batch = mk_batch(ts, 1, values, validity);
        let child = MockChild::new(schema, vec![batch]);
        let mut dim = InstantFnOp::new(
            child,
            InstantFnKind::DaysInMonth,
            MemoryReservation::new(1 << 20),
        );
        let b = &drive(&mut dim)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(31.0));
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: reservation too small for even a single-cell output
        let ts = vec![1_000];
        let batch = mk_batch(ts.clone(), 1, vec![1.0], vec![true]);
        let schema = mk_operator_schema(&ts, 1_000, 1);
        let child = MockChild::new(schema, vec![batch]);

        // when: 1-byte cap → can't fit any f64
        let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1));
        let results = drive(&mut op);

        // then
        let err = results
            .into_iter()
            .find_map(|r| r.err())
            .expect("expected MemoryLimit");
        assert!(matches!(err, QueryError::MemoryLimit { .. }));
    }

    #[test]
    fn should_passthrough_static_schema() {
        // given: child with a Static schema
        let ts = vec![1_000];
        let batch = mk_batch(ts.clone(), 2, vec![1.0, 2.0], vec![true, true]);
        let schema = mk_operator_schema(&ts, 1_000, 2);
        let expected_grid = schema.step_grid;
        let child = MockChild::new(schema, vec![batch]);

        // when
        let op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));

        // then: schema passes through unchanged — same step grid, same
        // Static series handle, same underlying roster pointer.
        assert!(!op.schema().series.is_deferred());
        assert_eq!(op.schema().step_grid, expected_grid);
        assert_eq!(op.schema().series.as_static().unwrap().len(), 2);
    }

    #[test]
    fn should_drain_child_and_end_stream() {
        // given: two batches then end-of-stream from the child.
        let ts = vec![1_000];
        let b1 = mk_batch(ts.clone(), 1, vec![1.0], vec![true]);
        let b2 = mk_batch(ts.clone(), 1, vec![2.0], vec![true]);
        let schema = mk_operator_schema(&ts, 1_000, 1);
        let child = MockChild::new(schema, vec![b1, b2]);

        // when
        let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
        let results = drive(&mut op);

        // then: exactly two batches, then the operator yields nothing
        // further. Re-polling after None stays None (idempotent).
        assert_eq!(results.len(), 2);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        matches!(op.next(&mut cx), Poll::Ready(None));
        matches!(op.next(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn should_apply_trig_function() {
        // given: values at 0, π/2, π — sin(0)=0, sin(π/2)=1, sin(π)≈0
        let ts = vec![1_000];
        let values = vec![0.0, std::f64::consts::FRAC_PI_2, std::f64::consts::PI];
        let validity = vec![true; 3];
        let schema = mk_operator_schema(&ts, 1_000, 3);

        let batch = mk_batch(ts.clone(), 3, values.clone(), validity.clone());
        let child = MockChild::new(schema.clone(), vec![batch]);
        let mut op = InstantFnOp::new(child, InstantFnKind::Sin, MemoryReservation::new(1 << 20));
        let b = &drive(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert!(approx_eq(b.get(0, 0).unwrap(), 0.0, 1e-12));
        assert!(approx_eq(b.get(0, 1).unwrap(), 1.0, 1e-12));
        assert!(approx_eq(b.get(0, 2).unwrap(), 0.0, 1e-12));

        // and cos(0)=1, cos(π/2)≈0, cos(π)=-1
        let batch = mk_batch(ts, 3, values, validity);
        let child = MockChild::new(schema, vec![batch]);
        let mut op = InstantFnOp::new(child, InstantFnKind::Cos, MemoryReservation::new(1 << 20));
        let b = &drive(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert!(approx_eq(b.get(0, 0).unwrap(), 1.0, 1e-12));
        assert!(approx_eq(b.get(0, 1).unwrap(), 0.0, 1e-12));
        assert!(approx_eq(b.get(0, 2).unwrap(), -1.0, 1e-12));
    }

    #[test]
    fn should_apply_round_with_to_nearest() {
        // given: values that round differently for to_nearest=1.0 vs 0.5
        // Prometheus tie-breaking: (v*inv + 0.5).floor() / inv — half-up
        // toward +∞. round(1.5, 1.0) = 2.0; round(2.5, 1.0) = 3.0;
        // round(1.25, 0.5) = 1.5 (since 1.25/0.5 + 0.5 = 3.0 → floor 3 →
        // 1.5).
        let ts = vec![1_000];
        let values = vec![1.5, 2.5, 1.25, -1.5];
        let validity = vec![true; 4];
        let schema = mk_operator_schema(&ts, 1_000, 4);

        let batch = mk_batch(ts.clone(), 4, values.clone(), validity.clone());
        let child = MockChild::new(schema.clone(), vec![batch]);
        let mut op = InstantFnOp::new(
            child,
            InstantFnKind::Round { to_nearest: 1.0 },
            MemoryReservation::new(1 << 20),
        );
        let b = &drive(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        assert_eq!(b.get(0, 0), Some(2.0));
        assert_eq!(b.get(0, 1), Some(3.0));
        assert_eq!(b.get(0, 2), Some(1.0));
        // -1.5 * 1 + 0.5 = -1.0 → floor(-1.0) = -1.0 → result -1.0
        assert_eq!(b.get(0, 3), Some(-1.0));

        let batch = mk_batch(ts, 4, values, validity);
        let child = MockChild::new(schema, vec![batch]);
        let mut op = InstantFnOp::new(
            child,
            InstantFnKind::Round { to_nearest: 0.5 },
            MemoryReservation::new(1 << 20),
        );
        let b = &drive(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];
        // round(1.5, 0.5) = 1.5; round(2.5, 0.5) = 2.5; round(1.25, 0.5) = 1.5
        assert!(approx_eq(b.get(0, 0).unwrap(), 1.5, 1e-12));
        assert!(approx_eq(b.get(0, 1).unwrap(), 2.5, 1e-12));
        assert!(approx_eq(b.get(0, 2).unwrap(), 1.5, 1e-12));
    }

    #[test]
    fn should_apply_sgn() {
        // given: -3, 0, 4, NaN
        let ts = vec![1_000];
        let values = vec![-3.0, 0.0, 4.0, f64::NAN];
        let validity = vec![true; 4];
        let batch = mk_batch(ts.clone(), 4, values, validity);
        let schema = mk_operator_schema(&ts, 1_000, 4);
        let child = MockChild::new(schema, vec![batch]);

        // when
        let mut op = InstantFnOp::new(child, InstantFnKind::Sgn, MemoryReservation::new(1 << 20));
        let b = &drive(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()[0];

        // then
        assert_eq!(b.get(0, 0), Some(-1.0));
        assert_eq!(b.get(0, 1), Some(0.0));
        assert_eq!(b.get(0, 2), Some(1.0));
        assert!(b.get(0, 3).unwrap().is_nan());
    }

    #[test]
    fn should_propagate_error_from_upstream() {
        // given: child yields an error. Op passes it through and stops.
        let ts = vec![1_000];
        let schema = mk_operator_schema(&ts, 1_000, 1);
        let child = MockChild::with_queue(
            schema,
            vec![Err(QueryError::MemoryLimit {
                requested: 10,
                cap: 0,
                already_reserved: 0,
            })],
        );

        // when
        let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
        let results = drive(&mut op);

        // then
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(QueryError::MemoryLimit { .. })));
    }

    // ========================================================================
    // tile-boundary stress (RFC 0007 6.3.9)
    // ========================================================================

    /// Build one tile batch with an explicit `series_range` (not `0..N`).
    fn mk_tile_batch(
        step_timestamps: Vec<i64>,
        total_series_count: usize,
        series_range: std::ops::Range<usize>,
        values: Vec<f64>,
        validity: Vec<bool>,
    ) -> StepBatch {
        let step_count = step_timestamps.len();
        let tile_series_count = series_range.len();
        assert_eq!(values.len(), step_count * tile_series_count);
        assert_eq!(validity.len(), values.len());
        let schema = mk_schema(total_series_count);
        let mut bits = BitSet::with_len(validity.len());
        for (i, &b) in validity.iter().enumerate() {
            if b {
                bits.set(i);
            }
        }
        StepBatch::new(
            Arc::from(step_timestamps),
            0..step_count,
            SchemaRef::Static(schema),
            series_range,
            values,
            bits,
        )
    }

    #[test]
    fn should_apply_instant_fn_across_multi_tile_inputs_over_512_series() {
        // given: 1024 series split into two series-tile batches covering the
        // same step range (mirroring `VectorSelectorOp`'s `series_chunk=512`
        // emission for rosters >512 series).
        const SERIES: usize = 1024;
        const TILE: usize = 512;
        let ts = vec![10_i64, 20_i64];
        let schema = mk_operator_schema(&ts, 10, SERIES);

        let vals_a: Vec<f64> = (0..(TILE * 2)).map(|i| -(i as f64)).collect();
        let vals_b: Vec<f64> = (0..(TILE * 2)).map(|i| -((TILE + i / 2) as f64)).collect();
        let batch_a = mk_tile_batch(ts.clone(), SERIES, 0..TILE, vals_a, vec![true; TILE * 2]);
        let batch_b = mk_tile_batch(
            ts.clone(),
            SERIES,
            TILE..SERIES,
            vals_b,
            vec![true; TILE * 2],
        );
        let child = MockChild::new(schema, vec![batch_a, batch_b]);

        // when
        let mut op = InstantFnOp::new(child, InstantFnKind::Abs, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: each output batch preserves its child's series_range and
        // every cell is the abs of the input — invariant under tiling.
        assert_eq!(outs.len(), 2, "InstantFn emits one output per input batch");
        assert_eq!(outs[0].series_range, 0..TILE);
        assert_eq!(outs[1].series_range, TILE..SERIES);
        // Spot-check a handful of cells across both tiles.
        assert_eq!(outs[0].get(0, 0), Some(0.0));
        assert_eq!(outs[0].get(0, 1), Some(1.0));
        assert_eq!(outs[0].get(1, TILE - 1), Some((TILE * 2 - 1) as f64));
        // tile B: original vals_b[i] = -(TILE + i/2), so abs = TILE + i/2.
        assert_eq!(outs[1].get(0, 0), Some(TILE as f64));
        assert_eq!(
            outs[1].get(1, TILE - 1),
            Some((TILE + (TILE * 2 - 1) / 2) as f64)
        );
    }

    // ========================================================================
    // end-to-end pipeline: VectorSelectorOp -> InstantFnOp(Abs)
    // ========================================================================

    #[test]
    fn should_apply_abs_over_vector_selector_pipeline() {
        use crate::promql::operators::vector_selector::{BatchShape, VectorSelectorOp};
        use crate::promql::source::{
            ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock, SamplesRequest,
            SeriesSource, TimeRange,
        };
        use futures::Stream;
        use futures::stream::{self};
        use promql_parser::parser::VectorSelector;
        use std::future::ready;

        /// Sync-on-poll mock — single-batch, honours `(start, end]` via
        /// TimeRange's `[start, end)` convention. Samples are in
        /// ascending timestamp order.
        struct MockSource {
            data: Vec<(Vec<i64>, Vec<f64>)>,
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
                        if *t >= request.time_range.start_ms
                            && *t < request.time_range.end_ms_exclusive
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

        // given: two series with negative values; selector walks them at
        // ts=10,20 with lookback=5.
        let source = Arc::new(MockSource {
            data: vec![
                (vec![10, 20], vec![-1.0, -2.0]),
                (vec![10, 20], vec![3.0, -4.0]),
            ],
        });
        let schema = mk_schema(2);
        let name: Arc<str> = Arc::from("m");
        let request_series: Arc<[Arc<[ResolvedSeriesRef]>]> = Arc::from(vec![
            Arc::from(vec![ResolvedSeriesRef::new(1, 0, name.clone())]),
            Arc::from(vec![ResolvedSeriesRef::new(1, 1, name.clone())]),
        ]);
        let grid = StepGrid {
            start_ms: 10,
            end_ms: 20,
            step_ms: 10,
            step_count: 2,
        };
        let reservation = MemoryReservation::new(1 << 20);
        let selector = VectorSelectorOp::new(
            source,
            schema,
            request_series,
            grid,
            None,
            None,
            5,
            reservation.clone(),
            BatchShape::new(2, 2),
        );

        // when: wire through InstantFn(Abs)
        let mut op = InstantFnOp::new(selector, InstantFnKind::Abs, reservation);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut batches = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(b))) => batches.push(b),
                Poll::Ready(Some(Err(err))) => panic!("unexpected error: {err:?}"),
                Poll::Pending => panic!("unexpected pending"),
            }
        }

        // then: all four cells are the abs() of the input values.
        assert_eq!(batches.len(), 1);
        let b = &batches[0];
        assert_eq!(b.get(0, 0), Some(1.0)); // abs(-1)
        assert_eq!(b.get(0, 1), Some(3.0)); // abs( 3)
        assert_eq!(b.get(1, 0), Some(2.0)); // abs(-2)
        assert_eq!(b.get(1, 1), Some(4.0)); // abs(-4)
    }
}

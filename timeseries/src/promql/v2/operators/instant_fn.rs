//! `InstantFn` operator — unit 3b.2.
//!
//! Pointwise scalar function over an upstream [`Operator`] producing
//! [`StepBatch`]es. Applies one PromQL scalar function cell-by-cell,
//! preserving the child's series schema and step grid (RFC 0007
//! §"Operator Taxonomy": "Pointwise scalar functions").
//!
//! Rather than one operator per PromQL function, the reducer is selected
//! by an [`InstantFnKind`] discriminant — the same "dispatch-as-data"
//! pattern used by [`RollupKind`](super::rollup::RollupKind). Each variant
//! has a tiny, no-alloc [`InstantFnKind::compute`] body; dispatch is a
//! single `match` per cell, no trait object, no vtable.
//!
//! # Supported kinds (MVP)
//!
//! | variant | PromQL function | notes |
//! |---|---|---|
//! | `Abs` / `Ceil` / `Floor` / `Exp` / `Ln` / `Log2` / `Log10` / `Sqrt` | `abs/ceil/floor/exp/ln/log2/log10/sqrt` | unary math |
//! | `Sin`/`Cos`/`Tan`/`Asin`/`Acos`/`Atan` | trig | |
//! | `Sinh`/`Cosh`/`Tanh`/`Asinh`/`Acosh`/`Atanh` | hyperbolic | |
//! | `Deg` / `Rad` | radians ↔ degrees | `f64::to_degrees` / `to_radians` |
//! | `Sgn` | `sgn(v)` | -1 / 0 / +1, NaN passthrough (Prometheus semantics) |
//! | `Round { to_nearest }` | `round(v[, to_nearest])` | plan-time `to_nearest`; default 1.0 |
//! | `Clamp { min, max }` | `clamp(v, min, max)` | plan-time scalar bounds |
//! | `ClampMin { min }` | `clamp_min(v, min)` | plan-time scalar bound |
//! | `ClampMax { max }` | `clamp_max(v, max)` | plan-time scalar bound |
//! | `Timestamp` | `timestamp(v)` | see §"Timestamp semantics" below |
//!
//! **Parity with the existing engine** (`timeseries/src/promql/functions.rs`):
//! every pointwise float→float function the legacy engine ships is covered
//! here: `abs`, `acos`, `acosh`, `asin`, `asinh`, `atan`, `atanh`, `ceil`,
//! `cos`, `cosh`, `deg`, `exp`, `floor`, `ln`, `log10`, `log2`, `rad`,
//! `round`, `sin`, `sinh`, `sqrt`, `tan`, `tanh`, `clamp`, `clamp_min`,
//! `clamp_max`, `timestamp`. Additions: `sgn` (not in the legacy engine;
//! Prometheus ships it). Deliberate exclusions per plan §3.4 scope:
//! `absent`/`absent_over_time` (schema-derivation; separate unit),
//! `histogram_quantile` (out of scope per RFC non-goals: native
//! histograms), `scalar`/`vector` (planner-level type coercions),
//! `pi`/`time` (nullary planner constants), `day_of_*`/`hour`/`minute`/
//! `month`/`year`/`days_in_month`/`day_of_week`/`day_of_year`
//! (date/time extractions on an input vector — pointwise but each needs
//! `chrono`-style calendar math; deferred to a later unit to keep the 3b.2
//! surface focused on float-pointwise cases). `label_replace` / `label_join`
//! mutate labels and are handled by a separate operator (not pointwise in
//! the "cell → cell" sense).
//!
//! # Timestamp semantics
//!
//! `timestamp()` in this operator returns **the output step timestamp in
//! seconds** (`step_timestamps[step_off] as f64 / 1000.0`). The legacy
//! engine (`timeseries/src/promql/functions.rs:870-881`,
//! `TimestampFunction`) returns the *source sample's* timestamp in seconds
//! — it reads `sample.timestamp_ms`, the original scrape time of the
//! lookback-selected sample. In v2's columnar model, `StepBatch` does not
//! carry per-cell source timestamps (RFC §"Core Data Model": `values:
//! Vec<f64>` only); the step timestamp is the closest available proxy and
//! matches the task spec's "`t/1000` (seconds)" definition. This is a
//! deliberate divergence, recorded in §5 Decisions Log. For instant
//! queries (step_count == 1, step == eval_timestamp) the two definitions
//! coincide; for range queries with lookback, v2's `timestamp()` reports
//! the evaluation step rather than the underlying sample's scrape time.
//! Phase 6 goldens involving `timestamp()` across range queries may flag
//! this — that would be an operator re-open, not a fixture change.
//!
//! # Validity policy
//!
//! - If the input cell's validity bit is clear, the output cell's
//!   validity bit is also clear (value is left as the NaN-filled default;
//!   callers must consult validity before reading).
//! - If the input cell is valid, the output cell is valid — even when the
//!   computed value is `NaN` or `±inf` (e.g. `ln(-1)` → `NaN`, `ln(0)` →
//!   `-inf`). This matches the legacy `UnaryFunction` path
//!   (`timeseries/src/promql/functions.rs:203-211`) which writes
//!   `sample.value = (self.op)(sample.value)` and preserves the sample
//!   regardless of the result. Downstream operators (e.g. `Binary`) read
//!   the NaN like any other value.
//! - The operator never flips validity from clear to set.
//!
//! Since validity is never modified relative to the input, the output
//! validity is a pointer-clone of the input's — no rebuild needed.
//!
//! # Memory accounting
//!
//! Output `StepBatch` `values` is a fresh `Vec<f64>`; validity is
//! pointer-cloned from the input (so only the values column is charged
//! to this operator's reservation). The [`OutValues`] RAII guard routes
//! allocation through [`MemoryReservation::try_grow`] and releases on
//! `Drop`. The input batch's reservation is the child operator's
//! concern, matching the 3a.1 / 3a.2 / 3b.1 convention: each operator
//! reserves only the bytes it allocates itself.
//!
//! Output `step_timestamps` is an `Arc` pointer-clone of the input's;
//! `SchemaRef` / `SeriesSchema` flows through unchanged.

use std::task::{Context, Poll};

use super::super::batch::StepBatch;
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};

// ---------------------------------------------------------------------------
// InstantFnKind — one operator, function selection as data
// ---------------------------------------------------------------------------

/// Discriminant selecting the per-cell pointwise reducer.
///
/// Designed as a small `Copy` enum (no allocations) so the reducer
/// dispatch is a single `match` per cell. Variants that take plan-time
/// scalar arguments (`Round`, `Clamp*`) carry them inline — the arguments
/// are constant across every step and series per PromQL semantics.
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

    /// `timestamp(v)` — see the module-level "Timestamp semantics" note.
    /// Returns the output step timestamp in seconds regardless of the
    /// input value (the input's validity bit still gates the output
    /// validity).
    Timestamp,
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
        }
    }
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
/// construction; releases on `Drop`. `finish()` transfers ownership of
/// the `Vec` to the caller and releases the reservation immediately
/// (matches 3a.1 / 3b.1 convention: operator-owned bytes leave the
/// operator at batch-emission time; downstream re-reserves if it needs
/// to retain the batch).
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

/// Pointwise scalar function operator. Applies [`InstantFnKind`]'s reducer
/// to each cell in every [`StepBatch`] pulled from `child`.
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
        let series_count = batch.series_count();
        let step_ts = batch.step_timestamps_slice();

        // Iterate cells in row-major (step, series) order — matches
        // `StepBatch`'s layout. Only touch valid cells; invalid cells
        // keep the NaN fill and the pointer-cloned validity bit clear.
        for (step_off, &step_ms) in step_ts.iter().enumerate().take(batch.step_count()) {
            for series_off in 0..series_count {
                let idx = step_off * series_count + series_off;
                if batch.validity.get(idx) {
                    out.values[idx] = self.kind.compute(batch.values[idx], step_ms);
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
    use crate::promql::v2::batch::{BitSet, SchemaRef, SeriesSchema};
    use crate::promql::v2::operator::StepGrid;
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
    fn should_apply_timestamp_emitting_step_timestamps() {
        // given: two steps at t=1_500 ms and 3_000 ms; timestamp() should
        // emit step_ms / 1000 regardless of the input value (the value
        // just needs to have validity=1).
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

        // then: v2 step-timestamp semantics — see module-level note for
        // the divergence from the legacy engine's source-sample-timestamp
        // behaviour.
        let b = &batches[0];
        assert!(approx_eq(b.get(0, 0).unwrap(), 1.5, 1e-9));
        assert!(approx_eq(b.get(1, 0).unwrap(), 3.0, 1e-9));
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
    // end-to-end pipeline: VectorSelectorOp -> InstantFnOp(Abs)
    // ========================================================================

    #[test]
    fn should_apply_abs_over_vector_selector_pipeline() {
        use crate::promql::v2::operators::vector_selector::{BatchShape, VectorSelectorOp};
        use crate::promql::v2::source::{
            CardinalityEstimate, ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock,
            SampleHint, SeriesSource, TimeRange,
        };
        use futures::Stream;
        use futures::stream::{self};
        use promql_parser::parser::VectorSelector;
        use std::future::{Future, ready};

        /// Sync-on-poll mock copied from the 3a.1 tests — single-batch,
        /// honours `(start, end]` via TimeRange's `[start, end)`
        /// convention. Samples are in ascending timestamp order.
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

        // given: two series with negative values; selector walks them at
        // ts=10,20 with lookback=5.
        let source = Arc::new(MockSource {
            data: vec![
                (vec![10, 20], vec![-1.0, -2.0]),
                (vec![10, 20], vec![3.0, -4.0]),
            ],
        });
        let schema = mk_schema(2);
        let hint_series: Arc<[ResolvedSeriesRef]> = Arc::from(vec![
            ResolvedSeriesRef::new(1, 0),
            ResolvedSeriesRef::new(1, 1),
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
            hint_series,
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

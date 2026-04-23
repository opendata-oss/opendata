//! `SubqueryOp` implements PromQL's subquery syntax (`expr[range:step]`)
//! — the "compute an inner range vector at a finer step, then feed it to
//! a range function" construct.
//!
//! A subquery is logically an inner range-vector producer and feeds
//! into a downstream [`RollupOp`](super::rollup::RollupOp), so `SubqueryOp` implements the same
//! [`WindowStream`] contract `MatrixSelectorOp` does — it emits
//! [`MatrixWindowBatch`]es, one per outer step. `Operator::next` is a
//! degenerate "immediate EOS", same arrangement as [`MatrixSelectorOp`](super::matrix_selector::MatrixSelectorOp).
//!
//! The hard part is that the inner child has to be re-evaluated for each
//! outer step (the inner window slides). The operator owns a
//! [`ChildFactory`] closure the planner supplies; it calls the factory
//! once per outer step to build a freshly-planned child covering
//! `(outer_t - range, outer_t]` at the inner step, drains the child, and
//! packs the resulting instant-vector samples into a
//! [`MatrixWindowBatch`].
//!
//! The subquery shares its parent's [`MemoryReservation`]; nested
//! per-subquery reservations are deferred. A [`SchemaRef::Deferred`] child
//! schema is a planner bug — `count_values` inside a subquery is out of
//! scope.

use std::task::{Context, Poll};

use super::super::batch::{SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::source::TimeRange;
use super::matrix_selector::{CellIndex, MatrixWindowBatch};
use super::rollup::WindowStream;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Factory shape
// ---------------------------------------------------------------------------

/// Factory the planner hands `SubqueryOp` at construction time.
///
/// Called once per outer step to build a freshly-planned child operator
/// for the inner `(TimeRange, step_ms)` window. The resulting child must
/// publish a schema whose series roster matches the plan-time series
/// (`SubqueryOp`'s output schema was stamped from the same roster); its
/// step grid is what the subquery re-grids onto.
pub type ChildFactory =
    Box<dyn FnMut(TimeRange, i64) -> Result<Box<dyn Operator + Send>, QueryError> + Send>;

// ---------------------------------------------------------------------------
// Byte-estimate helpers (mirror 3a.2's window_bytes shape)
// ---------------------------------------------------------------------------

#[inline]
fn window_bytes(cells: usize, samples: usize) -> usize {
    let cell_bytes = cells.saturating_mul(std::mem::size_of::<CellIndex>());
    let ts_bytes = samples.saturating_mul(std::mem::size_of::<i64>());
    let val_bytes = samples.saturating_mul(std::mem::size_of::<f64>());
    cell_bytes
        .saturating_add(ts_bytes)
        .saturating_add(val_bytes)
}

// ---------------------------------------------------------------------------
// WindowBuffers — RAII-guarded outer-step batch allocation
// ---------------------------------------------------------------------------

/// RAII wrapper around the working `timestamps` / `values` / `cells`
/// buffers for a single emitted [`MatrixWindowBatch`].
///
/// Reserves the cell-index array up front, grows the flat sample buffer
/// as samples arrive, releases the entire reservation on [`Drop`], and
/// transfers ownership of the inner vectors via [`Self::finish`]
/// (downstream re-reserves if it retains the batch).
struct WindowBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    cells: Vec<CellIndex>,
}

impl WindowBuffers {
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

    fn grow_samples(&mut self, extra: usize) -> Result<(), QueryError> {
        if extra == 0 {
            return Ok(());
        }
        let bytes = extra.saturating_mul(std::mem::size_of::<i64>() + std::mem::size_of::<f64>());
        self.reservation.try_grow(bytes)?;
        self.bytes = self.bytes.saturating_add(bytes);
        Ok(())
    }

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
// Subquery operator
// ---------------------------------------------------------------------------

/// Implements PromQL's subquery syntax `expr[range:step]`.
///
/// For each outer step, builds a fresh child operator via the
/// planner-supplied [`ChildFactory`] covering the inner window
/// `(outer_t - range, outer_t]` at the inner step, drains the child's
/// [`StepBatch`] stream, and packs the resulting instant-vector samples
/// into a [`MatrixWindowBatch`] for a downstream [`RollupOp`] to reduce.
///
/// See module docs for the architectural invariants, factory shape,
/// shared-reservation choice, and one-batch-per-outer-step emission
/// policy.
///
/// [`RollupOp`]: super::rollup::RollupOp
pub struct SubqueryOp {
    // Plan-time inputs ------------------------------------------------------
    factory: ChildFactory,
    schema: OperatorSchema,
    outer_step_timestamps: Arc<[i64]>,
    /// Per-outer-step effective evaluation timestamp after folding the
    /// subquery's `@` and `offset` modifiers. `effective_times[k]` is what
    /// the child sub-tree should be evaluated at for outer step `k`; the
    /// inner range window is `(effective_times[k] - range_ms,
    /// effective_times[k]]`.
    effective_times: Arc<[i64]>,
    /// `true` when the subquery has a non-trivial `@` or `offset`
    /// modifier and `effective_times` differs from `outer_step_timestamps`.
    /// Propagated to the emitted [`MatrixWindowBatch`] so the enclosing
    /// `RollupOp` computes window math over the actual sample window.
    has_effective_shift: bool,
    series: Arc<SeriesSchema>,
    range_ms: i64,
    inner_step_ms: i64,
    reservation: MemoryReservation,

    // Runtime state ---------------------------------------------------------
    next_outer_step: usize,
    errored: bool,
}

impl SubqueryOp {
    /// Build a subquery re-grid operator.
    ///
    /// * `factory` — planner-supplied factory that materialises a child
    ///   sub-tree for a given `(TimeRange, step_ms)` inner window. Called
    ///   once per outer step.
    /// * `series` — output series roster. Identical to the child's
    ///   published series roster (the sub-tree's output is data-dependent
    ///   but its labelset is plan-time-stable); stamped here so downstream
    ///   operators see it before any poll.
    /// * `outer_grid` — the **outer** step grid the subquery emits on.
    ///   Each outer step triggers one factory call and one
    ///   `MatrixWindowBatch` emission.
    /// * `range_ms` — bracketed window (e.g. `[5m:…]` → `300_000`). Must
    ///   be `> 0`.
    /// * `inner_step_ms` — subquery resolution (`[…:1m]` → `60_000`).
    ///   Must be `> 0`; the planner resolves the PromQL default when
    ///   omitted.
    /// * `reservation` — per-query reservation; **shared** with the
    ///   parent (no nested scope — nested-reservation scoping is a post-MVP
    ///   concern).
    /// * `effective_times` — per-outer-step effective evaluation times
    ///   with the subquery's `@` / `offset` already folded in. Must be the
    ///   same length as `outer_grid.step_count`. Use [`Self::new`] when
    ///   no `@` / `offset` modifiers are present — it defaults
    ///   `effective_times` to the outer step grid.
    pub fn with_effective_times(
        factory: ChildFactory,
        series: Arc<SeriesSchema>,
        outer_grid: StepGrid,
        range_ms: i64,
        inner_step_ms: i64,
        effective_times: Arc<[i64]>,
        reservation: MemoryReservation,
    ) -> Self {
        assert!(range_ms > 0, "subquery range must be > 0 ms");
        assert!(inner_step_ms > 0, "subquery inner step must be > 0 ms");
        assert_eq!(
            effective_times.len(),
            outer_grid.step_count,
            "effective_times must be length-aligned with outer grid",
        );
        let outer_step_timestamps: Arc<[i64]> = Arc::from(
            (0..outer_grid.step_count)
                .map(|k| outer_grid.start_ms + (k as i64) * outer_grid.step_ms)
                .collect::<Vec<_>>(),
        );
        let has_effective_shift = outer_step_timestamps
            .iter()
            .zip(effective_times.iter())
            .any(|(o, e)| o != e);
        let schema = OperatorSchema::new(SchemaRef::Static(series.clone()), outer_grid);
        Self {
            factory,
            schema,
            outer_step_timestamps,
            effective_times,
            has_effective_shift,
            series,
            range_ms,
            inner_step_ms,
            reservation,
            next_outer_step: 0,
            errored: false,
        }
    }

    /// Convenience constructor for subqueries without `@` / `offset`
    /// modifiers — the effective evaluation times equal the outer step grid.
    pub fn new(
        factory: ChildFactory,
        series: Arc<SeriesSchema>,
        outer_grid: StepGrid,
        range_ms: i64,
        inner_step_ms: i64,
        reservation: MemoryReservation,
    ) -> Self {
        let outer_ts: Arc<[i64]> = Arc::from(
            (0..outer_grid.step_count)
                .map(|k| outer_grid.start_ms + (k as i64) * outer_grid.step_ms)
                .collect::<Vec<_>>(),
        );
        Self::with_effective_times(
            factory,
            series,
            outer_grid,
            range_ms,
            inner_step_ms,
            outer_ts,
            reservation,
        )
    }

    /// Drive the child operator to completion, packing its emitted
    /// instant-vector samples into a `MatrixWindowBatch` covering the
    /// single outer step at `outer_step_idx`.
    fn build_outer_step_batch(
        &mut self,
        outer_step_idx: usize,
        cx: &mut Context<'_>,
    ) -> Poll<Result<MatrixWindowBatch, QueryError>> {
        let effective_t = self.effective_times[outer_step_idx];
        // Window `(effective - range, effective]`, encoded as the
        // inclusive-exclusive `TimeRange` `[effective - range + 1,
        // effective + 1)`. `effective_t` already folds in the subquery's
        // `@` / `offset` modifiers (see `SubqueryOp::with_effective_times`).
        let inner_window = TimeRange::new(
            effective_t.saturating_sub(self.range_ms).saturating_add(1),
            effective_t.saturating_add(1),
        );

        // Build the child. Factory failure is terminal for the subquery.
        let mut child = match (self.factory)(inner_window, self.inner_step_ms) {
            Ok(c) => c,
            Err(err) => return Poll::Ready(Err(err)),
        };

        // Validate: child series roster must match ours. A `Deferred`
        // child means `count_values` inside a subquery, which is out of
        // scope for v1 (RFC §"Core Data Model").
        debug_assert!(
            !child.schema().series.is_deferred(),
            "subquery child must publish a static schema (count_values in subquery is v1 out-of-scope)"
        );
        let series_count = self.series.len();

        // Drain the child. Each `StepBatch` is an instant-vector of the
        // inner expression's value at some inner step; the child may
        // emit multiple batches covering the inner grid in chunks.
        let outer_cells = series_count; // one row of cells for the single outer step
        let mut buffers = match WindowBuffers::allocate(&self.reservation, outer_cells) {
            Ok(b) => b,
            Err(err) => return Poll::Ready(Err(err)),
        };
        // Scratch per-series sample buffers — we collect into these so we
        // can pack per-cell slices contiguously into the flat output
        // arrays (consumer expects per-cell row-major layout matching
        // 3a.2's emission).
        let mut per_series_ts: Vec<Vec<i64>> = (0..series_count).map(|_| Vec::new()).collect();
        let mut per_series_vs: Vec<Vec<f64>> = (0..series_count).map(|_| Vec::new()).collect();

        loop {
            match child.next(cx) {
                Poll::Pending => {
                    // `SubqueryOp` does not persist partial child state
                    // across polls — the whole child sweep happens within
                    // a single `windows()` invocation. If the underlying
                    // storage is async-ready under back pressure, the
                    // *caller*'s task will re-poll us and we rebuild from
                    // scratch. Revisit once a `Concurrent` wrapper
                    // exercises the real async path.
                    return Poll::Pending;
                }
                Poll::Ready(None) => break,
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(batch))) => {
                    if let Err(err) = Self::absorb_batch(
                        &batch,
                        inner_window,
                        &mut per_series_ts,
                        &mut per_series_vs,
                    ) {
                        return Poll::Ready(Err(err));
                    }
                }
            }
        }

        // Pack per-series scratch into the flat output buffers. Each
        // cell's samples land contiguously; `CellIndex` records its
        // `[offset, offset+len)` slice.
        for series_off in 0..series_count {
            let ts = &per_series_ts[series_off];
            let vs = &per_series_vs[series_off];
            debug_assert_eq!(ts.len(), vs.len(), "scratch columns must be length-aligned");
            if let Err(err) = buffers.grow_samples(ts.len()) {
                return Poll::Ready(Err(err));
            }
            let cell_offset = buffers.timestamps.len() as u32;
            let cell_len = ts.len() as u32;
            buffers.timestamps.extend_from_slice(ts);
            buffers.values.extend_from_slice(vs);
            buffers.cells[series_off] = CellIndex {
                offset: cell_offset,
                len: cell_len,
            };
        }

        let (timestamps, values, cells) = buffers.finish();

        let effective_times = if self.has_effective_shift {
            Some(self.effective_times.clone())
        } else {
            None
        };
        Poll::Ready(Ok(MatrixWindowBatch {
            step_timestamps: self.outer_step_timestamps.clone(),
            step_range: outer_step_idx..(outer_step_idx + 1),
            series: SchemaRef::Static(self.series.clone()),
            series_range: 0..series_count,
            timestamps,
            values,
            cells,
            effective_times,
        }))
    }

    /// Absorb one inner-step [`StepBatch`] into the per-series scratch
    /// columns. Drops invalid cells (validity=0) and `STALE_NAN` values
    /// up front — consumers (`Rollup`) expect pre-filtered windows.
    fn absorb_batch(
        batch: &StepBatch,
        inner_window: TimeRange,
        per_series_ts: &mut [Vec<i64>],
        per_series_vs: &mut [Vec<f64>],
    ) -> Result<(), QueryError> {
        let step_ts = batch.step_timestamps_slice();
        let series_count = batch.series_count();
        // Each batch covers `batch.series_range`; those indices map into
        // the full plan-time series roster (which matches ours).
        let series_base = batch.series_range.start;
        for (step_off, &t) in step_ts.iter().enumerate() {
            // Drop inner-step samples that fall outside the outer
            // window. The child's grid *should* be aligned to the
            // window we requested, but nothing in the `Operator`
            // contract guarantees the child respects the range exactly
            // — e.g. a matrix-selector-flavoured child may emit a
            // lookback sample slightly outside. Defensive filter.
            if t < inner_window.start_ms || t >= inner_window.end_ms_exclusive {
                continue;
            }
            for series_off in 0..series_count {
                let Some(v) = batch.get(step_off, series_off) else {
                    continue;
                };
                if crate::model::is_stale_nan(v) {
                    continue;
                }
                let global_series = series_base + series_off;
                debug_assert!(
                    global_series < per_series_ts.len(),
                    "child emitted series index {global_series} outside subquery roster len {}",
                    per_series_ts.len(),
                );
                per_series_ts[global_series].push(t);
                per_series_vs[global_series].push(v);
            }
        }
        Ok(())
    }

    /// Secondary API — the useful one.
    ///
    /// Polls for the next outer-step [`MatrixWindowBatch`]. v1 emits one
    /// batch per outer step.
    pub fn windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
        if self.errored {
            return Poll::Ready(None);
        }
        if self.next_outer_step >= self.schema.step_grid.step_count {
            return Poll::Ready(None);
        }
        // Empty series roster short-circuit.
        if self.series.is_empty() {
            // Still emit one empty batch per outer step so consumers can
            // observe the outer grid; follows the `MatrixSelectorOp`
            // convention (empty windows are represented as empty
            // `CellIndex`es, not absent batches).
            let idx = self.next_outer_step;
            self.next_outer_step += 1;
            return Poll::Ready(Some(Ok(MatrixWindowBatch {
                step_timestamps: self.outer_step_timestamps.clone(),
                step_range: idx..(idx + 1),
                series: SchemaRef::Static(self.series.clone()),
                series_range: 0..0,
                timestamps: Vec::new(),
                values: Vec::new(),
                cells: Vec::new(),
                effective_times: None,
            })));
        }
        let idx = self.next_outer_step;
        match self.build_outer_step_batch(idx, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(batch)) => {
                self.next_outer_step += 1;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Err(err)) => {
                self.errored = true;
                Poll::Ready(Some(Err(err)))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Operator impl — degenerate, mirrors MatrixSelectorOp
// ---------------------------------------------------------------------------

impl Operator for SubqueryOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    /// Degenerate: subquery output is a matrix, which does not fit
    /// `StepBatch`'s single-float-per-cell shape. Consumers drive the
    /// operator via [`SubqueryOp::windows`] / [`WindowStream::poll_windows`].
    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        Poll::Ready(None)
    }
}

// ---------------------------------------------------------------------------
// WindowStream impl — drop-in for `RollupOp<SubqueryOp>`
// ---------------------------------------------------------------------------

impl WindowStream for SubqueryOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn poll_windows(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<MatrixWindowBatch, QueryError>>> {
        self.windows(cx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels, STALE_NAN};
    use crate::promql::batch::BitSet;
    use crate::promql::operators::rollup::{RollupKind, RollupOp};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    fn mk_labels(n: usize) -> Arc<SeriesSchema> {
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

    /// Minimal mock "inner plan" operator: returns a pre-scripted queue of
    /// `StepBatch`es (or errors), then end-of-stream.
    struct ScriptedChild {
        schema: OperatorSchema,
        queue: Vec<Result<StepBatch, QueryError>>,
    }

    impl ScriptedChild {
        fn new(schema: OperatorSchema, batches: Vec<StepBatch>) -> Self {
            Self {
                schema,
                queue: batches.into_iter().map(Ok).collect(),
            }
        }

        fn with_error(schema: OperatorSchema, err: QueryError) -> Self {
            Self {
                schema,
                queue: vec![Err(err)],
            }
        }
    }

    impl Operator for ScriptedChild {
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

    /// Build a single-step batch covering one inner step with `series_count`
    /// series. `values[i]` is the value for series i; `None` → validity 0.
    fn mk_batch(
        step_timestamps: Arc<[i64]>,
        step_idx: usize,
        series: Arc<SeriesSchema>,
        values: &[Option<f64>],
    ) -> StepBatch {
        let n = values.len();
        let mut vs = Vec::with_capacity(n);
        let mut validity = BitSet::with_len(n);
        for (i, v) in values.iter().enumerate() {
            match v {
                Some(x) => {
                    vs.push(*x);
                    validity.set(i);
                }
                None => vs.push(0.0),
            }
        }
        StepBatch::new(
            step_timestamps,
            step_idx..(step_idx + 1),
            SchemaRef::Static(series),
            0..n,
            vs,
            validity,
        )
    }

    /// Build an inner-grid schema + timestamps for `[outer_t - range, outer_t]`
    /// at `inner_step`.
    fn mk_inner_grid(outer_t: i64, range_ms: i64, inner_step_ms: i64) -> (Arc<[i64]>, StepGrid) {
        // Inner grid: first step strictly > outer_t - range, spaced by
        // inner_step, up to and including outer_t. Mirrors the production
        // `[range:step]` semantics.
        let mut ts = Vec::new();
        let mut t = outer_t
            .saturating_sub(range_ms)
            .saturating_add(inner_step_ms);
        while t <= outer_t {
            ts.push(t);
            t += inner_step_ms;
        }
        let start_ms = ts.first().copied().unwrap_or(outer_t);
        let end_ms = ts.last().copied().unwrap_or(outer_t);
        let step_count = ts.len();
        (
            Arc::from(ts),
            StepGrid {
                start_ms,
                end_ms,
                step_ms: inner_step_ms,
                step_count,
            },
        )
    }

    // -----------------------------------------------------------------------
    // required tests
    // -----------------------------------------------------------------------

    #[test]
    fn should_regrid_child_onto_inner_step() {
        // given: outer step at t=100, range=30ms, inner_step=10ms, outer
        // step=60ms (single outer step here). Window: (70, 100] ⇒ inner
        // ts should be 80, 90, 100.
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 60,
            step_count: 1,
        };
        let series = mk_labels(1);
        let series_clone = series.clone();

        let factory: ChildFactory = Box::new(move |tr: TimeRange, step_ms: i64| {
            // assert inner contract
            assert_eq!(tr.start_ms, 71); // 100 - 30 + 1
            assert_eq!(tr.end_ms_exclusive, 101); // 100 + 1
            assert_eq!(step_ms, 10);
            let (ts_arc, inner_grid) = mk_inner_grid(100, 30, 10);
            let mut batches = Vec::new();
            for (i, &t) in ts_arc.iter().enumerate() {
                batches.push(mk_batch(
                    ts_arc.clone(),
                    i,
                    series_clone.clone(),
                    &[Some(t as f64)],
                ));
            }
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });

        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        // when
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let batch = match op.windows(&mut cx) {
            Poll::Ready(Some(Ok(b))) => b,
            other => panic!("unexpected poll: {other:?}"),
        };

        // then: single cell for the outer step contains ts/vs 80, 90, 100.
        assert_eq!(batch.step_count(), 1);
        assert_eq!(batch.series_count(), 1);
        let (ts, vs) = batch.cell_samples(0, 0);
        assert_eq!(ts, &[80, 90, 100]);
        assert_eq!(vs, &[80.0, 90.0, 100.0]);
    }

    #[test]
    fn should_emit_one_matrix_batch_per_outer_step() {
        // given: outer grid with 3 steps
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 220,
            step_ms: 60,
            step_count: 3,
        };
        let series = mk_labels(1);
        let series_clone = series.clone();

        let factory: ChildFactory = Box::new(move |_tr, step_ms| {
            let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
            let batches: Vec<StepBatch> = ts_arc
                .iter()
                .enumerate()
                .map(|(i, &t)| mk_batch(ts_arc.clone(), i, series_clone.clone(), &[Some(t as f64)]))
                .collect();
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });

        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        // when
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut count = 0;
        loop {
            match op.windows(&mut cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(_))) => count += 1,
                Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
                Poll::Pending => panic!("unexpected Pending"),
            }
        }
        // then: exactly one batch per outer step (3).
        assert_eq!(count, 3);
    }

    #[test]
    fn should_invoke_factory_once_per_outer_step() {
        // given: outer grid with 4 steps; factory counter increments per call.
        let outer_grid = StepGrid {
            start_ms: 60,
            end_ms: 240,
            step_ms: 60,
            step_count: 4,
        };
        let series = mk_labels(1);
        let series_clone = series.clone();

        let counter = Arc::new(Mutex::new(0usize));
        let counter_clone = counter.clone();
        let factory: ChildFactory = Box::new(move |_tr, step_ms| {
            *counter_clone.lock().unwrap() += 1;
            let (ts_arc, inner_grid) = mk_inner_grid(60, 30, step_ms);
            let batches: Vec<StepBatch> = ts_arc
                .iter()
                .enumerate()
                .map(|(i, _)| mk_batch(ts_arc.clone(), i, series_clone.clone(), &[Some(1.0)]))
                .collect();
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });

        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        // when: drive to completion
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        while let Poll::Ready(Some(Ok(_))) = op.windows(&mut cx) {}

        // then: factory called once per outer step.
        assert_eq!(*counter.lock().unwrap(), 4);
    }

    #[test]
    fn should_pack_samples_into_matrix_window_batch_layout() {
        // given: 2 series, outer t=100, range=30, inner=10. Inner ts 80, 90, 100.
        // Series 0: [1.0, 2.0, 3.0]; Series 1: [10.0, 20.0, 30.0].
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 60,
            step_count: 1,
        };
        let series = mk_labels(2);
        let series_clone = series.clone();

        let factory: ChildFactory = Box::new(move |_tr, step_ms| {
            let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
            let batches = vec![
                mk_batch(
                    ts_arc.clone(),
                    0,
                    series_clone.clone(),
                    &[Some(1.0), Some(10.0)],
                ),
                mk_batch(
                    ts_arc.clone(),
                    1,
                    series_clone.clone(),
                    &[Some(2.0), Some(20.0)],
                ),
                mk_batch(
                    ts_arc.clone(),
                    2,
                    series_clone.clone(),
                    &[Some(3.0), Some(30.0)],
                ),
            ];
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });

        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        // when
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let batch = match op.windows(&mut cx) {
            Poll::Ready(Some(Ok(b))) => b,
            other => panic!("unexpected: {other:?}"),
        };

        // then: `cells` length = step_count * series_count = 2; per-cell
        // indexing yields the right samples.
        assert_eq!(batch.cells.len(), 2);
        let (ts0, vs0) = batch.cell_samples(0, 0);
        assert_eq!(ts0, &[80, 90, 100]);
        assert_eq!(vs0, &[1.0, 2.0, 3.0]);
        let (ts1, vs1) = batch.cell_samples(0, 1);
        assert_eq!(ts1, &[80, 90, 100]);
        assert_eq!(vs1, &[10.0, 20.0, 30.0]);
    }

    #[test]
    fn should_yield_end_of_stream_when_outer_grid_exhausted() {
        // given: 2 outer steps
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 160,
            step_ms: 60,
            step_count: 2,
        };
        let series = mk_labels(1);
        let series_clone = series.clone();
        let factory: ChildFactory = Box::new(move |_tr, step_ms| {
            let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
            let batches = vec![mk_batch(
                ts_arc.clone(),
                0,
                series_clone.clone(),
                &[Some(1.0)],
            )];
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });
        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        // when: drain exactly 2 batches, then one more poll
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(op.windows(&mut cx), Poll::Ready(Some(Ok(_)))));
        assert!(matches!(op.windows(&mut cx), Poll::Ready(Some(Ok(_)))));
        // then: end-of-stream
        assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
        // idempotent
        assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
    }

    #[test]
    fn should_preserve_series_order_from_child() {
        // given: 3 series. Inner emits values that encode the series index.
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 60,
            step_count: 1,
        };
        let series = mk_labels(3);
        let series_clone = series.clone();
        let factory: ChildFactory = Box::new(move |_tr, step_ms| {
            let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
            let batches = vec![mk_batch(
                ts_arc.clone(),
                0,
                series_clone.clone(),
                &[Some(0.0), Some(1.0), Some(2.0)],
            )];
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });
        // Inner step large enough to produce only one inner step inside
        // the window (80), so the scripted child's single batch stands alone.
        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            30,
            MemoryReservation::new(1 << 20),
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let batch = match op.windows(&mut cx) {
            Poll::Ready(Some(Ok(b))) => b,
            other => panic!("unexpected: {other:?}"),
        };

        // then: per-series cells carry the expected value in the right slot.
        for s in 0..3 {
            let (_, vs) = batch.cell_samples(0, s);
            assert_eq!(vs, &[s as f64], "series {s} value in wrong slot");
        }
    }

    #[test]
    fn should_skip_invalid_cells_in_inner_output() {
        // given: one series, 3 inner steps; the middle step's cell has
        // validity=0 and one cell carries STALE_NAN.
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 60,
            step_count: 1,
        };
        let series = mk_labels(1);
        let series_clone = series.clone();
        let stale = f64::from_bits(STALE_NAN);
        let factory: ChildFactory = Box::new(move |_tr, step_ms| {
            let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
            let batches = vec![
                mk_batch(ts_arc.clone(), 0, series_clone.clone(), &[Some(1.0)]),
                mk_batch(ts_arc.clone(), 1, series_clone.clone(), &[None]),
                mk_batch(ts_arc.clone(), 2, series_clone.clone(), &[Some(stale)]),
            ];
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });
        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let batch = match op.windows(&mut cx) {
            Poll::Ready(Some(Ok(b))) => b,
            other => panic!("unexpected: {other:?}"),
        };

        // then: only the single good sample at ts=80 made it through.
        let (ts, vs) = batch.cell_samples(0, 0);
        assert_eq!(ts, &[80]);
        assert_eq!(vs, &[1.0]);
    }

    #[test]
    fn should_propagate_error_from_child() {
        // given: child errors out on first poll.
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 60,
            step_count: 1,
        };
        let series = mk_labels(1);
        let series_clone = series.clone();
        let factory: ChildFactory = Box::new(move |_tr, step_ms| {
            let (_ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::with_error(
                schema,
                QueryError::Internal("boom".into()),
            )) as Box<dyn Operator + Send>)
        });
        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        // then: error propagated verbatim.
        match op.windows(&mut cx) {
            Poll::Ready(Some(Err(QueryError::Internal(msg)))) => assert_eq!(msg, "boom"),
            other => panic!("expected Internal error, got {other:?}"),
        }
        // Once errored, subsequent polls return end-of-stream.
        assert!(matches!(op.windows(&mut cx), Poll::Ready(None)));
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: tiny cap cannot fit even the cell-index allocation.
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 60,
            step_count: 1,
        };
        let series = mk_labels(4);
        let series_clone = series.clone();
        let factory: ChildFactory = Box::new(move |_tr, step_ms| {
            let (ts_arc, inner_grid) = mk_inner_grid(100, 30, step_ms);
            let batches = vec![mk_batch(
                ts_arc.clone(),
                0,
                series_clone.clone(),
                &[Some(1.0); 4],
            )];
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });
        let mut op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(4),
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let err = match op.windows(&mut cx) {
            Poll::Ready(Some(Err(e))) => e,
            other => panic!("expected MemoryLimit, got {other:?}"),
        };
        assert!(matches!(err, QueryError::MemoryLimit { .. }));
    }

    #[test]
    fn should_return_static_schema() {
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 60,
            step_count: 1,
        };
        let series = mk_labels(1);
        let factory: ChildFactory = Box::new(move |_tr, _step_ms| {
            // Factory is never invoked in this test (no poll).
            unreachable!("factory not invoked for schema-only test");
        });
        let op = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        // when/then
        let schema = <SubqueryOp as Operator>::schema(&op);
        assert!(!schema.series.is_deferred());
        assert!(schema.series.as_static().is_some());
    }

    #[test]
    fn should_plug_into_rollup_end_to_end() {
        // given: analog of `rate(expr[3s:1s])` — 2 outer steps, range=30ms,
        // inner step=10ms. Inner values are a perfect 10/s counter:
        // at ts t (ms) the value is t/10. Each inner window carries 3
        // samples spanning the range.
        //
        // First outer step (t=100): window (70, 100] → inner ts {80, 90, 100},
        // values {8, 9, 10}. Second outer step (t=130): window (100, 130]
        // → inner ts {110, 120, 130}, values {11, 12, 13}.
        //
        // Rollup's `rate` extrapolation:
        //   result = last - first; time_diff = (last_t - first_t)/1000
        //   duration_to_start = (first_t - window_start)/1000
        //   duration_to_end = (window_end - last_t)/1000
        //   range_seconds = range_ms/1000 = 0.03
        //
        // Step 1: first=8@80, last=10@100; time_diff = 0.02; avg_interval=0.01;
        //   duration_to_start = (80 - 70)/1000 = 0.01 (< threshold 0.011 → keep).
        //   Counter-zero clip: result>0 && first>=0 → duration_to_zero = 8*(0.02/2) = 0.08;
        //   duration_to_zero >= duration_to_start so no clip applied.
        //   duration_to_end = 0 → factor_unit = (0.02+0.01+0)/0.02 = 1.5
        //   rate = 2 * 1.5 / 0.03 = 100/s.
        //
        // Step 2: same geometry, result = 2. rate = 100/s.
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 130,
            step_ms: 30,
            step_count: 2,
        };
        let series = mk_labels(1);
        let series_clone = series.clone();

        let call_idx = Rc::new(RefCell::new(0u64));
        // The factory is `Send` but we want the counter for debugging; keep
        // it side-effect-free so the callback can stay `Send`.
        let _ = call_idx;

        let factory: ChildFactory = Box::new(move |tr: TimeRange, step_ms: i64| {
            // Compute outer_t from the window range: end_ms_exclusive - 1.
            let outer_t = tr.end_ms_exclusive - 1;
            let (ts_arc, inner_grid) = mk_inner_grid(outer_t, 30, step_ms);
            let batches: Vec<StepBatch> = ts_arc
                .iter()
                .enumerate()
                .map(|(i, &t)| {
                    mk_batch(
                        ts_arc.clone(),
                        i,
                        series_clone.clone(),
                        &[Some((t / 10) as f64)],
                    )
                })
                .collect();
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });

        let subquery = SubqueryOp::new(
            factory,
            series,
            outer_grid,
            30,
            10,
            MemoryReservation::new(1 << 20),
        );

        // Feed subquery directly into RollupOp<SubqueryOp> via WindowStream.
        let mut rollup = RollupOp::new(
            subquery,
            RollupKind::Rate,
            30,
            MemoryReservation::new(1 << 20),
        );

        // when
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut batches = Vec::new();
        loop {
            match rollup.next(&mut cx) {
                Poll::Ready(None) => break,
                Poll::Ready(Some(Ok(b))) => batches.push(b),
                Poll::Ready(Some(Err(e))) => panic!("unexpected error: {e:?}"),
                Poll::Pending => panic!("unexpected Pending"),
            }
        }

        // then: two step batches, each with rate = 100/s on cell (0, 0).
        assert_eq!(batches.len(), 2);
        let a = batches[0].get(0, 0).expect("rate valid");
        let b = batches[1].get(0, 0).expect("rate valid");
        assert!((a - 100.0).abs() < 1e-9, "first rate = {a}");
        assert!((b - 100.0).abs() < 1e-9, "second rate = {b}");
    }

    #[test]
    fn should_use_effective_times_for_inner_window() {
        // given: one outer step with `effective_times[0] = 200` — the
        // inner window must cover `(200 - range, 200]` even though the
        // outer step timestamp is `100`. This models an `@ 200` subquery
        // evaluated at `outer_t = 100`.
        let outer_grid = StepGrid {
            start_ms: 100,
            end_ms: 100,
            step_ms: 60,
            step_count: 1,
        };
        let series = mk_labels(1);
        let series_clone = series.clone();
        let received_range: Arc<Mutex<Option<TimeRange>>> = Arc::new(Mutex::new(None));
        let received_range_clone = received_range.clone();

        let factory: ChildFactory = Box::new(move |tr: TimeRange, step_ms: i64| {
            *received_range_clone.lock().unwrap() = Some(tr);
            let (ts_arc, inner_grid) = mk_inner_grid(tr.end_ms_exclusive - 1, 30, step_ms);
            let batches: Vec<StepBatch> = ts_arc
                .iter()
                .enumerate()
                .map(|(i, _)| mk_batch(ts_arc.clone(), i, series_clone.clone(), &[Some(1.0)]))
                .collect();
            let schema = OperatorSchema::new(SchemaRef::Static(series_clone.clone()), inner_grid);
            Ok(Box::new(ScriptedChild::new(schema, batches)) as Box<dyn Operator + Send>)
        });

        let mut op = SubqueryOp::with_effective_times(
            factory,
            series,
            outer_grid,
            30,
            10,
            Arc::from(vec![200i64]),
            MemoryReservation::new(1 << 20),
        );

        // when
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let _ = op.windows(&mut cx);

        // then: inner window covers `(170, 200]` — i.e. `start=171,
        // end_exclusive=201` — ignoring the outer step timestamp entirely.
        let tr = received_range.lock().unwrap().expect("factory invoked");
        assert_eq!(tr.start_ms, 171);
        assert_eq!(tr.end_ms_exclusive, 201);
    }
}

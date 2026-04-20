//! `RechunkOp` — the reshaping helper the planner inserts when two
//! adjacent operators disagree on the preferred `(step_chunk, series_chunk)`
//! tile shape. Same roster, same step grid, same values — only the
//! rectangle boundaries of the emitted [`StepBatch`]es change.
//!
//! Strategy: full materialisation. Buffer the entire
//! `step_count × series_count` grid into a dense scratch, then emit tiles
//! in the target shape. Worst-case memory is bounded at plan time because
//! both axes are plan-time constants; the scratch is charged once via
//! [`MemoryReservation::try_grow`] and released on drop / EOS.
//!
//! Passthrough short-circuit: if the first upstream batch already matches
//! the target shape, re-emit upstream batches verbatim with no copy and
//! no reservation. This is what makes it safe to leave a `RechunkOp` in
//! the plan even when upstream already produces the right rectangle.
//!
//! Upstream batches may arrive in any interleaving of step / series
//! ranges — the operator tolerates reordering.

use std::sync::Arc;
use std::task::{Context, Poll};

use super::super::batch::{BitSet, SchemaRef, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};

// ---------------------------------------------------------------------------
// Scratch buffer — charged against MemoryReservation, released on drop
// ---------------------------------------------------------------------------

/// Full-grid scratch buffer, charged against a [`MemoryReservation`].
struct Scratch {
    reservation: MemoryReservation,
    bytes: usize,
    step_count: usize,
    series_count: usize,
    /// Row-major by step, length = `step_count * series_count`.
    values: Vec<f64>,
    /// Parallel to [`Self::values`].
    validity: BitSet,
}

impl Scratch {
    fn allocate(
        reservation: &MemoryReservation,
        step_count: usize,
        series_count: usize,
    ) -> Result<Self, QueryError> {
        let cells = step_count.saturating_mul(series_count);
        let bytes = scratch_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            step_count,
            series_count,
            values: vec![f64::NAN; cells],
            validity: BitSet::with_len(cells),
        })
    }

    #[inline]
    fn cell_index(&self, step_off: usize, series_off: usize) -> usize {
        step_off * self.series_count + series_off
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
            self.bytes = 0;
        }
    }
}

#[inline]
fn scratch_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

// ---------------------------------------------------------------------------
// RechunkOp — the operator
// ---------------------------------------------------------------------------

/// Reshaping operator that repackages its child's [`StepBatch`]es to a
/// different `(step_chunk, series_chunk)` tile shape, leaving the roster,
/// grid, and values unchanged.
pub struct RechunkOp<C: Operator> {
    child: C,
    target_step_chunk: usize,
    target_series_chunk: usize,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    state: State,
}

enum State {
    Init,
    /// First upstream batch matches the target exactly; pass through.
    Passthrough,
    /// Draining all upstream batches into [`Scratch`].
    Draining {
        scratch: Scratch,
        step_timestamps: Arc<[i64]>,
        series: SchemaRef,
    },
    /// Emitting tiles from [`Scratch`]. `next_step` / `next_series` are the
    /// top-left cell (global offsets) of the next tile.
    Emitting {
        scratch: Scratch,
        step_timestamps: Arc<[i64]>,
        series: SchemaRef,
        next_step: usize,
        next_series: usize,
        step_count: usize,
        series_count: usize,
    },
    Done,
}

impl<C: Operator> RechunkOp<C> {
    /// `target_*_chunk` values must be `> 0`.
    pub fn new(
        child: C,
        target_step_chunk: usize,
        target_series_chunk: usize,
        reservation: MemoryReservation,
    ) -> Self {
        debug_assert!(
            target_step_chunk > 0,
            "RechunkOp target_step_chunk must be > 0",
        );
        debug_assert!(
            target_series_chunk > 0,
            "RechunkOp target_series_chunk must be > 0",
        );
        // Schema passes through from the child. `step_grid` is plan-time
        // invariant across the whole pipeline (RFC §"Core Data Model").
        let schema = child.schema().clone();
        Self {
            child,
            target_step_chunk,
            target_series_chunk,
            reservation,
            schema,
            state: State::Init,
        }
    }

    fn grid_shape(&self) -> (usize, usize) {
        let step_count = self.schema.step_grid.step_count;
        let series_count = match &self.schema.series {
            SchemaRef::Static(s) => s.len(),
            // `Rechunk` downstream of `count_values` is not a v1 scenario
            // (the deferred-schema operator is itself the only breaker
            // that binds at runtime). Fall back to 0 — the operator will
            // drain the child empty and emit nothing, which is the safe
            // behaviour until a future unit defines rechunk-after-deferred.
            SchemaRef::Deferred => 0,
        };
        (step_count, series_count)
    }

    fn ingest(scratch: &mut Scratch, batch: &StepBatch) {
        let step_count_in = batch.step_count();
        let series_count_in = batch.series_count();
        let step_start = batch.step_range.start;
        let series_start = batch.series_range.start;
        for step_off in 0..step_count_in {
            let global_step = step_start + step_off;
            for series_off in 0..series_count_in {
                let global_series = series_start + series_off;
                let in_idx = step_off * series_count_in + series_off;
                if batch.validity.get(in_idx) {
                    let out_idx = scratch.cell_index(global_step, global_series);
                    scratch.values[out_idx] = batch.values[in_idx];
                    scratch.validity.set(out_idx);
                }
                // absent input cells stay absent in scratch (`BitSet`
                // starts all-clear).
            }
        }
    }

    /// Extract a tile from the scratch buffer into a fresh `StepBatch`.
    ///
    /// The tile covers `step_range = [step_start, step_end)` and
    /// `series_range = [series_start, series_end)`. Values and validity
    /// are copied bit-for-bit; the scratch buffer is not modified. Memory
    /// for the output `Vec<f64>` / `BitSet` is *not* re-reserved — it's
    /// accounted-for in the scratch allocation, and the scratch is
    /// released when the last tile leaves (`Drop` on state transition).
    fn extract_tile(
        scratch: &Scratch,
        step_timestamps: Arc<[i64]>,
        series: SchemaRef,
        step_start: usize,
        step_end: usize,
        series_start: usize,
        series_end: usize,
    ) -> StepBatch {
        let out_step_count = step_end - step_start;
        let out_series_count = series_end - series_start;
        let cells = out_step_count * out_series_count;
        let mut values = vec![f64::NAN; cells];
        let mut validity = BitSet::with_len(cells);
        for step_off in 0..out_step_count {
            let scratch_step = step_start + step_off;
            for series_off in 0..out_series_count {
                let scratch_series = series_start + series_off;
                let src = scratch.cell_index(scratch_step, scratch_series);
                let dst = step_off * out_series_count + series_off;
                if scratch.validity.get(src) {
                    values[dst] = scratch.values[src];
                    validity.set(dst);
                }
            }
        }
        StepBatch::new(
            step_timestamps,
            step_start..step_end,
            series,
            series_start..series_end,
            values,
            validity,
        )
    }
}

impl<C: Operator> Operator for RechunkOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        loop {
            match std::mem::replace(&mut self.state, State::Done) {
                State::Done => {
                    return Poll::Ready(None);
                }
                State::Init => {
                    // Peek the first upstream batch. If its shape matches
                    // the target exactly and covers a tile boundary (step
                    // offset is a multiple of target_step_chunk, series
                    // offset likewise), passthrough. Otherwise, fall
                    // through to full materialisation.
                    match self.child.next(cx) {
                        Poll::Pending => {
                            self.state = State::Init;
                            return Poll::Pending;
                        }
                        Poll::Ready(None) => {
                            // child is empty; nothing to emit.
                            return Poll::Ready(None);
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(Some(Ok(batch))) => {
                            if batch_matches_target(
                                &batch,
                                self.target_step_chunk,
                                self.target_series_chunk,
                                self.schema.step_grid.step_count,
                                grid_series_count(&self.schema),
                            ) {
                                self.state = State::Passthrough;
                                return Poll::Ready(Some(Ok(batch)));
                            } else {
                                // Spill into full materialisation. Allocate
                                // scratch sized for the full grid.
                                let (step_count, series_count) = self.grid_shape();
                                let mut scratch = match Scratch::allocate(
                                    &self.reservation,
                                    step_count,
                                    series_count,
                                ) {
                                    Ok(s) => s,
                                    Err(e) => {
                                        self.state = State::Done;
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                };
                                // Remember the first batch's `step_timestamps`
                                // and `series` — they're invariant for the
                                // whole stream (plan-time shared).
                                let step_timestamps = batch.step_timestamps.clone();
                                let series = batch.series.clone();
                                Self::ingest(&mut scratch, &batch);
                                self.state = State::Draining {
                                    scratch,
                                    step_timestamps,
                                    series,
                                };
                                // fall through to the Draining arm on the
                                // next loop iteration.
                            }
                        }
                    }
                }
                State::Passthrough => {
                    // Pipe upstream batches through verbatim.
                    match self.child.next(cx) {
                        Poll::Pending => {
                            self.state = State::Passthrough;
                            return Poll::Pending;
                        }
                        Poll::Ready(None) => {
                            return Poll::Ready(None);
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(Some(Ok(batch))) => {
                            self.state = State::Passthrough;
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }
                }
                State::Draining {
                    mut scratch,
                    step_timestamps,
                    series,
                } => match self.child.next(cx) {
                    Poll::Pending => {
                        self.state = State::Draining {
                            scratch,
                            step_timestamps,
                            series,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(Some(Err(e))) => {
                        // drop scratch (releases reservation) and go Done.
                        drop(scratch);
                        self.state = State::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(Some(Ok(batch))) => {
                        Self::ingest(&mut scratch, &batch);
                        self.state = State::Draining {
                            scratch,
                            step_timestamps,
                            series,
                        };
                        // continue loop
                    }
                    Poll::Ready(None) => {
                        // Transition to tile emission. `step_count` and
                        // `series_count` are invariant across emission.
                        let step_count = scratch.step_count;
                        let series_count = scratch.series_count;
                        if step_count == 0 || series_count == 0 {
                            // scratch drops here, releasing reservation.
                            drop(scratch);
                            self.state = State::Done;
                            return Poll::Ready(None);
                        }
                        self.state = State::Emitting {
                            scratch,
                            step_timestamps,
                            series,
                            next_step: 0,
                            next_series: 0,
                            step_count,
                            series_count,
                        };
                        // continue loop — the Emitting arm will emit the
                        // first tile immediately.
                    }
                },
                State::Emitting {
                    scratch,
                    step_timestamps,
                    series,
                    next_step,
                    next_series,
                    step_count,
                    series_count,
                } => {
                    // Advance tile cursor. Tile bounds may be short if the
                    // grid doesn't divide evenly by the target chunks.
                    let step_end = (next_step + self.target_step_chunk).min(step_count);
                    let series_end = (next_series + self.target_series_chunk).min(series_count);
                    let batch = Self::extract_tile(
                        &scratch,
                        step_timestamps.clone(),
                        series.clone(),
                        next_step,
                        step_end,
                        next_series,
                        series_end,
                    );

                    // Compute the cursor for the *next* tile. Sweep
                    // series-major inside a step band, then advance to
                    // the next step band. (Order doesn't matter for
                    // correctness — consumers don't assume anything about
                    // tile emission order — but this ordering matches the
                    // natural readout of a row-major scratch buffer.)
                    let (new_next_series, new_next_step) = if series_end < series_count {
                        (series_end, next_step)
                    } else if step_end < step_count {
                        (0, step_end)
                    } else {
                        // Last tile — drop scratch on next state transition.
                        // Using a sentinel (step_count, series_count) so
                        // the next call flips to Done.
                        (series_count, step_count)
                    };

                    // If the next cursor is at the end, this was the last
                    // tile: drop the scratch and transition to Done.
                    let exhausted = new_next_step == step_count;
                    if exhausted {
                        drop(scratch);
                        self.state = State::Done;
                    } else {
                        self.state = State::Emitting {
                            scratch,
                            step_timestamps,
                            series,
                            next_step: new_next_step,
                            next_series: new_next_series,
                            step_count,
                            series_count,
                        };
                    }

                    return Poll::Ready(Some(Ok(batch)));
                }
            }
        }
    }
}

/// Series-count side of the grid shape — derived from the schema handle.
fn grid_series_count(schema: &OperatorSchema) -> usize {
    match &schema.series {
        SchemaRef::Static(s) => s.len(),
        SchemaRef::Deferred => 0,
    }
}

/// `true` when a batch's shape already matches the target (in which case
/// the rechunker can passthrough).
///
/// The passthrough short-circuit fires only when the *first* upstream
/// batch covers exactly one target tile and starts on a tile boundary.
/// If the upstream happens to emit the whole grid as one large batch
/// that doesn't match the target, we fall through to full
/// materialisation — correct, if less efficient. The planner won't
/// insert `Rechunk` in a same-shape plan anyway, so this is a best-
/// effort cheap path.
fn batch_matches_target(
    batch: &StepBatch,
    target_step_chunk: usize,
    target_series_chunk: usize,
    grid_step_count: usize,
    grid_series_count: usize,
) -> bool {
    // Exact-fit tile: the batch must be a single target tile. Starts on a
    // tile boundary, span is either the target chunk *or* a shorter
    // partial tile at the grid edge. Spans longer than the target — even
    // if they happen to be the full grid — must fall through to full
    // materialisation (otherwise we'd emit batches bigger than the
    // downstream expected).
    let step_start = batch.step_range.start;
    let step_len = batch.step_range.len();
    let series_start = batch.series_range.start;
    let series_len = batch.series_range.len();

    let step_ok = step_start.is_multiple_of(target_step_chunk)
        && (step_len == target_step_chunk
            || (step_start + step_len == grid_step_count && step_len < target_step_chunk));
    let series_ok = series_start.is_multiple_of(target_series_chunk)
        && (series_len == target_series_chunk
            || (series_start + series_len == grid_series_count
                && series_len < target_series_chunk));
    step_ok && series_ok
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels};
    use crate::promql::batch::SeriesSchema;
    use crate::promql::operator::StepGrid;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    fn mk_labels(i: usize) -> Labels {
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
    }

    fn mk_schema(n: usize) -> Arc<SeriesSchema> {
        let labels: Vec<Labels> = (0..n).map(mk_labels).collect();
        let fps: Vec<u128> = (0..n as u128).collect();
        Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
    }

    fn mk_grid(step_count: usize) -> StepGrid {
        StepGrid {
            start_ms: 1_000,
            end_ms: 1_000 + ((step_count as i64 - 1).max(0)) * 1_000,
            step_ms: 1_000,
            step_count,
        }
    }

    fn mk_timestamps(step_count: usize) -> Arc<[i64]> {
        Arc::from(
            (0..step_count)
                .map(|i| 1_000 + (i as i64) * 1_000)
                .collect::<Vec<i64>>(),
        )
    }

    /// Build a batch for a given `step_range × series_range` tile. Values
    /// are deterministic: `cell = global_step * 100 + global_series`.
    fn mk_batch_tile(
        step_timestamps: Arc<[i64]>,
        schema: Arc<SeriesSchema>,
        step_range: std::ops::Range<usize>,
        series_range: std::ops::Range<usize>,
    ) -> StepBatch {
        let step_count = step_range.len();
        let series_count = series_range.len();
        let cells = step_count * series_count;
        let mut values = Vec::with_capacity(cells);
        let mut validity = BitSet::with_len(cells);
        for step_off in 0..step_count {
            for series_off in 0..series_count {
                let gs = step_range.start + step_off;
                let gser = series_range.start + series_off;
                let idx = step_off * series_count + series_off;
                values.push((gs as f64) * 100.0 + (gser as f64));
                validity.set(idx);
            }
        }
        StepBatch::new(
            step_timestamps,
            step_range,
            SchemaRef::Static(schema),
            series_range,
            values,
            validity,
        )
    }

    /// Compute the deterministic value for `(global_step, global_series)`.
    fn expected(step: usize, series: usize) -> f64 {
        (step as f64) * 100.0 + (series as f64)
    }

    struct MockOp {
        schema: OperatorSchema,
        queue: Vec<Result<StepBatch, QueryError>>,
    }

    impl MockOp {
        fn new(schema: Arc<SeriesSchema>, grid: StepGrid, batches: Vec<StepBatch>) -> Self {
            Self {
                schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
                queue: batches.into_iter().map(Ok).collect(),
            }
        }
        fn with_queue(
            schema: Arc<SeriesSchema>,
            grid: StepGrid,
            queue: Vec<Result<StepBatch, QueryError>>,
        ) -> Self {
            Self {
                schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
                queue,
            }
        }
    }

    impl Operator for MockOp {
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

    fn drive<C: Operator>(op: &mut RechunkOp<C>) -> Vec<Result<StepBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(r)) => out.push(r),
                Poll::Pending => panic!("unexpected Pending from sync mock"),
            }
        }
    }

    // ========================================================================
    // required tests
    // ========================================================================

    #[test]
    fn should_passthrough_when_shapes_already_match() {
        // given: one upstream batch already matches target shape (8×4).
        let schema = mk_schema(4);
        let grid = mk_grid(8);
        let ts = mk_timestamps(8);
        let batch = mk_batch_tile(ts, schema.clone(), 0..8, 0..4);
        let original_values = batch.values.clone();
        let child = MockOp::new(schema, grid, vec![batch]);

        // when
        let mut op = RechunkOp::new(child, 8, 4, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one output, shape matches, values round-trip.
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].step_range, 0..8);
        assert_eq!(outs[0].series_range, 0..4);
        assert_eq!(outs[0].values, original_values);
    }

    #[test]
    fn should_rechunk_to_wider_step_tiles() {
        // given: two upstream batches (steps 0..8, 8..16) each 4 series wide;
        // target = 16 steps × 4 series → one output batch.
        let schema = mk_schema(4);
        let grid = mk_grid(16);
        let ts = mk_timestamps(16);
        let batch_a = mk_batch_tile(ts.clone(), schema.clone(), 0..8, 0..4);
        let batch_b = mk_batch_tile(ts, schema.clone(), 8..16, 0..4);
        let child = MockOp::new(schema, grid, vec![batch_a, batch_b]);

        // when
        let mut op = RechunkOp::new(child, 16, 4, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one batch of 16×4.
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.step_range, 0..16);
        assert_eq!(b.series_range, 0..4);
        for step in 0..16 {
            for series in 0..4 {
                assert_eq!(b.get(step, series), Some(expected(step, series)));
            }
        }
    }

    #[test]
    fn should_rechunk_to_narrower_series_tiles() {
        // given: one upstream batch of 4 steps × 8 series; target = 4 × 2
        // → expect 4 output batches, each 4 × 2.
        let schema = mk_schema(8);
        let grid = mk_grid(4);
        let ts = mk_timestamps(4);
        let batch = mk_batch_tile(ts, schema.clone(), 0..4, 0..8);
        let child = MockOp::new(schema, grid, vec![batch]);

        // when
        let mut op = RechunkOp::new(child, 4, 2, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        assert_eq!(outs.len(), 4);
        for (i, b) in outs.iter().enumerate() {
            assert_eq!(b.step_range, 0..4);
            assert_eq!(b.series_range, (i * 2)..(i * 2 + 2));
            for step in 0..4 {
                for so in 0..2 {
                    let series = i * 2 + so;
                    assert_eq!(b.get(step, so), Some(expected(step, series)));
                }
            }
        }
    }

    #[test]
    fn should_rechunk_along_both_axes() {
        // given: upstream emits a single 8 × 4 batch; target = 4 × 2 →
        // 2 step bands × 2 series bands = 4 output batches.
        let schema = mk_schema(4);
        let grid = mk_grid(8);
        let ts = mk_timestamps(8);
        let batch = mk_batch_tile(ts, schema.clone(), 0..8, 0..4);
        let child = MockOp::new(schema, grid, vec![batch]);

        // when
        let mut op = RechunkOp::new(child, 4, 2, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: 4 tiles, cover the full grid once.
        assert_eq!(outs.len(), 4);
        // Check that every cell appears exactly once with the right value.
        let mut covered = vec![vec![false; 4]; 8];
        for b in &outs {
            for step_off in 0..b.step_count() {
                for series_off in 0..b.series_count() {
                    let gs = b.step_range.start + step_off;
                    let gser = b.series_range.start + series_off;
                    assert!(!covered[gs][gser], "cell ({gs},{gser}) covered twice");
                    covered[gs][gser] = true;
                    assert_eq!(b.get(step_off, series_off), Some(expected(gs, gser)));
                }
            }
        }
        for row in &covered {
            assert!(row.iter().all(|&c| c), "not every cell covered");
        }
    }

    #[test]
    fn should_preserve_values_and_validity() {
        // given: 2×2 upstream batch with a mix of valid/invalid cells.
        let schema = mk_schema(2);
        let grid = mk_grid(2);
        let ts = mk_timestamps(2);
        // Cell layout (step, series): (0,0)=1.0 valid; (0,1) invalid;
        // (1,0)=3.0 valid; (1,1)=NaN valid (real NaN, distinct from absence).
        let values = vec![1.0, 99.0, 3.0, f64::NAN];
        let mut validity = BitSet::with_len(4);
        validity.set(0);
        // idx 1 (step 0, series 1) stays clear
        validity.set(2);
        validity.set(3); // real NaN is "valid" per RFC.
        let batch = StepBatch::new(
            ts,
            0..2,
            SchemaRef::Static(schema.clone()),
            0..2,
            values,
            validity,
        );
        let child = MockOp::new(schema, grid, vec![batch]);

        // when: rechunk to 1 × 1 (4 tiles).
        let mut op = RechunkOp::new(child, 1, 1, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: 4 tiles; each carries the exact cell.
        assert_eq!(outs.len(), 4);
        let find = |gs: usize, gser: usize| -> &StepBatch {
            outs.iter()
                .find(|b| b.step_range.start == gs && b.series_range.start == gser)
                .unwrap()
        };
        assert_eq!(find(0, 0).get(0, 0), Some(1.0));
        assert_eq!(find(0, 1).get(0, 0), None); // absence preserved
        assert_eq!(find(1, 0).get(0, 0), Some(3.0));
        // (1,1) is a real NaN — valid, but value is NaN.
        let v = find(1, 1);
        assert!(v.validity.get(0), "validity bit must be set for real NaN");
        assert!(v.values[0].is_nan(), "value must be NaN");
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: a 4×4 grid and a reservation cap of 1 byte — scratch alloc
        // must fail.
        let schema = mk_schema(4);
        let grid = mk_grid(4);
        let ts = mk_timestamps(4);
        let batch = mk_batch_tile(ts, schema.clone(), 0..4, 0..4);
        let child = MockOp::new(schema, grid, vec![batch]);

        // when: rechunk to 2×2 (won't match — forces full materialisation).
        let mut op = RechunkOp::new(child, 2, 2, MemoryReservation::new(1));
        let outs = drive(&mut op);

        // then: one error; subsequent polls Ready(None).
        assert_eq!(outs.len(), 1);
        assert!(matches!(outs[0], Err(QueryError::MemoryLimit { .. })));
    }

    #[test]
    fn should_release_memory_after_drain() {
        // given: a reservation and a full-materialisation rechunk.
        let schema = mk_schema(4);
        let grid = mk_grid(4);
        let ts = mk_timestamps(4);
        let batch = mk_batch_tile(ts, schema.clone(), 0..4, 0..4);
        let child = MockOp::new(schema, grid, vec![batch]);
        let res = MemoryReservation::new(1 << 20);
        let before = res.reserved();

        // when: rechunk 4×4 → 2×2 and fully drain.
        let mut op = RechunkOp::new(child, 2, 2, res.clone());
        let outs = drive(&mut op);

        // then: all tiles emitted without error, and reservation is back to
        // pre-call state.
        assert_eq!(outs.len(), 4);
        assert!(outs.iter().all(|r| r.is_ok()));
        drop(op);
        assert_eq!(res.reserved(), before);
    }

    #[test]
    fn should_propagate_error_from_upstream() {
        // given: child yields Err on the second poll. First ingests, second
        // errors during drain.
        let schema = mk_schema(4);
        let grid = mk_grid(8);
        let ts = mk_timestamps(8);
        let batch = mk_batch_tile(ts, schema.clone(), 0..4, 0..4);
        let queue = vec![Ok(batch), Err(QueryError::Internal("boom".to_string()))];
        let child = MockOp::with_queue(schema, grid, queue);

        // when: force full-materialisation by asking for a non-matching shape.
        let mut op = RechunkOp::new(child, 8, 4, MemoryReservation::new(1 << 20));
        let outs = drive(&mut op);

        // then: error surfaces; drain halts; subsequent polls Ready(None).
        assert_eq!(outs.len(), 1);
        assert!(matches!(outs[0], Err(QueryError::Internal(_))));
    }

    #[test]
    fn should_return_static_schema() {
        // given: child with Static schema (4 series, 8 steps).
        let schema = mk_schema(4);
        let grid = mk_grid(8);
        let child = MockOp::new(schema.clone(), grid, vec![]);

        // when
        let op = RechunkOp::new(child, 4, 2, MemoryReservation::new(1 << 20));

        // then: schema passes through unchanged.
        assert!(!op.schema().series.is_deferred());
        assert_eq!(op.schema().series.as_static().unwrap().len(), 4);
        assert_eq!(op.schema().step_grid.step_count, 8);
    }

    #[test]
    fn should_handle_child_end_of_stream_with_partial_tile() {
        // given: 5 steps × 3 series total, target = 2 × 2 → last tile along
        // each axis is shorter (1-step × 1-series corner).
        let schema = mk_schema(3);
        let grid = mk_grid(5);
        let ts = mk_timestamps(5);
        let batch = mk_batch_tile(ts, schema.clone(), 0..5, 0..3);
        let child = MockOp::new(schema, grid, vec![batch]);

        // when
        let mut op = RechunkOp::new(child, 2, 2, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: step bands = [0..2, 2..4, 4..5]; series bands = [0..2, 2..3]
        // → 3 × 2 = 6 tiles; shortest = 1×1.
        assert_eq!(outs.len(), 6);
        let mut covered = vec![vec![false; 3]; 5];
        for b in &outs {
            // tile step span ≤ 2, series span ≤ 2; short tiles allowed.
            assert!(b.step_count() <= 2);
            assert!(b.series_count() <= 2);
            for step_off in 0..b.step_count() {
                for series_off in 0..b.series_count() {
                    let gs = b.step_range.start + step_off;
                    let gser = b.series_range.start + series_off;
                    assert!(!covered[gs][gser]);
                    covered[gs][gser] = true;
                    assert_eq!(b.get(step_off, series_off), Some(expected(gs, gser)));
                }
            }
        }
        for row in &covered {
            assert!(row.iter().all(|&c| c));
        }
    }

    #[test]
    fn should_handle_empty_upstream() {
        // given: child yields None immediately.
        let schema = mk_schema(4);
        let grid = mk_grid(8);
        let child = MockOp::new(schema, grid, vec![]);

        // when
        let mut op = RechunkOp::new(child, 4, 2, MemoryReservation::new(1 << 20));
        let outs = drive(&mut op);

        // then: no output, no error.
        assert!(outs.is_empty());
    }

    #[test]
    fn should_rechunk_across_multiple_series_tiles_over_512_series() {
        // given: 1024 series × 2 steps delivered as three (step_tile ×
        // series_tile) batches with the same 512-wide series chunk that
        // `VectorSelectorOp` uses by default. Asking `RechunkOp` for a
        // single tile covering the full grid exercises its scratch
        // aggregation across multi-series-tile input.
        const SERIES: usize = 1024;
        const TILE: usize = 512;
        let schema = mk_schema(SERIES);
        let grid = mk_grid(2);
        let ts = mk_timestamps(2);

        let batch_a = mk_batch_tile(ts.clone(), schema.clone(), 0..2, 0..TILE);
        let batch_b = mk_batch_tile(ts.clone(), schema.clone(), 0..2, TILE..SERIES);
        let child = MockOp::new(schema, grid, vec![batch_a, batch_b]);

        // when: rechunk to a single 2 × 1024 output tile.
        let mut op = RechunkOp::new(child, 2, SERIES, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one tile with deterministic values at every cell across
        // both input tiles.
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.step_range, 0..2);
        assert_eq!(b.series_range, 0..SERIES);
        for step in 0..2 {
            for series in 0..SERIES {
                assert_eq!(b.get(step, series), Some(expected(step, series)));
            }
        }
    }

    #[test]
    fn should_handle_out_of_order_upstream_batches() {
        // given: upstream emits in reverse step order; scratch-based
        // ingest places each batch at its global (step, series) offset,
        // so output tiles still carry the right values.
        let schema = mk_schema(2);
        let grid = mk_grid(4);
        let ts = mk_timestamps(4);
        let batch_late = mk_batch_tile(ts.clone(), schema.clone(), 2..4, 0..2);
        let batch_early = mk_batch_tile(ts, schema.clone(), 0..2, 0..2);
        // Emission order: late first, then early. Force full materialisation
        // by asking for a target that doesn't match the first batch
        // (target = 4×2, first batch is 2×2).
        let child = MockOp::new(schema, grid, vec![batch_late, batch_early]);

        // when
        let mut op = RechunkOp::new(child, 4, 2, MemoryReservation::new(1 << 20));
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one 4×2 tile with correct values at every cell.
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.step_range, 0..4);
        assert_eq!(b.series_range, 0..2);
        for step in 0..4 {
            for series in 0..2 {
                assert_eq!(b.get(step, series), Some(expected(step, series)));
            }
        }
    }
}

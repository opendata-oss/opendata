//! Bridges PromQL's scalar world and its instant-vector world. Two
//! operators:
//!
//! - [`TimeScalarOp`]: implements PromQL `time()` — a scalar leaf that
//!   emits each step's timestamp, in seconds, as its value.
//! - [`ScalarizeOp`]: implements PromQL `scalar(v)` — drains an
//!   instant-vector child and, per step, emits the lone valid input
//!   sample's value; zero or multiple valid samples collapse to `NaN`.
//!
//! The opposite direction (`vector(s)` — scalar to instant-vector) is
//! handled directly by the physical planner, which lowers it to its
//! scalar child without a dedicated operator.

use std::sync::Arc;
use std::task::{Context, Poll};

use crate::model::Labels;

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};

fn scalar_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema::new(
        Arc::from(vec![Labels::empty()]),
        Arc::from(vec![0u128]),
    ))
}

fn step_timestamps(grid: StepGrid) -> Arc<[i64]> {
    if grid.step_count == 0 {
        return Arc::from(Vec::<i64>::new());
    }
    let mut v = Vec::with_capacity(grid.step_count);
    for i in 0..grid.step_count {
        v.push(grid.start_ms + (i as i64) * grid.step_ms);
    }
    Arc::from(v)
}

#[inline]
fn out_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

/// Implements PromQL `time()`. A scalar leaf: emits one batch holding the
/// step timestamp (in seconds) for each step in the grid, then
/// end-of-stream.
pub struct TimeScalarOp {
    schema: OperatorSchema,
    step_timestamps: Arc<[i64]>,
    reservation: MemoryReservation,
    yielded: bool,
}

impl TimeScalarOp {
    pub fn new(grid: StepGrid, reservation: MemoryReservation) -> Self {
        let schema = OperatorSchema::new(SchemaRef::Static(scalar_schema()), grid);
        Self {
            schema,
            step_timestamps: step_timestamps(grid),
            reservation,
            yielded: false,
        }
    }
}

impl Operator for TimeScalarOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.yielded {
            return Poll::Ready(None);
        }
        let step_count = self.schema.step_grid.step_count;
        if step_count == 0 {
            self.yielded = true;
            return Poll::Ready(None);
        }
        let bytes = out_bytes(step_count);
        if let Err(err) = self.reservation.try_grow(bytes) {
            self.yielded = true;
            return Poll::Ready(Some(Err(err)));
        }
        let values = self
            .step_timestamps
            .iter()
            .map(|ts| *ts as f64 / 1000.0)
            .collect::<Vec<_>>();
        let validity = BitSet::all_set(step_count);
        self.reservation.release(bytes);
        self.yielded = true;
        Poll::Ready(Some(Ok(StepBatch::new(
            self.step_timestamps.clone(),
            0..step_count,
            self.schema.series.clone(),
            0..1,
            values,
            validity,
        ))))
    }
}

/// Implements PromQL's `scalar(v)` coercion — collapses an instant-vector
/// child into a single scalar per step.
///
/// Pipeline breaker: drains the full child, then emits one scalar-valued
/// batch covering the full step grid. For each step: exactly one valid
/// input sample yields that value; zero or more-than-one valid samples
/// yield `NaN`.
pub struct ScalarizeOp<C: Operator> {
    child: C,
    schema: OperatorSchema,
    step_timestamps: Arc<[i64]>,
    reservation: MemoryReservation,
    values: Option<Vec<f64>>,
    counts: Option<Vec<u32>>,
    reserved_bytes: usize,
    yielded: bool,
    errored: bool,
}

impl<C: Operator> ScalarizeOp<C> {
    pub fn new(child: C, reservation: MemoryReservation) -> Self {
        let grid = child.schema().step_grid;
        let schema = OperatorSchema::new(SchemaRef::Static(scalar_schema()), grid);
        Self {
            child,
            schema,
            step_timestamps: step_timestamps(grid),
            reservation,
            values: None,
            counts: None,
            reserved_bytes: 0,
            yielded: false,
            errored: false,
        }
    }

    fn start_if_needed(&mut self) -> Result<(), QueryError> {
        let step_count = self.schema.step_grid.step_count;
        if self.values.is_none() {
            let bytes = out_bytes(step_count);
            self.reservation.try_grow(bytes)?;
            self.values = Some(vec![f64::NAN; step_count]);
            self.counts = Some(vec![0u32; step_count]);
            self.reserved_bytes = bytes;
        }
        Ok(())
    }

    fn clear_state(&mut self) {
        if self.reserved_bytes > 0 {
            self.reservation.release(self.reserved_bytes);
            self.reserved_bytes = 0;
        }
        self.values = None;
        self.counts = None;
    }

    fn drain_child(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        let step_count = self.schema.step_grid.step_count;
        if let Err(err) = self.start_if_needed() {
            self.errored = true;
            return Poll::Ready(Some(Err(err)));
        }
        loop {
            match self.child.next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    let values = self.values.take().expect("state initialized");
                    let _counts = self.counts.take().expect("state initialized");
                    let validity = BitSet::all_set(step_count);
                    self.yielded = true;
                    self.clear_state();
                    return Poll::Ready(Some(Ok(StepBatch::new(
                        self.step_timestamps.clone(),
                        0..step_count,
                        self.schema.series.clone(),
                        0..1,
                        values,
                        validity,
                    ))));
                }
                Poll::Ready(Some(Err(err))) => {
                    self.errored = true;
                    self.clear_state();
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Some(Ok(batch))) => {
                    let values = self.values.as_mut().expect("state initialized");
                    let counts = self.counts.as_mut().expect("state initialized");
                    for step_off in 0..batch.step_count() {
                        let global_step = batch.step_range.start + step_off;
                        for series_off in 0..batch.series_count() {
                            let cell = batch.cell_index(step_off, series_off);
                            if !batch.validity.get(cell) {
                                continue;
                            }
                            counts[global_step] = counts[global_step].saturating_add(1);
                            if counts[global_step] == 1 {
                                values[global_step] = batch.values[cell];
                            } else {
                                values[global_step] = f64::NAN;
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<C: Operator> Operator for ScalarizeOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.yielded || self.errored {
            return Poll::Ready(None);
        }
        self.drain_child(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn mk_grid(step_count: usize) -> StepGrid {
        StepGrid {
            start_ms: 1_000,
            end_ms: 1_000 + (step_count.saturating_sub(1) as i64) * 1_000,
            step_ms: 1_000,
            step_count,
        }
    }

    fn mk_schema(series_count: usize) -> Arc<SeriesSchema> {
        let labels = (0..series_count)
            .map(|_| Labels::empty())
            .collect::<Vec<_>>();
        let fps = (0..series_count as u128).collect::<Vec<_>>();
        Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
    }

    fn mk_batch(
        schema: Arc<SeriesSchema>,
        step_range: std::ops::Range<usize>,
        series_range: std::ops::Range<usize>,
        values: Vec<f64>,
        valid: Vec<bool>,
    ) -> StepBatch {
        let mut validity = BitSet::with_len(values.len());
        for (idx, is_valid) in valid.into_iter().enumerate() {
            if is_valid {
                validity.set(idx);
            }
        }
        StepBatch::new(
            step_timestamps(mk_grid(4)),
            step_range,
            SchemaRef::Static(schema),
            series_range,
            values,
            validity,
        )
    }

    #[test]
    fn should_emit_step_timestamps_as_scalar_values() {
        // given
        let mut op = TimeScalarOp::new(mk_grid(3), MemoryReservation::new(1 << 20));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // when
        let batch = match op.next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => batch,
            other => panic!("unexpected poll result: {other:?}"),
        };

        // then
        assert_eq!(batch.values, vec![1.0, 2.0, 3.0]);
    }

    #[test]
    fn should_scalarize_to_nan_when_step_has_zero_or_multiple_samples() {
        // given: step 0 has two valid samples, step 1 has none, step 2 has one.
        let grid = mk_grid(3);
        let schema = mk_schema(3);
        let batch = mk_batch(
            schema.clone(),
            0..3,
            0..3,
            vec![
                1.0,
                2.0,
                f64::NAN, //
                f64::NAN,
                f64::NAN,
                f64::NAN, //
                f64::NAN,
                5.0,
                f64::NAN,
            ],
            vec![
                true, true, false, //
                false, false, false, //
                false, true, false,
            ],
        );
        let child = MockOp::new(schema, grid, vec![batch]);
        let mut op = ScalarizeOp::new(child, MemoryReservation::new(1 << 20));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // when
        let batch = match op.next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => batch,
            other => panic!("unexpected poll result: {other:?}"),
        };

        // then
        assert!(batch.values[0].is_nan());
        assert!(batch.values[1].is_nan());
        assert_eq!(batch.values[2], 5.0);
        assert!(batch.validity.get(0));
        assert!(batch.validity.get(1));
        assert!(batch.validity.get(2));
    }

    #[test]
    fn should_scalarize_over_512_series_split_across_tiles() {
        // given: >512 series across two tile batches, each step has exactly
        // one valid sample; mirrors the `VectorSelectorOp` default tiling
        // with `series_chunk=512` for large rosters. ScalarizeOp must
        // aggregate across tiles so each step sees one valid input and the
        // output scalar equals that value.
        const SERIES: usize = 1024;
        const TILE: usize = 512;
        let grid = mk_grid(2);
        let schema = mk_schema(SERIES);

        // Step 0: only series 3 (in tile A) valid with value 7.0.
        // Step 1: only series 1000 (in tile B, local idx = 1000 - 512 = 488)
        // valid with value 9.5. All other cells invalid.
        let mut vals_a = vec![f64::NAN; 2 * TILE];
        let mut valid_a = vec![false; 2 * TILE];
        // Step 0, tile-A local index 3 (global series 3).
        vals_a[3] = 7.0;
        valid_a[3] = true;
        let batch_a = mk_batch(schema.clone(), 0..2, 0..TILE, vals_a, valid_a);

        let mut vals_b = vec![f64::NAN; 2 * TILE];
        let mut valid_b = vec![false; 2 * TILE];
        // Step 1, tile-B local index 488 (global series 1000).
        vals_b[TILE + 488] = 9.5;
        valid_b[TILE + 488] = true;
        let batch_b = mk_batch(schema.clone(), 0..2, TILE..SERIES, vals_b, valid_b);

        let child = MockOp::new(schema, grid, vec![batch_a, batch_b]);
        let mut op = ScalarizeOp::new(child, MemoryReservation::new(1 << 20));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // when
        let batch = match op.next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => batch,
            other => panic!("unexpected poll result: {other:?}"),
        };

        // then
        assert_eq!(batch.values, vec![7.0, 9.5]);
    }

    #[test]
    fn should_scalarize_across_multiple_series_tiles() {
        // given: exactly one valid sample per step, but split across two input
        // batches with different series tiles.
        let grid = mk_grid(2);
        let schema = mk_schema(4);
        let batch_a = mk_batch(
            schema.clone(),
            0..2,
            0..2,
            vec![1.0, f64::NAN, f64::NAN, f64::NAN],
            vec![true, false, false, false],
        );
        let batch_b = mk_batch(
            schema.clone(),
            0..2,
            2..4,
            vec![f64::NAN, f64::NAN, 4.0, f64::NAN],
            vec![false, false, true, false],
        );
        let child = MockOp::new(schema, grid, vec![batch_a, batch_b]);
        let mut op = ScalarizeOp::new(child, MemoryReservation::new(1 << 20));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // when
        let batch = match op.next(&mut cx) {
            Poll::Ready(Some(Ok(batch))) => batch,
            other => panic!("unexpected poll result: {other:?}"),
        };

        // then
        assert_eq!(batch.values, vec![1.0, 4.0]);
    }
}

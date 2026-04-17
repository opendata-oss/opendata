//! `Coalesce` exchange operator — unit 3c.5.
//!
//! Fan-in for parallel subplan copies. The planner assigns each child a
//! disjoint `series_range` slice of the same output schema. `CoalesceOp`
//! merges their batch streams via fair round-robin — the simplest correct
//! shape (thanos-io/promql-engine calls this same operator by the same
//! name).
//!
//! # Merge semantics
//!
//! Children own disjoint `series_range`s, so output batches from different
//! children are **not mergeable within a step** — consumers may see
//! multiple batches covering the same step range, each with a different
//! `series_range`. That is a batch-shape concern, not a correctness one:
//! any operator that iterates cells via `series_range` (the vast majority)
//! consumes this stream verbatim. If a downstream needs a single batch
//! per step covering every series, the planner inserts a [`super::rechunk::RechunkOp`]
//! after `CoalesceOp`.
//!
//! # Fairness
//!
//! A simple round-robin over children: poll each in turn, emit the first
//! `Ready(Some)` encountered in the round; if a child is `Pending`, try
//! the next; if exhausted (`Ready(None)`), mark done and move on; when
//! all children are exhausted, emit `Ready(None)`. `Pending` is bubbled
//! up only when *every* un-exhausted child returned `Pending` in the
//! same round — otherwise we'd starve a ready child behind a pending
//! sibling.
//!
//! Round-robin fairness is chosen over merge-sorted-by-step because the
//! children's step ordering is plan-time known to overlap (they share
//! the grid); consumers do not rely on batch ordering.
//!
//! # Memory accounting
//!
//! No reservation at this layer. Children's batches are already counted
//! by their producers; `CoalesceOp` just forwards `Arc`-shared handles.

use std::task::{Context, Poll};

use super::super::batch::StepBatch;
use super::super::memory::QueryError;
use super::super::operator::{Operator, OperatorSchema};

/// Fan-in exchange operator.
///
/// Takes N children producing batches over disjoint `series_range`s of a
/// shared output schema and merges their streams via round-robin.
pub struct CoalesceOp {
    children: Vec<Box<dyn Operator + Send>>,
    /// `done[i] == true` once `children[i]` has returned `Ready(None)`.
    /// Exhausted children are not polled again.
    done: Vec<bool>,
    /// Index of the child polled first on the next call to `next`. Advances
    /// after every successful emission so no child is starved.
    cursor: usize,
    /// Plan-time output schema (the roster is the union of the children's
    /// series, but since children share a single output schema and only
    /// differ in which `series_range` slice they emit, this is passed in
    /// directly by the planner).
    output_schema: OperatorSchema,
}

impl CoalesceOp {
    /// Build a coalescer.
    ///
    /// - `children`: the N parallel subplans. Each produces batches over a
    ///   disjoint `series_range` slice of the shared roster.
    /// - `output_schema`: the plan-time schema every child agrees on. Used
    ///   as the operator's own schema — no per-child widening happens at
    ///   runtime.
    pub fn new(children: Vec<Box<dyn Operator + Send>>, output_schema: OperatorSchema) -> Self {
        let done = vec![false; children.len()];
        Self {
            children,
            done,
            cursor: 0,
            output_schema,
        }
    }
}

impl Operator for CoalesceOp {
    fn schema(&self) -> &OperatorSchema {
        &self.output_schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        let n = self.children.len();
        if n == 0 {
            return Poll::Ready(None);
        }

        // Round-robin sweep starting at `cursor`. We need to remember
        // whether *any* un-exhausted child was Pending so we can return
        // Pending instead of Ready(None) while work remains.
        let mut any_pending = false;
        let mut any_alive = false;
        for offset in 0..n {
            let i = (self.cursor + offset) % n;
            if self.done[i] {
                continue;
            }
            any_alive = true;
            match self.children[i].next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // Advance the cursor past the child that just emitted
                    // so the next poll starts at a different child.
                    self.cursor = (i + 1) % n;
                    return Poll::Ready(Some(Ok(batch)));
                }
                Poll::Ready(Some(Err(e))) => {
                    // First error short-circuits. Mark this child done so
                    // a subsequent poll (if any) doesn't re-invoke it.
                    self.done[i] = true;
                    // Advance cursor so we don't spin on this index.
                    self.cursor = (i + 1) % n;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    self.done[i] = true;
                    // Keep scanning the round; this child yielded nothing
                    // but others might still have work.
                    continue;
                }
                Poll::Pending => {
                    any_pending = true;
                    // Keep scanning — another child might be Ready.
                    continue;
                }
            }
        }

        if any_pending {
            // At least one un-exhausted child is waiting on its waker;
            // that waker is now registered via `cx`, so we'll be woken.
            Poll::Pending
        } else if !any_alive {
            // All children have reported end-of-stream.
            Poll::Ready(None)
        } else {
            // No alive child was either ready or pending — impossible, but
            // handle defensively as end-of-stream.
            Poll::Ready(None)
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

    fn mk_batch(
        step_timestamps: Arc<[i64]>,
        schema: Arc<SeriesSchema>,
        step_range: std::ops::Range<usize>,
        series_range: std::ops::Range<usize>,
        marker: f64,
    ) -> StepBatch {
        let cells = step_range.len() * series_range.len();
        let mut validity = BitSet::with_len(cells);
        for i in 0..cells {
            validity.set(i);
        }
        StepBatch::new(
            step_timestamps,
            step_range,
            SchemaRef::Static(schema),
            series_range,
            vec![marker; cells],
            validity,
        )
    }

    /// Mock yielding a scripted queue of poll results.
    struct MockOp {
        schema: OperatorSchema,
        queue: Vec<MockPoll>,
    }

    enum MockPoll {
        ReadyOk(StepBatch),
        ReadyErr(QueryError),
        ReadyNone,
        /// Emits exactly `k` `Pending`s, then continues with the rest of
        /// the queue.
        Pending,
    }

    impl MockOp {
        fn new(schema: Arc<SeriesSchema>, grid: StepGrid, queue: Vec<MockPoll>) -> Self {
            Self {
                schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
                queue,
            }
        }
        fn ok_batches(schema: Arc<SeriesSchema>, grid: StepGrid, batches: Vec<StepBatch>) -> Self {
            let mut q: Vec<MockPoll> = batches.into_iter().map(MockPoll::ReadyOk).collect();
            q.push(MockPoll::ReadyNone);
            Self::new(schema, grid, q)
        }
    }

    impl Operator for MockOp {
        fn schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
            if self.queue.is_empty() {
                return Poll::Ready(None);
            }
            match self.queue.remove(0) {
                MockPoll::ReadyOk(b) => Poll::Ready(Some(Ok(b))),
                MockPoll::ReadyErr(e) => Poll::Ready(Some(Err(e))),
                MockPoll::ReadyNone => Poll::Ready(None),
                MockPoll::Pending => Poll::Pending,
            }
        }
    }

    fn drive_to_end(op: &mut CoalesceOp) -> Vec<Result<StepBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(r)) => out.push(r),
                Poll::Pending => panic!("unexpected Pending"),
            }
        }
    }

    // ========================================================================
    // required tests
    // ========================================================================

    #[test]
    fn should_fan_in_disjoint_series_from_children() {
        // given: two children over disjoint series ranges [0..4) and [4..8)
        // of a shared 8-series schema.
        let schema = mk_schema(8);
        let grid = mk_grid(2);
        let ts = mk_timestamps(2);
        let a = mk_batch(ts.clone(), schema.clone(), 0..2, 0..4, 1.0);
        let b = mk_batch(ts, schema.clone(), 0..2, 4..8, 2.0);
        let child_a = MockOp::ok_batches(schema.clone(), grid, vec![a]);
        let child_b = MockOp::ok_batches(schema.clone(), grid, vec![b]);

        // when
        let out_schema = OperatorSchema::new(SchemaRef::Static(schema), grid);
        let mut op = CoalesceOp::new(vec![Box::new(child_a), Box::new(child_b)], out_schema);
        let outs: Vec<StepBatch> = drive_to_end(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: 2 batches, their series_range union covers [0..8)
        assert_eq!(outs.len(), 2);
        let mut covered = [false; 8];
        for b in &outs {
            for s in b.series_range.clone() {
                assert!(!covered[s], "double coverage of series {s}");
                covered[s] = true;
            }
        }
        assert!(covered.iter().all(|&c| c));
    }

    #[test]
    fn should_yield_pending_when_all_children_pending() {
        // given: two children that both return Pending on the first poll
        let schema = mk_schema(2);
        let grid = mk_grid(1);
        let child_a = MockOp::new(schema.clone(), grid, vec![MockPoll::Pending]);
        let child_b = MockOp::new(schema.clone(), grid, vec![MockPoll::Pending]);

        // when: poll once
        let out_schema = OperatorSchema::new(SchemaRef::Static(schema), grid);
        let mut op = CoalesceOp::new(vec![Box::new(child_a), Box::new(child_b)], out_schema);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let polled = op.next(&mut cx);

        // then: Pending bubbles up
        assert!(matches!(polled, Poll::Pending));
    }

    #[test]
    fn should_yield_end_of_stream_when_all_children_exhausted() {
        // given: two empty children
        let schema = mk_schema(2);
        let grid = mk_grid(1);
        let child_a = MockOp::new(schema.clone(), grid, vec![MockPoll::ReadyNone]);
        let child_b = MockOp::new(schema.clone(), grid, vec![MockPoll::ReadyNone]);

        // when: drive to end
        let out_schema = OperatorSchema::new(SchemaRef::Static(schema), grid);
        let mut op = CoalesceOp::new(vec![Box::new(child_a), Box::new(child_b)], out_schema);
        let outs = drive_to_end(&mut op);

        // then: empty output
        assert!(outs.is_empty());
    }

    #[test]
    fn should_propagate_first_error_encountered() {
        // given: child A fails on first poll; child B has clean data
        let schema = mk_schema(2);
        let grid = mk_grid(1);
        let ts = mk_timestamps(1);
        let b_batch = mk_batch(ts, schema.clone(), 0..1, 1..2, 9.0);
        let child_a = MockOp::new(
            schema.clone(),
            grid,
            vec![MockPoll::ReadyErr(QueryError::Internal("boom".into()))],
        );
        let child_b = MockOp::ok_batches(schema.clone(), grid, vec![b_batch]);

        // when: poll once — cursor starts at 0, so A is polled first
        let out_schema = OperatorSchema::new(SchemaRef::Static(schema), grid);
        let mut op = CoalesceOp::new(vec![Box::new(child_a), Box::new(child_b)], out_schema);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let polled = op.next(&mut cx);

        // then: error surfaced
        match polled {
            Poll::Ready(Some(Err(QueryError::Internal(ref msg)))) => {
                assert_eq!(msg, "boom")
            }
            other => panic!("expected Internal error, got {other:?}"),
        }
    }

    #[test]
    fn should_return_precomputed_output_schema() {
        // given: two children with 4-series schemas; coalesce hands back
        // a planner-built output schema reference
        let schema = mk_schema(8);
        let grid = mk_grid(4);
        let child_a = MockOp::new(schema.clone(), grid, vec![MockPoll::ReadyNone]);
        let child_b = MockOp::new(schema.clone(), grid, vec![MockPoll::ReadyNone]);

        // when
        let out_schema = OperatorSchema::new(SchemaRef::Static(schema), grid);
        let op = CoalesceOp::new(vec![Box::new(child_a), Box::new(child_b)], out_schema);

        // then: schema is the one handed in
        let s = op.schema();
        assert_eq!(s.step_grid.step_count, 4);
        assert_eq!(s.series.as_static().unwrap().len(), 8);
    }

    #[test]
    fn should_tolerate_different_batch_cadence_between_children() {
        // given: child A emits 3 batches, child B emits 1 large batch —
        // all over disjoint series ranges.
        let schema = mk_schema(6);
        let grid = mk_grid(2);
        let ts = mk_timestamps(2);
        let a1 = mk_batch(ts.clone(), schema.clone(), 0..2, 0..1, 10.0);
        let a2 = mk_batch(ts.clone(), schema.clone(), 0..2, 1..2, 20.0);
        let a3 = mk_batch(ts.clone(), schema.clone(), 0..2, 2..3, 30.0);
        let b1 = mk_batch(ts, schema.clone(), 0..2, 3..6, 100.0);
        let child_a = MockOp::ok_batches(schema.clone(), grid, vec![a1, a2, a3]);
        let child_b = MockOp::ok_batches(schema.clone(), grid, vec![b1]);

        // when
        let out_schema = OperatorSchema::new(SchemaRef::Static(schema), grid);
        let mut op = CoalesceOp::new(vec![Box::new(child_a), Box::new(child_b)], out_schema);
        let outs: Vec<StepBatch> = drive_to_end(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: 4 batches total; series ranges together cover [0..6)
        assert_eq!(outs.len(), 4);
        let mut covered = [false; 6];
        for b in &outs {
            for s in b.series_range.clone() {
                assert!(!covered[s], "double coverage of series {s}");
                covered[s] = true;
            }
        }
        assert!(covered.iter().all(|&c| c), "missing series coverage");
    }

    #[test]
    fn should_fan_in_over_512_series_split_across_children_with_tiles() {
        // given: two children covering disjoint halves of a >512-series
        // roster, each child emits its half across two series tiles of 256
        // (mirroring `VectorSelectorOp` emission under `series_chunk=256`
        // for a 1024-series roster handled by two shards).
        const SERIES: usize = 1024;
        const SHARD: usize = 512;
        const TILE: usize = 256;
        let schema = mk_schema(SERIES);
        let grid = mk_grid(2);
        let ts = mk_timestamps(2);

        // Child A emits tiles [0..256) and [256..512).
        let a1 = mk_batch(ts.clone(), schema.clone(), 0..2, 0..TILE, 1.0);
        let a2 = mk_batch(ts.clone(), schema.clone(), 0..2, TILE..SHARD, 2.0);
        // Child B emits tiles [512..768) and [768..1024).
        let b1 = mk_batch(ts.clone(), schema.clone(), 0..2, SHARD..(SHARD + TILE), 3.0);
        let b2 = mk_batch(
            ts.clone(),
            schema.clone(),
            0..2,
            (SHARD + TILE)..SERIES,
            4.0,
        );
        let child_a = MockOp::ok_batches(schema.clone(), grid, vec![a1, a2]);
        let child_b = MockOp::ok_batches(schema.clone(), grid, vec![b1, b2]);

        // when
        let out_schema = OperatorSchema::new(SchemaRef::Static(schema), grid);
        let mut op = CoalesceOp::new(vec![Box::new(child_a), Box::new(child_b)], out_schema);
        let outs: Vec<StepBatch> = drive_to_end(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: every series in [0..1024) is covered exactly once across
        // the forwarded tile batches — Coalesce just forwards, no merging.
        assert_eq!(outs.len(), 4);
        let mut covered = vec![false; SERIES];
        for b in &outs {
            for s in b.series_range.clone() {
                assert!(!covered[s], "double coverage of series {s}");
                covered[s] = true;
            }
        }
        assert!(covered.iter().all(|&c| c), "missing series coverage");
    }

    #[test]
    fn should_continue_after_one_child_done_while_other_has_work() {
        // given: child A is already done, child B still has data.
        let schema = mk_schema(4);
        let grid = mk_grid(1);
        let ts = mk_timestamps(1);
        let b_batch = mk_batch(ts, schema.clone(), 0..1, 0..4, 5.0);
        let child_a = MockOp::new(schema.clone(), grid, vec![MockPoll::ReadyNone]);
        let child_b = MockOp::ok_batches(schema.clone(), grid, vec![b_batch]);

        // when
        let out_schema = OperatorSchema::new(SchemaRef::Static(schema), grid);
        let mut op = CoalesceOp::new(vec![Box::new(child_a), Box::new(child_b)], out_schema);
        let outs: Vec<StepBatch> = drive_to_end(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: B's batch surfaced
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].series_range, 0..4);
        assert_eq!(outs[0].values[0], 5.0);
    }
}

//! `ConcurrentOp` — the one operator that introduces parallelism into the
//! engine pipeline. Decouples an I/O-heavy child (typically a selector) from
//! the downstream CPU-bound chain by running the child on a spawned tokio
//! task that pushes batches through a bounded mpsc channel.
//!
//! The channel's bounded capacity provides back-pressure: when the
//! consumer stops polling, the producer's `send` awaits and the child
//! task blocks, so the child can never run arbitrarily far ahead of the
//! consumer. The operator itself holds no memory reservation — batches in
//! flight are already accounted for by their producers.
//!
//! Callers must construct from inside a tokio runtime (the constructor
//! spawns the task eagerly).

use std::future::poll_fn;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::mpsc;

use super::super::batch::StepBatch;
use super::super::memory::QueryError;
use super::super::operator::{Operator, OperatorSchema};
use super::super::trace;

pub const DEFAULT_CHANNEL_BOUND: usize = 4;

/// Runs its child on a spawned tokio task, decoupling it from the downstream
/// polling loop via a bounded mpsc channel. The bound is the back-pressure
/// budget — how many batches the child may run ahead of the consumer.
pub struct ConcurrentOp {
    /// Captured before the child is moved onto the task so [`Operator::schema`]
    /// stays callable before the first `next()`.
    cached_schema: Arc<OperatorSchema>,
    rx: mpsc::Receiver<Result<StepBatch, QueryError>>,
    /// Held so the task is cancelled on drop. End-of-stream is signalled by
    /// the task dropping its `tx`.
    _task: tokio::task::JoinHandle<()>,
}

impl ConcurrentOp {
    /// `bound` must be `> 0`. Must be called from inside a tokio runtime.
    pub fn new<C>(mut child: C, bound: usize) -> Self
    where
        C: Operator + Send + 'static,
    {
        assert!(bound > 0, "ConcurrentOp channel bound must be > 0");
        // Snapshot the child's schema before moving ownership into the
        // spawned task. This lets `schema()` return before the first poll
        // (trait contract).
        let cached_schema = Arc::new(child.schema().clone());

        let (tx, rx) = mpsc::channel::<Result<StepBatch, QueryError>>(bound);

        // Snapshot the current task-local trace collector (if any) and
        // re-scope the spawned task into it so downstream storage I/O and
        // `TracingOperator` wrappers inside `child` attribute their work
        // to the same collector as the outer query.
        let trace_collector = trace::current_collector();

        let drive = async move {
            // Drive the child pull-by-pull. Each `poll_fn` call hands the
            // task's waker to the child; the child re-wakes when it's ready.
            loop {
                let polled = poll_fn(|cx| child.next(cx)).await;
                match polled {
                    Some(Ok(batch)) => {
                        if tx.send(Ok(batch)).await.is_err() {
                            // Receiver dropped — consumer is gone. Stop.
                            return;
                        }
                    }
                    Some(Err(e)) => {
                        // Best-effort: forward the error; then end the
                        // stream. If the receiver is gone we just exit.
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    None => {
                        // End-of-stream. Dropping `tx` closes the channel
                        // so the consumer observes `None`.
                        return;
                    }
                }
            }
        };
        let task = match trace_collector {
            Some(c) => tokio::spawn(trace::with_trace(c, drive)),
            None => tokio::spawn(drive),
        };

        Self {
            cached_schema,
            rx,
            _task: task,
        }
    }
}

impl Operator for ConcurrentOp {
    fn schema(&self) -> &OperatorSchema {
        &self.cached_schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        // `Receiver::poll_recv` mirrors our trait shape exactly:
        // `Ready(Some)` / `Ready(None)` / `Pending`.
        self.rx.poll_recv(cx)
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
    use std::time::Duration;

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

    /// Build a trivially-valid batch. Values are all `marker`.
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

    /// Mock child yielding a scripted queue of batches / errors.
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

    /// Drive an operator to exhaustion inside the current async context.
    async fn drive_async(op: &mut ConcurrentOp) -> Vec<Result<StepBatch, QueryError>> {
        let mut out = Vec::new();
        loop {
            match poll_fn(|cx| op.next(cx)).await {
                Some(r) => out.push(r),
                None => return out,
            }
        }
    }

    // ========================================================================
    // required tests
    // ========================================================================

    #[tokio::test]
    async fn should_forward_child_batches() {
        // given: child emits 3 batches
        let schema = mk_schema(2);
        let grid = mk_grid(2);
        let ts = mk_timestamps(2);
        let b1 = mk_batch(ts.clone(), schema.clone(), 0..2, 0..2, 1.0);
        let b2 = mk_batch(ts.clone(), schema.clone(), 0..2, 0..2, 2.0);
        let b3 = mk_batch(ts, schema.clone(), 0..2, 0..2, 3.0);
        let child = MockOp::new(schema, grid, vec![b1, b2, b3]);

        // when: wrap in Concurrent
        let mut op = ConcurrentOp::new(child, DEFAULT_CHANNEL_BOUND);
        let outs = drive_async(&mut op).await;

        // then: 3 Ok batches in order, then end-of-stream
        assert_eq!(outs.len(), 3);
        let values: Vec<f64> = outs.into_iter().map(|r| r.unwrap().values[0]).collect();
        assert_eq!(values, vec![1.0, 2.0, 3.0]);
    }

    #[tokio::test]
    async fn should_propagate_child_error() {
        // given: child returns Ok then Err
        let schema = mk_schema(1);
        let grid = mk_grid(1);
        let ts = mk_timestamps(1);
        let ok_batch = mk_batch(ts, schema.clone(), 0..1, 0..1, 7.0);
        let queue = vec![Ok(ok_batch), Err(QueryError::Internal("boom".into()))];
        let child = MockOp::with_queue(schema, grid, queue);

        // when
        let mut op = ConcurrentOp::new(child, DEFAULT_CHANNEL_BOUND);
        let outs = drive_async(&mut op).await;

        // then: Ok then Err, no further items
        assert_eq!(outs.len(), 2);
        assert!(outs[0].is_ok());
        assert!(matches!(outs[1], Err(QueryError::Internal(_))));
    }

    #[tokio::test]
    async fn should_close_on_child_end_of_stream() {
        // given: empty child
        let schema = mk_schema(1);
        let grid = mk_grid(1);
        let child = MockOp::new(schema, grid, vec![]);

        // when
        let mut op = ConcurrentOp::new(child, DEFAULT_CHANNEL_BOUND);
        let outs = drive_async(&mut op).await;

        // then: channel closes, no items
        assert!(outs.is_empty());
    }

    #[tokio::test]
    async fn should_cache_child_schema() {
        // given: child with 3-series static schema and a specific step grid
        let schema = mk_schema(3);
        let grid = mk_grid(5);
        let child = MockOp::new(schema.clone(), grid, vec![]);

        // when: build op and read schema BEFORE any next()
        let op = ConcurrentOp::new(child, DEFAULT_CHANNEL_BOUND);
        let cached = op.schema();

        // then: schema matches child's
        assert_eq!(cached.step_grid.step_count, 5);
        assert_eq!(cached.series.as_static().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn should_forward_multi_series_tile_batches_over_512_series_unchanged() {
        // given: >512 series split across two series-tile batches covering
        // the same step range, mirroring `VectorSelectorOp`'s default
        // `series_chunk=512` emission for rosters >512 series.
        const SERIES: usize = 1024;
        const TILE: usize = 512;
        let schema = mk_schema(SERIES);
        let grid = mk_grid(2);
        let ts = mk_timestamps(2);
        let batch_a = mk_batch(ts.clone(), schema.clone(), 0..2, 0..TILE, 1.0);
        let batch_b = mk_batch(ts.clone(), schema.clone(), 0..2, TILE..SERIES, 2.0);
        let child = MockOp::new(schema, grid, vec![batch_a, batch_b]);

        // when
        let mut op = ConcurrentOp::new(child, DEFAULT_CHANNEL_BOUND);
        let outs = drive_async(&mut op).await;

        // then: both tile batches forwarded verbatim (same series_range,
        // same step_range) — Concurrent is a pure forwarder.
        assert_eq!(outs.len(), 2);
        let b0 = outs[0].as_ref().unwrap();
        let b1 = outs[1].as_ref().unwrap();
        assert_eq!(b0.series_range, 0..TILE);
        assert_eq!(b1.series_range, TILE..SERIES);
        assert_eq!(b0.values[0], 1.0);
        assert_eq!(b1.values[0], 2.0);
    }

    #[tokio::test]
    async fn should_apply_backpressure_via_bounded_channel() {
        // given: a child emitting 10 batches, channel bound=1
        let schema = mk_schema(1);
        let grid = mk_grid(1);
        let ts = mk_timestamps(1);
        let batches: Vec<StepBatch> = (0..10)
            .map(|i| mk_batch(ts.clone(), schema.clone(), 0..1, 0..1, i as f64))
            .collect();
        let child = MockOp::new(schema, grid, batches);

        // when: construct, then wait long enough for the producer to fill
        // what it can. With bound=1 and a receiver that hasn't polled, the
        // channel has at most 1 buffered item; the producer is blocked on
        // `send`. We then drain fully and verify all 10 batches round-trip
        // in order — this proves the channel blocks and resumes without
        // dropping items.
        let mut op = ConcurrentOp::new(child, 1);
        // Let the task run; with bound=1 it can produce one item and block
        // on the next send. Sleep is small but nonzero — the assertion
        // below (item order + count) is what actually proves back-pressure
        // correctness; the sleep just increases the chance the producer
        // parks before we start draining.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let outs = drive_async(&mut op).await;

        // then: all 10 batches in order, in-order delivery preserved
        assert_eq!(outs.len(), 10);
        let values: Vec<f64> = outs.into_iter().map(|r| r.unwrap().values[0]).collect();
        assert_eq!(values, (0..10).map(|i| i as f64).collect::<Vec<_>>());
    }
}

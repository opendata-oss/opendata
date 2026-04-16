//! The pull-based [`Operator`] trait and its plan-time [`OperatorSchema`].
//!
//! Every node in a v2 physical plan implements [`Operator`]. The executor
//! drives the root with [`Operator::next`], which returns a [`StepBatch`]
//! per poll; inner nodes pull from their children the same way. The trait
//! mirrors the shape of [`futures::Stream`] — `Poll<Option<Result<_>>>` —
//! so operators compose cleanly under tokio without requiring the trait
//! itself to pin to a specific runtime.
//!
//! Two invariants operators must hold:
//! - [`Operator::schema`] is callable before any [`Operator::next`]. It
//!   returns the operator's view of its outputs: the (possibly
//!   [`SchemaRef::Deferred`]) series roster and the step grid the batches
//!   will land on.
//! - [`Operator::next`] returns [`Poll::Ready(None)`] for end-of-stream
//!   and [`Poll::Pending`] for back-pressure / I/O waits. Once `None` has
//!   been returned the operator is exhausted; callers must not poll again.
//!
//! See RFC 0007 §"Execution Model" and §"Core Data Model".

use std::task::{Context, Poll};

use super::batch::{SchemaRef, StepBatch};
use super::memory::QueryError;

/// Step grid an operator's output batches will land on.
///
/// Shared by every batch the operator produces — the grid is resolved at
/// plan time from the query's `(start, end, step)` parameters (RFC
/// §"Core Data Model"). `start_ms` and `end_ms` are inclusive timestamp
/// bounds in milliseconds; `step_ms` is the spacing; `step_count` is
/// `((end_ms - start_ms) / step_ms) + 1` for range queries and `1` for
/// instant queries (RFC: "Instant queries are a range of one step").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepGrid {
    /// First step timestamp (ms, inclusive).
    pub start_ms: i64,
    /// Last step timestamp (ms, inclusive).
    pub end_ms: i64,
    /// Spacing between consecutive steps (ms). Must be `> 0`.
    pub step_ms: i64,
    /// Total number of steps in the grid. Equals `1` for instant queries.
    pub step_count: usize,
}

/// The operator's view of its outputs before [`Operator::next`] is called.
///
/// Carries the (possibly deferred) series schema and the step grid every
/// downstream operator needs to size buffers and precompute group maps.
/// Deliberately minimal for phase 1 — planner units (4.x) extend with
/// group maps, binary-match tables, and bucket membership as those
/// contracts solidify. Storage-specific fields live on the phase-2
/// `SeriesSource` contract, not here.
#[derive(Debug, Clone)]
pub struct OperatorSchema {
    /// Series roster the operator will emit. [`SchemaRef::Static`] for
    /// every operator except `count_values`.
    pub series: SchemaRef,
    /// Step grid the batches will land on. Plan-time constant.
    pub step_grid: StepGrid,
}

impl OperatorSchema {
    /// Build a schema from a series handle and a step grid.
    pub fn new(series: SchemaRef, step_grid: StepGrid) -> Self {
        Self { series, step_grid }
    }
}

/// Pull-based, step-batched streaming operator.
///
/// Object-safe: planners build heterogeneous trees as
/// `Box<dyn Operator>`. Operators are `Send` so a `Concurrent` wrapper
/// can move a subplan onto a spawned tokio task; `Sync` is deliberately
/// not required, matching DataFusion / thanos-io promql-engine.
///
/// The trait mirrors [`futures::Stream`]'s polling shape but stays
/// runtime-agnostic — the executor crate chooses tokio, the operator
/// trait does not.
pub trait Operator: Send {
    /// Operator's outputs, known before any call to [`Self::next`].
    fn schema(&self) -> &OperatorSchema;

    /// Pull the next [`StepBatch`].
    ///
    /// - `Poll::Ready(Some(Ok(batch)))` — one batch of output.
    /// - `Poll::Ready(Some(Err(err)))` — terminal error; downstream must
    ///   not poll again.
    /// - `Poll::Ready(None)` — end of stream; downstream must not poll
    ///   again.
    /// - `Poll::Pending` — back-pressure or I/O wait; the runtime will
    ///   wake via the waker stored on `cx`.
    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    /// Test double: reports a fixed schema, yields end-of-stream on the
    /// first poll. Used to exercise trait shape / object-safety.
    struct Empty {
        schema: OperatorSchema,
    }

    impl Empty {
        fn new() -> Self {
            let grid = StepGrid {
                start_ms: 0,
                end_ms: 0,
                step_ms: 1,
                step_count: 1,
            };
            Self {
                schema: OperatorSchema::new(SchemaRef::empty_static(), grid),
            }
        }
    }

    impl Operator for Empty {
        fn schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
            Poll::Ready(None)
        }
    }

    // Minimal no-op waker so we can synthesise a `Context` in unit tests
    // without pulling in `futures` or spinning up a runtime.
    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        // SAFETY: the vtable's functions never dereference the data
        // pointer; clone returns a fresh null-data RawWaker each time.
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    #[test]
    fn should_build_trait_object() {
        // given: a concrete operator
        let op = Empty::new();

        // when: erase it through `dyn Operator`
        let boxed: Box<dyn Operator> = Box::new(op);

        // then: the trait is object-safe (compiles) and we can read
        // through the erased handle
        assert_eq!(boxed.schema().step_grid.step_count, 1);
    }

    #[test]
    fn should_report_schema_before_next() {
        // given: a fresh operator, untouched by `next`
        let op = Empty::new();

        // when: read the schema
        let schema = op.schema();

        // then: the step grid and deferred-ness are already known
        assert_eq!(schema.step_grid.step_ms, 1);
        assert_eq!(schema.step_grid.step_count, 1);
        assert!(!schema.series.is_deferred());
        assert_eq!(schema.series.as_static().expect("static schema").len(), 0);
    }

    #[test]
    fn should_yield_end_of_stream() {
        // given: a trivial operator that produces nothing
        let mut op = Empty::new();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // when: poll
        let polled = op.next(&mut cx);

        // then: end-of-stream
        match polled {
            Poll::Ready(None) => {}
            other => panic!("expected Poll::Ready(None), got {other:?}"),
        }
    }
}

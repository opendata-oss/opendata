//! The [`Operator`] trait that every node in the query plan implements,
//! plus the two pieces of plan-time metadata every operator carries:
//! [`StepGrid`] (the `(start, end, step)` timeline batches land on) and
//! [`OperatorSchema`] (series roster + step grid — everything a consumer
//! needs before the first batch arrives).
//!
//! The trait is pull-based: consumers call `next` and drive work forward one
//! batch at a time. Its `Poll<Option<Result<_>>>` shape mirrors
//! [`futures::Stream`] but the trait stays runtime-agnostic — no `async fn`,
//! no pinning requirement — so planners can compose heterogeneous operators
//! as `Box<dyn Operator>`.
//!
//! Operator invariants:
//! - [`Operator::schema`] is callable before any [`Operator::next`], so
//!   downstream operators can size buffers and precompute maps up front.
//! - [`Operator::next`] returning `Poll::Ready(None)` is terminal; callers
//!   must not poll again.

use std::task::{Context, Poll};

use super::batch::{SchemaRef, StepBatch};
use super::memory::QueryError;

/// The timeline of evaluation points a query runs over, resolved at plan
/// time from the request's `(start, end, step)`. Every batch an operator
/// produces lands on this same grid (batches carry a `step_range` slice of
/// it). Instant queries collapse to `step_count == 1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepGrid {
    /// Inclusive, milliseconds.
    pub start_ms: i64,
    /// Inclusive, milliseconds.
    pub end_ms: i64,
    /// Spacing between consecutive steps (ms). Must be `> 0`.
    pub step_ms: i64,
    pub step_count: usize,
}

/// What an operator publishes about its output before producing any
/// batches. Carries the (possibly deferred) series schema and the step
/// grid, so downstream operators can size buffers, build group maps, and
/// pre-allocate accumulator grids without first pulling a sample.
#[derive(Debug, Clone)]
pub struct OperatorSchema {
    pub series: SchemaRef,
    pub step_grid: StepGrid,
}

impl OperatorSchema {
    pub fn new(series: SchemaRef, step_grid: StepGrid) -> Self {
        Self { series, step_grid }
    }
}

/// The one trait every query-plan node implements. Consumers call
/// [`Self::next`] to pull the next [`StepBatch`], or [`Self::schema`] to
/// learn the output shape before polling.
///
/// Object-safe on purpose — the planner builds heterogeneous operator
/// trees as `Box<dyn Operator>` without monomorphising over every
/// combination. `Send` is required so
/// [`ConcurrentOp`](super::operators::concurrent::ConcurrentOp) can move a
/// subplan onto a spawned task; `Sync` is deliberately not required,
/// because operators hold per-poll mutable state.
pub trait Operator: Send {
    fn schema(&self) -> &OperatorSchema;

    /// - `Ready(Some(Ok))` — one batch.
    /// - `Ready(Some(Err))` / `Ready(None)` — terminal; must not poll again.
    /// - `Pending` — back-pressure or I/O wait; runtime wakes via `cx`.
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

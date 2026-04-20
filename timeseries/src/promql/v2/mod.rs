//! PromQL execution engine. A columnar, step-major, pull-based pipeline:
//! each operator yields [`StepBatch`]es (rectangles of `series × step`
//! values) when polled, and the query result is the root operator's final
//! batch reshaped onto the HTTP wire format. See
//! `timeseries/rfcs/0007-promql-execution.md` for the full design.
//!
//! How the code is laid out:
//!
//! - [`batch`], [`operator`]: the wire shape ([`StepBatch`]) and the
//!   pull-based operator trait every node in the tree implements.
//! - [`memory`]: per-query [`MemoryReservation`] — every allocating operator
//!   routes through `try_grow` so memory limits are enforced centrally.
//! - [`source`], [`source_adapter`]: the storage-facing trait
//!   ([`SeriesSource`]) and the in-repo adapter over the per-bucket
//!   `QueryReader`.
//! - [`operators`]: concrete operator implementations (selectors, functions,
//!   aggregations, binary ops, subqueries, plumbing).
//! - [`plan`]: PromQL `Expr` → [`LogicalPlan`] lowering, rule-based
//!   optimisation, and physical-plan construction (series resolution, group
//!   maps, match tables).
//! - [`reshape`]: converts the stream of collected [`StepBatch`]es back to
//!   the [`QueryValue`](crate::model::QueryValue) shape the HTTP layer emits.
//! - [`trace`], [`index_cache`]: per-query tracing collector and
//!   inverted/forward-index dedup cache (shared across selectors).

pub(crate) mod batch;
pub(crate) mod index_cache;
pub(crate) mod memory;
pub(crate) mod operator;
pub(crate) mod operators;
pub(crate) mod plan;
pub(crate) mod reshape;
pub(crate) mod source;
pub(crate) mod source_adapter;
pub(crate) mod trace;

#[allow(unused_imports)]
pub(crate) use batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
#[allow(unused_imports)]
pub(crate) use memory::{MemoryReservation, QueryError};
#[allow(unused_imports)]
pub(crate) use operator::{Operator, OperatorSchema, StepGrid};
#[allow(unused_imports)]
pub(crate) use operators::aggregate::{AggregateKind, AggregateOp, GroupMap};
#[allow(unused_imports)]
pub(crate) use operators::binary::{
    BinaryOp, BinaryOpKind, BinaryShape, ConstScalarOp, MatchTable,
};
#[allow(unused_imports)]
pub(crate) use operators::coalesce::CoalesceOp;
#[allow(unused_imports)]
pub(crate) use operators::coercion::{ScalarizeOp, TimeScalarOp};
#[allow(unused_imports)]
pub(crate) use operators::concurrent::{ConcurrentOp, DEFAULT_CHANNEL_BOUND};
#[allow(unused_imports)]
pub(crate) use operators::count_values::CountValuesOp;
#[allow(unused_imports)]
pub(crate) use operators::instant_fn::{InstantFnKind, InstantFnOp};
#[allow(unused_imports)]
pub(crate) use operators::label_manip::{LabelManipKind, LabelManipOp};
#[allow(unused_imports)]
pub(crate) use operators::matrix_selector::{CellIndex, MatrixSelectorOp, MatrixWindowBatch};
#[allow(unused_imports)]
pub(crate) use operators::rechunk::RechunkOp;
#[allow(unused_imports)]
pub(crate) use operators::rollup::{MatrixWindowSource, RollupKind, RollupOp, WindowStream};
#[allow(unused_imports)]
pub(crate) use operators::subquery::{ChildFactory, SubqueryOp};
#[allow(unused_imports)]
pub(crate) use operators::vector_selector::VectorSelectorOp;
#[allow(unused_imports)]
pub(crate) use plan::{
    AggregateGrouping, AtModifier, BinaryMatching, Cardinality, InstantVectorSort, LogicalPlan,
    LoweringContext, MatchingAxis, Offset, PhysicalPlan, PlanError, build_physical_plan, lower,
};
#[allow(unused_imports)]
pub(crate) use source::{
    ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock, SamplesRequest, SeriesSource,
    TimeRange,
};
#[allow(unused_imports)]
pub(crate) use source_adapter::QueryReaderSource;

//! PromQL v2 execution engine.
//!
//! Columnar, step-major, pull-based operator pipeline. Replaces the row-oriented AST
//! interpreter in [`crate::promql::evaluator`] and [`crate::promql::pipeline`]. Gated
//! behind the `promql-v2` Cargo feature so the baseline build is untouched while the
//! new engine is under construction.
//!
//! See `timeseries/rfcs/0007-promql-execution.md` for the full design and
//! `timeseries/rfcs/0007-impl-plan.md` for how the module is being built out.
//!
//! Module layout:
//! - [`batch`]: `StepBatch`, `SeriesSchema`, `SchemaRef` (unit 1.3).
//! - [`memory`]: `MemoryReservation` and memory-limit error variants (unit 1.4).
//! - [`operator`]: the `Operator` trait (unit 1.5).
//! - [`source`]: the `SeriesSource` storage contract (unit 2.1).
//! - [`source_adapter`]: adapter over the existing `QueryReader` (unit 2.2).

pub(crate) mod batch;
pub(crate) mod memory;
pub(crate) mod operator;
pub(crate) mod operators;
pub(crate) mod plan;
pub(crate) mod reshape;
pub(crate) mod source;
pub(crate) mod source_adapter;

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
    CardinalityEstimate, ResolvedSeriesChunk, ResolvedSeriesRef, SampleBatch, SampleBlock,
    SampleHint, SeriesSource, TimeRange,
};
#[allow(unused_imports)]
pub(crate) use source_adapter::QueryReaderSource;

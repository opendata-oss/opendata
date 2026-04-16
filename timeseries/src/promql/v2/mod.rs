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

pub(crate) mod batch;
pub(crate) mod memory;
pub(crate) mod operator;

#[allow(unused_imports)]
pub(crate) use batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
#[allow(unused_imports)]
pub(crate) use memory::{MemoryReservation, QueryError};
#[allow(unused_imports)]
pub(crate) use operator::{Operator, OperatorSchema, StepGrid};

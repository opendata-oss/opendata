//! Concrete [`Operator`](super::operator::Operator) implementations for the
//! v2 execution engine.
//!
//! The submodule layout mirrors RFC 0007 §"Operator Taxonomy" — each phase
//! 3 unit drops one or more files in here:
//!
//! - [`vector_selector`] (unit 3a.1): leaf. Lookback, `@`, `offset`.
//! - `matrix_selector` (unit 3a.2): leaf. Sliding window.
//! - `rollup` (unit 3b.1): unified range-function driver.
//! - `instant_fn` (unit 3b.2): pointwise scalar functions.
//! - `binary` (unit 3b.3): vector/vector and vector/scalar binops.
//! - `aggregate` (units 3b.4 / 3c.1): streaming and pipeline-breaker aggregates.
//! - `subquery` (unit 3c.2): re-grids child onto inner step.
//! - `rechunk` (unit 3c.3): transposes series-major ↔ step-major.
//! - `count_values` (unit 3c.4): deferred-schema operator.
//! - `concurrent` / `coalesce` (unit 3c.5): exchange operators.
//!
//! Operator structs are re-exported from [`super`] via the prelude below so
//! callers can write `use crate::promql::v2::operators::prelude::*;`.

pub(crate) mod aggregate;
pub(crate) mod binary;
pub(crate) mod instant_fn;
pub(crate) mod matrix_selector;
pub(crate) mod rollup;
pub(crate) mod vector_selector;

/// Public operator surface. Re-exports every concrete operator struct
/// implemented so far, so downstream (planner, wiring) can pull them in
/// with one `use`.
#[allow(unused_imports)]
pub(crate) mod prelude {
    pub(crate) use super::aggregate::{AggregateKind, AggregateOp, GroupMap};
    pub(crate) use super::binary::{
        BinaryOp, BinaryOpKind, BinaryShape, ConstScalarOp, MatchTable,
    };
    pub(crate) use super::instant_fn::{InstantFnKind, InstantFnOp};
    pub(crate) use super::matrix_selector::{CellIndex, MatrixSelectorOp, MatrixWindowBatch};
    pub(crate) use super::rollup::{MatrixWindowSource, RollupKind, RollupOp, WindowStream};
    pub(crate) use super::vector_selector::VectorSelectorOp;
}

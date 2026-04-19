//! Every concrete [`Operator`](super::operator::Operator) implementation
//! that can appear in a v2 query plan. The planner composes these into a
//! tree; at runtime, the root operator is polled and each node pulls from
//! its children.
//!
//! Grouped by role:
//!
//! - Storage leaves (pull samples from a [`SeriesSource`]):
//!   [`vector_selector`] for instant-vector leaves, [`matrix_selector`]
//!   for bracketed range windows.
//! - Per-cell transforms (one output value per input cell, schema
//!   preserved): [`instant_fn`] (math functions like `abs`, `ln`),
//!   [`coercion`] (`scalar()`, `time()`), [`label_manip`]
//!   (`label_replace`, `label_join`).
//! - Range-function driver: [`rollup`] — reduces each bracketed window to
//!   one scalar for `rate`, `*_over_time`, and friends.
//! - Binary ops and match-time broadcasting: [`binary`] — pointwise
//!   arithmetic, comparisons, and set operations.
//! - Grouping aggregates: [`aggregate`] (`sum by (…)`, `topk`, …),
//!   [`count_values`] (deferred-schema — the one operator whose output
//!   series depend on sample values).
//! - Shape and plumbing (no semantic change, just data movement):
//!   [`subquery`], [`rechunk`], [`concurrent`], [`coalesce`].
//!
//! [`SeriesSource`]: super::source::SeriesSource

pub(crate) mod aggregate;
pub(crate) mod binary;
pub(crate) mod coalesce;
pub(crate) mod coercion;
pub(crate) mod concurrent;
pub(crate) mod count_values;
pub(crate) mod instant_fn;
pub(crate) mod label_manip;
pub(crate) mod matrix_selector;
pub(crate) mod rechunk;
pub(crate) mod rollup;
pub(crate) mod subquery;
pub(crate) mod vector_selector;

#[allow(unused_imports)]
pub(crate) mod prelude {
    pub(crate) use super::aggregate::{AggregateKind, AggregateOp, GroupMap};
    pub(crate) use super::binary::{
        BinaryOp, BinaryOpKind, BinaryShape, ConstScalarOp, MatchTable,
    };
    pub(crate) use super::coalesce::CoalesceOp;
    pub(crate) use super::coercion::{ScalarizeOp, TimeScalarOp};
    pub(crate) use super::concurrent::{ConcurrentOp, DEFAULT_CHANNEL_BOUND};
    pub(crate) use super::count_values::CountValuesOp;
    pub(crate) use super::instant_fn::{InstantFnKind, InstantFnOp};
    pub(crate) use super::label_manip::{LabelManipKind, LabelManipOp};
    pub(crate) use super::matrix_selector::{CellIndex, MatrixSelectorOp, MatrixWindowBatch};
    pub(crate) use super::rechunk::RechunkOp;
    pub(crate) use super::rollup::{MatrixWindowSource, RollupKind, RollupOp, WindowStream};
    pub(crate) use super::subquery::{ChildFactory, SubqueryOp};
    pub(crate) use super::vector_selector::VectorSelectorOp;
}

//! Plan phase of the v2 PromQL engine (units 4.1–4.5).
//!
//! This module owns the **logical plan IR** and the AST→IR lowering path. See
//! `timeseries/rfcs/0007-promql-execution.md` §"Architecture Overview" for the
//! stage diagram; this crate slice is everything between *Parse* and *Physical
//! Plan*.
//!
//! Submodules:
//! - [`plan_types`]: the [`LogicalPlan`] enum and supporting plan-time types
//!   ([`Offset`], [`AtModifier`], [`BinaryMatching`], [`AggregateGrouping`]).
//! - [`error`]: [`PlanError`], the structured plan-time error.
//! - [`lowering`]: the `promql_parser::Expr` → [`LogicalPlan`] translation plus
//!   its plan-time [`LoweringContext`].

pub mod cardinality;
pub mod error;
pub mod lowering;
pub mod optimize;
pub mod parallelism;
pub mod physical;
pub mod plan_types;

#[allow(unused_imports)]
pub use cardinality::{CardinalityLimits, enforce_cardinality_gate};
pub use error::PlanError;
pub use lowering::{LoweringContext, lower};
#[allow(unused_imports)]
pub use optimize::optimize;
#[allow(unused_imports)]
pub use parallelism::{ExchangeStats, Parallelism};
#[allow(unused_imports)]
pub use physical::{PhysicalPlan, build_physical_plan, build_physical_plan_with_stats};
pub use plan_types::{
    AggregateGrouping, AtModifier, BinaryMatching, Cardinality, LogicalPlan, MatchingAxis, Offset,
};

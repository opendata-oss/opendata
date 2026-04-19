//! Everything between *Parse* and a runnable operator tree. The plan
//! phase turns `promql_parser::Expr` into a tree of `Box<dyn Operator>`
//! ready to poll.
//!
//! The pipeline has two stages with a clear boundary:
//!
//! ```text
//!   promql_parser::Expr
//!       │  lower(..) — pure AST → LogicalPlan lowering, literal folding
//!       ▼
//!   LogicalPlan                   ←─ single rewrite surface
//!       │  optimize(..) — rule-based rewrites
//!       ▼
//!   LogicalPlan (optimised)
//!       │  build_physical_plan(..) — resolves series, compiles group
//!       │                            maps / match tables, wires operators
//!       ▼
//!   PhysicalPlan (dyn Operator tree, opaque)
//! ```
//!
//! [`LogicalPlan`] is the only rewrite surface the optimiser and EXPLAIN
//! touch; the physical tree is compiled and opaque.
//!
//! - [`plan_types`], [`error`]: [`LogicalPlan`] and plan-time errors.
//! - [`lowering`]: `Expr` → [`LogicalPlan`] under a caller-supplied
//!   [`LoweringContext`] (query grid, lookback, etc.).
//! - [`optimize`]: rule-based rewrites (constant folding, matcher dedup).
//! - [`parallelism`]: policy for where to insert exchange operators
//!   (`ConcurrentOp`).
//! - [`physical`]: resolves series via [`SeriesSource`], builds group
//!   maps and match tables from resolved schemas, and wires concrete
//!   operators.
//! - [`explain`]: JSON-serialisable plan tree for `/explain` endpoints.
//!
//! [`SeriesSource`]: super::source::SeriesSource

pub mod error;
pub mod explain;
pub mod lowering;
pub mod optimize;
pub mod parallelism;
pub mod physical;
pub mod plan_types;

pub use error::PlanError;
#[allow(unused_imports)]
pub use explain::{
    ExplainResult, PlanNode, SCHEMA_VERSION, describe_logical, describe_physical, pretty_print,
};
pub use lowering::{LoweringContext, lower};
#[allow(unused_imports)]
pub use optimize::optimize;
#[allow(unused_imports)]
pub use parallelism::{ExchangeStats, Parallelism};
#[allow(unused_imports)]
pub use physical::{
    InstantVectorSort, PhysicalPlan, build_physical_plan, build_physical_plan_with_stats,
};
pub use plan_types::{
    AggregateGrouping, AtModifier, BinaryMatching, Cardinality, LogicalPlan, MatchingAxis, Offset,
};

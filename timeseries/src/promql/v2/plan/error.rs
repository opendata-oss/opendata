//! Plan-time errors (unit 4.1).
//!
//! `PlanError` is deliberately separate from
//! [`crate::promql::v2::memory::QueryError`] (the execution-time error) — the
//! two have distinct lifecycles. A `PlanError` is produced before the executor
//! is ever invoked (during lowering / validation / physical binding);
//! `QueryError` is produced while streaming `StepBatch`es. Phase 5 wiring
//! translates `PlanError` into the API surface's `QueryError` shape.
//!
//! See RFC 0007 §"Error Handling & Observability" and the impl-plan §4.1
//! scope.
use thiserror::Error;

/// Errors raised while lowering `promql_parser::Expr` into a [`LogicalPlan`]
/// or while performing downstream plan-time validation.
///
/// Keep variants structured (not free-form `String`) so phase 5 wiring can
/// map each case to the right HTTP status without re-parsing error strings.
///
/// [`LogicalPlan`]: super::plan_types::LogicalPlan
#[derive(Debug, Clone, Error, PartialEq)]
pub enum PlanError {
    /// A PromQL function that we do not yet lower (or do not recognise).
    /// Distinct from `InvalidArgument` so the wiring layer can flag these as
    /// "engine gap" rather than user error.
    #[error("unknown or unsupported PromQL function: {0}")]
    UnknownFunction(String),

    /// A function was called with an argument of the wrong shape (e.g.
    /// `rate` on an instant vector instead of a matrix, or `clamp` with a
    /// non-literal bound that we cannot fold at plan time).
    #[error("invalid argument for `{function}`: expected {expected}, got {got}")]
    InvalidArgument {
        function: String,
        expected: String,
        got: String,
    },

    /// A top-level construct the engine does not support (extension nodes,
    /// matrix selectors outside a rollup context, etc.).
    #[error("unsupported PromQL expression: {0}")]
    UnsupportedExpression(String),

    /// A `StringLiteral` appeared outside of a function argument position.
    /// Surfaced as a dedicated variant because the distinction matters for
    /// the HTTP surface — a user typed a bare string somewhere.
    #[error("string literals are only valid as function arguments")]
    InvalidTopLevelString,

    /// The AST carried a feature we explicitly do not plan to support (e.g.
    /// `Expr::Extension`). Retained so the `PlanError` surface is total.
    #[error("PromQL feature not supported by v2 engine: {0}")]
    UnsupportedFeature(String),

    /// [`SeriesSource::resolve`](super::super::source::SeriesSource::resolve)
    /// failed while the physical planner was materialising a leaf selector's
    /// series roster. Carries the storage-side error rendered as a string so
    /// the wiring layer can surface it to the HTTP response without pulling
    /// the full `QueryError` enum through here.
    #[error("series source error: {0}")]
    SourceError(String),

    /// Physical-plan binding could not be completed — e.g. a label-matching
    /// clause was malformed relative to the resolved input schemas, or a
    /// plan shape the v2 engine rejects (like a schema-deferred child under
    /// a parent that needs a `Static` schema) was requested.
    #[error("invalid vector matching: {0}")]
    InvalidMatching(String),

    /// Physical planning exceeded the per-query memory reservation while
    /// building a plan-time artifact (group map, match table, resolved
    /// series roster). Carries the storage-side memory-limit diagnostic
    /// rendered as a string so the `PlanError` surface stays
    /// `QueryError`-free.
    #[error("plan-time memory limit exceeded: {0}")]
    MemoryLimit(String),

    /// Generic physical-plan binding failure: anything that doesn't fit the
    /// more-specific variants above. Kept last so future failures can slot
    /// in without adding a new variant per case.
    #[error("physical plan binding failed: {0}")]
    PhysicalPlanFailed(String),
}

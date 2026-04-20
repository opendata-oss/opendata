//! Errors that can happen before a single batch is polled — lowering
//! failures, unsupported PromQL constructs, and physical-planner
//! binding errors. Kept separate from the execution-time
//! [`crate::promql::memory::QueryError`] and structured (not
//! `String`) so the HTTP layer can map each variant to a status code
//! without re-parsing the message.
use thiserror::Error;

/// Everything that can go wrong during lowering, optimisation, or
/// physical planning. Mapped onto an HTTP status at the wire boundary.
#[derive(Debug, Clone, Error, PartialEq)]
pub enum PlanError {
    /// Distinct from [`Self::InvalidArgument`] so the wiring layer can flag
    /// these as "engine gap" rather than user error.
    #[error("unknown or unsupported PromQL function: {0}")]
    UnknownFunction(String),

    #[error("invalid argument for `{function}`: expected {expected}, got {got}")]
    InvalidArgument {
        function: String,
        expected: String,
        got: String,
    },

    /// Matrix selectors outside a rollup, extension nodes, etc.
    #[error("unsupported PromQL expression: {0}")]
    UnsupportedExpression(String),

    #[error("string literals are only valid as function arguments")]
    InvalidTopLevelString,

    /// Features we won't support (e.g. `Expr::Extension`).
    #[error("PromQL feature not supported by engine: {0}")]
    UnsupportedFeature(String),

    /// Storage error from [`SeriesSource::resolve`](super::super::source::SeriesSource::resolve),
    /// rendered as a string to keep `PlanError` `QueryError`-free.
    #[error("series source error: {0}")]
    SourceError(String),

    #[error("invalid vector matching: {0}")]
    InvalidMatching(String),

    /// Memory-limit diagnostic stringified.
    #[error("plan-time memory limit exceeded: {0}")]
    MemoryLimit(String),

    /// Catch-all for binding failures that don't fit the specific variants above.
    #[error("physical plan binding failed: {0}")]
    PhysicalPlanFailed(String),
}

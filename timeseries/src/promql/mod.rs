//! PromQL execution engine. A columnar, step-major, pull-based pipeline:
//! each operator yields [`StepBatch`](batch::StepBatch)es (rectangles of
//! `series × step` values) when polled, and the query result is the root
//! operator's final batch reshaped onto the HTTP wire format. See
//! `timeseries/rfcs/0007-promql-execution.md` for the full design.
//!
//! How the code is laid out:
//!
//! - [`batch`], [`operator`]: the wire shape ([`StepBatch`](batch::StepBatch))
//!   and the pull-based operator trait every node in the tree implements.
//! - [`memory`]: per-query [`MemoryReservation`](memory::MemoryReservation) —
//!   every allocating operator routes through `try_grow` so memory limits
//!   are enforced centrally.
//! - [`source`], [`source_adapter`]: the storage-facing trait
//!   ([`SeriesSource`](source::SeriesSource)) and the in-repo adapter over
//!   the per-bucket `QueryReader`.
//! - [`operators`]: concrete operator implementations (selectors, functions,
//!   aggregations, binary ops, subqueries, plumbing).
//! - [`plan`]: PromQL `Expr` → [`LogicalPlan`](plan::plan_types::LogicalPlan) lowering,
//!   rule-based optimisation, and physical-plan construction (series
//!   resolution, group maps, match tables).
//! - [`reshape`]: converts the stream of collected
//!   [`StepBatch`](batch::StepBatch)es back to the
//!   [`QueryValue`](crate::model::QueryValue) shape the HTTP layer emits.
//! - [`trace`], [`index_cache`]: per-query tracing collector and
//!   inverted/forward-index dedup cache (shared across selectors).

pub(crate) mod batch;
pub(crate) mod config;
pub(crate) mod index_cache;
pub(crate) mod memory;
pub(crate) mod openmetrics;
pub(crate) mod operator;
pub(crate) mod operators;
pub(crate) mod plan;
#[cfg(test)]
pub(crate) mod promqltest;
pub(crate) mod request;
pub(crate) mod reshape;
pub(crate) mod response;
#[cfg(feature = "http-server")]
pub(crate) mod scraper;
pub(crate) mod source;
pub(crate) mod source_adapter;
mod timestamp;
pub(crate) mod trace;

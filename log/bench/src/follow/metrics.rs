//! The metric handles shared across the follower runners and the arrivals writer.
//!
//! Metric handles from the `metrics` crate are cheap `Arc`-backed clones, so a
//! `FollowMetrics` is cloned freely into every spawned task.
//!
//! Latency is recorded two ways and **bucketed by lag at poll time** (RFC 0006),
//! carrying the lag bucket as a metric label so the per-bucket series share one
//! name and the reporter's quantiles come out per bucket:
//! - `poll_latency_us` — from when a poll became due to completion (includes any
//!   time the runner was busy with other keys; coordinated-omission-correct).
//! - `poll_service_us` — from when the poll started executing to completion (the
//!   database's intrinsic cost).
//!
//! `scheduling_lag_us` (due-to-start delay) is the saturation signal: as runners
//! fall behind their keys' due times it grows, alongside the lag distribution
//! shifting into deeper buckets.

use bencher::{Bench, Counter, Histogram};

use super::lag::{LAG_BUCKET_LABELS, NUM_LAG_BUCKETS};

#[derive(Clone)]
pub struct FollowMetrics {
    /// Polls completed.
    pub polls: Counter,
    /// Records returned to consumers.
    pub records_consumed: Counter,
    /// Records appended by the arrivals writer.
    pub arrivals: Counter,
    /// Due-to-completion latency, microseconds, per lag bucket
    /// (coordinated-omission-correct).
    pub poll_latency_us: Vec<Histogram>,
    /// Service-start-to-completion latency, microseconds, per lag bucket
    /// (intrinsic db cost).
    pub poll_service_us: Vec<Histogram>,
    /// Due-to-service-start delay, microseconds (runner falling behind).
    pub scheduling_lag_us: Histogram,
}

impl FollowMetrics {
    pub fn new(bench: &Bench) -> Self {
        let per_bucket = |name: &'static str| -> Vec<Histogram> {
            LAG_BUCKET_LABELS
                .iter()
                .map(|bucket| bench.histogram_labeled(name, &[("lag", *bucket)]))
                .collect()
        };
        let metrics = Self {
            polls: bench.counter("polls"),
            records_consumed: bench.counter("records_consumed"),
            arrivals: bench.counter("arrivals"),
            poll_latency_us: per_bucket("poll_latency_us"),
            poll_service_us: per_bucket("poll_service_us"),
            scheduling_lag_us: bench.histogram("scheduling_lag_us"),
        };
        debug_assert_eq!(metrics.poll_latency_us.len(), NUM_LAG_BUCKETS);
        metrics
    }
}

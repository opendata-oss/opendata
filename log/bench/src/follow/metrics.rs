//! The metric handles shared across the scheduler, workers, and writer.
//!
//! Metric handles from the `metrics` crate are cheap `Arc`-backed clones, so a
//! `FollowMetrics` is cloned freely into every spawned task.
//!
//! Latency is recorded two ways (RFC 0006 "Scheduling"):
//! - `poll_latency_us` is measured from the consumer's **scheduled due time** to
//!   completion. Because the schedule is open-loop, this counts any time a poll
//!   spent waiting for an execution slot — it is coordinated-omission-correct, so
//!   queueing under load shows up instead of being hidden.
//! - `poll_service_us` is measured from when the poll actually starts executing to
//!   completion — the database's intrinsic cost, with queueing excluded.
//!
//! `scheduling_lag_us` and `dispatch_backlog` are the saturation signals: as
//! offered load approaches capacity they diverge, marking the sustainable
//! throughput boundary.
//!
//! Poll latency and service time are **bucketed by lag at poll time** (RFC 0006):
//! the lag bucket is carried as a metric label, so the per-bucket series share one
//! metric name and the reporter's quantiles come out per bucket. `scheduling_lag`
//! and `dispatch_backlog` are queueing/saturation signals and stay unbucketed.

use bencher::{Bench, Counter, Gauge, Histogram};

use super::lag::{LAG_BUCKET_LABELS, NUM_LAG_BUCKETS};

#[derive(Clone)]
pub struct FollowMetrics {
    /// Polls completed.
    pub polls: Counter,
    /// Records returned to consumers.
    pub records_consumed: Counter,
    /// Records appended by the arrivals writer.
    pub arrivals: Counter,
    /// Polls skipped because the consumer's previous poll was still in flight
    /// (the consumer could not keep up with its own schedule).
    pub polls_coalesced: Counter,
    /// Polls dropped because the bounded dispatch queue was full (saturation).
    pub dispatch_dropped: Counter,
    /// Scheduled-due-to-completion latency, microseconds, per lag bucket
    /// (coordinated-omission-correct).
    pub poll_latency_us: Vec<Histogram>,
    /// Service-start-to-completion latency, microseconds, per lag bucket
    /// (intrinsic db cost).
    pub poll_service_us: Vec<Histogram>,
    /// Scheduled-due-to-service-start delay, microseconds (queueing under load).
    pub scheduling_lag_us: Histogram,
    /// Current depth of the bounded dispatch queue.
    pub dispatch_backlog: Gauge,
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
            polls_coalesced: bench.counter("polls_coalesced"),
            dispatch_dropped: bench.counter("dispatch_dropped"),
            poll_latency_us: per_bucket("poll_latency_us"),
            poll_service_us: per_bucket("poll_service_us"),
            scheduling_lag_us: bench.histogram("scheduling_lag_us"),
            dispatch_backlog: bench.gauge("dispatch_backlog"),
        };
        debug_assert_eq!(metrics.poll_latency_us.len(), NUM_LAG_BUCKETS);
        metrics
    }
}

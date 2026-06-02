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
//! Lag-bucketing of these metrics (carrying the lag bucket as a label) lands in
//! M3; for now they are recorded unbucketed.

use bencher::{Bench, Counter, Gauge, Histogram};

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
    /// Scheduled-due-to-completion latency, microseconds (coordinated-omission-correct).
    pub poll_latency_us: Histogram,
    /// Service-start-to-completion latency, microseconds (intrinsic db cost).
    pub poll_service_us: Histogram,
    /// Scheduled-due-to-service-start delay, microseconds (queueing under load).
    pub scheduling_lag_us: Histogram,
    /// Current depth of the bounded dispatch queue.
    pub dispatch_backlog: Gauge,
}

impl FollowMetrics {
    pub fn new(bench: &Bench) -> Self {
        Self {
            polls: bench.counter("polls"),
            records_consumed: bench.counter("records_consumed"),
            arrivals: bench.counter("arrivals"),
            polls_coalesced: bench.counter("polls_coalesced"),
            dispatch_dropped: bench.counter("dispatch_dropped"),
            poll_latency_us: bench.histogram("poll_latency_us"),
            poll_service_us: bench.histogram("poll_service_us"),
            scheduling_lag_us: bench.histogram("scheduling_lag_us"),
            dispatch_backlog: bench.gauge("dispatch_backlog"),
        }
    }
}

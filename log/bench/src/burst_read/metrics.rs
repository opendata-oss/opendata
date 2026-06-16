//! Lean metric handles for the burst read-capacity benchmark.
//!
//! Far smaller than the follow bench's set: this is a fixed, closed-loop pool of
//! sessions, so there is no session-arrival schedule and no Little's-law occupancy
//! to measure (occupancy *is* `active_sessions`). We keep only the per-poll cost
//! signals — poll count, records consumed, and per-poll service time bucketed by
//! lag at poll time (RFC 0006's read-cost axis). The lag bucket is a metric label
//! so the per-bucket series share one name.

use bencher::{Bench, Counter, Histogram};

use crate::read::lag::{LAG_BUCKET_LABELS, NUM_LAG_BUCKETS};

#[derive(Clone)]
pub struct ReadMetrics {
    /// Polls completed (across all sessions).
    pub polls: Counter,
    /// Records returned to consumers.
    pub records_consumed: Counter,
    /// Per-poll service time, microseconds, per lag bucket (intrinsic read cost).
    pub poll_service_us: Vec<Histogram>,
}

impl ReadMetrics {
    pub fn new(bench: &Bench) -> Self {
        let poll_service_us: Vec<Histogram> = LAG_BUCKET_LABELS
            .iter()
            .map(|bucket| bench.histogram_labeled("poll_service_us", &[("lag", *bucket)]))
            .collect();
        let metrics = Self {
            polls: bench.counter("polls"),
            records_consumed: bench.counter("records_consumed"),
            poll_service_us,
        };
        debug_assert_eq!(metrics.poll_service_us.len(), NUM_LAG_BUCKETS);
        metrics
    }
}

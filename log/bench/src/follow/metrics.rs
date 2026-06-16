//! The metric handles shared across the session runners and the arrivals writer.
//!
//! Metric handles from the `metrics` crate are cheap `Arc`-backed clones, so a
//! `FollowMetrics` is cloned freely into every spawned task.
//!
//! The follow workload is a population of **sessions**: a session wakes for one
//! key, drains its backlog to the tail closed-loop (fetch → process → fetch), and
//! ends. Latency has two levels:
//!
//! - `poll_service_us` — per-poll service time (start → completion), the
//!   database's intrinsic read cost, **bucketed by lag at poll time** (RFC 0006):
//!   how per-poll cost depends on how far behind the reader was. The lag bucket is
//!   carried as a metric label so the per-bucket series share one name.
//! - `session_sched_lag_us` — a session's scheduled-arrival → execution-start delay
//!   (queueing in the bounded session pool). This is the hard-saturation backstop:
//!   it diverges when offered session load exceeds the pool ceiling.
//!
//! Session-level *progress* (per-session goodput, time-to-catch-up) is reported
//! through the console summary from sketches held in `FollowState`, since those
//! are bucketed by backlog-at-session-start rather than the per-poll lag label.

use bencher::{Bench, Counter, Histogram};

use crate::read::lag::{LAG_BUCKET_LABELS, NUM_LAG_BUCKETS};

#[derive(Clone)]
pub struct FollowMetrics {
    /// Polls completed (across all sessions).
    pub polls: Counter,
    /// Records returned to consumers.
    pub records_consumed: Counter,
    /// Records appended by the arrivals writer.
    pub arrivals: Counter,
    /// Sessions completed.
    pub sessions: Counter,
    /// Per-poll service time, microseconds, per lag bucket (intrinsic db cost).
    pub poll_service_us: Vec<Histogram>,
    /// Session scheduled-arrival → execution-start delay, microseconds (the
    /// bounded pool queueing; the hard-saturation signal).
    pub session_sched_lag_us: Histogram,
}

impl FollowMetrics {
    pub fn new(bench: &Bench) -> Self {
        let poll_service_us: Vec<Histogram> = LAG_BUCKET_LABELS
            .iter()
            .map(|bucket| bench.histogram_labeled("poll_service_us", &[("lag", *bucket)]))
            .collect();
        let metrics = Self {
            polls: bench.counter("polls"),
            records_consumed: bench.counter("records_consumed"),
            arrivals: bench.counter("arrivals"),
            sessions: bench.counter("sessions"),
            poll_service_us,
            session_sched_lag_us: bench.histogram("session_sched_lag_us"),
        };
        debug_assert_eq!(metrics.poll_service_us.len(), NUM_LAG_BUCKETS);
        metrics
    }
}

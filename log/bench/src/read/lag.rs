//! Per-key lag tracking.
//!
//! Lag is defined in terms of the offered load, not any backend's addressing
//! (RFC 0006): the benchmark generates the arrivals, so it knows how many records
//! it appended to each key and how many each consumer acknowledged, and lag is
//! their difference in records. It needs no time index and no deterministic
//! segment boundaries.
//!
//! Lag is a **workload condition, not a backend result.** A consumer's lag is set
//! by how it polls: the polling-frequency distribution (plus arrival rate and
//! offline behavior) defines a target lag distribution, and the read workload
//! approximates that distribution one poll at a time. Polling rarely just produces
//! large lag — that says nothing about the store.
//!
//! Its purpose is to be the **analysis axis**. At the moment a poll fires we read
//! the consumer's current lag and use it to *bucket* the metrics that do depend on
//! the store — poll latency and GETs/poll (RFC 0006 "Metrics"; bucketing lands in
//! M3). Reporting cost as a function of lag is what separates the workload
//! condition from the backend's response, and is what makes the caught-up-cheap vs.
//! deep-catch-up asymmetry visible. Aggregate lag itself is only ever a workload
//! characterization or a drain sanity-check, never a score.

use std::sync::atomic::{AtomicU64, Ordering};

/// Lag buckets, labeled in records. Reads are bucketed by lag at poll time
/// (RFC 0006): backend cost — poll latency and, later, GETs/poll — is reported as
/// a function of how far behind the consumer was. The bucket string is carried as
/// a metric label so the per-bucket series share one metric name.
pub const LAG_BUCKET_LABELS: [&str; 7] =
    ["0", "1-9", "10-99", "100-999", "1k-9k", "10k-99k", "100k+"];

/// Number of lag buckets.
pub const NUM_LAG_BUCKETS: usize = LAG_BUCKET_LABELS.len();

/// Map a lag (in records) to its bucket index in [`LAG_BUCKET_LABELS`].
pub fn lag_bucket(lag: u64) -> usize {
    match lag {
        0 => 0,
        1..=9 => 1,
        10..=99 => 2,
        100..=999 => 3,
        1_000..=9_999 => 4,
        10_000..=99_999 => 5,
        _ => 6,
    }
}

/// Tracks appended-minus-acknowledged record counts for a dense key population.
///
/// Keys are dense integer ids `0..cardinality`. Counters use relaxed ordering:
/// they are independent per-key tallies, and lag is only ever read as an
/// approximate, point-in-time quantity for bucketing — no cross-counter
/// invariant requires stronger ordering.
pub struct LagTracker {
    appended: Vec<AtomicU64>,
    acked: Vec<AtomicU64>,
}

impl LagTracker {
    /// Create a tracker for `cardinality` keys, all starting at zero lag.
    pub fn new(cardinality: usize) -> Self {
        Self {
            appended: (0..cardinality).map(|_| AtomicU64::new(0)).collect(),
            acked: (0..cardinality).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    /// Number of keys tracked.
    pub fn len(&self) -> usize {
        self.appended.len()
    }

    /// Record that `n` records arrived for key `id`.
    pub fn record_appended(&self, id: usize, n: u64) {
        self.appended[id].fetch_add(n, Ordering::Relaxed);
    }

    /// Record that a consumer acknowledged `n` records for key `id`.
    pub fn record_acked(&self, id: usize, n: u64) {
        self.acked[id].fetch_add(n, Ordering::Relaxed);
    }

    /// Current lag for key `id`, in records (saturating at zero).
    pub fn lag(&self, id: usize) -> u64 {
        let appended = self.appended[id].load(Ordering::Relaxed);
        let acked = self.acked[id].load(Ordering::Relaxed);
        appended.saturating_sub(acked)
    }

    /// Sum of lag across all keys, in records.
    pub fn total_lag(&self) -> u64 {
        (0..self.len()).map(|id| self.lag(id)).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_is_appended_minus_acked() {
        let t = LagTracker::new(2);
        assert_eq!(t.lag(0), 0);

        t.record_appended(0, 10);
        assert_eq!(t.lag(0), 10);

        t.record_acked(0, 4);
        assert_eq!(t.lag(0), 6);

        // Key 1 is independent.
        assert_eq!(t.lag(1), 0);
        t.record_appended(1, 3);
        assert_eq!(t.total_lag(), 6 + 3);
    }

    #[test]
    fn over_acknowledging_saturates_at_zero() {
        let t = LagTracker::new(1);
        t.record_appended(0, 2);
        t.record_acked(0, 5);
        assert_eq!(t.lag(0), 0);
        assert_eq!(t.total_lag(), 0);
    }

    #[test]
    fn lag_bucket_boundaries() {
        assert_eq!(lag_bucket(0), 0);
        assert_eq!(lag_bucket(1), 1);
        assert_eq!(lag_bucket(9), 1);
        assert_eq!(lag_bucket(10), 2);
        assert_eq!(lag_bucket(999), 3);
        assert_eq!(lag_bucket(1_000), 4);
        assert_eq!(lag_bucket(99_999), 5);
        assert_eq!(lag_bucket(100_000), 6);
        assert_eq!(lag_bucket(u64::MAX), NUM_LAG_BUCKETS - 1);
    }
}

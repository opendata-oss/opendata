//! Policy knobs for where the physical planner inserts "exchange"
//! operators — the pieces that introduce parallelism or merging into an
//! otherwise sequential operator tree.
//!
//! v1 ships one exchange operator: `Concurrent` (see
//! [`super::super::operators::concurrent::ConcurrentOp`]). `Coalesce`-based
//! vertical sharding (splitting a subplan across series and fanning back
//! in) is deferred; it requires per-series-independence above the leaf,
//! which isn't safe without end-to-end cross-validation.
//!
//! The `Concurrent` insertion rule is threshold-based: wrap selector
//! leaves whose resolved series count is ≥ `concurrent_threshold_series`.
//! Default 64 — high enough that unit-test-scale queries skip the
//! tokio-spawn overhead, low enough that real queries ship with
//! decoupled I/O. `0` wraps every leaf; `u64::MAX` disables wrapping
//! entirely.
//!
//! Wrapping happens in `super::physical::build_node` for
//! `VectorSelector` (wraps the op directly) and `Rollup` over
//! `MatrixSelector` (wraps the whole rollup, because
//! `MatrixSelectorOp::next` is degenerate). Other operators are CPU-bound
//! and never wrapped.

use crate::promql::operators::concurrent::DEFAULT_CHANNEL_BOUND;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parallelism {
    /// `0` ⇒ wrap every leaf; `u64::MAX` ⇒ disable wrapping.
    pub concurrent_threshold_series: u64,

    /// `0` (default) disables `Coalesce` insertion.
    pub coalesce_max_shards: usize,

    /// Channel bound for each inserted `ConcurrentOp`.
    pub channel_bound: usize,
}

impl Parallelism {
    pub const DEFAULT_CONCURRENT_THRESHOLD_SERIES: u64 = 64;

    pub fn new(
        concurrent_threshold_series: u64,
        coalesce_max_shards: usize,
        channel_bound: usize,
    ) -> Self {
        Self {
            concurrent_threshold_series,
            coalesce_max_shards,
            channel_bound,
        }
    }

    /// Inclusive threshold.
    #[inline]
    pub fn should_wrap_concurrent(&self, series_count: u64) -> bool {
        if self.concurrent_threshold_series == u64::MAX {
            return false;
        }
        series_count >= self.concurrent_threshold_series
    }

    /// Always `false` under [`Self::default`] (v1).
    #[inline]
    pub fn coalesce_enabled(&self) -> bool {
        self.coalesce_max_shards > 0
    }
}

impl Default for Parallelism {
    fn default() -> Self {
        Self {
            concurrent_threshold_series: Self::DEFAULT_CONCURRENT_THRESHOLD_SERIES,
            coalesce_max_shards: 0,
            channel_bound: DEFAULT_CHANNEL_BOUND,
        }
    }
}

// ---------------------------------------------------------------------------
// Exchange statistics — test-only observability
// ---------------------------------------------------------------------------

/// Counts of exchange insertions per `build_physical_plan`. Production
/// discards these; tests capture them via
/// [`super::physical::build_physical_plan_with_stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExchangeStats {
    pub concurrent_wrapped: usize,
    /// Leaves that were eligible but below the threshold.
    pub concurrent_skipped: usize,
    /// Always `0` in v1.
    pub coalesce_inserted: usize,
}

impl ExchangeStats {
    #[inline]
    pub(super) fn record_wrap(&mut self) {
        self.concurrent_wrapped = self.concurrent_wrapped.saturating_add(1);
    }

    #[inline]
    pub(super) fn record_skip(&mut self) {
        self.concurrent_skipped = self.concurrent_skipped.saturating_add(1);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_use_default_parallelism_when_not_specified() {
        // given: the default Parallelism
        let p = Parallelism::default();
        // then: threshold matches the documented default and coalesce is off
        assert_eq!(p.concurrent_threshold_series, 64);
        assert_eq!(p.coalesce_max_shards, 0);
        assert_eq!(p.channel_bound, DEFAULT_CHANNEL_BOUND);
        assert!(!p.coalesce_enabled());
    }

    #[test]
    fn should_wrap_when_series_count_meets_threshold() {
        // given: default threshold of 64
        let p = Parallelism::default();
        // when/then
        assert!(!p.should_wrap_concurrent(0));
        assert!(!p.should_wrap_concurrent(63));
        assert!(p.should_wrap_concurrent(64));
        assert!(p.should_wrap_concurrent(1_000));
    }

    #[test]
    fn should_wrap_every_leaf_when_threshold_is_zero() {
        // given: threshold = 0 (wrap every leaf)
        let p = Parallelism::new(0, 0, DEFAULT_CHANNEL_BOUND);
        // when/then
        assert!(p.should_wrap_concurrent(0));
        assert!(p.should_wrap_concurrent(1));
        assert!(p.should_wrap_concurrent(u64::MAX - 1));
    }

    #[test]
    fn should_disable_wrap_when_threshold_is_max() {
        // given: threshold = u64::MAX (wrap never)
        let p = Parallelism::new(u64::MAX, 0, DEFAULT_CHANNEL_BOUND);
        // when/then
        assert!(!p.should_wrap_concurrent(0));
        assert!(!p.should_wrap_concurrent(1_000_000));
        assert!(!p.should_wrap_concurrent(u64::MAX));
    }

    #[test]
    fn should_track_exchange_stats() {
        // given: a fresh stats struct
        let mut stats = ExchangeStats::default();
        // when: record some wraps and skips
        stats.record_wrap();
        stats.record_wrap();
        stats.record_skip();
        // then: counts tracked separately
        assert_eq!(stats.concurrent_wrapped, 2);
        assert_eq!(stats.concurrent_skipped, 1);
        assert_eq!(stats.coalesce_inserted, 0);
    }
}

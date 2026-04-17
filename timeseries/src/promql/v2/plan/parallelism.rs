//! Plan-time parallelism / exchange-operator insertion (unit 4.5).
//!
//! Per-query parallelism in v2 comes from two exchange operators
//! ([`super::super::operators::concurrent::ConcurrentOp`] and
//! [`super::super::operators::coalesce::CoalesceOp`] — both implemented in
//! unit 3c.5). This unit's job is to decide **where** the physical planner
//! inserts them.
//!
//! # v1 scope: Concurrent only, Coalesce deliberately deferred
//!
//! Per the unit brief's recommendation: this module configures and tracks
//! `Concurrent` insertion only. `Coalesce`-based vertical sharding is
//! skipped for v1 — §5 Decisions Log 4.5 captures the rationale, but the
//! short version is:
//!
//! - `Coalesce` requires a narrow safety envelope: the operator chain above
//!   the leaf must be per-series-independent. Inserting it before we have
//!   end-to-end cross-validation (Phases 5/6) risks shipping a parallelism
//!   split that silently drops or duplicates series.
//! - `Concurrent` alone delivers the RFC's "per-operator async streaming"
//!   goal: every I/O-heavy leaf runs on its own tokio task, decoupled from
//!   CPU-bound downstream evaluation. Phase 6 measurement can justify
//!   adding `Coalesce` later.
//!
//! # Insertion strategy (Concurrent)
//!
//! Threshold-based: wrap a selector leaf's output operator in
//! [`ConcurrentOp`](super::super::operators::concurrent::ConcurrentOp) when
//! the leaf resolves to **≥ `concurrent_threshold_series`** series. The
//! default threshold ([`Parallelism::DEFAULT_CONCURRENT_THRESHOLD_SERIES`])
//! is **64**, picked for:
//!
//! - Low enough that any non-trivial production query (typical selectors
//!   match thousands of series) ships with decoupled I/O.
//! - High enough that unit-test-scale selectors (a handful of series) skip
//!   the wrap — constructing a `ConcurrentOp` requires a live tokio runtime,
//!   and small queries don't benefit from the per-wrap task-spawn overhead.
//!
//! Setting the threshold to `0` forces every eligible leaf to be wrapped;
//! `u64::MAX` disables the wrap entirely. [`Parallelism::default`] uses
//! sensible defaults for both knobs.
//!
//! # Which leaves get wrapped
//!
//! Two call sites in [`super::physical::build_node`] query
//! [`Parallelism::should_wrap_concurrent`] after the physical leaf is
//! constructed:
//!
//! - `LogicalPlan::VectorSelector` → produces a
//!   [`VectorSelectorOp`](super::super::operators::vector_selector::VectorSelectorOp);
//!   the op is wrapped directly.
//! - `LogicalPlan::Rollup` over a `MatrixSelector` → the I/O leaf
//!   (`MatrixSelectorOp`) is consumed by [`RollupOp`](super::super::operators::rollup::RollupOp)
//!   via a `WindowStream` bridge. `MatrixSelectorOp::next` is degenerate
//!   (returns `Ready(None)` — see §3a.2 Decisions Log), so it cannot be
//!   wrapped in `ConcurrentOp` directly. Instead we wrap the **whole
//!   `RollupOp`** (rollup owns the I/O leaf; wrapping rollup + leaf as a
//!   single subplan is a superset of "wrap the I/O boundary" and preserves
//!   the async decoupling between sample pulls and downstream evaluation).
//!
//! Other operators (Binary, Aggregate, InstantFn, CountValues, Rechunk,
//! Subquery) do **not** get wrapped — they are CPU-bound consumers, and
//! the task brief explicitly scopes `Concurrent` to I/O-bound leaves.
//!
//! # Observability
//!
//! `build_physical_plan` returns a [`super::physical::PhysicalPlan`] — it
//! does not expose exchange stats on the public surface. Tests reach for
//! the internal [`ExchangeStats`] shape via the module-private
//! [`record_wrap`] / [`record_skip`] hooks; in production the stats are
//! discarded.

use crate::promql::v2::operators::concurrent::DEFAULT_CHANNEL_BOUND;

/// Plan-time configuration for exchange-operator insertion.
///
/// Copied freely through the plan pipeline (small `Copy` struct). Defaults
/// via [`Parallelism::default`] suit production workloads; unit tests
/// typically override `concurrent_threshold_series` explicitly to force or
/// suppress wraps on small fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parallelism {
    /// Wrap a selector leaf in `ConcurrentOp` when its resolved series
    /// count is **≥** this value. `0` → wrap every leaf (eager wrap);
    /// `u64::MAX` → never wrap (gate disabled).
    ///
    /// Default: [`Self::DEFAULT_CONCURRENT_THRESHOLD_SERIES`] (64).
    pub concurrent_threshold_series: u64,

    /// Maximum number of vertical-shard children a single selector is
    /// split into by [`CoalesceOp`](super::super::operators::coalesce::CoalesceOp).
    /// `0` (the default) disables `Coalesce` insertion entirely — v1 does
    /// not ship coalesce-based vertical sharding (see module docs).
    pub coalesce_max_shards: usize,

    /// Bounded mpsc capacity handed to each inserted `ConcurrentOp`. Small
    /// values cap peak memory buffered between producer and consumer; large
    /// values amortise wakeup cost. The operator's own default
    /// ([`DEFAULT_CHANNEL_BOUND`]) is used when this field equals that
    /// constant.
    pub channel_bound: usize,
}

impl Parallelism {
    /// Default "wrap leaves with ≥ 64 series" threshold. Picked to match
    /// the unit brief's guidance; tune per deployment once Phase 6 profiling
    /// provides data.
    pub const DEFAULT_CONCURRENT_THRESHOLD_SERIES: u64 = 64;

    /// Construct with explicit knobs.
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

    /// `true` when a leaf resolving to `series_count` series should be
    /// wrapped in a [`ConcurrentOp`](super::super::operators::concurrent::ConcurrentOp).
    ///
    /// The threshold is inclusive: `series_count == threshold` wraps.
    /// `u64::MAX` sentinel disables the wrap for any finite series count.
    #[inline]
    pub fn should_wrap_concurrent(&self, series_count: u64) -> bool {
        if self.concurrent_threshold_series == u64::MAX {
            return false;
        }
        series_count >= self.concurrent_threshold_series
    }

    /// `true` when `Coalesce`-based vertical sharding is enabled. v1 ships
    /// with this disabled (see module docs) so this always returns `false`
    /// under [`Self::default`].
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

/// Counts of exchange-operator insertions made during one
/// `build_physical_plan` invocation. Production code discards these; tests
/// capture them via the `ExchangeStats` returned by
/// [`super::physical::build_physical_plan_with_stats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExchangeStats {
    /// Count of selector leaves that were wrapped in `ConcurrentOp`.
    pub concurrent_wrapped: usize,
    /// Count of selector leaves that were eligible but skipped (below the
    /// series-count threshold). Useful for test assertions that validate
    /// "threshold was consulted" without asserting on the specific cardinal.
    pub concurrent_skipped: usize,
    /// Count of `CoalesceOp` insertions. Always `0` in v1.
    pub coalesce_inserted: usize,
}

impl ExchangeStats {
    /// Record that a leaf was wrapped.
    #[inline]
    pub(super) fn record_wrap(&mut self) {
        self.concurrent_wrapped = self.concurrent_wrapped.saturating_add(1);
    }

    /// Record that a leaf was eligible but skipped.
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

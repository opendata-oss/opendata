//! Background metadata warmer for keeping newest bucket metadata in cache.
//!
//! The warmer maintains a newest-first target set of buckets up to a
//! configured byte budget. It periodically refreshes the target set and
//! rewarms stale buckets whose cache entries may have been evicted by
//! compaction or LRU pressure.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::config::MetadataWarmConfig;
use crate::model::TimeBucket;

/// State of a single bucket in the warm target set.
#[derive(Debug, Clone)]
pub(crate) struct BucketWarmEntry {
    pub bucket: TimeBucket,
    pub forward_index_bytes: u64,
    pub inverted_index_bytes: u64,
    pub total_bytes: u64,
    pub last_warmed_at: Instant,
}

/// Why a bucket is being warmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WarmReason {
    Initial,
    NewBucket,
    Rewarm,
}

/// Action the warmer should take for each bucket in a cycle.
#[derive(Debug, Clone)]
pub(crate) enum BucketAction {
    /// Warm this bucket (new or stale).
    Warm { bucket: TimeBucket, reason: WarmReason },
    /// Skip — entry is fresh.
    Fresh { bucket: TimeBucket },
}

/// Summary of a cycle's planned actions.
#[derive(Debug, Default)]
pub(crate) struct CyclePlan {
    pub actions: Vec<BucketAction>,
    pub selected_count: usize,
    pub new_count: usize,
    pub rewarm_count: usize,
    pub fresh_count: usize,
    pub removed_count: usize,
}

/// Persistent warmer state across cycles.
#[derive(Debug)]
pub(crate) struct MetadataWarmState {
    pub target_bytes: u64,
    pub estimated_target_bytes: u64,
    pub entries: BTreeMap<TimeBucket, BucketWarmEntry>,
    pub total_cycles: u64,
    pub total_bucket_warms: u64,
    pub total_bytes_read: u64,
    pub total_failures: u64,
    pub consecutive_failures: u64,
}

impl MetadataWarmState {
    pub(crate) fn new(target_bytes: u64) -> Self {
        Self {
            target_bytes,
            estimated_target_bytes: 0,
            entries: BTreeMap::new(),
            total_cycles: 0,
            total_bucket_warms: 0,
            total_bytes_read: 0,
            total_failures: 0,
            consecutive_failures: 0,
        }
    }

    /// Select the newest-first target set from the given buckets up to the
    /// byte budget. Returns the planned actions for this cycle.
    ///
    /// `known_sizes` provides per-bucket byte estimates from previous cycles.
    /// Buckets without known sizes use `default_bucket_bytes` as an estimate.
    pub(crate) fn plan_cycle(
        &mut self,
        all_buckets: &[TimeBucket],
        config: &MetadataWarmConfig,
    ) -> CyclePlan {
        let rewarm_interval = config.rewarm_interval;
        let now = Instant::now();

        // Sort newest-first (higher start = newer).
        let mut sorted: Vec<TimeBucket> = all_buckets.to_vec();
        sorted.sort_by(|a, b| b.start.cmp(&a.start));

        // Select buckets up to the byte budget.
        let mut selected: Vec<TimeBucket> = Vec::new();
        let mut budget_used: u64 = 0;
        for bucket in &sorted {
            let estimated_bytes = self
                .entries
                .get(bucket)
                .map(|e| e.total_bytes)
                .unwrap_or(0);
            // If we have no prior data, we still include the bucket (we'll learn
            // its size after warming). Budget enforcement uses known sizes.
            if budget_used > 0 && estimated_bytes > 0 && budget_used + estimated_bytes > config.preload_bytes {
                break;
            }
            selected.push(*bucket);
            budget_used += estimated_bytes;
        }

        let selected_count = selected.len();

        // Determine action for each selected bucket.
        let mut actions = Vec::with_capacity(selected_count);
        let mut new_count = 0u32;
        let mut rewarm_count = 0u32;
        let mut fresh_count = 0u32;

        for bucket in &selected {
            match self.entries.get(bucket) {
                None => {
                    // New bucket — needs initial warm.
                    let reason = if self.total_cycles == 0 {
                        WarmReason::Initial
                    } else {
                        WarmReason::NewBucket
                    };
                    actions.push(BucketAction::Warm {
                        bucket: *bucket,
                        reason,
                    });
                    new_count += 1;
                }
                Some(entry) => {
                    if now.duration_since(entry.last_warmed_at) >= rewarm_interval {
                        actions.push(BucketAction::Warm {
                            bucket: *bucket,
                            reason: WarmReason::Rewarm,
                        });
                        rewarm_count += 1;
                    } else {
                        actions.push(BucketAction::Fresh {
                            bucket: *bucket,
                        });
                        fresh_count += 1;
                    }
                }
            }
        }

        // Remove entries for buckets no longer in the target set.
        let selected_set: std::collections::HashSet<TimeBucket> =
            selected.iter().copied().collect();
        let removed: Vec<TimeBucket> = self
            .entries
            .keys()
            .filter(|k| !selected_set.contains(k))
            .copied()
            .collect();
        let removed_count = removed.len();
        for bucket in &removed {
            self.entries.remove(bucket);
        }

        self.estimated_target_bytes = budget_used;

        CyclePlan {
            actions,
            selected_count,
            new_count: new_count as usize,
            rewarm_count: rewarm_count as usize,
            fresh_count: fresh_count as usize,
            removed_count,
        }
    }

    /// Record a successful warm of a bucket.
    pub(crate) fn record_warm(
        &mut self,
        bucket: TimeBucket,
        forward_index_bytes: u64,
        inverted_index_bytes: u64,
    ) {
        let total_bytes = forward_index_bytes + inverted_index_bytes;
        self.entries.insert(
            bucket,
            BucketWarmEntry {
                bucket,
                forward_index_bytes,
                inverted_index_bytes,
                total_bytes,
                last_warmed_at: Instant::now(),
            },
        );
        self.total_bucket_warms += 1;
        self.total_bytes_read += total_bytes;
    }

    /// Record a failed warm attempt.
    pub(crate) fn record_failure(&mut self) {
        self.total_failures += 1;
        self.consecutive_failures += 1;
    }

    /// Mark cycle completion (success).
    pub(crate) fn complete_cycle(&mut self) {
        self.total_cycles += 1;
        self.consecutive_failures = 0;
        // Recompute estimated target bytes from actual entries.
        self.estimated_target_bytes = self.entries.values().map(|e| e.total_bytes).sum();
    }

    /// Mark cycle completion (failure).
    pub(crate) fn fail_cycle(&mut self) {
        self.total_cycles += 1;
        // consecutive_failures already incremented by record_failure()
    }

    /// The oldest bucket in the target set (lowest start time).
    pub(crate) fn oldest_target_bucket(&self) -> Option<&TimeBucket> {
        self.entries.keys().next()
    }

    /// The newest bucket in the target set (highest start time).
    pub(crate) fn newest_target_bucket(&self) -> Option<&TimeBucket> {
        self.entries.keys().next_back()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn bucket(start: u32) -> TimeBucket {
        TimeBucket::hour(start)
    }

    fn config_with_budget(bytes: u64) -> MetadataWarmConfig {
        MetadataWarmConfig {
            enabled: true,
            preload_bytes: bytes,
            refresh_interval: Duration::from_secs(60),
            rewarm_interval: Duration::from_secs(300),
            max_concurrent_buckets: 4,
        }
    }

    #[test]
    fn should_select_newest_buckets_until_budget_is_reached() {
        // given — all buckets have known sizes
        let config = config_with_budget(500);
        let mut state = MetadataWarmState::new(500);

        for (start, bytes) in [(180, 200), (120, 200), (60, 300), (0, 100)] {
            state.entries.insert(
                bucket(start),
                BucketWarmEntry {
                    bucket: bucket(start),
                    forward_index_bytes: bytes / 2,
                    inverted_index_bytes: bytes / 2,
                    total_bytes: bytes,
                    last_warmed_at: Instant::now() - Duration::from_secs(600),
                },
            );
        }

        let all_buckets = vec![bucket(0), bucket(60), bucket(120), bucket(180)];

        // when
        let plan = state.plan_cycle(&all_buckets, &config);

        // then — newest-first: bucket(180)=200, budget=200. bucket(120)=200, budget=400.
        // bucket(60)=300 → 400+300=700 > 500 → excluded. bucket(0) not reached.
        assert_eq!(plan.selected_count, 2);
    }

    #[test]
    fn should_allow_budget_to_cross_by_one_bucket() {
        // given — budget = 100, but first bucket has no known size
        let config = config_with_budget(100);
        let mut state = MetadataWarmState::new(100);
        let all_buckets = vec![bucket(60), bucket(120)];

        // when — no prior sizes known, so both get included (budget_used stays 0)
        let plan = state.plan_cycle(&all_buckets, &config);

        // then
        assert_eq!(plan.selected_count, 2);
    }

    #[test]
    fn should_mark_new_bucket_for_warm() {
        // given
        let config = config_with_budget(1000);
        let mut state = MetadataWarmState::new(1000);
        state.total_cycles = 1; // not initial

        let all_buckets = vec![bucket(120)];

        // when
        let plan = state.plan_cycle(&all_buckets, &config);

        // then
        assert_eq!(plan.new_count, 1);
        match &plan.actions[0] {
            BucketAction::Warm { reason, .. } => assert_eq!(*reason, WarmReason::NewBucket),
            _ => panic!("expected Warm action"),
        }
    }

    #[test]
    fn should_mark_initial_warm_on_first_cycle() {
        // given
        let config = config_with_budget(1000);
        let mut state = MetadataWarmState::new(1000);
        // total_cycles = 0 → initial

        let all_buckets = vec![bucket(120)];

        // when
        let plan = state.plan_cycle(&all_buckets, &config);

        // then
        match &plan.actions[0] {
            BucketAction::Warm { reason, .. } => assert_eq!(*reason, WarmReason::Initial),
            _ => panic!("expected Warm action"),
        }
    }

    #[test]
    fn should_mark_stale_bucket_for_rewarm() {
        // given
        let config = config_with_budget(1000);
        let mut state = MetadataWarmState::new(1000);
        state.total_cycles = 1;
        state.entries.insert(
            bucket(120),
            BucketWarmEntry {
                bucket: bucket(120),
                forward_index_bytes: 100,
                inverted_index_bytes: 100,
                total_bytes: 200,
                last_warmed_at: Instant::now() - Duration::from_secs(600), // > rewarm_interval
            },
        );

        let all_buckets = vec![bucket(120)];

        // when
        let plan = state.plan_cycle(&all_buckets, &config);

        // then
        assert_eq!(plan.rewarm_count, 1);
        match &plan.actions[0] {
            BucketAction::Warm { reason, .. } => assert_eq!(*reason, WarmReason::Rewarm),
            _ => panic!("expected Warm action"),
        }
    }

    #[test]
    fn should_skip_bucket_when_entry_is_fresh() {
        // given
        let config = config_with_budget(1000);
        let mut state = MetadataWarmState::new(1000);
        state.total_cycles = 1;
        state.entries.insert(
            bucket(120),
            BucketWarmEntry {
                bucket: bucket(120),
                forward_index_bytes: 100,
                inverted_index_bytes: 100,
                total_bytes: 200,
                last_warmed_at: Instant::now(), // just warmed
            },
        );

        let all_buckets = vec![bucket(120)];

        // when
        let plan = state.plan_cycle(&all_buckets, &config);

        // then
        assert_eq!(plan.fresh_count, 1);
        assert_eq!(plan.new_count, 0);
        assert_eq!(plan.rewarm_count, 0);
    }

    #[test]
    fn should_remove_entries_not_in_target_set() {
        // given
        let config = config_with_budget(100);
        let mut state = MetadataWarmState::new(100);
        state.entries.insert(
            bucket(60),
            BucketWarmEntry {
                bucket: bucket(60),
                forward_index_bytes: 200,
                inverted_index_bytes: 200,
                total_bytes: 400,
                last_warmed_at: Instant::now(),
            },
        );
        // Only bucket(120) in the target set; budget is 100 and bucket(60) costs 400
        let all_buckets = vec![bucket(120)];

        // when
        let plan = state.plan_cycle(&all_buckets, &config);

        // then
        assert_eq!(plan.removed_count, 1);
        assert!(!state.entries.contains_key(&bucket(60)));
    }

    #[test]
    fn should_handle_empty_bucket_list() {
        // given
        let config = config_with_budget(1000);
        let mut state = MetadataWarmState::new(1000);

        // when
        let plan = state.plan_cycle(&[], &config);

        // then
        assert_eq!(plan.selected_count, 0);
        assert!(plan.actions.is_empty());
    }

    #[test]
    fn should_record_warm_and_update_entry() {
        // given
        let mut state = MetadataWarmState::new(1000);

        // when
        state.record_warm(bucket(120), 500, 300);

        // then
        let entry = state.entries.get(&bucket(120)).unwrap();
        assert_eq!(entry.forward_index_bytes, 500);
        assert_eq!(entry.inverted_index_bytes, 300);
        assert_eq!(entry.total_bytes, 800);
        assert_eq!(state.total_bucket_warms, 1);
        assert_eq!(state.total_bytes_read, 800);
    }

    #[test]
    fn should_track_consecutive_failures() {
        // given
        let mut state = MetadataWarmState::new(1000);

        // when
        state.record_failure();
        state.record_failure();

        // then
        assert_eq!(state.consecutive_failures, 2);
        assert_eq!(state.total_failures, 2);

        // when — successful cycle resets consecutive
        state.complete_cycle();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.total_failures, 2); // total unchanged
    }
}

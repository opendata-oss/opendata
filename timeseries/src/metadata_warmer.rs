//! Background metadata warmer for keeping newest bucket metadata in cache.
//!
//! The warmer maintains a newest-first target set of buckets up to a
//! configured byte budget. It periodically refreshes the target set and
//! rewarms stale buckets whose cache entries may have been evicted by
//! compaction or LRU pressure.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use common::StorageRead;
use tokio::sync::watch;

use crate::config::MetadataWarmConfig;
use crate::load_coordinator::ReadLoadCoordinator;
use crate::model::TimeBucket;
use crate::storage::OpenTsdbStorageReadExt;

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
    Warm {
        bucket: TimeBucket,
        reason: WarmReason,
    },
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
            let estimated_bytes = self.entries.get(bucket).map(|e| e.total_bytes).unwrap_or(0);
            // If we have no prior data, we still include the bucket (we'll learn
            // its size after warming). Budget enforcement uses known sizes.
            if budget_used > 0
                && estimated_bytes > 0
                && budget_used + estimated_bytes > config.preload_bytes
            {
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
                        actions.push(BucketAction::Fresh { bucket: *bucket });
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

// ── Background warmer task ───────────────────────────────────────────

/// Handle to a running metadata warmer. Drop to stop.
pub(crate) struct MetadataWarmerHandle {
    shutdown_tx: watch::Sender<bool>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl MetadataWarmerHandle {
    /// Signal the warmer to stop and wait for it to finish.
    pub(crate) async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join_handle.await;
    }
}

/// Start the metadata warmer background task.
///
/// Returns a handle that can be used to stop the warmer.
/// If the config is disabled, returns None.
pub(crate) fn start_metadata_warmer(
    config: MetadataWarmConfig,
    storage: Arc<dyn StorageRead>,
    coordinator: ReadLoadCoordinator,
    #[cfg(feature = "http-server")] metrics: Option<Arc<crate::server::metrics::Metrics>>,
) -> Option<MetadataWarmerHandle> {
    if !config.enabled {
        tracing::info!("metadata warmer disabled (TSDB_METADATA_PRELOAD_BYTES=0)");
        return None;
    }

    tracing::info!(
        target_bytes = config.preload_bytes,
        refresh_interval_secs = config.refresh_interval.as_secs(),
        rewarm_interval_secs = config.rewarm_interval.as_secs(),
        max_concurrent_buckets = config.max_concurrent_buckets,
        "starting metadata warmer"
    );

    #[cfg(feature = "http-server")]
    if let Some(ref m) = metrics {
        m.warmer.enabled.set(1);
        m.warmer.target_bytes.set(config.preload_bytes as i64);
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let join_handle = tokio::spawn(warmer_loop(
        config,
        storage,
        coordinator,
        shutdown_rx,
        #[cfg(feature = "http-server")]
        metrics,
    ));

    Some(MetadataWarmerHandle {
        shutdown_tx,
        join_handle,
    })
}

async fn warmer_loop(
    config: MetadataWarmConfig,
    storage: Arc<dyn StorageRead>,
    coordinator: ReadLoadCoordinator,
    mut shutdown_rx: watch::Receiver<bool>,
    #[cfg(feature = "http-server")] metrics: Option<Arc<crate::server::metrics::Metrics>>,
) {
    let mut state = MetadataWarmState::new(config.preload_bytes);

    loop {
        let cycle_start = Instant::now();

        let cycle_result = run_cycle(
            &config,
            &storage,
            &coordinator,
            &mut state,
            #[cfg(feature = "http-server")]
            metrics.as_deref(),
        )
        .await;

        let cycle_secs = cycle_start.elapsed().as_secs_f64();
        let _is_ok = cycle_result.is_ok();

        if let Err(ref e) = cycle_result {
            state.fail_cycle();
            tracing::warn!(
                cycle = state.total_cycles,
                error = %e,
                consecutive_failures = state.consecutive_failures,
                "metadata warmer cycle failed"
            );
        } else {
            state.complete_cycle();
        }

        #[cfg(feature = "http-server")]
        if let Some(ref m) = metrics {
            publish_warmer_cycle_metrics(m, &state, cycle_secs, _is_ok);
        }

        tracing::info!(
            cycle = state.total_cycles,
            duration_secs = format!("{:.3}", cycle_secs),
            warmed_buckets = state.entries.len(),
            estimated_bytes = state.estimated_target_bytes,
            oldest_start = state.oldest_target_bucket().map(|b| b.start),
            newest_start = state.newest_target_bucket().map(|b| b.start),
            "metadata warmer cycle complete"
        );

        // Wait for refresh interval or shutdown.
        tokio::select! {
            _ = tokio::time::sleep(config.refresh_interval) => {}
            _ = shutdown_rx.changed() => {
                tracing::info!("metadata warmer shutting down");
                return;
            }
        }
    }
}

async fn run_cycle(
    config: &MetadataWarmConfig,
    storage: &Arc<dyn StorageRead>,
    coordinator: &ReadLoadCoordinator,
    state: &mut MetadataWarmState,
    #[cfg(feature = "http-server")] metrics: Option<&crate::server::metrics::Metrics>,
) -> crate::util::Result<()> {
    // 1. Warm bucket list.
    let warm_result = storage.warm_bucket_list().await?;
    state.total_bytes_read += warm_result.bytes_read;

    #[cfg(feature = "http-server")]
    if let Some(m) = metrics {
        use crate::server::metrics::{WarmerBytesLabels, WarmerReason, WarmerUnit};
        m.warmer
            .bytes_read_total
            .get_or_create(&WarmerBytesLabels {
                unit: WarmerUnit::BucketList,
                reason: if state.total_cycles == 0 {
                    WarmerReason::Initial
                } else {
                    WarmerReason::Rewarm
                },
            })
            .inc_by(warm_result.bytes_read);
    }

    // 2. Plan cycle.
    let plan = state.plan_cycle(&warm_result.buckets, config);

    tracing::debug!(
        selected = plan.selected_count,
        new = plan.new_count,
        rewarm = plan.rewarm_count,
        fresh = plan.fresh_count,
        removed = plan.removed_count,
        "metadata warmer cycle plan"
    );

    // 3. Execute warm actions with bounded concurrency.
    let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_buckets));

    let warm_actions: Vec<_> = plan
        .actions
        .iter()
        .filter_map(|action| match action {
            BucketAction::Warm { bucket, reason } => Some((*bucket, *reason)),
            BucketAction::Fresh { .. } => None,
        })
        .collect();

    #[cfg(feature = "http-server")]
    if let Some(m) = metrics {
        m.warmer.inflight_buckets.set(warm_actions.len() as i64);
    }

    // Run bucket warms concurrently, bounded by semaphore.
    let mut results = Vec::with_capacity(warm_actions.len());
    let mut join_set = tokio::task::JoinSet::new();

    for (bucket, reason) in warm_actions {
        let storage = storage.clone();
        let coordinator = coordinator.clone();
        let sem = semaphore.clone();
        join_set.spawn(async move {
            let _sem_permit = sem.acquire().await.unwrap();
            // Acquire metadata permit to respect global budget.
            let (_permit, _wait) = coordinator.acquire_metadata().await;
            let t0 = Instant::now();
            let fi_result = storage.warm_forward_index_bytes(bucket).await;
            let ii_result = storage.warm_inverted_index_bytes(bucket).await;
            let elapsed = t0.elapsed();
            drop(_permit);
            (bucket, reason, fi_result, ii_result, elapsed)
        });
    }

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok((bucket, reason, fi_result, ii_result, elapsed)) => {
                match (&fi_result, &ii_result) {
                    (Ok(fi_bytes), Ok(ii_bytes)) => {
                        state.record_warm(bucket, *fi_bytes, *ii_bytes);
                        tracing::trace!(
                            bucket_start = bucket.start,
                            reason = ?reason,
                            forward_index_bytes = fi_bytes,
                            inverted_index_bytes = ii_bytes,
                            duration_ms = elapsed.as_millis() as u64,
                            "warmed bucket metadata"
                        );

                        #[cfg(feature = "http-server")]
                        if let Some(m) = metrics {
                            publish_bucket_warm_metrics(
                                m,
                                reason,
                                *fi_bytes,
                                *ii_bytes,
                                elapsed.as_secs_f64(),
                                true,
                            );
                        }
                    }
                    _ => {
                        state.record_failure();
                        if let Err(ref e) = fi_result {
                            tracing::warn!(bucket_start = bucket.start, error = %e, "failed to warm forward index");
                        }
                        if let Err(ref e) = ii_result {
                            tracing::warn!(bucket_start = bucket.start, error = %e, "failed to warm inverted index");
                        }

                        #[cfg(feature = "http-server")]
                        if let Some(m) = metrics {
                            publish_bucket_warm_metrics(
                                m,
                                reason,
                                0,
                                0,
                                elapsed.as_secs_f64(),
                                false,
                            );
                        }
                    }
                }
                results.push((bucket, fi_result, ii_result));
            }
            Err(e) => {
                tracing::warn!(error = %e, "metadata warmer task panicked");
                state.record_failure();
            }
        }
    }

    #[cfg(feature = "http-server")]
    if let Some(m) = metrics {
        m.warmer.inflight_buckets.set(0);
    }

    Ok(())
}

#[cfg(feature = "http-server")]
fn publish_warmer_cycle_metrics(
    metrics: &crate::server::metrics::Metrics,
    state: &MetadataWarmState,
    cycle_secs: f64,
    is_ok: bool,
) {
    use crate::server::metrics::{QueryStatus, WarmerCycleLabels};

    let status = if is_ok {
        QueryStatus::Ok
    } else {
        QueryStatus::Error
    };

    metrics
        .warmer
        .cycles_total
        .get_or_create(&WarmerCycleLabels {
            status: status.clone(),
        })
        .inc();
    metrics
        .warmer
        .cycle_duration_seconds
        .get_or_create(&WarmerCycleLabels { status })
        .observe(cycle_secs);

    metrics
        .warmer
        .warmed_buckets
        .set(state.entries.len() as i64);
    metrics
        .warmer
        .estimated_target_bytes
        .set(state.estimated_target_bytes as i64);
    metrics
        .warmer
        .consecutive_failures
        .set(state.consecutive_failures as i64);

    if let Some(oldest) = state.oldest_target_bucket() {
        metrics
            .warmer
            .oldest_target_bucket_start_minutes
            .set(oldest.start as i64);
    }
}

#[cfg(feature = "http-server")]
fn publish_bucket_warm_metrics(
    metrics: &crate::server::metrics::Metrics,
    reason: WarmReason,
    fi_bytes: u64,
    ii_bytes: u64,
    duration_secs: f64,
    is_ok: bool,
) {
    use crate::server::metrics::{
        QueryStatus, WarmerBucketWarmLabels, WarmerBytesLabels, WarmerReason, WarmerUnit,
    };

    let status = if is_ok {
        QueryStatus::Ok
    } else {
        QueryStatus::Error
    };
    let reason_label = match reason {
        WarmReason::Initial => WarmerReason::Initial,
        WarmReason::NewBucket => WarmerReason::NewBucket,
        WarmReason::Rewarm => WarmerReason::Rewarm,
    };

    // Forward index
    metrics
        .warmer
        .bucket_warms_total
        .get_or_create(&WarmerBucketWarmLabels {
            unit: WarmerUnit::ForwardIndex,
            reason: reason_label.clone(),
            status: status.clone(),
        })
        .inc();
    metrics
        .warmer
        .bucket_warm_duration_seconds
        .get_or_create(&WarmerBucketWarmLabels {
            unit: WarmerUnit::ForwardIndex,
            reason: reason_label.clone(),
            status: status.clone(),
        })
        .observe(duration_secs);
    if fi_bytes > 0 {
        metrics
            .warmer
            .bytes_read_total
            .get_or_create(&WarmerBytesLabels {
                unit: WarmerUnit::ForwardIndex,
                reason: reason_label.clone(),
            })
            .inc_by(fi_bytes);
    }

    // Inverted index
    metrics
        .warmer
        .bucket_warms_total
        .get_or_create(&WarmerBucketWarmLabels {
            unit: WarmerUnit::InvertedIndex,
            reason: reason_label.clone(),
            status: status.clone(),
        })
        .inc();
    metrics
        .warmer
        .bucket_warm_duration_seconds
        .get_or_create(&WarmerBucketWarmLabels {
            unit: WarmerUnit::InvertedIndex,
            reason: reason_label.clone(),
            status,
        })
        .observe(duration_secs);
    if ii_bytes > 0 {
        metrics
            .warmer
            .bytes_read_total
            .get_or_create(&WarmerBytesLabels {
                unit: WarmerUnit::InvertedIndex,
                reason: reason_label,
            })
            .inc_by(ii_bytes);
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

//! L0-only compaction scheduler for performance testing.
//!
//! This module provides a compaction scheduler that only compacts L0 SSTs
//! into sorted runs, never compacting sorted runs with each other. This is
//! useful for measuring the compaction throughput ceiling during benchmarks.

use std::collections::HashSet;

use slatedb::compactor::{CompactionScheduler, CompactionSchedulerSupplier, CompactorStateView};
use slatedb::compactor::{CompactionSpec, SourceId};
use slatedb::config::CompactorOptions;

use crate::config::L0OnlyCompactionConfig;

/// A compaction scheduler that only compacts L0 SSTs.
///
/// Each compaction takes a batch of L0 SSTs and writes them into a new sorted
/// run. Sorted runs are never merged with each other, which isolates L0
/// compaction throughput for benchmarking.
pub struct L0OnlyCompactionScheduler {
    min_compaction_sources: usize,
    max_compaction_sources: usize,
    max_concurrent_compactions: usize,
}

impl L0OnlyCompactionScheduler {
    pub fn new(config: &L0OnlyCompactionConfig) -> Self {
        Self {
            min_compaction_sources: config.min_compaction_sources,
            max_compaction_sources: config.max_compaction_sources,
            max_concurrent_compactions: config.max_concurrent_compactions,
        }
    }
}

impl CompactionScheduler for L0OnlyCompactionScheduler {
    fn propose(&self, state: &CompactorStateView) -> Vec<CompactionSpec> {
        let manifest = state.manifest();

        // Collect source IDs already used by active compactions.
        let mut sources_used: HashSet<SourceId> = HashSet::new();
        let mut active_count = 0usize;
        if let Some(compactions) = state.compactions() {
            for c in compactions.recent_compactions().filter(|c| c.active()) {
                active_count += 1;
                for src in c.spec().sources() {
                    sources_used.insert(*src);
                }
                sources_used.insert(SourceId::SortedRun(c.spec().destination()));
            }
        }

        // Collect available L0 SSTs not in use, in manifest order (newest-first).
        let available: Vec<SourceId> = manifest
            .l0
            .iter()
            .map(|view| SourceId::SstView(view.id))
            .filter(|id| !sources_used.contains(id))
            .collect();

        // Destination SR id: one past the highest existing SR id.
        let base_dst = manifest.compacted.first().map_or(0, |sr| sr.id + 1);

        // Split available L0s into consecutive batches from the tail (oldest).
        // Each batch is capped at max_compaction_sources and stays in manifest
        // order so that validate() sees consecutive entries.
        let mut specs = Vec::new();
        let mut end = available.len();
        let mut next_dst = base_dst;

        while active_count + specs.len() < self.max_concurrent_compactions {
            if end < self.min_compaction_sources {
                break;
            }

            // Skip destination SR IDs already claimed by active compactions.
            while sources_used.contains(&SourceId::SortedRun(next_dst)) {
                next_dst += 1;
            }

            let take = end.min(self.max_compaction_sources);
            let start = end - take;
            let batch: Vec<SourceId> = available[start..end].to_vec();
            end = start;

            specs.push(CompactionSpec::new(batch, next_dst));
            next_dst += 1;
        }

        specs
    }

    fn validate(
        &self,
        state: &CompactorStateView,
        spec: &CompactionSpec,
    ) -> Result<(), slatedb::Error> {
        // All sources must be L0 SSTs.
        for source in spec.sources() {
            if let SourceId::SortedRun(_) = source {
                return Err(slatedb::Error::invalid(
                    "L0-only scheduler does not allow sorted run sources".to_string(),
                ));
            }
        }

        // Verify sources are consecutive in the manifest's L0 list.
        let l0_ids: Vec<SourceId> = state
            .manifest()
            .l0
            .iter()
            .map(|view| SourceId::SstView(view.id))
            .collect();

        if spec.sources().is_empty() {
            return Err(slatedb::Error::invalid(
                "compaction spec has no sources".to_string(),
            ));
        }

        // Find position of first source in L0 list.
        let first_pos = l0_ids
            .iter()
            .position(|id| id == &spec.sources()[0])
            .ok_or_else(|| slatedb::Error::invalid("source not found in L0 list".to_string()))?;

        for (i, src) in spec.sources().iter().enumerate() {
            if first_pos + i >= l0_ids.len() || l0_ids[first_pos + i] != *src {
                return Err(slatedb::Error::invalid(
                    "sources are not consecutive in L0 list".to_string(),
                ));
            }
        }

        Ok(())
    }
}

/// Supplier that creates [`L0OnlyCompactionScheduler`] instances.
pub struct L0OnlyCompactionSchedulerSupplier {
    config: L0OnlyCompactionConfig,
}

impl L0OnlyCompactionSchedulerSupplier {
    pub fn new(config: L0OnlyCompactionConfig) -> Self {
        Self { config }
    }
}

impl CompactionSchedulerSupplier for L0OnlyCompactionSchedulerSupplier {
    fn compaction_scheduler(
        &self,
        _options: &CompactorOptions,
    ) -> Box<dyn CompactionScheduler + Send + Sync> {
        Box::new(L0OnlyCompactionScheduler::new(&self.config))
    }
}

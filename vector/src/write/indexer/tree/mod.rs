use crate::DistanceMetric;
use crate::Result;
use crate::write::delta::VectorWrite;
use crate::write::indexer::tree::centroids::CentroidCache;
use crate::write::indexer::tree::merge::MergeCentroids;
use crate::write::indexer::tree::root::SplitRoot;
use crate::write::indexer::tree::split::{ReassignVector, SplitCentroids};
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState};
use crate::write::indexer::tree::vector::{ReassignVectors, WriteVectors};
use common::StorageRead;
use common::storage::RecordOp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, debug_span};

pub(crate) mod centroids;
mod merge;
mod root;
mod split;
pub(crate) mod state;
mod vector;
mod posting_list;

#[derive(Debug, Default)]
pub(crate) struct IndexerStats {
    pub(crate) inserts: usize,
    pub(crate) updates: usize,
    pub(crate) merges: HashMap<u16, usize>,
}

pub(crate) struct IndexerOpts {
    pub(crate) dimensions: usize,
    pub(crate) distance_metric: DistanceMetric,
    pub(crate) root_threshold_vectors: usize,
    pub(crate) merge_threshold_vectors: usize,
    pub(crate) split_threshold_vectors: usize,
    pub(crate) split_search_neighbourhood: usize,
    pub(crate) indexed_fields: HashSet<String>,
    pub(crate) chunk_target: usize,
}

pub(crate) struct IndexUpdateResults {
    pub(crate) ops: Vec<RecordOp>,
    pub(crate) stats: IndexerStats,
    pub(crate) centroid_cache: Arc<dyn CentroidCache>,
    pub(crate) leaf_centroids: usize,
    pub(crate) centroid_tree_depth: usize,
}

pub(crate) struct Indexer {
    opts: Arc<IndexerOpts>,
    state: VectorIndexState,
}

impl Indexer {
    pub(crate) fn new(opts: IndexerOpts, state: VectorIndexState) -> Self {
        Self {
            opts: Arc::new(opts),
            state,
        }
    }

    fn leaf_centroid_count(&self) -> usize {
        self.state
            .centroids()
            .values()
            .filter(|centroid| centroid.level == 0)
            .count()
    }

    fn query_centroid_cache(&self) -> Arc<dyn CentroidCache> {
        Arc::new(self.state.centroid_cache()) as Arc<dyn CentroidCache>
    }

    pub(crate) async fn update_index(
        &mut self,
        updates: Vec<VectorWrite>,
        update_epoch: u64,
        snapshot: Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Result<IndexUpdateResults> {
        let update_span = debug_span!(
            "update_index",
            update_epoch,
            snapshot_epoch,
            write_count = updates.len(),
            depth = self.state.centroids_meta().depth as u16
        );
        let update_start = Instant::now();
        let mut stats = IndexerStats::default();
        let mut delta = VectorIndexDelta::new(&self.state);

        // write all vectors
        let write = WriteVectors::new(&self.opts, &snapshot, snapshot_epoch, updates);
        let write_start = Instant::now();
        let (inserts, updates) = write.execute(&self.state, &mut delta).await?;
        debug!(
            parent: &update_span,
            op = "write_vectors",
            elapsed_ms = elapsed_ms(write_start),
            inserts,
            updates,
            "completed"
        );
        stats.inserts = inserts;
        stats.updates = updates;

        // update the centroid tree level by level
        let depth = self.state.centroids_meta().depth as u16;
        for level in 0..depth {
            // apply merges
            let merge = MergeCentroids::new(&self.opts, level, depth, &snapshot, snapshot_epoch);
            let merge_start = Instant::now();
            let (reassigns, merge_count) = merge.execute(&self.state, &mut delta).await?;
            debug!(
                parent: &update_span,
                op = "merge_centroids",
                level,
                depth,
                elapsed_ms = elapsed_ms(merge_start),
                merge_count,
                reassignment_count = reassigns.len(),
                "completed"
            );
            *stats.merges.entry(level).or_default() += merge_count;

            // apply merge reassignments
            let reassign_after_merge_start = Instant::now();
            let reassign_after_merge_count = self
                .reassign_vectors(&snapshot, snapshot_epoch, &mut delta, reassigns)
                .await?;
            debug!(
                parent: &update_span,
                op = "reassign_vectors_after_merge",
                level,
                elapsed_ms = elapsed_ms(reassign_after_merge_start),
                reassigned_count = reassign_after_merge_count,
                "completed"
            );

            // apply split reassign loop
            let mut split_round = 0usize;
            loop {
                let split =
                    SplitCentroids::new(&self.opts, level, depth, &snapshot, snapshot_epoch);
                let split_start = Instant::now();
                let result = split.execute(&self.state, &mut delta).await?;
                debug!(
                    parent: &update_span,
                    op = "split_centroids",
                    level,
                    depth,
                    split_round,
                    elapsed_ms = elapsed_ms(split_start),
                    split_count = result.splits.len(),
                    reassignment_count = result.reassignments.len(),
                    "completed"
                );
                if result.splits.is_empty() {
                    break;
                }
                let reassign_after_split_start = Instant::now();
                let reassign_after_split_count = self
                    .reassign_vectors(&snapshot, snapshot_epoch, &mut delta, result.reassignments)
                    .await?;
                debug!(
                    parent: &update_span,
                    op = "reassign_vectors_after_split",
                    level,
                    split_round,
                    elapsed_ms = elapsed_ms(reassign_after_split_start),
                    reassigned_count = reassign_after_split_count,
                    "completed"
                );
                split_round += 1;
            }
        }

        // maybe split root
        let split_root = SplitRoot::new(&self.opts, &snapshot, snapshot_epoch);
        let split_root_start = Instant::now();
        split_root.execute(&mut delta, &self.state).await?;
        debug!(
            parent: &update_span,
            op = "split_root",
            elapsed_ms = elapsed_ms(split_root_start),
            "completed"
        );

        let freeze_start = Instant::now();
        let ops = delta.freeze(update_epoch, &mut self.state);
        debug!(
            parent: &update_span,
            op = "freeze_delta",
            update_epoch,
            elapsed_ms = elapsed_ms(freeze_start),
            record_op_count = ops.len(),
            "completed"
        );
        debug!(
            parent: &update_span,
            elapsed_ms = elapsed_ms(update_start),
            leaf_centroid_count = self.leaf_centroid_count(),
            "completed"
        );
        Ok(IndexUpdateResults {
            ops,
            stats,
            centroid_cache: self.query_centroid_cache(),
            leaf_centroids: self.leaf_centroid_count(),
            centroid_tree_depth: self.state.centroids_meta().depth as usize,
        })
    }

    async fn reassign_vectors(
        &self,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        delta: &mut VectorIndexDelta,
        reassigns: Vec<ReassignVector>,
    ) -> Result<usize> {
        let total_reassign_count = reassigns.len();
        let reassign_start = Instant::now();
        let mut total_reassigned = 0;
        let mut reassigns_by_level: HashMap<u16, Vec<ReassignVector>> = HashMap::new();
        for r in reassigns {
            let entry = reassigns_by_level.entry(r.level).or_default();
            entry.push(r);
        }
        // reassign from the top down to improve recall at higher levels first
        for level in (0..self.state.centroids_meta().depth as u16).rev() {
            let Some(reassigns) = reassigns_by_level.remove(&level) else {
                continue;
            };
            let level_reassign_count = reassigns.len();
            let reassign =
                ReassignVectors::new(&self.opts, snapshot, snapshot_epoch, reassigns, level);
            let level_start = Instant::now();
            let reassigned = reassign.execute(&self.state, delta).await?;
            total_reassigned += reassigned;
            debug!(
                level,
                snapshot_epoch,
                requested_reassignments = level_reassign_count,
                reassigned_count = reassigned,
                elapsed_ms = elapsed_ms(level_start),
                op = "reassign_vectors_level",
                "completed"
            );
        }
        assert!(reassigns_by_level.is_empty());
        debug!(
            snapshot_epoch,
            requested_reassignments = total_reassign_count,
            reassigned_count = total_reassigned,
            elapsed_ms = elapsed_ms(reassign_start),
            op = "reassign_vectors",
            "completed"
        );
        Ok(total_reassigned)
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

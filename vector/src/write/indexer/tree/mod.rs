use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use common::storage::RecordOp;
use common::StorageRead;
use crate::DistanceMetric;
use crate::write::delta::VectorWrite;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState};
use crate::Result;
use crate::write::indexer::tree::merge::MergeCentroids;
use crate::write::indexer::tree::root::SplitRoot;
use crate::write::indexer::tree::split::{ReassignVector, SplitCentroids};
use crate::write::indexer::tree::vector::{ReassignVectors, WriteVectors};

mod state;
mod centroids;
mod vector;
mod split;
mod merge;
mod root;

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

struct Indexer {
    opts: Arc<IndexerOpts>,
    state: VectorIndexState,
}

impl Indexer {
    pub(crate) fn new() -> Self {
        todo!()
    }

    pub(crate) async fn update_index(
        &mut self,
        updates: Vec<VectorWrite>,
        update_epoch: u64,
        snapshot: Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Result<(Vec<RecordOp>, IndexerStats)> {
        let mut stats = IndexerStats::default();
        let mut delta = VectorIndexDelta::new(&self.state);

        // write all vectors
        let write = WriteVectors::new(&self.opts, &snapshot, snapshot_epoch, updates);
        let (inserts, updates) = write.execute(&self.state, &mut delta).await?;
        stats.inserts = inserts;
        stats.updates = updates;

        // update the centroid tree level by level
        let depth = self.state.centroids_meta().depth as u16;
        for level in 0..depth {
            // apply merges
            let merge = MergeCentroids::new(
                &self.opts,
                level,
                depth,
                &snapshot,
                snapshot_epoch
            );
            let (reassigns, merge_count) = merge.execute(&self.state, &mut delta).await?;
            *stats.merges.entry(level).or_default() += merge_count;

            // apply merge reassignments
            self.reassign_vectors(&snapshot, snapshot_epoch, &mut delta, reassigns).await?;

            // apply split reassign loop
            loop {
                let split = SplitCentroids::new(
                    &self.opts,
                    level,
                    depth,
                    &snapshot,
                    snapshot_epoch,
                );
                let result = split.execute(&self.state, &mut delta).await?;
                if result.splits.is_empty() {
                    break;
                }
                self.reassign_vectors(&snapshot, snapshot_epoch, &mut delta, result.reassignments).await?;
            }
        }

        // maybe split root
        let split_root = SplitRoot::new(&self.opts, &snapshot, snapshot_epoch);
        split_root.execute(&mut delta, &self.state).await?;

        let ops = delta.freeze(&mut self.state);
        Ok((ops, stats))
    }

    async fn reassign_vectors(
        &self,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        delta: &mut VectorIndexDelta,
        reassigns: Vec<ReassignVector>
    ) -> Result<usize> {
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
            let reassign = ReassignVectors::new(&self.opts, snapshot, snapshot_epoch, reassigns, level);
            total_reassigned += reassign.execute(&self.state, delta).await?;
        }
        assert!(reassigns_by_level.is_empty());
        Ok(total_reassigned)
    }
}
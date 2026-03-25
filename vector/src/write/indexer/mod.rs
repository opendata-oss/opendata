mod drivers;
mod merge;
mod split;
pub(crate) mod state;
#[cfg(test)]
pub(crate) mod test_utils;
mod vector;

use crate::DistanceMetric;
use crate::Result;
use crate::write::indexer::merge::MergeCentroids;
use crate::write::indexer::split::SplitCentroids;
use crate::write::indexer::state::{CentroidChunkManager, VectorIndexDelta, VectorIndexState};
use crate::write::indexer::vector::{ReassignVectors, WriteVectors};
use crate::write::delta::VectorWrite;
use crate::hnsw::CentroidGraph;
use bytes::Bytes;
use common::StorageRead;
use common::sequence::AllocatedSeqBlock;
use common::storage::RecordOp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

const INDEXING_ROUNDS: usize = 1;

#[derive(Debug, Default)]
pub(crate) struct IndexerStats {
    pub(crate) inserts: usize,
    pub(crate) updates: usize,
    pub(crate) splits: usize,
    pub(crate) merges: usize,
    pub(crate) split_candidates_evaluated: usize,
    pub(crate) split_candidates_returned: usize,
    pub(crate) vectors_reassigned: usize,
}

pub(crate) struct IndexerOpts {
    pub(crate) dimensions: usize,
    pub(crate) distance_metric: DistanceMetric,
    pub(crate) merge_threshold_vectors: usize,
    pub(crate) split_threshold_vectors: usize,
    pub(crate) split_search_neighbourhood: usize,
    pub(crate) indexed_fields: HashSet<String>,
    pub(crate) chunk_target: usize,
}

pub(crate) struct Indexer {
    opts: Arc<IndexerOpts>,
    state: VectorIndexState,
}

impl Indexer {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        opts: IndexerOpts,
        dictionary: HashMap<String, u64>,
        centroid_counts: HashMap<u64, u64>,
        centroid_graph: Arc<dyn CentroidGraph>,
        sequence_block_key: Bytes,
        sequence_block: AllocatedSeqBlock,
        initial_chunk_id: u32,
        initial_chunk_count: usize,
    ) -> Self {
        let chunk_manager = CentroidChunkManager::new(
            opts.dimensions,
            opts.chunk_target,
            initial_chunk_id,
            initial_chunk_count,
        );
        Self {
            opts: Arc::new(opts),
            state: VectorIndexState::new(
                dictionary,
                centroid_counts,
                centroid_graph,
                sequence_block_key,
                sequence_block,
                chunk_manager,
            ),
        }
    }

    pub(crate) async fn update_index(
        &mut self,
        updates: Vec<VectorWrite>,
        snapshot: Arc<dyn StorageRead>,
    ) -> Result<(Vec<RecordOp>, IndexerStats)> {
        let mut stats = IndexerStats::default();
        let mut delta = VectorIndexDelta::new(&self.state);
        // write all vectors
        let write = WriteVectors::new(&self.opts, &snapshot, updates);
        let (inserts, updates) = write.execute(&self.state, &mut delta).await?;
        stats.inserts = inserts;
        stats.updates = updates;
        for _ in 0..INDEXING_ROUNDS {
            // apply merges
            let merge = MergeCentroids::new(&self.opts, &snapshot);
            let (reassigns, merge_count) = merge.execute(&self.state, &mut delta).await?;
            stats.merges += merge_count;
            // apply merge reassignments
            let reassign = ReassignVectors::new(&self.opts, &snapshot, reassigns);
            stats.vectors_reassigned += reassign.execute(&self.state, &mut delta).await?;
            // run split-reassign loop until no more splits. don't run merges as part of this loop
            // as it is not guaranteed to converge
            loop {
                let split = SplitCentroids::new(&self.opts, &snapshot);
                let result = split.execute(&self.state, &mut delta).await?;
                debug!("processed splits: {:?}", result);
                stats.splits += result.splits.len();
                stats.split_candidates_evaluated += result.candidates_evaluated;
                stats.split_candidates_returned += result.candidates_returned;
                if result.splits.is_empty() {
                    break;
                }
                let reassign = ReassignVectors::new(&self.opts, &snapshot, result.reassignments);
                stats.vectors_reassigned += reassign.execute(&self.state, &mut delta).await?;
            }
        }
        debug!(
            inserts = stats.inserts,
            updates = stats.updates,
            splits = stats.splits,
            merges = stats.merges,
            split_candidates_evaluated = stats.split_candidates_evaluated,
            split_candidates_returned = stats.split_candidates_returned,
            vectors_reassigned = stats.vectors_reassigned,
            "indexer update complete"
        );
        Ok((delta.freeze(&mut self.state), stats))
    }
}

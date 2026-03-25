mod drivers;
mod merge;
mod split;
pub(crate) mod state;
#[cfg(test)]
pub(crate) mod test_utils;
mod vector;

use crate::DistanceMetric;
use crate::Result;
use crate::batched::indexer::merge::MergeCentroids;
use crate::batched::indexer::split::SplitCentroids;
use crate::batched::indexer::state::{CentroidChunkManager, VectorIndexDelta, VectorIndexState};
use crate::batched::indexer::vector::{ReassignVectors, WriteVectors};
use crate::delta::VectorWrite;
use crate::hnsw::CentroidGraph;
use bytes::Bytes;
use common::StorageRead;
use common::sequence::AllocatedSeqBlock;
use common::storage::RecordOp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

const INDEXING_ROUNDS: usize = 1;

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
    ) -> Result<Vec<RecordOp>> {
        let mut delta = VectorIndexDelta::new(&self.state);
        // write all vectors
        let write = WriteVectors::new(&self.opts, &snapshot, updates);
        write.execute(&self.state, &mut delta).await?;
        for _ in 0..INDEXING_ROUNDS {
            // apply merges
            let merge = MergeCentroids::new(&self.opts, &snapshot);
            let reassigns = merge.execute(&self.state, &mut delta).await?;
            // apply merge reassignments
            let reassign = ReassignVectors::new(&self.opts, &snapshot, reassigns);
            reassign.execute(&self.state, &mut delta).await?;
            // run split-reassign loop until no more splits. don't run merges as part of this loop
            // as it is not guaranteed to converge
            loop {
                let split = SplitCentroids::new(&self.opts, &snapshot);
                let result = split.execute(&self.state, &mut delta).await?;
                debug!("processed splits: {:?}", result);
                if result.splits.is_empty() {
                    break;
                }
                let reassign = ReassignVectors::new(&self.opts, &snapshot, result.reassignments);
                reassign.execute(&self.state, &mut delta).await?;
            }
        }
        Ok(delta.freeze(&mut self.state))
    }
}

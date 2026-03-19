use crate::DistanceMetric;
use crate::batched::indexer::merge::MergeCentroids;
use crate::batched::indexer::split::SplitCentroids;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::batched::indexer::vector::{ReassignVectors, WriteVectors};
use crate::delta::VectorWrite;
use common::StorageRead;
use common::storage::RecordOp;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use bytes::Bytes;
use common::sequence::AllocatedSeqBlock;
use crate::hnsw::CentroidGraph;
use crate::Result;

pub(crate) struct IndexerOpts {
    pub(crate) dimensions: usize,
    pub(crate) distance_metric: DistanceMetric,
    pub(crate) merge_threshold_vectors: usize,
    pub(crate) split_threshold_vectors: usize,
    pub(crate) split_search_neighbourhood: usize,
    pub(crate) indexed_fields: HashSet<String>,
}

pub(crate) struct Indexer {
    opts: Arc<IndexerOpts>,
    state: VectorIndexState,
}

impl Indexer {
    pub(crate) fn new(
        opts: IndexerOpts,
        dictionary: HashMap<String, u64>,
        centroid_counts: HashMap<u64, u64>,
        centroid_graph: Arc<dyn CentroidGraph>,
        sequence_block_key: Bytes,
        sequence_block: AllocatedSeqBlock,
    ) -> Self {
        Self {
            opts: Arc::new(opts),
            state: VectorIndexState::new(
                dictionary,
                centroid_counts,
                centroid_graph,
                sequence_block_key,
                sequence_block
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
        write
            .execute(&self.state, &mut delta)
            .await?;
        // apply merges
        let merge = MergeCentroids::new(&self.opts, &snapshot);
        let reassigns = merge
            .execute(&self.state, &mut delta)
            .await?;
        // apply merge reassignments
        let reassign = ReassignVectors::new(&self.opts, &snapshot, reassigns);
        reassign
            .execute(&self.state, &mut delta)
            .await?;
        // run split-reassign loop until no more splits. don't run merges as part of this loop
        // as it is not guaranteed to converge
        loop {
            let split = SplitCentroids::new(&self.opts, &snapshot);
            let result = split
                .execute(&self.state, &mut delta)
                .await?;
            if result.splits == 0 {
                break;
            }
            let reassign = ReassignVectors::new(&self.opts, &snapshot, result.reassignments);
            reassign
                .execute(&self.state, &mut delta)
                .await?;
        }
        Ok(delta.freeze(&mut self.state))
    }
}

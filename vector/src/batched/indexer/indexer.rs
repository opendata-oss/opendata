use std::collections::HashSet;
use std::sync::Arc;
use common::storage::RecordOp;
use common::StorageRead;
use crate::batched::indexer::merge::MergeCentroids;
use crate::batched::indexer::split::SplitCentroids;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::batched::indexer::vector::{ReassignVectors, WriteVectors};
use crate::delta::VectorWrite;
use crate::DistanceMetric;

pub(crate) struct IndexerOpts {
    pub(crate) dimensions: usize,
    pub(crate) distance_metric: DistanceMetric,
    pub(crate) merge_threshold_vectors: usize,
    pub(crate) split_threshold_vectors: usize,
    pub(crate) split_search_neighbourhood: usize,
    pub(crate) indexed_fields: HashSet<String>,
}

struct Indexer {
    opts: Arc<IndexerOpts>,
    indexed_fields: HashSet<String>,
    dimensions: usize,
    state: VectorIndexState
}

impl Indexer {
    async fn update_index(
        &mut self,
        updates: Vec<VectorWrite>,
        snapshot: Arc<dyn StorageRead>
    ) -> Vec<RecordOp> {
        let mut delta = VectorIndexDelta::new(&self.state);
        // write all vectors
        let write = WriteVectors::new(&self.opts, &snapshot, updates);
        write.execute(&self.state, &mut delta).await.expect("failed write");
        // apply merges
        let merge = MergeCentroids::new(&self.opts, &snapshot);
        let reassigns = merge.execute(&self.state, &mut delta).await.expect("failed merge");
        // apply merge reassignments
        let reassign = ReassignVectors::new(&self.opts, &snapshot, reassigns);
        reassign.execute(&self.state, &mut delta).await.expect("failed reassign");
        // run split-reassign loop until no more splits. don't run merges as part of this loop
        // as it is not guaranteed to converge
        loop {
            let split = SplitCentroids::new(&self.opts, &snapshot);
            let result = split.execute(&self.state, &mut delta).await.expect("failed split");
            if result.splits == 0 {
                break;
            }
            let reassign = ReassignVectors::new(&self.opts, &snapshot, result.reassignments);
            reassign.execute(&self.state, &mut delta).await.expect("failed reassign");
        }
        delta.freeze(&mut self.state)
    }
}
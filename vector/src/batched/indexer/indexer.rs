use std::sync::Arc;
use common::storage::RecordOp;
use common::StorageRead;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::delta::VectorWrite;

struct Indexer {
    state: VectorIndexState
}

impl Indexer {
    fn update_index(
        &mut self,
        updates: Vec<VectorWrite>,
        snapshot: Arc<dyn StorageRead>
    ) -> Vec<RecordOp> {
        let delta = VectorIndexDelta::new(&self.state);
        // insert all vectors
        // apply merges
        // handle reassignment
        // split-reassign loop until no more splits
        delta.freeze(&mut self.state)
    }
}

struct ReassignVectors {

}

impl ReassignVectors {
    fn execute() {
        // resolve new assignments, filtering out vectors that dont need reassignment

        // get old vector data so inverted indexes can be updated

        // update postings/inverted indexes
    }
}

struct MergeCentroids {

}

impl MergeCentroids {
    fn execute() {
        // find all centroids that need to be merged

        // find neighbouring centroids that can be merge partners

        // execute merges (delete centroids only)

        // read all relevant postings

        // return reassign set
    }
}
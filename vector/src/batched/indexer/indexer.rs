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
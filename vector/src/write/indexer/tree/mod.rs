use std::collections::HashSet;
use std::sync::Arc;
use common::storage::RecordOp;
use common::StorageRead;
use crate::DistanceMetric;
use crate::write::delta::VectorWrite;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState};
use crate::Result;

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
        let inserts = 0;
        let updates = 0;
        let mut delta = VectorIndexDelta::new(&self.state);
        // write all vectors

        for level in 0..self.state.centroids_meta().depth as u16 {
            // apply merges

            // apply split reassign loop
        }

        // maybe split root

        let ops = delta.freeze(&mut self.state);
        let stats = IndexerStats {
            inserts: 0,
            updates: 0,
        };
        Ok((ops, stats))
    }
}
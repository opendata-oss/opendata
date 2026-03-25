use crate::batched::indexer::IndexerOpts;
use crate::batched::indexer::state::{CentroidChunkManager, VectorIndexDelta, VectorIndexState};
use crate::batched::indexer::vector::WriteVectors;
use crate::delta::VectorWrite;
use crate::hnsw::build_centroid_graph;
use crate::model::AttributeValue;
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;
use crate::storage::merge_operator::VectorDbMergeOperator;
use common::storage::in_memory::InMemoryStorage;
use common::{SequenceAllocator, Storage, StorageRead};
use std::collections::HashMap;
use std::sync::Arc;

pub struct IndexerOpTestHarness {
    pub storage: Arc<dyn Storage>,
    pub state: VectorIndexState,
}

impl IndexerOpTestHarness {
    /// Create a harness with a single centroid at the origin.
    pub async fn with_single_centroid(centroid_id: u64, dimensions: usize) -> Self {
        Self::with_centroids(
            vec![CentroidEntry::new(centroid_id, vec![0.0; dimensions])],
            dimensions,
        )
        .await
    }

    /// Create a harness with the given centroids.
    pub async fn with_centroids(centroids: Vec<CentroidEntry>, dimensions: usize) -> Self {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator::new(dimensions),
        )));
        let seq_key = bytes::Bytes::from_static(&[0x01, 0x02]);
        let allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();
        let centroid_counts: HashMap<u64, u64> =
            centroids.iter().map(|c| (c.centroid_id, 0u64)).collect();
        let graph = build_centroid_graph(centroids, DistanceMetric::L2).unwrap();
        let centroid_graph: Arc<dyn crate::hnsw::CentroidGraph> = Arc::from(graph);
        let (key, block) = allocator.freeze();
        let state = VectorIndexState::new(
            HashMap::new(),
            centroid_counts,
            centroid_graph,
            key,
            block,
            CentroidChunkManager::new(dimensions, 4096, 0, 1),
        );
        Self { storage, state }
    }

    /// Create a simple VectorWrite with a vector attribute.
    pub fn make_write(&self, id: &str, values: Vec<f32>) -> VectorWrite {
        VectorWrite {
            external_id: id.to_string(),
            values: values.clone(),
            attributes: vec![("vector".to_string(), AttributeValue::Vector(values))],
        }
    }

    /// Run WriteVectors, freeze the delta, and apply the resulting ops to storage.
    pub async fn write_and_apply(&mut self, opts: &Arc<IndexerOpts>, writes: Vec<VectorWrite>) {
        let snapshot = self.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&self.state);
        let wv = WriteVectors::new(opts, &(snapshot as Arc<dyn StorageRead>), writes);
        wv.execute(&self.state, &mut delta).await.unwrap();
        let ops = delta.freeze(&mut self.state);
        self.storage.apply(ops).await.unwrap();
    }
}

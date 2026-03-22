use crate::batched::delta::{VectorDbDeltaView, VectorDbWriteDelta};
use crate::batched::indexer::Indexer;
use async_trait::async_trait;
use common::Storage;
use common::coordinator::Flusher;
use common::storage::StorageSnapshot;
use std::ops::Range;
use std::sync::Arc;

pub(crate) struct VectorDbFlusher {
    storage: Arc<dyn Storage>,
    last_snapshot: Arc<dyn StorageSnapshot>,
    indexer: Indexer,
    /// Set after update_index succeeds but a subsequent storage operation fails.
    /// Once set, the in-memory index state is out of sync with storage and the
    /// flusher is no longer usable.
    poisoned: Option<String>,
}

impl VectorDbFlusher {
    pub(crate) fn new(
        storage: Arc<dyn Storage>,
        initial_snapshot: Arc<dyn StorageSnapshot>,
        indexer: Indexer,
    ) -> Self {
        Self {
            storage,
            last_snapshot: initial_snapshot,
            indexer,
            poisoned: None,
        }
    }
}

#[async_trait]
impl Flusher<VectorDbWriteDelta> for VectorDbFlusher {
    async fn flush_delta(
        &mut self,
        frozen: Arc<VectorDbDeltaView>,
        _epoch_range: &Range<u64>,
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        if let Some(err) = &self.poisoned {
            return Err(format!("flusher is poisoned due to prior error: {err}"));
        }

        // do indexing work — this mutates in-memory index state
        let updates = self
            .indexer
            .update_index(frozen.writes.clone(), self.last_snapshot.clone())
            .await
            .map_err(|e| e.to_string())?;

        // From this point, in-memory state has diverged from storage.
        // If any subsequent operation fails, poison the flusher.
        let result = self.apply_and_snapshot(updates).await;
        if let Err(err) = &result {
            self.poisoned = Some(err.clone());
        }
        result
    }

    async fn flush_storage(&self) -> Result<(), String> {
        self.storage.flush().await.map_err(|e| e.to_string())
    }
}

impl VectorDbFlusher {
    async fn apply_and_snapshot(
        &mut self,
        updates: Vec<common::storage::RecordOp>,
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        self.storage
            .apply(updates)
            .await
            .map_err(|e| e.to_string())?;

        let snapshot = self.storage.snapshot().await.map_err(|e| e.to_string())?;
        self.last_snapshot = snapshot.clone();
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batched::delta::VectorDbDeltaView;
    use crate::batched::indexer::{Indexer, IndexerOpts};
    use crate::delta::VectorWrite;
    use crate::hnsw::build_centroid_graph;
    use crate::model::AttributeValue;
    use crate::serde::centroid_chunk::CentroidEntry;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::key::{IdDictionaryKey, VectorDataKey};
    use crate::serde::vector_data::VectorDataValue;
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use common::coordinator::Flusher;
    use common::storage::in_memory::{FailingStorage, InMemoryStorage};
    use common::{SequenceAllocator, Storage};
    use std::collections::{HashMap, HashSet};

    const DIMS: usize = 3;

    /// Create an InMemoryStorage with the vector merge operator.
    fn create_storage() -> Arc<dyn Storage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator::new(DIMS),
        )))
    }

    /// Create a FailingStorage wrapping a storage with the vector merge operator.
    fn create_failing_storage() -> Arc<FailingStorage> {
        FailingStorage::wrap(create_storage())
    }

    /// Build a flusher with an Indexer set up for an empty db with one centroid.
    async fn create_flusher(storage: Arc<dyn Storage>) -> VectorDbFlusher {
        let seq_key = bytes::Bytes::from_static(&[0x01, 0x02]);
        let id_allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();

        // Bootstrap one centroid (gets ID 0)
        let centroid = CentroidEntry::new(0, vec![0.0; DIMS]);
        let graph = build_centroid_graph(vec![centroid], DistanceMetric::L2).unwrap();
        let centroid_graph: Arc<dyn crate::hnsw::CentroidGraph> = Arc::from(graph);

        let (seq_block_key, seq_block) = id_allocator.freeze();

        let indexer = Indexer::new(
            IndexerOpts {
                dimensions: DIMS,
                distance_metric: DistanceMetric::L2,
                merge_threshold_vectors: 0,
                split_threshold_vectors: usize::MAX,
                split_search_neighbourhood: 4,
                indexed_fields: HashSet::new(),
                chunk_target: 4096,
            },
            HashMap::new(),
            HashMap::from([(0, 0)]),
            centroid_graph,
            seq_block_key,
            seq_block,
            0,
            1,
        );

        let snapshot = storage.snapshot().await.unwrap();
        VectorDbFlusher::new(storage, snapshot, indexer)
    }

    fn make_writes(n: usize) -> Vec<VectorWrite> {
        (0..n)
            .map(|i| {
                let values = vec![i as f32, 0.0, 0.0];
                VectorWrite {
                    external_id: format!("vec-{i}"),
                    values: values.clone(),
                    attributes: vec![("vector".to_string(), AttributeValue::Vector(values))],
                }
            })
            .collect()
    }

    fn make_frozen(writes: Vec<VectorWrite>) -> Arc<VectorDbDeltaView> {
        Arc::new(VectorDbDeltaView { writes })
    }

    #[tokio::test]
    async fn should_write_vectors_to_storage() {
        // given
        let storage = create_storage();
        let mut flusher = create_flusher(storage.clone()).await;
        let writes = make_writes(10);
        let frozen = make_frozen(writes);

        // when
        let _snapshot = flusher.flush_delta(frozen, &(0..1)).await.unwrap();

        // then — verify forward index entries exist for all 10 vectors via raw storage reads
        let mut internal_ids = Vec::new();
        for i in 0..10 {
            let ext_id = format!("vec-{i}");
            // Check ID dictionary entry exists
            let dict_key = IdDictionaryKey::new(&ext_id).encode();
            let dict_record = storage
                .get(dict_key)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("missing dictionary entry for {ext_id}"));
            let internal_id = {
                let mut slice = dict_record.value.as_ref();
                common::serde::encoding::decode_u64(&mut slice).unwrap()
            };
            // Check vector data record exists and has the right external_id
            let data_key = VectorDataKey::new(internal_id).encode();
            let data_record = storage
                .get(data_key)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("missing vector data for {ext_id}"));
            let data = VectorDataValue::decode_from_bytes(&data_record.value, DIMS).unwrap();
            assert_eq!(data.external_id(), ext_id);
            internal_ids.push(internal_id);
        }

        // then — all internal IDs should be unique (10 distinct vectors written)
        let posting_ids: HashSet<u64> = internal_ids.iter().copied().collect();
        assert_eq!(posting_ids.len(), 10);
    }

    #[tokio::test]
    async fn should_propagate_apply_error() {
        // given
        let storage = create_failing_storage();
        let mut flusher = create_flusher(storage.clone() as Arc<dyn Storage>).await;
        storage.fail_apply(common::StorageError::Storage("test apply error".into()));

        // when
        let result = flusher
            .flush_delta(make_frozen(make_writes(1)), &(0..1))
            .await;

        // then
        let err = result.err().expect("expected apply error");
        assert!(
            err.contains("test apply error"),
            "expected test apply error message, got: {err}"
        );
    }

    #[tokio::test]
    async fn should_propagate_snapshot_error_after_apply() {
        // given
        let storage = create_failing_storage();
        let mut flusher = create_flusher(storage.clone() as Arc<dyn Storage>).await;
        storage.fail_snapshot(common::StorageError::Storage("test snapshot error".into()));

        // when
        let result = flusher
            .flush_delta(make_frozen(make_writes(1)), &(0..1))
            .await;

        // then
        let err = result.err().expect("expected snapshot error");
        assert!(
            err.contains("test snapshot error"),
            "expected test snapshot error message, got: {err}"
        );
    }

    #[tokio::test]
    async fn should_propagate_flush_storage_error() {
        // given
        let storage = create_failing_storage();
        let flusher = create_flusher(storage.clone() as Arc<dyn Storage>).await;
        storage.fail_flush(common::StorageError::Storage("test flush error".into()));

        // when
        let result = flusher.flush_storage().await;

        // then
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("test flush error"),
            "expected test flush error message"
        );
    }

    #[tokio::test]
    async fn should_poison_after_post_index_failure() {
        // given — apply fails after update_index mutates in-memory state
        let storage = create_failing_storage();
        let mut flusher = create_flusher(storage.clone() as Arc<dyn Storage>).await;
        storage.fail_apply(common::StorageError::Storage("apply failed".into()));

        // when — first flush fails due to apply error
        let result = flusher
            .flush_delta(make_frozen(make_writes(1)), &(0..1))
            .await;
        assert!(result.is_err());

        // then — subsequent flush is rejected immediately as poisoned
        storage.fail_apply(common::StorageError::Storage("should not reach".into()));
        let result = flusher
            .flush_delta(make_frozen(make_writes(1)), &(1..2))
            .await;
        let err = result.err().expect("expected poisoned error");
        assert!(
            err.contains("poisoned"),
            "expected poisoned error message, got: {err}"
        );
    }
}

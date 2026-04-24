use crate::db::LastAppliedSnapshot;
use crate::storage::record::build_write_batch;
use crate::write::delta::{VectorDbDeltaView, VectorDbWriteDelta};
use crate::write::indexer::tree::centroids::{
    CachedCentroidReader, CentroidCache, LeveledCentroidIndex, StoredCentroidReader,
};
use crate::write::indexer::tree::{IndexUpdateResults, Indexer};
use crate::{Config, DistanceMetric};
use async_trait::async_trait;
use common::coordinator::Flusher;
use slatedb::{Db, DbSnapshot};
use std::ops::Range;
use std::sync::{Arc, Mutex};

struct VectorDbFlusherOpts {
    dimensions: u16,
    distance_metric: DistanceMetric,
}

pub(crate) struct VectorDbFlusher {
    opts: VectorDbFlusherOpts,
    storage: Arc<Db>,
    last_snapshot: Arc<DbSnapshot>,
    last_snapshot_epoch: u64,
    indexer: Indexer,
    last_applied_snapshot: Arc<Mutex<LastAppliedSnapshot>>,
    /// Set after update_index succeeds but a subsequent storage operation fails.
    /// Once set, the in-memory index state is out of sync with storage and the
    /// flusher is no longer usable.
    poisoned: Option<String>,
}

impl VectorDbFlusher {
    pub(crate) fn new(
        config: &Config,
        storage: Arc<Db>,
        initial_snapshot: Arc<DbSnapshot>,
        initial_snapshot_epoch: u64,
        indexer: Indexer,
        last_applied_snapshot: Arc<Mutex<LastAppliedSnapshot>>,
    ) -> Self {
        Self {
            opts: VectorDbFlusherOpts {
                dimensions: config.dimensions,
                distance_metric: config.distance_metric,
            },
            storage,
            last_snapshot: initial_snapshot,
            last_snapshot_epoch: initial_snapshot_epoch,
            indexer,
            last_applied_snapshot,
            poisoned: None,
        }
    }
}

#[async_trait]
impl Flusher<VectorDbWriteDelta> for VectorDbFlusher {
    async fn flush_delta(
        &mut self,
        frozen: Arc<VectorDbDeltaView>,
        epoch_range: &Range<u64>,
    ) -> Result<Arc<DbSnapshot>, String> {
        if let Some(err) = &self.poisoned {
            return Err(format!("flusher is poisoned due to prior error: {err}"));
        }

        let update_epoch = epoch_range.end.saturating_sub(1);
        // do indexing work — this mutates in-memory index state
        let result = self
            .indexer
            .update_index(
                frozen.writes.clone(),
                update_epoch,
                self.last_snapshot.clone(),
                self.last_snapshot_epoch,
            )
            .await
            .map_err(|e| e.to_string())?;

        // From this point, in-memory state has diverged from storage.
        // If any subsequent operation fails, poison the flusher.
        let result = self.apply_and_snapshot(result, update_epoch).await;
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
    #[allow(unused_variables)]
    async fn validate(&self, snapshot: Arc<DbSnapshot>) {
        #[cfg(debug_assertions)]
        {
            self.indexer.validate(snapshot).await;
        }
    }

    async fn apply_and_snapshot(
        &mut self,
        index_outputs: IndexUpdateResults,
        snapshot_epoch: u64,
    ) -> Result<Arc<DbSnapshot>, String> {
        if !index_outputs.ops.is_empty() {
            let batch = build_write_batch(index_outputs.ops);
            self.storage.write(batch).await.map_err(|e| e.to_string())?;
        }

        let snapshot = self.storage.snapshot().await.map_err(|e| e.to_string())?;
        self.validate(snapshot.clone()).await;
        let stored_reader = StoredCentroidReader::new(
            self.opts.dimensions as usize,
            snapshot.clone(),
            snapshot_epoch,
        );
        let cached_reader = CachedCentroidReader::<DbSnapshot>::new(
            &(index_outputs.centroid_cache.clone() as Arc<dyn CentroidCache>),
            stored_reader,
        );
        let query_centroid_index = LeveledCentroidIndex::new(
            index_outputs.centroid_tree_depth,
            self.opts.distance_metric,
            Arc::new(cached_reader),
        );
        *self.last_applied_snapshot.lock().expect("lock poisoned") = LastAppliedSnapshot {
            snapshot: snapshot.clone(),
            centroid_cache: index_outputs.centroid_cache,
            centroid_index: Arc::new(query_centroid_index),
            centroid_count: index_outputs.leaf_centroids,
        };
        self.last_snapshot = snapshot.clone();
        self.last_snapshot_epoch = snapshot_epoch;
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AttributeValue;
    use crate::serde::Decode;
    use crate::serde::centroid_info::CentroidInfoValue;
    use crate::serde::centroid_stats::CentroidStatsValue;
    use crate::serde::centroids::CentroidsValue;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::key::{
        CentroidInfoKey, CentroidStatsKey, CentroidsKey, IdDictionaryKey, PostingListKey,
        VectorDataKey,
    };
    use crate::serde::posting_list::{PostingListValue, PostingUpdate};
    use crate::serde::vector_data::VectorDataValue;
    use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use crate::write::delta::VectorDbDeltaView;
    use crate::write::delta::VectorWrite;
    use crate::write::indexer::tree::IndexerOpts;
    use crate::write::indexer::tree::centroids::TreeDepth;
    use crate::write::indexer::tree::centroids::{
        AllCentroidsCacheWriter, CachedCentroidReader, CentroidCache, LeveledCentroidIndex,
        StoredCentroidReader,
    };
    use crate::write::indexer::tree::posting_list::{Posting, PostingList};
    use crate::write::indexer::tree::state::VectorIndexState;
    use common::SequenceAllocator;
    use common::coordinator::Flusher;
    use common::storage::testing::FailingObjectStore;
    use common::{StorageBuilder, StorageError};
    use slatedb::WriteBatch;
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    const DIMS: usize = 3;

    fn leaf_centroid_id(id: u64) -> VectorId {
        VectorId::centroid_id(1, id)
    }

    /// Create an in-memory Db with the vector merge operator.
    async fn create_storage() -> Arc<Db> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let built = StorageBuilder::from_object_store("test", object_store)
            .await
            .unwrap()
            .with_merge_operator(Arc::new(VectorDbMergeOperator::new(DIMS)))
            .build()
            .await
            .unwrap();
        built.db
    }

    /// Create an in-memory Db wrapped by a FailingObjectStore for fault injection.
    async fn create_failing_storage() -> (Arc<Db>, Arc<FailingObjectStore>) {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let failing = FailingObjectStore::new(inner);
        let obj_store: Arc<dyn ObjectStore> = failing.clone();
        let built = StorageBuilder::from_object_store("test", obj_store)
            .await
            .unwrap()
            .with_merge_operator(Arc::new(VectorDbMergeOperator::new(DIMS)))
            .build()
            .await
            .unwrap();
        (built.db, failing)
    }

    /// Build a flusher with an Indexer set up for an empty db with one centroid.
    async fn create_flusher(storage: Arc<Db>) -> VectorDbFlusher {
        let seq_key = bytes::Bytes::from_static(&[0x01, 0x02]);
        let id_allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();
        let centroid_seq_key = bytes::Bytes::from_static(&[0x01, 0x03]);
        let centroid_id_allocator = SequenceAllocator::load(storage.as_ref(), centroid_seq_key)
            .await
            .unwrap();

        let (seq_block_key, seq_block) = id_allocator.freeze();
        let (centroid_seq_block_key, centroid_seq_block) = centroid_id_allocator.freeze();
        let mut batch = WriteBatch::new();
        batch.put(
            CentroidsKey::new().encode(),
            CentroidsValue::new(3).encode_to_bytes(),
        );
        batch.put(
            PostingListKey::new(ROOT_VECTOR_ID).encode(),
            PostingListValue::from_posting_updates(vec![PostingUpdate::append(
                leaf_centroid_id(1),
                vec![0.0; DIMS],
            )])
            .unwrap()
            .encode_to_bytes(),
        );
        batch.put(
            CentroidInfoKey::new(leaf_centroid_id(1)).encode(),
            CentroidInfoValue::new(1, vec![0.0; DIMS], ROOT_VECTOR_ID).encode_to_bytes(),
        );
        batch.put(
            CentroidStatsKey::new(leaf_centroid_id(1)).encode(),
            CentroidStatsValue::new(0).encode_to_bytes(),
        );
        storage.write(batch).await.unwrap();
        let centroid_cache = AllCentroidsCacheWriter::new(
            Arc::new(PostingList::from_iter(vec![Posting::new(
                leaf_centroid_id(1),
                vec![0.0; DIMS],
            )])),
            vec![],
        );
        let state = VectorIndexState::new(
            HashMap::new(),
            crate::serde::centroids::CentroidsValue::new(3),
            1,
            HashMap::from([(
                leaf_centroid_id(1),
                CentroidInfoValue::new(1, vec![0.0; DIMS], ROOT_VECTOR_ID),
            )]),
            HashMap::from([(1, HashMap::from([(leaf_centroid_id(1), 0)]))]),
            seq_block_key,
            seq_block,
            centroid_seq_block_key,
            centroid_seq_block,
            centroid_cache,
        );
        let cache = Arc::new(state.centroid_cache());
        let snapshot = storage.snapshot().await.unwrap();
        let reader = Arc::new(CachedCentroidReader::new(
            &(cache.clone() as Arc<dyn CentroidCache>),
            StoredCentroidReader::new(DIMS, snapshot.clone(), 0),
        ));
        let query_centroid_index = Arc::new(LeveledCentroidIndex::new(
            TreeDepth::of(3),
            DistanceMetric::L2,
            reader,
        ));

        let config = Config {
            dimensions: DIMS as u16,
            distance_metric: DistanceMetric::L2,
            ..Default::default()
        };
        let indexer = Indexer::new(
            IndexerOpts {
                dimensions: DIMS,
                distance_metric: DistanceMetric::L2,
                root_threshold_vectors: usize::MAX,
                merge_threshold_vectors: 0,
                split_threshold_vectors: usize::MAX,
                split_search_neighbourhood: 4,
                indexed_fields: HashSet::new(),
            },
            state,
        );
        VectorDbFlusher::new(
            &config,
            storage,
            snapshot.clone(),
            0,
            indexer,
            Arc::new(Mutex::new(LastAppliedSnapshot {
                snapshot,
                centroid_cache: cache,
                centroid_index: query_centroid_index,
                centroid_count: 1,
            })),
        )
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

    // Suppress the unused warning for the StorageError import; it's used only by
    // reference in tests when matching object-store-level failure injection.
    #[allow(dead_code)]
    fn _ref_storage_error(_: StorageError) {}

    #[tokio::test]
    async fn should_write_vectors_to_storage() {
        // given
        let storage = create_storage().await;
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
            let dict_value = storage
                .get(&dict_key)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("missing dictionary entry for {ext_id}"));
            let internal_id = {
                let mut slice = dict_value.as_ref();
                VectorId::decode(&mut slice).unwrap()
            };
            // Check vector data record exists and has the right external_id
            let data_key = VectorDataKey::new(internal_id).encode();
            let data_value = storage
                .get(&data_key)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("missing vector data for {ext_id}"));
            let data = VectorDataValue::decode_from_bytes(&data_value, DIMS).unwrap();
            assert_eq!(data.external_id(), ext_id);
            internal_ids.push(internal_id);
        }

        // then — all internal IDs should be unique (10 distinct vectors written)
        let posting_ids: HashSet<_> = internal_ids.iter().copied().collect();
        assert_eq!(posting_ids.len(), 10);
    }

    #[tokio::test]
    async fn should_propagate_apply_error() {
        // given
        let (storage, failing) = create_failing_storage().await;
        let mut flusher = create_flusher(storage.clone()).await;
        // Inject a non-retryable Precondition (default FailKind); slatedb would
        // retry any other object_store::Error forever.
        failing.set_fail_put(true);

        // when
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            flusher.flush_delta(make_frozen(make_writes(1)), &(0..1)),
        )
        .await
        .expect("flush_delta hung past 5s; retry barrier is broken");

        // then — slatedb surfaces the injected failure as a storage error
        let err = result.err().expect("expected apply error");
        assert!(
            err.contains("injected failure") || err.to_lowercase().contains("fail"),
            "expected injected failure to surface, got: {err}"
        );
    }

    /// `db.snapshot()` fails with `SlateDBError::Closed` after the db is
    /// closed. An empty delta produces no index ops, so the write leg in
    /// `apply_and_snapshot` is skipped and the failure is isolated to the
    /// snapshot step.
    #[tokio::test]
    async fn should_propagate_snapshot_error() {
        // given
        let (storage, _failing) = create_failing_storage().await;
        let mut flusher = create_flusher(storage.clone()).await;
        // Close the db so the next db.snapshot() returns Closed
        storage.close().await.unwrap();

        // when — empty writes means index_outputs.ops is empty, so
        // apply_and_snapshot jumps straight to snapshot()
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            flusher.flush_delta(make_frozen(make_writes(0)), &(0..1)),
        )
        .await
        .expect("flush_delta hung past 5s");

        // then
        assert!(
            result.is_err(),
            "expected snapshot error after db.close(), got Ok"
        );
    }

    #[tokio::test]
    async fn should_propagate_flush_storage_error() {
        use slatedb::config::WriteOptions;
        // given — non-durable write so data stays in memtable, then inject a
        // non-retryable put failure; flush_storage drains the memtable via
        // object-store puts and should surface the error.
        let (storage, failing) = create_failing_storage().await;
        let flusher = create_flusher(storage.clone()).await;
        let mut batch = WriteBatch::new();
        batch.put(b"__probe", b"v");
        storage
            .write_with_options(
                batch,
                &WriteOptions {
                    await_durable: false,
                },
            )
            .await
            .unwrap();
        failing.set_fail_put(true);

        // when
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), flusher.flush_storage())
                .await
                .expect("flush_storage hung past 5s; retry barrier is broken");

        // then
        assert!(result.is_err(), "expected flush error, got Ok");
    }

    #[tokio::test]
    async fn should_poison_after_post_index_failure() {
        // given — put fails after update_index mutates in-memory state
        let (storage, failing) = create_failing_storage().await;
        let mut flusher = create_flusher(storage.clone()).await;
        failing.set_fail_put(true);

        // when — first flush fails due to put error
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            flusher.flush_delta(make_frozen(make_writes(1)), &(0..1)),
        )
        .await
        .expect("first flush_delta hung past 5s; retry barrier is broken");
        assert!(result.is_err());

        // then — subsequent flush is rejected immediately as poisoned
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            flusher.flush_delta(make_frozen(make_writes(1)), &(1..2)),
        )
        .await
        .expect("second flush_delta hung past 5s");
        let err = result.err().expect("expected poisoned error");
        assert!(
            err.contains("poisoned"),
            "expected poisoned error message, got: {err}"
        );
    }
}

use crate::AttributeValue;
use crate::Result;
use crate::hnsw::CentroidGraph;
use crate::serde::FieldValue;
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::key::{PostingListKey, VectorDataKey};
use crate::serde::posting_list::{
    PostingList, PostingListValue, PostingUpdate, merge_decoded_posting_lists,
};
use crate::serde::vector_data::{Field, VectorDataValue};
use crate::storage::{VectorDbStorageReadExt, record};
use bytes::Bytes;
use common::sequence::AllocatedSeqBlock;
use common::storage::RecordOp;
use common::{Record, SequenceAllocator, StorageRead};
use futures::future::BoxFuture;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct CentroidChunkManager {
    current_chunk_count: usize,
    current_chunk_id: u32,
    chunk_target: usize,
    dimensions: usize,
}

impl CentroidChunkManager {
    pub(crate) fn new(
        dimensions: usize,
        chunk_target: usize,
        current_chunk_id: u32,
        current_chunk_count: usize,
    ) -> Self {
        Self {
            current_chunk_count,
            current_chunk_id,
            chunk_target,
            dimensions,
        }
    }
}

impl CentroidChunkManager {
    fn allocate_centroids(&mut self, centroids: Vec<CentroidEntry>) -> Vec<RecordOp> {
        let mut chunk_batches: HashMap<u32, Vec<CentroidEntry>> = HashMap::new();
        for entry in centroids {
            if self.current_chunk_count >= self.chunk_target {
                self.current_chunk_id += 1;
                self.current_chunk_count = 0;
            }
            chunk_batches
                .entry(self.current_chunk_id)
                .or_default()
                .push(entry.clone());
            self.current_chunk_count += 1;
        }
        let mut ops = Vec::with_capacity(chunk_batches.len());
        for (chunk_id, entries) in chunk_batches {
            ops.push(record::merge_centroid_chunk(
                chunk_id,
                entries,
                self.dimensions,
            ));
        }
        ops
    }
}

/// In-memory preserved state of vector index
pub(crate) struct VectorIndexState {
    dictionary: HashMap<String, u64>,
    centroid_counts: HashMap<u64, u64>,
    centroid_graph: Arc<dyn CentroidGraph>,
    sequence_block_key: Bytes,
    sequence_block: AllocatedSeqBlock,
    chunk_manager: CentroidChunkManager,
}

impl VectorIndexState {
    pub(crate) fn new(
        dictionary: HashMap<String, u64>,
        centroid_counts: HashMap<u64, u64>,
        centroid_graph: Arc<dyn CentroidGraph>,
        sequence_block_key: Bytes,
        sequence_block: AllocatedSeqBlock,
        chunk_manager: CentroidChunkManager,
    ) -> Self {
        Self {
            dictionary,
            centroid_counts,
            centroid_graph,
            sequence_block_key,
            sequence_block,
            chunk_manager,
        }
    }

    pub(crate) fn dictionary(&self) -> &HashMap<String, u64> {
        &self.dictionary
    }

    pub(crate) fn centroid_counts(&self) -> &HashMap<u64, u64> {
        &self.centroid_counts
    }
}

pub(crate) struct VectorIndexDelta {
    new_centroids: HashMap<u64, CentroidEntry>,
    deleted_centroids: HashSet<u64>,
    centroid_count_deltas: HashMap<u64, i64>,
    posting_updates: HashMap<u64, Vec<PostingUpdate>>,
    inverted_index_updates: HashMap<Bytes, RoaringTreemap>,
    dictionary_updates: HashMap<String, u64>,
    vector_updates: HashMap<u64, VectorDataValue>,
    vector_deletes: HashSet<u64>,
    id_allocator: SequenceAllocator,
    current_posting: HashMap<u64, u64>,
    ops: Vec<RecordOp>,
}

impl VectorIndexDelta {
    pub(crate) fn new(initial_state: &VectorIndexState) -> Self {
        Self {
            new_centroids: HashMap::new(),
            deleted_centroids: HashSet::new(),
            centroid_count_deltas: HashMap::new(),
            posting_updates: HashMap::new(),
            inverted_index_updates: HashMap::new(),
            dictionary_updates: HashMap::new(),
            vector_updates: HashMap::new(),
            vector_deletes: HashSet::new(),
            current_posting: HashMap::new(),
            id_allocator: SequenceAllocator::new(
                initial_state.sequence_block_key.clone(),
                initial_state.sequence_block.clone(),
            ),
            ops: vec![],
        }
    }

    pub(crate) fn add_vector(
        &mut self,
        external_id: &str,
        attributes: &[(String, AttributeValue)],
    ) -> u64 {
        let (vector_id, seq_alloc_put) = self.id_allocator.allocate_one();
        if let Some(seq_alloc_put) = seq_alloc_put {
            self.ops.push(RecordOp::Put(seq_alloc_put.into()));
        }
        let fields: Vec<Field> = attributes
            .iter()
            .map(|(name, value)| Field::new(name, value.clone().into()))
            .collect();
        let value = VectorDataValue::new(external_id, fields);
        self.vector_updates.insert(vector_id, value);
        self.dictionary_updates
            .insert(String::from(external_id), vector_id);
        vector_id
    }

    pub(crate) fn delete_vector(&mut self, vector_id: u64) {
        self.vector_deletes.insert(vector_id);
    }

    pub(crate) fn add_centroid(&mut self, vector: Vec<f32>) -> CentroidEntry {
        let (id, seq_alloc_put) = self.id_allocator.allocate_one();
        if let Some(seq_alloc_put) = seq_alloc_put {
            self.ops.push(RecordOp::Put(seq_alloc_put.into()));
        }
        let centroid = CentroidEntry::new(id, vector);
        self.centroid_count_deltas.insert(id, 0);
        self.new_centroids.insert(id, centroid.clone());
        centroid
    }

    pub(crate) fn delete_centroids(&mut self, centroids: Vec<u64>) {
        for c in &centroids {
            self.centroid_count_deltas.remove(c);
            self.new_centroids.remove(c);
        }
        self.deleted_centroids.extend(centroids);
    }

    pub(crate) fn add_to_posting(&mut self, centroid_id: u64, vector_id: u64, vector: Vec<f32>) {
        self.current_posting.insert(vector_id, centroid_id);
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::append(vector_id, vector));
        let c = self.centroid_count_deltas.entry(centroid_id).or_insert(0);
        *c += 1;
    }

    pub(crate) fn remove_from_posting(&mut self, centroid_id: u64, vector_id: u64) {
        if self.current_posting.get(&vector_id).cloned() == Some(centroid_id) {
            self.current_posting.remove(&vector_id);
        }
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::delete(vector_id));
        let c = self.centroid_count_deltas.entry(centroid_id).or_insert(0);
        *c -= 1;
    }

    pub(crate) fn add_to_inverted_index(
        &mut self,
        field_name: String,
        field_value: FieldValue,
        vector_id: u64,
    ) {
        let key = crate::serde::key::MetadataIndexKey::new(field_name, field_value).encode();
        #[allow(clippy::unwrap_or_default)]
        self.inverted_index_updates
            .entry(key)
            .or_insert_with(RoaringTreemap::new)
            .insert(vector_id);
    }

    pub(crate) fn freeze(self, state: &mut VectorIndexState) -> Vec<RecordOp> {
        let VectorIndexDelta {
            new_centroids,
            deleted_centroids,
            centroid_count_deltas,
            posting_updates,
            inverted_index_updates,
            dictionary_updates,
            vector_updates,
            vector_deletes,
            id_allocator,
            current_posting: _,
            mut ops,
        } = self;

        // === Apply mutations to state ===

        // Update centroid counts
        for (&centroid_id, &delta) in &centroid_count_deltas {
            let count = state.centroid_counts.entry(centroid_id).or_insert(0);
            *count = count.saturating_add_signed(delta);
        }
        for &centroid_id in &deleted_centroids {
            state.centroid_counts.remove(&centroid_id);
        }

        // Update dictionary
        for (external_id, &internal_id) in &dictionary_updates {
            state.dictionary.insert(external_id.clone(), internal_id);
        }

        // Delete centroids from graph before adding new ones so new centroids
        // are not connected to deleted centroids.
        for &centroid_id in &deleted_centroids {
            let _ = state.centroid_graph.remove_centroid(centroid_id);
        }
        for entry in new_centroids.values() {
            let _ = state.centroid_graph.add_centroid(entry);
        }

        // Update sequence allocator
        let (key, block) = id_allocator.freeze();
        state.sequence_block_key = key;
        state.sequence_block = block;

        // === Construct record ops ===

        // Dictionary puts
        for (external_id, &internal_id) in &dictionary_updates {
            ops.push(record::put_id_dictionary(external_id, internal_id));
        }

        // Vector data puts
        for (vector_id, value) in vector_updates {
            let key = VectorDataKey::new(vector_id).encode();
            let encoded = value.encode_to_bytes();
            ops.push(RecordOp::Put(Record::new(key, encoded).into()));
        }

        // Vector data deletes
        for vector_id in &vector_deletes {
            ops.push(record::delete_vector_data(*vector_id));
        }

        // New centroid chunks
        let centroid_entries: Vec<CentroidEntry> = new_centroids.into_values().collect();
        if !centroid_entries.is_empty() {
            ops.extend(state.chunk_manager.allocate_centroids(centroid_entries));
        }

        // Posting list merges
        for (centroid_id, updates) in posting_updates {
            if let Ok(op) = record::merge_posting_list(centroid_id, updates) {
                ops.push(op);
            }
        }

        // Centroid stats deltas (skip deleted centroids)
        for (&centroid_id, &delta) in &centroid_count_deltas {
            if !deleted_centroids.contains(&centroid_id) && delta != 0 {
                ops.push(record::merge_centroid_stats(centroid_id, delta as i32));
            }
        }

        // Metadata inverted index merges
        for (encoded_key, vector_ids) in &inverted_index_updates {
            if let Ok(op) = record::merge_metadata_index_bitmap(encoded_key.clone(), vector_ids) {
                ops.push(op);
            }
        }

        // Deleted centroids bitmap
        if !deleted_centroids.is_empty() {
            let bitmap = deleted_centroids
                .iter()
                .copied()
                .collect::<RoaringTreemap>();
            let op = record::merge_deleted_vectors(bitmap)
                .expect("failure to construct deleted vectors row");
            ops.push(op);
        }

        // Centroid posting and stats tombstones at the end
        for &centroid_id in &deleted_centroids {
            let posting_key = PostingListKey::new(centroid_id).encode();
            ops.push(RecordOp::Delete(posting_key));
            let stats_key = crate::serde::key::CentroidStatsKey::new(centroid_id).encode();
            ops.push(RecordOp::Delete(stats_key));
        }

        ops
    }
}

pub(crate) struct VectorIndexView<'a> {
    delta: &'a VectorIndexDelta,
    state: &'a VectorIndexState,
    snapshot: Arc<dyn StorageRead>,
}

impl<'a> VectorIndexView<'a> {
    pub(crate) fn new(
        delta: &'a VectorIndexDelta,
        state: &'a VectorIndexState,
        snapshot: Arc<dyn StorageRead>,
    ) -> Self {
        Self {
            delta,
            state,
            snapshot,
        }
    }

    pub(crate) fn vector_id(&self, external_id: &str) -> Option<u64> {
        if let Some(id) = self.delta.dictionary_updates.get(external_id) {
            return Some(*id);
        }
        self.state.dictionary().get(external_id).cloned()
    }

    pub(crate) fn posting_list(
        &self,
        centroid_id: u64,
        dimensions: usize,
    ) -> Result<BoxFuture<'static, Result<PostingList>>> {
        let mut all_postings = Vec::with_capacity(2);
        if let Some(current) = self.delta.posting_updates.get(&centroid_id) {
            all_postings.push(PostingListValue::from_posting_updates(current.clone())?);
        }
        let snapshot = self.snapshot.clone();
        Ok(Box::pin(async move {
            all_postings.push(snapshot.get_posting_list(centroid_id, dimensions).await?);
            Ok(merge_decoded_posting_lists(all_postings).into())
        }))
    }

    pub(crate) fn vector_data(
        &self,
        vector_id: u64,
        dimensions: usize,
    ) -> BoxFuture<'static, Result<Option<VectorDataValue>>> {
        if self.delta.vector_deletes.contains(&vector_id) {
            Box::pin(async { Ok(None) })
        } else if let Some(d) = self.delta.vector_updates.get(&vector_id) {
            let v = d.clone();
            Box::pin(async move { Ok(Some(v)) })
        } else {
            let snapshot = self.snapshot.clone();
            Box::pin(async move { snapshot.get_vector_data(vector_id, dimensions).await })
        }
    }

    /// The last written posting for the vector in this delta only
    pub(crate) fn last_written_posting(&self, vector_id: u64) -> Option<u64> {
        self.delta.current_posting.get(&vector_id).cloned()
    }

    pub(crate) fn centroid_counts(&self) -> HashMap<u64, u64> {
        let mut counts = self.state.centroid_counts().clone();
        for (&k, &v) in self.delta.centroid_count_deltas.iter() {
            let base_count = counts.entry(k).or_insert(0);
            *base_count = base_count.saturating_add_signed(v);
        }
        counts
    }

    pub(crate) fn centroid_graph(&self) -> Arc<DirtyCentroidGraph> {
        Arc::new(DirtyCentroidGraph {
            new_centroids: self.delta.new_centroids.clone(),
            deleted_centroids: self.delta.deleted_centroids.clone(),
            inner: self.state.centroid_graph.clone(),
        })
    }
}

pub(crate) struct DirtyCentroidGraph {
    new_centroids: HashMap<u64, CentroidEntry>,
    deleted_centroids: HashSet<u64>,
    inner: Arc<dyn CentroidGraph>,
}

impl DirtyCentroidGraph {
    pub(crate) fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        let include: Vec<_> = self.new_centroids.values().collect();
        self.inner
            .search_with_include_exclude(query, k, &include, &self.deleted_centroids)
    }

    pub(crate) fn centroid(&self, centroid_id: u64) -> Option<CentroidEntry> {
        if self.deleted_centroids.contains(&centroid_id) {
            None
        } else if let Some(c) = self.new_centroids.get(&centroid_id) {
            Some(c.clone())
        } else {
            self.inner
                .get_centroid_vector(centroid_id)
                .map(|v| CentroidEntry::new(centroid_id, v))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::build_centroid_graph;
    use crate::model::AttributeValue;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::storage::VectorDbStorageReadExt;
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use common::storage::in_memory::InMemoryStorage;
    use common::{SequenceAllocator, Storage};

    const DIMS: usize = 2;

    /// Create storage + state for an empty db with one centroid (ID 1000) at the origin.
    async fn setup() -> (Arc<dyn Storage>, VectorIndexState) {
        setup_with_centroids(vec![CentroidEntry::new(1000, vec![0.0; DIMS])]).await
    }

    async fn setup_with_centroids(
        centroids: Vec<CentroidEntry>,
    ) -> (Arc<dyn Storage>, VectorIndexState) {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator::new(DIMS),
        )));
        let seq_key = Bytes::from_static(&[0x01, 0x02]);
        let allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();
        let counts: HashMap<u64, u64> = centroids.iter().map(|c| (c.centroid_id, 0u64)).collect();
        let graph = build_centroid_graph(centroids, DistanceMetric::L2).unwrap();
        let centroid_graph: Arc<dyn CentroidGraph> = Arc::from(graph);
        let (key, block) = allocator.freeze();
        let state = VectorIndexState::new(
            HashMap::new(),
            counts,
            centroid_graph,
            key,
            block,
            CentroidChunkManager::new(DIMS, 4096, 0, 1),
        );
        (storage, state)
    }

    fn make_attributes(values: Vec<f32>) -> Vec<(String, AttributeValue)> {
        vec![("vector".to_string(), AttributeValue::Vector(values))]
    }

    // ---- add_vector ----

    #[tokio::test]
    async fn add_vector_should_write_dictionary_and_vector_data() {
        // given
        let (storage, mut state) = setup().await;
        let mut delta = VectorIndexDelta::new(&state);

        // when
        let id = delta.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        let ops = delta.freeze(&mut state);
        storage.apply(ops).await.unwrap();

        // then
        assert_eq!(state.dictionary()["v1"], id);
        let stored_id = storage.lookup_internal_id("v1").await.unwrap().unwrap();
        assert_eq!(stored_id, id);
        let data = storage.get_vector_data(id, DIMS).await.unwrap().unwrap();
        assert_eq!(data.external_id(), "v1");
        assert_eq!(data.vector_field(), &[1.0, 2.0]);
    }

    // ---- delete_vector ----

    #[tokio::test]
    async fn delete_vector_should_remove_from_storage() {
        // given
        let (storage, mut state) = setup().await;
        let mut delta = VectorIndexDelta::new(&state);
        let id = delta.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // when
        let mut delta = VectorIndexDelta::new(&state);
        delta.delete_vector(id);
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // then
        assert!(storage.get_vector_data(id, DIMS).await.unwrap().is_none());
    }

    // ---- add_centroid ----

    #[tokio::test]
    async fn add_centroid_should_write_chunk_and_update_graph() {
        // given
        let (storage, mut state) = setup().await;
        let mut delta = VectorIndexDelta::new(&state);

        // when
        let entry = delta.add_centroid(vec![1.0, 0.0]);
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // then
        assert_eq!(
            state.centroid_graph.get_centroid_vector(entry.centroid_id),
            Some(vec![1.0, 0.0])
        );
        assert_eq!(*state.centroid_counts().get(&entry.centroid_id).unwrap(), 0);
        let scan = storage.scan_all_centroids(DIMS).await.unwrap();
        assert!(
            scan.entries
                .iter()
                .any(|e| e.centroid_id == entry.centroid_id)
        );
    }

    // ---- delete_centroids ----

    #[tokio::test]
    async fn delete_centroids_should_remove_from_graph_and_write_deletions() {
        // given — add a centroid with some postings (so stats exist)
        let (storage, mut state) = setup().await;
        let mut delta = VectorIndexDelta::new(&state);
        let entry = delta.add_centroid(vec![1.0, 0.0]);
        delta.add_to_posting(entry.centroid_id, 10, vec![1.0, 0.0]);
        storage.apply(delta.freeze(&mut state)).await.unwrap();
        // verify stats exist before deletion
        let stats = storage.get_centroid_stats(entry.centroid_id).await.unwrap();
        assert_eq!(stats.num_vectors, 1);

        // when
        let mut delta = VectorIndexDelta::new(&state);
        delta.delete_centroids(vec![entry.centroid_id]);
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // then
        assert!(
            state
                .centroid_graph
                .get_centroid_vector(entry.centroid_id)
                .is_none()
        );
        assert!(!state.centroid_counts().contains_key(&entry.centroid_id));
        let deletions = storage.get_deleted_vectors().await.unwrap();
        assert!(deletions.contains(entry.centroid_id));
        let stats = storage.get_centroid_stats(entry.centroid_id).await.unwrap();
        assert_eq!(stats.num_vectors, 0, "centroid stats should be deleted");
        assert!(
            storage
                .get_posting_list(entry.centroid_id, DIMS)
                .await
                .unwrap()
                .is_empty()
        );
    }

    // ---- add_to_posting ----

    #[tokio::test]
    async fn add_to_posting_should_write_posting_and_update_count() {
        let (storage, mut state) = setup().await;
        let mut delta = VectorIndexDelta::new(&state);

        delta.add_to_posting(1000, 10, vec![1.0, 0.0]);
        delta.add_to_posting(1000, 11, vec![0.0, 1.0]);
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // state
        assert_eq!(*state.centroid_counts().get(&1000).unwrap(), 2);

        // storage: posting list
        let posting = storage.get_posting_list(1000, DIMS).await.unwrap();
        assert_eq!(posting.len(), 2);
        let ids: HashSet<u64> = posting.iter().map(|p| p.id()).collect();
        assert!(ids.contains(&10));
        assert!(ids.contains(&11));

        // storage: centroid stats
        let stats = storage.get_centroid_stats(1000).await.unwrap();
        assert_eq!(stats.num_vectors, 2);
    }

    // ---- remove_from_posting ----

    #[tokio::test]
    async fn remove_from_posting_should_update_count_and_produce_posting_merge() {
        let (_storage, mut state) = setup().await;
        let mut delta = VectorIndexDelta::new(&state);

        // add two, remove one — all in the same delta
        delta.add_to_posting(1000, 10, vec![1.0, 0.0]);
        delta.add_to_posting(1000, 11, vec![0.0, 1.0]);
        delta.remove_from_posting(1000, 10);
        let ops = delta.freeze(&mut state);

        // state: net count = 1
        assert_eq!(*state.centroid_counts().get(&1000).unwrap(), 1);

        // ops: posting merge and stats merge are present
        let posting_key = PostingListKey::new(1000).encode();
        assert!(
            ops.iter()
                .any(|op| matches!(op, RecordOp::Merge(r) if r.record.key == posting_key))
        );
        let stats_key = crate::serde::key::CentroidStatsKey::new(1000).encode();
        assert!(
            ops.iter()
                .any(|op| matches!(op, RecordOp::Merge(r) if r.record.key == stats_key))
        );
    }

    // ---- add_to_inverted_index ----

    #[tokio::test]
    async fn add_to_inverted_index_should_write_metadata_bitmap() {
        let (storage, mut state) = setup().await;
        let mut delta = VectorIndexDelta::new(&state);

        delta.add_to_inverted_index(
            "color".to_string(),
            FieldValue::String("red".to_string()),
            10,
        );
        delta.add_to_inverted_index(
            "color".to_string(),
            FieldValue::String("red".to_string()),
            11,
        );
        delta.add_to_inverted_index(
            "color".to_string(),
            FieldValue::String("blue".to_string()),
            12,
        );
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        let red = storage
            .get_metadata_index("color", FieldValue::String("red".to_string()))
            .await
            .unwrap();
        assert_eq!(red.len(), 2);
        assert!(red.contains(10));
        assert!(red.contains(11));

        let blue = storage
            .get_metadata_index("color", FieldValue::String("blue".to_string()))
            .await
            .unwrap();
        assert_eq!(blue.len(), 1);
        assert!(blue.contains(12));
    }

    // ---- freeze: delete before add ----

    #[tokio::test]
    async fn freeze_should_delete_centroids_before_adding_new_ones() {
        let (_storage, mut state) = setup_with_centroids(vec![
            CentroidEntry::new(1000, vec![0.0; DIMS]),
            CentroidEntry::new(1001, vec![1.0, 1.0]),
        ])
        .await;

        let mut delta = VectorIndexDelta::new(&state);
        delta.delete_centroids(vec![1000]);
        let new_entry = delta.add_centroid(vec![1.0, 0.0]);
        delta.freeze(&mut state);

        assert!(state.centroid_graph.get_centroid_vector(1000).is_none());
        assert!(
            state
                .centroid_graph
                .get_centroid_vector(new_entry.centroid_id)
                .is_some()
        );
    }

    // ---- freeze: skip stats for deleted centroids ----

    #[tokio::test]
    async fn freeze_should_delete_stats_for_deleted_centroids() {
        let (storage, mut state) = setup().await;

        // Write some stats first
        let mut delta = VectorIndexDelta::new(&state);
        delta.add_to_posting(1000, 10, vec![1.0, 0.0]);
        delta.add_to_posting(1000, 11, vec![0.0, 1.0]);
        storage.apply(delta.freeze(&mut state)).await.unwrap();
        assert_eq!(
            storage.get_centroid_stats(1000).await.unwrap().num_vectors,
            2
        );

        // when — delete the centroid
        let mut delta = VectorIndexDelta::new(&state);
        delta.delete_centroids(vec![1000]);
        let ops = delta.freeze(&mut state);

        // then — should NOT have a stats merge (no point updating stats for deleted centroid)
        let stats_key = crate::serde::key::CentroidStatsKey::new(1000).encode();
        assert!(
            !ops.iter()
                .any(|op| matches!(op, RecordOp::Merge(r) if r.record.key == stats_key))
        );

        // then — SHOULD have a stats delete tombstone
        assert!(
            ops.iter()
                .any(|op| matches!(op, RecordOp::Delete(k) if k.as_ref() == stats_key.as_ref()))
        );

        // then — after applying, stats should be gone
        storage.apply(ops).await.unwrap();
        assert_eq!(
            storage.get_centroid_stats(1000).await.unwrap().num_vectors,
            0
        );
    }

    // ======== VectorIndexView tests ========

    // ---- vector_id ----

    #[tokio::test]
    async fn view_vector_id_should_prefer_delta_over_state() {
        let (storage, mut state) = setup().await;

        // Put "v1" in state via a prior delta
        let mut delta = VectorIndexDelta::new(&state);
        let old_id = delta.add_vector("v1", &make_attributes(vec![1.0, 0.0]));
        storage.apply(delta.freeze(&mut state)).await.unwrap();
        assert_eq!(state.dictionary()["v1"], old_id);

        // New delta adds "v1" again (upsert) — gets a new ID
        let mut delta = VectorIndexDelta::new(&state);
        let new_id = delta.add_vector("v1", &make_attributes(vec![2.0, 0.0]));
        assert_ne!(old_id, new_id);

        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);

        // view should return the delta's new ID, not the state's old one
        assert_eq!(view.vector_id("v1"), Some(new_id));
    }

    #[tokio::test]
    async fn view_vector_id_should_fall_back_to_state() {
        let (storage, mut state) = setup().await;

        // Put "v1" in state
        let mut delta = VectorIndexDelta::new(&state);
        let id = delta.add_vector("v1", &make_attributes(vec![1.0, 0.0]));
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // Fresh delta — no updates for "v1"
        let delta = VectorIndexDelta::new(&state);
        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);

        assert_eq!(view.vector_id("v1"), Some(id));
        assert_eq!(view.vector_id("unknown"), None);
    }

    // ---- vector_data ----

    #[tokio::test]
    async fn view_vector_data_should_return_delta_update_over_storage() {
        let (storage, mut state) = setup().await;

        // Write v1 to storage
        let mut delta = VectorIndexDelta::new(&state);
        let id = delta.add_vector("v1", &make_attributes(vec![1.0, 0.0]));
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // New delta overwrites v1
        let mut delta = VectorIndexDelta::new(&state);
        let new_id = delta.add_vector("v1", &make_attributes(vec![9.0, 9.0]));

        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);

        // delta's version wins
        let data = view.vector_data(new_id, DIMS).await.unwrap().unwrap();
        assert_eq!(data.vector_field(), &[9.0, 9.0]);

        // old ID falls through to storage
        let old_data = view.vector_data(id, DIMS).await.unwrap().unwrap();
        assert_eq!(old_data.vector_field(), &[1.0, 0.0]);
    }

    #[tokio::test]
    async fn view_vector_data_should_return_none_for_deleted() {
        let (storage, mut state) = setup().await;

        // Write v1 to storage
        let mut delta = VectorIndexDelta::new(&state);
        let id = delta.add_vector("v1", &make_attributes(vec![1.0, 0.0]));
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // New delta deletes v1
        let mut delta = VectorIndexDelta::new(&state);
        delta.delete_vector(id);

        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);

        assert!(view.vector_data(id, DIMS).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn view_vector_data_should_read_from_storage_for_unknown() {
        let (storage, mut state) = setup().await;

        // Write v1 to storage
        let mut delta = VectorIndexDelta::new(&state);
        let id = delta.add_vector("v1", &make_attributes(vec![1.0, 0.0]));
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // Fresh delta — no updates
        let delta = VectorIndexDelta::new(&state);
        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);

        let data = view.vector_data(id, DIMS).await.unwrap().unwrap();
        assert_eq!(data.vector_field(), &[1.0, 0.0]);

        // Unknown ID returns None
        assert!(view.vector_data(9999, DIMS).await.unwrap().is_none());
    }

    // ---- posting_list ----

    #[tokio::test]
    async fn view_posting_list_should_merge_delta_and_storage() {
        let (storage, mut state) = setup().await;

        // Write postings to storage
        let mut delta = VectorIndexDelta::new(&state);
        delta.add_to_posting(1000, 10, vec![1.0, 0.0]);
        storage.apply(delta.freeze(&mut state)).await.unwrap();

        // New delta adds another posting
        let mut delta = VectorIndexDelta::new(&state);
        delta.add_to_posting(1000, 11, vec![0.0, 1.0]);

        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);

        // Should see both: 10 from storage, 11 from delta
        let posting = view.posting_list(1000, DIMS).unwrap().await.unwrap();
        assert_eq!(posting.len(), 2);
        let ids: HashSet<u64> = posting.iter().map(|p| p.id()).collect();
        assert!(ids.contains(&10));
        assert!(ids.contains(&11));
    }

    // ---- last_written_posting ----

    #[tokio::test]
    async fn view_last_written_posting_should_reflect_delta() {
        let (storage, state) = setup().await;

        let mut delta = VectorIndexDelta::new(&state);
        delta.add_to_posting(1000, 10, vec![1.0, 0.0]);
        // Move vector 10 to a different centroid within the same delta
        delta.remove_from_posting(1000, 10);
        delta.add_to_posting(2000, 10, vec![1.0, 0.0]);

        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);

        // Should reflect the latest posting in the delta
        assert_eq!(view.last_written_posting(10), Some(2000));
        // Unknown vector returns None
        assert_eq!(view.last_written_posting(99), None);
    }

    // ---- centroid_counts ----

    #[tokio::test]
    async fn view_centroid_counts_should_merge_state_and_delta() {
        let (storage, mut state) = setup().await;

        // Add postings to state via a prior delta (centroid 1000 gets count=2)
        let mut delta = VectorIndexDelta::new(&state);
        delta.add_to_posting(1000, 10, vec![1.0, 0.0]);
        delta.add_to_posting(1000, 11, vec![0.0, 1.0]);
        storage.apply(delta.freeze(&mut state)).await.unwrap();
        assert_eq!(*state.centroid_counts().get(&1000).unwrap(), 2);

        // New delta adds one more posting
        let mut delta = VectorIndexDelta::new(&state);
        delta.add_to_posting(1000, 12, vec![0.5, 0.5]);

        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);

        // Should be state(2) + delta(+1) = 3
        let counts = view.centroid_counts();
        assert_eq!(*counts.get(&1000).unwrap(), 3);
    }

    // ---- centroid_graph (DirtyCentroidGraph) ----

    #[tokio::test]
    async fn view_centroid_graph_should_include_new_and_exclude_deleted() {
        let (storage, state) = setup_with_centroids(vec![
            CentroidEntry::new(1000, vec![0.0, 0.0]),
            CentroidEntry::new(1001, vec![1.0, 0.0]),
        ])
        .await;

        let mut delta = VectorIndexDelta::new(&state);
        // Delete centroid 1000, add a new one at [0, 1]
        delta.delete_centroids(vec![1000]);
        let new_c = delta.add_centroid(vec![0.0, 1.0]);

        let snapshot = storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &state, snapshot);
        let graph = view.centroid_graph();

        // Deleted centroid should not be found
        assert!(graph.centroid(1000).is_none());

        // Existing centroid should still be found
        assert!(graph.centroid(1001).is_some());

        // New centroid should be found
        assert!(graph.centroid(new_c.centroid_id).is_some());
        assert_eq!(
            graph.centroid(new_c.centroid_id).unwrap().vector,
            vec![0.0, 1.0]
        );

        // Search near [0, 1] should find new centroid, not deleted one
        let results = graph.search(&[0.0, 0.9], 1);
        assert_eq!(results[0], new_c.centroid_id);
    }
}

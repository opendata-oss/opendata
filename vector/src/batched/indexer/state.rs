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
            self.centroid_count_deltas.remove(&c);
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

        // Centroid posting tombstones at the end
        for &centroid_id in &deleted_centroids {
            let key = PostingListKey::new(centroid_id).encode();
            ops.push(RecordOp::Delete(key));
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
        self.state.dictionary.get(external_id).cloned()
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

    pub(crate) fn vector_data_for_external_id(
        &self,
        external_id: &str,
        dimensions: usize,
    ) -> BoxFuture<'static, Result<Option<(u64, VectorDataValue)>>> {
        let Some(vector_id) = self.vector_id(external_id) else {
            return Box::pin(async { Ok(None) });
        };
        let fut = self.vector_data(vector_id, dimensions);
        Box::pin(async move { Ok(fut.await?.map(|d| (vector_id, d))) })
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
        let mut counts = self.state.centroid_counts.clone();
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
            .search_with_include_exclude(&query, k, &include, &self.deleted_centroids)
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

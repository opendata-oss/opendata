use crate::AttributeValue;
use crate::Result;
use crate::serde::FieldValue;
use crate::serde::centroid_info::CentroidInfoEntry;
use crate::serde::centroids::CentroidsValue;
use crate::serde::key::VectorDataKey;
use crate::serde::posting_list::{
    PostingList, PostingListValue, PostingUpdate, merge_decoded_posting_lists,
};
use crate::serde::vector_data::{Field, VectorDataValue};
use crate::storage::{VectorDbStorageReadExt, record};
use crate::write::indexer::tree::centroids::{
    CentroidReader, LeveledCentroidIndex, StoredCentroidReader,
};
use bytes::Bytes;
use common::sequence::AllocatedSeqBlock;
use common::storage::RecordOp;
use common::{Record, SequenceAllocator, StorageRead};
use futures::future::BoxFuture;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// In-memory preserved state of vector index
pub(crate) struct VectorIndexState {
    dictionary: HashMap<String, u64>,
    centroids_meta: CentroidsValue,
    root_centroid_count: u64,
    centroids: HashMap<u64, CentroidInfoEntry>,
    centroid_counts: HashMap<u16, HashMap<u64, u64>>,
    sequence_block_key: Bytes,
    sequence_block: AllocatedSeqBlock,
    centroid_sequence_block_key: Bytes,
    centroid_sequence_block: AllocatedSeqBlock,
}

impl VectorIndexState {
    pub(crate) fn new(
        dictionary: HashMap<String, u64>,
        centroids_meta: CentroidsValue,
        root_centroid_count: u64,
        centroids: HashMap<u64, CentroidInfoEntry>,
        centroid_counts: HashMap<u16, HashMap<u64, u64>>,
        sequence_block_key: Bytes,
        sequence_block: AllocatedSeqBlock,
        centroid_sequence_block_key: Bytes,
        centroid_sequence_block: AllocatedSeqBlock,
    ) -> Self {
        Self {
            dictionary,
            centroids_meta,
            root_centroid_count,
            centroids,
            centroid_counts,
            sequence_block_key,
            sequence_block,
            centroid_sequence_block_key,
            centroid_sequence_block,
        }
    }

    pub(crate) fn centroids_meta(&self) -> &CentroidsValue {
        &self.centroids_meta
    }

    pub(crate) fn dictionary(&self) -> &HashMap<String, u64> {
        &self.dictionary
    }

    pub(crate) fn centroid_counts(&self) -> &HashMap<u16, HashMap<u64, u64>> {
        &self.centroid_counts
    }

    pub(crate) fn centroids(&self) -> &HashMap<u64, CentroidInfoEntry> {
        &self.centroids
    }
}

pub(crate) struct ForwardIndexDelta {
    dictionary_updates: HashMap<String, u64>,
    vector_updates: HashMap<u64, VectorDataValue>,
    vector_deletes: HashSet<u64>,
    id_allocator: SequenceAllocator,
    ops: Vec<RecordOp>,
}

impl ForwardIndexDelta {
    pub(crate) fn new(initial_state: &VectorIndexState) -> Self {
        Self {
            dictionary_updates: HashMap::new(),
            vector_updates: HashMap::new(),
            vector_deletes: HashSet::new(),
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

    pub(crate) fn freeze(self, state: &mut VectorIndexState, output_ops: &mut Vec<RecordOp>) {
        let ForwardIndexDelta {
            dictionary_updates,
            vector_updates,
            vector_deletes,
            id_allocator,
            ops,
        } = self;

        // === Apply mutations to state ===
        for (external_id, &internal_id) in &dictionary_updates {
            state.dictionary.insert(external_id.clone(), internal_id);
        }
        let (key, block) = id_allocator.freeze();
        state.sequence_block_key = key;
        state.sequence_block = block;

        // === Construct record ops ===
        output_ops.extend(ops.into_iter());

        // Dictionary puts
        for (external_id, &internal_id) in &dictionary_updates {
            output_ops.push(record::put_id_dictionary(external_id, internal_id));
        }

        // Vector data puts
        for (vector_id, value) in vector_updates {
            let key = VectorDataKey::new(vector_id).encode();
            let encoded = value.encode_to_bytes();
            output_ops.push(RecordOp::Put(Record::new(key, encoded).into()));
        }

        // Vector data deletes
        for vector_id in &vector_deletes {
            output_ops.push(record::delete_vector_data(*vector_id));
        }
    }
}

pub(crate) struct SearchIndexDelta {
    centroids_meta: Option<CentroidsValue>,
    upserted_centroids: HashMap<u64, CentroidInfoEntry>,
    deleted_centroids: HashSet<u64>,
    centroid_count_deltas: HashMap<u16, HashMap<u64, i64>>,
    root_count_delta: i64,
    root: Option<Vec<PostingUpdate>>,
    root_updates: Vec<PostingUpdate>,
    posting_updates: HashMap<u64, Vec<PostingUpdate>>,
    inverted_index_updates: HashMap<Bytes, RoaringTreemap>,
    id_allocator: SequenceAllocator,
    current_posting: HashMap<u64, u64>,
    ops: Vec<RecordOp>,
}

impl SearchIndexDelta {
    fn new(initial_state: &VectorIndexState) -> Self {
        Self {
            centroids_meta: None,
            upserted_centroids: HashMap::new(),
            deleted_centroids: HashSet::new(),
            centroid_count_deltas: HashMap::new(),
            root_count_delta: 0,
            root: None,
            root_updates: vec![],
            posting_updates: HashMap::new(),
            inverted_index_updates: HashMap::new(),
            id_allocator: SequenceAllocator::new(
                initial_state.centroid_sequence_block_key.clone(),
                initial_state.centroid_sequence_block.clone(),
            ),
            current_posting: HashMap::new(),
            ops: vec![],
        }
    }

    pub(crate) fn set_centroids_meta(&mut self, centroids_meta: CentroidsValue) {
        self.centroids_meta = Some(centroids_meta);
    }

    pub(crate) fn add_to_root(&mut self, centroid_id: u64, vector: Vec<f32>) {
        self.root_updates.push(PostingUpdate::Append {
            id: centroid_id,
            vector,
        });
        self.root_count_delta += 1;
    }

    pub(crate) fn remove_from_root(&mut self, centroid_id: u64) {
        self.root_updates
            .push(PostingUpdate::Delete { id: centroid_id });
        self.root_count_delta -= 1;
    }

    pub(crate) fn set_root(&mut self, postings: PostingList) {
        self.root_count_delta = postings.len() as i64;
        self.root = Some(postings.into_iter().map(|posting| posting.into()).collect());
        self.root_updates = vec![];
    }

    pub(crate) fn update_centroid(&mut self, centroid_id: u64, entry: CentroidInfoEntry) {
        self.upserted_centroids.insert(centroid_id, entry);
    }

    pub(crate) fn add_centroid(
        &mut self,
        level: u16,
        vector: Vec<f32>,
        parent: Option<u64>,
    ) -> (u64, CentroidInfoEntry) {
        let (id, seq_alloc_put) = self.id_allocator.allocate_one();
        if let Some(seq_alloc_put) = seq_alloc_put {
            self.ops.push(RecordOp::Put(seq_alloc_put.into()));
        }
        let centroid = CentroidInfoEntry::new(level as u8, vector, parent);
        let deltas = self
            .centroid_count_deltas
            .entry(level)
            .or_insert(HashMap::new());
        deltas.insert(id, 0);
        self.upserted_centroids.insert(id, centroid.clone());
        (id, centroid)
    }

    pub(crate) fn delete_centroids(&mut self, level: u16, centroids: Vec<u64>) {
        for c in &centroids {
            if let Some(deltas) = self.centroid_count_deltas.get_mut(&level) {
                deltas.remove(c);
            }
            self.upserted_centroids.remove(c);
        }
        self.deleted_centroids.extend(centroids);
    }

    pub(crate) fn add_to_posting(
        &mut self,
        level: u16,
        centroid_id: u64,
        vector_id: u64,
        vector: Vec<f32>,
    ) {
        self.current_posting.insert(vector_id, centroid_id);
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::append(vector_id, vector));
        let delta = self
            .centroid_count_deltas
            .entry(level)
            .or_insert(HashMap::new());
        let c = delta.entry(centroid_id).or_insert(0);
        *c += 1;
    }

    pub(crate) fn remove_from_posting(&mut self, level: u16, centroid_id: u64, vector_id: u64) {
        if self.current_posting.get(&vector_id).cloned() == Some(centroid_id) {
            self.current_posting.remove(&vector_id);
        }
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::delete(vector_id));
        let delta = self
            .centroid_count_deltas
            .entry(level)
            .or_insert(HashMap::new());
        let c = delta.entry(centroid_id).or_insert(0);
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

    pub(crate) fn freeze(self, state: &mut VectorIndexState, output_ops: &mut Vec<RecordOp>) {
        // TODO: fill me in
    }
}

pub(crate) struct VectorIndexDelta {
    pub(crate) forward_index: ForwardIndexDelta,
    pub(crate) search_index: SearchIndexDelta,
}

impl VectorIndexDelta {
    pub(crate) fn new(state: &VectorIndexState) -> Self {
        Self {
            forward_index: ForwardIndexDelta::new(&state),
            search_index: SearchIndexDelta::new(&state),
        }
    }

    pub(crate) fn freeze(self, state: &mut VectorIndexState) -> Vec<RecordOp> {
        let mut ops = vec![];
        self.forward_index.freeze(state, &mut ops);
        self.search_index.freeze(state, &mut ops);
        ops
    }
}

struct DirtyCentroidReader<'a> {
    stored: StoredCentroidReader,
    delta: &'a SearchIndexDelta,
}

impl<'a> CentroidReader for DirtyCentroidReader<'a> {
    fn read_root(&self) -> BoxFuture<'static, crate::Result<PostingListValue>> {
        if let Some(root) = &self.delta.root {
            let mut root = root.clone();
            root.extend(self.delta.root_updates.iter().cloned());
            Box::pin(async move { Ok(PostingListValue::from_posting_updates(root)?) })
        } else {
            let stored = self.stored.clone();
            let updates = self.delta.root_updates.clone();
            Box::pin(async move {
                let updates = PostingListValue::from_posting_updates(updates)?;
                let root = stored.read_root().await?;
                Ok(merge_decoded_posting_lists(vec![updates, root]))
            })
        }
    }

    fn read_postings(
        &self,
        centroid_id: u64,
    ) -> Result<BoxFuture<'static, crate::Result<PostingListValue>>> {
        // TODO: this is basically the same as the view below
        let mut all_postings = Vec::with_capacity(2);
        if let Some(current) = self.delta.posting_updates.get(&centroid_id) {
            all_postings.push(PostingListValue::from_posting_updates(current.clone())?);
        }
        let stored = self.stored.clone();
        Ok(Box::pin(async move {
            all_postings.push(stored.read_postings(centroid_id)?.await?);
            Ok(merge_decoded_posting_lists(all_postings).into())
        }))
    }
}

pub(crate) struct VectorIndexView<'a> {
    delta: &'a VectorIndexDelta,
    state: &'a VectorIndexState,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
}

impl<'a> VectorIndexView<'a> {
    pub(crate) fn new(
        delta: &'a VectorIndexDelta,
        state: &'a VectorIndexState,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Self {
        Self {
            delta,
            state,
            snapshot: snapshot.clone(),
            snapshot_epoch,
        }
    }

    pub(crate) fn centroids_meta(&self) -> &CentroidsValue {
        if let Some(meta) = self.delta.search_index.centroids_meta.as_ref() {
            meta
        } else {
            self.state.centroids_meta()
        }
    }

    pub(crate) fn vector_id(&self, external_id: &str) -> Option<u64> {
        if let Some(id) = self.delta.forward_index.dictionary_updates.get(external_id) {
            return Some(*id);
        }
        self.state.dictionary().get(external_id).cloned()
    }

    pub(crate) fn vector_data(
        &self,
        vector_id: u64,
        dimensions: usize,
    ) -> BoxFuture<'static, Result<Option<VectorDataValue>>> {
        if self.delta.forward_index.vector_deletes.contains(&vector_id) {
            Box::pin(async { Ok(None) })
        } else if let Some(d) = self.delta.forward_index.vector_updates.get(&vector_id) {
            let v = d.clone();
            Box::pin(async move { Ok(Some(v)) })
        } else {
            let snapshot = self.snapshot.clone();
            Box::pin(async move { snapshot.get_vector_data(vector_id, dimensions).await })
        }
    }

    pub(crate) fn centroid_counts(&self, level: u16) -> HashMap<u64, u64> {
        let mut counts = self
            .state
            .centroid_counts()
            .get(&level)
            .cloned()
            .unwrap_or_default();
        let Some(level_delta) = self.delta.search_index.centroid_count_deltas.get(&level) else {
            return counts;
        };
        for (&k, &v) in level_delta.iter() {
            let base_count = counts.entry(k).or_insert(0);
            *base_count = base_count.saturating_add_signed(v);
        }
        counts
    }

    pub(crate) fn root_count(&self) -> u64 {
        (self.state.root_centroid_count as i64 + self.delta.search_index.root_count_delta) as u64
    }

    pub(crate) fn root_posting_list(
        &self,
        dimensions: usize,
    ) -> Result<BoxFuture<'static, Result<PostingListValue>>> {
        let stored =
            StoredCentroidReader::new(dimensions, self.snapshot.clone(), self.snapshot_epoch);
        let reader = DirtyCentroidReader {
            stored,
            delta: &self.delta.search_index,
        };
        Ok(reader.read_root())
    }

    pub(crate) fn posting_list(
        &self,
        centroid_id: u64,
        dimensions: usize,
    ) -> Result<BoxFuture<'static, Result<PostingList>>> {
        let mut all_postings = Vec::with_capacity(2);
        if let Some(current) = self.delta.search_index.posting_updates.get(&centroid_id) {
            all_postings.push(PostingListValue::from_posting_updates(current.clone())?);
        }
        let snapshot = self.snapshot.clone();
        Ok(Box::pin(async move {
            all_postings.push(snapshot.get_posting_list(centroid_id, dimensions).await?);
            Ok(merge_decoded_posting_lists(all_postings).into())
        }))
    }

    /// The last written posting for the vector in this delta only
    pub(crate) fn last_written_posting(&self, vector_id: u64) -> Option<u64> {
        self.delta
            .search_index
            .current_posting
            .get(&vector_id)
            .cloned()
    }

    pub(crate) fn centroid(&self, centroid_id: u64) -> Option<&CentroidInfoEntry> {
        if let Some(centroid) = self.delta.search_index.upserted_centroids.get(&centroid_id) {
            Some(centroid)
        } else if self
            .delta
            .search_index
            .deleted_centroids
            .contains(&centroid_id)
        {
            None
        } else {
            self.state.centroids.get(&centroid_id)
        }
    }

    pub(crate) fn centroid_index(&self, dimensions: usize) -> LeveledCentroidIndex<'a> {
        let stored =
            StoredCentroidReader::new(dimensions, self.snapshot.clone(), self.snapshot_epoch);
        let reader = DirtyCentroidReader {
            stored,
            delta: &self.delta.search_index,
        };
        LeveledCentroidIndex::new(Arc::new(reader))
    }
}

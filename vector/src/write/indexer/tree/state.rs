use crate::AttributeValue;
use crate::Result;
use crate::serde::FieldValue;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::centroids::CentroidsValue;
use crate::serde::collection_meta::DistanceMetric;
use crate::serde::key::{
    CentroidInfoKey, CentroidStatsKey, CentroidsKey, PostingListKey, VectorDataKey,
};
use crate::serde::posting_list::{PostingList, PostingListValue, PostingUpdate};
use crate::serde::vector_data::{Field, VectorDataValue};
use crate::storage::{VectorDbStorageReadExt, record};
use crate::write::indexer::tree::centroids::{AllCentroidsCacheWriter, CachedCentroidReader, CentroidCache, CentroidReader, LeveledCentroidIndex, MaybeCached, StoredCentroidReader};
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
    centroids: HashMap<u64, CentroidInfoValue>,
    centroid_counts: HashMap<u16, HashMap<u64, u64>>,
    sequence_block_key: Bytes,
    sequence_block: AllocatedSeqBlock,
    centroid_sequence_block_key: Bytes,
    centroid_sequence_block: AllocatedSeqBlock,
    centroid_cache: AllCentroidsCacheWriter,
}

impl VectorIndexState {
    pub(crate) fn new(
        dictionary: HashMap<String, u64>,
        centroids_meta: CentroidsValue,
        root_centroid_count: u64,
        centroids: HashMap<u64, CentroidInfoValue>,
        centroid_counts: HashMap<u16, HashMap<u64, u64>>,
        sequence_block_key: Bytes,
        sequence_block: AllocatedSeqBlock,
        centroid_sequence_block_key: Bytes,
        centroid_sequence_block: AllocatedSeqBlock,
        centroid_cache: AllCentroidsCacheWriter,
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
            centroid_cache,
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

    pub(crate) fn centroids(&self) -> &HashMap<u64, CentroidInfoValue> {
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
    upserted_centroids: HashMap<u64, CentroidInfoValue>,
    deleted_centroids: HashSet<u64>,
    centroid_count_deltas: HashMap<u16, HashMap<u64, i64>>,
    root: Option<Vec<PostingUpdate>>,
    root_centroid_count: u64,
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
            root_centroid_count: initial_state.root_centroid_count,
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
        self.root_centroid_count += 1;
    }

    pub(crate) fn remove_from_root(&mut self, centroid_id: u64) {
        self.root_updates
            .push(PostingUpdate::Delete { id: centroid_id });
        assert!(self.root_centroid_count > 0);
        self.root_centroid_count = self.root_centroid_count.saturating_sub(1);
    }

    pub(crate) fn set_root(&mut self, postings: PostingList) {
        self.root_centroid_count = postings.len() as u64;
        self.root = Some(postings.into_iter().map(|posting| posting.into()).collect());
        self.root_updates = vec![];
    }

    pub(crate) fn update_centroid(&mut self, centroid_id: u64, entry: CentroidInfoValue) {
        self.upserted_centroids.insert(centroid_id, entry);
    }

    pub(crate) fn add_centroid(
        &mut self,
        level: u16,
        vector: Vec<f32>,
        parent: Option<u64>,
    ) -> (u64, CentroidInfoValue) {
        let (id, seq_alloc_put) = self.id_allocator.allocate_one();
        if let Some(seq_alloc_put) = seq_alloc_put {
            self.ops.push(RecordOp::Put(seq_alloc_put.into()));
        }
        let centroid = CentroidInfoValue::new(level as u8, vector, parent);
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

    pub(crate) fn freeze(
        self,
        epoch: u64,
        state: &mut VectorIndexState,
        output_ops: &mut Vec<RecordOp>
    ) {
        let SearchIndexDelta {
            centroids_meta,
            upserted_centroids,
            deleted_centroids,
            centroid_count_deltas,
            root_centroid_count,
            root,
            root_updates,
            posting_updates,
            inverted_index_updates,
            id_allocator,
            current_posting: _,
            ops,
        } = self;

        let deleted_centroid_levels: HashMap<u64, u8> = deleted_centroids
            .iter()
            .filter_map(|centroid_id| {
                state
                    .centroids
                    .get(centroid_id)
                    .map(|centroid| (*centroid_id, centroid.level))
            })
            .collect();

        // === Apply mutations to state ===
        if let Some(centroids_meta) = &centroids_meta {
            state.centroids_meta = centroids_meta.clone();
        }

        state.root_centroid_count = root_centroid_count;

        for (&level, deltas) in &centroid_count_deltas {
            let counts = state.centroid_counts.entry(level).or_default();
            for (&centroid_id, &delta) in deltas {
                let count = counts.entry(centroid_id).or_insert(0);
                *count = count.saturating_add_signed(delta);
            }
        }

        for centroid_id in &deleted_centroids {
            state.centroids.remove(centroid_id);
            for counts in state.centroid_counts.values_mut() {
                counts.remove(centroid_id);
            }
        }
        for (&centroid_id, centroid) in &upserted_centroids {
            state.centroids.insert(centroid_id, centroid.clone());
        }

        state.centroid_cache.update_postings(epoch, todo!(), todo!(), todo!(), todo!());

        let (key, block) = id_allocator.freeze();
        state.centroid_sequence_block_key = key;
        state.centroid_sequence_block = block;

        // === Construct record ops ===
        output_ops.extend(ops.into_iter());

        if let Some(centroids_meta) = centroids_meta {
            let key = CentroidsKey::new().encode();
            output_ops.push(RecordOp::Put(
                Record::new(key, centroids_meta.encode_to_bytes()).into(),
            ));
        }

        if let Some(root) = root {
            let key = PostingListKey::new(0).encode();
            let value = PostingListValue::from_posting_updates(root)
                .expect("root postings should always encode")
                .encode_to_bytes();
            output_ops.push(RecordOp::Put(Record::new(key, value).into()));
        } else if !root_updates.is_empty() {
            let op = record::merge_posting_list(0, root_updates)
                .expect("root posting updates should encode");
            output_ops.push(op);
        }

        for (centroid_id, centroid) in upserted_centroids {
            let key = CentroidInfoKey::new(centroid_id).encode();
            output_ops.push(RecordOp::Put(
                Record::new(key, centroid.encode_to_bytes()).into(),
            ));
        }

        for (centroid_id, updates) in posting_updates {
            if let Ok(op) = record::merge_posting_list(centroid_id, updates) {
                output_ops.push(op);
            }
        }

        for (&level, deltas) in &centroid_count_deltas {
            let Some(counts) = state.centroid_counts.get(&level) else {
                continue;
            };
            for &centroid_id in deltas.keys() {
                if !deleted_centroids.contains(&centroid_id) {
                    let count = counts
                        .get(&centroid_id)
                        .copied()
                        .expect("centroid count should be present after freeze");
                    let key = CentroidStatsKey::new(level as u8, centroid_id).encode();
                    let value = CentroidStatsValue::new(
                        i32::try_from(count).expect("centroid count should fit in i32"),
                    )
                    .encode_to_bytes();
                    output_ops.push(RecordOp::Put(Record::new(key, value).into()));
                }
            }
        }

        for (encoded_key, vector_ids) in &inverted_index_updates {
            if let Ok(op) = record::merge_metadata_index_bitmap(encoded_key.clone(), vector_ids) {
                output_ops.push(op);
            }
        }

        for centroid_id in &deleted_centroids {
            output_ops.push(RecordOp::Delete(
                CentroidInfoKey::new(*centroid_id).encode(),
            ));
            output_ops.push(RecordOp::Delete(PostingListKey::new(*centroid_id).encode()));
            if let Some(level) = deleted_centroid_levels.get(centroid_id) {
                output_ops.push(RecordOp::Delete(
                    CentroidStatsKey::new(*level, *centroid_id).encode(),
                ));
            }
        }
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

    pub(crate) fn freeze(self, epoch: u64, state: &mut VectorIndexState) -> Vec<RecordOp> {
        let mut ops = vec![];
        self.forward_index.freeze(state, &mut ops);
        self.search_index.freeze(epoch, state, &mut ops);
        ops
    }
}

struct DirtyCentroidReader<'a> {
    reader: CachedCentroidReader,
    delta: &'a SearchIndexDelta,
}

impl<'a> DirtyCentroidReader<'a> {
    fn apply_updates_to_posting(
        posting_list: &PostingList,
        updates: &[PostingUpdate],
    ) -> Arc<PostingList> {
        let mut all_updates = Vec::with_capacity(posting_list.len() + updates.len());
        all_updates.extend(
            posting_list
                .iter()
                .map(|p| PostingUpdate::append(p.id(), p.vector().to_vec())),
        );
        all_updates.extend(updates.iter().cloned());
        Arc::new(
            PostingListValue::from_posting_updates(all_updates)
                .expect("unreachable")
                .into(),
        )
    }
}

impl<'a> CentroidReader for DirtyCentroidReader<'a> {
    fn read_root(&self) -> MaybeCached<Arc<PostingList>> {
        if let Some(root) = &self.delta.root {
            let mut root = root.clone();
            root.extend(self.delta.root_updates.iter().cloned());
            let root = PostingListValue::from_posting_updates(root).expect("unreachable");
            MaybeCached::Value(Arc::new(root.into()))
        } else {
            let stored = self.reader.clone();
            let updates = self.delta.root_updates.clone();
            let root = stored.read_root();
            root.map(move |p| {
                if !updates.is_empty() {
                    Self::apply_updates_to_posting(p.as_ref(), &updates)
                } else {
                    p
                }
            })
        }
    }

    fn read_postings(&self, centroid_id: u64) -> MaybeCached<Arc<PostingList>> {
        let updates = self.delta.posting_updates.get(&centroid_id).cloned();
        let stored = self.reader.clone();
        let posting = stored.read_postings(centroid_id);
        posting.map(|p| {
            if let Some(updates) = updates {
                Self::apply_updates_to_posting(p.as_ref(), &updates)
            } else {
                p
            }
        })
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
        self.delta.search_index.root_centroid_count
    }

    pub(crate) fn root_posting_list(&self, dimensions: usize) -> MaybeCached<Arc<PostingList>> {
        self.centroid_reader(dimensions).read_root()
    }

    pub(crate) fn posting_list(
        &self,
        centroid_id: u64,
        dimensions: usize,
    ) -> MaybeCached<Arc<PostingList>> {
        self.centroid_reader(dimensions).read_postings(centroid_id)
    }

    /// The last written posting for the vector in this delta only
    pub(crate) fn last_written_posting(&self, vector_id: u64) -> Option<u64> {
        self.delta
            .search_index
            .current_posting
            .get(&vector_id)
            .cloned()
    }

    pub(crate) fn centroid(&self, centroid_id: u64) -> Option<&CentroidInfoValue> {
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

    fn centroid_reader(&self, dimensions: usize) -> DirtyCentroidReader<'a> {
        let stored =
            StoredCentroidReader::new(dimensions, self.snapshot.clone(), self.snapshot_epoch);
        let cached_reader = CachedCentroidReader::new(
            &(Arc::new(self.state.centroid_cache.cache()) as Arc<dyn CentroidCache>),
            stored
        );
        DirtyCentroidReader {
            reader: cached_reader,
            delta: &self.delta.search_index,
        }
    }

    pub(crate) fn centroid_index(
        &self,
        dimensions: usize,
        distance_metric: DistanceMetric,
    ) -> LeveledCentroidIndex<'a> {
        LeveledCentroidIndex::new(
            self.centroids_meta().depth as u16,
            distance_metric,
            Arc::new(self.centroid_reader(dimensions)),
        )
    }
}
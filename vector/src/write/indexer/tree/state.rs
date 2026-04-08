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
use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use crate::serde::vector_data::{Field, VectorDataValue};
use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
use crate::storage::{VectorDbStorageReadExt, record};
use crate::write::indexer::tree::centroids::{
    AllCentroidsCache, AllCentroidsCacheWriter, CachedCentroidReader, CentroidCache,
    CentroidReader, LeveledCentroidIndex, MaybeCached, StoredCentroidReader, TreeDepth, TreeLevel,
};
use crate::write::indexer::tree::posting_list::{Posting, PostingList};
use bytes::Bytes;
use common::sequence::AllocatedSeqBlock;
use common::storage::RecordOp;
use common::{Record, SequenceAllocator, StorageRead};
use futures::future::BoxFuture;
use log::info;
use roaring::RoaringTreemap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::debug;

/// In-memory preserved state of vector index
#[derive(Debug)]
pub(crate) struct VectorIndexState {
    dictionary: HashMap<String, VectorId>,
    centroids_meta: CentroidsValue,
    root_centroid_count: u64,
    centroids: HashMap<VectorId, CentroidInfoValue>,
    centroid_counts: HashMap<u8, HashMap<VectorId, u64>>,
    sequence_block_key: Bytes,
    sequence_block: AllocatedSeqBlock,
    centroid_sequence_block_key: Bytes,
    centroid_sequence_block: AllocatedSeqBlock,
    centroid_cache: AllCentroidsCacheWriter,
}

impl VectorIndexState {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        dictionary: HashMap<String, VectorId>,
        centroids_meta: CentroidsValue,
        root_centroid_count: u64,
        centroids: HashMap<VectorId, CentroidInfoValue>,
        centroid_counts: HashMap<u8, HashMap<VectorId, u64>>,
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

    pub(crate) fn dictionary(&self) -> &HashMap<String, VectorId> {
        &self.dictionary
    }

    pub(crate) fn centroid_counts(&self) -> &HashMap<u8, HashMap<VectorId, u64>> {
        &self.centroid_counts
    }

    pub(crate) fn root_centroid_count(&self) -> u64 {
        self.root_centroid_count
    }

    pub(crate) fn centroids(&self) -> &HashMap<VectorId, CentroidInfoValue> {
        &self.centroids
    }

    pub(crate) fn centroid_cache(&self) -> AllCentroidsCache {
        self.centroid_cache.cache()
    }
}

#[derive(Debug)]
pub(crate) struct ForwardIndexDelta {
    dictionary_updates: HashMap<String, VectorId>,
    vector_updates: HashMap<VectorId, VectorDataValue>,
    vector_deletes: HashSet<VectorId>,
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
    ) -> VectorId {
        let (vector_id, seq_alloc_put) = self.id_allocator.allocate_one();
        if let Some(seq_alloc_put) = seq_alloc_put {
            self.ops.push(RecordOp::Put(seq_alloc_put.into()));
        }
        let vector_id = VectorId::data_vector_id(vector_id);
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

    pub(crate) fn delete_vector(&mut self, vector_id: VectorId) {
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
        output_ops.extend(ops);

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

#[derive(Debug)]
pub(crate) struct SearchIndexDelta {
    centroids_meta: CentroidsValue,
    upserted_centroids: HashMap<VectorId, CentroidInfoValue>,
    new_centroids: HashSet<VectorId>,
    deleted_centroids: HashSet<VectorId>,
    centroid_count_deltas: HashMap<u8, HashMap<VectorId, i64>>,
    root: Option<Vec<PostingUpdate>>,
    root_centroid_count: u64,
    root_updates: Vec<PostingUpdate>,
    posting_updates: HashMap<VectorId, Vec<PostingUpdate>>,
    inverted_index_updates: HashMap<Bytes, RoaringTreemap>,
    id_allocator: SequenceAllocator,
    current_posting: HashMap<VectorId, VectorId>,
    ops: Vec<RecordOp>,
}

impl SearchIndexDelta {
    fn new(initial_state: &VectorIndexState) -> Self {
        Self {
            centroids_meta: initial_state.centroids_meta.clone(),
            upserted_centroids: HashMap::new(),
            new_centroids: HashSet::new(),
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

    pub(crate) fn promote_root(&mut self, new_root_centroids: Vec<Vec<f32>>) -> TreeLevel {
        self.centroids_meta.depth += 1;
        let depth = TreeDepth::of(self.centroids_meta.depth);
        let new_level = TreeLevel::root(depth).next_level_down();
        let mut new_root_postings = PostingList::with_capacity(new_root_centroids.len());
        for new_c_vec in new_root_centroids {
            let (new_c_id, new_c) = self.add_centroid(new_level, new_c_vec, ROOT_VECTOR_ID);
            info!("writing new root centroid {}/{}", new_level, new_c_id);
            new_root_postings.push(Posting::new(new_c_id, new_c.vector))
        }
        for p in &new_root_postings {
            assert_eq!(p.id().level(), depth.max_inner_level())
        }
        self.root_centroid_count = new_root_postings.len() as u64;
        self.root = Some(
            new_root_postings
                .into_iter()
                .map(|posting| posting.into())
                .collect(),
        );
        self.root_updates = Vec::new();
        new_level
    }

    pub(crate) fn add_to_root(&mut self, centroid_id: VectorId, vector: Vec<f32>) {
        debug!("adding new root centroid {:?}", centroid_id);
        let depth = TreeDepth::of(self.centroids_meta.depth);
        assert_eq!(depth.max_inner_level(), centroid_id.level());
        assert!(centroid_id.is_centroid());
        self.root_updates.push(PostingUpdate::Append {
            id: centroid_id,
            vector: Arc::new(vector),
        });
        self.root_centroid_count += 1;
    }

    pub(crate) fn remove_from_root(&mut self, centroid_id: VectorId) {
        debug!("removing root centroid {:?}", centroid_id);
        let depth = TreeDepth::of(self.centroids_meta.depth);
        assert_eq!(depth.max_inner_level(), centroid_id.level());
        self.root_updates
            .push(PostingUpdate::Delete { id: centroid_id });
        assert!(self.root_centroid_count > 0);
        self.root_centroid_count = self.root_centroid_count.saturating_sub(1);
    }

    pub(crate) fn update_centroid(&mut self, centroid_id: VectorId, entry: CentroidInfoValue) {
        assert_eq!(centroid_id.level(), entry.level);
        let depth = TreeDepth::of(self.centroids_meta.depth);
        assert_eq!(
            TreeLevel::of(centroid_id.level(), depth).next_level_up(),
            TreeLevel::of(entry.parent_vector_id.level(), depth)
        );
        self.upserted_centroids.insert(centroid_id, entry);
    }

    fn tree_level(&self, level: u8) -> TreeLevel {
        TreeLevel::of(level, TreeDepth::of(self.centroids_meta.depth))
    }

    #[cfg(test)]
    pub(crate) fn add_test_centroid(&mut self, centroid_id: VectorId, entry: CentroidInfoValue) {
        assert_eq!(centroid_id.level(), entry.level);
        let depth = TreeDepth::of(self.centroids_meta.depth);
        assert_eq!(
            TreeLevel::of(centroid_id.level(), depth).next_level_up(),
            TreeLevel::of(entry.parent_vector_id.level(), depth)
        );
        self.upserted_centroids.insert(centroid_id, entry);
        self.new_centroids.insert(centroid_id);
        self.centroid_count_deltas
            .entry(centroid_id.level())
            .or_default()
            .entry(centroid_id)
            .or_insert(0);
    }

    pub(crate) fn add_centroid(
        &mut self,
        level: TreeLevel,
        vector: Vec<f32>,
        parent: VectorId,
    ) -> (VectorId, CentroidInfoValue) {
        let parent_level = self.tree_level(parent.level());
        assert_eq!(level.next_level_up(), parent_level);
        let (id, seq_alloc_put) = self.id_allocator.allocate_one();
        let id = VectorId::centroid_id(level.level(), id);
        assert!(id.is_centroid());
        assert_eq!(level.level(), id.level());
        if let Some(seq_alloc_put) = seq_alloc_put {
            self.ops.push(RecordOp::Put(seq_alloc_put.into()));
        }
        let centroid = CentroidInfoValue::new(level.level(), vector, parent);
        let deltas = self.centroid_count_deltas.entry(id.level()).or_default();
        deltas.insert(id, 0);
        self.upserted_centroids.insert(id, centroid.clone());
        self.new_centroids.insert(id);
        (id, centroid)
    }

    pub(crate) fn delete_centroids(&mut self, centroids: Vec<VectorId>) {
        for c in &centroids {
            if let Some(deltas) = self.centroid_count_deltas.get_mut(&c.level()) {
                deltas.remove(c);
            }
            self.upserted_centroids.remove(c);
            self.posting_updates.remove(c);
            self.new_centroids.remove(c);
        }
        self.deleted_centroids.extend(centroids);
    }

    pub(crate) fn add_to_posting(
        &mut self,
        centroid_id: VectorId,
        vector_id: VectorId,
        vector: Vec<f32>,
    ) {
        assert_eq!(vector_id.level() + 1, centroid_id.level());
        debug!("postings({}): add vector {}", centroid_id, vector_id);
        self.current_posting.insert(vector_id, centroid_id);
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::append(vector_id, vector));
        let delta = self
            .centroid_count_deltas
            .entry(centroid_id.level())
            .or_default();
        let c = delta.entry(centroid_id).or_insert(0);
        *c += 1;
    }

    pub(crate) fn remove_from_posting(&mut self, centroid_id: VectorId, vector_id: VectorId) {
        assert_eq!(vector_id.level() + 1, centroid_id.level());
        debug!("postings({}): remove vector {}", centroid_id, vector_id);
        if let Some(current_posting) = self.current_posting.get(&vector_id).copied() {
            assert_eq!(current_posting, centroid_id);
            self.current_posting.remove(&vector_id);
        }
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::delete(vector_id));
        let delta = self
            .centroid_count_deltas
            .entry(centroid_id.level())
            .or_default();
        let c = delta.entry(centroid_id).or_insert(0);
        *c -= 1;
    }

    pub(crate) fn add_to_inverted_index(
        &mut self,
        field_name: String,
        field_value: FieldValue,
        vector_id: VectorId,
    ) {
        let key = crate::serde::key::MetadataIndexKey::new(field_name, field_value).encode();
        #[allow(clippy::unwrap_or_default)]
        self.inverted_index_updates
            .entry(key)
            .or_insert_with(RoaringTreemap::new)
            .insert(vector_id.id());
    }

    pub(crate) fn freeze(
        self,
        epoch: u64,
        state: &mut VectorIndexState,
        output_ops: &mut Vec<RecordOp>,
    ) {
        let SearchIndexDelta {
            centroids_meta,
            upserted_centroids,
            new_centroids,
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

        // === Apply mutations to state ===
        state.centroids_meta = centroids_meta.clone();

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

        let cache_posting_updates = posting_updates
            .iter()
            .filter_map(|(&centroid_id, updates)| {
                state.centroids.get(&centroid_id).and_then(|centroid| {
                    if centroid.level > 0 {
                        Some((centroid_id, updates.clone()))
                    } else {
                        None
                    }
                })
            })
            .collect();
        state.centroid_cache.update_postings(
            epoch,
            root.clone(),
            root_updates.clone(),
            &new_centroids,
            cache_posting_updates,
            &deleted_centroids,
        );

        let (key, block) = id_allocator.freeze();
        state.centroid_sequence_block_key = key;
        state.centroid_sequence_block = block;

        // === Construct record ops ===
        output_ops.extend(ops);

        let key = CentroidsKey::new().encode();
        output_ops.push(RecordOp::Put(
            Record::new(key, centroids_meta.encode_to_bytes()).into(),
        ));

        if let Some(root) = root {
            let key = PostingListKey::new(ROOT_VECTOR_ID).encode();
            let value = PostingListValue::from_posting_updates(root)
                .expect("root postings should always encode")
                .encode_to_bytes();
            output_ops.push(RecordOp::Put(Record::new(key, value).into()));
        }
        if !root_updates.is_empty() {
            let op = record::merge_posting_list(ROOT_VECTOR_ID, root_updates)
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
                    let key = CentroidStatsKey::new(centroid_id).encode();
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
            output_ops.push(RecordOp::Delete(
                CentroidStatsKey::new(*centroid_id).encode(),
            ));
        }
        debug!("centroid counts: {:?}", state.centroid_counts);
    }
}

#[derive(Debug)]
pub(crate) struct VectorIndexDelta {
    pub(crate) forward_index: ForwardIndexDelta,
    pub(crate) search_index: SearchIndexDelta,
}

impl VectorIndexDelta {
    pub(crate) fn new(state: &VectorIndexState) -> Self {
        Self {
            forward_index: ForwardIndexDelta::new(state),
            search_index: SearchIndexDelta::new(state),
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
        Arc::new(posting_list.update_in_place(updates.to_vec()))
    }
}

impl<'a> CentroidReader for DirtyCentroidReader<'a> {
    fn read_root(&self) -> MaybeCached<Arc<PostingList>> {
        if let Some(root) = &self.delta.root {
            let mut root = root.clone();
            root.extend(self.delta.root_updates.iter().cloned());
            let root = PostingListValue::from_posting_updates(root).expect("unreachable");
            MaybeCached::Value(Arc::new(PostingList::from_value(root)))
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

    fn read_postings(&self, centroid_id: VectorId) -> MaybeCached<Arc<PostingList>> {
        if centroid_id == ROOT_VECTOR_ID {
            return self.read_root();
        }
        if self.delta.deleted_centroids.contains(&centroid_id) {
            return MaybeCached::Value(Arc::new(PostingList::empty()));
        }
        let updates = self.delta.posting_updates.get(&centroid_id).cloned();
        let stored = self.reader.clone();
        let posting = stored.read_postings(centroid_id);
        posting.map(move |p| {
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
        &self.delta.search_index.centroids_meta
    }

    pub(crate) fn vector_id(&self, external_id: &str) -> Option<VectorId> {
        if let Some(id) = self.delta.forward_index.dictionary_updates.get(external_id) {
            return Some(*id);
        }
        self.state.dictionary().get(external_id).cloned()
    }

    pub(crate) fn vector_data(
        &self,
        vector_id: VectorId,
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

    pub(crate) fn centroid_counts(&self, level: TreeLevel) -> HashMap<VectorId, u64> {
        assert!(level.is_inner());
        let mut counts = self
            .state
            .centroid_counts()
            .get(&level.level())
            .cloned()
            .unwrap_or_default();
        let level_delta = self
            .delta
            .search_index
            .centroid_count_deltas
            .get(&level.level())
            .cloned()
            .unwrap_or_default();
        for (&k, &v) in level_delta.iter() {
            let base_count = counts.entry(k).or_insert(0);
            *base_count = base_count.saturating_add_signed(v);
        }
        for deleted in &self.delta.search_index.deleted_centroids {
            counts.remove(deleted);
        }
        counts
    }

    pub(crate) fn root_count(&self) -> u64 {
        self.delta.search_index.root_centroid_count
    }

    pub(crate) fn root_posting_list(&self, dimensions: usize) -> MaybeCached<Arc<PostingList>> {
        self.centroid_reader(dimensions).read_root().map(|p| {
            assert!(!p.is_empty());
            p
        })
    }

    pub(crate) fn posting_list(
        &self,
        centroid_id: VectorId,
        dimensions: usize,
    ) -> MaybeCached<Arc<PostingList>> {
        self.centroid_reader(dimensions).read_postings(centroid_id)
    }

    /// The last written posting for the vector in this delta only
    pub(crate) fn last_written_posting(&self, vector_id: VectorId) -> Option<VectorId> {
        self.delta
            .search_index
            .current_posting
            .get(&vector_id)
            .cloned()
    }

    pub(crate) fn centroid(&self, centroid_id: VectorId) -> Option<&CentroidInfoValue> {
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
            stored,
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
            TreeDepth::of(self.centroids_meta().depth),
            distance_metric,
            Arc::new(self.centroid_reader(dimensions)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AttributeValue;
    use crate::serde::FieldValue;
    use crate::serde::key::{CentroidSeqBlockKey, SeqBlockKey};
    use crate::storage::VectorDbStorageReadExt;
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use crate::write::indexer::tree::centroids::{CentroidCache, TreeDepth, TreeLevel};
    use common::Storage;
    use common::sequence::DEFAULT_BLOCK_SIZE;
    use common::serde::seq_block::SeqBlock;
    use common::storage::in_memory::InMemoryStorage;

    const DIMS: usize = 2;
    const EPOCH: u64 = 1;

    async fn setup() -> (Arc<dyn Storage>, VectorIndexState) {
        setup_with_prefill(0, 1).await
    }

    async fn setup_with_vector_allocator_prefill(
        prefill: u64,
    ) -> (Arc<dyn Storage>, VectorIndexState) {
        setup_with_prefill(prefill, 1).await
    }

    async fn setup_with_centroid_allocator_prefill(
        prefill: u64,
    ) -> (Arc<dyn Storage>, VectorIndexState) {
        setup_with_prefill(0, prefill).await
    }

    async fn setup_with_prefill(
        vector_prefill: u64,
        centroid_prefill: u64,
    ) -> (Arc<dyn Storage>, VectorIndexState) {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator::new(DIMS),
        )));

        let seq_key = SeqBlockKey.encode();
        let mut vector_allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();
        if vector_prefill > 0 {
            let (_, put) = vector_allocator.allocate(vector_prefill);
            if let Some(put) = put {
                storage.put(vec![put.into()]).await.unwrap();
            }
        }

        let centroid_seq_key = CentroidSeqBlockKey.encode();
        let mut centroid_allocator = SequenceAllocator::load(storage.as_ref(), centroid_seq_key)
            .await
            .unwrap();
        if centroid_prefill > 0 {
            let (_, put) = centroid_allocator.allocate(centroid_prefill);
            if let Some(put) = put {
                storage.put(vec![put.into()]).await.unwrap();
            }
        }

        let (sequence_block_key, sequence_block) = vector_allocator.freeze();
        let (centroid_sequence_block_key, centroid_sequence_block) = centroid_allocator.freeze();
        let centroid_cache = AllCentroidsCacheWriter::new(Arc::new(PostingList::empty()), vec![]);

        (
            storage,
            VectorIndexState::new(
                HashMap::new(),
                CentroidsValue::new(3),
                0,
                HashMap::new(),
                HashMap::new(),
                sequence_block_key,
                sequence_block,
                centroid_sequence_block_key,
                centroid_sequence_block,
                centroid_cache,
            ),
        )
    }

    fn make_attributes(values: Vec<f32>) -> Vec<(String, AttributeValue)> {
        vec![
            ("vector".to_string(), AttributeValue::Vector(values)),
            (
                "indexed_color".to_string(),
                AttributeValue::String("blue".to_string()),
            ),
            ("non_indexed_count".to_string(), AttributeValue::Int64(7)),
        ]
    }

    fn make_value(
        values: Vec<f32>,
        indexed_color: &str,
        non_indexed_count: i64,
    ) -> VectorDataValue {
        VectorDataValue::new(
            "v1",
            vec![
                Field::new("vector", AttributeValue::Vector(values).into()),
                Field::new(
                    "indexed_color",
                    AttributeValue::String(indexed_color.to_string()).into(),
                ),
                Field::new(
                    "non_indexed_count",
                    AttributeValue::Int64(non_indexed_count).into(),
                ),
            ],
        )
    }

    async fn apply_search_delta(
        storage: &Arc<dyn Storage>,
        state: &mut VectorIndexState,
        delta: SearchIndexDelta,
    ) {
        let mut ops = vec![];
        delta.freeze(EPOCH, state, &mut ops);
        storage.apply(ops).await.unwrap();
    }

    fn leaf_centroid(id: u64) -> VectorId {
        VectorId::centroid_id(1, id)
    }

    fn internal_centroid(level: u8, id: u64) -> VectorId {
        VectorId::centroid_id(level, id)
    }

    async fn assert_root_posting(
        storage: &Arc<dyn Storage>,
        state: &VectorIndexState,
        expected: Vec<(VectorId, Vec<f32>)>,
    ) {
        let expected_storage = PostingListValue::from_posting_updates(
            expected
                .iter()
                .map(|(id, vector)| PostingUpdate::append(*id, vector.clone()))
                .collect(),
        )
        .unwrap();
        let actual_storage = storage.get_root_posting_list(DIMS).await.unwrap();
        assert_eq!(actual_storage, expected_storage);

        let expected_cache = PostingList::from_value(expected_storage);
        let actual_cache = state.centroid_cache().root(u64::MAX).unwrap();
        assert_eq!(actual_cache.as_ref(), &expected_cache);
    }

    async fn seed_root_centroid(
        storage: &Arc<dyn Storage>,
        state: &mut VectorIndexState,
        vector: Vec<f32>,
    ) -> VectorId {
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(state);
        let (centroid_id, centroid) =
            delta.add_centroid(TreeLevel::leaf(depth), vector, ROOT_VECTOR_ID);
        delta.add_to_root(centroid_id, centroid.vector.clone());
        apply_search_delta(storage, state, delta).await;
        centroid_id
    }

    async fn seed_promoted_root(
        storage: &Arc<dyn Storage>,
        state: &mut VectorIndexState,
        vectors: Vec<Vec<f32>>,
    ) -> Vec<VectorId> {
        let mut delta = SearchIndexDelta::new(state);
        let new_level = delta.promote_root(vectors.clone());
        apply_search_delta(storage, state, delta).await;
        vectors
            .into_iter()
            .enumerate()
            .map(|(idx, _)| internal_centroid(new_level.level(), idx as u64 + 1))
            .collect()
    }

    fn postings_to_vec(postings: &PostingList) -> Vec<(VectorId, Vec<f32>)> {
        postings
            .iter()
            .map(|p| (p.id(), p.vector().to_vec()))
            .collect()
    }

    fn dirty_reader<'a>(
        delta: &'a SearchIndexDelta,
        state: &'a VectorIndexState,
        snapshot: Arc<dyn StorageRead>,
    ) -> DirtyCentroidReader<'a> {
        let stored = StoredCentroidReader::new(DIMS, snapshot, EPOCH);
        let cache: Arc<dyn CentroidCache> = Arc::new(state.centroid_cache());
        let cached = CachedCentroidReader::new(&cache, stored);
        DirtyCentroidReader {
            reader: cached,
            delta,
        }
    }

    fn vector_index_view<'a>(
        delta: &'a VectorIndexDelta,
        state: &'a VectorIndexState,
        snapshot: &'a Arc<dyn StorageRead>,
    ) -> VectorIndexView<'a> {
        VectorIndexView::new(delta, state, snapshot, EPOCH)
    }

    #[tokio::test]
    async fn add_vector_should_write_dictionary_and_vector_data() {
        // given:
        let (storage, mut state) = setup().await;
        let mut delta = ForwardIndexDelta::new(&state);

        // when:
        let id = delta.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        let mut ops = vec![];
        delta.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();

        // then:
        assert_eq!(state.dictionary()["v1"], id);
        assert_eq!(storage.lookup_internal_id("v1").await.unwrap(), Some(id));
        let data = storage.get_vector_data(id, DIMS).await.unwrap().unwrap();
        let expected = make_value(vec![1.0, 2.0], "blue", 7);
        assert_eq!(data, expected);
    }

    #[tokio::test]
    async fn delete_vector_should_remove_from_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let mut delta = ForwardIndexDelta::new(&state);
        let id = delta.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        let mut ops = vec![];
        delta.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();

        // when:
        let mut delta = ForwardIndexDelta::new(&state);
        delta.delete_vector(id);
        let mut ops = vec![];
        delta.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();

        // then:
        assert!(storage.get_vector_data(id, DIMS).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn add_multiple_vectors_should_allocate_unique_sequence_numbers() {
        // given:
        let (storage, mut state) = setup().await;
        let mut delta = ForwardIndexDelta::new(&state);

        // when:
        let id0 = delta.add_vector("v0", &make_attributes(vec![1.0, 0.0]));
        let id1 = delta.add_vector("v1", &make_attributes(vec![0.0, 1.0]));
        let id2 = delta.add_vector("v2", &make_attributes(vec![1.0, 1.0]));
        let mut ops = vec![];
        delta.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();

        // then:
        assert_eq!(id0, VectorId::data_vector_id(0));
        assert_eq!(id1, VectorId::data_vector_id(1));
        assert_eq!(id2, VectorId::data_vector_id(2));
        assert_ne!(id0, id1);
        assert_ne!(id0, id2);
        assert_ne!(id1, id2);
        assert_eq!(storage.lookup_internal_id("v0").await.unwrap(), Some(id0));
        assert_eq!(storage.lookup_internal_id("v1").await.unwrap(), Some(id1));
        assert_eq!(storage.lookup_internal_id("v2").await.unwrap(), Some(id2));
    }

    #[tokio::test]
    async fn add_vector_should_write_new_seq_block_when_allocator_returns_one() {
        // given:
        let (storage, mut state) =
            setup_with_vector_allocator_prefill(DEFAULT_BLOCK_SIZE - 1).await;
        let mut delta = ForwardIndexDelta::new(&state);

        // when:
        let id0 = delta.add_vector("v0", &make_attributes(vec![1.0, 0.0]));
        let id1 = delta.add_vector("v1", &make_attributes(vec![0.0, 1.0]));
        let mut ops = vec![];
        delta.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();

        // then:
        assert_eq!(id0, VectorId::data_vector_id(DEFAULT_BLOCK_SIZE - 1));
        assert_eq!(id1, VectorId::data_vector_id(DEFAULT_BLOCK_SIZE));
        let record = storage
            .get(SeqBlockKey.encode())
            .await
            .unwrap()
            .expect("seq block should be written");
        let block = SeqBlock::deserialize(&record.value).unwrap();
        assert_eq!(block.base_sequence, DEFAULT_BLOCK_SIZE);
        assert_eq!(block.block_size, DEFAULT_BLOCK_SIZE);
    }

    #[tokio::test]
    async fn promote_centroid_root_should_update_meta_storage_state_and_cache() {
        // given:
        let (storage, mut state) = setup().await;
        let mut delta = SearchIndexDelta::new(&state);

        // when:
        let new_level = delta.promote_root(vec![vec![1.0, 0.0], vec![0.0, 1.0]]);
        let mut ops = vec![];
        delta.freeze(EPOCH, &mut state, &mut ops);
        storage.apply(ops).await.unwrap();

        // then:
        assert_eq!(new_level.level(), 2);
        assert_eq!(state.centroids_meta(), &CentroidsValue::new(4));
        assert_eq!(
            storage.get_centroids_meta().await.unwrap(),
            Some(CentroidsValue::new(4))
        );
        assert_eq!(state.root_centroid_count(), 2);
        let root = PostingList::from_value(storage.get_root_posting_list(DIMS).await.unwrap());
        let expected = vec![
            (internal_centroid(2, 1), vec![1.0, 0.0]),
            (internal_centroid(2, 2), vec![0.0, 1.0]),
        ];
        assert_root_posting(&storage, &state, expected.clone()).await;
        for (centroid_id, vector) in expected {
            assert_eq!(
                storage.get_centroid_info(centroid_id).await.unwrap(),
                Some(CentroidInfoValue::new(2, vector, ROOT_VECTOR_ID))
            );
            assert_eq!(
                storage
                    .get_centroid_stats(centroid_id)
                    .await
                    .unwrap()
                    .num_vectors,
                0
            );
        }
        assert_eq!(root.len(), 2);
    }

    #[tokio::test]
    async fn add_root_centroid_should_update_storage_state_and_cache() {
        // given:
        let (storage, mut state) = setup().await;

        // when:
        let id = seed_root_centroid(&storage, &mut state, vec![1.0, 0.0]).await;

        // then:
        assert_eq!(state.root_centroid_count(), 1);
        assert_root_posting(&storage, &state, vec![(id, vec![1.0, 0.0])]).await;
    }

    #[tokio::test]
    async fn delete_root_centroid_should_update_storage_state_and_cache() {
        // given:
        let (storage, mut state) = setup().await;
        let id0 = seed_root_centroid(&storage, &mut state, vec![1.0, 0.0]).await;
        let id1 = seed_root_centroid(&storage, &mut state, vec![0.0, 1.0]).await;

        // when:
        let mut delta = SearchIndexDelta::new(&state);
        delta.remove_from_root(id0);
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        assert_eq!(state.root_centroid_count(), 1);
        let posting = PostingList::from_value(storage.get_root_posting_list(DIMS).await.unwrap());
        assert_eq!(
            posting
                .iter()
                .map(|p| (p.id(), p.vector().to_vec()))
                .collect::<Vec<_>>(),
            vec![(id1, vec![0.0, 1.0])]
        );
        let cached = state.centroid_cache().root(u64::MAX).unwrap();
        assert_eq!(cached.as_ref(), &posting);
    }

    #[tokio::test]
    async fn promote_centroid_root_and_add_to_root_should_write_correct_root_posting() {
        // given:
        let (storage, mut state) = setup().await;
        let mut delta = SearchIndexDelta::new(&state);

        // when:
        let new_level = delta.promote_root(vec![vec![1.0, 0.0], vec![0.0, 1.0]]);
        let (extra_id, extra) = delta.add_centroid(new_level, vec![2.0, 2.0], ROOT_VECTOR_ID);
        delta.add_to_root(extra_id, extra.vector.clone());
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        assert_root_posting(
            &storage,
            &state,
            vec![
                (internal_centroid(2, 1), vec![1.0, 0.0]),
                (internal_centroid(2, 2), vec![0.0, 1.0]),
                (internal_centroid(2, 3), vec![2.0, 2.0]),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn add_inner_centroid_should_write_centroid_info_and_count() {
        // given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);

        // when:
        let (child_id, child) = delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        assert_eq!(
            storage.get_centroid_info(child_id).await.unwrap(),
            Some(child.clone())
        );
        assert_eq!(
            storage
                .get_centroid_stats(child_id)
                .await
                .unwrap()
                .num_vectors,
            0
        );
        assert_eq!(
            state
                .centroid_counts()
                .get(&1)
                .and_then(|m| m.get(&child_id))
                .copied(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn add_multiple_inner_centroids_should_allocate_unique_ids() {
        // given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);

        // when:
        let (id0, _) = delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        let (id1, _) = delta.add_centroid(TreeLevel::leaf(depth), vec![4.0, 4.0], parent);
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        assert_ne!(id0, id1);
        assert!(storage.get_centroid_info(id0).await.unwrap().is_some());
        assert!(storage.get_centroid_info(id1).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn add_inner_centroid_should_write_new_sequence_block_when_allocator_returns_one() {
        // given:
        let (storage, mut state) =
            setup_with_centroid_allocator_prefill(DEFAULT_BLOCK_SIZE - 1).await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);

        // when:
        let (id, _) = delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        assert_eq!(id, leaf_centroid(DEFAULT_BLOCK_SIZE));
        let record = storage
            .get(CentroidSeqBlockKey.encode())
            .await
            .unwrap()
            .expect("centroid seq block should be written");
        let block = SeqBlock::deserialize(&record.value).unwrap();
        assert_eq!(block.base_sequence, DEFAULT_BLOCK_SIZE);
        assert_eq!(block.block_size, DEFAULT_BLOCK_SIZE);
    }

    #[tokio::test]
    async fn delete_inner_centroid_should_remove_it_from_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);
        let (child_id, _) = delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        apply_search_delta(&storage, &mut state, delta).await;

        // when:
        let mut delta = SearchIndexDelta::new(&state);
        delta.delete_centroids(vec![child_id]);
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        assert!(storage.get_centroid_info(child_id).await.unwrap().is_none());
        assert_eq!(
            storage
                .get_centroid_stats(child_id)
                .await
                .unwrap()
                .num_vectors,
            0
        );
        assert!(!state.centroids().contains_key(&child_id));
    }

    #[tokio::test]
    async fn add_vector_to_posting_should_update_storage_cache_and_counts() {
        // given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);

        // when:
        let (child_id, child) = delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        delta.add_to_posting(parent, child_id, child.vector.clone());
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        let posting =
            PostingList::from_value(storage.get_posting_list(parent, DIMS).await.unwrap());
        assert_eq!(
            posting
                .iter()
                .map(|p| (p.id(), p.vector().to_vec()))
                .collect::<Vec<_>>(),
            vec![(child_id, vec![3.0, 3.0])]
        );
        let cached = state.centroid_cache().posting(parent, u64::MAX).unwrap();
        assert_eq!(cached.as_ref(), &posting);
        assert_eq!(
            storage
                .get_centroid_stats(parent)
                .await
                .unwrap()
                .num_vectors,
            1
        );
        assert_eq!(
            state
                .centroid_counts()
                .get(&parent.level())
                .and_then(|m| m.get(&parent))
                .copied(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn delete_vector_from_posting_should_update_storage_cache_and_counts() {
        //given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);
        let (child_id, child) = delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        delta.add_to_posting(parent, child_id, child.vector.clone());
        apply_search_delta(&storage, &mut state, delta).await;

        // when:
        let mut delta = SearchIndexDelta::new(&state);
        delta.remove_from_posting(parent, child_id);
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        let posting =
            PostingList::from_value(storage.get_posting_list(parent, DIMS).await.unwrap());
        assert!(posting.is_empty());
        let cached = state.centroid_cache().posting(parent, u64::MAX).unwrap();
        assert_eq!(cached.as_ref(), &posting);
        assert_eq!(
            storage
                .get_centroid_stats(parent)
                .await
                .unwrap()
                .num_vectors,
            0
        );
        assert_eq!(
            state
                .centroid_counts()
                .get(&parent.level())
                .and_then(|m| m.get(&parent))
                .copied(),
            Some(0)
        );
    }

    #[tokio::test]
    async fn update_centroid_should_write_updated_centroid_to_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);
        let (child_id, _) = delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        apply_search_delta(&storage, &mut state, delta).await;

        // when:
        let mut delta = SearchIndexDelta::new(&state);
        let updated = CentroidInfoValue::new(1, vec![9.0, 9.0], parent);
        delta.update_centroid(child_id, updated.clone());
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        assert_eq!(
            storage.get_centroid_info(child_id).await.unwrap(),
            Some(updated)
        );
    }

    #[tokio::test]
    async fn add_to_inverted_index_should_write_bitmap_to_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let mut delta = SearchIndexDelta::new(&state);

        // when:
        delta.add_to_inverted_index(
            "indexed_color".to_string(),
            FieldValue::String("blue".to_string()),
            VectorId::data_vector_id(10),
        );
        delta.add_to_inverted_index(
            "indexed_color".to_string(),
            FieldValue::String("blue".to_string()),
            VectorId::data_vector_id(11),
        );
        apply_search_delta(&storage, &mut state, delta).await;

        // then:
        let value = storage
            .get_metadata_index("indexed_color", FieldValue::String("blue".to_string()))
            .await
            .unwrap();
        assert_eq!(value.vector_ids.len(), 2);
        assert!(value.vector_ids.contains(10));
        assert!(value.vector_ids.contains(11));
    }

    #[tokio::test]
    async fn read_root_should_read_root_from_delta_and_apply_updates_if_present() {
        // given:
        let (storage, state) = setup().await;
        let mut delta = SearchIndexDelta::new(&state);
        let new_level = delta.promote_root(vec![vec![1.0, 0.0], vec![0.0, 1.0]]);
        let (extra_id, extra) = delta.add_centroid(new_level, vec![2.0, 2.0], ROOT_VECTOR_ID);
        delta.add_to_root(extra_id, extra.vector.clone());
        let snapshot = storage.snapshot().await.unwrap();
        let reader = dirty_reader(&delta, &state, snapshot);

        // when:
        let root = reader.read_root().get().await.unwrap();

        // then:
        assert_eq!(
            postings_to_vec(root.as_ref()),
            vec![
                (internal_centroid(2, 1), vec![1.0, 0.0]),
                (internal_centroid(2, 2), vec![0.0, 1.0]),
                (internal_centroid(2, 3), vec![2.0, 2.0]),
            ]
        );
    }

    #[tokio::test]
    async fn read_root_should_read_root_from_storage_and_apply_updates_if_present() {
        // given:
        let (storage, mut state) = setup().await;
        let id0 = seed_root_centroid(&storage, &mut state, vec![1.0, 0.0]).await;
        let id1 = seed_root_centroid(&storage, &mut state, vec![0.0, 1.0]).await;
        let mut delta = SearchIndexDelta::new(&state);
        delta.remove_from_root(id0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let (extra_id, extra) =
            delta.add_centroid(TreeLevel::leaf(depth), vec![2.0, 2.0], ROOT_VECTOR_ID);
        delta.add_to_root(extra_id, extra.vector.clone());
        let snapshot = storage.snapshot().await.unwrap();
        let reader = dirty_reader(&delta, &state, snapshot);

        // when:
        let root = reader.read_root().get().await.unwrap();

        // then:
        assert_eq!(
            postings_to_vec(root.as_ref()),
            vec![(id1, vec![0.0, 1.0]), (extra_id, vec![2.0, 2.0])]
        );
    }

    #[tokio::test]
    async fn read_postings_should_read_root_if_centroid_id_is_root() {
        // given:
        let (storage, mut state) = setup().await;
        let id = seed_root_centroid(&storage, &mut state, vec![1.0, 0.0]).await;
        let delta = SearchIndexDelta::new(&state);
        let snapshot = storage.snapshot().await.unwrap();
        let reader = dirty_reader(&delta, &state, snapshot);

        // when:
        let root = reader.read_postings(ROOT_VECTOR_ID).get().await.unwrap();

        // then:
        assert_eq!(postings_to_vec(root.as_ref()), vec![(id, vec![1.0, 0.0])]);
    }

    #[tokio::test]
    async fn read_postings_should_filter_out_deleted_centroids() {
        // given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);
        let (child_id, child) = delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        delta.add_to_posting(parent, child_id, child.vector.clone());
        apply_search_delta(&storage, &mut state, delta).await;
        let mut delta = SearchIndexDelta::new(&state);
        delta.delete_centroids(vec![parent]);
        let snapshot = storage.snapshot().await.unwrap();
        let reader = dirty_reader(&delta, &state, snapshot);

        // when:
        let posting = reader.read_postings(parent).get().await.unwrap();

        // then:
        assert!(posting.is_empty());
    }

    #[tokio::test]
    async fn read_postings_should_apply_updates_to_postings_from_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);
        let (child0, child0_info) =
            delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        delta.add_to_posting(parent, child0, child0_info.vector.clone());
        apply_search_delta(&storage, &mut state, delta).await;
        let mut delta = SearchIndexDelta::new(&state);
        delta.remove_from_posting(parent, child0);
        let (child1, child1_info) =
            delta.add_centroid(TreeLevel::leaf(depth), vec![4.0, 4.0], parent);
        delta.add_to_posting(parent, child1, child1_info.vector.clone());
        let snapshot = storage.snapshot().await.unwrap();
        let reader = dirty_reader(&delta, &state, snapshot);

        // when:
        let posting = reader.read_postings(parent).get().await.unwrap();

        // then:
        assert_eq!(
            postings_to_vec(posting.as_ref()),
            vec![(child1, vec![4.0, 4.0])]
        );
    }

    #[tokio::test]
    async fn read_postings_should_return_postings_directly_if_no_updates() {
        // given:
        let (storage, mut state) = setup().await;
        let parent = seed_promoted_root(&storage, &mut state, vec![vec![1.0, 0.0]])
            .await
            .remove(0);
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let mut delta = SearchIndexDelta::new(&state);
        let (child_id, child_info) =
            delta.add_centroid(TreeLevel::leaf(depth), vec![3.0, 3.0], parent);
        delta.add_to_posting(parent, child_id, child_info.vector.clone());
        apply_search_delta(&storage, &mut state, delta).await;
        let delta = SearchIndexDelta::new(&state);
        let snapshot = storage.snapshot().await.unwrap();
        let reader = dirty_reader(&delta, &state, snapshot);

        // when:
        let posting = reader.read_postings(parent).get().await.unwrap();

        // then:
        assert_eq!(
            postings_to_vec(posting.as_ref()),
            vec![(child_id, vec![3.0, 3.0])]
        );
    }

    #[tokio::test]
    async fn vector_id_should_look_up_delta_before_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let mut seed = ForwardIndexDelta::new(&state);
        let stored_id = seed.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        let mut ops = vec![];
        seed.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();
        let delta_id = VectorId::data_vector_id(99);
        let mut delta = VectorIndexDelta::new(&state);
        delta
            .forward_index
            .dictionary_updates
            .insert("v1".to_string(), delta_id);

        let snapshot: Arc<dyn StorageRead> = storage.snapshot().await.unwrap();
        let view = vector_index_view(&delta, &state, &snapshot);

        // when/then:
        assert_eq!(stored_id, VectorId::data_vector_id(0));
        assert_eq!(view.vector_id("v1"), Some(delta_id));
    }

    #[tokio::test]
    async fn vector_id_should_look_up_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let mut seed = ForwardIndexDelta::new(&state);
        let stored_id = seed.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        let mut ops = vec![];
        seed.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();
        let delta = VectorIndexDelta::new(&state);
        let snapshot: Arc<dyn StorageRead> = storage.snapshot().await.unwrap();
        let view = vector_index_view(&delta, &state, &snapshot);

        // when/then:
        assert_eq!(view.vector_id("v1"), Some(stored_id));
    }

    #[tokio::test]
    async fn vector_data_should_filter_out_deletes_from_delta() {
        // given:
        let (storage, mut state) = setup().await;
        let mut seed = ForwardIndexDelta::new(&state);
        let vector_id = seed.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        let mut ops = vec![];
        seed.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();
        let mut delta = VectorIndexDelta::new(&state);
        delta.forward_index.delete_vector(vector_id);
        let snapshot: Arc<dyn StorageRead> = storage.snapshot().await.unwrap();
        let view = vector_index_view(&delta, &state, &snapshot);

        // when/then:
        assert_eq!(view.vector_data(vector_id, DIMS).await.unwrap(), None);
    }

    #[tokio::test]
    async fn vector_data_should_look_up_delta_before_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let mut seed = ForwardIndexDelta::new(&state);
        let vector_id = seed.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        let mut ops = vec![];
        seed.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();
        let mut delta = VectorIndexDelta::new(&state);
        delta
            .forward_index
            .vector_updates
            .insert(vector_id, make_value(vec![5.0, 6.0], "green", 11));
        let snapshot: Arc<dyn StorageRead> = storage.snapshot().await.unwrap();
        let view = vector_index_view(&delta, &state, &snapshot);

        // when/then:
        assert_eq!(
            view.vector_data(vector_id, DIMS).await.unwrap(),
            Some(make_value(vec![5.0, 6.0], "green", 11))
        );
    }

    #[tokio::test]
    async fn vector_data_should_look_up_storage() {
        // given:
        let (storage, mut state) = setup().await;
        let mut seed = ForwardIndexDelta::new(&state);
        let vector_id = seed.add_vector("v1", &make_attributes(vec![1.0, 2.0]));
        let mut ops = vec![];
        seed.freeze(&mut state, &mut ops);
        storage.apply(ops).await.unwrap();
        let delta = VectorIndexDelta::new(&state);
        let snapshot: Arc<dyn StorageRead> = storage.snapshot().await.unwrap();
        let view = vector_index_view(&delta, &state, &snapshot);

        // when/then:
        assert_eq!(
            view.vector_data(vector_id, DIMS).await.unwrap(),
            Some(make_value(vec![1.0, 2.0], "blue", 7))
        );
    }

    #[tokio::test]
    async fn centroid_counts_should_get_centroid_counts_by_level_and_apply_changes_from_delta() {
        // given:
        let (storage, mut state) = setup().await;
        let depth = TreeDepth::of(state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let existing = leaf_centroid(10);
        let deleted = leaf_centroid(11);
        let added = leaf_centroid(12);
        state
            .centroid_counts
            .insert(level.level(), HashMap::from([(existing, 5), (deleted, 2)]));
        let mut delta = VectorIndexDelta::new(&state);
        delta
            .search_index
            .centroid_count_deltas
            .insert(level.level(), HashMap::from([(existing, -2), (added, 4)]));
        delta.search_index.deleted_centroids.insert(deleted);
        let snapshot: Arc<dyn StorageRead> = storage.snapshot().await.unwrap();
        let view = vector_index_view(&delta, &state, &snapshot);

        // when/then:
        assert_eq!(
            view.centroid_counts(level),
            HashMap::from([(existing, 3), (added, 4)])
        );
    }

    #[tokio::test]
    async fn last_written_posting_should_get_the_last_written_posting_from_delta() {
        // given:
        let (storage, state) = setup().await;
        let vector_id = VectorId::data_vector_id(42);
        let centroid_id = leaf_centroid(7);
        let mut delta = VectorIndexDelta::new(&state);
        delta
            .search_index
            .current_posting
            .insert(vector_id, centroid_id);
        let snapshot: Arc<dyn StorageRead> = storage.snapshot().await.unwrap();
        let view = vector_index_view(&delta, &state, &snapshot);

        // when/then:
        assert_eq!(view.last_written_posting(vector_id), Some(centroid_id));
    }
}

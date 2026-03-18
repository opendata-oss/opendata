use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use bytes::Bytes;
use roaring::RoaringTreemap;
use common::storage::RecordOp;
use common::StorageRead;
use crate::AttributeValue;
use crate::Result;
use crate::hnsw::CentroidGraph;
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::FieldValue;
use crate::serde::posting_list::{merge_decoded_posting_lists, PostingList, PostingListValue, PostingUpdate};
use crate::serde::vector_data::{Field, VectorDataValue};
use crate::storage::{record, VectorDbStorageReadExt};

#[derive(Debug, Clone)]
struct CentroidChunkManager {
    current_chunk_count: usize,
    current_chunk_id: u32,
    chunk_target: usize,
    dimensions: usize,
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
#[derive(Clone)]
struct VectorIndexCtx {
    dictionary: HashMap<String, u64>,
    centroid_counts: HashMap<u64, u64>,
    centroid_graph: Arc<dyn CentroidGraph>,
}

#[derive(Clone)]
struct VectorIndexDelta {
    new_centroids: HashMap<u64, CentroidEntry>,
    deleted_centroids: HashSet<u64>,
    centroid_count_deltas: HashMap<u64, i64>,
    posting_updates: HashMap<u64, Vec<PostingUpdate>>,
    inverted_index_updates: HashMap<Bytes, RoaringTreemap>,
    dictionary_updates: HashMap<String, u64>,
    vector_updates: HashMap<u64, VectorDataValue>,
    vector_deletes: HashSet<u64>,
}

impl VectorIndexDelta {
    fn update_dictionary(&mut self, external_id: &str, vector_id: u64) {
        self.dictionary_updates.insert(String::from(external_id), vector_id);
    }

    fn add_vector(
        &mut self,
        vector_id: u64,
        external_id: &str,
        attributes: &[(String, AttributeValue)]
    ) {
        let fields: Vec<Field> = attributes
            .iter()
            .map(|(name, value)| Field::new(name, value.clone().into()))
            .collect();
        let value = VectorDataValue::new(external_id, fields);
        self.vector_updates.insert(vector_id, value);
    }

    fn delete_vector(&mut self, vector_id: u64) {
        self.vector_deletes.insert(vector_id);
    }

    fn add_centroids(&mut self, centroids: Vec<CentroidEntry>) {
        for c in &centroids {
            assert!(!self.centroid_count_deltas.contains_key(&c.centroid_id));
            assert!(!self.new_centroids.contains_key(&c.centroid_id));
            self.deleted_centroids.remove(&c.centroid_id);
        }
        self.new_centroids
            .extend(centroids.into_iter().map(|c| (c.centroid_id, c)))
    }

    fn delete_centroids(&mut self, centroids: Vec<u64>) {
        for c in &centroids {
            self.centroid_count_deltas.remove(&c);
            self.new_centroids.remove(c);
        }
        self.deleted_centroids.extend(centroids);
    }

    fn add_to_posting(&mut self, centroid_id: u64, vector_id: u64, vector: Vec<f32>) {
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::append(vector_id, vector));
        let c = self.centroid_count_deltas.entry(vector_id).or_insert(0);
        *c += 1;
    }

    fn remove_from_posting(&mut self, centroid_id: u64, vector_id: u64) {
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::delete(vector_id));
        let c = self.centroid_count_deltas.entry(vector_id).or_insert(0);
        *c -= 1;
    }

    fn add_to_inverted_index(
        &mut self,
        field_name: String,
        field_value: FieldValue,
        vector_id: u64
    ) {
        let key = crate::serde::key::MetadataIndexKey::new(field_name, field_value).encode();
        #[allow(clippy::unwrap_or_default)]
        self.inverted_index_updates
            .entry(key)
            .or_insert_with(RoaringTreemap::new)
            .insert(vector_id);
    }

    fn freeze(self, ctx: VectorIndexCtx) -> (VectorIndexCtx, Vec<RecordOp>) {
        // apply all mutations to ctx
        // construct ops that need to be written to storage
        todo!()
    }
}

struct VectorIndexView<'a> {
    delta: &'a VectorIndexDelta,
    ctx: VectorIndexCtx,
    snapshot: Arc<dyn StorageRead>
}

impl<'a> VectorIndexView<'a> {
    fn get_posting_list(
        &self,
        centroid_id: u64,
        dimensions: usize,
    ) -> Result<Pin<Box<dyn Future<Output=Result<PostingList>> + 'static>>> {
        let mut all_postings = Vec::with_capacity(2);
        if let Some(current) = self.delta.posting_updates.get(&centroid_id) {
            all_postings.push(PostingListValue::from_posting_updates(current.clone())?);
        }
        let snapshot = self.snapshot.clone();
        Ok(Box::pin(async move {
            all_postings.push(
                snapshot
                    .get_posting_list(centroid_id, dimensions)
                    .await?,
            );
            Ok(merge_decoded_posting_lists(all_postings).into())
        }))
    }

    fn get_vector_data(
        &self,
        vector_id: u64,
        dimensions: usize,
    ) -> Pin<Box<dyn Future<Output=Result<Option<VectorDataValue>>> + 'static>> {
        if self.delta.vector_deletes.contains(&vector_id) {
            Box::pin(async { Ok(None) })
        } else if let Some(d) = self.delta.vector_updates.get(&vector_id) {
            let v = d.clone();
            Box::pin(async move { Ok(Some(v)) })
        } else {
            let snapshot = self.snapshot.clone();
            Box::pin(async move {
                snapshot.get_vector_data(vector_id, dimensions).await
            })
        }
    }
}
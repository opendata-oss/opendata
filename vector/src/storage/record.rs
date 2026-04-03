//! Functions for creating RecordOp instances for vector database operations.
//!
//! These functions build RecordOp instances for common write patterns without
//! requiring a storage reference, since they only encode keys and values.

use crate::error::Result;
use bytes::BytesMut;
use common::Record;
use common::storage::RecordOp;
use roaring::RoaringTreemap;
use tracing::debug;
use crate::model::AttributeValue;
use crate::serde::Encode;
use crate::serde::centroid_chunk::{CentroidChunkValue, CentroidEntry};
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::deletions::DeletionsValue;
use crate::serde::key::{
    CentroidChunkKey, CentroidStatsKey, DeletionsKey, IdDictionaryKey, PostingListKey,
    VectorDataKey,
};
use crate::serde::metadata_index::MetadataIndexValue;
use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use crate::serde::vector_data::{Field, VectorDataValue};
use crate::serde::vector_id::VectorId;

/// Create a RecordOp to update the IdDictionary mapping.
pub fn put_id_dictionary(external_id: &str, internal_id: VectorId) -> RecordOp {
    debug!("put_id_dictionary {} {}", external_id, internal_id);
    let key = IdDictionaryKey::new(external_id).encode();
    let mut value_buf = BytesMut::with_capacity(8);
    internal_id.encode(&mut value_buf);
    let value = value_buf.freeze();
    debug!("put_id_dictionary {:?} {:?}", key, value);
    RecordOp::Put(Record::new(key, value).into())
}

/// Create a RecordOp to delete an IdDictionary mapping.
#[allow(dead_code)]
pub fn delete_id_dictionary(external_id: &str) -> RecordOp {
    let key = IdDictionaryKey::new(external_id).encode();
    RecordOp::Delete(key)
}

/// Create a RecordOp to write vector data (including external_id, vector, and metadata).
///
/// The `attributes` must include a "vector" field with the embedding values.
#[allow(dead_code)]
pub fn put_vector_data(
    internal_id: VectorId,
    external_id: &str,
    attributes: &[(String, AttributeValue)],
) -> RecordOp {
    let key = VectorDataKey::new(internal_id).encode();
    let fields: Vec<Field> = attributes
        .iter()
        .map(|(name, value)| Field::new(name, value.clone().into()))
        .collect();
    let value = VectorDataValue::new(external_id, fields).encode_to_bytes();
    RecordOp::Put(Record::new(key, value).into())
}

/// Create a RecordOp to delete vector data.
pub fn delete_vector_data(internal_id: VectorId) -> RecordOp {
    let key = VectorDataKey::new(internal_id).encode();
    RecordOp::Delete(key)
}

/// Create a RecordOp to merge posting updates into a posting list.
pub fn merge_posting_list(centroid_id: VectorId, postings: Vec<PostingUpdate>) -> Result<RecordOp> {
    let key = PostingListKey::new(centroid_id).encode();
    let value = PostingListValue::from_posting_updates(postings)?.encode_to_bytes();
    Ok(RecordOp::Merge(Record::new(key, value).into()))
}

/// Create a RecordOp to merge vector IDs into the deleted vectors bitmap.
pub fn merge_deleted_vectors(vector_ids: RoaringTreemap) -> Result<RecordOp> {
    let key = DeletionsKey::new().encode();
    let value = DeletionsValue::from_treemap(vector_ids).encode_to_bytes()?;
    Ok(RecordOp::Merge(Record::new(key, value).into()))
}

/// Create a RecordOp to write a centroid chunk.
pub fn put_centroid_chunk(
    chunk_id: u32,
    entries: Vec<CentroidEntry>,
    dimensions: usize,
) -> RecordOp {
    let key = CentroidChunkKey::new(chunk_id).encode();
    let value = CentroidChunkValue::new(entries).encode_to_bytes(dimensions);
    RecordOp::Put(Record::new(key, value).into())
}

/// Create a RecordOp to delete a centroid chunk.
#[allow(dead_code)]
pub fn delete_centroid_chunk(chunk_id: u32) -> RecordOp {
    let key = CentroidChunkKey::new(chunk_id).encode();
    RecordOp::Delete(key)
}

/// Create a RecordOp to merge a vector count delta into centroid stats.
#[allow(dead_code)]
pub fn merge_centroid_stats(centroid_id: VectorId, delta: i32) -> RecordOp {
    let key = CentroidStatsKey::new(centroid_id).encode();
    let value = CentroidStatsValue::new(delta).encode_to_bytes();
    RecordOp::Merge(Record::new(key, value).into())
}

/// Create a RecordOp to merge a batch of vector IDs into a metadata index entry.
///
/// The metadata index maps (field_name, field_value) → set of vector IDs.
/// The merge operator unions RoaringTreemap bitmaps, so this adds all the
/// vector IDs to the existing set for that field/value pair.
///
/// `encoded_key` is the pre-encoded MetadataIndexKey bytes.
pub fn merge_metadata_index_bitmap(
    encoded_key: bytes::Bytes,
    vector_ids: &RoaringTreemap,
) -> Result<RecordOp> {
    let bitmap = MetadataIndexValue::from_treemap(vector_ids.clone());
    let encoded = bitmap.encode_to_bytes()?;
    Ok(RecordOp::Merge(Record::new(encoded_key, encoded).into()))
}

/// Create a RecordOp to merge new centroid entries into an existing centroid chunk.
pub fn merge_centroid_chunk(
    chunk_id: u32,
    entries: Vec<CentroidEntry>,
    dimensions: usize,
) -> RecordOp {
    let key = CentroidChunkKey::new(chunk_id).encode();
    let value = CentroidChunkValue::new(entries).encode_to_bytes(dimensions);
    RecordOp::Merge(Record::new(key, value).into())
}

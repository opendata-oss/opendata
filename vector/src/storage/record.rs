//! Functions for creating RecordOp instances for vector database operations.
//!
//! These functions build RecordOp instances for common write patterns without
//! requiring a storage reference, since they only encode keys and values.

use crate::error::Result;
use crate::model::AttributeValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::id_dictionary::IdDictionaryValue;
use crate::serde::key::{
    CentroidStatsKey, IdDictionaryKey, PostingListKey, VectorDataKey, VectorIndexDataKey,
};
use crate::serde::metadata_index::MetadataIndexValue;
use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use crate::serde::vector_data::{Field, VectorDataValue};
use crate::serde::vector_id::VectorId;
use crate::serde::vector_index_data::VectorIndexDataValue;
use common::Record;
use common::storage::RecordOp;
use tracing::debug;

/// Create a RecordOp to update the IdDictionary mapping.
pub fn put_id_dictionary(external_id: &str, internal_id: VectorId) -> RecordOp {
    debug!("put_id_dictionary {} {}", external_id, internal_id);
    let key = IdDictionaryKey::new(external_id).encode();
    let value = IdDictionaryValue::new(internal_id);
    let value = value.encode_to_bytes();
    debug!("put_id_dictionary {:?} {:?}", key, value);
    RecordOp::Put(Record::new(key, value).into())
}

/// Create a RecordOp to delete an IdDictionary mapping.
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

/// Create a RecordOp to write vector index data.
pub fn put_vector_index_data(internal_id: VectorId, value: VectorIndexDataValue) -> RecordOp {
    let key = VectorIndexDataKey::new(internal_id).encode();
    let encoded = value.encode_to_bytes();
    RecordOp::Put(Record::new(key, encoded).into())
}

/// Create a RecordOp to delete vector index data.
pub fn delete_vector_index_data(internal_id: VectorId) -> RecordOp {
    let key = VectorIndexDataKey::new(internal_id).encode();
    RecordOp::Delete(key)
}

/// Create a RecordOp to merge posting updates into a posting list.
pub fn merge_posting_list(centroid_id: VectorId, postings: Vec<PostingUpdate>) -> Result<RecordOp> {
    let key = PostingListKey::new(centroid_id).encode();
    let value = PostingListValue::from_posting_updates(postings)?.encode_to_bytes();
    Ok(RecordOp::Merge(Record::new(key, value).into()))
}

/// Create a RecordOp to merge a vector count delta into centroid stats.
#[allow(dead_code)]
pub fn merge_centroid_stats(centroid_id: VectorId, delta: i32) -> RecordOp {
    let key = CentroidStatsKey::new(centroid_id).encode();
    let value = CentroidStatsValue::new(delta).encode_to_bytes();
    RecordOp::Merge(Record::new(key, value).into())
}

/// Create a RecordOp to merge metadata index updates.
pub fn merge_metadata_index_bitmap(
    encoded_key: bytes::Bytes,
    value: &MetadataIndexValue,
) -> Result<RecordOp> {
    let encoded = value.encode_to_bytes()?;
    Ok(RecordOp::Merge(Record::new(encoded_key, encoded).into()))
}

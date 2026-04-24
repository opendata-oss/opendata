//! Functions for creating storage operations for vector database.
//!
//! These functions build [`StorageOp`] instances for common write patterns
//! without requiring a storage reference, since they only encode keys and values.

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
use bytes::Bytes;
use slatedb::WriteBatch;
use tracing::debug;

/// An operation to apply to storage via a slatedb `WriteBatch`.
#[derive(Debug, Clone)]
pub(crate) enum StorageOp {
    Put { key: Bytes, value: Bytes },
    Merge { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

impl StorageOp {
    pub fn put(key: Bytes, value: Bytes) -> Self {
        Self::Put { key, value }
    }
    pub fn merge(key: Bytes, value: Bytes) -> Self {
        Self::Merge { key, value }
    }
    pub fn delete(key: Bytes) -> Self {
        Self::Delete { key }
    }
}

/// Apply a [`StorageOp`] into the provided [`WriteBatch`].
pub(crate) fn apply_to_batch(batch: &mut WriteBatch, op: StorageOp) {
    match op {
        StorageOp::Put { key, value } => batch.put(key, value),
        StorageOp::Merge { key, value } => batch.merge(key, value),
        StorageOp::Delete { key } => batch.delete(key),
    }
}

/// Fold a slice of [`StorageOp`]s into a fresh [`WriteBatch`].
pub(crate) fn build_write_batch(ops: Vec<StorageOp>) -> WriteBatch {
    let mut batch = WriteBatch::new();
    for op in ops {
        apply_to_batch(&mut batch, op);
    }
    batch
}

/// Create a StorageOp to update the IdDictionary mapping.
pub fn put_id_dictionary(external_id: &str, internal_id: VectorId) -> StorageOp {
    debug!("put_id_dictionary {} {}", external_id, internal_id);
    let key = IdDictionaryKey::new(external_id).encode();
    let value = IdDictionaryValue::new(internal_id);
    let value = value.encode_to_bytes();
    debug!("put_id_dictionary {:?} {:?}", key, value);
    StorageOp::put(key, value)
}

/// Create a StorageOp to delete an IdDictionary mapping.
#[allow(dead_code)]
pub fn delete_id_dictionary(external_id: &str) -> StorageOp {
    let key = IdDictionaryKey::new(external_id).encode();
    StorageOp::delete(key)
}

/// Create a StorageOp to write vector data (including external_id, vector, and metadata).
///
/// The `attributes` must include a "vector" field with the embedding values.
#[allow(dead_code)]
pub fn put_vector_data(
    internal_id: VectorId,
    external_id: &str,
    attributes: &[(String, AttributeValue)],
) -> StorageOp {
    let key = VectorDataKey::new(internal_id).encode();
    let fields: Vec<Field> = attributes
        .iter()
        .map(|(name, value)| Field::new(name, value.clone().into()))
        .collect();
    let value = VectorDataValue::new(external_id, fields).encode_to_bytes();
    StorageOp::put(key, value)
}

/// Create a StorageOp to delete vector data.
pub fn delete_vector_data(internal_id: VectorId) -> StorageOp {
    let key = VectorDataKey::new(internal_id).encode();
    StorageOp::delete(key)
}

/// Create a StorageOp to write vector index data.
pub fn put_vector_index_data(internal_id: VectorId, value: VectorIndexDataValue) -> StorageOp {
    let key = VectorIndexDataKey::new(internal_id).encode();
    let encoded = value.encode_to_bytes();
    StorageOp::put(key, encoded)
}

/// Create a StorageOp to delete vector index data.
pub fn delete_vector_index_data(internal_id: VectorId) -> StorageOp {
    let key = VectorIndexDataKey::new(internal_id).encode();
    StorageOp::delete(key)
}

/// Create a StorageOp to merge posting updates into a posting list.
pub fn merge_posting_list(
    centroid_id: VectorId,
    postings: Vec<PostingUpdate>,
) -> Result<StorageOp> {
    let key = PostingListKey::new(centroid_id).encode();
    let value = PostingListValue::from_posting_updates(postings)?.encode_to_bytes();
    Ok(StorageOp::merge(key, value))
}

/// Create a StorageOp to merge a vector count delta into centroid stats.
#[allow(dead_code)]
pub fn merge_centroid_stats(centroid_id: VectorId, delta: i32) -> StorageOp {
    let key = CentroidStatsKey::new(centroid_id).encode();
    let value = CentroidStatsValue::new(delta).encode_to_bytes();
    StorageOp::merge(key, value)
}

/// Create a StorageOp to merge metadata index updates.
pub fn merge_metadata_index_bitmap(
    encoded_key: bytes::Bytes,
    value: &MetadataIndexValue,
) -> Result<StorageOp> {
    let encoded = value.encode_to_bytes()?;
    Ok(StorageOp::merge(encoded_key, encoded))
}

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use common::storage::RecordOp;
use common::{Record, Storage, StorageRead};
use roaring::RoaringTreemap;

use crate::model::AttributeValue;
use crate::serde::Encode;
use crate::serde::key::{IdDictionaryKey, PostingListKey, VectorDataKey, VectorMetaKey};
use crate::serde::posting_list::PostingListValue;
use crate::serde::vector_data::VectorDataValue;
use crate::serde::vector_meta::{MetadataField, VectorMetaValue};

pub(crate) mod merge_operator;

/// Extension trait for StorageRead that provides vector database-specific loading methods.
#[async_trait]
pub(crate) trait VectorDbStorageReadExt: StorageRead {
    /// Look up internal ID from external ID in the ID dictionary.
    async fn lookup_internal_id(&self, external_id: &str) -> Result<Option<u64>> {
        let key = IdDictionaryKey::new(external_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let mut slice = record.value.as_ref();
                let internal_id = common::serde::encoding::decode_u64(&mut slice)
                    .context("failed to decode internal ID from ID dictionary")?;
                Ok(Some(internal_id))
            }
            None => Ok(None),
        }
    }

    /// Load a vector's data by internal ID.
    #[allow(dead_code)]
    async fn get_vector_data(&self, internal_id: u64) -> Result<Option<VectorDataValue>> {
        let key = VectorDataKey::new(internal_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = VectorDataValue::decode_from_bytes(&record.value)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Load a vector's metadata by internal ID.
    #[allow(dead_code)]
    async fn get_vector_meta(&self, internal_id: u64) -> Result<Option<VectorMetaValue>> {
        let key = VectorMetaKey::new(internal_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = VectorMetaValue::decode_from_bytes(&record.value)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Load a posting list for a centroid.
    #[allow(dead_code)]
    async fn get_posting_list(&self, centroid_id: u32) -> Result<PostingListValue> {
        let key = PostingListKey::new(centroid_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = PostingListValue::decode_from_bytes(&record.value)?;
                Ok(value)
            }
            None => Ok(PostingListValue::new()),
        }
    }

    /// Load the deleted vectors bitmap (centroid_id = 0).
    #[allow(dead_code)]
    async fn get_deleted_vectors(&self) -> Result<RoaringTreemap> {
        let key = PostingListKey::deleted_vectors().encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = PostingListValue::decode_from_bytes(&record.value)?;
                Ok(value.vector_ids)
            }
            None => Ok(RoaringTreemap::new()),
        }
    }
}

// Implement the trait for all types that implement StorageRead
impl<T: ?Sized + StorageRead> VectorDbStorageReadExt for T {}

/// Extension trait for Storage that provides vector database-specific write helpers.
///
/// These methods build RecordOp instances for common write patterns.
pub(crate) trait VectorDbStorageExt: Storage {
    /// Create a RecordOp to update the IdDictionary mapping.
    fn put_id_dictionary(&self, external_id: &str, internal_id: u64) -> Result<RecordOp> {
        let key = IdDictionaryKey::new(external_id).encode();
        let mut value_buf = BytesMut::with_capacity(8);
        internal_id.encode(&mut value_buf);
        Ok(RecordOp::Put(Record::new(key, value_buf.freeze())))
    }

    /// Create a RecordOp to delete an IdDictionary mapping.
    fn delete_id_dictionary(&self, external_id: &str) -> Result<RecordOp> {
        let key = IdDictionaryKey::new(external_id).encode();
        Ok(RecordOp::Delete(key))
    }

    /// Create a RecordOp to write vector data.
    fn put_vector_data(&self, internal_id: u64, values: Vec<f32>) -> Result<RecordOp> {
        let key = VectorDataKey::new(internal_id).encode();
        let value = VectorDataValue::new(values).encode_to_bytes();
        Ok(RecordOp::Put(Record::new(key, value)))
    }

    /// Create a RecordOp to write vector metadata.
    fn put_vector_meta(
        &self,
        internal_id: u64,
        external_id: &str,
        metadata: &[(String, AttributeValue)],
    ) -> Result<RecordOp> {
        let key = VectorMetaKey::new(internal_id).encode();
        let fields: Vec<MetadataField> = metadata
            .iter()
            .map(|(name, value)| {
                MetadataField::new(name, crate::model::attribute_value_to_field_value(value))
            })
            .collect();
        let value = VectorMetaValue::new(external_id, fields).encode_to_bytes();
        Ok(RecordOp::Put(Record::new(key, value)))
    }

    /// Create a RecordOp to delete vector data.
    fn delete_vector_data(&self, internal_id: u64) -> Result<RecordOp> {
        let key = VectorDataKey::new(internal_id).encode();
        Ok(RecordOp::Delete(key))
    }

    /// Create a RecordOp to delete vector metadata.
    fn delete_vector_meta(&self, internal_id: u64) -> Result<RecordOp> {
        let key = VectorMetaKey::new(internal_id).encode();
        Ok(RecordOp::Delete(key))
    }

    /// Create a RecordOp to merge vector IDs into a posting list.
    fn merge_posting_list(&self, centroid_id: u32, vector_ids: RoaringTreemap) -> Result<RecordOp> {
        let key = PostingListKey::new(centroid_id).encode();
        let value = PostingListValue::from_treemap(vector_ids).encode_to_bytes()?;
        Ok(RecordOp::Merge(Record::new(key, value)))
    }

    /// Create a RecordOp to merge vector IDs into the deleted vectors bitmap.
    fn merge_deleted_vectors(&self, vector_ids: RoaringTreemap) -> Result<RecordOp> {
        let key = PostingListKey::deleted_vectors().encode();
        let value = PostingListValue::from_treemap(vector_ids).encode_to_bytes()?;
        Ok(RecordOp::Merge(Record::new(key, value)))
    }
}

// Implement the trait for all types that implement Storage
impl<T: ?Sized + Storage> VectorDbStorageExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AttributeValue;
    use common::storage::in_memory::InMemoryStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_read_and_write_vector_data() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let values = vec![1.0, 2.0, 3.0];

        // when - write
        let op = storage.put_vector_data(42, values.clone()).unwrap();
        storage.apply(vec![op]).await.unwrap();

        // then - read
        let result = storage.get_vector_data(42).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().vector, values);
    }

    #[tokio::test]
    async fn should_read_and_write_vector_meta() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let metadata = vec![
            (
                "category".to_string(),
                AttributeValue::String("shoes".to_string()),
            ),
            ("price".to_string(), AttributeValue::Int64(99)),
        ];

        // when - write
        let op = storage.put_vector_meta(42, "vec-1", &metadata).unwrap();
        storage.apply(vec![op]).await.unwrap();

        // then - read
        let result = storage.get_vector_meta(42).await.unwrap();
        assert!(result.is_some());
        let meta = result.unwrap();
        assert_eq!(meta.external_id, "vec-1");
        assert_eq!(meta.fields.len(), 2);
    }

    #[tokio::test]
    async fn should_read_empty_posting_list_when_not_exists() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());

        // when
        let result = storage.get_posting_list(1).await.unwrap();

        // then
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn should_read_empty_deleted_vectors_when_not_exists() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());

        // when
        let result = storage.get_deleted_vectors().await.unwrap();

        // then
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn should_write_and_read_id_dictionary() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());

        // when - write
        let op = storage.put_id_dictionary("vec-1", 42).unwrap();
        storage.apply(vec![op]).await.unwrap();

        // then - read using IdDictionary directly
        let key = IdDictionaryKey::new("vec-1").encode();
        let record = storage.get(key).await.unwrap();
        assert!(record.is_some());
    }
}

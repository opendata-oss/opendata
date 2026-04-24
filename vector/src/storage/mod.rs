use async_trait::async_trait;
use common::StorageError;
use common::default_scan_options;
use slatedb::DbRead;

use crate::error::{Error, Result};
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::centroids::CentroidsValue;
use crate::serde::id_dictionary::IdDictionaryValue;
use crate::serde::key::{
    CentroidInfoKey, CentroidStatsKey, CentroidsKey, IdDictionaryKey, MetadataIndexKey,
    PostingListKey, VectorDataKey, VectorIndexDataKey,
};
use crate::serde::metadata_index::MetadataIndexValue;
use crate::serde::posting_list::PostingListValue;
use crate::serde::vector_data::VectorDataValue;
use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
use crate::serde::vector_index_data::VectorIndexDataValue;
use bytes::{BufMut, BytesMut};
use std::ops::Bound::Included;

pub(crate) mod merge_operator;
pub(crate) mod record;

/// Extension trait for StorageRead that provides vector database-specific loading methods.
///
/// These methods are marked as `#[allow(dead_code)]` because they are used by the query path
/// (VectorDb::search and related methods) which is only called in tests and during actual
/// queries. The compiler doesn't see them as used during compilation of the library crate
/// alone, but they are essential for the search functionality.
#[async_trait]
pub(crate) trait VectorDbStorageReadExt: DbRead + Send + Sync {
    /// Look up internal ID from external ID in the ID dictionary.
    async fn lookup_internal_id(&self, external_id: &str) -> Result<Option<VectorId>> {
        let key = IdDictionaryKey::new(external_id).encode();
        let value = self.get(&key).await.map_err(StorageError::from_storage)?;
        match value {
            Some(value) => {
                let decoded = IdDictionaryValue::decode_from_bytes(value.as_ref())?;
                Ok(Some(decoded.vector_id))
            }
            None => Ok(None),
        }
    }

    /// Load a vector's data by internal ID.
    ///
    /// Requires dimensions to decode the vector field.
    async fn get_vector_data(
        &self,
        internal_id: VectorId,
        dimensions: usize,
    ) -> Result<Option<VectorDataValue>> {
        let key = VectorDataKey::new(internal_id).encode();
        let value = self.get(&key).await.map_err(StorageError::from_storage)?;
        match value {
            Some(value) => {
                let decoded = VectorDataValue::decode_from_bytes(&value, dimensions)?;
                Ok(Some(decoded))
            }
            None => Ok(None),
        }
    }

    async fn get_vector_index_data(
        &self,
        internal_id: VectorId,
    ) -> Result<Option<VectorIndexDataValue>> {
        let key = VectorIndexDataKey::new(internal_id).encode();
        let value = self.get(&key).await.map_err(StorageError::from_storage)?;
        match value {
            Some(value) => {
                let decoded = VectorIndexDataValue::decode_from_bytes(&value)?;
                Ok(Some(decoded))
            }
            None => Ok(None),
        }
    }

    async fn get_root_posting_list(&self, dimensions: usize) -> Result<PostingListValue> {
        self.get_posting_list(ROOT_VECTOR_ID, dimensions).await
    }

    async fn get_centroids_meta(&self) -> Result<Option<CentroidsValue>> {
        let key = CentroidsKey::new().encode();
        let value = self.get(&key).await.map_err(StorageError::from_storage)?;
        match value {
            Some(value) => Ok(Some(CentroidsValue::decode_from_bytes(&value)?)),
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    async fn get_centroid_info(&self, centroid_id: VectorId) -> Result<Option<CentroidInfoValue>> {
        let key = CentroidInfoKey::new(centroid_id).encode();
        let value = self.get(&key).await.map_err(StorageError::from_storage)?;
        match value {
            Some(value) => Ok(Some(CentroidInfoValue::decode_from_bytes(&value)?)),
            None => Ok(None),
        }
    }

    /// Load a posting list for a centroid.
    ///
    /// Requires dimensions to decode the embedded vector data.
    async fn get_posting_list(
        &self,
        centroid_id: VectorId,
        dimensions: usize,
    ) -> Result<PostingListValue> {
        assert!(centroid_id.is_tree_node());
        let key = PostingListKey::new(centroid_id).encode();
        // this is a hack to use a scan to read a single posting list to force slatedb to
        // load the data from all underlying sorted runs in parallel
        let mut key_next = BytesMut::with_capacity(key.len() + 1);
        key_next.put(key.as_ref());
        key_next.put_u8(0);
        let key_next = key_next.freeze();
        assert!(key < key_next);
        let range = common::BytesRange::new(Included(key.clone()), Included(key_next));
        let mut iter = self
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        let first = iter.next().await.map_err(StorageError::from_storage)?;
        match first {
            Some(kv) => {
                if kv.key != key {
                    return Ok(PostingListValue::new());
                }
                let value = PostingListValue::decode_from_bytes(&kv.value, dimensions)?;
                Ok(value)
            }
            None => Ok(PostingListValue::new()),
        }
    }

    /// Load centroid stats (vector count) for a centroid.
    ///
    /// Returns a zero count if no stats exist yet.
    #[allow(dead_code)]
    async fn get_centroid_stats(&self, centroid_id: VectorId) -> Result<CentroidStatsValue> {
        let key = CentroidStatsKey::new(centroid_id).encode();
        let value = self.get(&key).await.map_err(StorageError::from_storage)?;
        match value {
            Some(value) => {
                let decoded = CentroidStatsValue::decode_from_bytes(&value).map_err(|e| {
                    Error::Encoding(format!("failed to decode CentroidStatsValue: {e}"))
                })?;
                Ok(decoded)
            }
            None => Ok(CentroidStatsValue::new(0)),
        }
    }

    /// Scan all centroid stats records.
    ///
    /// Returns a list of `(level, centroid_id)` to accumulated vector count.
    #[allow(dead_code)]
    async fn scan_all_centroid_stats(&self) -> Result<Vec<(VectorId, CentroidStatsValue)>> {
        let mut prefix_buf = bytes::BytesMut::with_capacity(3);
        crate::serde::RecordType::CentroidStats
            .prefix()
            .write_to(&mut prefix_buf);
        let prefix = prefix_buf.freeze();

        let range = common::BytesRange::prefix(prefix);
        let mut iter = self
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;

        let mut stats = Vec::new();
        while let Some(kv) = iter.next().await.map_err(StorageError::from_storage)? {
            let key = CentroidStatsKey::decode(&kv.key)?;
            let value = CentroidStatsValue::decode_from_bytes(&kv.value).map_err(|e| {
                Error::Encoding(format!("failed to decode CentroidStatsValue: {e}"))
            })?;
            stats.push((key.centroid_id, value));
        }

        Ok(stats)
    }

    async fn scan_all_centroid_info(&self) -> Result<Vec<(VectorId, CentroidInfoValue)>> {
        let mut prefix_buf = bytes::BytesMut::with_capacity(3);
        crate::serde::RecordType::CentroidInfo
            .prefix()
            .write_to(&mut prefix_buf);
        let prefix = prefix_buf.freeze();

        let range = common::BytesRange::prefix(prefix);
        let mut iter = self
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;

        let mut centroids = Vec::new();
        while let Some(kv) = iter.next().await.map_err(StorageError::from_storage)? {
            let key = CentroidInfoKey::decode(&kv.key)?;
            let value = CentroidInfoValue::decode_from_bytes(&kv.value)?;
            centroids.push((key.centroid_id, value));
        }
        Ok(centroids)
    }

    async fn scan_all_posting_lists(
        &self,
        dimensions: usize,
    ) -> Result<Vec<(VectorId, PostingListValue)>> {
        let mut prefix_buf = bytes::BytesMut::with_capacity(3);
        crate::serde::RecordType::PostingList
            .prefix()
            .write_to(&mut prefix_buf);
        let prefix = prefix_buf.freeze();

        let range = common::BytesRange::prefix(prefix);
        let mut iter = self
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;

        let mut posting_lists = Vec::new();
        while let Some(kv) = iter.next().await.map_err(StorageError::from_storage)? {
            let key = PostingListKey::decode(&kv.key)?;
            let value = PostingListValue::decode_from_bytes(&kv.value, dimensions)?;
            posting_lists.push((key.centroid_id, value));
        }
        Ok(posting_lists)
    }

    async fn scan_all_inner_posting_lists(
        &self,
        dimensions: usize,
    ) -> Result<Vec<(VectorId, PostingListValue)>> {
        let range = PostingListKey::inner_level_bytes_range();
        let mut iter = self
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;

        let mut posting_lists = Vec::new();
        while let Some(kv) = iter.next().await.map_err(StorageError::from_storage)? {
            let key = PostingListKey::decode(&kv.key)?;
            let value = PostingListValue::decode_from_bytes(&kv.value, dimensions)?;
            posting_lists.push((key.centroid_id, value));
        }
        Ok(posting_lists)
    }

    /// Load a metadata index entry for a specific field/value pair.
    ///
    /// Returns a bitmap of vector IDs that have the given value for the field.
    /// Returns an empty bitmap if no entry exists.
    #[allow(dead_code)]
    async fn get_metadata_index(
        &self,
        field: &str,
        value: crate::serde::FieldValue,
    ) -> Result<MetadataIndexValue> {
        let key = MetadataIndexKey::new(field, value).encode();
        let value = self.get(&key).await.map_err(StorageError::from_storage)?;
        match value {
            Some(value) => {
                let decoded = MetadataIndexValue::decode_from_bytes(&value)?;
                Ok(decoded)
            }
            None => Ok(MetadataIndexValue::new()),
        }
    }
}

// Implement the trait for all types that implement StorageRead
impl<T: ?Sized + DbRead + Send + Sync> VectorDbStorageReadExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AttributeValue;
    use crate::serde::id_dictionary::IdDictionaryValue;
    use crate::serde::posting_list::PostingUpdate;
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use crate::storage::record;
    use crate::write::indexer::tree::posting_list::PostingList;
    use opendata_macros::storage_test;
    use slatedb::{Db, WriteBatch};

    async fn batch_write(storage: &Db, ops: Vec<record::StorageOp>) {
        let mut batch = WriteBatch::new();
        for op in ops {
            record::apply_to_batch(&mut batch, op);
        }
        storage.write(batch).await.unwrap();
    }

    #[storage_test]
    async fn should_read_and_write_vector_data(storage: Arc<Db>) {
        // given
        let values = vec![1.0, 2.0, 3.0];
        let attributes = vec![
            ("vector".to_string(), AttributeValue::Vector(values.clone())),
            (
                "category".to_string(),
                AttributeValue::String("shoes".to_string()),
            ),
            ("price".to_string(), AttributeValue::Int64(99)),
        ];

        // when - write
        let op = record::put_vector_data(VectorId::data_vector_id(42), "vec-1", &attributes);
        batch_write(&storage, vec![op]).await;

        // then - read
        let result = storage
            .get_vector_data(VectorId::data_vector_id(42), 3)
            .await
            .unwrap();
        assert!(result.is_some());
        let data = result.unwrap();
        assert_eq!(data.vector_field(), values.as_slice());
        assert_eq!(data.external_id(), "vec-1");
        assert_eq!(data.string_field("category"), Some("shoes"));
        assert_eq!(data.int64_field("price"), Some(99));
    }

    #[storage_test]
    async fn should_read_empty_posting_list_when_not_exists(storage: Arc<Db>) {
        // given
        let centroid_id = VectorId::centroid_id(1, 1);

        // when
        let result = storage.get_posting_list(centroid_id, 3).await.unwrap();

        // then
        assert!(result.is_empty());
    }

    #[storage_test]
    async fn should_write_and_read_id_dictionary(storage: Arc<Db>) {
        // given
        let id = VectorId::data_vector_id(42);

        // when - write
        let op = record::put_id_dictionary("vec-1", id);
        batch_write(&storage, vec![op]).await;

        // then - read using IdDictionary directly
        let key = IdDictionaryKey::new("vec-1").encode();
        let value = storage.get(&key).await.unwrap().unwrap();
        let decoded = IdDictionaryValue::decode_from_bytes(&value).unwrap();
        assert_eq!(decoded.vector_id, id);
    }

    #[storage_test]
    async fn should_write_and_read_vector_index_data(storage: Arc<Db>) {
        // given
        let vector_id = VectorId::data_vector_id(42);
        let postings = vec![VectorId::centroid_id(1, 7)];
        let value = VectorIndexDataValue::new(
            postings.clone(),
            vec![crate::serde::vector_data::Field::string(
                "indexed_color",
                "blue",
            )],
        );

        // when
        let op = record::put_vector_index_data(vector_id, value.clone());
        batch_write(&storage, vec![op]).await;

        // then
        let result = storage.get_vector_index_data(vector_id).await.unwrap();
        assert_eq!(result, Some(value));
    }

    #[storage_test(merge_operator = VectorDbMergeOperator::new(3))]
    async fn should_write_and_read_posting_list(storage: Arc<Db>) {
        // given
        let postings = vec![
            PostingUpdate::append(VectorId::data_vector_id(1), vec![1.0, 2.0, 3.0]),
            PostingUpdate::append(VectorId::data_vector_id(2), vec![4.0, 5.0, 6.0]),
        ];
        let centroid_id = VectorId::centroid_id(1, 1);

        // when - write
        let op = record::merge_posting_list(centroid_id, postings).unwrap();
        batch_write(&storage, vec![op]).await;

        // then - read
        let result = storage.get_posting_list(centroid_id, 3).await.unwrap();
        let result: Vec<_> = PostingList::from_value(result).into_iter().collect();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id(), VectorId::data_vector_id(1));
        assert_eq!(result[1].id(), VectorId::data_vector_id(2));
    }
}

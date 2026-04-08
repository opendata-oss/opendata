use async_trait::async_trait;
use common::{BytesRange, StorageRead};

use crate::error::{Error, Result};
use crate::serde::centroid_chunk::CentroidChunkValue;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::centroids::CentroidsValue;
use crate::serde::deletions::DeletionsValue;
use crate::serde::id_dictionary::IdDictionaryValue;
use crate::serde::key::{
    CentroidChunkKey, CentroidInfoKey, CentroidStatsKey, CentroidsKey, DeletionsKey,
    IdDictionaryKey, MetadataIndexKey, PostingListKey, VectorDataKey,
};
use crate::serde::metadata_index::MetadataIndexValue;
use crate::serde::posting_list::PostingListValue;
use crate::serde::vector_data::VectorDataValue;
use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
use bytes::{BufMut, BytesMut};
use std::ops::Bound::Included;

pub(crate) mod merge_operator;
pub(crate) mod record;

/// Result of scanning all centroid chunks from storage.
#[allow(dead_code)]
pub(crate) struct CentroidScanResult {
    /// All centroid entries across all chunks.
    pub(crate) entries: Vec<crate::serde::centroid_chunk::CentroidEntry>,
    /// The chunk ID of the last (highest-numbered) chunk seen.
    pub(crate) last_chunk_id: u32,
    /// The number of entries in the last chunk.
    pub(crate) last_chunk_count: usize,
}

/// Extension trait for StorageRead that provides vector database-specific loading methods.
///
/// These methods are marked as `#[allow(dead_code)]` because they are used by the query path
/// (VectorDb::search and related methods) which is only called in tests and during actual
/// queries. The compiler doesn't see them as used during compilation of the library crate
/// alone, but they are essential for the search functionality.
#[async_trait]
pub(crate) trait VectorDbStorageReadExt: StorageRead {
    /// Look up internal ID from external ID in the ID dictionary.
    async fn lookup_internal_id(&self, external_id: &str) -> Result<Option<VectorId>> {
        let key = IdDictionaryKey::new(external_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let slice = record.value.as_ref();
                let value = IdDictionaryValue::decode_from_bytes(slice)?;
                Ok(Some(value.vector_id))
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
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = VectorDataValue::decode_from_bytes(&record.value, dimensions)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn get_root_posting_list(&self, dimensions: usize) -> Result<PostingListValue> {
        self.get_posting_list(ROOT_VECTOR_ID, dimensions).await
    }

    async fn get_centroids_meta(&self) -> Result<Option<CentroidsValue>> {
        let key = CentroidsKey::new().encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => Ok(Some(CentroidsValue::decode_from_bytes(&record.value)?)),
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    async fn get_centroid_info(&self, centroid_id: VectorId) -> Result<Option<CentroidInfoValue>> {
        let key = CentroidInfoKey::new(centroid_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => Ok(Some(CentroidInfoValue::decode_from_bytes(&record.value)?)),
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
        let record = self
            .scan(BytesRange::new(Included(key.clone()), Included(key_next)))
            .await?;
        match record.first() {
            Some(record) => {
                if record.key != key {
                    return Ok(PostingListValue::new());
                }
                let value = PostingListValue::decode_from_bytes(&record.value, dimensions)?;
                Ok(value)
            }
            None => Ok(PostingListValue::new()),
        }
    }

    /// Load the deleted vectors bitmap.
    #[allow(dead_code)]
    async fn get_deleted_vectors(&self) -> Result<DeletionsValue> {
        let key = DeletionsKey::new().encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = DeletionsValue::decode_from_bytes(&record.value)?;
                Ok(value)
            }
            None => Ok(DeletionsValue::new()),
        }
    }

    /// Load a centroid chunk by chunk_id.
    #[allow(dead_code)]
    async fn get_centroid_chunk(
        &self,
        chunk_id: u32,
        dimensions: usize,
    ) -> Result<Option<CentroidChunkValue>> {
        let key = CentroidChunkKey::new(chunk_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = CentroidChunkValue::decode_from_bytes(&record.value, dimensions)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Load centroid stats (vector count) for a centroid.
    ///
    /// Returns a zero count if no stats exist yet.
    #[allow(dead_code)]
    async fn get_centroid_stats(&self, centroid_id: VectorId) -> Result<CentroidStatsValue> {
        let key = CentroidStatsKey::new(centroid_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = CentroidStatsValue::decode_from_bytes(&record.value).map_err(|e| {
                    Error::Encoding(format!("failed to decode CentroidStatsValue: {e}"))
                })?;
                Ok(value)
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
        let records = self.scan(range).await?;

        let mut stats = Vec::new();
        for record in records {
            let key = CentroidStatsKey::decode(&record.key)?;
            let value = CentroidStatsValue::decode_from_bytes(&record.value).map_err(|e| {
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
        let records = self.scan(range).await?;

        let mut centroids = Vec::with_capacity(records.len());
        for record in records {
            let key = CentroidInfoKey::decode(&record.key)?;
            let value = CentroidInfoValue::decode_from_bytes(&record.value)?;
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
        let records = self.scan(range).await?;

        let mut posting_lists = Vec::with_capacity(records.len());
        for record in records {
            let key = PostingListKey::decode(&record.key)?;
            let value = PostingListValue::decode_from_bytes(&record.value, dimensions)?;
            posting_lists.push((key.centroid_id, value));
        }
        Ok(posting_lists)
    }

    async fn scan_all_inner_posting_lists(
        &self,
        dimensions: usize,
    ) -> Result<Vec<(VectorId, PostingListValue)>> {
        let records = self.scan(PostingListKey::inner_level_bytes_range()).await?;

        let mut posting_lists = Vec::with_capacity(records.len());
        for record in records {
            let key = PostingListKey::decode(&record.key)?;
            let value = PostingListValue::decode_from_bytes(&record.value, dimensions)?;
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
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = MetadataIndexValue::decode_from_bytes(&record.value)?;
                Ok(value)
            }
            None => Ok(MetadataIndexValue::new()),
        }
    }

    /// Scan all centroid chunks to load centroids.
    ///
    /// This scans all records with the CentroidChunk prefix and collects
    /// all centroid entries from all chunks. Also returns the last chunk's
    /// ID and entry count for initializing chunk tracking state.
    #[allow(dead_code)]
    async fn scan_all_centroids(&self, dimensions: usize) -> Result<CentroidScanResult> {
        // Create prefix for all CentroidChunk records
        let mut prefix_buf = bytes::BytesMut::with_capacity(3);
        crate::serde::RecordType::CentroidChunk
            .prefix()
            .write_to(&mut prefix_buf);
        let prefix = prefix_buf.freeze();

        // Use BytesRange::prefix to create the scan range
        let range = common::BytesRange::prefix(prefix);
        let records = self.scan(range).await?;

        let mut all_centroids = Vec::new();
        let mut last_chunk_id: u32 = 0;
        let mut last_chunk_count: usize = 0;
        for record in records {
            let key = CentroidChunkKey::decode(&record.key)?;
            let chunk = CentroidChunkValue::decode_from_bytes(&record.value, dimensions)?;
            let count = chunk.entries.len();
            all_centroids.extend(chunk.entries);
            last_chunk_id = key.chunk_id;
            last_chunk_count = count;
        }

        Ok(CentroidScanResult {
            entries: all_centroids,
            last_chunk_id,
            last_chunk_count,
        })
    }
}

// Implement the trait for all types that implement StorageRead
impl<T: ?Sized + StorageRead> VectorDbStorageReadExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AttributeValue;
    use crate::serde::id_dictionary::IdDictionaryValue;
    use crate::serde::posting_list::{PostingList, PostingUpdate};
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use crate::storage::record;
    use common::Storage;
    use common::storage::in_memory::InMemoryStorage;
    use roaring::RoaringTreemap;
    use std::sync::Arc;

    #[tokio::test]
    async fn should_read_and_write_vector_data() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
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
        storage.apply(vec![op]).await.unwrap();

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

    #[tokio::test]
    async fn should_read_empty_posting_list_when_not_exists() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let centroid_id = VectorId::centroid_id(1, 1);

        // when
        let result = storage.get_posting_list(centroid_id, 3).await.unwrap();

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
        let id = VectorId::data_vector_id(42);

        // when - write
        let op = record::put_id_dictionary("vec-1", id);
        storage.apply(vec![op]).await.unwrap();

        // then - read using IdDictionary directly
        let key = IdDictionaryKey::new("vec-1").encode();
        let record = storage.get(key).await.unwrap();
        let value = IdDictionaryValue::decode_from_bytes(&record.unwrap().value).unwrap();
        assert_eq!(value.vector_id, id);
    }

    #[tokio::test]
    async fn should_write_and_read_posting_list() {
        // given
        let merge_op = Arc::new(VectorDbMergeOperator::new(3));
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(merge_op));
        let postings = vec![
            PostingUpdate::append(VectorId::data_vector_id(1), vec![1.0, 2.0, 3.0]),
            PostingUpdate::append(VectorId::data_vector_id(2), vec![4.0, 5.0, 6.0]),
        ];
        let centroid_id = VectorId::centroid_id(1, 1);

        // when - write
        let op = record::merge_posting_list(centroid_id, postings).unwrap();
        storage.apply(vec![op]).await.unwrap();

        // then - read
        let result = storage.get_posting_list(centroid_id, 3).await.unwrap();
        let result: PostingList = result.into();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id(), VectorId::data_vector_id(1));
        assert_eq!(result[1].id(), VectorId::data_vector_id(2));
    }

    #[tokio::test]
    async fn should_write_and_read_deleted_vectors() {
        // given
        let merge_op = Arc::new(VectorDbMergeOperator::new(3));
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(merge_op));
        let mut deleted = RoaringTreemap::new();
        deleted.insert(1);
        deleted.insert(2);
        deleted.insert(3);

        // when - write
        let op = record::merge_deleted_vectors(deleted).unwrap();
        storage.apply(vec![op]).await.unwrap();

        // then - read
        let result = storage.get_deleted_vectors().await.unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(1));
        assert!(result.contains(2));
        assert!(result.contains(3));
    }
}

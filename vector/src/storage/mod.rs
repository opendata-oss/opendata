use anyhow::{Context, Result};
use async_trait::async_trait;
use common::StorageRead;

use crate::serde::centroid_chunk::CentroidChunkValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::deletions::DeletionsValue;
use crate::serde::key::{
    CentroidChunkKey, CentroidStatsKey, DeletionsKey, IdDictionaryKey, PostingListKey,
    VectorDataKey,
};
use crate::serde::posting_list::PostingListValue;
use crate::serde::vector_data::VectorDataValue;

pub(crate) mod merge_operator;
pub(crate) mod record;

/// Result of scanning all centroid chunks from storage.
pub(crate) struct CentroidScanResult {
    /// All centroid entries across all chunks.
    pub(crate) entries: Vec<crate::serde::centroid_chunk::CentroidEntry>,
    /// The chunk ID of the last (highest-numbered) chunk seen.
    #[allow(dead_code)]
    pub(crate) last_chunk_id: u32,
    /// The number of entries in the last chunk.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    ///
    /// Requires dimensions to decode the vector field.
    #[allow(dead_code)]
    async fn get_vector_data(
        &self,
        internal_id: u64,
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

    /// Load a posting list for a centroid.
    ///
    /// Requires dimensions to decode the embedded vector data.
    #[allow(dead_code)]
    async fn get_posting_list(
        &self,
        centroid_id: u64,
        dimensions: usize,
    ) -> Result<PostingListValue> {
        let key = PostingListKey::new(centroid_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
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
    async fn get_centroid_stats(&self, centroid_id: u64) -> Result<CentroidStatsValue> {
        let key = CentroidStatsKey::new(centroid_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = CentroidStatsValue::decode_from_bytes(&record.value)
                    .context("failed to decode CentroidStatsValue")?;
                Ok(value)
            }
            None => Ok(CentroidStatsValue::new(0)),
        }
    }

    /// Scan all centroid stats records.
    ///
    /// Returns a map of centroid_id to accumulated vector count.
    #[allow(dead_code)]
    async fn scan_all_centroid_stats(&self) -> Result<Vec<(u64, CentroidStatsValue)>> {
        let mut prefix_buf = bytes::BytesMut::with_capacity(2);
        crate::serde::RecordType::CentroidStats
            .prefix()
            .write_to(&mut prefix_buf);
        let prefix = prefix_buf.freeze();

        let range = common::BytesRange::prefix(prefix);
        let records = self.scan(range).await?;

        let mut stats = Vec::new();
        for record in records {
            let key = CentroidStatsKey::decode(&record.key)?;
            let value = CentroidStatsValue::decode_from_bytes(&record.value)
                .context("failed to decode CentroidStatsValue")?;
            stats.push((key.centroid_id, value));
        }

        Ok(stats)
    }

    /// Scan all centroid chunks to load centroids.
    ///
    /// This scans all records with the CentroidChunk prefix and collects
    /// all centroid entries from all chunks. Also returns the last chunk's
    /// ID and entry count for initializing chunk tracking state.
    async fn scan_all_centroids(&self, dimensions: usize) -> Result<CentroidScanResult> {
        // Create prefix for all CentroidChunk records
        let mut prefix_buf = bytes::BytesMut::with_capacity(2);
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
        let op = record::put_vector_data(42, "vec-1", &attributes);
        storage.apply(vec![op]).await.unwrap();

        // then - read
        let result = storage.get_vector_data(42, 3).await.unwrap();
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

        // when
        let result = storage.get_posting_list(1, 3).await.unwrap();

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
        let op = record::put_id_dictionary("vec-1", 42);
        storage.apply(vec![op]).await.unwrap();

        // then - read using IdDictionary directly
        let key = IdDictionaryKey::new("vec-1").encode();
        let record = storage.get(key).await.unwrap();
        assert!(record.is_some());
    }

    #[tokio::test]
    async fn should_write_and_read_posting_list() {
        // given
        let merge_op = Arc::new(VectorDbMergeOperator::new(3));
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(merge_op));
        let postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0, 3.0]),
            PostingUpdate::append(2, vec![4.0, 5.0, 6.0]),
        ];

        // when - write
        let op = record::merge_posting_list(1, postings).unwrap();
        storage.apply(vec![op]).await.unwrap();

        // then - read
        let result = storage.get_posting_list(1, 3).await.unwrap();
        let result: PostingList = result.into();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id(), 1);
        assert_eq!(result[1].id(), 2);
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

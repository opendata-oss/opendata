use async_trait::async_trait;
use common::storage::slate::SlateDbStorage;
use common::{
    BytesRange, StorageBuilder, StorageRead, StorageSemantics, new_slatedb_compactor_builder,
};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::model::Config;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::centroids::CentroidsValue;
use crate::serde::deletions::DeletionsValue;
use crate::serde::field_stats::FieldStatsValue;
use crate::serde::id_dictionary::IdDictionaryValue;
use crate::serde::key::{
    CentroidInfoKey, CentroidStatsKey, CentroidsKey, DeletionsKey, FieldStatsKey, IdDictionaryKey,
    MetadataIndexKey, PostingListKey, VectorDataKey, VectorIndexDataKey,
};
use crate::serde::metadata_index::MetadataIndexValue;
use crate::serde::posting_list::PostingListValue;
use crate::serde::vector_bitmap::VectorBitmap;
use crate::serde::vector_data::VectorDataValue;
use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
use crate::serde::vector_index_data::VectorIndexDataValue;
use bytes::{BufMut, BytesMut};
use std::ops::Bound::Included;

pub(crate) mod compaction_filter;
pub(crate) mod compaction_scheduler;
pub(crate) mod merge_operator;
pub(crate) mod record;
pub(crate) mod segment_extractor;

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

    async fn get_vector_index_data(
        &self,
        internal_id: VectorId,
    ) -> Result<Option<VectorIndexDataValue>> {
        let key = VectorIndexDataKey::new(internal_id).encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = VectorIndexDataValue::decode_from_bytes(&record.value)?;
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
        let mut prefix_buf = bytes::BytesMut::with_capacity(crate::serde::PREFIX_AND_TAG_LEN);
        crate::serde::RecordType::CentroidStats.write_prefix(&mut prefix_buf);
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
        let mut prefix_buf = bytes::BytesMut::with_capacity(crate::serde::PREFIX_AND_TAG_LEN);
        crate::serde::RecordType::CentroidInfo.write_prefix(&mut prefix_buf);
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
        let mut prefix_buf = bytes::BytesMut::with_capacity(crate::serde::PREFIX_AND_TAG_LEN);
        crate::serde::RecordType::PostingList.write_prefix(&mut prefix_buf);
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

    /// Load the singleton FTS deletions bitmap.
    ///
    /// Returns a bitmap of internal vector IDs that have been deleted or
    /// replaced. Returns an empty bitmap if the record does not exist yet.
    async fn get_deletions(&self) -> Result<VectorBitmap> {
        let key = DeletionsKey::new().encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let value = DeletionsValue::decode_from_bytes(&record.value).map_err(|e| {
                    Error::Encoding(format!("failed to decode DeletionsValue: {e}"))
                })?;
                Ok(value.0)
            }
            None => Ok(VectorBitmap::new()),
        }
    }

    /// Scan all per-field [`FieldStatsValue`] records, returning
    /// `(field name, stats)` pairs. Used by the FieldStats poller to feed the
    /// custom compaction scheduler's in-memory per-field tracker (RFC-0006).
    async fn scan_field_stats(&self) -> Result<Vec<(String, FieldStatsValue)>> {
        let mut prefix_buf = bytes::BytesMut::with_capacity(crate::serde::PREFIX_AND_TAG_LEN);
        crate::serde::RecordType::FtsFieldStats.write_prefix(&mut prefix_buf);
        let prefix = prefix_buf.freeze();

        let range = common::BytesRange::prefix(prefix);
        let records = self.scan(range).await?;

        let mut stats = Vec::with_capacity(records.len());
        for record in records {
            let key = FieldStatsKey::decode(&record.key)?;
            let value = FieldStatsValue::decode_from_bytes(&record.value)
                .map_err(|e| Error::Encoding(format!("failed to decode FieldStatsValue: {e}")))?;
            stats.push((key.field, value));
        }
        Ok(stats)
    }
}

// Implement the trait for all types that implement StorageRead
impl<T: ?Sized + StorageRead> VectorDbStorageReadExt for T {}

/// Builds the storage backing a vector database, installing the shared vector
/// wiring used by both [`VectorDb`](crate::VectorDb) and
/// [`VectorDbAdmin`](crate::VectorDbAdmin):
///
/// - the per-segment routing extractor (Default/ANN/FTS each get their own
///   SlateDB segment so the FTS segment can be compacted independently);
/// - the merge operator (on both the `DbBuilder` write path and the compactor);
/// - for SlateDB with compaction enabled, a compactor carrying the FTS
///   delete-cleanup filter and — when `fts_delete_tracker` is `Some` — the
///   custom scheduler that forces a major FTS compaction once outstanding
///   deletes cross `config.delete_compaction_threshold` (RFC-0006).
///
/// Pass `Some(tracker)` for the writer, whose flusher refreshes the tracker
/// after each flush; pass `None` for clients with no flusher (e.g.
/// `VectorDbAdmin`), which then keep SlateDB's default size-tiered scheduler —
/// the filter still runs on whatever compactions size-tiered schedules.
///
/// All compaction wiring is a no-op for the in-memory backend and when
/// compaction is disabled.
pub(crate) async fn build_vector_storage(
    config: &Config,
    builder: StorageBuilder,
    fts_delete_tracker: Option<Arc<compaction_scheduler::FtsDeleteTracker>>,
) -> Result<Arc<dyn common::Storage>> {
    let merge_op: Arc<merge_operator::VectorDbMergeOperator> = Arc::new(
        merge_operator::VectorDbMergeOperator::new(config.dimensions as usize),
    );
    // Route each vector segment into its own SlateDB segment.
    let builder = segment_extractor::builder_with_vector_segments(builder);
    // Build the FTS-aware compactor and install it via the `map_slatedb` escape
    // hatch. `new_slatedb_compactor_builder` returns `None` for the in-memory
    // backend and when compaction is disabled, leaving the builder untouched.
    let builder = match new_slatedb_compactor_builder(&config.storage)
        .map_err(|e| Error::Storage(format!("Failed to create compactor: {e}")))?
    {
        Some(compactor_builder) => {
            let mut compactor_builder = compactor_builder.with_compaction_filter_supplier(
                compaction_filter::VectorCompactionFilterSupplier::shared(),
            );
            if let Some(tracker) = fts_delete_tracker {
                compactor_builder = compactor_builder.with_scheduler_supplier(
                    compaction_scheduler::VectorCompactionSchedulerSupplier::shared(
                        tracker,
                        config.delete_compaction_threshold,
                    ),
                );
            }
            // The merge operator lets the compactor apply vector merges. (SlateDB
            // also propagates the DbBuilder's merge operator onto the compactor,
            // so this is belt-and-suspenders, but keeps the compactor builder
            // self-contained.)
            let compactor_builder = compactor_builder.with_merge_operator(Arc::new(
                SlateDbStorage::merge_operator_adapter(merge_op.clone()),
            ));
            builder.map_slatedb(move |db| db.with_compactor_builder(compactor_builder))
        }
        None => builder,
    };
    builder
        .with_semantics(StorageSemantics::new().with_merge_operator(merge_op))
        .build()
        .await
        .map_err(|e| Error::Storage(format!("Failed to create storage: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AttributeValue;
    use crate::serde::id_dictionary::IdDictionaryValue;
    use crate::serde::posting_list::PostingUpdate;
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use crate::storage::record;
    use crate::write::indexer::tree::posting_list::PostingList;
    use common::Storage;
    use common::storage::in_memory::InMemoryStorage;

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
    async fn should_write_and_read_vector_index_data() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let vector_id = VectorId::data_vector_id(42);
        let postings = vec![VectorId::centroid_id(1, 7)];
        let value = VectorIndexDataValue::new(
            postings.clone(),
            vec![crate::serde::vector_data::Field::string(
                "indexed_color",
                "blue",
            )],
            vec!["body".to_string()],
        );

        // when
        let op = record::put_vector_index_data(vector_id, value.clone());
        storage.apply(vec![op]).await.unwrap();

        // then
        let result = storage.get_vector_index_data(vector_id).await.unwrap();
        assert_eq!(result, Some(value));
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
        let result: Vec<_> = PostingList::from_value(result, false).into_iter().collect();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].id(), VectorId::data_vector_id(1));
        assert_eq!(result[1].id(), VectorId::data_vector_id(2));
    }
}

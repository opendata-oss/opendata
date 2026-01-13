//! Vector database implementation with atomic flush semantics.
//!
//! This module provides the main `VectorDb` struct that handles:
//! - Vector ingestion with validation
//! - In-memory delta buffering
//! - Atomic flush with ID allocation
//! - Snapshot management for consistency
//!
//! The implementation follows the MiniTsdb pattern from the timeseries module,
//! adapted for vector data with external ID tracking and atomic upsert semantics.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use common::sequence::{SeqBlockStore, SequenceAllocator};
use common::storage::{Storage, StorageRead};
use roaring::RoaringTreemap;
use tokio::sync::{Mutex, RwLock};

use crate::delta::{VectorDbDelta, VectorDbDeltaBuilder};
use crate::dictionary::Dictionary;
use crate::model::{Config, Vector};
use crate::serde::key::SeqBlockKey;
use crate::storage::VectorDbStorageExt;

/// Vector database for storing and querying embedding vectors.
///
/// `VectorDb` provides a high-level API for ingesting vectors with metadata.
/// It handles internal details like ID allocation, centroid assignment (stubbed),
/// and metadata index maintenance automatically.
pub struct VectorDb {
    config: Config,
    storage: Arc<dyn Storage>,
    dictionary: Arc<Dictionary>,
    id_allocator: Arc<SequenceAllocator>,

    /// Pending delta accumulating ingested data not yet flushed to storage.
    pending_delta: Mutex<VectorDbDelta>,
    /// Receiver for waiting for updates to pending delta
    pending_delta_watch_rx: tokio::sync::watch::Receiver<Instant>,
    /// Sender for updating the pending delta when flushing.
    pending_delta_watch_tx: tokio::sync::watch::Sender<Instant>,
    /// Storage snapshot used for queries.
    snapshot: RwLock<Arc<dyn StorageRead>>,
    /// Mutex to ensure only one flush operation can run at a time.
    flush_mutex: Arc<Mutex<()>>,
}

impl VectorDb {
    /// Open or create a vector database with the given configuration.
    ///
    /// If the database already exists, the configuration must be compatible:
    /// - `dimensions` must match exactly
    /// - `distance_metric` must match exactly
    ///
    /// Other configuration options (like `flush_interval`) can be changed
    /// on subsequent opens.
    pub async fn open(config: Config) -> Result<Self> {
        let storage = Arc::clone(&config.storage);

        // Initialize dictionary for external ID → internal ID mapping
        let dictionary = Arc::new(Dictionary::new(Arc::clone(&storage)).await?);

        // Initialize sequence allocator for internal ID generation
        let seq_key = SeqBlockKey.encode();
        let block_store = SeqBlockStore::new(Arc::clone(&storage), seq_key);
        let id_allocator = Arc::new(SequenceAllocator::new(block_store));
        id_allocator.initialize().await?;

        // Get initial snapshot
        let snapshot = storage.snapshot().await?;

        // Create watch channel for pending delta backpressure
        let (tx, rx) = tokio::sync::watch::channel(Instant::now());

        Ok(Self {
            config,
            storage,
            dictionary,
            id_allocator,
            pending_delta: Mutex::new(VectorDbDelta::empty()),
            pending_delta_watch_tx: tx,
            pending_delta_watch_rx: rx,
            snapshot: RwLock::new(snapshot),
            flush_mutex: Arc::new(Mutex::new(())),
        })
    }

    /// Write vectors to the database.
    ///
    /// This is the primary write method. It accepts a batch of vectors and
    /// returns when the data has been accepted for ingestion (but not
    /// necessarily flushed to durable storage).
    ///
    /// # Atomicity
    ///
    /// This operation is atomic: either all vectors in the batch are accepted,
    /// or none are. This matches the behavior of `TimeSeries::write()`.
    ///
    /// # Upsert Semantics
    ///
    /// Writing a vector with an ID that already exists performs an upsert:
    /// the old vector is deleted and replaced with the new one. The system
    /// allocates a new internal ID for the updated vector and marks the old
    /// internal ID as deleted. This ensures index structures are updated
    /// correctly without expensive read-modify-write cycles.
    ///
    /// # Validation
    ///
    /// The following validations are performed:
    /// - Vector dimensions must match `Config::dimensions`
    /// - Attribute names must be defined in `Config::metadata_fields` (if specified)
    /// - Attribute types must match the schema
    pub async fn write(&self, vectors: Vec<Vector>) -> Result<()> {
        // Block until the pending delta is young enough (not older than 2 * flush_interval)
        let max_age = self.config.flush_interval * 2;
        let mut receiver = self.pending_delta_watch_rx.clone();
        receiver
            .wait_for(|t| t.elapsed() <= max_age)
            .await
            .context("pending delta watch_rx disconnected")?;

        // Build delta from vectors
        let mut builder = VectorDbDeltaBuilder::new(&self.config);
        for vector in vectors {
            builder.write(vector).await?;
        }
        let delta = builder.build();

        // Merge into pending delta
        {
            let mut pending = self.pending_delta.lock().await;
            pending.merge(delta);
        }

        Ok(())
    }

    /// Force flush all pending data to durable storage.
    ///
    /// Normally data is flushed according to `flush_interval`, but this
    /// method can be used to ensure durability immediately.
    ///
    /// # Atomic Flush
    ///
    /// The flush operation is atomic:
    /// 1. Lookup old internal IDs from dictionary (if they exist)
    /// 2. Allocate new internal IDs from sequence allocator
    /// 3. Build all RecordOps (dictionary updates, deletes, new records)
    /// 4. Apply everything in one atomic batch via `storage.apply()`
    ///
    /// This ensures dictionary updates, deletes, and new records are all
    /// applied together, maintaining consistency.
    pub async fn flush(&self) -> Result<()> {
        let _flush_guard = self.flush_mutex.lock().await;

        // Take the pending delta (replace with empty)
        let (delta, created_at) = {
            let mut pending = self.pending_delta.lock().await;
            if pending.is_empty() {
                return Ok(());
            }
            let delta = std::mem::replace(&mut *pending, VectorDbDelta::empty());
            (delta, Instant::now())
        };

        // Notify any waiting writers
        self.pending_delta_watch_tx.send_if_modified(|current| {
            if created_at > *current {
                *current = created_at;
                true
            } else {
                false
            }
        });

        // Build RecordOps atomically
        let mut ops = Vec::new();

        // Batch posting list updates (collect all updates per centroid)
        use std::collections::HashMap;
        let mut posting_lists: HashMap<u32, RoaringTreemap> = HashMap::new();
        let mut deleted_vectors = RoaringTreemap::new();

        for (external_id, pending_vec) in delta.vectors {
            // 1. Lookup old internal_id (if exists)
            let external_id_bytes = Bytes::from(external_id.clone());
            let old_internal_id = self.dictionary.lookup(external_id_bytes.clone()).await?;

            // 2. Allocate new internal_id
            let new_internal_id = self.id_allocator.allocate_one().await?;

            // 3. Update IdDictionary
            if old_internal_id.is_some() {
                ops.push(self.storage.delete_id_dictionary(&external_id)?);
            }
            ops.push(
                self.storage
                    .put_id_dictionary(&external_id, new_internal_id)?,
            );

            // 4. Handle old vector deletion (if upsert)
            if let Some(old_id) = old_internal_id {
                // Add to deleted bitmap for batch merge later
                deleted_vectors.insert(old_id);

                // Tombstone old records
                ops.push(self.storage.delete_vector_data(old_id)?);
                ops.push(self.storage.delete_vector_meta(old_id)?);
            }

            // 5. Write new vector records
            // VectorData
            ops.push(
                self.storage
                    .put_vector_data(new_internal_id, pending_vec.values().to_vec())?,
            );

            // VectorMeta
            let metadata: Vec<_> = pending_vec
                .metadata()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            ops.push(self.storage.put_vector_meta(
                new_internal_id,
                pending_vec.external_id(),
                &metadata,
            )?);

            // 6. Batch PostingList assignment to centroid_id=1 (STUB)
            // TODO: Real centroid assignment via HNSW
            posting_lists
                .entry(1)
                .or_insert_with(RoaringTreemap::new)
                .insert(new_internal_id);
        }

        // Serialize and merge batched posting lists
        if !deleted_vectors.is_empty() {
            ops.push(self.storage.merge_deleted_vectors(deleted_vectors)?);
        }

        for (centroid_id, bitmap) in posting_lists {
            ops.push(self.storage.merge_posting_list(centroid_id, bitmap)?);
        }

        // ATOMIC: Apply all operations in one batch
        self.storage.apply(ops).await?;

        // Update snapshot for queries
        let new_snapshot = self.storage.snapshot().await?;
        let mut snapshot_guard = self.snapshot.write().await;
        *snapshot_guard = new_snapshot;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MetadataFieldSpec, Vector};
    use crate::serde::FieldType;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::key::{IdDictionaryKey, VectorDataKey, VectorMetaKey};
    use crate::serde::vector_data::VectorDataValue;
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use common::storage::in_memory::InMemoryStorage;
    use std::time::Duration;

    fn create_test_config() -> Config {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator,
        )));
        Config {
            storage,
            dimensions: 3,
            distance_metric: DistanceMetric::Cosine,
            flush_interval: Duration::from_secs(60),
            metadata_fields: vec![
                MetadataFieldSpec::new("category", FieldType::String, true),
                MetadataFieldSpec::new("price", FieldType::Int64, true),
            ],
        }
    }

    #[tokio::test]
    async fn should_open_vector_db() {
        // given
        let config = create_test_config();

        // when
        let result = VectorDb::open(config).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_write_and_flush_vectors() {
        // given
        let config = create_test_config();
        let storage = Arc::clone(&config.storage);
        let db = VectorDb::open(config).await.unwrap();

        let vectors = vec![
            Vector::builder("vec-1", vec![1.0, 0.0, 0.0])
                .attribute("category", "shoes")
                .attribute("price", 99i64)
                .build(),
            Vector::builder("vec-2", vec![0.0, 1.0, 0.0])
                .attribute("category", "boots")
                .attribute("price", 149i64)
                .build(),
        ];

        // when
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // then - verify records exist in storage
        // Check VectorData records
        let vec1_data_key = VectorDataKey::new(0).encode();
        let vec1_data = storage.get(vec1_data_key).await.unwrap();
        assert!(vec1_data.is_some());

        let vec2_data_key = VectorDataKey::new(1).encode();
        let vec2_data = storage.get(vec2_data_key).await.unwrap();
        assert!(vec2_data.is_some());

        // Check VectorMeta records
        let vec1_meta_key = VectorMetaKey::new(0).encode();
        let vec1_meta = storage.get(vec1_meta_key).await.unwrap();
        assert!(vec1_meta.is_some());

        // Check IdDictionary
        let dict_key1 = IdDictionaryKey::new("vec-1").encode();
        let dict_entry1 = storage.get(dict_key1).await.unwrap();
        assert!(dict_entry1.is_some());
    }

    #[tokio::test]
    async fn should_upsert_existing_vector() {
        // given
        let config = create_test_config();
        let storage = Arc::clone(&config.storage);
        let db = VectorDb::open(config).await.unwrap();

        // First write
        let vector1 = Vector::builder("vec-1", vec![1.0, 0.0, 0.0])
            .attribute("category", "shoes")
            .attribute("price", 99i64)
            .build();
        db.write(vec![vector1]).await.unwrap();
        db.flush().await.unwrap();

        // when - upsert with same ID but different values
        let vector2 = Vector::builder("vec-1", vec![2.0, 3.0, 4.0])
            .attribute("category", "boots")
            .attribute("price", 199i64)
            .build();
        db.write(vec![vector2]).await.unwrap();
        db.flush().await.unwrap();

        // then - verify new vector data
        let vec_data_key = VectorDataKey::new(1).encode(); // New internal ID
        let vec_data = storage.get(vec_data_key).await.unwrap();
        assert!(vec_data.is_some());
        let decoded = VectorDataValue::decode_from_bytes(&vec_data.unwrap().value).unwrap();
        assert_eq!(decoded.vector, vec![2.0, 3.0, 4.0]);

        // Verify only one IdDictionary entry
        let dict_key = IdDictionaryKey::new("vec-1").encode();
        let dict_entry = storage.get(dict_key).await.unwrap();
        assert!(dict_entry.is_some());
    }

    #[tokio::test]
    async fn should_reject_vectors_with_wrong_dimensions() {
        // given
        let config = create_test_config();
        let db = VectorDb::open(config).await.unwrap();

        let vector = Vector::new("vec-1", vec![1.0, 2.0]); // Wrong: 2 instead of 3

        // when
        let result = db.write(vec![vector]).await;

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("dimension mismatch")
        );
    }

    #[tokio::test]
    async fn should_flush_empty_delta_without_error() {
        // given
        let config = create_test_config();
        let db = VectorDb::open(config).await.unwrap();

        // when
        let result = db.flush().await;

        // then
        assert!(result.is_ok());
    }
}

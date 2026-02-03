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
use common::sequence::{SeqBlockStore, SequenceAllocator};
use common::storage::factory::create_storage;
use common::storage::{Storage, StorageRead};
use roaring::RoaringTreemap;
use tokio::sync::{Mutex, RwLock};

use crate::storage::merge_operator::VectorDbMergeOperator;

use std::collections::HashMap;

use crate::delta::{VectorDbDelta, VectorDbDeltaBuilder};
use crate::distance;
use crate::hnsw::{CentroidGraph, build_centroid_graph};
use crate::model::{AttributeValue, Config, Vector};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::key::SeqBlockKey;
use crate::serde::posting_list::{PostingList, PostingUpdate};
use crate::storage::{VectorDbStorageExt, VectorDbStorageReadExt};

/// Vector database for storing and querying embedding vectors.
///
/// `VectorDb` provides a high-level API for ingesting vectors with metadata.
/// It handles internal details like ID allocation, centroid assignment (stubbed),
/// and metadata index maintenance automatically.
pub struct VectorDb {
    config: Config,
    storage: Arc<dyn Storage>,
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
    /// In-memory HNSW graph for centroid search (loaded lazily).
    centroid_graph: RwLock<Option<Box<dyn CentroidGraph>>>,
}

/// A search result with vector, score, and metadata.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal vector ID
    pub internal_id: u64,
    /// External vector ID (user-provided)
    pub external_id: String,
    /// Similarity score (interpretation depends on distance metric)
    ///
    /// - L2: Lower scores = more similar
    /// - Cosine: Higher scores = more similar (range: -1 to 1)
    /// - DotProduct: Higher scores = more similar
    pub score: f32,
    /// attribute key-value pairs
    pub attributes: HashMap<String, AttributeValue>,
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
        let merge_op = VectorDbMergeOperator::new(config.dimensions as usize);
        let storage = create_storage(&config.storage, Some(Arc::new(merge_op)))
            .await
            .context("Failed to create storage")?;

        Self::new(storage, config).await
    }

    /// Create a vector database with the given storage and configuration.
    ///
    /// This is a crate-visible constructor for tests that need direct access
    /// to the underlying storage for verification.
    pub(crate) async fn new(storage: Arc<dyn Storage>, config: Config) -> Result<Self> {
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
            id_allocator,
            pending_delta: Mutex::new(VectorDbDelta::empty()),
            pending_delta_watch_tx: tx,
            pending_delta_watch_rx: rx,
            snapshot: RwLock::new(snapshot),
            flush_mutex: Arc::new(Mutex::new(())),
            centroid_graph: RwLock::new(None),
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
    /// or none are. This matches the behavior of `TimeSeriesDb::write()`.
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
    /// 1. Lookup old internal IDs from storage (if they exist)
    /// 2. Allocate new internal IDs from sequence allocator
    /// 3. Build all RecordOps (ID dictionary updates, deletes, new records)
    /// 4. Apply everything in one atomic batch via `storage.apply()`
    ///
    /// This ensures ID dictionary updates, deletes, and new records are all
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
        let mut posting_updates: HashMap<u32, Vec<PostingUpdate>> = HashMap::new();
        let mut deleted_vectors = RoaringTreemap::new();

        for (external_id, pending_vec) in delta.vectors {
            // 1. Lookup old internal_id (if exists) from ID dictionary
            let old_internal_id = self.storage.lookup_internal_id(&external_id).await?;

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

                // Tombstone old vector data record
                ops.push(self.storage.delete_vector_data(old_id)?);
            }

            // 5. Write new vector data (includes external_id, vector, and metadata)
            let attributes: Vec<_> = pending_vec
                .attributes()
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            ops.push(self.storage.put_vector_data(
                new_internal_id,
                pending_vec.external_id(),
                &attributes,
            )?);

            // 6. Assign vector to nearest centroid using HNSW
            let centroid_id = self.assign_to_centroid(pending_vec.values()).await?;
            posting_updates
                .entry(centroid_id)
                .or_default()
                .push(PostingUpdate::append(
                    new_internal_id,
                    pending_vec.values().to_vec(),
                ));
        }

        // Serialize and merge batched posting lists
        if !deleted_vectors.is_empty() {
            ops.push(self.storage.merge_deleted_vectors(deleted_vectors)?);
        }

        for (centroid_id, updates) in posting_updates {
            ops.push(self.storage.merge_posting_list(centroid_id, updates)?);
        }

        // ATOMIC: Apply all operations in one batch
        self.storage.apply(ops).await?;

        // Update snapshot for queries
        let new_snapshot = self.storage.snapshot().await?;
        let mut snapshot_guard = self.snapshot.write().await;
        *snapshot_guard = new_snapshot;

        Ok(())
    }

    /// Bootstrap centroids from vectors (for testing).
    ///
    /// This method writes centroid chunks to storage and builds the in-memory
    /// HNSW graph. It's primarily intended for testing to initialize a known
    /// set of centroids without running clustering algorithms.
    ///
    /// # Arguments
    /// * `centroids` - Vector of centroid entries with their IDs and vectors
    ///
    /// # Errors
    /// Returns an error if:
    /// - Centroid dimensions don't match the collection's configured dimensions
    /// - Storage write fails
    /// - HNSW graph construction fails
    pub async fn bootstrap_centroids(&self, centroids: Vec<CentroidEntry>) -> Result<()> {
        // Validate centroid dimensions
        for centroid in &centroids {
            if centroid.dimensions() != self.config.dimensions as usize {
                return Err(anyhow::anyhow!(
                    "Centroid dimension mismatch: expected {}, got {}",
                    self.config.dimensions,
                    centroid.dimensions()
                ));
            }
        }

        // Validate we don't exceed max chunk size (default 4096)
        // TODO: Support splitting centroids across multiple chunks when count > chunk_target
        const DEFAULT_CHUNK_TARGET: usize = 4096;
        if centroids.len() > DEFAULT_CHUNK_TARGET {
            return Err(anyhow::anyhow!(
                "Too many centroids for single chunk: {} > {}. Multi-chunk support not yet implemented.",
                centroids.len(),
                DEFAULT_CHUNK_TARGET
            ));
        }

        // Write centroids to storage (single chunk for simplicity)
        let op = self.storage.put_centroid_chunk(
            0,
            centroids.clone(),
            self.config.dimensions as usize,
        )?;
        self.storage.apply(vec![op]).await?;

        // Build HNSW graph
        let graph = build_centroid_graph(centroids, self.config.distance_metric)?;
        let mut graph_guard = self.centroid_graph.write().await;
        *graph_guard = Some(graph);

        Ok(())
    }

    /// Load centroids from storage and build HNSW graph.
    ///
    /// This method scans all centroid chunks from storage and constructs
    /// the in-memory HNSW graph for fast centroid search during queries.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Storage read fails
    /// - No centroids found in storage
    /// - HNSW graph construction fails
    pub async fn load_centroids(&self) -> Result<()> {
        let snapshot = self.snapshot.read().await;
        let centroids = snapshot
            .scan_all_centroids(self.config.dimensions as usize)
            .await?;

        if centroids.is_empty() {
            return Err(anyhow::anyhow!("No centroids found in storage"));
        }

        let graph = build_centroid_graph(centroids, self.config.distance_metric)?;
        let mut graph_guard = self.centroid_graph.write().await;
        *graph_guard = Some(graph);

        Ok(())
    }

    /// Ensure centroids are loaded, loading them if needed.
    ///
    /// This is called lazily from search() to load centroids on first query.
    async fn ensure_centroids_loaded(&self) -> Result<()> {
        let guard = self.centroid_graph.read().await;
        if guard.is_some() {
            return Ok(());
        }
        drop(guard);

        // Try to load centroids from storage
        match self.load_centroids().await {
            Ok(()) => Ok(()),
            Err(e) => {
                // If no centroids in storage, that's okay - they can be bootstrapped later
                if e.to_string().contains("No centroids found") {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Assign a vector to its nearest centroid using HNSW search.
    ///
    /// # Arguments
    /// * `vector` - Vector to assign
    ///
    /// # Returns
    /// The centroid_id of the nearest centroid, or 1 if no centroids are loaded
    async fn assign_to_centroid(&self, vector: &[f32]) -> Result<u32> {
        let graph_guard = self.centroid_graph.read().await;

        match &*graph_guard {
            Some(graph) => {
                let results = graph.search(vector, 1);
                results
                    .first()
                    .copied()
                    .ok_or_else(|| anyhow::anyhow!("No centroids available"))
            }
            None => {
                // Fallback: If no centroids loaded yet, assign to centroid_id=1
                // This maintains backward compatibility with current stub
                // TODO: Remove this fallback once LIRE centroid splitting is implemented
                // and centroids are always initialized on collection creation
                Ok(1)
            }
        }
    }

    /// Search for k-nearest neighbors to a query vector.
    ///
    /// This implements the SPANN-style query algorithm:
    /// 1. Search HNSW for nearest centroids
    /// 2. Load posting lists for those centroids
    /// 3. Filter deleted vectors
    /// 4. Score candidates and return top-k
    ///
    /// # Arguments
    /// * `query` - Query vector
    /// * `k` - Number of nearest neighbors to return
    ///
    /// # Returns
    /// Vector of SearchResults sorted by similarity (best first)
    ///
    /// # Errors
    /// Returns an error if:
    /// - Query dimensions don't match collection dimensions
    /// - No centroids are loaded
    /// - Storage read fails
    pub async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        // 1. Validate query dimensions
        if query.len() != self.config.dimensions as usize {
            return Err(anyhow::anyhow!(
                "Query dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                query.len()
            ));
        }

        // 2. Ensure centroids are loaded
        self.ensure_centroids_loaded().await?;

        // Check if centroids are actually loaded
        let graph_guard = self.centroid_graph.read().await;
        if graph_guard.is_none() {
            return Ok(Vec::new());
        }
        drop(graph_guard);

        // 3. Search HNSW for nearest centroids (expand by ~10-100x for recall)
        // clamp(10, 100) ensures we search at least 10 centroids (for small k)
        // and at most 100 centroids (to cap memory/latency for large k)
        let num_centroids = k.clamp(10, 100);
        let centroid_ids = self.search_centroids(query, num_centroids).await?;

        if centroid_ids.is_empty() {
            return Ok(Vec::new());
        }

        // 4. Load posting lists and deleted vectors
        let snapshot = self.snapshot.read().await;
        let candidates = self
            .load_candidates(&centroid_ids, snapshot.as_ref())
            .await?;

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // 5. Load deleted vectors bitmap and filter
        let deleted = snapshot.get_deleted_vectors().await?;
        let candidates: Vec<(u64, Vec<f32>)> = candidates
            .into_iter()
            .filter(|(id, _)| !deleted.contains(*id))
            .collect();

        // 6. Score candidates and return top-k
        let results = self
            .score_and_rank(query, &candidates, k, snapshot.as_ref())
            .await?;

        Ok(results)
    }

    /// Search HNSW graph for nearest centroids.
    async fn search_centroids(&self, query: &[f32], k: usize) -> Result<Vec<u32>> {
        let graph_guard = self.centroid_graph.read().await;

        match &*graph_guard {
            Some(graph) => Ok(graph.search(query, k)),
            None => Ok(Vec::new()),
        }
    }

    /// Load candidate vector IDs and their vectors from posting lists.
    async fn load_candidates(
        &self,
        centroid_ids: &[u32],
        snapshot: &dyn StorageRead,
    ) -> Result<Vec<(u64, Vec<f32>)>> {
        let mut all_candidates = Vec::new();
        let dimensions = self.config.dimensions as usize;

        for &centroid_id in centroid_ids {
            let posting_list: PostingList = snapshot
                .get_posting_list(centroid_id, dimensions)
                .await?
                .into();
            // Extract IDs and vectors from postings (only include appends, skip deletes)
            for posting in posting_list.iter() {
                all_candidates.push((posting.id(), posting.vector().to_vec()));
            }
        }

        Ok(all_candidates)
    }

    /// Score candidates and return top-k results.
    ///
    /// TODO: Use a min/max heap to maintain only top-k results in memory instead of
    /// materializing all candidates. This would reduce memory usage for large candidate sets.
    ///
    /// TODO: Consider pipelining this operation so we load and score candidates in batches,
    /// keeping only the current top-k in memory. This would enable processing arbitrarily
    /// large candidate sets without loading them all at once.
    async fn score_and_rank(
        &self,
        query: &[f32],
        candidates: &[(u64, Vec<f32>)],
        k: usize,
        snapshot: &dyn StorageRead,
    ) -> Result<Vec<SearchResult>> {
        let mut scored_results = Vec::new();
        let dimensions = self.config.dimensions as usize;

        // Load and score each candidate
        for (internal_id, vector) in candidates {
            // Load vector data (for external_id and metadata)
            let vector_data = snapshot.get_vector_data(*internal_id, dimensions).await?;
            if vector_data.is_none() {
                continue; // Skip if vector not found (shouldn't happen)
            }
            let vector_data = vector_data.unwrap();

            // Compute distance/similarity score using vector from posting list
            let score = distance::compute_distance(query, vector, self.config.distance_metric);

            // Convert metadata fields to HashMap (includes vector field)
            let metadata: HashMap<String, AttributeValue> = vector_data
                .fields()
                .map(|field| {
                    (
                        field.field_name.clone(),
                        crate::model::field_value_to_attribute_value(&field.value),
                    )
                })
                .collect();

            scored_results.push(SearchResult {
                internal_id: *internal_id,
                external_id: vector_data.external_id().to_string(),
                score,
                attributes: metadata,
            });
        }

        // Sort by score
        // For L2, lower is better; for Cosine/DotProduct, higher is better
        match self.config.distance_metric {
            crate::serde::collection_meta::DistanceMetric::L2 => {
                scored_results.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap());
            }
            crate::serde::collection_meta::DistanceMetric::Cosine
            | crate::serde::collection_meta::DistanceMetric::DotProduct => {
                scored_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
            }
        }

        // Return top-k
        scored_results.truncate(k);
        Ok(scored_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MetadataFieldSpec, Vector};
    use crate::serde::FieldType;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::key::{IdDictionaryKey, VectorDataKey};
    use crate::serde::vector_data::VectorDataValue;
    use common::StorageConfig;
    use common::storage::in_memory::InMemoryStorage;
    use std::time::Duration;

    fn create_test_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::Cosine,
            flush_interval: Duration::from_secs(60),
            metadata_fields: vec![
                MetadataFieldSpec::new("category", FieldType::String, true),
                MetadataFieldSpec::new("price", FieldType::Int64, true),
            ],
        }
    }

    fn create_test_storage() -> Arc<dyn Storage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator::new(3),
        )))
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
        let storage = create_test_storage();
        let config = create_test_config();
        let db = VectorDb::new(Arc::clone(&storage), config).await.unwrap();

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
        // Check VectorData records (now contain external_id, vector, and metadata)
        let vec1_data_key = VectorDataKey::new(0).encode();
        let vec1_data = storage.get(vec1_data_key).await.unwrap();
        assert!(vec1_data.is_some());

        let vec2_data_key = VectorDataKey::new(1).encode();
        let vec2_data = storage.get(vec2_data_key).await.unwrap();
        assert!(vec2_data.is_some());

        // Check IdDictionary
        let dict_key1 = IdDictionaryKey::new("vec-1").encode();
        let dict_entry1 = storage.get(dict_key1).await.unwrap();
        assert!(dict_entry1.is_some());
    }

    #[tokio::test]
    async fn should_upsert_existing_vector() {
        // given
        let storage = create_test_storage();
        let config = create_test_config();
        let db = VectorDb::new(Arc::clone(&storage), config).await.unwrap();

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
        let decoded = VectorDataValue::decode_from_bytes(&vec_data.unwrap().value, 3).unwrap();
        assert_eq!(decoded.vector_field(), &[2.0, 3.0, 4.0]);

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

    // End-to-end search tests

    fn create_test_config_with_dimensions(dimensions: u16) -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions,
            distance_metric: DistanceMetric::Cosine,
            flush_interval: Duration::from_secs(60),
            metadata_fields: vec![],
        }
    }

    #[tokio::test]
    async fn should_bootstrap_and_query_vectors() {
        // given - create test database with 128 dimensions
        let config = create_test_config_with_dimensions(128);
        let db = VectorDb::open(config).await.unwrap();

        // given - create 4 clusters of 25 vectors each
        let mut all_vectors = Vec::new();
        let cluster_centers = vec![
            vec![1.0; 128],  // Cluster 1: all ones
            vec![-1.0; 128], // Cluster 2: all negative ones
            {
                let mut v = vec![0.0; 128];
                v[0] = 10.0;
                v
            }, // Cluster 3: sparse
            {
                let mut v = vec![0.0; 128];
                for i in (0..128).step_by(2) {
                    v[i] = 1.0;
                }
                v
            }, // Cluster 4: alternating
        ];

        for (cluster_id, center) in cluster_centers.iter().enumerate() {
            for i in 0..25 {
                let mut v = center.clone();
                // Add small noise
                v[0] += (i as f32) * 0.01;

                let vector = Vector::new(format!("vec-{}-{}", cluster_id, i), v);
                all_vectors.push(vector);
            }
        }

        // when - write vectors and flush
        db.write(all_vectors.clone()).await.unwrap();
        db.flush().await.unwrap();

        // when - bootstrap centroids (use cluster centers)
        let centroids: Vec<CentroidEntry> = cluster_centers
            .into_iter()
            .enumerate()
            .map(|(i, vector)| CentroidEntry::new((i + 1) as u32, vector))
            .collect();
        db.bootstrap_centroids(centroids).await.unwrap();

        // when - search for vector similar to cluster 0
        let query = vec![1.0; 128];
        let results = db.search(&query, 10).await.unwrap();

        // then - should find vectors from cluster 0
        assert_eq!(results.len(), 10);

        // Verify all results are from cluster 0 (external_id starts with "vec-0-")
        for result in &results {
            assert!(
                result.external_id.starts_with("vec-0-"),
                "Expected cluster 0 vector, got: {}",
                result.external_id
            );
        }

        // Verify results are sorted by score (cosine similarity, higher = better)
        for i in 1..results.len() {
            assert!(
                results[i - 1].score >= results[i].score,
                "Results not sorted by score"
            );
        }
    }

    #[tokio::test]
    async fn should_handle_deleted_vectors_in_search() {
        // given - create database and bootstrap centroids
        let config = create_test_config_with_dimensions(3);
        let db = VectorDb::open(config).await.unwrap();

        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0, 0.0])];
        db.bootstrap_centroids(centroids).await.unwrap();

        // given - write initial vector
        let vector1 = Vector::new("vec-1", vec![1.0, 0.0, 0.0]);
        db.write(vec![vector1]).await.unwrap();
        db.flush().await.unwrap();

        // when - upsert the same vector (old version should be marked as deleted)
        let vector2 = Vector::new("vec-1", vec![0.9, 0.1, 0.0]);
        db.write(vec![vector2]).await.unwrap();
        db.flush().await.unwrap();

        // when - search
        let results = db.search(&[1.0, 0.0, 0.0], 10).await.unwrap();

        // then - should only return the new version (not the deleted one)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].external_id, "vec-1");
        let vector = results[0]
            .attributes
            .iter()
            .find(|f| f.0 == "vector")
            .unwrap()
            .1;
        let AttributeValue::Vector(vector) = vector.clone() else {
            panic!("unexpected attr type");
        };
        assert_eq!(vector, vec![0.9, 0.1, 0.0]);
    }

    #[tokio::test]
    async fn should_search_with_l2_distance_metric() {
        // given - create database with L2 metric
        let config = Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            flush_interval: Duration::from_secs(60),
            metadata_fields: vec![],
        };
        let db = VectorDb::open(config).await.unwrap();

        // given - bootstrap centroids
        let centroids = vec![CentroidEntry::new(1, vec![0.0, 0.0, 0.0])];
        db.bootstrap_centroids(centroids).await.unwrap();

        // given - write vectors at different distances from origin
        let vectors = vec![
            Vector::new("close", vec![0.1, 0.1, 0.1]), // Close to origin
            Vector::new("medium", vec![1.0, 1.0, 1.0]), // Medium distance
            Vector::new("far", vec![5.0, 5.0, 5.0]),   // Far from origin
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - search for vectors near origin
        let results = db.search(&[0.0, 0.0, 0.0], 3).await.unwrap();

        // then - should be sorted by L2 distance (lower = better)
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].external_id, "close");
        assert_eq!(results[1].external_id, "medium");
        assert_eq!(results[2].external_id, "far");

        // Verify scores are increasing (L2 distance)
        for i in 1..results.len() {
            assert!(
                results[i - 1].score <= results[i].score,
                "L2 distances not sorted correctly"
            );
        }
    }

    #[tokio::test]
    async fn should_reject_query_with_wrong_dimensions() {
        // given - database with 3 dimensions
        let config = create_test_config_with_dimensions(3);
        let db = VectorDb::open(config).await.unwrap();

        // when - query with wrong dimensions
        let result = db.search(&[1.0, 2.0], 10).await;

        // then - should fail
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Query dimension mismatch")
        );
    }

    #[tokio::test]
    async fn should_return_empty_results_when_no_centroids() {
        // given - database without centroids
        let config = create_test_config_with_dimensions(3);
        let db = VectorDb::open(config).await.unwrap();

        // when - search
        let results = db.search(&[1.0, 0.0, 0.0], 10).await.unwrap();

        // then - should return empty results
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn should_return_empty_results_when_no_vectors() {
        // given - database with centroids but no vectors
        let config = create_test_config_with_dimensions(3);
        let db = VectorDb::open(config).await.unwrap();

        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0, 0.0])];
        db.bootstrap_centroids(centroids).await.unwrap();

        // when - search
        let results = db.search(&[1.0, 0.0, 0.0], 10).await.unwrap();

        // then - should return empty results
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn should_limit_results_to_k() {
        // given - database with many vectors
        let config = create_test_config_with_dimensions(2);
        let db = VectorDb::open(config).await.unwrap();

        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0])];
        db.bootstrap_centroids(centroids).await.unwrap();

        // Insert 20 vectors
        let vectors: Vec<Vector> = (0..20)
            .map(|i| Vector::new(format!("vec-{}", i), vec![1.0 + (i as f32) * 0.01, 0.0]))
            .collect();
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - search for k=5
        let results = db.search(&[1.0, 0.0], 5).await.unwrap();

        // then - should return exactly 5 results
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn should_search_across_multiple_centroids() {
        // given - database with 3 centroids
        let config = create_test_config_with_dimensions(2);
        let db = VectorDb::open(config).await.unwrap();

        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
            CentroidEntry::new(3, vec![-1.0, 0.0]),
        ];
        db.bootstrap_centroids(centroids).await.unwrap();

        // Insert vectors in each cluster
        let vectors = vec![
            Vector::new("c1-1", vec![0.9, 0.0]),
            Vector::new("c1-2", vec![1.1, 0.0]),
            Vector::new("c2-1", vec![0.0, 0.9]),
            Vector::new("c2-2", vec![0.0, 1.1]),
            Vector::new("c3-1", vec![-0.9, 0.0]),
            Vector::new("c3-2", vec![-1.1, 0.0]),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - search in between centroids 1 and 2
        let results = db.search(&[0.7, 0.7], 10).await.unwrap();

        // then - should find vectors from multiple centroids
        assert!(!results.is_empty());
        // The algorithm expands to multiple centroids, so we should get results
        // from both cluster 1 and cluster 2
    }
}

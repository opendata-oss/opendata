//! Vector database implementation with atomic flush semantics.
//!
//! This module provides the main `VectorDb` struct that handles:
//! - Vector ingestion with validation
//! - In-memory delta buffering via WriteCoordinator
//! - Atomic flush with ID allocation
//! - Snapshot management for consistency
//!
//! The implementation uses the WriteCoordinator pattern for write path:
//! - Validation and ID allocation happen in write()
//! - Delta handles dictionary lookup, centroid assignment, and builds RecordOps
//! - Flusher applies ops atomically to storage

use crate::delta::{VectorDbDeltaContext, VectorDbWriteDelta, VectorWrite};
use crate::distance;
use crate::flusher::VectorDbFlusher;
use crate::hnsw::{CentroidGraph, build_centroid_graph};
use crate::model::{
    AttributeValue, Config, SearchResult, VECTOR_FIELD_NAME, Vector, attributes_to_map,
};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::key::SeqBlockKey;
use crate::serde::posting_list::PostingList;
use crate::storage::VectorDbStorageReadExt;
use crate::storage::merge_operator::VectorDbMergeOperator;
use anyhow::{Context, Result};
use common::SequenceAllocator;
use common::coordinator::{Durability, WriteCoordinator, WriteCoordinatorConfig};
use common::storage::factory::create_storage;
use common::storage::{Storage, StorageRead, StorageSnapshot};
use common::{StorageRuntime, StorageSemantics};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub(crate) const WRITE_CHANNEL: &str = "write";

/// Vector database for storing and querying embedding vectors.
///
/// `VectorDb` provides a high-level API for ingesting vectors with metadata.
/// It handles internal details like ID allocation, centroid assignment,
/// and metadata index maintenance automatically.
pub struct VectorDb {
    config: Config,
    #[allow(dead_code)]
    storage: Arc<dyn Storage>,

    /// The WriteCoordinator itself (stored to keep it alive).
    write_coordinator: WriteCoordinator<VectorDbWriteDelta, VectorDbFlusher>,

    /// In-memory HNSW graph for centroid search (immutable after initialization).
    centroid_graph: Arc<dyn CentroidGraph>,
}

impl VectorDb {
    /// Open or create a vector database with the given configuration and centroids.
    ///
    /// If the database already exists (centroids are already stored), the provided
    /// centroids are ignored and the stored centroids are used instead.
    ///
    /// If the database is new, the provided centroids are written to storage and
    /// used to build the HNSW index.
    ///
    /// # Arguments
    /// * `config` - Database configuration
    /// * `centroids` - Initial centroids to use if database is new
    ///
    /// # Configuration Compatibility
    /// If the database already exists, the configuration must be compatible:
    /// - `dimensions` must match exactly
    /// - `distance_metric` must match exactly
    ///
    /// Other configuration options (like `flush_interval`) can be changed
    /// on subsequent opens.
    pub async fn open(config: Config, centroids: Vec<CentroidEntry>) -> Result<Self> {
        let merge_op = VectorDbMergeOperator::new(config.dimensions as usize);
        let storage = create_storage(
            &config.storage,
            StorageRuntime::new(),
            StorageSemantics::new().with_merge_operator(Arc::new(merge_op)),
        )
        .await
        .context("Failed to create storage")?;

        Self::new(storage, config, centroids).await
    }

    /// Create a vector database with the given storage, configuration, and centroids.
    ///
    /// If centroids already exist in storage, the provided centroids are ignored.
    /// Otherwise, the provided centroids are written to storage.
    pub(crate) async fn new(
        storage: Arc<dyn Storage>,
        config: Config,
        centroids: Vec<CentroidEntry>,
    ) -> Result<Self> {
        // Initialize sequence allocator for internal ID generation
        let seq_key = SeqBlockKey.encode();
        let id_allocator = SequenceAllocator::load(storage.as_ref(), seq_key).await?;

        // Get initial snapshot
        let snapshot = storage.snapshot().await?;

        // For now, load the full ID dictionary from storage into memory at startup
        // Eventually, we should load this in the background and allow the delta to
        // read ids that are not yet loaded from storage
        let dictionary = Arc::new(DashMap::new());
        {
            Self::load_dictionary_from_storage(snapshot.as_ref(), &dictionary).await?;
        }

        // For now, just force bootstrap centroids. Eventually we'll derive these automatically
        // from the vectors
        let centroid_graph =
            Self::load_or_create_centroids(&storage, snapshot.as_ref(), &config, centroids).await?;

        // Create flusher for the WriteCoordinator
        let flusher = VectorDbFlusher {
            storage: Arc::clone(&storage),
        };

        // Create initial image for the delta (shares dictionary, centroid_graph, and id_allocator)
        let ctx = VectorDbDeltaContext {
            dimensions: config.dimensions as usize,
            dictionary: Arc::clone(&dictionary),
            centroid_graph: Arc::clone(&centroid_graph),
            id_allocator,
        };

        // start write coordinator
        let coordinator_config = WriteCoordinatorConfig {
            queue_capacity: 1000,
            flush_interval: Duration::from_secs(5),
            flush_size_threshold: 64 * 1024 * 1024,
        };
        let mut write_coordinator = WriteCoordinator::new(
            coordinator_config,
            vec![WRITE_CHANNEL.to_string()],
            ctx,
            snapshot.clone(),
            flusher,
        );
        write_coordinator.start();

        Ok(Self {
            config,
            storage,
            write_coordinator,
            centroid_graph,
        })
    }

    /// Load centroids from storage if they exist, otherwise create them from the provided entries.
    async fn load_or_create_centroids(
        storage: &Arc<dyn Storage>,
        snapshot: &dyn StorageSnapshot,
        config: &Config,
        centroids: Vec<CentroidEntry>,
    ) -> Result<Arc<dyn CentroidGraph>> {
        // Check if centroids already exist in storage
        let existing_centroids = snapshot
            .scan_all_centroids(config.dimensions as usize)
            .await?;

        if !existing_centroids.is_empty() {
            // Use existing centroids from storage
            let graph = build_centroid_graph(existing_centroids, config.distance_metric)?;
            return Ok(Arc::from(graph));
        }

        // No existing centroids - validate and write the provided ones
        if centroids.is_empty() {
            return Err(anyhow::anyhow!(
                "Centroids must be provided when creating a new database"
            ));
        }

        // Validate centroid dimensions
        for centroid in &centroids {
            if centroid.dimensions() != config.dimensions as usize {
                return Err(anyhow::anyhow!(
                    "Centroid dimension mismatch: expected {}, got {}",
                    config.dimensions,
                    centroid.dimensions()
                ));
            }
        }

        // Validate we don't exceed max chunk size (default 4096)
        const DEFAULT_CHUNK_TARGET: usize = 4096;
        if centroids.len() > DEFAULT_CHUNK_TARGET {
            return Err(anyhow::anyhow!(
                "Too many centroids for single chunk: {} > {}. Multi-chunk support not yet implemented.",
                centroids.len(),
                DEFAULT_CHUNK_TARGET
            ));
        }

        // Write centroids to storage
        let op = crate::storage::record::put_centroid_chunk(
            0,
            centroids.clone(),
            config.dimensions as usize,
        );
        storage.apply(vec![op]).await?;

        // Build and return the graph
        let graph = build_centroid_graph(centroids, config.distance_metric)?;
        Ok(Arc::from(graph))
    }

    /// Load ID dictionary entries from storage into the in-memory DashMap.
    async fn load_dictionary_from_storage(
        snapshot: &dyn StorageRead,
        dictionary: &DashMap<String, u64>,
    ) -> Result<()> {
        // Create prefix for all IdDictionary records
        let mut prefix_buf = bytes::BytesMut::with_capacity(2);
        crate::serde::RecordType::IdDictionary
            .prefix()
            .write_to(&mut prefix_buf);
        let prefix = prefix_buf.freeze();

        // Scan all IdDictionary records
        let range = common::BytesRange::prefix(prefix);
        let records = snapshot.scan(range).await?;

        for record in records {
            // Decode the key to get external_id
            let key = crate::serde::key::IdDictionaryKey::decode(&record.key)?;
            let external_id = key.external_id.clone();

            // Decode the value to get internal_id
            let mut slice = record.value.as_ref();
            let internal_id = common::serde::encoding::decode_u64(&mut slice)
                .context("failed to decode internal ID from ID dictionary")?;

            dictionary.insert(external_id, internal_id);
        }

        Ok(())
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
        // Validate and prepare all vectors
        let mut writes = Vec::with_capacity(vectors.len());
        for vector in vectors {
            writes.push(self.prepare_vector_write(vector)?);
        }

        // Send all writes to coordinator in a single batch and wait to be applied
        let mut write_handle = self
            .write_coordinator
            .handle(WRITE_CHANNEL)
            .write(writes)
            .await?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(())
    }

    /// Validate and prepare a vector write for the coordinator.
    ///
    /// This validates the vector. The delta handles ID allocation,
    /// dictionary lookup, and centroid assignment.
    fn prepare_vector_write(&self, vector: Vector) -> Result<VectorWrite> {
        // Validate external ID length
        if vector.id.len() > 64 {
            return Err(anyhow::anyhow!(
                "External ID too long: {} bytes (max 64)",
                vector.id.len()
            ));
        }

        // Convert attributes to map for validation
        let attributes = attributes_to_map(&vector.attributes);

        // Extract and validate "vector" attribute
        let values = match attributes.get(VECTOR_FIELD_NAME) {
            Some(AttributeValue::Vector(v)) => v.clone(),
            Some(_) => {
                return Err(anyhow::anyhow!(
                    "Field '{}' must have type Vector",
                    VECTOR_FIELD_NAME
                ));
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Missing required field '{}'",
                    VECTOR_FIELD_NAME
                ));
            }
        };

        // Validate dimensions
        if values.len() != self.config.dimensions as usize {
            return Err(anyhow::anyhow!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                values.len()
            ));
        }

        // Validate attributes against schema (if schema is defined)
        if !self.config.metadata_fields.is_empty() {
            self.validate_attributes(&attributes)?;
        }

        // Convert attributes to vec of tuples for VectorWrite
        let attributes_vec: Vec<(String, AttributeValue)> = attributes.into_iter().collect();

        Ok(VectorWrite {
            external_id: vector.id,
            values,
            attributes: attributes_vec,
        })
    }

    /// Validates attributes against the configured schema.
    fn validate_attributes(&self, metadata: &HashMap<String, AttributeValue>) -> Result<()> {
        // Build a map of field name -> expected type for quick lookup
        let schema: HashMap<&str, crate::serde::FieldType> = self
            .config
            .metadata_fields
            .iter()
            .map(|spec| (spec.name.as_str(), spec.field_type))
            .collect();

        // Check each provided attribute (skip VECTOR_FIELD_NAME which is always allowed)
        for (field_name, value) in metadata {
            // Skip the special "vector" field
            if field_name == VECTOR_FIELD_NAME {
                continue;
            }

            match schema.get(field_name.as_str()) {
                Some(expected_type) => {
                    // Validate type matches
                    let actual_type = match value {
                        AttributeValue::String(_) => crate::serde::FieldType::String,
                        AttributeValue::Int64(_) => crate::serde::FieldType::Int64,
                        AttributeValue::Float64(_) => crate::serde::FieldType::Float64,
                        AttributeValue::Bool(_) => crate::serde::FieldType::Bool,
                        AttributeValue::Vector(_) => crate::serde::FieldType::Vector,
                    };

                    if actual_type != *expected_type {
                        return Err(anyhow::anyhow!(
                            "Type mismatch for field '{}': expected {:?}, got {:?}",
                            field_name,
                            expected_type,
                            actual_type
                        ));
                    }
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "Unknown metadata field: '{}'. Valid fields: {:?}",
                        field_name,
                        schema.keys().collect::<Vec<_>>()
                    ));
                }
            }
        }

        Ok(())
    }

    /// Force flush all pending data to storage.
    ///
    /// Normally data is flushed according to `flush_interval`, and is then
    /// readable. This method can be used to make writes readable immediately.
    /// TODO: extend with an option to make durable, or support reading unflushed
    ///       and change the meaning here to mean flushed durably
    ///
    /// # Atomic Flush
    ///
    /// The flush operation is atomic:
    /// 1. All pending writes are frozen into an immutable delta
    /// 2. RecordOps are applied in one batch via `storage.apply()`
    /// 3. The snapshot is updated for queries
    ///
    /// This ensures ID dictionary updates, deletes, and new records are all
    /// applied together, maintaining consistency.
    pub async fn flush(&self) -> Result<()> {
        let mut handle = self
            .write_coordinator
            .handle(WRITE_CHANNEL)
            .flush(false)
            .await?;
        handle.wait(Durability::Flushed).await?;
        Ok(())
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

        // 2. Search HNSW for nearest centroids (expand by ~10-100x for recall)
        // clamp(10, 100) ensures we search at least 10 centroids (for small k)
        // and at most 100 centroids (to cap memory/latency for large k)
        let num_centroids = k.clamp(10, 100);
        let centroid_ids = self.centroid_graph.search(query, num_centroids);

        if centroid_ids.is_empty() {
            return Ok(Vec::new());
        }

        // 3. Clone snapshot reference (lock briefly, then release)
        let snapshot = self.write_coordinator.view().snapshot.clone();

        // 4. Load posting lists
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
                .map(|field| (field.field_name.clone(), field.value.clone().into()))
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

    fn create_test_centroids(dimensions: usize) -> Vec<CentroidEntry> {
        vec![CentroidEntry::new(1, vec![1.0; dimensions])]
    }

    #[tokio::test]
    async fn should_open_vector_db() {
        // given
        let config = create_test_config();
        let centroids = create_test_centroids(3);

        // when
        let result = VectorDb::open(config, centroids).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_write_and_flush_vectors() {
        // given
        let storage = create_test_storage();
        let config = create_test_config();
        let centroids = create_test_centroids(3);
        let db = VectorDb::new(Arc::clone(&storage), config, centroids)
            .await
            .unwrap();

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
        let centroids = create_test_centroids(3);
        let db = VectorDb::new(Arc::clone(&storage), config, centroids)
            .await
            .unwrap();

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
        let centroids = create_test_centroids(3);
        let db = VectorDb::open(config, centroids).await.unwrap();

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
        let centroids = create_test_centroids(3);
        let db = VectorDb::open(config, centroids).await.unwrap();

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
    async fn should_query_vectors() {
        // given - create test database with 128 dimensions
        let config = create_test_config_with_dimensions(128);

        // Create 4 centroids for the clusters
        let cluster_centers = [
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

        let centroids: Vec<CentroidEntry> = cluster_centers
            .iter()
            .enumerate()
            .map(|(i, vector)| CentroidEntry::new((i + 1) as u32, vector.clone()))
            .collect();

        let db = VectorDb::open(config, centroids).await.unwrap();

        // Create 4 clusters of 25 vectors each
        let mut all_vectors = Vec::new();
        for (cluster_id, center) in cluster_centers.iter().enumerate() {
            for i in 0..25 {
                let mut v = center.clone();
                // Add small noise
                v[0] += (i as f32) * 0.01;

                let vector = Vector::new(format!("vec-{}-{}", cluster_id, i), v);
                all_vectors.push(vector);
            }
        }

        // Write vectors and flush
        db.write(all_vectors.clone()).await.unwrap();
        db.flush().await.unwrap();

        // Search for vector similar to cluster 0
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
        // given - create database with centroids
        let config = create_test_config_with_dimensions(3);
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0, 0.0])];
        let db = VectorDb::open(config, centroids).await.unwrap();

        // Write initial vector
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
        let centroids = vec![CentroidEntry::new(1, vec![0.0, 0.0, 0.0])];
        let db = VectorDb::open(config, centroids).await.unwrap();

        // Write vectors at different distances from origin
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
        let centroids = create_test_centroids(3);
        let db = VectorDb::open(config, centroids).await.unwrap();

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
    async fn should_return_empty_results_when_no_vectors() {
        // given - database with centroids but no vectors
        let config = create_test_config_with_dimensions(3);
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0, 0.0])];
        let db = VectorDb::open(config, centroids).await.unwrap();

        // when - search
        let results = db.search(&[1.0, 0.0, 0.0], 10).await.unwrap();

        // then - should return empty results
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn should_limit_results_to_k() {
        // given - database with many vectors
        let config = create_test_config_with_dimensions(2);
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0])];
        let db = VectorDb::open(config, centroids).await.unwrap();

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
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
            CentroidEntry::new(3, vec![-1.0, 0.0]),
        ];
        let db = VectorDb::open(config, centroids).await.unwrap();

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
    }

    #[tokio::test]
    async fn should_load_dictionary_on_reopen() {
        // given - create database and write vectors
        let storage = create_test_storage();
        let config = create_test_config();
        let centroids = create_test_centroids(3);

        {
            let db = VectorDb::new(Arc::clone(&storage), config.clone(), centroids.clone())
                .await
                .unwrap();
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
            db.write(vectors).await.unwrap();
            db.flush().await.unwrap();
        }

        // when - reopen database (centroids should be loaded from storage)
        let db2 = VectorDb::new(Arc::clone(&storage), config, vec![])
            .await
            .unwrap();

        // then - should be able to search (dictionary and centroids loaded from storage)
        let results = db2.search(&[1.0, 0.0, 0.0], 10).await.unwrap();
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn should_fail_if_no_centroids_provided_for_new_db() {
        // given - new database without centroids
        let config = create_test_config();

        // when
        let result = VectorDb::open(config, vec![]).await;

        // then
        match result {
            Err(e) => assert!(
                e.to_string().contains("Centroids must be provided"),
                "unexpected error: {}",
                e
            ),
            Ok(_) => panic!("expected error when no centroids provided"),
        }
    }
}

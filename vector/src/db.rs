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

use crate::VectorDbReader;
use crate::delta::{
    VectorDbDeltaContext, VectorDbDeltaOpts, VectorDbWrite, VectorDbWriteDelta, VectorWrite,
};
use crate::error::{Error, Result};
use crate::flusher::VectorDbFlusher;
use crate::hnsw::{CentroidGraph, build_centroid_graph};
use crate::lire::rebalancer::{IndexRebalancer, IndexRebalancerOpts};
use crate::model::{
    AttributeValue, Config, Query, SearchResult, VECTOR_FIELD_NAME, Vector, attributes_to_map,
};
use crate::query_engine::{QueryEngine, QueryEngineOptions};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::key::SeqBlockKey;
use crate::storage::VectorDbStorageReadExt;
use crate::storage::merge_operator::VectorDbMergeOperator;
use async_trait::async_trait;
use common::SequenceAllocator;
use common::coordinator::{Durability, WriteCoordinator, WriteCoordinatorConfig};
use common::storage::{Storage, StorageRead, StorageSnapshot};
use common::{StorageBuilder, StorageSemantics};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub(crate) const WRITE_CHANNEL: &str = "write";
pub(crate) const REBALANCE_CHANNEL: &str = "rebalance";

/// Trait for querying the vector db
#[async_trait]
pub trait VectorDbRead {
    /// Search for k-nearest neighbors to a query vector.
    ///
    /// This implements the SPANN-style query algorithm:
    /// 1. Search HNSW for nearest centroids
    /// 2. Load posting lists for those centroids
    /// 3. Filter deleted vectors
    /// 4. Score candidates and return top-k
    ///
    /// # Arguments
    /// * `query` - search query
    ///
    /// # Returns
    /// Vector of SearchResults sorted by similarity (best first)
    ///
    /// # Errors
    /// Returns an error if:
    /// - Query dimensions don't match collection dimensions
    /// - Storage read fails
    async fn search(&self, query: &Query) -> Result<Vec<SearchResult>>;

    async fn search_with_nprobe(&self, query: &Query, nprobe: usize) -> Result<Vec<SearchResult>>;

    /// Retrieve a vector record by its external ID.
    ///
    /// This is a point lookup operation that retrieves a single record with all its fields.
    /// Returns `None` if the record doesn't exist or has been deleted.
    ///
    /// # Arguments
    ///
    /// * `id` - External ID of the record to retrieve
    ///
    /// # Returns
    ///
    /// `Some(VectorRecord)` if found, `None` if not found or deleted.
    async fn get(&self, id: &str) -> Result<Option<Vector>>;
}

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
    pub async fn open(config: Config) -> Result<Self> {
        let sb = StorageBuilder::new(&config.storage)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create storage: {e}")))?;
        Self::open_with_storage(config, sb).await
    }

    pub async fn open_with_storage(config: Config, builder: StorageBuilder) -> Result<Self> {
        let centroid1: Vec<f32> = vec![0.0f32; config.dimensions as usize];
        Self::open_with_centroids(config, vec![centroid1], builder).await
    }

    pub async fn open_with_centroids(
        config: Config,
        centroids: Vec<Vec<f32>>,
        builder: StorageBuilder,
    ) -> Result<Self> {
        let merge_op = VectorDbMergeOperator::new(config.dimensions as usize);
        let storage = builder
            .with_semantics(StorageSemantics::new().with_merge_operator(Arc::new(merge_op)))
            .build()
            .await
            .map_err(|e| Error::Storage(format!("Failed to create storage: {e}")))?;

        Self::load_or_init_db(storage, config, centroids).await
    }

    /// Create a vector database with the given storage, configuration, and centroids.
    /// The fn is internal to this module. It is intended to be used by the public api and
    /// by tests.
    ///
    /// If centroids already exist in storage, the provided centroids are ignored.
    /// Otherwise, the provided centroids are written to storage.
    async fn load_or_init_db(
        storage: Arc<dyn Storage>,
        config: Config,
        centroids: Vec<Vec<f32>>,
    ) -> Result<Self> {
        // Initialize sequence allocator for internal ID generation
        let seq_key = SeqBlockKey.encode();
        let mut id_allocator = SequenceAllocator::load(storage.as_ref(), seq_key).await?;

        // Get initial snapshot
        let snapshot = storage.snapshot().await?;

        // For now, load the full ID dictionary from storage into memory at startup
        // Eventually, we should load this in the background and allow the delta to
        // read ids that are not yet loaded from storage
        let dictionary = Arc::new(DashMap::new());
        {
            Self::load_dictionary_from_storage(snapshot.as_ref(), &dictionary).await?;
        }

        // Load centroid counts from storage
        let centroid_counts = Self::load_centroid_counts_from_storage(snapshot.as_ref()).await?;

        // For now, just force bootstrap centroids. Eventually we'll derive these automatically
        // from the vectors
        let (centroid_graph, current_chunk_id, current_chunk_count) =
            Self::load_or_create_centroids(
                &storage,
                snapshot.as_ref(),
                &config,
                centroids,
                &mut id_allocator,
            )
            .await?;

        // Create flusher for the WriteCoordinator
        let flusher = VectorDbFlusher {
            storage: Arc::clone(&storage),
        };

        let handle_tx = Arc::new(OnceLock::new());
        let rebalancer = IndexRebalancer::new(
            IndexRebalancerOpts {
                dimensions: config.dimensions as usize,
                distance_metric: config.distance_metric,
                split_search_neighbourhood: config.split_search_neighbourhood,
                split_threshold_vectors: config.split_threshold_vectors,
                merge_threshold_vectors: config.merge_threshold_vectors,
                max_rebalance_tasks: config.max_rebalance_tasks,
            },
            centroid_graph.clone(),
            centroid_counts,
            handle_tx.clone(),
        );

        let pause_handle = Arc::new(OnceLock::new());
        let ctx = VectorDbDeltaContext {
            opts: VectorDbDeltaOpts {
                dimensions: config.dimensions as usize,
                chunk_target: config.chunk_target as usize,
                max_pending_and_running_rebalance_tasks: config
                    .max_pending_and_running_rebalance_tasks,
                rebalance_backpressure_resume_threshold: config
                    .rebalance_backpressure_resume_threshold,
                split_threshold_vectors: config.split_threshold_vectors,
                indexed_fields: VectorDbDeltaOpts::indexed_fields_from(&config.metadata_fields),
            },
            dictionary: Arc::clone(&dictionary),
            centroid_graph: Arc::clone(&centroid_graph),
            id_allocator,
            current_chunk_id,
            current_chunk_count,
            rebalancer,
            pause_handle: pause_handle.clone(),
        };

        // start write coordinator
        let coordinator_config = WriteCoordinatorConfig {
            queue_capacity: 1000,
            flush_interval: Duration::from_secs(5),
            flush_size_threshold: 64 * 1024 * 1024,
        };
        let mut write_coordinator = WriteCoordinator::new(
            coordinator_config,
            vec![WRITE_CHANNEL.to_string(), REBALANCE_CHANNEL.to_string()],
            ctx,
            snapshot.clone(),
            flusher,
        );
        handle_tx
            .set(write_coordinator.handle(REBALANCE_CHANNEL))
            .map_err(|_e| "unreachable")
            .unwrap();
        pause_handle
            .set(write_coordinator.pause_handle(WRITE_CHANNEL))
            .map_err(|_e| "unreachable")
            .unwrap();
        write_coordinator.start();

        Ok(Self {
            config,
            storage,
            write_coordinator,
            centroid_graph,
        })
    }

    /// Load centroids from storage if they exist, otherwise create them from the provided entries.
    /// Returns the centroid graph and the last chunk's ID and entry count, used for initializing
    /// chunk tracking state.
    async fn load_or_create_centroids(
        storage: &Arc<dyn Storage>,
        snapshot: &dyn StorageSnapshot,
        config: &Config,
        centroids: Vec<Vec<f32>>,
        id_allocator: &mut SequenceAllocator,
    ) -> Result<(Arc<dyn CentroidGraph>, u32, usize)> {
        // Check if centroids already exist in storage
        let scan_result = snapshot
            .scan_all_centroids(config.dimensions as usize)
            .await?;

        if !scan_result.entries.is_empty() {
            let last_chunk_id = scan_result.last_chunk_id;
            let last_chunk_count = scan_result.last_chunk_count;
            // Filter out centroids that have been deleted (tracked in deletions bitmap)
            let deletions = snapshot.get_deleted_vectors().await?;
            let live_centroids: Vec<CentroidEntry> = scan_result
                .entries
                .into_iter()
                .filter(|c| !deletions.contains(c.centroid_id))
                .collect();
            let graph = build_centroid_graph(live_centroids, config.distance_metric)?;
            return Ok((Arc::from(graph), last_chunk_id, last_chunk_count));
        }

        // No existing centroids - validate and write the provided ones
        if centroids.is_empty() {
            return Err(Error::InvalidInput(
                "Centroids must be provided when creating a new database".to_string(),
            ));
        }

        // Validate centroid dimensions
        for centroid in &centroids {
            if centroid.len() != config.dimensions as usize {
                return Err(Error::InvalidInput(format!(
                    "Centroid dimension mismatch: expected {}, got {}",
                    config.dimensions,
                    centroid.len()
                )));
            }
        }

        // Allocate IDs and build CentroidEntries
        let mut ops = Vec::new();
        let mut entries = Vec::with_capacity(centroids.len());
        for vector in centroids {
            let (centroid_id, seq_alloc_put) = id_allocator.allocate_one();
            if let Some(seq_alloc_put) = seq_alloc_put {
                ops.push(common::storage::RecordOp::Put(seq_alloc_put.into()));
            }
            entries.push(CentroidEntry::new(centroid_id, vector));
        }

        // Write centroids to storage in chunks
        let chunk_target = config.chunk_target as usize;
        let num_chunks = entries.chunks(chunk_target).len();
        for (chunk_idx, chunk_entries) in entries.chunks(chunk_target).enumerate() {
            ops.push(crate::storage::record::put_centroid_chunk(
                chunk_idx as u32,
                chunk_entries.to_vec(),
                config.dimensions as usize,
            ));
        }
        storage.apply(ops).await?;

        // Compute last chunk state from what we just wrote
        let last_chunk_id = if num_chunks == 0 {
            0
        } else {
            (num_chunks - 1) as u32
        };
        let last_chunk_count = if entries.is_empty() {
            0
        } else {
            entries.len() - (last_chunk_id as usize * chunk_target)
        };

        // Build and return the graph
        let graph = build_centroid_graph(entries, config.distance_metric)?;
        Ok((Arc::from(graph), last_chunk_id, last_chunk_count))
    }

    /// Load ID dictionary entries from storage into the in-memory DashMap.
    async fn load_dictionary_from_storage(
        snapshot: &dyn StorageRead,
        dictionary: &DashMap<String, u64>,
    ) -> Result<()> {
        // Create prefix for all IdDictionary records
        let mut prefix_buf = bytes::BytesMut::with_capacity(3);
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
            let internal_id = common::serde::encoding::decode_u64(&mut slice).map_err(|e| {
                Error::Encoding(format!(
                    "failed to decode internal ID from ID dictionary: {e}"
                ))
            })?;

            dictionary.insert(external_id, internal_id);
        }

        Ok(())
    }

    /// Load centroid counts from storage into a HashMap.
    ///
    /// Scans all CentroidStats records and extracts the accumulated num_vectors
    /// for each centroid.
    async fn load_centroid_counts_from_storage(
        snapshot: &dyn StorageRead,
    ) -> Result<HashMap<u64, u64>> {
        let stats = snapshot.scan_all_centroid_stats().await?;
        let mut counts = HashMap::new();
        for (centroid_id, value) in stats {
            counts.insert(centroid_id, value.num_vectors.max(0) as u64);
        }
        Ok(counts)
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
            .write(VectorDbWrite::Write(writes))
            .await
            .map_err(|e| Error::Internal(format!("{}", e)))?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| Error::Internal(format!("{}", e)))?;

        Ok(())
    }

    /// Write vectors to the database with a timeout.
    ///
    /// This is the primary write method. It accepts a batch of vectors and
    /// returns when the data has been accepted for ingestion (but not
    /// necessarily flushed to durable storage).
    ///
    /// The write may time out if the db is busy compacting or indexing.
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
    pub async fn write_timeout(&self, vectors: Vec<Vector>, timeout: Duration) -> Result<()> {
        // Validate and prepare all vectors
        let mut writes = Vec::with_capacity(vectors.len());
        for vector in vectors {
            writes.push(self.prepare_vector_write(vector)?);
        }

        // Send all writes to coordinator in a single batch and wait to be applied
        let mut write_handle = self
            .write_coordinator
            .handle(WRITE_CHANNEL)
            .write_timeout(VectorDbWrite::Write(writes), timeout)
            .await
            .map_err(|e| Error::Internal(format!("{}", e)))?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| Error::Internal(format!("{}", e)))?;

        Ok(())
    }

    /// Validate and prepare a vector write for the coordinator.
    ///
    /// This validates the vector. The delta handles ID allocation,
    /// dictionary lookup, and centroid assignment.
    fn prepare_vector_write(&self, vector: Vector) -> Result<VectorWrite> {
        // Validate external ID length
        if vector.id.len() > 64 {
            return Err(Error::InvalidInput(format!(
                "External ID too long: {} bytes (max 64)",
                vector.id.len()
            )));
        }

        // Convert attributes to map for validation
        let attributes = attributes_to_map(&vector.attributes);

        // Extract and validate "vector" attribute
        let values = match attributes.get(VECTOR_FIELD_NAME) {
            Some(AttributeValue::Vector(v)) => v.clone(),
            Some(_) => {
                return Err(Error::InvalidInput(format!(
                    "Field '{}' must have type Vector",
                    VECTOR_FIELD_NAME
                )));
            }
            None => {
                return Err(Error::InvalidInput(format!(
                    "Missing required field '{}'",
                    VECTOR_FIELD_NAME
                )));
            }
        };

        // Validate dimensions
        if values.len() != self.config.dimensions as usize {
            return Err(Error::InvalidInput(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                values.len()
            )));
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
                        return Err(Error::InvalidInput(format!(
                            "Type mismatch for field '{}': expected {:?}, got {:?}",
                            field_name, expected_type, actual_type
                        )));
                    }
                }
                None => {
                    return Err(Error::InvalidInput(format!(
                        "Unknown metadata field: '{}'. Valid fields: {:?}",
                        field_name,
                        schema.keys().collect::<Vec<_>>()
                    )));
                }
            }
        }

        Ok(())
    }

    /// Force flush all pending data to durable storage.
    ///
    /// Flushes the in-memory delta to the storage memtable, then persists
    /// to durable storage. After this returns, data is both readable and
    /// durable.
    ///
    /// # Atomic Flush
    ///
    /// The flush operation is atomic:
    /// 1. All pending writes are frozen into an immutable delta
    /// 2. RecordOps are applied in one batch via `storage.apply()`
    /// 3. The snapshot is updated for queries
    /// 4. Data is flushed to durable storage
    ///
    /// This ensures ID dictionary updates, deletes, and new records are all
    /// applied together, maintaining consistency.
    pub async fn flush(&self) -> Result<()> {
        let mut handle = self
            .write_coordinator
            .handle(WRITE_CHANNEL)
            .flush(true)
            .await
            .map_err(|e| Error::Internal(format!("{}", e)))?;
        handle
            .wait(Durability::Durable)
            .await
            .map_err(|e| Error::Internal(format!("{}", e)))?;
        Ok(())
    }

    /// Closes the vector database, flushing any pending data and releasing resources.
    ///
    /// All written data is flushed to durable storage before the database is
    /// closed. For SlateDB-backed storage, this also releases the database
    /// fence.
    pub async fn close(self) -> Result<()> {
        self.flush().await?;
        self.write_coordinator
            .stop()
            .await
            .map_err(Error::Internal)?;
        self.storage.close().await?;
        Ok(())
    }

    pub fn num_centroids(&self) -> usize {
        self.centroid_graph.len()
    }

    /// Create a QueryEngine from the current snapshot for executing queries.
    pub(crate) fn query_engine(&self) -> QueryEngine {
        let snapshot = self.write_coordinator.view().snapshot.clone();
        let options = QueryEngineOptions {
            dimensions: self.config.dimensions,
            distance_metric: self.config.distance_metric,
            query_pruning_factor: self.config.query_pruning_factor,
        };
        QueryEngine::new(options, self.centroid_graph.clone(), snapshot)
    }

    /// Search using brute-force centroid lookup (for diagnostics).
    pub async fn search_exact_nprobe(
        &self,
        query: &Query,
        nprobe: usize,
    ) -> Result<Vec<SearchResult>> {
        self.query_engine().search_exact_nprobe(query, nprobe).await
    }

    pub async fn snapshot(&self) -> Box<dyn VectorDbRead> {
        Box::new(VectorDbReader::new(self.query_engine())) as Box<dyn VectorDbRead>
    }
}

#[async_trait]
impl VectorDbRead for VectorDb {
    async fn search(&self, query: &Query) -> Result<Vec<SearchResult>> {
        self.query_engine().search(query).await
    }

    async fn search_with_nprobe(&self, query: &Query, nprobe: usize) -> Result<Vec<SearchResult>> {
        self.query_engine().search_with_nprobe(query, nprobe).await
    }

    async fn get(&self, id: &str) -> Result<Option<Vector>> {
        self.query_engine().get(id).await
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
    use opendata_macros::storage_test;
    use std::time::Duration;

    fn create_test_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            flush_interval: Duration::from_secs(60),
            split_threshold_vectors: 10_000,
            merge_threshold_vectors: 200,
            split_search_neighbourhood: 8,
            chunk_target: 4096,
            metadata_fields: vec![
                MetadataFieldSpec::new("category", FieldType::String, true),
                MetadataFieldSpec::new("price", FieldType::Int64, true),
            ],
            ..Default::default()
        }
    }

    fn create_test_centroids(dimensions: usize) -> Vec<Vec<f32>> {
        vec![vec![1.0; dimensions]]
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

    #[storage_test(merge_operator = VectorDbMergeOperator::new(3))]
    async fn should_write_and_flush_vectors(storage: Arc<dyn Storage>) {
        // given
        let config = create_test_config();
        let centroids = create_test_centroids(3);
        let db = VectorDb::load_or_init_db(Arc::clone(&storage), config, centroids)
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
        // Note: centroid IDs are allocated from the same sequence as vector IDs.
        // With 1 centroid (ID 0), vectors start at ID 1.
        let vec1_data_key = VectorDataKey::new(1).encode();
        let vec1_data = storage.get(vec1_data_key).await.unwrap();
        assert!(vec1_data.is_some());

        let vec2_data_key = VectorDataKey::new(2).encode();
        let vec2_data = storage.get(vec2_data_key).await.unwrap();
        assert!(vec2_data.is_some());

        // Check IdDictionary
        let dict_key1 = IdDictionaryKey::new("vec-1").encode();
        let dict_entry1 = storage.get(dict_key1).await.unwrap();
        assert!(dict_entry1.is_some());
    }

    #[storage_test(merge_operator = VectorDbMergeOperator::new(3))]
    async fn should_upsert_existing_vector(storage: Arc<dyn Storage>) {
        // given
        let config = create_test_config();
        let centroids = create_test_centroids(3);
        let db = VectorDb::load_or_init_db(Arc::clone(&storage), config, centroids)
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
        // Centroid takes ID 0, first write gets ID 1, upsert gets ID 2
        let vec_data_key = VectorDataKey::new(2).encode(); // New internal ID
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

    #[storage_test(merge_operator = VectorDbMergeOperator::new(3))]
    async fn should_load_dictionary_on_reopen(storage: Arc<dyn Storage>) {
        // given - create database and write vectors
        let config = create_test_config();
        let centroids = create_test_centroids(3);

        {
            let db =
                VectorDb::load_or_init_db(Arc::clone(&storage), config.clone(), centroids.clone())
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
        let db2 = VectorDb::load_or_init_db(Arc::clone(&storage), config, vec![])
            .await
            .unwrap();

        // then - should be able to search (dictionary and centroids loaded from storage)
        let results = db2
            .search(&Query::new(vec![1.0, 0.0, 0.0]).with_limit(10))
            .await
            .unwrap();
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn flush_should_be_durable_across_reopen() {
        use common::storage::config::{
            LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
        };

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage_config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp_dir.path().to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
        });

        let config = Config {
            storage: storage_config.clone(),
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            ..Default::default()
        };

        // Write vectors and flush
        let db = VectorDb::open(config.clone()).await.unwrap();
        db.write(vec![
            Vector::new("vec-1", vec![1.0, 0.0, 0.0]),
            Vector::new("vec-2", vec![0.0, 1.0, 0.0]),
        ])
        .await
        .unwrap();
        db.flush().await.unwrap();
        drop(db);

        // Reopen from durable state — data should be visible
        let db2 = VectorDb::open(config).await.unwrap();
        let results = db2
            .search(&Query::new(vec![1.0, 0.0, 0.0]).with_limit(10))
            .await
            .unwrap();
        assert!(
            !results.is_empty(),
            "expected data to be durable after flush, but search returned no results"
        );
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn close_without_explicit_flush_guarantees_durability() {
        use common::storage::config::{
            LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
        };

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage_config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp_dir.path().to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
        });

        let config = Config {
            storage: storage_config.clone(),
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            ..Default::default()
        };

        // Write a vector and close without calling flush()
        {
            let db = VectorDb::open(config.clone()).await.unwrap();
            db.write(vec![Vector::new("vec-1", vec![1.0, 0.0, 0.0])])
                .await
                .unwrap();
            db.close().await.unwrap();
        }

        // Reopen and verify the vector survived
        let db2 = VectorDb::open(config).await.unwrap();
        let results = db2
            .search(&Query::new(vec![1.0, 0.0, 0.0]).with_limit(1))
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].vector.id, "vec-1");
    }

    #[tokio::test]
    async fn should_fail_if_no_centroids_provided_for_new_db() {
        // given - new database without centroids
        let config = create_test_config();

        // when
        let sb = StorageBuilder::new(&config.storage).await.unwrap();
        let result = VectorDb::open_with_centroids(config, vec![], sb).await;

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

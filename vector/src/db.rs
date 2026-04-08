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
use crate::error::{Error, Result};
use crate::model::{
    AttributeValue, Config, Query, SearchOptions, SearchResult, VECTOR_FIELD_NAME, Vector,
    attributes_to_map,
};
use crate::query_engine::{QueryEngine, QueryEngineOptions};
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::centroids::CentroidsValue;
use crate::serde::key::{
    CentroidInfoKey, CentroidSeqBlockKey, CentroidStatsKey, CentroidsKey, PostingListKey,
    SeqBlockKey,
};
use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use crate::serde::vector_id::{LEAF_LEVEL, ROOT_VECTOR_ID, VectorId};
use crate::storage::VectorDbStorageReadExt;
use crate::storage::merge_operator::VectorDbMergeOperator;
use crate::write::delta::{VectorDbWrite, VectorDbWriteDelta, VectorWrite};
use crate::write::flusher::VectorDbFlusher;
use crate::write::indexer::tree::Indexer;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::centroids::{
    AllCentroidsCache, AllCentroidsCacheWriter, CachedCentroidReader, CentroidCache,
    LeveledCentroidIndex, StoredCentroidReader, TreeDepth,
};
use crate::write::indexer::tree::posting_list::PostingList;
use crate::write::indexer::tree::state::VectorIndexState;
use async_trait::async_trait;
use common::Record;
use common::SequenceAllocator;
use common::coordinator::{Durability, WriteCoordinator, WriteCoordinatorConfig};
use common::storage::{Storage, StorageRead, StorageSnapshot};
use common::{StorageBuilder, StorageSemantics};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) const WRITE_CHANNEL: &str = "write";

pub(crate) struct LoadedTreeIndex {
    centroids_meta: CentroidsValue,
    root_centroid_count: u64,
    centroids: HashMap<VectorId, CentroidInfoValue>,
    centroid_counts: HashMap<u8, HashMap<VectorId, u64>>,
    centroid_cache: AllCentroidsCacheWriter,
    query_centroid_index: Arc<LeveledCentroidIndex<'static>>,
    num_leaf_centroids: usize,
}

/// Trait for querying the vector db
#[async_trait]
pub trait VectorDbRead {
    /// Convenience method that calls [`search_with_options`](Self::search_with_options)
    /// with default options.
    async fn search(&self, query: &Query) -> Result<Vec<SearchResult>> {
        self.search_with_options(query, SearchOptions::default())
            .await
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
    /// * `query` - search query
    /// * `options` - search options
    ///
    /// # Returns
    /// Vector of SearchResults sorted by similarity (best first)
    ///
    /// # Errors
    /// Returns an error if:
    /// - Query dimensions don't match collection dimensions
    /// - Storage read fails
    async fn search_with_options(
        &self,
        query: &Query,
        options: SearchOptions,
    ) -> Result<Vec<SearchResult>>;

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

/// TODO: we shouldn't need this. We should be able to hold a ref to the centroid cache
///       and then construct the centroid index directly from that plus the snapshot
#[derive(Clone)]
pub(crate) struct LastAppliedSnapshot {
    pub(crate) snapshot: Arc<dyn StorageSnapshot>,
    pub(crate) centroid_index: Arc<LeveledCentroidIndex<'static>>,
    pub(crate) centroid_cache: Arc<AllCentroidsCache>,
    pub(crate) centroid_count: usize,
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

    /// snapshot state for queries
    last_applied_snapshot: Arc<Mutex<LastAppliedSnapshot>>,
}

impl VectorDb {
    pub(crate) fn indexer_opts(config: &Config) -> IndexerOpts {
        let indexed_fields: HashSet<String> = config
            .metadata_fields
            .iter()
            .filter(|s| s.indexed)
            .map(|s| s.name.clone())
            .collect();
        IndexerOpts {
            dimensions: config.dimensions as usize,
            distance_metric: config.distance_metric,
            root_threshold_vectors: config.split_threshold_vectors,
            merge_threshold_vectors: config.merge_threshold_vectors,
            split_threshold_vectors: config.split_threshold_vectors,
            split_search_neighbourhood: config.split_search_neighbourhood,
            indexed_fields,
        }
    }

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
        let seq_key = SeqBlockKey.encode();
        let id_allocator = SequenceAllocator::load(storage.as_ref(), seq_key).await?;
        let centroid_seq_key = CentroidSeqBlockKey.encode();
        let mut centroid_id_allocator =
            SequenceAllocator::load(storage.as_ref(), centroid_seq_key).await?;

        let initial_snapshot = storage.snapshot().await?;

        Self::bootstrap_tree_index_if_needed(
            &storage,
            initial_snapshot.as_ref(),
            &config,
            centroids,
            &mut centroid_id_allocator,
        )
        .await?;

        let snapshot = storage.snapshot().await?;
        let loaded_tree = Self::load_tree_index(
            snapshot.clone(),
            0,
            config.dimensions as usize,
            config.distance_metric,
        )
        .await?;
        let last_applied_snapshot = Arc::new(Mutex::new(LastAppliedSnapshot {
            snapshot: snapshot.clone(),
            centroid_cache: Arc::new(loaded_tree.centroid_cache.cache()),
            centroid_index: loaded_tree.query_centroid_index,
            centroid_count: loaded_tree.num_leaf_centroids,
        }));

        let _ = id_allocator.freeze();
        let _ = centroid_id_allocator.freeze();

        let indexer_state =
            Self::load_indexer_state(storage.clone(), snapshot.clone(), &config, 0).await?;
        let indexer = Indexer::new(Self::indexer_opts(&config), indexer_state);

        let flusher = VectorDbFlusher::new(
            &config,
            Arc::clone(&storage),
            snapshot.clone(),
            0,
            indexer,
            last_applied_snapshot.clone(),
        );

        let coordinator_config = WriteCoordinatorConfig {
            queue_capacity: 1000,
            flush_interval: Duration::from_secs(5),
            flush_size_threshold: 10000,
        };
        let mut write_coordinator = WriteCoordinator::new(
            coordinator_config,
            vec![WRITE_CHANNEL.to_string()],
            (),
            snapshot.clone(),
            flusher,
        );
        write_coordinator.start();

        Ok(Self {
            config,
            storage,
            write_coordinator,
            last_applied_snapshot,
        })
    }

    async fn bootstrap_tree_index_if_needed(
        storage: &Arc<dyn Storage>,
        snapshot: &dyn StorageSnapshot,
        config: &Config,
        centroids: Vec<Vec<f32>>,
        centroid_id_allocator: &mut SequenceAllocator,
    ) -> Result<()> {
        if snapshot.get_centroids_meta().await?.is_some() {
            return Ok(());
        }

        if centroids.is_empty() {
            return Err(Error::InvalidInput(
                "Centroids must be provided when creating a new database".to_string(),
            ));
        }

        for centroid in &centroids {
            if centroid.len() != config.dimensions as usize {
                return Err(Error::InvalidInput(format!(
                    "Centroid dimension mismatch: expected {}, got {}",
                    config.dimensions,
                    centroid.len()
                )));
            }
        }

        let mut ops = Vec::new();
        let (root_id, seq_alloc_put) = centroid_id_allocator.allocate_one();
        if let Some(seq_alloc_put) = seq_alloc_put {
            ops.push(common::storage::RecordOp::Put(seq_alloc_put.into()));
        }
        assert_eq!(root_id, 0, "root centroid must be id 0");

        let mut root_postings = Vec::with_capacity(centroids.len());
        for vector in centroids {
            let (centroid_id, seq_alloc_put) = centroid_id_allocator.allocate_one();
            if let Some(seq_alloc_put) = seq_alloc_put {
                ops.push(common::storage::RecordOp::Put(seq_alloc_put.into()));
            }
            let centroid_id = VectorId::centroid_id(1, centroid_id);
            let centroid_info = CentroidInfoValue::new(1, vector.clone(), ROOT_VECTOR_ID);
            ops.push(common::storage::RecordOp::Put(
                Record::new(
                    CentroidInfoKey::new(centroid_id).encode(),
                    centroid_info.encode_to_bytes(),
                )
                .into(),
            ));
            ops.push(common::storage::RecordOp::Put(
                Record::new(
                    CentroidStatsKey::new(centroid_id).encode(),
                    CentroidStatsValue::new(0).encode_to_bytes(),
                )
                .into(),
            ));
            root_postings.push(PostingUpdate::append(centroid_id, vector));
        }

        ops.push(common::storage::RecordOp::Put(
            Record::new(
                CentroidsKey::new().encode(),
                CentroidsValue::new(3).encode_to_bytes(),
            )
            .into(),
        ));
        ops.push(common::storage::RecordOp::Put(
            Record::new(
                PostingListKey::new(ROOT_VECTOR_ID).encode(),
                PostingListValue::from_posting_updates(root_postings)?.encode_to_bytes(),
            )
            .into(),
        ));
        storage.apply(ops).await?;
        Ok(())
    }

    pub(crate) async fn load_tree_index(
        snapshot: Arc<dyn StorageSnapshot>,
        snapshot_epoch: u64,
        dimensions: usize,
        distance_metric: crate::DistanceMetric,
    ) -> Result<LoadedTreeIndex> {
        let centroids_meta = snapshot
            .get_centroids_meta()
            .await?
            .ok_or_else(|| Error::Storage("missing centroid tree metadata".to_string()))?;
        let root_posting_list =
            PostingList::from_value(snapshot.get_root_posting_list(dimensions).await?);
        let centroids: HashMap<VectorId, CentroidInfoValue> = snapshot
            .scan_all_centroid_info()
            .await?
            .into_iter()
            .collect();
        let centroid_counts = Self::load_centroid_counts_from_storage(snapshot.as_ref()).await?;
        let num_leaf_centroids = centroid_counts
            .get(&1)
            .map(HashMap::len)
            .unwrap_or_default();
        let centroid_postings = snapshot
            .scan_all_inner_posting_lists(dimensions)
            .await?
            .into_iter()
            .filter_map(|(centroid_id, posting_list)| {
                assert_ne!(centroid_id, ROOT_VECTOR_ID);
                centroids.get(&centroid_id).map(|centroid| {
                    assert!(centroid.level > LEAF_LEVEL);
                    (centroid_id, Arc::new(PostingList::from_value(posting_list)))
                })
            })
            .collect();
        let nroot_postings = root_posting_list.len() as u64;
        let centroid_cache =
            AllCentroidsCacheWriter::new(Arc::new(root_posting_list), centroid_postings);
        let cache = Arc::new(centroid_cache.cache()) as Arc<dyn CentroidCache>;
        let reader = Arc::new(CachedCentroidReader::new(
            &cache,
            StoredCentroidReader::new(dimensions, snapshot, snapshot_epoch),
        ));
        let ann_index = Arc::new(LeveledCentroidIndex::new(
            TreeDepth::of(centroids_meta.depth),
            distance_metric,
            reader,
        ));
        let query_centroid_index = ann_index;

        Ok(LoadedTreeIndex {
            centroids_meta,
            root_centroid_count: nroot_postings,
            centroids,
            centroid_counts,
            centroid_cache,
            query_centroid_index,
            num_leaf_centroids,
        })
    }

    pub(crate) async fn load_indexer_state(
        storage: Arc<dyn Storage>,
        snapshot: Arc<dyn StorageSnapshot>,
        config: &Config,
        snapshot_epoch: u64,
    ) -> Result<VectorIndexState> {
        let seq_key = SeqBlockKey.encode();
        let id_allocator = SequenceAllocator::load(storage.as_ref(), seq_key).await?;
        let centroid_seq_key = CentroidSeqBlockKey.encode();
        let centroid_id_allocator =
            SequenceAllocator::load(storage.as_ref(), centroid_seq_key).await?;
        let dictionary = Self::load_dictionary_from_storage(snapshot.as_ref()).await?;
        let loaded_tree = Self::load_tree_index(
            snapshot,
            snapshot_epoch,
            config.dimensions as usize,
            config.distance_metric,
        )
        .await?;
        let (seq_block_key, seq_block) = id_allocator.freeze();
        let (centroid_seq_block_key, centroid_seq_block) = centroid_id_allocator.freeze();

        Ok(VectorIndexState::new(
            dictionary,
            loaded_tree.centroids_meta,
            loaded_tree.root_centroid_count,
            loaded_tree.centroids,
            loaded_tree.centroid_counts,
            seq_block_key,
            seq_block,
            centroid_seq_block_key,
            centroid_seq_block,
            loaded_tree.centroid_cache,
        ))
    }

    /// Load ID dictionary entries from storage into a HashMap.
    pub(crate) async fn load_dictionary_from_storage(
        snapshot: &dyn StorageRead,
    ) -> Result<HashMap<String, VectorId>> {
        // Create prefix for all IdDictionary records
        let mut prefix_buf = bytes::BytesMut::with_capacity(3);
        crate::serde::RecordType::IdDictionary
            .prefix()
            .write_to(&mut prefix_buf);
        let prefix = prefix_buf.freeze();

        // Scan all IdDictionary records
        let range = common::BytesRange::prefix(prefix);
        let records = snapshot.scan(range).await?;

        records
            .into_par_iter()
            .map(|record| {
                let key = crate::serde::key::IdDictionaryKey::decode(&record.key)?;
                let value = crate::serde::id_dictionary::IdDictionaryValue::decode_from_bytes(
                    &record.value,
                )?;
                let internal_id = value.vector_id;
                Ok((key.external_id, internal_id))
            })
            .collect()
    }

    /// Load centroid counts from storage into a HashMap.
    ///
    /// Scans all CentroidStats records and extracts the accumulated num_vectors
    /// for each centroid.
    pub(crate) async fn load_centroid_counts_from_storage(
        snapshot: &dyn StorageRead,
    ) -> Result<HashMap<u8, HashMap<VectorId, u64>>> {
        let stats = snapshot.scan_all_centroid_stats().await?;
        let mut counts = HashMap::new();
        for (centroid_id, value) in stats {
            counts
                .entry(centroid_id.level())
                .or_insert_with(HashMap::new)
                .insert(centroid_id, value.num_vectors.max(0) as u64);
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

        // Normalize vector if metric requires it.
        let mut values = values;
        self.config.distance_metric.normalize_if_needed(&mut values);

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
        self.last_applied_snapshot
            .lock()
            .expect("lock_poisoned")
            .centroid_count
    }

    pub async fn validate_cache(&self) {
        let las = self
            .last_applied_snapshot
            .lock()
            .expect("lock_poisoned")
            .clone();
        crate::write::indexer::tree::validator::validate_state_and_storage_consistent(
            &las,
            self.config.dimensions as usize,
        )
        .await
        .expect("validation failed");
    }

    /// Create a QueryEngine from the current snapshot for executing queries.
    pub(crate) fn query_engine(&self) -> QueryEngine {
        let (snapshot, centroid_index) = {
            let guard = self.last_applied_snapshot.lock().expect("lock poisoned");
            (guard.snapshot.clone(), guard.centroid_index.clone())
        };
        let options = QueryEngineOptions {
            dimensions: self.config.dimensions,
            distance_metric: self.config.distance_metric,
            query_pruning_factor: self.config.query_pruning_factor,
        };
        QueryEngine::new(options, centroid_index, snapshot)
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
    async fn search_with_options(
        &self,
        query: &Query,
        options: SearchOptions,
    ) -> Result<Vec<SearchResult>> {
        self.query_engine()
            .search_with_options(query, options)
            .await
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
    use crate::serde::vector_id::VectorId;
    use common::StorageConfig;
    use opendata_macros::storage_test;
    use std::time::Duration;

    fn create_test_config() -> Config {
        create_test_config_with_metric(DistanceMetric::L2)
    }

    fn create_test_config_with_metric(metric: DistanceMetric) -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: metric,
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

    fn data_id(id: u64) -> VectorId {
        VectorId::data_vector_id(id)
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
        // Vector IDs are allocated independently from centroid IDs.
        let vec1_data_key = VectorDataKey::new(data_id(0)).encode();
        let vec1_data = storage.get(vec1_data_key).await.unwrap();
        assert!(vec1_data.is_some());

        let vec2_data_key = VectorDataKey::new(data_id(1)).encode();
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
        // Vector IDs are allocated independently from centroid IDs, so the first write
        // gets ID 0 and the upsert gets ID 1.
        let vec_data_key = VectorDataKey::new(data_id(1)).encode(); // New internal ID
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

    #[tokio::test]
    async fn should_normalize_vector_on_prepare_write_for_cosine() {
        // given
        let config = create_test_config_with_metric(DistanceMetric::Cosine);
        let db = VectorDb::open(config).await.unwrap();
        let vector = Vector::new("vec", vec![3.0, 4.0, 0.0]);

        // when
        let result = db.prepare_vector_write(vector).unwrap();

        // then - vector should be L2-normalized (3/5, 4/5, 0)
        let norm: f32 = result.values.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-6,
            "expected unit vector, got norm {}",
            norm
        );
        assert!((result.values[0] - 0.6).abs() < 1e-6);
        assert!((result.values[1] - 0.8).abs() < 1e-6);
        assert!((result.values[2] - 0.0).abs() < 1e-6);

        // attributes should retain the original unnormalized vector
        let attr_vec = result
            .attributes
            .iter()
            .find(|(k, _)| k == VECTOR_FIELD_NAME)
            .map(|(_, v)| v);
        match attr_vec {
            Some(AttributeValue::Vector(v)) => {
                assert_eq!(*v, vec![3.0, 4.0, 0.0]);
            }
            other => panic!("expected Vector attribute, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_keep_zero_vector_unchanged_for_cosine() {
        // given
        let config = create_test_config_with_metric(DistanceMetric::Cosine);
        let db = VectorDb::open(config).await.unwrap();
        let vector = Vector::new("vec-zero", vec![0.0, 0.0, 0.0]);

        // when
        let result = db.prepare_vector_write(vector).unwrap();

        // then - zero vector stays zero (no division by zero)
        assert_eq!(result.values, vec![0.0, 0.0, 0.0]);

        // attributes should retain the original vector
        let attr_vec = result
            .attributes
            .iter()
            .find(|(k, _)| k == VECTOR_FIELD_NAME)
            .map(|(_, v)| v);
        match attr_vec {
            Some(AttributeValue::Vector(v)) => {
                assert_eq!(*v, vec![0.0, 0.0, 0.0]);
            }
            other => panic!("expected Vector attribute, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_preserve_already_normalized_vector_for_cosine() {
        // given
        let config = create_test_config_with_metric(DistanceMetric::Cosine);
        let db = VectorDb::open(config).await.unwrap();
        let v = vec![
            1.0 / 3.0_f32.sqrt(),
            1.0 / 3.0_f32.sqrt(),
            1.0 / 3.0_f32.sqrt(),
        ];
        let vector = Vector::new("vec-unit", v.clone());

        // when
        let result = db.prepare_vector_write(vector).unwrap();

        // then - already-normalized vector should be essentially unchanged
        for (actual, expected) in result.values.iter().zip(v.iter()) {
            assert!(
                (actual - expected).abs() < 1e-6,
                "expected {}, got {}",
                expected,
                actual
            );
        }
    }

    #[tokio::test]
    async fn should_reject_wrong_dimensions_for_cosine() {
        // given
        let config = create_test_config_with_metric(DistanceMetric::Cosine);
        let db = VectorDb::open(config).await.unwrap();
        let vector = Vector::new("vec-bad", vec![1.0, 2.0]); // 2 dims instead of 3

        // when
        let result = db.prepare_vector_write(vector);

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
    async fn should_return_normalized_values_with_correct_id_for_cosine() {
        // given
        let config = create_test_config_with_metric(DistanceMetric::Cosine);
        let db = VectorDb::open(config).await.unwrap();
        let vector = Vector::new("my-id", vec![0.0, 5.0, 0.0]);

        // when
        let result = db.prepare_vector_write(vector).unwrap();

        // then
        assert_eq!(result.external_id, "my-id");
        assert!((result.values[0] - 0.0).abs() < 1e-6);
        assert!((result.values[1] - 1.0).abs() < 1e-6);
        assert!((result.values[2] - 0.0).abs() < 1e-6);

        // attributes should retain the original unnormalized vector
        let attr_vec = result
            .attributes
            .iter()
            .find(|(k, _)| k == VECTOR_FIELD_NAME)
            .map(|(_, v)| v);
        match attr_vec {
            Some(AttributeValue::Vector(v)) => {
                assert_eq!(*v, vec![0.0, 5.0, 0.0]);
            }
            other => panic!("expected Vector attribute, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_not_normalize_vector_on_prepare_write_for_l2() {
        // given
        let config = create_test_config(); // L2 metric
        let db = VectorDb::open(config).await.unwrap();
        let vector = Vector::new("vec-l2", vec![3.0, 4.0, 0.0]);

        // when
        let result = db.prepare_vector_write(vector).unwrap();

        // then - values should be unchanged
        assert_eq!(result.values, vec![3.0, 4.0, 0.0]);

        // attributes should also be unchanged
        let attr_vec = result
            .attributes
            .iter()
            .find(|(k, _)| k == VECTOR_FIELD_NAME)
            .map(|(_, v)| v);
        match attr_vec {
            Some(AttributeValue::Vector(v)) => {
                assert_eq!(*v, vec![3.0, 4.0, 0.0]);
            }
            other => panic!("expected Vector attribute, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_not_normalize_vector_on_prepare_write_for_dot_product() {
        // given
        let config = create_test_config_with_metric(DistanceMetric::DotProduct);
        let db = VectorDb::open(config).await.unwrap();
        let vector = Vector::new("vec-dot", vec![3.0, 4.0, 0.0]);

        // when
        let result = db.prepare_vector_write(vector).unwrap();

        // then - values should be unchanged
        assert_eq!(result.values, vec![3.0, 4.0, 0.0]);

        // attributes should also be unchanged
        let attr_vec = result
            .attributes
            .iter()
            .find(|(k, _)| k == VECTOR_FIELD_NAME)
            .map(|(_, v)| v);
        match attr_vec {
            Some(AttributeValue::Vector(v)) => {
                assert_eq!(*v, vec![3.0, 4.0, 0.0]);
            }
            other => panic!("expected Vector attribute, got {:?}", other),
        }
    }
}

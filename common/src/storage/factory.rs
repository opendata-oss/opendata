//! Storage factory for creating storage instances from configuration.
//!
//! This module provides factory functions for creating storage backends
//! based on configuration, supporting both InMemory and SlateDB backends.

use std::sync::Arc;

use slatedb::config::Settings;
use slatedb::object_store::{self, ObjectStore};
use slatedb::{DbBuilder, DbReader};
use tokio::runtime::Handle;

use super::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use super::in_memory::InMemoryStorage;
use super::slate::{SlateDbStorage, SlateDbStorageReader};
use super::{MergeOperator, Storage, StorageError, StorageRead, StorageResult};

/// Runtime options for storage that cannot be serialized.
///
/// This struct holds non-serializable runtime configuration like tokio
/// runtime handles. Users can configure these options and pass them to
/// system builders (e.g., `LogDbBuilder`, `TsdbBuilder`).
///
/// # Example
///
/// ```rust,ignore
/// use common::StorageRuntime;
///
/// // Create a separate runtime for compaction
/// let compaction_runtime = tokio::runtime::Builder::new_multi_thread()
///     .worker_threads(2)
///     .enable_all()
///     .build()
///     .unwrap();
///
/// let runtime = StorageRuntime::new()
///     .with_compaction_runtime(compaction_runtime.handle().clone());
///
/// // Pass to a system builder
/// let mut builder = LogDbBuilder::new(config);
/// *builder.storage_mut() = runtime;
/// let log = builder.build().await?;
/// ```
#[derive(Default)]
pub struct StorageRuntime {
    pub(crate) compaction_runtime: Option<Handle>,
}

impl StorageRuntime {
    /// Creates a new storage runtime with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a separate runtime for SlateDB compaction tasks.
    ///
    /// When provided, SlateDB's compaction tasks will run on this runtime
    /// instead of the runtime used for user operations. This is important
    /// when calling the database from sync code using `block_on`, as it
    /// prevents deadlocks between user operations and background compaction.
    ///
    /// This option only affects SlateDB storage; it is ignored for in-memory storage.
    pub fn with_compaction_runtime(mut self, handle: Handle) -> Self {
        self.compaction_runtime = Some(handle);
        self
    }
}

/// Storage semantics configured by system crates.
///
/// This struct holds semantic concerns like merge operators that are specific
/// to each system (log, timeseries, vector). End users should not use this
/// directly - each system configures its own semantics internally.
///
/// # Internal Use Only
///
/// This type is public so that system crates (timeseries, vector, log) can
/// access it, but it is not intended for end-user consumption.
///
/// # Example (for system crate implementers)
///
/// ```rust,ignore
/// // In timeseries crate:
/// let semantics = StorageSemantics::new()
///     .with_merge_operator(Arc::new(TimeSeriesMergeOperator));
/// let storage = create_storage(&config, runtime, semantics).await?;
/// ```
#[derive(Default)]
pub struct StorageSemantics {
    pub(crate) merge_operator: Option<Arc<dyn MergeOperator>>,
}

impl StorageSemantics {
    /// Creates new storage semantics with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the merge operator for merge operations.
    ///
    /// The merge operator defines how values are combined during compaction.
    /// Each system (timeseries, vector) defines its own merge semantics.
    pub fn with_merge_operator(mut self, op: Arc<dyn MergeOperator>) -> Self {
        self.merge_operator = Some(op);
        self
    }
}

/// Creates an object store from configuration without initializing SlateDB.
///
/// This is useful for cleanup operations where you need to access the object store
/// after the database has been closed.
pub fn create_object_store(config: &ObjectStoreConfig) -> StorageResult<Arc<dyn ObjectStore>> {
    match config {
        ObjectStoreConfig::InMemory => Ok(Arc::new(object_store::memory::InMemory::new())),
        ObjectStoreConfig::Aws(aws_config) => {
            let store = object_store::aws::AmazonS3Builder::from_env()
                .with_region(&aws_config.region)
                .with_bucket_name(&aws_config.bucket)
                .build()
                .map_err(|e| {
                    StorageError::Storage(format!("Failed to create AWS S3 store: {}", e))
                })?;
            Ok(Arc::new(store))
        }
        ObjectStoreConfig::Local(local_config) => {
            std::fs::create_dir_all(&local_config.path).map_err(|e| {
                StorageError::Storage(format!(
                    "Failed to create storage directory '{}': {}",
                    local_config.path, e
                ))
            })?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(&local_config.path)
                .map_err(|e| {
                StorageError::Storage(format!("Failed to create local filesystem store: {}", e))
            })?;
            Ok(Arc::new(store))
        }
    }
}

/// Creates a storage instance based on configuration, runtime options, and semantics.
///
/// This is the primary factory function for creating storage backends. It combines:
/// - `config`: Serializable storage configuration (backend type, paths, etc.)
/// - `runtime`: Non-serializable runtime options (compaction runtime handle)
/// - `semantics`: System-specific semantics (merge operators)
///
/// # Arguments
///
/// * `config` - The storage configuration specifying the backend type and settings.
/// * `runtime` - Runtime options like compaction runtime handles.
/// * `semantics` - System-specific semantics like merge operators.
///
/// # Returns
///
/// Returns an `Arc<dyn Storage>` on success, or a `StorageError` on failure.
///
/// # Example (for system crate implementers)
///
/// ```rust,ignore
/// // In a system crate's builder:
/// let storage = create_storage(
///     &self.config.storage,
///     self.storage_runtime.unwrap_or_default(),
///     StorageSemantics::new().with_merge_operator(Arc::new(MyMergeOp)),
/// ).await?;
/// ```
pub async fn create_storage(
    config: &StorageConfig,
    runtime: StorageRuntime,
    semantics: StorageSemantics,
) -> StorageResult<Arc<dyn Storage>> {
    match config {
        StorageConfig::InMemory => {
            let storage = match semantics.merge_operator {
                Some(op) => InMemoryStorage::with_merge_operator(op),
                None => InMemoryStorage::new(),
            };
            Ok(Arc::new(storage))
        }
        StorageConfig::SlateDb(slate_config) => {
            let storage = create_slatedb_storage(slate_config, runtime, semantics).await?;
            Ok(Arc::new(storage))
        }
    }
}

/// Creates a read-only storage instance based on configuration.
///
/// This function creates a storage backend that only supports read operations.
/// For SlateDB, it uses `DbReader` which does not participate in fencing,
/// allowing multiple readers to coexist with a single writer.
///
/// # Arguments
///
/// * `config` - The storage configuration specifying the backend type and settings.
/// * `semantics` - System-specific semantics like merge operators.
///
/// # Returns
///
/// Returns an `Arc<dyn StorageRead>` on success, or a `StorageError` on failure.
pub async fn create_storage_read(
    config: &StorageConfig,
    semantics: StorageSemantics,
) -> StorageResult<Arc<dyn StorageRead>> {
    match config {
        StorageConfig::InMemory => {
            // InMemory has no fencing, reuse existing implementation
            let storage = match semantics.merge_operator {
                Some(op) => InMemoryStorage::with_merge_operator(op),
                None => InMemoryStorage::new(),
            };
            Ok(Arc::new(storage))
        }
        StorageConfig::SlateDb(slate_config) => {
            let object_store = create_object_store(&slate_config.object_store)?;
            let mut options = slatedb::config::DbReaderOptions::default();
            if let Some(op) = semantics.merge_operator {
                let adapter = SlateDbStorage::merge_operator_adapter(op);
                options.merge_operator = Some(Arc::new(adapter));
            }
            let reader = DbReader::open(
                slate_config.path.clone(),
                object_store,
                None, // checkpoint_id - use latest state
                options,
            )
            .await
            .map_err(|e| {
                StorageError::Storage(format!("Failed to create SlateDB reader: {}", e))
            })?;
            Ok(Arc::new(SlateDbStorageReader::new(Arc::new(reader))))
        }
    }
}

async fn create_slatedb_storage(
    config: &SlateDbStorageConfig,
    runtime: StorageRuntime,
    semantics: StorageSemantics,
) -> StorageResult<SlateDbStorage> {
    let object_store = create_object_store(&config.object_store)?;

    // Load SlateDB settings
    let settings = match &config.settings_path {
        Some(path) => Settings::from_file(path).map_err(|e| {
            StorageError::Storage(format!(
                "Failed to load SlateDB settings from {}: {}",
                path, e
            ))
        })?,
        None => Settings::load().unwrap_or_default(),
    };

    // Build the database
    let mut db_builder = DbBuilder::new(config.path.clone(), object_store).with_settings(settings);

    // Add merge operator if provided
    if let Some(op) = semantics.merge_operator {
        let adapter = SlateDbStorage::merge_operator_adapter(op);
        db_builder = db_builder.with_merge_operator(Arc::new(adapter));
    }

    // Add compaction runtime if provided
    if let Some(handle) = runtime.compaction_runtime {
        db_builder = db_builder.with_compaction_runtime(handle);
    }

    let db = db_builder
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB: {}", e)))?;

    Ok(SlateDbStorage::new(Arc::new(db)))
}

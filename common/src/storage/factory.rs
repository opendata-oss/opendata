//! Storage factory for creating storage instances from configuration.
//!
//! This module provides factory functions and builders for creating storage backends
//! based on configuration, supporting both InMemory and SlateDB backends.

use std::sync::Arc;

use slatedb::DbBuilder;
use slatedb::config::Settings;
use slatedb::object_store::{self, ObjectStore};
use tokio::runtime::Handle;

use super::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use super::in_memory::InMemoryStorage;
use super::slate::SlateDbStorage;
use super::{MergeOperator, Storage, StorageError, StorageResult};

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

/// Creates a storage instance based on the provided configuration.
///
/// This is a convenience function that uses default options. For more control,
/// use [`StorageBuilder`].
///
/// # Arguments
///
/// * `config` - The storage configuration specifying the backend type and settings.
/// * `merge_operator` - Optional merge operator for merge operations. Required if
///   the storage will use merge operations.
///
/// # Returns
///
/// Returns an `Arc<dyn Storage>` on success, or a `StorageError` on failure.
pub async fn create_storage(
    config: &StorageConfig,
    merge_operator: Option<Arc<dyn MergeOperator>>,
) -> StorageResult<Arc<dyn Storage>> {
    let mut builder = StorageBuilder::new(config.clone());
    if let Some(op) = merge_operator {
        builder = builder.with_merge_operator(op);
    }
    builder.build().await
}

/// Builder for creating storage instances with custom options.
///
/// This builder provides a fluent API for configuring storage, including
/// runtime options that cannot be serialized in configuration files.
///
/// # Example
///
/// ```rust,ignore
/// use common::storage::factory::StorageBuilder;
/// use common::storage::config::StorageConfig;
///
/// // Create a separate runtime for compaction
/// let compaction_runtime = tokio::runtime::Builder::new_multi_thread()
///     .worker_threads(2)
///     .enable_all()
///     .build()
///     .unwrap();
///
/// let storage = StorageBuilder::new(config)
///     .with_compaction_runtime(compaction_runtime.handle().clone())
///     .build()
///     .await?;
/// ```
pub struct StorageBuilder {
    config: StorageConfig,
    merge_operator: Option<Arc<dyn MergeOperator>>,
    compaction_runtime: Option<Handle>,
}

impl StorageBuilder {
    /// Creates a new storage builder with the given configuration.
    pub fn new(config: StorageConfig) -> Self {
        Self {
            config,
            merge_operator: None,
            compaction_runtime: None,
        }
    }

    /// Sets the merge operator for merge operations.
    ///
    /// This is typically used internally by higher-level abstractions
    /// and not by end users directly.
    pub fn with_merge_operator(mut self, op: Arc<dyn MergeOperator>) -> Self {
        self.merge_operator = Some(op);
        self
    }

    /// Sets a separate runtime for SlateDB compaction tasks.
    ///
    /// When provided, SlateDB's compaction tasks will run on this runtime
    /// instead of the runtime used for user operations. This is important
    /// when calling SlateDB from sync code using `block_on`, as it prevents
    /// deadlocks between user operations and background compaction.
    ///
    /// This option only affects SlateDB storage; it is ignored for in-memory storage.
    pub fn with_compaction_runtime(mut self, handle: Handle) -> Self {
        self.compaction_runtime = Some(handle);
        self
    }

    /// Builds the storage instance.
    pub async fn build(self) -> StorageResult<Arc<dyn Storage>> {
        match &self.config {
            StorageConfig::InMemory => {
                let storage = match self.merge_operator {
                    Some(op) => InMemoryStorage::with_merge_operator(op),
                    None => InMemoryStorage::new(),
                };
                Ok(Arc::new(storage))
            }
            StorageConfig::SlateDb(slate_config) => {
                let storage = create_slatedb_storage(
                    slate_config,
                    self.merge_operator,
                    self.compaction_runtime,
                )
                .await?;
                Ok(Arc::new(storage))
            }
        }
    }
}

async fn create_slatedb_storage(
    config: &SlateDbStorageConfig,
    merge_operator: Option<Arc<dyn MergeOperator>>,
    compaction_runtime: Option<Handle>,
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
    if let Some(op) = merge_operator {
        let adapter = SlateDbStorage::merge_operator_adapter(op);
        db_builder = db_builder.with_merge_operator(Arc::new(adapter));
    }

    // Add compaction runtime if provided
    if let Some(runtime_handle) = compaction_runtime {
        db_builder = db_builder.with_compaction_runtime(runtime_handle);
    }

    let db = db_builder
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB: {}", e)))?;

    Ok(SlateDbStorage::new(Arc::new(db)))
}

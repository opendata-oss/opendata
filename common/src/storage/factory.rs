//! Storage factory for creating storage instances from configuration.
//!
//! This module provides a factory function to create storage backends based on
//! configuration, supporting both InMemory and SlateDB backends.

use std::sync::Arc;

use slatedb::DbBuilder;
use slatedb::config::Settings;
use slatedb::object_store;

use super::config::{ObjectStoreConfig, StorageConfig};
use super::in_memory::InMemoryStorage;
use super::slate::SlateDbStorage;
use super::{MergeOperator, Storage, StorageError, StorageResult};

/// Creates a storage instance based on the provided configuration.
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
///
/// # Examples
///
/// ```rust,ignore
/// use common::storage::config::StorageConfig;
/// use common::storage::factory::create_storage;
///
/// // Create in-memory storage (default)
/// let storage = create_storage(&StorageConfig::default(), None).await?;
///
/// // Create SlateDB storage with custom config
/// let config = StorageConfig::SlateDb(SlateDbStorageConfig {
///     path: "my-data".to_string(),
///     object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
///         path: "/tmp/slatedb".to_string(),
///     }),
///     settings_path: None,
/// });
/// let storage = create_storage(&config, Some(my_merge_op)).await?;
/// ```
pub async fn create_storage(
    config: &StorageConfig,
    merge_operator: Option<Arc<dyn MergeOperator>>,
) -> StorageResult<Arc<dyn Storage>> {
    match config {
        StorageConfig::InMemory => {
            let storage = match merge_operator {
                Some(op) => InMemoryStorage::with_merge_operator(op),
                None => InMemoryStorage::new(),
            };
            Ok(Arc::new(storage))
        }
        StorageConfig::SlateDb(slate_config) => {
            let storage = create_slatedb_storage(slate_config, merge_operator).await?;
            Ok(Arc::new(storage))
        }
    }
}

async fn create_slatedb_storage(
    config: &super::config::SlateDbStorageConfig,
    merge_operator: Option<Arc<dyn MergeOperator>>,
) -> StorageResult<SlateDbStorage> {
    // Create object store based on configuration
    let object_store: Arc<dyn object_store::ObjectStore> = match &config.object_store {
        ObjectStoreConfig::InMemory => Arc::new(object_store::memory::InMemory::new()),
        ObjectStoreConfig::Aws(aws_config) => {
            let store = object_store::aws::AmazonS3Builder::new()
                .with_region(&aws_config.region)
                .with_bucket_name(&aws_config.bucket)
                .build()
                .map_err(|e| {
                    StorageError::Storage(format!("Failed to create AWS S3 store: {}", e))
                })?;
            Arc::new(store)
        }
        ObjectStoreConfig::Local(local_config) => {
            // Create the directory if it doesn't exist
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
            Arc::new(store)
        }
    };

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

    let db = db_builder
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB: {}", e)))?;

    Ok(SlateDbStorage::new(Arc::new(db)))
}

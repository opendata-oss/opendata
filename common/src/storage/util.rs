//! Storage utilities.
//!
//! This module provides utility functions for storage operations,
//! such as deleting data from object stores after benchmarks or tests.

use futures::StreamExt;
use slatedb::object_store::{ObjectStore, path::Path};

use super::config::StorageConfig;
use super::factory::create_object_store;
use super::{StorageError, StorageResult};

/// Deletes all data for the given storage configuration.
///
/// Deletes all objects under the configured path prefix. Call this after the
/// database has been closed.
pub async fn delete(config: &StorageConfig) -> StorageResult<()> {
    let object_store = create_object_store(&config.object_store)?;
    let prefix = Path::from(config.path.as_str());
    delete_prefix(&*object_store, &prefix).await
}

/// Deletes all objects under a given prefix in an object store.
///
/// This is a lower-level function for cases where you already have an object store.
pub async fn delete_prefix(object_store: &dyn ObjectStore, prefix: &Path) -> StorageResult<()> {
    let mut list_stream = object_store.list(Some(prefix));

    while let Some(result) = list_stream.next().await {
        let meta =
            result.map_err(|e| StorageError::Storage(format!("Failed to list objects: {}", e)))?;

        object_store.delete(&meta.location).await.map_err(|e| {
            StorageError::Storage(format!(
                "Failed to delete object {:?}: {}",
                meta.location, e
            ))
        })?;
    }

    Ok(())
}

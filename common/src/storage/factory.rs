//! Storage factory for creating storage instances from configuration.
//!
//! This module provides factory functions for creating storage backends
//! based on configuration, supporting both InMemory and SlateDB backends.

use std::sync::Arc;

use super::config::{BlockCacheConfig, ObjectStoreConfig, StorageConfig};
use super::in_memory::InMemoryStorage;
use super::metrics_recorder::{MetricsRsRecorder, MixtricsBridge as MetricsRsRegistry};
use super::slate::{SlateDbStorage, SlateDbStorageReader};
use super::{MergeOperator, Storage, StorageError, StorageRead, StorageResult};
use slatedb::DbReader;
use slatedb::config::Settings;
pub use slatedb::db_cache::CachedEntry;
pub use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
pub use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::{CachedKey, DbCache};
use slatedb::object_store::{self, ObjectStore};
pub use slatedb::{CompactorBuilder, DbBuilder};
use tracing::info;
use uuid::Uuid;

/// Handle to a foyer hybrid cache that we own and must close explicitly on
/// shutdown. Cloneable because foyer's `HybridCache` is Arc-backed.
///
/// TODO(slatedb 0.13): remove this and the surrounding plumbing. SlateDB 0.13
/// adds a `close()` hook to the `DbCache` trait and drives cache shutdown from
/// `Db::close()` / `DbReader::close()`, so callers won't need to hold a side
/// handle to the hybrid cache just to close it deterministically.
pub(crate) type OwnedHybridCache = foyer::HybridCache<CachedKey, CachedEntry>;

/// Block cache we constructed internally — keep the `HybridCache` handle so
/// we can call `close().await` from `StorageRead::close()` instead of relying
/// on foyer's Drop-based close, which races the runtime shutdown.
struct ManagedBlockCache {
    db_cache: Arc<dyn DbCache>,
    hybrid: OwnedHybridCache,
}

/// Builder for creating storage instances from configuration.
///
/// `StorageBuilder` provides layered access to the underlying SlateDB
/// [`DbBuilder`], replacing the previous `StorageRuntime` middleman.
///
/// # Example
///
/// ```rust,ignore
/// use common::{StorageBuilder, StorageSemantics, create_object_store};
/// use common::storage::factory::CompactorBuilder;
///
/// // Simple usage:
/// let storage = StorageBuilder::new(&config.storage).await?
///     .with_semantics(StorageSemantics::new().with_merge_operator(Arc::new(MyOp)))
///     .build()
///     .await?;
///
/// // Escape hatch for low-level SlateDB configuration:
/// let storage = StorageBuilder::new(&config.storage).await?
///     .map_slatedb(|db| {
///         let obj_store = create_object_store(&slate_config.object_store).unwrap();
///         db.with_compactor_builder(
///             CompactorBuilder::new(slate_config.path.clone(), obj_store)
///                 .with_runtime(compaction_runtime.handle().clone())
///         )
///     })
///     .build()
///     .await?;
/// ```
pub struct StorageBuilder {
    inner: StorageBuilderInner,
    semantics: StorageSemantics,
    managed_cache: Option<OwnedHybridCache>,
}

enum StorageBuilderInner {
    InMemory,
    SlateDb(Box<DbBuilder<String>>),
}

impl StorageBuilder {
    /// Creates a new `StorageBuilder` from a [`StorageConfig`].
    ///
    /// For SlateDB configs this creates a [`DbBuilder`] with the configured
    /// path, object store, settings, and block cache (if configured). For
    /// InMemory configs it stores a sentinel so that `build()` returns an
    /// `InMemoryStorage`.
    pub async fn new(config: &StorageConfig) -> StorageResult<Self> {
        let mut managed_cache: Option<OwnedHybridCache> = None;
        let inner = match config {
            StorageConfig::InMemory => StorageBuilderInner::InMemory,
            StorageConfig::SlateDb(slate_config) => {
                let object_store = create_object_store(&slate_config.object_store)?;
                let settings = match &slate_config.settings_path {
                    Some(path) => Settings::from_file(path).map_err(|e| {
                        StorageError::Storage(format!(
                            "Failed to load SlateDB settings from {}: {}",
                            path, e
                        ))
                    })?,
                    None => Settings::load().unwrap_or_default(),
                };
                info!(
                    "create slatedb storage with config: {:?}, settings: {:?}",
                    slate_config, settings
                );
                let mut db_builder =
                    DbBuilder::new(slate_config.path.clone(), object_store).with_settings(settings);
                if let Some(managed) =
                    create_block_cache_from_config(&slate_config.block_cache).await?
                {
                    db_builder = db_builder.with_db_cache(managed.db_cache);
                    managed_cache = Some(managed.hybrid);
                }
                StorageBuilderInner::SlateDb(Box::new(db_builder))
            }
        };
        Ok(Self {
            inner,
            semantics: StorageSemantics::default(),
            managed_cache,
        })
    }

    /// Sets the [`StorageSemantics`] (merge operator, etc.) for this builder.
    pub fn with_semantics(mut self, semantics: StorageSemantics) -> Self {
        self.semantics = semantics;
        self
    }

    /// Maps over the underlying [`DbBuilder`] for low-level SlateDB configuration.
    ///
    /// This is the escape hatch for any SlateDB knob not exposed by
    /// `StorageBuilder` itself (compactor builder, block cache, GC runtime, etc.).
    /// Use `db.with_db_cache(...)` inside the closure to override the
    /// config-driven block cache.
    ///
    /// For InMemory storage this is a no-op.
    pub fn map_slatedb(mut self, f: impl FnOnce(DbBuilder<String>) -> DbBuilder<String>) -> Self {
        if let StorageBuilderInner::SlateDb(db) = self.inner {
            self.inner = StorageBuilderInner::SlateDb(Box::new(f(*db)));
        }
        self
    }

    /// Builds the storage instance.
    ///
    /// Applies semantics (merge operator) to the `DbBuilder` and calls `.build()`.
    pub async fn build(self) -> StorageResult<Arc<dyn Storage>> {
        match self.inner {
            StorageBuilderInner::InMemory => {
                let storage = match self.semantics.merge_operator {
                    Some(op) => InMemoryStorage::with_merge_operator(op),
                    None => InMemoryStorage::new(),
                };
                Ok(Arc::new(storage))
            }
            StorageBuilderInner::SlateDb(db_builder) => {
                let mut db_builder = *db_builder;
                db_builder = db_builder.with_metrics_recorder(Arc::new(MetricsRsRecorder));
                if let Some(op) = self.semantics.merge_operator {
                    let adapter = SlateDbStorage::merge_operator_adapter(op);
                    db_builder = db_builder.with_merge_operator(Arc::new(adapter));
                }
                let db = db_builder.build().await.map_err(|e| {
                    StorageError::Storage(format!("Failed to create SlateDB: {}", e))
                })?;
                Ok(Arc::new(SlateDbStorage::new_with_managed_cache(
                    Arc::new(db),
                    self.managed_cache,
                )))
            }
        }
    }
}

/// Runtime options for read-only storage instances.
///
/// This struct holds non-serializable runtime configuration for `DbReader`.
/// Unlike `StorageBuilder`, it only exposes options relevant to readers
/// (currently just block cache).
#[derive(Default, Clone)]
pub struct StorageReaderRuntime {
    pub(crate) block_cache: Option<Arc<dyn DbCache>>,
    pub(crate) object_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) checkpoint_id: Option<Uuid>,
}

impl StorageReaderRuntime {
    /// Creates a new reader runtime with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a block cache for SlateDB reads.
    ///
    /// When provided, the `DbReader` will use this cache for SST block lookups,
    /// reducing disk I/O on repeated reads. Use `FoyerCache::new_with_opts`
    /// to control capacity.
    ///
    /// This option only affects SlateDB storage; it is ignored for in-memory storage.
    pub fn with_block_cache(mut self, cache: Arc<dyn DbCache>) -> Self {
        self.block_cache = Some(cache);
        self
    }

    pub fn with_object_store(mut self, object_store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }

    /// Pins this reader to a specific SlateDB checkpoint.
    ///
    /// When set, the reader serves a consistent view of the database as of
    /// the checkpoint and does not advance with newer writes. The checkpoint
    /// must already exist in storage (typically created via
    /// [`crate::Storage::create_checkpoint`]). Only meaningful for SlateDB
    /// storage.
    pub fn with_checkpoint_id(mut self, id: Uuid) -> Self {
        self.checkpoint_id = Some(id);
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
/// let storage = StorageBuilder::new(&config).await?
///     .with_semantics(semantics)
///     .build()
///     .await?;
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
/// * `reader_options` - SlateDB reader options (e.g., manifest_poll_interval).
///   These are passed directly to `DbReader::open` for SlateDB storage.
///   Ignored for InMemory storage.
///
/// # Returns
///
/// Returns an `Arc<dyn StorageRead>` on success, or a `StorageError` on failure.
pub async fn create_storage_read(
    config: &StorageConfig,
    runtime: StorageReaderRuntime,
    semantics: StorageSemantics,
    reader_options: slatedb::config::DbReaderOptions,
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
            let object_store = if let Some(object_store) = &runtime.object_store {
                object_store.clone()
            } else {
                create_object_store(&slate_config.object_store)?
            };

            let mut builder = DbReader::builder(slate_config.path.clone(), object_store)
                .with_options(reader_options)
                .with_metrics_recorder(Arc::new(MetricsRsRecorder));
            if let Some(checkpoint_id) = runtime.checkpoint_id {
                builder = builder.with_checkpoint_id(checkpoint_id);
            }
            if let Some(op) = semantics.merge_operator {
                let adapter = SlateDbStorage::merge_operator_adapter(op);
                builder = builder.with_merge_operator(Arc::new(adapter));
            }
            // Prefer runtime-provided cache, fall back to config. The
            // runtime-provided cache is owned by the caller, so we don't hold
            // a handle to close it on shutdown.
            let mut managed_cache: Option<OwnedHybridCache> = None;
            if let Some(cache) = runtime.block_cache {
                builder = builder.with_db_cache(cache);
            } else if let Some(managed) =
                create_block_cache_from_config(&slate_config.block_cache).await?
            {
                builder = builder.with_db_cache(managed.db_cache);
                managed_cache = Some(managed.hybrid);
            }
            let reader = builder.build().await.map_err(|e| {
                StorageError::Storage(format!("Failed to create SlateDB reader: {}", e))
            })?;
            Ok(Arc::new(SlateDbStorageReader::new_with_managed_cache(
                Arc::new(reader),
                managed_cache,
            )))
        }
    }
}

/// Creates a block cache from the serializable config, if present. Returns
/// both the `DbCache` trait object handed to SlateDB and a `HybridCache`
/// handle the caller keeps so it can close the cache deterministically on
/// shutdown.
async fn create_block_cache_from_config(
    config: &Option<BlockCacheConfig>,
) -> StorageResult<Option<ManagedBlockCache>> {
    let Some(config) = config else {
        return Ok(None);
    };
    match config {
        BlockCacheConfig::FoyerHybrid(foyer_config) => {
            use foyer::{
                DirectFsDeviceOptions, Engine, HybridCacheBuilder, HybridCachePolicy,
                LargeEngineOptions,
            };

            let memory_capacity = usize::try_from(foyer_config.memory_capacity).map_err(|_| {
                StorageError::Storage(format!(
                    "memory_capacity {} exceeds usize::MAX on this platform",
                    foyer_config.memory_capacity
                ))
            })?;
            let disk_capacity = usize::try_from(foyer_config.disk_capacity).map_err(|_| {
                StorageError::Storage(format!(
                    "disk_capacity {} exceeds usize::MAX on this platform",
                    foyer_config.disk_capacity
                ))
            })?;

            let buffer_pool_size = usize::try_from(foyer_config.effective_buffer_pool_size())
                .map_err(|_| {
                    StorageError::Storage(format!(
                        "buffer_pool_size {} exceeds usize::MAX on this platform",
                        foyer_config.effective_buffer_pool_size()
                    ))
                })?;
            let submit_queue_size_threshold =
                usize::try_from(foyer_config.submit_queue_size_threshold).map_err(|_| {
                    StorageError::Storage(format!(
                        "submit_queue_size_threshold {} exceeds usize::MAX on this platform",
                        foyer_config.submit_queue_size_threshold
                    ))
                })?;

            let policy = match foyer_config.write_policy {
                super::config::FoyerWritePolicy::WriteOnInsertion => {
                    HybridCachePolicy::WriteOnInsertion
                }
                super::config::FoyerWritePolicy::WriteOnEviction => {
                    HybridCachePolicy::WriteOnEviction
                }
            };

            let cache = HybridCacheBuilder::new()
                .with_name("slatedb_block_cache")
                .with_metrics_registry(Box::new(MetricsRsRegistry))
                .with_policy(policy)
                .memory(memory_capacity)
                .with_weighter(|_, v: &CachedEntry| v.size())
                .storage(Engine::Large(
                    LargeEngineOptions::new()
                        .with_flushers(foyer_config.flushers)
                        .with_buffer_pool_size(buffer_pool_size)
                        .with_submit_queue_size_threshold(submit_queue_size_threshold),
                ))
                .with_device_options(
                    DirectFsDeviceOptions::new(&foyer_config.disk_path)
                        .with_capacity(disk_capacity),
                )
                .build()
                .await
                .map_err(|e| {
                    StorageError::Storage(format!("Failed to create hybrid cache: {}", e))
                })?;

            info!(
                memory_mb = foyer_config.memory_capacity / (1024 * 1024),
                disk_mb = foyer_config.disk_capacity / (1024 * 1024),
                disk_path = %foyer_config.disk_path,
                write_policy = ?foyer_config.write_policy,
                flushers = foyer_config.flushers,
                buffer_pool_mb = foyer_config.effective_buffer_pool_size() / (1024 * 1024),
                submit_queue_threshold_mb =
                    foyer_config.submit_queue_size_threshold / (1024 * 1024),
                "hybrid block cache enabled"
            );

            let db_cache =
                Arc::new(FoyerHybridCache::new_with_cache(cache.clone())) as Arc<dyn DbCache>;
            Ok(Some(ManagedBlockCache {
                db_cache,
                hybrid: cache,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::config::{
        FoyerHybridCacheConfig, LocalObjectStoreConfig, SlateDbStorageConfig,
    };

    fn foyer_cache_config(
        memory_capacity: u64,
        disk_capacity: u64,
        disk_path: String,
    ) -> FoyerHybridCacheConfig {
        FoyerHybridCacheConfig {
            memory_capacity,
            disk_capacity,
            disk_path,
            write_policy: Default::default(),
            flushers: 4,
            buffer_pool_size: None,
            submit_queue_size_threshold: 1024 * 1024 * 1024,
        }
    }

    fn slatedb_config_with_local_dir(dir: &std::path::Path) -> StorageConfig {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
        })
    }

    #[tokio::test]
    async fn should_create_storage_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("block-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                cache_dir.to_str().unwrap().to_string(),
            ))),
        });

        let storage = StorageBuilder::new(&config).await.unwrap().build().await;

        assert!(
            storage.is_ok(),
            "expected config-driven block cache to work"
        );
    }

    #[tokio::test]
    async fn should_create_reader_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("block-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let slate_config = SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                cache_dir.to_str().unwrap().to_string(),
            ))),
        };

        // First open a writer so the reader has a manifest to read
        let writer = StorageBuilder::new(&StorageConfig::SlateDb(slate_config.clone()))
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        // Close writer before opening reader (SlateDB fencing)
        drop(writer);

        let reader = create_storage_read(
            &StorageConfig::SlateDb(slate_config),
            StorageReaderRuntime::new(),
            StorageSemantics::new(),
            slatedb::config::DbReaderOptions::default(),
        )
        .await;

        assert!(
            reader.is_ok(),
            "expected config-driven block cache on reader to work"
        );
    }

    #[cfg(target_pointer_width = "32")]
    #[tokio::test]
    async fn should_error_when_capacity_exceeds_usize() {
        // On 32-bit platforms, u64::MAX > usize::MAX triggers our overflow check.
        // On 64-bit this is a no-op, so gate on 32-bit.
        let config = BlockCacheConfig::FoyerHybrid(foyer_cache_config(
            u64::MAX,
            4 * 1024 * 1024,
            "/tmp/unused".to_string(),
        ));

        let result = create_block_cache_from_config(&Some(config)).await;
        assert!(result.is_err());
    }

    /// Helper: creates a SlateDb config whose block_cache disk_path is a regular file
    /// (not a directory), which foyer deterministically rejects.
    fn config_with_invalid_block_cache_disk_path(
        obj_dir: &std::path::Path,
        bad_disk_path: &str,
    ) -> StorageConfig {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: obj_dir.to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                bad_disk_path.to_string(),
            ))),
        })
    }

    // Note: foyer panics (unwrap inside DirectFsDevice) on invalid disk paths
    // rather than returning an error. We isolate the panic to the create_storage
    // call via tokio::spawn so setup unwrap() failures don't mask regressions.
    #[tokio::test]
    async fn should_fail_when_config_cache_disk_path_is_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        // Use a regular file as disk_path — foyer expects a directory
        let bad_path = tmp.path().join("not-a-dir");
        std::fs::write(&bad_path, b"").unwrap();

        let config = config_with_invalid_block_cache_disk_path(
            &tmp.path().join("obj"),
            bad_path.to_str().unwrap(),
        );

        // Isolate the expected panic to just the build call
        let handle = tokio::spawn(async move {
            let _ = StorageBuilder::new(&config).await.unwrap().build().await;
        });
        let result = handle.await;
        assert!(
            result.is_err() && result.unwrap_err().is_panic(),
            "expected foyer to panic on invalid disk_path"
        );
    }

    #[tokio::test]
    async fn should_fail_reader_when_config_cache_disk_path_is_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        let bad_path = tmp.path().join("not-a-dir");
        std::fs::write(&bad_path, b"").unwrap();

        let slate_config = SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                bad_path.to_str().unwrap().to_string(),
            ))),
        };

        // First open a writer (without cache) so the reader has a manifest
        let writer = StorageBuilder::new(&StorageConfig::SlateDb(SlateDbStorageConfig {
            block_cache: None,
            ..slate_config.clone()
        }))
        .await
        .unwrap()
        .build()
        .await
        .unwrap();
        drop(writer);

        // Isolate the expected panic to just the create_storage_read call
        let handle = tokio::spawn(async move {
            let _ = create_storage_read(
                &StorageConfig::SlateDb(slate_config),
                StorageReaderRuntime::new(),
                StorageSemantics::new(),
                slatedb::config::DbReaderOptions::default(),
            )
            .await;
        });
        let result = handle.await;
        assert!(
            result.is_err() && result.unwrap_err().is_panic(),
            "expected foyer to panic on invalid disk_path for reader"
        );
    }

    #[tokio::test]
    async fn reader_runtime_cache_should_take_precedence_over_config_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let bad_path = tmp.path().join("not-a-dir");
        std::fs::write(&bad_path, b"").unwrap();

        let slate_config = SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp.path().join("obj").to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                bad_path.to_str().unwrap().to_string(),
            ))),
        };

        // First open a writer (without cache) so the reader has a manifest
        let writer = StorageBuilder::new(&StorageConfig::SlateDb(SlateDbStorageConfig {
            block_cache: None,
            ..slate_config.clone()
        }))
        .await
        .unwrap()
        .build()
        .await
        .unwrap();
        drop(writer);

        // Runtime cache should bypass the invalid config cache
        let runtime_cache = FoyerCache::new_with_opts(FoyerCacheOptions {
            max_capacity: 1024 * 1024,
            shards: 1,
        });
        let runtime = StorageReaderRuntime::new().with_block_cache(Arc::new(runtime_cache));

        let result = create_storage_read(
            &StorageConfig::SlateDb(slate_config),
            runtime,
            StorageSemantics::new(),
            slatedb::config::DbReaderOptions::default(),
        )
        .await;

        assert!(
            result.is_ok(),
            "reader runtime cache should take precedence, skipping invalid config cache"
        );
    }

    #[tokio::test]
    async fn should_return_none_when_no_block_cache_configured() {
        let result = create_block_cache_from_config(&None).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_work_without_block_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let config = slatedb_config_with_local_dir(tmp.path());

        let storage = StorageBuilder::new(&config).await.unwrap().build().await;

        assert!(storage.is_ok());
    }
}

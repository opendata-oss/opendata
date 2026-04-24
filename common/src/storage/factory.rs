//! Storage factory for building SlateDB instances from configuration.
//!
//! Exposes [`StorageBuilder`] for writers (returns [`BuiltDb`]) and
//! [`create_storage_read`] for readers (returns [`BuiltDbReader`]). Both carry
//! an optional handle to a foyer hybrid cache that callers must close on
//! shutdown — see the [`BuiltDb`] docs for the contract.

use std::sync::Arc;

use super::config::{BlockCacheConfig, ObjectStoreConfig, StorageConfig};
use super::metrics_recorder::{MetricsRsRecorder, MixtricsBridge as MetricsRsRegistry};
use super::{StorageError, StorageResult};
use slatedb::config::{ScanOptions, Settings};
pub use slatedb::db_cache::CachedEntry;
pub use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
pub use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::{CachedKey, DbCache};
use slatedb::object_store::{self, ObjectStore};
pub use slatedb::{CompactorBuilder, DbBuilder};
use slatedb::{Db, DbReader, IterationOrder, MergeOperator};
use tracing::info;

/// Handle to a foyer hybrid cache that we own and must close explicitly on
/// shutdown. Cloneable because foyer's `HybridCache` is Arc-backed.
///
/// TODO(slatedb 0.13): remove this and the surrounding plumbing. SlateDB 0.13
/// adds a `close()` hook to the `DbCache` trait and drives cache shutdown from
/// `Db::close()` / `DbReader::close()`, so callers won't need to hold a side
/// handle to the hybrid cache just to close it deterministically.
pub type OwnedHybridCache = foyer::HybridCache<CachedKey, CachedEntry>;

/// Block cache we constructed internally — keep the `HybridCache` handle so
/// we can close it from the owner after SlateDB is closed, instead of relying
/// on foyer's Drop-based close, which races the runtime shutdown.
struct ManagedBlockCache {
    db_cache: Arc<dyn DbCache>,
    hybrid: OwnedHybridCache,
}

/// Returns the default scan options used when iterating over a SlateDB
/// database in common callers.
pub fn default_scan_options() -> ScanOptions {
    ScanOptions {
        durability_filter: Default::default(),
        dirty: false,
        read_ahead_bytes: 1024 * 1024,
        cache_blocks: true,
        max_fetch_tasks: 4,
        order: IterationOrder::Ascending,
    }
}

/// Bundle returned by [`StorageBuilder::build`].
///
/// # Shutdown contract
///
/// Callers must close **both** the db and (if present) the managed cache,
/// in that order, during graceful shutdown:
///
/// ```rust,ignore
/// let BuiltDb { db, managed_cache } = StorageBuilder::new(&config).await?.build().await?;
/// // ... use db ...
/// db.close().await?;
/// if let Some(cache) = managed_cache {
///     cache.close().await.ok(); // best-effort, only cache warmth at stake
/// }
/// ```
///
/// The cache close is best-effort: a failure only costs cache warmth, not
/// durability. The close-after-db ordering ensures no inserts race the
/// flusher shutdown.
///
/// TODO(slatedb 0.13): when `DbCache` owns its close lifecycle, this
/// simplifies to just the `Arc<Db>`.
pub struct BuiltDb {
    pub db: Arc<Db>,
    /// The object store slatedb writes to. Exposed so callers can probe the
    /// underlying infrastructure directly — e.g. a readiness endpoint that
    /// must not be short-circuited by slatedb's memtable / block caches.
    pub object_store: Arc<dyn ObjectStore>,
    /// Path prefix under which slatedb stores its data in `object_store`.
    /// Handy for `object_store.list_with_delimiter(Some(&prefix))` style
    /// health probes.
    pub path: String,
    pub managed_cache: Option<OwnedHybridCache>,
}

/// Bundle returned by [`create_storage_read`].
///
/// See [`BuiltDb`] for the shutdown contract — the same applies here with
/// `reader.close()` in place of `db.close()`.
pub struct BuiltDbReader {
    pub reader: Arc<DbReader>,
    pub object_store: Arc<dyn ObjectStore>,
    pub path: String,
    pub managed_cache: Option<OwnedHybridCache>,
}

/// Builder for creating a SlateDB writer instance from a [`StorageConfig`].
///
/// Wraps slatedb's [`DbBuilder`] with opinionated defaults (metrics recorder,
/// config-driven block cache). Use [`map_slatedb`](Self::map_slatedb) as an
/// escape hatch for any low-level knob we don't expose directly.
///
/// # Example
///
/// ```rust,ignore
/// let BuiltDb { db, managed_cache } = StorageBuilder::new(&config).await?
///     .with_merge_operator(Arc::new(MyOp))
///     .build()
///     .await?;
/// ```
pub struct StorageBuilder {
    inner: DbBuilder<String>,
    managed_cache: Option<OwnedHybridCache>,
    object_store: Arc<dyn ObjectStore>,
    path: String,
}

impl StorageBuilder {
    /// Creates a new `StorageBuilder` from a [`StorageConfig`].
    pub async fn new(config: &StorageConfig) -> StorageResult<Self> {
        let object_store = create_object_store(&config.object_store)?;
        Self::from_parts(config, object_store).await
    }

    /// Creates a `StorageBuilder` against a caller-supplied [`ObjectStore`].
    ///
    /// Useful for tests that want to wrap the object store (e.g. a
    /// [`crate::storage::testing::FailingObjectStore`] for fault injection).
    pub async fn from_object_store(
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> StorageResult<Self> {
        let config = StorageConfig {
            path: path.into(),
            ..StorageConfig::default()
        };
        Self::from_parts(&config, object_store).await
    }

    async fn from_parts(
        config: &StorageConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> StorageResult<Self> {
        let settings = match &config.settings_path {
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
            config, settings
        );
        let mut db_builder =
            DbBuilder::new(config.path.clone(), object_store.clone()).with_settings(settings);
        let mut managed_cache: Option<OwnedHybridCache> = None;
        if let Some(managed) = create_block_cache_from_config(&config.block_cache).await? {
            db_builder = db_builder.with_db_cache(managed.db_cache);
            managed_cache = Some(managed.hybrid);
        }
        Ok(Self {
            inner: db_builder,
            managed_cache,
            object_store,
            path: config.path.clone(),
        })
    }

    /// Sets a merge operator on the builder.
    ///
    /// Takes slatedb's native [`MergeOperator`] directly — callers pass their
    /// own impl without an adapter.
    pub fn with_merge_operator(mut self, op: Arc<dyn MergeOperator + Send + Sync>) -> Self {
        self.inner = self.inner.with_merge_operator(op);
        self
    }

    /// Maps over the underlying [`DbBuilder`] for low-level SlateDB configuration.
    ///
    /// This is the escape hatch for any SlateDB knob not exposed by
    /// `StorageBuilder` itself (compactor builder, custom db cache, GC runtime, etc.).
    pub fn map_slatedb(mut self, f: impl FnOnce(DbBuilder<String>) -> DbBuilder<String>) -> Self {
        self.inner = f(self.inner);
        self
    }

    /// Builds the SlateDB database.
    pub async fn build(self) -> StorageResult<BuiltDb> {
        let db_builder = self
            .inner
            .with_metrics_recorder(Arc::new(MetricsRsRecorder));
        let db = db_builder
            .build()
            .await
            .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB: {}", e)))?;
        Ok(BuiltDb {
            db: Arc::new(db),
            object_store: self.object_store,
            path: self.path,
            managed_cache: self.managed_cache,
        })
    }
}

/// Runtime options for read-only storage instances.
///
/// Holds non-serializable runtime configuration for [`DbReader`]: optional
/// merge operator, optional block cache override, and optional object store
/// override.
#[derive(Default, Clone)]
pub struct StorageReaderRuntime {
    pub(crate) block_cache: Option<Arc<dyn DbCache>>,
    pub(crate) object_store: Option<Arc<dyn ObjectStore>>,
    pub(crate) merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>,
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
    pub fn with_block_cache(mut self, cache: Arc<dyn DbCache>) -> Self {
        self.block_cache = Some(cache);
        self
    }

    /// Sets a caller-supplied object store, overriding what would be derived
    /// from [`StorageConfig::object_store`].
    pub fn with_object_store(mut self, object_store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }

    /// Sets a merge operator on the reader. Required when the database was
    /// written with merges that have not yet been fully compacted.
    pub fn with_merge_operator(
        mut self,
        merge_operator: Arc<dyn MergeOperator + Send + Sync>,
    ) -> Self {
        self.merge_operator = Some(merge_operator);
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

/// Creates a read-only SlateDB instance.
///
/// Uses [`DbReader`] under the hood so multiple readers can coexist with a
/// single writer without fencing conflicts.
pub async fn create_storage_read(
    config: &StorageConfig,
    runtime: StorageReaderRuntime,
    reader_options: slatedb::config::DbReaderOptions,
) -> StorageResult<BuiltDbReader> {
    let object_store = if let Some(object_store) = &runtime.object_store {
        object_store.clone()
    } else {
        create_object_store(&config.object_store)?
    };

    let mut builder = DbReader::builder(config.path.clone(), object_store.clone())
        .with_options(reader_options)
        .with_metrics_recorder(Arc::new(MetricsRsRecorder));
    if let Some(op) = runtime.merge_operator {
        builder = builder.with_merge_operator(op);
    }
    // Prefer runtime-provided cache, fall back to config. The
    // runtime-provided cache is owned by the caller, so we don't hold
    // a handle to close it on shutdown.
    let mut managed_cache: Option<OwnedHybridCache> = None;
    if let Some(cache) = runtime.block_cache {
        builder = builder.with_db_cache(cache);
    } else if let Some(managed) = create_block_cache_from_config(&config.block_cache).await? {
        builder = builder.with_db_cache(managed.db_cache);
        managed_cache = Some(managed.hybrid);
    }
    let reader = builder
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB reader: {}", e)))?;
    Ok(BuiltDbReader {
        reader: Arc::new(reader),
        object_store,
        path: config.path.clone(),
        managed_cache,
    })
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
    use crate::storage::config::{FoyerHybridCacheConfig, LocalObjectStoreConfig};

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

    fn config_with_local_dir(dir: &std::path::Path) -> StorageConfig {
        StorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
        }
    }

    #[tokio::test]
    async fn should_create_storage_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("block-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let config = StorageConfig {
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

        let built = StorageBuilder::new(&config).await.unwrap().build().await;

        assert!(built.is_ok(), "expected config-driven block cache to work");
    }

    #[tokio::test]
    async fn should_create_reader_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("block-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let config = StorageConfig {
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
        let writer = StorageBuilder::new(&config)
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        // Close writer before opening reader (SlateDB fencing)
        drop(writer);

        let reader = create_storage_read(
            &config,
            StorageReaderRuntime::new(),
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

    /// Helper: creates a config whose block_cache disk_path is a regular file
    /// (not a directory), which foyer deterministically rejects.
    fn config_with_invalid_block_cache_disk_path(
        obj_dir: &std::path::Path,
        bad_disk_path: &str,
    ) -> StorageConfig {
        StorageConfig {
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
        }
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

        let config_with_cache = StorageConfig {
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
        let writer_config = StorageConfig {
            block_cache: None,
            ..config_with_cache.clone()
        };
        let writer = StorageBuilder::new(&writer_config)
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        drop(writer);

        // Isolate the expected panic to just the create_storage_read call
        let handle = tokio::spawn(async move {
            let _ = create_storage_read(
                &config_with_cache,
                StorageReaderRuntime::new(),
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

        let config_with_cache = StorageConfig {
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
        let writer_config = StorageConfig {
            block_cache: None,
            ..config_with_cache.clone()
        };
        let writer = StorageBuilder::new(&writer_config)
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
            &config_with_cache,
            runtime,
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
        let config = config_with_local_dir(tmp.path());

        let built = StorageBuilder::new(&config).await.unwrap().build().await;

        assert!(built.is_ok());
    }

    #[tokio::test]
    async fn should_build_from_custom_object_store() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let built = StorageBuilder::from_object_store("custom-path", object_store)
            .await
            .unwrap()
            .build()
            .await;
        assert!(built.is_ok());
    }
}

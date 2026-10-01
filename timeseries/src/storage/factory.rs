//! Test-support construction helpers for the native SlateDB backend.
//!
//! Production code opens storage via [`Storage::try_new`] /
//! [`StorageReader::try_new`](super::slate::StorageReader::try_new); this
//! module only hosts the in-memory test storage that replaced the old
//! `InMemoryStorage` backend.

#[cfg(any(test, feature = "testing"))]
use super::slate::Storage;
#[cfg(test)]
use super::slate::StorageReader;

#[cfg(any(test, feature = "testing"))]
fn in_memory_config() -> common::storage::config::SlateDbStorageConfig {
    use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig};
    SlateDbStorageConfig {
        path: "test".to_string(),
        object_store: ObjectStoreConfig::InMemory,
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    }
}

/// Test-only: build a [`Storage`] over a fresh in-memory object store, wired
/// with the OpenTSDB merge operator. Replaces the old `InMemoryStorage` test
/// backend now that timeseries is SlateDB-native. Each call gets an isolated
/// object store, so storages do not share state.
#[cfg(any(test, feature = "testing"))]
pub(crate) async fn in_memory_storage() -> Storage {
    Storage::try_new(&in_memory_config())
        .await
        .expect("in-memory SlateDB storage build failed")
}

/// Test-only: an in-memory writer [`Storage`] plus the shared object store it
/// lives on, so tests can additionally open non-fencing [`StorageReader`]s
/// over the same data.
#[cfg(test)]
pub(crate) struct SharedInMemoryStorage {
    pub(crate) storage: std::sync::Arc<Storage>,
    config: common::storage::config::SlateDbStorageConfig,
    object_store: std::sync::Arc<dyn slatedb::object_store::ObjectStore>,
}

#[cfg(test)]
impl SharedInMemoryStorage {
    /// Opens a read-only `DbReader` handle over the shared object store.
    ///
    /// The reader's view is fixed at open time (plus manifest polling), so
    /// call this after the writer has flushed the data the test expects to
    /// read.
    pub(crate) async fn reader(&self) -> StorageReader {
        StorageReader::try_new_with_object_store(
            &self.config,
            slatedb::config::DbReaderOptions::default(),
            None,
            self.object_store.clone(),
        )
        .await
        .expect("in-memory SlateDB reader build failed")
    }
}

/// Test-only: like [`in_memory_storage`] but keeps a handle to the object
/// store so readers can be opened against the same data.
#[cfg(test)]
pub(crate) async fn in_memory_shared_storage() -> SharedInMemoryStorage {
    use std::sync::Arc;
    let config = in_memory_config();
    let object_store: Arc<dyn slatedb::object_store::ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let storage = Storage::try_new_with_object_store(&config, object_store.clone())
        .await
        .expect("in-memory SlateDB storage build failed");
    SharedInMemoryStorage {
        storage: Arc::new(storage),
        config,
        object_store,
    }
}

/// Test-only: [`Store`](super::slate::Store) implementation wrapping a real
/// in-memory [`Storage`] that fails a selected operation (once) with an
/// injected error, so error propagation can be verified per operation with a
/// distinct error. Reads delegate to the wrapped storage.
#[cfg(test)]
pub(crate) struct FailingStorage {
    inner: std::sync::Arc<Storage>,
    apply_error: std::sync::Mutex<Option<common::StorageError>>,
    snapshot_error: std::sync::Mutex<Option<common::StorageError>>,
    flush_error: std::sync::Mutex<Option<common::StorageError>>,
}

#[cfg(test)]
mod failing_storage {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use common::BytesRange;
    use common::storage::{RecordOp, StorageError, StorageResult, WriteResult};
    use roaring::RoaringBitmap;
    use slatedb::DbIterator;

    use super::FailingStorage;
    use super::in_memory_storage;
    use crate::index::{ForwardIndex, InvertedIndex, SeriesSpec};
    use crate::model::{Label, SeriesFingerprint, SeriesId, TimeBucket};
    use crate::storage::slate::{StorageRead, StorageSnapshot, Store};

    impl FailingStorage {
        pub(crate) async fn wrap_in_memory() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(in_memory_storage().await),
                apply_error: Mutex::new(None),
                snapshot_error: Mutex::new(None),
                flush_error: Mutex::new(None),
            })
        }

        pub(crate) fn fail_apply(&self, e: StorageError) {
            *self.apply_error.lock().unwrap() = Some(e);
        }

        pub(crate) fn fail_snapshot(&self, e: StorageError) {
            *self.snapshot_error.lock().unwrap() = Some(e);
        }

        pub(crate) fn fail_flush(&self, e: StorageError) {
            *self.flush_error.lock().unwrap() = Some(e);
        }
    }

    #[async_trait]
    impl Store for FailingStorage {
        async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult> {
            if let Some(e) = self.apply_error.lock().unwrap().take() {
                return Err(e);
            }
            self.inner.apply(ops).await
        }

        async fn snapshot(&self) -> StorageResult<StorageSnapshot> {
            if let Some(e) = self.snapshot_error.lock().unwrap().take() {
                return Err(e);
            }
            self.inner.snapshot().await
        }

        async fn flush(&self) -> StorageResult<()> {
            if let Some(e) = self.flush_error.lock().unwrap().take() {
                return Err(e);
            }
            self.inner.flush().await
        }
    }

    #[async_trait]
    impl StorageRead for FailingStorage {
        async fn get(&self, key: Bytes) -> StorageResult<Option<Bytes>> {
            self.inner.get(key).await
        }

        async fn scan(&self, range: BytesRange) -> StorageResult<DbIterator> {
            self.inner.scan(range).await
        }

        async fn get_buckets_in_range(
            &self,
            start_secs: Option<i64>,
            end_secs: Option<i64>,
        ) -> crate::util::Result<Vec<TimeBucket>> {
            self.inner.get_buckets_in_range(start_secs, end_secs).await
        }

        async fn get_buckets_for_ranges(
            &self,
            ranges: &[(i64, i64)],
        ) -> crate::util::Result<Vec<TimeBucket>> {
            self.inner.get_buckets_for_ranges(ranges).await
        }

        async fn get_forward_index(&self, bucket: TimeBucket) -> crate::util::Result<ForwardIndex> {
            self.inner.get_forward_index(bucket).await
        }

        async fn get_inverted_index(
            &self,
            bucket: TimeBucket,
        ) -> crate::util::Result<InvertedIndex> {
            self.inner.get_inverted_index(bucket).await
        }

        async fn get_inverted_index_terms(
            &self,
            bucket: &TimeBucket,
            terms: &[Label],
        ) -> crate::util::Result<InvertedIndex> {
            self.inner.get_inverted_index_terms(bucket, terms).await
        }

        async fn get_inverted_index_term(
            &self,
            bucket: &TimeBucket,
            term: &Label,
        ) -> crate::util::Result<Option<RoaringBitmap>> {
            self.inner.get_inverted_index_term(bucket, term).await
        }

        async fn get_forward_index_series(
            &self,
            bucket: &TimeBucket,
            series_ids: &[SeriesId],
        ) -> crate::util::Result<ForwardIndex> {
            self.inner
                .get_forward_index_series(bucket, series_ids)
                .await
        }

        async fn get_forward_index_one(
            &self,
            bucket: &TimeBucket,
            series_id: SeriesId,
        ) -> crate::util::Result<Option<SeriesSpec>> {
            self.inner.get_forward_index_one(bucket, series_id).await
        }

        async fn load_series_dictionary<F>(
            &self,
            bucket: &TimeBucket,
            insert: F,
        ) -> crate::util::Result<u32>
        where
            F: FnMut(SeriesFingerprint, SeriesId) + Send,
        {
            self.inner.load_series_dictionary(bucket, insert).await
        }

        async fn get_label_values(
            &self,
            bucket: &TimeBucket,
            label_name: &str,
        ) -> crate::util::Result<Vec<String>> {
            self.inner.get_label_values(bucket, label_name).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::storage::config::{
        BlockCacheConfig, FoyerHybridCacheConfig, LocalObjectStoreConfig, ObjectStoreConfig,
        SlateDbStorageConfig,
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

    fn slatedb_config_with_local_dir(dir: &std::path::Path) -> SlateDbStorageConfig {
        SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }
    }

    #[tokio::test]
    async fn should_create_storage_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cache_dir = tmp.path().join("block-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let config = SlateDbStorageConfig {
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
            meta_cache: None,
        };

        let storage = Storage::try_new(&config).await;
        assert!(
            storage.is_ok(),
            "expected config-driven block cache to work"
        );
    }

    #[tokio::test]
    async fn should_create_reader_with_block_cache_from_config() {
        let tmp = tempfile::tempdir().unwrap();
        let obj_path = tmp.path().join("obj").to_str().unwrap().to_string();

        // Writer and reader are distinct instances (in production, distinct
        // processes), so they use separate local block-cache directories.
        let writer_cache = tmp.path().join("writer-cache");
        let reader_cache = tmp.path().join("reader-cache");
        std::fs::create_dir_all(&writer_cache).unwrap();
        std::fs::create_dir_all(&reader_cache).unwrap();

        let with_cache = |cache_dir: &std::path::Path| SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: obj_path.clone(),
            }),
            settings_path: None,
            block_cache: Some(BlockCacheConfig::FoyerHybrid(foyer_cache_config(
                1024 * 1024,
                4 * 1024 * 1024,
                cache_dir.to_str().unwrap().to_string(),
            ))),
            meta_cache: None,
        };

        // Open a writer first so the reader has a manifest to read, then drop it
        // (SlateDB fencing) before opening the reader.
        let writer = Storage::try_new(&with_cache(&writer_cache)).await.unwrap();
        drop(writer);

        let reader = StorageReader::try_new(
            &with_cache(&reader_cache),
            slatedb::config::DbReaderOptions::default(),
            None,
        )
        .await;
        assert!(
            reader.is_ok(),
            "expected config-driven block cache on reader to work"
        );
    }

    #[tokio::test]
    async fn should_work_without_block_cache() {
        let tmp = tempfile::tempdir().unwrap();
        let config = slatedb_config_with_local_dir(tmp.path());
        let storage = Storage::try_new(&config).await;
        assert!(storage.is_ok());
        storage.unwrap().close().await.unwrap();
    }

    /// A SlateDb config whose block-cache `disk_path` is a regular file (not a
    /// directory), which foyer deterministically rejects.
    fn config_with_invalid_block_cache_disk_path(
        obj_dir: &std::path::Path,
        bad_disk_path: &str,
    ) -> SlateDbStorageConfig {
        SlateDbStorageConfig {
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
            meta_cache: None,
        }
    }

    // Confirms `Storage::try_new` surfaces a block-cache build failure rather
    // than silently succeeding. foyer panics (an unwrap inside the device
    // builder) on an invalid disk_path rather than returning an error, so the
    // failing call is isolated to a spawned task to keep the panic from
    // aborting the test. This exercises timeseries' construction wiring; the
    // cache builder itself is unit-tested in `common::storage::factory`.
    #[tokio::test]
    async fn should_fail_when_config_cache_disk_path_is_invalid() {
        let tmp = tempfile::tempdir().unwrap();
        // Use a regular file as disk_path — foyer expects a directory.
        let bad_path = tmp.path().join("not-a-dir");
        std::fs::write(&bad_path, b"").unwrap();

        let config = config_with_invalid_block_cache_disk_path(
            &tmp.path().join("obj"),
            bad_path.to_str().unwrap(),
        );

        let handle = tokio::spawn(async move {
            let _ = Storage::try_new(&config).await;
        });
        let result = handle.await;
        assert!(
            result.is_err() && result.unwrap_err().is_panic(),
            "expected foyer to panic on invalid disk_path"
        );
    }
}

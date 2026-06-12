//! Direct slatedb access for LogDb's count path.
//!
//! Mirrors slatedb's own writer/reader split: [`LogDirect`] is a
//! read-only handle that exposes a manifest snapshot and an `SstReader`
//! pinned to the same path + object store. Constructed independently of
//! `SlateDbStorage` so its block cache and `DbReader` polling interval
//! can be tuned without affecting the writer.
//!
//! LogDb's count flow uses [`LogDirect`] when present to walk persisted
//! SSTs via [`common::storage::sst_blocks::count_in_range`]; without it,
//! count falls back to a plain scan.

use std::sync::Arc;

use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig, StorageConfig};
use common::storage::factory::create_object_store;
use common::{BytesRange, StorageError, StorageResult, storage::sst_blocks};
use slatedb::DbReader;
use slatedb::SstReader;
use slatedb::config::DbReaderOptions;
use slatedb::db_cache::DbCache;

/// LogDb's slatedb-direct read handle.
///
/// Holds a `DbReader` (for live manifest snapshots) plus an `SstReader`
/// (for SST-level reads). Both are independent of the writer's `Db` —
/// they can use a separate block cache and polling interval.
pub(crate) struct LogDirect {
    reader: Arc<DbReader>,
    sst_reader: SstReader,
}

impl LogDirect {
    /// Builds a `LogDirect` backed by a fresh `DbReader` against the same
    /// path/object store. The optional block cache is used for SST block
    /// lookups; `None` is permitted but reads will hit object storage on
    /// every block fetch.
    pub(crate) async fn from_config(
        config: &SlateDbStorageConfig,
        block_cache: Option<Arc<dyn DbCache>>,
    ) -> StorageResult<Self> {
        let object_store = create_object_store(&config.object_store)?;
        let mut builder = DbReader::builder(config.path.clone(), object_store.clone())
            .with_options(DbReaderOptions::default());
        if let Some(cache) = block_cache.clone() {
            builder = builder.with_db_cache(cache);
        }
        let reader = builder.build().await.map_err(|e| {
            StorageError::Storage(format!("Failed to create LogDirect DbReader: {}", e))
        })?;
        let sst_reader = SstReader::new(config.path.clone(), object_store, block_cache, None);
        Ok(Self {
            reader: Arc::new(reader),
            sst_reader,
        })
    }

    /// Builds a `LogDirect` only when `config` is slatedb-backed by a
    /// persistent object store. Returns `Ok(None)` for in-memory configs
    /// and for slatedb configs whose object store is `InMemory` —
    /// `InMemory` instances are process-local, so a fresh one built here
    /// would never see the writer's data (and `DbReader::build` would
    /// fail outright on the empty store). Count falls back to scanning
    /// the writer's storage in that case.
    pub(crate) async fn maybe_from_storage_config(
        config: &StorageConfig,
    ) -> StorageResult<Option<Self>> {
        match config {
            StorageConfig::SlateDb(slate) => match slate.object_store {
                ObjectStoreConfig::InMemory => Ok(None),
                _ => Ok(Some(Self::from_config(slate, None).await?)),
            },
            StorageConfig::InMemory => Ok(None),
        }
    }

    /// Test helper: build from already-constructed slatedb components, so the
    /// caller can share the underlying object store with another handle.
    #[cfg(test)]
    pub(crate) fn from_components(reader: Arc<DbReader>, sst_reader: SstReader) -> Self {
        Self { reader, sst_reader }
    }

    /// Counts records in `range` by walking the manifest's persisted
    /// SSTs. Returns counts plus a witness key for the highest counted
    /// entry; callers wanting writes still in the memtable should scan
    /// `(covered_to, range.end)` themselves.
    pub(crate) async fn count_in_range(
        &self,
        range: &BytesRange,
    ) -> StorageResult<sst_blocks::CountResult> {
        sst_blocks::count_in_range(&self.reader.manifest(), &self.sst_reader, range).await
    }
}

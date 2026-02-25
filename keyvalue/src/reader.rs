//! Read-only key-value access and the [`KeyValueRead`] trait.

use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common::storage::factory::create_storage_read;
use common::{StorageRead, StorageReaderRuntime, StorageSemantics};

use crate::config::Config;
use crate::error::Result;
use crate::model::KeyValueEntry;
use crate::storage::{KeyValueScanIterator, KeyValueStorageRead};

/// Trait for read operations on the key-value store.
///
/// This trait defines the common read interface shared by both
/// [`KeyValueDb`](crate::KeyValueDb) and [`KeyValueDbReader`].
#[async_trait]
pub trait KeyValueRead {
    /// Gets the value for a key, or None if not found.
    async fn get(&self, key: Bytes) -> Result<Option<Bytes>>;

    /// Scans key-value pairs in lexicographic key order.
    ///
    /// Returns an iterator over entries whose keys fall within the range.
    /// Pass `..` to scan all entries.
    async fn scan(&self, key_range: impl RangeBounds<Bytes> + Send) -> Result<KeyValueIterator>;
}

/// A read-only view of the key-value store.
///
/// `KeyValueDbReader` provides access to all read operations via the
/// [`KeyValueRead`] trait, but not write operations.
pub struct KeyValueDbReader {
    storage: KeyValueStorageRead,
}

impl KeyValueDbReader {
    /// Opens a read-only view of the key-value store.
    pub async fn open(config: Config) -> Result<Self> {
        let storage: Arc<dyn StorageRead> = create_storage_read(
            &config.storage,
            StorageReaderRuntime::new(),
            StorageSemantics::new(),
            slatedb::config::DbReaderOptions::default(),
        )
        .await?;
        let kv_storage = KeyValueStorageRead::new(storage);
        Ok(Self {
            storage: kv_storage,
        })
    }

    /// Creates a KeyValueDbReader from an existing storage implementation.
    #[cfg(test)]
    pub(crate) fn new(storage: Arc<dyn StorageRead>) -> Self {
        Self {
            storage: KeyValueStorageRead::new(storage),
        }
    }
}

#[async_trait]
impl KeyValueRead for KeyValueDbReader {
    async fn get(&self, key: Bytes) -> Result<Option<Bytes>> {
        self.storage.get(&key).await
    }

    async fn scan(&self, key_range: impl RangeBounds<Bytes> + Send) -> Result<KeyValueIterator> {
        let inner = self.storage.scan(key_range).await?;
        Ok(KeyValueIterator::new(inner))
    }
}

/// Iterator over key-value pairs.
pub struct KeyValueIterator {
    pub(crate) inner: KeyValueScanIterator,
}

impl KeyValueIterator {
    /// Creates a new iterator from the internal scan iterator.
    pub(crate) fn new(inner: KeyValueScanIterator) -> Self {
        Self { inner }
    }

    /// Returns the next entry, or None if iteration is complete.
    pub async fn next(&mut self) -> Result<Option<KeyValueEntry>> {
        self.inner.next().await
    }
}

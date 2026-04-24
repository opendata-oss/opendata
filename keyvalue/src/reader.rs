//! Read-only key-value access and the [`KeyValueRead`] trait.

use std::ops::RangeBounds;

use async_trait::async_trait;
use bytes::Bytes;
use common::{StorageReaderRuntime, create_storage_read};
use slatedb::DbReader;

use crate::config::Config;
use crate::error::Result;
use crate::model::KeyValueEntry;
use crate::storage::{KeyValueScanIterator, KeyValueStorageRead};

#[cfg(test)]
use slatedb::Db;
#[cfg(test)]
use std::sync::Arc;

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

/// Underlying read source for [`KeyValueDbReader`].
///
/// `DbRead` is not object-safe, so we can't hold an `Arc<dyn DbRead>`. Instead
/// we enumerate the concrete sources we actually support.
enum ReaderSource {
    Reader(KeyValueStorageRead<DbReader>),
    #[cfg(test)]
    Db(KeyValueStorageRead<Db>),
}

impl ReaderSource {
    async fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        match self {
            ReaderSource::Reader(r) => r.get(key).await,
            #[cfg(test)]
            ReaderSource::Db(r) => r.get(key).await,
        }
    }

    async fn scan(&self, range: impl RangeBounds<Bytes>) -> Result<KeyValueScanIterator> {
        match self {
            ReaderSource::Reader(r) => r.scan(range).await,
            #[cfg(test)]
            ReaderSource::Db(r) => r.scan(range).await,
        }
    }
}

/// A read-only view of the key-value store.
///
/// `KeyValueDbReader` provides access to all read operations via the
/// [`KeyValueRead`] trait, but not write operations.
pub struct KeyValueDbReader {
    storage: ReaderSource,
}

impl KeyValueDbReader {
    /// Opens a read-only view of the key-value store.
    pub async fn open(config: Config) -> Result<Self> {
        let built = create_storage_read(
            &config.storage,
            StorageReaderRuntime::new(),
            slatedb::config::DbReaderOptions::default(),
        )
        .await?;
        Ok(Self {
            storage: ReaderSource::Reader(KeyValueStorageRead::new(built.reader)),
        })
    }

    /// Creates a KeyValueDbReader from an existing SlateDB instance.
    ///
    /// Useful in tests where a writer and reader share the same `Db` against an
    /// in-memory object store.
    #[cfg(test)]
    pub(crate) fn from_db(db: Arc<Db>) -> Self {
        Self {
            storage: ReaderSource::Db(KeyValueStorageRead::new(db)),
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

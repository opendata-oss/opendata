//! KeyValue-specific storage wrappers.
//!
//! This module provides [`KeyValueStorage`] and [`KeyValueStorageRead`] which wrap
//! the underlying storage traits with key encoding/decoding.

use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use common::storage::RecordOp;
use common::{Record, Storage, StorageIterator, StorageRead};

use crate::config::WriteOptions;
use crate::error::{Error, Result};
use crate::model::KeyValueEntry;
use crate::serde::{decode_key, encode_key, encode_key_range};

/// Read-only key-value storage operations.
///
/// Wraps `Arc<dyn StorageRead>` with key encoding/decoding.
#[derive(Clone)]
pub(crate) struct KeyValueStorageRead {
    storage: Arc<dyn StorageRead>,
}

impl KeyValueStorageRead {
    /// Creates a new read-only storage wrapper.
    pub(crate) fn new(storage: Arc<dyn StorageRead>) -> Self {
        Self { storage }
    }

    /// Gets a value by user key.
    pub(crate) async fn get(&self, key: &Bytes) -> Result<Option<Bytes>> {
        let storage_key = encode_key(key);
        let record = self
            .storage
            .get(storage_key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(record.map(|r| r.value))
    }

    /// Scans key-value pairs within a user key range.
    pub(crate) async fn scan(
        &self,
        key_range: impl RangeBounds<Bytes>,
    ) -> Result<KeyValueScanIterator> {
        let storage_range = encode_key_range(key_range);
        let inner = self
            .storage
            .scan_iter(storage_range)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(KeyValueScanIterator { inner })
    }
}

/// Iterator over key-value pairs from storage.
pub(crate) struct KeyValueScanIterator {
    inner: Box<dyn StorageIterator + Send>,
}

impl KeyValueScanIterator {
    /// Returns the next entry, or None if iteration is complete.
    pub(crate) async fn next(&mut self) -> Result<Option<KeyValueEntry>> {
        let record = self
            .inner
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        match record {
            Some(r) => {
                let user_key = decode_key(&r.key)?;
                Ok(Some(KeyValueEntry {
                    key: user_key,
                    value: r.value,
                }))
            }
            None => Ok(None),
        }
    }
}

/// Read-write key-value storage operations.
///
/// Wraps `Arc<dyn Storage>` with key encoding/decoding.
#[derive(Clone)]
pub(crate) struct KeyValueStorage {
    storage: Arc<dyn Storage>,
}

impl KeyValueStorage {
    /// Creates a new storage wrapper.
    pub(crate) fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// Creates a new storage with an in-memory backend.
    #[cfg(test)]
    pub(crate) fn in_memory() -> Self {
        use common::storage::in_memory::InMemoryStorage;
        Self::new(Arc::new(InMemoryStorage::new()))
    }

    /// Returns a read-only view of this storage.
    pub(crate) fn as_read(&self) -> KeyValueStorageRead {
        KeyValueStorageRead::new(Arc::clone(&self.storage) as Arc<dyn StorageRead>)
    }

    /// Puts a key-value pair.
    pub(crate) async fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.put_with_options(key, value, WriteOptions::default())
            .await
    }

    /// Puts a key-value pair with custom options.
    pub(crate) async fn put_with_options(
        &self,
        key: Bytes,
        value: Bytes,
        options: WriteOptions,
    ) -> Result<()> {
        let storage_key = encode_key(&key);
        let record = Record::new(storage_key, value);
        let storage_options = common::WriteOptions {
            await_durable: options.await_durable,
        };
        self.storage
            .put_with_options(vec![record.into()], storage_options)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Deletes a key. No-op if key does not exist.
    pub(crate) async fn delete(&self, key: Bytes) -> Result<()> {
        self.delete_with_options(key, WriteOptions::default()).await
    }

    /// Deletes a key with custom options.
    pub(crate) async fn delete_with_options(
        &self,
        key: Bytes,
        options: WriteOptions,
    ) -> Result<()> {
        let storage_key = encode_key(&key);
        let op = RecordOp::Delete(storage_key);

        // Note: common::Storage::apply doesn't have an options variant,
        // so we flush after if await_durable is requested.
        self.storage
            .apply(vec![op])
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        if options.await_durable {
            self.flush().await?;
        }
        Ok(())
    }

    /// Flushes pending writes to durable storage.
    pub(crate) async fn flush(&self) -> Result<()> {
        self.storage
            .flush()
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Closes the storage, releasing resources.
    pub(crate) async fn close(&self) -> Result<()> {
        self.storage
            .close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_put_and_get_value() {
        // given
        let storage = KeyValueStorage::in_memory();
        let key = Bytes::from("test-key");
        let value = Bytes::from("test-value");

        // when
        storage.put(key.clone(), value.clone()).await.unwrap();
        let result = storage.as_read().get(&key).await.unwrap();

        // then
        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn should_return_none_for_missing_key() {
        // given
        let storage = KeyValueStorage::in_memory();

        // when
        let result = storage
            .as_read()
            .get(&Bytes::from("missing"))
            .await
            .unwrap();

        // then
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_delete_existing_key() {
        // given
        let storage = KeyValueStorage::in_memory();
        let key = Bytes::from("to-delete");
        storage
            .put(key.clone(), Bytes::from("value"))
            .await
            .unwrap();

        // when
        storage.delete(key.clone()).await.unwrap();
        let result = storage.as_read().get(&key).await.unwrap();

        // then
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_delete_nonexistent_key_without_error() {
        // given
        let storage = KeyValueStorage::in_memory();

        // when
        let result = storage.delete(Bytes::from("nonexistent")).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_scan_all_entries() {
        // given
        let storage = KeyValueStorage::in_memory();
        storage
            .put(Bytes::from("a"), Bytes::from("1"))
            .await
            .unwrap();
        storage
            .put(Bytes::from("b"), Bytes::from("2"))
            .await
            .unwrap();
        storage
            .put(Bytes::from("c"), Bytes::from("3"))
            .await
            .unwrap();

        // when
        let mut iter = storage.as_read().scan(..).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, Bytes::from("a"));
        assert_eq!(entries[1].key, Bytes::from("b"));
        assert_eq!(entries[2].key, Bytes::from("c"));
    }

    #[tokio::test]
    async fn should_scan_key_range() {
        // given
        let storage = KeyValueStorage::in_memory();
        storage
            .put(Bytes::from("a"), Bytes::from("1"))
            .await
            .unwrap();
        storage
            .put(Bytes::from("b"), Bytes::from("2"))
            .await
            .unwrap();
        storage
            .put(Bytes::from("c"), Bytes::from("3"))
            .await
            .unwrap();
        storage
            .put(Bytes::from("d"), Bytes::from("4"))
            .await
            .unwrap();

        // when - scan b..d (exclusive end)
        let mut iter = storage
            .as_read()
            .scan(Bytes::from("b")..Bytes::from("d"))
            .await
            .unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, Bytes::from("b"));
        assert_eq!(entries[1].key, Bytes::from("c"));
    }

    #[tokio::test]
    async fn should_overwrite_existing_key() {
        // given
        let storage = KeyValueStorage::in_memory();
        let key = Bytes::from("key");
        storage.put(key.clone(), Bytes::from("old")).await.unwrap();

        // when
        storage.put(key.clone(), Bytes::from("new")).await.unwrap();
        let result = storage.as_read().get(&key).await.unwrap();

        // then
        assert_eq!(result, Some(Bytes::from("new")));
    }
}

//! Core KeyValueDb implementation with read and write APIs.

use std::ops::RangeBounds;

use async_trait::async_trait;
use bytes::Bytes;
use common::storage::factory::create_storage;
use common::{StorageRuntime, StorageSemantics};

use crate::config::{Config, WriteOptions};
use crate::error::{Error, Result};
use crate::reader::{KeyValueIterator, KeyValueRead};
use crate::storage::KeyValueStorage;

/// The main key-value interface providing read and write operations.
///
/// `KeyValueDb` is the primary entry point for interacting with the key-value
/// store. It provides methods to put, get, delete, and scan key-value pairs.
///
/// # Read Operations
///
/// Read operations are provided via the [`KeyValueRead`] trait, which `KeyValueDb`
/// implements. This allows generic code to work with either `KeyValueDb` or
/// [`KeyValueDbReader`](crate::KeyValueDbReader).
///
/// # Thread Safety
///
/// `KeyValueDb` is designed to be shared across threads. All methods take `&self`
/// and internal synchronization is handled automatically.
///
/// # Example
///
/// ```ignore
/// use keyvalue::{KeyValueDb, KeyValueRead, Config};
/// use bytes::Bytes;
///
/// let kv = KeyValueDb::open(config).await?;
///
/// // Write data
/// kv.put(Bytes::from("user:123"), Bytes::from("alice")).await?;
///
/// // Read data
/// let value = kv.get(Bytes::from("user:123")).await?;
/// assert_eq!(value, Some(Bytes::from("alice")));
///
/// // Delete data
/// kv.delete(Bytes::from("user:123")).await?;
/// ```
pub struct KeyValueDb {
    storage: KeyValueStorage,
}

impl KeyValueDb {
    /// Opens or creates a key-value store with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying storage backend and settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    pub async fn open(config: Config) -> Result<Self> {
        let storage = create_storage(
            &config.storage,
            StorageRuntime::new(),
            StorageSemantics::new(),
        )
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;

        let kv_storage = KeyValueStorage::new(storage);
        Ok(Self {
            storage: kv_storage,
        })
    }

    /// Puts a key-value pair, overwriting any existing value.
    ///
    /// This method uses default write options. Use [`put_with_options`] for
    /// custom durability settings.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store.
    /// * `value` - The value to associate with the key.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails due to storage issues.
    ///
    /// [`put_with_options`]: KeyValueDb::put_with_options
    pub async fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        self.storage.put(key, value).await
    }

    /// Puts a key-value pair with custom options.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store.
    /// * `value` - The value to associate with the key.
    /// * `options` - Write options controlling durability behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails due to storage issues.
    pub async fn put_with_options(
        &self,
        key: Bytes,
        value: Bytes,
        options: WriteOptions,
    ) -> Result<()> {
        self.storage.put_with_options(key, value, options).await
    }

    /// Deletes a key. No-op if key does not exist.
    ///
    /// This method uses default write options. Use [`delete_with_options`] for
    /// custom durability settings.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails due to storage issues.
    ///
    /// [`delete_with_options`]: KeyValueDb::delete_with_options
    pub async fn delete(&self, key: Bytes) -> Result<()> {
        self.storage.delete(key).await
    }

    /// Deletes a key with custom options.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete.
    /// * `options` - Write options controlling durability behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails due to storage issues.
    pub async fn delete_with_options(&self, key: Bytes, options: WriteOptions) -> Result<()> {
        self.storage.delete_with_options(key, options).await
    }

    /// Flushes pending writes to durable storage.
    ///
    /// This ensures that all writes that have been acknowledged are persisted
    /// to durable storage.
    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await
    }

    /// Closes the store, releasing resources.
    ///
    /// This method should be called before dropping the store to ensure
    /// proper cleanup.
    pub async fn close(self) -> Result<()> {
        self.storage.close().await
    }

    /// Creates a KeyValueDb from an existing storage implementation.
    #[cfg(test)]
    pub(crate) fn new(storage: std::sync::Arc<dyn common::Storage>) -> Self {
        Self {
            storage: KeyValueStorage::new(storage),
        }
    }
}

#[async_trait]
impl KeyValueRead for KeyValueDb {
    async fn get(&self, key: Bytes) -> Result<Option<Bytes>> {
        self.storage.as_read().get(&key).await
    }

    async fn scan(&self, key_range: impl RangeBounds<Bytes> + Send) -> Result<KeyValueIterator> {
        let inner = self.storage.as_read().scan(key_range).await?;
        Ok(KeyValueIterator::new(inner))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::StorageConfig;
    use common::storage::factory::create_storage;

    use super::*;
    use crate::reader::KeyValueDbReader;

    fn test_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
        }
    }

    #[tokio::test]
    async fn should_open_keyvalue_with_in_memory_config() {
        // given
        let config = test_config();

        // when
        let result = KeyValueDb::open(config).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_put_and_get_single_key() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        let key = Bytes::from("user:123");
        let value = Bytes::from("alice");

        // when
        kv.put(key.clone(), value.clone()).await.unwrap();
        let result = kv.get(key).await.unwrap();

        // then
        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn should_put_and_get_multiple_keys() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();

        // when
        kv.put(Bytes::from("key1"), Bytes::from("value1"))
            .await
            .unwrap();
        kv.put(Bytes::from("key2"), Bytes::from("value2"))
            .await
            .unwrap();
        kv.put(Bytes::from("key3"), Bytes::from("value3"))
            .await
            .unwrap();

        // then
        assert_eq!(
            kv.get(Bytes::from("key1")).await.unwrap(),
            Some(Bytes::from("value1"))
        );
        assert_eq!(
            kv.get(Bytes::from("key2")).await.unwrap(),
            Some(Bytes::from("value2"))
        );
        assert_eq!(
            kv.get(Bytes::from("key3")).await.unwrap(),
            Some(Bytes::from("value3"))
        );
    }

    #[tokio::test]
    async fn should_return_none_for_missing_key() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        kv.put(Bytes::from("existing"), Bytes::from("value"))
            .await
            .unwrap();

        // when
        let result = kv.get(Bytes::from("missing")).await.unwrap();

        // then
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_delete_existing_key() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        let key = Bytes::from("to-delete");
        kv.put(key.clone(), Bytes::from("value")).await.unwrap();

        // when
        kv.delete(key.clone()).await.unwrap();
        let result = kv.get(key).await.unwrap();

        // then
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_delete_nonexistent_key_without_error() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();

        // when
        let result = kv.delete(Bytes::from("nonexistent")).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_overwrite_existing_key() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        let key = Bytes::from("key");
        kv.put(key.clone(), Bytes::from("old-value")).await.unwrap();

        // when
        kv.put(key.clone(), Bytes::from("new-value")).await.unwrap();
        let result = kv.get(key).await.unwrap();

        // then
        assert_eq!(result, Some(Bytes::from("new-value")));
    }

    #[tokio::test]
    async fn should_scan_all_entries() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        kv.put(Bytes::from("a"), Bytes::from("1")).await.unwrap();
        kv.put(Bytes::from("b"), Bytes::from("2")).await.unwrap();
        kv.put(Bytes::from("c"), Bytes::from("3")).await.unwrap();

        // when
        let mut iter = kv.scan(..).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, Bytes::from("a"));
        assert_eq!(entries[0].value, Bytes::from("1"));
        assert_eq!(entries[1].key, Bytes::from("b"));
        assert_eq!(entries[1].value, Bytes::from("2"));
        assert_eq!(entries[2].key, Bytes::from("c"));
        assert_eq!(entries[2].value, Bytes::from("3"));
    }

    #[tokio::test]
    async fn should_scan_key_range() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        kv.put(Bytes::from("a"), Bytes::from("1")).await.unwrap();
        kv.put(Bytes::from("b"), Bytes::from("2")).await.unwrap();
        kv.put(Bytes::from("c"), Bytes::from("3")).await.unwrap();
        kv.put(Bytes::from("d"), Bytes::from("4")).await.unwrap();

        // when - scan b..d (exclusive end)
        let mut iter = kv.scan(Bytes::from("b")..Bytes::from("d")).await.unwrap();
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
    async fn should_scan_from_starting_key() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        kv.put(Bytes::from("a"), Bytes::from("1")).await.unwrap();
        kv.put(Bytes::from("b"), Bytes::from("2")).await.unwrap();
        kv.put(Bytes::from("c"), Bytes::from("3")).await.unwrap();

        // when - scan from b onwards
        let mut iter = kv.scan(Bytes::from("b")..).await.unwrap();
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
    async fn should_scan_up_to_ending_key() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        kv.put(Bytes::from("a"), Bytes::from("1")).await.unwrap();
        kv.put(Bytes::from("b"), Bytes::from("2")).await.unwrap();
        kv.put(Bytes::from("c"), Bytes::from("3")).await.unwrap();

        // when - scan up to c (exclusive)
        let mut iter = kv.scan(..Bytes::from("c")).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, Bytes::from("a"));
        assert_eq!(entries[1].key, Bytes::from("b"));
    }

    #[tokio::test]
    async fn should_return_empty_iterator_for_empty_range() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        kv.put(Bytes::from("a"), Bytes::from("1")).await.unwrap();
        kv.put(Bytes::from("b"), Bytes::from("2")).await.unwrap();

        // when - scan range that doesn't include any keys
        let mut iter = kv.scan(Bytes::from("x")..Bytes::from("z")).await.unwrap();
        let entry = iter.next().await.unwrap();

        // then
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn should_read_via_keyvalue_reader() {
        // given - create shared storage
        let storage = create_storage(
            &StorageConfig::InMemory,
            StorageRuntime::new(),
            StorageSemantics::new(),
        )
        .await
        .unwrap();
        let kv = KeyValueDb::new(storage.clone());
        kv.put(Bytes::from("key1"), Bytes::from("value1"))
            .await
            .unwrap();
        kv.put(Bytes::from("key2"), Bytes::from("value2"))
            .await
            .unwrap();

        // when - create KeyValueDbReader sharing the same storage
        let reader = KeyValueDbReader::new(storage as Arc<dyn common::StorageRead>);
        let result1 = reader.get(Bytes::from("key1")).await.unwrap();
        let result2 = reader.get(Bytes::from("key2")).await.unwrap();

        // then
        assert_eq!(result1, Some(Bytes::from("value1")));
        assert_eq!(result2, Some(Bytes::from("value2")));
    }

    #[tokio::test]
    async fn should_scan_via_keyvalue_reader() {
        // given - create shared storage
        let storage = create_storage(
            &StorageConfig::InMemory,
            StorageRuntime::new(),
            StorageSemantics::new(),
        )
        .await
        .unwrap();
        let kv = KeyValueDb::new(storage.clone());
        kv.put(Bytes::from("a"), Bytes::from("1")).await.unwrap();
        kv.put(Bytes::from("b"), Bytes::from("2")).await.unwrap();
        kv.put(Bytes::from("c"), Bytes::from("3")).await.unwrap();

        // when - create KeyValueDbReader sharing the same storage
        let reader = KeyValueDbReader::new(storage as Arc<dyn common::StorageRead>);
        let mut iter = reader.scan(..).await.unwrap();
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
    async fn should_put_with_await_durable_option() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        let options = WriteOptions {
            await_durable: true,
        };

        // when
        kv.put_with_options(Bytes::from("key"), Bytes::from("value"), options)
            .await
            .unwrap();

        // then
        let result = kv.get(Bytes::from("key")).await.unwrap();
        assert_eq!(result, Some(Bytes::from("value")));
    }

    #[tokio::test]
    async fn should_delete_with_await_durable_option() {
        // given
        let kv = KeyValueDb::open(test_config()).await.unwrap();
        kv.put(Bytes::from("key"), Bytes::from("value"))
            .await
            .unwrap();
        let options = WriteOptions {
            await_durable: true,
        };

        // when
        kv.delete_with_options(Bytes::from("key"), options)
            .await
            .unwrap();

        // then
        let result = kv.get(Bytes::from("key")).await.unwrap();
        assert!(result.is_none());
    }
}

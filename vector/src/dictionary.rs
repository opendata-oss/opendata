//! Vector database dictionary for external ID to internal ID mappings.
//!
//! This module provides the dictionary component for the vector database,
//! mapping external vector IDs (arbitrary byte keys) to internal u64 vector IDs.
//!
//! ## Usage
//!
//! ```ignore
//! use vector::dictionary::Dictionary;
//!
//! // Create dictionary - sequence allocation is handled internally
//! let dictionary = Dictionary::new(storage).await?;
//!
//! // Upsert a vector ID mapping (insert or update)
//! let external_id = Bytes::from("vec-user-123");
//! let (internal_id, old_id) = dictionary.upsert(external_id.clone()).await?;
//!
//! // Lookup existing vector ID
//! let found = dictionary.lookup(external_id).await?;
//! assert_eq!(found, Some(internal_id));
//! ```

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};

use crate::serde::key::SeqBlockKey;
use common::Record;
use common::sequence::{SeqBlockStore, SequenceAllocator};
use common::serde::encoding::{
    decode_u64 as decode_u64_encoding, encode_u64 as encode_u64_encoding,
};
use common::storage::{RecordOp, Storage};

/// Vector database dictionary for external ID to internal ID mappings.
///
/// The Dictionary maintains the mapping between external vector IDs (arbitrary
/// byte strings) and internal u64 vector IDs. It uses a `SequenceAllocator`
/// for monotonic internal ID generation.
///
/// ## Thread Safety
///
/// `Dictionary` is thread-safe and can be shared across tasks via `Arc`.
/// The `upsert` operation uses an RwLock to ensure atomic check-then-act
/// semantics, preventing race conditions when multiple tasks upsert the
/// same key concurrently.
///
/// ## Value Encoding
///
/// Values are stored as 8-byte little-endian u64 integers with no header or
/// versioning. This matches the existing `IdDictionaryValue` format used in
/// the vector database.
pub struct Dictionary {
    storage: Arc<dyn Storage>,
    id_allocator: Arc<SequenceAllocator>,
    lock: tokio::sync::RwLock<()>,
}

impl Dictionary {
    /// Creates a new Dictionary.
    ///
    /// Initializes the dictionary with the given storage backend. The sequence
    /// allocator for internal IDs is created and initialized automatically using
    /// the vector database's SeqBlockKey.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend for persisting mappings
    ///
    /// # Errors
    ///
    /// Returns an error if sequence allocator initialization fails.
    pub async fn new(storage: Arc<dyn Storage>) -> Result<Self> {
        // Create sequence allocator using vector's SeqBlockKey
        let seq_key = SeqBlockKey.encode();
        let block_store = SeqBlockStore::new(Arc::clone(&storage), seq_key);
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await?;

        Ok(Self {
            storage,
            id_allocator: Arc::new(allocator),
            lock: tokio::sync::RwLock::new(()),
        })
    }

    /// Looks up the internal ID for a key.
    ///
    /// Returns `Ok(Some(internal_id))` if found, `Ok(None)` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Storage operation fails
    /// - Value is not a valid 8-byte u64
    pub async fn lookup(&self, key: Bytes) -> Result<Option<u64>> {
        match self.storage.get(key).await? {
            Some(record) => {
                let value = decode_u64(&record.value)
                    .context("failed to decode internal ID from storage")?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Upserts a key mapping, allocating a new internal ID.
    ///
    /// This operation is atomic and thread-safe. If the key already exists,
    /// the old mapping is deleted before creating the new one.
    ///
    /// # Returns
    ///
    /// A tuple of (new_id, old_id_option):
    /// - `new_id`: The newly allocated internal ID for this key
    /// - `old_id_option`: The previous internal ID if the key existed, None otherwise
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - ID allocation fails
    /// - Storage operation fails
    pub async fn upsert(&self, key: Bytes) -> Result<(u64, Option<u64>)> {
        // Acquire write lock for the entire operation
        let _guard = self.lock.write().await;

        #[cfg(test)]
        fail::fail_point!("dict_after_lock");

        // Look up existing value
        let old_id = match self.storage.get(key.clone()).await? {
            Some(record) => {
                Some(decode_u64(&record.value).context("failed to decode old internal ID")?)
            }
            None => None,
        };

        #[cfg(test)]
        fail::fail_point!("dict_after_lookup");

        // Allocate new internal ID
        let new_id = self.id_allocator.allocate_one().await?;

        #[cfg(test)]
        fail::fail_point!("dict_before_apply");

        // Build operations: delete old (if exists) + put new
        let mut ops = Vec::new();
        if old_id.is_some() {
            ops.push(RecordOp::Delete(key.clone()));

            // TODO(agavra): For full vector deletion support, we need to:
            // 1. Atomically delete values from forward index
            // 2. Insert tombstone posting in centroid deletion list (centroid_id=0)
        }
        ops.push(RecordOp::Put(Record::new(key, encode_u64(new_id))));

        // Apply atomically
        self.storage.apply(ops).await?;

        Ok((new_id, old_id))
    }
}

/// Encodes a u64 as 8-byte little-endian using standard encoding.
fn encode_u64(value: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(8);
    encode_u64_encoding(value, &mut buf);
    buf.freeze()
}

/// Decodes a u64 from 8-byte little-endian using standard encoding.
fn decode_u64(buf: &[u8]) -> Result<u64> {
    let mut slice = buf;
    decode_u64_encoding(&mut slice).context("invalid u64 encoding: expected 8-byte value")
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::storage::in_memory::InMemoryStorage;

    async fn create_test_dictionary() -> Dictionary {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        Dictionary::new(storage).await.unwrap()
    }

    #[tokio::test]
    async fn should_upsert_and_lookup_key() {
        // given
        let dict = create_test_dictionary().await;
        let key = Bytes::from("test-key");

        // when
        let (internal_id, old_id) = dict.upsert(key.clone()).await.unwrap();

        // then
        assert_eq!(old_id, None);
        let found = dict.lookup(key).await.unwrap();
        assert_eq!(found, Some(internal_id));
    }

    #[tokio::test]
    async fn should_return_none_for_nonexistent_key() {
        // given
        let dict = create_test_dictionary().await;

        // when
        let found = dict.lookup(Bytes::from("nonexistent")).await.unwrap();

        // then
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn should_allocate_sequential_ids() {
        // given
        let dict = create_test_dictionary().await;

        // when
        let (id1, _) = dict.upsert(Bytes::from("key1")).await.unwrap();
        let (id2, _) = dict.upsert(Bytes::from("key2")).await.unwrap();
        let (id3, _) = dict.upsert(Bytes::from("key3")).await.unwrap();

        // then
        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
    }

    #[tokio::test]
    async fn should_return_old_id_on_upsert() {
        // given
        let dict = create_test_dictionary().await;
        let key = Bytes::from("duplicate");
        let (id1, old1) = dict.upsert(key.clone()).await.unwrap();

        // then - first upsert has no old ID
        assert_eq!(old1, None);

        // when - upsert again
        let (id2, old2) = dict.upsert(key.clone()).await.unwrap();

        // then
        assert_ne!(id1, id2, "Second upsert should allocate a new ID");
        assert_eq!(old2, Some(id1), "Should return the previous ID");
        let found = dict.lookup(key).await.unwrap();
        assert_eq!(found, Some(id2), "Lookup should return the most recent ID");
    }

    #[tokio::test]
    async fn should_handle_concurrent_upserts() {
        // given
        let dict = Arc::new(create_test_dictionary().await);
        let mut handles = vec![];

        // when - spawn 10 tasks upserting different keys
        for i in 0..10 {
            let dict_clone = Arc::clone(&dict);
            let handle = tokio::spawn(async move {
                let key = Bytes::from(format!("key-{}", i));
                dict_clone.upsert(key).await.unwrap()
            });
            handles.push(handle);
        }

        // Collect all internal IDs
        let mut internal_ids = vec![];
        for handle in handles {
            let (new_id, _) = handle.await.unwrap();
            internal_ids.push(new_id);
        }

        // then - all IDs should be unique
        internal_ids.sort();
        internal_ids.dedup();
        assert_eq!(internal_ids.len(), 10, "All IDs should be unique");
    }

    #[tokio::test]
    async fn should_atomically_delete_and_insert_on_upsert() {
        // given
        let dict = create_test_dictionary().await;
        let key = Bytes::from("update-key");

        // when - first upsert
        let (id1, old1) = dict.upsert(key.clone()).await.unwrap();

        // then - no old ID on first insert
        assert_eq!(old1, None);

        // when - second upsert
        let (id2, old2) = dict.upsert(key.clone()).await.unwrap();

        // then - old ID should match first insert
        assert_eq!(old2, Some(id1));
        assert_ne!(id1, id2);

        // then - lookup returns newest ID
        let found = dict.lookup(key).await.unwrap();
        assert_eq!(found, Some(id2));
    }

    #[tokio::test]
    async fn should_prevent_concurrent_upsert_races_with_lock() {
        use std::sync::Barrier;

        // given
        let dict = Arc::new(create_test_dictionary().await);
        let key = Bytes::from("race-key");

        // Insert initial value
        let (initial_id, _) = dict.upsert(key.clone()).await.unwrap();

        // Setup barrier to force two threads to race at the lookup point
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = barrier.clone();

        fail::cfg_callback("dict_after_lock", move || {
            barrier_clone.wait();
        })
        .unwrap();

        // when - spawn two threads that will race to upsert the same key
        let dict1 = dict.clone();
        let key1 = key.clone();
        let handle1 = tokio::spawn(async move { dict1.upsert(key1).await.unwrap() });

        let dict2 = dict.clone();
        let key2 = key.clone();
        let handle2 = tokio::spawn(async move { dict2.upsert(key2).await.unwrap() });

        let (result1, result2) = tokio::join!(handle1, handle2);
        let (new_id1, old_id1) = result1.unwrap();
        let (new_id2, old_id2) = result2.unwrap();

        // then - one should see initial_id, the other should see the first upsert's ID
        assert!(
            (old_id1 == Some(initial_id) && old_id2 == Some(new_id1))
                || (old_id2 == Some(initial_id) && old_id1 == Some(new_id2)),
            "One upsert should see initial ID, the other should see the first upsert's new ID"
        );

        // Final lookup should return the last upsert's ID
        let final_id = dict.lookup(key).await.unwrap().unwrap();
        assert!(final_id == new_id1 || final_id == new_id2);

        // Cleanup failpoint
        fail::remove("dict_after_lock");
    }

    #[tokio::test]
    async fn should_decode_invalid_value_as_error() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let dict = Dictionary::new(Arc::clone(&storage)).await.unwrap();

        // Manually insert invalid value (not 8 bytes)
        let key = Bytes::from("bad-key");
        let bad_value = Bytes::from(vec![1, 2, 3]); // Only 3 bytes
        storage
            .put(vec![Record::new(key.clone(), bad_value)])
            .await
            .unwrap();

        // when
        let result = dict.lookup(key).await;

        // then
        assert!(result.is_err(), "should fail to decode invalid value");
    }
}

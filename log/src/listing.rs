//! Key listing for log streams.
//!
//! This module provides efficient key enumeration via per-segment listing records.
//! When entries are appended, listing records track which keys are present in each
//! segment. This enables key discovery without scanning all log entries.
//!
//! # Components
//!
//! - [`ListingReader`]: Read-only access to listing entries
//! - [`ListingCache`]: In-memory cache for deduplicating listing entries on the write path
//! - [`LogKeyIterator`]: Iterator over keys from listing entries

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use common::{Record, StorageRead};

use crate::error::{Error, Result};
use crate::model::SegmentId;
use crate::serde::{ListingEntryKey, ListingEntryValue};

/// A key returned from the listing iterator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogKey {
    /// The user-provided key
    pub key: Bytes,
}

impl LogKey {
    /// Creates a new log key.
    pub fn new(key: Bytes) -> Self {
        Self { key }
    }
}

/// Iterator over keys from listing entries.
///
/// Iterates over distinct keys present in the specified segment range.
/// Keys are deduplicated and returned in lexicographic order.
pub struct LogKeyIterator {
    /// Iterator over keys.
    keys: std::collections::btree_set::IntoIter<Bytes>,
}

impl LogKeyIterator {
    /// Creates a new key iterator by scanning listing entries.
    ///
    /// This loads all keys from the segment range into memory for deduplication.
    /// The async API is preserved for future streaming implementations.
    pub(crate) async fn open(
        storage: Arc<dyn StorageRead>,
        segment_range: Range<SegmentId>,
    ) -> Result<Self> {
        // Empty range means no keys
        if segment_range.start >= segment_range.end {
            return Ok(Self {
                keys: BTreeSet::new().into_iter(),
            });
        }

        // Scan all listing entries in the segment range
        let scan_range = ListingEntryKey::scan_range(segment_range);
        let mut iter = storage
            .scan_iter(scan_range)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        // Collect keys into BTreeSet (deduplicates and sorts)
        let mut keys = BTreeSet::new();
        while let Some(record) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let entry_key = ListingEntryKey::deserialize(&record.key)?;
            keys.insert(entry_key.key);
        }

        Ok(Self {
            keys: keys.into_iter(),
        })
    }

    /// Returns the next key, or `None` if exhausted.
    pub async fn next(&mut self) -> Result<Option<LogKey>> {
        Ok(self.keys.next().map(LogKey::new))
    }
}

/// In-memory cache of keys seen in the current segment.
///
/// Used to generate listing entry records during ingestion, avoiding
/// duplicate writes for the same key within a segment.
pub(crate) struct ListingCache {
    /// The segment ID this cache is tracking.
    current_segment_id: Option<SegmentId>,
    /// Keys seen in the current segment.
    keys: BTreeSet<Bytes>,
}

impl ListingCache {
    /// Creates a new empty cache.
    pub(crate) fn new() -> Self {
        Self {
            current_segment_id: None,
            keys: BTreeSet::new(),
        }
    }

    /// Assigns listing entries for the given keys.
    ///
    /// For each key that is new to the segment (not already cached and not
    /// a duplicate within this batch), appends a listing entry record to
    /// `records` and updates the cache.
    ///
    /// If `segment_id` differs from the cached segment, the cache is reset.
    pub(crate) fn assign_keys(
        &mut self,
        segment_id: SegmentId,
        keys: &[Bytes],
        records: &mut Vec<Record>,
    ) {
        if self.current_segment_id != Some(segment_id) {
            self.keys.clear();
            self.current_segment_id = Some(segment_id);
        }

        let value = ListingEntryValue::new().serialize();

        for key in keys {
            if self.keys.contains(key) {
                continue;
            }

            let storage_key = ListingEntryKey::new(segment_id, key.clone()).serialize();
            records.push(Record::new(storage_key, value.clone()));
            self.keys.insert(key.clone());
        }
    }

    /// Checks if a key is new for the given segment.
    #[cfg(test)]
    fn is_new(&self, segment_id: SegmentId, key: &Bytes) -> bool {
        if self.current_segment_id != Some(segment_id) {
            return true;
        }
        !self.keys.contains(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LogStorage;

    mod log_key_iterator {
        use super::*;

        async fn write_listing_entry(storage: &LogStorage, segment_id: u32, key: &[u8]) {
            let storage_key =
                ListingEntryKey::new(segment_id, Bytes::copy_from_slice(key)).serialize();
            let value = ListingEntryValue::new().serialize();
            storage
                .put_with_options(
                    vec![common::Record::new(storage_key, value)],
                    common::WriteOptions::default(),
                )
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn should_return_empty_for_empty_range() {
            // given
            let storage = LogStorage::in_memory();

            // when
            let mut iter = storage.as_read().list_keys(0..0).await.unwrap();

            // then
            assert!(iter.next().await.unwrap().is_none());
        }

        #[tokio::test]
        async fn should_return_empty_when_no_listing_entries() {
            // given
            let storage = LogStorage::in_memory();

            // when
            let mut iter = storage.as_read().list_keys(0..10).await.unwrap();

            // then
            assert!(iter.next().await.unwrap().is_none());
        }

        #[tokio::test]
        async fn should_iterate_keys_in_single_segment() {
            // given
            let storage = LogStorage::in_memory();
            write_listing_entry(&storage, 0, b"key-a").await;
            write_listing_entry(&storage, 0, b"key-b").await;
            write_listing_entry(&storage, 0, b"key-c").await;

            // when
            let mut iter = storage.as_read().list_keys(0..1).await.unwrap();

            // then - keys returned in lexicographic order
            let mut keys = Vec::new();
            while let Some(key) = iter.next().await.unwrap() {
                keys.push(key.key);
            }
            assert_eq!(keys.len(), 3);
            assert_eq!(keys[0], Bytes::from("key-a"));
            assert_eq!(keys[1], Bytes::from("key-b"));
            assert_eq!(keys[2], Bytes::from("key-c"));
        }

        #[tokio::test]
        async fn should_iterate_keys_across_multiple_segments() {
            // given
            let storage = LogStorage::in_memory();
            write_listing_entry(&storage, 0, b"key-a").await;
            write_listing_entry(&storage, 1, b"key-b").await;
            write_listing_entry(&storage, 2, b"key-c").await;

            // when
            let mut iter = storage.as_read().list_keys(0..3).await.unwrap();

            // then
            let mut keys = Vec::new();
            while let Some(key) = iter.next().await.unwrap() {
                keys.push(key.key);
            }
            assert_eq!(keys.len(), 3);
        }

        #[tokio::test]
        async fn should_deduplicate_keys_across_segments() {
            // given - same key in multiple segments
            let storage = LogStorage::in_memory();
            write_listing_entry(&storage, 0, b"shared-key").await;
            write_listing_entry(&storage, 1, b"shared-key").await;
            write_listing_entry(&storage, 2, b"shared-key").await;

            // when
            let mut iter = storage.as_read().list_keys(0..3).await.unwrap();

            // then - only one instance of the key
            let mut keys = Vec::new();
            while let Some(key) = iter.next().await.unwrap() {
                keys.push(key.key);
            }
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], Bytes::from("shared-key"));
        }

        #[tokio::test]
        async fn should_respect_segment_range() {
            // given
            let storage = LogStorage::in_memory();
            write_listing_entry(&storage, 0, b"key-0").await;
            write_listing_entry(&storage, 1, b"key-1").await;
            write_listing_entry(&storage, 2, b"key-2").await;
            write_listing_entry(&storage, 3, b"key-3").await;

            // when - only query segments 1..3
            let mut iter = storage.as_read().list_keys(1..3).await.unwrap();

            // then - only keys from segments 1 and 2
            let mut keys = Vec::new();
            while let Some(key) = iter.next().await.unwrap() {
                keys.push(key.key);
            }
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], Bytes::from("key-1"));
            assert_eq!(keys[1], Bytes::from("key-2"));
        }

        #[tokio::test]
        async fn should_return_keys_in_lexicographic_order() {
            // given - keys inserted out of order
            let storage = LogStorage::in_memory();
            write_listing_entry(&storage, 0, b"zebra").await;
            write_listing_entry(&storage, 0, b"apple").await;
            write_listing_entry(&storage, 0, b"mango").await;

            // when
            let mut iter = storage.as_read().list_keys(0..1).await.unwrap();

            // then - keys returned in lexicographic order
            let mut keys = Vec::new();
            while let Some(key) = iter.next().await.unwrap() {
                keys.push(key.key);
            }
            assert_eq!(keys[0], Bytes::from("apple"));
            assert_eq!(keys[1], Bytes::from("mango"));
            assert_eq!(keys[2], Bytes::from("zebra"));
        }
    }

    mod listing_cache {
        use super::*;

        #[test]
        fn should_add_records_for_new_keys() {
            // given
            let mut cache = ListingCache::new();
            let keys = vec![Bytes::from("key1"), Bytes::from("key2")];
            let mut records = Vec::new();

            // when
            cache.assign_keys(0, &keys, &mut records);

            // then
            assert_eq!(records.len(), 2);
        }

        #[test]
        fn should_exclude_cached_keys() {
            // given
            let mut cache = ListingCache::new();
            let mut records1 = Vec::new();
            cache.assign_keys(
                0,
                &[Bytes::from("key1"), Bytes::from("key2")],
                &mut records1,
            );

            // when - second batch with overlap
            let mut records2 = Vec::new();
            cache.assign_keys(
                0,
                &[Bytes::from("key2"), Bytes::from("key3")],
                &mut records2,
            );

            // then - only key3 is new
            assert_eq!(records2.len(), 1);
        }

        #[test]
        fn should_dedupe_keys_within_batch() {
            // given
            let mut cache = ListingCache::new();
            let keys = vec![
                Bytes::from("key1"),
                Bytes::from("key2"),
                Bytes::from("key1"), // duplicate
            ];
            let mut records = Vec::new();

            // when
            cache.assign_keys(0, &keys, &mut records);

            // then
            assert_eq!(records.len(), 2);
        }

        #[test]
        fn should_include_all_keys_after_segment_change() {
            // given
            let mut cache = ListingCache::new();
            let keys = vec![Bytes::from("key1"), Bytes::from("key2")];
            let mut records0 = Vec::new();
            cache.assign_keys(0, &keys, &mut records0);

            // when - new segment with same keys
            let mut records1 = Vec::new();
            cache.assign_keys(1, &keys, &mut records1);

            // then - all keys are new in new segment
            assert_eq!(records1.len(), 2);
        }

        #[test]
        fn should_clear_cache_when_segment_changes() {
            // given
            let mut cache = ListingCache::new();
            let mut records0 = Vec::new();
            cache.assign_keys(0, &[Bytes::from("key1")], &mut records0);

            // when - different segment
            let mut records1 = Vec::new();
            cache.assign_keys(1, &[Bytes::from("key2")], &mut records1);

            // then - key1 should be new again (cache cleared)
            assert!(cache.is_new(1, &Bytes::from("key1")));
            // but key2 should not be new
            assert!(!cache.is_new(1, &Bytes::from("key2")));
        }

        #[test]
        fn should_create_correct_listing_entry_records() {
            // given
            let mut cache = ListingCache::new();
            let keys = vec![Bytes::from("mykey")];
            let mut records = Vec::new();

            // when
            cache.assign_keys(42, &keys, &mut records);

            // then
            assert_eq!(records.len(), 1);
            let expected_key = ListingEntryKey::new(42, Bytes::from("mykey")).serialize();
            let expected_value = ListingEntryValue::new().serialize();
            assert_eq!(records[0].key, expected_key);
            assert_eq!(records[0].value, expected_value);
        }
    }
}

//! Core Log implementation with read and write APIs.
//!
//! This module provides the [`Log`] struct, the primary entry point for
//! interacting with OpenData Log. It exposes both write operations ([`append`])
//! and read operations ([`scan`], [`count`]) via the [`LogRead`] trait.

use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common::clock::{Clock, SystemClock};
use common::storage::factory::create_storage;
use common::{Record as StorageRecord, WriteOptions as StorageWriteOptions};
use tokio::sync::RwLock;

use crate::config::{CountOptions, ListOptions, ScanOptions, WriteOptions};
use crate::error::{Error, Result};
use crate::listing::ListingCache;
use crate::listing::LogKeyIterator;
use crate::model::Record;
use crate::range::normalize;
use crate::reader::{LogIterator, LogRead, list_keys_in_segments};
use crate::segment::SegmentCache;
use crate::sequence::SequenceAllocator;
use crate::serde::LogEntryBuilder;
use crate::storage::LogStorage;

/// Inner state for the write path.
///
/// Wrapped in a single `RwLock` for simplicity. A more sophisticated
/// concurrency strategy may be needed for high-throughput scenarios.
struct LogInner {
    sequence_allocator: SequenceAllocator,
    segment_cache: SegmentCache,
    listing_cache: ListingCache,
}

/// The main log interface providing read and write operations.
///
/// `Log` is the primary entry point for interacting with OpenData Log.
/// It provides methods to append records, scan entries, and count records
/// within a key's log.
///
/// # Read Operations
///
/// Read operations are provided via the [`LogRead`] trait, which `Log`
/// implements. This allows generic code to work with either `Log` or
/// [`LogReader`](crate::LogReader).
///
/// # Thread Safety
///
/// `Log` is designed to be shared across threads. All methods take `&self`
/// and internal synchronization is handled automatically.
///
/// # Writer Semantics
///
/// Currently, each log supports a single writer. Multi-writer support may
/// be added in the future, but would require each key to have a single
/// writer to maintain monotonic ordering within that key's log.
///
/// # Example
///
/// ```ignore
/// use log::{Log, LogRead, Record, WriteOptions};
/// use bytes::Bytes;
///
/// // Open a log (implementation details TBD)
/// let log = Log::open(config).await?;
///
/// // Append records
/// let records = vec![
///     Record { key: Bytes::from("user:123"), value: Bytes::from("event-a") },
///     Record { key: Bytes::from("user:456"), value: Bytes::from("event-b") },
/// ];
/// log.append(records).await?;
///
/// // Scan entries for a specific key
/// let mut iter = log.scan(Bytes::from("user:123"), ..).await?;
/// while let Some(entry) = iter.next().await? {
///     println!("seq={}: {:?}", entry.sequence, entry.value);
/// }
/// ```
pub struct Log {
    storage: LogStorage,
    clock: Arc<dyn Clock>,
    inner: RwLock<LogInner>,
}

impl Log {
    /// Opens or creates a log with the given configuration.
    ///
    /// This is the primary entry point for creating a `Log` instance. The
    /// configuration specifies the storage backend and other settings.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying storage backend and settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use log::{Log, Config};
    ///
    /// let log = Log::open(test_config()).await?;
    /// ```
    pub async fn open(config: crate::config::Config) -> crate::error::Result<Self> {
        let storage = create_storage(&config.storage, None)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let log_storage = LogStorage::new(storage);

        let clock: Arc<dyn Clock> = Arc::new(SystemClock);

        let log_storage_read = log_storage.as_read();
        let sequence_allocator = SequenceAllocator::open(&log_storage_read).await?;
        let segment_cache = SegmentCache::open(&log_storage_read, config.segmentation).await?;
        let listing_cache = ListingCache::new();

        let inner = LogInner {
            sequence_allocator,
            segment_cache,
            listing_cache,
        };

        Ok(Self {
            storage: log_storage,
            clock,
            inner: RwLock::new(inner),
        })
    }

    /// Appends records to the log.
    ///
    /// Records are assigned sequence numbers in the order they appear in the
    /// input vector. All records in a single append call are written atomically.
    ///
    /// This method uses default write options. Use [`append_with_options`] for
    /// custom durability settings.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to append. Each record specifies its target
    ///   key and value.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails due to storage issues.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let records = vec![
    ///     Record { key: Bytes::from("events"), value: Bytes::from("event-1") },
    ///     Record { key: Bytes::from("events"), value: Bytes::from("event-2") },
    /// ];
    /// log.append(records).await?;
    /// ```
    ///
    /// [`append_with_options`]: Log::append_with_options
    pub async fn append(&self, records: Vec<Record>) -> Result<()> {
        self.append_with_options(records, WriteOptions::default())
            .await
    }

    /// Appends records to the log with custom options.
    ///
    /// Records are assigned sequence numbers in the order they appear in the
    /// input vector. All records in a single append call are written atomically.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to append. Each record specifies its target
    ///   key and value.
    /// * `options` - Write options controlling durability behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails due to storage issues.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let records = vec![
    ///     Record { key: Bytes::from("events"), value: Bytes::from("critical-event") },
    /// ];
    /// let options = WriteOptions { await_durable: true };
    /// log.append_with_options(records, options).await?;
    /// ```
    pub async fn append_with_options(
        &self,
        records: Vec<Record>,
        options: WriteOptions,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let current_time_ms = self.current_time_ms();
        let mut inner = self.inner.write().await;

        // Three-phase commit: build (fallible) → write (fatal) → apply (infallible).
        // Deltas capture intent without mutating state, allowing clean abort on build errors.

        // Build phase: accumulate deltas and storage records.
        let mut storage_records: Vec<StorageRecord> = Vec::new();
        let seq_delta = inner
            .sequence_allocator
            .build_delta(records.len() as u64, &mut storage_records);
        let seg_delta = inner.segment_cache.build_delta(
            current_time_ms,
            seq_delta.base_sequence(),
            &mut storage_records,
        );
        let keys: Vec<_> = records.iter().map(|r| r.key.clone()).collect();
        let listing_delta =
            inner
                .listing_cache
                .build_delta(&seg_delta, &keys, &mut storage_records);
        LogEntryBuilder::build(
            seg_delta.segment(),
            seq_delta.base_sequence(),
            &records,
            &mut storage_records,
        );

        // Write phase: atomic write to storage.
        let storage_options = StorageWriteOptions {
            await_durable: options.await_durable,
        };
        self.storage
            .put_with_options(storage_records, storage_options)
            .await?;

        // Apply phase: update in-memory caches.
        inner.sequence_allocator.apply_delta(seq_delta);
        inner.segment_cache.apply_delta(seg_delta);
        inner.listing_cache.apply_delta(listing_delta);

        Ok(())
    }

    /// Returns the current time in milliseconds since Unix epoch.
    fn current_time_ms(&self) -> i64 {
        self.clock
            .now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Forces creation of a new segment, sealing the current one.
    ///
    /// This is an internal API for testing multi-segment scenarios. It forces
    /// subsequent appends to write to a new segment, regardless of any
    /// configured seal interval.
    #[cfg(test)]
    pub(crate) async fn seal_segment(&self) -> Result<()> {
        use crate::segment::LogSegment;
        use crate::serde::SegmentMeta;

        let current_time_ms = self.current_time_ms();
        let mut inner = self.inner.write().await;
        let next_seq = inner.sequence_allocator.peek_next_sequence();

        // Determine next segment ID
        let segment_id = match inner.segment_cache.latest() {
            Some(latest) => latest.id() + 1,
            None => 0,
        };

        // Create and write the new segment
        let meta = SegmentMeta::new(next_seq, current_time_ms);
        let segment = LogSegment::new(segment_id, meta);
        self.storage.write_segment(&segment).await?;

        // Update cache
        inner.segment_cache.insert(segment);

        Ok(())
    }

    /// Creates a Log from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new(storage: Arc<dyn common::Storage>) -> Result<Self> {
        use crate::config::SegmentConfig;

        let log_storage = LogStorage::new(storage);
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);

        let log_storage_read = log_storage.as_read();
        let sequence_allocator = SequenceAllocator::open(&log_storage_read).await?;
        let segment_cache = SegmentCache::open(&log_storage_read, SegmentConfig::default()).await?;
        let listing_cache = ListingCache::new();

        let inner = LogInner {
            sequence_allocator,
            segment_cache,
            listing_cache,
        };

        Ok(Self {
            storage: log_storage,
            clock,
            inner: RwLock::new(inner),
        })
    }
}

#[async_trait]
impl LogRead for Log {
    async fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        _options: ScanOptions,
    ) -> Result<LogIterator> {
        let seq_range = normalize(&seq_range);
        let inner = self.inner.read().await;
        Ok(LogIterator::open(
            self.storage.as_read(),
            &inner.segment_cache,
            key,
            seq_range,
        ))
    }

    async fn count_with_options(
        &self,
        _key: Bytes,
        _seq_range: impl RangeBounds<u64> + Send,
        _options: CountOptions,
    ) -> Result<u64> {
        todo!()
    }

    async fn list_with_options(
        &self,
        seq_range: impl RangeBounds<u64> + Send,
        _options: ListOptions,
    ) -> Result<LogKeyIterator> {
        let seq_range = normalize(&seq_range);
        let inner = self.inner.read().await;
        let segments = inner.segment_cache.find_covering(&seq_range);
        list_keys_in_segments(&self.storage.as_read(), &segments).await
    }
}

#[cfg(test)]
mod tests {
    use common::StorageConfig;
    use common::storage::factory::create_storage;

    use super::*;
    use crate::config::Config;
    use crate::config::ListOptions;
    use crate::reader::LogReader;

    fn test_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn should_open_log_with_in_memory_config() {
        // given
        let config = test_config();

        // when
        let result = Log::open(config).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_append_single_record() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        let records = vec![Record {
            key: Bytes::from("orders"),
            value: Bytes::from("order-1"),
        }];

        // when
        log.append(records).await.unwrap();

        // then - verify entry can be read back
        let mut iter = log.scan(Bytes::from("orders"), ..).await.unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 0);
        assert_eq!(entry.value, Bytes::from("order-1"));
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_append_multiple_records_in_batch() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        let records = vec![
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-1"),
            },
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-2"),
            },
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-3"),
            },
        ];

        // when
        log.append(records).await.unwrap();

        // then - verify entries with sequential sequence numbers
        let mut iter = log.scan(Bytes::from("orders"), ..).await.unwrap();

        let entry0 = iter.next().await.unwrap().unwrap();
        assert_eq!(entry0.sequence, 0);
        assert_eq!(entry0.value, Bytes::from("order-1"));

        let entry1 = iter.next().await.unwrap().unwrap();
        assert_eq!(entry1.sequence, 1);
        assert_eq!(entry1.value, Bytes::from("order-2"));

        let entry2 = iter.next().await.unwrap().unwrap();
        assert_eq!(entry2.sequence, 2);
        assert_eq!(entry2.value, Bytes::from("order-3"));

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_append_empty_records_without_error() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        let records: Vec<Record> = vec![];

        // when
        let result = log.append(records).await;

        // then
        assert!(result.is_ok());

        // verify no entries exist
        let mut iter = log.scan(Bytes::from("any-key"), ..).await.unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_assign_sequential_sequences_across_appends() {
        // given
        let log = Log::open(test_config()).await.unwrap();

        // when - first append
        log.append(vec![
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-1"),
            },
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-2"),
            },
        ])
        .await
        .unwrap();

        // when - second append
        log.append(vec![Record {
            key: Bytes::from("events"),
            value: Bytes::from("event-3"),
        }])
        .await
        .unwrap();

        // then - verify sequences are 0, 1, 2 across appends
        let mut iter = log.scan(Bytes::from("events"), ..).await.unwrap();

        let entry0 = iter.next().await.unwrap().unwrap();
        assert_eq!(entry0.sequence, 0);

        let entry1 = iter.next().await.unwrap().unwrap();
        assert_eq!(entry1.sequence, 1);

        let entry2 = iter.next().await.unwrap().unwrap();
        assert_eq!(entry2.sequence, 2);

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_store_records_with_correct_keys_and_values() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        let records = vec![
            Record {
                key: Bytes::from("topic-a"),
                value: Bytes::from("message-a"),
            },
            Record {
                key: Bytes::from("topic-b"),
                value: Bytes::from("message-b"),
            },
        ];

        // when
        log.append(records).await.unwrap();

        // then - verify entries for topic-a
        let mut iter_a = log.scan(Bytes::from("topic-a"), ..).await.unwrap();
        let entry_a = iter_a.next().await.unwrap().unwrap();
        assert_eq!(entry_a.key, Bytes::from("topic-a"));
        assert_eq!(entry_a.value, Bytes::from("message-a"));
        assert!(iter_a.next().await.unwrap().is_none());

        // then - verify entries for topic-b
        let mut iter_b = log.scan(Bytes::from("topic-b"), ..).await.unwrap();
        let entry_b = iter_b.next().await.unwrap().unwrap();
        assert_eq!(entry_b.key, Bytes::from("topic-b"));
        assert_eq!(entry_b.value, Bytes::from("message-b"));
        assert!(iter_b.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_scan_all_entries_for_key() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-1"),
            },
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-2"),
            },
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-3"),
            },
        ])
        .await
        .unwrap();

        // when
        let mut iter = log.scan(Bytes::from("orders"), ..).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[0].value, Bytes::from("order-1"));
        assert_eq!(entries[1].sequence, 1);
        assert_eq!(entries[1].value, Bytes::from("order-2"));
        assert_eq!(entries[2].sequence, 2);
        assert_eq!(entries[2].value, Bytes::from("order-3"));
    }

    #[tokio::test]
    async fn should_scan_with_sequence_range() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-0"),
            },
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-1"),
            },
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-2"),
            },
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-3"),
            },
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-4"),
            },
        ])
        .await
        .unwrap();

        // when - scan sequences 1..4 (exclusive end)
        let mut iter = log.scan(Bytes::from("events"), 1..4).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 3);
    }

    #[tokio::test]
    async fn should_scan_from_starting_sequence() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("logs"),
                value: Bytes::from("log-0"),
            },
            Record {
                key: Bytes::from("logs"),
                value: Bytes::from("log-1"),
            },
            Record {
                key: Bytes::from("logs"),
                value: Bytes::from("log-2"),
            },
        ])
        .await
        .unwrap();

        // when - scan from sequence 1 onwards
        let mut iter = log.scan(Bytes::from("logs"), 1..).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
    }

    #[tokio::test]
    async fn should_scan_up_to_ending_sequence() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("logs"),
                value: Bytes::from("log-0"),
            },
            Record {
                key: Bytes::from("logs"),
                value: Bytes::from("log-1"),
            },
            Record {
                key: Bytes::from("logs"),
                value: Bytes::from("log-2"),
            },
        ])
        .await
        .unwrap();

        // when - scan up to sequence 2 (exclusive)
        let mut iter = log.scan(Bytes::from("logs"), ..2).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[tokio::test]
    async fn should_scan_only_entries_for_specified_key() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("key-a"),
                value: Bytes::from("value-a-0"),
            },
            Record {
                key: Bytes::from("key-b"),
                value: Bytes::from("value-b-0"),
            },
            Record {
                key: Bytes::from("key-a"),
                value: Bytes::from("value-a-1"),
            },
            Record {
                key: Bytes::from("key-b"),
                value: Bytes::from("value-b-1"),
            },
        ])
        .await
        .unwrap();

        // when - scan only key-a
        let mut iter = log.scan(Bytes::from("key-a"), ..).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then - should only have entries for key-a
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, Bytes::from("key-a"));
        assert_eq!(entries[0].value, Bytes::from("value-a-0"));
        assert_eq!(entries[1].key, Bytes::from("key-a"));
        assert_eq!(entries[1].value, Bytes::from("value-a-1"));
    }

    #[tokio::test]
    async fn should_return_empty_iterator_for_unknown_key() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![Record {
            key: Bytes::from("existing"),
            value: Bytes::from("value"),
        }])
        .await
        .unwrap();

        // when - scan for non-existent key
        let mut iter = log.scan(Bytes::from("unknown"), ..).await.unwrap();
        let entry = iter.next().await.unwrap();

        // then
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn should_return_empty_iterator_for_empty_range() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("key"),
                value: Bytes::from("value-0"),
            },
            Record {
                key: Bytes::from("key"),
                value: Bytes::from("value-1"),
            },
        ])
        .await
        .unwrap();

        // when - scan range that doesn't include any existing sequences
        let mut iter = log.scan(Bytes::from("key"), 10..20).await.unwrap();
        let entry = iter.next().await.unwrap();

        // then
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn should_scan_entries_via_log_reader() {
        // given - create shared storage
        let storage = create_storage(&StorageConfig::InMemory, None)
            .await
            .unwrap();
        let log = Log::new(storage.clone()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-1"),
            },
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-2"),
            },
            Record {
                key: Bytes::from("orders"),
                value: Bytes::from("order-3"),
            },
        ])
        .await
        .unwrap();

        // when - create LogReader sharing the same storage
        let reader = LogReader::new(storage).await.unwrap();
        let mut iter = reader.scan(Bytes::from("orders"), ..).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[0].value, Bytes::from("order-1"));
        assert_eq!(entries[1].sequence, 1);
        assert_eq!(entries[1].value, Bytes::from("order-2"));
        assert_eq!(entries[2].sequence, 2);
        assert_eq!(entries[2].value, Bytes::from("order-3"));
    }

    #[tokio::test]
    async fn should_scan_across_multiple_segments() {
        // given - log with entries across multiple segments
        let log = Log::open(test_config()).await.unwrap();

        // write to segment 0
        log.append(vec![
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-0"),
            },
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-1"),
            },
        ])
        .await
        .unwrap();

        // seal and create segment 1
        log.seal_segment().await.unwrap();

        // write to segment 1
        log.append(vec![
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-2"),
            },
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-3"),
            },
        ])
        .await
        .unwrap();

        // when - scan all entries
        let mut iter = log.scan(Bytes::from("events"), ..).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then - should see all 4 entries in order
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[0].value, Bytes::from("event-0"));
        assert_eq!(entries[1].sequence, 1);
        assert_eq!(entries[1].value, Bytes::from("event-1"));
        assert_eq!(entries[2].sequence, 2);
        assert_eq!(entries[2].value, Bytes::from("event-2"));
        assert_eq!(entries[3].sequence, 3);
        assert_eq!(entries[3].value, Bytes::from("event-3"));
    }

    #[tokio::test]
    async fn should_scan_range_spanning_segments() {
        // given - log with entries across multiple segments
        let log = Log::open(test_config()).await.unwrap();

        // segment 0: seq 0, 1
        log.append(vec![
            Record {
                key: Bytes::from("data"),
                value: Bytes::from("seg0-0"),
            },
            Record {
                key: Bytes::from("data"),
                value: Bytes::from("seg0-1"),
            },
        ])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 1: seq 2, 3
        log.append(vec![
            Record {
                key: Bytes::from("data"),
                value: Bytes::from("seg1-2"),
            },
            Record {
                key: Bytes::from("data"),
                value: Bytes::from("seg1-3"),
            },
        ])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 2: seq 4, 5
        log.append(vec![
            Record {
                key: Bytes::from("data"),
                value: Bytes::from("seg2-4"),
            },
            Record {
                key: Bytes::from("data"),
                value: Bytes::from("seg2-5"),
            },
        ])
        .await
        .unwrap();

        // when - scan range 1..5 (spans segments 0, 1, 2)
        let mut iter = log.scan(Bytes::from("data"), 1..5).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then - should see entries 1, 2, 3, 4
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 3);
        assert_eq!(entries[3].sequence, 4);
    }

    #[tokio::test]
    async fn should_scan_single_segment_in_multi_segment_log() {
        // given - log with entries across multiple segments
        let log = Log::open(test_config()).await.unwrap();

        // segment 0: seq 0, 1
        log.append(vec![
            Record {
                key: Bytes::from("key"),
                value: Bytes::from("v0"),
            },
            Record {
                key: Bytes::from("key"),
                value: Bytes::from("v1"),
            },
        ])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 1: seq 2, 3
        log.append(vec![
            Record {
                key: Bytes::from("key"),
                value: Bytes::from("v2"),
            },
            Record {
                key: Bytes::from("key"),
                value: Bytes::from("v3"),
            },
        ])
        .await
        .unwrap();

        // when - scan only segment 1's range
        let mut iter = log.scan(Bytes::from("key"), 2..4).await.unwrap();
        let mut entries = vec![];
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }

        // then - should see only segment 1's entries
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 2);
        assert_eq!(entries[1].sequence, 3);
    }

    #[tokio::test]
    async fn should_list_keys_returns_iterator() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("key-a"),
                value: Bytes::from("value-a"),
            },
            Record {
                key: Bytes::from("key-b"),
                value: Bytes::from("value-b"),
            },
        ])
        .await
        .unwrap();

        // when
        let _iter = log.list(..).await.unwrap();

        // then - iterator is returned (full iteration tested when LogKeyIterator is implemented)
    }

    #[tokio::test]
    async fn should_list_with_options_returns_iterator() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        }])
        .await
        .unwrap();

        // when
        let _iter = log
            .list_with_options(.., ListOptions::default())
            .await
            .unwrap();

        // then - iterator is returned
    }

    #[tokio::test]
    async fn should_list_via_log_reader() {
        // given - create shared storage
        let storage = create_storage(&StorageConfig::InMemory, None)
            .await
            .unwrap();
        let log = Log::new(storage.clone()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("key-a"),
                value: Bytes::from("value-a"),
            },
            Record {
                key: Bytes::from("key-b"),
                value: Bytes::from("value-b"),
            },
        ])
        .await
        .unwrap();

        // when - create LogReader sharing the same storage
        let reader = LogReader::new(storage).await.unwrap();
        let _iter = reader.list(..).await.unwrap();

        // then - iterator is returned
    }

    #[tokio::test]
    async fn should_list_keys_in_single_segment() {
        // given
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("key-a"),
                value: Bytes::from("value-a"),
            },
            Record {
                key: Bytes::from("key-b"),
                value: Bytes::from("value-b"),
            },
            Record {
                key: Bytes::from("key-c"),
                value: Bytes::from("value-c"),
            },
        ])
        .await
        .unwrap();

        // when
        let mut iter = log.list(..).await.unwrap();
        let mut keys = vec![];
        while let Some(key) = iter.next().await.unwrap() {
            keys.push(key.key);
        }

        // then - keys returned in lexicographic order
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], Bytes::from("key-a"));
        assert_eq!(keys[1], Bytes::from("key-b"));
        assert_eq!(keys[2], Bytes::from("key-c"));
    }

    #[tokio::test]
    async fn should_list_keys_across_segments_after_roll() {
        // given - log with entries across multiple segments
        let log = Log::open(test_config()).await.unwrap();

        // write to segment 0
        log.append(vec![
            Record {
                key: Bytes::from("key-a"),
                value: Bytes::from("value-a-0"),
            },
            Record {
                key: Bytes::from("key-b"),
                value: Bytes::from("value-b-0"),
            },
        ])
        .await
        .unwrap();

        // seal and create segment 1
        log.seal_segment().await.unwrap();

        // write to segment 1 with different keys
        log.append(vec![
            Record {
                key: Bytes::from("key-c"),
                value: Bytes::from("value-c-1"),
            },
            Record {
                key: Bytes::from("key-d"),
                value: Bytes::from("value-d-1"),
            },
        ])
        .await
        .unwrap();

        // when
        let mut iter = log.list(..).await.unwrap();
        let mut keys = vec![];
        while let Some(key) = iter.next().await.unwrap() {
            keys.push(key.key);
        }

        // then - all keys from both segments
        assert_eq!(keys.len(), 4);
        assert_eq!(keys[0], Bytes::from("key-a"));
        assert_eq!(keys[1], Bytes::from("key-b"));
        assert_eq!(keys[2], Bytes::from("key-c"));
        assert_eq!(keys[3], Bytes::from("key-d"));
    }

    #[tokio::test]
    async fn should_deduplicate_keys_across_segments() {
        // given - same key written to multiple segments
        let log = Log::open(test_config()).await.unwrap();

        // write to segment 0
        log.append(vec![Record {
            key: Bytes::from("shared-key"),
            value: Bytes::from("value-0"),
        }])
        .await
        .unwrap();

        // seal and create segment 1
        log.seal_segment().await.unwrap();

        // write same key to segment 1
        log.append(vec![Record {
            key: Bytes::from("shared-key"),
            value: Bytes::from("value-1"),
        }])
        .await
        .unwrap();

        // seal and create segment 2
        log.seal_segment().await.unwrap();

        // write same key to segment 2
        log.append(vec![Record {
            key: Bytes::from("shared-key"),
            value: Bytes::from("value-2"),
        }])
        .await
        .unwrap();

        // when
        let mut iter = log.list(..).await.unwrap();
        let mut keys = vec![];
        while let Some(key) = iter.next().await.unwrap() {
            keys.push(key.key);
        }

        // then - key appears only once despite being in 3 segments
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], Bytes::from("shared-key"));
    }

    #[tokio::test]
    async fn should_list_keys_in_lexicographic_order() {
        // given - keys inserted out of order
        let log = Log::open(test_config()).await.unwrap();
        log.append(vec![
            Record {
                key: Bytes::from("zebra"),
                value: Bytes::from("value"),
            },
            Record {
                key: Bytes::from("apple"),
                value: Bytes::from("value"),
            },
            Record {
                key: Bytes::from("mango"),
                value: Bytes::from("value"),
            },
        ])
        .await
        .unwrap();

        // when
        let mut iter = log.list(..).await.unwrap();
        let mut keys = vec![];
        while let Some(key) = iter.next().await.unwrap() {
            keys.push(key.key);
        }

        // then - sorted lexicographically
        assert_eq!(keys[0], Bytes::from("apple"));
        assert_eq!(keys[1], Bytes::from("mango"));
        assert_eq!(keys[2], Bytes::from("zebra"));
    }

    #[tokio::test]
    async fn should_list_empty_when_no_entries() {
        // given
        let log = Log::open(test_config()).await.unwrap();

        // when
        let mut iter = log.list(..).await.unwrap();

        // then
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_list_keys_respects_sequence_range() {
        // given - entries in different segments with known sequences
        let log = Log::open(test_config()).await.unwrap();

        // segment 0: sequences 0, 1
        log.append(vec![
            Record {
                key: Bytes::from("key-seg0"),
                value: Bytes::from("value"),
            },
            Record {
                key: Bytes::from("key-seg0-b"),
                value: Bytes::from("value"),
            },
        ])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 1: sequences 2, 3
        log.append(vec![
            Record {
                key: Bytes::from("key-seg1"),
                value: Bytes::from("value"),
            },
            Record {
                key: Bytes::from("key-seg1-b"),
                value: Bytes::from("value"),
            },
        ])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 2: sequences 4, 5
        log.append(vec![
            Record {
                key: Bytes::from("key-seg2"),
                value: Bytes::from("value"),
            },
            Record {
                key: Bytes::from("key-seg2-b"),
                value: Bytes::from("value"),
            },
        ])
        .await
        .unwrap();

        // when - list only keys from segment 1 (sequences 2..4)
        let mut iter = log.list(2..4).await.unwrap();
        let mut keys = vec![];
        while let Some(key) = iter.next().await.unwrap() {
            keys.push(key.key);
        }

        // then - only keys from segment 1
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], Bytes::from("key-seg1"));
        assert_eq!(keys[1], Bytes::from("key-seg1-b"));
    }
}

//! Core Log implementation with read and write APIs.
//!
//! This module provides the [`Log`] struct, the primary entry point for
//! interacting with OpenData Log. It exposes both write operations ([`append`])
//! and read operations ([`scan`], [`count`]) via the [`LogRead`] trait.

use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use common::storage::factory::create_storage;
use common::{
    BytesRange, Record as StorageRecord, Storage, StorageIterator, StorageRead,
    WriteOptions as StorageWriteOptions,
};

use crate::config::{CountOptions, ScanOptions, WriteOptions};
use crate::error::{Error, Result};
use crate::model::{LogEntry, Record};
use crate::reader::LogRead;
use crate::sequence::{SeqBlockStore, SequenceAllocator};
use crate::serde::LogEntryKey;

/// An iterator over log entries for a specific key.
///
/// Created by [`LogRead::scan`] or [`LogRead::scan_with_options`]. Yields
/// entries in sequence number order within the specified range.
///
/// # Streaming Behavior
///
/// The iterator fetches entries lazily as they are consumed. Large scans
/// do not load all entries into memory at once.
///
/// # Example
///
/// ```ignore
/// let mut iter = log.scan(Bytes::from("orders"), 100..);
/// while let Some(entry) = iter.next().await? {
///     process_entry(entry);
/// }
/// ```
pub struct LogIterator {
    /// The underlying storage iterator
    inner: Box<dyn StorageIterator + Send>,
}

impl LogIterator {
    /// Opens a new LogIterator for the given storage and key range.
    ///
    /// This initializes the underlying storage iterator immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage iterator cannot be created.
    pub(crate) async fn open(storage: Arc<dyn StorageRead>, range: BytesRange) -> Result<Self> {
        let inner = storage
            .scan_iter(range)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Advances the iterator and returns the next log entry.
    ///
    /// Returns `Ok(Some(entry))` if there is another entry in the range,
    /// `Ok(None)` if the iteration is complete, or `Err` if an error occurred.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a storage failure while reading entries.
    pub async fn next(&mut self) -> Result<Option<LogEntry>> {
        let inner = &mut self.inner;

        // Get next record from storage
        let Some(record) = inner
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        else {
            return Ok(None);
        };

        // Decode the key to get the user key and sequence
        let entry_key = LogEntryKey::decode(&record.key)?;

        Ok(Some(LogEntry {
            key: entry_key.key,
            sequence: entry_key.sequence,
            value: record.value,
        }))
    }
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
    storage: Arc<dyn Storage>,
    sequence_allocator: SequenceAllocator,
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

        let block_store = SeqBlockStore::new(Arc::clone(&storage));
        let sequence_allocator = SequenceAllocator::new(block_store);
        sequence_allocator.initialize().await?;

        Ok(Self {
            storage,
            sequence_allocator,
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

        // Allocate sequence numbers for all records in the batch
        let base_sequence = self
            .sequence_allocator
            .allocate(records.len() as u64)
            .await?;

        // Build storage records with encoded keys
        let storage_records: Vec<StorageRecord> = records
            .into_iter()
            .enumerate()
            .map(|(i, record)| {
                let sequence = base_sequence + i as u64;
                let entry_key = LogEntryKey::new(record.key, sequence);
                StorageRecord::new(entry_key.encode(), record.value)
            })
            .collect();

        // Convert log write options to storage write options
        let storage_options = StorageWriteOptions {
            await_durable: options.await_durable,
        };

        // Write to storage
        self.storage
            .put_with_options(storage_records, storage_options)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        Ok(())
    }

    /// Creates a Log from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new(storage: Arc<dyn Storage>) -> Result<Self> {
        let block_store = SeqBlockStore::new(Arc::clone(&storage));
        let sequence_allocator = SequenceAllocator::new(block_store);
        sequence_allocator.initialize().await?;

        Ok(Self {
            storage,
            sequence_allocator,
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
        let range = LogEntryKey::scan_range(&key, seq_range);
        LogIterator::open(Arc::clone(&self.storage) as Arc<dyn StorageRead>, range).await
    }

    async fn count_with_options(
        &self,
        _key: Bytes,
        _seq_range: impl RangeBounds<u64> + Send,
        _options: CountOptions,
    ) -> Result<u64> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use common::{BytesRange, StorageConfig};

    use super::*;
    use crate::config::Config;

    fn test_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
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
        let result = log.append(records).await;

        // then
        assert!(result.is_ok());

        // verify record was stored
        let stored = log.storage.scan(BytesRange::unbounded()).await.unwrap();
        // Should have 2 records: the SeqBlock and the LogEntry
        assert_eq!(stored.len(), 2);
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
        let result = log.append(records).await;

        // then
        assert!(result.is_ok());

        // verify records were stored with sequential sequence numbers
        let stored = log.storage.scan(BytesRange::unbounded()).await.unwrap();
        // Should have 4 records: 1 SeqBlock + 3 LogEntries
        assert_eq!(stored.len(), 4);

        // Decode and verify sequence numbers
        let log_entries: Vec<_> = stored
            .iter()
            .filter_map(|r| LogEntryKey::decode(&r.key).ok())
            .collect();
        assert_eq!(log_entries.len(), 3);
        assert_eq!(log_entries[0].sequence, 0);
        assert_eq!(log_entries[1].sequence, 1);
        assert_eq!(log_entries[2].sequence, 2);
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

        // verify no records were stored (except possibly none)
        let stored = log.storage.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(stored.len(), 0);
    }

    #[tokio::test]
    async fn should_assign_sequential_sequences_across_appends() {
        // given
        let log = Log::open(test_config()).await.unwrap();

        // when - first append
        log.append(vec![
            Record {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
            },
            Record {
                key: Bytes::from("key2"),
                value: Bytes::from("value2"),
            },
        ])
        .await
        .unwrap();

        // when - second append
        log.append(vec![Record {
            key: Bytes::from("key3"),
            value: Bytes::from("value3"),
        }])
        .await
        .unwrap();

        // then - verify sequences are 0, 1, 2
        let stored = log.storage.scan(BytesRange::unbounded()).await.unwrap();

        let mut log_entries: Vec<_> = stored
            .iter()
            .filter_map(|r| LogEntryKey::decode(&r.key).ok())
            .collect();
        log_entries.sort_by_key(|e| e.sequence);

        assert_eq!(log_entries.len(), 3);
        assert_eq!(log_entries[0].sequence, 0);
        assert_eq!(log_entries[1].sequence, 1);
        assert_eq!(log_entries[2].sequence, 2);
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

        // then - verify keys and values are correctly stored
        let stored = log.storage.scan(BytesRange::unbounded()).await.unwrap();

        let log_records: Vec<_> = stored
            .iter()
            .filter_map(|r| {
                LogEntryKey::decode(&r.key)
                    .ok()
                    .map(|k| (k, r.value.clone()))
            })
            .collect();

        assert_eq!(log_records.len(), 2);

        // Find by sequence and verify
        let entry_0 = log_records.iter().find(|(k, _)| k.sequence == 0).unwrap();
        assert_eq!(entry_0.0.key, Bytes::from("topic-a"));
        assert_eq!(entry_0.1, Bytes::from("message-a"));

        let entry_1 = log_records.iter().find(|(k, _)| k.sequence == 1).unwrap();
        assert_eq!(entry_1.0.key, Bytes::from("topic-b"));
        assert_eq!(entry_1.1, Bytes::from("message-b"));
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
        use crate::reader::LogReader;
        use common::storage::factory::create_storage;

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
        let reader = LogReader::new(storage);
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
}

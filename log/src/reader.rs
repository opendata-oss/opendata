//! Read-only log access and the [`LogRead`] trait.
//!
//! This module provides:
//! - [`LogRead`]: The trait defining read operations on the log.
//! - [`LogReader`]: A read-only view of the log that implements `LogRead`.

use std::ops::{Range, RangeBounds};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;

use common::StorageRead;
use common::storage::factory::create_storage;

use crate::config::{Config, CountOptions, ListOptions, ScanOptions, SegmentConfig};
use crate::error::{Error, Result};
use crate::listing::LogKeyIterator;
use crate::model::LogEntry;
use crate::range::normalize;
use crate::segment::{LogSegment, SegmentCache};
use crate::storage::{LogStorageRead, SegmentIterator};

/// Trait for read operations on the log.
///
/// This trait defines the common read interface shared by [`Log`](crate::Log)
/// and [`LogReader`]. It provides methods for scanning entries and counting
/// records within a key's log.
///
/// # Implementors
///
/// - [`Log`](crate::Log): The main log interface with both read and write access.
/// - [`LogReader`]: A read-only view of the log.
///
/// # Example
///
/// ```ignore
/// use log::LogRead;
/// use bytes::Bytes;
///
/// async fn process_log(reader: &impl LogRead) -> Result<()> {
///     // Works with both Log and LogReader
///     let mut iter = reader.scan(Bytes::from("orders"), ..);
///     while let Some(entry) = iter.next().await? {
///         println!("seq={}: {:?}", entry.sequence, entry.value);
///     }
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait LogRead {
    /// Scans entries for a key within a sequence number range.
    ///
    /// Returns an iterator that yields entries in sequence number order.
    /// The range is specified using Rust's standard range syntax.
    ///
    /// This method uses default scan options. Use [`scan_with_options`] for
    /// custom read behavior.
    ///
    /// # Read Visibility
    ///
    /// An active scan may or may not see records appended after the initial
    /// call. However, all records returned will always respect the correct
    /// ordering of records (no reordering).
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to scan.
    /// * `seq_range` - The sequence number range to scan. Supports all Rust
    ///   range types (`..`, `start..`, `..end`, `start..end`, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails due to storage issues.
    ///
    /// [`scan_with_options`]: LogRead::scan_with_options
    async fn scan(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
    ) -> Result<LogIterator> {
        self.scan_with_options(key, seq_range, ScanOptions::default())
            .await
    }

    /// Scans entries for a key within a sequence number range with custom options.
    ///
    /// Returns an iterator that yields entries in sequence number order.
    /// See [`scan`](LogRead::scan) for read visibility semantics.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to scan.
    /// * `seq_range` - The sequence number range to scan.
    /// * `options` - Scan options controlling read behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails due to storage issues.
    async fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        options: ScanOptions,
    ) -> Result<LogIterator>;

    /// Counts entries for a key within a sequence number range.
    ///
    /// Returns the number of entries in the specified range. This is useful
    /// for computing lag (how far behind a consumer is) or progress metrics.
    ///
    /// This method uses default count options (exact count). Use
    /// [`count_with_options`] for approximate counts.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to count.
    /// * `seq_range` - The sequence number range to count.
    ///
    /// # Errors
    ///
    /// Returns an error if the count fails due to storage issues.
    ///
    /// [`count_with_options`]: LogRead::count_with_options
    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<u64> + Send) -> Result<u64> {
        self.count_with_options(key, seq_range, CountOptions::default())
            .await
    }

    /// Counts entries for a key within a sequence number range with custom options.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to count.
    /// * `seq_range` - The sequence number range to count.
    /// * `options` - Count options, including whether to return an approximate count.
    ///
    /// # Errors
    ///
    /// Returns an error if the count fails due to storage issues.
    async fn count_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        options: CountOptions,
    ) -> Result<u64>;

    /// Lists distinct keys within a sequence number range.
    ///
    /// Returns an iterator over keys that have entries in the specified range.
    /// Each key is returned exactly once, even if it appears in multiple segments.
    ///
    /// This method uses default list options. Use [`list_with_options`] for
    /// custom behavior.
    ///
    /// # Arguments
    ///
    /// * `seq_range` - The sequence number range to list keys from.
    ///
    /// # Errors
    ///
    /// Returns an error if the list operation fails due to storage issues.
    ///
    /// [`list_with_options`]: LogRead::list_with_options
    async fn list(&self, seq_range: impl RangeBounds<u64> + Send) -> Result<LogKeyIterator> {
        self.list_with_options(seq_range, ListOptions::default())
            .await
    }

    /// Lists distinct keys within a sequence number range with custom options.
    ///
    /// Returns an iterator over keys that have entries in the specified range.
    /// Each key is returned exactly once, even if it appears in multiple segments.
    ///
    /// # Arguments
    ///
    /// * `seq_range` - The sequence number range to list keys from.
    /// * `options` - List options controlling behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the list operation fails due to storage issues.
    async fn list_with_options(
        &self,
        seq_range: impl RangeBounds<u64> + Send,
        options: ListOptions,
    ) -> Result<LogKeyIterator>;
}

/// A read-only view of the log.
///
/// `LogReader` provides access to all read operations via the [`LogRead`]
/// trait, but not write operations. This is useful for:
///
/// - Consumers that should not have write access
/// - Sharing read access across multiple components
/// - Separating read and write concerns in your application
///
/// # Obtaining a LogReader
///
/// A `LogReader` is created by calling [`LogReader::open`]:
///
/// ```ignore
/// let reader = LogReader::open(config).await?;
/// ```
///
/// # Thread Safety
///
/// `LogReader` is designed to be cloned and shared across threads.
/// All methods take `&self` and are safe to call concurrently.
///
/// # Example
///
/// ```ignore
/// use log::{LogReader, LogRead};
/// use bytes::Bytes;
///
/// async fn consume_events(reader: LogReader, key: Bytes) -> Result<()> {
///     let mut checkpoint: u64 = 0;
///
///     loop {
///         let mut iter = reader.scan(key.clone(), checkpoint..);
///         while let Some(entry) = iter.next().await? {
///             process_entry(&entry);
///             checkpoint = entry.sequence + 1;
///         }
///
///         // Check how far behind we are
///         let lag = reader.count(key.clone(), checkpoint..).await?;
///         if lag == 0 {
///             // Caught up, wait for new entries
///             tokio::time::sleep(Duration::from_millis(100)).await;
///         }
///     }
/// }
/// ```
pub struct LogReader {
    storage: LogStorageRead,
    segments: RwLock<SegmentCache>,
}

impl LogReader {
    /// Opens a read-only view of the log with the given configuration.
    ///
    /// This creates a `LogReader` that can scan and count entries but cannot
    /// append new records. Use this when you only need read access to the log.
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
    /// use log::{LogReader, LogRead, Config};
    /// use bytes::Bytes;
    ///
    /// let reader = LogReader::open(config).await?;
    /// let mut iter = reader.scan(Bytes::from("orders"), ..).await?;
    /// while let Some(entry) = iter.next().await? {
    ///     println!("seq={}: {:?}", entry.sequence, entry.value);
    /// }
    /// ```
    pub async fn open(config: Config) -> Result<Self> {
        let storage: Arc<dyn StorageRead> = create_storage(&config.storage, None)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let log_storage = LogStorageRead::new(storage);
        let segments = SegmentCache::open(&log_storage, SegmentConfig::default()).await?;
        Ok(Self {
            storage: log_storage,
            segments: RwLock::new(segments),
        })
    }

    /// Creates a LogReader from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new(storage: Arc<dyn StorageRead>) -> Result<Self> {
        let log_storage = LogStorageRead::new(storage);
        let segments = SegmentCache::open(&log_storage, SegmentConfig::default()).await?;
        Ok(Self {
            storage: log_storage,
            segments: RwLock::new(segments),
        })
    }
}

#[async_trait]
impl LogRead for LogReader {
    async fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        _options: ScanOptions,
    ) -> Result<LogIterator> {
        let seq_range = normalize(&seq_range);
        let segments = self.segments.read().await;
        Ok(LogIterator::open(
            self.storage.clone(),
            &segments,
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
        let segments = self.segments.read().await.find_covering(&seq_range);
        list_keys_in_segments(&self.storage, &segments).await
    }
}

/// Converts a list of segments to a segment ID range.
///
/// Returns a range from the first segment ID to one past the last segment ID.
/// If the list is empty, returns an empty range (0..0).
pub(crate) fn segments_to_range(segments: &[LogSegment]) -> Range<u32> {
    if segments.is_empty() {
        return 0..0;
    }
    let start = segments.first().unwrap().id();
    let end = segments.last().unwrap().id() + 1;
    start..end
}

/// Lists keys from the given segments.
///
/// Helper for `list_with_options` implementations that handles the common
/// pattern of converting segments to a range and listing keys.
pub(crate) async fn list_keys_in_segments(
    storage: &LogStorageRead,
    segments: &[LogSegment],
) -> Result<LogKeyIterator> {
    let segment_range = segments_to_range(segments);
    storage.list_keys(segment_range).await
}

/// Iterator over log entries across multiple segments.
///
/// Iterates through segments in order, fetching entries for the given key
/// within the sequence range. Instantiates a `SegmentIterator` for each
/// segment as needed.
pub struct LogIterator {
    storage: LogStorageRead,
    segments: Vec<LogSegment>,
    key: Bytes,
    seq_range: Range<u64>,
    current_segment_idx: usize,
    current_iter: Option<SegmentIterator>,
}

impl LogIterator {
    /// Opens a new iterator by looking up segments covering the sequence range.
    pub(crate) fn open(
        storage: LogStorageRead,
        segment_cache: &SegmentCache,
        key: Bytes,
        seq_range: Range<u64>,
    ) -> Self {
        let segments = segment_cache.find_covering(&seq_range);
        Self {
            storage,
            segments,
            key,
            seq_range,
            current_segment_idx: 0,
            current_iter: None,
        }
    }

    /// Creates a new iterator over the given segments.
    #[cfg(test)]
    pub(crate) fn new(
        storage: LogStorageRead,
        segments: Vec<LogSegment>,
        key: Bytes,
        seq_range: Range<u64>,
    ) -> Self {
        Self {
            storage,
            segments,
            key,
            seq_range,
            current_segment_idx: 0,
            current_iter: None,
        }
    }

    /// Returns the next log entry, or None if iteration is complete.
    pub async fn next(&mut self) -> Result<Option<LogEntry>> {
        loop {
            // If we have a current iterator, try to get the next entry
            if let Some(iter) = &mut self.current_iter {
                if let Some(entry) = iter.next().await? {
                    return Ok(Some(entry));
                }
                // Current segment exhausted, move to next
                self.current_iter = None;
                self.current_segment_idx += 1;
            }

            // No current iterator, try to advance to next segment
            if !self.advance_segment().await? {
                return Ok(None);
            }
        }
    }

    /// Advances to the next segment and creates its iterator.
    ///
    /// Returns `true` if a new iterator was created, `false` if no more segments.
    async fn advance_segment(&mut self) -> Result<bool> {
        if self.current_segment_idx >= self.segments.len() {
            return Ok(false);
        }

        let segment = &self.segments[self.current_segment_idx];
        let iter = self
            .storage
            .scan_entries(segment, &self.key, self.seq_range.clone())
            .await?;
        self.current_iter = Some(iter);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::SegmentMeta;
    use crate::storage::LogStorage;

    fn entry(key: &[u8], seq: u64, value: &[u8]) -> LogEntry {
        LogEntry {
            key: Bytes::copy_from_slice(key),
            sequence: seq,
            value: Bytes::copy_from_slice(value),
        }
    }

    #[tokio::test]
    async fn should_return_none_when_no_segments() {
        let storage = LogStorage::in_memory();
        let segments = vec![];

        let mut iter =
            LogIterator::new(storage.as_read(), segments, Bytes::from("key"), 0..u64::MAX);

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_entries_in_single_segment() {
        let storage = LogStorage::in_memory();
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));
        storage
            .write_entry(&segment, &entry(b"key", 0, b"value0"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key", 1, b"value1"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key", 2, b"value2"))
            .await
            .unwrap();

        let mut iter = LogIterator::new(
            storage.as_read(),
            vec![segment],
            Bytes::from("key"),
            0..u64::MAX,
        );

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 0);
        assert_eq!(entry.value.as_ref(), b"value0");

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 1);
        assert_eq!(entry.value.as_ref(), b"value1");

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 2);
        assert_eq!(entry.value.as_ref(), b"value2");

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_entries_across_multiple_segments() {
        let storage = LogStorage::in_memory();
        let segment0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
        let segment1 = LogSegment::new(1, SegmentMeta::new(100, 2000));
        // Entries in segment 0 (start_seq = 0)
        storage
            .write_entry(&segment0, &entry(b"key", 0, b"value0"))
            .await
            .unwrap();
        storage
            .write_entry(&segment0, &entry(b"key", 1, b"value1"))
            .await
            .unwrap();
        // Entries in segment 1 (start_seq = 100)
        storage
            .write_entry(&segment1, &entry(b"key", 100, b"value100"))
            .await
            .unwrap();
        storage
            .write_entry(&segment1, &entry(b"key", 101, b"value101"))
            .await
            .unwrap();

        let mut iter = LogIterator::new(
            storage.as_read(),
            vec![segment0, segment1],
            Bytes::from("key"),
            0..u64::MAX,
        );

        // Entries from segment 0
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 0);
        assert_eq!(entry.value.as_ref(), b"value0");

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 1);
        assert_eq!(entry.value.as_ref(), b"value1");

        // Entries from segment 1
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 100);
        assert_eq!(entry.value.as_ref(), b"value100");

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 101);
        assert_eq!(entry.value.as_ref(), b"value101");

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_filter_by_sequence_range() {
        let storage = LogStorage::in_memory();
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));
        storage
            .write_entry(&segment, &entry(b"key", 0, b"value0"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key", 1, b"value1"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key", 2, b"value2"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key", 3, b"value3"))
            .await
            .unwrap();

        let mut iter = LogIterator::new(storage.as_read(), vec![segment], Bytes::from("key"), 1..3);

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 1);

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 2);

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_filter_entries_for_specified_key() {
        let storage = LogStorage::in_memory();
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));
        storage
            .write_entry(&segment, &entry(b"key1", 0, b"k1v0"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key2", 0, b"k2v0"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key1", 1, b"k1v1"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key2", 1, b"k2v1"))
            .await
            .unwrap();

        let mut iter = LogIterator::new(
            storage.as_read(),
            vec![segment],
            Bytes::from("key1"),
            0..u64::MAX,
        );

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.key.as_ref(), b"key1");
        assert_eq!(entry.sequence, 0);

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.key.as_ref(), b"key1");
        assert_eq!(entry.sequence, 1);

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_return_none_when_no_entries_in_range() {
        let storage = LogStorage::in_memory();
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));
        storage
            .write_entry(&segment, &entry(b"key", 0, b"value0"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key", 1, b"value1"))
            .await
            .unwrap();

        let mut iter =
            LogIterator::new(storage.as_read(), vec![segment], Bytes::from("key"), 10..20);

        assert!(iter.next().await.unwrap().is_none());
    }
}

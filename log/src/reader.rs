//! Read-only log access and the [`LogRead`] trait.
//!
//! This module provides:
//! - [`LogRead`]: The trait defining read operations on the log.
//! - [`LogReader`]: A read-only view of the log that implements `LogRead`.

use std::ops::{Bound, RangeBounds};

use async_trait::async_trait;
use bytes::Bytes;

use std::sync::Arc;

use common::storage::factory::create_storage;
use common::{StorageIterator, StorageRead};

use crate::config::{Config, CountOptions, ScanOptions};
use crate::error::{Error, Result};
use crate::segment::{SegmentRead, SegmentReader};
use crate::serde::LogEntryKey;

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
    segment_reader: SegmentReader,
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
        let segment_reader = SegmentReader::open(storage).await?;
        Ok(Self { segment_reader })
    }

    /// Creates a LogReader from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new(storage: Arc<dyn StorageRead>) -> Result<Self> {
        let segment_reader = SegmentReader::open(storage).await?;
        Ok(Self { segment_reader })
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
        LogIteratorBuilder::new(&self.segment_reader, key, seq_range)
            .build()
            .await
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

use crate::model::LogEntry;
use crate::segment::LogSegment;

/// Iterator over log entries within a single segment.
///
/// Wraps a `StorageIterator` and handles range validation and `LogEntry`
/// deserialization.
struct SegmentIterator {
    inner: Box<dyn StorageIterator + Send>,
    seq_range: std::ops::Range<u64>,
}

impl SegmentIterator {
    fn new(inner: Box<dyn StorageIterator + Send>, seq_range: std::ops::Range<u64>) -> Self {
        Self { inner, seq_range }
    }

    /// Returns the next log entry within the sequence range, or None if exhausted.
    async fn next(&mut self) -> Result<Option<LogEntry>> {
        loop {
            let Some(record) = self
                .inner
                .next()
                .await
                .map_err(|e| Error::Storage(e.to_string()))?
            else {
                return Ok(None);
            };

            let entry_key = LogEntryKey::deserialize(&record.key)?;

            // Skip entries outside our sequence range
            if entry_key.sequence < self.seq_range.start {
                continue;
            }
            if entry_key.sequence >= self.seq_range.end {
                return Ok(None);
            }

            return Ok(Some(LogEntry {
                key: entry_key.key,
                sequence: entry_key.sequence,
                value: record.value,
            }));
        }
    }
}

/// Converts any `RangeBounds<u64>` to a normalized `Range<u64>`.
fn normalize_range(seq_range: impl RangeBounds<u64>) -> std::ops::Range<u64> {
    let start = match seq_range.start_bound() {
        Bound::Included(&s) => s,
        Bound::Excluded(&s) => s.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match seq_range.end_bound() {
        Bound::Included(&e) => e.saturating_add(1),
        Bound::Excluded(&e) => e,
        Bound::Unbounded => u64::MAX,
    };
    start..end
}

/// Builder for creating a `LogIterator`.
///
/// Handles the conversion from `RangeBounds<u64>` to `Range<u64>` and performs
/// segment lookup when building.
pub(crate) struct LogIteratorBuilder<'a> {
    segments: &'a dyn SegmentRead,
    key: Bytes,
    range: std::ops::Range<u64>,
}

impl<'a> LogIteratorBuilder<'a> {
    /// Creates a new builder.
    pub fn new(
        segments: &'a dyn SegmentRead,
        key: Bytes,
        seq_range: impl RangeBounds<u64>,
    ) -> Self {
        Self {
            segments,
            key,
            range: normalize_range(seq_range),
        }
    }

    /// Builds the `LogIterator`, performing segment lookup.
    pub async fn build(self) -> Result<LogIterator> {
        let storage = self.segments.storage();
        let segments = self.segments.find_covering(self.range.clone()).await?;
        Ok(LogIterator {
            storage,
            segments,
            key: self.key,
            seq_range: self.range,
            current_segment_idx: 0,
            current_iter: None,
        })
    }
}

/// Iterator over log entries across multiple segments.
///
/// Iterates through segments in order, fetching entries for the given key
/// within the sequence range. Instantiates a `SegmentIterator` for each
/// segment as needed.
pub struct LogIterator {
    storage: Arc<dyn StorageRead>,
    segments: Vec<LogSegment>,
    key: Bytes,
    seq_range: std::ops::Range<u64>,
    current_segment_idx: usize,
    current_iter: Option<SegmentIterator>,
}

impl LogIterator {
    /// Creates a new iterator over the given segments.
    #[cfg(test)]
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        segments: Vec<LogSegment>,
        key: Bytes,
        seq_range: std::ops::Range<u64>,
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
        let range = self.segment_scan_range(segment);
        let inner = self
            .storage
            .scan_iter(range)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        self.current_iter = Some(SegmentIterator::new(inner, self.seq_range.clone()));
        Ok(true)
    }

    /// Computes the storage key range for scanning a segment.
    fn segment_scan_range(&self, segment: &LogSegment) -> common::BytesRange {
        LogEntryKey::scan_range(segment.id(), &self.key, self.seq_range.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::SegmentMeta;
    use common::Storage;
    use common::storage::in_memory::InMemoryStorage;

    async fn write_entry(
        storage: &InMemoryStorage,
        segment_id: u32,
        key: &[u8],
        seq: u64,
        value: &[u8],
    ) {
        let entry_key = LogEntryKey::new(segment_id, Bytes::copy_from_slice(key), seq);
        let record = common::Record {
            key: entry_key.serialize(),
            value: Bytes::copy_from_slice(value),
        };
        storage.put(vec![record]).await.unwrap();
    }

    #[tokio::test]
    async fn should_return_none_when_no_segments() {
        let storage = Arc::new(InMemoryStorage::new());
        let segments = vec![];

        let mut iter = LogIterator::new(storage, segments, Bytes::from("key"), 0..u64::MAX);

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_entries_in_single_segment() {
        let storage = Arc::new(InMemoryStorage::new());
        write_entry(&storage, 0, b"key", 0, b"value0").await;
        write_entry(&storage, 0, b"key", 1, b"value1").await;
        write_entry(&storage, 0, b"key", 2, b"value2").await;

        let segments = vec![LogSegment::new(0, SegmentMeta::new(0, 1000))];

        let mut iter = LogIterator::new(storage, segments, Bytes::from("key"), 0..u64::MAX);

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
        let storage = Arc::new(InMemoryStorage::new());
        // Entries in segment 0
        write_entry(&storage, 0, b"key", 0, b"value0").await;
        write_entry(&storage, 0, b"key", 1, b"value1").await;
        // Entries in segment 1
        write_entry(&storage, 1, b"key", 100, b"value100").await;
        write_entry(&storage, 1, b"key", 101, b"value101").await;

        let segments = vec![
            LogSegment::new(0, SegmentMeta::new(0, 1000)),
            LogSegment::new(1, SegmentMeta::new(100, 2000)),
        ];

        let mut iter = LogIterator::new(storage, segments, Bytes::from("key"), 0..u64::MAX);

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
        let storage = Arc::new(InMemoryStorage::new());
        write_entry(&storage, 0, b"key", 0, b"value0").await;
        write_entry(&storage, 0, b"key", 1, b"value1").await;
        write_entry(&storage, 0, b"key", 2, b"value2").await;
        write_entry(&storage, 0, b"key", 3, b"value3").await;

        let segments = vec![LogSegment::new(0, SegmentMeta::new(0, 1000))];

        let mut iter = LogIterator::new(storage, segments, Bytes::from("key"), 1..3);

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 1);

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 2);

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_filter_entries_for_specified_key() {
        let storage = Arc::new(InMemoryStorage::new());
        write_entry(&storage, 0, b"key1", 0, b"k1v0").await;
        write_entry(&storage, 0, b"key2", 0, b"k2v0").await;
        write_entry(&storage, 0, b"key1", 1, b"k1v1").await;
        write_entry(&storage, 0, b"key2", 1, b"k2v1").await;

        let segments = vec![LogSegment::new(0, SegmentMeta::new(0, 1000))];

        let mut iter = LogIterator::new(storage, segments, Bytes::from("key1"), 0..u64::MAX);

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
        let storage = Arc::new(InMemoryStorage::new());
        write_entry(&storage, 0, b"key", 0, b"value0").await;
        write_entry(&storage, 0, b"key", 1, b"value1").await;

        let segments = vec![LogSegment::new(0, SegmentMeta::new(0, 1000))];

        let mut iter = LogIterator::new(storage, segments, Bytes::from("key"), 10..20);

        assert!(iter.next().await.unwrap().is_none());
    }

    mod normalize_range_tests {
        use super::*;

        #[test]
        fn should_normalize_full_range() {
            let range = normalize_range(..);
            assert_eq!(range, 0..u64::MAX);
        }

        #[test]
        fn should_normalize_range_from() {
            let range = normalize_range(100..);
            assert_eq!(range, 100..u64::MAX);
        }

        #[test]
        fn should_normalize_range_to() {
            let range = normalize_range(..100);
            assert_eq!(range, 0..100);
        }

        #[test]
        fn should_normalize_range() {
            let range = normalize_range(50..150);
            assert_eq!(range, 50..150);
        }

        #[test]
        fn should_normalize_range_inclusive() {
            let range = normalize_range(50..=150);
            assert_eq!(range, 50..151);
        }

        #[test]
        fn should_normalize_range_to_inclusive() {
            let range = normalize_range(..=100);
            assert_eq!(range, 0..101);
        }

        #[test]
        fn should_handle_max_value_inclusive() {
            let range = normalize_range(0..=u64::MAX);
            // saturating_add prevents overflow
            assert_eq!(range, 0..u64::MAX);
        }

        #[test]
        fn should_handle_excluded_start() {
            use std::ops::Bound;
            let range = normalize_range((Bound::Excluded(10), Bound::Unbounded));
            assert_eq!(range, 11..u64::MAX);
        }
    }
}

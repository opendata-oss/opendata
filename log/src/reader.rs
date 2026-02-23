//! Read-only log access and the [`LogRead`] trait.
//!
//! This module provides:
//! - [`LogRead`]: The trait defining read operations on the log.
//! - [`LogDbReader`]: A read-only view of the log that implements `LogRead`.

use std::ops::{Range, RangeBounds};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::config::{CountOptions, ReaderConfig, ScanOptions, SegmentConfig};
use crate::error::{Error, Result};
use crate::listing::LogKeyIterator;
use crate::model::{LogEntry, Segment, SegmentId, Sequence};
use crate::range::{normalize_segment_id, normalize_sequence};
use crate::segment::{LogSegment, SegmentCache};
use crate::storage::{LogStorageRead as _, SegmentIterator};
use common::storage::factory::create_storage_read;
use common::{StorageRead, StorageSemantics};

/// Trait for read operations on the log.
///
/// This trait defines the common read interface shared by [`LogDb`](crate::LogDb)
/// and [`LogDbReader`]. It provides methods for scanning entries and counting
/// records within a key's log.
///
/// # Implementors
///
/// - [`LogDb`](crate::LogDb): The main log interface with both read and write access.
/// - [`LogDbReader`]: A read-only view of the log.
///
/// # Example
///
/// ```ignore
/// use log::LogRead;
/// use bytes::Bytes;
///
/// async fn process_log(reader: &impl LogRead) -> Result<()> {
///     // Works with both LogDb and LogDbReader
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
        seq_range: impl RangeBounds<Sequence> + Send,
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
        seq_range: impl RangeBounds<Sequence> + Send,
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
    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<Sequence> + Send) -> Result<u64> {
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
        seq_range: impl RangeBounds<Sequence> + Send,
        options: CountOptions,
    ) -> Result<u64>;

    /// Lists distinct keys within a segment range.
    ///
    /// Returns an iterator over keys that have entries in the specified segments.
    /// Each key is returned exactly once, even if it appears in multiple segments.
    ///
    /// Pass `..` to list keys from all segments.
    ///
    /// # Arguments
    ///
    /// * `segment_range` - The segment ID range to list keys from.
    ///
    /// # Errors
    ///
    /// Returns an error if the list operation fails due to storage issues.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // List all keys
    /// let mut iter = log.list_keys(..).await?;
    ///
    /// // List keys from specific segments
    /// let segments = log.list_segments(100..200).await?;
    /// let start = segments.first().map(|s| s.id).unwrap_or(0);
    /// let end = segments.last().map(|s| s.id + 1).unwrap_or(0);
    /// let mut iter = log.list_keys(start..end).await?;
    /// ```
    async fn list_keys(
        &self,
        segment_range: impl RangeBounds<SegmentId> + Send,
    ) -> Result<LogKeyIterator>;

    /// Lists segments overlapping a sequence number range.
    ///
    /// Returns all segments that overlap the specified sequence range. This is
    /// a precise operation—segments have well-defined boundaries, so there is
    /// no approximation.
    ///
    /// Pass `..` to list all segments.
    ///
    /// # Arguments
    ///
    /// * `seq_range` - The sequence number range to filter segments by.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails due to storage issues.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // List all segments
    /// let segments = log.list_segments(..).await?;
    ///
    /// // List segments overlapping a specific range
    /// let segments = log.list_segments(100..200).await?;
    /// ```
    async fn list_segments(
        &self,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> Result<Vec<Segment>>;
}

/// Shared read component used by both `LogDb` and `LogDbReader`.
///
/// Contains the storage and segment cache needed for read operations.
/// Wrapped in `Arc<RwLock<_>>` by both consumers.
pub(crate) struct LogReadView {
    pub(crate) storage: Arc<dyn StorageRead>,
    pub(crate) segments: SegmentCache,
}

impl LogReadView {
    /// Creates a new `LogReadView`.
    pub(crate) fn new(storage: Arc<dyn StorageRead>, segments: SegmentCache) -> Self {
        Self { storage, segments }
    }

    /// Replaces the underlying storage snapshot with a new one.
    pub(crate) fn update_snapshot(&mut self, snapshot: Arc<dyn StorageRead>) {
        self.storage = snapshot;
    }

    /// Inserts new segments into the segment cache.
    pub(crate) fn apply_new_segments(&mut self, segments: &[LogSegment]) {
        for segment in segments {
            self.segments.insert(segment.clone());
        }
    }

    /// Scans entries for a key within a sequence number range with custom options.
    pub(crate) fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: Range<Sequence>,
        _options: &ScanOptions,
    ) -> LogIterator {
        LogIterator::open(Arc::clone(&self.storage), &self.segments, key, seq_range)
    }

    /// Lists distinct keys within a segment range.
    pub(crate) async fn list_keys(
        &self,
        segment_range: Range<SegmentId>,
    ) -> Result<LogKeyIterator> {
        let keys = self.storage.list_keys(segment_range).await?;
        Ok(LogKeyIterator::from_keys(keys))
    }

    /// Lists segments overlapping a sequence number range.
    pub(crate) fn list_segments(&self, seq_range: &Range<Sequence>) -> Vec<Segment> {
        self.segments
            .find_covering(seq_range)
            .into_iter()
            .map(|s| s.into())
            .collect()
    }
}

/// A read-only view of the log.
///
/// `LogDbReader` provides access to all read operations via the [`LogRead`]
/// trait, but not write operations. This is useful for:
///
/// - Consumers that should not have write access
/// - Sharing read access across multiple components
/// - Separating read and write concerns in your application
///
/// # Obtaining a LogDbReader
///
/// A `LogDbReader` is created by calling [`LogDbReader::open`]:
///
/// ```ignore
/// let reader = LogDbReader::open(config).await?;
/// ```
///
/// # Thread Safety
///
/// `LogDbReader` is designed to be cloned and shared across threads.
/// All methods take `&self` and are safe to call concurrently.
///
/// # Example
///
/// ```ignore
/// use log::{LogDbReader, LogRead};
/// use bytes::Bytes;
///
/// async fn consume_events(reader: LogDbReader, key: Bytes) -> Result<()> {
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
pub struct LogDbReader {
    read_view: Arc<RwLock<LogReadView>>,
    shutdown_tx: watch::Sender<bool>,
    refresh_task: Option<JoinHandle<()>>,
}

impl LogDbReader {
    /// Opens a read-only view of the log with the given configuration.
    ///
    /// This creates a `LogDbReader` that can scan and count entries but cannot
    /// append new records. Use this when you only need read access to the log.
    ///
    /// When `refresh_interval` is set, the reader periodically discovers new
    /// data written by other processes.
    ///
    /// # Arguments
    ///
    /// * `config` - Reader configuration including storage and refresh settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use log::{LogDbReader, LogRead, ReaderConfig};
    /// use common::StorageConfig;
    /// use bytes::Bytes;
    ///
    /// let config = ReaderConfig {
    ///     storage: StorageConfig::default(),
    ///     ..Default::default()
    /// };
    /// let reader = LogDbReader::open(config).await?;
    ///
    /// // Reader will automatically discover new data
    /// let mut iter = reader.scan(Bytes::from("orders"), ..).await?;
    /// while let Some(entry) = iter.next().await? {
    ///     println!("seq={}: {:?}", entry.sequence, entry.value);
    /// }
    ///
    /// // Gracefully shut down when done
    /// reader.close().await;
    /// ```
    pub async fn open(config: ReaderConfig) -> Result<Self> {
        let reader_options = slatedb::config::DbReaderOptions {
            manifest_poll_interval: config.refresh_interval,
            ..Default::default()
        };
        let storage: Arc<dyn StorageRead> =
            create_storage_read(&config.storage, StorageSemantics::new(), reader_options)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        let segments = SegmentCache::open(storage.as_ref(), SegmentConfig::default()).await?;
        let read_view = Arc::new(RwLock::new(LogReadView::new(storage, segments)));

        let (shutdown_tx, refresh_task) =
            Self::spawn_refresh_task(Arc::clone(&read_view), config.refresh_interval);

        Ok(Self {
            read_view,
            shutdown_tx,
            refresh_task: Some(refresh_task),
        })
    }

    /// Spawns a background task that periodically refreshes the segment cache.
    fn spawn_refresh_task(
        read_view: Arc<RwLock<LogReadView>>,
        interval: Duration,
    ) -> (watch::Sender<bool>, JoinHandle<()>) {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Get the latest segment ID for incremental refresh
                        let after_segment_id = {
                            let view = read_view.read().await;
                            view.segments.latest().map(|s| s.id())
                        };

                        // Refresh the cache
                        let mut view = read_view.write().await;
                        let storage = Arc::clone(&view.storage);
                        if let Err(e) = view.segments.refresh(storage.as_ref(), after_segment_id).await {
                            tracing::warn!("Failed to refresh segment cache: {}", e);
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
        });

        (shutdown_tx, task)
    }

    /// Creates a LogDbReader from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new(storage: Arc<dyn StorageRead>) -> Result<Self> {
        let segments = SegmentCache::open(storage.as_ref(), SegmentConfig::default()).await?;
        let read_view = Arc::new(RwLock::new(LogReadView::new(storage, segments)));
        let (shutdown_tx, _) = watch::channel(false);
        Ok(Self {
            read_view,
            shutdown_tx,
            refresh_task: None,
        })
    }

    /// Closes the reader, stopping the background refresh task.
    ///
    /// This method consumes `self` and gracefully shuts down the background
    /// refresh task. It waits up to 5 seconds for the task to complete.
    pub async fn close(self) {
        // Signal shutdown
        let _ = self.shutdown_tx.send(true);

        // Wait for the task to complete with timeout
        if let Some(task) = self.refresh_task {
            let timeout = tokio::time::timeout(Duration::from_secs(5), task).await;
            if timeout.is_err() {
                tracing::warn!("Refresh task did not stop within timeout");
            }
        }
    }
}

#[async_trait]
impl LogRead for LogDbReader {
    async fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<Sequence> + Send,
        options: ScanOptions,
    ) -> Result<LogIterator> {
        let seq_range = normalize_sequence(&seq_range);
        let view = self.read_view.read().await;
        Ok(view.scan_with_options(key, seq_range, &options))
    }

    async fn count_with_options(
        &self,
        _key: Bytes,
        _seq_range: impl RangeBounds<Sequence> + Send,
        _options: CountOptions,
    ) -> Result<u64> {
        todo!()
    }

    async fn list_keys(
        &self,
        segment_range: impl RangeBounds<SegmentId> + Send,
    ) -> Result<LogKeyIterator> {
        let segment_range = normalize_segment_id(&segment_range);
        let view = self.read_view.read().await;
        view.list_keys(segment_range).await
    }

    async fn list_segments(
        &self,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> Result<Vec<Segment>> {
        let seq_range = normalize_sequence(&seq_range);
        let view = self.read_view.read().await;
        Ok(view.list_segments(&seq_range))
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
    seq_range: Range<Sequence>,
    current_segment_idx: usize,
    current_iter: Option<SegmentIterator>,
}

impl LogIterator {
    /// Opens a new iterator by looking up segments covering the sequence range.
    pub(crate) fn open(
        storage: Arc<dyn StorageRead>,
        segment_cache: &SegmentCache,
        key: Bytes,
        seq_range: Range<Sequence>,
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
        storage: Arc<dyn StorageRead>,
        segments: Vec<LogSegment>,
        key: Bytes,
        seq_range: Range<Sequence>,
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
    use crate::storage::{LogStorageWrite, in_memory_storage};

    fn entry(key: &[u8], seq: u64, value: &[u8]) -> LogEntry {
        LogEntry {
            key: Bytes::copy_from_slice(key),
            sequence: seq,
            value: Bytes::copy_from_slice(value),
        }
    }

    #[tokio::test]
    async fn should_return_none_when_no_segments() {
        let storage = in_memory_storage();
        let segments = vec![];

        let mut iter = LogIterator::new(
            storage.clone() as Arc<dyn StorageRead>,
            segments,
            Bytes::from("key"),
            0..u64::MAX,
        );

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_iterate_entries_in_single_segment() {
        let storage = in_memory_storage();
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
            storage.clone() as Arc<dyn StorageRead>,
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
        let storage = in_memory_storage();
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
            storage.clone() as Arc<dyn StorageRead>,
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
        let storage = in_memory_storage();
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

        let mut iter = LogIterator::new(
            storage.clone() as Arc<dyn StorageRead>,
            vec![segment],
            Bytes::from("key"),
            1..3,
        );

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 1);

        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.sequence, 2);

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_filter_entries_for_specified_key() {
        let storage = in_memory_storage();
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
            storage.clone() as Arc<dyn StorageRead>,
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
        let storage = in_memory_storage();
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));
        storage
            .write_entry(&segment, &entry(b"key", 0, b"value0"))
            .await
            .unwrap();
        storage
            .write_entry(&segment, &entry(b"key", 1, b"value1"))
            .await
            .unwrap();

        let mut iter = LogIterator::new(
            storage.clone() as Arc<dyn StorageRead>,
            vec![segment],
            Bytes::from("key"),
            10..20,
        );

        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn open_spawns_refresh_task() {
        use common::StorageConfig;

        let config = ReaderConfig {
            storage: StorageConfig::InMemory,
            refresh_interval: Duration::from_millis(100),
        };

        let reader = LogDbReader::open(config).await.unwrap();

        // Verify background task is running
        assert!(reader.refresh_task.is_some());

        // Clean up
        reader.close().await;
    }

    #[tokio::test]
    async fn close_stops_refresh_task_gracefully() {
        use common::StorageConfig;

        let config = ReaderConfig {
            storage: StorageConfig::InMemory,
            refresh_interval: Duration::from_millis(50),
        };

        let reader = LogDbReader::open(config).await.unwrap();
        assert!(reader.refresh_task.is_some());

        // Close should complete without timeout
        let close_result =
            tokio::time::timeout(Duration::from_secs(1), async { reader.close().await }).await;

        assert!(
            close_result.is_ok(),
            "close() should complete within timeout"
        );
    }
}

//! Read-only log access and the [`LogRead`] trait.
//!
//! This module provides:
//! - [`LogRead`]: The trait defining read operations on the log.
//! - [`LogDbReader`]: A read-only view of the log that implements `LogRead`.

use std::collections::BTreeSet;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::RwLock;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::config::{ReaderConfig, ScanOptions, SegmentConfig};
use crate::direct::LogDirect;
use crate::error::{Error, Result};
use crate::listing::LogKeyIterator;
use crate::model::{LogEntry, Segment, SegmentId, Sequence};
use crate::range::{normalize_segment_id, normalize_sequence};
use crate::segment::{LogSegment, SegmentCache};
use crate::serde::LogEntryKey;
use crate::storage::{LogStorageRead as _, SegmentIterator};
use common::storage::factory::create_storage_read;
use common::{StorageRead, StorageReaderRuntime, StorageSemantics};

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
/// ```no_run
/// use log::{LogRead, Result};
/// use bytes::Bytes;
///
/// async fn process_log(reader: &(impl LogRead + Sync)) -> Result<()> {
///     let mut iter = reader.scan(Bytes::from("orders"), ..).await?;
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
    /// Returns the exact number of entries in the specified range. Useful
    /// for computing lag (how far behind a consumer is) or progress metrics.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to count.
    /// * `seq_range` - The sequence number range to count.
    ///
    /// # Errors
    ///
    /// Returns an error if the count fails due to storage issues.
    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<Sequence> + Send) -> Result<u64>;

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
    /// ```no_run
    /// # use log::{LogDb, LogRead, Config};
    /// # use common::StorageConfig;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
    /// # let log = LogDb::open(config).await?;
    /// // List all keys
    /// let mut iter = log.list_keys(..).await?;
    ///
    /// // List keys from specific segments
    /// let segments = log.list_segments(100..200).await?;
    /// let start = segments.first().map(|s| s.id).unwrap_or(0);
    /// let end = segments.last().map(|s| s.id + 1).unwrap_or(0);
    /// let mut iter = log.list_keys(start..end).await?;
    /// # Ok(())
    /// # }
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
    /// ```no_run
    /// # use log::{LogDb, LogRead, Config};
    /// # use common::StorageConfig;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
    /// # let log = LogDb::open(config).await?;
    /// // List all segments
    /// let segments = log.list_segments(..).await?;
    ///
    /// // List segments overlapping a specific range
    /// let segments = log.list_segments(100..200).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn list_segments(
        &self,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> Result<Vec<Segment>>;
}

/// Clips `query` to the sequence window owned by `segments[i]`.
///
/// Relies on the tiling guarantee from
/// [`SegmentCache::find_covering`](crate::segment::SegmentCache::find_covering):
/// segments come back ordered, and each segment owns sequences up to the
/// next segment's `start_seq`. The last segment in the covering set has no
/// successor, so its upper bound is taken from `query.end`.
fn segment_window(segments: &[LogSegment], i: usize, query: &Range<Sequence>) -> Range<Sequence> {
    let next_start = segments
        .get(i + 1)
        .map(|s| s.meta().start_seq)
        .unwrap_or(u64::MAX);
    let lo = query.start.max(segments[i].meta().start_seq);
    let hi = query.end.min(next_start);
    lo..hi
}

/// Shared read component used by both `LogDb` and `LogDbReader`.
///
/// Contains the storage and segment cache needed for read operations.
/// Wrapped in `Arc<RwLock<_>>` by both consumers.
pub(crate) struct LogReadView {
    pub(crate) storage: Arc<dyn StorageRead>,
    pub(crate) direct: Option<Arc<LogDirect>>,
    pub(crate) segments: SegmentCache,
    /// Exclusive global sequence floor this snapshot is known-complete through:
    /// every record with `seq < frontier` is observable here. The writer sets
    /// this from its sequence watermark; a standalone reader sets it from sealed
    /// segment boundaries. Advanced atomically with the snapshot (see RFC 0007).
    pub(crate) frontier: Sequence,
    /// Exclusive global sequence witnessed by scans: one past the highest
    /// sequence any scan has read. A scan of one key lifts this for all keys,
    /// advancing the frontier *within* the active segment between seals (the
    /// cross-key lift; see RFC 0007). Sound because visibility is
    /// prefix-consistent and the snapshot only moves forward, so anything
    /// witnessed in a past snapshot is complete in the current one.
    pub(crate) observed: Arc<AtomicU64>,
}

impl LogReadView {
    /// Creates a new `LogReadView`.
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        direct: Option<Arc<LogDirect>>,
        segments: SegmentCache,
        frontier: Sequence,
    ) -> Self {
        Self {
            storage,
            direct,
            segments,
            frontier,
            observed: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The effective frontier for a new scan: the maximum of the floor and the
    /// witnessed watermark. Read together with the snapshot under the caller's
    /// read lock, so it never names a sequence past what this snapshot covers.
    fn scan_frontier(&self) -> Sequence {
        self.frontier.max(self.observed.load(Ordering::Relaxed))
    }

    /// Replaces the underlying storage snapshot and the frontier it observes.
    ///
    /// The two move together so an observer can never see the frontier run
    /// ahead of the snapshot the scan actually reads.
    pub(crate) fn update_snapshot(&mut self, snapshot: Arc<dyn StorageRead>, frontier: Sequence) {
        self.storage = snapshot;
        self.frontier = frontier;
    }

    /// Reloads segments from the current storage snapshot.
    ///
    /// When `after_segment_id` is `Some`, only newer segments are appended.
    /// When `None`, the segment cache is fully replaced from storage. The
    /// standalone reader uses the full-reload path so it converges on both
    /// newly-created and deleted segments.
    pub(crate) async fn refresh_segments(
        &mut self,
        after_segment_id: Option<SegmentId>,
    ) -> crate::error::Result<()> {
        self.segments
            .refresh(self.storage.as_ref(), after_segment_id)
            .await
    }

    /// Drops every cached segment with id `<= through_id`. Used by subscriber
    /// tasks to converge to the writer's published deletion watermark.
    pub(crate) fn drop_segments_through(&mut self, through_id: SegmentId) {
        self.segments.drop_through(through_id);
    }

    /// Sets the frontier from sealed segment boundaries.
    ///
    /// The standalone reader's frontier source: it has no writer watermark, so
    /// it derives a sound (if coarse) frontier from the segments it has loaded.
    /// Only the reader paths call this — the writer sets a tighter frontier from
    /// its sequence watermark in `advance_read_view_to`, and must not be
    /// clobbered by the coarser segment-derived value. See RFC 0007.
    pub(crate) fn refresh_frontier_from_segments(&mut self) {
        self.frontier = self.segments.sealed_frontier();
    }

    /// Scans entries for a key within a sequence number range with custom options.
    pub(crate) fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: Range<Sequence>,
        _options: &ScanOptions,
    ) -> LogIterator {
        LogIterator::open(
            Arc::clone(&self.storage),
            &self.segments,
            key,
            seq_range,
            self.scan_frontier(),
            Arc::clone(&self.observed),
        )
    }

    /// Counts entries for `key` in `seq_range`, exact.
    ///
    /// Fans out across overlapping segments — clipped to each segment's
    /// window. When [`LogDirect`] is available, walks persisted SSTs via
    /// `count_in_range` for the bulk count and scans `(covered_to, seg_hi)`
    /// to pick up anything still in the memtable. Otherwise falls back to
    /// a plain scan tally.
    pub(crate) async fn count(&self, key: Bytes, seq_range: Range<Sequence>) -> Result<u64> {
        let segments = self.segments.find_covering(&seq_range);
        let mut total = 0u64;
        for (i, segment) in segments.iter().enumerate() {
            let window = segment_window(&segments, i, &seq_range);
            if window.start >= window.end {
                continue;
            }
            let n = match self.direct.as_deref() {
                Some(direct) => {
                    self.count_segment_via_direct(direct, segment, &key, window)
                        .await?
                }
                None => self.storage.count_entries(segment, &key, window).await?,
            };
            total = total.saturating_add(n);
        }
        Ok(total)
    }

    /// Counts entries in a single segment slice using [`LogDirect`].
    ///
    /// Walks persisted SSTs via `count_in_range` and tops up with a tail
    /// scan above the witness key. The tail scan covers two cases at once:
    /// writes still in the memtable, and writes flushed since `direct`'s
    /// manifest snapshot was taken (the DbReader polls, so its view can lag
    /// the writer).
    async fn count_segment_via_direct(
        &self,
        direct: &LogDirect,
        segment: &LogSegment,
        key: &Bytes,
        window: Range<Sequence>,
    ) -> Result<u64> {
        let byte_range = LogEntryKey::scan_range(segment, key, window.clone());
        let result = direct.count_in_range(&byte_range).await?;
        // LogDb is append-only, so only puts contribute. Tombstones or
        // merges in this byte range would indicate an unsupported op.
        let mut total = result.counts.num_puts;

        let scan_lo = match result.covered_to {
            Some(covered_key) => LogEntryKey::deserialize(&covered_key, segment.meta().start_seq)?
                .sequence
                .saturating_add(1),
            None => window.start,
        };
        if scan_lo < window.end {
            total = total.saturating_add(
                self.storage
                    .count_entries(segment, key, scan_lo..window.end)
                    .await?,
            );
        }
        Ok(total)
    }

    /// Lists distinct keys within a segment range.
    ///
    /// Stitches per-segment scans together by walking the segment cache —
    /// the key layout interleaves listings with log entries across
    /// segment boundaries, so a single wide-range storage scan is not safe.
    pub(crate) async fn list_keys(
        &self,
        segment_range: Range<SegmentId>,
    ) -> Result<LogKeyIterator> {
        let mut keys = BTreeSet::new();
        for segment in self.segments.all() {
            if !segment_range.contains(&segment.id()) {
                continue;
            }
            let per_segment = self.storage.list_keys_in_segment(segment.id()).await?;
            keys.extend(per_segment);
        }
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
/// ```no_run
/// # use log::{LogDbReader, ReaderConfig};
/// # use common::StorageConfig;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ReaderConfig { storage: StorageConfig::default(), ..Default::default() };
/// let reader = LogDbReader::open(config).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Thread Safety
///
/// `LogDbReader` is designed to be cloned and shared across threads.
/// All methods take `&self` and are safe to call concurrently.
///
/// # Example
///
/// ```no_run
/// use log::{LogDbReader, LogRead, LogEntry};
/// use bytes::Bytes;
/// use std::time::Duration;
///
/// async fn consume_events(reader: LogDbReader, key: Bytes) -> log::Result<()> {
///     let mut checkpoint: u64 = 0;
///
///     loop {
///         let mut iter = reader.scan(key.clone(), checkpoint..).await?;
///         while let Some(entry) = iter.next().await? {
///             println!("entry: {:?}", entry);
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
    /// ```no_run
    /// use log::{LogDbReader, LogRead, ReaderConfig};
    /// use common::StorageConfig;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(config: ReaderConfig) -> Result<Self> {
        let reader_options = slatedb::config::DbReaderOptions {
            manifest_poll_interval: config.refresh_interval,
            ..Default::default()
        };
        let storage: Arc<dyn StorageRead> = create_storage_read(
            &config.storage,
            StorageReaderRuntime::new(),
            StorageSemantics::new(),
            reader_options,
        )
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
        let direct = LogDirect::maybe_from_storage_config(&config.storage)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(Arc::new);
        let segments = SegmentCache::open(storage.as_ref(), SegmentConfig::default()).await?;
        // Standalone readers have no writer watermark, so seed the frontier
        // from sealed segment boundaries (RFC 0007).
        let frontier = segments.sealed_frontier();
        let read_view = Arc::new(RwLock::new(LogReadView::new(
            storage, direct, segments, frontier,
        )));

        let (shutdown_tx, refresh_task) =
            Self::spawn_refresh_task(Arc::clone(&read_view), config.refresh_interval);

        Ok(Self {
            read_view,
            shutdown_tx,
            refresh_task: Some(refresh_task),
        })
    }

    /// Spawns a background task that periodically refreshes the segment cache.
    ///
    /// This path fully reloads segments from storage on each tick so a
    /// standalone reader observes retention-driven `SegmentMeta` deletes as
    /// well as newly created segments.
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
                        let mut view = read_view.write().await;
                        let storage = Arc::clone(&view.storage);
                        if let Err(e) = view.segments.refresh(storage.as_ref(), None).await {
                            tracing::warn!("Failed to refresh segment cache: {}", e);
                        }
                        // Advance the frontier as newly-sealed segments appear.
                        view.refresh_frontier_from_segments();
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
        Self::new_inner(storage, None).await
    }

    /// Creates a LogDbReader paired with a `LogDirect` handle so tests can
    /// exercise the SST-walk count path through the reader.
    #[cfg(test)]
    pub(crate) async fn new_with_direct(
        storage: Arc<dyn StorageRead>,
        direct: Arc<LogDirect>,
    ) -> Result<Self> {
        Self::new_inner(storage, Some(direct)).await
    }

    #[cfg(test)]
    async fn new_inner(
        storage: Arc<dyn StorageRead>,
        direct: Option<Arc<LogDirect>>,
    ) -> Result<Self> {
        let segments = SegmentCache::open(storage.as_ref(), SegmentConfig::default()).await?;
        // Standalone readers have no writer watermark, so seed the frontier
        // from sealed segment boundaries (RFC 0007).
        let frontier = segments.sealed_frontier();
        let read_view = Arc::new(RwLock::new(LogReadView::new(
            storage, direct, segments, frontier,
        )));
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

    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<Sequence> + Send) -> Result<u64> {
        let seq_range = normalize_sequence(&seq_range);
        let view = self.read_view.read().await;
        view.count(key, seq_range).await
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
    /// Exclusive global sequence the underlying snapshot observed, captured at
    /// open. Used to lift `next_sequence` once the scan drains.
    frontier: Sequence,
    /// Shared witnessed watermark for the read view. Bumped to one past each
    /// sequence this scan reads, so later scans of other keys benefit.
    observed: Arc<AtomicU64>,
    /// Highest sequence yielded so far, if any.
    last_yielded: Option<Sequence>,
    /// Set once `next()` has returned `None` — i.e. the scan reached the end of
    /// its range within the snapshot rather than being abandoned early.
    drained: bool,
}

impl LogIterator {
    /// Opens a new iterator by looking up segments covering the sequence range.
    pub(crate) fn open(
        storage: Arc<dyn StorageRead>,
        segment_cache: &SegmentCache,
        key: Bytes,
        seq_range: Range<Sequence>,
        frontier: Sequence,
        observed: Arc<AtomicU64>,
    ) -> Self {
        let segments = segment_cache.find_covering(&seq_range);
        Self {
            storage,
            segments,
            key,
            seq_range,
            current_segment_idx: 0,
            current_iter: None,
            frontier,
            observed,
            last_yielded: None,
            drained: false,
        }
    }

    /// Creates a new iterator over the given segments, with no frontier
    /// information (`next_sequence` reflects only what was consumed).
    #[cfg(test)]
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        segments: Vec<LogSegment>,
        key: Bytes,
        seq_range: Range<Sequence>,
    ) -> Self {
        Self::new_with_frontier(storage, segments, key, seq_range, 0)
    }

    /// Like [`new`](Self::new) but with an explicit observed frontier, so tests
    /// can exercise the drained-scan lift in [`next_sequence`](Self::next_sequence).
    /// Uses a private witnessed watermark — to test the shared lift, scan
    /// through a [`LogReadView`].
    #[cfg(test)]
    pub(crate) fn new_with_frontier(
        storage: Arc<dyn StorageRead>,
        segments: Vec<LogSegment>,
        key: Bytes,
        seq_range: Range<Sequence>,
        frontier: Sequence,
    ) -> Self {
        Self {
            storage,
            segments,
            key,
            seq_range,
            current_segment_idx: 0,
            current_iter: None,
            frontier,
            observed: Arc::new(AtomicU64::new(0)),
            last_yielded: None,
            drained: false,
        }
    }

    /// Returns the next log entry, or None if iteration is complete.
    pub async fn next(&mut self) -> Result<Option<LogEntry>> {
        loop {
            // If we have a current iterator, try to get the next entry
            if let Some(iter) = &mut self.current_iter {
                if let Some(entry) = iter.next().await? {
                    self.last_yielded = Some(entry.sequence);
                    // Lift the read view's witnessed watermark so later scans of
                    // other keys resume past this point (RFC 0007 cross-key lift).
                    self.observed
                        .fetch_max(entry.sequence.saturating_add(1), Ordering::Relaxed);
                    return Ok(Some(entry));
                }
                // Current segment exhausted, move to next
                self.current_iter = None;
                self.current_segment_idx += 1;
            }

            // No current iterator, try to advance to next segment
            if !self.advance_segment().await? {
                self.drained = true;
                return Ok(None);
            }
        }
    }

    /// Exclusive upper bound on the global sequence this scan observed — the
    /// next sequence a reader should fetch to resume losslessly. See RFC 0007.
    ///
    /// `next_sequence() == N` means: under this scan's read visibility, the scan
    /// observed `key` completely through `seq < N`, so `scan(key, N..)` omits
    /// nothing this scan would have returned.
    ///
    /// - While iterating, or if abandoned before the end: `last_yielded + 1`
    ///   (or the scan's `start` if nothing was yielded) — the scan only covered
    ///   as far as it consumed.
    /// - After `next()` returns `None`: lifted to the observed frontier, clamped
    ///   to the requested range end. The frontier advances even for keys with no
    ///   records, because it reflects the whole snapshot rather than this key.
    pub fn next_sequence(&self) -> Sequence {
        let consumed = self
            .last_yielded
            .map(|seq| seq.saturating_add(1))
            .unwrap_or(self.seq_range.start);
        if self.drained {
            // Coverage is `[start, min(frontier, end))`: the scan only looks
            // within its range, and only sees what the snapshot holds.
            consumed.max(self.frontier.min(self.seq_range.end))
        } else {
            consumed
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
    use crate::storage::LogStorageWrite;
    use common::Storage;
    use opendata_macros::storage_test;

    fn entry(key: &[u8], seq: u64, value: &[u8]) -> LogEntry {
        LogEntry {
            key: Bytes::copy_from_slice(key),
            sequence: seq,
            value: Bytes::copy_from_slice(value),
        }
    }

    #[storage_test]
    async fn should_return_none_when_no_segments(storage: Arc<dyn Storage>) {
        let segments = vec![];

        let mut iter = LogIterator::new(
            storage.clone() as Arc<dyn StorageRead>,
            segments,
            Bytes::from("key"),
            0..u64::MAX,
        );

        assert!(iter.next().await.unwrap().is_none());
    }

    #[storage_test]
    async fn next_sequence_empty_drained_scan_returns_frontier(storage: Arc<dyn Storage>) {
        let mut iter = LogIterator::new_with_frontier(
            storage.clone() as Arc<dyn StorageRead>,
            vec![],
            Bytes::from("key"),
            0..u64::MAX,
            100,
        );
        assert!(iter.next().await.unwrap().is_none());
        assert_eq!(
            iter.next_sequence(),
            100,
            "an empty scan that drained lifts to the observed frontier"
        );
    }

    #[storage_test]
    async fn next_sequence_clamps_frontier_to_range_end(storage: Arc<dyn Storage>) {
        // Frontier sits beyond the requested end: a bounded scan only covered
        // its own window, so it resumes at the end, not the larger frontier.
        let mut iter = LogIterator::new_with_frontier(
            storage.clone() as Arc<dyn StorageRead>,
            vec![],
            Bytes::from("key"),
            5..100,
            1000,
        );
        assert!(iter.next().await.unwrap().is_none());
        assert_eq!(iter.next_sequence(), 100);
    }

    #[storage_test]
    async fn next_sequence_uses_frontier_below_range_end(storage: Arc<dyn Storage>) {
        // Snapshot only observed through 50, even though the range extends to
        // 100: resume at 50, since [50, 100) was never observed.
        let mut iter = LogIterator::new_with_frontier(
            storage.clone() as Arc<dyn StorageRead>,
            vec![],
            Bytes::from("key"),
            5..100,
            50,
        );
        assert!(iter.next().await.unwrap().is_none());
        assert_eq!(iter.next_sequence(), 50);
    }

    #[storage_test]
    async fn next_sequence_before_first_read_returns_start(storage: Arc<dyn Storage>) {
        // Nothing consumed and not drained → resume at the scan's start, never
        // the frontier (we have not yet observed anything).
        let iter = LogIterator::new_with_frontier(
            storage.clone() as Arc<dyn StorageRead>,
            vec![],
            Bytes::from("key"),
            5..u64::MAX,
            100,
        );
        assert_eq!(iter.next_sequence(), 5);
    }

    #[storage_test]
    async fn next_sequence_reflects_consumed_before_drain(storage: Arc<dyn Storage>) {
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));
        for i in 0..3 {
            storage
                .write_entry(&segment, &entry(b"key", i, b"v"))
                .await
                .unwrap();
        }
        let mut iter = LogIterator::new_with_frontier(
            storage.clone() as Arc<dyn StorageRead>,
            vec![segment],
            Bytes::from("key"),
            0..u64::MAX,
            100,
        );
        // One record consumed, still not drained → last yielded + 1, never the
        // frontier (there may be more records we haven't read).
        assert_eq!(iter.next().await.unwrap().unwrap().sequence, 0);
        assert_eq!(iter.next_sequence(), 1);
    }

    #[storage_test]
    async fn next_sequence_lifts_past_last_record_once_drained(storage: Arc<dyn Storage>) {
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));
        for i in 0..3 {
            storage
                .write_entry(&segment, &entry(b"key", i, b"v"))
                .await
                .unwrap();
        }
        let mut iter = LogIterator::new_with_frontier(
            storage.clone() as Arc<dyn StorageRead>,
            vec![segment],
            Bytes::from("key"),
            0..u64::MAX,
            100,
        );
        while iter.next().await.unwrap().is_some() {}
        // Last record was seq 2, but the snapshot was observed through 100, so
        // a follower resumes at 100 and skips the empty [3, 100) gap.
        assert_eq!(iter.next_sequence(), 100);
    }

    #[storage_test]
    async fn observed_lift_advances_idle_key_within_active_segment(storage: Arc<dyn Storage>) {
        // No sealed segments, so the frontier floor is 0. A scan of a hot key
        // witnesses its sequences and lifts the read view's shared watermark,
        // so a later scan of an idle key resumes past them — the cross-key lift
        // operating inside the active segment, with no seal involved.
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));
        for i in 0..4 {
            storage
                .write_entry(&segment, &entry(b"hot", i, b"v"))
                .await
                .unwrap();
        }
        let mut segments = SegmentCache::open(storage.as_ref(), SegmentConfig::default())
            .await
            .unwrap();
        segments.insert(segment);

        let view = LogReadView::new(storage.clone() as Arc<dyn StorageRead>, None, segments, 0);
        let opts = ScanOptions::default();

        // Idle key before any witnessing: only the floor (0) is available.
        let mut cold = view.scan_with_options(Bytes::from("cold"), 0..u64::MAX, &opts);
        assert!(cold.next().await.unwrap().is_none());
        assert_eq!(cold.next_sequence(), 0);

        // Drain the hot key, witnessing seq 0..4.
        let mut hot = view.scan_with_options(Bytes::from("hot"), 0..u64::MAX, &opts);
        while hot.next().await.unwrap().is_some() {}

        // The idle key now resumes past the witnessed records, despite no seal.
        let mut cold = view.scan_with_options(Bytes::from("cold"), 0..u64::MAX, &opts);
        assert!(cold.next().await.unwrap().is_none());
        assert_eq!(cold.next_sequence(), 4);
    }

    #[storage_test]
    async fn should_iterate_entries_in_single_segment(storage: Arc<dyn Storage>) {
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

    #[storage_test]
    async fn should_iterate_entries_across_multiple_segments(storage: Arc<dyn Storage>) {
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

    #[storage_test]
    async fn should_filter_by_sequence_range(storage: Arc<dyn Storage>) {
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

    #[storage_test]
    async fn should_filter_entries_for_specified_key(storage: Arc<dyn Storage>) {
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

    #[storage_test]
    async fn should_return_none_when_no_entries_in_range(storage: Arc<dyn Storage>) {
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

//! Read-only log access and the [`LogRead`] trait.
//!
//! This module provides:
//! - [`LogRead`]: The trait defining read operations on the log.
//! - [`LogDbReader`]: A read-only view of the log that implements `LogRead`.

use std::collections::BTreeSet;
use std::ops::{Range, RangeBounds};
use std::sync::Arc;
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
use common::{L0Stats, SortedRunStats, StorageRead, StorageReaderRuntime, StorageSemantics};

/// How a key's records in one segment's slice of the query are distributed
/// across that segment's LSM tree — the L0 tier vs the sorted-run tier.
///
/// Present only when the read went through the SST-walk path (a `LogDirect`
/// handle is available). A scan fallback — the in-memory backend, or a
/// SlateDB config whose object store is in-memory — reports `None` via
/// [`SegmentInspection::reads`], since no manifest walk occurs.
///
/// `l0.records + sorted_runs.records` is the persisted-SST portion of the
/// segment's count; not-yet-flushed writes are reported as
/// [`SegmentInspection::tail_scanned`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SegmentReadStats {
    /// The L0 tier's record distribution; see [`L0Stats`].
    pub l0: L0Stats,
    /// The sorted-run tier's record distribution; see [`SortedRunStats`].
    pub sorted_runs: SortedRunStats,
}

impl SegmentReadStats {
    /// Folds another segment's distribution into this one, summing every
    /// tier counter. Used to build the aggregate [`Inspection::reads`].
    fn add(&mut self, other: &SegmentReadStats) {
        self.l0.ssts_total += other.l0.ssts_total;
        self.l0.ssts_with_data += other.l0.ssts_with_data;
        self.l0.blocks_with_data += other.l0.blocks_with_data;
        self.l0.records += other.l0.records;
        self.sorted_runs.runs += other.sorted_runs.runs;
        self.sorted_runs.ssts_total += other.sorted_runs.ssts_total;
        self.sorted_runs.ssts_with_data += other.sorted_runs.ssts_with_data;
        self.sorted_runs.blocks_with_data += other.sorted_runs.blocks_with_data;
        self.sorted_runs.records += other.sorted_runs.records;
    }
}

/// How a single covering segment contributed to an [`Inspection`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentInspection {
    /// The segment that produced these records.
    pub segment_id: SegmentId,
    /// Records for the key found in this segment's slice of the query.
    pub count: u64,
    /// Records counted by the memtable/lagging-snapshot tail scan rather
    /// than the SST walk — writes not yet reflected in persisted SSTs. Kept
    /// separate so `reads` stays an honest account of persisted data.
    pub tail_scanned: u64,
    /// How the segment's persisted records split across L0 and sorted runs,
    /// or `None` when the count was produced entirely by a scan (no
    /// `LogDirect`; see [`SegmentReadStats`]).
    pub reads: Option<SegmentReadStats>,
}

/// Result of [`LogRead::inspect`]: the record count for a key/range plus a
/// per-segment breakdown of how that data is distributed across the LSM
/// tree, for tuning segmentation and compaction.
///
/// [`total`](Inspection::total) is exactly what [`LogRead::count`] returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inspection {
    /// Total records for the key in the queried range.
    pub total: u64,
    /// Tier distribution summed across all covering segments. Only segments
    /// counted via the SST-walk path contribute (see
    /// [`SegmentInspection::reads`]); on the scan-fallback path this stays
    /// zero while [`total`](Inspection::total) is still exact.
    pub reads: SegmentReadStats,
    /// Per covering-segment breakdown, in ascending segment order — the
    /// primary view, since each segment is its own LSM tree.
    pub segments: Vec<SegmentInspection>,
}

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
    /// Provided for every `LogRead` implementor: it runs the same LSM walk as
    /// [`inspect`](LogRead::inspect) and returns only [`Inspection::total`].
    /// Reach for `inspect` directly when you also want the per-segment record
    /// distribution. Implementors may override this if they can count more
    /// cheaply, but the default is correct for all of them.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to count.
    /// * `seq_range` - The sequence number range to count.
    ///
    /// # Errors
    ///
    /// Returns an error if the count fails due to storage issues.
    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<Sequence> + Send) -> Result<u64> {
        Ok(self.inspect(key, seq_range).await?.total)
    }

    /// Inspects entries for a key within a sequence number range, returning
    /// the record count plus a breakdown of how that data is laid out in the
    /// LSM tree.
    ///
    /// Like [`count`](LogRead::count) but also reports, per covering segment,
    /// how the key's records are distributed across the L0 and sorted-run
    /// tiers (record counts, SSTs holding data, blocks spanned). See
    /// [`Inspection`]. Useful for understanding read amplification and tuning
    /// segmentation/compaction.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to inspect.
    /// * `seq_range` - The sequence number range to inspect.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails due to storage issues.
    async fn inspect(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> Result<Inspection>;

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
}

impl LogReadView {
    /// Creates a new `LogReadView`.
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        direct: Option<Arc<LogDirect>>,
        segments: SegmentCache,
    ) -> Self {
        Self {
            storage,
            direct,
            segments,
        }
    }

    /// Replaces the underlying storage snapshot with a new one.
    pub(crate) fn update_snapshot(&mut self, snapshot: Arc<dyn StorageRead>) {
        self.storage = snapshot;
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

    /// Scans entries for a key within a sequence number range with custom options.
    pub(crate) fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: Range<Sequence>,
        _options: &ScanOptions,
    ) -> LogIterator {
        LogIterator::open(Arc::clone(&self.storage), &self.segments, key, seq_range)
    }

    /// Inspects entries for `key` in `seq_range`: exact count plus the
    /// per-segment record-distribution breakdown.
    ///
    /// Fans out across overlapping segments — clipped to each segment's
    /// window. When [`LogDirect`] is available, walks persisted SSTs via
    /// `count_in_range` (capturing per-tier stats) for the bulk count and
    /// scans `(covered_to, seg_hi)` to pick up anything still in the
    /// memtable. Otherwise falls back to a plain scan tally with no
    /// SST-level detail.
    pub(crate) async fn inspect(
        &self,
        key: Bytes,
        seq_range: Range<Sequence>,
    ) -> Result<Inspection> {
        let segments = self.segments.find_covering(&seq_range);
        let mut inspection = Inspection {
            total: 0,
            reads: SegmentReadStats::default(),
            segments: Vec::new(),
        };
        for (i, segment) in segments.iter().enumerate() {
            let window = segment_window(&segments, i, &seq_range);
            if window.start >= window.end {
                continue;
            }
            let seg = match self.direct.as_deref() {
                Some(direct) => {
                    self.inspect_segment_via_direct(direct, segment, &key, window)
                        .await?
                }
                None => SegmentInspection {
                    segment_id: segment.id(),
                    count: self.storage.count_entries(segment, &key, window).await?,
                    tail_scanned: 0,
                    // No manifest walk on the scan-fallback path.
                    reads: None,
                },
            };
            inspection.total = inspection.total.saturating_add(seg.count);
            // Aggregate the per-segment tier distribution into the total.
            if let Some(reads) = &seg.reads {
                inspection.reads.add(reads);
            }
            inspection.segments.push(seg);
        }
        Ok(inspection)
    }

    /// Inspects entries in a single segment slice using [`LogDirect`].
    ///
    /// Walks persisted SSTs via `count_in_range` and tops up with a tail
    /// scan above the witness key. The tail scan covers two cases at once:
    /// writes still in the memtable, and writes flushed since `direct`'s
    /// manifest snapshot was taken (the DbReader polls, so its view can lag
    /// the writer). The tail's contribution is reported separately as
    /// [`SegmentInspection::tail_scanned`] so the SST stats stay honest.
    async fn inspect_segment_via_direct(
        &self,
        direct: &LogDirect,
        segment: &LogSegment,
        key: &Bytes,
        window: Range<Sequence>,
    ) -> Result<SegmentInspection> {
        let byte_range = LogEntryKey::scan_range(segment, key, window.clone());
        let result = direct.count_in_range(&byte_range).await?;
        // LogDb is append-only, so only puts contribute. Tombstones or
        // merges in this byte range would indicate an unsupported op.
        let sst_count = result.counts.num_puts;

        let scan_lo = match &result.covered_to {
            Some(covered_key) => LogEntryKey::deserialize(covered_key, segment.meta().start_seq)?
                .sequence
                .saturating_add(1),
            None => window.start,
        };
        let tail_scanned = if scan_lo < window.end {
            self.storage
                .count_entries(segment, key, scan_lo..window.end)
                .await?
        } else {
            0
        };

        let stats = result.stats;
        Ok(SegmentInspection {
            segment_id: segment.id(),
            count: sst_count.saturating_add(tail_scanned),
            tail_scanned,
            reads: Some(SegmentReadStats {
                l0: stats.l0,
                sorted_runs: stats.sorted_runs,
            }),
        })
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
        let read_view = Arc::new(RwLock::new(LogReadView::new(storage, direct, segments)));

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
        let read_view = Arc::new(RwLock::new(LogReadView::new(storage, direct, segments)));
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

    async fn inspect(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> Result<Inspection> {
        let seq_range = normalize_sequence(&seq_range);
        let view = self.read_view.read().await;
        view.inspect(key, seq_range).await
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

//! Core LogDb implementation with read and write APIs.
//!
//! This module provides the [`LogDb`] struct, the primary entry point for
//! interacting with OpenData Log. It exposes both write operations
//! ([`try_append`](LogDb::try_append), [`append_timeout`](LogDb::append_timeout))
//! and read operations ([`scan`], [`count`]) via the [`LogRead`] trait.

use std::collections::VecDeque;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use std::sync::OnceLock;

use common::clock::{Clock, SystemClock};
use common::coordinator::{Durability, EpochWatcher, EpochWatermarks};
use common::storage::config::StorageConfig;
use common::{CompactorBuilder, StorageBuilder, create_object_store};
use slatedb::compactor::CompactionSchedulerSupplier;
use tokio::sync::RwLock;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::compaction::{CompactorView, CompactorViewCell, LogCompactionSchedulerSupplier};
use crate::config::{ReadVisibility, RetentionConfig, ScanOptions, SegmentConfig};
use crate::error::{AppendResult, Error, Result};
use crate::listing::ListingCache;
use crate::listing::LogKeyIterator;
use crate::model::{AppendOutput, Record, Segment, SegmentId, Sequence};
use crate::range::{normalize_segment_id, normalize_sequence};
use crate::reader::{LogIterator, LogRead, LogReadView};
use crate::segment::SegmentCache;
use crate::serde::SEQ_BLOCK_KEY;
use crate::view_tracker::{ViewEntry, ViewTracker};
use crate::writer::{
    LogWrite, LogWriteHandle, LogWriter, LogWriterConfig, RetentionPolicy, WrittenView,
};

/// The main log interface providing read and write operations.
///
/// `LogDb` is the primary entry point for interacting with OpenData Log.
/// It provides methods to append records, scan entries, and count records
/// within a key's log.
///
/// # Read Operations
///
/// Read operations are provided via the [`LogRead`] trait, which `LogDb`
/// implements. This allows generic code to work with either `LogDb` or
/// [`LogDbReader`](crate::LogDbReader).
///
/// # Thread Safety
///
/// `LogDb` is designed to be shared across threads. All methods take `&self`
/// and internal synchronization is handled automatically.
///
/// # Visibility and Durability
///
/// Visibility depends on `Config::read_visibility`:
///
/// - **Memory mode** (`read_visibility = memory`, default): reads wait for the
///   written watermark, so appends are visible quickly (typically without
///   explicit `flush()`), but data returned may not yet be crash-safe.
/// - **Remote mode** (`read_visibility = remote`): reads observe only snapshots
///   confirmed durable by storage, so unflushed writes are not visible.
///
/// [`flush()`](LogDb::flush) always forces durability in storage. In durable
/// mode it also establishes an internal read barrier: subsequent reads wait
/// until the durable read view has caught up to at least that flush point.
///
/// # Writer Semantics
///
/// Currently, each log supports a single writer. Multi-writer support may
/// be added in the future, but would require each key to have a single
/// writer to maintain monotonic ordering within that key's log.
///
/// # Example
///
/// ```
/// # use log::{LogDb, LogRead, Config, Record};
/// # use bytes::Bytes;
/// # use common::StorageConfig;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
/// let log = LogDb::open(config).await?;
///
/// // Append records
/// let records = vec![
///     Record { key: Bytes::from("user:123"), value: Bytes::from("event-a") },
///     Record { key: Bytes::from("user:456"), value: Bytes::from("event-b") },
/// ];
/// log.try_append(records).await?;
/// log.flush().await?;
///
/// // Scan entries for a specific key
/// let mut iter = log.scan(Bytes::from("user:123"), ..).await?;
/// while let Some(entry) = iter.next().await? {
///     println!("seq={}: {:?}", entry.sequence, entry.value);
/// }
/// # Ok(())
/// # }
/// ```
pub struct LogDb {
    handle: LogWriteHandle,
    writer_task: JoinHandle<()>,
    storage: Arc<dyn common::Storage>,
    clock: Arc<dyn Clock>,
    read_view: Arc<RwLock<LogReadView>>,
    epoch_watcher: EpochWatcher,
    read_subscriber_task: JoinHandle<()>,
    compactor_view_task: Option<JoinHandle<()>>,
    read_visibility: ReadVisibility,
}

impl LogDb {
    /// Opens or creates a log with the given configuration.
    ///
    /// This is the primary entry point for creating a `LogDb` instance. The
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
    /// use log::{LogDb, Config};
    ///
    /// let log = LogDb::open(test_config()).await?;
    /// ```
    pub async fn open(config: crate::config::Config) -> Result<Self> {
        LogDbBuilder::new(config).build().await
    }

    /// Appends records to the log without blocking.
    ///
    /// Records are assigned sequence numbers in the order they appear in the
    /// input vector. All records in a single append call are written atomically.
    ///
    /// Fails immediately with [`AppendError::QueueFull`](crate::AppendError::QueueFull) if the write queue
    /// is full. The returned error contains the original batch so callers can
    /// retry without cloning.
    ///
    /// Durability is **not** awaited. Call [`flush()`](LogDb::flush) after
    /// appending to ensure records are persisted.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to append. Each record specifies its target
    ///   key and value.
    ///
    /// # Returns
    ///
    /// On success, returns an [`AppendOutput`] containing the starting sequence
    /// number assigned to the batch.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let records = vec![
    ///     Record { key: Bytes::from("events"), value: Bytes::from("event-1") },
    ///     Record { key: Bytes::from("events"), value: Bytes::from("event-2") },
    /// ];
    /// let result = log.try_append(records).await?;
    /// println!("Appended at seq {}", result.start_sequence);
    /// ```
    pub async fn try_append(&self, records: Vec<Record>) -> AppendResult<AppendOutput> {
        self.append_inner(records, None).await
    }

    /// Appends records to the log, blocking up to `timeout` for queue space.
    ///
    /// Records are assigned sequence numbers in the order they appear in the
    /// input vector. All records in a single append call are written atomically.
    ///
    /// Returns [`AppendError::Timeout`](crate::AppendError::Timeout) if the queue does not drain within the
    /// deadline. The returned error contains the original batch for retry.
    ///
    /// Durability is **not** awaited. Call [`flush()`](LogDb::flush) after
    /// appending to ensure records are persisted.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to append.
    /// * `timeout` - Maximum duration to wait for queue space.
    ///
    /// # Returns
    ///
    /// On success, returns an [`AppendOutput`] containing the starting sequence
    /// number assigned to the batch.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let records = vec![
    ///     Record { key: Bytes::from("events"), value: Bytes::from("critical-event") },
    /// ];
    /// let result = log.append_timeout(records, Duration::from_secs(5)).await?;
    /// println!("Started at sequence {}", result.start_sequence);
    /// ```
    pub async fn append_timeout(
        &self,
        records: Vec<Record>,
        timeout: Duration,
    ) -> AppendResult<AppendOutput> {
        self.append_inner(records, Some(timeout)).await
    }

    /// Shared implementation for `try_append` and `append_timeout`.
    async fn append_inner(
        &self,
        records: Vec<Record>,
        timeout: Option<Duration>,
    ) -> AppendResult<AppendOutput> {
        if records.is_empty() {
            return Ok(AppendOutput { start_sequence: 0 });
        }

        let write = LogWrite {
            records,
            timestamp_ms: self.current_time_ms(),
        };

        let result = if let Some(t) = timeout {
            self.handle.append_timeout(write, t).await
        } else {
            self.handle.try_append(write).await
        }?;

        // Safe to unwrap: append_inner is only called with non-empty records,
        // and the writer returns Some for non-empty writes.
        Ok(result.expect("non-empty append must produce output"))
    }

    /// Returns the current time in milliseconds since Unix epoch.
    fn current_time_ms(&self) -> i64 {
        self.clock
            .now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Checks if the underlying storage is accessible.
    ///
    /// This performs a lightweight read operation to verify that the storage
    /// backend is responding. Use this for health/readiness checks.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if storage is accessible, or an error if the check fails.
    #[cfg(feature = "http-server")]
    pub(crate) async fn check_storage(&self) -> Result<()> {
        // Read the sequence block - this is a single key lookup that verifies
        // storage is accessible without scanning or listing data.
        let seq_key = Bytes::from_static(&crate::serde::SEQ_BLOCK_KEY);
        let _ = self.storage.get(seq_key).await?;
        Ok(())
    }

    /// Forces creation of a new segment, sealing the current one.
    ///
    /// This is an internal API for testing multi-segment scenarios. It forces
    /// subsequent appends to write to a new segment, regardless of any
    /// configured seal interval.
    #[cfg(test)]
    pub(crate) async fn seal_segment(&self) -> Result<()> {
        self.handle.force_seal(self.current_time_ms()).await?;
        self.flush().await?;
        Ok(())
    }

    /// Test-only delete of a sealed segment's `SegmentMeta` record.
    #[cfg(test)]
    pub(crate) async fn delete_segment_meta(&self, segment_id: SegmentId) -> Result<()> {
        self.handle.delete_segment_meta(segment_id).await
    }

    /// Flushes all pending writes to durable storage.
    ///
    /// This method ensures that all acknowledged writes are durably persisted
    /// to storage.
    pub async fn flush(&self) -> Result<()> {
        self.handle.flush().await?;
        Ok(())
    }

    /// Waits for read-side visibility to reach the current requirement.
    async fn sync_reads(&self) -> Result<()> {
        let (target, durability) = match self.read_visibility {
            ReadVisibility::Remote => (self.handle.durable_epoch(), Durability::Durable),
            ReadVisibility::Memory => (self.handle.written_epoch(), Durability::Written),
        };
        self.epoch_watcher
            .clone()
            .wait(target, durability)
            .await
            .map_err(|_| Error::Internal("writer shut down".into()))?;
        Ok(())
    }

    /// Closes the log, flushing any pending data and releasing resources.
    ///
    /// All appended data is flushed to durable storage before the log is
    /// closed. For SlateDB-backed storage, this also releases the database
    /// fence.
    pub async fn close(self) -> Result<()> {
        self.flush().await?;
        // Drop the handle to signal the writer to stop
        drop(self.handle);
        let _ = self.writer_task.await;
        self.read_subscriber_task.abort();
        if let Some(task) = self.compactor_view_task {
            task.abort();
        }
        self.storage
            .close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    /// Creates a LogDb from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new(storage: Arc<dyn common::Storage>) -> Result<Self> {
        Self::from_storage(
            storage,
            None,
            SegmentConfig::default(),
            RetentionConfig::default(),
            ReadVisibility::Memory,
            Arc::new(SystemClock),
            None,
        )
        .await
    }

    /// Creates a LogDb with a paired `LogDirect` handle so tests can
    /// exercise the SST-walk count path.
    #[cfg(test)]
    pub(crate) async fn new_with_direct(
        storage: Arc<dyn common::Storage>,
        direct: Arc<crate::direct::LogDirect>,
    ) -> Result<Self> {
        Self::from_storage(
            storage,
            Some(direct),
            SegmentConfig::default(),
            RetentionConfig::default(),
            ReadVisibility::Memory,
            Arc::new(SystemClock),
            None,
        )
        .await
    }

    /// Creates a LogDb with `ReadVisibility::Remote` from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new_durable(storage: Arc<dyn common::Storage>) -> Result<Self> {
        Self::from_storage(
            storage,
            None,
            SegmentConfig::default(),
            RetentionConfig::default(),
            ReadVisibility::Remote,
            Arc::new(SystemClock),
            None,
        )
        .await
    }

    /// Shared construction logic used by `LogDb::new` and `LogDbBuilder::build`.
    /// When `compactor_view_cell` is `Some`, a durable-gated compactor view
    /// receiver is installed there so the embedded compaction scheduler can
    /// track the live set.
    async fn from_storage(
        storage: Arc<dyn common::Storage>,
        direct: Option<Arc<crate::direct::LogDirect>>,
        segment_config: SegmentConfig,
        retention_config: RetentionConfig,
        read_visibility: ReadVisibility,
        clock: Arc<dyn Clock>,
        compactor_view_cell: Option<crate::compaction::CompactorViewCell>,
    ) -> Result<Self> {
        let seq_key = Bytes::from_static(&SEQ_BLOCK_KEY);
        let sequence_allocator = common::SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        let snapshot = storage
            .snapshot()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let segment_cache = SegmentCache::open(snapshot.as_ref(), segment_config).await?;
        let listing_cache = ListingCache::new();

        let writer_config = LogWriterConfig {
            retention: retention_config.retention.map(|retention| RetentionPolicy {
                retention,
                check_interval: retention_config.check_interval,
            }),
            ..LogWriterConfig::default()
        };
        let (writer, mut handle) = LogWriter::new(
            storage.clone(),
            sequence_allocator,
            segment_cache.clone(),
            listing_cache,
            Arc::clone(&clock),
            writer_config,
        )
        .await
        .map_err(Error::Storage)?;

        let written_rx = handle.written_rx();
        let initial_segment_id = segment_cache.latest().map(|s| s.id());
        let initial_deleted_segment_id = segment_cache.initial_deleted_segment_id();
        let compactor_view_task = if let Some(cell) = compactor_view_cell.as_ref() {
            let (compactor_rx, task) = spawn_compactor_view_publisher(
                written_rx.clone(),
                &storage,
                initial_segment_id,
                initial_deleted_segment_id,
            );
            // Err from `set` would mean the cell was already initialised — that
            // never happens on our paths, so log it loudly rather than swallow.
            if cell.set(compactor_rx).is_err() {
                tracing::warn!("compactor view cell was already initialised");
            }
            Some(task)
        } else {
            None
        };
        let writer_task = handle.spawn(writer);

        let read_view = Arc::new(RwLock::new(LogReadView::new(
            snapshot as Arc<dyn common::StorageRead>,
            direct,
            segment_cache,
        )));

        let (epoch_watcher, read_subscriber_task) = if read_visibility.is_remote() {
            spawn_durable_subscriber(
                written_rx,
                Arc::clone(&read_view),
                &storage,
                initial_segment_id,
                initial_deleted_segment_id,
            )
        } else {
            spawn_written_subscriber(
                written_rx,
                Arc::clone(&read_view),
                initial_segment_id,
                initial_deleted_segment_id,
            )
        };

        Ok(Self {
            handle,
            writer_task,
            storage,
            clock,
            read_view,
            epoch_watcher,
            read_subscriber_task,
            compactor_view_task,
            read_visibility,
        })
    }
}

#[async_trait]
impl LogRead for LogDb {
    async fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<Sequence> + Send,
        options: ScanOptions,
    ) -> Result<LogIterator> {
        self.sync_reads().await?;
        let seq_range = normalize_sequence(&seq_range);
        let view = self.read_view.read().await;
        Ok(view.scan_with_options(key, seq_range, &options))
    }

    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<Sequence> + Send) -> Result<u64> {
        self.sync_reads().await?;
        let seq_range = normalize_sequence(&seq_range);
        let view = self.read_view.read().await;
        view.count(key, seq_range).await
    }

    async fn list_keys(
        &self,
        segment_range: impl RangeBounds<SegmentId> + Send,
    ) -> Result<LogKeyIterator> {
        self.sync_reads().await?;
        let segment_range = normalize_segment_id(&segment_range);
        let view = self.read_view.read().await;
        view.list_keys(segment_range).await
    }

    async fn list_segments(
        &self,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> Result<Vec<Segment>> {
        self.sync_reads().await?;
        let seq_range = normalize_sequence(&seq_range);
        let view = self.read_view.read().await;
        Ok(view.list_segments(&seq_range))
    }
}

/// Builder for creating LogDb instances with custom options.
///
/// This builder provides configuration validation and LogDb construction.
pub struct LogDbBuilder {
    config: crate::config::Config,
    clock: Option<Arc<dyn Clock>>,
}

impl LogDbBuilder {
    /// Creates a new log builder with the given configuration.
    pub fn new(config: crate::config::Config) -> Self {
        Self {
            config,
            clock: None,
        }
    }

    /// Overrides the wall-clock source. Test-only — production callers always
    /// use the default [`common::clock::SystemClock`].
    ///
    /// LogDb consults this clock to timestamp segment metadata and to decide
    /// when the configured `seal_interval` has elapsed. Tests pass a
    /// [`common::clock::MockClock`] to drive segment rolls deterministically.
    #[cfg(test)]
    pub(crate) fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Builds the LogDb instance.
    pub async fn build(self) -> Result<LogDb> {
        self.config.validate_retention()?;
        self.config.validate_compaction()?;
        let sb = StorageBuilder::new(&self.config.storage)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        // Route every log record to a SlateDB segment keyed by the 6-byte
        // routing prefix `[subsystem, version, segment_id]`. No-op for the
        // in-memory backend; for slatedb-backed storage this installs the
        // extractor on the underlying `DbBuilder` (see RFC 0024).
        let sb = sb.map_slatedb(|db| {
            db.with_segment_extractor(crate::segment_extractor::LogSegmentExtractor::shared())
        });
        // Install the LogDb compaction scheduler for every SlateDB-backed log.
        let compactor_view_cell: CompactorViewCell = Arc::new(OnceLock::new());
        let sb = if let StorageConfig::SlateDb(ref slate_config) = self.config.storage {
            let path = slate_config.path.clone();
            let object_store = create_object_store(&slate_config.object_store)
                .map_err(|e| Error::Storage(e.to_string()))?;
            let supplier: Arc<dyn CompactionSchedulerSupplier> =
                Arc::new(LogCompactionSchedulerSupplier {
                    cell: Arc::clone(&compactor_view_cell),
                    options: self.config.compaction.clone(),
                });
            sb.map_slatedb(move |db| {
                db.with_compactor_builder(
                    CompactorBuilder::new(path, object_store).with_scheduler_supplier(supplier),
                )
            })
        } else {
            sb
        };
        let storage = sb
            .build()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let direct = crate::direct::LogDirect::maybe_from_storage_config(&self.config.storage)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .map(Arc::new);

        let clock: Arc<dyn Clock> = self.clock.unwrap_or_else(|| Arc::new(SystemClock));

        // Hand the cell to `from_storage` only when we installed the supplier.
        let cell_for_writer =
            matches!(self.config.storage, StorageConfig::SlateDb(_)).then_some(compactor_view_cell);

        LogDb::from_storage(
            storage,
            direct,
            self.config.segmentation,
            self.config.retention,
            self.config.read_visibility,
            clock,
            cell_for_writer,
        )
        .await
    }
}

fn spawn_compactor_view_publisher(
    mut written_rx: watch::Receiver<WrittenView>,
    storage: &Arc<dyn common::Storage>,
    initial_segment_id: Option<SegmentId>,
    initial_deleted_segment_id: Option<SegmentId>,
) -> (watch::Receiver<CompactorView>, JoinHandle<()>) {
    let (compactor_tx, compactor_rx) = watch::channel(CompactorView {
        last_segment_id: initial_segment_id,
        last_deleted_segment_id: initial_deleted_segment_id,
    });
    let mut durable_rx = storage.subscribe_durable();

    let task = tokio::spawn(async move {
        let mut pending_deletes: VecDeque<(u64, Option<SegmentId>)> = VecDeque::new();
        let mut last_durable_seq = *durable_rx.borrow();

        loop {
            tokio::select! {
                result = written_rx.changed() => {
                    if result.is_err() {
                        break;
                    }
                    let view = written_rx.borrow_and_update().clone();
                    pending_deletes.push_back((view.seqnum, view.last_deleted_segment_id));
                    compactor_tx.send_modify(|state| {
                        state.last_segment_id = view.last_segment_id;
                    });
                    drain_compactor_deletes(
                        &mut pending_deletes,
                        last_durable_seq,
                        &compactor_tx,
                    );
                }
                result = durable_rx.changed() => {
                    if result.is_err() {
                        break;
                    }
                    last_durable_seq = *durable_rx.borrow_and_update();
                    drain_compactor_deletes(
                        &mut pending_deletes,
                        last_durable_seq,
                        &compactor_tx,
                    );
                }
            }
        }
    });

    (compactor_rx, task)
}

fn drain_compactor_deletes(
    pending_deletes: &mut VecDeque<(u64, Option<SegmentId>)>,
    durable_seq: u64,
    compactor_tx: &watch::Sender<CompactorView>,
) {
    let mut latest_durable_delete = None;
    while pending_deletes
        .front()
        .is_some_and(|(seqnum, _)| *seqnum <= durable_seq)
    {
        latest_durable_delete = pending_deletes
            .pop_front()
            .map(|(_, deleted_id)| deleted_id);
    }

    if let Some(last_deleted_segment_id) = latest_durable_delete {
        compactor_tx.send_modify(|state| {
            state.last_deleted_segment_id = last_deleted_segment_id;
        });
    }
}

/// Tries to advance the tracker and, on success, applies the new snapshot
/// to the shared read view and reconciles the local segment cache against
/// the writer's published watermarks.
///
/// Two independent reconciliations happen on a tick:
///
/// - **Adds.** When `last_segment_id` advanced, scan storage for the new
///   segments and append them.
/// - **Drops.** When `last_deleted_segment_id` advanced, drop every
///   locally-cached segment with id `<= new value`. Sound because retention
///   only deletes from the low end of the log (RFC 0005).
async fn try_advance_read_view(
    tracker: &mut ViewTracker,
    read_view: &RwLock<LogReadView>,
    watermarks: &EpochWatermarks,
    known_segment_id: &mut Option<SegmentId>,
    known_deleted_segment_id: &mut Option<SegmentId>,
    durability: Durability,
    through_seq: u64,
) {
    if let Some(advanced) = tracker.advance(through_seq) {
        let mut rv = read_view.write().await;
        rv.update_snapshot(advanced.snapshot as Arc<dyn common::StorageRead>);

        // Refresh segments from the snapshot when the writer reports a new segment.
        if advanced.last_segment_id != *known_segment_id {
            match rv.refresh_segments(*known_segment_id).await {
                Ok(()) => {
                    *known_segment_id = advanced.last_segment_id;
                }
                Err(e) => {
                    tracing::warn!("failed to refresh segments: {e}");
                }
            }
        }

        // Drop segments that have been deleted off the low end since our
        // last observation.
        if advanced.last_deleted_segment_id > *known_deleted_segment_id {
            if let Some(through_id) = advanced.last_deleted_segment_id {
                rv.drop_segments_through(through_id);
            }
            *known_deleted_segment_id = advanced.last_deleted_segment_id;
        }

        match durability {
            Durability::Written => watermarks.update_written(advanced.epoch),
            Durability::Durable | Durability::Applied => watermarks.update_durable(advanced.epoch),
        }
    }
}

fn spawn_written_subscriber(
    mut written_rx: watch::Receiver<WrittenView>,
    read_view: Arc<RwLock<LogReadView>>,
    initial_segment_id: Option<SegmentId>,
    initial_deleted_segment_id: Option<SegmentId>,
) -> (EpochWatcher, JoinHandle<()>) {
    let (watermarks, watcher) = EpochWatermarks::new();
    let mut tracker = ViewTracker::new();
    let mut known_segment_id = initial_segment_id;
    let mut known_deleted_segment_id = initial_deleted_segment_id;
    let task = tokio::spawn(async move {
        while written_rx.changed().await.is_ok() {
            let view = written_rx.borrow_and_update().clone();
            let seqnum = view.seqnum;
            tracker.push(ViewEntry {
                seqnum,
                epoch: view.epoch,
                snapshot: view.snapshot,
                last_segment_id: view.last_segment_id,
                last_deleted_segment_id: view.last_deleted_segment_id,
            });

            try_advance_read_view(
                &mut tracker,
                &read_view,
                &watermarks,
                &mut known_segment_id,
                &mut known_deleted_segment_id,
                Durability::Written,
                seqnum,
            )
            .await;
        }
    });
    (watcher, task)
}

/// Spawns subscriber tasks for `ReadVisibility::Remote` mode.
///
/// A single task watches both the WrittenView channel and the durable
/// sequence watermark. When a new WrittenView arrives, it's pushed into
/// the ViewTracker and the "written" watermark is advanced. When
/// the durable_seq advances, the tracker is drained and the LogReadView
/// and "durable" watermark are updated.
fn spawn_durable_subscriber(
    mut written_rx: watch::Receiver<WrittenView>,
    read_view: Arc<RwLock<LogReadView>>,
    storage: &Arc<dyn common::Storage>,
    initial_segment_id: Option<SegmentId>,
    initial_deleted_segment_id: Option<SegmentId>,
) -> (EpochWatcher, JoinHandle<()>) {
    let (watermarks, watcher) = EpochWatermarks::new();
    let mut durable_rx = storage.subscribe_durable();
    let mut tracker = ViewTracker::new();
    let mut known_segment_id = initial_segment_id;
    let mut known_deleted_segment_id = initial_deleted_segment_id;

    let task = tokio::spawn(async move {
        // Track the latest known durable_seq so we can re-check after new
        // writes arrive (the durable notification may arrive before the
        // WrittenView is pushed).
        let mut last_durable_seq: u64 = *durable_rx.borrow();

        loop {
            tokio::select! {
                result = written_rx.changed() => {
                    if result.is_err() {
                        break;
                    }
                    let view = written_rx.borrow_and_update().clone();
                    tracker.push(ViewEntry {
                        seqnum: view.seqnum,
                        epoch: view.epoch,
                        snapshot: view.snapshot,
                        last_segment_id: view.last_segment_id,
                        last_deleted_segment_id: view.last_deleted_segment_id,
                    });
                    watermarks.update_written(view.epoch);

                    // Re-check: the durable watermark may already cover this entry
                    try_advance_read_view(
                        &mut tracker, &read_view, &watermarks,
                        &mut known_segment_id, &mut known_deleted_segment_id,
                        Durability::Durable, last_durable_seq,
                    )
                    .await;
                }
                result = durable_rx.changed() => {
                    if result.is_err() {
                        break;
                    }
                    last_durable_seq = *durable_rx.borrow_and_update();
                    try_advance_read_view(
                        &mut tracker, &read_view, &watermarks,
                        &mut known_segment_id, &mut known_deleted_segment_id,
                        Durability::Durable, last_durable_seq,
                    )
                    .await;
                }
            }
        }
    });

    (watcher, task)
}

#[cfg(test)]
mod tests {
    use common::Storage;
    use common::StorageBuilder;
    use common::StorageConfig;
    use common::storage::in_memory::InMemoryStorage;

    use super::*;
    use crate::config::Config;
    use crate::reader::LogDbReader;

    fn test_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            ..Default::default()
        }
    }

    async fn make_written_view(
        storage: &Arc<dyn common::Storage>,
        seqnum: u64,
        last_segment_id: Option<SegmentId>,
        last_deleted_segment_id: Option<SegmentId>,
    ) -> WrittenView {
        WrittenView {
            epoch: seqnum,
            snapshot: storage.snapshot().await.unwrap(),
            seqnum,
            last_segment_id,
            last_deleted_segment_id,
        }
    }

    #[tokio::test]
    async fn compactor_view_should_publish_new_segments_at_written_latency() {
        let storage = Arc::new(InMemoryStorage::new().with_deferred_durability());
        let storage_dyn = storage.clone() as Arc<dyn common::Storage>;
        let initial_view = make_written_view(&storage_dyn, 0, None, None).await;
        let (written_tx, written_rx) = watch::channel(initial_view);
        let (mut compactor_rx, task) =
            spawn_compactor_view_publisher(written_rx, &storage_dyn, None, None);

        let write_result = storage
            .apply(vec![common::storage::RecordOp::Delete(Bytes::from_static(
                b"segment-meta-1",
            ))])
            .await
            .unwrap();
        written_tx.send_replace(
            make_written_view(&storage_dyn, write_result.seqnum, Some(3), None).await,
        );

        tokio::time::timeout(Duration::from_secs(1), compactor_rx.changed())
            .await
            .expect("timed out waiting for compactor view")
            .expect("compactor view channel closed");
        let view = *compactor_rx.borrow_and_update();
        assert_eq!(view.last_segment_id, Some(3));
        assert_eq!(view.last_deleted_segment_id, None);

        task.abort();
    }

    #[tokio::test]
    async fn compactor_view_should_delay_deleted_segments_until_durable() {
        let storage = Arc::new(InMemoryStorage::new().with_deferred_durability());
        let storage_dyn = storage.clone() as Arc<dyn common::Storage>;
        let initial_view = make_written_view(&storage_dyn, 0, Some(2), None).await;
        let (written_tx, written_rx) = watch::channel(initial_view);
        let (mut compactor_rx, task) =
            spawn_compactor_view_publisher(written_rx, &storage_dyn, Some(2), None);

        let write_result = storage
            .apply(vec![common::storage::RecordOp::Delete(Bytes::from_static(
                b"segment-meta-1",
            ))])
            .await
            .unwrap();
        written_tx.send_replace(
            make_written_view(&storage_dyn, write_result.seqnum, Some(2), Some(1)).await,
        );

        tokio::time::timeout(Duration::from_secs(1), compactor_rx.changed())
            .await
            .expect("timed out waiting for written compactor update")
            .expect("compactor view channel closed");
        let view = *compactor_rx.borrow_and_update();
        assert_eq!(view.last_segment_id, Some(2));
        assert_eq!(
            view.last_deleted_segment_id, None,
            "delete should remain hidden until durable",
        );

        storage.flush_to(write_result.seqnum);

        tokio::time::timeout(Duration::from_secs(1), compactor_rx.changed())
            .await
            .expect("timed out waiting for durable compactor update")
            .expect("compactor view channel closed");
        let view = *compactor_rx.borrow_and_update();
        assert_eq!(view.last_deleted_segment_id, Some(1));

        task.abort();
    }

    #[tokio::test]
    async fn should_open_log_with_in_memory_config() {
        // given
        let config = test_config();

        // when
        let result = LogDb::open(config).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_append_single_record() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        let records = vec![Record {
            key: Bytes::from("orders"),
            value: Bytes::from("order-1"),
        }];

        // when
        log.try_append(records).await.unwrap();

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
        let log = LogDb::open(test_config()).await.unwrap();
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
        log.try_append(records).await.unwrap();

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
        let log = LogDb::open(test_config()).await.unwrap();
        let records: Vec<Record> = vec![];

        // when
        let result = log.try_append(records).await;

        // then
        assert!(result.is_ok());

        // verify no entries exist
        let mut iter = log.scan(Bytes::from("any-key"), ..).await.unwrap();
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_assign_sequential_sequences_across_appends() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();

        // when - first append
        log.try_append(vec![
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
        log.try_append(vec![Record {
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
        let log = LogDb::open(test_config()).await.unwrap();
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
        log.try_append(records).await.unwrap();

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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![Record {
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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let storage = StorageBuilder::new(&StorageConfig::InMemory)
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        let log = LogDb::new(storage.clone()).await.unwrap();
        log.try_append(vec![
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
        log.flush().await.unwrap();

        // when - create LogDbReader sharing the same storage
        let reader = LogDbReader::new(storage).await.unwrap();
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
        let log = LogDb::open(test_config()).await.unwrap();

        // write to segment 0
        log.try_append(vec![
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
        log.try_append(vec![
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
        let log = LogDb::open(test_config()).await.unwrap();

        // segment 0: seq 0, 1
        log.try_append(vec![
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
        log.try_append(vec![
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
        log.try_append(vec![
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
        let log = LogDb::open(test_config()).await.unwrap();

        // segment 0: seq 0, 1
        log.try_append(vec![
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
        log.try_append(vec![
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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let _iter = log.list_keys(..).await.unwrap();

        // then - iterator is returned (full iteration tested when LogKeyIterator is implemented)
    }

    #[tokio::test]
    async fn should_list_keys_via_log_reader() {
        // given - create shared storage
        let storage = StorageBuilder::new(&StorageConfig::InMemory)
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        let log = LogDb::new(storage.clone()).await.unwrap();
        log.try_append(vec![
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
        log.flush().await.unwrap();

        // when - create LogDbReader sharing the same storage
        let reader = LogDbReader::new(storage).await.unwrap();
        let _iter = reader.list_keys(..).await.unwrap();

        // then - iterator is returned
    }

    #[tokio::test]
    async fn should_list_keys_in_single_segment() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let mut iter = log.list_keys(..).await.unwrap();
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
        let log = LogDb::open(test_config()).await.unwrap();

        // write to segment 0
        log.try_append(vec![
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
        log.try_append(vec![
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
        let mut iter = log.list_keys(..).await.unwrap();
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
        let log = LogDb::open(test_config()).await.unwrap();

        // write to segment 0
        log.try_append(vec![Record {
            key: Bytes::from("shared-key"),
            value: Bytes::from("value-0"),
        }])
        .await
        .unwrap();

        // seal and create segment 1
        log.seal_segment().await.unwrap();

        // write same key to segment 1
        log.try_append(vec![Record {
            key: Bytes::from("shared-key"),
            value: Bytes::from("value-1"),
        }])
        .await
        .unwrap();

        // seal and create segment 2
        log.seal_segment().await.unwrap();

        // write same key to segment 2
        log.try_append(vec![Record {
            key: Bytes::from("shared-key"),
            value: Bytes::from("value-2"),
        }])
        .await
        .unwrap();

        // when
        let mut iter = log.list_keys(..).await.unwrap();
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
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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
        let mut iter = log.list_keys(..).await.unwrap();
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
        let log = LogDb::open(test_config()).await.unwrap();

        // when
        let mut iter = log.list_keys(..).await.unwrap();

        // then
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_list_keys_respects_segment_range() {
        // given - entries in different segments. First user segment is id 1.
        let log = LogDb::open(test_config()).await.unwrap();

        // segment 1
        log.try_append(vec![
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

        // segment 2
        log.try_append(vec![
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

        log.seal_segment().await.unwrap();

        // segment 3
        log.try_append(vec![
            Record {
                key: Bytes::from("key-seg3"),
                value: Bytes::from("value"),
            },
            Record {
                key: Bytes::from("key-seg3-b"),
                value: Bytes::from("value"),
            },
        ])
        .await
        .unwrap();

        // when - list only keys from segment 2
        let mut iter = log.list_keys(2..3).await.unwrap();
        let mut keys = vec![];
        while let Some(key) = iter.next().await.unwrap() {
            keys.push(key.key);
        }

        // then - only keys from segment 2
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], Bytes::from("key-seg2"));
        assert_eq!(keys[1], Bytes::from("key-seg2-b"));
    }

    #[tokio::test]
    async fn should_list_segments_returns_empty_when_no_segments() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();

        // when
        let segments = log.list_segments(..).await.unwrap();

        // then
        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn should_list_segments_returns_single_segment() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        }])
        .await
        .unwrap();

        // when
        let segments = log.list_segments(..).await.unwrap();

        // then — first user segment is id 1 (id 0 is reserved system segment)
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id, 1);
        assert_eq!(segments[0].start_seq, 0);
    }

    #[tokio::test]
    async fn should_list_segments_returns_multiple_segments() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();

        // first user segment (id 1)
        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value-0"),
        }])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 2
        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value-1"),
        }])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 3
        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value-2"),
        }])
        .await
        .unwrap();

        // when
        let segments = log.list_segments(..).await.unwrap();

        // then
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id, 1);
        assert_eq!(segments[0].start_seq, 0);
        assert_eq!(segments[1].id, 2);
        assert_eq!(segments[1].start_seq, 1);
        assert_eq!(segments[2].id, 3);
        assert_eq!(segments[2].start_seq, 2);
    }

    #[tokio::test]
    async fn should_list_segments_filters_by_sequence_range() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();

        // segment 0: seq 0, 1
        log.try_append(vec![
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
        log.try_append(vec![
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

        log.seal_segment().await.unwrap();

        // segment 2: seq 4, 5
        log.try_append(vec![
            Record {
                key: Bytes::from("key"),
                value: Bytes::from("v4"),
            },
            Record {
                key: Bytes::from("key"),
                value: Bytes::from("v5"),
            },
        ])
        .await
        .unwrap();

        // when - query range that spans the middle segment (id 2)
        let segments = log.list_segments(2..4).await.unwrap();

        // then - only segment 2 matches
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id, 2);
        assert_eq!(segments[0].start_seq, 2);
    }

    #[tokio::test]
    async fn should_list_segments_via_log_reader() {
        // given
        let storage = StorageBuilder::new(&StorageConfig::InMemory)
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        let log = LogDb::new(storage.clone()).await.unwrap();

        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value-0"),
        }])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value-1"),
        }])
        .await
        .unwrap();
        log.flush().await.unwrap();

        // when
        let reader = LogDbReader::new(storage).await.unwrap();
        let segments = reader.list_segments(..).await.unwrap();

        // then
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id, 1);
        assert_eq!(segments[1].id, 2);
    }

    #[tokio::test]
    async fn should_list_segments_includes_start_time() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        }])
        .await
        .unwrap();

        // when
        let segments = log.list_segments(..).await.unwrap();

        // then - start_time_ms should be a reasonable timestamp (after year 2020)
        assert_eq!(segments.len(), 1);
        assert!(segments[0].start_time_ms > 1577836800000); // 2020-01-01
    }

    #[tokio::test]
    async fn should_try_append_single_record() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        let records = vec![Record {
            key: Bytes::from("orders"),
            value: Bytes::from("order-1"),
        }];

        // when
        let result = log.try_append(records).await.unwrap();

        // then
        assert_eq!(result.start_sequence, 0);
        let mut iter = log.scan(Bytes::from("orders"), ..).await.unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.value, Bytes::from("order-1"));
    }

    #[tokio::test]
    async fn should_append_timeout_single_record() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        let records = vec![Record {
            key: Bytes::from("orders"),
            value: Bytes::from("order-1"),
        }];

        // when
        let result = log
            .append_timeout(records, Duration::from_secs(5))
            .await
            .unwrap();

        // then
        assert_eq!(result.start_sequence, 0);
        let mut iter = log.scan(Bytes::from("orders"), ..).await.unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.value, Bytes::from("order-1"));
    }

    #[tokio::test]
    async fn should_return_empty_records_on_try_append_empty() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();

        // when
        let result = log.try_append(vec![]).await.unwrap();

        // then
        assert_eq!(result.start_sequence, 0);
    }

    #[tokio::test]
    async fn should_return_empty_records_on_append_timeout_empty() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();

        // when
        let result = log
            .append_timeout(vec![], Duration::from_secs(1))
            .await
            .unwrap();

        // then
        assert_eq!(result.start_sequence, 0);
    }

    #[tokio::test]
    async fn should_scan_without_flush() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
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

        // when/then - reads see unflushed data via sync_to_flushed
        let mut iter = log.scan(Bytes::from("key"), ..).await.unwrap();
        let e0 = iter.next().await.unwrap().unwrap();
        assert_eq!(e0.sequence, 0);
        assert_eq!(e0.value, Bytes::from("v0"));
        let e1 = iter.next().await.unwrap().unwrap();
        assert_eq!(e1.sequence, 1);
        assert_eq!(e1.value, Bytes::from("v1"));
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_list_keys_without_flush() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
            Record {
                key: Bytes::from("alpha"),
                value: Bytes::from("v"),
            },
            Record {
                key: Bytes::from("beta"),
                value: Bytes::from("v"),
            },
        ])
        .await
        .unwrap();

        // when/then - reads see unflushed data via sync_to_flushed
        let mut iter = log.list_keys(..).await.unwrap();
        let mut keys = vec![];
        while let Some(key) = iter.next().await.unwrap() {
            keys.push(key.key);
        }
        assert_eq!(keys, vec![Bytes::from("alpha"), Bytes::from("beta")]);
    }

    #[tokio::test(start_paused = true)]
    async fn should_delete_expired_segments_via_periodic_retention_loop() {
        // Drives the writer's `run`-loop `select!` branch end-to-end through
        // the public API, reached via the periodic `tokio::time::interval`
        // inside `LogWriter::run` (rather than calling `tick_retention`
        // directly).
        use crate::Config;
        use common::clock::MockClock;
        use std::time::SystemTime;

        let t0_secs: u64 = 10_000;
        let retention = Duration::from_secs(60);
        let seal_interval = Duration::from_secs(60);
        let check_interval = Duration::from_millis(10);

        let clock = Arc::new(MockClock::with_time(
            SystemTime::UNIX_EPOCH + Duration::from_secs(t0_secs),
        ));
        let config = Config {
            storage: StorageConfig::InMemory,
            segmentation: SegmentConfig {
                seal_interval: Some(seal_interval),
            },
            retention: RetentionConfig {
                retention: Some(retention),
                check_interval,
            },
            ..Config::default()
        };
        let log = LogDbBuilder::new(config)
            .with_clock(Arc::clone(&clock) as Arc<dyn Clock>)
            .build()
            .await
            .unwrap();

        // Create segment 1 at T=t0, then seal it (segment 2 also starts at t0).
        // Segment 1's effective end_time is segment 2's start_time_ms = t0_ms.
        log.try_append(vec![Record {
            key: Bytes::from("k"),
            value: Bytes::from("v"),
        }])
        .await
        .unwrap();
        log.seal_segment().await.unwrap();

        let before: Vec<_> = log
            .list_segments(..)
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(before, vec![1, 2]);

        // Move the writer's wall-clock past the retention cutoff for segment 1.
        clock.set_time(SystemTime::UNIX_EPOCH + Duration::from_secs(t0_secs + 70));

        // Advance `tokio::time` past one `check_interval` so the writer's
        // interval branch in `select!` fires. Under `start_paused`, sleeping
        // both bumps virtual time and yields, giving the spawned writer
        // task room to run `tick_retention` to completion.
        tokio::time::sleep(check_interval + Duration::from_millis(1)).await;
        // tick_retention has several internal `.await` points (apply +
        // snapshot + broadcast); a few extra yields drain them before we
        // check the public surface.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        // Local reader should now see segment 1 dropped from the view.
        let after: Vec<_> = log
            .list_segments(..)
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(after, vec![2]);
    }

    #[tokio::test]
    async fn should_drop_deleted_segment_from_local_reader_view() {
        // given: three sealed user segments
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![Record {
            key: Bytes::from("k"),
            value: Bytes::from("v0"),
        }])
        .await
        .unwrap();
        log.seal_segment().await.unwrap();
        log.try_append(vec![Record {
            key: Bytes::from("k"),
            value: Bytes::from("v1"),
        }])
        .await
        .unwrap();
        log.seal_segment().await.unwrap();
        log.try_append(vec![Record {
            key: Bytes::from("k"),
            value: Bytes::from("v2"),
        }])
        .await
        .unwrap();

        let before: Vec<_> = log
            .list_segments(..)
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(before, vec![1, 2, 3]);

        // when: retention deletes the oldest segment's metadata
        log.delete_segment_meta(1).await.unwrap();

        // then: local readers converge — segment 1 is no longer visible
        // without reopening the LogDb.
        let after: Vec<_> = log
            .list_segments(..)
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(after, vec![2, 3]);
    }

    #[tokio::test]
    async fn should_list_segments_without_flush() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        }])
        .await
        .unwrap();

        // when/then - reads see unflushed data via sync_to_flushed
        let segments = log.list_segments(..).await.unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id, 1);
        assert_eq!(segments[0].start_seq, 0);
    }

    #[tokio::test]
    async fn count_returns_zero_for_empty_log() {
        let log = LogDb::open(test_config()).await.unwrap();
        let n = log.count(Bytes::from("k"), ..).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn count_counts_every_entry_for_key() {
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(
            (0..10)
                .map(|i| Record {
                    key: Bytes::from("orders"),
                    value: Bytes::from(format!("order-{i}")),
                })
                .collect(),
        )
        .await
        .unwrap();
        let n = log.count(Bytes::from("orders"), ..).await.unwrap();
        assert_eq!(n, 10);
    }

    #[tokio::test]
    async fn count_isolates_per_key() {
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
            Record {
                key: Bytes::from("a"),
                value: Bytes::from("1"),
            },
            Record {
                key: Bytes::from("b"),
                value: Bytes::from("1"),
            },
            Record {
                key: Bytes::from("a"),
                value: Bytes::from("2"),
            },
            Record {
                key: Bytes::from("b"),
                value: Bytes::from("2"),
            },
        ])
        .await
        .unwrap();
        assert_eq!(log.count(Bytes::from("a"), ..).await.unwrap(), 2);
        assert_eq!(log.count(Bytes::from("b"), ..).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn count_respects_sequence_range() {
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(
            (0..10)
                .map(|i| Record {
                    key: Bytes::from("events"),
                    value: Bytes::from(format!("event-{i}")),
                })
                .collect(),
        )
        .await
        .unwrap();
        // [3, 7) covers seqs 3,4,5,6 — 4 entries.
        assert_eq!(log.count(Bytes::from("events"), 3..7).await.unwrap(), 4);
        // Lower bound only.
        assert_eq!(log.count(Bytes::from("events"), 5..).await.unwrap(), 5);
        // Upper bound only.
        assert_eq!(log.count(Bytes::from("events"), ..4).await.unwrap(), 4);
        // Out-of-range upper.
        assert_eq!(log.count(Bytes::from("events"), 100..200).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn count_sees_writes_without_flush() {
        // Mirrors should_scan_without_flush — count's internal sync_reads must
        // pull in unflushed writes the same way scan does.
        let log = LogDb::open(test_config()).await.unwrap();
        log.try_append(vec![
            Record {
                key: Bytes::from("k"),
                value: Bytes::from("v0"),
            },
            Record {
                key: Bytes::from("k"),
                value: Bytes::from("v1"),
            },
        ])
        .await
        .unwrap();
        assert_eq!(log.count(Bytes::from("k"), ..).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn count_spans_multiple_segments() {
        // Segments roll on a 1-byte seal_interval (effectively every write).
        // The count path has to fan out across segments and stitch the
        // per-segment counts back together.
        let config = Config {
            storage: StorageConfig::InMemory,
            segmentation: SegmentConfig {
                seal_interval: Some(Duration::from_nanos(1)),
            },
            ..Default::default()
        };
        let log = LogDb::open(config).await.unwrap();
        for i in 0..5u8 {
            log.try_append(vec![Record {
                key: Bytes::from("k"),
                value: Bytes::from(vec![i]),
            }])
            .await
            .unwrap();
            // Small sleep so seal_interval triggers a fresh segment.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let segments = log.list_segments(..).await.unwrap();
        assert!(segments.len() >= 2, "expected multiple segments");
        assert_eq!(log.count(Bytes::from("k"), ..).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn count_via_log_reader() {
        // LogDbReader shares storage with LogDb; what the writer flushes,
        // the reader counts. Mirrors `should_scan_entries_via_log_reader`.
        let storage = StorageBuilder::new(&StorageConfig::InMemory)
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        let log = LogDb::new(storage.clone()).await.unwrap();
        log.try_append(
            (0..5)
                .map(|i| Record {
                    key: Bytes::from("orders"),
                    value: Bytes::from(format!("order-{i}")),
                })
                .collect(),
        )
        .await
        .unwrap();
        log.flush().await.unwrap();

        let reader = LogDbReader::new(storage).await.unwrap();
        assert_eq!(reader.count(Bytes::from("orders"), ..).await.unwrap(), 5);
        assert_eq!(reader.count(Bytes::from("orders"), 1..4).await.unwrap(), 3);
        assert_eq!(
            reader.count(Bytes::from("nonexistent"), ..).await.unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn count_across_segments_via_log_reader() {
        // The reader's fan-out across segments is the same code path as
        // LogDb's (LogReadView::count), but this asserts that the reader's
        // segment cache is populated correctly to see all segments.
        let storage = StorageBuilder::new(&StorageConfig::InMemory)
            .await
            .unwrap()
            .build()
            .await
            .unwrap();
        let log = LogDb::from_storage(
            storage.clone(),
            None,
            SegmentConfig {
                seal_interval: Some(Duration::from_nanos(1)),
            },
            RetentionConfig::default(),
            ReadVisibility::Memory,
            Arc::new(SystemClock),
            None,
        )
        .await
        .unwrap();
        for i in 0..5u8 {
            log.try_append(vec![Record {
                key: Bytes::from("k"),
                value: Bytes::from(vec![i]),
            }])
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        log.flush().await.unwrap();

        let reader = LogDbReader::new(storage).await.unwrap();
        let segments = reader.list_segments(..).await.unwrap();
        assert!(segments.len() >= 2, "expected multiple segments");
        assert_eq!(reader.count(Bytes::from("k"), ..).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn slate_reader_count_sees_flushed_data() {
        // Slatedb-backed reader: writes go through LogDb, get flushed, then
        // the reader's count_in_range path walks the same manifest.
        let (storage, direct) = slate_storage_with_direct().await;
        let log = LogDb::new_with_direct(storage.clone(), direct.clone())
            .await
            .unwrap();
        log.try_append(
            (0..10u8)
                .map(|i| Record {
                    key: Bytes::from("k"),
                    value: Bytes::from(vec![i]),
                })
                .collect(),
        )
        .await
        .unwrap();
        log.flush().await.unwrap();

        let reader = LogDbReader::new_with_direct(storage, direct).await.unwrap();
        assert_eq!(reader.count(Bytes::from("k"), ..).await.unwrap(), 10);
        assert_eq!(reader.count(Bytes::from("k"), 2..7).await.unwrap(), 5);
    }

    /// Helper: creates a SlateDB-backed storage using an in-memory object store.
    /// With `start_paused = true`, SlateDB's WAL flush timer is frozen so writes
    /// remain non-durable until an explicit `flush()` call.
    async fn slate_storage() -> Arc<dyn common::Storage> {
        use common::storage::slate::SlateDbStorage;
        use slatedb::DbBuilder;
        use slatedb::object_store::memory::InMemory;

        let path = "/test/read_durable";
        let object_store = Arc::new(InMemory::new());
        let db = DbBuilder::new(path, object_store).build().await.unwrap();
        Arc::new(SlateDbStorage::new(Arc::new(db)))
    }

    /// Same as [`slate_storage`] but also builds a [`LogDirect`] handle
    /// sharing the same in-memory object store. `InMemory` instances are
    /// process-local, so storage and direct must share the same `Arc` for
    /// the direct path to see what storage writes.
    async fn slate_storage_with_direct() -> (Arc<dyn common::Storage>, Arc<crate::direct::LogDirect>)
    {
        use common::storage::slate::SlateDbStorage;
        use slatedb::config::DbReaderOptions;
        use slatedb::object_store::memory::InMemory;
        use slatedb::{DbBuilder, DbReader, SstReader};

        let path = "/test/slate_with_direct";
        let object_store: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = Arc::new(
            DbBuilder::new(path, object_store.clone())
                .build()
                .await
                .unwrap(),
        );
        let storage: Arc<dyn common::Storage> = Arc::new(SlateDbStorage::new(db));

        // Short manifest poll interval so the reader picks up freshly-
        // flushed writes quickly — otherwise the slate count tests would
        // race the default 1s polling tick.
        let reader_options = DbReaderOptions {
            manifest_poll_interval: Duration::from_millis(5),
            ..Default::default()
        };
        let reader = DbReader::builder(path, object_store.clone())
            .with_options(reader_options)
            .build()
            .await
            .unwrap();
        let sst_reader = SstReader::new(path, object_store, None, None);
        let direct = crate::direct::LogDirect::from_components(Arc::new(reader), sst_reader);
        (storage, Arc::new(direct))
    }

    #[tokio::test(start_paused = true)]
    async fn should_not_see_unflushed_writes_in_read_durable_mode() {
        let storage = slate_storage().await;
        let log = LogDb::new_durable(storage).await.unwrap();

        // Append records — data is in memtable but not durable (time is paused)
        log.try_append(vec![Record {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        }])
        .await
        .unwrap();

        // In read_durable mode, sync_to_flushed is a no-op — the read view
        // only advances when data becomes durable. So scan returns immediately
        // but yields no results because the view hasn't advanced yet.
        let mut iter = log.scan(Bytes::from("key1"), ..).await.unwrap();
        assert!(
            iter.next().await.unwrap().is_none(),
            "should not see non-durable writes"
        );

        // Now flush — makes data durable, which advances the read view
        log.flush().await.unwrap();

        let mut iter = log.scan(Bytes::from("key1"), ..).await.unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.value, Bytes::from("value1"));
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn should_read_durable_preserves_multiple_appends() {
        let storage = slate_storage().await;
        let log = LogDb::new_durable(storage).await.unwrap();

        log.try_append(vec![
            Record {
                key: Bytes::from("k"),
                value: Bytes::from("v1"),
            },
            Record {
                key: Bytes::from("k"),
                value: Bytes::from("v2"),
            },
        ])
        .await
        .unwrap();

        log.try_append(vec![Record {
            key: Bytes::from("k"),
            value: Bytes::from("v3"),
        }])
        .await
        .unwrap();

        // Not visible yet — read view hasn't advanced
        let mut iter = log.scan(Bytes::from("k"), ..).await.unwrap();
        assert!(
            iter.next().await.unwrap().is_none(),
            "should not see non-durable writes"
        );

        // Flush and verify all data
        log.flush().await.unwrap();

        let mut iter = log.scan(Bytes::from("k"), ..).await.unwrap();
        let e0 = iter.next().await.unwrap().unwrap();
        assert_eq!(e0.value, Bytes::from("v1"));
        let e1 = iter.next().await.unwrap().unwrap();
        assert_eq!(e1.value, Bytes::from("v2"));
        let e2 = iter.next().await.unwrap().unwrap();
        assert_eq!(e2.value, Bytes::from("v3"));
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_not_see_unflushed_writes_in_read_durable_mode_in_memory() {
        let storage =
            Arc::new(common::storage::in_memory::InMemoryStorage::new().with_deferred_durability());
        let log = LogDb::new_durable(Arc::clone(&storage) as Arc<dyn common::Storage>)
            .await
            .unwrap();

        // Append records — written but not durable
        log.try_append(vec![Record {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        }])
        .await
        .unwrap();

        // Scan returns empty — data is not yet durable
        let mut iter = log.scan(Bytes::from("key1"), ..).await.unwrap();
        assert!(
            iter.next().await.unwrap().is_none(),
            "should not see non-durable writes"
        );

        // Flush makes data durable
        common::Storage::flush(storage.as_ref()).await.unwrap();
        // This bypasses LogDb::flush barrier tracking, so allow subscriber propagation.
        tokio::task::yield_now().await;

        let mut iter = log.scan(Bytes::from("key1"), ..).await.unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.value, Bytes::from("value1"));
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn should_see_writes_immediately_in_flushed_mode() {
        // In flushed (default) mode, writes become visible via sync_to_flushed
        // even without an explicit flush() and even with a paused clock.
        let storage = slate_storage().await;
        let log = LogDb::new(storage).await.unwrap();

        log.try_append(vec![Record {
            key: Bytes::from("key1"),
            value: Bytes::from("value1"),
        }])
        .await
        .unwrap();

        // No flush, no clock advance — scan should still see the write
        let mut iter = log.scan(Bytes::from("key1"), ..).await.unwrap();
        let entry = iter.next().await.unwrap().unwrap();
        assert_eq!(entry.value, Bytes::from("value1"));
        assert!(iter.next().await.unwrap().is_none());
    }

    /// Exercises the count_in_range + scan split on slatedb-backed storage.
    /// We write N entries, flush so some land in an L0 SST, then write more
    /// without flushing — count must include both the SST-flushed prefix
    /// (via the manifest walk) and the memtable tail (via the scan above
    /// covered_to).
    #[tokio::test]
    async fn slate_count_sees_both_flushed_and_unflushed() {
        let (storage, direct) = slate_storage_with_direct().await;
        let log = LogDb::new_with_direct(storage, direct).await.unwrap();

        // First batch — flush to put these into an L0 SST.
        log.try_append(
            (0..5u8)
                .map(|i| Record {
                    key: Bytes::from("k"),
                    value: Bytes::from(vec![i]),
                })
                .collect(),
        )
        .await
        .unwrap();
        log.flush().await.unwrap();

        // Second batch — leave in the memtable.
        log.try_append(
            (5..10u8)
                .map(|i| Record {
                    key: Bytes::from("k"),
                    value: Bytes::from(vec![i]),
                })
                .collect(),
        )
        .await
        .unwrap();

        assert_eq!(log.count(Bytes::from("k"), ..).await.unwrap(), 10);
        // Range that ends inside the flushed prefix — only count_in_range
        // contributes, scan portion is empty.
        assert_eq!(log.count(Bytes::from("k"), ..3).await.unwrap(), 3);
        // Range that starts inside the unflushed tail — only the scan side
        // should contribute (covered_to from count_in_range is below the
        // range start, so scan_lo gets pulled to seg_lo).
        assert_eq!(log.count(Bytes::from("k"), 7..).await.unwrap(), 3);
    }
}

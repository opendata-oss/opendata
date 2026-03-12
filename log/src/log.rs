//! Core LogDb implementation with read and write APIs.
//!
//! This module provides the [`LogDb`] struct, the primary entry point for
//! interacting with OpenData Log. It exposes both write operations
//! ([`try_append`](LogDb::try_append), [`append_timeout`](LogDb::append_timeout))
//! and read operations ([`scan`], [`count`]) via the [`LogRead`] trait.

use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use common::StorageBuilder;
use common::clock::{Clock, SystemClock};
use common::coordinator::{Durability, EpochWatcher, EpochWatermarks};
use tokio::sync::RwLock;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::config::{CountOptions, ReadVisibility, ScanOptions, SegmentConfig};
use crate::error::{AppendResult, Error, Result};
use crate::listing::ListingCache;
use crate::listing::LogKeyIterator;
use crate::model::{AppendOutput, Record, Segment, SegmentId, Sequence};
use crate::range::{normalize_segment_id, normalize_sequence};
use crate::reader::{LogIterator, LogRead, LogReadView};
use crate::segment::SegmentCache;
use crate::serde::SEQ_BLOCK_KEY;
use crate::view_tracker::{ViewEntry, ViewTracker};
use crate::writer::{LogWrite, LogWriteHandle, LogWriter, LogWriterConfig, WrittenView};

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

    /// Registers storage engine metrics into the given Prometheus registry.
    #[cfg(feature = "http-server")]
    pub fn register_metrics(&self, registry: &mut prometheus_client::registry::Registry) {
        self.storage.register_metrics(registry);
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
        self.storage
            .close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    /// Creates a LogDb from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new(storage: Arc<dyn common::Storage>) -> Result<Self> {
        Self::from_storage(storage, SegmentConfig::default(), ReadVisibility::Memory).await
    }

    /// Creates a LogDb with `ReadVisibility::Remote` from an existing storage implementation.
    #[cfg(test)]
    pub(crate) async fn new_durable(storage: Arc<dyn common::Storage>) -> Result<Self> {
        Self::from_storage(storage, SegmentConfig::default(), ReadVisibility::Remote).await
    }

    /// Shared construction logic used by both `LogDb::new` and `LogDbBuilder::build`.
    async fn from_storage(
        storage: Arc<dyn common::Storage>,
        segment_config: SegmentConfig,
        read_visibility: ReadVisibility,
    ) -> Result<Self> {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);

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

        let (writer, mut handle) = LogWriter::new(
            storage.clone(),
            sequence_allocator,
            segment_cache.clone(),
            listing_cache,
            LogWriterConfig::default(),
        )
        .await
        .map_err(Error::Storage)?;

        let written_rx = handle.written_rx();
        let writer_task = handle.spawn(writer);

        let initial_segment_id = segment_cache.latest().map(|s| s.id());
        let read_view = Arc::new(RwLock::new(LogReadView::new(
            snapshot as Arc<dyn common::StorageRead>,
            segment_cache,
        )));

        let (epoch_watcher, read_subscriber_task) = if read_visibility.is_remote() {
            spawn_durable_subscriber(
                written_rx,
                Arc::clone(&read_view),
                &storage,
                initial_segment_id,
            )
        } else {
            spawn_written_subscriber(written_rx, Arc::clone(&read_view), initial_segment_id)
        };

        Ok(Self {
            handle,
            writer_task,
            storage,
            clock,
            read_view,
            epoch_watcher,
            read_subscriber_task,
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
/// This builder provides a fluent API for configuring a LogDb, including
/// low-level SlateDB knobs via [`StorageBuilder::map_slatedb`].
///
/// # Example
///
/// ```ignore
/// use log::LogDbBuilder;
/// use log::Config;
/// use common::{StorageBuilder, CompactorBuilder, create_object_store};
///
/// // Create a separate runtime for compaction (important for sync/JNI usage)
/// let compaction_runtime = tokio::runtime::Builder::new_multi_thread()
///     .worker_threads(2)
///     .enable_all()
///     .build()
///     .unwrap();
///
/// let mut sb = StorageBuilder::new(&config.storage).await.unwrap();
/// sb = sb.map_slatedb(|db| {
///     let obj_store = create_object_store(&slate_config.object_store).unwrap();
///     db.with_compactor_builder(
///         CompactorBuilder::new(slate_config.path.clone(), obj_store)
///             .with_runtime(compaction_runtime.handle().clone())
///     )
/// });
///
/// let log = LogDbBuilder::new(config)
///     .with_storage_builder(sb)
///     .build()
///     .await?;
/// ```
pub struct LogDbBuilder {
    config: crate::config::Config,
    storage_builder: Option<StorageBuilder>,
}

impl LogDbBuilder {
    /// Creates a new log builder with the given configuration.
    pub fn new(config: crate::config::Config) -> Self {
        Self {
            config,
            storage_builder: None,
        }
    }

    /// Sets a pre-configured [`StorageBuilder`].
    ///
    /// Use this to configure low-level SlateDB knobs like compaction runtime.
    /// If not called, a default `StorageBuilder` is created from the config.
    pub fn with_storage_builder(mut self, builder: StorageBuilder) -> Self {
        self.storage_builder = Some(builder);
        self
    }

    /// Builds the LogDb instance.
    pub async fn build(self) -> Result<LogDb> {
        let sb = match self.storage_builder {
            Some(sb) => sb,
            None => StorageBuilder::new(&self.config.storage)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?,
        };
        let storage = sb
            .build()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        LogDb::from_storage(
            storage,
            self.config.segmentation,
            self.config.read_visibility,
        )
        .await
    }
}

/// Tries to advance the tracker and, on success, applies the new snapshot
/// to the shared read view and refreshes segments if needed.
async fn try_advance_read_view(
    tracker: &mut ViewTracker,
    read_view: &RwLock<LogReadView>,
    watermarks: &EpochWatermarks,
    known_segment_id: &mut Option<SegmentId>,
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
) -> (EpochWatcher, JoinHandle<()>) {
    let (watermarks, watcher) = EpochWatermarks::new();
    let mut tracker = ViewTracker::new();
    let mut known_segment_id = initial_segment_id;
    let task = tokio::spawn(async move {
        while written_rx.changed().await.is_ok() {
            let view = written_rx.borrow_and_update().clone();
            let seqnum = view.seqnum;
            tracker.push(ViewEntry {
                seqnum,
                epoch: view.epoch,
                snapshot: view.snapshot,
                last_segment_id: view.last_segment_id,
            });

            try_advance_read_view(
                &mut tracker,
                &read_view,
                &watermarks,
                &mut known_segment_id,
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
) -> (EpochWatcher, JoinHandle<()>) {
    let (watermarks, watcher) = EpochWatermarks::new();
    let mut durable_rx = storage.subscribe_durable();
    let mut tracker = ViewTracker::new();
    let mut known_segment_id = initial_segment_id;

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
                    });
                    watermarks.update_written(view.epoch);

                    // Re-check: the durable watermark may already cover this entry
                    try_advance_read_view(
                        &mut tracker, &read_view, &watermarks, &mut known_segment_id,
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
                        &mut tracker, &read_view, &watermarks, &mut known_segment_id,
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
    use common::StorageBuilder;
    use common::StorageConfig;

    use super::*;
    use crate::config::Config;
    use crate::reader::LogDbReader;

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
        // given - entries in different segments
        let log = LogDb::open(test_config()).await.unwrap();

        // segment 0
        log.try_append(vec![
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

        // when - list only keys from segment 1
        let mut iter = log.list_keys(1..2).await.unwrap();
        let mut keys = vec![];
        while let Some(key) = iter.next().await.unwrap() {
            keys.push(key.key);
        }

        // then - only keys from segment 1
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], Bytes::from("key-seg1"));
        assert_eq!(keys[1], Bytes::from("key-seg1-b"));
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

        // then
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id, 0);
        assert_eq!(segments[0].start_seq, 0);
    }

    #[tokio::test]
    async fn should_list_segments_returns_multiple_segments() {
        // given
        let log = LogDb::open(test_config()).await.unwrap();

        // segment 0
        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value-0"),
        }])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 1
        log.try_append(vec![Record {
            key: Bytes::from("key"),
            value: Bytes::from("value-1"),
        }])
        .await
        .unwrap();

        log.seal_segment().await.unwrap();

        // segment 2
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
        assert_eq!(segments[0].id, 0);
        assert_eq!(segments[0].start_seq, 0);
        assert_eq!(segments[1].id, 1);
        assert_eq!(segments[1].start_seq, 1);
        assert_eq!(segments[2].id, 2);
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

        // when - query range that spans segment 1
        let segments = log.list_segments(2..4).await.unwrap();

        // then - only segment 1 matches
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id, 1);
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
        assert_eq!(segments[0].id, 0);
        assert_eq!(segments[1].id, 1);
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
        assert_eq!(segments[0].id, 0);
        assert_eq!(segments[0].start_seq, 0);
    }

    /// Helper: creates a SlateDB-backed storage using an in-memory object store.
    /// With `start_paused = true`, SlateDB's WAL flush timer is frozen so writes
    /// remain non-durable until an explicit `flush()` call.
    async fn slate_storage() -> Arc<dyn common::Storage> {
        use common::storage::slate::SlateDbStorage;
        use slatedb::DbBuilder;
        use slatedb::object_store::memory::InMemory;

        let object_store = Arc::new(InMemory::new());
        let db = DbBuilder::new("/test/read_durable", object_store)
            .build()
            .await
            .unwrap();
        Arc::new(SlateDbStorage::new(Arc::new(db)))
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
}

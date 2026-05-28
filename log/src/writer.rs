//! Purpose-built log writer task and handle.
//!
//! This module replaces the generic write coordinator with a streamlined writer
//! that processes appends directly via an mpsc queue. Each append writes
//! immediately to SlateDB's memtable; durability is achieved through explicit
//! `flush()` calls.
//!
//! # Durability levels
//!
//! The log writer skips [`Durability::Applied`] because there is no in-memory
//! delta stage — writes go straight to SlateDB. The watermarks advance as:
//!
//! - **Written** — after each append or seal, once the data is in SlateDB's
//!   memtable (visible to snapshot reads, but not yet on disk).
//! - **Durable** — after `storage.flush()`, once SlateDB has synced to disk.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::Ttl::NoExpiry;
use common::clock::Clock;
use common::coordinator::{EpochWatcher, EpochWatermarks};
use common::storage::{PutOptions, RecordOp};
use common::{PutRecordOp, SequenceAllocator, WriteOptions};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::compaction::{CompactorView, CompactorViewCell};
use crate::error::AppendError;
use crate::listing::ListingCache;
use crate::model::{AppendOutput, Record as UserRecord, SegmentId, Sequence};
use crate::segment::{LogSegment, SegmentAssignment, SegmentCache};
use crate::serde::{LogEntryKey, SegmentMetaKey};

/// Snapshot of writer state broadcast to subscribers after each write.
///
/// The two segment-id watermarks below are monotonic and (combined with
/// RFC 0005's "retention only deletes from the low end" invariant) fully
/// describe the live user-segment range. Safe under `watch` coalescing.
#[derive(Clone)]
pub(crate) struct WrittenView {
    pub epoch: u64,
    pub snapshot: Arc<dyn common::storage::StorageSnapshot>,
    /// Storage engine sequence number for this write.
    pub seqnum: u64,
    /// Exclusive upper bound of global sequences allocated as of this write.
    /// Equals `SequenceAllocator::peek_next_sequence()` at broadcast time.
    pub next_sequence: Sequence,
    /// Id of the largest user segment created so far. `None` before the first
    /// segment is created.
    pub last_segment_id: Option<SegmentId>,
    /// Id of the largest user segment whose `SegmentMeta` has been deleted.
    /// `None` until the first delete is observed.
    pub last_deleted_segment_id: Option<SegmentId>,
}

/// The write type for the log writer.
///
/// Bundles user records with a timestamp.
pub(crate) struct LogWrite {
    pub records: Vec<UserRecord>,
    pub timestamp_ms: i64,
}

/// Commands sent to the writer task via the mpsc queue.
pub(crate) enum WriterCommand {
    Append {
        write: LogWrite,
        result_tx: oneshot::Sender<Result<Option<AppendOutput>, String>>,
    },
    #[cfg(test)]
    ForceSeal {
        timestamp_ms: i64,
        result_tx: oneshot::Sender<Result<(), String>>,
    },
    #[cfg(test)]
    DeleteSegmentMeta {
        segment_id: SegmentId,
        result_tx: oneshot::Sender<Result<(), String>>,
    },
    Flush {
        result_tx: oneshot::Sender<Result<u64, String>>,
    },
}

/// Configuration for the log writer.
pub(crate) struct LogWriterConfig {
    pub queue_capacity: usize,
    /// Retention policy. When `Some`, the writer's main loop runs a periodic
    /// retention check that deletes `SegmentMeta` records for expired sealed
    /// segments. `None` disables the check entirely.
    pub retention: Option<RetentionPolicy>,
    /// Shared cell the writer publishes the compactor-facing live-set view
    /// into. `None` disables compactor publication (no embedded compaction).
    pub compactor_view: Option<CompactorViewCell>,
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            retention: None,
            compactor_view: None,
        }
    }
}

/// Retention policy controlling the writer's periodic expiry check.
#[derive(Debug, Clone)]
pub(crate) struct RetentionPolicy {
    /// Segments whose end time is older than `now() - retention` expire.
    pub retention: Duration,
    /// How often the writer evaluates expiry.
    pub check_interval: Duration,
}

/// Cumulative write-path timing counters, used to isolate the LogDb-internal
/// contribution to per-append latency from the SlateDB-internal contribution.
///
/// Per-phase counters accumulate exactly once per `handle_append` call, so
/// dividing any of them by `append_count` yields a per-append average.
///
/// Updated only from the single writer task, so atomic stores use `Relaxed`.
#[derive(Default)]
pub struct WriteStats {
    /// Number of `handle_append` invocations measured.
    pub append_count: AtomicU64,
    /// Cumulative wall-clock time spent inside `handle_append`, in nanoseconds.
    pub append_total_ns: AtomicU64,

    // ---- LogDb-internal phases (each measured once per handle_append) ----
    /// Time in `sequence_allocator.allocate`.
    pub seq_alloc_ns: AtomicU64,
    /// Time in `segment_cache.assign_segment`.
    pub segment_assign_ns: AtomicU64,
    /// Time in the listing-cache step: materializing the `Vec<Bytes>` of keys
    /// and calling `listing_cache.assign_new_keys`.
    pub listing_assign_ns: AtomicU64,
    /// Time in `add_entries` (LogEntryKey serialization + record construction).
    pub add_entries_ns: AtomicU64,
    /// Time rebuilding `Vec<PutRecordOp>` into `Vec<RecordOp>` before apply.
    pub ops_rebuild_ns: AtomicU64,
    /// Time in `broadcast` excluding `storage.snapshot` (the watch channel
    /// send, epoch bump, and watermarks update).
    pub broadcast_other_ns: AtomicU64,

    // ---- SlateDB-internal (storage seam) ----
    /// Cumulative wall-clock time inside `storage.apply_with_options` (the
    /// SlateDB `write_with_options` call), in nanoseconds. Counts every apply,
    /// including segment-meta deletes from retention.
    pub storage_apply_ns: AtomicU64,
    pub storage_apply_count: AtomicU64,
    /// Cumulative wall-clock time inside `storage.snapshot` (the SlateDB
    /// `snapshot` call invoked from `broadcast`), in nanoseconds.
    pub storage_snapshot_ns: AtomicU64,
    pub storage_snapshot_count: AtomicU64,
}

impl WriteStats {
    pub fn snapshot(&self) -> WriteStatsSnapshot {
        WriteStatsSnapshot {
            append_count: self.append_count.load(Ordering::Relaxed),
            append_total_ns: self.append_total_ns.load(Ordering::Relaxed),
            seq_alloc_ns: self.seq_alloc_ns.load(Ordering::Relaxed),
            segment_assign_ns: self.segment_assign_ns.load(Ordering::Relaxed),
            listing_assign_ns: self.listing_assign_ns.load(Ordering::Relaxed),
            add_entries_ns: self.add_entries_ns.load(Ordering::Relaxed),
            ops_rebuild_ns: self.ops_rebuild_ns.load(Ordering::Relaxed),
            broadcast_other_ns: self.broadcast_other_ns.load(Ordering::Relaxed),
            storage_apply_ns: self.storage_apply_ns.load(Ordering::Relaxed),
            storage_apply_count: self.storage_apply_count.load(Ordering::Relaxed),
            storage_snapshot_ns: self.storage_snapshot_ns.load(Ordering::Relaxed),
            storage_snapshot_count: self.storage_snapshot_count.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time read of [`WriteStats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct WriteStatsSnapshot {
    pub append_count: u64,
    pub append_total_ns: u64,
    pub seq_alloc_ns: u64,
    pub segment_assign_ns: u64,
    pub listing_assign_ns: u64,
    pub add_entries_ns: u64,
    pub ops_rebuild_ns: u64,
    pub broadcast_other_ns: u64,
    pub storage_apply_ns: u64,
    pub storage_apply_count: u64,
    pub storage_snapshot_ns: u64,
    pub storage_snapshot_count: u64,
}

/// The log writer task. Owns all write-side state and runs as a spawned async task.
pub(crate) struct LogWriter {
    storage: Arc<dyn common::Storage>,
    sequence_allocator: SequenceAllocator,
    segment_cache: SegmentCache,
    listing_cache: ListingCache,
    epoch: u64,
    written_tx: watch::Sender<WrittenView>,
    watermarks: EpochWatermarks,
    last_segment_id: Option<SegmentId>,
    last_deleted_segment_id: Option<SegmentId>,
    clock: Arc<dyn Clock>,
    retention: Option<RetentionPolicy>,
    stats: Arc<WriteStats>,
    /// Shared cell publishing the compactor-facing live-set view. `None` when
    /// embedded compaction is disabled.
    compactor_view: Option<CompactorViewCell>,
    /// Segment deletions awaiting durability before they are released to the
    /// compactor view, as `(write seqnum, resulting last_deleted_segment_id)`.
    /// Drained in `run`'s durable branch once `seqnum` is durable, so drain
    /// proposals never run ahead of a durable `SegmentMeta` deletion. Always
    /// empty when `compactor_view` is `None`.
    pending_deletes: VecDeque<(u64, Option<SegmentId>)>,
}

impl LogWriter {
    /// Creates a new writer and returns the writer + channels for spawning.
    pub(crate) async fn new(
        storage: Arc<dyn common::Storage>,
        sequence_allocator: SequenceAllocator,
        segment_cache: SegmentCache,
        listing_cache: ListingCache,
        clock: Arc<dyn Clock>,
        config: LogWriterConfig,
    ) -> Result<(Self, LogWriteHandle), String> {
        let (cmd_tx, cmd_rx) = mpsc::channel(config.queue_capacity);

        let initial_snapshot = storage.snapshot().await.map_err(|e| e.to_string())?;
        let last_segment_id = segment_cache.latest().map(|s| s.id());
        let last_deleted_segment_id = segment_cache.initial_deleted_segment_id();
        // Anything persisted before construction is already covered by the
        // storage's initial durable watermark, so `peek_next_sequence` is the
        // exclusive upper bound of already-durable global sequences.
        let initial_next_sequence = sequence_allocator.peek_next_sequence();
        let initial_view = WrittenView {
            epoch: 0,
            snapshot: initial_snapshot,
            seqnum: 0,
            next_sequence: initial_next_sequence,
            last_segment_id,
            last_deleted_segment_id,
        };
        let (written_tx, written_rx) = watch::channel(initial_view);
        let (watermarks, watcher) = EpochWatermarks::new();
        let stats = Arc::new(WriteStats::default());

        let writer = Self {
            storage,
            sequence_allocator,
            segment_cache,
            listing_cache,
            epoch: 0,
            written_tx,
            watermarks,
            last_segment_id,
            last_deleted_segment_id,
            clock,
            retention: config.retention,
            stats: Arc::clone(&stats),
            compactor_view: config.compactor_view,
            pending_deletes: VecDeque::new(),
        };

        let handle = LogWriteHandle {
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            written_rx,
            watcher,
            stats,
        };

        Ok((writer, handle))
    }

    /// Runs the writer loop, processing commands and periodic retention
    /// ticks when retention is configured.
    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<WriterCommand>) {
        let mut retention_tick = self
            .retention
            .as_ref()
            .map(|p| tokio::time::interval(p.check_interval));
        let mut durable_rx = self.storage.subscribe_durable();
        let mut durable_closed = false;
        loop {
            // Only listen for durability advances while deletes await their
            // durable gate. In steady state `pending_deletes` is empty, so the
            // durable branch parks forever and plain appends never wake the
            // writer here. Once the durable channel closes (storage shutting
            // down) we stop listening, so the branch can't busy-spin on `Err`.
            let awaiting_durable = !self.pending_deletes.is_empty() && !durable_closed;
            tokio::select! {
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else { break };
                    self.handle_command(cmd).await;
                }
                _ = async {
                    match retention_tick.as_mut() {
                        Some(t) => { t.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    self.tick_retention().await;
                }
                result = async {
                    if awaiting_durable {
                        durable_rx.changed().await
                    } else {
                        std::future::pending().await
                    }
                } => {
                    // A closed durable channel means storage is shutting down;
                    // stop draining but keep serving until the command channel
                    // closes.
                    if result.is_err() {
                        durable_closed = true;
                    } else {
                        let durable_seq = *durable_rx.borrow_and_update();
                        self.drain_compactor_deletes(durable_seq);
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, cmd: WriterCommand) {
        match cmd {
            WriterCommand::Append { write, result_tx } => {
                let result = self.handle_append(write).await;
                let _ = result_tx.send(result);
            }
            #[cfg(test)]
            WriterCommand::ForceSeal {
                timestamp_ms,
                result_tx,
            } => {
                let result = self.handle_force_seal(timestamp_ms).await;
                let _ = result_tx.send(result);
            }
            #[cfg(test)]
            WriterCommand::DeleteSegmentMeta {
                segment_id,
                result_tx,
            } => {
                let result = self.handle_delete_segment_meta(segment_id).await;
                let _ = result_tx.send(result);
            }
            WriterCommand::Flush { result_tx } => {
                let result = self.handle_flush().await;
                let _ = result_tx.send(result);
            }
        }
    }

    /// Periodic retention check: deletes `SegmentMeta` for any sealed segment
    /// whose end time (the successor's `start_time_ms`) is past
    /// `now - retention`. Per-segment failures log and are retried next tick.
    async fn tick_retention(&mut self) {
        let Some(policy) = self.retention.clone() else {
            return;
        };
        let now_ms = match self.clock.now().duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => d.as_millis() as i64,
            Err(e) => {
                // Falling back to 0 would silently disable retention; skip
                // the tick and retry once the clock is sane again.
                tracing::warn!("retention: clock before UNIX_EPOCH, skipping tick: {e}");
                return;
            }
        };
        let cutoff = now_ms - policy.retention.as_millis() as i64;

        let segments = self.segment_cache.all();
        let expired: Vec<SegmentId> = segments
            .windows(2)
            .filter(|w| w[1].meta().start_time_ms < cutoff)
            .map(|w| w[0].id())
            .collect();

        for id in expired {
            if let Err(e) = self.handle_delete_segment_meta(id).await {
                tracing::warn!("retention: failed to delete SegmentMeta for segment {id}: {e}");
            }
        }
    }

    /// Processes an append command: allocates sequences, assigns segments,
    /// builds records, and writes directly to storage.
    async fn handle_append(&mut self, write: LogWrite) -> Result<Option<AppendOutput>, String> {
        let count = write.records.len() as u64;
        if count == 0 {
            return Ok(None);
        }
        let started = Instant::now();

        let mut puts = Vec::new();

        // 1. Allocate sequences
        let phase = Instant::now();
        let (base_seq, maybe_block_record) = self.sequence_allocator.allocate(count);
        if let Some(r) = maybe_block_record {
            puts.push(PutRecordOp::new_with_options(
                r,
                PutOptions { ttl: NoExpiry },
            ));
        }
        self.stats
            .seq_alloc_ns
            .fetch_add(phase.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // 2. Assign segment (appends segment metadata record if new)
        let phase = Instant::now();
        let assignment =
            self.segment_cache
                .assign_segment(write.timestamp_ms, base_seq, &mut puts, false);
        self.stats
            .segment_assign_ns
            .fetch_add(phase.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // 3. Assign listing entries for new keys
        let phase = Instant::now();
        let keys: Vec<Bytes> = write.records.iter().map(|r| r.key.clone()).collect();
        self.listing_cache
            .assign_new_keys(assignment.segment.id(), &keys, &mut puts);
        self.stats
            .listing_assign_ns
            .fetch_add(phase.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // 4. Build log entry records
        let phase = Instant::now();
        self.add_entries(&assignment.segment, base_seq, &write.records, &mut puts);
        self.stats
            .add_entries_ns
            .fetch_add(phase.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // 5. Write to storage and broadcast
        let phase = Instant::now();
        let ops: Vec<RecordOp> = puts.into_iter().map(RecordOp::Put).collect();
        self.stats
            .ops_rebuild_ns
            .fetch_add(phase.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.apply_and_broadcast(ops, Some(&assignment)).await?;

        self.stats
            .append_total_ns
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats.append_count.fetch_add(1, Ordering::Relaxed);

        Ok(Some(AppendOutput {
            start_sequence: base_seq,
        }))
    }

    /// Seals the current segment, forcing subsequent writes into a new one.
    #[cfg(test)]
    async fn handle_force_seal(&mut self, timestamp_ms: i64) -> Result<(), String> {
        let next_seq = self.sequence_allocator.peek_next_sequence();
        let mut puts = Vec::new();
        let assignment = self
            .segment_cache
            .assign_segment(timestamp_ms, next_seq, &mut puts, true);

        if !puts.is_empty() {
            let ops: Vec<RecordOp> = puts.into_iter().map(RecordOp::Put).collect();
            self.apply_and_broadcast(ops, Some(&assignment)).await?;
        }

        Ok(())
    }

    /// Deletes a sealed segment's `SegmentMeta` record.
    ///
    /// Idempotent at the storage layer: a missing key in SlateDB is a harmless
    /// no-op. The local cache is updated only after the storage write succeeds,
    /// so a failed delete leaves the segment live (retention will retry).
    async fn handle_delete_segment_meta(&mut self, segment_id: SegmentId) -> Result<(), String> {
        let key = SegmentMetaKey::new(segment_id).serialize();
        let ops = vec![RecordOp::Delete(key)];
        let write_result = self.apply(ops).await?;
        self.segment_cache.remove(segment_id);
        // Deletion watermark is monotonic — only advance. Advancing here is
        // safe even though `apply` doesn't tell us whether the key existed:
        // SlateDB transitions to a terminal error state on any write failure
        // (see `Db::check_closed`), so a successful `apply` implies every
        // prior `apply` on this writer also succeeded. Retention's caller
        // (`tick_retention`) iterates expired ids in ascending order, so
        // successful advances are contiguous from the low end.
        if self
            .last_deleted_segment_id
            .is_none_or(|prev| prev < segment_id)
        {
            self.last_deleted_segment_id = Some(segment_id);
        }
        // Queue the deletion for the compactor. It stays hidden from the
        // live-set view until `write_result.seqnum` is durable (released by the
        // durable branch in `run`), so drain proposals never run ahead of a
        // durable `SegmentMeta` deletion. A fresh delete's seqnum always
        // exceeds the current durable watermark, so no immediate drain applies.
        if self.compactor_view.is_some() {
            self.pending_deletes
                .push_back((write_result.seqnum, self.last_deleted_segment_id));
        }
        self.broadcast(write_result.seqnum).await
    }

    /// Writes ops to storage and broadcasts the resulting view.
    ///
    /// Used by the append/seal paths where no post-write state change is
    /// needed before the broadcast. For deletes, prefer the explicit
    /// `apply` + `broadcast` pair so cache/watermark updates can happen
    /// between them.
    async fn apply_and_broadcast(
        &mut self,
        ops: Vec<RecordOp>,
        assignment: Option<&SegmentAssignment>,
    ) -> Result<(), String> {
        let write_result = self.apply(ops).await?;
        if let Some(assignment) = assignment
            && assignment.is_new
        {
            self.last_segment_id = Some(assignment.segment.id());
            self.publish_compactor_segment();
        }
        self.broadcast(write_result.seqnum).await
    }

    /// Publishes the latest `last_segment_id` to the compactor view at written
    /// latency. Called only when a new segment is created, so plain appends do
    /// no compactor work. No-op when compaction is disabled.
    fn publish_compactor_segment(&self) {
        let Some(cell) = self.compactor_view.as_ref() else {
            return;
        };
        let current = **cell.load();
        cell.store(Arc::new(CompactorView {
            last_segment_id: self.last_segment_id,
            ..current
        }));
    }

    /// Releases queued segment deletions to the compactor view once their write
    /// is durable. Pops every pending delete with `seqnum <= durable_seq` and
    /// publishes the highest resulting `last_deleted_segment_id`. No-op when
    /// compaction is disabled or nothing is newly durable.
    fn drain_compactor_deletes(&mut self, durable_seq: u64) {
        let Some(cell) = self.compactor_view.as_ref() else {
            return;
        };
        let mut latest_durable_delete = None;
        while self
            .pending_deletes
            .front()
            .is_some_and(|(seqnum, _)| *seqnum <= durable_seq)
        {
            latest_durable_delete = self
                .pending_deletes
                .pop_front()
                .map(|(_, deleted_id)| deleted_id);
        }
        if let Some(last_deleted_segment_id) = latest_durable_delete {
            let current = **cell.load();
            cell.store(Arc::new(CompactorView {
                last_deleted_segment_id,
                ..current
            }));
        }
    }

    /// Writes ops to storage. Returns the resulting `WriteResult`.
    async fn apply(&mut self, ops: Vec<RecordOp>) -> Result<common::storage::WriteResult, String> {
        let options = WriteOptions {
            await_durable: false,
        };
        let started = Instant::now();
        let result = self
            .storage
            .apply_with_options(ops, options)
            .await
            .map_err(|e| e.to_string());
        self.stats
            .storage_apply_ns
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        self.stats
            .storage_apply_count
            .fetch_add(1, Ordering::Relaxed);
        result
    }

    /// Takes a fresh storage snapshot, bumps the epoch, and broadcasts a
    /// new `WrittenView` using current watermarks. Must be called after a
    /// successful `apply` (and any post-write state updates).
    async fn broadcast(&mut self, seqnum: u64) -> Result<(), String> {
        let snapshot_start = Instant::now();
        let snapshot = self.storage.snapshot().await.map_err(|e| e.to_string())?;
        let after_snapshot = Instant::now();
        self.stats.storage_snapshot_ns.fetch_add(
            (after_snapshot - snapshot_start).as_nanos() as u64,
            Ordering::Relaxed,
        );
        self.stats
            .storage_snapshot_count
            .fetch_add(1, Ordering::Relaxed);
        self.epoch += 1;
        self.written_tx.send_replace(WrittenView {
            epoch: self.epoch,
            snapshot,
            seqnum,
            next_sequence: self.sequence_allocator.peek_next_sequence(),
            last_segment_id: self.last_segment_id,
            last_deleted_segment_id: self.last_deleted_segment_id,
        });
        self.watermarks.update_written(self.epoch);
        self.stats.broadcast_other_ns.fetch_add(
            after_snapshot.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );
        Ok(())
    }

    /// Flushes all pending writes to durable storage.
    async fn handle_flush(&mut self) -> Result<u64, String> {
        self.storage.flush().await.map_err(|e| e.to_string())?;
        self.watermarks.update_durable(self.epoch);
        Ok(self.epoch)
    }

    /// Builds log entry storage records from user records and appends them.
    fn add_entries(
        &self,
        segment: &LogSegment,
        base_sequence: u64,
        user_records: &[UserRecord],
        records: &mut Vec<PutRecordOp>,
    ) {
        let segment_start_seq = segment.meta().start_seq;
        for (i, user_record) in user_records.iter().enumerate() {
            let sequence = base_sequence + i as u64;
            let entry_key = LogEntryKey::new(segment.id(), user_record.key.clone(), sequence);
            let storage_record = common::Record::new(
                entry_key.serialize(segment_start_seq),
                user_record.value.clone(),
            );
            records.push(PutRecordOp::new_with_options(
                storage_record,
                PutOptions { ttl: NoExpiry },
            ));
        }
    }
}

/// Handle for sending commands to the writer task.
pub(crate) struct LogWriteHandle {
    cmd_tx: mpsc::Sender<WriterCommand>,
    /// Holds the receiver until `spawn()` is called.
    cmd_rx: Option<mpsc::Receiver<WriterCommand>>,
    written_rx: watch::Receiver<WrittenView>,
    watcher: EpochWatcher,
    stats: Arc<WriteStats>,
}

impl LogWriteHandle {
    /// Receives an append result from the writer task.
    async fn recv_append(
        rx: oneshot::Receiver<Result<Option<AppendOutput>, String>>,
    ) -> Result<Option<AppendOutput>, AppendError> {
        rx.await
            .map_err(|_| AppendError::Shutdown)?
            .map_err(AppendError::Storage)
    }

    /// Receives a unit result from the writer task.
    #[cfg(test)]
    async fn recv_cmd(
        rx: oneshot::Receiver<Result<(), String>>,
    ) -> Result<(), crate::error::Error> {
        rx.await
            .map_err(|_| crate::error::Error::Internal("writer shut down".into()))?
            .map_err(crate::error::Error::Storage)
    }

    /// Receives an epoch result from the writer task.
    async fn recv_epoch(
        rx: oneshot::Receiver<Result<u64, String>>,
    ) -> Result<u64, crate::error::Error> {
        rx.await
            .map_err(|_| crate::error::Error::Internal("writer shut down".into()))?
            .map_err(crate::error::Error::Storage)
    }

    /// Spawns the writer task and returns the join handle.
    pub(crate) fn spawn(&mut self, writer: LogWriter) -> JoinHandle<()> {
        let rx = self
            .cmd_rx
            .take()
            .expect("spawn() must be called exactly once");
        tokio::spawn(writer.run(rx))
    }

    /// Non-blocking append. Fails with QueueFull if the channel is full.
    pub(crate) async fn try_append(
        &self,
        write: LogWrite,
    ) -> Result<Option<AppendOutput>, AppendError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.cmd_tx
            .try_send(WriterCommand::Append { write, result_tx })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(WriterCommand::Append { write, .. }) => {
                    AppendError::QueueFull(write.records)
                }
                _ => AppendError::Shutdown,
            })?;
        Self::recv_append(result_rx).await
    }

    /// Blocking append with timeout.
    pub(crate) async fn append_timeout(
        &self,
        write: LogWrite,
        timeout: Duration,
    ) -> Result<Option<AppendOutput>, AppendError> {
        let (result_tx, result_rx) = oneshot::channel();
        self.cmd_tx
            .send_timeout(WriterCommand::Append { write, result_tx }, timeout)
            .await
            .map_err(|e| match e {
                mpsc::error::SendTimeoutError::Timeout(WriterCommand::Append { write, .. }) => {
                    AppendError::Timeout(write.records)
                }
                _ => AppendError::Shutdown,
            })?;
        Self::recv_append(result_rx).await
    }

    /// Forces the current segment to seal, creating a new one for subsequent writes.
    #[cfg(test)]
    pub(crate) async fn force_seal(&self, timestamp_ms: i64) -> Result<(), crate::error::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.cmd_tx
            .try_send(WriterCommand::ForceSeal {
                timestamp_ms,
                result_tx,
            })
            .map_err(|_| crate::error::Error::Internal("writer shut down".into()))?;
        Self::recv_cmd(result_rx).await
    }

    /// Flush all pending writes to durable storage.
    pub(crate) async fn flush(&self) -> Result<u64, crate::error::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.cmd_tx
            .send(WriterCommand::Flush { result_tx })
            .await
            .map_err(|_| crate::error::Error::Internal("writer shut down".into()))?;
        Self::recv_epoch(result_rx).await
    }

    /// Test-only command-driven delete of a sealed segment's `SegmentMeta`
    /// record. Production deletes go through `LogWriter::tick_retention`.
    #[cfg(test)]
    pub(crate) async fn delete_segment_meta(
        &self,
        segment_id: SegmentId,
    ) -> Result<(), crate::error::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.cmd_tx
            .send(WriterCommand::DeleteSegmentMeta {
                segment_id,
                result_tx,
            })
            .await
            .map_err(|_| crate::error::Error::Internal("writer shut down".into()))?;
        Self::recv_cmd(result_rx).await
    }

    /// The highest epoch that has reached written visibility.
    pub(crate) fn written_epoch(&self) -> u64 {
        *self.watcher.written_rx.borrow()
    }

    /// Returns a clone of the written view receiver.
    pub(crate) fn written_rx(&self) -> watch::Receiver<WrittenView> {
        self.written_rx.clone()
    }

    /// Returns the durable epoch (highest epoch that has been flushed to disk).
    pub(crate) fn durable_epoch(&self) -> u64 {
        *self.watcher.durable_rx.borrow()
    }

    /// Returns a clone of the cumulative write-path timing stats. The Arc is
    /// shared with the writer task; reads see whatever values were stored at
    /// the time of the load.
    pub(crate) fn stats(&self) -> Arc<WriteStats> {
        Arc::clone(&self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SegmentConfig;
    use crate::serde::SEQ_BLOCK_KEY;
    use common::{
        Storage,
        storage::in_memory::{FailingStorage, InMemoryStorage},
    };
    use opendata_macros::storage_test;

    async fn create_writer() -> (LogWriter, LogWriteHandle, Arc<dyn common::Storage>) {
        create_writer_with_config(LogWriterConfig::default()).await
    }

    async fn create_writer_with_config(
        config: LogWriterConfig,
    ) -> (LogWriter, LogWriteHandle, Arc<dyn common::Storage>) {
        let storage = std::sync::Arc::new(InMemoryStorage::default());
        create_writer_with_storage(storage, config).await
    }

    async fn create_writer_with_storage(
        storage: Arc<dyn common::Storage>,
        config: LogWriterConfig,
    ) -> (LogWriter, LogWriteHandle, Arc<dyn common::Storage>) {
        let clock: Arc<dyn Clock> = Arc::new(common::clock::SystemClock);
        create_writer_with_storage_and_clock(storage, clock, config).await
    }

    async fn create_writer_with_storage_and_clock(
        storage: Arc<dyn common::Storage>,
        clock: Arc<dyn Clock>,
        config: LogWriterConfig,
    ) -> (LogWriter, LogWriteHandle, Arc<dyn common::Storage>) {
        let seq_key = Bytes::from_static(&SEQ_BLOCK_KEY);
        let sequence_allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();
        let segment_cache = SegmentCache::open(storage.as_ref(), SegmentConfig::default())
            .await
            .unwrap();
        let listing_cache = ListingCache::new();

        let (writer, handle) = LogWriter::new(
            storage.clone(),
            sequence_allocator,
            segment_cache,
            listing_cache,
            clock,
            config,
        )
        .await
        .unwrap();

        (writer, handle, storage)
    }

    fn make_write(keys: &[&str], timestamp_ms: i64) -> LogWrite {
        LogWrite {
            records: keys
                .iter()
                .map(|k| UserRecord {
                    key: Bytes::from(k.to_string()),
                    value: Bytes::from(format!("value-{}", k)),
                })
                .collect(),
            timestamp_ms,
        }
    }

    #[tokio::test]
    async fn should_append_and_read_back() {
        let (writer, mut handle, storage) = create_writer().await;
        let _task = handle.spawn(writer);

        let result = handle
            .try_append(make_write(&["key1"], 1000))
            .await
            .unwrap();
        assert_eq!(result.unwrap().start_sequence, 0);

        // Verify written epoch advanced
        assert_eq!(handle.written_epoch(), 1);

        // Verify data is in storage
        handle.flush().await.unwrap();
        let seq_key = Bytes::from_static(&SEQ_BLOCK_KEY);
        let record = storage.get(seq_key).await.unwrap();
        assert!(record.is_some());
    }

    #[tokio::test]
    async fn should_append_multiple_batches() {
        let (writer, mut handle, _storage) = create_writer().await;
        let _task = handle.spawn(writer);

        let r1 = handle
            .try_append(make_write(&["k1", "k2"], 1000))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r1.start_sequence, 0);

        let r2 = handle
            .try_append(make_write(&["k3"], 1000))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2.start_sequence, 2);

        assert_eq!(handle.written_epoch(), 2);
    }

    #[tokio::test]
    async fn should_handle_empty_append() {
        let (writer, mut handle, _storage) = create_writer().await;
        let _task = handle.spawn(writer);

        let result = handle.try_append(make_write(&[], 1000)).await.unwrap();
        assert!(result.is_none());
        // Epoch should not advance for empty writes
        assert_eq!(handle.written_epoch(), 0);
    }

    #[tokio::test]
    async fn should_force_seal() {
        let (writer, mut handle, storage) = create_writer().await;
        let _task = handle.spawn(writer);

        // Append something to create the first user segment (id 1)
        handle
            .try_append(make_write(&["key1"], 1000))
            .await
            .unwrap();
        assert_eq!(handle.written_epoch(), 1);

        // Force seal — should create segment 2
        handle.force_seal(2000).await.unwrap();
        assert_eq!(handle.written_epoch(), 2);

        // Verify two user segments exist in storage
        handle.flush().await.unwrap();
        use crate::storage::LogStorageRead as _;
        let segments = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 1);
        assert_eq!(segments[1].id(), 2);

        // Subsequent append goes to segment 2
        let result = handle
            .try_append(make_write(&["key2"], 3000))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.start_sequence, 1);
    }

    #[tokio::test]
    async fn should_write_entries_to_storage() {
        let (writer, mut handle, storage) = create_writer().await;
        let _task = handle.spawn(writer);

        handle
            .try_append(make_write(&["mykey", "mykey"], 1000))
            .await
            .unwrap();
        handle.flush().await.unwrap();

        // Verify entries are readable from storage
        use crate::storage::LogStorageRead as _;
        let segments = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(segments.len(), 1);

        let key = Bytes::from("mykey");
        let mut iter = storage
            .scan_entries(&segments[0], &key, 0..u64::MAX)
            .await
            .unwrap();
        let e0 = iter.next().await.unwrap().unwrap();
        assert_eq!(e0.sequence, 0);
        assert_eq!(e0.value, Bytes::from("value-mykey"));
        let e1 = iter.next().await.unwrap().unwrap();
        assert_eq!(e1.sequence, 1);
        assert_eq!(e1.value, Bytes::from("value-mykey"));
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_broadcast_written_view_with_snapshot_and_segments() {
        let (writer, mut handle, _storage) = create_writer().await;
        let mut written_rx = handle.written_rx();
        let _task = handle.spawn(writer);

        handle
            .try_append(make_write(&["key1"], 1000))
            .await
            .unwrap();

        // Written view should reflect the append
        written_rx.changed().await.unwrap();
        let view = written_rx.borrow_and_update().clone();
        assert_eq!(view.epoch, 1);
        assert!(view.last_segment_id.is_some());
    }

    #[tokio::test]
    async fn should_advance_written_watermark_on_append() {
        let (writer, mut handle, _storage) = create_writer().await;
        let _task = handle.spawn(writer);

        assert_eq!(handle.written_epoch(), 0);
        assert_eq!(handle.durable_epoch(), 0);

        handle.try_append(make_write(&["k1"], 1000)).await.unwrap();

        // Written watermark should advance, durable should not
        assert_eq!(handle.written_epoch(), 1);
        assert_eq!(handle.durable_epoch(), 0);
    }

    #[tokio::test]
    async fn should_advance_durable_watermark_on_flush() {
        let (writer, mut handle, _storage) = create_writer().await;
        let _task = handle.spawn(writer);

        handle.try_append(make_write(&["k1"], 1000)).await.unwrap();
        assert_eq!(handle.written_epoch(), 1);
        assert_eq!(handle.durable_epoch(), 0);

        handle.flush().await.unwrap();

        // Both watermarks should now be at epoch 1
        assert_eq!(handle.written_epoch(), 1);
        assert_eq!(handle.durable_epoch(), 1);
    }

    #[storage_test]
    async fn should_propagate_put_error_on_append(storage: Arc<dyn Storage>) {
        let failing = FailingStorage::wrap(storage);
        let (writer, mut handle, _) =
            create_writer_with_storage(failing.clone(), LogWriterConfig::default()).await;
        let _task = handle.spawn(writer);

        // Enable apply failure after writer is running. Appends flow through
        // Storage::apply_with_options so we fail at that seam.
        failing.fail_apply(common::StorageError::Storage("test put error".into()));

        let result = handle.try_append(make_write(&["key1"], 1000)).await;
        assert!(
            matches!(&result, Err(AppendError::Storage(msg)) if msg.contains("test put error")),
            "expected Storage with test put error, got: {:?}",
            result,
        );

        // Epoch should not have advanced
        assert_eq!(handle.written_epoch(), 0);
    }

    #[storage_test]
    async fn should_propagate_snapshot_error_on_append(storage: Arc<dyn Storage>) {
        let failing = FailingStorage::wrap(storage);
        let (writer, mut handle, _) =
            create_writer_with_storage(failing.clone(), LogWriterConfig::default()).await;
        let _task = handle.spawn(writer);

        // Snapshot is taken inside write_and_broadcast after a successful put.
        // Fail only snapshot, not put.
        failing.fail_snapshot(common::StorageError::Storage("test snapshot error".into()));

        let result = handle.try_append(make_write(&["key1"], 1000)).await;
        assert!(
            matches!(&result, Err(AppendError::Storage(msg)) if msg.contains("test snapshot error")),
            "expected Storage with test snapshot error, got: {:?}",
            result,
        );
    }

    #[storage_test]
    async fn should_propagate_flush_error(storage: Arc<dyn Storage>) {
        let failing = FailingStorage::wrap(storage);
        let (writer, mut handle, _) =
            create_writer_with_storage(failing.clone(), LogWriterConfig::default()).await;
        let _task = handle.spawn(writer);

        // Append succeeds
        handle
            .try_append(make_write(&["key1"], 1000))
            .await
            .unwrap();

        // Enable flush failure
        failing.fail_flush(common::StorageError::Storage("test flush error".into()));

        let result = handle.flush().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("test flush error"),
            "expected test flush error, got: {}",
            err,
        );
    }

    #[storage_test]
    async fn should_enter_fatal_state_after_put_error(storage: Arc<dyn Storage>) {
        let failing = FailingStorage::wrap(storage);
        let (writer, mut handle, _) =
            create_writer_with_storage(failing.clone(), LogWriterConfig::default()).await;
        let _task = handle.spawn(writer);

        // First append fails. Appends go through Storage::apply_with_options.
        failing.fail_apply_once(common::StorageError::Storage("test put error".into()));
        let result = handle.try_append(make_write(&["key1"], 1000)).await;
        assert!(result.is_err());

        // FIXME: After a write failure the writer should enter a fatal state and
        // reject subsequent appends. Currently it recovers, which is incorrect.
        let result = handle
            .try_append(make_write(&["key2"], 2000))
            .await
            .unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn should_return_shutdown_on_try_append_when_writer_dropped() {
        let (writer, mut handle, _) = create_writer().await;
        // Spawn the writer then immediately drop its task to simulate crash
        let task = handle.spawn(writer);
        task.abort();
        let _ = task.await;

        let result = handle.try_append(make_write(&["k"], 1000)).await;
        assert!(
            matches!(result, Err(AppendError::Shutdown)),
            "expected Shutdown, got: {:?}",
            result,
        );
    }

    #[tokio::test]
    async fn should_delete_segment_meta_and_advance_watermarks() {
        // given: writer has produced two user segments
        let (writer, mut handle, storage) = create_writer().await;
        let mut written_rx = handle.written_rx();
        let _task = handle.spawn(writer);

        handle.try_append(make_write(&["k1"], 1000)).await.unwrap();
        handle.force_seal(2000).await.unwrap();
        handle.flush().await.unwrap();

        use crate::storage::LogStorageRead as _;
        let segs_before = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(
            segs_before.iter().map(|s| s.id()).collect::<Vec<_>>(),
            vec![1, 2]
        );

        let epoch_before = handle.written_epoch();

        // when: delete segment 1's SegmentMeta
        handle.delete_segment_meta(1).await.unwrap();

        // then: storage scan no longer sees segment 1
        handle.flush().await.unwrap();
        let segs_after = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(
            segs_after.iter().map(|s| s.id()).collect::<Vec<_>>(),
            vec![2]
        );

        // and: the broadcast carries the deletion watermark
        let view = written_rx.borrow_and_update().clone();
        assert_eq!(view.last_deleted_segment_id, Some(1));
        assert_eq!(view.last_segment_id, Some(2));
        assert!(handle.written_epoch() > epoch_before);
    }

    #[tokio::test]
    async fn should_be_idempotent_when_deleting_unknown_segment() {
        // given: writer with no segments yet
        let (writer, mut handle, _storage) = create_writer().await;
        let mut written_rx = handle.written_rx();
        let _task = handle.spawn(writer);

        // when: delete a never-created segment
        handle.delete_segment_meta(42).await.unwrap();

        // then: the watermark still advances (subscribers can converge),
        // and no error is surfaced
        let view = written_rx.borrow_and_update().clone();
        assert_eq!(view.last_deleted_segment_id, Some(42));
    }

    #[tokio::test]
    async fn should_not_regress_deletion_watermark() {
        // given: a writer with two sealed segments deleted out of order
        let (writer, mut handle, _storage) = create_writer().await;
        let mut written_rx = handle.written_rx();
        let _task = handle.spawn(writer);
        handle.try_append(make_write(&["k1"], 1000)).await.unwrap();
        handle.force_seal(2000).await.unwrap();
        handle.force_seal(3000).await.unwrap();

        // when: delete the higher id first, then the lower id
        handle.delete_segment_meta(2).await.unwrap();
        handle.delete_segment_meta(1).await.unwrap();

        // then: watermark is the max of the two, not the most recent
        let view = written_rx.borrow_and_update().clone();
        assert_eq!(view.last_deleted_segment_id, Some(2));
    }

    // ---- Compactor-view publication tests ----

    use arc_swap::ArcSwap;

    fn empty_compactor_view() -> CompactorViewCell {
        Arc::new(ArcSwap::from_pointee(CompactorView {
            last_segment_id: None,
            last_deleted_segment_id: None,
        }))
    }

    async fn wait_for_deleted(cell: &CompactorViewCell, expected: Option<SegmentId>) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if cell.load().last_deleted_segment_id == expected {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("compactor view did not reflect durable delete in time");
    }

    #[tokio::test]
    async fn compactor_view_should_publish_new_segment_at_written_latency() {
        let cell = empty_compactor_view();
        let config = LogWriterConfig {
            compactor_view: Some(cell.clone()),
            ..LogWriterConfig::default()
        };
        let (writer, mut handle, _storage) = create_writer_with_config(config).await;
        let _task = handle.spawn(writer);

        // The first append creates segment 1; its id is published immediately,
        // without waiting for durability.
        handle.try_append(make_write(&["k1"], 1000)).await.unwrap();

        let view = **cell.load();
        assert_eq!(view.last_segment_id, Some(1));
        assert_eq!(view.last_deleted_segment_id, None);
    }

    #[tokio::test]
    async fn compactor_view_should_delay_deleted_segment_until_durable() {
        let storage: Arc<dyn common::Storage> =
            Arc::new(InMemoryStorage::new().with_deferred_durability());
        let cell = empty_compactor_view();
        let config = LogWriterConfig {
            compactor_view: Some(cell.clone()),
            ..LogWriterConfig::default()
        };
        let (writer, mut handle, _storage) = create_writer_with_storage(storage, config).await;
        let _task = handle.spawn(writer);

        // Create two user segments (ids 1 and 2).
        handle.try_append(make_write(&["k1"], 1000)).await.unwrap();
        handle.force_seal(2000).await.unwrap();
        assert_eq!(cell.load().last_segment_id, Some(2));

        // Delete segment 1. Its write isn't durable yet (deferred durability),
        // so the deletion stays hidden from the compactor view.
        handle.delete_segment_meta(1).await.unwrap();
        assert_eq!(
            cell.load().last_deleted_segment_id,
            None,
            "delete should remain hidden until durable",
        );

        // Flushing makes the delete durable; the writer's durable branch then
        // releases it to the compactor view.
        handle.flush().await.unwrap();
        wait_for_deleted(&cell, Some(1)).await;
    }

    #[storage_test]
    async fn should_propagate_storage_error_on_delete(storage: Arc<dyn Storage>) {
        let failing = FailingStorage::wrap(storage);
        let (writer, mut handle, _) =
            create_writer_with_storage(failing.clone(), LogWriterConfig::default()).await;
        let _task = handle.spawn(writer);

        // Force the next apply call to fail.
        failing.fail_apply(common::StorageError::Storage("delete failed".into()));

        let result = handle.delete_segment_meta(1).await;
        assert!(
            matches!(&result, Err(crate::error::Error::Storage(msg)) if msg.contains("delete failed")),
            "expected Storage error, got: {:?}",
            result,
        );
    }

    // ---- Retention tick tests ----
    //
    // These tests drive `tick_retention` directly on a non-spawned writer.
    // That tests the policy logic in isolation; the periodic interval that
    // calls into it from `run()` is tokio's responsibility.

    use common::clock::MockClock;

    fn retention_policy(secs: u64) -> RetentionPolicy {
        RetentionPolicy {
            retention: Duration::from_secs(secs),
            check_interval: Duration::from_millis(1),
        }
    }

    async fn create_writer_for_retention(
        clock: Arc<MockClock>,
        retention_secs: u64,
    ) -> (LogWriter, LogWriteHandle, Arc<dyn common::Storage>) {
        let storage = Arc::new(InMemoryStorage::default());
        let config = LogWriterConfig {
            retention: Some(retention_policy(retention_secs)),
            ..LogWriterConfig::default()
        };
        create_writer_with_storage_and_clock(storage, clock, config).await
    }

    fn ms_after_epoch(secs: u64) -> i64 {
        (secs as i64) * 1000
    }

    fn epoch_plus(secs: u64) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[tokio::test]
    async fn tick_retention_is_noop_when_no_segments() {
        let clock = Arc::new(MockClock::with_time(epoch_plus(10_000)));
        let (mut writer, _h, storage) = create_writer_for_retention(Arc::clone(&clock), 60).await;

        writer.tick_retention().await;

        use crate::storage::LogStorageRead as _;
        let segs = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(segs.len(), 0);
    }

    #[tokio::test]
    async fn tick_retention_skips_the_active_segment() {
        // Active segment alone has no successor → no end-time → never expires.
        let clock = Arc::new(MockClock::with_time(epoch_plus(10_000)));
        let (mut writer, _h, storage) = create_writer_for_retention(Arc::clone(&clock), 60).await;

        writer
            .handle_append(LogWrite {
                records: vec![UserRecord {
                    key: Bytes::from("k"),
                    value: Bytes::from("v"),
                }],
                timestamp_ms: ms_after_epoch(1_000),
            })
            .await
            .unwrap();

        clock.set_time(epoch_plus(100_000)); // far past any plausible retention
        writer.tick_retention().await;

        use crate::storage::LogStorageRead as _;
        let segs = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(segs.iter().map(|s| s.id()).collect::<Vec<_>>(), vec![1]);
    }

    #[tokio::test]
    async fn tick_retention_keeps_sealed_segment_within_retention() {
        // Segment 1 sealed at T=2000s, segment 2 active. At T=2030s with 60s
        // retention, segment 1's end (2000s) is not yet older than 1970s.
        let clock = Arc::new(MockClock::with_time(epoch_plus(2_030)));
        let (mut writer, _h, storage) = create_writer_for_retention(Arc::clone(&clock), 60).await;
        writer
            .handle_append(LogWrite {
                records: vec![UserRecord {
                    key: Bytes::from("k"),
                    value: Bytes::from("v"),
                }],
                timestamp_ms: ms_after_epoch(1_000),
            })
            .await
            .unwrap();
        writer
            .handle_force_seal(ms_after_epoch(2_000))
            .await
            .unwrap();

        writer.tick_retention().await;

        use crate::storage::LogStorageRead as _;
        let segs = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(segs.iter().map(|s| s.id()).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[tokio::test]
    async fn tick_retention_deletes_expired_sealed_segment() {
        // Segment 1 sealed at T=2000s (its successor's start). At T=10_000s
        // with 60s retention, the cutoff is 9940s; segment 1's end (2000s)
        // is well past expiry.
        let clock = Arc::new(MockClock::with_time(epoch_plus(10_000)));
        let (mut writer, _h, storage) = create_writer_for_retention(Arc::clone(&clock), 60).await;
        writer
            .handle_append(LogWrite {
                records: vec![UserRecord {
                    key: Bytes::from("k"),
                    value: Bytes::from("v"),
                }],
                timestamp_ms: ms_after_epoch(1_000),
            })
            .await
            .unwrap();
        writer
            .handle_force_seal(ms_after_epoch(2_000))
            .await
            .unwrap();

        writer.tick_retention().await;

        use crate::storage::LogStorageRead as _;
        let segs = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(segs.iter().map(|s| s.id()).collect::<Vec<_>>(), vec![2]);
    }

    #[tokio::test]
    async fn tick_retention_deletes_all_expired_segments_in_one_tick() {
        let clock = Arc::new(MockClock::with_time(epoch_plus(10_000)));
        let (mut writer, _h, storage) = create_writer_for_retention(Arc::clone(&clock), 60).await;
        writer
            .handle_append(LogWrite {
                records: vec![UserRecord {
                    key: Bytes::from("k"),
                    value: Bytes::from("v0"),
                }],
                timestamp_ms: ms_after_epoch(1_000),
            })
            .await
            .unwrap();
        writer
            .handle_force_seal(ms_after_epoch(2_000))
            .await
            .unwrap();
        writer
            .handle_append(LogWrite {
                records: vec![UserRecord {
                    key: Bytes::from("k"),
                    value: Bytes::from("v1"),
                }],
                timestamp_ms: ms_after_epoch(2_000),
            })
            .await
            .unwrap();
        writer
            .handle_force_seal(ms_after_epoch(3_000))
            .await
            .unwrap();

        writer.tick_retention().await;

        use crate::storage::LogStorageRead as _;
        let segs = storage.scan_segments(1..u32::MAX).await.unwrap();
        // Both seg 1 (end 2000s) and seg 2 (end 3000s) are well past expiry.
        assert_eq!(segs.iter().map(|s| s.id()).collect::<Vec<_>>(), vec![3]);
    }

    #[tokio::test]
    async fn tick_retention_is_idempotent_across_ticks() {
        let clock = Arc::new(MockClock::with_time(epoch_plus(10_000)));
        let (mut writer, _h, storage) = create_writer_for_retention(Arc::clone(&clock), 60).await;
        writer
            .handle_append(LogWrite {
                records: vec![UserRecord {
                    key: Bytes::from("k"),
                    value: Bytes::from("v"),
                }],
                timestamp_ms: ms_after_epoch(1_000),
            })
            .await
            .unwrap();
        writer
            .handle_force_seal(ms_after_epoch(2_000))
            .await
            .unwrap();

        writer.tick_retention().await;
        writer.tick_retention().await; // second tick: cache no longer has seg 1

        use crate::storage::LogStorageRead as _;
        let segs = storage.scan_segments(1..u32::MAX).await.unwrap();
        assert_eq!(segs.iter().map(|s| s.id()).collect::<Vec<_>>(), vec![2]);
    }

    #[tokio::test]
    async fn tick_retention_advances_deletion_watermark() {
        let clock = Arc::new(MockClock::with_time(epoch_plus(10_000)));
        let (mut writer, handle, _storage) =
            create_writer_for_retention(Arc::clone(&clock), 60).await;
        let mut written_rx = handle.written_rx();
        writer
            .handle_append(LogWrite {
                records: vec![UserRecord {
                    key: Bytes::from("k"),
                    value: Bytes::from("v"),
                }],
                timestamp_ms: ms_after_epoch(1_000),
            })
            .await
            .unwrap();
        writer
            .handle_force_seal(ms_after_epoch(2_000))
            .await
            .unwrap();

        writer.tick_retention().await;

        let view = written_rx.borrow_and_update().clone();
        assert_eq!(view.last_deleted_segment_id, Some(1));
    }

    #[tokio::test]
    async fn tick_retention_does_nothing_when_disabled() {
        // Build writer with retention = None. Even after segments are sealed,
        // tick_retention should be a no-op (run() never calls it in this case,
        // but invoking it directly should still be safe).
        let storage = Arc::new(InMemoryStorage::default());
        let clock: Arc<dyn Clock> = Arc::new(MockClock::with_time(epoch_plus(10_000)));
        let (mut writer, _h, _storage) = create_writer_with_storage_and_clock(
            storage.clone() as Arc<dyn common::Storage>,
            clock,
            LogWriterConfig::default(),
        )
        .await;
        writer
            .handle_append(LogWrite {
                records: vec![UserRecord {
                    key: Bytes::from("k"),
                    value: Bytes::from("v"),
                }],
                timestamp_ms: ms_after_epoch(1_000),
            })
            .await
            .unwrap();
        writer
            .handle_force_seal(ms_after_epoch(2_000))
            .await
            .unwrap();

        writer.tick_retention().await;

        use crate::storage::LogStorageRead as _;
        let segs = (storage as Arc<dyn common::Storage>)
            .scan_segments(1..u32::MAX)
            .await
            .unwrap();
        assert_eq!(segs.iter().map(|s| s.id()).collect::<Vec<_>>(), vec![1, 2]);
    }

    #[tokio::test]
    async fn should_return_shutdown_on_flush_when_writer_dropped() {
        let (writer, mut handle, _) = create_writer().await;
        let task = handle.spawn(writer);
        task.abort();
        let _ = task.await;

        let result = handle.flush().await;
        assert!(result.is_err());
    }
}

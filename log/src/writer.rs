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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::Ttl::NoExpiry;
use common::coordinator::{EpochWatcher, EpochWatermarks};
use common::storage::PutOptions;
use common::{PutRecordOp, SequenceAllocator, WriteOptions};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::error::AppendError;
use crate::listing::ListingCache;
use crate::model::{AppendOutput, Record as UserRecord};
use crate::segment::{LogSegment, SegmentAssignment, SegmentCache};
use crate::serde::LogEntryKey;

/// Snapshot of writer state broadcast to subscribers after each write.
#[derive(Clone)]
pub(crate) struct WrittenView {
    pub epoch: u64,
    pub snapshot: Arc<dyn common::storage::StorageSnapshot>,
    pub segments: Arc<[LogSegment]>,
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
    Flush {
        result_tx: oneshot::Sender<Result<(), String>>,
    },
}

/// Configuration for the log writer.
pub(crate) struct LogWriterConfig {
    pub queue_capacity: usize,
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
        }
    }
}

/// The log writer task. Owns all write-side state and runs as a spawned async task.
pub(crate) struct LogWriter {
    storage: Arc<dyn common::Storage>,
    sequence_allocator: SequenceAllocator,
    segment_cache: SegmentCache,
    listing_cache: ListingCache,
    epoch: u64,
    written_tx: watch::Sender<WrittenView>,
    segments_snapshot: Arc<[LogSegment]>,
    watermarks: EpochWatermarks,
}

impl LogWriter {
    /// Creates a new writer and returns the writer + channels for spawning.
    pub(crate) async fn new(
        storage: Arc<dyn common::Storage>,
        sequence_allocator: SequenceAllocator,
        segment_cache: SegmentCache,
        listing_cache: ListingCache,
        config: LogWriterConfig,
    ) -> Result<(Self, LogWriteHandle), String> {
        let (cmd_tx, cmd_rx) = mpsc::channel(config.queue_capacity);

        let initial_snapshot = storage.snapshot().await.map_err(|e| e.to_string())?;
        let segments_snapshot: Arc<[LogSegment]> = segment_cache.all().into();
        let initial_view = WrittenView {
            epoch: 0,
            snapshot: initial_snapshot,
            segments: Arc::clone(&segments_snapshot),
        };
        let (written_tx, written_rx) = watch::channel(initial_view);
        let (watermarks, watcher) = EpochWatermarks::new();

        let writer = Self {
            storage,
            sequence_allocator,
            segment_cache,
            listing_cache,
            epoch: 0,
            written_tx,
            segments_snapshot,
            watermarks,
        };

        let handle = LogWriteHandle {
            cmd_tx,
            cmd_rx: Some(cmd_rx),
            written_rx,
            watcher,
        };

        Ok((writer, handle))
    }

    /// Runs the writer loop, processing commands from the receiver.
    pub(crate) async fn run(mut self, mut rx: mpsc::Receiver<WriterCommand>) {
        while let Some(cmd) = rx.recv().await {
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
                WriterCommand::Flush { result_tx } => {
                    let result = self.handle_flush().await;
                    let _ = result_tx.send(result);
                }
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

        let mut records = Vec::new();

        // 1. Allocate sequences
        let (base_seq, maybe_block_record) = self.sequence_allocator.allocate(count);
        if let Some(r) = maybe_block_record {
            records.push(PutRecordOp::new_with_options(
                r,
                PutOptions { ttl: NoExpiry },
            ));
        }

        // 2. Assign segment (appends segment metadata record if new)
        let assignment =
            self.segment_cache
                .assign_segment(write.timestamp_ms, base_seq, &mut records, false);

        // 3. Assign listing entries for new keys
        let keys: Vec<Bytes> = write.records.iter().map(|r| r.key.clone()).collect();
        self.listing_cache
            .assign_new_keys(assignment.segment.id(), &keys, &mut records);

        // 4. Build log entry records
        self.add_entries(&assignment.segment, base_seq, &write.records, &mut records);

        // 5. Write to storage and broadcast
        self.write_and_broadcast(records, &assignment).await?;

        Ok(Some(AppendOutput {
            start_sequence: base_seq,
        }))
    }

    /// Seals the current segment, forcing subsequent writes into a new one.
    #[cfg(test)]
    async fn handle_force_seal(&mut self, timestamp_ms: i64) -> Result<(), String> {
        let next_seq = self.sequence_allocator.peek_next_sequence();
        let mut records = Vec::new();
        let assignment =
            self.segment_cache
                .assign_segment(timestamp_ms, next_seq, &mut records, true);

        if !records.is_empty() {
            self.write_and_broadcast(records, &assignment).await?;
        }

        Ok(())
    }

    /// Writes records to storage, updates the segments snapshot if needed,
    /// takes a new storage snapshot, bumps the epoch, and broadcasts.
    async fn write_and_broadcast(
        &mut self,
        records: Vec<PutRecordOp>,
        assignment: &SegmentAssignment,
    ) -> Result<(), String> {
        let options = WriteOptions {
            await_durable: false,
        };
        self.storage
            .put_with_options(records, options)
            .await
            .map_err(|e| e.to_string())?;

        if assignment.is_new {
            self.segments_snapshot = self.segment_cache.all().into();
        }

        let snapshot = self.storage.snapshot().await.map_err(|e| e.to_string())?;
        self.epoch += 1;
        self.written_tx.send_replace(WrittenView {
            epoch: self.epoch,
            snapshot,
            segments: Arc::clone(&self.segments_snapshot),
        });
        self.watermarks.update_written(self.epoch);
        Ok(())
    }

    /// Flushes all pending writes to durable storage.
    async fn handle_flush(&mut self) -> Result<(), String> {
        self.storage.flush().await.map_err(|e| e.to_string())?;
        self.watermarks.update_durable(self.epoch);
        Ok(())
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
    async fn recv_cmd(
        rx: oneshot::Receiver<Result<(), String>>,
    ) -> Result<(), crate::error::Error> {
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
    pub(crate) async fn flush(&self) -> Result<(), crate::error::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.cmd_tx
            .send(WriterCommand::Flush { result_tx })
            .await
            .map_err(|_| crate::error::Error::Internal("writer shut down".into()))?;
        Self::recv_cmd(result_rx).await
    }

    /// The highest epoch flushed to storage (but not necessarily durable).
    pub(crate) fn flushed_epoch(&self) -> u64 {
        *self.watcher.written_rx.borrow()
    }

    /// Returns a clone of the written view receiver.
    pub(crate) fn written_rx(&self) -> watch::Receiver<WrittenView> {
        self.written_rx.clone()
    }

    /// Returns the durable epoch (highest epoch that has been flushed to disk).
    #[cfg(test)]
    pub(crate) fn durable_epoch(&self) -> u64 {
        *self.watcher.durable_rx.borrow()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SegmentConfig;
    use crate::serde::SEQ_BLOCK_KEY;
    use crate::storage::in_memory_storage;
    use common::storage::in_memory::FailingStorage;

    async fn create_writer() -> (LogWriter, LogWriteHandle, Arc<dyn common::Storage>) {
        create_writer_with_config(LogWriterConfig::default()).await
    }

    async fn create_writer_with_config(
        config: LogWriterConfig,
    ) -> (LogWriter, LogWriteHandle, Arc<dyn common::Storage>) {
        let storage = in_memory_storage();
        create_writer_with_storage(storage, config).await
    }

    async fn create_writer_with_storage(
        storage: Arc<dyn common::Storage>,
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

        // Verify flushed epoch advanced
        assert_eq!(handle.flushed_epoch(), 1);

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

        assert_eq!(handle.flushed_epoch(), 2);
    }

    #[tokio::test]
    async fn should_handle_empty_append() {
        let (writer, mut handle, _storage) = create_writer().await;
        let _task = handle.spawn(writer);

        let result = handle.try_append(make_write(&[], 1000)).await.unwrap();
        assert!(result.is_none());
        // Epoch should not advance for empty writes
        assert_eq!(handle.flushed_epoch(), 0);
    }

    #[tokio::test]
    async fn should_force_seal() {
        let (writer, mut handle, storage) = create_writer().await;
        let _task = handle.spawn(writer);

        // Append something to create segment 0
        handle
            .try_append(make_write(&["key1"], 1000))
            .await
            .unwrap();
        assert_eq!(handle.flushed_epoch(), 1);

        // Force seal — should create segment 1
        handle.force_seal(2000).await.unwrap();
        assert_eq!(handle.flushed_epoch(), 2);

        // Verify two segments exist in storage
        handle.flush().await.unwrap();
        use crate::storage::LogStorageRead as _;
        let segments = storage.scan_segments(0..u32::MAX).await.unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);

        // Subsequent append goes to segment 1
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
        let segments = storage.scan_segments(0..u32::MAX).await.unwrap();
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
        assert_eq!(view.segments.len(), 1);
    }

    #[tokio::test]
    async fn should_advance_flushed_watermark_on_append() {
        let (writer, mut handle, _storage) = create_writer().await;
        let _task = handle.spawn(writer);

        assert_eq!(handle.flushed_epoch(), 0);
        assert_eq!(handle.durable_epoch(), 0);

        handle.try_append(make_write(&["k1"], 1000)).await.unwrap();

        // Written watermark should advance, durable should not
        assert_eq!(handle.flushed_epoch(), 1);
        assert_eq!(handle.durable_epoch(), 0);
    }

    #[tokio::test]
    async fn should_advance_durable_watermark_on_flush() {
        let (writer, mut handle, _storage) = create_writer().await;
        let _task = handle.spawn(writer);

        handle.try_append(make_write(&["k1"], 1000)).await.unwrap();
        assert_eq!(handle.flushed_epoch(), 1);
        assert_eq!(handle.durable_epoch(), 0);

        handle.flush().await.unwrap();

        // Both watermarks should now be at epoch 1
        assert_eq!(handle.flushed_epoch(), 1);
        assert_eq!(handle.durable_epoch(), 1);
    }

    #[tokio::test]
    async fn should_propagate_put_error_on_append() {
        let inner = in_memory_storage();
        let failing = FailingStorage::wrap(inner);
        let (writer, mut handle, _) =
            create_writer_with_storage(failing.clone(), LogWriterConfig::default()).await;
        let _task = handle.spawn(writer);

        // Enable put failure after writer is running
        failing.fail_put(common::StorageError::Storage("test put error".into()));

        let result = handle.try_append(make_write(&["key1"], 1000)).await;
        assert!(
            matches!(&result, Err(AppendError::Storage(msg)) if msg.contains("test put error")),
            "expected Storage with test put error, got: {:?}",
            result,
        );

        // Epoch should not have advanced
        assert_eq!(handle.flushed_epoch(), 0);
    }

    #[tokio::test]
    async fn should_propagate_snapshot_error_on_append() {
        let inner = in_memory_storage();
        let failing = FailingStorage::wrap(inner);
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

    #[tokio::test]
    async fn should_propagate_flush_error() {
        let inner = in_memory_storage();
        let failing = FailingStorage::wrap(inner);
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

    #[tokio::test]
    async fn should_enter_fatal_state_after_put_error() {
        let inner = in_memory_storage();
        let failing = FailingStorage::wrap(inner);
        let (writer, mut handle, _) =
            create_writer_with_storage(failing.clone(), LogWriterConfig::default()).await;
        let _task = handle.spawn(writer);

        // First append fails
        failing.fail_put_once(common::StorageError::Storage("test put error".into()));
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
    async fn should_return_shutdown_on_flush_when_writer_dropped() {
        let (writer, mut handle, _) = create_writer().await;
        let task = handle.spawn(writer);
        task.abort();
        let _ = task.await;

        let result = handle.flush().await;
        assert!(result.is_err());
    }
}

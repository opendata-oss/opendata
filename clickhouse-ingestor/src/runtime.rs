//! `BufferConsumerRuntime` — the polling loop that wires the layers.
//!
//! Drives `tokio::select!` between [`buffer::Consumer::next_batch`] and
//! the open commit group's `time_until_max_age` deadline so a partial
//! group flushes on the timer even when the producer is idle.
//!
//! The loop sequence:
//!
//! 1. Wait for a Buffer batch *or* a timer wakeup.
//! 2. Decode metadata envelopes per entry, validate against the
//!    configured signal/encoding.
//! 3. Run the signal decoder.
//! 4. Append to the commit group, advancing the input high-watermark
//!    even when the decoded record list is empty.
//! 5. If `should_flush()` trips, drain the group, plan insert chunks,
//!    execute them through the writer (skipped in dry-run), then ack
//!    the contiguous range and flush per the [`AckFlushPolicy`].
//! 6. Repeat.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::ack::{AckController, AckFlushPolicy};
use crate::adapter::Adapter;
use crate::commit_group::{CommitGroup, CommitGroupBatch, CommitGroupThresholds, RecordSize};
use crate::envelope::{ConfiguredEnvelope, decode_envelopes, validate_consistent};
use crate::error::{IngestorError, IngestorResult};
use crate::signal::SignalDecoder;
use crate::source::split_into_raw_entries;
use crate::writer::ClickHouseWriter;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeOptions {
    /// Path to the manifest in the configured object store.
    pub manifest_path: String,
    /// Object-store path prefix for batch data objects.
    pub data_path_prefix: String,
    /// Configured envelope every entry must match.
    pub configured_envelope: ConfiguredEnvelope,
    /// Commit-group thresholds (rows/bytes/age).
    pub commit_group: CommitGroupThresholds,
    /// Ack flush policy (per group, every N groups).
    pub ack_flush_policy: AckFlushPolicy,
    /// When true, run the full pipeline but skip the ClickHouse insert
    /// and the Buffer ack/flush. Toggling this requires a process
    /// restart; the runtime never re-reads it mid-flight.
    pub dry_run: bool,
    /// How long to sleep when `next_batch` returns `None` (no entries
    /// available yet).
    pub poll_interval: Duration,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            manifest_path: "ingest/otel/logs/manifest".into(),
            data_path_prefix: "ingest/otel/logs/data".into(),
            configured_envelope: ConfiguredEnvelope {
                version: 1,
                signal_type: crate::envelope::SignalType::Logs,
                encoding: crate::envelope::PayloadEncoding::OtlpProtobuf,
            },
            commit_group: CommitGroupThresholds::default(),
            ack_flush_policy: AckFlushPolicy::default(),
            dry_run: true,
            poll_interval: Duration::from_millis(250),
        }
    }
}

/// Counters published via the watch channel for tests/metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeProgress {
    pub batches_read: u64,
    pub commit_groups_flushed: u64,
    pub rows_planned: u64,
    pub rows_inserted: u64,
    pub last_decoded_sequence: Option<u64>,
    pub last_acked_sequence: Option<u64>,
}

pub struct BufferConsumerRuntime<D, A>
where
    D: SignalDecoder,
    A: Adapter,
{
    consumer: buffer::Consumer,
    decoder: D,
    adapter: A,
    writer: Option<ClickHouseWriter>,
    ack_controller: AckController,
    options: RuntimeOptions,
    progress_tx: watch::Sender<RuntimeProgress>,
    progress_rx: watch::Receiver<RuntimeProgress>,
}

impl<D, A> BufferConsumerRuntime<D, A>
where
    D: SignalDecoder<Output = Vec<A::Input>>,
    A: Adapter,
    A::Input: RecordSize,
{
    /// Construct a runtime. The caller owns the [`buffer::Consumer`] and
    /// hands it in here so manifest mutations stay serialized.
    pub fn new(
        consumer: buffer::Consumer,
        decoder: D,
        adapter: A,
        writer: Option<ClickHouseWriter>,
        options: RuntimeOptions,
    ) -> Self {
        let (progress_tx, progress_rx) = watch::channel(RuntimeProgress::default());
        let ack_controller = AckController::new(options.ack_flush_policy);
        Self {
            consumer,
            decoder,
            adapter,
            writer,
            ack_controller,
            options,
            progress_tx,
            progress_rx,
        }
    }

    pub fn options(&self) -> &RuntimeOptions {
        &self.options
    }

    /// Subscribe to a stream of progress snapshots; tests use this to
    /// observe forward progress without poking at internals.
    pub fn progress(&self) -> watch::Receiver<RuntimeProgress> {
        self.progress_rx.clone()
    }

    /// Run until cancellation. On cancellation we drain any in-flight
    /// commit group and force a final flush so the durable
    /// high-watermark matches what we've seen.
    pub async fn run(mut self, shutdown: CancellationToken) -> IngestorResult<()> {
        let mut group: CommitGroup<A::Input> = CommitGroup::new(self.options.commit_group);
        let mut progress = RuntimeProgress::default();
        info!(
            manifest_path = %self.options.manifest_path,
            dry_run = self.options.dry_run,
            "starting buffer consumer runtime",
        );

        loop {
            // Compute the timer deadline for this iteration. We always
            // build a sleep — even when the group is empty we want to
            // wake periodically to re-check shutdown without blocking
            // forever on `next_batch`.
            let timer_remaining = group
                .time_until_max_age(std::time::Instant::now())
                .unwrap_or(self.options.commit_group.max_age);

            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("shutdown requested; finalizing in-flight group");
                    break;
                }
                next = self.consumer.next_batch() => {
                    match next? {
                        Some(batch) => {
                            progress.batches_read = progress.batches_read.saturating_add(1);
                            progress.last_decoded_sequence = Some(batch.sequence);
                            self.handle_batch(batch, &mut group)?;
                        }
                        None => {
                            tokio::time::sleep(self.options.poll_interval).await;
                        }
                    }
                }
                _ = tokio::time::sleep(timer_remaining) => {
                    // Timer wakeup; the flush check below will catch the
                    // max-age trip if it happened.
                }
            }

            if group.should_flush() {
                let drained = group.drain();
                self.flush_group(drained, &mut progress).await?;
            }

            let _ = self.progress_tx.send(progress);
        }

        // Drain on shutdown so the last partial group's input progress
        // is durable.
        if !group.is_empty() {
            let drained = group.drain();
            self.flush_group(drained, &mut progress).await?;
        }
        if !self.options.dry_run {
            // Belt-and-suspenders flush in case the policy was EveryN
            // and we exited mid-window.
            self.ack_controller.force_flush(&mut self.consumer).await?;
        }
        let _ = self.progress_tx.send(progress);
        info!("runtime exited cleanly");
        Ok(())
    }

    fn handle_batch(
        &self,
        batch: buffer::ConsumedBatch,
        group: &mut CommitGroup<A::Input>,
    ) -> IngestorResult<()>
    where
        A::Input: RecordSize,
    {
        let raw = split_into_raw_entries(batch, &self.options.manifest_path);
        let envelopes = decode_envelopes(&raw)?;
        validate_consistent(&envelopes, &self.options.configured_envelope)?;
        let decoded = self.decoder.decode(&raw, &envelopes)?;
        debug!(
            sequence = raw.sequence,
            entries = raw.entries.len(),
            decoded_records = decoded.len(),
            "decoded buffer batch",
        );
        group.append(decoded, raw.sequence);
        Ok(())
    }

    async fn flush_group(
        &mut self,
        drained: CommitGroupBatch<A::Input>,
        progress: &mut RuntimeProgress,
    ) -> IngestorResult<()> {
        let high = drained.high_sequence;
        let low = drained.low_sequence;
        let row_count = drained.records.len();
        progress.rows_planned = progress.rows_planned.saturating_add(row_count as u64);

        let chunks = self.adapter.plan(drained).map_err(|e| match e {
            IngestorError::Adapter(_) => e,
            other => IngestorError::Adapter(other.to_string()),
        })?;

        if !self.options.dry_run {
            if let Some(writer) = &self.writer {
                writer.execute_all(&chunks).await?;
                progress.rows_inserted = progress.rows_inserted.saturating_add(row_count as u64);
            } else if !chunks.is_empty() {
                return Err(IngestorError::Config(
                    "writer is not configured but dry_run is false".into(),
                ));
            }

            self.ack_controller
                .on_commit_group_success(&mut self.consumer, low, high)
                .await?;
            progress.last_acked_sequence = Some(high);
        } else {
            debug!(low, high, row_count, "dry-run: skipping insert and ack",);
        }
        progress.commit_groups_flushed = progress.commit_groups_flushed.saturating_add(1);
        if let Some(last) = progress.last_decoded_sequence
            && last < high
        {
            progress.last_decoded_sequence = Some(high);
        }
        Ok(())
    }
}

/// Force a non-empty group: used by tests that want to assert the
/// flush path without depending on timer behavior.
pub fn force_flush_threshold() -> CommitGroupThresholds {
    CommitGroupThresholds {
        max_rows: 1,
        max_bytes: 1,
        max_age: Duration::from_millis(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_target_logs() {
        let opts = RuntimeOptions::default();
        assert_eq!(opts.manifest_path, "ingest/otel/logs/manifest");
        assert_eq!(opts.configured_envelope.version, 1);
        assert!(opts.dry_run, "default must be dry-run for safety");
    }

    #[test]
    fn force_flush_threshold_trips_immediately() {
        let mut g = CommitGroup::<crate::signal::DecodedLogRecord>::new(force_flush_threshold());
        // Empty group never flushes; that's the "input progress only"
        // safety. Add a zero-row batch with a sequence to advance.
        g.append(vec![], 1);
        // age threshold is 1ms; sleep beyond it
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(g.should_flush());
    }
}

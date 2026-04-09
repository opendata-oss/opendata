//! Background ingest consumer that reads OTLP metrics from an ingest queue.

use std::sync::Arc;

use bytes::Bytes;
use ingest::{CollectedBatch, Collector, Metadata};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::otel::OtelConverter;
use crate::promql::config::IngestConsumerConfig;
use crate::tsdb::Tsdb;

// ── Metadata protocol constants ──

const METADATA_VERSION: u8 = 1;
const SIGNAL_TYPE_METRICS: u8 = 1;
const PAYLOAD_ENCODING_OTLP_PROTO: u8 = 1;
const METADATA_LEN: usize = 4;
const PREFETCH_BATCH_BUFFER: usize = 1;
const ACK_SEQUENCE_BUFFER: usize = 1;
const MAX_ERROR_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug)]
struct IngestMetadata {
    signal_type: u8,
    payload_encoding: u8,
}

impl IngestMetadata {
    fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < METADATA_LEN {
            return Err(format!(
                "metadata too short: expected {} bytes, got {}",
                METADATA_LEN,
                payload.len()
            ));
        }
        if payload[0] != METADATA_VERSION {
            return Err(format!("unknown metadata version: {}", payload[0]));
        }
        Ok(Self {
            signal_type: payload[1],
            payload_encoding: payload[2],
        })
    }
}

// ── IngestConsumer ──

/// Handle returned by [`IngestConsumer::run`] that enables graceful shutdown.
///
/// Dropping this handle does **not** stop the consumer — call [`shutdown`](Self::shutdown)
/// to cancel the poll loop and flush pending acks before the collector is released.
pub(crate) struct ConsumerHandle {
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

impl ConsumerHandle {
    /// Signal the consumer to stop and wait for it to flush pending acks.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        if let Err(e) = self.join.await {
            tracing::error!("Ingest consumer task panicked: {e}");
        }
    }
}

pub(crate) struct IngestConsumer {
    tsdb: Arc<Tsdb>,
    converter: Arc<OtelConverter>,
    config: IngestConsumerConfig,
}

impl IngestConsumer {
    pub fn new(
        tsdb: Arc<Tsdb>,
        converter: Arc<OtelConverter>,
        config: IngestConsumerConfig,
    ) -> Self {
        Self {
            tsdb,
            converter,
            config,
        }
    }

    /// Initialize the collector and spawn the poll loop as a background task.
    ///
    /// Returns a [`ConsumerHandle`] that must be used to shut down the consumer
    /// gracefully, or an error if the collector cannot be created or initialized.
    pub async fn run(self: Arc<Self>) -> Result<ConsumerHandle, ingest::Error> {
        let collector_config = ingest::CollectorConfig {
            object_store: self.config.object_store.clone(),
            manifest_path: self.config.manifest_path.clone(),
            data_path_prefix: self.config.data_path_prefix.clone(),
            gc_interval: self.config.gc_interval,
            gc_grace_period: self.config.gc_grace_period,
        };
        let collector = Collector::new(collector_config, None).await?;
        self.start(collector).await
    }

    /// Like [`run`](Self::run), but uses a pre-built object store for the collector.
    ///
    /// Useful in tests where the ingestor and collector must share an in-memory store.
    #[cfg(test)]
    pub async fn run_with_object_store(
        self: Arc<Self>,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    ) -> Result<ConsumerHandle, ingest::Error> {
        let collector_config = ingest::CollectorConfig {
            object_store: self.config.object_store.clone(),
            manifest_path: self.config.manifest_path.clone(),
            data_path_prefix: self.config.data_path_prefix.clone(),
            gc_interval: self.config.gc_interval,
            gc_grace_period: self.config.gc_grace_period,
        };
        let collector = Collector::with_object_store(collector_config, object_store, None).await?;
        self.start(collector).await
    }

    async fn start(
        self: &Arc<Self>,
        collector: Collector,
    ) -> Result<ConsumerHandle, ingest::Error> {
        tracing::info!(
            manifest_path = %self.config.manifest_path,
            poll_interval = ?self.config.poll_interval,
            "Ingest consumer started"
        );

        let tsdb = self.tsdb.clone();
        let converter = self.converter.clone();
        let poll_interval = self.config.poll_interval;
        let cancel = CancellationToken::new();

        let join = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                poll_loop(tsdb, converter, poll_interval, collector, cancel).await;
            }
        });

        Ok(ConsumerHandle { cancel, join })
    }
}

// TODO: improve error handling — distinguish Fenced (fatal) from transient Storage errors,
// add backoff for repeated failures, and retry acks before giving up.
async fn poll_loop(
    tsdb: Arc<Tsdb>,
    converter: Arc<OtelConverter>,
    poll_interval: std::time::Duration,
    collector: Collector,
    cancel: CancellationToken,
) {
    let (batch_tx, mut batch_rx) = mpsc::channel(PREFETCH_BATCH_BUFFER);
    let (ack_tx, ack_rx) = mpsc::channel(ACK_SEQUENCE_BUFFER);
    let collector_join = tokio::spawn(collector_loop(
        collector,
        poll_interval,
        cancel.clone(),
        batch_tx,
        ack_rx,
    ));

    // Consumer loop: process one prefetched batch at a time and send its
    // sequence back for acknowledgement once processing completes, because the
    // collector stays owned by the producer task and all collector access must
    // remain serialized through that single owner.
    loop {
        let batch = tokio::select! {
            _ = cancel.cancelled() => break,
            maybe_batch = batch_rx.recv() => match maybe_batch {
                Some(batch) => batch,
                None => break,
            }
        };

        let sequence = batch.sequence;
        process_batch(&tsdb, &converter, &batch).await;

        if let Err(e) = ack_tx.send(sequence).await {
            tracing::error!(sequence, "Failed to send ack to collector task: {e}");
            break;
        }

        if cancel.is_cancelled() {
            break;
        }
    }

    tracing::info!("Ingest consumer shutting down, flushing pending acks...");
    drop(ack_tx);
    if let Err(e) = collector_join.await {
        tracing::error!("Collector task panicked: {e}");
    }
}

async fn collector_loop(
    mut collector: Collector,
    poll_interval: std::time::Duration,
    cancel: CancellationToken,
    batch_tx: mpsc::Sender<CollectedBatch>,
    mut ack_rx: mpsc::Receiver<u64>,
) {
    let mut error_backoff = poll_interval;

    // Producer loop:
    // 1. drain any ready ack
    // 2. fetch immediately when the batch channel has room
    // 3. otherwise wait for the next useful event
    loop {
        if cancel.is_cancelled() {
            break;
        }

        match ack_rx.try_recv() {
            Ok(sequence) => {
                ack_sequence(&mut collector, sequence).await;
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        let mut wait_delay = None;

        if batch_tx.capacity() > 0 {
            match collector.next_batch().await {
                Ok(Some(batch)) => {
                    error_backoff = poll_interval;
                    if let Err(e) = batch_tx.send(batch).await {
                        tracing::error!("Failed to send batch to consumer task: {e}");
                        break;
                    }
                    continue;
                }
                Ok(None) => {
                    error_backoff = poll_interval;
                    wait_delay = Some(poll_interval);
                }
                Err(e) => {
                    tracing::error!(retry_in = ?error_backoff, "Failed to read next batch: {e}");
                    wait_delay = Some(error_backoff);
                    error_backoff = error_backoff
                        .checked_mul(2)
                        .unwrap_or(MAX_ERROR_RETRY_BACKOFF)
                        .min(MAX_ERROR_RETRY_BACKOFF);
                }
            }
        }

        match wait_for_work(&cancel, &mut ack_rx, &batch_tx, wait_delay, &mut collector).await {
            WaitOutcome::Wake => {}
            WaitOutcome::Stop => break,
        }
    }

    drain_pending_acks(&mut collector, &mut ack_rx).await;

    if let Err(e) = collector.close().await {
        tracing::error!("Failed to flush collector on shutdown: {e}");
    }
}

enum WaitOutcome {
    Wake,
    Stop,
}

async fn wait_for_work(
    cancel: &CancellationToken,
    ack_rx: &mut mpsc::Receiver<u64>,
    batch_tx: &mpsc::Sender<CollectedBatch>,
    delay: Option<std::time::Duration>,
    collector: &mut Collector,
) -> WaitOutcome {
    let wait_for_capacity = batch_tx.capacity() == 0;
    let mut timer = delay.map(|delay| Box::pin(sleep(delay)));
    let timer_enabled = timer.is_some();

    tokio::select! {
        _ = cancel.cancelled() => WaitOutcome::Stop,
        ack = ack_rx.recv() => match ack {
            Some(sequence) => {
                ack_sequence(collector, sequence).await;
                WaitOutcome::Wake
            }
            None => WaitOutcome::Stop,
        },
        permit = batch_tx.reserve(), if wait_for_capacity => match permit {
            Ok(permit) => {
                drop(permit);
                WaitOutcome::Wake
            }
            Err(_) => WaitOutcome::Stop,
        },
        _ = async {
            if let Some(timer) = timer.as_mut() {
                timer.await;
            }
        }, if timer_enabled => WaitOutcome::Wake,
    }
}

async fn drain_pending_acks(collector: &mut Collector, ack_rx: &mut mpsc::Receiver<u64>) {
    while let Ok(sequence) = ack_rx.try_recv() {
        ack_sequence(collector, sequence).await;
    }
}

async fn ack_sequence(collector: &mut Collector, sequence: u64) {
    if let Err(e) = collector.ack(sequence).await {
        tracing::error!(sequence, "Failed to ack batch: {e}");
    }
}

async fn process_batch(tsdb: &Tsdb, converter: &OtelConverter, batch: &CollectedBatch) {
    for (idx, entry) in batch.entries.iter().enumerate() {
        let metadata = find_metadata_for_entry(&batch.metadata, idx as u32);
        if let Err(reason) = process_entry(tsdb, converter, entry, metadata).await {
            metrics::counter!(crate::tsdb_metrics::TSDB_INGEST_ENTRIES_SKIPPED).increment(1);
            tracing::warn!(
                sequence = batch.sequence,
                entry_index = idx,
                "Skipping entry: {reason}"
            );
        }
    }
}

async fn process_entry(
    tsdb: &Tsdb,
    converter: &OtelConverter,
    entry: &Bytes,
    metadata: Option<&Metadata>,
) -> Result<(), String> {
    let meta = metadata.ok_or("no metadata for entry")?;
    let parsed = IngestMetadata::decode(&meta.payload)?;

    if parsed.signal_type != SIGNAL_TYPE_METRICS {
        return Err(format!("unsupported signal type: {}", parsed.signal_type));
    }
    if parsed.payload_encoding != PAYLOAD_ENCODING_OTLP_PROTO {
        return Err(format!(
            "unsupported payload encoding: {}",
            parsed.payload_encoding
        ));
    }

    let request = ExportMetricsServiceRequest::decode(entry.as_ref())
        .map_err(|e| format!("protobuf decode failed: {e}"))?;

    let series = converter
        .convert(&request)
        .map_err(|e| format!("OtelConverter failed: {e}"))?;

    tsdb.ingest_samples(series, None)
        .await
        .map_err(|e| format!("tsdb ingest failed: {e}"))?;

    Ok(())
}

/// Find the metadata that applies to a given entry index.
///
/// Metadata ranges are sorted by `start_index`. The applicable range for
/// entry `idx` is the last metadata whose `start_index <= idx`.
fn find_metadata_for_entry(metadata: &[Metadata], idx: u32) -> Option<&Metadata> {
    let pos = metadata.partition_point(|m| m.start_index <= idx);
    if pos > 0 {
        Some(&metadata[pos - 1])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_valid_metadata() {
        // given
        let payload = vec![1, 1, 1, 0];

        // when
        let meta = IngestMetadata::decode(&payload).unwrap();

        // then
        assert_eq!(meta.signal_type, SIGNAL_TYPE_METRICS);
        assert_eq!(meta.payload_encoding, PAYLOAD_ENCODING_OTLP_PROTO);
    }

    #[test]
    fn should_reject_unknown_version() {
        // given
        let payload = vec![99, 1, 1, 0];

        // when
        let result = IngestMetadata::decode(&payload);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown metadata version"));
    }

    #[test]
    fn should_reject_short_metadata() {
        // given
        let payload = vec![1, 1];

        // when
        let result = IngestMetadata::decode(&payload);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("metadata too short"));
    }

    #[test]
    fn should_find_metadata_for_entry() {
        // given
        let metadata = vec![
            Metadata {
                start_index: 0,
                ingestion_time_ms: 1000,
                payload: Bytes::from_static(&[1, 1, 1, 0]),
            },
            Metadata {
                start_index: 3,
                ingestion_time_ms: 2000,
                payload: Bytes::from_static(&[1, 2, 1, 0]),
            },
        ];

        // when/then - entry 0 maps to first metadata
        let m = find_metadata_for_entry(&metadata, 0).unwrap();
        assert_eq!(m.start_index, 0);

        // entry 2 still maps to first metadata
        let m = find_metadata_for_entry(&metadata, 2).unwrap();
        assert_eq!(m.start_index, 0);

        // entry 3 maps to second metadata
        let m = find_metadata_for_entry(&metadata, 3).unwrap();
        assert_eq!(m.start_index, 3);

        // entry 5 maps to second metadata
        let m = find_metadata_for_entry(&metadata, 5).unwrap();
        assert_eq!(m.start_index, 3);
    }
}

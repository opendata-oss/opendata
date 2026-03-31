//! Background ingest consumer that reads OTLP metrics from an ingest queue.

use std::sync::Arc;

use bytes::Bytes;
use ingest::{CollectedBatch, Collector, Metadata};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;
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
        };
        let collector = Collector::new(collector_config)?;
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
        };
        let collector = Collector::with_object_store(collector_config, object_store);
        self.start(collector).await
    }

    async fn start(
        self: &Arc<Self>,
        mut collector: Collector,
    ) -> Result<ConsumerHandle, ingest::Error> {
        collector.initialize(None).await?;

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
    mut collector: Collector,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            result = collector.next_batch() => {
                match result {
                    Ok(Some(batch)) => {
                        process_batch(&tsdb, &converter, &batch).await;
                        if let Err(e) = collector.ack(batch.sequence).await {
                            tracing::error!(
                                sequence = batch.sequence,
                                "Failed to ack batch: {e}"
                            );
                        }
                    }
                    Ok(None) => {
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            _ = sleep(poll_interval) => {}
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to read next batch: {e}");
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            _ = sleep(poll_interval) => {}
                        }
                    }
                }
            }
        }
    }

    tracing::info!("Ingest consumer shutting down, flushing pending acks...");
    if let Err(e) = collector.close().await {
        tracing::error!("Failed to flush collector on shutdown: {e}");
    }
}

async fn process_batch(tsdb: &Tsdb, converter: &OtelConverter, batch: &CollectedBatch) {
    for (idx, entry) in batch.entries.iter().enumerate() {
        let metadata = find_metadata_for_entry(&batch.metadata, idx as u32);
        if let Err(reason) = process_entry(tsdb, converter, entry, metadata).await {
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

    tsdb.ingest_samples(series)
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

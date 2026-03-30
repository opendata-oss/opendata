//! Consumes batches from the ingest queue and writes OTLP metrics to the TSDB.
//!
//! This module reads `ExportMetricsServiceRequest` entries from the ingest
//! [`Collector`] and converts them into time series samples via [`OtelConverter`].

use std::sync::Arc;
use std::time::Duration;

use ingest::{Collector as IngestCollector, CollectorConfig};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;

use crate::error::Error;
use crate::otel::{OtelConfig, OtelConverter};
use crate::tsdb::TsdbEngine;

const METADATA_VERSION: u8 = 1;
const SIGNAL_TYPE_METRICS: u8 = 1;
const PAYLOAD_ENCODING_OTLP_PROTO: u8 = 1;

/// Configuration for the ingest consumer.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct IngestConsumerConfig {
    /// Storage configuration for reading ingest data.
    pub storage: common::StorageConfig,
    /// Path to the queue manifest in object storage.
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
    /// How long to sleep when no batches are available.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// OTEL conversion settings.
    #[serde(default)]
    pub otel: OtelServerConfig,
}

fn default_manifest_path() -> String {
    "ingest/otel/metrics/manifest".to_string()
}

fn default_poll_interval_ms() -> u64 {
    1000
}

/// OTEL label settings mirroring the server config.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct OtelServerConfig {
    #[serde(default = "default_true")]
    pub include_resource_attrs: bool,
    #[serde(default = "default_true")]
    pub include_scope_attrs: bool,
}

fn default_true() -> bool {
    true
}

/// Runs the ingest consumer loop, reading batches from the queue and writing
/// to the TSDB. Returns only on error or cancellation.
pub(crate) async fn run(
    config: IngestConsumerConfig,
    tsdb: Arc<TsdbEngine>,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), Error> {
    let collector_config = CollectorConfig {
        storage: config.storage,
        manifest_path: config.manifest_path,
    };

    let collector = IngestCollector::new(collector_config)
        .map_err(|e| Error::Storage(format!("failed to create ingest collector: {e}")))?;

    collector
        .initialize(None)
        .await
        .map_err(|e| Error::Storage(format!("failed to initialize ingest collector: {e}")))?;

    let converter = OtelConverter::new(OtelConfig {
        include_resource_attrs: config.otel.include_resource_attrs,
        include_scope_attrs: config.otel.include_scope_attrs,
    });

    let poll_interval = Duration::from_millis(config.poll_interval_ms);

    tracing::info!("ingest consumer started");

    loop {
        if shutdown.is_cancelled() {
            tracing::info!("ingest consumer shutting down");
            collector
                .close()
                .await
                .map_err(|e| Error::Storage(format!("failed to close ingest collector: {e}")))?;
            return Ok(());
        }

        let batch = collector
            .next_batch()
            .await
            .map_err(|e| Error::Storage(format!("failed to read next batch: {e}")))?;

        let batch = match batch {
            Some(b) => b,
            None => {
                tokio::select! {
                    _ = tokio::time::sleep(poll_interval) => continue,
                    _ = shutdown.cancelled() => continue,
                }
            }
        };

        // Validate metadata if present
        for meta in &batch.metadata {
            if meta.payload.len() >= 4 {
                let version = meta.payload[0];
                let signal_type = meta.payload[1];
                let payload_encoding = meta.payload[2];
                let flags = meta.payload[3];

                if version != METADATA_VERSION {
                    tracing::warn!(
                        version,
                        sequence = batch.sequence,
                        "skipping batch with unsupported metadata version"
                    );
                    collector
                        .ack(batch.sequence)
                        .await
                        .map_err(|e| Error::Storage(format!("failed to ack: {e}")))?;
                    continue;
                }
                if signal_type != SIGNAL_TYPE_METRICS {
                    tracing::warn!(
                        signal_type,
                        sequence = batch.sequence,
                        "skipping batch with non-metrics signal type"
                    );
                    collector
                        .ack(batch.sequence)
                        .await
                        .map_err(|e| Error::Storage(format!("failed to ack: {e}")))?;
                    continue;
                }
                if payload_encoding != PAYLOAD_ENCODING_OTLP_PROTO {
                    tracing::warn!(
                        payload_encoding,
                        sequence = batch.sequence,
                        "skipping batch with unsupported payload encoding"
                    );
                    collector
                        .ack(batch.sequence)
                        .await
                        .map_err(|e| Error::Storage(format!("failed to ack: {e}")))?;
                    continue;
                }
                if flags != 0 {
                    tracing::warn!(
                        flags,
                        sequence = batch.sequence,
                        "skipping batch with unknown flags"
                    );
                    collector
                        .ack(batch.sequence)
                        .await
                        .map_err(|e| Error::Storage(format!("failed to ack: {e}")))?;
                    continue;
                }
            }
        }

        let mut total_series = 0usize;
        let mut total_samples = 0usize;

        for entry in &batch.entries {
            let request = match ExportMetricsServiceRequest::decode(entry.as_ref()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        sequence = batch.sequence,
                        error = %e,
                        "skipping entry with invalid OTLP protobuf"
                    );
                    continue;
                }
            };

            let series = match converter.convert(&request) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        sequence = batch.sequence,
                        error = %e,
                        "skipping entry with conversion error"
                    );
                    continue;
                }
            };

            total_samples += series.iter().map(|s| s.samples.len()).sum::<usize>();
            total_series += series.len();

            tsdb.ingest_samples(series)
                .await
                .map_err(|e| Error::Storage(format!("failed to ingest samples: {e}")))?;
        }

        tracing::debug!(
            sequence = batch.sequence,
            entries = batch.entries.len(),
            series = total_series,
            samples = total_samples,
            "processed ingest batch"
        );

        collector
            .ack(batch.sequence)
            .await
            .map_err(|e| Error::Storage(format!("failed to ack: {e}")))?;
    }
}

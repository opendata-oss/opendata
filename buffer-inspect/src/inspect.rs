//! Manifest inspection logic.
//!
//! Reads a manifest via [`buffer::BufferReader`], then for each entry in the
//! requested range fetches the referenced batch object, decodes per-entry
//! metadata envelopes, and produces a JSON-serializable summary. Signal
//! decoding (OTLP logs, OTLP metrics) is best-effort: unknown envelopes are
//! reported with their raw bytes rather than crashing the tool.

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use buffer::{BufferReader, ManifestEntry, Metadata};
use bytes::Bytes;
use clap::ValueEnum;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;
use serde::Serialize;

/// CLI-supplied options that shape what `inspect_manifest` returns.
pub struct InspectOptions {
    pub from_sequence: Option<u64>,
    pub limit: usize,
    pub decode_signal: SignalDecodeMode,
    pub records_per_entry: usize,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SignalDecodeMode {
    /// Dispatch decoding on each entry's envelope.
    Auto,
    /// Decode every entry as OTLP metrics regardless of envelope.
    Metrics,
    /// Decode every entry as OTLP logs regardless of envelope.
    Logs,
    /// Skip signal decoding; just print envelope and payload sizes.
    Raw,
}

/// Top-level JSON shape emitted by `inspect_manifest`.
#[derive(Serialize)]
pub struct ManifestReport {
    pub manifest_path: String,
    pub epoch: u64,
    pub next_sequence: u64,
    pub total_entries: usize,
    pub returned: usize,
    pub entries: Vec<EntryReport>,
}

#[derive(Serialize)]
pub struct EntryReport {
    pub sequence: u64,
    pub location: String,
    pub batch_record_count: Option<usize>,
    pub envelopes: Vec<EnvelopeReport>,
    pub records: Vec<RecordReport>,
    /// Populated when batch fetch or decode failed; the entry summary still
    /// renders so the operator can see manifest-level information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_error: Option<String>,
}

#[derive(Serialize)]
pub struct EnvelopeReport {
    pub start_index: u32,
    pub ingestion_time_ms: i64,
    /// Decoded envelope when the payload is the 4-byte OpenData envelope
    /// header. `None` for opaque or non-conforming payloads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decoded: Option<DecodedEnvelope>,
    pub raw_bytes_base64: String,
}

#[derive(Serialize, Copy, Clone, Debug)]
pub struct DecodedEnvelope {
    pub version: u8,
    pub signal_type: u8,
    pub signal_name: &'static str,
    pub encoding: u8,
    pub encoding_name: &'static str,
    pub reserved: u8,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
pub enum RecordReport {
    Logs(LogsRecord),
    Metrics(MetricsRecord),
    Raw(RawRecord),
    DecodeError(DecodeErrorRecord),
}

#[derive(Serialize)]
pub struct LogsRecord {
    pub record_index: u32,
    pub timestamp_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: String,
    pub body_summary: String,
    pub service_name: Option<String>,
    pub resource_attributes: BTreeMap<String, String>,
    pub log_attributes: BTreeMap<String, String>,
    pub trace_id_hex: Option<String>,
    pub span_id_hex: Option<String>,
}

#[derive(Serialize)]
pub struct MetricsRecord {
    pub record_index: u32,
    pub resource_metric_count: usize,
    pub scope_metric_count: usize,
    pub metric_count: usize,
    pub data_point_count: usize,
    pub metric_names: Vec<String>,
    pub service_name: Option<String>,
}

#[derive(Serialize)]
pub struct RawRecord {
    pub record_index: u32,
    pub size_bytes: usize,
}

#[derive(Serialize)]
pub struct DecodeErrorRecord {
    pub record_index: u32,
    pub stage: &'static str,
    pub error: String,
}

/// Inspect the manifest and the requested range of entries, returning a
/// fully-decoded [`ManifestReport`] suitable for JSON serialization.
pub async fn inspect_manifest(
    reader: &BufferReader,
    options: &InspectOptions,
) -> Result<ManifestReport> {
    let view = reader
        .read_manifest()
        .await
        .with_context(|| format!("reading manifest at {}", reader.manifest_path()))?;

    let all = view.entries();
    let total_entries = all.len();
    let selected = select_entries(all, options.from_sequence, options.limit);

    let mut entries = Vec::with_capacity(selected.len());
    for entry in selected {
        entries.push(inspect_entry(reader, entry, options).await);
    }

    Ok(ManifestReport {
        manifest_path: reader.manifest_path().to_string(),
        epoch: view.epoch,
        next_sequence: view.next_sequence,
        total_entries,
        returned: entries.len(),
        entries,
    })
}

fn select_entries(
    all: &[ManifestEntry],
    from_sequence: Option<u64>,
    limit: usize,
) -> Vec<&ManifestEntry> {
    let limit = limit.max(1);
    match from_sequence {
        Some(seq) => all
            .iter()
            .filter(|e| e.sequence >= seq)
            .take(limit)
            .collect(),
        None => {
            let start = all.len().saturating_sub(limit);
            all[start..].iter().collect()
        }
    }
}

async fn inspect_entry(
    reader: &BufferReader,
    entry: &ManifestEntry,
    options: &InspectOptions,
) -> EntryReport {
    let envelopes: Vec<EnvelopeReport> = entry.metadata.iter().map(envelope_report).collect();

    let batch_result = reader.read_batch(&entry.location).await;
    let (records, batch_record_count, batch_error) = match batch_result {
        Ok(payloads) => {
            let count = payloads.len();
            let records = decode_records(&payloads, &entry.metadata, options);
            (records, Some(count), None)
        }
        Err(err) => (Vec::new(), None, Some(err.to_string())),
    };

    EntryReport {
        sequence: entry.sequence,
        location: entry.location.clone(),
        batch_record_count,
        envelopes,
        records,
        batch_error,
    }
}

fn envelope_report(metadata: &Metadata) -> EnvelopeReport {
    let raw = metadata.payload.as_ref();
    EnvelopeReport {
        start_index: metadata.start_index,
        ingestion_time_ms: metadata.ingestion_time_ms,
        decoded: decode_envelope(raw).ok(),
        raw_bytes_base64: BASE64.encode(raw),
    }
}

fn decode_envelope(payload: &[u8]) -> Result<DecodedEnvelope> {
    if payload.len() < 4 {
        return Err(anyhow!(
            "envelope payload is {} bytes; expected at least 4",
            payload.len()
        ));
    }
    let version = payload[0];
    let signal = payload[1];
    let encoding = payload[2];
    let reserved = payload[3];
    Ok(DecodedEnvelope {
        version,
        signal_type: signal,
        signal_name: signal_name(signal),
        encoding,
        encoding_name: encoding_name(encoding),
        reserved,
    })
}

fn signal_name(signal_type: u8) -> &'static str {
    match signal_type {
        1 => "metrics",
        2 => "logs",
        _ => "unknown",
    }
}

fn encoding_name(encoding: u8) -> &'static str {
    match encoding {
        1 => "otlp_protobuf",
        _ => "unknown",
    }
}

/// Map a record's flat index into the per-range envelope it belongs to.
fn envelope_for_record(metadata: &[Metadata], record_index: u32) -> Option<&Metadata> {
    metadata
        .iter()
        .rev()
        .find(|m| m.start_index <= record_index)
}

fn decode_records(
    payloads: &[Bytes],
    metadata: &[Metadata],
    options: &InspectOptions,
) -> Vec<RecordReport> {
    let take = payloads.len().min(options.records_per_entry);
    let mut out = Vec::with_capacity(take);
    for (idx, payload) in payloads.iter().take(take).enumerate() {
        let record_index = idx as u32;
        let mode = match options.decode_signal {
            SignalDecodeMode::Auto => match envelope_for_record(metadata, record_index)
                .and_then(|m| decode_envelope(m.payload.as_ref()).ok())
                .map(|env| env.signal_type)
            {
                Some(1) => SignalDecodeMode::Metrics,
                Some(2) => SignalDecodeMode::Logs,
                _ => SignalDecodeMode::Raw,
            },
            other => other,
        };

        let report = match mode {
            SignalDecodeMode::Logs => decode_logs_payload(record_index, payload),
            SignalDecodeMode::Metrics => decode_metrics_payload(record_index, payload),
            SignalDecodeMode::Raw | SignalDecodeMode::Auto => RecordReport::Raw(RawRecord {
                record_index,
                size_bytes: payload.len(),
            }),
        };
        out.push(report);
    }
    out
}

fn decode_logs_payload(record_index: u32, payload: &Bytes) -> RecordReport {
    match ExportLogsServiceRequest::decode(payload.as_ref()) {
        Ok(req) => {
            let mut service_name = None;
            let mut resource_attributes = BTreeMap::new();
            let mut log_attributes = BTreeMap::new();
            let mut timestamp_unix_nano = 0u64;
            let mut severity_number = 0;
            let mut severity_text = String::new();
            let mut body_summary = String::new();
            let mut trace_id_hex = None;
            let mut span_id_hex = None;

            if let Some(resource_logs) = req.resource_logs.first() {
                if let Some(resource) = &resource_logs.resource {
                    for kv in &resource.attributes {
                        if let Some(value) = string_value(&kv.value) {
                            if kv.key == "service.name" {
                                service_name = Some(value.clone());
                            }
                            resource_attributes.insert(kv.key.clone(), value);
                        }
                    }
                }
                if let Some(scope_logs) = resource_logs.scope_logs.first()
                    && let Some(rec) = scope_logs.log_records.first() {
                        timestamp_unix_nano = rec.time_unix_nano;
                        severity_number = rec.severity_number;
                        severity_text = rec.severity_text.clone();
                        body_summary = body_to_string(&rec.body);
                        for kv in &rec.attributes {
                            if let Some(value) = string_value(&kv.value) {
                                log_attributes.insert(kv.key.clone(), value);
                            }
                        }
                        if !rec.trace_id.is_empty() {
                            trace_id_hex = Some(hex_encode(&rec.trace_id));
                        }
                        if !rec.span_id.is_empty() {
                            span_id_hex = Some(hex_encode(&rec.span_id));
                        }
                    }
            }

            RecordReport::Logs(LogsRecord {
                record_index,
                timestamp_unix_nano,
                severity_number,
                severity_text,
                body_summary,
                service_name,
                resource_attributes,
                log_attributes,
                trace_id_hex,
                span_id_hex,
            })
        }
        Err(err) => RecordReport::DecodeError(DecodeErrorRecord {
            record_index,
            stage: "otlp_logs",
            error: err.to_string(),
        }),
    }
}

fn decode_metrics_payload(record_index: u32, payload: &Bytes) -> RecordReport {
    match ExportMetricsServiceRequest::decode(payload.as_ref()) {
        Ok(req) => {
            let mut service_name = None;
            let mut metric_names: Vec<String> = Vec::new();
            let mut scope_metric_count = 0usize;
            let mut metric_count = 0usize;
            let mut data_point_count = 0usize;

            for resource_metrics in &req.resource_metrics {
                if let Some(resource) = &resource_metrics.resource
                    && service_name.is_none() {
                        for kv in &resource.attributes {
                            if kv.key == "service.name" {
                                service_name = string_value(&kv.value);
                                break;
                            }
                        }
                    }
                for scope in &resource_metrics.scope_metrics {
                    scope_metric_count += 1;
                    for metric in &scope.metrics {
                        metric_count += 1;
                        if metric_names.len() < 16 {
                            metric_names.push(metric.name.clone());
                        }
                        data_point_count += metric_data_point_count(metric);
                    }
                }
            }

            RecordReport::Metrics(MetricsRecord {
                record_index,
                resource_metric_count: req.resource_metrics.len(),
                scope_metric_count,
                metric_count,
                data_point_count,
                metric_names,
                service_name,
            })
        }
        Err(err) => RecordReport::DecodeError(DecodeErrorRecord {
            record_index,
            stage: "otlp_metrics",
            error: err.to_string(),
        }),
    }
}

fn string_value(
    value: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>,
) -> Option<String> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match value.as_ref().and_then(|v| v.value.as_ref()) {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        Some(Value::BytesValue(bytes)) => Some(BASE64.encode(bytes)),
        Some(Value::ArrayValue(_)) | Some(Value::KvlistValue(_)) => Some("<complex>".to_string()),
        None => None,
    }
}

fn body_to_string(body: &Option<opentelemetry_proto::tonic::common::v1::AnyValue>) -> String {
    string_value(body).unwrap_or_default()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

fn metric_data_point_count(metric: &opentelemetry_proto::tonic::metrics::v1::Metric) -> usize {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    match &metric.data {
        Some(Data::Gauge(g)) => g.data_points.len(),
        Some(Data::Sum(s)) => s.data_points.len(),
        Some(Data::Histogram(h)) => h.data_points.len(),
        Some(Data::ExponentialHistogram(eh)) => eh.data_points.len(),
        Some(Data::Summary(s)) => s.data_points.len(),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use buffer::Metadata;

    fn metadata(start_index: u32, signal: u8) -> Metadata {
        Metadata {
            start_index,
            ingestion_time_ms: 0,
            payload: Bytes::copy_from_slice(&[1, signal, 1, 0]),
        }
    }

    #[test]
    fn envelope_for_record_picks_largest_start_index_at_or_below() {
        let ranges = vec![metadata(0, 1), metadata(2, 2), metadata(5, 1)];
        assert_eq!(envelope_for_record(&ranges, 0).unwrap().start_index, 0);
        assert_eq!(envelope_for_record(&ranges, 1).unwrap().start_index, 0);
        assert_eq!(envelope_for_record(&ranges, 2).unwrap().start_index, 2);
        assert_eq!(envelope_for_record(&ranges, 3).unwrap().start_index, 2);
        assert_eq!(envelope_for_record(&ranges, 5).unwrap().start_index, 5);
        assert_eq!(envelope_for_record(&ranges, 9).unwrap().start_index, 5);
    }

    #[test]
    fn decode_envelope_rejects_short_payload() {
        assert!(decode_envelope(&[1, 2]).is_err());
    }

    #[test]
    fn decode_envelope_reads_signal_and_encoding() {
        let env = decode_envelope(&[1, 2, 1, 0]).expect("decode");
        assert_eq!(env.version, 1);
        assert_eq!(env.signal_type, 2);
        assert_eq!(env.signal_name, "logs");
        assert_eq!(env.encoding, 1);
        assert_eq!(env.encoding_name, "otlp_protobuf");
        assert_eq!(env.reserved, 0);
    }

    #[test]
    fn decode_logs_payload_summarizes_first_record() {
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use opentelemetry_proto::tonic::resource::v1::Resource;

        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("controller".into())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 0,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("hello".into())),
                        }),
                        attributes: vec![KeyValue {
                            key: "topic".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("foo".into())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0xde, 0xad, 0xbe, 0xef],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut buf = Vec::with_capacity(req.encoded_len());
        req.encode(&mut buf).expect("encode");

        let report = decode_logs_payload(0, &Bytes::from(buf));
        match report {
            RecordReport::Logs(logs) => {
                assert_eq!(logs.severity_text, "INFO");
                assert_eq!(logs.severity_number, 9);
                assert_eq!(logs.body_summary, "hello");
                assert_eq!(logs.service_name.as_deref(), Some("controller"));
                assert_eq!(
                    logs.log_attributes.get("topic").map(String::as_str),
                    Some("foo")
                );
                assert_eq!(logs.trace_id_hex.as_deref(), Some("deadbeef"));
            }
            other => panic!(
                "expected logs record, got {other:?}",
                other = serde_json::to_string(&other).unwrap()
            ),
        }
    }

    #[test]
    fn raw_decode_mode_skips_signal_decoding() {
        let payloads = vec![Bytes::from_static(b"raw-bytes")];
        let metadata = vec![metadata(0, 99)]; // unknown signal
        let opts = InspectOptions {
            from_sequence: None,
            limit: 1,
            decode_signal: SignalDecodeMode::Raw,
            records_per_entry: 1,
        };
        let records = decode_records(&payloads, &metadata, &opts);
        assert_eq!(records.len(), 1);
        match &records[0] {
            RecordReport::Raw(raw) => {
                assert_eq!(raw.record_index, 0);
                assert_eq!(raw.size_bytes, 9);
            }
            _ => panic!("expected raw record"),
        }
    }

    #[test]
    fn auto_decode_falls_back_to_raw_on_unknown_signal() {
        let payloads = vec![Bytes::from_static(b"opaque")];
        let metadata = vec![metadata(0, 7)]; // not metrics, not logs
        let opts = InspectOptions {
            from_sequence: None,
            limit: 1,
            decode_signal: SignalDecodeMode::Auto,
            records_per_entry: 1,
        };
        let records = decode_records(&payloads, &metadata, &opts);
        match &records[0] {
            RecordReport::Raw(_) => {}
            _ => panic!("auto mode should fall through to raw for unknown signals"),
        }
    }

    #[test]
    fn select_entries_takes_last_n_when_no_from_sequence() {
        let entries: Vec<ManifestEntry> = (0..5u64)
            .map(|seq| ManifestEntry {
                sequence: seq,
                location: format!("loc-{seq}"),
                metadata: vec![],
            })
            .collect();
        let picked = select_entries(&entries, None, 2);
        let seqs: Vec<u64> = picked.iter().map(|e| e.sequence).collect();
        assert_eq!(seqs, vec![3, 4]);
    }

    #[test]
    fn select_entries_starts_at_from_sequence() {
        let entries: Vec<ManifestEntry> = (0..5u64)
            .map(|seq| ManifestEntry {
                sequence: seq,
                location: format!("loc-{seq}"),
                metadata: vec![],
            })
            .collect();
        let picked = select_entries(&entries, Some(2), 2);
        let seqs: Vec<u64> = picked.iter().map(|e| e.sequence).collect();
        assert_eq!(seqs, vec![2, 3]);
    }

    #[tokio::test]
    async fn inspect_manifest_summarizes_real_otlp_logs_payload() {
        use std::sync::Arc;
        use std::time::Duration;

        use buffer::{BufferReader, CompressionType, Producer, ProducerConfig};
        use common::ObjectStoreConfig;
        use common::clock::SystemClock;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use slatedb::object_store::ObjectStore;
        use slatedb::object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_path = "ingest/test/inspect-logs-manifest";

        let producer_config = ProducerConfig {
            object_store: ObjectStoreConfig::InMemory,
            data_path_prefix: "ingest/test/inspect-logs-data".to_string(),
            manifest_path: manifest_path.to_string(),
            flush_interval: Duration::from_secs(24 * 60 * 60),
            flush_size_bytes: 64 * 1024 * 1024,
            max_buffered_inputs: 1000,
            batch_compression: CompressionType::None,
        };
        let producer =
            Producer::with_object_store(producer_config, Arc::clone(&store), Arc::new(SystemClock))
                .expect("producer");

        // Build a real OTLP logs export request and feed it through the
        // producer with a logs envelope, exactly like the opendata-go logs
        // exporter would.
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("controller".into())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 0,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("reconciled topic foo".into())),
                        }),
                        attributes: vec![KeyValue {
                            key: "topic".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("foo".into())),
                            }),
                        }],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0xde, 0xad, 0xbe, 0xef],
                        span_id: vec![],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut payload = Vec::with_capacity(req.encoded_len());
        req.encode(&mut payload).expect("encode");

        let envelope = Bytes::copy_from_slice(&[1, 2, 1, 0]); // logs, OTLP protobuf
        producer
            .produce(vec![Bytes::from(payload)], envelope)
            .await
            .expect("produce");
        producer.flush().await.expect("flush");

        let reader = BufferReader::new(Arc::clone(&store), manifest_path.to_string());
        let options = InspectOptions {
            from_sequence: None,
            limit: 5,
            decode_signal: SignalDecodeMode::Auto,
            records_per_entry: 5,
        };
        let report = inspect_manifest(&reader, &options).await.expect("inspect");

        assert_eq!(report.manifest_path, manifest_path);
        assert_eq!(report.total_entries, 1);
        assert_eq!(report.entries.len(), 1);
        let entry = &report.entries[0];
        assert_eq!(entry.batch_record_count, Some(1));
        assert_eq!(entry.envelopes.len(), 1);
        let env = entry.envelopes[0].decoded.expect("envelope should decode");
        assert_eq!(env.signal_name, "logs");
        assert_eq!(env.encoding_name, "otlp_protobuf");

        assert_eq!(entry.records.len(), 1);
        match &entry.records[0] {
            RecordReport::Logs(logs) => {
                assert_eq!(logs.severity_text, "INFO");
                assert_eq!(logs.body_summary, "reconciled topic foo");
                assert_eq!(logs.service_name.as_deref(), Some("controller"));
                assert_eq!(
                    logs.log_attributes.get("topic").map(String::as_str),
                    Some("foo")
                );
                assert_eq!(logs.trace_id_hex.as_deref(), Some("deadbeef"));
            }
            other => panic!(
                "expected Logs record, got {}",
                serde_json::to_string(other).unwrap()
            ),
        }

        // Crucially: the inspector must not have advanced the manifest
        // epoch. Re-reading via BufferReader should show the same epoch.
        let after = reader.read_manifest().await.expect("re-read");
        assert_eq!(after.epoch, 0, "BufferReader must not advance the epoch");

        producer.close().await.expect("close producer");
    }
}

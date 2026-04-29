//! Signal-specific payload decoding.
//!
//! Converts a [`RawBufferBatch`] into typed records carrying Buffer source
//! coordinates. The trait is the seam between the generic ingestor and the
//! data shape; the alpha implementation is [`OtlpLogsDecoder`].
//!
//! `record_index` is a *flat* index assigned by the decoder so that
//! `(sequence, entry_index, record_index)` uniquely identifies a logical
//! row even when a single OTLP payload contains a tree of resource ->
//! scope -> log records.

use std::collections::BTreeMap;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use prost::Message;

use crate::envelope::MetadataEnvelope;
use crate::error::{IngestorError, IngestorResult};
use crate::source::{RawBufferBatch, RawEntry};

/// Buffer source coordinates carried forward to the adapter.
#[derive(Debug, Clone)]
pub struct SourceCoordinates {
    pub sequence: u64,
    pub entry_index: u32,
    pub record_index: u32,
    pub manifest_path: String,
    pub data_path: String,
    pub ingestion_time_ms: i64,
}

pub trait SignalDecoder {
    type Output;

    /// Decode a batch of entries. Envelopes are passed in lockstep with
    /// `batch.entries` so the decoder can dispatch on per-entry envelopes
    /// in future implementations; the alpha relies on
    /// [`crate::envelope::validate_consistent`] having already enforced
    /// uniformity within a batch.
    fn decode(
        &self,
        batch: &RawBufferBatch,
        envelopes: &[MetadataEnvelope],
    ) -> IngestorResult<Self::Output>;
}

/// One flattened OTLP log record with its Buffer source coordinates and
/// resource/scope/log attributes pre-merged into a single map.
#[derive(Debug, Clone)]
pub struct DecodedLogRecord {
    pub source: SourceCoordinates,
    pub timestamp_unix_nano: u64,
    pub observed_timestamp_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: String,
    pub body: String,
    pub service_name: Option<String>,
    pub resource_attributes: BTreeMap<String, String>,
    pub scope_name: Option<String>,
    pub log_attributes: BTreeMap<String, String>,
    pub trace_id_hex: String,
    pub span_id_hex: String,
}

/// Convenience alias retained for documentation; the decoder itself
/// returns `Vec<DecodedLogRecord>` so the runtime's
/// `D::Output = Vec<A::Input>` bound is satisfied without an extra
/// wrapper type.
pub type DecodedLogs = Vec<DecodedLogRecord>;

#[derive(Debug, Default)]
pub struct OtlpLogsDecoder;

impl OtlpLogsDecoder {
    pub fn new() -> Self {
        Self
    }
}

impl SignalDecoder for OtlpLogsDecoder {
    type Output = Vec<DecodedLogRecord>;

    fn decode(
        &self,
        batch: &RawBufferBatch,
        _envelopes: &[MetadataEnvelope],
    ) -> IngestorResult<Self::Output> {
        let mut records = Vec::new();
        for entry in &batch.entries {
            decode_entry(batch, entry, &mut records)?;
        }
        Ok(records)
    }
}

fn decode_entry(
    batch: &RawBufferBatch,
    entry: &RawEntry,
    records: &mut Vec<DecodedLogRecord>,
) -> IngestorResult<()> {
    let req = ExportLogsServiceRequest::decode(entry.raw_bytes.as_ref()).map_err(|e| {
        IngestorError::SignalDecode(format!(
            "OTLP logs decode failed for sequence={} entry_index={}: {}",
            batch.sequence, entry.entry_index, e
        ))
    })?;

    let mut record_index: u32 = 0;
    for resource_logs in &req.resource_logs {
        let mut resource_attributes = BTreeMap::new();
        let mut service_name = None;
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

        for scope_logs in &resource_logs.scope_logs {
            let scope_name = scope_logs.scope.as_ref().map(|s| s.name.clone());
            for log in &scope_logs.log_records {
                let mut log_attributes = BTreeMap::new();
                merge_attributes(&log.attributes, &mut log_attributes);
                if let Some(scope) = &scope_logs.scope {
                    merge_attributes(&scope.attributes, &mut log_attributes);
                }

                records.push(DecodedLogRecord {
                    source: SourceCoordinates {
                        sequence: batch.sequence,
                        entry_index: entry.entry_index,
                        record_index,
                        manifest_path: batch.manifest_path.clone(),
                        data_path: batch.data_object_path.clone(),
                        ingestion_time_ms: entry.ingestion_time_ms,
                    },
                    timestamp_unix_nano: log.time_unix_nano,
                    observed_timestamp_unix_nano: log.observed_time_unix_nano,
                    severity_number: log.severity_number,
                    severity_text: log.severity_text.clone(),
                    body: any_value_to_string(&log.body),
                    service_name: service_name.clone(),
                    resource_attributes: resource_attributes.clone(),
                    scope_name: scope_name.clone(),
                    log_attributes,
                    trace_id_hex: hex::encode(&log.trace_id),
                    span_id_hex: hex::encode(&log.span_id),
                });
                record_index += 1;
            }
        }
    }
    Ok(())
}

fn merge_attributes(src: &[KeyValue], dst: &mut BTreeMap<String, String>) {
    for kv in src {
        if let Some(value) = string_value(&kv.value) {
            dst.insert(kv.key.clone(), value);
        }
    }
}

fn string_value(value: &Option<AnyValue>) -> Option<String> {
    match value.as_ref().and_then(|v| v.value.as_ref()) {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        Some(Value::BytesValue(bytes)) => Some(hex::encode(bytes)),
        Some(Value::ArrayValue(_)) | Some(Value::KvlistValue(_)) => Some("<complex>".into()),
        None => None,
    }
}

fn any_value_to_string(value: &Option<AnyValue>) -> String {
    string_value(value).unwrap_or_default()
}

#[allow(dead_code)]
fn log_record_summary(rec: &LogRecord) -> String {
    // Helper retained for future tooling; not load-bearing.
    format!("ts={} sev={}", rec.time_unix_nano, rec.severity_text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{MetadataEnvelope, PayloadEncoding, SignalType};
    use crate::source::{RawBufferBatch, RawEntry};
    use bytes::Bytes;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn make_request(records_per_resource: &[usize]) -> ExportLogsServiceRequest {
        let resource_logs = records_per_resource
            .iter()
            .enumerate()
            .map(|(r_idx, &count)| ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue(format!("svc-{r_idx}"))),
                        }),
                    }],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: (0..count)
                        .map(|i| LogRecord {
                            time_unix_nano: 1_000 + i as u64,
                            observed_time_unix_nano: 0,
                            severity_number: 9,
                            severity_text: "INFO".into(),
                            body: Some(AnyValue {
                                value: Some(Value::StringValue(format!("body-{r_idx}-{i}"))),
                            }),
                            attributes: vec![KeyValue {
                                key: "rec".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue(format!("{r_idx}/{i}"))),
                                }),
                            }],
                            dropped_attributes_count: 0,
                            flags: 0,
                            trace_id: vec![0xaa; 16],
                            span_id: vec![0xbb; 8],
                            event_name: String::new(),
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect();
        ExportLogsServiceRequest { resource_logs }
    }

    fn batch_with_payloads(payloads: Vec<Vec<u8>>) -> RawBufferBatch {
        RawBufferBatch {
            sequence: 42,
            manifest_path: "ingest/test/manifest".into(),
            data_object_path: "ingest/test/data/abc.batch".into(),
            entries: payloads
                .into_iter()
                .enumerate()
                .map(|(i, payload)| RawEntry {
                    entry_index: i as u32,
                    raw_bytes: Bytes::from(payload),
                    raw_metadata: Bytes::from_static(&[1, 2, 1, 0]),
                    ingestion_time_ms: 99,
                })
                .collect(),
        }
    }

    fn logs_envelope_for(batch: &RawBufferBatch) -> Vec<MetadataEnvelope> {
        batch
            .entries
            .iter()
            .map(|_| MetadataEnvelope {
                version: 1,
                signal_type: SignalType::Logs,
                encoding: PayloadEncoding::OtlpProtobuf,
                reserved: 0,
            })
            .collect()
    }

    #[test]
    fn flattens_resource_scope_log_tree_and_assigns_record_index() {
        // Two resource_logs blocks: 2 records and 3 records.
        let req = make_request(&[2, 3]);
        let payload = req.encode_to_vec();
        let batch = batch_with_payloads(vec![payload]);
        let envelopes = logs_envelope_for(&batch);

        let decoder = OtlpLogsDecoder::new();
        let decoded = decoder.decode(&batch, &envelopes).expect("decode");
        assert_eq!(decoded.len(), 5);
        let indices: Vec<u32> = decoded.iter().map(|r| r.source.record_index).collect();
        assert_eq!(indices, vec![0, 1, 2, 3, 4]);
        // Resource attributes flow through.
        assert_eq!(decoded[0].service_name.as_deref(), Some("svc-0"));
        assert_eq!(decoded[2].service_name.as_deref(), Some("svc-1"));
        // Source coordinates use the per-batch sequence and per-entry index.
        for rec in &decoded {
            assert_eq!(rec.source.sequence, 42);
            assert_eq!(rec.source.entry_index, 0);
            assert_eq!(rec.source.manifest_path, "ingest/test/manifest");
            assert_eq!(rec.source.ingestion_time_ms, 99);
        }
        assert_eq!(decoded[0].body, "body-0-0");
    }

    #[test]
    fn record_index_resets_per_entry() {
        // Two entries, each with one record. record_index should be 0 within
        // each entry — the (sequence, entry_index, record_index) triple is
        // what's globally unique, not record_index alone.
        let req = make_request(&[1]);
        let payload = req.encode_to_vec();
        let batch = batch_with_payloads(vec![payload.clone(), payload]);
        let envelopes = logs_envelope_for(&batch);

        let decoded = OtlpLogsDecoder::new()
            .decode(&batch, &envelopes)
            .expect("decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].source.entry_index, 0);
        assert_eq!(decoded[1].source.entry_index, 1);
        assert_eq!(decoded[0].source.record_index, 0);
        assert_eq!(decoded[1].source.record_index, 0);
    }

    #[test]
    fn empty_payload_yields_zero_rows_but_no_error() {
        let req = ExportLogsServiceRequest {
            resource_logs: vec![],
        };
        let payload = req.encode_to_vec();
        let batch = batch_with_payloads(vec![payload]);
        let envelopes = logs_envelope_for(&batch);

        let decoded = OtlpLogsDecoder::new()
            .decode(&batch, &envelopes)
            .expect("decode");
        assert!(decoded.is_empty(), "expected no decoded records");
    }

    #[test]
    fn malformed_payload_returns_signal_decode_error() {
        let batch = batch_with_payloads(vec![b"not-a-protobuf-message".to_vec()]);
        let envelopes = logs_envelope_for(&batch);
        let err = OtlpLogsDecoder::new()
            .decode(&batch, &envelopes)
            .unwrap_err();
        match err {
            IngestorError::SignalDecode(_) => {}
            other => panic!("expected SignalDecode, got {other:?}"),
        }
    }

    #[test]
    fn trace_and_span_ids_hex_encoded() {
        let req = make_request(&[1]);
        let payload = req.encode_to_vec();
        let batch = batch_with_payloads(vec![payload]);
        let envelopes = logs_envelope_for(&batch);
        let decoded = OtlpLogsDecoder::new()
            .decode(&batch, &envelopes)
            .expect("decode");
        assert_eq!(decoded[0].trace_id_hex, "a".repeat(32));
        assert_eq!(decoded[0].span_id_hex, "b".repeat(16));
    }
}

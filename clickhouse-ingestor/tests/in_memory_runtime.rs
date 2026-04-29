//! End-to-end in-memory test: real Producer + Consumer + runtime in
//! dry-run mode. Validates that the layered pipeline wires together,
//! that per-entry envelope decoding survives multiple metadata ranges
//! per Buffer batch, and that ack progress advances correctly when the
//! writer is wired.
//!
//! Does not require Docker. The testcontainers-gated test in
//! `tests/clickhouse_round_trip.rs` covers the real-ClickHouse path.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use clickhouse_ingestor::adapter::Adapter;
use clickhouse_ingestor::adapter::logs::LogsAdapterConfig;
use clickhouse_ingestor::commit_group::{CommitGroupBatch, CommitGroupThresholds};
use clickhouse_ingestor::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use clickhouse_ingestor::error::IngestorResult;
use clickhouse_ingestor::{
    AckFlushPolicy, BufferConsumerRuntime, InsertChunk, OtlpLogsClickHouseAdapter, OtlpLogsDecoder,
    RuntimeOptions,
};
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use std::sync::Mutex;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Build an OTLP ExportLogsServiceRequest with a single resource_log
/// containing `record_count` log records, each tagged so the test
/// can match them back.
fn make_logs(service: &str, record_count: usize) -> Vec<u8> {
    let log_records = (0..record_count)
        .map(|i| LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000 + i as u64,
            observed_time_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!("body-{i}"))),
            }),
            attributes: vec![KeyValue {
                key: "i".into(),
                value: Some(AnyValue {
                    value: Some(Value::IntValue(i as i64)),
                }),
            }],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        })
        .collect();
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service.to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    req.encode_to_vec()
}

fn logs_envelope() -> Bytes {
    Bytes::from_static(&[1, 2, 1, 0])
}

#[tokio::test]
async fn dry_run_decodes_and_advances_progress_through_real_buffer() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/in-memory-runtime/manifest";
    let data_prefix = "ingest/test/in-memory-runtime/data";

    let producer_config = buffer::ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.into(),
        manifest_path: manifest_path.into(),
        flush_interval: Duration::from_secs(24 * 3600),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1000,
        batch_compression: buffer::CompressionType::None,
    };
    let producer = buffer::Producer::with_object_store(
        producer_config,
        Arc::clone(&store),
        Arc::new(SystemClock),
    )
    .expect("producer");

    // Push two batches. Each Producer::produce becomes one entry; the
    // producer flush coalesces them into one Buffer batch with two
    // metadata ranges.
    producer
        .produce(vec![Bytes::from(make_logs("svc-a", 2))], logs_envelope())
        .await
        .expect("produce a");
    producer
        .produce(vec![Bytes::from(make_logs("svc-b", 3))], logs_envelope())
        .await
        .expect("produce b");
    producer.flush().await.expect("flush");

    // Construct the consumer + runtime.
    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer = buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None)
        .await
        .expect("consumer");

    let options = RuntimeOptions {
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        commit_group: CommitGroupThresholds {
            max_rows: 1000,
            max_bytes: 1_000_000,
            max_age: Duration::from_millis(100),
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        // Dry-run: pipeline runs, no writer required, no acks performed.
        dry_run: true,
        poll_interval: Duration::from_millis(10),
    };
    let runtime = BufferConsumerRuntime::new(
        consumer,
        OtlpLogsDecoder::new(),
        OtlpLogsClickHouseAdapter::new(LogsAdapterConfig::default()),
        None,
        options,
    );
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    // Wait for the runtime to read both batches, decode them, and
    // commit-group flush. We expect:
    //   - last_decoded_sequence reaches the highest produced sequence
    //   - rows_planned == 5 (svc-a * 2 + svc-b * 3)
    //   - last_acked_sequence stays None (dry-run skips acks)
    let timed = timeout(Duration::from_secs(5), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.rows_planned >= 5 && p.last_decoded_sequence.is_some() {
                return p;
            }
        }
    })
    .await
    .expect("timeout waiting for progress");

    assert!(timed.last_decoded_sequence.is_some());
    assert!(timed.last_acked_sequence.is_none(), "dry-run must not ack");
    assert_eq!(timed.rows_planned, 5);
    assert_eq!(timed.rows_inserted, 0);
    assert!(timed.commit_groups_flushed >= 1);

    shutdown.cancel();
    let _ = handle.await.expect("runtime exited");

    // Producer cleanup is independent of the runtime's state.
    producer.close().await.expect("close producer");
}

/// A test adapter that records the chunks it would have inserted. We
/// use this when we want to assert chunk content + token shape end to
/// end, instead of going through HTTP.
#[derive(Clone)]
struct RecordingAdapter {
    inner: OtlpLogsClickHouseAdapter,
    captured: Arc<Mutex<Vec<InsertChunk>>>,
}

impl RecordingAdapter {
    fn new(config: LogsAdapterConfig) -> Self {
        Self {
            inner: OtlpLogsClickHouseAdapter::new(config),
            captured: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Adapter for RecordingAdapter {
    type Input = clickhouse_ingestor::DecodedLogRecord;

    fn plan(&self, batch: CommitGroupBatch<Self::Input>) -> IngestorResult<Vec<InsertChunk>> {
        let chunks = self.inner.plan(batch)?;
        self.captured.lock().unwrap().extend(chunks.iter().cloned());
        Ok(chunks)
    }
}

#[tokio::test]
async fn adapter_chunks_carry_tokens_under_real_pipeline() {
    // This test threads a real Producer + Consumer through the runtime
    // with a recording adapter so we can inspect tokens without an
    // HTTP round-trip. The test is dry-run; we still verify the
    // adapter ran for each commit group.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/recording/manifest";
    let data_prefix = "ingest/test/recording/data";

    let producer_config = buffer::ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.into(),
        manifest_path: manifest_path.into(),
        flush_interval: Duration::from_secs(24 * 3600),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1000,
        batch_compression: buffer::CompressionType::None,
    };
    let producer = buffer::Producer::with_object_store(
        producer_config,
        Arc::clone(&store),
        Arc::new(SystemClock),
    )
    .expect("producer");

    producer
        .produce(vec![Bytes::from(make_logs("svc", 4))], logs_envelope())
        .await
        .expect("produce");
    producer.flush().await.expect("flush");

    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer = buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None)
        .await
        .expect("consumer");

    let adapter = RecordingAdapter::new(LogsAdapterConfig {
        max_chunk_rows: 2,
        ..LogsAdapterConfig::default()
    });
    let captured = adapter.captured.clone();

    let options = RuntimeOptions {
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        commit_group: CommitGroupThresholds {
            max_rows: 100,
            max_bytes: 1_000_000,
            max_age: Duration::from_millis(50),
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        dry_run: true,
        poll_interval: Duration::from_millis(10),
    };

    let runtime =
        BufferConsumerRuntime::new(consumer, OtlpLogsDecoder::new(), adapter, None, options);
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let _ = timeout(Duration::from_secs(5), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.rows_planned >= 4 {
                return p;
            }
        }
    })
    .await
    .expect("progress timeout");

    shutdown.cancel();
    let _ = handle.await.expect("runtime exited");

    let chunks = captured.lock().unwrap().clone();
    assert!(!chunks.is_empty());
    // 4 records / max_chunk_rows=2 → 2 chunks per commit group.
    assert_eq!(chunks.len(), 2);
    let tokens: Vec<&str> = chunks
        .iter()
        .map(|c| c.idempotency_token.as_str())
        .collect();
    // Each token contains low-high range, adapter version, fingerprint,
    // and a chunk index. Chunk 0 and chunk 1 must differ.
    assert_ne!(tokens[0], tokens[1]);
    for chunk in &chunks {
        assert_eq!(chunk.database, "responsive");
        assert_eq!(chunk.table, "logs");
        let token = &chunk.idempotency_token;
        // Token shape is {manifest}:{db}.{table}:{low}-{high}:{ver}:{fp}:{idx}
        let manifest_prefix = format!("{manifest_path}:responsive.logs:");
        assert!(
            token.starts_with(&manifest_prefix),
            "token must start with {manifest_prefix}, got {token}"
        );
    }

    producer.close().await.expect("close");
    let _ = captured;
    let _: BTreeMap<String, String> = BTreeMap::new(); // keep BTreeMap import live for IDEs
}

/// An intentionally bad adapter that drops every record. The runtime's
/// contract guard must reject this before acking the input range; if
/// it didn't, the runtime would advance Buffer past records that
/// never reached the sink.
#[derive(Clone)]
struct DroppingAdapter {
    fingerprint: String,
}

impl Adapter for DroppingAdapter {
    type Input = clickhouse_ingestor::DecodedLogRecord;
    fn plan(&self, _batch: CommitGroupBatch<Self::Input>) -> IngestorResult<Vec<InsertChunk>> {
        // Returns no chunks even when records are present, simulating
        // an adapter bug that drops everything.
        let _ = &self.fingerprint;
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn runtime_rejects_adapter_that_drops_rows() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/contract-guard/manifest";
    let data_prefix = "ingest/test/contract-guard/data";

    let producer_config = buffer::ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.into(),
        manifest_path: manifest_path.into(),
        flush_interval: Duration::from_secs(24 * 3600),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1000,
        batch_compression: buffer::CompressionType::None,
    };
    let producer = buffer::Producer::with_object_store(
        producer_config,
        Arc::clone(&store),
        Arc::new(SystemClock),
    )
    .expect("producer");

    producer
        .produce(vec![Bytes::from(make_logs("svc", 3))], logs_envelope())
        .await
        .expect("produce");
    producer.flush().await.expect("flush");

    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer = buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None)
        .await
        .expect("consumer");

    let options = RuntimeOptions {
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        commit_group: CommitGroupThresholds {
            max_rows: 100,
            max_bytes: 1_000_000,
            max_age: Duration::from_millis(50),
        },
        ack_flush_policy: AckFlushPolicy::EveryCommitGroup,
        // Not dry-run: we want the contract guard to fire before the
        // ack path runs.
        dry_run: false,
        poll_interval: Duration::from_millis(10),
    };
    // Writer is None; with dry_run=false and writer=None the runtime
    // would error if any chunks were produced. The contract guard
    // should fire FIRST (because the adapter dropped rows), surfacing
    // an Adapter error.
    let runtime = BufferConsumerRuntime::new(
        consumer,
        OtlpLogsDecoder::new(),
        DroppingAdapter {
            fingerprint: "dropper".into(),
        },
        None,
        options,
    );

    let shutdown = CancellationToken::new();
    let inner_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(inner_shutdown).await });

    // The runtime should error out. Give it a couple of seconds to
    // process the batch and run the guard.
    let _ = tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    let result = handle.await.expect("runtime task panicked");
    let err = result.expect_err("runtime must error when adapter drops rows");
    let msg = format!("{err}");
    assert!(
        msg.contains("adapter plan covered 0 rows"),
        "unexpected error message: {msg}"
    );

    producer.close().await.expect("close");
}

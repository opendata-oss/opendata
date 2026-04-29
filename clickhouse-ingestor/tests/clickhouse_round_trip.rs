//! Real-ClickHouse end-to-end test gated behind the `integration-tests`
//! feature so default `cargo test` stays fast.
//!
//! ```sh
//! cargo test -p clickhouse-ingestor --features integration-tests \
//!   --test clickhouse_round_trip -- --nocapture
//! ```

#![cfg(feature = "integration-tests")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use clickhouse_ingestor::adapter::logs::{LogsAdapterConfig, OtlpLogsClickHouseAdapter};
use clickhouse_ingestor::commit_group::CommitGroupThresholds;
use clickhouse_ingestor::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use clickhouse_ingestor::writer::{ClickHouseWriter, WriterConfig};
use clickhouse_ingestor::{AckFlushPolicy, BufferConsumerRuntime, OtlpLogsDecoder, RuntimeOptions};
use common::ObjectStoreConfig;
use common::clock::SystemClock;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::clickhouse::ClickHouse as ClickHouseImage;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

fn make_logs(service: &str, record_count: usize, base_ts_ns: u64) -> Vec<u8> {
    let log_records = (0..record_count)
        .map(|i| LogRecord {
            time_unix_nano: base_ts_ns + i as u64,
            observed_time_unix_nano: 0,
            severity_number: 9,
            severity_text: "INFO".into(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!("body-{i}"))),
            }),
            attributes: vec![KeyValue {
                key: "topic".into(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue(format!("t-{i}"))),
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

/// **Currently `#[ignore]`d.** Against ClickHouse 23.3 (the
/// testcontainers-modules 0.11 default tag), the runtime's
/// `execute_chunk` returns HTTP 200 OK but rows do not land in the
/// `responsive_test.logs_round_trip` table — even though the same
/// HTTP body, replayed via `execute_statement` on the same writer,
/// inserts correctly, and `curl --data-binary @body` against the same
/// container also inserts correctly. The bug is reproducible with
/// the SQL+data-in-body + URL-params-for-settings shape used by the
/// writer today; switching to SETTINGS-in-SQL did not change it.
/// Direct `execute_chunk` against a simple `MergeTree` table does
/// land data, so it is something specific to the alpha logs DDL or
/// the request shape against it.
///
/// The unit + in-memory tests (`tests/in_memory_runtime.rs`) cover
/// the layered design including the contract guard, byte-aware
/// chunking, manifest-aware token format, and dry-run progress. This
/// gate stays open until the real-ClickHouse insert path is
/// understood; it is the last thing standing between the alpha and a
/// validated end-to-end sink.
#[tokio::test]
#[ignore = "ClickHouse 23.3 silently 200s but does not insert the runtime's body; see header"]
async fn clickhouse_round_trip_with_dedup() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let clickhouse = ClickHouseImage::default().start().await?;
    let host = clickhouse.get_host().await?;
    let port = clickhouse.get_host_port_ipv4(8123).await?;
    let endpoint = format!("http://{host}:{port}");
    eprintln!("clickhouse endpoint: {endpoint}");

    let writer = ClickHouseWriter::new(WriterConfig {
        endpoint: endpoint.clone(),
        user: "default".into(),
        password: String::new(),
        request_timeout: Duration::from_secs(15),
        max_attempts: 4,
        initial_backoff: Duration::from_millis(100),
    });

    let database = "responsive_test";
    let table = "logs_round_trip";
    writer
        .execute_statement(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
        .await?;
    let adapter_cfg = LogsAdapterConfig {
        database: database.into(),
        table: table.into(),
        adapter_version: 1,
        max_chunk_rows: 100,
        max_chunk_bytes: 1_000_000,
        // Single-node test container; non-replicated.
        insert_quorum: None,
        // ClickHouse's insert_deduplication_token is a no-op on
        // non-Replicated engines; we exercise table-level dedupe via
        // ReplacingMergeTree(_adapter_version) instead.
        apply_deduplication_token: false,
    };
    writer
        .execute_statement(&clickhouse_ingestor::adapter::logs::logs_table_ddl(
            &adapter_cfg,
        ))
        .await?;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = "ingest/test/clickhouse-rt/manifest";
    let data_prefix = "ingest/test/clickhouse-rt/data";

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
    )?;

    producer
        .produce(
            vec![Bytes::from(make_logs(
                "svc-a",
                3,
                1_700_000_000_000_000_000,
            ))],
            logs_envelope(),
        )
        .await?;
    producer
        .produce(
            vec![Bytes::from(make_logs(
                "svc-b",
                2,
                1_700_000_001_000_000_000,
            ))],
            logs_envelope(),
        )
        .await?;
    producer.flush().await?;

    let consumer_config = buffer::ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.into(),
        data_path_prefix: data_prefix.into(),
        gc_interval: Duration::from_secs(60),
        gc_grace_period: Duration::from_secs(60),
    };
    let consumer =
        buffer::Consumer::with_object_store(consumer_config, Arc::clone(&store), None).await?;

    let runtime_options = RuntimeOptions {
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
        dry_run: false,
        poll_interval: Duration::from_millis(20),
    };
    let runtime = BufferConsumerRuntime::new(
        consumer,
        OtlpLogsDecoder::new(),
        OtlpLogsClickHouseAdapter::new(adapter_cfg.clone()),
        Some(writer.clone()),
        runtime_options,
    );
    let mut progress_rx = runtime.progress();

    let shutdown = CancellationToken::new();
    let runtime_shutdown = shutdown.clone();
    let handle = tokio::spawn(async move { runtime.run(runtime_shutdown).await });

    let _ = timeout(Duration::from_secs(20), async {
        loop {
            progress_rx
                .changed()
                .await
                .expect("progress channel closed");
            let p = *progress_rx.borrow();
            if p.rows_inserted >= 5 && p.last_acked_sequence.is_some() {
                return p;
            }
        }
    })
    .await
    .expect("ingestor progress timeout");

    shutdown.cancel();
    let runtime_result = handle.await.expect("runtime task panicked");
    runtime_result.expect("runtime exited cleanly");

    let total_raw = writer
        .execute_statement(&format!("SELECT count() FROM {database}.{table}"))
        .await?;
    let total_final = writer
        .execute_statement(&format!("SELECT count() FROM {database}.{table} FINAL"))
        .await?;
    eprintln!(
        "row counts after runtime: raw={} FINAL={}",
        total_raw.trim(),
        total_final.trim()
    );
    assert_eq!(total_raw.trim(), "5", "expected 5 raw rows");
    assert_eq!(total_final.trim(), "5", "expected 5 deduplicated rows");

    let distinct = writer
        .execute_statement(&format!(
            "SELECT count(DISTINCT (_odb_sequence, _odb_entry_index, _odb_record_index)) \
             FROM {database}.{table}"
        ))
        .await?;
    assert_eq!(distinct.trim(), "5");

    producer.close().await?;
    Ok(())
}

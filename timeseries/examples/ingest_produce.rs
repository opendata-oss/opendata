/// Standalone ingestor that writes a few OTLP gauge metrics to MinIO.
/// Build:  cargo build -p opendata-timeseries --features "http-server,otel" --example ingest_produce
/// Run:    Same env vars as the server (AWS_*), then ./target/debug/examples/ingest_produce
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{Gauge, Metric, NumberDataPoint, ScopeMetrics, metric, number_data_point},
    resource::v1::Resource,
};
use prost::Message;

fn build_gauge(name: &str, value: f64, host: &str) -> ExportMetricsServiceRequest {
    let ts_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    ExportMetricsServiceRequest {
        resource_metrics: vec![opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "e2e-test".to_string(),
                    version: "1.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                metrics: vec![Metric {
                    name: name.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![KeyValue {
                                key: "host".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue(host.to_string())),
                                }),
                            }],
                            start_time_unix_nano: 0,
                            time_unix_nano: ts_nanos,
                            value: Some(number_data_point::Value::AsDouble(value)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

#[tokio::main]
async fn main() {
    let config = ingest::IngestorConfig {
        object_store: common::ObjectStoreConfig::Aws(
            common::storage::config::AwsObjectStoreConfig {
                region: "us-east-1".to_string(),
                bucket: "ingest-test".to_string(),
            },
        ),
        data_path_prefix: "ingest".to_string(),
        manifest_path: "ingest/manifest".to_string(),
        flush_interval: Duration::from_millis(100),
        flush_size_bytes: 64 * 1024 * 1024,
        max_buffered_inputs: 1000,
        batch_compression: ingest::CompressionType::None,
    };

    let ingestor = ingest::Ingestor::new(config, Arc::new(common::clock::SystemClock)).unwrap();

    // Metadata: version=1, signal=metrics(1), encoding=otlp_proto(1), reserved=0
    let metadata = Bytes::from_static(&[1, 1, 1, 0]);

    // Produce 5 batches with 2 metrics each
    for i in 0..5 {
        let g1 = build_gauge("cpu_temperature", 65.0 + i as f64, "server1");
        let g2 = build_gauge(
            "memory_usage_bytes",
            1_000_000.0 * (i + 1) as f64,
            "server1",
        );

        let entries = vec![
            Bytes::from(g1.encode_to_vec()),
            Bytes::from(g2.encode_to_vec()),
        ];

        let handle = ingestor.ingest(entries, metadata.clone()).await.unwrap();
        handle.watcher.clone().await_durable().await.unwrap();
        println!("batch {} flushed", i);
    }

    ingestor.close().await.unwrap();
    println!("ingestor closed, 5 batches produced");
}

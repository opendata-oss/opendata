//! Microbenchmarks for OTLP → Series conversion.
//!
//! Measures the cost of `OtelConverter::convert()` — converting an
//! `ExportMetricsServiceRequest` into `Vec<Series>`.
//!
//! Run:
//!   cargo bench -p opendata-timeseries --features otel --bench otel_conversion

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
    },
    resource::v1::Resource,
};
use timeseries::otel::{OtelConfig, OtelConverter};

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn make_gauge(name: &str, num_datapoints: usize) -> Metric {
    let data_points: Vec<NumberDataPoint> = (0..num_datapoints)
        .map(|i| NumberDataPoint {
            attributes: vec![
                kv("host", &format!("host-{i}")),
                kv("region", "us-east-1"),
                kv("env", "prod"),
            ],
            start_time_unix_nano: 0,
            time_unix_nano: 1_000_000_000 + i as u64 * 1_000_000,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(i as f64 * 1.5)),
        })
        .collect();
    Metric {
        name: name.to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge { data_points })),
    }
}

fn make_counter(name: &str, num_datapoints: usize) -> Metric {
    let data_points: Vec<NumberDataPoint> = (0..num_datapoints)
        .map(|i| NumberDataPoint {
            attributes: vec![
                kv("host", &format!("host-{i}")),
                kv("region", "us-east-1"),
                kv("env", "prod"),
            ],
            start_time_unix_nano: 0,
            time_unix_nano: 1_000_000_000 + i as u64 * 1_000_000,
            exemplars: vec![],
            flags: 0,
            value: Some(number_data_point::Value::AsDouble(i as f64 * 100.0)),
        })
        .collect();
    Metric {
        name: name.to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Sum(Sum {
            data_points,
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        })),
    }
}

fn make_histogram(name: &str, num_datapoints: usize) -> Metric {
    let data_points: Vec<HistogramDataPoint> = (0..num_datapoints)
        .map(|i| HistogramDataPoint {
            attributes: vec![
                kv("host", &format!("host-{i}")),
                kv("region", "us-east-1"),
                kv("env", "prod"),
            ],
            start_time_unix_nano: 0,
            time_unix_nano: 1_000_000_000 + i as u64 * 1_000_000,
            count: 100,
            sum: Some(5000.0),
            bucket_counts: vec![10, 30, 40, 15, 5],
            explicit_bounds: vec![10.0, 50.0, 100.0, 500.0],
            exemplars: vec![],
            flags: 0,
            min: None,
            max: None,
        })
        .collect();
    Metric {
        name: name.to_string(),
        description: String::new(),
        unit: String::new(),
        metadata: vec![],
        data: Some(metric::Data::Histogram(Histogram {
            data_points,
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        })),
    }
}

fn build_request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", "bench-svc"),
                    kv("host.name", "bench-host"),
                ],
                dropped_attributes_count: 0,
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "bench-scope".to_string(),
                    version: "0.1.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                metrics,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_gauge_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("otel_gauge_conversion");
    let builder = OtelConverter::new(OtelConfig::default());

    for &(num_metrics, dps_per_metric) in &[(10, 1), (100, 1), (100, 10), (1000, 1)] {
        let total_dps = num_metrics * dps_per_metric;
        let metrics: Vec<Metric> = (0..num_metrics)
            .map(|i| make_gauge(&format!("gauge_{i}"), dps_per_metric))
            .collect();
        let request = build_request(metrics);

        group.throughput(Throughput::Elements(total_dps as u64));
        group.bench_function(
            BenchmarkId::new("build", format!("{num_metrics}m×{dps_per_metric}dp")),
            |b| {
                b.iter(|| {
                    let series = builder.convert(black_box(&request)).unwrap();
                    black_box(series);
                });
            },
        );
    }

    group.finish();
}

fn bench_mixed_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("otel_mixed_conversion");
    let builder = OtelConverter::new(OtelConfig::default());

    for &total_metrics in &[30, 300, 900] {
        let per_type = total_metrics / 3;
        let mut metrics = Vec::with_capacity(total_metrics);
        for i in 0..per_type {
            metrics.push(make_gauge(&format!("gauge_{i}"), 1));
        }
        for i in 0..per_type {
            metrics.push(make_counter(&format!("counter_{i}"), 1));
        }
        for i in 0..per_type {
            metrics.push(make_histogram(&format!("histogram_{i}"), 1));
        }
        let request = build_request(metrics);

        // Gauges produce 1 series, counters produce 1, histograms produce ~6
        // (sum, count, +inf bucket, plus one per explicit bound).
        let expected_series = per_type + per_type + per_type * 6;
        group.throughput(Throughput::Elements(expected_series as u64));
        group.bench_function(
            BenchmarkId::new("build", format!("{total_metrics}m_mixed")),
            |b| {
                b.iter(|| {
                    let series = builder.convert(black_box(&request)).unwrap();
                    black_box(series);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(otel_benches, bench_gauge_conversion, bench_mixed_conversion);

criterion_main!(otel_benches);

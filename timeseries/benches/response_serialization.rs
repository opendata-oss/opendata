//! Microbenchmarks for Prometheus response serialization.
//!
//! Measures the full path from model-level data through response construction
//! to JSON bytes. This captures both the conversion overhead (e.g. per-sample
//! String allocations, HashMap construction) and the serialization cost.
//!
//! Run:
//!   cargo bench -p opendata-timeseries --features testing --bench response_serialization

use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use timeseries::testing::{
    MatrixSeries, QueryRangeResponse, QueryRangeResult, QueryResponse, QueryResult, VectorSeries,
};

/// Build a metric HashMap with `__name__` and `n` extra labels.
fn make_metric(name: &str, num_extra: usize) -> HashMap<String, String> {
    let mut metric = HashMap::new();
    metric.insert("__name__".to_string(), name.to_string());
    for i in 0..num_extra {
        metric.insert(format!("label_{i}"), format!("value_{i}"));
    }
    metric
}

/// Build the full range response, including HashMap + to_string() per sample.
fn make_range_response(num_series: usize, num_samples: usize) -> QueryRangeResponse {
    let result: Vec<MatrixSeries> = (0..num_series)
        .map(|i| {
            let metric = make_metric(&format!("metric_{i}"), 5);
            let values: Vec<(f64, String)> = (0..num_samples)
                .map(|j| {
                    let ts = 3600.0 + j as f64 * 60.0;
                    let val = i as f64 + j as f64 * 0.1;
                    (ts, val.to_string())
                })
                .collect();
            MatrixSeries { metric, values }
        })
        .collect();
    QueryRangeResponse {
        status: "success".to_string(),
        data: Some(QueryRangeResult {
            result_type: "matrix".to_string(),
            result,
        }),
        error: None,
        error_type: None,
    }
}

/// Build the full vector response, including HashMap + to_string() per sample.
fn make_vector_response(num_series: usize) -> QueryResponse {
    let result: Vec<VectorSeries> = (0..num_series)
        .map(|i| {
            let metric = make_metric(&format!("metric_{i}"), 5);
            let value = (3900.0, (i as f64 * 1.5).to_string());
            VectorSeries { metric, value }
        })
        .collect();
    QueryResponse {
        status: "success".to_string(),
        data: Some(QueryResult {
            result_type: "vector".to_string(),
            result: serde_json::to_value(result).unwrap(),
        }),
        error: None,
        error_type: None,
    }
}

/// Build a scalar response.
fn make_scalar_response() -> QueryResponse {
    QueryResponse {
        status: "success".to_string(),
        data: Some(QueryResult {
            result_type: "scalar".to_string(),
            result: serde_json::to_value((3900.0_f64, "42.5")).unwrap(),
        }),
        error: None,
        error_type: None,
    }
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Benchmarks: construct range response (HashMap + to_string per sample) + serialize to JSON.
fn bench_range_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_response");

    for &(num_series, num_samples) in &[
        (10, 10),
        (10, 100),
        (100, 100),
        (1000, 100),
    ] {
        let total_points = num_series * num_samples;

        group.throughput(Throughput::Elements(total_points as u64));
        group.bench_function(
            BenchmarkId::new("to_json", format!("{num_series}s×{num_samples}p")),
            |b| {
                b.iter(|| {
                    let resp = make_range_response(black_box(num_series), black_box(num_samples));
                    let json = serde_json::to_vec(&resp).unwrap();
                    black_box(json);
                });
            },
        );
    }

    group.finish();
}

/// Benchmarks: construct vector response (HashMap + to_string per sample) + serialize to JSON.
fn bench_vector_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_response");

    for &num_series in &[10, 100, 1000] {
        group.throughput(Throughput::Elements(num_series as u64));
        group.bench_function(
            BenchmarkId::new("to_json", format!("{num_series}s")),
            |b| {
                b.iter(|| {
                    let resp = make_vector_response(black_box(num_series));
                    let json = serde_json::to_vec(&resp).unwrap();
                    black_box(json);
                });
            },
        );
    }

    group.finish();
}

/// Benchmarks scalar response construction + serialization.
fn bench_scalar_response(c: &mut Criterion) {
    c.bench_function("scalar_response/to_json", |b| {
        b.iter(|| {
            let resp = make_scalar_response();
            let json = serde_json::to_vec(black_box(&resp)).unwrap();
            black_box(json);
        });
    });
}

criterion_group!(
    response_benches,
    bench_range_response,
    bench_vector_response,
    bench_scalar_response,
);

criterion_main!(response_benches);

#![cfg(feature = "testing")]
//! Integration tests for OTEL → TSDB → PromQL roundtrip.
//!
//! Exercises the full end-to-end path: build OTLP request → convert to Series
//! via [`OtelConverter`] → ingest into TSDB → flush → query back via PromQL
//! → assert results.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use opentelemetry_proto::tonic::metrics::v1::ResourceMetrics;
use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ScopeMetrics, Sum, Summary, SummaryDataPoint, metric, number_data_point,
        summary_data_point,
    },
    resource::v1::Resource,
};
use timeseries::testing::{
    self, MetadataResponse, QueryResponse, QueryResultValue, SeriesResponse, TestTsdb,
};
use timeseries::{OtelConfig, OtelConverter};
use tower::ServiceExt;

/// Timestamp in milliseconds — 3,900,000 ms = 3900 s, within bucket hour-60.
const SAMPLE_TS_MS: u64 = 3_900_000;

/// Same instant in nanoseconds (for OTLP data points).
const SAMPLE_TS_NANOS: u64 = SAMPLE_TS_MS * 1_000_000;

/// Same instant expressed in seconds (for query params).
const SAMPLE_TS_SECS: i64 = 3900;

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn make_request(resource_metrics: Vec<ResourceMetrics>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest { resource_metrics }
}

fn make_resource_metrics(
    resource_attrs: Vec<KeyValue>,
    scope_metrics: Vec<ScopeMetrics>,
) -> ResourceMetrics {
    ResourceMetrics {
        resource: Some(Resource {
            attributes: resource_attrs,
            dropped_attributes_count: 0,
        }),
        scope_metrics,
        schema_url: String::new(),
    }
}

fn make_scope_metrics(scope_name: &str, scope_version: &str, metrics: Vec<Metric>) -> ScopeMetrics {
    ScopeMetrics {
        scope: Some(InstrumentationScope {
            name: scope_name.to_string(),
            version: scope_version.to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }),
        metrics,
        schema_url: String::new(),
    }
}

fn make_gauge(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<NumberDataPoint>,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge { data_points })),
    }
}

fn make_sum(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<NumberDataPoint>,
    temporality: i32,
    monotonic: bool,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::Sum(Sum {
            data_points,
            aggregation_temporality: temporality,
            is_monotonic: monotonic,
        })),
    }
}

fn make_histogram(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<HistogramDataPoint>,
    temporality: i32,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::Histogram(Histogram {
            data_points,
            aggregation_temporality: temporality,
        })),
    }
}

fn make_summary(
    name: &str,
    unit: &str,
    description: &str,
    data_points: Vec<SummaryDataPoint>,
) -> Metric {
    Metric {
        name: name.to_string(),
        description: description.to_string(),
        unit: unit.to_string(),
        metadata: vec![],
        data: Some(metric::Data::Summary(Summary { data_points })),
    }
}

fn make_number_dp(
    value: number_data_point::Value,
    time_unix_nano: u64,
    attrs: Vec<KeyValue>,
) -> NumberDataPoint {
    NumberDataPoint {
        attributes: attrs,
        start_time_unix_nano: 0,
        time_unix_nano,
        exemplars: vec![],
        flags: 0,
        value: Some(value),
    }
}

fn make_histogram_dp(
    time_unix_nano: u64,
    count: u64,
    sum: f64,
    bucket_counts: Vec<u64>,
    explicit_bounds: Vec<f64>,
    attrs: Vec<KeyValue>,
) -> HistogramDataPoint {
    HistogramDataPoint {
        attributes: attrs,
        start_time_unix_nano: 0,
        time_unix_nano,
        count,
        sum: Some(sum),
        bucket_counts,
        explicit_bounds,
        exemplars: vec![],
        flags: 0,
        min: None,
        max: None,
    }
}

fn make_summary_dp(
    time_unix_nano: u64,
    count: u64,
    sum: f64,
    quantile_values: Vec<summary_data_point::ValueAtQuantile>,
    attrs: Vec<KeyValue>,
) -> SummaryDataPoint {
    SummaryDataPoint {
        attributes: attrs,
        start_time_unix_nano: 0,
        time_unix_nano,
        count,
        sum,
        quantile_values,
        flags: 0,
    }
}

async fn setup() -> (Router, TestTsdb) {
    let tsdb = testing::create_test_tsdb().await;
    let app = testing::http::build_app(&tsdb);
    (app, tsdb)
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

/// Convert an OTLP request to Series and ingest + flush into the test TSDB.
async fn build_and_ingest(tsdb: &TestTsdb, request: &ExportMetricsServiceRequest) {
    let builder = OtelConverter::new(OtelConfig::default());
    let series = builder.convert(request).expect("OtelConverter::convert");
    tsdb.ingest_samples(series).await;
    tsdb.flush().await;
}

#[tokio::test]
async fn test_gauge_roundtrip() {
    let (app, tsdb) = setup().await;

    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "cpu_temperature",
                "",
                "CPU temperature",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(72.5),
                    SAMPLE_TS_NANOS,
                    vec![kv("host", "server1")],
                )],
            )],
        )],
    )]);

    build_and_ingest(&tsdb, &request).await;

    let uri = format!(
        "/api/v1/query?query=cpu_temperature&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");

    let data = parsed.data.unwrap();
    let results = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector variant"),
    };
    assert!(!results.is_empty(), "should have at least one result");
    assert_eq!(results[0].0.value, 72.5);
    assert_eq!(results[0].0.labels.get("host"), Some("server1"));
}

#[tokio::test]
async fn test_counter_roundtrip() {
    let (app, tsdb) = setup().await;

    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_sum(
                "http_requests",
                "",
                "Total HTTP requests",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(100.0),
                    SAMPLE_TS_NANOS,
                    vec![kv("method", "GET")],
                )],
                AggregationTemporality::Cumulative as i32,
                true,
            )],
        )],
    )]);

    build_and_ingest(&tsdb, &request).await;

    // Monotonic cumulative sum should be queryable with _total suffix.
    let uri = format!(
        "/api/v1/query?query=http_requests_total&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");

    let data = parsed.data.unwrap();
    let results = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector variant"),
    };
    assert!(!results.is_empty(), "should find http_requests_total");
    assert_eq!(results[0].0.value, 100.0);
}

#[tokio::test]
async fn test_histogram_bucket_sum_count_queryable() {
    let (app, tsdb) = setup().await;

    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_histogram(
                "http_request_duration",
                "",
                "Request duration",
                vec![make_histogram_dp(
                    SAMPLE_TS_NANOS,
                    10,
                    5.5,
                    vec![2, 3, 5, 10],
                    vec![0.1, 0.5, 1.0],
                    vec![],
                )],
                AggregationTemporality::Cumulative as i32,
            )],
        )],
    )]);

    build_and_ingest(&tsdb, &request).await;

    // Query _bucket
    let uri = format!(
        "/api/v1/query?query=http_request_duration_bucket&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");
    let data = parsed.data.unwrap();
    let buckets = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert!(!buckets.is_empty(), "should have _bucket series");

    // Query _sum
    let uri = format!(
        "/api/v1/query?query=http_request_duration_sum&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    let data = parsed.data.unwrap();
    let sums = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert_eq!(sums.len(), 1);
    assert_eq!(sums[0].0.value, 5.5);

    // Query _count
    let uri = format!(
        "/api/v1/query?query=http_request_duration_count&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    let data = parsed.data.unwrap();
    let counts = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert_eq!(counts.len(), 1);
    assert_eq!(counts[0].0.value, 10.0);
}

#[tokio::test]
async fn test_summary_quantiles_queryable() {
    let (app, tsdb) = setup().await;

    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_summary(
                "rpc_duration",
                "",
                "RPC duration",
                vec![make_summary_dp(
                    SAMPLE_TS_NANOS,
                    100,
                    500.0,
                    vec![
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.5,
                            value: 4.0,
                        },
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.99,
                            value: 8.0,
                        },
                    ],
                    vec![],
                )],
            )],
        )],
    )]);

    build_and_ingest(&tsdb, &request).await;

    // Query the base metric name — should return the quantile series.
    let uri = format!("/api/v1/query?query=rpc_duration&time={}", SAMPLE_TS_SECS);
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    let data = parsed.data.unwrap();
    let results = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert_eq!(results.len(), 2, "should have 2 quantile series");

    // Query _sum
    let uri = format!(
        "/api/v1/query?query=rpc_duration_sum&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    let data = parsed.data.unwrap();
    let sums = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert_eq!(sums.len(), 1);
    assert_eq!(sums[0].0.value, 500.0);

    // Query _count
    let uri = format!(
        "/api/v1/query?query=rpc_duration_count&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    let data = parsed.data.unwrap();
    let counts = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert_eq!(counts.len(), 1);
    assert_eq!(counts[0].0.value, 100.0);
}

#[tokio::test]
async fn test_resource_and_scope_labels_in_series() {
    let (app, tsdb) = setup().await;

    let request = make_request(vec![make_resource_metrics(
        vec![kv("service.name", "my-svc")],
        vec![make_scope_metrics(
            "my.library",
            "2.0",
            vec![make_gauge(
                "cpu_temp",
                "",
                "",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    SAMPLE_TS_NANOS,
                    vec![],
                )],
            )],
        )],
    )]);

    build_and_ingest(&tsdb, &request).await;

    // Use /api/v1/series to discover labels on the ingested series.
    let form_body = format!(
        "match[]=cpu_temp&start={}&end={}",
        SAMPLE_TS_SECS - 300,
        SAMPLE_TS_SECS + 300,
    );
    let req = Request::post("/api/v1/series")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(form_body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: SeriesResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");
    let data = parsed.data.unwrap();
    assert_eq!(data.len(), 1);

    let labels = &data[0];
    assert_eq!(
        labels.get("service_name"),
        Some("my-svc"),
        "resource attr service.name should appear as service_name label"
    );
    assert_eq!(
        labels.get("otel_scope_name"),
        Some("my.library"),
        "scope name should appear as otel_scope_name label"
    );
}

#[tokio::test]
async fn test_metadata_preserved() {
    let (app, tsdb) = setup().await;

    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![make_gauge(
                "cpu_temp",
                "Cel",
                "CPU temperature",
                vec![make_number_dp(
                    number_data_point::Value::AsDouble(70.0),
                    SAMPLE_TS_NANOS,
                    vec![],
                )],
            )],
        )],
    )]);

    build_and_ingest(&tsdb, &request).await;

    let req = Request::get("/api/v1/metadata")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: MetadataResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");
    let data = parsed.data.unwrap();

    // The metric name gets unit suffix appended: cpu_temp + Cel → cpu_temp_Cel
    let metric_name = data
        .keys()
        .find(|k| k.starts_with("cpu_temp"))
        .expect("should find cpu_temp* in metadata");
    let entries = &data[metric_name];
    assert!(!entries.is_empty());
    assert_eq!(entries[0].0.metric_type.as_ref().unwrap().as_str(), "gauge");
    assert_eq!(
        entries[0].0.description.as_deref().unwrap(),
        "CPU temperature"
    );
    assert!(
        entries[0].0.unit.is_some(),
        "unit should be preserved in metadata"
    );
}

#[tokio::test]
async fn test_multiple_metric_types_single_request() {
    let (app, tsdb) = setup().await;

    let request = make_request(vec![make_resource_metrics(
        vec![],
        vec![make_scope_metrics(
            "test",
            "1.0",
            vec![
                // Gauge
                make_gauge(
                    "temperature",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(22.5),
                        SAMPLE_TS_NANOS,
                        vec![],
                    )],
                ),
                // Counter
                make_sum(
                    "requests",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(999.0),
                        SAMPLE_TS_NANOS,
                        vec![],
                    )],
                    AggregationTemporality::Cumulative as i32,
                    true,
                ),
                // Histogram
                make_histogram(
                    "latency",
                    "",
                    "",
                    vec![make_histogram_dp(
                        SAMPLE_TS_NANOS,
                        50,
                        25.0,
                        vec![10, 20, 20, 50],
                        vec![0.01, 0.1, 1.0],
                        vec![],
                    )],
                    AggregationTemporality::Cumulative as i32,
                ),
            ],
        )],
    )]);

    build_and_ingest(&tsdb, &request).await;

    // Query each metric independently.

    // Gauge
    let uri = format!("/api/v1/query?query=temperature&time={}", SAMPLE_TS_SECS);
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    let data = parsed.data.unwrap();
    let results = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.value, 22.5);

    // Counter (with _total suffix)
    let uri = format!("/api/v1/query?query=requests_total&time={}", SAMPLE_TS_SECS);
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    let data = parsed.data.unwrap();
    let results = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.value, 999.0);

    // Histogram _count
    let uri = format!("/api/v1/query?query=latency_count&time={}", SAMPLE_TS_SECS);
    let req = Request::get(&uri).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    let data = parsed.data.unwrap();
    let results = match data.result {
        QueryResultValue::Vector(v) => v,
        _ => panic!("expected Vector"),
    };
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0.value, 50.0);
}

#![cfg(feature = "testing")]
//! Integration tests for the timeseries HTTP server.
//!
//! Exercises HTTP endpoints using Axum's `oneshot()` test infrastructure
//! with a real SlateDB-backed TSDB (in-memory object store).

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use timeseries::testing::{
    self, LabelValuesResponse, LabelsResponse, QueryRangeResponse, QueryResponse, SeriesResponse,
    Tsdb, VectorSeries,
};
use timeseries::{Label, MetricType, Sample, Series};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Timestamp in milliseconds — 3,900,000 ms = 3900 s, within bucket hour-60
/// (bucket covers seconds 3600–7199).
const SAMPLE_TS_MS: i64 = 3_900_000;

/// Same instant expressed in seconds (for query params).
const SAMPLE_TS_SECS: i64 = 3900;

async fn setup() -> (Router, Arc<Tsdb>) {
    let tsdb = testing::create_test_tsdb().await;
    let app = testing::build_app(tsdb.clone());
    (app, tsdb)
}

async fn setup_with_data() -> (Router, Arc<Tsdb>) {
    let (app, tsdb) = setup().await;
    ingest_test_data(&tsdb).await;
    (app, tsdb)
}

async fn ingest_test_data(tsdb: &Tsdb) {
    let series = vec![
        Series {
            labels: vec![
                Label::metric_name("http_requests_total"),
                Label::new("method", "GET"),
                Label::new("status", "200"),
            ],
            metric_type: Some(MetricType::Gauge),
            unit: None,
            description: None,
            samples: vec![Sample::new(SAMPLE_TS_MS, 42.0)],
        },
        Series {
            labels: vec![
                Label::metric_name("http_requests_total"),
                Label::new("method", "POST"),
                Label::new("status", "201"),
            ],
            metric_type: Some(MetricType::Gauge),
            unit: None,
            description: None,
            samples: vec![Sample::new(SAMPLE_TS_MS + 1, 7.0)],
        },
    ];

    tsdb.ingest_samples(series).await.unwrap();
    tsdb.flush().await.unwrap();
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

// ---------------------------------------------------------------------------
// Health / readiness / metrics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_healthy() {
    let (app, _) = setup().await;
    let req = Request::get("/-/healthy").body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "OK");
}

#[tokio::test]
async fn test_ready() {
    let (app, _) = setup().await;
    let req = Request::get("/-/ready").body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "OK");
}

#[tokio::test]
async fn test_metrics() {
    let (app, _) = setup().await;
    let req = Request::get("/metrics").body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert!(!body.is_empty());
    assert!(body.contains("http_requests_total"));
}

// ---------------------------------------------------------------------------
// UI redirect
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ui_redirect() {
    let (app, _) = setup().await;
    let req = Request::get("/").body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(resp.headers().get("location").unwrap(), "/query");
}

// ---------------------------------------------------------------------------
// /api/v1/query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_get() {
    let (app, _) = setup_with_data().await;
    let uri = format!(
        "/api/v1/query?query=http_requests_total&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    let parsed: QueryResponse = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed.status, "success");
    assert!(parsed.data.is_some());
}

#[tokio::test]
async fn test_query_post() {
    let (app, _) = setup_with_data().await;
    let form_body = format!(
        "query=http_requests_total&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::post("/api/v1/query")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(form_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");
    assert!(parsed.data.is_some());
}

#[tokio::test]
async fn test_query_bad_syntax() {
    let (app, _) = setup().await;
    // PromQL parse errors are returned as 200 with status: "error" in the
    // JSON body (matching Prometheus behaviour).
    let req = Request::get("/api/v1/query?query=)))&time=100")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "error");
    assert!(parsed.error_type.is_some());
}

// ---------------------------------------------------------------------------
// /api/v1/query_range
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_query_range_get() {
    let (app, _) = setup_with_data().await;
    let uri = format!(
        "/api/v1/query_range?query=http_requests_total&start={}&end={}&step=60s",
        SAMPLE_TS_SECS - 300,
        SAMPLE_TS_SECS + 300,
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryRangeResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");
    assert!(parsed.data.is_some());
}

#[tokio::test]
async fn test_query_range_post() {
    let (app, _) = setup_with_data().await;
    let form_body = format!(
        "query=http_requests_total&start={}&end={}&step=60s",
        SAMPLE_TS_SECS - 300,
        SAMPLE_TS_SECS + 300,
    );
    let req = Request::post("/api/v1/query_range")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(form_body))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryRangeResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");
    assert!(parsed.data.is_some());
}

#[tokio::test]
async fn test_query_range_missing_params() {
    let (app, _) = setup().await;
    // Missing start, end, step
    let req = Request::get("/api/v1/query_range?query=up")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// /api/v1/series
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_series() {
    let (app, _) = setup_with_data().await;
    // NOTE: serde_urlencoded does not support Vec<String> from repeated
    // query params, so match[] cannot be passed via GET or POST form.
    // Test that the endpoint responds and the response deserializes into
    // the production SeriesResponse type. Without match[] the handler
    // returns an error response — we verify that format here.
    let uri = format!(
        "/api/v1/series?start={}&end={}",
        SAMPLE_TS_SECS - 300,
        SAMPLE_TS_SECS + 300,
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: SeriesResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    // Empty match[] defaults to error in the handler.
    assert_eq!(parsed.status, "error");
}

// ---------------------------------------------------------------------------
// /api/v1/labels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_labels() {
    let (app, _) = setup_with_data().await;
    let uri = format!(
        "/api/v1/labels?start={}&end={}",
        SAMPLE_TS_SECS - 300,
        SAMPLE_TS_SECS + 300,
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: LabelsResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");
    let data = parsed.data.unwrap();
    assert!(data.contains(&"__name__".to_string()));
    assert!(data.contains(&"method".to_string()));
    assert!(data.contains(&"status".to_string()));
}

// ---------------------------------------------------------------------------
// /api/v1/label/{name}/values
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_label_values() {
    let (app, _) = setup_with_data().await;
    let uri = format!(
        "/api/v1/label/__name__/values?start={}&end={}",
        SAMPLE_TS_SECS - 300,
        SAMPLE_TS_SECS + 300,
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: LabelValuesResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");
    let data = parsed.data.unwrap();
    assert!(data.contains(&"http_requests_total".to_string()));
}

// ---------------------------------------------------------------------------
// Roundtrip: ingest → query via HTTP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_roundtrip() {
    let (app, _) = setup_with_data().await;
    let uri = format!(
        "/api/v1/query?query=http_requests_total%7Bmethod%3D%22GET%22%7D&time={}",
        SAMPLE_TS_SECS
    );
    let req = Request::get(&uri).body(Body::empty()).unwrap();

    let resp = app.oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let parsed: QueryResponse = serde_json::from_str(&body_string(resp).await).unwrap();
    assert_eq!(parsed.status, "success");

    let data = parsed.data.unwrap();
    assert_eq!(data.result_type, "vector");

    // Deserialize the result as a vector of VectorSeries
    let results: Vec<VectorSeries> = serde_json::from_value(data.result).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].metric.get("__name__").unwrap(),
        "http_requests_total"
    );
    assert_eq!(results[0].metric.get("method").unwrap(), "GET");
    // Value should be "42" (string representation)
    assert_eq!(results[0].value.1, "42");
}

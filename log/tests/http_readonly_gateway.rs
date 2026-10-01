#![cfg(feature = "http-server")]
//! Integration tests for the read-only HTTP gateway.
//!
//! These drive the real router built by [`log::server::LogServer::into_router`]
//! (the same one the server serves) so the read-only wiring is exercised end to
//! end: a reader-backed gateway serves reads, the append route is absent (404),
//! and readiness flows through `LogDbReader`. A read-write counterpart pins the
//! append route from the other side.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use common::StorageConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use log::server::{LogServer, LogServerConfig};
use log::{Config, LogDb, LogDbReader, LogRead, ReaderConfig, Record};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tempfile::TempDir;
use tower::ServiceExt;

/// A throwaway Prometheus handle for building a server in tests; the recorder
/// is not installed globally, so metric increments are no-ops.
fn test_metrics_handle() -> PrometheusHandle {
    PrometheusBuilder::new().build_recorder().handle()
}

fn local_storage_config(dir: &TempDir) -> StorageConfig {
    StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "log-data".to_string(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: dir.path().to_string_lossy().to_string(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    })
}

async fn wait_until<F, Fut>(timeout: Duration, interval: Duration, mut predicate: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "condition was not satisfied within {:?}",
            timeout
        );
        tokio::time::sleep(interval).await;
    }
}

/// Persists two records to "events" with a writer, then returns a read-only
/// gateway router backed by a `LogDbReader` that has discovered them. The
/// `TempDir` and writer are returned so the caller keeps the backing storage
/// alive for the duration of the test.
async fn setup_readonly_gateway() -> (Router, TempDir, Arc<LogDb>) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let storage = local_storage_config(&temp_dir);

    let writer = LogDb::open(Config {
        storage: storage.clone(),
        ..Default::default()
    })
    .await
    .expect("Failed to open writer");
    writer
        .try_append(vec![
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-1"),
            },
            Record {
                key: Bytes::from("events"),
                value: Bytes::from("event-2"),
            },
        ])
        .await
        .expect("Failed to append");
    writer.flush().await.expect("Failed to flush");
    let writer = Arc::new(writer);

    let reader = LogDbReader::open(ReaderConfig {
        storage,
        refresh_interval: Duration::from_millis(50),
    })
    .await
    .expect("Failed to open reader");

    // Wait for the reader's background refresh to discover the flushed data
    // before fronting it with the gateway.
    wait_until(
        Duration::from_secs(5),
        Duration::from_millis(50),
        || async { reader.count(Bytes::from("events"), ..).await.unwrap_or(0) == 2 },
    )
    .await;

    let app = LogServer::new_read_only(
        Arc::new(reader),
        LogServerConfig::default(),
        test_metrics_handle(),
    )
    .into_router();
    (app, temp_dir, writer)
}

#[tokio::test]
async fn read_only_gateway_serves_scans() {
    let (app, _temp_dir, _writer) = setup_readonly_gateway().await;

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/scan?key=events")
        .header(header::ACCEPT, "application/protobuf+json")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    let values = json["values"].as_array().unwrap();
    assert_eq!(values.len(), 2);

    let value0 = STANDARD
        .decode(values[0]["value"].as_str().unwrap())
        .unwrap();
    assert_eq!(value0, b"event-1");
    let value1 = STANDARD
        .decode(values[1]["value"].as_str().unwrap())
        .unwrap();
    assert_eq!(value1, b"event-2");
}

#[tokio::test]
async fn read_only_gateway_serves_count() {
    let (app, _temp_dir, _writer) = setup_readonly_gateway().await;

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/count?key=events")
        .header(header::ACCEPT, "application/protobuf+json")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["count"], 2);
}

#[tokio::test]
async fn read_only_gateway_rejects_append_with_404() {
    let (app, _temp_dir, _writer) = setup_readonly_gateway().await;

    let key_b64 = STANDARD.encode("events");
    let value_b64 = STANDARD.encode("should-not-write");
    let body = format!(
        r#"{{"records": [{{"key": "{}", "value": "{}"}}], "awaitDurable": false}}"#,
        key_b64, value_b64
    );

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf+json")
        .body(Body::from(body))
        .unwrap();

    // The append route is not registered in read-only mode, so axum has no
    // match and returns 404 rather than reaching the handler.
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn read_only_gateway_reports_ready() {
    let (app, _temp_dir, _writer) = setup_readonly_gateway().await;

    let request = Request::builder()
        .method("GET")
        .uri("/-/ready")
        .body(Body::empty())
        .unwrap();

    // Readiness flows through LogDbReader::check_storage.
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn read_write_gateway_accepts_append() {
    // Counterpart to the read-only 404 test: the same router builder registers
    // the append route for a read-write backend, so the write succeeds.
    let log = Arc::new(
        LogDb::open(Config {
            storage: StorageConfig::InMemory,
            ..Default::default()
        })
        .await
        .expect("Failed to open log"),
    );
    let app = LogServer::new(log, LogServerConfig::default(), test_metrics_handle()).into_router();

    let key_b64 = STANDARD.encode("events");
    let value_b64 = STANDARD.encode("event-1");
    let body = format!(
        r#"{{"records": [{{"key": "{}", "value": "{}"}}], "awaitDurable": false}}"#,
        key_b64, value_b64
    );

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf+json")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

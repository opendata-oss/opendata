#![cfg(feature = "http-server")]
//! Integration tests for HTTP API with JSON and protobuf formats.
//!
//! Tests both ProtoJSON (application/protobuf+json) and binary protobuf (application/protobuf)
//! request/response formats per RFC 0004.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::routing::{get, post};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use common::StorageConfig;
use log::server::handlers::{
    AppState, handle_append, handle_list_keys, handle_list_segments, handle_scan,
};
use log::server::metrics::Metrics;
use log::server::proto;
use log::{Config, LogDb, Record};
use prost::Message;
use tower::ServiceExt;

async fn setup_test_app() -> (Router, Arc<LogDb>) {
    let config = Config {
        storage: StorageConfig::InMemory,
        ..Default::default()
    };
    let log = Arc::new(LogDb::open(config).await.expect("Failed to open log"));
    let metrics = Arc::new(Metrics::new());

    let state = AppState {
        log: log.clone(),
        metrics,
    };

    let app = Router::new()
        .route("/api/v1/log/append", post(handle_append))
        .route("/api/v1/log/scan", get(handle_scan))
        .route("/api/v1/log/keys", get(handle_list_keys))
        .route("/api/v1/log/segments", get(handle_list_segments))
        .with_state(state);

    (app, log)
}

// ============================================================================
// JSON Format Tests
// ============================================================================

#[tokio::test]
async fn test_append_json_request_and_response() {
    let (app, _log) = setup_test_app().await;

    // Encode key and value as base64: "test-key" -> "dGVzdC1rZXk=", "test-value" -> "dGVzdC12YWx1ZQ=="
    let key_b64 = STANDARD.encode("test-key");
    let value_b64 = STANDARD.encode("test-value");

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

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["recordsAppended"], 1);
    assert_eq!(json["startSequence"], 0);
}

#[tokio::test]
async fn test_append_multiple_records_json() {
    let (app, _log) = setup_test_app().await;

    let key1_b64 = STANDARD.encode("key-1");
    let value1_b64 = STANDARD.encode("value-1");
    let key2_b64 = STANDARD.encode("key-2");
    let value2_b64 = STANDARD.encode("value-2");

    let body = format!(
        r#"{{"records": [{{"key": "{}", "value": "{}"}}, {{"key": "{}", "value": "{}"}}], "awaitDurable": false}}"#,
        key1_b64, value1_b64, key2_b64, value2_b64
    );

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf+json")
        .body(Body::from(body))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["recordsAppended"], 2);
    assert_eq!(json["startSequence"], 0);
}

#[tokio::test]
async fn test_scan_json_response() {
    let (app, log) = setup_test_app().await;

    // Append some records directly
    log.try_append(vec![
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
    .unwrap();
    log.flush().await.unwrap();

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

    let key_b64 = json["key"].as_str().unwrap();
    let key_decoded = STANDARD.decode(key_b64).unwrap();
    assert_eq!(key_decoded, b"events");

    let values = json["values"].as_array().unwrap();
    assert_eq!(values.len(), 2);

    // First value
    assert_eq!(values[0]["sequence"], 0);
    let value_decoded = STANDARD
        .decode(values[0]["value"].as_str().unwrap())
        .unwrap();
    assert_eq!(value_decoded, b"event-1");

    // Second value
    assert_eq!(values[1]["sequence"], 1);
    let value_decoded = STANDARD
        .decode(values[1]["value"].as_str().unwrap())
        .unwrap();
    assert_eq!(value_decoded, b"event-2");
}

#[tokio::test]
async fn test_list_keys_json_response() {
    let (app, log) = setup_test_app().await;

    log.try_append(vec![
        Record {
            key: Bytes::from("orders"),
            value: Bytes::from("order-1"),
        },
        Record {
            key: Bytes::from("events"),
            value: Bytes::from("event-1"),
        },
    ])
    .await
    .unwrap();
    log.flush().await.unwrap();

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/keys")
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

    let keys = json["keys"].as_array().unwrap();
    assert_eq!(keys.len(), 2);

    // Keys are returned in lexicographic order: "events" before "orders"
    let key1_decoded = STANDARD.decode(keys[0]["key"].as_str().unwrap()).unwrap();
    let key2_decoded = STANDARD.decode(keys[1]["key"].as_str().unwrap()).unwrap();
    assert_eq!(key1_decoded, b"events");
    assert_eq!(key2_decoded, b"orders");
}

#[tokio::test]
async fn test_list_segments_json_response() {
    let (app, log) = setup_test_app().await;

    log.try_append(vec![Record {
        key: Bytes::from("key"),
        value: Bytes::from("value"),
    }])
    .await
    .unwrap();
    log.flush().await.unwrap();

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/segments")
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

    let segments = json["segments"].as_array().unwrap();
    assert_eq!(segments.len(), 1);

    // Check camelCase field names
    assert_eq!(segments[0]["id"], 0);
    assert_eq!(segments[0]["startSeq"], 0);
    assert!(segments[0]["startTimeMs"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn test_error_response_json_format() {
    let (app, _log) = setup_test_app().await;

    // Send invalid JSON
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf+json")
        .body(Body::from("not valid json"))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "error");
    assert!(json["message"].as_str().unwrap().contains("Invalid JSON"));
}

// ============================================================================
// Protobuf Format Tests
// ============================================================================

#[tokio::test]
async fn test_append_protobuf_request_and_response() {
    let (app, _log) = setup_test_app().await;

    let proto_request = proto::AppendRequest {
        records: vec![proto::Record {
            key: Some(Bytes::from("proto-key")),
            value: Some(Bytes::from("proto-value")),
        }],
        await_durable: false,
    };

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf")
        .header(header::ACCEPT, "application/protobuf")
        .body(Body::from(proto_request.encode_to_vec()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/protobuf"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let proto_response = proto::AppendResponse::decode(body.as_ref()).unwrap();

    assert_eq!(proto_response.status, "success");
    assert_eq!(proto_response.records_appended, 1);
    assert_eq!(proto_response.start_sequence, 0);
}

#[tokio::test]
async fn test_append_protobuf_request_json_response() {
    // Test protobuf request with JSON response (Accept: application/protobuf+json)
    let (app, _log) = setup_test_app().await;

    let proto_request = proto::AppendRequest {
        records: vec![proto::Record {
            key: Some(Bytes::from("mixed-key")),
            value: Some(Bytes::from("mixed-value")),
        }],
        await_durable: false,
    };

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf")
        .header(header::ACCEPT, "application/protobuf+json")
        .body(Body::from(proto_request.encode_to_vec()))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["recordsAppended"], 1);
}

#[tokio::test]
async fn test_scan_protobuf_response() {
    let (app, log) = setup_test_app().await;

    log.try_append(vec![
        Record {
            key: Bytes::from("proto-events"),
            value: Bytes::from("event-a"),
        },
        Record {
            key: Bytes::from("proto-events"),
            value: Bytes::from("event-b"),
        },
    ])
    .await
    .unwrap();
    log.flush().await.unwrap();

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/scan?key=proto-events")
        .header(header::ACCEPT, "application/protobuf")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/protobuf"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let proto_response = proto::ScanResponse::decode(body.as_ref()).unwrap();

    assert_eq!(proto_response.status, "success");
    assert_eq!(
        *proto_response.key.as_ref().unwrap(),
        Bytes::from("proto-events")
    );
    assert_eq!(proto_response.values.len(), 2);
    assert_eq!(proto_response.values[0].sequence, 0);
    assert_eq!(proto_response.values[0].value, Bytes::from("event-a"));
    assert_eq!(proto_response.values[1].sequence, 1);
    assert_eq!(proto_response.values[1].value, Bytes::from("event-b"));
}

#[tokio::test]
async fn test_list_keys_protobuf_response() {
    let (app, log) = setup_test_app().await;

    log.try_append(vec![
        Record {
            key: Bytes::from("alpha"),
            value: Bytes::from("value"),
        },
        Record {
            key: Bytes::from("beta"),
            value: Bytes::from("value"),
        },
    ])
    .await
    .unwrap();
    log.flush().await.unwrap();

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/keys")
        .header(header::ACCEPT, "application/protobuf")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/protobuf"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let proto_response = proto::KeysResponse::decode(body.as_ref()).unwrap();

    assert_eq!(proto_response.status, "success");
    assert_eq!(proto_response.keys.len(), 2);
    assert_eq!(proto_response.keys[0].key, Bytes::from("alpha"));
    assert_eq!(proto_response.keys[1].key, Bytes::from("beta"));
}

#[tokio::test]
async fn test_list_segments_protobuf_response() {
    let (app, log) = setup_test_app().await;

    log.try_append(vec![Record {
        key: Bytes::from("seg-key"),
        value: Bytes::from("seg-value"),
    }])
    .await
    .unwrap();
    log.flush().await.unwrap();

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/segments")
        .header(header::ACCEPT, "application/protobuf")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/protobuf"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let proto_response = proto::SegmentsResponse::decode(body.as_ref()).unwrap();

    assert_eq!(proto_response.status, "success");
    assert_eq!(proto_response.segments.len(), 1);
    assert_eq!(proto_response.segments[0].id, 0);
    assert_eq!(proto_response.segments[0].start_seq, 0);
    assert!(proto_response.segments[0].start_time_ms > 0);
}

#[tokio::test]
async fn test_append_protobuf_error_on_invalid_body() {
    let (app, _log) = setup_test_app().await;

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf")
        .body(Body::from(vec![0xFF, 0xFF, 0xFF])) // Invalid protobuf
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "error");
    assert!(
        json["message"]
            .as_str()
            .unwrap()
            .contains("Invalid protobuf")
    );
}

// ============================================================================
// Roundtrip Tests
// ============================================================================

#[tokio::test]
async fn test_json_append_then_scan_roundtrip() {
    let (app, _log) = setup_test_app().await;

    // Append via JSON
    let key_b64 = STANDARD.encode("roundtrip-key");
    let value_b64 = STANDARD.encode("roundtrip-value");

    let append_body = format!(
        r#"{{"records": [{{"key": "{}", "value": "{}"}}], "awaitDurable": true}}"#,
        key_b64, value_b64
    );

    let append_request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf+json")
        .body(Body::from(append_body))
        .unwrap();

    let append_response = app.clone().oneshot(append_request).await.unwrap();
    assert_eq!(append_response.status(), StatusCode::OK);

    // Scan via JSON
    let scan_request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/scan?key=roundtrip-key")
        .header(header::ACCEPT, "application/protobuf+json")
        .body(Body::empty())
        .unwrap();

    let scan_response = app.oneshot(scan_request).await.unwrap();
    assert_eq!(scan_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(scan_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let values = json["values"].as_array().unwrap();
    assert_eq!(values.len(), 1);

    let value_decoded = STANDARD
        .decode(values[0]["value"].as_str().unwrap())
        .unwrap();
    assert_eq!(value_decoded, b"roundtrip-value");
}

#[tokio::test]
async fn test_protobuf_append_then_scan_roundtrip() {
    let (app, _log) = setup_test_app().await;

    // Append via protobuf
    let proto_request = proto::AppendRequest {
        records: vec![proto::Record {
            key: Some(Bytes::from("proto-roundtrip")),
            value: Some(Bytes::from("proto-roundtrip-value")),
        }],
        await_durable: true,
    };

    let append_request = Request::builder()
        .method("POST")
        .uri("/api/v1/log/append")
        .header(header::CONTENT_TYPE, "application/protobuf")
        .header(header::ACCEPT, "application/protobuf")
        .body(Body::from(proto_request.encode_to_vec()))
        .unwrap();

    let append_response = app.clone().oneshot(append_request).await.unwrap();
    assert_eq!(append_response.status(), StatusCode::OK);

    // Scan via protobuf
    let scan_request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/scan?key=proto-roundtrip")
        .header(header::ACCEPT, "application/protobuf")
        .body(Body::empty())
        .unwrap();

    let scan_response = app.oneshot(scan_request).await.unwrap();
    assert_eq!(scan_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(scan_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let proto_response = proto::ScanResponse::decode(body.as_ref()).unwrap();

    assert_eq!(proto_response.values.len(), 1);
    assert_eq!(
        proto_response.values[0].value,
        Bytes::from("proto-roundtrip-value")
    );
}

#[tokio::test]
async fn test_default_response_format_is_json() {
    // When no Accept header is provided, response should be JSON
    let (app, log) = setup_test_app().await;

    log.try_append(vec![Record {
        key: Bytes::from("default-test"),
        value: Bytes::from("value"),
    }])
    .await
    .unwrap();
    log.flush().await.unwrap();

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/scan?key=default-test")
        // No Accept header
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Should be valid JSON with new field names
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    // Verify "values" field exists (not "entries")
    assert!(json["values"].is_array());
}

// ============================================================================
// Follow / Long-poll Tests
// ============================================================================

// Uses start_paused so tokio::time::Instant auto-advances, making the test
// deterministic regardless of CI runner speed. Virtual elapsed time is
// asserted to verify the deadline cap actually bounds the sleep.
#[tokio::test(start_paused = true)]
async fn test_scan_follow_with_short_timeout_returns_empty() {
    let (app, _log) = setup_test_app().await;

    // follow=true with timeout_ms=10 < poll_interval (100ms).
    // The deadline cap ensures we sleep only 10ms, not a full poll interval.
    let start = tokio::time::Instant::now();
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/scan?key=no-data&follow=true&timeout_ms=10")
        .header(header::ACCEPT, "application/protobuf+json")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["values"].as_array().unwrap().len(), 0);
    // Virtual time should be ~10ms (the timeout), not 100ms (the poll interval).
    assert!(
        elapsed >= Duration::from_millis(10),
        "expected >= 10ms but got {:?}",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(100),
        "expected < 100ms but got {:?} — deadline cap may not be working",
        elapsed
    );
}

#[tokio::test(start_paused = true)]
async fn test_scan_follow_at_poll_boundary_returns_empty() {
    let (app, _log) = setup_test_app().await;

    // timeout_ms=100 matches the poll interval exactly — should still
    // complete and return empty without hanging.
    let start = tokio::time::Instant::now();
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/log/scan?key=no-data&follow=true&timeout_ms=100")
        .header(header::ACCEPT, "application/protobuf+json")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "success");
    assert_eq!(json["values"].as_array().unwrap().len(), 0);
    // Virtual time should be ~100ms (one poll interval = the timeout).
    assert!(
        elapsed >= Duration::from_millis(100),
        "expected >= 100ms but got {:?} — may be returning immediately",
        elapsed
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "expected < 200ms but got {:?}",
        elapsed
    );
}

//! HTTP integration tests for the OpenData Graph server.
//!
//! Mirrors the timeseries `http_server.rs` test pattern: builds an axum
//! Router via `build_app()` and exercises endpoints with `oneshot()`.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use common::StorageConfig;
use graph::db::GraphDb;
use graph::server::build_app;
use graph::Config;

/// Create an in-memory GraphDb and build the HTTP router.
async fn setup() -> axum::Router {
    let config = Config {
        storage: StorageConfig::InMemory,
        ..Default::default()
    };

    let db = Arc::new(
        GraphDb::open_with_config(&config)
            .await
            .expect("Failed to open graph database"),
    );

    build_app(db)
}

// ─── Health endpoints ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_healthy() {
    let app = setup().await;
    let req = Request::get("/-/healthy").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ready() {
    let app = setup().await;
    let req = Request::get("/-/ready").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ─── Query endpoint ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_query_create_node() {
    let app = setup().await;

    let body = serde_json::json!({
        "query": "CREATE (:Person {name: 'Alice', age: 30})"
    });

    let req = Request::post("/api/v1/graph/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_query_create_and_match_via_http() {
    let app = setup().await;

    // CREATE a node
    let create_body = serde_json::json!({
        "query": "CREATE (:Person {name: 'Alice'})"
    });
    let req = Request::post("/api/v1/graph/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&create_body).unwrap()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // MATCH the node back
    let match_body = serde_json::json!({
        "query": "MATCH (n:Person) RETURN n.name"
    });
    let req = Request::post("/api/v1/graph/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&match_body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();

    assert!(json["columns"].as_array().is_some());
    assert!(json["rows"].as_array().is_some());
    assert!(
        !json["rows"].as_array().unwrap().is_empty(),
        "Should find the created node"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_query_bad_syntax_returns_400() {
    let app = setup().await;

    let body = serde_json::json!({
        "query": "THIS IS NOT VALID GQL !!!"
    });

    let req = Request::post("/api/v1/graph/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "Invalid GQL should return 400"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_not_found_route() {
    let app = setup().await;
    let req = Request::get("/no-such-route").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_query_missing_body_returns_error() {
    let app = setup().await;

    let req = Request::post("/api/v1/graph/query")
        .header("content-type", "application/json")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // Missing JSON body should result in a client error
    assert!(
        resp.status().is_client_error(),
        "Empty body should return 4xx"
    );
}

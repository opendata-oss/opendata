//! HTTP route handlers for the log server.
//!
//! Per RFC 0004, handlers support both binary protobuf (`application/protobuf`)
//! and ProtoJSON (`application/protobuf+json`) formats.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;

use super::error::ApiError;
use super::metrics::Metrics;
use super::proto::{
    AppendResponse, CountResponse, KeysResponse, ScanResponse, Segment, SegmentsResponse, Value,
};
use super::request::{AppendRequest, CountParams, ListKeysParams, ListSegmentsParams, ScanParams};
use super::response::{ApiResponse, ResponseFormat, to_api_response};
use crate::LogDb;
use crate::reader::LogRead;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub log: Arc<LogDb>,
    pub metrics: Arc<Metrics>,
}

/// Handle POST /api/v1/log/append
///
/// Supports both `Content-Type: application/protobuf` and `Content-Type: application/protobuf+json`.
/// Returns response in format matching the `Accept` header.
pub async fn handle_append(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<ApiResponse, ApiError> {
    let format = ResponseFormat::from_headers(&headers);

    // Parse request body based on Content-Type
    let request = AppendRequest::from_body(&headers, &body)?;

    let count = request.records.len();
    let bytes_count: usize = request
        .records
        .iter()
        .map(|rec| rec.key.len() + rec.value.len())
        .sum();

    let result = state.log.try_append(request.records).await?;

    if request.await_durable {
        state.log.flush().await?;
    }

    state.metrics.log_append_records_total.inc_by(count as u64);
    state
        .metrics
        .log_append_bytes_total
        .inc_by(bytes_count as u64);

    let response = AppendResponse::success(count as i32, result.start_sequence);
    Ok(to_api_response(response, format))
}

/// Handle GET /api/v1/log/scan
///
/// Returns response in format matching the `Accept` header.
/// Supports long-polling via `follow=true` and `timeout_ms` parameters.
pub async fn handle_scan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ScanParams>,
) -> Result<ApiResponse, ApiError> {
    let format = ResponseFormat::from_headers(&headers);
    let key = params.key();
    let range = params.seq_range();
    let limit = params.limit.unwrap_or(32);

    let entries = scan_entries(&state, key.clone(), range.clone(), limit).await?;

    // If we have entries or follow is disabled, return immediately
    if !entries.is_empty() || !params.follow.unwrap_or(false) {
        let values: Vec<Value> = entries
            .iter()
            .map(|e| Value {
                sequence: e.sequence,
                value: e.value.clone(),
            })
            .collect();
        let response = ScanResponse::success(key, values);
        return Ok(to_api_response(response, format));
    }

    // Long-poll: wait for new entries
    let timeout = Duration::from_millis(params.timeout_ms.unwrap_or(30000));
    let deadline = Instant::now() + timeout;
    let poll_interval = Duration::from_millis(100);

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            let response = ScanResponse::success(key, vec![]);
            return Ok(to_api_response(response, format));
        }
        tokio::time::sleep(poll_interval.min(remaining)).await;

        let entries = scan_entries(&state, key.clone(), range.clone(), limit).await?;
        if !entries.is_empty() {
            let values: Vec<Value> = entries
                .iter()
                .map(|e| Value {
                    sequence: e.sequence,
                    value: e.value.clone(),
                })
                .collect();
            let response = ScanResponse::success(key, values);
            return Ok(to_api_response(response, format));
        }
    }
}

/// Helper function to scan entries from the log.
async fn scan_entries(
    state: &AppState,
    key: Bytes,
    range: std::ops::Range<u64>,
    limit: usize,
) -> Result<Vec<crate::LogEntry>, ApiError> {
    let mut iter = state.log.scan(key, range).await?;
    let mut entries = Vec::new();
    while let Some(entry) = iter.next().await.map_err(ApiError::from)? {
        entries.push(entry);
        if entries.len() >= limit {
            break;
        }
    }

    let bytes_scanned: usize = entries
        .iter()
        .map(|entry| entry.key.len() + entry.value.len())
        .sum();
    state
        .metrics
        .log_records_scanned_total
        .inc_by(entries.len() as u64);
    state
        .metrics
        .log_bytes_scanned_total
        .inc_by(bytes_scanned as u64);
    Ok(entries)
}

/// Handle GET /api/v1/log/keys
///
/// Returns response in format matching the `Accept` header.
pub async fn handle_list_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ListKeysParams>,
) -> Result<ApiResponse, ApiError> {
    let format = ResponseFormat::from_headers(&headers);
    let segment_range = params.segment_range();
    let limit = params.limit.unwrap_or(32);

    let mut iter = state.log.list_keys(segment_range).await?;
    let mut keys: Vec<bytes::Bytes> = Vec::new();

    while let Some(log_key) = iter.next().await.map_err(ApiError::from)? {
        keys.push(log_key.key);
        if keys.len() >= limit {
            break;
        }
    }

    let response = KeysResponse::success(keys);
    Ok(to_api_response(response, format))
}

/// Handle GET /api/v1/log/segments
///
/// Returns response in format matching the `Accept` header.
pub async fn handle_list_segments(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ListSegmentsParams>,
) -> Result<ApiResponse, ApiError> {
    let format = ResponseFormat::from_headers(&headers);
    let seq_range = params.seq_range();

    let segments = state.log.list_segments(seq_range).await?;
    let segment_entries: Vec<Segment> = segments
        .into_iter()
        .map(|s| Segment {
            id: s.id,
            start_seq: s.start_seq,
            start_time_ms: s.start_time_ms,
        })
        .collect();

    let response = SegmentsResponse::success(segment_entries);
    Ok(to_api_response(response, format))
}

/// Handle GET /api/v1/log/count
///
/// Returns response in format matching the `Accept` header.
pub async fn handle_count(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<CountParams>,
) -> Result<ApiResponse, ApiError> {
    let format = ResponseFormat::from_headers(&headers);
    let key = params.key();
    let range = params.seq_range();

    let count = state.log.count(key, range).await?;

    let response = CountResponse::success(count);
    Ok(to_api_response(response, format))
}

/// Handle GET /metrics
pub async fn handle_metrics(State(state): State<AppState>) -> String {
    state.metrics.encode()
}

/// Handle GET /-/healthy
///
/// Returns 200 OK if the service is running.
pub async fn handle_healthy() -> (axum::http::StatusCode, &'static str) {
    (axum::http::StatusCode::OK, "OK")
}

/// Handle GET /-/ready
///
/// Returns 200 OK if the service is ready to serve requests.
/// Performs a lightweight storage check to verify the log backend is accessible.
pub async fn handle_ready(State(state): State<AppState>) -> (axum::http::StatusCode, &'static str) {
    // Verify storage is accessible with a lightweight read operation.
    // This reads the sequence block key, which verifies the storage backend
    // is responding without scanning or listing data.
    match state.log.check_storage().await {
        Ok(_) => (axum::http::StatusCode::OK, "OK"),
        Err(_) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Not Ready"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use axum::http::StatusCode;
    use common::StorageConfig;
    use common::storage::config::ObjectStoreConfig;
    use common::storage::testing::FailingObjectStore;
    use common::{StorageBuilder, storage::factory::BuiltDb};
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;

    fn test_config() -> Config {
        Config {
            storage: StorageConfig {
                path: "test-handlers".to_string(),
                object_store: ObjectStoreConfig::InMemory,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn should_return_ok_for_healthy() {
        // given/when
        let (status, body) = handle_healthy().await;

        // then
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "OK");
    }

    #[tokio::test]
    async fn should_return_ok_for_ready_when_log_accessible() {
        // given
        let log = Arc::new(LogDb::open(test_config()).await.unwrap());
        let metrics = Arc::new(Metrics::new());
        let state = AppState { log, metrics };

        // when
        let (status, body) = handle_ready(State(state)).await;

        // then
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "OK");
    }

    /// `check_storage` lists the data prefix on the object store directly —
    /// a small, unconditional I/O probe that bypasses slatedb's caches.
    /// Toggling `fail_list` on the FailingObjectStore should surface as a
    /// 503 from the ready endpoint.
    #[tokio::test]
    async fn should_return_503_for_ready_when_storage_fails() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let failing = FailingObjectStore::new(inner);
        let obj_store: Arc<dyn ObjectStore> = failing.clone();
        let BuiltDb {
            db,
            object_store,
            path,
            managed_cache: _,
        } = StorageBuilder::from_object_store("test-ready", obj_store)
            .await
            .unwrap()
            .build()
            .await
            .unwrap();

        let log = Arc::new(
            LogDb::new_with_object_store(db, object_store, path)
                .await
                .unwrap(),
        );
        let metrics = Arc::new(Metrics::new());
        let state = AppState { log, metrics };

        failing.set_fail_list(true);

        let (status, body) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle_ready(State(state)),
        )
        .await
        .expect("handle_ready hung past 5s; retry barrier is broken");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "Not Ready");
    }
}

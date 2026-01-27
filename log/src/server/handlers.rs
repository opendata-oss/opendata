//! HTTP route handlers for the log server.
//!
//! Per RFC 0004, handlers support both binary protobuf (`application/protobuf`)
//! and ProtoJSON (`application/protobuf+json`) formats.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;

use super::error::ApiError;
use super::metrics::{AppendLabels, Metrics, OperationStatus, ScanLabels};
use super::request::{AppendRequest, CountParams, ListKeysParams, ListSegmentsParams, ScanParams};
use super::response::{
    ApiResponse, AppendResponse, CountResponse, IntoApiResponse, ListKeysResponse,
    ListSegmentsResponse, ResponseFormat, ScanResponse, ScanValue, SegmentEntry,
};
use crate::Log;
use crate::config::WriteOptions;
use crate::reader::LogRead;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub log: Arc<Log>,
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
    let options = WriteOptions {
        await_durable: request.await_durable,
    };

    match state
        .log
        .append_with_options(request.records, options)
        .await
    {
        Ok(result) => {
            state.metrics.log_append_records_total.inc_by(count as u64);
            state
                .metrics
                .log_append_requests_total
                .get_or_create(&AppendLabels {
                    status: OperationStatus::Success,
                })
                .inc();

            let response = AppendResponse::success(result.records_appended, result.start_sequence);
            Ok(response.into_api_response(format))
        }
        Err(e) => {
            state
                .metrics
                .log_append_requests_total
                .get_or_create(&AppendLabels {
                    status: OperationStatus::Error,
                })
                .inc();

            Err(ApiError::from(e))
        }
    }
}

/// Handle GET /api/v1/log/scan
///
/// Returns response in format matching the `Accept` header.
pub async fn handle_scan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<ScanParams>,
) -> Result<ApiResponse, ApiError> {
    let format = ResponseFormat::from_headers(&headers);
    let key = params.key();
    let range = params.seq_range();
    let limit = params.limit.unwrap_or(1000);

    match state.log.scan(key.clone(), range).await {
        Ok(mut iter) => {
            let mut entries = Vec::new();
            while let Some(entry) = iter.next().await.map_err(ApiError::from)? {
                entries.push(entry);
                if entries.len() >= limit {
                    break;
                }
            }

            state
                .metrics
                .log_scan_requests_total
                .get_or_create(&ScanLabels {
                    status: OperationStatus::Success,
                })
                .inc();

            let scan_values: Vec<ScanValue> =
                entries.iter().map(ScanValue::from_log_entry).collect();
            let response = ScanResponse::success(key, scan_values);
            Ok(response.into_api_response(format))
        }
        Err(e) => {
            state
                .metrics
                .log_scan_requests_total
                .get_or_create(&ScanLabels {
                    status: OperationStatus::Error,
                })
                .inc();

            Err(ApiError::from(e))
        }
    }
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
    let limit = params.limit.unwrap_or(1000);

    let mut iter = state.log.list_keys(segment_range).await?;
    let mut keys: Vec<bytes::Bytes> = Vec::new();

    while let Some(log_key) = iter.next().await.map_err(ApiError::from)? {
        keys.push(log_key.key);
        if keys.len() >= limit {
            break;
        }
    }

    let response = ListKeysResponse::success(keys);
    Ok(response.into_api_response(format))
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
    let segment_entries: Vec<SegmentEntry> = segments
        .into_iter()
        .map(|s| SegmentEntry {
            id: s.id,
            start_seq: s.start_seq,
            start_time_ms: s.start_time_ms,
        })
        .collect();

    let response = ListSegmentsResponse::success(segment_entries);
    Ok(response.into_api_response(format))
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
    Ok(response.into_api_response(format))
}

/// Handle GET /metrics
pub async fn handle_metrics(State(state): State<AppState>) -> String {
    state.metrics.encode()
}

//! HTTP route handlers for the log server.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::Json;
use base64::Engine;

use super::error::ApiError;
use super::metrics::{AppendLabels, Metrics, OperationStatus, ScanLabels};
use super::request::{AppendBody, CountParams, ListKeysParams, ScanParams};
use super::response::{
    AppendResponse, CountResponse, KeyEntry, ListKeysResponse, ScanEntry, ScanResponse,
};
use crate::config::WriteOptions;
use crate::{Log, LogRead};

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub log: Arc<Log>,
    pub metrics: Arc<Metrics>,
}

/// Handle POST /api/v1/log/append
pub async fn handle_append(
    State(state): State<AppState>,
    Json(body): Json<AppendBody>,
) -> Result<Json<AppendResponse>, ApiError> {
    let records = body.decode_records()?;
    let count = records.len();

    let options = WriteOptions {
        await_durable: body.await_durable,
    };

    match state.log.append_with_options(records, options).await {
        Ok(()) => {
            // Record metrics
            state.metrics.log_append_records_total.inc_by(count as u64);
            state
                .metrics
                .log_append_requests_total
                .get_or_create(&AppendLabels {
                    status: OperationStatus::Success,
                })
                .inc();

            Ok(Json(AppendResponse::success(count)))
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
pub async fn handle_scan(
    State(state): State<AppState>,
    Query(params): Query<ScanParams>,
) -> Result<Json<ScanResponse>, ApiError> {
    let key = params.decode_key()?;
    let range = params.seq_range();
    let limit = params.limit.unwrap_or(1000);

    match state.log.scan(key, range).await {
        Ok(mut iter) => {
            let mut entries = Vec::new();
            while let Some(entry) = iter.next().await.map_err(|e| ApiError::from(e))? {
                entries.push(ScanEntry::from(&entry));
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

            Ok(Json(ScanResponse::success(entries)))
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
pub async fn handle_list_keys(
    State(state): State<AppState>,
    Query(params): Query<ListKeysParams>,
) -> Result<Json<ListKeysResponse>, ApiError> {
    let range = params.seq_range();
    let limit = params.limit.unwrap_or(1000);

    let mut iter = state.log.list(range).await?;
    let mut keys = Vec::new();

    while let Some(log_key) = iter.next().await.map_err(|e| ApiError::from(e))? {
        keys.push(KeyEntry {
            key: base64::engine::general_purpose::STANDARD.encode(&log_key.key),
        });
        if keys.len() >= limit {
            break;
        }
    }

    Ok(Json(ListKeysResponse::success(keys)))
}

/// Handle GET /api/v1/log/count
pub async fn handle_count(
    State(state): State<AppState>,
    Query(params): Query<CountParams>,
) -> Result<Json<CountResponse>, ApiError> {
    let key = params.decode_key()?;
    let range = params.seq_range();

    let count = state.log.count(key, range).await?;

    Ok(Json(CountResponse::success(count)))
}

/// Handle GET /metrics
pub async fn handle_metrics(State(state): State<AppState>) -> String {
    state.metrics.encode()
}

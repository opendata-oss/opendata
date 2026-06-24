//! HTTP route handlers for the log server.
//!
//! Per RFC 0004, handlers support both binary protobuf (`application/protobuf`)
//! and ProtoJSON (`application/protobuf+json`) formats.

use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::HeaderMap;

use super::error::ApiError;
use super::metrics::Metrics;
use super::proto::{
    AppendResponse, CountResponse, KeysResponse, ScanResponse, Segment as ProtoSegment,
    SegmentsResponse, Value,
};
use super::request::{AppendRequest, CountParams, ListKeysParams, ListSegmentsParams, ScanParams};
use super::response::{ApiResponse, ResponseFormat, to_api_response};
use crate::error::AppendError;
use crate::reader::LogRead;
use crate::{
    AppendOutput, AppendResult, LogDb, LogDbReader, LogIterator, LogKeyIterator, Record, Segment,
    SegmentId, Sequence,
};

/// Storage backend behind the HTTP server.
///
/// A full read-write [`LogDb`] exposes every route; a [`LogDbReader`] backs a
/// read-only gateway where the append route is not registered (see
/// [`LogServer`](super::http::LogServer)). Read methods delegate to the
/// [`LogRead`] implementation shared by both variants; write methods are only
/// meaningful on the read-write variant.
#[derive(Clone)]
pub enum LogBackend {
    /// Full read-write log.
    ReadWrite(Arc<LogDb>),
    /// Read-only view that periodically discovers data written elsewhere.
    ReadOnly(Arc<LogDbReader>),
}

impl LogBackend {
    /// Appends records to the log.
    ///
    /// Errors on a read-only backend. The append route is not registered in
    /// read-only mode, so this arm is a defensive guard rather than a reachable
    /// code path.
    pub async fn try_append(&self, records: Vec<Record>) -> AppendResult<AppendOutput> {
        match self {
            Self::ReadWrite(log) => log.try_append(records).await,
            Self::ReadOnly(_) => Err(AppendError::Storage("log gateway is read-only".to_string())),
        }
    }

    /// Flushes pending writes. A no-op on a read-only backend.
    pub async fn flush(&self) -> crate::Result<()> {
        match self {
            Self::ReadWrite(log) => log.flush().await,
            Self::ReadOnly(_) => Ok(()),
        }
    }

    /// Verifies the storage backend is reachable (readiness probe).
    pub async fn check_storage(&self) -> crate::Result<()> {
        match self {
            Self::ReadWrite(log) => log.check_storage().await,
            Self::ReadOnly(reader) => reader.check_storage().await,
        }
    }

    /// Scans entries for a key within a sequence range.
    pub async fn scan(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> crate::Result<LogIterator> {
        match self {
            Self::ReadWrite(log) => log.scan(key, seq_range).await,
            Self::ReadOnly(reader) => reader.scan(key, seq_range).await,
        }
    }

    /// Counts entries for a key within a sequence range.
    pub async fn count(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> crate::Result<u64> {
        match self {
            Self::ReadWrite(log) => log.count(key, seq_range).await,
            Self::ReadOnly(reader) => reader.count(key, seq_range).await,
        }
    }

    /// Lists distinct keys within a segment range.
    pub async fn list_keys(
        &self,
        segment_range: impl RangeBounds<SegmentId> + Send,
    ) -> crate::Result<LogKeyIterator> {
        match self {
            Self::ReadWrite(log) => log.list_keys(segment_range).await,
            Self::ReadOnly(reader) => reader.list_keys(segment_range).await,
        }
    }

    /// Lists segments overlapping a sequence range.
    pub async fn list_segments(
        &self,
        seq_range: impl RangeBounds<Sequence> + Send,
    ) -> crate::Result<Vec<Segment>> {
        match self {
            Self::ReadWrite(log) => log.list_segments(seq_range).await,
            Self::ReadOnly(reader) => reader.list_segments(seq_range).await,
        }
    }
}

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub log: LogBackend,
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
    let mut range = params.seq_range();
    let limit = params.limit.unwrap_or(32);

    let (entries, mut next_sequence) =
        scan_entries(&state, key.clone(), range.clone(), limit).await?;

    // If we have entries or follow is disabled, return immediately.
    if !entries.is_empty() || !params.follow.unwrap_or(false) {
        return Ok(to_api_response(
            scan_response(key, &entries, next_sequence),
            format,
        ));
    }

    // Empty and following: advance the cursor past the observed-empty range so
    // each poll only scans newly-arrived data, then long-poll for it.
    range.start = next_sequence;
    let timeout = Duration::from_millis(params.timeout_ms.unwrap_or(30000));
    let deadline = Instant::now() + timeout;
    let poll_interval = Duration::from_millis(100);

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(to_api_response(
                scan_response(key, &[], next_sequence),
                format,
            ));
        }
        tokio::time::sleep(poll_interval.min(remaining)).await;

        let (entries, ns) = scan_entries(&state, key.clone(), range.clone(), limit).await?;
        next_sequence = ns;
        if !entries.is_empty() {
            return Ok(to_api_response(
                scan_response(key, &entries, next_sequence),
                format,
            ));
        }
        range.start = ns;
    }
}

/// Builds a successful scan response from borrowed entries.
fn scan_response(key: Bytes, entries: &[crate::LogEntry], next_sequence: u64) -> ScanResponse {
    let values: Vec<Value> = entries
        .iter()
        .map(|e| Value {
            sequence: e.sequence,
            value: e.value.clone(),
        })
        .collect();
    ScanResponse::success(key, values, next_sequence)
}

/// Helper function to scan entries from the log.
///
/// Returns the entries read plus the iterator's `next_sequence` — the exclusive
/// global sequence to resume from (see RFC 0007). When the scan drains without
/// hitting `limit`, this is lifted to the observed frontier so callers skip the
/// empty tail; when truncated by `limit`, it is one past the last entry.
async fn scan_entries(
    state: &AppState,
    key: Bytes,
    range: std::ops::Range<u64>,
    limit: usize,
) -> Result<(Vec<crate::LogEntry>, u64), ApiError> {
    let mut iter = state.log.scan(key, range).await?;
    let mut entries = Vec::new();
    while let Some(entry) = iter.next().await.map_err(ApiError::from)? {
        entries.push(entry);
        if entries.len() >= limit {
            break;
        }
    }
    let next_sequence = iter.next_sequence();

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
    Ok((entries, next_sequence))
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
    let segment_entries: Vec<ProtoSegment> = segments
        .into_iter()
        .map(|s| ProtoSegment {
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
    use common::{MergeRecordOp, PutRecordOp, StorageConfig};

    fn test_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
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
        let state = AppState {
            log: LogBackend::ReadWrite(log),
            metrics,
        };

        // when
        let (status, body) = handle_ready(State(state)).await;

        // then
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "OK");
    }

    #[tokio::test]
    async fn should_return_503_for_ready_when_storage_fails() {
        use async_trait::async_trait;
        use bytes::Bytes;
        use common::storage::{RecordOp, StorageSnapshot};
        use common::{BytesRange, Record, Storage, StorageIterator, StorageRead};
        use std::sync::atomic::{AtomicBool, Ordering};

        // A mock storage that can be configured to fail after initialization
        struct ToggleFailStorage {
            should_fail: AtomicBool,
        }

        impl ToggleFailStorage {
            fn new() -> Self {
                Self {
                    should_fail: AtomicBool::new(false),
                }
            }

            fn set_failing(&self, fail: bool) {
                self.should_fail.store(fail, Ordering::SeqCst);
            }

            fn check_failure(&self) -> common::StorageResult<()> {
                if self.should_fail.load(Ordering::SeqCst) {
                    Err(common::StorageError::Storage("storage unavailable".into()))
                } else {
                    Ok(())
                }
            }
        }

        struct EmptyIterator;

        #[async_trait]
        impl StorageIterator for EmptyIterator {
            async fn next(&mut self) -> common::StorageResult<Option<Record>> {
                Ok(None)
            }
        }

        #[async_trait]
        impl StorageRead for ToggleFailStorage {
            async fn get(&self, _key: Bytes) -> common::StorageResult<Option<Record>> {
                self.check_failure()?;
                Ok(None)
            }

            async fn scan_iter(
                &self,
                _range: BytesRange,
            ) -> common::StorageResult<Box<dyn StorageIterator + Send + 'static>> {
                self.check_failure()?;
                Ok(Box::new(EmptyIterator))
            }
        }

        impl StorageSnapshot for ToggleFailStorage {}

        #[async_trait]
        impl Storage for ToggleFailStorage {
            async fn apply_with_options(
                &self,
                _ops: Vec<RecordOp>,
                _options: common::WriteOptions,
            ) -> common::StorageResult<common::storage::WriteResult> {
                self.check_failure()?;
                Ok(common::storage::WriteResult { seqnum: 0 })
            }

            async fn put_with_options(
                &self,
                _records: Vec<PutRecordOp>,
                _options: common::WriteOptions,
            ) -> common::StorageResult<common::storage::WriteResult> {
                self.check_failure()?;
                Ok(common::storage::WriteResult { seqnum: 0 })
            }

            async fn merge_with_options(
                &self,
                _records: Vec<MergeRecordOp>,
                _options: common::WriteOptions,
            ) -> common::StorageResult<common::storage::WriteResult> {
                self.check_failure()?;
                Ok(common::storage::WriteResult { seqnum: 0 })
            }

            async fn snapshot(&self) -> common::StorageResult<Arc<dyn StorageSnapshot>> {
                self.check_failure()?;
                Ok(Arc::new(ToggleFailStorage::new()))
            }

            fn subscribe_durable(&self) -> tokio::sync::watch::Receiver<u64> {
                let (_, rx) = tokio::sync::watch::channel(0);
                rx
            }

            async fn flush(&self) -> common::StorageResult<()> {
                self.check_failure()
            }

            async fn create_checkpoint(&self) -> common::StorageResult<common::CheckpointInfo> {
                self.check_failure()?;
                Err(common::StorageError::Storage(
                    "checkpoints not supported".to_string(),
                ))
            }
        }

        // given - a log backed by configurable storage
        let storage = Arc::new(ToggleFailStorage::new());
        let log = Arc::new(LogDb::new(storage.clone()).await.unwrap());
        let metrics = Arc::new(Metrics::new());
        let state = AppState {
            log: LogBackend::ReadWrite(log),
            metrics,
        };

        // Configure storage to fail after initialization
        storage.set_failing(true);

        // when
        let (status, body) = handle_ready(State(state)).await;

        // then
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "Not Ready");
    }
}

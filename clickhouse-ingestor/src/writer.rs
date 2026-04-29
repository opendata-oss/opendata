//! ClickHouse writer.
//!
//! Sync inserts only. Each chunk goes out as one HTTP `INSERT INTO ...
//! FORMAT JSONEachRow` request with `async_insert=0`,
//! `insert_deduplication_token=<chunk token>`, and an optional
//! `insert_quorum`. JSONEachRow keeps `Map(LowCardinality(String), String)`
//! columns straightforward; switching to `RowBinaryWithNamesAndTypes` is a
//! future optimization (see RFC 0003 open questions).
//!
//! Errors are classified into retryable vs non-retryable so the runtime
//! can apply backoff for retryable failures and halt for the rest.

use std::time::Duration;

use serde_json::Value as JsonValue;
use thiserror::Error;
use tracing::{debug, warn};

use crate::adapter::InsertChunk;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterErrorClass {
    /// Network glitch, timeout, 5xx, 429. Caller should back off and retry
    /// the same chunk with the same token.
    Retryable,
    /// Schema mismatch, 4xx other than 429. The runtime should halt and
    /// surface the error to an operator.
    NonRetryable,
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("clickhouse insert failed (retryable): {message}")]
    Retryable { message: String },
    #[error("clickhouse insert failed (non-retryable): {message}")]
    NonRetryable { message: String },
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl WriterError {
    pub fn class(&self) -> WriterErrorClass {
        match self {
            WriterError::Retryable { .. } => WriterErrorClass::Retryable,
            WriterError::NonRetryable { .. } | WriterError::Serialization(_) => {
                WriterErrorClass::NonRetryable
            }
        }
    }
}

/// Configuration for the writer.
#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub endpoint: String,
    pub user: String,
    pub password: String,
    /// Per-chunk request timeout. Applied via `clickhouse::Client`'s
    /// underlying HTTP client; exceeded → retryable.
    pub request_timeout: Duration,
    /// Maximum retry attempts per chunk. After exhaustion the writer
    /// surfaces a non-retryable error.
    pub max_attempts: u32,
    /// Initial backoff between retry attempts; doubles each attempt.
    pub initial_backoff: Duration,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:8123".into(),
            user: "default".into(),
            password: String::new(),
            request_timeout: Duration::from_secs(30),
            max_attempts: 6,
            initial_backoff: Duration::from_millis(100),
        }
    }
}

/// ClickHouse insert writer.
///
/// Drives `reqwest` directly so the ingestor can exchange a small,
/// classified error type with the runtime. We don't pull in the official
/// `clickhouse` crate because the alpha uses generic `InsertChunk` rows
/// rather than typed Rust structs, which is the workflow that crate is
/// optimized for.
#[derive(Clone)]
pub struct ClickHouseWriter {
    config: WriterConfig,
}

impl ClickHouseWriter {
    pub fn new(config: WriterConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &WriterConfig {
        &self.config
    }

    /// Build a fresh reqwest client per call. The runtime's testcontainers
    /// flow surfaced silent insert drops when a pooled HTTP/1.1
    /// connection got reused across tokio tasks; per-call clients
    /// (with HTTP/1.1, pool disabled) keep the wire shape predictable.
    /// We can switch to a shared client once the underlying issue is
    /// understood.
    fn http_client(&self) -> Result<reqwest::Client, WriterError> {
        reqwest::Client::builder()
            .timeout(self.config.request_timeout)
            .http1_only()
            .pool_max_idle_per_host(0)
            .build()
            .map_err(|e| WriterError::NonRetryable {
                message: format!("reqwest client: {e}"),
            })
    }

    /// Execute every chunk in order. Retryable failures back off and
    /// retry the same chunk; non-retryable failures halt the run.
    pub async fn execute_all(&self, chunks: &[InsertChunk]) -> Result<(), WriterError> {
        for chunk in chunks {
            self.execute_chunk(chunk).await?;
        }
        Ok(())
    }

    /// Execute a single chunk with classified retry. Same-token replays
    /// are dedup-safe at the table level.
    pub async fn execute_chunk(&self, chunk: &InsertChunk) -> Result<(), WriterError> {
        let body = render_jsoneachrow(chunk)?;
        // SQL is the bare INSERT (no SETTINGS clause). Settings ride
        // as URL query params so the body shape exactly matches the
        // simple-insert path that ClickHouse always accepts.
        let sql = render_insert_sql_clean(chunk);

        let mut attempt: u32 = 0;
        let mut backoff = self.config.initial_backoff;
        loop {
            attempt += 1;
            match self.execute_once(&sql, &body, chunk).await {
                Ok(()) => {
                    debug!(
                        attempt,
                        token = %chunk.idempotency_token,
                        rows = chunk.rows_count(),
                        "clickhouse insert succeeded",
                    );
                    return Ok(());
                }
                Err(err) => match err.class() {
                    WriterErrorClass::Retryable if attempt < self.config.max_attempts => {
                        metrics::counter!(
                            crate::metrics::RETRY_COUNT_TOTAL,
                            "reason" => "retryable",
                        )
                        .increment(1);
                        warn!(
                            attempt,
                            token = %chunk.idempotency_token,
                            "clickhouse insert failed retryably, backing off: {err}",
                        );
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                    }
                    WriterErrorClass::Retryable => {
                        return Err(WriterError::NonRetryable {
                            message: format!(
                                "retry budget ({}) exhausted: {err}",
                                self.config.max_attempts
                            ),
                        });
                    }
                    WriterErrorClass::NonRetryable => return Err(err),
                },
            }
        }
    }

    async fn execute_once(
        &self,
        sql_clean: &str,
        body: &str,
        chunk: &InsertChunk,
    ) -> Result<(), WriterError> {
        // SQL+data combined in body (no SETTINGS clause); per-request
        // settings ride as URL query params. This matches the curl
        // shape ClickHouse 23.3 accepts most consistently.
        let mut combined = String::with_capacity(sql_clean.len() + 2 + body.len());
        combined.push_str(sql_clean);
        combined.push('\n');
        combined.push_str(body);
        if let Ok(path) = std::env::var("INGESTOR_DUMP_INSERT_BODY") {
            let _ = std::fs::write(&path, combined.as_bytes());
            tracing::debug!(path = %path, "dumped insert body");
        }

        let url = build_insert_url(&self.config.endpoint, chunk, self.config.request_timeout);
        let body_bytes = combined.into_bytes();
        let body_len = body_bytes.len();
        tracing::debug!(
            url = %url,
            body_len,
            "issuing clickhouse insert",
        );
        let http = self.http_client()?;
        let mut req = http
            .post(&url)
            .header(reqwest::header::CONTENT_LENGTH, body_len.to_string())
            .body(body_bytes);
        if !self.config.user.is_empty() {
            req = req.header("X-ClickHouse-User", &self.config.user);
        }
        if !self.config.password.is_empty() {
            req = req.header("X-ClickHouse-Key", &self.config.password);
        }
        let resp = req.send().await.map_err(|e| classify_reqwest(&e))?;
        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            tracing::debug!(
                token = %chunk.idempotency_token,
                rows = chunk.rows_count(),
                response_bytes = resp_body.len(),
                "insert ok"
            );
            return Ok(());
        }
        Err(classify_status(status.as_u16(), &resp_body))
    }

    /// Run a SQL statement (DDL, SELECT, or INSERT-with-data-in-body).
    ///
    /// We POST the SQL as the body rather than via the `?query=`
    /// parameter so reqwest sets a Content-Length header naturally —
    /// older ClickHouse builds reject chunked POSTs with HTTP 411, and
    /// GET implies readonly so it can't run DDL.
    pub async fn execute_statement(&self, sql: &str) -> Result<String, WriterError> {
        let url = self.config.endpoint.trim_end_matches('/').to_string();
        let body_bytes = sql.as_bytes().to_vec();
        let body_len = body_bytes.len();
        let http = self.http_client()?;
        let mut req = http
            .post(url)
            .header(reqwest::header::CONTENT_LENGTH, body_len.to_string())
            .body(body_bytes);
        if !self.config.user.is_empty() {
            req = req.header("X-ClickHouse-User", &self.config.user);
        }
        if !self.config.password.is_empty() {
            req = req.header("X-ClickHouse-Key", &self.config.password);
        }
        let resp = req.send().await.map_err(|e| classify_reqwest(&e))?;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if status.is_success() {
            Ok(body)
        } else {
            Err(classify_status(status.as_u16(), &body))
        }
    }
}

fn classify_reqwest(err: &reqwest::Error) -> WriterError {
    if err.is_timeout() || err.is_connect() {
        return WriterError::Retryable {
            message: format!("network/timeout: {err}"),
        };
    }
    if err.is_status() {
        // Status-class errors are handled by `classify_status` for the
        // body inspection; this branch is the fall-through.
        return WriterError::Retryable {
            message: format!("http status: {err}"),
        };
    }
    WriterError::Retryable {
        message: format!("request error: {err}"),
    }
}

fn classify_status(status: u16, body: &str) -> WriterError {
    if status == 429 || (500..600).contains(&status) {
        return WriterError::Retryable {
            message: format!("status {status}: {body}"),
        };
    }
    WriterError::NonRetryable {
        message: format!("status {status}: {body}"),
    }
}

fn render_insert_sql_clean(chunk: &InsertChunk) -> String {
    // No SETTINGS clause, no column list. JSONEachRow keys rows by
    // field name. This is the universally-accepted INSERT shape;
    // settings ride as URL query params.
    format!(
        "INSERT INTO {}.{} FORMAT JSONEachRow",
        chunk.database, chunk.table
    )
}

fn build_insert_url(endpoint: &str, chunk: &InsertChunk, timeout: Duration) -> String {
    use std::fmt::Write;
    let mut url = String::new();
    url.push_str(endpoint.trim_end_matches('/'));
    url.push_str("/?async_insert=0");
    url.push_str("&date_time_input_format=best_effort");
    if chunk.settings.apply_deduplication_token {
        let _ = write!(
            &mut url,
            "&insert_deduplication_token={}",
            urlencoding::encode(&chunk.settings.insert_deduplication_token),
        );
    }
    if let Some(quorum) = &chunk.settings.insert_quorum {
        let _ = write!(&mut url, "&insert_quorum={}", urlencoding::encode(quorum));
    }
    if timeout > Duration::ZERO {
        let _ = write!(&mut url, "&max_execution_time={}", timeout.as_secs());
    }
    url
}

/// Serialize the chunk's rows into ndjson for the `JSONEachRow` body.
fn render_jsoneachrow(chunk: &InsertChunk) -> Result<String, WriterError> {
    let mut out = String::with_capacity(chunk.rows.len() * 256);
    for row in &chunk.rows {
        if row.len() != chunk.columns.len() {
            return Err(WriterError::Serialization(format!(
                "row has {} values but {} columns are expected",
                row.len(),
                chunk.columns.len()
            )));
        }
        let mut obj = serde_json::Map::with_capacity(row.len());
        for (col, value) in chunk.columns.iter().zip(row.iter()) {
            obj.insert((*col).to_string(), value.to_json());
        }
        let line = serde_json::to_string(&JsonValue::Object(obj))
            .map_err(|e| WriterError::Serialization(e.to_string()))?;
        out.push_str(&line);
        out.push('\n');
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::{ClickHouseSettings, RowValue};

    fn chunk(rows: Vec<Vec<RowValue>>) -> InsertChunk {
        InsertChunk {
            database: "responsive".into(),
            table: "logs".into(),
            columns: vec!["a", "b"],
            rows,
            settings: ClickHouseSettings {
                insert_quorum: Some("auto".into()),
                insert_deduplication_token: "tok".into(),
                apply_deduplication_token: true,
            },
            idempotency_token: "tok".into(),
            chunk_index: 0,
            observability_labels: vec![],
        }
    }

    #[test]
    fn insert_sql_is_well_formed() {
        let c = chunk(vec![]);
        let sql = render_insert_sql_clean(&c);
        assert_eq!(sql, "INSERT INTO responsive.logs FORMAT JSONEachRow");
    }

    #[test]
    fn jsoneachrow_emits_one_object_per_row() {
        let c = chunk(vec![
            vec![RowValue::String("x".into()), RowValue::UInt64(1)],
            vec![RowValue::String("y".into()), RowValue::UInt64(2)],
        ]);
        let body = render_jsoneachrow(&c).expect("render");
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 2);
        let row0: JsonValue = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(row0["a"], "x");
        assert_eq!(row0["b"], 1);
        let row1: JsonValue = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(row1["a"], "y");
        assert_eq!(row1["b"], 2);
    }

    #[test]
    fn jsoneachrow_rejects_row_column_mismatch() {
        let mut c = chunk(vec![vec![RowValue::String("x".into())]]);
        c.columns = vec!["a", "b"];
        let err = render_jsoneachrow(&c).unwrap_err();
        match err {
            WriterError::Serialization(msg) => assert!(msg.contains("row has 1 values")),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn classify_status_503_is_retryable() {
        let err = classify_status(503, "service unavailable");
        assert_eq!(err.class(), WriterErrorClass::Retryable);
    }

    #[test]
    fn classify_status_429_is_retryable() {
        let err = classify_status(429, "too many requests");
        assert_eq!(err.class(), WriterErrorClass::Retryable);
    }

    #[test]
    fn classify_status_400_is_non_retryable() {
        let err = classify_status(400, "bad request");
        assert_eq!(err.class(), WriterErrorClass::NonRetryable);
    }

    #[test]
    fn insert_url_carries_settings_as_query_params() {
        let c = chunk(vec![]);
        let url = build_insert_url("http://localhost:8123", &c, Duration::from_secs(5));
        assert!(url.starts_with("http://localhost:8123/?async_insert=0"));
        assert!(url.contains("date_time_input_format=best_effort"));
        assert!(url.contains("insert_deduplication_token=tok"));
        assert!(url.contains("insert_quorum=auto"));
        assert!(url.contains("max_execution_time=5"));
    }

    #[test]
    fn insert_url_omits_token_when_disabled() {
        let mut c = chunk(vec![]);
        c.settings.apply_deduplication_token = false;
        let url = build_insert_url("http://localhost:8123", &c, Duration::from_secs(5));
        assert!(!url.contains("insert_deduplication_token"));
    }
}

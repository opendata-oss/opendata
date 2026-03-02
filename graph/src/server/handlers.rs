//! HTTP route handlers for the graph server.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use serde::{Deserialize, Serialize};

use super::error::ApiError;
use crate::db::GraphDb;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<GraphDb>,
}

/// Query request body.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    /// The query string (GQL by default).
    pub query: String,
}

/// Query response body.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

/// Handle POST /api/v1/graph/query
#[cfg(feature = "gql")]
pub async fn handle_query(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let result = state.db.execute(&request.query)?;

    let columns = result.columns.clone();
    let rows = result
        .iter()
        .map(|row| row.iter().map(grafeo_value_to_json).collect())
        .collect();

    Ok(Json(QueryResponse { columns, rows }))
}

/// Handle GET /-/healthy
pub async fn handle_healthy() -> &'static str {
    "OK"
}

/// Handle GET /-/ready
pub async fn handle_ready() -> &'static str {
    "OK"
}

/// Convert a grafeo Value to a serde_json::Value.
#[cfg(feature = "gql")]
fn grafeo_value_to_json(v: &grafeo_common::types::Value) -> serde_json::Value {
    use grafeo_common::types::Value;
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int64(i) => serde_json::json!(i),
        Value::Float64(f) => serde_json::json!(f),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::Bytes(b) => serde_json::json!(hex_encode(b)),
        Value::Timestamp(ts) => serde_json::json!(ts.to_string()),
        Value::List(items) => {
            serde_json::Value::Array(items.iter().map(grafeo_value_to_json).collect())
        }
        Value::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| (k.to_string(), grafeo_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        Value::Vector(v) => {
            serde_json::Value::Array(v.iter().map(|f| serde_json::json!(f)).collect())
        }
    }
}

#[cfg(feature = "gql")]
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").unwrap();
    }
    s
}

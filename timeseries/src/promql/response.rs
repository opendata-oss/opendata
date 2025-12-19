use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Error response matching Prometheus API format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub status: String, // "error"
    #[serde(rename = "errorType")]
    pub error_type: String,
    pub error: String,
}

impl ErrorResponse {
    pub fn new(error_type: impl Into<String>, error: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            error_type: error_type.into(),
            error: error.into(),
        }
    }

    pub fn bad_data(error: impl Into<String>) -> Self {
        Self::new("bad_data", error)
    }

    pub fn execution(error: impl Into<String>) -> Self {
        Self::new("execution", error)
    }

    pub fn internal(error: impl Into<String>) -> Self {
        Self::new("internal", error)
    }

    pub fn timeout(error: impl Into<String>) -> Self {
        Self::new("timeout", error)
    }
}

/// Response for /api/v1/query (instant query)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub status: String, // "success" or "error"
    pub data: Option<QueryResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    #[serde(rename = "resultType")]
    pub result_type: String, // "vector", "scalar", "matrix", "string"
    pub result: serde_json::Value,
}

/// Response for /api/v1/query_range (range query)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRangeResponse {
    pub status: String, // "success" or "error"
    pub data: Option<QueryRangeResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRangeResult {
    #[serde(rename = "resultType")]
    pub result_type: String, // typically "matrix"
    pub result: Vec<MatrixSeries>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixSeries {
    pub metric: HashMap<String, String>,
    pub values: Vec<(f64, String)>, // (timestamp, value)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSeries {
    pub metric: HashMap<String, String>,
    pub value: (f64, String), // (timestamp, value)
}

/// Response for /api/v1/series (series listing)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesResponse {
    pub status: String, // "success" or "error"
    pub data: Option<Vec<HashMap<String, String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

/// Response for /api/v1/labels (label names)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelsResponse {
    pub status: String, // "success" or "error"
    pub data: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

/// Response for /api/v1/label/{name}/values (label values)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelValuesResponse {
    pub status: String, // "success" or "error"
    pub data: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

/// Response for /api/v1/metadata (metric metadata)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataResponse {
    pub status: String, // "success" or "error"
    pub data: Option<HashMap<String, Vec<MetricMetadata>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricMetadata {
    #[serde(rename = "type")]
    pub metric_type: String, // "gauge", "counter", "histogram", "summary"
    pub help: String,
    pub unit: String,
}

/// Response for /federate (federation endpoint)
#[derive(Debug, Clone)]
pub struct FederateResponse {
    pub content_type: String, // "text/plain; version=0.0.4"
    pub body: Vec<u8>,        // Prometheus text format
}

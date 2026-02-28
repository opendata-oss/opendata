use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::QueryError;
use crate::model::{self, QueryValue, RangeSample};

/// Convert a `QueryError` into a Prometheus-style `ErrorResponse`.
pub(crate) fn query_error_response(err: QueryError) -> ErrorResponse {
    match err {
        QueryError::InvalidQuery(msg) => ErrorResponse::bad_data(msg),
        QueryError::Execution(msg) => ErrorResponse::execution(msg),
        QueryError::Timeout => ErrorResponse::timeout("query timed out"),
    }
}

/// Convert an instant query result into a Prometheus `QueryResponse`.
pub(crate) fn query_value_to_response(
    result: Result<QueryValue, QueryError>,
) -> QueryResponse {
    match result {
        Ok(QueryValue::Scalar {
            timestamp_ms,
            value,
        }) => {
            let scalar_result = (timestamp_ms as f64 / 1000.0, value.to_string());
            QueryResponse {
                status: "success".to_string(),
                data: Some(QueryResult {
                    result_type: "scalar".to_string(),
                    result: serde_json::to_value(scalar_result).unwrap(),
                }),
                error: None,
                error_type: None,
            }
        }
        Ok(QueryValue::Vector(samples)) => {
            let result: Vec<VectorSeries> = samples
                .into_iter()
                .map(|sample| VectorSeries {
                    metric: sample.labels.into(),
                    value: (
                        sample.timestamp_ms as f64 / 1000.0,
                        sample.value.to_string(),
                    ),
                })
                .collect();

            QueryResponse {
                status: "success".to_string(),
                data: Some(QueryResult {
                    result_type: "vector".to_string(),
                    result: serde_json::to_value(result).unwrap(),
                }),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            QueryResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

/// Convert a range query result into a Prometheus `QueryRangeResponse`.
pub(crate) fn range_result_to_response(
    result: Result<Vec<RangeSample>, QueryError>,
) -> QueryRangeResponse {
    match result {
        Ok(range_samples) => {
            let result: Vec<MatrixSeries> = range_samples
                .into_iter()
                .map(|rs| MatrixSeries {
                    metric: rs.labels.into(),
                    values: rs
                        .samples
                        .into_iter()
                        .map(|(ts_ms, value)| (ts_ms as f64 / 1000.0, value.to_string()))
                        .collect(),
                })
                .collect();

            QueryRangeResponse {
                status: "success".to_string(),
                data: Some(QueryRangeResult {
                    result_type: "matrix".to_string(),
                    result,
                }),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            QueryRangeResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

/// Convert a series listing result into a Prometheus `SeriesResponse`.
pub(crate) fn series_to_response(
    result: Result<Vec<crate::model::Labels>, QueryError>,
    limit: Option<usize>,
) -> SeriesResponse {
    match result {
        Ok(labels_vec) => {
            let mut data: Vec<HashMap<String, String>> =
                labels_vec.into_iter().map(|l| l.into()).collect();

            // Sort for consistent output (by metric name, then all labels)
            data.sort_by(|a, b| {
                let a_name = a.get("__name__").map(|s| s.as_str()).unwrap_or("");
                let b_name = b.get("__name__").map(|s| s.as_str()).unwrap_or("");
                a_name.cmp(b_name).then_with(|| {
                    let mut a_labels: Vec<_> = a.iter().collect();
                    let mut b_labels: Vec<_> = b.iter().collect();
                    a_labels.sort();
                    b_labels.sort();
                    a_labels.cmp(&b_labels)
                })
            });

            if let Some(limit) = limit {
                data.truncate(limit);
            }

            SeriesResponse {
                status: "success".to_string(),
                data: Some(data),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            SeriesResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

/// Convert a labels result into a Prometheus `LabelsResponse`.
pub(crate) fn labels_to_response(
    result: Result<Vec<String>, QueryError>,
    limit: Option<usize>,
) -> LabelsResponse {
    match result {
        Ok(mut data) => {
            if let Some(limit) = limit {
                data.truncate(limit);
            }
            LabelsResponse {
                status: "success".to_string(),
                data: Some(data),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            LabelsResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

/// Convert a label values result into a Prometheus `LabelValuesResponse`.
pub(crate) fn label_values_to_response(
    result: Result<Vec<String>, QueryError>,
    limit: Option<usize>,
) -> LabelValuesResponse {
    match result {
        Ok(mut data) => {
            if let Some(limit) = limit {
                data.truncate(limit);
            }
            LabelValuesResponse {
                status: "success".to_string(),
                data: Some(data),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            LabelValuesResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

/// Convert a metadata result into a Prometheus `MetadataResponse`.
pub(crate) fn metadata_to_response(
    result: Result<Vec<model::MetricMetadata>, QueryError>,
    limit: Option<usize>,
    limit_per_metric: Option<usize>,
) -> MetadataResponse {
    match result {
        Ok(entries) => {
            let mut data: HashMap<String, Vec<MetricMetadata>> = HashMap::new();
            for m in entries {
                data.entry(m.metric_name).or_default().push(MetricMetadata {
                    metric_type: m
                        .metric_type
                        .map(|t| t.as_str().to_string())
                        .unwrap_or_default(),
                    help: m.description.unwrap_or_default(),
                    unit: m.unit.unwrap_or_default(),
                });
            }

            if let Some(limit) = limit {
                data = data.into_iter().take(limit).collect();
            }

            if let Some(limit_per_metric) = limit_per_metric {
                for entries in data.values_mut() {
                    entries.truncate(limit_per_metric);
                }
            }

            MetadataResponse {
                status: "success".to_string(),
                data: Some(data),
                error: None,
                error_type: None,
            }
        }
        Err(e) => {
            let err = query_error_response(e);
            MetadataResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            }
        }
    }
}

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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<HashMap<String, Vec<MetricMetadata>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetricMetadata {
    #[serde(rename = "type")]
    pub metric_type: String, // "gauge", "counter", "histogram", "summary"
    pub help: String,
    pub unit: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that all response structs omit the `data` key when it is None.
    #[test]
    fn error_responses_omit_data_field() {
        let query = QueryResponse {
            status: "error".into(),
            data: None,
            error: Some("bad".into()),
            error_type: Some("bad_data".into()),
        };
        let json = serde_json::to_value(&query).unwrap();
        assert!(json.get("data").is_none(), "QueryResponse: {json}");

        let query_range = QueryRangeResponse {
            status: "error".into(),
            data: None,
            error: Some("bad".into()),
            error_type: Some("bad_data".into()),
        };
        let json = serde_json::to_value(&query_range).unwrap();
        assert!(json.get("data").is_none(), "QueryRangeResponse: {json}");

        let series = SeriesResponse {
            status: "error".into(),
            data: None,
            error: Some("bad".into()),
            error_type: Some("bad_data".into()),
        };
        let json = serde_json::to_value(&series).unwrap();
        assert!(json.get("data").is_none(), "SeriesResponse: {json}");

        let labels = LabelsResponse {
            status: "error".into(),
            data: None,
            error: Some("bad".into()),
            error_type: Some("bad_data".into()),
        };
        let json = serde_json::to_value(&labels).unwrap();
        assert!(json.get("data").is_none(), "LabelsResponse: {json}");

        let label_values = LabelValuesResponse {
            status: "error".into(),
            data: None,
            error: Some("bad".into()),
            error_type: Some("bad_data".into()),
        };
        let json = serde_json::to_value(&label_values).unwrap();
        assert!(json.get("data").is_none(), "LabelValuesResponse: {json}");

        let metadata = MetadataResponse {
            status: "error".into(),
            data: None,
            error: Some("bad".into()),
            error_type: Some("bad_data".into()),
        };
        let json = serde_json::to_value(&metadata).unwrap();
        assert!(json.get("data").is_none(), "MetadataResponse: {json}");
    }
}

/// Response for /federate (federation endpoint)
#[derive(Debug, Clone)]
pub struct FederateResponse {
    pub content_type: String, // "text/plain; version=0.0.4"
    pub body: Vec<u8>,        // Prometheus text format
}

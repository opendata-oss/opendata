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
    use crate::model::{InstantSample, Labels, Label, MetricType};

    // -----------------------------------------------------------------------
    // query_value_to_response
    // -----------------------------------------------------------------------

    #[test]
    fn query_value_scalar_response() {
        let result = Ok(QueryValue::Scalar {
            timestamp_ms: 4000_000,
            value: 42.5,
        });
        let resp = query_value_to_response(result);

        assert_eq!(resp.status, "success");
        let data = resp.data.unwrap();
        assert_eq!(data.result_type, "scalar");
        // Scalar result is a tuple [timestamp_secs, "value"]
        let arr: (f64, String) = serde_json::from_value(data.result).unwrap();
        assert_eq!(arr.0, 4000.0, "timestamp should be converted from ms to seconds");
        assert_eq!(arr.1, "42.5");
    }

    #[test]
    fn query_value_vector_response() {
        let samples = vec![InstantSample {
            labels: Labels::new(vec![
                Label::metric_name("up"),
                Label::new("job", "prometheus"),
            ]),
            timestamp_ms: 3900_000,
            value: 1.0,
        }];
        let result = Ok(QueryValue::Vector(samples));
        let resp = query_value_to_response(result);

        assert_eq!(resp.status, "success");
        let data = resp.data.unwrap();
        assert_eq!(data.result_type, "vector");
        let results: Vec<VectorSeries> = serde_json::from_value(data.result).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric.get("__name__").unwrap(), "up");
        assert_eq!(results[0].metric.get("job").unwrap(), "prometheus");
        assert_eq!(results[0].value.0, 3900.0);
        assert_eq!(results[0].value.1, "1");
    }

    #[test]
    fn query_value_error_response() {
        let resp = query_value_to_response(Err(QueryError::InvalidQuery("bad syntax".into())));
        assert_eq!(resp.status, "error");
        assert_eq!(resp.error_type.as_deref(), Some("bad_data"));
        assert!(resp.data.is_none());

        let resp = query_value_to_response(Err(QueryError::Execution("boom".into())));
        assert_eq!(resp.error_type.as_deref(), Some("execution"));

        let resp = query_value_to_response(Err(QueryError::Timeout));
        assert_eq!(resp.error_type.as_deref(), Some("timeout"));
    }

    // -----------------------------------------------------------------------
    // series_to_response — sorting + limit
    // -----------------------------------------------------------------------

    #[test]
    fn series_response_sorts_and_limits() {
        let labels_vec = vec![
            Labels::new(vec![Label::metric_name("zz_metric"), Label::new("env", "prod")]),
            Labels::new(vec![Label::metric_name("aa_metric"), Label::new("env", "dev")]),
            Labels::new(vec![Label::metric_name("mm_metric"), Label::new("env", "staging")]),
        ];
        let resp = series_to_response(Ok(labels_vec), Some(2));

        assert_eq!(resp.status, "success");
        let data = resp.data.unwrap();
        assert_eq!(data.len(), 2, "limit should truncate to 2");
        // Should be sorted by __name__
        assert_eq!(data[0].get("__name__").unwrap(), "aa_metric");
        assert_eq!(data[1].get("__name__").unwrap(), "mm_metric");
    }

    // -----------------------------------------------------------------------
    // metadata_to_response — limit + limit_per_metric
    // -----------------------------------------------------------------------

    #[test]
    fn metadata_response_limits() {
        let entries = vec![
            model::MetricMetadata {
                metric_name: "cpu".into(),
                metric_type: Some(MetricType::Gauge),
                description: Some("CPU usage".into()),
                unit: Some("percent".into()),
            },
            model::MetricMetadata {
                metric_name: "cpu".into(),
                metric_type: Some(MetricType::Gauge),
                description: Some("CPU total".into()),
                unit: None,
            },
            model::MetricMetadata {
                metric_name: "mem".into(),
                metric_type: Some(MetricType::Gauge),
                description: Some("Memory".into()),
                unit: Some("bytes".into()),
            },
        ];

        // limit_per_metric caps entries per metric
        let resp = metadata_to_response(Ok(entries.clone()), None, Some(1));
        assert_eq!(resp.status, "success");
        let data = resp.data.unwrap();
        for (_metric, entries) in &data {
            assert!(entries.len() <= 1, "limit_per_metric=1 should cap each metric to 1 entry");
        }

        // limit caps the number of metrics
        let resp = metadata_to_response(Ok(entries), Some(1), None);
        let data = resp.data.unwrap();
        assert_eq!(data.len(), 1, "limit=1 should return only 1 metric");
    }

    #[test]
    fn metadata_response_converts_types() {
        let entries = vec![model::MetricMetadata {
            metric_name: "requests".into(),
            metric_type: Some(MetricType::Gauge),
            description: Some("Total requests".into()),
            unit: Some("1".into()),
        }];
        let resp = metadata_to_response(Ok(entries), None, None);
        let data = resp.data.unwrap();
        let meta = &data["requests"][0];
        assert_eq!(meta.metric_type, "gauge");
        assert_eq!(meta.help, "Total requests");
        assert_eq!(meta.unit, "1");
    }

    // -----------------------------------------------------------------------
    // Property-based tests
    // -----------------------------------------------------------------------

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        /// Generate an arbitrary metric name (1–20 lowercase alpha chars).
        fn arb_metric_name() -> impl Strategy<Value = String> {
            "[a-z][a-z_]{0,19}".prop_filter("non-empty", |s| !s.is_empty())
        }

        /// Generate an arbitrary label key (1–10 lowercase alpha chars).
        fn arb_label_key() -> impl Strategy<Value = String> {
            "[a-z]{1,10}"
        }

        /// Generate an arbitrary label value.
        fn arb_label_value() -> impl Strategy<Value = String> {
            "[a-zA-Z0-9_]{0,20}"
        }

        /// Generate a Labels with a __name__ and 0–3 extra labels.
        fn arb_labels() -> impl Strategy<Value = Labels> {
            (
                arb_metric_name(),
                prop::collection::vec((arb_label_key(), arb_label_value()), 0..4),
            )
                .prop_map(|(name, pairs)| {
                    let mut labels = vec![Label::metric_name(&name)];
                    for (k, v) in pairs {
                        labels.push(Label::new(k, v));
                    }
                    Labels::new(labels)
                })
        }

        /// Generate a MetricMetadata entry.
        fn arb_metadata() -> impl Strategy<Value = model::MetricMetadata> {
            (arb_metric_name(), any::<bool>(), any::<bool>()).prop_map(
                |(name, has_desc, has_unit)| model::MetricMetadata {
                    metric_name: name,
                    metric_type: Some(MetricType::Gauge),
                    description: if has_desc {
                        Some("desc".into())
                    } else {
                        None
                    },
                    unit: if has_unit {
                        Some("unit".into())
                    } else {
                        None
                    },
                },
            )
        }

        proptest! {
            /// Scalar timestamp is always converted from ms to seconds.
            #[test]
            fn scalar_timestamp_is_ms_to_secs(ts_ms in 0i64..=i64::MAX / 2) {
                let resp = query_value_to_response(Ok(QueryValue::Scalar {
                    timestamp_ms: ts_ms,
                    value: 1.0,
                }));
                let data = resp.data.unwrap();
                let (ts_secs, _): (f64, String) =
                    serde_json::from_value(data.result).unwrap();
                prop_assert!(
                    (ts_secs - ts_ms as f64 / 1000.0).abs() < 1e-6,
                    "expected {} / 1000 = {}, got {}",
                    ts_ms,
                    ts_ms as f64 / 1000.0,
                    ts_secs,
                );
            }

            /// Vector response preserves all samples and converts timestamps.
            #[test]
            fn vector_preserves_samples_and_converts_timestamps(
                timestamps in prop::collection::vec(0i64..=i64::MAX / 2, 1..10),
            ) {
                let samples: Vec<InstantSample> = timestamps
                    .iter()
                    .enumerate()
                    .map(|(i, &ts)| InstantSample {
                        labels: Labels::new(vec![Label::metric_name(
                            &format!("m{i}"),
                        )]),
                        timestamp_ms: ts,
                        value: i as f64,
                    })
                    .collect();
                let n = samples.len();
                let resp = query_value_to_response(Ok(QueryValue::Vector(samples)));
                let data = resp.data.unwrap();
                let results: Vec<VectorSeries> =
                    serde_json::from_value(data.result).unwrap();
                prop_assert_eq!(results.len(), n);
                for (result, &ts_ms) in results.iter().zip(timestamps.iter()) {
                    let expected = ts_ms as f64 / 1000.0;
                    prop_assert!(
                        (result.value.0 - expected).abs() < 1e-6,
                        "ts_ms={ts_ms}, expected={expected}, got={}",
                        result.value.0,
                    );
                }
            }

            /// series_to_response output is always sorted by __name__.
            #[test]
            fn series_output_is_sorted(
                labels_vec in prop::collection::vec(arb_labels(), 0..20),
            ) {
                let resp = series_to_response(Ok(labels_vec), None);
                let data = resp.data.unwrap();
                let names: Vec<&str> = data
                    .iter()
                    .map(|m| m.get("__name__").map(|s| s.as_str()).unwrap_or(""))
                    .collect();
                for w in names.windows(2) {
                    prop_assert!(w[0] <= w[1], "not sorted: {:?} > {:?}", w[0], w[1]);
                }
            }

            /// series_to_response with limit always returns at most `limit` entries.
            #[test]
            fn series_limit_is_respected(
                labels_vec in prop::collection::vec(arb_labels(), 0..20),
                limit in 0usize..25,
            ) {
                let input_len = labels_vec.len();
                let resp = series_to_response(Ok(labels_vec), Some(limit));
                let data = resp.data.unwrap();
                prop_assert!(data.len() <= limit, "len {} > limit {}", data.len(), limit);
                prop_assert_eq!(data.len(), input_len.min(limit));
            }

            /// metadata_to_response with limit caps the number of distinct metrics.
            #[test]
            fn metadata_limit_caps_metrics(
                entries in prop::collection::vec(arb_metadata(), 0..20),
                limit in 1usize..10,
            ) {
                let resp = metadata_to_response(Ok(entries), Some(limit), None);
                let data = resp.data.unwrap();
                prop_assert!(
                    data.len() <= limit,
                    "metric count {} > limit {}",
                    data.len(),
                    limit,
                );
            }

            /// metadata_to_response with limit_per_metric caps entries per metric.
            #[test]
            fn metadata_limit_per_metric_caps_entries(
                entries in prop::collection::vec(arb_metadata(), 0..20),
                limit_per in 1usize..5,
            ) {
                let resp = metadata_to_response(Ok(entries), None, Some(limit_per));
                let data = resp.data.unwrap();
                for (metric, entries) in &data {
                    prop_assert!(
                        entries.len() <= limit_per,
                        "metric {metric}: {} entries > limit_per_metric {limit_per}",
                        entries.len(),
                    );
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Existing tests
    // -----------------------------------------------------------------------

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

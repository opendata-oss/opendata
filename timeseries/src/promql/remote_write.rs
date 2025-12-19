//! Prometheus Remote Write 1.0 protocol handler.
//!
//! Implements the Prometheus Remote Write 1.0 specification:
//! https://prometheus.io/docs/specs/prw/remote_write_spec/

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use prost::Message;

use crate::model::{Attribute, MetricType, Sample, SampleWithAttributes};
use crate::tsdb::Tsdb;
use crate::util::{OpenTsdbError, Result};

// ============================================================================
// Protobuf message types (Remote Write 1.0)
// ============================================================================

/// WriteRequest is the top-level message for remote write requests.
#[derive(Clone, PartialEq, Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// TimeSeries represents a single time series with labels and samples.
#[derive(Clone, PartialEq, Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<ProtobufSample>,
}

/// Label is a name-value pair for metric identification.
#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// Sample holds a value and timestamp for a time series data point.
/// Named ProtobufSample to avoid conflict with crate::model::Sample.
#[derive(Clone, PartialEq, Message)]
pub struct ProtobufSample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

// ============================================================================
// Conversion logic
// ============================================================================

/// Convert a WriteRequest into a Vec<SampleWithAttributes>.
///
/// Each TimeSeries in the WriteRequest produces one SampleWithAttributes per sample,
/// all sharing the same label set.
pub fn convert_write_request(request: WriteRequest) -> Vec<SampleWithAttributes> {
    let mut result = Vec::new();

    for ts in request.timeseries {
        // Convert labels to Attributes
        let attributes: Vec<Attribute> = ts
            .labels
            .into_iter()
            .map(|l| Attribute {
                key: l.name,
                value: l.value,
            })
            .collect();

        // Create a SampleWithAttributes for each sample in the time series
        for sample in ts.samples {
            result.push(SampleWithAttributes {
                attributes: attributes.clone(),
                metric_unit: None, // Remote Write 1.0 doesn't include unit info
                metric_type: MetricType::Gauge, // Default to Gauge since type info not in 1.0
                sample: Sample {
                    // Convert from milliseconds (i64) to milliseconds (u64)
                    timestamp: sample.timestamp as u64,
                    value: sample.value,
                },
            });
        }
    }

    result
}

/// Parse a snappy-compressed protobuf WriteRequest.
pub fn parse_remote_write(body: &[u8]) -> Result<Vec<SampleWithAttributes>> {
    // Decompress snappy (block format)
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| OpenTsdbError::InvalidInput(format!("Snappy decompression failed: {}", e)))?;

    // Decode protobuf
    let request = WriteRequest::decode(decompressed.as_slice())
        .map_err(|e| OpenTsdbError::InvalidInput(format!("Protobuf decode failed: {}", e)))?;

    Ok(convert_write_request(request))
}

// ============================================================================
// HTTP handler
// ============================================================================

/// Application state for the remote write handler.
#[derive(Clone)]
pub struct RemoteWriteState {
    pub tsdb: Arc<Tsdb>,
}

/// Error response for remote write requests.
pub struct RemoteWriteError(OpenTsdbError);

impl IntoResponse for RemoteWriteError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            OpenTsdbError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "bad_data"),
            OpenTsdbError::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            OpenTsdbError::Encoding(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            OpenTsdbError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };

        let body = serde_json::json!({
            "status": "error",
            "errorType": error_type,
            "error": self.0.to_string()
        });

        (status, axum::Json(body)).into_response()
    }
}

impl From<OpenTsdbError> for RemoteWriteError {
    fn from(err: OpenTsdbError) -> Self {
        RemoteWriteError(err)
    }
}

/// Handle POST /api/v1/write
///
/// Accepts Prometheus Remote Write 1.0 format:
/// - Content-Type: application/x-protobuf
/// - Content-Encoding: snappy
/// - Body: Snappy-compressed protobuf WriteRequest
pub async fn handle_remote_write(
    State(state): State<super::server::AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<StatusCode, RemoteWriteError> {
    // Validate Content-Type header
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if !content_type.starts_with("application/x-protobuf") {
        return Err(OpenTsdbError::InvalidInput(format!(
            "Invalid Content-Type: expected 'application/x-protobuf', got '{}'",
            content_type
        ))
        .into());
    }

    // Validate Content-Encoding header
    let content_encoding = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_encoding != "snappy" {
        return Err(OpenTsdbError::InvalidInput(format!(
            "Invalid Content-Encoding: expected 'snappy', got '{}'",
            content_encoding
        ))
        .into());
    }

    // Parse the remote write request
    let samples = parse_remote_write(&body)?;

    // Ingest samples into the TSDB
    state.tsdb.ingest_samples(samples).await?;

    // Return 204 No Content on success (as per spec: empty response body)
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // ==================== PROTOBUF MESSAGE TESTS ====================

    #[test]
    fn should_encode_and_decode_write_request() {
        // given
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".to_string(),
                        value: "http_requests_total".to_string(),
                    },
                    Label {
                        name: "method".to_string(),
                        value: "GET".to_string(),
                    },
                ],
                samples: vec![ProtobufSample {
                    value: 42.0,
                    timestamp: 1700000000000,
                }],
            }],
        };

        // when
        let encoded = request.encode_to_vec();
        let decoded = WriteRequest::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].labels.len(), 2);
        assert_eq!(decoded.timeseries[0].samples.len(), 1);
        assert_eq!(decoded.timeseries[0].samples[0].value, 42.0);
    }

    // ==================== CONVERSION TESTS ====================

    #[test]
    fn should_convert_write_request_to_samples() {
        // given
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".to_string(),
                        value: "http_requests".to_string(),
                    },
                    Label {
                        name: "env".to_string(),
                        value: "prod".to_string(),
                    },
                ],
                samples: vec![
                    ProtobufSample {
                        value: 100.0,
                        timestamp: 1700000000000,
                    },
                    ProtobufSample {
                        value: 150.0,
                        timestamp: 1700000001000,
                    },
                ],
            }],
        };

        // when
        let samples = convert_write_request(request);

        // then
        assert_eq!(samples.len(), 2);

        // First sample
        assert_eq!(samples[0].sample.value, 100.0);
        assert_eq!(samples[0].sample.timestamp, 1700000000000);
        assert_eq!(samples[0].attributes.len(), 2);
        assert!(
            samples[0]
                .attributes
                .iter()
                .any(|a| a.key == "__name__" && a.value == "http_requests")
        );
        assert!(
            samples[0]
                .attributes
                .iter()
                .any(|a| a.key == "env" && a.value == "prod")
        );

        // Second sample
        assert_eq!(samples[1].sample.value, 150.0);
        assert_eq!(samples[1].sample.timestamp, 1700000001000);
    }

    #[test]
    fn should_convert_multiple_timeseries() {
        // given
        let request = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![Label {
                        name: "__name__".to_string(),
                        value: "metric_a".to_string(),
                    }],
                    samples: vec![ProtobufSample {
                        value: 1.0,
                        timestamp: 1000,
                    }],
                },
                TimeSeries {
                    labels: vec![Label {
                        name: "__name__".to_string(),
                        value: "metric_b".to_string(),
                    }],
                    samples: vec![ProtobufSample {
                        value: 2.0,
                        timestamp: 2000,
                    }],
                },
            ],
        };

        // when
        let samples = convert_write_request(request);

        // then
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].sample.value, 1.0);
        assert_eq!(samples[1].sample.value, 2.0);
    }

    #[test]
    fn should_handle_empty_write_request() {
        // given
        let request = WriteRequest { timeseries: vec![] };

        // when
        let samples = convert_write_request(request);

        // then
        assert!(samples.is_empty());
    }

    #[test]
    fn should_handle_timeseries_with_no_samples() {
        // given
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: "empty_metric".to_string(),
                }],
                samples: vec![],
            }],
        };

        // when
        let samples = convert_write_request(request);

        // then
        assert!(samples.is_empty());
    }

    // ==================== SNAPPY COMPRESSION TESTS ====================

    #[test]
    fn should_parse_snappy_compressed_request() {
        // given
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: "test_metric".to_string(),
                }],
                samples: vec![ProtobufSample {
                    value: 42.0,
                    timestamp: 1700000000000,
                }],
            }],
        };

        let encoded = request.encode_to_vec();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&encoded)
            .expect("compression should succeed");

        // when
        let samples = parse_remote_write(&compressed).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].sample.value, 42.0);
    }

    #[rstest]
    #[case::empty_body(&[], "Snappy decompression failed")]
    #[case::invalid_snappy(&[0xFF, 0xFF, 0xFF], "Snappy decompression failed")]
    fn should_return_error_for_invalid_snappy(#[case] body: &[u8], #[case] expected_error: &str) {
        // when
        let result = parse_remote_write(body);

        // then
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(expected_error),
            "Expected error containing '{}', got '{}'",
            expected_error,
            err
        );
    }

    #[test]
    fn should_return_error_for_invalid_protobuf() {
        // given: valid snappy but invalid protobuf
        let invalid_proto = b"not a valid protobuf";
        let compressed = snap::raw::Encoder::new()
            .compress_vec(invalid_proto)
            .expect("compression should succeed");

        // when
        let result = parse_remote_write(&compressed);

        // then
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Protobuf decode failed"),
            "Expected protobuf error, got '{}'",
            err
        );
    }

    // ==================== METRIC TYPE TESTS ====================

    #[test]
    fn should_default_to_gauge_metric_type() {
        // given
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: "test".to_string(),
                }],
                samples: vec![ProtobufSample {
                    value: 1.0,
                    timestamp: 1000,
                }],
            }],
        };

        // when
        let samples = convert_write_request(request);

        // then
        assert!(matches!(samples[0].metric_type, MetricType::Gauge));
        assert!(samples[0].metric_unit.is_none());
    }

    // ==================== TIMESTAMP CONVERSION TESTS ====================

    #[rstest]
    #[case::positive_timestamp(1700000000000i64, 1700000000000u64)]
    #[case::zero_timestamp(0i64, 0u64)]
    #[case::small_timestamp(1000i64, 1000u64)]
    fn should_convert_timestamp_correctly(#[case] input: i64, #[case] expected: u64) {
        // given
        let request = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![],
                samples: vec![ProtobufSample {
                    value: 1.0,
                    timestamp: input,
                }],
            }],
        };

        // when
        let samples = convert_write_request(request);

        // then
        assert_eq!(samples[0].sample.timestamp, expected);
    }

    // ==================== LARGE REQUEST TESTS ====================

    #[test]
    fn should_handle_large_write_request() {
        // given: 1000 time series with 10 samples each
        let timeseries: Vec<TimeSeries> = (0..1000)
            .map(|i| TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".to_string(),
                        value: format!("metric_{}", i),
                    },
                    Label {
                        name: "instance".to_string(),
                        value: format!("host_{}", i % 10),
                    },
                ],
                samples: (0..10)
                    .map(|j| ProtobufSample {
                        value: (i * 10 + j) as f64,
                        timestamp: 1700000000000 + j * 1000,
                    })
                    .collect(),
            })
            .collect();

        let request = WriteRequest { timeseries };

        let encoded = request.encode_to_vec();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&encoded)
            .expect("compression should succeed");

        // when
        let samples = parse_remote_write(&compressed).unwrap();

        // then
        assert_eq!(samples.len(), 10000); // 1000 series * 10 samples
    }
}

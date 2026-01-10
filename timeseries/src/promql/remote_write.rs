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

use crate::error::Error;
use crate::model::{Label, MetricType, Sample, Series};
use crate::tsdb::Tsdb;
use crate::util::Result;

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
    pub labels: Vec<ProtobufLabel>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<ProtobufSample>,
}

/// ProtobufLabel is a name-value pair for metric identification.
/// Named ProtobufLabel to avoid conflict with crate::series::Label.
#[derive(Clone, PartialEq, Message)]
pub struct ProtobufLabel {
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

/// Convert a WriteRequest into a Vec<Series>.
///
/// Each TimeSeries in the WriteRequest produces one Series containing all its samples.
pub fn convert_write_request(request: WriteRequest) -> Vec<Series> {
    request
        .timeseries
        .into_iter()
        .map(|ts| {
            let labels: Vec<Label> = ts
                .labels
                .into_iter()
                .map(|l| Label::new(l.name, l.value))
                .collect();

            let samples: Vec<Sample> = ts
                .samples
                .into_iter()
                .map(|s| Sample::new(s.timestamp, s.value))
                .collect();

            Series {
                labels,
                metric_type: Some(MetricType::Gauge), // Default to Gauge since type info not in 1.0
                unit: None,
                description: None,
                samples,
            }
        })
        .collect()
}

/// Parse a snappy-compressed protobuf WriteRequest.
pub fn parse_remote_write(body: &[u8]) -> Result<Vec<Series>> {
    // Decompress snappy (block format)
    let decompressed = snap::raw::Decoder::new()
        .decompress_vec(body)
        .map_err(|e| Error::InvalidInput(format!("Snappy decompression failed: {}", e)))?;

    // Decode protobuf
    let request = WriteRequest::decode(decompressed.as_slice())
        .map_err(|e| Error::InvalidInput(format!("Protobuf decode failed: {}", e)))?;

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
pub struct RemoteWriteError(Error);

impl IntoResponse for RemoteWriteError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            Error::InvalidInput(_) => (StatusCode::BAD_REQUEST, "bad_data"),
            Error::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            Error::Encoding(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            Error::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };

        let body = serde_json::json!({
            "status": "error",
            "errorType": error_type,
            "error": self.0.to_string()
        });

        (status, axum::Json(body)).into_response()
    }
}

impl From<Error> for RemoteWriteError {
    fn from(err: Error) -> Self {
        RemoteWriteError(err)
    }
}

/// Handle POST /api/v1/write
///
/// Accepts Prometheus Remote Write 1.0 format:
/// - Content-Type: application/x-protobuf
/// - Content-Encoding: snappy
/// - Body: Snappy-compressed protobuf WriteRequest
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        request_id = uuid::Uuid::new_v4().to_string(),
        body_size = body.len(),
        content_type = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or(""),
        content_encoding = headers.get("content-encoding").and_then(|v| v.to_str().ok()).unwrap_or(""),
        series_count = tracing::field::Empty,
        samples_count = tracing::field::Empty
    )
)]
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
        return Err(Error::InvalidInput(format!(
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
        return Err(Error::InvalidInput(format!(
            "Invalid Content-Encoding: expected 'snappy', got '{}'",
            content_encoding
        ))
        .into());
    }

    // Parse the remote write request
    let samples = {
        let _parse_span = tracing::debug_span!("parse_remote_write").entered();
        parse_remote_write(&body)?
    };

    // Record parsed metrics
    let total_samples: usize = samples.iter().map(|s| s.samples.len()).sum();
    tracing::Span::current().record("series_count", samples.len());
    tracing::Span::current().record("samples_count", total_samples);

    tracing::debug!(
        series_count = samples.len(),
        samples_count = total_samples,
        "Parsed remote write request"
    );

    // Ingest samples into the TSDB
    match state.tsdb.ingest_samples(samples).await {
        Ok(()) => {
            // Increment successful ingestion counter
            state
                .metrics
                .remote_write_samples_ingested
                .inc_by(total_samples as u64);
            tracing::debug!("Successfully ingested remote write request");

            // Return 204 No Content on success (as per spec: empty response body)
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) => {
            // Increment failed ingestion counter
            state
                .metrics
                .remote_write_samples_failed
                .inc_by(total_samples as u64);
            tracing::error!("Failed to ingest remote write request: {}", e);
            Err(e.into())
        }
    }
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
                    ProtobufLabel {
                        name: "__name__".to_string(),
                        value: "http_requests_total".to_string(),
                    },
                    ProtobufLabel {
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
                    ProtobufLabel {
                        name: "__name__".to_string(),
                        value: "http_requests".to_string(),
                    },
                    ProtobufLabel {
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
        let series_list = convert_write_request(request);

        // then - one series with two samples
        assert_eq!(series_list.len(), 1);

        let series = &series_list[0];
        assert_eq!(series.labels.len(), 2);
        assert!(
            series
                .labels
                .iter()
                .any(|l| l.name == "__name__" && l.value == "http_requests")
        );
        assert!(
            series
                .labels
                .iter()
                .any(|l| l.name == "env" && l.value == "prod")
        );

        // Two samples in the series
        assert_eq!(series.samples.len(), 2);
        assert_eq!(series.samples[0].value, 100.0);
        assert_eq!(series.samples[0].timestamp_ms, 1700000000000);
        assert_eq!(series.samples[1].value, 150.0);
        assert_eq!(series.samples[1].timestamp_ms, 1700000001000);
    }

    #[test]
    fn should_convert_multiple_timeseries() {
        // given
        let request = WriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![ProtobufLabel {
                        name: "__name__".to_string(),
                        value: "metric_a".to_string(),
                    }],
                    samples: vec![ProtobufSample {
                        value: 1.0,
                        timestamp: 1000,
                    }],
                },
                TimeSeries {
                    labels: vec![ProtobufLabel {
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
        let series_list = convert_write_request(request);

        // then - two series, each with one sample
        assert_eq!(series_list.len(), 2);
        assert_eq!(series_list[0].samples[0].value, 1.0);
        assert_eq!(series_list[1].samples[0].value, 2.0);
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
                labels: vec![ProtobufLabel {
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
                labels: vec![ProtobufLabel {
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
        let series_list = parse_remote_write(&compressed).unwrap();

        // then
        assert_eq!(series_list.len(), 1);
        assert_eq!(series_list[0].samples[0].value, 42.0);
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
                labels: vec![ProtobufLabel {
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
        let series_list = convert_write_request(request);

        // then
        assert!(matches!(
            series_list[0].metric_type,
            Some(MetricType::Gauge)
        ));
        assert!(series_list[0].unit.is_none());
    }

    // ==================== TIMESTAMP CONVERSION TESTS ====================

    #[rstest]
    #[case::positive_timestamp(1700000000000i64, 1700000000000i64)]
    #[case::zero_timestamp(0i64, 0i64)]
    #[case::small_timestamp(1000i64, 1000i64)]
    fn should_convert_timestamp_correctly(#[case] input: i64, #[case] expected: i64) {
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
        let series_list = convert_write_request(request);

        // then
        assert_eq!(series_list[0].samples[0].timestamp_ms, expected);
    }

    // ==================== LARGE REQUEST TESTS ====================

    #[test]
    fn should_handle_large_write_request() {
        // given: 1000 time series with 10 samples each
        let timeseries: Vec<TimeSeries> = (0..1000)
            .map(|i| TimeSeries {
                labels: vec![
                    ProtobufLabel {
                        name: "__name__".to_string(),
                        value: format!("metric_{}", i),
                    },
                    ProtobufLabel {
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
        let series_list = parse_remote_write(&compressed).unwrap();

        // then
        assert_eq!(series_list.len(), 1000); // 1000 series
        assert_eq!(series_list[0].samples.len(), 10); // 10 samples each
    }
}

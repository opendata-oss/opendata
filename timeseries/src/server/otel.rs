//! OTLP/HTTP metrics ingest handler.
//!
//! Handles `POST /v1/metrics` requests carrying
//! `ExportMetricsServiceRequest` protobuf payloads.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;

use crate::error::Error;

/// Minimal wire-compatible protobuf for `google.rpc.Status`.
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

/// Minimal wire-compatible protobuf for `google.protobuf.Any`.
#[derive(Clone, PartialEq, Message)]
struct Any {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

/// Error response for OTLP metrics ingest requests.
pub struct OtelIngestError {
    status: StatusCode,
    rpc_code: i32,
    message: String,
}

impl IntoResponse for OtelIngestError {
    fn into_response(self) -> Response {
        let status = RpcStatus {
            code: self.rpc_code,
            message: self.message,
            details: vec![],
        };
        let encoded = status.encode_to_vec();

        (
            self.status,
            [("content-type", "application/x-protobuf")],
            encoded,
        )
            .into_response()
    }
}

impl From<Error> for OtelIngestError {
    fn from(err: Error) -> Self {
        match err {
            Error::InvalidInput(message) => Self {
                status: StatusCode::BAD_REQUEST,
                rpc_code: 3, // INVALID_ARGUMENT
                message,
            },
            Error::Storage(message) | Error::Encoding(message) | Error::Internal(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                rpc_code: 13, // INTERNAL
                message,
            },
            Error::Backpressure => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                rpc_code: 14, // UNAVAILABLE
                message: "Backpressure: write queue is full".to_string(),
            },
        }
    }
}

/// Handle OTLP/HTTP metrics ingest request.
#[tracing::instrument(
    level = "debug",
    skip_all,
    fields(
        request_id = uuid::Uuid::new_v4().to_string(),
        body_size = body.len(),
        content_type = headers.get("content-type").and_then(|v| v.to_str().ok()).unwrap_or(""),
        series_count = tracing::field::Empty,
        samples_count = tracing::field::Empty
    )
)]
pub async fn handle_otel_metrics(
    State(state): State<super::http::AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Response, OtelIngestError> {
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

    let request = ExportMetricsServiceRequest::decode(body.as_ref())
        .map_err(|e| Error::InvalidInput(format!("Protobuf decode failed: {e}")))?;

    let series = state.otel_converter.convert(&request)?;
    let total_samples: usize = series.iter().map(|s| s.samples.len()).sum();

    tracing::Span::current().record("series_count", series.len());
    tracing::Span::current().record("samples_count", total_samples);

    state.tsdb.ingest_samples(series).await?;

    let response = ExportMetricsServiceResponse {
        partial_success: None,
    };
    let encoded = response.encode_to_vec();

    Ok((
        StatusCode::OK,
        [("content-type", "application/x-protobuf")],
        encoded,
    )
        .into_response())
}

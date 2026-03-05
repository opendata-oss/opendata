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
use crate::{OtelConfig, OtelConverter};

/// Error response for OTLP metrics ingest requests.
pub struct OtelIngestError(Error);

impl IntoResponse for OtelIngestError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
            Error::Storage(_) | Error::Encoding(_) | Error::Internal(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Error::Backpressure => StatusCode::SERVICE_UNAVAILABLE,
        };

        (status, self.0.to_string()).into_response()
    }
}

impl From<Error> for OtelIngestError {
    fn from(err: Error) -> Self {
        Self(err)
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

    let converter = OtelConverter::new(OtelConfig::default());
    let series = converter.convert(&request)?;
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

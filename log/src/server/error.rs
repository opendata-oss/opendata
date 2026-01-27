//! HTTP error types for the log server.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::Error;

/// Error wrapper for converting log errors to HTTP responses.
///
/// Per RFC 0004, error responses have the format:
/// ```json
/// { "status": "error", "message": "..." }
/// ```
pub struct ApiError(pub Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
            Error::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Encoding(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = serde_json::json!({
            "status": "error",
            "message": self.0.to_string()
        });

        (status, Json(body)).into_response()
    }
}

impl From<Error> for ApiError {
    fn from(err: Error) -> Self {
        ApiError(err)
    }
}

impl From<&str> for ApiError {
    fn from(msg: &str) -> Self {
        ApiError(Error::InvalidInput(msg.to_string()))
    }
}

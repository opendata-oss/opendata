//! HTTP error types for the log server.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;

use crate::Error;

/// Error wrapper for converting log errors to HTTP responses.
pub struct ApiError(pub Error);

impl IntoResponse for ApiError {
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

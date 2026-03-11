//! HTTP response types for the vector server.

use axum::Json;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use prost::Message;
use serde::Serialize;

/// Content type for binary protobuf.
pub(super) const CONTENT_TYPE_PROTOBUF: &str = "application/protobuf";

/// Content type for ProtoJSON.
pub(super) const CONTENT_TYPE_PROTOJSON: &str = "application/protobuf+json";

/// Check if a media type string indicates binary protobuf (not ProtoJSON).
pub(super) fn is_binary_protobuf(media_type: &str) -> bool {
    media_type.contains(CONTENT_TYPE_PROTOBUF) && !media_type.contains(CONTENT_TYPE_PROTOJSON)
}

/// Desired response format based on Accept header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseFormat {
    Json,
    Protobuf,
}

impl ResponseFormat {
    /// Determine response format from request headers.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let wants_protobuf = headers
            .get(header::ACCEPT)
            .and_then(|v| v.to_str().ok())
            .map(is_binary_protobuf)
            .unwrap_or(false);

        if wants_protobuf {
            ResponseFormat::Protobuf
        } else {
            ResponseFormat::Json
        }
    }
}

/// Response type that can be either JSON or protobuf.
pub enum ApiResponse {
    Json(Json<serde_json::Value>),
    Protobuf(Vec<u8>),
}

impl IntoResponse for ApiResponse {
    fn into_response(self) -> Response {
        match self {
            ApiResponse::Json(json) => json.into_response(),
            ApiResponse::Protobuf(bytes) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, CONTENT_TYPE_PROTOBUF)],
                bytes,
            )
                .into_response(),
        }
    }
}

/// Convert a proto response to ApiResponse based on format.
pub fn to_api_response<T: Message + Serialize>(response: T, format: ResponseFormat) -> ApiResponse {
    match format {
        ResponseFormat::Json => ApiResponse::Json(Json(serde_json::to_value(&response).unwrap())),
        ResponseFormat::Protobuf => ApiResponse::Protobuf(response.encode_to_vec()),
    }
}

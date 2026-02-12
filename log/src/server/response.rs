//! HTTP response types for the log server.

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
    /// Returns Protobuf only for `application/protobuf`, not `application/protobuf+json`.
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

#[cfg(test)]
mod tests {
    use super::super::proto;
    use bytes::Bytes;

    #[test]
    fn should_create_success_scan_response() {
        // given
        let values = vec![proto::Value {
            sequence: 0,
            value: Bytes::from("value"),
        }];

        // when
        let response = proto::ScanResponse::success(Bytes::from("key"), values);

        // then
        assert_eq!(response.status, "success");
        assert_eq!(*response.key.as_ref().unwrap(), Bytes::from("key"));
        assert_eq!(response.values.len(), 1);
    }

    #[test]
    fn should_serialize_scan_response_with_camel_case() {
        // given
        let response = proto::ScanResponse::success(
            Bytes::from("test-key"),
            vec![proto::Value {
                sequence: 42,
                value: Bytes::from("test-value"),
            }],
        );

        // when
        let json = serde_json::to_string(&response).unwrap();

        // then
        // "test-key" -> "dGVzdC1rZXk=", "test-value" -> "dGVzdC12YWx1ZQ=="
        assert!(json.contains(r#""status":"success""#));
        assert!(json.contains(r#""key":"dGVzdC1rZXk=""#));
        assert!(json.contains(r#""sequence":42"#));
        assert!(json.contains(r#""value":"dGVzdC12YWx1ZQ==""#));
        assert!(json.contains(r#""values":"#));
    }

    #[test]
    fn should_create_success_append_response() {
        // given/when
        let response = proto::AppendResponse::success(5, 100);

        // then
        assert_eq!(response.status, "success");
        assert_eq!(response.records_appended, 5);
        assert_eq!(response.start_sequence, 100);
    }

    #[test]
    fn should_serialize_append_response_with_camel_case() {
        // given
        let response = proto::AppendResponse::success(3, 42);

        // when
        let json = serde_json::to_string(&response).unwrap();

        // then
        assert!(json.contains(r#""recordsAppended":3"#));
        assert!(json.contains(r#""startSequence":42"#));
    }

    #[test]
    fn should_serialize_list_keys_response_with_wrapped_keys() {
        // given
        let response =
            proto::KeysResponse::success(vec![Bytes::from("events"), Bytes::from("orders")]);

        // when
        let json = serde_json::to_string(&response).unwrap();

        // then
        // "events" -> "ZXZlbnRz", "orders" -> "b3JkZXJz"
        assert!(json.contains(r#""keys":[{"key":"ZXZlbnRz"},{"key":"b3JkZXJz"}]"#));
    }

    #[test]
    fn should_serialize_segments_response_with_camel_case() {
        // given
        let response = proto::SegmentsResponse::success(vec![proto::Segment {
            id: 0,
            start_seq: 100,
            start_time_ms: 1705766400000,
        }]);

        // when
        let json = serde_json::to_string(&response).unwrap();

        // then
        assert!(json.contains(r#""id":0"#));
        assert!(json.contains(r#""startSeq":100"#));
        assert!(json.contains(r#""startTimeMs":1705766400000"#));
    }
}

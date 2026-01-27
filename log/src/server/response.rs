//! HTTP response types for the log server.

use axum::Json;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use prost::Message;
use serde::Serialize;
use serde_with::{base64::Base64, serde_as};

use super::proto;
use crate::LogEntry;

/// Content type for binary protobuf.
const CONTENT_TYPE_PROTOBUF: &str = "application/protobuf";

/// Content type for ProtoJSON.
const CONTENT_TYPE_PROTOJSON: &str = "application/protobuf+json";

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
            .map(|s| {
                // Check for exact match or with parameters (e.g., "application/protobuf; q=1.0")
                // But not application/protobuf+json
                s.contains(CONTENT_TYPE_PROTOBUF) && !s.contains(CONTENT_TYPE_PROTOJSON)
            })
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

/// Trait for response types that can be converted to ApiResponse.
///
/// Implementors provide conversion to both JSON and protobuf formats,
/// allowing handlers to delegate format selection to the response type.
pub trait IntoApiResponse {
    /// The protobuf message type for this response.
    type Proto: Message;

    /// Convert to the protobuf representation.
    fn into_proto(self) -> Self::Proto;

    /// Convert to ApiResponse in the specified format.
    fn into_api_response(self, format: ResponseFormat) -> ApiResponse
    where
        Self: Sized + Serialize,
    {
        match format {
            ResponseFormat::Json => ApiResponse::Json(Json(serde_json::to_value(self).unwrap())),
            ResponseFormat::Protobuf => ApiResponse::Protobuf(self.into_proto().encode_to_vec()),
        }
    }
}

/// Key wrapper for JSON serialization and deserialization.
#[serde_as]
#[derive(Debug, Serialize, serde::Deserialize)]
pub struct KeyJson {
    /// Key value (base64-encoded bytes).
    #[serde_as(as = "Base64")]
    pub value: Bytes,
}

impl KeyJson {
    /// Create a KeyJson from bytes.
    pub fn new(value: Bytes) -> Self {
        Self { value }
    }
}

/// A single value in a scan response.
///
/// Per RFC 0004, values only contain sequence and value (not key).
/// The key is included at the top level of the ScanResponse.
#[serde_as]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanValue {
    /// Sequence number.
    pub sequence: u64,
    /// Value (base64-encoded bytes).
    #[serde_as(as = "Base64")]
    pub value: Bytes,
}

impl ScanValue {
    /// Create a scan value from a LogEntry.
    pub fn from_log_entry(entry: &LogEntry) -> Self {
        Self {
            sequence: entry.sequence,
            value: entry.value.clone(),
        }
    }
}

/// Response for scan requests.
///
/// Per RFC 0004, the key is at the top level and values contain only sequence + value.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanResponse {
    /// Status of the response.
    pub status: String,
    /// The key that was scanned (wrapped in Key message).
    pub key: KeyJson,
    /// Values returned by the scan.
    pub values: Vec<ScanValue>,
}

impl ScanResponse {
    /// Create a successful scan response.
    pub fn success(key: Bytes, values: Vec<ScanValue>) -> Self {
        Self {
            status: "success".to_string(),
            key: KeyJson::new(key),
            values,
        }
    }
}

impl IntoApiResponse for ScanResponse {
    type Proto = proto::ScanResponse;

    fn into_proto(self) -> Self::Proto {
        proto::ScanResponse {
            status: self.status,
            key: Some(proto::Key {
                value: self.key.value,
            }),
            values: self
                .values
                .into_iter()
                .map(|e| proto::Value {
                    sequence: e.sequence,
                    value: e.value,
                })
                .collect(),
        }
    }
}

/// Response for append requests.
///
/// Per RFC 0004, includes the starting sequence number.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppendResponse {
    /// Status of the response.
    pub status: String,
    /// Number of records appended.
    pub records_appended: usize,
    /// Sequence number of the first appended record.
    pub start_sequence: u64,
}

impl AppendResponse {
    /// Create a successful append response.
    pub fn success(records_appended: usize, start_sequence: u64) -> Self {
        Self {
            status: "success".to_string(),
            records_appended,
            start_sequence,
        }
    }
}

impl IntoApiResponse for AppendResponse {
    type Proto = proto::AppendResponse;

    fn into_proto(self) -> Self::Proto {
        proto::AppendResponse {
            status: self.status,
            records_appended: self.records_appended as i32,
            start_sequence: self.start_sequence,
        }
    }
}

/// Response for list keys requests.
///
/// Per RFC 0004, keys is an array of Key messages.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListKeysResponse {
    /// Status of the response.
    pub status: String,
    /// Keys in the log (wrapped in Key messages).
    pub keys: Vec<KeyJson>,
}

impl ListKeysResponse {
    /// Create a successful list keys response.
    pub fn success(keys: Vec<Bytes>) -> Self {
        Self {
            status: "success".to_string(),
            keys: keys.into_iter().map(KeyJson::new).collect(),
        }
    }
}

impl IntoApiResponse for ListKeysResponse {
    type Proto = proto::KeysResponse;

    fn into_proto(self) -> Self::Proto {
        proto::KeysResponse {
            status: self.status,
            keys: self
                .keys
                .into_iter()
                .map(|k| proto::Key { value: k.value })
                .collect(),
        }
    }
}

/// A segment entry in a list segments response.
///
/// Per RFC 0004, uses camelCase field names.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentEntry {
    /// Unique segment identifier.
    pub id: u32,
    /// First sequence number in this segment.
    pub start_seq: u64,
    /// Wall-clock time when this segment was created (ms since epoch).
    pub start_time_ms: i64,
}

/// Response for list segments requests.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSegmentsResponse {
    /// Status of the response.
    pub status: String,
    /// Segments in the log.
    pub segments: Vec<SegmentEntry>,
}

impl ListSegmentsResponse {
    /// Create a successful list segments response.
    pub fn success(segments: Vec<SegmentEntry>) -> Self {
        Self {
            status: "success".to_string(),
            segments,
        }
    }
}

impl IntoApiResponse for ListSegmentsResponse {
    type Proto = proto::SegmentsResponse;

    fn into_proto(self) -> Self::Proto {
        proto::SegmentsResponse {
            status: self.status,
            segments: self
                .segments
                .into_iter()
                .map(|s| proto::Segment {
                    id: s.id,
                    start_seq: s.start_seq,
                    start_time_ms: s.start_time_ms,
                })
                .collect(),
        }
    }
}

/// Response for count requests.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CountResponse {
    /// Status of the response.
    pub status: String,
    /// Number of entries for the key.
    pub count: u64,
}

impl CountResponse {
    /// Create a successful count response.
    pub fn success(count: u64) -> Self {
        Self {
            status: "success".to_string(),
            count,
        }
    }
}

impl IntoApiResponse for CountResponse {
    type Proto = proto::CountResponse;

    fn into_proto(self) -> Self::Proto {
        proto::CountResponse {
            status: self.status,
            count: self.count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_convert_log_entry_to_scan_value() {
        // given
        let entry = LogEntry {
            key: Bytes::from("test-key"),
            sequence: 42,
            value: Bytes::from("test-value"),
        };

        // when
        let scan_value = ScanValue::from_log_entry(&entry);

        // then
        assert_eq!(scan_value.sequence, 42);
        assert_eq!(scan_value.value, Bytes::from("test-value"));
    }

    #[test]
    fn should_create_success_scan_response() {
        // given
        let values = vec![ScanValue {
            sequence: 0,
            value: Bytes::from("value"),
        }];

        // when
        let response = ScanResponse::success(Bytes::from("key"), values);

        // then
        assert_eq!(response.status, "success");
        assert_eq!(response.key.value, Bytes::from("key"));
        assert_eq!(response.values.len(), 1);
    }

    #[test]
    fn should_serialize_scan_response_with_camel_case() {
        // given
        let response = ScanResponse::success(
            Bytes::from("test-key"),
            vec![ScanValue {
                sequence: 42,
                value: Bytes::from("test-value"),
            }],
        );

        // when
        let json = serde_json::to_string(&response).unwrap();

        // then
        // "test-key" -> "dGVzdC1rZXk=", "test-value" -> "dGVzdC12YWx1ZQ=="
        assert!(json.contains(r#""status":"success""#));
        assert!(json.contains(r#""key":{"value":"dGVzdC1rZXk="}"#));
        assert!(json.contains(r#""sequence":42"#));
        assert!(json.contains(r#""value":"dGVzdC12YWx1ZQ==""#));
        assert!(json.contains(r#""values":"#));
    }

    #[test]
    fn should_create_success_append_response() {
        // given/when
        let response = AppendResponse::success(5, 100);

        // then
        assert_eq!(response.status, "success");
        assert_eq!(response.records_appended, 5);
        assert_eq!(response.start_sequence, 100);
    }

    #[test]
    fn should_serialize_append_response_with_camel_case() {
        // given
        let response = AppendResponse::success(3, 42);

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
            ListKeysResponse::success(vec![Bytes::from("events"), Bytes::from("orders")]);

        // when
        let json = serde_json::to_string(&response).unwrap();

        // then
        // "events" -> "ZXZlbnRz", "orders" -> "b3JkZXJz"
        assert!(json.contains(r#""keys":[{"value":"ZXZlbnRz"},{"value":"b3JkZXJz"}]"#));
    }

    #[test]
    fn should_serialize_segments_response_with_camel_case() {
        // given
        let response = ListSegmentsResponse::success(vec![SegmentEntry {
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

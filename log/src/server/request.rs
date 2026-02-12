//! HTTP request types for the log server.

use axum::http::{HeaderMap, header};
use bytes::Bytes;
use prost::Message;
use serde::Deserialize;

use super::proto;
use super::response::is_binary_protobuf;
use crate::Error;

/// Check if the request body is protobuf based on Content-Type header.
/// Only matches `application/protobuf`, not `application/protobuf+json`.
fn is_protobuf_content(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(is_binary_protobuf)
        .unwrap_or(false)
}

/// Query parameters for scan requests.
#[derive(Debug, Deserialize)]
pub struct ScanParams {
    /// Key to scan (plain string, URL-encoded if needed).
    pub key: String,
    /// Start sequence number (inclusive).
    pub start_seq: Option<u64>,
    /// End sequence number (exclusive).
    pub end_seq: Option<u64>,
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
    /// If true, long-poll until data is available or timeout.
    pub follow: Option<bool>,
    /// Timeout in milliseconds for long-polling (default: 30000).
    pub timeout_ms: Option<u64>,
}

impl ScanParams {
    /// Get the key as bytes.
    pub fn key(&self) -> Bytes {
        Bytes::from(self.key.clone())
    }

    /// Get the sequence range as start..end.
    pub fn seq_range(&self) -> std::ops::Range<u64> {
        let start = self.start_seq.unwrap_or(0);
        let end = self.end_seq.unwrap_or(u64::MAX);
        start..end
    }
}

/// Query parameters for list keys requests.
#[derive(Debug, Deserialize)]
pub struct ListKeysParams {
    /// Start segment ID (inclusive).
    pub start_segment: Option<u32>,
    /// End segment ID (exclusive).
    pub end_segment: Option<u32>,
    /// Maximum number of keys to return.
    pub limit: Option<usize>,
}

impl ListKeysParams {
    /// Get the segment range as start..end.
    pub fn segment_range(&self) -> std::ops::Range<u32> {
        let start = self.start_segment.unwrap_or(0);
        let end = self.end_segment.unwrap_or(u32::MAX);
        start..end
    }
}

/// Query parameters for list segments requests.
#[derive(Debug, Deserialize)]
pub struct ListSegmentsParams {
    /// Start sequence number (inclusive).
    pub start_seq: Option<u64>,
    /// End sequence number (exclusive).
    pub end_seq: Option<u64>,
}

impl ListSegmentsParams {
    /// Get the sequence range as start..end.
    pub fn seq_range(&self) -> std::ops::Range<u64> {
        let start = self.start_seq.unwrap_or(0);
        let end = self.end_seq.unwrap_or(u64::MAX);
        start..end
    }
}

/// Query parameters for count requests.
#[derive(Debug, Deserialize)]
pub struct CountParams {
    /// Key to count (plain string, URL-encoded if needed).
    pub key: String,
    /// Start sequence number (inclusive).
    pub start_seq: Option<u64>,
    /// End sequence number (exclusive).
    pub end_seq: Option<u64>,
}

impl CountParams {
    /// Get the key as bytes.
    pub fn key(&self) -> Bytes {
        Bytes::from(self.key.clone())
    }

    /// Get the sequence range as start..end.
    pub fn seq_range(&self) -> std::ops::Range<u64> {
        let start = self.start_seq.unwrap_or(0);
        let end = self.end_seq.unwrap_or(u64::MAX);
        start..end
    }
}

/// Unified append request that can be parsed from either JSON or protobuf.
///
/// Per RFC 0004, supports both `Content-Type: application/json` (ProtoJSON)
/// and `Content-Type: application/x-protobuf` (binary protobuf).
#[derive(Debug)]
pub struct AppendRequest {
    /// Records to append.
    pub records: Vec<crate::Record>,
    /// Whether to wait for durable write.
    pub await_durable: bool,
}

impl AppendRequest {
    /// Parse an append request from the raw body based on Content-Type header.
    ///
    /// - `application/x-protobuf`: Parse as binary protobuf
    /// - `application/json` (or other): Parse as ProtoJSON
    pub fn from_body(headers: &HeaderMap, body: &[u8]) -> Result<Self, Error> {
        if is_protobuf_content(headers) {
            Self::from_protobuf(body)
        } else {
            Self::from_json(body)
        }
    }

    /// Parse from binary protobuf.
    fn from_protobuf(body: &[u8]) -> Result<Self, Error> {
        let proto_request = proto::AppendRequest::decode(body)
            .map_err(|e| Error::InvalidInput(format!("Invalid protobuf: {}", e)))?;

        Self::from_proto_request(proto_request)
    }

    /// Parse from JSON (ProtoJSON format).
    fn from_json(body: &[u8]) -> Result<Self, Error> {
        let proto_request: proto::AppendRequest = serde_json::from_slice(body)
            .map_err(|e| Error::InvalidInput(format!("Invalid JSON: {}", e)))?;

        Self::from_proto_request(proto_request)
    }

    /// Convert a proto AppendRequest into the internal representation.
    fn from_proto_request(proto_request: proto::AppendRequest) -> Result<Self, Error> {
        let mut records = Vec::with_capacity(proto_request.records.len());
        for (i, r) in proto_request.records.into_iter().enumerate() {
            let key = r
                .key
                .ok_or_else(|| Error::InvalidInput(format!("record[{}]: key is required", i)))?;
            let value = r
                .value
                .ok_or_else(|| Error::InvalidInput(format!("record[{}]: value is required", i)))?;
            records.push(crate::Record { key, value });
        }

        Ok(Self {
            records,
            await_durable: proto_request.await_durable,
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn protobuf_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/protobuf"),
        );
        headers
    }

    fn protojson_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/protobuf+json"),
        );
        headers
    }

    #[test]
    fn should_get_key_as_bytes() {
        // given
        let params = ScanParams {
            key: "my-key".to_string(),
            start_seq: None,
            end_seq: None,
            limit: None,
            follow: None,
            timeout_ms: None,
        };

        // when
        let key = params.key();

        // then
        assert_eq!(key.as_ref(), b"my-key");
    }

    #[test]
    fn should_parse_append_request_from_json() {
        // given - camelCase JSON with base64 encoded bytes
        // "test-key" -> "dGVzdC1rZXk=", "test-value" -> "dGVzdC12YWx1ZQ=="
        let json = br#"{
            "records": [{"key": "dGVzdC1rZXk=", "value": "dGVzdC12YWx1ZQ=="}],
            "awaitDurable": true
        }"#;

        // when
        let request = AppendRequest::from_body(&protojson_headers(), json).unwrap();

        // then
        assert_eq!(request.records.len(), 1);
        assert_eq!(request.records[0].key, Bytes::from("test-key"));
        assert_eq!(request.records[0].value, Bytes::from("test-value"));
        assert!(request.await_durable);
    }

    #[test]
    fn should_parse_append_request_from_json_without_await_durable() {
        // given - awaitDurable defaults to false when not specified
        // "key" -> "a2V5", "value" -> "dmFsdWU="
        let json = br#"{
            "records": [{"key": "a2V5", "value": "dmFsdWU="}]
        }"#;

        // when
        let request = AppendRequest::from_body(&protojson_headers(), json).unwrap();

        // then
        assert!(!request.await_durable);
    }

    #[test]
    fn should_parse_append_request_from_protobuf() {
        // given
        let proto_request = proto::AppendRequest {
            records: vec![proto::Record {
                key: Some(Bytes::from("proto-key")),
                value: Some(Bytes::from("proto-value")),
            }],
            await_durable: true,
        };
        let body = proto_request.encode_to_vec();

        // when
        let request = AppendRequest::from_body(&protobuf_headers(), &body).unwrap();

        // then
        assert_eq!(request.records.len(), 1);
        assert_eq!(request.records[0].key, Bytes::from("proto-key"));
        assert_eq!(request.records[0].value, Bytes::from("proto-value"));
        assert!(request.await_durable);
    }

    #[test]
    fn should_return_error_for_missing_key() {
        // given - record without key
        let json = br#"{
            "records": [{"value": "dmFsdWU="}]
        }"#;

        // when
        let result = AppendRequest::from_body(&protojson_headers(), json);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("key is required"));
    }

    #[test]
    fn should_return_error_for_missing_value() {
        // given - record without value
        let json = br#"{
            "records": [{"key": "a2V5"}]
        }"#;

        // when
        let result = AppendRequest::from_body(&protojson_headers(), json);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("value is required")
        );
    }

    #[test]
    fn should_return_error_for_invalid_json() {
        // given
        let body = b"not valid json";

        // when
        let result = AppendRequest::from_body(&protojson_headers(), body);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid JSON"));
    }

    #[test]
    fn should_return_error_for_invalid_protobuf() {
        // given
        let body = &[0xFF, 0xFF, 0xFF];

        // when
        let result = AppendRequest::from_body(&protobuf_headers(), body);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid protobuf"));
    }

    #[test]
    fn should_return_default_seq_range() {
        // given
        let params = ScanParams {
            key: "test".to_string(),
            start_seq: None,
            end_seq: None,
            limit: None,
            follow: None,
            timeout_ms: None,
        };

        // when
        let range = params.seq_range();

        // then
        assert_eq!(range.start, 0);
        assert_eq!(range.end, u64::MAX);
    }

    #[test]
    fn should_use_provided_seq_range() {
        // given
        let params = ScanParams {
            key: "test".to_string(),
            start_seq: Some(10),
            end_seq: Some(100),
            limit: None,
            follow: None,
            timeout_ms: None,
        };

        // when
        let range = params.seq_range();

        // then
        assert_eq!(range.start, 10);
        assert_eq!(range.end, 100);
    }

    #[test]
    fn should_default_follow_to_false() {
        // given - deserialize params with default follow
        let json = r#"{"key": "test"}"#;

        // when
        let params: ScanParams = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(params.follow, None);
        assert!(params.timeout_ms.is_none());
    }

    #[test]
    fn should_parse_follow_and_timeout_params() {
        // given
        let json = r#"{"key": "test", "follow": true, "timeout_ms": 5000}"#;

        // when
        let params: ScanParams = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(params.follow, Some(true));
        assert_eq!(params.timeout_ms, Some(5000));
    }
}

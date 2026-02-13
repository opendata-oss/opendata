//! Protobuf message definitions for the log server HTTP API.
//!
//! These types support both binary protobuf encoding (application/protobuf)
//! and ProtoJSON encoding (application/protobuf+json) per RFC 0004-http-apis.

use prost::Message;
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};

/// Key wraps a bytes value for keys.
#[serde_as]
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
pub struct Key {
    #[prost(bytes = "bytes", tag = "1")]
    #[serde_as(as = "Base64")]
    pub key: bytes::Bytes,
}

/// AppendRequest is the request body for POST /api/v1/log/append.
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppendRequest {
    #[prost(message, repeated, tag = "1")]
    pub records: Vec<Record>,
    #[prost(bool, tag = "2")]
    #[serde(default)]
    pub await_durable: bool,
}

/// Record represents a single log record with key and value.
#[serde_as]
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    #[prost(bytes = "bytes", optional, tag = "1")]
    #[serde_as(as = "Option<Base64>")]
    pub key: Option<bytes::Bytes>,
    #[prost(bytes = "bytes", optional, tag = "2")]
    #[serde_as(as = "Option<Base64>")]
    pub value: Option<bytes::Bytes>,
}

/// AppendResponse is the response for POST /api/v1/log/append.
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppendResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(int32, tag = "2")]
    pub records_appended: i32,
    #[prost(uint64, tag = "3")]
    pub start_sequence: u64,
}

impl AppendResponse {
    /// Create a successful append response.
    pub fn success(records_appended: i32, start_sequence: u64) -> Self {
        Self {
            status: "success".to_string(),
            records_appended,
            start_sequence,
        }
    }
}

/// ScanResponse is the response for GET /api/v1/log/scan.
#[serde_as]
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(bytes = "bytes", optional, tag = "2")]
    #[serde_as(as = "Option<Base64>")]
    pub key: Option<bytes::Bytes>,
    #[prost(message, repeated, tag = "3")]
    pub values: Vec<Value>,
}

impl ScanResponse {
    /// Create a successful scan response.
    pub fn success(key: bytes::Bytes, values: Vec<Value>) -> Self {
        Self {
            status: "success".to_string(),
            key: Some(key),
            values,
        }
    }
}

/// Value represents a single log entry in scan results.
#[serde_as]
#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Value {
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    #[prost(bytes = "bytes", tag = "2")]
    #[serde_as(as = "Base64")]
    pub value: bytes::Bytes,
}

/// SegmentsResponse is the response for GET /api/v1/log/segments.
#[derive(Clone, PartialEq, Message, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentsResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(message, repeated, tag = "2")]
    pub segments: Vec<Segment>,
}

impl SegmentsResponse {
    /// Create a successful segments response.
    pub fn success(segments: Vec<Segment>) -> Self {
        Self {
            status: "success".to_string(),
            segments,
        }
    }
}

/// Segment represents a log segment.
#[derive(Clone, PartialEq, Message, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(uint64, tag = "2")]
    pub start_seq: u64,
    #[prost(int64, tag = "3")]
    pub start_time_ms: i64,
}

/// KeysResponse is the response for GET /api/v1/log/keys.
#[derive(Clone, PartialEq, Message, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeysResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(message, repeated, tag = "2")]
    pub keys: Vec<Key>,
}

impl KeysResponse {
    /// Create a successful keys response.
    pub fn success(keys: Vec<bytes::Bytes>) -> Self {
        Self {
            status: "success".to_string(),
            keys: keys.into_iter().map(|k| Key { key: k }).collect(),
        }
    }
}

/// CountResponse is the response for GET /api/v1/log/count.
#[derive(Clone, PartialEq, Message, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CountResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(uint64, tag = "2")]
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

/// ErrorResponse is returned for all error cases.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Message)]
pub struct ErrorResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(string, tag = "2")]
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_append_request() {
        // given
        let request = AppendRequest {
            records: vec![Record {
                key: Some(bytes::Bytes::from("test-key")),
                value: Some(bytes::Bytes::from("test-value")),
            }],
            await_durable: true,
        };

        // when
        let encoded = request.encode_to_vec();
        let decoded = AppendRequest::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(
            *decoded.records[0].key.as_ref().unwrap(),
            bytes::Bytes::from("test-key")
        );
        assert_eq!(
            *decoded.records[0].value.as_ref().unwrap(),
            bytes::Bytes::from("test-value")
        );
        assert!(decoded.await_durable);
    }

    #[test]
    fn should_encode_and_decode_append_response() {
        // given
        let response = AppendResponse {
            status: "success".to_string(),
            records_appended: 5,
            start_sequence: 42,
        };

        // when
        let encoded = response.encode_to_vec();
        let decoded = AppendResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert_eq!(decoded.records_appended, 5);
        assert_eq!(decoded.start_sequence, 42);
    }

    #[test]
    fn should_encode_and_decode_scan_response() {
        // given
        let response = ScanResponse {
            status: "success".to_string(),
            key: Some(bytes::Bytes::from("my-key")),
            values: vec![Value {
                sequence: 10,
                value: bytes::Bytes::from("my-value"),
            }],
        };

        // when
        let encoded = response.encode_to_vec();
        let decoded = ScanResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert_eq!(*decoded.key.as_ref().unwrap(), bytes::Bytes::from("my-key"));
        assert_eq!(decoded.values.len(), 1);
        assert_eq!(decoded.values[0].sequence, 10);
        assert_eq!(decoded.values[0].value, bytes::Bytes::from("my-value"));
    }

    #[test]
    fn should_encode_and_decode_keys_response() {
        // given
        let response = KeysResponse {
            status: "success".to_string(),
            keys: vec![
                Key {
                    key: bytes::Bytes::from("key-a"),
                },
                Key {
                    key: bytes::Bytes::from("key-b"),
                },
            ],
        };

        // when
        let encoded = response.encode_to_vec();
        let decoded = KeysResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert_eq!(decoded.keys.len(), 2);
        assert_eq!(decoded.keys[0].key, bytes::Bytes::from("key-a"));
        assert_eq!(decoded.keys[1].key, bytes::Bytes::from("key-b"));
    }

    #[test]
    fn should_encode_and_decode_segments_response() {
        // given
        let response = SegmentsResponse {
            status: "success".to_string(),
            segments: vec![Segment {
                id: 0,
                start_seq: 0,
                start_time_ms: 1705766400000,
            }],
        };

        // when
        let encoded = response.encode_to_vec();
        let decoded = SegmentsResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert_eq!(decoded.segments.len(), 1);
        assert_eq!(decoded.segments[0].id, 0);
        assert_eq!(decoded.segments[0].start_seq, 0);
        assert_eq!(decoded.segments[0].start_time_ms, 1705766400000);
    }

    #[test]
    fn should_encode_and_decode_error_response() {
        // given
        let response = ErrorResponse {
            status: "error".to_string(),
            message: "Something went wrong".to_string(),
        };

        // when
        let encoded = response.encode_to_vec();
        let decoded = ErrorResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "error");
        assert_eq!(decoded.message, "Something went wrong");
    }
}

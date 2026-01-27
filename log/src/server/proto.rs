//! Protobuf message definitions for the log server HTTP API.
//!
//! These types support both binary protobuf encoding (application/x-protobuf)
//! and ProtoJSON encoding (application/json) per RFC 0004-http-apis.

use prost::Message;

/// AppendRequest is the request body for POST /api/v1/log/append.
#[derive(Clone, PartialEq, Message)]
pub struct AppendRequest {
    #[prost(message, repeated, tag = "1")]
    pub records: Vec<Record>,
    #[prost(bool, tag = "2")]
    pub await_durable: bool,
}

/// Record represents a single log record with key and value.
#[derive(Clone, PartialEq, Message)]
pub struct Record {
    #[prost(bytes = "bytes", tag = "1")]
    pub key: bytes::Bytes,
    #[prost(bytes = "bytes", tag = "2")]
    pub value: bytes::Bytes,
}

/// AppendResponse is the response for POST /api/v1/log/append.
#[derive(Clone, PartialEq, Message)]
pub struct AppendResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(int32, tag = "2")]
    pub records_appended: i32,
    #[prost(uint64, tag = "3")]
    pub start_sequence: u64,
}

/// ScanResponse is the response for GET /api/v1/log/scan.
#[derive(Clone, PartialEq, Message)]
pub struct ScanResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(bytes = "bytes", tag = "2")]
    pub key: bytes::Bytes,
    #[prost(message, repeated, tag = "3")]
    pub entries: Vec<Entry>,
}

/// Entry represents a single log entry in scan results.
#[derive(Clone, PartialEq, Message)]
pub struct Entry {
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    #[prost(bytes = "bytes", tag = "2")]
    pub value: bytes::Bytes,
}

/// SegmentsResponse is the response for GET /api/v1/log/segments.
#[derive(Clone, PartialEq, Message)]
pub struct SegmentsResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(message, repeated, tag = "2")]
    pub segments: Vec<Segment>,
}

/// Segment represents a log segment.
#[derive(Clone, PartialEq, Message)]
pub struct Segment {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(uint64, tag = "2")]
    pub start_seq: u64,
    #[prost(int64, tag = "3")]
    pub start_time_ms: i64,
}

/// KeysResponse is the response for GET /api/v1/log/keys.
#[derive(Clone, PartialEq, Message)]
pub struct KeysResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(bytes = "bytes", repeated, tag = "2")]
    pub keys: Vec<bytes::Bytes>,
}

/// CountResponse is the response for GET /api/v1/log/count.
#[derive(Clone, PartialEq, Message)]
pub struct CountResponse {
    #[prost(string, tag = "1")]
    pub status: String,
    #[prost(uint64, tag = "2")]
    pub count: u64,
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
                key: bytes::Bytes::from("test-key"),
                value: bytes::Bytes::from("test-value"),
            }],
            await_durable: true,
        };

        // when
        let encoded = request.encode_to_vec();
        let decoded = AppendRequest::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, bytes::Bytes::from("test-key"));
        assert_eq!(decoded.records[0].value, bytes::Bytes::from("test-value"));
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
            key: bytes::Bytes::from("my-key"),
            entries: vec![Entry {
                sequence: 10,
                value: bytes::Bytes::from("my-value"),
            }],
        };

        // when
        let encoded = response.encode_to_vec();
        let decoded = ScanResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert_eq!(decoded.key, bytes::Bytes::from("my-key"));
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].sequence, 10);
        assert_eq!(decoded.entries[0].value, bytes::Bytes::from("my-value"));
    }

    #[test]
    fn should_encode_and_decode_keys_response() {
        // given
        let response = KeysResponse {
            status: "success".to_string(),
            keys: vec![bytes::Bytes::from("key-a"), bytes::Bytes::from("key-b")],
        };

        // when
        let encoded = response.encode_to_vec();
        let decoded = KeysResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert_eq!(decoded.keys.len(), 2);
        assert_eq!(decoded.keys[0], bytes::Bytes::from("key-a"));
        assert_eq!(decoded.keys[1], bytes::Bytes::from("key-b"));
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

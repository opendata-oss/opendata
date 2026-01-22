//! HTTP request types for the log server.

use base64::Engine;
use bytes::Bytes;
use serde::Deserialize;

use crate::Error;

/// Query parameters for scan requests.
#[derive(Debug, Deserialize)]
pub struct ScanParams {
    /// Base64-encoded key to scan.
    pub key: String,
    /// Start sequence number (inclusive).
    pub start_seq: Option<u64>,
    /// End sequence number (exclusive).
    pub end_seq: Option<u64>,
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
}

impl ScanParams {
    /// Decode the base64-encoded key.
    pub fn decode_key(&self) -> Result<Bytes, Error> {
        base64::engine::general_purpose::STANDARD
            .decode(&self.key)
            .map(Bytes::from)
            .map_err(|e| Error::InvalidInput(format!("invalid base64 key: {}", e)))
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
    /// Start sequence number (inclusive).
    pub start_seq: Option<u64>,
    /// End sequence number (exclusive).
    pub end_seq: Option<u64>,
    /// Maximum number of keys to return.
    pub limit: Option<usize>,
}

impl ListKeysParams {
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
    /// Base64-encoded key to count.
    pub key: String,
    /// Start sequence number (inclusive).
    pub start_seq: Option<u64>,
    /// End sequence number (exclusive).
    pub end_seq: Option<u64>,
}

impl CountParams {
    /// Decode the base64-encoded key.
    pub fn decode_key(&self) -> Result<Bytes, Error> {
        base64::engine::general_purpose::STANDARD
            .decode(&self.key)
            .map(Bytes::from)
            .map_err(|e| Error::InvalidInput(format!("invalid base64 key: {}", e)))
    }

    /// Get the sequence range as start..end.
    pub fn seq_range(&self) -> std::ops::Range<u64> {
        let start = self.start_seq.unwrap_or(0);
        let end = self.end_seq.unwrap_or(u64::MAX);
        start..end
    }
}

/// A single record in an append request.
#[derive(Debug, Deserialize)]
pub struct AppendRecord {
    /// Base64-encoded key.
    pub key: String,
    /// Base64-encoded value.
    pub value: String,
}

impl AppendRecord {
    /// Decode to a log Record.
    pub fn decode(&self) -> Result<crate::Record, Error> {
        let key = base64::engine::general_purpose::STANDARD
            .decode(&self.key)
            .map(Bytes::from)
            .map_err(|e| Error::InvalidInput(format!("invalid base64 key: {}", e)))?;
        let value = base64::engine::general_purpose::STANDARD
            .decode(&self.value)
            .map(Bytes::from)
            .map_err(|e| Error::InvalidInput(format!("invalid base64 value: {}", e)))?;
        Ok(crate::Record { key, value })
    }
}

/// Request body for append operations.
#[derive(Debug, Deserialize)]
pub struct AppendBody {
    /// Records to append.
    pub records: Vec<AppendRecord>,
    /// Whether to wait for durable write.
    #[serde(default)]
    pub await_durable: bool,
}

impl AppendBody {
    /// Decode all records.
    pub fn decode_records(&self) -> Result<Vec<crate::Record>, Error> {
        self.records.iter().map(|r| r.decode()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_base64_key() {
        // given - "test" encoded as base64
        let params = ScanParams {
            key: "dGVzdA==".to_string(),
            start_seq: None,
            end_seq: None,
            limit: None,
        };

        // when
        let key = params.decode_key().unwrap();

        // then
        assert_eq!(key.as_ref(), b"test");
    }

    #[test]
    fn should_return_error_for_invalid_base64() {
        // given
        let params = ScanParams {
            key: "not-valid-base64!!!".to_string(),
            start_seq: None,
            end_seq: None,
            limit: None,
        };

        // when
        let result = params.decode_key();

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_decode_append_record() {
        // given - "key" and "value" encoded as base64
        let record = AppendRecord {
            key: "a2V5".to_string(),
            value: "dmFsdWU=".to_string(),
        };

        // when
        let decoded = record.decode().unwrap();

        // then
        assert_eq!(decoded.key.as_ref(), b"key");
        assert_eq!(decoded.value.as_ref(), b"value");
    }

    #[test]
    fn should_return_default_seq_range() {
        // given
        let params = ScanParams {
            key: "dGVzdA==".to_string(),
            start_seq: None,
            end_seq: None,
            limit: None,
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
            key: "dGVzdA==".to_string(),
            start_seq: Some(10),
            end_seq: Some(100),
            limit: None,
        };

        // when
        let range = params.seq_range();

        // then
        assert_eq!(range.start, 10);
        assert_eq!(range.end, 100);
    }
}

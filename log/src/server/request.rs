//! HTTP request types for the log server.

use bytes::Bytes;
use serde::Deserialize;

use crate::Error;

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

/// A single record in an append request.
#[derive(Debug, Deserialize)]
pub struct AppendRecord {
    /// Key (plain string).
    pub key: String,
    /// Value (plain string).
    pub value: String,
}

impl AppendRecord {
    /// Convert to a log Record.
    pub fn to_record(&self) -> Result<crate::Record, Error> {
        Ok(crate::Record {
            key: Bytes::from(self.key.clone()),
            value: Bytes::from(self.value.clone()),
        })
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
    /// Convert all records to log Records.
    pub fn to_records(&self) -> Result<Vec<crate::Record>, Error> {
        self.records.iter().map(|r| r.to_record()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_get_key_as_bytes() {
        // given
        let params = ScanParams {
            key: "my-key".to_string(),
            start_seq: None,
            end_seq: None,
            limit: None,
        };

        // when
        let key = params.key();

        // then
        assert_eq!(key.as_ref(), b"my-key");
    }

    #[test]
    fn should_convert_append_record() {
        // given
        let record = AppendRecord {
            key: "events".to_string(),
            value: r#"{"event": "click"}"#.to_string(),
        };

        // when
        let log_record = record.to_record().unwrap();

        // then
        assert_eq!(log_record.key.as_ref(), b"events");
        assert_eq!(log_record.value.as_ref(), br#"{"event": "click"}"#);
    }

    #[test]
    fn should_return_default_seq_range() {
        // given
        let params = ScanParams {
            key: "test".to_string(),
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
            key: "test".to_string(),
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

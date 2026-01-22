//! HTTP response types for the log server.

use serde::Serialize;

use crate::LogEntry;

/// A single entry in a scan response.
#[derive(Debug, Serialize)]
pub struct ScanEntry {
    /// Key (UTF-8 string).
    pub key: String,
    /// Sequence number.
    pub sequence: u64,
    /// Value (UTF-8 string).
    pub value: String,
}

impl From<&LogEntry> for ScanEntry {
    fn from(entry: &LogEntry) -> Self {
        Self {
            key: String::from_utf8_lossy(&entry.key).into_owned(),
            sequence: entry.sequence,
            value: String::from_utf8_lossy(&entry.value).into_owned(),
        }
    }
}

/// Response for scan requests.
#[derive(Debug, Serialize)]
pub struct ScanResponse {
    /// Status of the response.
    pub status: String,
    /// Entries returned by the scan.
    pub entries: Vec<ScanEntry>,
}

impl ScanResponse {
    /// Create a successful scan response.
    pub fn success(entries: Vec<ScanEntry>) -> Self {
        Self {
            status: "success".to_string(),
            entries,
        }
    }
}

/// Response for append requests.
#[derive(Debug, Serialize)]
pub struct AppendResponse {
    /// Status of the response.
    pub status: String,
    /// Number of records appended.
    pub records_appended: usize,
}

impl AppendResponse {
    /// Create a successful append response.
    pub fn success(records_appended: usize) -> Self {
        Self {
            status: "success".to_string(),
            records_appended,
        }
    }
}

/// A key entry in a list keys response.
#[derive(Debug, Serialize)]
pub struct KeyEntry {
    /// Key (UTF-8 string).
    pub key: String,
}

/// Response for list keys requests.
#[derive(Debug, Serialize)]
pub struct ListKeysResponse {
    /// Status of the response.
    pub status: String,
    /// Keys in the log.
    pub keys: Vec<KeyEntry>,
}

impl ListKeysResponse {
    /// Create a successful list keys response.
    pub fn success(keys: Vec<KeyEntry>) -> Self {
        Self {
            status: "success".to_string(),
            keys,
        }
    }
}

/// A segment entry in a list segments response.
#[derive(Debug, Serialize)]
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

/// Response for count requests.
#[derive(Debug, Serialize)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn should_convert_log_entry_to_scan_entry() {
        // given
        let entry = LogEntry {
            key: Bytes::from("test-key"),
            sequence: 42,
            value: Bytes::from("test-value"),
        };

        // when
        let scan_entry = ScanEntry::from(&entry);

        // then
        assert_eq!(scan_entry.key, "test-key");
        assert_eq!(scan_entry.sequence, 42);
        assert_eq!(scan_entry.value, "test-value");
    }

    #[test]
    fn should_create_success_scan_response() {
        // given
        let entries = vec![ScanEntry {
            key: "key".to_string(),
            sequence: 0,
            value: "value".to_string(),
        }];

        // when
        let response = ScanResponse::success(entries);

        // then
        assert_eq!(response.status, "success");
        assert_eq!(response.entries.len(), 1);
    }

    #[test]
    fn should_create_success_append_response() {
        // given/when
        let response = AppendResponse::success(5);

        // then
        assert_eq!(response.status, "success");
        assert_eq!(response.records_appended, 5);
    }
}

//! HTTP response types for the log server.

use base64::Engine;
use serde::Serialize;

use crate::LogEntry;

/// A single entry in a scan response.
#[derive(Debug, Serialize)]
pub struct ScanEntry {
    /// Base64-encoded key.
    pub key: String,
    /// Sequence number.
    pub sequence: u64,
    /// Base64-encoded value.
    pub value: String,
}

impl From<&LogEntry> for ScanEntry {
    fn from(entry: &LogEntry) -> Self {
        Self {
            key: base64::engine::general_purpose::STANDARD.encode(&entry.key),
            sequence: entry.sequence,
            value: base64::engine::general_purpose::STANDARD.encode(&entry.value),
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
    /// Base64-encoded key.
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
        assert_eq!(scan_entry.sequence, 42);
        // Verify base64 encoding
        assert_eq!(scan_entry.key, "dGVzdC1rZXk=");
        assert_eq!(scan_entry.value, "dGVzdC12YWx1ZQ==");
    }

    #[test]
    fn should_create_success_scan_response() {
        // given
        let entries = vec![ScanEntry {
            key: "a2V5".to_string(),
            sequence: 0,
            value: "dmFsdWU=".to_string(),
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

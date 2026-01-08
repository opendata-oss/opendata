#![allow(dead_code)]

//! Serde for log storage
//!
//! This module provides encoding and decoding for log records stored in SlateDB.
//! The encoding scheme is designed to preserve lexicographic ordering of keys
//! while supporting variable-length user keys.
//!
//! # Key Format
//!
//! All keys start with a version byte and record type discriminator:
//!
//! ```text
//! | version (u8) | type (u8) | ... record-specific fields ... |
//! ```
//!
//! # Record Types
//!
//! - `LogEntry` (0x01): User data entries with key and sequence number
//! - `SeqBlock` (0x02): Sequence number block allocation tracking
//!
//! # TerminatedBytes Encoding
//!
//! Variable-length user keys use a terminated encoding that preserves
//! lexicographic ordering. Keys are escaped and terminated with `0x00`:
//!
//! - `0x00` → `0x01 0x01`
//! - `0x01` → `0x01 0x02`
//! - `0xFF` → `0x01 0x03`
//! - All other bytes unchanged
//! - Terminated with `0x00` delimiter
//!
//! Using `0x00` as the terminator ensures shorter keys sort before longer
//! keys with the same prefix (e.g., "/foo" < "/foo/bar"). This simplifies
//! prefix-based range queries: start at `prefix + 0x00`, end at `prefix + 0xFF`.

use std::ops::{Bound, RangeBounds};

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::terminated_bytes;

use crate::error::Error;

impl From<common::serde::DeserializeError> for Error {
    fn from(err: common::serde::DeserializeError) -> Self {
        Error::Encoding(err.message)
    }
}

/// Key format version (currently 0x01)
pub const KEY_VERSION: u8 = 0x01;

/// Record type discriminators for log storage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// Log entry record containing user key, sequence, and value
    LogEntry = 0x01,
    /// Block allocation record for sequence number tracking
    SeqBlock = 0x02,
}

impl RecordType {
    /// Returns the ID of this record type
    pub fn id(&self) -> u8 {
        *self as u8
    }

    /// Converts a u8 id back to a RecordType
    pub fn from_id(id: u8) -> Result<Self, Error> {
        match id {
            0x01 => Ok(RecordType::LogEntry),
            0x02 => Ok(RecordType::SeqBlock),
            _ => Err(Error::Encoding(format!(
                "invalid record type: 0x{:02x}",
                id
            ))),
        }
    }
}

/// Key for a log entry record.
///
/// The key encodes the user key and sequence number in a format that
/// preserves lexicographic ordering:
///
/// ```text
/// | version (u8) | type (u8) | key (TerminatedBytes) | sequence (u64 BE) |
/// ```
///
/// This ensures that:
/// - All entries for a given key are contiguous
/// - Within a key, entries are ordered by sequence number
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntryKey {
    /// The user-provided key identifying the log stream
    pub key: Bytes,
    /// The sequence number assigned to this entry
    pub sequence: u64,
}

impl LogEntryKey {
    /// Creates a new log entry key
    pub fn new(key: Bytes, sequence: u64) -> Self {
        Self { key, sequence }
    }

    /// Encodes the key to bytes for storage
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(KEY_VERSION);
        buf.put_u8(RecordType::LogEntry.id());
        terminated_bytes::serialize(&self.key, &mut buf);
        buf.put_u64(self.sequence);
        buf.freeze()
    }

    /// Decodes a log entry key from bytes
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 2 {
            return Err(Error::Encoding(
                "buffer too short for log entry key".to_string(),
            ));
        }

        if data[0] != KEY_VERSION {
            return Err(Error::Encoding(format!(
                "invalid key version: expected 0x{:02x}, got 0x{:02x}",
                KEY_VERSION, data[0]
            )));
        }

        let record_type = RecordType::from_id(data[1])?;
        if record_type != RecordType::LogEntry {
            return Err(Error::Encoding(format!(
                "invalid record type: expected LogEntry, got {:?}",
                record_type
            )));
        }

        let mut buf = &data[2..];
        let key = terminated_bytes::deserialize(&mut buf)?;

        if buf.len() < 8 {
            return Err(Error::Encoding(
                "buffer too short for sequence number".to_string(),
            ));
        }

        let sequence = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);

        Ok(LogEntryKey { key, sequence })
    }

    /// Creates a prefix for scanning all entries of a given key.
    ///
    /// The prefix includes the version, type, and terminated key bytes,
    /// allowing efficient range scans for a single key's log.
    pub fn key_prefix(key: &[u8]) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(KEY_VERSION);
        buf.put_u8(RecordType::LogEntry.id());
        terminated_bytes::serialize(key, &mut buf);
        buf.freeze()
    }

    /// Creates a storage key range for scanning entries of a key within a sequence range.
    ///
    /// Converts a user key and sequence number range into a [`BytesRange`] suitable
    /// for storage scanning. The resulting range will match all log entries for the
    /// specified key whose sequence numbers fall within the given bounds.
    ///
    /// # Arguments
    ///
    /// * `key` - The user key identifying the log stream
    /// * `seq_range` - The sequence number range to scan (supports all Rust range types)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Scan sequences 10..20 for key "orders"
    /// let range = LogEntryKey::scan_range(b"orders", 10..20);
    ///
    /// // Scan all sequences from 100 onwards
    /// let range = LogEntryKey::scan_range(b"orders", 100..);
    ///
    /// // Scan all sequences
    /// let range = LogEntryKey::scan_range(b"orders", ..);
    /// ```
    pub fn scan_range(key: &[u8], seq_range: impl RangeBounds<u64>) -> BytesRange {
        let prefix = Self::key_prefix(key);

        let start = match seq_range.start_bound() {
            Bound::Included(&seq) => {
                let mut start_key = BytesMut::from(prefix.as_ref());
                start_key.put_u64(seq);
                Bound::Included(start_key.freeze())
            }
            Bound::Excluded(&seq) => {
                let mut start_key = BytesMut::from(prefix.as_ref());
                start_key.put_u64(seq);
                Bound::Excluded(start_key.freeze())
            }
            Bound::Unbounded => {
                let mut start_key = BytesMut::from(prefix.as_ref());
                start_key.put_u64(0);
                Bound::Included(start_key.freeze())
            }
        };

        let end = match seq_range.end_bound() {
            Bound::Included(&seq) => {
                let mut end_key = BytesMut::from(prefix.as_ref());
                end_key.put_u64(seq);
                Bound::Included(end_key.freeze())
            }
            Bound::Excluded(&seq) => {
                let mut end_key = BytesMut::from(prefix.as_ref());
                end_key.put_u64(seq);
                Bound::Excluded(end_key.freeze())
            }
            Bound::Unbounded => {
                let mut end_key = BytesMut::from(prefix.as_ref());
                end_key.put_u64(u64::MAX);
                Bound::Included(end_key.freeze())
            }
        };

        BytesRange::new(start, end)
    }
}

/// Key for the SeqBlock record (static, singleton key).
///
/// ```text
/// | version (u8) | type (u8) |
/// ```
///
/// There is exactly one SeqBlock record in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastSeqBlockKey;

impl LastSeqBlockKey {
    /// Encodes the SeqBlock key
    pub fn encode(&self) -> Bytes {
        Bytes::from(vec![KEY_VERSION, RecordType::SeqBlock.id()])
    }

    /// Decodes and validates a SeqBlock key
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 2 {
            return Err(Error::Encoding(
                "buffer too short for SeqBlock key".to_string(),
            ));
        }

        if data[0] != KEY_VERSION {
            return Err(Error::Encoding(format!(
                "invalid key version: expected 0x{:02x}, got 0x{:02x}",
                KEY_VERSION, data[0]
            )));
        }

        let record_type = RecordType::from_id(data[1])?;
        if record_type != RecordType::SeqBlock {
            return Err(Error::Encoding(format!(
                "invalid record type: expected SeqBlock, got {:?}",
                record_type
            )));
        }

        Ok(LastSeqBlockKey)
    }
}

/// Value for the SeqBlock record.
///
/// Stores the current sequence block allocation:
///
/// ```text
/// | base_sequence (u64 BE) | block_size (u64 BE) |
/// ```
///
/// The allocated range is `[base_sequence, base_sequence + block_size)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqBlock {
    /// Base sequence number of the allocated block
    pub base_sequence: u64,
    /// Size of the allocated block
    pub block_size: u64,
}

impl SeqBlock {
    /// Creates a new SeqBlock value
    pub fn new(base_sequence: u64, block_size: u64) -> Self {
        Self {
            base_sequence,
            block_size,
        }
    }

    /// Encodes the value to bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64(self.base_sequence);
        buf.put_u64(self.block_size);
        buf.freeze()
    }

    /// Decodes a SeqBlock value from bytes
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 16 {
            return Err(Error::Encoding(format!(
                "buffer too short for SeqBlock value: need 16 bytes, got {}",
                data.len()
            )));
        }

        let base_sequence = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let block_size = u64::from_be_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);

        Ok(SeqBlock {
            base_sequence,
            block_size,
        })
    }

    /// Returns the next sequence number after this block
    pub fn next_base(&self) -> u64 {
        self.base_sequence + self.block_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_convert_record_type_to_id_and_back() {
        // given
        let log_entry = RecordType::LogEntry;
        let seq_block = RecordType::SeqBlock;

        // when/then
        assert_eq!(log_entry.id(), 0x01);
        assert_eq!(seq_block.id(), 0x02);
        assert_eq!(RecordType::from_id(0x01).unwrap(), RecordType::LogEntry);
        assert_eq!(RecordType::from_id(0x02).unwrap(), RecordType::SeqBlock);
    }

    #[test]
    fn should_reject_invalid_record_type() {
        // given
        let invalid_byte = 0x99;

        // when
        let result = RecordType::from_id(invalid_byte);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_serialize_and_deserialize_log_entry_key() {
        // given
        let key = LogEntryKey::new(Bytes::from("orders"), 12345);

        // when
        let encoded = key.encode();
        let decoded = LogEntryKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_serialize_and_deserialize_log_entry_key_with_special_bytes() {
        // given - key containing all special bytes: terminator (0x00), escape (0x01), range end (0xFF)
        let key = LogEntryKey::new(Bytes::from_static(&[0x61, 0x00, 0x01, 0xFF, 0x62]), 99999);

        // when
        let encoded = key.encode();
        let decoded = LogEntryKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_serialize_log_entry_key_with_correct_structure() {
        // given
        let key = LogEntryKey::new(Bytes::from("test"), 256);

        // when
        let encoded = key.encode();

        // then
        assert_eq!(encoded[0], KEY_VERSION);
        assert_eq!(encoded[1], RecordType::LogEntry.id());
        // "test" + terminator = [t, e, s, t, 0x00]
        // sequence 256 = 0x0000000000000100
    }

    #[test]
    fn should_order_log_entries_by_key_then_sequence() {
        // given
        let entries = [
            LogEntryKey::new(Bytes::from("a"), 2),
            LogEntryKey::new(Bytes::from("a"), 1),
            LogEntryKey::new(Bytes::from("b"), 1),
            LogEntryKey::new(Bytes::from("a"), 3),
        ];

        // when
        let mut encoded: Vec<Bytes> = entries.iter().map(|e| e.encode()).collect();
        encoded.sort();

        // then - should be ordered: a:1, a:2, a:3, b:1
        let decoded: Vec<LogEntryKey> = encoded
            .iter()
            .map(|e| LogEntryKey::decode(e).unwrap())
            .collect();

        assert_eq!(decoded[0], LogEntryKey::new(Bytes::from("a"), 1));
        assert_eq!(decoded[1], LogEntryKey::new(Bytes::from("a"), 2));
        assert_eq!(decoded[2], LogEntryKey::new(Bytes::from("a"), 3));
        assert_eq!(decoded[3], LogEntryKey::new(Bytes::from("b"), 1));
    }

    #[test]
    fn should_generate_key_prefix_for_scanning() {
        // given
        let user_key = b"orders";
        let entry1 = LogEntryKey::new(Bytes::from_static(user_key), 1);
        let entry2 = LogEntryKey::new(Bytes::from_static(user_key), 100);
        let other_entry = LogEntryKey::new(Bytes::from("other"), 1);

        // when
        let prefix = LogEntryKey::key_prefix(user_key);

        // then
        assert!(entry1.encode().starts_with(&prefix));
        assert!(entry2.encode().starts_with(&prefix));
        assert!(!other_entry.encode().starts_with(&prefix));
    }

    #[test]
    fn should_fail_deserialize_log_entry_key_with_wrong_version() {
        // given
        let data = LogEntryKey::new(Bytes::from("test"), 1).encode();
        let mut modified = data.to_vec();
        modified[0] = 0x99; // wrong version

        // when
        let result = LogEntryKey::decode(&modified);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_fail_deserialize_log_entry_key_with_wrong_type() {
        // given - SeqBlock type instead of LogEntry
        let data = vec![KEY_VERSION, RecordType::SeqBlock.id()];

        // when
        let result = LogEntryKey::decode(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_serialize_and_deserialize_seq_block_key() {
        // given
        let key = LastSeqBlockKey;

        // when
        let encoded = key.encode();
        let decoded = LastSeqBlockKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0], KEY_VERSION);
        assert_eq!(encoded[1], RecordType::SeqBlock.id());
    }

    #[test]
    fn should_fail_deserialize_seq_block_key_with_wrong_type() {
        // given
        let data = vec![KEY_VERSION, RecordType::LogEntry.id()];

        // when
        let result = LastSeqBlockKey::decode(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_serialize_and_deserialize_seq_block_value() {
        // given
        let value = SeqBlock::new(1000, 100);

        // when
        let encoded = value.encode();
        let decoded = SeqBlock::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert_eq!(encoded.len(), 16);
    }

    #[test]
    fn should_calculate_next_base() {
        // given
        let value = SeqBlock::new(1000, 100);

        // when
        let next = value.next_base();

        // then
        assert_eq!(next, 1100);
    }

    #[test]
    fn should_fail_deserialize_seq_block_value_too_short() {
        // given
        let data = vec![0u8; 15]; // need 16 bytes

        // when
        let result = SeqBlock::decode(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_serialize_seq_block_value_in_big_endian() {
        // given
        let value = SeqBlock::new(0x0102030405060708, 0x1112131415161718);

        // when
        let encoded = value.encode();

        // then
        assert_eq!(
            encoded.as_ref(),
            &[
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // base_sequence
                0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // block_size
            ]
        );
    }
}

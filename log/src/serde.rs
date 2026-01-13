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
use common::serde::varint::var_u64;

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
    /// Segment metadata record
    SegmentMeta = 0x03,
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
            0x03 => Ok(RecordType::SegmentMeta),
            _ => Err(Error::Encoding(format!(
                "invalid record type: 0x{:02x}",
                id
            ))),
        }
    }
}

/// Key for a log entry record.
///
/// The key serializes the segment ID, user key, and sequence number in a format
/// that preserves lexicographic ordering:
///
/// ```text
/// | version (u8) | type (u8) | segment_id (u32 BE) | terminated_key | sequence (var_u64) |
/// ```
///
/// The sequence number uses variable-length encoding (see [`common::serde::varint::var_u64`])
/// to keep keys compact for small sequence numbers.
///
/// The ordering (segment_id before key) ensures entries are grouped by segment,
/// enabling efficient scans within a single segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntryKey {
    /// The segment this entry belongs to
    pub segment_id: SegmentId,
    /// The user-provided key identifying the log stream
    pub key: Bytes,
    /// The sequence number assigned to this entry
    pub sequence: u64,
}

impl LogEntryKey {
    /// Creates a new log entry key.
    pub fn new(segment_id: SegmentId, key: Bytes, sequence: u64) -> Self {
        Self {
            segment_id,
            key,
            sequence,
        }
    }

    /// Serializes the key to bytes for storage.
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(KEY_VERSION);
        buf.put_u8(RecordType::LogEntry.id());
        buf.put_u32(self.segment_id);
        terminated_bytes::serialize(&self.key, &mut buf);
        var_u64::serialize(self.sequence, &mut buf);
        buf.freeze()
    }

    /// Deserializes a log entry key from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 6 {
            return Err(Error::Encoding(
                "buffer too short for new log entry key".to_string(),
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

        let segment_id = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);

        let mut buf = &data[6..];
        let key = terminated_bytes::deserialize(&mut buf)?;
        let sequence = var_u64::deserialize(&mut buf)?;

        Ok(LogEntryKey {
            segment_id,
            key,
            sequence,
        })
    }

    /// Creates a prefix for scanning all entries in a segment for a given key.
    pub fn segment_key_prefix(segment_id: SegmentId, key: &[u8]) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(KEY_VERSION);
        buf.put_u8(RecordType::LogEntry.id());
        buf.put_u32(segment_id);
        terminated_bytes::serialize(key, &mut buf);
        buf.freeze()
    }

    /// Creates a storage key range for scanning entries within a segment.
    ///
    /// Returns a range that matches all entries for the given segment and key
    /// whose sequence numbers fall within the specified bounds.
    pub fn scan_range(
        segment_id: SegmentId,
        key: &[u8],
        seq_range: impl RangeBounds<u64>,
    ) -> BytesRange {
        let prefix = Self::segment_key_prefix(segment_id, key);

        let start = match seq_range.start_bound() {
            Bound::Included(&seq) => {
                let mut start_key = BytesMut::from(prefix.as_ref());
                var_u64::serialize(seq, &mut start_key);
                Bound::Included(start_key.freeze())
            }
            Bound::Excluded(&seq) => {
                let mut start_key = BytesMut::from(prefix.as_ref());
                var_u64::serialize(seq, &mut start_key);
                Bound::Excluded(start_key.freeze())
            }
            Bound::Unbounded => {
                let mut start_key = BytesMut::from(prefix.as_ref());
                var_u64::serialize(0, &mut start_key);
                Bound::Included(start_key.freeze())
            }
        };

        let end = match seq_range.end_bound() {
            Bound::Included(&seq) => {
                let mut end_key = BytesMut::from(prefix.as_ref());
                var_u64::serialize(seq, &mut end_key);
                Bound::Included(end_key.freeze())
            }
            Bound::Excluded(&seq) => {
                let mut end_key = BytesMut::from(prefix.as_ref());
                var_u64::serialize(seq, &mut end_key);
                Bound::Excluded(end_key.freeze())
            }
            Bound::Unbounded => {
                let mut end_key = BytesMut::from(prefix.as_ref());
                var_u64::serialize(u64::MAX, &mut end_key);
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
    pub fn serialize(&self) -> Bytes {
        Bytes::from(vec![KEY_VERSION, RecordType::SeqBlock.id()])
    }

    /// Decodes and validates a SeqBlock key
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
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
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64(self.base_sequence);
        buf.put_u64(self.block_size);
        buf.freeze()
    }

    /// Decodes a SeqBlock value from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
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

/// Segment identifier type.
pub type SegmentId = u32;

/// Key for a segment metadata record.
///
/// ```text
/// | version (u8) | type (u8=0x03) | segment_id (u32 BE) |
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMetaKey {
    /// The segment identifier
    pub segment_id: SegmentId,
}

impl SegmentMetaKey {
    /// Creates a new segment metadata key
    pub fn new(segment_id: SegmentId) -> Self {
        Self { segment_id }
    }

    /// Encodes the key to bytes for storage
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(6);
        buf.put_u8(KEY_VERSION);
        buf.put_u8(RecordType::SegmentMeta.id());
        buf.put_u32(self.segment_id);
        buf.freeze()
    }

    /// Decodes a segment metadata key from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 6 {
            return Err(Error::Encoding(
                "buffer too short for SegmentMeta key".to_string(),
            ));
        }

        if data[0] != KEY_VERSION {
            return Err(Error::Encoding(format!(
                "invalid key version: expected 0x{:02x}, got 0x{:02x}",
                KEY_VERSION, data[0]
            )));
        }

        let record_type = RecordType::from_id(data[1])?;
        if record_type != RecordType::SegmentMeta {
            return Err(Error::Encoding(format!(
                "invalid record type: expected SegmentMeta, got {:?}",
                record_type
            )));
        }

        let segment_id = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);

        Ok(SegmentMetaKey { segment_id })
    }

    /// Creates a storage key range for scanning segment metadata within a segment ID range.
    pub fn scan_range(range: impl RangeBounds<SegmentId>) -> BytesRange {
        let start = match range.start_bound() {
            Bound::Included(&id) => Bound::Included(SegmentMetaKey::new(id).serialize()),
            Bound::Excluded(&id) => Bound::Excluded(SegmentMetaKey::new(id).serialize()),
            Bound::Unbounded => Bound::Included(SegmentMetaKey::new(0).serialize()),
        };

        let end = match range.end_bound() {
            Bound::Included(&id) => Bound::Included(SegmentMetaKey::new(id).serialize()),
            Bound::Excluded(&id) => Bound::Excluded(SegmentMetaKey::new(id).serialize()),
            Bound::Unbounded => Bound::Included(SegmentMetaKey::new(SegmentId::MAX).serialize()),
        };

        BytesRange::new(start, end)
    }
}

/// Value for a segment metadata record.
///
/// ```text
/// | start_seq (u64 BE) | start_time_ms (i64 BE) |
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    /// The first sequence number in this segment
    pub start_seq: u64,
    /// Wall-clock time when this segment was created (milliseconds since epoch)
    pub start_time_ms: i64,
}

impl SegmentMeta {
    /// Creates a new segment metadata value
    pub fn new(start_seq: u64, start_time_ms: i64) -> Self {
        Self {
            start_seq,
            start_time_ms,
        }
    }

    /// Encodes the value to bytes
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_u64(self.start_seq);
        buf.put_i64(self.start_time_ms);
        buf.freeze()
    }

    /// Decodes a segment metadata value from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 16 {
            return Err(Error::Encoding(format!(
                "buffer too short for SegmentMeta value: need 16 bytes, got {}",
                data.len()
            )));
        }

        let start_seq = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let start_time_ms = i64::from_be_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);

        Ok(SegmentMeta {
            start_seq,
            start_time_ms,
        })
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
    fn should_serialize_and_deserialize_seq_block_key() {
        // given
        let key = LastSeqBlockKey;

        // when
        let serialized = key.serialize();
        let deserialized = LastSeqBlockKey::deserialize(&serialized).unwrap();

        // then
        assert_eq!(deserialized, key);
        assert_eq!(serialized.len(), 2);
        assert_eq!(serialized[0], KEY_VERSION);
        assert_eq!(serialized[1], RecordType::SeqBlock.id());
    }

    #[test]
    fn should_fail_deserialize_seq_block_key_with_wrong_type() {
        // given
        let data = vec![KEY_VERSION, RecordType::LogEntry.id()];

        // when
        let result = LastSeqBlockKey::deserialize(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_serialize_and_deserialize_seq_block_value() {
        // given
        let value = SeqBlock::new(1000, 100);

        // when
        let serialized = value.serialize();
        let deserialized = SeqBlock::deserialize(&serialized).unwrap();

        // then
        assert_eq!(deserialized, value);
        assert_eq!(serialized.len(), 16);
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
        let result = SeqBlock::deserialize(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_serialize_seq_block_value_in_big_endian() {
        // given
        let value = SeqBlock::new(0x0102030405060708, 0x1112131415161718);

        // when
        let serialized = value.serialize();

        // then
        assert_eq!(
            serialized.as_ref(),
            &[
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // base_sequence
                0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // block_size
            ]
        );
    }

    #[test]
    fn should_serialize_and_deserialize_log_entry_key() {
        // given
        let key = LogEntryKey::new(42, Bytes::from("test_key"), 12345);

        // when
        let serialized = key.serialize();
        let deserialized = LogEntryKey::deserialize(&serialized).unwrap();

        // then
        assert_eq!(deserialized.segment_id, 42);
        assert_eq!(deserialized.key, Bytes::from("test_key"));
        assert_eq!(deserialized.sequence, 12345);
    }

    #[test]
    fn should_serialize_log_entry_key_with_correct_structure() {
        // given
        let key = LogEntryKey::new(1, Bytes::from("k"), 100);

        // when
        let serialized = key.serialize();

        // then
        // version (1) + type (1) + segment_id (4) + key "k" (1) + terminator (1) + sequence (varint, 2 bytes for 100) = 10
        assert_eq!(serialized.len(), 10);
        assert_eq!(serialized[0], KEY_VERSION);
        assert_eq!(serialized[1], RecordType::LogEntry.id());
        // segment_id = 1 in big endian
        assert_eq!(&serialized[2..6], &[0, 0, 0, 1]);
        // key "k" + terminator
        assert_eq!(serialized[6], b'k');
        assert_eq!(serialized[7], 0x00); // terminator
        // sequence = 100 as varint: length code 1 (2 bytes total), value 100
        // First byte: (1 << 4) | (100 >> 8) = 0x10
        // Second byte: 100 & 0xFF = 0x64
        assert_eq!(&serialized[8..10], &[0x10, 0x64]);
    }

    #[test]
    fn should_order_log_entries_by_segment_then_key_then_sequence() {
        // given
        let key1 = LogEntryKey::new(0, Bytes::from("a"), 1);
        let key2 = LogEntryKey::new(0, Bytes::from("a"), 2);
        let key3 = LogEntryKey::new(0, Bytes::from("b"), 1);
        let key4 = LogEntryKey::new(1, Bytes::from("a"), 1);

        // when
        let s1 = key1.serialize();
        let s2 = key2.serialize();
        let s3 = key3.serialize();
        let s4 = key4.serialize();

        // then - segment_id ordering takes precedence
        assert!(s1 < s2, "same segment/key, seq 1 < seq 2");
        assert!(s2 < s3, "same segment, key 'a' < key 'b'");
        assert!(s3 < s4, "segment 0 < segment 1");
    }

    #[test]
    fn should_generate_segment_key_prefix() {
        // given
        let segment_id = 5;
        let key = b"orders";

        // when
        let prefix = LogEntryKey::segment_key_prefix(segment_id, key);

        // then
        // version (1) + type (1) + segment_id (4) + key (6) + terminator (1) = 13
        assert_eq!(prefix.len(), 13);
        assert_eq!(prefix[0], KEY_VERSION);
        assert_eq!(prefix[1], RecordType::LogEntry.id());
        assert_eq!(&prefix[2..6], &[0, 0, 0, 5]); // segment_id
        assert_eq!(&prefix[6..12], b"orders");
        assert_eq!(prefix[12], 0x00); // terminator
    }

    #[test]
    fn should_fail_deserialize_log_entry_key_too_short() {
        // given
        let data = vec![KEY_VERSION, RecordType::LogEntry.id(), 0, 0, 0]; // only 5 bytes

        // when
        let result = LogEntryKey::deserialize(&data);

        // then
        assert!(result.is_err());
    }
}

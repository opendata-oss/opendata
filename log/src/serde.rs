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
//! - `SeqBlock` (0x02): Sequence number block allocation tracking (value type in `common` crate)
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

use std::ops::{Bound, Range};

use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::{KeyPrefix, RecordTag};
use common::serde::terminated_bytes;
use common::serde::varint::var_u64;

use crate::error::Error;
use crate::segment::LogSegment;

impl From<common::serde::DeserializeError> for Error {
    fn from(err: common::serde::DeserializeError) -> Self {
        Error::Encoding(err.message)
    }
}

/// Key format version (currently 0x01)
pub const KEY_VERSION: u8 = 0x01;

/// Storage key for the SeqBlock record.
pub const SEQ_BLOCK_KEY: [u8; 2] = [KEY_VERSION, 0x02]; // RecordType::SeqBlock

/// Record type discriminators for log storage.
///
/// Record types are encoded in the high 4 bits of the record tag byte,
/// following RFC 0001: Record Key Prefix. The low 4 bits are reserved
/// (set to 0 for log records).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// Log entry record containing user key, sequence, and value
    LogEntry = 0x01,
    /// Block allocation record for sequence number tracking
    SeqBlock = 0x02,
    /// Segment metadata record
    SegmentMeta = 0x03,
    /// Listing entry record for key discovery
    ListingEntry = 0x04,
}

impl RecordType {
    /// Returns the record type ID (1-15).
    pub fn id(&self) -> u8 {
        *self as u8
    }

    /// Converts a record type ID back to a RecordType.
    pub fn from_id(id: u8) -> Result<Self, Error> {
        match id {
            0x01 => Ok(RecordType::LogEntry),
            0x02 => Ok(RecordType::SeqBlock),
            0x03 => Ok(RecordType::SegmentMeta),
            0x04 => Ok(RecordType::ListingEntry),
            _ => Err(Error::Encoding(format!(
                "invalid record type: 0x{:02x}",
                id
            ))),
        }
    }

    /// Creates a RecordTag for this record type.
    ///
    /// Log records use 0 for the reserved bits.
    pub fn tag(&self) -> RecordTag {
        RecordTag::new(self.id(), 0)
    }

    /// Creates a KeyPrefix for this record type with the current version.
    pub fn prefix(&self) -> KeyPrefix {
        KeyPrefix::new(KEY_VERSION, self.tag())
    }
}

/// Key for a log entry record.
///
/// The key serializes the segment ID, user key, and relative sequence number in a format
/// that preserves lexicographic ordering:
///
/// ```text
/// | version (u8) | type (u8) | segment_id (u32 BE) | terminated_key | relative_seq (var_u64) |
/// ```
///
/// The `relative_seq` is the entry's sequence number relative to the segment's `start_seq`
/// (i.e., it resets to 0 at the start of each segment). This keeps keys compact since most
/// relative offsets within a segment are small. The sequence number uses variable-length
/// encoding (see [`common::serde::varint::var_u64`]).
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
    ///
    /// The sequence number is stored relative to `segment_start_seq`, so the
    /// caller must provide the segment's start sequence.
    pub fn serialize(&self, segment_start_seq: u64) -> Bytes {
        let relative_seq = self.sequence - segment_start_seq;
        let mut buf = BytesMut::new();
        RecordType::LogEntry.prefix().write_to(&mut buf);
        buf.put_u32(self.segment_id);
        terminated_bytes::serialize(&self.key, &mut buf);
        var_u64::serialize(relative_seq, &mut buf);
        buf.freeze()
    }

    /// Deserializes a log entry key from bytes.
    ///
    /// The sequence number is stored relative to `segment_start_seq`, so the
    /// caller must provide the segment's start sequence to recover the absolute
    /// sequence number.
    pub fn deserialize(data: &[u8], segment_start_seq: u64) -> Result<Self, Error> {
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        let record_type = RecordType::from_id(prefix.tag().record_type())?;
        if record_type != RecordType::LogEntry {
            return Err(Error::Encoding(format!(
                "invalid record type: expected LogEntry, got {:?}",
                record_type
            )));
        }

        if data.len() < 6 {
            return Err(Error::Encoding(
                "buffer too short for log entry key".to_string(),
            ));
        }

        let segment_id = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);

        let mut buf = &data[6..];
        let key = terminated_bytes::deserialize(&mut buf)?;
        let relative_seq = var_u64::deserialize(&mut buf)?;
        let sequence = segment_start_seq + relative_seq;

        Ok(LogEntryKey {
            segment_id,
            key,
            sequence,
        })
    }

    /// Creates a storage key range for scanning entries within a segment.
    ///
    /// Returns a range that matches all entries for the given segment and key
    /// whose sequence numbers fall within the specified range (inclusive start,
    /// exclusive end).
    pub fn scan_range(segment: &LogSegment, key: &[u8], seq_range: Range<u64>) -> BytesRange {
        let start_key = Self::build_scan_key(segment, key, seq_range.start);
        let end_key = Self::build_scan_key(segment, key, seq_range.end);
        BytesRange::new(Bound::Included(start_key), Bound::Excluded(end_key))
    }

    /// Builds a complete scan key with segment prefix and relative sequence.
    fn build_scan_key(segment: &LogSegment, key: &[u8], seq: u64) -> Bytes {
        let relative_seq = seq.saturating_sub(segment.meta().start_seq);
        let mut buf = BytesMut::new();
        RecordType::LogEntry.prefix().write_to(&mut buf);
        buf.put_u32(segment.id());
        terminated_bytes::serialize(key, &mut buf);
        var_u64::serialize(relative_seq, &mut buf);
        buf.freeze()
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
        RecordType::SegmentMeta.prefix().write_to(&mut buf);
        buf.put_u32(self.segment_id);
        buf.freeze()
    }

    /// Decodes a segment metadata key from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        let record_type = RecordType::from_id(prefix.tag().record_type())?;
        if record_type != RecordType::SegmentMeta {
            return Err(Error::Encoding(format!(
                "invalid record type: expected SegmentMeta, got {:?}",
                record_type
            )));
        }

        if data.len() < 6 {
            return Err(Error::Encoding(
                "buffer too short for SegmentMeta key".to_string(),
            ));
        }

        let segment_id = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);

        Ok(SegmentMetaKey { segment_id })
    }

    /// Creates a storage key range for scanning segment metadata within a segment ID range.
    pub fn scan_range(range: Range<SegmentId>) -> BytesRange {
        let start = Bound::Included(SegmentMetaKey::new(range.start).serialize());
        let end = Bound::Excluded(SegmentMetaKey::new(range.end).serialize());
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

/// Key for a listing entry record.
///
/// Tracks key presence within a segment for efficient key enumeration.
/// The key format places segment_id before the user key, ensuring all
/// listing records for a segment are contiguous for efficient prefix scans.
///
/// ```text
/// | version (u8) | type (u8=0x04) | segment_id (u32 BE) | key (Bytes) |
/// ```
///
/// Unlike log entry keys, the user key is stored as raw bytes without
/// terminated encoding since it occupies the suffix position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListingEntryKey {
    /// The segment this listing entry belongs to
    pub segment_id: SegmentId,
    /// The user-provided key
    pub key: Bytes,
}

impl ListingEntryKey {
    /// Creates a new listing entry key.
    pub fn new(segment_id: SegmentId, key: Bytes) -> Self {
        Self { segment_id, key }
    }

    /// Serializes the key to bytes for storage.
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::new();
        RecordType::ListingEntry.prefix().write_to(&mut buf);
        buf.put_u32(self.segment_id);
        buf.put_slice(&self.key);
        buf.freeze()
    }

    /// Deserializes a listing entry key from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        let record_type = RecordType::from_id(prefix.tag().record_type())?;
        if record_type != RecordType::ListingEntry {
            return Err(Error::Encoding(format!(
                "invalid record type: expected ListingEntry, got {:?}",
                record_type
            )));
        }

        if data.len() < 6 {
            return Err(Error::Encoding(
                "buffer too short for listing entry key".to_string(),
            ));
        }

        let segment_id = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);
        let key = Bytes::copy_from_slice(&data[6..]);

        Ok(ListingEntryKey { segment_id, key })
    }

    /// Creates a storage key range for scanning listing entries across segments.
    ///
    /// Returns a range that matches all listing entries for segments within
    /// the specified range [start, end).
    pub fn scan_range(range: Range<SegmentId>) -> BytesRange {
        let start = Bound::Included(Self::segment_prefix(range.start));
        let end = Bound::Excluded(Self::segment_prefix(range.end));
        BytesRange::new(start, end)
    }

    /// Returns the prefix key for a segment (smallest possible key for segment).
    fn segment_prefix(segment_id: SegmentId) -> Bytes {
        let mut buf = BytesMut::with_capacity(6);
        RecordType::ListingEntry.prefix().write_to(&mut buf);
        buf.put_u32(segment_id);
        buf.freeze()
    }
}

/// Value for a listing entry record.
///
/// The value is empty—presence of the record indicates the key exists
/// in the segment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListingEntryValue;

impl ListingEntryValue {
    /// Creates a new listing entry value.
    pub fn new() -> Self {
        Self
    }

    /// Serializes the value to bytes for storage.
    pub fn serialize(&self) -> Bytes {
        Bytes::new()
    }

    /// Deserializes a listing entry value from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        if !data.is_empty() {
            return Err(Error::Encoding(format!(
                "listing entry value should be empty, got {} bytes",
                data.len()
            )));
        }
        Ok(ListingEntryValue)
    }
}

/// Builder for log entry storage records.
///
/// Converts user records into storage records with properly encoded keys.
/// Uses the delta pattern: receives segment and sequence info from their
/// respective deltas and produces storage records to be written atomically.
///
/// # Usage
///
/// ```ignore
/// use common::Record as StorageRecord;
///
/// let mut records = Vec::new();
/// // ... build sequence and segment deltas ...
///
/// LogEntryBuilder::build(&seg_delta, &seq_delta, &user_records, &mut records);
/// ```
pub(crate) struct LogEntryBuilder;

impl LogEntryBuilder {
    /// Builds log entry storage records from user records.
    ///
    /// For each user record, creates a storage record with:
    /// - Key: Encoded `LogEntryKey` with segment_id, user key, and sequence
    /// - Value: The user-provided value (unchanged)
    ///
    /// Records are appended to the provided `records` vec.
    pub(crate) fn build(
        segment: &crate::segment::LogSegment,
        base_sequence: u64,
        user_records: &[crate::model::Record],
        records: &mut Vec<common::Record>,
    ) {
        let segment_start_seq = segment.meta().start_seq;

        for (i, user_record) in user_records.iter().enumerate() {
            let sequence = base_sequence + i as u64;
            let entry_key = LogEntryKey::new(segment.id(), user_record.key.clone(), sequence);
            let storage_record = common::Record::new(
                entry_key.serialize(segment_start_seq),
                user_record.value.clone(),
            );
            records.push(storage_record);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::RangeBounds;

    #[test]
    fn should_convert_record_type_to_id_and_back() {
        // given
        let log_entry = RecordType::LogEntry;
        let seq_block = RecordType::SeqBlock;
        let segment_meta = RecordType::SegmentMeta;
        let listing_entry = RecordType::ListingEntry;

        // when/then
        assert_eq!(log_entry.id(), 0x01);
        assert_eq!(seq_block.id(), 0x02);
        assert_eq!(segment_meta.id(), 0x03);
        assert_eq!(listing_entry.id(), 0x04);
        assert_eq!(RecordType::from_id(0x01).unwrap(), RecordType::LogEntry);
        assert_eq!(RecordType::from_id(0x02).unwrap(), RecordType::SeqBlock);
        assert_eq!(RecordType::from_id(0x03).unwrap(), RecordType::SegmentMeta);
        assert_eq!(RecordType::from_id(0x04).unwrap(), RecordType::ListingEntry);
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
        let segment_start_seq = 10000;
        let key = LogEntryKey::new(42, Bytes::from("test_key"), 12345);

        // when
        let serialized = key.serialize(segment_start_seq);
        let deserialized = LogEntryKey::deserialize(&serialized, segment_start_seq).unwrap();

        // then
        assert_eq!(deserialized.segment_id, 42);
        assert_eq!(deserialized.key, Bytes::from("test_key"));
        assert_eq!(deserialized.sequence, 12345);
    }

    #[test]
    fn should_serialize_log_entry_key_with_correct_structure() {
        // given
        let segment_start_seq = 0;
        let key = LogEntryKey::new(1, Bytes::from("k"), 100);

        // when
        let serialized = key.serialize(segment_start_seq);

        // then
        // version (1) + tag (1) + segment_id (4) + key "k" (1) + terminator (1) + relative_seq (varint, 2 bytes for 100) = 10
        assert_eq!(serialized.len(), 10);
        assert_eq!(serialized[0], KEY_VERSION);
        // Record tag: type 0x01 in high nibble, reserved 0x00 in low nibble = 0x10
        assert_eq!(serialized[1], RecordType::LogEntry.tag().as_byte());
        assert_eq!(serialized[1], 0x10);
        // segment_id = 1 in big endian
        assert_eq!(&serialized[2..6], &[0, 0, 0, 1]);
        // key "k" + terminator
        assert_eq!(serialized[6], b'k');
        assert_eq!(serialized[7], 0x00); // terminator
        // relative_seq = 100 as varint: length code 1 (2 bytes total), value 100
        // First byte: (1 << 4) | (100 >> 8) = 0x10
        // Second byte: 100 & 0xFF = 0x64
        assert_eq!(&serialized[8..10], &[0x10, 0x64]);
    }

    #[test]
    fn should_serialize_relative_sequence() {
        // given
        let segment_start_seq = 1000;
        let key = LogEntryKey::new(1, Bytes::from("k"), 1005); // relative_seq = 5

        // when
        let serialized = key.serialize(segment_start_seq);

        // then
        // relative_seq = 5 fits in 1 byte (length code 0)
        // version (1) + type (1) + segment_id (4) + key "k" (1) + terminator (1) + relative_seq (1) = 9
        assert_eq!(serialized.len(), 9);
        // relative_seq = 5 as varint: length code 0, value 5
        assert_eq!(serialized[8], 0x05);
    }

    #[test]
    fn should_order_log_entries_by_segment_then_key_then_sequence() {
        // given - all in segment 0 with start_seq 0
        let segment_start_seq = 0;
        let key1 = LogEntryKey::new(0, Bytes::from("a"), 1);
        let key2 = LogEntryKey::new(0, Bytes::from("a"), 2);
        let key3 = LogEntryKey::new(0, Bytes::from("b"), 1);
        // segment 1 has its own start_seq
        let segment1_start_seq = 100;
        let key4 = LogEntryKey::new(1, Bytes::from("a"), 101);

        // when
        let s1 = key1.serialize(segment_start_seq);
        let s2 = key2.serialize(segment_start_seq);
        let s3 = key3.serialize(segment_start_seq);
        let s4 = key4.serialize(segment1_start_seq);

        // then - segment_id ordering takes precedence
        assert!(s1 < s2, "same segment/key, seq 1 < seq 2");
        assert!(s2 < s3, "same segment, key 'a' < key 'b'");
        assert!(s3 < s4, "segment 0 < segment 1");
    }

    #[test]
    fn should_create_record_tag() {
        // given/when
        let log_entry_tag = RecordType::LogEntry.tag();
        let seq_block_tag = RecordType::SeqBlock.tag();
        let segment_meta_tag = RecordType::SegmentMeta.tag();
        let listing_entry_tag = RecordType::ListingEntry.tag();

        // then - record type in high 4 bits, reserved (0) in low 4 bits
        assert_eq!(log_entry_tag.as_byte(), 0x10);
        assert_eq!(seq_block_tag.as_byte(), 0x20);
        assert_eq!(segment_meta_tag.as_byte(), 0x30);
        assert_eq!(listing_entry_tag.as_byte(), 0x40);
    }

    #[test]
    fn should_fail_deserialize_log_entry_key_too_short() {
        // given
        let data = vec![KEY_VERSION, RecordType::LogEntry.tag().as_byte(), 0, 0, 0]; // only 5 bytes

        // when
        let result = LogEntryKey::deserialize(&data, 0);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_serialize_and_deserialize_listing_entry_key() {
        // given
        let key = ListingEntryKey::new(42, Bytes::from("test_key"));

        // when
        let serialized = key.serialize();
        let deserialized = ListingEntryKey::deserialize(&serialized).unwrap();

        // then
        assert_eq!(deserialized.segment_id, 42);
        assert_eq!(deserialized.key, Bytes::from("test_key"));
    }

    #[test]
    fn should_serialize_listing_entry_key_with_correct_structure() {
        // given
        let key = ListingEntryKey::new(1, Bytes::from("k"));

        // when
        let serialized = key.serialize();

        // then
        // version (1) + tag (1) + segment_id (4) + key "k" (1) = 7
        assert_eq!(serialized.len(), 7);
        assert_eq!(serialized[0], KEY_VERSION);
        // Record tag: type 0x04 in high nibble, reserved 0x00 in low nibble = 0x40
        assert_eq!(serialized[1], RecordType::ListingEntry.tag().as_byte());
        assert_eq!(serialized[1], 0x40);
        // segment_id = 1 in big endian
        assert_eq!(&serialized[2..6], &[0, 0, 0, 1]);
        // key "k" (raw bytes, no terminator)
        assert_eq!(serialized[6], b'k');
    }

    #[test]
    fn should_serialize_listing_entry_key_with_empty_key() {
        // given
        let key = ListingEntryKey::new(1, Bytes::new());

        // when
        let serialized = key.serialize();
        let deserialized = ListingEntryKey::deserialize(&serialized).unwrap();

        // then
        assert_eq!(serialized.len(), 6); // version + tag + segment_id only
        assert_eq!(deserialized.segment_id, 1);
        assert_eq!(deserialized.key, Bytes::new());
    }

    #[test]
    fn should_order_listing_entries_by_segment_then_key() {
        // given
        let key1 = ListingEntryKey::new(0, Bytes::from("a"));
        let key2 = ListingEntryKey::new(0, Bytes::from("b"));
        let key3 = ListingEntryKey::new(1, Bytes::from("a"));

        // when
        let s1 = key1.serialize();
        let s2 = key2.serialize();
        let s3 = key3.serialize();

        // then
        assert!(s1 < s2, "same segment, key 'a' < key 'b'");
        assert!(s2 < s3, "segment 0 < segment 1");
    }

    #[test]
    fn should_create_listing_entry_scan_range() {
        // given
        let range = 1..3;

        // when
        let scan_range = ListingEntryKey::scan_range(range);

        // then
        let start_key = ListingEntryKey::new(1, Bytes::new()).serialize();
        let end_key = ListingEntryKey::new(3, Bytes::new()).serialize();

        // Range should be [segment 1 prefix, segment 3 prefix)
        assert_eq!(scan_range.start_bound(), Bound::Included(&start_key));
        assert_eq!(scan_range.end_bound(), Bound::Excluded(&end_key));
    }

    #[test]
    fn should_serialize_and_deserialize_listing_entry_value() {
        // given
        let value = ListingEntryValue::new();

        // when
        let serialized = value.serialize();
        let deserialized = ListingEntryValue::deserialize(&serialized).unwrap();

        // then
        assert!(serialized.is_empty());
        assert_eq!(deserialized, ListingEntryValue);
    }

    #[test]
    fn should_fail_deserialize_listing_entry_value_with_data() {
        // given
        let data = vec![0x01, 0x02];

        // when
        let result = ListingEntryValue::deserialize(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_fail_deserialize_listing_entry_key_too_short() {
        // given
        let data = vec![
            KEY_VERSION,
            RecordType::ListingEntry.tag().as_byte(),
            0,
            0,
            0,
        ]; // only 5 bytes

        // when
        let result = ListingEntryKey::deserialize(&data);

        // then
        assert!(result.is_err());
    }

    mod proptests {
        use proptest::prelude::*;

        use super::*;

        proptest! {
            #[test]
            fn should_preserve_sequence_ordering(a: u64, b: u64) {
                let segment_start_seq = 0;
                let key_a = LogEntryKey::new(0, Bytes::from("key"), a);
                let key_b = LogEntryKey::new(0, Bytes::from("key"), b);

                let enc_a = key_a.serialize(segment_start_seq);
                let enc_b = key_b.serialize(segment_start_seq);

                prop_assert_eq!(
                    a.cmp(&b),
                    enc_a.cmp(&enc_b),
                    "ordering mismatch: a={}, b={}, enc_a={:?}, enc_b={:?}",
                    a, b, enc_a.as_ref(), enc_b.as_ref()
                );
            }

            #[test]
            fn should_include_listing_entry_in_scan_range(
                start in 0u32..1000,
                range_size in 1u32..100,
                offset in 0u32..100,
                key_bytes in prop::collection::vec(any::<u8>(), 1..100),
            ) {
                let end = start.saturating_add(range_size);
                let segment_id = start.saturating_add(offset % range_size);

                let key = ListingEntryKey::new(segment_id, Bytes::from(key_bytes));
                let serialized = key.serialize();

                let scan_range = ListingEntryKey::scan_range(start..end);

                prop_assert!(
                    scan_range.contains(&serialized),
                    "listing entry for segment {} with key should be in range {}..{}, \
                     serialized={:?}, range_start={:?}, range_end={:?}",
                    segment_id, start, end,
                    serialized.as_ref(),
                    scan_range.start_bound(),
                    scan_range.end_bound()
                );
            }
        }
    }
}

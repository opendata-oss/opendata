#![allow(dead_code)]

//! Serde for log storage
//!
//! This module provides encoding and decoding for log records stored in SlateDB.
//! The encoding scheme is designed to preserve lexicographic ordering of keys
//! while supporting variable-length user keys.
//!
//! # Key Format
//!
//! All log storage keys share a 7-byte segmented prefix:
//!
//! ```text
//! | subsystem (u8) | version (u8) | segment_id (u32 BE) | record_type (u8) | ... |
//! ```
//!
//! Placing `segment_id` before `record_type` lets a SlateDB segment extractor
//! recognize the segment boundary with a fixed 6-byte prefix
//! `[subsystem, version, segment_id]`, routing all records for a given
//! segment to the same SlateDB segment regardless of record type.
//!
//! # System Segment
//!
//! Segment id `0` is reserved as the **system segment**: it never holds user
//! log entries. Records that are not bound to a specific user segment (the
//! global sequence block, and per-segment metadata records describing other
//! segments) live in segment `0`. Real user segments are numbered from `1`.
//!
//! # Record Types
//!
//! - `LogEntry`     (`0x10`): user data entry, in its owning segment
//! - `SeqBlock`     (`0x20`): global sequence block, in the system segment
//! - `SegmentMeta`  (`0x30`): per-segment metadata, in the system segment
//! - `ListingEntry` (`0x40`): per-segment key listing, in its owning segment
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
use common::serde::key_prefix::{KEY_PREFIX_LEN, KeyPrefix};
use common::serde::record_tag::RecordTag;
use common::serde::terminated_bytes;
use common::serde::varint::var_u64;

use crate::error::Error;
use crate::model::SegmentId;
use crate::segment::LogSegment;

impl From<common::serde::DeserializeError> for Error {
    fn from(err: common::serde::DeserializeError) -> Self {
        Error::Encoding(err.message)
    }
}

/// Key format version (currently 0x02 — segment_id precedes record_type).
pub const KEY_VERSION: u8 = 0x02;

/// Subsystem byte for log storage (see [`common::serde::subsystem`]).
pub const SUBSYSTEM: u8 = common::serde::subsystem::LOG;

/// Reserved segment id for system records (sequence block, per-segment
/// metadata describing other segments). User log segments start at `1`.
pub const SYSTEM_SEGMENT_ID: SegmentId = 0;

/// First segment id assigned to user log data. Segment `0` is reserved as the
/// system segment.
pub const FIRST_USER_SEGMENT_ID: SegmentId = 1;

/// Storage key for the SeqBlock record (lives in the system segment).
pub const SEQ_BLOCK_KEY: [u8; 7] = [
    SUBSYSTEM,
    KEY_VERSION,
    0,
    0,
    0,
    0,    // SYSTEM_SEGMENT_ID encoded as u32 BE
    0x20, // RecordType::SeqBlock tag byte
];

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
}

/// Offset of the segment id (u32 BE) inside a log key, immediately after the
/// 2-byte common prefix.
const SEGMENT_ID_OFFSET: usize = KEY_PREFIX_LEN;

/// Offset of the record-type tag byte inside a log key, after the segment id.
const RECORD_TYPE_OFFSET: usize = SEGMENT_ID_OFFSET + 4;

/// Length of the v2 segmented key prefix: `[subsystem, version, seg_id(4), record_type]`.
const SEGMENTED_PREFIX_LEN: usize = RECORD_TYPE_OFFSET + 1;

/// Writes the v2 segmented prefix into `buf`:
/// `[subsystem, version, seg_id BE, record_type]`.
fn write_segmented_prefix(buf: &mut BytesMut, segment_id: SegmentId, record_type: u8) {
    KeyPrefix::new(SUBSYSTEM, KEY_VERSION).write_to(buf);
    buf.put_u32(segment_id);
    buf.put_u8(record_type);
}

/// Parses a v2 segmented prefix, validating the subsystem/version/tag.
/// Returns the segment id along with the remaining (post-prefix) bytes.
fn parse_segmented_prefix(data: &[u8], expected_tag: u8) -> Result<(SegmentId, &[u8]), Error> {
    KeyPrefix::from_bytes_with_validation(data, SUBSYSTEM, KEY_VERSION)?;
    if data.len() < SEGMENTED_PREFIX_LEN {
        return Err(Error::Encoding(format!(
            "buffer too short for log key: need {} bytes, got {}",
            SEGMENTED_PREFIX_LEN,
            data.len()
        )));
    }
    let segment_id = u32::from_be_bytes([
        data[SEGMENT_ID_OFFSET],
        data[SEGMENT_ID_OFFSET + 1],
        data[SEGMENT_ID_OFFSET + 2],
        data[SEGMENT_ID_OFFSET + 3],
    ]);
    if data[RECORD_TYPE_OFFSET] != expected_tag {
        return Err(Error::Encoding(format!(
            "invalid record tag: expected 0x{:02x}, got 0x{:02x}",
            expected_tag, data[RECORD_TYPE_OFFSET]
        )));
    }
    Ok((segment_id, &data[SEGMENTED_PREFIX_LEN..]))
}

/// Key for a log entry record.
///
/// The key serializes the segment ID, user key, and relative sequence number in a format
/// that preserves lexicographic ordering:
///
/// ```text
/// | subsystem (u8) | version (u8) | segment_id (u32 BE) | record_type (u8=0x10) | terminated_key | relative_seq (var_u64) |
/// ```
///
/// The `relative_seq` is the entry's sequence number relative to the segment's `start_seq`
/// (i.e., it resets to 0 at the start of each segment). This keeps keys compact since most
/// relative offsets within a segment are small. The sequence number uses variable-length
/// encoding (see [`common::serde::varint::var_u64`]).
///
/// Placing `segment_id` before `record_type` keeps every record for a given segment
/// contiguous in storage, regardless of record type — the property a fixed-length
/// SlateDB segment extractor relies on.
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
        write_segmented_prefix(
            &mut buf,
            self.segment_id,
            RecordType::LogEntry.tag().as_byte(),
        );
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
        let (segment_id, mut rest) =
            parse_segmented_prefix(data, RecordType::LogEntry.tag().as_byte())?;
        let key = terminated_bytes::deserialize(&mut rest)?;
        let relative_seq = var_u64::deserialize(&mut rest)?;
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
        write_segmented_prefix(&mut buf, segment.id(), RecordType::LogEntry.tag().as_byte());
        terminated_bytes::serialize(key, &mut buf);
        var_u64::serialize(relative_seq, &mut buf);
        buf.freeze()
    }
}

/// Key for a segment metadata record.
///
/// `SegmentMeta` records describe user log segments. They are written into the
/// system segment (seg_id `0`), with the described segment's id encoded as a
/// suffix so the records sort by described segment:
///
/// ```text
/// | subsystem (u8) | version (u8) | SYSTEM_SEGMENT_ID (u32 BE) | record_type (u8=0x30) | described_segment_id (u32 BE) |
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMetaKey {
    /// The user segment this metadata record describes.
    pub segment_id: SegmentId,
}

impl SegmentMetaKey {
    /// Creates a new segment metadata key
    pub fn new(segment_id: SegmentId) -> Self {
        Self { segment_id }
    }

    /// Encodes the key to bytes for storage
    pub fn serialize(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(SEGMENTED_PREFIX_LEN + 4);
        write_segmented_prefix(
            &mut buf,
            SYSTEM_SEGMENT_ID,
            RecordType::SegmentMeta.tag().as_byte(),
        );
        buf.put_u32(self.segment_id);
        buf.freeze()
    }

    /// Decodes a segment metadata key from bytes.
    ///
    /// Strict: rejects keys that aren't routed to the system segment, and
    /// rejects trailing bytes past the described-segment-id suffix. The
    /// caller has already validated subsystem / version / tag via
    /// [`parse_segmented_prefix`].
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        let (storage_segment_id, rest) =
            parse_segmented_prefix(data, RecordType::SegmentMeta.tag().as_byte())?;
        if storage_segment_id != SYSTEM_SEGMENT_ID {
            return Err(Error::Encoding(format!(
                "SegmentMeta records must live in the system segment (got seg_id {})",
                storage_segment_id
            )));
        }
        if rest.len() != 4 {
            return Err(Error::Encoding(format!(
                "SegmentMeta key has wrong described-segment-id suffix length: expected 4 bytes, got {}",
                rest.len()
            )));
        }
        let segment_id = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
        Ok(SegmentMetaKey { segment_id })
    }

    /// Creates a storage key range for scanning `SegmentMeta` records whose
    /// *described* segment id falls in `range`.
    ///
    /// All records live in the system segment (storage seg_id `0`); the
    /// described segment id is the fixed-width 4-byte suffix, so the range
    /// endpoints encode `seg_id=0` at the front and the described range at
    /// the back. The scan is contiguous within the system segment.
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
/// Listing records share the segment's 6-byte prefix `[sub, ver, segment_id]`
/// with log entries, so they land in the same SlateDB segment under a
/// segment extractor.
///
/// ```text
/// | subsystem (u8) | version (u8) | segment_id (u32 BE) | record_type (u8=0x40) | key (Bytes) |
/// ```
///
/// Unlike log entry keys, the user key is stored as raw bytes without
/// terminated encoding since it occupies the suffix position. Listings for a
/// given segment are enumerated with a single contiguous prefix scan; cross-
/// segment listings are stitched together by walking the segment cache.
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
        write_segmented_prefix(
            &mut buf,
            self.segment_id,
            RecordType::ListingEntry.tag().as_byte(),
        );
        buf.put_slice(&self.key);
        buf.freeze()
    }

    /// Deserializes a listing entry key from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, Error> {
        let (segment_id, rest) =
            parse_segmented_prefix(data, RecordType::ListingEntry.tag().as_byte())?;
        let key = Bytes::copy_from_slice(rest);
        Ok(ListingEntryKey { segment_id, key })
    }

    /// Creates a storage key range for scanning listing entries within a
    /// single segment. Cross-segment iteration is the caller's responsibility:
    /// the v2 layout interleaves listing records with log entries across
    /// segment boundaries, so a wide cross-segment range scan is not safe.
    pub fn scan_range_for_segment(segment_id: SegmentId) -> BytesRange {
        BytesRange::prefix(Self::segment_prefix(segment_id))
    }

    /// Returns the listing prefix for a segment (the smallest valid listing key
    /// for that segment).
    fn segment_prefix(segment_id: SegmentId) -> Bytes {
        let mut buf = BytesMut::with_capacity(SEGMENTED_PREFIX_LEN);
        write_segmented_prefix(
            &mut buf,
            segment_id,
            RecordType::ListingEntry.tag().as_byte(),
        );
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
        // subsystem (1) + version (1) + segment_id (4) + tag (1) + key "k" (1) + terminator (1) + relative_seq (varint, 2 bytes for 100) = 11
        assert_eq!(serialized.len(), 11);
        assert_eq!(serialized[0], SUBSYSTEM);
        assert_eq!(serialized[1], KEY_VERSION);
        // segment_id = 1 in big endian
        assert_eq!(&serialized[2..6], &[0, 0, 0, 1]);
        // Record tag: type 0x01 in high nibble, reserved 0x00 in low nibble = 0x10
        assert_eq!(serialized[6], RecordType::LogEntry.tag().as_byte());
        assert_eq!(serialized[6], 0x10);
        // key "k" + terminator
        assert_eq!(serialized[7], b'k');
        assert_eq!(serialized[8], 0x00); // terminator
        // relative_seq = 100 as varint: length code 1 (2 bytes total), value 100
        // First byte: (1 << 4) | (100 >> 8) = 0x10
        // Second byte: 100 & 0xFF = 0x64
        assert_eq!(&serialized[9..11], &[0x10, 0x64]);
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
        // subsystem (1) + version (1) + segment_id (4) + tag (1) + key "k" (1) + terminator (1) + relative_seq (1) = 10
        assert_eq!(serialized.len(), 10);
        // relative_seq = 5 as varint: length code 0, value 5
        assert_eq!(serialized[9], 0x05);
    }

    #[test]
    fn should_order_log_entries_by_segment_then_key_then_sequence() {
        // given - all in segment 1 with start_seq 0
        let seg1_start_seq = 0;
        let key1 = LogEntryKey::new(1, Bytes::from("a"), 1);
        let key2 = LogEntryKey::new(1, Bytes::from("a"), 2);
        let key3 = LogEntryKey::new(1, Bytes::from("b"), 1);
        // segment 2 has its own start_seq
        let seg2_start_seq = 100;
        let key4 = LogEntryKey::new(2, Bytes::from("a"), 101);

        // when
        let s1 = key1.serialize(seg1_start_seq);
        let s2 = key2.serialize(seg1_start_seq);
        let s3 = key3.serialize(seg1_start_seq);
        let s4 = key4.serialize(seg2_start_seq);

        // then - segment_id ordering takes precedence
        assert!(s1 < s2, "same segment/key, seq 1 < seq 2");
        assert!(s2 < s3, "same segment, key 'a' < key 'b'");
        assert!(s3 < s4, "segment 1 < segment 2");
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
        // given — 6 bytes (one short of the 7-byte segmented prefix)
        let data = vec![SUBSYSTEM, KEY_VERSION, 0, 0, 0, 1];

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
        // subsystem (1) + version (1) + segment_id (4) + tag (1) + key "k" (1) = 8
        assert_eq!(serialized.len(), 8);
        assert_eq!(serialized[0], SUBSYSTEM);
        assert_eq!(serialized[1], KEY_VERSION);
        // segment_id = 1 in big endian
        assert_eq!(&serialized[2..6], &[0, 0, 0, 1]);
        // Record tag: type 0x04 in high nibble, reserved 0x00 in low nibble = 0x40
        assert_eq!(serialized[6], RecordType::ListingEntry.tag().as_byte());
        assert_eq!(serialized[6], 0x40);
        // key "k" (raw bytes, no terminator)
        assert_eq!(serialized[7], b'k');
    }

    #[test]
    fn should_serialize_listing_entry_key_with_empty_key() {
        // given
        let key = ListingEntryKey::new(1, Bytes::new());

        // when
        let serialized = key.serialize();
        let deserialized = ListingEntryKey::deserialize(&serialized).unwrap();

        // then
        assert_eq!(serialized.len(), SEGMENTED_PREFIX_LEN); // sub + ver + segment_id + tag, no user key
        assert_eq!(deserialized.segment_id, 1);
        assert_eq!(deserialized.key, Bytes::new());
    }

    #[test]
    fn should_order_listing_entries_by_segment_then_key() {
        // given
        let key1 = ListingEntryKey::new(1, Bytes::from("a"));
        let key2 = ListingEntryKey::new(1, Bytes::from("b"));
        let key3 = ListingEntryKey::new(2, Bytes::from("a"));

        // when
        let s1 = key1.serialize();
        let s2 = key2.serialize();
        let s3 = key3.serialize();

        // then
        assert!(s1 < s2, "same segment, key 'a' < key 'b'");
        assert!(s2 < s3, "segment 1 < segment 2");
    }

    #[test]
    fn should_create_listing_entry_scan_range_for_segment() {
        // given
        let segment_id = 7;

        // when
        let scan_range = ListingEntryKey::scan_range_for_segment(segment_id);

        // then — start is the segment's listing prefix (= empty-key listing key)
        let start_key = ListingEntryKey::new(segment_id, Bytes::new()).serialize();
        assert_eq!(scan_range.start_bound(), Bound::Included(&start_key));
        // end is the lex-incremented prefix — non-empty for a non-0xFF prefix
        match scan_range.end_bound() {
            Bound::Excluded(b) => {
                assert!(b > &start_key, "end bound must be strictly after start");
            }
            other => panic!("expected Excluded end bound, got {:?}", other),
        }
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
        // given — 6 bytes, one short of the segmented prefix
        let data = vec![SUBSYSTEM, KEY_VERSION, 0, 0, 0, 1];

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
                let key_a = LogEntryKey::new(1, Bytes::from("key"), a);
                let key_b = LogEntryKey::new(1, Bytes::from("key"), b);

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
            fn should_include_listing_entry_in_per_segment_scan_range(
                segment_id in 1u32..100_000,
                key_bytes in prop::collection::vec(any::<u8>(), 1..100),
            ) {
                let key = ListingEntryKey::new(segment_id, Bytes::from(key_bytes));
                let serialized = key.serialize();

                let scan_range = ListingEntryKey::scan_range_for_segment(segment_id);

                prop_assert!(
                    scan_range.contains(&serialized),
                    "listing entry for segment {} should be in its per-segment range, \
                     serialized={:?}, range_start={:?}, range_end={:?}",
                    segment_id,
                    serialized.as_ref(),
                    scan_range.start_bound(),
                    scan_range.end_bound()
                );
            }

            #[test]
            fn should_exclude_other_segments_from_per_segment_scan_range(
                segment_id in 1u32..100_000,
                other_segment_id in 1u32..100_000,
                key_bytes in prop::collection::vec(any::<u8>(), 0..100),
            ) {
                prop_assume!(segment_id != other_segment_id);
                let key = ListingEntryKey::new(other_segment_id, Bytes::from(key_bytes));
                let serialized = key.serialize();

                let scan_range = ListingEntryKey::scan_range_for_segment(segment_id);

                prop_assert!(
                    !scan_range.contains(&serialized),
                    "listing entry for segment {} should NOT be in segment {}'s range",
                    other_segment_id, segment_id
                );
            }
        }
    }
}

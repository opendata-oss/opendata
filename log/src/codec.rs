#![allow(dead_code)]

//! Codec for log storage encoding and decoding.
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
//! - `LastBlock` (0x02): Sequence number block allocation tracking
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

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::Error;

/// Key format version (currently 0x01)
pub const KEY_VERSION: u8 = 0x01;

/// Terminator byte for TerminatedBytes encoding (lowest byte value)
const TERMINATOR_BYTE: u8 = 0x00;

/// Escape character for TerminatedBytes encoding
const ESCAPE_BYTE: u8 = 0x01;

/// Reserved byte for range query upper bounds (highest byte value)
const RANGE_END_BYTE: u8 = 0xFF;

/// Record type discriminators for log storage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// Log entry record containing user key, sequence, and value
    LogEntry = 0x01,
    /// Block allocation record for sequence number tracking
    LastBlock = 0x02,
}

impl RecordType {
    /// Returns the byte discriminator for this record type
    pub fn as_byte(&self) -> u8 {
        *self as u8
    }

    /// Converts a byte to a RecordType
    pub fn from_byte(byte: u8) -> Result<Self, Error> {
        match byte {
            0x01 => Ok(RecordType::LogEntry),
            0x02 => Ok(RecordType::LastBlock),
            _ => Err(Error::Encoding(format!(
                "invalid record type: 0x{:02x}",
                byte
            ))),
        }
    }
}

/// Encodes raw bytes with escape sequences and `0x00` terminator.
///
/// Writes directly to the provided buffer. The encoding:
/// - `0x00` → `0x01 0x01`
/// - `0x01` → `0x01 0x02`
/// - `0xFF` → `0x01 0x03`
/// - All other bytes unchanged
/// - Terminated with `0x00`
///
/// Using `0x00` as the terminator ensures shorter keys sort before longer
/// keys with the same prefix, enabling simple prefix-based range queries.
fn encode_terminated(data: &[u8], buf: &mut BytesMut) {
    for &byte in data {
        match byte {
            TERMINATOR_BYTE => {
                buf.put_u8(ESCAPE_BYTE);
                buf.put_u8(0x01);
            }
            ESCAPE_BYTE => {
                buf.put_u8(ESCAPE_BYTE);
                buf.put_u8(0x02);
            }
            RANGE_END_BYTE => {
                buf.put_u8(ESCAPE_BYTE);
                buf.put_u8(0x03);
            }
            _ => buf.put_u8(byte),
        }
    }
    buf.put_u8(TERMINATOR_BYTE);
}

/// Decodes terminated bytes from a buffer, advancing past the terminator.
///
/// Returns the decoded raw bytes. The input buffer is advanced past the
/// terminator byte.
fn decode_terminated(buf: &mut &[u8]) -> Result<Bytes, Error> {
    let mut result = BytesMut::new();
    let mut i = 0;

    while i < buf.len() {
        let byte = buf[i];

        if byte == TERMINATOR_BYTE {
            // Found terminator, consume it and return
            *buf = &buf[i + 1..];
            return Ok(result.freeze());
        }

        if byte == ESCAPE_BYTE {
            // Escape sequence - need next byte
            if i + 1 >= buf.len() {
                return Err(Error::Encoding(
                    "truncated escape sequence in terminated bytes".to_string(),
                ));
            }
            let next = buf[i + 1];
            match next {
                0x01 => result.put_u8(TERMINATOR_BYTE),
                0x02 => result.put_u8(ESCAPE_BYTE),
                0x03 => result.put_u8(RANGE_END_BYTE),
                _ => {
                    return Err(Error::Encoding(format!(
                        "invalid escape sequence: 0x01 0x{:02x}",
                        next
                    )));
                }
            }
            i += 2;
        } else {
            result.put_u8(byte);
            i += 1;
        }
    }

    Err(Error::Encoding(
        "unterminated bytes sequence (missing 0x00 terminator)".to_string(),
    ))
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
        buf.put_u8(RecordType::LogEntry.as_byte());
        encode_terminated(&self.key, &mut buf);
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

        let record_type = RecordType::from_byte(data[1])?;
        if record_type != RecordType::LogEntry {
            return Err(Error::Encoding(format!(
                "invalid record type: expected LogEntry, got {:?}",
                record_type
            )));
        }

        let mut buf = &data[2..];
        let key = decode_terminated(&mut buf)?;

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
        buf.put_u8(RecordType::LogEntry.as_byte());
        encode_terminated(key, &mut buf);
        buf.freeze()
    }
}

/// Key for the LastBlock record (static, singleton key).
///
/// ```text
/// | version (u8) | type (u8) |
/// ```
///
/// There is exactly one LastBlock record in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastBlockKey;

impl LastBlockKey {
    /// Encodes the LastBlock key
    pub fn encode(&self) -> Bytes {
        Bytes::from(vec![KEY_VERSION, RecordType::LastBlock.as_byte()])
    }

    /// Decodes and validates a LastBlock key
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 2 {
            return Err(Error::Encoding(
                "buffer too short for LastBlock key".to_string(),
            ));
        }

        if data[0] != KEY_VERSION {
            return Err(Error::Encoding(format!(
                "invalid key version: expected 0x{:02x}, got 0x{:02x}",
                KEY_VERSION, data[0]
            )));
        }

        let record_type = RecordType::from_byte(data[1])?;
        if record_type != RecordType::LastBlock {
            return Err(Error::Encoding(format!(
                "invalid record type: expected LastBlock, got {:?}",
                record_type
            )));
        }

        Ok(LastBlockKey)
    }
}

/// Value for the LastBlock record.
///
/// Stores the current sequence block allocation:
///
/// ```text
/// | base_sequence (u64 BE) | block_size (u64 BE) |
/// ```
///
/// The allocated range is `[base_sequence, base_sequence + block_size)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastBlockValue {
    /// Base sequence number of the allocated block
    pub base_sequence: u64,
    /// Size of the allocated block
    pub block_size: u64,
}

impl LastBlockValue {
    /// Creates a new LastBlock value
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

    /// Decodes a LastBlock value from bytes
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if data.len() < 16 {
            return Err(Error::Encoding(format!(
                "buffer too short for LastBlock value: need 16 bytes, got {}",
                data.len()
            )));
        }

        let base_sequence = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let block_size = u64::from_be_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);

        Ok(LastBlockValue {
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
    fn should_convert_record_type_to_byte_and_back() {
        // given
        let log_entry = RecordType::LogEntry;
        let last_block = RecordType::LastBlock;

        // when/then
        assert_eq!(log_entry.as_byte(), 0x01);
        assert_eq!(last_block.as_byte(), 0x02);
        assert_eq!(RecordType::from_byte(0x01).unwrap(), RecordType::LogEntry);
        assert_eq!(RecordType::from_byte(0x02).unwrap(), RecordType::LastBlock);
    }

    #[test]
    fn should_reject_invalid_record_type() {
        // given
        let invalid_byte = 0x99;

        // when
        let result = RecordType::from_byte(invalid_byte);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_and_decode_simple_bytes() {
        // given
        let data = b"hello";
        let mut buf = BytesMut::new();

        // when
        encode_terminated(data, &mut buf);
        let mut slice = buf.as_ref();
        let decoded = decode_terminated(&mut slice).unwrap();

        // then
        assert_eq!(decoded.as_ref(), b"hello");
        assert!(slice.is_empty());
    }

    #[test]
    fn should_encode_and_decode_bytes_with_escape_char() {
        // given - data containing 0x01 (escape char)
        let data = &[0x61, 0x01, 0x62]; // "a" + 0x01 + "b"
        let mut buf = BytesMut::new();

        // when
        encode_terminated(data, &mut buf);

        // then - should be escaped as 0x01 0x02
        assert_eq!(buf.as_ref(), &[0x61, 0x01, 0x02, 0x62, 0x00]);

        // when - decode
        let mut slice = buf.as_ref();
        let decoded = decode_terminated(&mut slice).unwrap();

        // then
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn should_encode_and_decode_bytes_with_terminator_char() {
        // given - data containing 0x00 (terminator char)
        let data = &[0x61, 0x00, 0x62]; // "a" + 0x00 + "b"
        let mut buf = BytesMut::new();

        // when
        encode_terminated(data, &mut buf);

        // then - should be escaped as 0x01 0x01
        assert_eq!(buf.as_ref(), &[0x61, 0x01, 0x01, 0x62, 0x00]);

        // when - decode
        let mut slice = buf.as_ref();
        let decoded = decode_terminated(&mut slice).unwrap();

        // then
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn should_encode_and_decode_bytes_with_all_special_chars() {
        // given - data with all special chars: 0x00, 0x01, 0xFF
        let data = &[0x00, 0x01, 0xFF, 0x00, 0x01, 0xFF];
        let mut buf = BytesMut::new();

        // when
        encode_terminated(data, &mut buf);
        let mut slice = buf.as_ref();
        let decoded = decode_terminated(&mut slice).unwrap();

        // then
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn should_encode_empty_bytes() {
        // given
        let data: &[u8] = &[];
        let mut buf = BytesMut::new();

        // when
        encode_terminated(data, &mut buf);

        // then - just the terminator
        assert_eq!(buf.as_ref(), &[0x00]);

        // when - decode
        let mut slice = buf.as_ref();
        let decoded = decode_terminated(&mut slice).unwrap();

        // then
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_fail_decode_without_terminator() {
        // given - no terminator
        let data = &[0x61, 0x62, 0x63];

        // when
        let mut slice = &data[..];
        let result = decode_terminated(&mut slice);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_fail_decode_with_truncated_escape() {
        // given - escape at end without following byte
        let data = &[0x61, 0x01];

        // when
        let mut slice = &data[..];
        let result = decode_terminated(&mut slice);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_fail_decode_with_invalid_escape_sequence() {
        // given - invalid escape (0x01 followed by 0x04, which is not a valid escape)
        let data = &[0x61, 0x01, 0x04, 0x00];

        // when
        let mut slice = &data[..];
        let result = decode_terminated(&mut slice);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_preserve_ordering_for_non_prefix_keys() {
        // given - keys where neither is a prefix of the other
        // These should maintain lexicographic ordering
        let pairs = [
            (b"apple".as_slice(), b"banana".as_slice()),
            (b"cat", b"dog"),
            (b"x", b"y"),
        ];

        for (key_a, key_b) in pairs {
            // when
            let mut buf_a = BytesMut::new();
            encode_terminated(key_a, &mut buf_a);
            let mut buf_b = BytesMut::new();
            encode_terminated(key_b, &mut buf_b);

            // then - lexicographic ordering preserved when no prefix relationship
            assert!(
                buf_a.as_ref() < buf_b.as_ref(),
                "Expected {:?} < {:?} after encoding",
                key_a,
                key_b
            );
        }
    }

    #[test]
    fn should_group_prefix_keys_contiguously() {
        // given - keys where one is a prefix of another
        // The terminated encoding ensures contiguity: all entries for "a" are
        // grouped together, all entries for "ab" are grouped together.
        // Note: shorter keys sort BEFORE longer keys with the same prefix
        // because 0x00 (terminator) < any regular byte.
        let mut buf_a = BytesMut::new();
        encode_terminated(b"a", &mut buf_a);
        let mut buf_ab = BytesMut::new();
        encode_terminated(b"ab", &mut buf_ab);
        let mut buf_abc = BytesMut::new();
        encode_terminated(b"abc", &mut buf_abc);

        // then - shorter prefixes sort before longer ones
        // a < ab < abc (because at the divergence point, 0x00 < regular bytes)
        assert!(buf_a.as_ref() < buf_ab.as_ref());
        assert!(buf_ab.as_ref() < buf_abc.as_ref());
    }

    #[test]
    fn should_not_allow_key_prefix_collision() {
        // given - key "a" should not be a prefix of key "ab" after encoding
        // This ensures entries for different keys don't interleave
        let mut buf_a = BytesMut::new();
        encode_terminated(b"a", &mut buf_a);
        let mut buf_ab = BytesMut::new();
        encode_terminated(b"ab", &mut buf_ab);

        // then - "a" encoding should not be a prefix of "ab" encoding
        // because "a" has terminator before "b" appears
        assert!(!buf_ab.starts_with(&buf_a));
    }

    #[test]
    fn should_support_prefix_range_queries() {
        // given - keys with a common prefix "/foo" and one unrelated key
        let keys = [
            b"/foo".as_slice(),
            b"/foo/bar",
            b"/foobar",
            b"/food",
            b"/bar", // unrelated
        ];

        // Encode all keys
        let mut encoded: Vec<(_, _)> = keys
            .iter()
            .map(|k| {
                let mut buf = BytesMut::new();
                encode_terminated(k, &mut buf);
                (*k, buf.freeze())
            })
            .collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));

        // Build range bounds for prefix "/foo"
        let mut start = BytesMut::from(b"/foo".as_slice());
        start.put_u8(TERMINATOR_BYTE); // 0x00
        let mut end = BytesMut::from(b"/foo".as_slice());
        end.put_u8(RANGE_END_BYTE); // 0xFF

        // then - all keys with prefix "/foo" fall within [start, end)
        let in_range: Vec<_> = encoded
            .iter()
            .filter(|(_, e)| e.as_ref() >= start.as_ref() && e.as_ref() < end.as_ref())
            .map(|(k, _)| *k)
            .collect();

        // All "/foo*" keys should be in range, but "/bar" should not
        assert_eq!(in_range.len(), 4);
        assert!(in_range.contains(&b"/foo".as_slice()));
        assert!(in_range.contains(&b"/foo/bar".as_slice()));
        assert!(in_range.contains(&b"/foobar".as_slice()));
        assert!(in_range.contains(&b"/food".as_slice()));
    }

    #[test]
    fn should_encode_and_decode_log_entry_key() {
        // given
        let key = LogEntryKey::new(Bytes::from("orders"), 12345);

        // when
        let encoded = key.encode();
        let decoded = LogEntryKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_log_entry_key_with_special_bytes() {
        // given - key containing all special bytes: terminator (0x00), escape (0x01), range end (0xFF)
        let key = LogEntryKey::new(Bytes::from_static(&[0x61, 0x00, 0x01, 0xFF, 0x62]), 99999);

        // when
        let encoded = key.encode();
        let decoded = LogEntryKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_log_entry_key_with_correct_structure() {
        // given
        let key = LogEntryKey::new(Bytes::from("test"), 256);

        // when
        let encoded = key.encode();

        // then
        assert_eq!(encoded[0], KEY_VERSION);
        assert_eq!(encoded[1], RecordType::LogEntry.as_byte());
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
    fn should_fail_decode_log_entry_key_with_wrong_version() {
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
    fn should_fail_decode_log_entry_key_with_wrong_type() {
        // given - LastBlock type instead of LogEntry
        let data = vec![KEY_VERSION, RecordType::LastBlock.as_byte()];

        // when
        let result = LogEntryKey::decode(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_and_decode_last_block_key() {
        // given
        let key = LastBlockKey;

        // when
        let encoded = key.encode();
        let decoded = LastBlockKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0], KEY_VERSION);
        assert_eq!(encoded[1], RecordType::LastBlock.as_byte());
    }

    #[test]
    fn should_fail_decode_last_block_key_with_wrong_type() {
        // given
        let data = vec![KEY_VERSION, RecordType::LogEntry.as_byte()];

        // when
        let result = LastBlockKey::decode(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_and_decode_last_block_value() {
        // given
        let value = LastBlockValue::new(1000, 100);

        // when
        let encoded = value.encode();
        let decoded = LastBlockValue::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert_eq!(encoded.len(), 16);
    }

    #[test]
    fn should_calculate_next_base() {
        // given
        let value = LastBlockValue::new(1000, 100);

        // when
        let next = value.next_base();

        // then
        assert_eq!(next, 1100);
    }

    #[test]
    fn should_fail_decode_last_block_value_too_short() {
        // given
        let data = vec![0u8; 15]; // need 16 bytes

        // when
        let result = LastBlockValue::decode(&data);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_last_block_value_in_big_endian() {
        // given
        let value = LastBlockValue::new(0x0102030405060708, 0x1112131415161718);

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

//! Common record key prefix encoding for OpenData storage systems.
//!
//! This module implements RFC 0001: Record Key Prefix. All OpenData records
//! stored in SlateDB use keys with a standardized 2-byte prefix:
//!
//! ```text
//! ┌─────────┬────────────┬─────────────────────┐
//! │ version │ record_tag │  ... record fields  │
//! │ 1 byte  │   1 byte   │    (varies)         │
//! └─────────┴────────────┴─────────────────────┘
//! ```
//!
//! # Version Byte
//!
//! The first byte identifies the key format version. Each subsystem manages
//! its version independently.
//!
//! # Record Tag Byte
//!
//! The second byte is a composite tag:
//!
//! ```text
//! ┌────────────┬────────────┐
//! │  bits 7-4  │  bits 3-0  │
//! │ record type│  reserved  │
//! └────────────┴────────────┘
//! ```
//!
//! - **Record Type (high 4 bits):** Identifies the kind of record (values 0x1–0xF).
//! - **Reserved (low 4 bits):** Subsystem-specific use (e.g., bucket granularity).

use bytes::{BufMut, Bytes, BytesMut};

use super::DeserializeError;

/// A 2-byte key prefix containing version and record tag.
///
/// This type encapsulates the standard prefix used by all OpenData records.
/// It provides methods for serialization, deserialization, and validation.
///
/// # Examples
///
/// ```
/// use common::serde::key_prefix::{KeyPrefix, RecordTag};
///
/// // Create a prefix
/// let prefix = KeyPrefix::new(0x01, RecordTag::new(0x02, 0x00));
/// assert_eq!(prefix.version(), 0x01);
/// assert_eq!(prefix.tag().record_type(), 0x02);
///
/// // Serialize and deserialize
/// let bytes = prefix.to_bytes();
/// let parsed = KeyPrefix::from_bytes(&bytes).unwrap();
/// assert_eq!(parsed, prefix);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPrefix {
    version: u8,
    tag: RecordTag,
}

impl KeyPrefix {
    /// Creates a new key prefix with the given version and record tag.
    pub fn new(version: u8, tag: RecordTag) -> Self {
        Self { version, tag }
    }

    /// Returns the version byte.
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Returns the record tag.
    pub fn tag(&self) -> RecordTag {
        self.tag
    }

    /// Parses a key prefix from a byte slice.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is too short (less than 2 bytes)
    /// - The record type is 0 (reserved)
    pub fn from_bytes(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < 2 {
            return Err(DeserializeError {
                message: format!(
                    "buffer too short for key prefix: need 2 bytes, got {}",
                    data.len()
                ),
            });
        }
        let version = data[0];
        let tag = RecordTag::from_byte(data[1])?;
        Ok(Self { version, tag })
    }

    /// Parses a key prefix, validating the version matches expected.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is too short
    /// - The version doesn't match
    /// - The record type is 0
    pub fn from_bytes_versioned(
        data: &[u8],
        expected_version: u8,
    ) -> Result<Self, DeserializeError> {
        let prefix = Self::from_bytes(data)?;
        if prefix.version != expected_version {
            return Err(DeserializeError {
                message: format!(
                    "invalid key version: expected 0x{:02x}, got 0x{:02x}",
                    expected_version, prefix.version
                ),
            });
        }
        Ok(prefix)
    }

    /// Serializes the prefix to a 2-byte array.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(vec![self.version, self.tag.as_byte()])
    }

    /// Writes the prefix to a buffer.
    pub fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.tag.as_byte());
    }
}

/// Record tag combining record type (high 4 bits) and reserved bits (low 4 bits).
///
/// The record tag is the second byte of the key prefix. It encodes the record
/// type in the high 4 bits, leaving the low 4 bits for subsystem-specific use.
///
/// # Examples
///
/// ```
/// use common::serde::key_prefix::RecordTag;
///
/// // Create a tag with type 0x01 and reserved bits 0x00
/// let tag = RecordTag::new(0x01, 0x00);
/// assert_eq!(tag.record_type(), 0x01);
/// assert_eq!(tag.reserved(), 0x00);
///
/// // Create a tag with type 0x05 and reserved bits 0x03
/// let tag = RecordTag::new(0x05, 0x03);
/// assert_eq!(tag.as_byte(), 0x53);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordTag(u8);

impl RecordTag {
    /// Creates a new record tag with the given record type and reserved bits.
    ///
    /// # Panics
    ///
    /// Panics if `record_type` is 0 or greater than 15, or if `reserved` is
    /// greater than 15.
    pub fn new(record_type: u8, reserved: u8) -> Self {
        assert!(
            record_type > 0 && record_type <= 0x0F,
            "record type {} must be in range 1-15",
            record_type
        );
        assert!(
            reserved <= 0x0F,
            "reserved bits {} must be in range 0-15",
            reserved
        );
        RecordTag((record_type << 4) | reserved)
    }

    /// Creates a record tag from a raw byte value.
    ///
    /// Returns an error if the record type (high 4 bits) is 0.
    pub fn from_byte(byte: u8) -> Result<Self, DeserializeError> {
        let record_type = (byte & 0xF0) >> 4;
        if record_type == 0 {
            return Err(DeserializeError {
                message: format!(
                    "invalid record tag: 0x{:02x} (record type 0 is reserved)",
                    byte
                ),
            });
        }
        Ok(RecordTag(byte))
    }

    /// Returns the record type (high 4 bits).
    pub fn record_type(&self) -> u8 {
        (self.0 & 0xF0) >> 4
    }

    /// Returns the reserved bits (low 4 bits).
    pub fn reserved(&self) -> u8 {
        self.0 & 0x0F
    }

    /// Returns the raw byte representation.
    pub fn as_byte(&self) -> u8 {
        self.0
    }

    /// Returns a new record tag with the same record type but different reserved bits.
    ///
    /// # Panics
    ///
    /// Panics if `reserved` is greater than 15.
    pub fn with_reserved(&self, reserved: u8) -> Self {
        assert!(
            reserved <= 0x0F,
            "reserved bits {} must be in range 0-15",
            reserved
        );
        RecordTag((self.0 & 0xF0) | reserved)
    }

    /// Returns a range covering all tags with the given record type.
    ///
    /// This is useful for creating scan ranges that match all records of a
    /// given type regardless of their reserved bits.
    pub fn type_range(record_type: u8) -> std::ops::Range<u8> {
        assert!(
            record_type > 0 && record_type <= 0x0F,
            "record type {} must be in range 1-15",
            record_type
        );
        let start = record_type << 4;
        start..(start + 0x10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_record_tag() {
        // given
        let record_type = 0x05;
        let reserved = 0x03;

        // when
        let tag = RecordTag::new(record_type, reserved);

        // then
        assert_eq!(tag.as_byte(), 0x53);
        assert_eq!(tag.record_type(), 0x05);
        assert_eq!(tag.reserved(), 0x03);
    }

    #[test]
    fn should_create_tag_with_zero_reserved() {
        // given
        let record_type = 0x01;
        let reserved = 0x00;

        // when
        let tag = RecordTag::new(record_type, reserved);

        // then
        assert_eq!(tag.as_byte(), 0x10);
        assert_eq!(tag.record_type(), 0x01);
        assert_eq!(tag.reserved(), 0x00);
    }

    #[test]
    fn should_create_tag_with_max_values() {
        // given
        let record_type = 0x0F;
        let reserved = 0x0F;

        // when
        let tag = RecordTag::new(record_type, reserved);

        // then
        assert_eq!(tag.as_byte(), 0xFF);
        assert_eq!(tag.record_type(), 0x0F);
        assert_eq!(tag.reserved(), 0x0F);
    }

    #[test]
    #[should_panic(expected = "record type 0 must be in range 1-15")]
    fn should_panic_on_zero_record_type() {
        RecordTag::new(0, 0);
    }

    #[test]
    #[should_panic(expected = "record type 16 must be in range 1-15")]
    fn should_panic_on_record_type_overflow() {
        RecordTag::new(16, 0);
    }

    #[test]
    #[should_panic(expected = "reserved bits 16 must be in range 0-15")]
    fn should_panic_on_reserved_overflow() {
        RecordTag::new(1, 16);
    }

    #[test]
    fn should_parse_tag_from_byte() {
        // given
        let byte = 0x53;

        // when
        let tag = RecordTag::from_byte(byte).unwrap();

        // then
        assert_eq!(tag.record_type(), 0x05);
        assert_eq!(tag.reserved(), 0x03);
    }

    #[test]
    fn should_reject_zero_record_type_byte() {
        // given
        let byte = 0x0F; // record type 0, reserved 15

        // when
        let result = RecordTag::from_byte(byte);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .message
                .contains("record type 0 is reserved")
        );
    }

    #[test]
    fn should_compute_type_range() {
        // given
        let record_type = 0x03;

        // when
        let range = RecordTag::type_range(record_type);

        // then
        assert_eq!(range.start, 0x30);
        assert_eq!(range.end, 0x40);
    }

    #[test]
    fn should_create_tag_with_different_reserved_bits() {
        // given
        let tag = RecordTag::new(0x05, 0x00);

        // when
        let new_tag = tag.with_reserved(0x0A);

        // then
        assert_eq!(new_tag.record_type(), 0x05);
        assert_eq!(new_tag.reserved(), 0x0A);
        assert_eq!(new_tag.as_byte(), 0x5A);
    }

    #[test]
    #[should_panic(expected = "reserved bits 16 must be in range 0-15")]
    fn should_panic_on_with_reserved_overflow() {
        let tag = RecordTag::new(0x01, 0x00);
        tag.with_reserved(16);
    }

    #[test]
    fn should_create_key_prefix() {
        // given
        let version = 0x01;
        let tag = RecordTag::new(0x02, 0x05);

        // when
        let prefix = KeyPrefix::new(version, tag);

        // then
        assert_eq!(prefix.version(), version);
        assert_eq!(prefix.tag().as_byte(), tag.as_byte());
    }

    #[test]
    fn should_write_and_read_key_prefix() {
        // given
        let prefix = KeyPrefix::new(0x01, RecordTag::new(0x02, 0x05));
        let mut buf = BytesMut::new();

        // when
        prefix.write_to(&mut buf);
        let parsed = KeyPrefix::from_bytes(&buf).unwrap();

        // then
        assert_eq!(parsed, prefix);
    }

    #[test]
    fn should_serialize_key_prefix_to_bytes() {
        // given
        let prefix = KeyPrefix::new(0x01, RecordTag::new(0x02, 0x05));

        // when
        let bytes = prefix.to_bytes();

        // then
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes[0], 0x01);
        assert_eq!(bytes[1], 0x25);
    }

    #[test]
    fn should_parse_key_prefix_versioned() {
        // given
        let expected_version = 0x01;
        let data = [expected_version, 0x25]; // version 1, type 2, reserved 5

        // when
        let prefix = KeyPrefix::from_bytes_versioned(&data, expected_version).unwrap();

        // then
        assert_eq!(prefix.version(), expected_version);
        assert_eq!(prefix.tag().record_type(), 0x02);
        assert_eq!(prefix.tag().reserved(), 0x05);
    }

    #[test]
    fn should_reject_wrong_version() {
        // given
        let data = [0x02, 0x10]; // wrong version

        // when
        let result = KeyPrefix::from_bytes_versioned(&data, 0x01);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("invalid key version"));
    }

    #[test]
    fn should_reject_short_buffer() {
        // given
        let data = [0x01]; // only 1 byte

        // when
        let result = KeyPrefix::from_bytes(&data);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("buffer too short"));
    }
}

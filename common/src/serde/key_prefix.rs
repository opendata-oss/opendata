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
//! its version independently. A version of 0x00 is reserved as an invalid version.
//!
//! # Record Tag Byte
//!
//! The second byte stores the full record tag value.
//!
//! - **Record Tag:** Identifies the kind of record.
//! - `0x00` is reserved as an invalid tag when decoding serialized keys.

use bytes::{BufMut, Bytes, BytesMut};

use super::DeserializeError;

/// A 2-byte key prefix containing version and record tag.
///
/// This type encapsulates the standard prefix used by all OpenData records.
/// It provides methods for serialization, deserialization, and validation.
/// Callers constructing a prefix directly must provide non-zero `version` and
/// `record_tag` values.
///
/// # Examples
///
/// ```
/// use common::serde::key_prefix::KeyPrefix;
///
/// // Create a prefix
/// let prefix = KeyPrefix::new(0x01, 0x02);
/// assert_eq!(prefix.version(), 0x01);
/// assert_eq!(prefix.tag(), 0x02);
///
/// // Serialize and deserialize
/// let bytes = prefix.to_bytes();
/// let parsed = KeyPrefix::from_bytes(&bytes).unwrap();
/// assert_eq!(parsed, prefix);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyPrefix {
    version: u8,
    tag: u8,
}

impl KeyPrefix {
    /// Creates a new key prefix with the given version and record tag.
    ///
    /// # Panics
    ///
    /// Panics if `version` is 0 or `tag` is 0.
    ///
    pub fn new(version: u8, tag: u8) -> Self {
        assert!(version > 0, "key version 0 is reserved");
        assert!(tag > 0, "record tag 0 is reserved");
        Self { version, tag }
    }

    /// Returns the version byte.
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Returns the record tag.
    pub fn tag(&self) -> u8 {
        self.tag
    }

    /// Parses a key prefix from a byte slice.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is too short (less than 2 bytes)
    /// - The key version is 0 (reserved)
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
        let version = validate_key_version(data[0])?;
        let tag = validate_record_tag(data[1])?;
        Ok(Self { version, tag })
    }

    /// Parses a key prefix, validating the version matches expected.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is too short
    /// - The key version is 0
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
        Bytes::from(vec![self.version, self.tag])
    }

    /// Writes the prefix to a buffer.
    pub fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.tag);
    }
}

fn validate_key_version(byte: u8) -> Result<u8, DeserializeError> {
    if byte == 0 {
        return Err(DeserializeError {
            message: format!(
                "invalid key version: 0x{:02x} (version 0 is reserved)",
                byte
            ),
        });
    }
    Ok(byte)
}

fn validate_record_tag(byte: u8) -> Result<u8, DeserializeError> {
    if byte == 0 {
        return Err(DeserializeError {
            message: format!("invalid record tag: 0x{:02x} (tag 0 is reserved)", byte),
        });
    }
    Ok(byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_key_prefix() {
        // given
        let version = 0x01;
        let tag = 0x02;

        // when
        let prefix = KeyPrefix::new(version, tag);

        // then
        assert_eq!(prefix.version(), version);
        assert_eq!(prefix.tag(), tag);
    }

    #[test]
    #[should_panic(expected = "key version 0 is reserved")]
    fn should_panic_on_zero_version() {
        KeyPrefix::new(0, 0x02);
    }

    #[test]
    #[should_panic(expected = "record tag 0 is reserved")]
    fn should_panic_on_zero_record_tag() {
        KeyPrefix::new(0x01, 0);
    }

    #[test]
    fn should_parse_tag_from_byte() {
        // given
        let bytes = [0x42, 0x24];

        // when
        let key_prefix = KeyPrefix::from_bytes(&bytes).unwrap();

        // then
        assert_eq!(key_prefix.version(), 0x42);
        assert_eq!(key_prefix.tag(), 0x24);
    }

    #[test]
    fn should_reject_zero_version_byte() {
        // given
        let bytes = [0x00, 0x53];

        // when
        let result = KeyPrefix::from_bytes(&bytes);

        // then
        matches!(result, Err(DeserializeError { message }) if  message.contains("version 0 is reserved"));
    }

    #[test]
    fn should_reject_zero_record_type_byte() {
        // given
        let bytes = [0x53, 0x00];

        // when
        let result = KeyPrefix::from_bytes(&bytes);

        // then
        matches!(result, Err(DeserializeError { message }) if  message.contains("tag 0 is reserved"));
    }

    #[test]
    fn should_write_and_read_key_prefix() {
        // given
        let prefix = KeyPrefix::new(0x01, 0x02);
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
        let prefix = KeyPrefix::new(0x01, 0x02);

        // when
        let bytes = prefix.to_bytes();

        // then
        assert_eq!(bytes.len(), 2);
        assert_eq!(bytes[0], 0x01);
        assert_eq!(bytes[1], 0x02);
    }

    #[test]
    fn should_parse_key_prefix_versioned() {
        // given
        let expected_version = 0x01;
        let data = [expected_version, 0x25];

        // when
        let prefix = KeyPrefix::from_bytes_versioned(&data, expected_version).unwrap();

        // then
        assert_eq!(prefix.version(), expected_version);
        assert_eq!(prefix.tag(), 0x25);
    }

    #[test]
    fn should_reject_wrong_version() {
        // given
        let data = [0x02, 0x10]; // wrong version

        // when
        let result = KeyPrefix::from_bytes_versioned(&data, 0x01);

        // then
        matches!(result, Err(DeserializeError { message }) if  message.contains("invalid key version"));
    }

    #[test]
    fn should_reject_short_buffer() {
        // given
        let data = [0x01]; // only 1 byte

        // when
        let result = KeyPrefix::from_bytes(&data);

        // then
        matches!(result, Err(DeserializeError { message }) if  message.contains("buffer too short"));
    }
}

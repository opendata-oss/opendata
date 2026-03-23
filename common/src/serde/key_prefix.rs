//! Common record key prefix encoding for OpenData storage systems.
//!
//! This module implements RFC 0001: Record Key Prefix. All OpenData records
//! stored in SlateDB use keys with a standardized 3-byte prefix:
//!
//! ```text
//! ┌───────────┬─────────┬────────────┬─────────────────────┐
//! │ subsystem │ version │ record_tag │  ... record fields  │
//! │  1 byte   │ 1 byte  │   1 byte   │    (varies)         │
//! └───────────┴─────────┴────────────┴─────────────────────┘
//! ```
//!
//! # Subsystem Byte
//!
//! The first byte identifies the subsystem that owns the key.
//! A value of 0x00 is reserved as an invalid subsystem.
//!
//! # Version Byte
//!
//! The second byte identifies the key format version. Each subsystem manages
//! its version independently. A version of 0x00 is reserved as an invalid version.
//!
//! # Record Tag Byte
//!
//! The third byte stores the full record tag value.
//!
//! - **Record Tag:** Identifies the kind of record.
//! - `0x00` is reserved as an invalid tag when decoding serialized keys.

use bytes::{BufMut, Bytes, BytesMut};

use super::DeserializeError;

/// A 3-byte key prefix containing subsystem, version, and record tag.
///
/// This type encapsulates the standard prefix used by all OpenData records.
/// It provides methods for serialization, deserialization, and validation.
/// Callers constructing a prefix directly must provide non-zero `subsystem`,
/// `version`, and `record_tag` values.
///
/// # Examples
///
/// ```
/// use common::serde::key_prefix::KeyPrefix;
///
/// // Create a prefix
/// let prefix = KeyPrefix::new(0x10, 0x01, 0x02);
/// assert_eq!(prefix.subsystem(), 0x10);
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
    subsystem: u8,
    version: u8,
    tag: u8,
}

impl KeyPrefix {
    /// Creates a new key prefix with the given subsystem, version, and record tag.
    ///
    /// # Panics
    ///
    /// Panics if `subsystem` is 0, `version` is 0, or `tag` is 0.
    pub fn new(subsystem: u8, version: u8, tag: u8) -> Self {
        assert!(subsystem > 0, "subsystem 0 is reserved");
        assert!(version > 0, "key version 0 is reserved");
        assert!(tag > 0, "record tag 0 is reserved");
        Self {
            subsystem,
            version,
            tag,
        }
    }

    /// Returns the subsystem byte.
    pub fn subsystem(&self) -> u8 {
        self.subsystem
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
    /// - The buffer is too short (less than 3 bytes)
    /// - The subsystem is 0 (reserved)
    /// - The key version is 0 (reserved)
    /// - The record type is 0 (reserved)
    pub fn from_bytes(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < 3 {
            return Err(DeserializeError {
                message: format!(
                    "buffer too short for key prefix: need 3 bytes, got {}",
                    data.len()
                ),
            });
        }
        let subsystem = validate_subsystem(data[0])?;
        let version = validate_key_version(data[1])?;
        let tag = validate_record_tag(data[2])?;
        Ok(Self {
            subsystem,
            version,
            tag,
        })
    }

    /// Parses a key prefix, validating the subsystem and version match expected values.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The buffer is too short
    /// - The subsystem is 0
    /// - The subsystem doesn't match `expected_subsystem`
    /// - The key version is 0
    /// - The version doesn't match `expected_version`
    /// - The record type is 0
    pub fn from_bytes_with_validation(
        data: &[u8],
        expected_subsystem: u8,
        expected_version: u8,
    ) -> Result<Self, DeserializeError> {
        let prefix = Self::from_bytes(data)?;
        if prefix.subsystem != expected_subsystem {
            return Err(DeserializeError {
                message: format!(
                    "invalid subsystem: expected 0x{:02x}, got 0x{:02x}",
                    expected_subsystem, prefix.subsystem
                ),
            });
        }
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

    /// Serializes the prefix to a 3-byte array.
    pub fn to_bytes(&self) -> Bytes {
        Bytes::from(vec![self.subsystem, self.version, self.tag])
    }

    /// Writes the prefix to a buffer.
    pub fn write_to(&self, buf: &mut BytesMut) {
        buf.put_u8(self.subsystem);
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

fn validate_subsystem(byte: u8) -> Result<u8, DeserializeError> {
    if byte == 0 {
        return Err(DeserializeError {
            message: format!(
                "invalid subsystem: 0x{:02x} (subsystem 0 is reserved)",
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
        let subsystem = 0x10;
        let version = 0x01;
        let tag = 0x02;

        // when
        let prefix = KeyPrefix::new(subsystem, version, tag);

        // then
        assert_eq!(prefix.subsystem(), subsystem);
        assert_eq!(prefix.version(), version);
        assert_eq!(prefix.tag(), tag);
    }

    #[test]
    #[should_panic(expected = "subsystem 0 is reserved")]
    fn should_panic_on_zero_subsystem() {
        KeyPrefix::new(0, 0x01, 0x02);
    }

    #[test]
    #[should_panic(expected = "key version 0 is reserved")]
    fn should_panic_on_zero_version() {
        KeyPrefix::new(0x10, 0, 0x02);
    }

    #[test]
    #[should_panic(expected = "record tag 0 is reserved")]
    fn should_panic_on_zero_record_tag() {
        KeyPrefix::new(0x10, 0x01, 0);
    }

    #[test]
    fn should_parse_tag_from_byte() {
        // given
        let bytes = [0x42, 0x17, 0x24];

        // when
        let key_prefix = KeyPrefix::from_bytes(&bytes).unwrap();

        // then
        assert_eq!(key_prefix.subsystem(), 0x42);
        assert_eq!(key_prefix.version(), 0x17);
        assert_eq!(key_prefix.tag(), 0x24);
    }

    #[test]
    fn should_reject_zero_subsystem_byte() {
        // given
        let bytes = [0x00, 0x17, 0x53];

        // when
        let result = KeyPrefix::from_bytes(&bytes);

        // then
        assert!(
            matches!(result, Err(DeserializeError { message }) if  message.contains("subsystem 0 is reserved"))
        );
    }

    #[test]
    fn should_reject_zero_version_byte() {
        // given
        let bytes = [0x53, 0x00, 0x24];

        // when
        let result = KeyPrefix::from_bytes(&bytes);

        // then
        assert!(
            matches!(result, Err(DeserializeError { message }) if  message.contains("version 0 is reserved"))
        );
    }

    #[test]
    fn should_reject_zero_record_type_byte() {
        // given
        let bytes = [0x53, 0x17, 0x00];

        // when
        let result = KeyPrefix::from_bytes(&bytes);

        // then
        assert!(
            matches!(result, Err(DeserializeError { message }) if  message.contains("tag 0 is reserved"))
        );
    }

    #[test]
    fn should_write_and_read_key_prefix() {
        // given
        let prefix = KeyPrefix::new(0x10, 0x01, 0x02);
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
        let prefix = KeyPrefix::new(0x10, 0x01, 0x02);

        // when
        let bytes = prefix.to_bytes();

        // then
        assert_eq!(bytes.len(), 3);
        assert_eq!(bytes[0], 0x10);
        assert_eq!(bytes[1], 0x01);
        assert_eq!(bytes[2], 0x02);
    }

    #[test]
    fn should_parse_key_prefix_with_validation() {
        // given
        let expected_subsystem = 0x10;
        let expected_version = 0x01;
        let data = [expected_subsystem, expected_version, 0x25];

        // when
        let prefix =
            KeyPrefix::from_bytes_with_validation(&data, expected_subsystem, expected_version)
                .unwrap();

        // then
        assert_eq!(prefix.subsystem(), expected_subsystem);
        assert_eq!(prefix.version(), expected_version);
        assert_eq!(prefix.tag(), 0x25);
    }

    #[test]
    fn should_reject_wrong_subsystem() {
        // given
        let data = [0x10, 0x01, 0x25];

        // when
        let result = KeyPrefix::from_bytes_with_validation(&data, 0x20, 0x01);

        // then
        assert!(
            matches!(result, Err(DeserializeError { message }) if  message.contains("invalid subsystem"))
        );
    }

    #[test]
    fn should_reject_wrong_version() {
        // given
        let data = [0x10, 0x02, 0x10]; // wrong version

        // when
        let result = KeyPrefix::from_bytes_with_validation(&data, 0x10, 0x01);

        // then
        assert!(
            matches!(result, Err(DeserializeError { message }) if  message.contains("invalid key version"))
        );
    }

    #[test]
    fn should_reject_short_buffer() {
        // given
        let data = [0x01, 0x10]; // only 2 bytes

        // when
        let result = KeyPrefix::from_bytes(&data);

        // then
        assert!(
            matches!(result, Err(DeserializeError { message }) if  message.contains("buffer too short"))
        );
    }
}

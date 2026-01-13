//! Shared encoding utilities for key-value serialization.
//!
//! This module provides common encoding/decoding primitives used by OpenData
//! storage systems.
//!
//! ## Why Encode/Decode Traits Are Not Defined Here
//!
//! Rust's orphan rules prevent implementing a trait for a type when both the
//! trait AND the type are defined in external crates. This prevents conflicting
//! implementations across the ecosystem.
//!
//! For example, if we defined `Encode` here in `common`, then `timeseries`
//! couldn't implement `common::Encode` for `u32` or `SeriesId` (which is a type
//! alias for `u32`) because:
//! - `Encode` would be foreign to `timeseries` (defined in `common`)
//! - `u32` is foreign to both (defined in `std`)
//!
//! The rule is: at least one of {trait, type} must be local to the crate doing
//! the implementation.
//!
//! Therefore, each storage crate (timeseries, vector) defines its own
//! `Encode`/`Decode` traits locally, allowing them to implement these traits
//! for primitives and type aliases. The generic functions like `encode_array`
//! must also be defined locally since they're bounded by the local traits.

use bytes::BytesMut;

/// Encoding error with a descriptive message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingError {
    pub message: String,
}

impl std::error::Error for EncodingError {}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<super::DeserializeError> for EncodingError {
    fn from(err: super::DeserializeError) -> Self {
        EncodingError {
            message: err.message,
        }
    }
}

/// Encode a UTF-8 string.
///
/// Format: `len: u16` (little-endian) + `len` bytes of UTF-8
pub fn encode_utf8(s: &str, buf: &mut BytesMut) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len > u16::MAX as usize {
        panic!("String too long for UTF-8 encoding: {} bytes", len);
    }
    buf.extend_from_slice(&(len as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// Decode a UTF-8 string.
///
/// Format: `len: u16` (little-endian) + `len` bytes of UTF-8
pub fn decode_utf8(buf: &mut &[u8]) -> Result<String, EncodingError> {
    if buf.len() < 2 {
        return Err(EncodingError {
            message: "Buffer too short for UTF-8 length".to_string(),
        });
    }
    let len = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];

    if buf.len() < len {
        return Err(EncodingError {
            message: format!(
                "Buffer too short for UTF-8 payload: need {} bytes, have {}",
                len,
                buf.len()
            ),
        });
    }

    let bytes = &buf[..len];
    *buf = &buf[len..];

    String::from_utf8(bytes.to_vec()).map_err(|e| EncodingError {
        message: format!("Invalid UTF-8: {}", e),
    })
}

/// Encode an optional non-empty UTF-8 string.
///
/// Format: Same as Utf8, but `len = 0` means `None`
pub fn encode_optional_utf8(opt: Option<&str>, buf: &mut BytesMut) {
    match opt {
        Some(s) => encode_utf8(s, buf),
        None => {
            buf.extend_from_slice(&0u16.to_le_bytes());
        }
    }
}

/// Decode an optional non-empty UTF-8 string.
///
/// Format: Same as Utf8, but `len = 0` means `None`
pub fn decode_optional_utf8(buf: &mut &[u8]) -> Result<Option<String>, EncodingError> {
    if buf.len() < 2 {
        return Err(EncodingError {
            message: "Buffer too short for optional UTF-8 length".to_string(),
        });
    }
    let len = u16::from_le_bytes([buf[0], buf[1]]);
    if len == 0 {
        *buf = &buf[2..];
        return Ok(None);
    }
    decode_utf8(buf).map(Some)
}

/// Decode the count prefix of an array.
///
/// Returns the count as a usize and advances the buffer past the count bytes.
pub fn decode_array_count(buf: &mut &[u8]) -> Result<usize, EncodingError> {
    if buf.len() < 2 {
        return Err(EncodingError {
            message: "Buffer too short for array count".to_string(),
        });
    }
    let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];
    Ok(count)
}

/// Encode the count prefix of an array.
///
/// Panics if the count exceeds u16::MAX.
pub fn encode_array_count(count: usize, buf: &mut BytesMut) {
    if count > u16::MAX as usize {
        panic!("Array too long: {} items", count);
    }
    buf.extend_from_slice(&(count as u16).to_le_bytes());
}

/// Validate that a buffer length is divisible by the element size for fixed-element arrays.
pub fn validate_fixed_element_array_len(
    buf_len: usize,
    element_size: usize,
) -> Result<usize, EncodingError> {
    if !buf_len.is_multiple_of(element_size) {
        return Err(EncodingError {
            message: format!(
                "Buffer length {} is not divisible by element size {}",
                buf_len, element_size
            ),
        });
    }
    Ok(buf_len / element_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_utf8() {
        // given
        let s = "Hello, World!";
        let mut buf = BytesMut::new();

        // when
        encode_utf8(s, &mut buf);
        let mut slice = buf.as_ref();
        let decoded = decode_utf8(&mut slice).unwrap();

        // then
        assert_eq!(decoded, s);
        assert!(slice.is_empty());
    }

    #[test]
    fn should_encode_and_decode_utf8_with_unicode() {
        // given
        let s = "Hello, 世界!";
        let mut buf = BytesMut::new();

        // when
        encode_utf8(s, &mut buf);
        let mut slice = buf.as_ref();
        let decoded = decode_utf8(&mut slice).unwrap();

        // then
        assert_eq!(decoded, s);
        assert!(slice.is_empty());
    }

    #[test]
    fn should_encode_and_decode_optional_utf8_some() {
        // given
        let s = Some("test");
        let mut buf = BytesMut::new();

        // when
        encode_optional_utf8(s, &mut buf);
        let mut slice = buf.as_ref();
        let decoded = decode_optional_utf8(&mut slice).unwrap();

        // then
        assert_eq!(decoded, s.map(|s| s.to_string()));
        assert!(slice.is_empty());
    }

    #[test]
    fn should_encode_and_decode_optional_utf8_none() {
        // given
        let s: Option<&str> = None;
        let mut buf = BytesMut::new();

        // when
        encode_optional_utf8(s, &mut buf);
        let mut slice = buf.as_ref();
        let decoded = decode_optional_utf8(&mut slice).unwrap();

        // then
        assert_eq!(decoded, None);
        assert!(slice.is_empty());
    }

    #[test]
    fn should_return_error_for_truncated_utf8() {
        // given
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&10u16.to_le_bytes()); // claim 10 bytes
        buf.extend_from_slice(b"short"); // only 5 bytes

        // when
        let mut slice = buf.as_ref();
        let result = decode_utf8(&mut slice);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Buffer too short"));
    }
}

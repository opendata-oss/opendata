// Common types, constants, error handling, record tag encoding, and common encoding utilities

use crate::model::BucketSize;
use bytes::BytesMut;

/// Key format version (currently 0x01)
pub const KEY_VERSION: u8 = 0x01;

/// Record type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    BucketList = 0x01,
    SeriesDictionary = 0x02,
    ForwardIndex = 0x03,
    InvertedIndex = 0x04,
    TimeSeries = 0x05,
}

impl RecordType {
    pub fn from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(RecordType::BucketList),
            0x02 => Ok(RecordType::SeriesDictionary),
            0x03 => Ok(RecordType::ForwardIndex),
            0x04 => Ok(RecordType::InvertedIndex),
            0x05 => Ok(RecordType::TimeSeries),
            _ => Err(EncodingError {
                message: format!("Invalid record type: 0x{:02x}", value),
            }),
        }
    }
}

/// Encoding error with a descriptive message
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

/// Encode a record tag from record type and optional bucket size
///
/// - High 4 bits: record type (1-15)
/// - Low 4 bits: bucket size (1-15) for bucket-scoped records, 0x00 for global-scoped
pub fn encode_record_tag(record_type: RecordType, bucket_size: Option<BucketSize>) -> u8 {
    let type_bits = (record_type as u8) << 4;
    let size_bits = bucket_size.unwrap_or(0) & 0x0F;
    type_bits | size_bits
}

/// Decode a record tag into record type and optional bucket size
pub fn decode_record_tag(tag: u8) -> Result<(RecordType, Option<BucketSize>), EncodingError> {
    let record_type = RecordType::from_u8((tag >> 4) & 0x0F)?;
    let bucket_size = tag & 0x0F;
    let size = if bucket_size == 0 {
        None
    } else {
        Some(bucket_size)
    };
    Ok((record_type, size))
}

/// Encode a UTF-8 string
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

/// Decode a UTF-8 string
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

/// Encode an optional non-empty UTF-8 string
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

/// Decode an optional non-empty UTF-8 string
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

/// Trait for types that can be encoded to bytes
pub trait Encode {
    fn encode(&self, buf: &mut BytesMut);
}

/// Trait for types that can be decoded from bytes
pub trait Decode: Sized {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError>;
}

/// Encode an array of encodable items
///
/// Format: `count: u16` (little-endian) + `count` serialized elements
pub fn encode_array<T: Encode>(items: &[T], buf: &mut BytesMut) {
    let count = items.len();
    if count > u16::MAX as usize {
        panic!("Array too long: {} items", count);
    }
    buf.extend_from_slice(&(count as u16).to_le_bytes());
    for item in items {
        item.encode(buf);
    }
}

/// Decode an array of decodable items
///
/// Format: `count: u16` (little-endian) + `count` serialized elements
pub fn decode_array<T: Decode>(buf: &mut &[u8]) -> Result<Vec<T>, EncodingError> {
    if buf.len() < 2 {
        return Err(EncodingError {
            message: "Buffer too short for array count".to_string(),
        });
    }
    let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];

    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(T::decode(buf)?);
    }
    Ok(items)
}

/// Encode a single array (no count prefix)
///
/// Format: Serialized elements back-to-back with no additional padding
pub fn encode_single_array<T: Encode>(items: &[T], buf: &mut BytesMut) {
    for item in items {
        item.encode(buf);
    }
}

/// Decode a single array (no count prefix)
///
/// Note: This requires knowing the number of elements from context or consuming all remaining bytes
pub fn decode_single_array<T: Decode>(
    buf: &mut &[u8],
    count: usize,
) -> Result<Vec<T>, EncodingError> {
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        items.push(T::decode(buf)?);
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_record_tag_global_scoped() {
        // given
        let record_type = RecordType::BucketList;
        let bucket_size = None;

        // when
        let encoded = encode_record_tag(record_type, bucket_size);
        let (decoded_type, decoded_size) = decode_record_tag(encoded).unwrap();

        // then
        assert_eq!(decoded_type, record_type);
        assert_eq!(decoded_size, None);
    }

    #[test]
    fn should_encode_and_decode_record_tag_bucket_scoped() {
        // given
        let record_type = RecordType::TimeSeries;
        let bucket_size = Some(3);

        // when
        let encoded = encode_record_tag(record_type, bucket_size);
        let (decoded_type, decoded_size) = decode_record_tag(encoded).unwrap();

        // then
        assert_eq!(decoded_type, record_type);
        assert_eq!(decoded_size, bucket_size);
    }

    #[test]
    fn should_encode_and_decode_utf8() {
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
    }
}

pub mod bucket_list;
pub mod dictionary;
pub mod forward_index;
pub mod inverted_index;
pub mod key;
pub mod timeseries;

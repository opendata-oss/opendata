#![allow(dead_code)]

pub mod bucket_list;
pub mod dictionary;
pub mod forward_index;
pub mod inverted_index;
pub mod key;
pub mod timeseries;

use crate::model::{RecordTag, TimeBucket};
use bytes::{BufMut, BytesMut};
use common::BytesRange;

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
    /// Returns the ID of this record type (1-15)
    pub fn id(&self) -> u8 {
        *self as u8
    }

    /// Converts a u8 id back to a RecordType
    pub fn from_id(id: u8) -> Result<Self, EncodingError> {
        match id {
            0x01 => Ok(RecordType::BucketList),
            0x02 => Ok(RecordType::SeriesDictionary),
            0x03 => Ok(RecordType::ForwardIndex),
            0x04 => Ok(RecordType::InvertedIndex),
            0x05 => Ok(RecordType::TimeSeries),
            _ => Err(EncodingError {
                message: format!("Invalid record type: 0x{:02x}", id),
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

impl RecordTag {
    /// Creates a new bucket-scoped record tag
    pub fn new_bucket_scoped(
        record_type: RecordType,
        bucket_size: crate::model::BucketSize,
    ) -> Self {
        let type_id = record_type.id();
        assert!(
            type_id <= 0x0F,
            "Record type ID {} exceeds 4-bit range",
            type_id
        );
        assert!(
            bucket_size <= 0x0F,
            "Bucket size {} exceeds 4-bit range",
            bucket_size
        );
        RecordTag((type_id << 4) | bucket_size)
    }

    /// Creates a new global-scoped record tag (bucket size = 0)
    pub fn new_global_scoped(record_type: RecordType) -> Self {
        let type_id = record_type.id();
        assert!(
            type_id <= 0x0F,
            "Record type ID {} exceeds 4-bit range",
            type_id
        );
        RecordTag(type_id << 4)
    }

    /// Creates a RecordTag from a byte
    pub fn from_byte(byte: u8) -> Result<Self, EncodingError> {
        Ok(RecordTag(byte))
    }

    /// Extracts the record type from this tag
    pub fn record_type(&self) -> Result<RecordType, EncodingError> {
        let type_id = (self.0 & 0xF0) >> 4;
        RecordType::from_id(type_id)
    }

    /// Extracts the bucket size from this tag
    /// Returns None if the tag is global-scoped (low 4 bits are 0)
    pub fn bucket_size(&self) -> Option<crate::model::BucketSize> {
        let size_id = self.0 & 0x0F;
        if size_id == 0 { None } else { Some(size_id) }
    }

    /// Returns the byte representation of this tag
    pub fn as_byte(&self) -> u8 {
        self.0
    }

    /// Returns a range of bytes that covers all records of the given type
    /// This is useful for creating scan ranges
    pub fn record_type_range(record_type: RecordType) -> std::ops::Range<u8> {
        let type_id = record_type.id();
        assert!(
            type_id <= 0x0F,
            "Record type ID {} exceeds 4-bit range",
            type_id
        );
        let start = type_id << 4;
        let end = start | 0x0F;
        start..(end + 1)
    }
}

/// Trait for record keys that have a record type
pub trait RecordKey {
    const RECORD_TYPE: RecordType;
}

/// Trait for record keys that are scoped to a specific time bucket.
/// Provides methods to create scan ranges and decode bucket prefixes.
pub trait TimeBucketScoped: RecordKey {
    /// Returns the time bucket for this record
    fn bucket(&self) -> TimeBucket;

    /// Decodes and validates the bucket-scoped prefix of a key.
    /// Returns the TimeBucket if the record type matches the expected type.
    fn decode_bucket_prefix(bytes: &[u8]) -> Result<TimeBucket, EncodingError> {
        if bytes.len() < 7 {
            return Err(EncodingError {
                message: "Buffer too short for bucket prefix".to_string(),
            });
        }

        if bytes[0] != KEY_VERSION {
            return Err(EncodingError {
                message: format!(
                    "Invalid key version: expected 0x{:02x}, got 0x{:02x}",
                    KEY_VERSION, bytes[0]
                ),
            });
        }

        let tag = RecordTag::from_byte(bytes[1])?;
        let record_type = tag.record_type()?;

        if record_type != Self::RECORD_TYPE {
            return Err(EncodingError {
                message: format!(
                    "Invalid record type: expected {:?}, got {:?}",
                    Self::RECORD_TYPE,
                    record_type
                ),
            });
        }

        let bucket_size = tag.bucket_size().ok_or_else(|| EncodingError {
            message: "Record should be bucket-scoped".to_string(),
        })?;

        let start_epoch_min = u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);

        Ok(TimeBucket {
            start: start_epoch_min,
            size: bucket_size,
        })
    }

    /// Create a BytesRange that covers all records of this type
    /// for the given time bucket.
    fn bucket_range(bucket: &TimeBucket) -> BytesRange {
        let mut buf = BytesMut::new();
        buf.put_u8(KEY_VERSION);
        buf.put_u8(RecordTag::new_bucket_scoped(Self::RECORD_TYPE, bucket.size).as_byte());
        buf.put_u32(bucket.start);
        BytesRange::prefix(buf.freeze())
    }
}

/// Helper function to write the bucket-scoped prefix to a buffer
pub fn write_bucket_scoped_prefix<T: TimeBucketScoped>(buf: &mut BytesMut, record: &T) {
    let bucket = record.bucket();
    buf.put_u8(KEY_VERSION);
    buf.put_u8(RecordTag::new_bucket_scoped(T::RECORD_TYPE, bucket.size).as_byte());
    buf.put_u32(bucket.start);
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

/// Encode a fixed-element array (no count prefix)
///
/// Format: Serialized elements back-to-back with no additional padding
pub fn encode_fixed_element_array<T: Encode>(items: &[T], buf: &mut BytesMut) {
    for item in items {
        item.encode(buf);
    }
}

/// Decode a fixed-element array (no count prefix)
///
/// The number of elements is computed by dividing the buffer length by the element size.
/// This function validates that the buffer length is divisible by the element size.
pub fn decode_fixed_element_array<T: Decode>(
    buf: &mut &[u8],
    element_size: usize,
) -> Result<Vec<T>, EncodingError> {
    if !buf.len().is_multiple_of(element_size) {
        return Err(EncodingError {
            message: format!(
                "Buffer length {} is not divisible by element size {}",
                buf.len(),
                element_size
            ),
        });
    }

    let count = buf.len() / element_size;
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
        let record_tag = RecordTag::new_global_scoped(RecordType::BucketList);

        // when
        let encoded = record_tag.as_byte();
        let decoded = RecordTag::from_byte(encoded).unwrap();

        // then
        assert_eq!(decoded.as_byte(), record_tag.as_byte());
        assert_eq!(decoded.record_type().unwrap(), RecordType::BucketList);
        assert_eq!(decoded.bucket_size(), None);
    }

    #[test]
    fn should_encode_and_decode_record_tag_bucket_scoped() {
        // given
        let record_tag = RecordTag::new_bucket_scoped(RecordType::TimeSeries, 3);

        // when
        let encoded = record_tag.as_byte();
        let decoded = RecordTag::from_byte(encoded).unwrap();

        // then
        assert_eq!(decoded.as_byte(), record_tag.as_byte());
        assert_eq!(decoded.record_type().unwrap(), RecordType::TimeSeries);
        assert_eq!(decoded.bucket_size(), Some(3));
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

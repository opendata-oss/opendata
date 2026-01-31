//! Serialization/deserialization for vector database storage.
//!
//! This module implements the key/value encoding scheme defined in RFC 0001.

pub mod centroid_chunk;
pub mod collection_meta;
pub mod deletions;
pub mod id_dictionary;
pub mod key;
pub mod metadata_index;
pub mod posting_list;
pub mod vector_bitmap;
pub mod vector_data;
pub mod vector_meta;

use bytes::BytesMut;

// Re-export encoding utilities from common
pub use common::serde::encoding::{
    EncodingError, decode_optional_utf8, decode_utf8, encode_optional_utf8, encode_utf8,
};
use common::serde::key_prefix::{KeyPrefix, RecordTag};

/// Key format version (currently 0x01)
pub const KEY_VERSION: u8 = 0x01;

/// Record type enumeration for vector database records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    CollectionMeta = 0x01,
    Deletions = 0x02,
    CentroidChunk = 0x03,
    PostingList = 0x04,
    IdDictionary = 0x05,
    VectorData = 0x06,
    VectorMeta = 0x07,
    MetadataIndex = 0x08,
    SeqBlock = 0x09,
}

impl RecordType {
    /// Returns the ID of this record type (1-15).
    pub fn id(&self) -> u8 {
        *self as u8
    }

    /// Converts a u8 id back to a RecordType.
    pub fn from_id(id: u8) -> Result<Self, EncodingError> {
        match id {
            0x01 => Ok(RecordType::CollectionMeta),
            0x02 => Ok(RecordType::Deletions),
            0x03 => Ok(RecordType::CentroidChunk),
            0x04 => Ok(RecordType::PostingList),
            0x05 => Ok(RecordType::IdDictionary),
            0x06 => Ok(RecordType::VectorData),
            0x07 => Ok(RecordType::VectorMeta),
            0x08 => Ok(RecordType::MetadataIndex),
            0x09 => Ok(RecordType::SeqBlock),
            _ => Err(EncodingError {
                message: format!("Invalid record type: 0x{:02x}", id),
            }),
        }
    }

    /// Creates a RecordTag for this record type (reserved bits = 0).
    pub fn tag(&self) -> RecordTag {
        RecordTag::new(self.id(), 0)
    }

    /// Creates a KeyPrefix with the current version for this record type.
    pub fn prefix(&self) -> KeyPrefix {
        KeyPrefix::new(KEY_VERSION, self.tag())
    }
}

/// Extracts the RecordType from a RecordTag.
pub fn record_type_from_tag(tag: RecordTag) -> Result<RecordType, EncodingError> {
    RecordType::from_id(tag.record_type())
}

/// Trait for record keys that have a record type.
pub trait RecordKey {
    const RECORD_TYPE: RecordType;
}

/// Type discriminant for metadata field values.
///
/// Used in:
/// - `CollectionMeta`: Defines the schema type for each metadata field
/// - `FieldValue`: Tags the variant in serialized form
/// - `MetadataIndexKey`: Identifies the value type in index keys
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FieldType {
    String = 0,
    Int64 = 1,
    Float64 = 2,
    Bool = 3,
}

impl FieldType {
    pub fn from_byte(byte: u8) -> Result<Self, EncodingError> {
        match byte {
            0 => Ok(FieldType::String),
            1 => Ok(FieldType::Int64),
            2 => Ok(FieldType::Float64),
            3 => Ok(FieldType::Bool),
            _ => Err(EncodingError {
                message: format!("Invalid field type: {}", byte),
            }),
        }
    }
}

/// A metadata field value (tagged union).
///
/// Used in:
/// - `VectorMeta`: Stores actual metadata values for each vector
/// - `MetadataIndexKey`: Encodes values in index keys for filtering
///
/// ## Encoding
///
/// This type has two encoding modes:
/// - **Value encoding** (via `Encode`/`Decode` traits): Little-endian, used in `VectorMeta`
/// - **Sortable encoding** (via `encode_sortable`/`decode_sortable`): Big-endian with
///   sign-bit transformations, used in `MetadataIndexKey` for correct lexicographic ordering
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

impl FieldValue {
    /// Returns the type discriminant for this value.
    pub fn field_type(&self) -> FieldType {
        match self {
            FieldValue::String(_) => FieldType::String,
            FieldValue::Int64(_) => FieldType::Int64,
            FieldValue::Float64(_) => FieldType::Float64,
            FieldValue::Bool(_) => FieldType::Bool,
        }
    }

    /// Encode the field value for use in keys (sortable encoding).
    ///
    /// Uses big-endian sortable encodings for numeric types:
    /// - Int64: XOR with sign bit to make negative numbers sort before positive
    /// - Float64: IEEE 754 sortable encoding
    ///
    /// This encoding preserves lexicographic ordering in keys:
    /// - Strings: Natural byte ordering via TerminatedBytes
    /// - Integers: -100 < 0 < 100
    /// - Floats: -1.0 < 0.0 < 1.0
    /// - Bools: false < true
    pub fn encode_sortable(&self, buf: &mut BytesMut) {
        use bytes::BufMut;
        use common::serde::sortable::{encode_f64_sortable, encode_i64_sortable};
        use common::serde::terminated_bytes;

        match self {
            FieldValue::String(s) => {
                buf.put_u8(FieldType::String as u8);
                terminated_bytes::serialize(s.as_bytes(), buf);
            }
            FieldValue::Int64(v) => {
                buf.put_u8(FieldType::Int64 as u8);
                buf.put_u64(encode_i64_sortable(*v));
            }
            FieldValue::Float64(v) => {
                buf.put_u8(FieldType::Float64 as u8);
                buf.put_u64(encode_f64_sortable(*v));
            }
            FieldValue::Bool(v) => {
                buf.put_u8(FieldType::Bool as u8);
                buf.put_u8(if *v { 1 } else { 0 });
            }
        }
    }

    /// Decode a field value from sortable key encoding.
    pub fn decode_sortable(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        use common::serde::sortable::{decode_f64_sortable, decode_i64_sortable};
        use common::serde::terminated_bytes;

        if buf.is_empty() {
            return Err(EncodingError {
                message: "Buffer too short for FieldValue type".to_string(),
            });
        }

        let field_type = FieldType::from_byte(buf[0])?;
        *buf = &buf[1..];

        match field_type {
            FieldType::String => {
                let bytes = terminated_bytes::deserialize(buf).map_err(|e| EncodingError {
                    message: format!("Failed to decode string value: {}", e),
                })?;
                let s = String::from_utf8(bytes.to_vec()).map_err(|e| EncodingError {
                    message: format!("Invalid UTF-8 in string value: {}", e),
                })?;
                Ok(FieldValue::String(s))
            }
            FieldType::Int64 => {
                if buf.len() < 8 {
                    return Err(EncodingError {
                        message: "Buffer too short for Int64 value".to_string(),
                    });
                }
                let sortable = u64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]);
                *buf = &buf[8..];
                Ok(FieldValue::Int64(decode_i64_sortable(sortable)))
            }
            FieldType::Float64 => {
                if buf.len() < 8 {
                    return Err(EncodingError {
                        message: "Buffer too short for Float64 value".to_string(),
                    });
                }
                let sortable = u64::from_be_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]);
                *buf = &buf[8..];
                Ok(FieldValue::Float64(decode_f64_sortable(sortable)))
            }
            FieldType::Bool => {
                if buf.is_empty() {
                    return Err(EncodingError {
                        message: "Buffer too short for Bool value".to_string(),
                    });
                }
                let value = buf[0] != 0;
                *buf = &buf[1..];
                Ok(FieldValue::Bool(value))
            }
        }
    }
}

/// Trait for types that can be encoded to bytes.
pub trait Encode {
    fn encode(&self, buf: &mut BytesMut);
}

/// Trait for types that can be decoded from bytes.
pub trait Decode: Sized {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError>;
}

/// Encode an array of encodable items.
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

/// Decode an array of decodable items.
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

/// Encode a fixed-element array (no count prefix).
///
/// Format: Serialized elements back-to-back with no additional padding
pub fn encode_fixed_element_array<T: Encode>(items: &[T], buf: &mut BytesMut) {
    for item in items {
        item.encode(buf);
    }
}

/// Decode a fixed-element array (no count prefix).
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

// Implement Encode/Decode for primitive types

impl Encode for f32 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for f32 {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 4 {
            return Err(EncodingError {
                message: "Buffer too short for f32".to_string(),
            });
        }
        let value = f32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        *buf = &buf[4..];
        Ok(value)
    }
}

impl Encode for u8 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&[*self]);
    }
}

impl Decode for u8 {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.is_empty() {
            return Err(EncodingError {
                message: "Buffer too short for u8".to_string(),
            });
        }
        let value = buf[0];
        *buf = &buf[1..];
        Ok(value)
    }
}

impl Encode for u16 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for u16 {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for u16".to_string(),
            });
        }
        let value = u16::from_le_bytes([buf[0], buf[1]]);
        *buf = &buf[2..];
        Ok(value)
    }
}

impl Encode for u32 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for u32 {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 4 {
            return Err(EncodingError {
                message: "Buffer too short for u32".to_string(),
            });
        }
        let value = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        *buf = &buf[4..];
        Ok(value)
    }
}

impl Encode for u64 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for u64 {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 8 {
            return Err(EncodingError {
                message: "Buffer too short for u64".to_string(),
            });
        }
        let value = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        *buf = &buf[8..];
        Ok(value)
    }
}

impl Encode for i64 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for i64 {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 8 {
            return Err(EncodingError {
                message: "Buffer too short for i64".to_string(),
            });
        }
        let value = i64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        *buf = &buf[8..];
        Ok(value)
    }
}

impl Encode for f64 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.to_le_bytes());
    }
}

impl Decode for f64 {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 8 {
            return Err(EncodingError {
                message: "Buffer too short for f64".to_string(),
            });
        }
        let value = f64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        *buf = &buf[8..];
        Ok(value)
    }
}

/// Encode implementation for FieldValue (value encoding, little-endian).
///
/// This is used in `VectorMeta` for storing metadata values.
impl Encode for FieldValue {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            FieldValue::String(s) => {
                buf.extend_from_slice(&[FieldType::String as u8]);
                encode_utf8(s, buf);
            }
            FieldValue::Int64(v) => {
                buf.extend_from_slice(&[FieldType::Int64 as u8]);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            FieldValue::Float64(v) => {
                buf.extend_from_slice(&[FieldType::Float64 as u8]);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            FieldValue::Bool(v) => {
                buf.extend_from_slice(&[FieldType::Bool as u8]);
                buf.extend_from_slice(&[if *v { 1 } else { 0 }]);
            }
        }
    }
}

/// Decode implementation for FieldValue (value encoding, little-endian).
impl Decode for FieldValue {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.is_empty() {
            return Err(EncodingError {
                message: "Buffer too short for FieldValue type".to_string(),
            });
        }

        let field_type = FieldType::from_byte(buf[0])?;
        *buf = &buf[1..];

        match field_type {
            FieldType::String => {
                let s = decode_utf8(buf)?;
                Ok(FieldValue::String(s))
            }
            FieldType::Int64 => {
                if buf.len() < 8 {
                    return Err(EncodingError {
                        message: "Buffer too short for Int64 value".to_string(),
                    });
                }
                let v = i64::from_le_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]);
                *buf = &buf[8..];
                Ok(FieldValue::Int64(v))
            }
            FieldType::Float64 => {
                if buf.len() < 8 {
                    return Err(EncodingError {
                        message: "Buffer too short for Float64 value".to_string(),
                    });
                }
                let v = f64::from_le_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]);
                *buf = &buf[8..];
                Ok(FieldValue::Float64(v))
            }
            FieldType::Bool => {
                if buf.is_empty() {
                    return Err(EncodingError {
                        message: "Buffer too short for Bool value".to_string(),
                    });
                }
                let v = buf[0] != 0;
                *buf = &buf[1..];
                Ok(FieldValue::Bool(v))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_record_tag() {
        // given
        let record_tag = RecordType::CollectionMeta.tag();

        // when
        let encoded = record_tag.as_byte();
        let decoded = RecordTag::from_byte(encoded).unwrap();

        // then
        assert_eq!(decoded.as_byte(), record_tag.as_byte());
        assert_eq!(
            record_type_from_tag(decoded).unwrap(),
            RecordType::CollectionMeta
        );
    }

    #[test]
    fn should_convert_all_record_types() {
        // given
        let types = [
            RecordType::CollectionMeta,
            RecordType::Deletions,
            RecordType::CentroidChunk,
            RecordType::PostingList,
            RecordType::IdDictionary,
            RecordType::VectorData,
            RecordType::VectorMeta,
            RecordType::MetadataIndex,
            RecordType::SeqBlock,
        ];

        for record_type in types {
            // when
            let id = record_type.id();
            let recovered = RecordType::from_id(id).unwrap();

            // then
            assert_eq!(recovered, record_type);
        }
    }

    #[test]
    fn should_encode_and_decode_f32() {
        // given
        let value = 1.23456f32;
        let mut buf = BytesMut::new();

        // when
        value.encode(&mut buf);
        let mut slice = buf.as_ref();
        let decoded = f32::decode(&mut slice).unwrap();

        // then
        assert_eq!(decoded, value);
        assert!(slice.is_empty());
    }

    #[test]
    fn should_encode_and_decode_u64() {
        // given
        let value = 0xDEADBEEF_CAFEBABE_u64;
        let mut buf = BytesMut::new();

        // when
        value.encode(&mut buf);
        let mut slice = buf.as_ref();
        let decoded = u64::decode(&mut slice).unwrap();

        // then
        assert_eq!(decoded, value);
        assert!(slice.is_empty());
    }

    #[test]
    fn should_encode_and_decode_fixed_element_array() {
        // given
        let values = vec![1.0f32, 2.0, 3.0, 4.0];
        let mut buf = BytesMut::new();

        // when
        encode_fixed_element_array(&values, &mut buf);
        let mut slice = buf.as_ref();
        let decoded: Vec<f32> = decode_fixed_element_array(&mut slice, 4).unwrap();

        // then
        assert_eq!(decoded, values);
        assert!(slice.is_empty());
    }
}

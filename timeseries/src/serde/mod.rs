pub mod bucket_list;
pub mod dictionary;
pub mod forward_index;
pub mod inverted_index;
pub mod key;
pub mod timeseries;

use crate::model::{BucketSize, RecordTag, TimeBucket};
use bytes::{BufMut, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::KeyPrefix;

// Re-export encoding utilities from common
pub use common::serde::encoding::{
    EncodingError, decode_optional_utf8, decode_utf8, encode_optional_utf8, encode_utf8,
};

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

/// Key format version (currently 0x01)
pub const KEY_VERSION: u8 = 0x01;

/// Record type enumeration for timeseries storage.
///
/// Record types are encoded in the high 4 bits of the record tag byte,
/// following RFC 0001: Record Key Prefix. The low 4 bits are used for
/// bucket size in bucket-scoped records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    BucketList = 0x01,
    SeriesDictionary = 0x02,
    ForwardIndex = 0x03,
    InvertedIndex = 0x04,
    TimeSeries = 0x05,
}

impl RecordType {
    /// Returns the record type ID (1-15).
    pub fn id(&self) -> u8 {
        *self as u8
    }

    /// Converts a record type ID back to a RecordType.
    pub fn from_id(id: u8) -> Result<Self, EncodingError> {
        match id {
            0x01 => Ok(RecordType::BucketList),
            0x02 => Ok(RecordType::SeriesDictionary),
            0x03 => Ok(RecordType::ForwardIndex),
            0x04 => Ok(RecordType::InvertedIndex),
            0x05 => Ok(RecordType::TimeSeries),
            _ => Err(EncodingError {
                message: format!("invalid record type: 0x{:02x}", id),
            }),
        }
    }

    /// Creates a global-scoped RecordTag (reserved bits = 0).
    pub fn tag(&self) -> RecordTag {
        RecordTag::new(self.id(), 0)
    }

    /// Creates a bucket-scoped RecordTag with the given bucket size.
    pub fn tag_with_bucket_size(&self, bucket_size: BucketSize) -> RecordTag {
        RecordTag::new(self.id(), bucket_size)
    }

    /// Creates a global-scoped KeyPrefix with the current version.
    pub fn prefix(&self) -> KeyPrefix {
        KeyPrefix::new(KEY_VERSION, self.tag())
    }

    /// Creates a bucket-scoped KeyPrefix with the current version.
    pub fn prefix_with_bucket_size(&self, bucket_size: BucketSize) -> KeyPrefix {
        KeyPrefix::new(KEY_VERSION, self.tag_with_bucket_size(bucket_size))
    }
}

/// Extracts the RecordType from a RecordTag.
pub fn record_type_from_tag(tag: RecordTag) -> Result<RecordType, EncodingError> {
    RecordType::from_id(tag.record_type())
}

/// Extracts the bucket size from a RecordTag.
///
/// Returns None if the reserved bits are 0 (global-scoped record).
pub fn bucket_size_from_tag(tag: RecordTag) -> Option<BucketSize> {
    let size = tag.reserved();
    if size == 0 { None } else { Some(size) }
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
        let prefix = KeyPrefix::from_bytes_versioned(bytes, KEY_VERSION)?;
        let record_type = record_type_from_tag(prefix.tag())?;

        if record_type != Self::RECORD_TYPE {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected {:?}, got {:?}",
                    Self::RECORD_TYPE,
                    record_type
                ),
            });
        }

        let bucket_size = bucket_size_from_tag(prefix.tag()).ok_or_else(|| EncodingError {
            message: "record should be bucket-scoped".to_string(),
        })?;

        if bytes.len() < 6 {
            return Err(EncodingError {
                message: "buffer too short for bucket prefix".to_string(),
            });
        }

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
        Self::RECORD_TYPE
            .prefix_with_bucket_size(bucket.size)
            .write_to(&mut buf);
        buf.put_u32(bucket.start);
        BytesRange::prefix(buf.freeze())
    }
}

/// Helper function to write the bucket-scoped prefix to a buffer
pub fn write_bucket_scoped_prefix<T: TimeBucketScoped>(buf: &mut BytesMut, record: &T) {
    let bucket = record.bucket();
    T::RECORD_TYPE
        .prefix_with_bucket_size(bucket.size)
        .write_to(buf);
    buf.put_u32(bucket.start);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_record_tag_global_scoped() {
        // given
        let record_tag = RecordType::BucketList.tag();

        // when
        let encoded = record_tag.as_byte();
        let decoded = RecordTag::from_byte(encoded).unwrap();

        // then
        assert_eq!(decoded.as_byte(), record_tag.as_byte());
        assert_eq!(
            record_type_from_tag(decoded).unwrap(),
            RecordType::BucketList
        );
        assert_eq!(bucket_size_from_tag(decoded), None);
    }

    #[test]
    fn should_encode_and_decode_record_tag_bucket_scoped() {
        // given
        let record_tag = RecordType::TimeSeries.tag_with_bucket_size(3);

        // when
        let encoded = record_tag.as_byte();
        let decoded = RecordTag::from_byte(encoded).unwrap();

        // then
        assert_eq!(decoded.as_byte(), record_tag.as_byte());
        assert_eq!(
            record_type_from_tag(decoded).unwrap(),
            RecordType::TimeSeries
        );
        assert_eq!(bucket_size_from_tag(decoded), Some(3));
    }
}

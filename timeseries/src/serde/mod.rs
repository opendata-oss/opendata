pub mod bucket_list;
pub mod dictionary;
pub mod forward_index;
pub mod inverted_index;
pub mod key;
pub mod timeseries;

use crate::model::{BucketSize, BucketStart, TimeBucket};
use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::{KEY_PREFIX_LEN, KeyPrefix};

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

/// Key format version (currently 0x02).
pub const KEY_VERSION: u8 = 0x02;

/// Subsystem byte for timeseries storage (see [`common::serde::subsystem`]).
pub const SUBSYSTEM: u8 = common::serde::subsystem::TIMESERIES;

/// Length of the bucket-scoped header: `[subsystem(1), version(1),
/// time_bucket(4 BE), bucket_size(1), record_type(1)]`.
pub const PREFIX_AND_RECORD_TYPE_LEN: usize = KEY_PREFIX_LEN + 4 + 1 + 1;

/// Record type enumeration for timeseries storage.
///
/// Encoded as a single byte at position 7 of every bucket-scoped key. The
/// `BucketList` global record is the lone exception and lives at position 2
/// of a 3-byte global-scoped key — it's removed in a follow-up commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    BucketList = 0x01,
    SeriesDictionary = 0x02,
    ForwardIndex = 0x03,
    InvertedIndex = 0x04,
    TimeSeries = 0x05,
}

impl RecordType {
    /// Returns the record type ID.
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

    /// Encodes the 3-byte global-scoped prefix `[subsystem, version,
    /// record_type]`. Only valid for `BucketList`; bucket-scoped records use
    /// [`write_record_prefix`].
    pub fn encode_prefix(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(KEY_PREFIX_LEN + 1);
        KeyPrefix::new(SUBSYSTEM, KEY_VERSION).write_to(&mut buf);
        buf.put_u8(self.id());
        buf.freeze()
    }
}

/// Writes the 8-byte bucket-scoped header `[subsystem, version,
/// time_bucket(4 BE), bucket_size, record_type]` to `buf`.
///
/// `bucket.size == 0` is reserved and panics.
pub fn write_record_prefix(buf: &mut BytesMut, bucket: &TimeBucket, record_type: RecordType) {
    assert!(bucket.size != 0, "bucket_size 0 is reserved");
    KeyPrefix::new(SUBSYSTEM, KEY_VERSION).write_to(buf);
    buf.put_u32(bucket.start);
    buf.put_u8(bucket.size);
    buf.put_u8(record_type.id());
}

/// Reads the bucket-scoped header from `buf`, validating subsystem, version,
/// `bucket_size`, and the record type ID. Returns the parsed `TimeBucket` and
/// `RecordType`.
pub fn parse_bucket_record_type(buf: &[u8]) -> Result<(TimeBucket, RecordType), EncodingError> {
    KeyPrefix::from_bytes_with_validation(buf, SUBSYSTEM, KEY_VERSION)?;
    if buf.len() < PREFIX_AND_RECORD_TYPE_LEN {
        return Err(EncodingError {
            message: format!(
                "Buffer too short for bucket-scoped header: need {} bytes, got {}",
                PREFIX_AND_RECORD_TYPE_LEN,
                buf.len()
            ),
        });
    }
    let start = BucketStart::from_be_bytes([
        buf[KEY_PREFIX_LEN],
        buf[KEY_PREFIX_LEN + 1],
        buf[KEY_PREFIX_LEN + 2],
        buf[KEY_PREFIX_LEN + 3],
    ]);
    let size: BucketSize = buf[KEY_PREFIX_LEN + 4];
    if size == 0 {
        return Err(EncodingError {
            message: "bucket_size 0 is reserved".to_string(),
        });
    }
    let record_type = RecordType::from_id(buf[KEY_PREFIX_LEN + 5])?;
    Ok((TimeBucket { start, size }, record_type))
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

    /// Decodes and validates the bucket-scoped header of a key.
    /// Returns the `TimeBucket` when the encoded record type matches
    /// `Self::RECORD_TYPE`.
    fn decode_bucket_prefix(bytes: &[u8]) -> Result<TimeBucket, EncodingError> {
        let (bucket, record_type) = parse_bucket_record_type(bytes)?;
        if record_type != Self::RECORD_TYPE {
            return Err(EncodingError {
                message: format!(
                    "invalid record type: expected {:?}, got {:?}",
                    Self::RECORD_TYPE,
                    record_type
                ),
            });
        }
        Ok(bucket)
    }

    /// Create a BytesRange that covers all records of this type
    /// for the given time bucket.
    fn bucket_range(bucket: &TimeBucket) -> BytesRange {
        let mut buf = BytesMut::new();
        write_record_prefix(&mut buf, bucket, Self::RECORD_TYPE);
        BytesRange::prefix(buf.freeze())
    }
}

/// Helper function to write the bucket-scoped header to a buffer.
pub fn write_bucket_scoped_prefix<T: TimeBucketScoped>(buf: &mut BytesMut, record: &T) {
    write_record_prefix(buf, &record.bucket(), T::RECORD_TYPE);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_global_scoped_prefix() {
        // given
        let encoded = RecordType::BucketList.encode_prefix();

        // then
        assert_eq!(encoded.len(), KEY_PREFIX_LEN + 1);
        assert_eq!(encoded[0], SUBSYSTEM);
        assert_eq!(encoded[1], KEY_VERSION);
        assert_eq!(encoded[2], RecordType::BucketList.id());
    }

    #[test]
    fn should_round_trip_bucket_scoped_header() {
        // given
        let bucket = TimeBucket {
            start: 12345,
            size: 1,
        };
        let mut buf = BytesMut::new();

        // when
        write_record_prefix(&mut buf, &bucket, RecordType::TimeSeries);
        let (decoded_bucket, decoded_type) = parse_bucket_record_type(&buf).unwrap();

        // then
        assert_eq!(buf.len(), PREFIX_AND_RECORD_TYPE_LEN);
        assert_eq!(decoded_bucket, bucket);
        assert_eq!(decoded_type, RecordType::TimeSeries);
    }

    #[test]
    #[should_panic(expected = "bucket_size 0 is reserved")]
    fn should_panic_when_writing_zero_bucket_size() {
        let mut buf = BytesMut::new();
        write_record_prefix(
            &mut buf,
            &TimeBucket { start: 0, size: 0 },
            RecordType::TimeSeries,
        );
    }

    #[test]
    fn parse_bucket_record_type_rejects_zero_bucket_size() {
        let mut buf = BytesMut::new();
        KeyPrefix::new(SUBSYSTEM, KEY_VERSION).write_to(&mut buf);
        buf.put_u32(0);
        buf.put_u8(0);
        buf.put_u8(RecordType::TimeSeries.id());

        let err = parse_bucket_record_type(&buf).unwrap_err();

        assert!(err.message.contains("bucket_size 0 is reserved"));
    }

    #[test]
    fn parse_bucket_record_type_rejects_short_buffer() {
        let buf = [SUBSYSTEM, KEY_VERSION, 0, 0, 0, 0, 1];
        let err = parse_bucket_record_type(&buf).unwrap_err();
        assert!(err.message.contains("Buffer too short"));
    }
}

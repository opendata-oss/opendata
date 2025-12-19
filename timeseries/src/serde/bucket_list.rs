// BucketList value structure

use super::*;
use crate::model::{BucketSize, BucketStart};
use bytes::{Bytes, BytesMut};

/// BucketList value: FixedElementArray<(bucket_size: u8, time_bucket: u32)>
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketListValue {
    pub buckets: Vec<(BucketSize, BucketStart)>,
}

impl BucketListValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        encode_fixed_element_array(&self.buckets, &mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        // Each (BucketSize, BucketStart) tuple is 5 bytes: 1 byte (u8) + 4 bytes (u32)
        const TUPLE_SIZE: usize = 1 + 4;

        let mut slice = buf;
        let buckets = decode_fixed_element_array(&mut slice, TUPLE_SIZE)?;
        Ok(BucketListValue { buckets })
    }
}

impl Encode for (BucketSize, BucketStart) {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&[self.0]);
        buf.extend_from_slice(&self.1.to_le_bytes());
    }
}

impl Decode for (BucketSize, BucketStart) {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 1 + 4 {
            return Err(EncodingError {
                message: "Buffer too short for (TimeBucketSize, TimeBucket)".to_string(),
            });
        }
        let bucket_size = buf[0];
        *buf = &buf[1..];
        let time_bucket = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        *buf = &buf[4..];
        Ok((bucket_size, time_bucket))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_bucket_list_value() {
        // given
        let value = BucketListValue {
            buckets: vec![(1, 100), (2, 200), (3, 300)],
        };

        // when
        let encoded = value.encode();
        let decoded = BucketListValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_empty_bucket_list_value() {
        // given
        let value = BucketListValue { buckets: vec![] };

        // when
        let encoded = value.encode();
        let decoded = BucketListValue::decode(encoded.as_ref()).unwrap();

        // then
        assert_eq!(decoded, value);
    }
}

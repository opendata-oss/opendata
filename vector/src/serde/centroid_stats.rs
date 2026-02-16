//! CentroidStats value encoding/decoding.
//!
//! Stores the number of vectors assigned to a centroid. Used as a merge-based
//! counter: each write is a delta that gets summed by the merge operator.
//!
//! ## Value Layout
//!
//! ```text
//! +-------------------------------+
//! |  num_vectors: i32 LE (4B)     |
//! +-------------------------------+
//! ```

use super::EncodingError;
use bytes::{Bytes, BytesMut};

/// Per-centroid vector count, stored as an i32 delta for merge-based summation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentroidStatsValue {
    pub num_vectors: i32,
}

impl CentroidStatsValue {
    pub fn new(num_vectors: i32) -> Self {
        Self { num_vectors }
    }

    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(&self.num_vectors.to_le_bytes());
        buf.freeze()
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 4 {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for CentroidStatsValue: expected 4 bytes, got {}",
                    buf.len()
                ),
            });
        }
        let num_vectors = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        Ok(Self { num_vectors })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_positive_value() {
        // given
        let value = CentroidStatsValue::new(42);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidStatsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert_eq!(encoded.len(), 4);
    }

    #[test]
    fn should_encode_and_decode_zero() {
        // given
        let value = CentroidStatsValue::new(0);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidStatsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_negative_value() {
        // given
        let value = CentroidStatsValue::new(-5);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidStatsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_reject_short_buffer() {
        // given
        let buf = [0u8; 3];

        // when
        let result = CentroidStatsValue::decode_from_bytes(&buf);

        // then
        assert!(result.is_err());
    }
}

//! CentroidInfo value encoding/decoding.
//!
//! Stores per-centroid metadata for the centroid tree.
//!
//! ## Value Layout
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │  level:           u8                                     │
//! │  vector:          Array<f32>                             │
//! │  has_parent:      u8 (0 = none, 1 = some)               │
//! │  parent_vector_id: u64 LE (present when has_parent)     │
//! └────────────────────────────────────────────────────────────┘
//! ```

use super::{Decode, Encode, EncodingError, decode_array, encode_array};
use bytes::{BufMut, Bytes, BytesMut};
use crate::serde::vector_id::{VectorId, ROOT_VECTOR_ID};

/// Per-centroid metadata value.
#[derive(Debug, Clone, PartialEq)]
pub struct CentroidInfoValue {
    pub level: u8,
    pub vector: Vec<f32>,
    pub parent_vector_id: VectorId,
}

impl CentroidInfoValue {
    pub fn new(level: u8, vector: Vec<f32>, parent_vector_id: VectorId) -> Self {
        assert!(parent_vector_id.level() == level + 1 || parent_vector_id == ROOT_VECTOR_ID);
        Self {
            level,
            vector,
            parent_vector_id,
        }
    }

    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.encode(&mut buf);
        buf.freeze()
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut slice = buf;
        Self::decode(&mut slice)
    }
}

impl Encode for CentroidInfoValue {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.level);
        encode_array(&self.vector, buf);
        self.parent_vector_id.encode(buf);
    }
}

impl Decode for CentroidInfoValue {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.is_empty() {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for CentroidInfoValue level: expected at least 1 byte, got {}",
                    buf.len()
                ),
            });
        }

        let level = buf[0];
        *buf = &buf[1..];

        let vector = decode_array(buf)?;

        if buf.is_empty() {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for CentroidInfoValue parent flag: expected at least 1 byte, got {}",
                    buf.len()
                ),
            });
        }
        *buf = &buf[1..];
        let parent_vector_id = VectorId::decode(buf)?;
        Ok(Self {
            level,
            vector,
            parent_vector_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_centroid_info_with_parent() {
        // given
        let value = CentroidInfoValue::new(2, vec![1.0, 2.0, 3.0], VectorId::centroid_id(1, 123));

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidInfoValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_centroid_info_with_root_parent() {
        // given
        let value = CentroidInfoValue::new(3, vec![4.0, 5.0], ROOT_VECTOR_ID);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidInfoValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_high_dimensional_centroid_info() {
        // given
        let value = CentroidInfoValue::new(7, vec![0.0, 1.0, 2.0, 3.0, 4.0], ROOT_VECTOR_ID);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidInfoValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_reject_invalid_parent_flag() {
        // given
        let buf = [1u8, 0u8, 0u8, 2u8];

        // when
        let result = CentroidInfoValue::decode_from_bytes(&buf);

        // then
        assert!(result.is_err());
    }
}

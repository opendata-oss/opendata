//! Centroids value encoding/decoding.
//!
//! Stores singleton metadata for the centroid tree.
//!
//! ## Value Layout
//!
//! ```text
//! +-------------------------------+
//! |  depth: u8 (1B)               |
//! +-------------------------------+
//! ```

use super::EncodingError;
use bytes::{Bytes, BytesMut};

/// Singleton centroid tree metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentroidsValue {
    pub depth: u8,
}

impl CentroidsValue {
    pub fn new(depth: u8) -> Self {
        Self { depth }
    }

    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1);
        buf.extend_from_slice(&[self.depth]);
        buf.freeze()
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.is_empty() {
            return Err(EncodingError {
                message: "Buffer too short for CentroidsValue: expected 1 byte, got 0".to_string(),
            });
        }

        Ok(Self { depth: buf[0] })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_centroids_value() {
        // given
        let value = CentroidsValue::new(3);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert_eq!(encoded.len(), 1);
    }

    #[test]
    fn should_reject_short_buffer() {
        // given
        let buf = [];

        // when
        let result = CentroidsValue::decode_from_bytes(&buf);

        // then
        assert!(result.is_err());
    }
}

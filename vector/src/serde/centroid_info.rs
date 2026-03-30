//! CentroidInfo value encoding/decoding.
//!
//! Stores per-centroid metadata for the centroid tree.
//!
//! ## Value Layout
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │  entries: Array<CentroidInfoEntry>                        │
//! │                                                           │
//! │  CentroidInfoEntry                                        │
//! │  ┌──────────────────────────────────────────────────────┐ │
//! │  │  level:           u8                                 │ │
//! │  │  has_parent:      u8 (0 = none, 1 = some)           │ │
//! │  │  parent_vector_id: u64 LE (present when has_parent) │ │
//! │  └──────────────────────────────────────────────────────┘ │
//! └────────────────────────────────────────────────────────────┘
//! ```

use super::{Decode, Encode, EncodingError, decode_array, encode_array};
use bytes::{BufMut, Bytes, BytesMut};

/// One centroid metadata entry.
#[derive(Debug, Clone, PartialEq)]
pub struct CentroidInfoEntry {
    pub level: u8,
    pub vector: Vec<f32>,
    pub parent_vector_id: Option<u64>,
}

impl CentroidInfoEntry {
    pub fn new(level: u8, vector: Vec<f32>, parent_vector_id: Option<u64>) -> Self {
        Self {
            level,
            vector,
            parent_vector_id,
        }
    }
}

impl Encode for CentroidInfoEntry {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.level);
        match self.parent_vector_id {
            Some(parent_vector_id) => {
                buf.put_u8(1);
                buf.put_u64_le(parent_vector_id);
            }
            None => buf.put_u8(0),
        }
    }
}

impl Decode for CentroidInfoEntry {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for CentroidInfoEntry header: expected at least 2 bytes, got {}",
                    buf.len()
                ),
            });
        }

        let level = buf[0];
        let has_parent = buf[1];
        *buf = &buf[2..];

        let parent_vector_id = match has_parent {
            0 => None,
            1 => {
                if buf.len() < 8 {
                    return Err(EncodingError {
                        message: format!(
                            "Buffer too short for CentroidInfoEntry parent_vector_id: expected 8 bytes, got {}",
                            buf.len()
                        ),
                    });
                }
                let parent_vector_id = u64::from_le_bytes([
                    buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
                ]);
                *buf = &buf[8..];
                Some(parent_vector_id)
            }
            _ => {
                return Err(EncodingError {
                    message: format!(
                        "Invalid CentroidInfoEntry parent flag: expected 0 or 1, got {}",
                        has_parent
                    ),
                });
            }
        };

        Ok(Self {
            level,
            parent_vector_id,
        })
    }
}

/// Per-centroid metadata value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentroidInfoValue {
    pub entries: Vec<CentroidInfoEntry>,
}

impl CentroidInfoValue {
    pub fn new(entries: Vec<CentroidInfoEntry>) -> Self {
        Self { entries }
    }

    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        encode_array(&self.entries, &mut buf);
        buf.freeze()
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut slice = buf;
        let entries = decode_array(&mut slice)?;
        Ok(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_centroid_info_with_parent() {
        // given
        let value = CentroidInfoValue::new(vec![CentroidInfoEntry::new(2, Some(99))]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidInfoValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_centroid_info_without_parent() {
        // given
        let value = CentroidInfoValue::new(vec![CentroidInfoEntry::new(3, None)]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidInfoValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_multiple_centroid_info_entries() {
        // given
        let value = CentroidInfoValue::new(vec![
            CentroidInfoEntry::new(0, Some(10)),
            CentroidInfoEntry::new(1, Some(20)),
            CentroidInfoEntry::new(2, None),
        ]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CentroidInfoValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_reject_invalid_parent_flag() {
        // given
        let buf = [1u8, 2u8];

        // when
        let result = CentroidInfoValue::decode_from_bytes(&buf);

        // then
        assert!(result.is_err());
    }
}

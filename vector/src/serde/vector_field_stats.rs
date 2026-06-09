//! `VectorFieldStats` value encoding/decoding for FTS (RFC-0006).
//!
//! A per-vector record (keyed by [`VectorFieldStatsKey`](crate::serde::key::VectorFieldStatsKey))
//! recording, for each indexed `Text` field the document populated, the
//! document's length in tokens. The indexer writes this as a PUT alongside the
//! term postings/stats. The FTS compaction filter reads it when a vector is
//! deleted to compute the corpus-level [`FieldStatsValue`](crate::serde::field_stats::FieldStatsValue)
//! deltas (subtracting the document's count and length from each of its fields),
//! then drops the record.
//!
//! ## Value Layout (little-endian)
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────┐
//! │  count:  u16                                                 │
//! │  count × {                                                   │
//! │     field_len: u16                                          │
//! │     field:     field_len bytes (UTF-8)                      │
//! │     length:    u32   (document length in tokens)            │
//! │  }                                                          │
//! └──────────────────────────────────────────────────────────────┘
//! ```
//!
//! Entries are written in ascending field-name order so the encoding is
//! deterministic and round-trips stably.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::EncodingError;

/// Per-vector text-field token lengths.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct VectorFieldStatsValue {
    /// `(text field name, document length in tokens)` for every indexed `Text`
    /// field the document populated, sorted ascending by field name.
    pub(crate) lengths: Vec<(String, u32)>,
}

impl VectorFieldStatsValue {
    // `new`/`encode_to_bytes` are written by the indexer's FTS write path
    // (not yet landed); the compaction filter only reads via `decode_from_bytes`.
    #[allow(dead_code)]
    pub(crate) fn new(mut lengths: Vec<(String, u32)>) -> Self {
        lengths.sort_by(|a, b| a.0.cmp(&b.0));
        Self { lengths }
    }

    #[allow(dead_code)]
    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        let count = u16::try_from(self.lengths.len())
            .expect("VectorFieldStatsValue exceeds u16::MAX fields");
        buf.put_u16_le(count);
        for (field, length) in &self.lengths {
            let field_len =
                u16::try_from(field.len()).expect("FTS field name exceeds u16::MAX bytes");
            buf.put_u16_le(field_len);
            buf.extend_from_slice(field.as_bytes());
            buf.put_u32_le(*length);
        }
        buf.freeze()
    }

    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut cursor = buf;
        if cursor.len() < 2 {
            return Err(EncodingError {
                message: "VectorFieldStatsValue too short for count".to_string(),
            });
        }
        let count = cursor.get_u16_le() as usize;
        let mut lengths = Vec::with_capacity(count);
        for _ in 0..count {
            if cursor.len() < 2 {
                return Err(EncodingError {
                    message: "VectorFieldStatsValue truncated at field length".to_string(),
                });
            }
            let field_len = cursor.get_u16_le() as usize;
            if cursor.len() < field_len + 4 {
                return Err(EncodingError {
                    message: "VectorFieldStatsValue truncated within field entry".to_string(),
                });
            }
            let field = std::str::from_utf8(&cursor[..field_len])
                .map_err(|e| EncodingError {
                    message: format!("VectorFieldStatsValue field is not valid UTF-8: {e}"),
                })?
                .to_string();
            cursor.advance(field_len);
            let length = cursor.get_u32_le();
            lengths.push((field, length));
        }
        Ok(Self { lengths })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_vector_field_stats() {
        // given
        let value =
            VectorFieldStatsValue::new(vec![("title".to_string(), 5), ("body".to_string(), 137)]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorFieldStatsValue::decode_from_bytes(&encoded).unwrap();

        // then - decoded matches and entries are sorted by field name
        assert_eq!(decoded, value);
        assert_eq!(
            decoded.lengths,
            vec![("body".to_string(), 137), ("title".to_string(), 5)]
        );
    }

    #[test]
    fn should_round_trip_empty() {
        // given
        let value = VectorFieldStatsValue::default();

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorFieldStatsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert!(decoded.lengths.is_empty());
    }

    #[test]
    fn should_reject_truncated_buffer() {
        // given - claims 1 entry but has no field bytes
        let mut buf = BytesMut::new();
        buf.put_u16_le(1);
        buf.put_u16_le(4); // field_len = 4 but no bytes follow

        // when / then
        assert!(VectorFieldStatsValue::decode_from_bytes(&buf).is_err());
    }
}

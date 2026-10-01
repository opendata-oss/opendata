//! `FieldStats` value encoding/decoding for FTS (RFC-0006).
//!
//! Tracks per-field corpus statistics used for BM25 scoring:
//! - `count`: number of documents that have the field
//! - `total_length`: total document length (in tokens) across all documents
//! - `deletes`: number of deleted documents (reserved for delete handling in
//!   later milestones; M0 always writes 0)
//!
//! Each merge operand contributes signed deltas to all three counters so
//! writes can be applied without read-modify-write.

use bytes::{Bytes, BytesMut};

use super::EncodingError;

/// Per-field corpus stats delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct FieldStatsValue {
    pub(crate) count: i64,
    pub(crate) total_length: i64,
    pub(crate) deletes: i64,
}

impl FieldStatsValue {
    pub(crate) fn new(count: i64, total_length: i64, deletes: i64) -> Self {
        Self {
            count,
            total_length,
            deletes,
        }
    }

    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(24);
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(&self.total_length.to_le_bytes());
        buf.extend_from_slice(&self.deletes.to_le_bytes());
        buf.freeze()
    }

    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 24 {
            return Err(EncodingError {
                message: format!(
                    "FieldStatsValue too short: need 24 bytes, have {}",
                    buf.len()
                ),
            });
        }
        let count = i64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        let total_length = i64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        let deletes = i64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);
        Ok(Self {
            count,
            total_length,
            deletes,
        })
    }
}

/// Sum existing + all operands componentwise.
pub(crate) fn merge_batch_field_stats(existing: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    let mut accum = FieldStatsValue::default();
    if let Some(b) = existing
        && let Ok(v) = FieldStatsValue::decode_from_bytes(&b)
    {
        accum.count = accum.count.saturating_add(v.count);
        accum.total_length = accum.total_length.saturating_add(v.total_length);
        accum.deletes = accum.deletes.saturating_add(v.deletes);
    }
    for op in operands {
        if let Ok(v) = FieldStatsValue::decode_from_bytes(op) {
            accum.count = accum.count.saturating_add(v.count);
            accum.total_length = accum.total_length.saturating_add(v.total_length);
            accum.deletes = accum.deletes.saturating_add(v.deletes);
        }
    }
    accum.encode_to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_field_stats() {
        // given
        let value = FieldStatsValue::new(10, 100, 2);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = FieldStatsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_sum_existing_and_operands_componentwise() {
        // given
        let existing = FieldStatsValue::new(5, 50, 1).encode_to_bytes();
        let op0 = FieldStatsValue::new(3, 30, 0).encode_to_bytes();
        let op1 = FieldStatsValue::new(-1, -10, 0).encode_to_bytes();

        // when
        let merged = merge_batch_field_stats(Some(existing), &[op0, op1]);
        let decoded = FieldStatsValue::decode_from_bytes(&merged).unwrap();

        // then
        assert_eq!(decoded, FieldStatsValue::new(7, 70, 1));
    }
}

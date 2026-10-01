//! `TermStats` value encoding/decoding for FTS (RFC-0006).
//!
//! Tracks per-term document frequency as a signed delta so writes can be
//! applied via merge operators without read-modify-write.
//!
//! ## Value Layout (little-endian)
//!
//! ```text
//! ┌─────────────────────────────┐
//! │  freq: i64                  │
//! └─────────────────────────────┘
//! ```
//!
//! ## Merge Semantics
//!
//! Each merge operand contributes a signed delta to the running document
//! frequency. The merge operator sums all operand deltas plus any existing
//! value into a single i64.

use bytes::{Bytes, BytesMut};

use super::EncodingError;

/// Per-term document-frequency delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct TermStatsValue {
    /// Signed delta to the number of documents containing the term.
    pub(crate) freq: i64,
}

impl TermStatsValue {
    pub(crate) fn new(freq: i64) -> Self {
        Self { freq }
    }

    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        buf.extend_from_slice(&self.freq.to_le_bytes());
        buf.freeze()
    }

    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 8 {
            return Err(EncodingError {
                message: format!("TermStatsValue too short: need 8 bytes, have {}", buf.len()),
            });
        }
        let freq = i64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        Ok(Self { freq })
    }
}

/// Sum existing + all operands into a single freq delta.
pub(crate) fn merge_batch_term_stats(existing: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    let mut total: i64 = 0;
    if let Some(b) = existing
        && let Ok(v) = TermStatsValue::decode_from_bytes(&b)
    {
        total = total.saturating_add(v.freq);
    }
    for op in operands {
        if let Ok(v) = TermStatsValue::decode_from_bytes(op) {
            total = total.saturating_add(v.freq);
        }
    }
    TermStatsValue::new(total).encode_to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_term_stats_value() {
        // given
        let value = TermStatsValue::new(42);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = TermStatsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_sum_existing_and_operands() {
        // given
        let existing = TermStatsValue::new(10).encode_to_bytes();
        let op0 = TermStatsValue::new(5).encode_to_bytes();
        let op1 = TermStatsValue::new(-2).encode_to_bytes();

        // when
        let merged = merge_batch_term_stats(Some(existing), &[op0, op1]);
        let decoded = TermStatsValue::decode_from_bytes(&merged).unwrap();

        // then
        assert_eq!(decoded.freq, 13);
    }

    #[test]
    fn should_sum_operands_only() {
        // given
        let op0 = TermStatsValue::new(3).encode_to_bytes();
        let op1 = TermStatsValue::new(4).encode_to_bytes();

        // when
        let merged = merge_batch_term_stats(None, &[op0, op1]);
        let decoded = TermStatsValue::decode_from_bytes(&merged).unwrap();

        // then
        assert_eq!(decoded.freq, 7);
    }
}

use bytes::Bytes;
use common::storage::MergeOperator;

use crate::serde::RecordType;

/// Merge operator for graph storage records.
///
/// Most record types use last-write-wins semantics. The only exception
/// is metadata counters (record type 0xE0), which use additive merge
/// to support atomic counter increments.
pub(crate) struct GraphMergeOperator;

impl MergeOperator for GraphMergeOperator {
    fn merge(&self, key: &Bytes, existing_value: Option<Bytes>, new_value: Bytes) -> Bytes {
        // Need at least 2 bytes for key prefix
        if key.len() < 2 {
            return new_value;
        }

        let record_type = (key[1] & 0xF0) >> 4;

        match record_type {
            rt if rt == RecordType::Metadata as u8 => {
                merge_i64_counter(existing_value, new_value)
            }
            _ => new_value, // last-write-wins
        }
    }
}

/// Merges two i64 counters by addition.
///
/// Both existing and new values are interpreted as little-endian i64.
/// Returns the sum as little-endian bytes.
fn merge_i64_counter(existing: Option<Bytes>, new: Bytes) -> Bytes {
    let existing_val = existing
        .as_ref()
        .and_then(|b| {
            if b.len() >= 8 {
                Some(i64::from_le_bytes(b[..8].try_into().unwrap()))
            } else {
                None
            }
        })
        .unwrap_or(0);

    let new_val = if new.len() >= 8 {
        i64::from_le_bytes(new[..8].try_into().unwrap())
    } else {
        0
    };

    Bytes::copy_from_slice(&(existing_val + new_val).to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::keys::MetadataKey;
    use crate::serde::MetadataSubType;

    #[test]
    fn should_merge_counters_additively() {
        // given
        let op = GraphMergeOperator;
        let key = MetadataKey {
            sub_type: MetadataSubType::NodeCount,
        }
        .encode();
        let existing = Bytes::copy_from_slice(&10i64.to_le_bytes());
        let delta = Bytes::copy_from_slice(&5i64.to_le_bytes());

        // when
        let result = op.merge(&key, Some(existing), delta);

        // then
        let merged = i64::from_le_bytes(result[..8].try_into().unwrap());
        assert_eq!(merged, 15);
    }

    #[test]
    fn should_merge_counter_with_no_existing() {
        // given
        let op = GraphMergeOperator;
        let key = MetadataKey {
            sub_type: MetadataSubType::EdgeCount,
        }
        .encode();
        let delta = Bytes::copy_from_slice(&7i64.to_le_bytes());

        // when
        let result = op.merge(&key, None, delta);

        // then
        let merged = i64::from_le_bytes(result[..8].try_into().unwrap());
        assert_eq!(merged, 7);
    }

    #[test]
    fn should_merge_negative_counter_delta() {
        // given
        let op = GraphMergeOperator;
        let key = MetadataKey {
            sub_type: MetadataSubType::NodeCount,
        }
        .encode();
        let existing = Bytes::copy_from_slice(&10i64.to_le_bytes());
        let delta = Bytes::copy_from_slice(&(-3i64).to_le_bytes());

        // when
        let result = op.merge(&key, Some(existing), delta);

        // then
        let merged = i64::from_le_bytes(result[..8].try_into().unwrap());
        assert_eq!(merged, 7);
    }

    #[test]
    fn should_last_write_wins_for_non_metadata() {
        // given
        let op = GraphMergeOperator;
        // Use a node record key (record type 1, not metadata)
        let key = Bytes::from_static(&[0x01, 0x10, 0x00]);
        let existing = Bytes::from("old");
        let new = Bytes::from("new");

        // when
        let result = op.merge(&key, Some(existing), new.clone());

        // then
        assert_eq!(result, new);
    }
}

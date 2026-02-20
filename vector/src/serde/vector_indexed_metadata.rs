//! VectorIndexedMetadata value encoding/decoding.
//!
//! Tracks which centroids a vector is assigned to, enabling proper cleanup
//! during reassignment when boundary replication is active.
//!
//! ## Value Encoding (Put)
//!
//! `count:u32-LE` followed by `centroid_id:u64-LE` for each entry.
//!
//! ## Merge Encoding (Delta)
//!
//! `num_removals:u32-LE, [removal_id:u64-LE], num_additions:u32-LE, [addition_id:u64-LE]`
//!
//! A merge describes a delta to the centroid assignment: which centroids to remove
//! and which to add. Used during split/merge operations to update metadata without
//! reading the existing value.

use bytes::{Bytes, BytesMut};

use super::EncodingError;

/// Tracks the set of centroids a vector has been assigned to.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorIndexedMetadataValue {
    pub(crate) centroid_ids: Vec<u64>,
}

impl VectorIndexedMetadataValue {
    pub(crate) fn new(centroid_ids: Vec<u64>) -> Self {
        Self { centroid_ids }
    }

    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(4 + self.centroid_ids.len() * 8);
        buf.extend_from_slice(&(self.centroid_ids.len() as u32).to_le_bytes());
        for &id in &self.centroid_ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        buf.freeze()
    }

    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 4 {
            return Err(EncodingError {
                message: "Buffer too short for VectorIndexedMetadataValue count".to_string(),
            });
        }
        let count = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let expected_len = 4 + count * 8;
        if buf.len() < expected_len {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for VectorIndexedMetadataValue: expected {} bytes, got {}",
                    expected_len,
                    buf.len()
                ),
            });
        }
        let mut centroid_ids = Vec::with_capacity(count);
        let mut offset = 4;
        for _ in 0..count {
            let id = u64::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7],
            ]);
            centroid_ids.push(id);
            offset += 8;
        }
        Ok(Self { centroid_ids })
    }

    /// Apply a merge delta: remove specified centroids, then add new ones.
    /// Idempotent: removing a non-existent ID is a no-op, adding a duplicate is a no-op.
    pub(crate) fn apply_merge(&mut self, delta: &VectorIndexedMetadataMergeValue) {
        self.centroid_ids.retain(|id| !delta.removals.contains(id));
        for &id in &delta.additions {
            if !self.centroid_ids.contains(&id) {
                self.centroid_ids.push(id);
            }
        }
    }
}

/// A merge delta for VectorIndexedMetadata: describes centroid removals and additions.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorIndexedMetadataMergeValue {
    pub(crate) removals: Vec<u64>,
    pub(crate) additions: Vec<u64>,
}

impl VectorIndexedMetadataMergeValue {
    pub(crate) fn new(removals: Vec<u64>, additions: Vec<u64>) -> Self {
        Self {
            removals,
            additions,
        }
    }

    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        let size = 4 + self.removals.len() * 8 + 4 + self.additions.len() * 8;
        let mut buf = BytesMut::with_capacity(size);
        buf.extend_from_slice(&(self.removals.len() as u32).to_le_bytes());
        for &id in &self.removals {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        buf.extend_from_slice(&(self.additions.len() as u32).to_le_bytes());
        for &id in &self.additions {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        buf.freeze()
    }

    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 4 {
            return Err(EncodingError {
                message: "Buffer too short for VectorIndexedMetadataMergeValue".to_string(),
            });
        }
        let mut offset = 0;

        let num_removals = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]) as usize;
        offset += 4;

        let removals_end = offset + num_removals * 8;
        if buf.len() < removals_end + 4 {
            return Err(EncodingError {
                message: "Buffer too short for VectorIndexedMetadataMergeValue removals"
                    .to_string(),
            });
        }

        let mut removals = Vec::with_capacity(num_removals);
        for _ in 0..num_removals {
            let id = u64::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7],
            ]);
            removals.push(id);
            offset += 8;
        }

        let num_additions = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]) as usize;
        offset += 4;

        let additions_end = offset + num_additions * 8;
        if buf.len() < additions_end {
            return Err(EncodingError {
                message: "Buffer too short for VectorIndexedMetadataMergeValue additions"
                    .to_string(),
            });
        }

        let mut additions = Vec::with_capacity(num_additions);
        for _ in 0..num_additions {
            let id = u64::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
                buf[offset + 4],
                buf[offset + 5],
                buf[offset + 6],
                buf[offset + 7],
            ]);
            additions.push(id);
            offset += 8;
        }

        Ok(Self {
            removals,
            additions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_single_centroid() {
        // given
        let value = VectorIndexedMetadataValue::new(vec![42]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_multiple_centroids() {
        // given
        let value = VectorIndexedMetadataValue::new(vec![1, 5, 42, 100]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_empty() {
        // given
        let value = VectorIndexedMetadataValue::new(vec![]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_reject_truncated_buffer() {
        // given
        let buf = [0u8; 3]; // too short for count

        // when
        let result = VectorIndexedMetadataValue::decode_from_bytes(&buf);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_and_decode_merge_value() {
        // given
        let merge = VectorIndexedMetadataMergeValue::new(vec![1, 2], vec![3, 4]);

        // when
        let encoded = merge.encode_to_bytes();
        let decoded = VectorIndexedMetadataMergeValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, merge);
    }

    #[test]
    fn should_encode_and_decode_empty_merge_value() {
        // given
        let merge = VectorIndexedMetadataMergeValue::new(vec![], vec![]);

        // when
        let encoded = merge.encode_to_bytes();
        let decoded = VectorIndexedMetadataMergeValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, merge);
    }

    #[test]
    fn should_apply_merge_removing_and_adding() {
        // given
        let mut value = VectorIndexedMetadataValue::new(vec![1, 2, 3]);
        let delta = VectorIndexedMetadataMergeValue::new(vec![2], vec![4]);

        // when
        value.apply_merge(&delta);

        // then
        assert_eq!(value.centroid_ids, vec![1, 3, 4]);
    }

    #[test]
    fn should_apply_merge_idempotently() {
        // given
        let mut value = VectorIndexedMetadataValue::new(vec![1, 3]);
        let delta = VectorIndexedMetadataMergeValue::new(vec![2], vec![3]); // remove 2 (not present), add 3 (already present)

        // when
        value.apply_merge(&delta);

        // then
        assert_eq!(value.centroid_ids, vec![1, 3]);
    }

    #[test]
    fn should_apply_merge_to_empty() {
        // given
        let mut value = VectorIndexedMetadataValue::new(vec![]);
        let delta = VectorIndexedMetadataMergeValue::new(vec![1], vec![2, 3]);

        // when
        value.apply_merge(&delta);

        // then
        assert_eq!(value.centroid_ids, vec![2, 3]);
    }

    #[test]
    fn should_apply_split_merge_replacing_centroid() {
        // given - vector assigned to centroids [5, 10], centroid 5 is split into 50, 51
        let mut value = VectorIndexedMetadataValue::new(vec![5, 10]);
        let delta = VectorIndexedMetadataMergeValue::new(vec![5], vec![50]);

        // when
        value.apply_merge(&delta);

        // then
        assert_eq!(value.centroid_ids, vec![10, 50]);
    }
}

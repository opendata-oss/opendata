//! `Deletions` value encoding/decoding for FTS (RFC-0006).
//!
//! A singleton record whose value is a [`VectorBitmap`] (RoaringTreemap) of the
//! internal vector ids that have been deleted or replaced. Deletes are applied
//! via merge operands that UNION their bitmaps into the running set, so writes
//! never require a read-modify-write.
//!
//! ## Value Layout
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │  vector_ids: RoaringTreemap serialization    │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! ## Merge Semantics
//!
//! Each merge operand contributes a bitmap of deleted ids. The merge operator
//! unions all operand bitmaps plus any existing value into a single bitmap.

use bytes::Bytes;

use super::EncodingError;
use super::vector_bitmap::VectorBitmap;

/// Singleton set of deleted/replaced internal vector ids.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct DeletionsValue(pub(crate) VectorBitmap);

impl DeletionsValue {
    pub(crate) fn new(bitmap: VectorBitmap) -> Self {
        Self(bitmap)
    }

    pub(crate) fn encode_to_bytes(&self) -> Result<Bytes, EncodingError> {
        self.0.encode_to_bytes()
    }

    pub(crate) fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        Ok(Self(VectorBitmap::decode_from_bytes(buf)?))
    }
}

/// Union existing + all operands into a single deletions bitmap.
///
/// Starts from the existing value (or empty) and unions every operand. A decode
/// failure panics rather than being skipped: dropping a deletions operand would
/// resurrect an already-deleted vector on the query path, so we fail loudly and
/// preserve the data instead.
pub(crate) fn merge_batch_deletions(existing: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    let mut merged = VectorBitmap::new();
    if let Some(b) = existing {
        let v = DeletionsValue::decode_from_bytes(&b)
            .expect("Failed to decode existing DeletionsValue");
        merged.union_with(&v.0);
    }
    for op in operands {
        let v =
            DeletionsValue::decode_from_bytes(op).expect("Failed to decode operand DeletionsValue");
        merged.union_with(&v.0);
    }
    DeletionsValue::new(merged)
        .encode_to_bytes()
        .expect("Failed to serialize merged DeletionsValue")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_round_trip_deletions_value() {
        // given
        let mut bitmap = VectorBitmap::new();
        bitmap.insert(1);
        bitmap.insert(100);
        bitmap.insert(u64::MAX);
        let value = DeletionsValue::new(bitmap);

        // when
        let encoded = value.encode_to_bytes().unwrap();
        let decoded = DeletionsValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert!(decoded.0.contains(1));
        assert!(decoded.0.contains(100));
        assert!(decoded.0.contains(u64::MAX));
    }

    #[test]
    fn should_union_existing_and_operands() {
        // given - existing {1, 2}, operands {2, 3} (overlap) and {10, 11} (disjoint)
        let existing = DeletionsValue::new(VectorBitmap::from_treemap({
            let mut t = roaring::RoaringTreemap::new();
            t.insert(1);
            t.insert(2);
            t
        }))
        .encode_to_bytes()
        .unwrap();
        let op0 = DeletionsValue::new(VectorBitmap::from_treemap({
            let mut t = roaring::RoaringTreemap::new();
            t.insert(2);
            t.insert(3);
            t
        }))
        .encode_to_bytes()
        .unwrap();
        let op1 = DeletionsValue::new(VectorBitmap::from_treemap({
            let mut t = roaring::RoaringTreemap::new();
            t.insert(10);
            t.insert(11);
            t
        }))
        .encode_to_bytes()
        .unwrap();

        // when
        let merged = merge_batch_deletions(Some(existing), &[op0, op1]);
        let decoded = DeletionsValue::decode_from_bytes(&merged).unwrap();

        // then
        assert_eq!(decoded.0.len(), 5);
        for id in [1, 2, 3, 10, 11] {
            assert!(decoded.0.contains(id), "missing {}", id);
        }
    }

    #[test]
    fn should_union_operands_only() {
        // given - no existing value; operands overlap on 3
        let op0 = DeletionsValue::new(VectorBitmap::from_treemap({
            let mut t = roaring::RoaringTreemap::new();
            t.insert(3);
            t.insert(4);
            t
        }))
        .encode_to_bytes()
        .unwrap();
        let op1 = DeletionsValue::new(VectorBitmap::from_treemap({
            let mut t = roaring::RoaringTreemap::new();
            t.insert(3);
            t.insert(7);
            t
        }))
        .encode_to_bytes()
        .unwrap();

        // when
        let merged = merge_batch_deletions(None, &[op0, op1]);
        let decoded = DeletionsValue::decode_from_bytes(&merged).unwrap();

        // then
        assert_eq!(decoded.0.len(), 3);
        for id in [3, 4, 7] {
            assert!(decoded.0.contains(id), "missing {}", id);
        }
    }
}

//! VectorBitmap - shared bitmap type for storing sets of vector IDs.
//!
//! This type is used by both `DeletionsValue` and `MetadataIndexValue` to
//! store compressed sets of vector IDs using RoaringTreemap.

use super::EncodingError;
use bytes::Bytes;
use roaring::RoaringTreemap;
use std::io::Cursor;

/// A compressed bitmap storing a set of vector IDs.
///
/// Uses `RoaringTreemap` for efficient compression and fast set operations.
///
/// ## Value Layout
///
/// ```text
/// +----------------------------------------------------------------+
/// |  vector_ids:  RoaringTreemap serialization                     |
/// |               (compressed u64 bitmap, variable length)         |
/// +----------------------------------------------------------------+
/// ```
#[derive(Debug, Clone)]
pub struct VectorBitmap {
    /// Compressed set of vector IDs.
    pub vector_ids: RoaringTreemap,
}

impl VectorBitmap {
    pub fn new() -> Self {
        Self {
            vector_ids: RoaringTreemap::new(),
        }
    }

    pub fn from_treemap(vector_ids: RoaringTreemap) -> Self {
        Self { vector_ids }
    }

    /// Create a VectorBitmap containing a single vector ID.
    pub fn singleton(vector_id: u64) -> Self {
        let mut treemap = RoaringTreemap::new();
        treemap.insert(vector_id);
        Self {
            vector_ids: treemap,
        }
    }

    /// Insert a vector ID into the bitmap.
    pub fn insert(&mut self, vector_id: u64) -> bool {
        self.vector_ids.insert(vector_id)
    }

    /// Remove a vector ID from the bitmap.
    pub fn remove(&mut self, vector_id: u64) -> bool {
        self.vector_ids.remove(vector_id)
    }

    /// Check if a vector ID is in the bitmap.
    pub fn contains(&self, vector_id: u64) -> bool {
        self.vector_ids.contains(vector_id)
    }

    /// Returns the number of vector IDs in the bitmap.
    pub fn len(&self) -> u64 {
        self.vector_ids.len()
    }

    /// Returns true if the bitmap is empty.
    pub fn is_empty(&self) -> bool {
        self.vector_ids.is_empty()
    }

    /// Returns an iterator over the vector IDs.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.vector_ids.iter()
    }

    /// Union (OR) this bitmap with another.
    pub fn union_with(&mut self, other: &Self) {
        self.vector_ids |= &other.vector_ids;
    }

    /// Difference (AND-NOT) this bitmap with another.
    pub fn difference_with(&mut self, other: &Self) {
        self.vector_ids -= &other.vector_ids;
    }

    /// Intersection (AND) this bitmap with another.
    pub fn intersect_with(&mut self, other: &Self) {
        self.vector_ids &= &other.vector_ids;
    }

    pub fn encode_to_bytes(&self) -> Result<Bytes, EncodingError> {
        let mut buf = Vec::new();
        self.vector_ids
            .serialize_into(&mut buf)
            .map_err(|e| EncodingError {
                message: format!("Failed to serialize RoaringTreemap: {}", e),
            })?;
        Ok(Bytes::from(buf))
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        let cursor = Cursor::new(buf);
        let vector_ids = RoaringTreemap::deserialize_from(cursor).map_err(|e| EncodingError {
            message: format!("Failed to deserialize RoaringTreemap: {}", e),
        })?;
        Ok(VectorBitmap { vector_ids })
    }
}

impl Default for VectorBitmap {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for VectorBitmap {
    fn eq(&self, other: &Self) -> bool {
        self.vector_ids == other.vector_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_empty_bitmap() {
        // given
        let bitmap = VectorBitmap::new();

        // when
        let encoded = bitmap.encode_to_bytes().unwrap();
        let decoded = VectorBitmap::decode_from_bytes(&encoded).unwrap();

        // then
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_encode_and_decode_bitmap_with_ids() {
        // given
        let mut bitmap = VectorBitmap::new();
        bitmap.insert(1);
        bitmap.insert(100);
        bitmap.insert(10000);
        bitmap.insert(u64::MAX);

        // when
        let encoded = bitmap.encode_to_bytes().unwrap();
        let decoded = VectorBitmap::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.len(), 4);
        assert!(decoded.contains(1));
        assert!(decoded.contains(100));
        assert!(decoded.contains(10000));
        assert!(decoded.contains(u64::MAX));
    }

    #[test]
    fn should_create_singleton() {
        // given / when
        let bitmap = VectorBitmap::singleton(42);

        // then
        assert_eq!(bitmap.len(), 1);
        assert!(bitmap.contains(42));
    }

    #[test]
    fn should_perform_union() {
        // given
        let mut b1 = VectorBitmap::new();
        b1.insert(1);
        b1.insert(2);

        let mut b2 = VectorBitmap::new();
        b2.insert(2);
        b2.insert(3);

        // when
        b1.union_with(&b2);

        // then
        assert_eq!(b1.len(), 3);
        assert!(b1.contains(1));
        assert!(b1.contains(2));
        assert!(b1.contains(3));
    }

    #[test]
    fn should_perform_difference() {
        // given
        let mut b1 = VectorBitmap::new();
        b1.insert(1);
        b1.insert(2);
        b1.insert(3);

        let mut b2 = VectorBitmap::new();
        b2.insert(2);

        // when
        b1.difference_with(&b2);

        // then
        assert_eq!(b1.len(), 2);
        assert!(b1.contains(1));
        assert!(!b1.contains(2));
        assert!(b1.contains(3));
    }

    #[test]
    fn should_perform_intersection() {
        // given
        let mut b1 = VectorBitmap::new();
        b1.insert(1);
        b1.insert(2);
        b1.insert(3);

        let mut b2 = VectorBitmap::new();
        b2.insert(2);
        b2.insert(3);
        b2.insert(4);

        // when
        b1.intersect_with(&b2);

        // then
        assert_eq!(b1.len(), 2);
        assert!(!b1.contains(1));
        assert!(b1.contains(2));
        assert!(b1.contains(3));
        assert!(!b1.contains(4));
    }

    #[test]
    fn should_iterate_over_ids() {
        // given
        let mut bitmap = VectorBitmap::new();
        bitmap.insert(3);
        bitmap.insert(1);
        bitmap.insert(2);

        // when
        let ids: Vec<u64> = bitmap.iter().collect();

        // then
        assert_eq!(ids, vec![1, 2, 3]); // Sorted order
    }
}

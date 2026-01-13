//! PostingList value encoding/decoding.
//!
//! Maps centroid IDs to the set of vector IDs assigned to that cluster.
//!
//! ## Role in SPANN Index
//!
//! In a SPANN-style vector index, vectors are clustered around centroids. Each
//! centroid has an associated **posting list** containing the IDs of all vectors
//! assigned to that cluster.
//!
//! During search:
//! 1. Find the k nearest centroids to the query vector
//! 2. Load the posting lists for those centroids
//! 3. Compute exact distances for vectors in those posting lists
//! 4. Return top results
//!
//! ## RoaringTreemap Compression
//!
//! Vector IDs are stored using `RoaringTreemap`, a 64-bit extension of Roaring
//! bitmaps. This provides:
//!
//! - **Compression**: Sequential IDs (from block allocation) compress extremely well
//! - **Fast set operations**: Union, intersection, difference in O(n) time
//! - **Sorted iteration**: IDs are always returned in sorted order
//!
//! ## Special Centroid IDs
//!
//! | Centroid ID    | Purpose                                           |
//! |----------------|---------------------------------------------------|
//! | `0x00000000`   | **Deleted vectors bitmap** - vectors pending cleanup |
//! | `0x00000001+`  | Normal cluster centroids                          |
//!
//! The deleted bitmap (centroid_id = 0) is loaded once during search and subtracted
//! from candidate sets, avoiding per-vector tombstone records.
//!
//! ## Boundary Vectors
//!
//! Vectors near cluster boundaries may appear in multiple posting lists to improve
//! recall. The target duplication factor is 1.5-2x (each vector appears in 1.5-2
//! posting lists on average). LIRE maintenance manages this duplication.
//!
//! ## Merge Operators
//!
//! Posting lists use SlateDB merge operators to avoid read-modify-write:
//! - **Add vector**: Merge with bitmap containing new ID (OR)
//! - **Remove vector**: Merge with deletion marker (AND-NOT)
//!
//! See also: `MetadataIndexValue` (type alias to this type for metadata filtering)

use super::EncodingError;
use bytes::Bytes;
use roaring::RoaringTreemap;
use std::io::Cursor;

/// PostingList value storing vector IDs for a centroid cluster.
///
/// Each posting list maps a single centroid ID to the set of vectors assigned
/// to that cluster. The key is `PostingListKey { centroid_id }`, and this struct
/// is the value.
///
/// ## Value Layout
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │  vector_ids:  RoaringTreemap serialization                     │
/// │               (compressed u64 bitmap, variable length)         │
/// └────────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Typical Sizes
///
/// - Target posting list size: 1,000-5,000 vectors
/// - Sequential IDs compress to ~1-2 bytes per ID on average
/// - A 5,000-vector posting list is typically 5-15 KB
///
/// ## Set Operations
///
/// The `union_with`, `intersect_with`, and `difference_with` methods enable
/// efficient query evaluation:
///
/// - **Union**: Combine candidates from multiple centroids
/// - **Intersect**: Apply metadata filters
/// - **Difference**: Remove deleted vectors
#[derive(Debug, Clone)]
pub struct PostingListValue {
    /// Compressed set of vector IDs (internal u64 IDs, not external string IDs).
    pub vector_ids: RoaringTreemap,
}

impl PostingListValue {
    pub fn new() -> Self {
        Self {
            vector_ids: RoaringTreemap::new(),
        }
    }

    pub fn from_treemap(vector_ids: RoaringTreemap) -> Self {
        Self { vector_ids }
    }

    /// Create a PostingListValue containing a single vector ID.
    pub fn singleton(vector_id: u64) -> Self {
        let mut treemap = RoaringTreemap::new();
        treemap.insert(vector_id);
        Self {
            vector_ids: treemap,
        }
    }

    /// Insert a vector ID into the posting list.
    pub fn insert(&mut self, vector_id: u64) -> bool {
        self.vector_ids.insert(vector_id)
    }

    /// Remove a vector ID from the posting list.
    pub fn remove(&mut self, vector_id: u64) -> bool {
        self.vector_ids.remove(vector_id)
    }

    /// Check if a vector ID is in the posting list.
    pub fn contains(&self, vector_id: u64) -> bool {
        self.vector_ids.contains(vector_id)
    }

    /// Returns the number of vector IDs in the posting list.
    pub fn len(&self) -> u64 {
        self.vector_ids.len()
    }

    /// Returns true if the posting list is empty.
    pub fn is_empty(&self) -> bool {
        self.vector_ids.is_empty()
    }

    /// Returns an iterator over the vector IDs.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.vector_ids.iter()
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
        Ok(PostingListValue { vector_ids })
    }

    /// Union (OR) this posting list with another.
    pub fn union_with(&mut self, other: &Self) {
        self.vector_ids |= &other.vector_ids;
    }

    /// Difference (AND-NOT) this posting list with another.
    pub fn difference_with(&mut self, other: &Self) {
        self.vector_ids -= &other.vector_ids;
    }

    /// Intersection (AND) this posting list with another.
    pub fn intersect_with(&mut self, other: &Self) {
        self.vector_ids &= &other.vector_ids;
    }
}

impl Default for PostingListValue {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for PostingListValue {
    fn eq(&self, other: &Self) -> bool {
        self.vector_ids == other.vector_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_empty_posting_list() {
        // given
        let value = PostingListValue::new();

        // when
        let encoded = value.encode_to_bytes().unwrap();
        let decoded = PostingListValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_encode_and_decode_posting_list_with_ids() {
        // given
        let mut value = PostingListValue::new();
        value.insert(1);
        value.insert(100);
        value.insert(10000);
        value.insert(u64::MAX);

        // when
        let encoded = value.encode_to_bytes().unwrap();
        let decoded = PostingListValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.len(), 4);
        assert!(decoded.contains(1));
        assert!(decoded.contains(100));
        assert!(decoded.contains(10000));
        assert!(decoded.contains(u64::MAX));
    }

    #[test]
    fn should_create_singleton_posting_list() {
        // given
        let value = PostingListValue::singleton(42);

        // when / then
        assert_eq!(value.len(), 1);
        assert!(value.contains(42));
    }

    #[test]
    fn should_perform_union() {
        // given
        let mut pl1 = PostingListValue::new();
        pl1.insert(1);
        pl1.insert(2);

        let mut pl2 = PostingListValue::new();
        pl2.insert(2);
        pl2.insert(3);

        // when
        pl1.union_with(&pl2);

        // then
        assert_eq!(pl1.len(), 3);
        assert!(pl1.contains(1));
        assert!(pl1.contains(2));
        assert!(pl1.contains(3));
    }

    #[test]
    fn should_perform_difference() {
        // given
        let mut pl1 = PostingListValue::new();
        pl1.insert(1);
        pl1.insert(2);
        pl1.insert(3);

        let mut pl2 = PostingListValue::new();
        pl2.insert(2);

        // when
        pl1.difference_with(&pl2);

        // then
        assert_eq!(pl1.len(), 2);
        assert!(pl1.contains(1));
        assert!(!pl1.contains(2));
        assert!(pl1.contains(3));
    }

    #[test]
    fn should_perform_intersection() {
        // given
        let mut pl1 = PostingListValue::new();
        pl1.insert(1);
        pl1.insert(2);
        pl1.insert(3);

        let mut pl2 = PostingListValue::new();
        pl2.insert(2);
        pl2.insert(3);
        pl2.insert(4);

        // when
        pl1.intersect_with(&pl2);

        // then
        assert_eq!(pl1.len(), 2);
        assert!(!pl1.contains(1));
        assert!(pl1.contains(2));
        assert!(pl1.contains(3));
        assert!(!pl1.contains(4));
    }

    #[test]
    fn should_handle_large_posting_list() {
        // given
        let mut value = PostingListValue::new();
        for i in 0..10000 {
            value.insert(i);
        }

        // when
        let encoded = value.encode_to_bytes().unwrap();
        let decoded = PostingListValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.len(), 10000);
        for i in 0..10000 {
            assert!(decoded.contains(i));
        }
    }

    #[test]
    fn should_iterate_over_vector_ids() {
        // given
        let mut value = PostingListValue::new();
        value.insert(3);
        value.insert(1);
        value.insert(2);

        // when
        let ids: Vec<u64> = value.iter().collect();

        // then
        assert_eq!(ids, vec![1, 2, 3]); // Sorted order
    }
}

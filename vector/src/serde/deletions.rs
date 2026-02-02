//! Deletions value encoding/decoding.
//!
//! Stores a bitmap of deleted vector IDs. This bitmap is consulted when
//! evaluating postings to filter out deleted vectors.

/// Deletions value storing the set of deleted vector IDs.
///
/// This is a type alias to `VectorBitmap` since both store vector ID sets
/// with the same RoaringTreemap format and operations. The alias provides
/// semantic clarity when working with deleted vectors.
///
/// See `VectorBitmap` for available methods.
pub type DeletionsValue = super::vector_bitmap::VectorBitmap;

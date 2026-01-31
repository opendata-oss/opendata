//! MetadataIndex value encoding/decoding.
//!
//! Inverted index mapping metadata field/value pairs to vector IDs.
//!
//! ## Role in Filtered Search
//!
//! MetadataIndex enables efficient filtering during vector search. Each index
//! entry maps a metadata field/value pair to the set of vectors that have that
//! value.
//!
//! For example, to find vectors where `category="shoes"`:
//! 1. Look up the MetadataIndex entry for `(category, "shoes")`
//! 2. Get the set of vector IDs with that metadata value
//! 3. Intersect with posting list candidates during search

/// MetadataIndex value storing vector IDs for a metadata field/value pair.
///
/// This is a type alias to `VectorBitmap` since both store vector ID sets
/// with the same RoaringTreemap format and operations. The alias provides
/// semantic clarity when working with metadata indexes vs other bitmap uses.
///
/// See `VectorBitmap` for available methods including:
/// - `union_with`: Combine results for OR queries
/// - `intersect_with`: Apply AND filters
/// - `difference_with`: Exclude vectors with certain metadata
pub type MetadataIndexValue = super::vector_bitmap::VectorBitmap;

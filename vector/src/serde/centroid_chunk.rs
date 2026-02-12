//! CentroidChunk value encoding/decoding.
//!
//! Stores a chunk of cluster centroids for SPANN-style approximate nearest neighbor search.
//!
//! ## Background: SPANN Index Architecture
//!
//! SPANN (Scalable Partition-based Approximate Nearest Neighbor) is a disk-based indexing
//! strategy that enables billion-scale vector search with bounded memory. The key insight
//! is to keep only cluster centroids in memory while storing the actual vectors on disk
//! in posting lists.
//!
//! The index works by clustering vectors into groups, where each group has a representative
//! **centroid** (typically the cluster's mean vector). During search:
//!
//! 1. Find the k nearest centroids to the query vector using an in-memory index (HNSW)
//! 2. Load the posting lists for those centroids from disk
//! 3. Compute exact distances for vectors in those posting lists
//! 4. Return the top results
//!
//! This approach trades some recall for massive memory savings—instead of keeping all
//! vectors in memory, only 0.1-1% (the centroids) need to be loaded.
//!
//! ## Centroid Chunking
//!
//! Centroids are split across multiple `CentroidChunk` records rather than stored
//! individually. This chunking strategy:
//!
//! - **Reduces key count**: Storing one centroid per key would explode the number of
//!   keys fetched on startup, increasing latency.
//! - **Enables block-level caching**: SlateDB can cache entire chunks efficiently.
//! - **Supports partial reads**: Only touched chunks need to be read from disk.
//!
//! All centroid chunks are fully scanned on startup (`scan(version|0x02|*)`) to load
//! the in-memory HNSW graph. The `chunk_id` exists solely to give each chunk a distinct
//! record key.
//!
//! ## Centroid IDs
//!
//! Each centroid carries an explicit `centroid_id` (u64) stored alongside its vector.
//! These IDs:
//!
//! - Start at 1 (0 is reserved for the deleted vectors bitmap in PostingList)
//! - Are never reassigned once issued
//! - Remain stable when chunks are rewritten or reordered
//! - Are referenced by PostingList keys to map centroids to their assigned vectors
//!
//! ## Sizing Guidelines
//!
//! From RFC 0001:
//! - `CHUNK_TARGET` centroids per chunk (default 4096, ~25 MB per chunk at 1536 dims)
//! - Centroid ratio typically 0.1-1% of total vectors
//! - Example: 10M vectors → 10K-100K centroids → 3-25 chunks
//! - Higher ratios improve recall at the cost of memory

use super::{Encode, EncodingError, decode_fixed_element_array, encode_fixed_element_array};
use bytes::{Bytes, BytesMut};

/// A single centroid entry with its ID and vector.
///
/// Each centroid represents a cluster in the SPANN index. The centroid vector
/// is typically the mean of all vectors assigned to that cluster. During search,
/// the query vector is compared against centroids to find relevant clusters,
/// then the posting lists for those clusters are loaded to find candidate vectors.
///
/// See `PostingList` for the mapping from `centroid_id` to vector IDs.
#[derive(Debug, Clone, PartialEq)]
pub struct CentroidEntry {
    /// Stable identifier for this centroid (starts at 1, never reused).
    ///
    /// This ID is used as the key suffix in `PostingListKey` to look up the
    /// vector IDs assigned to this cluster. ID 0 is reserved for the deleted
    /// vectors bitmap.
    pub centroid_id: u64,
    /// Centroid vector with `dimensions` elements (from `CollectionMeta`).
    ///
    /// This is the representative vector for the cluster, typically computed
    /// as the mean of assigned vectors. The dimensionality must match all
    /// other vectors in the collection.
    pub vector: Vec<f32>,
}

impl CentroidEntry {
    pub fn new(centroid_id: u64, vector: Vec<f32>) -> Self {
        Self {
            centroid_id,
            vector,
        }
    }

    /// Returns the dimensionality of the centroid vector.
    pub fn dimensions(&self) -> usize {
        self.vector.len()
    }
}

impl Encode for CentroidEntry {
    fn encode(&self, buf: &mut BytesMut) {
        buf.extend_from_slice(&self.centroid_id.to_le_bytes());
        encode_fixed_element_array(&self.vector, buf);
    }
}

/// Decode a centroid entry with known dimensionality.
///
/// Note: This is a standalone function rather than a `Decode` trait impl because
/// decoding requires the `dimensions` parameter from `CollectionMeta`. The `Decode`
/// trait signature doesn't allow passing external context.
pub fn decode_centroid_entry(
    buf: &mut &[u8],
    dimensions: usize,
) -> Result<CentroidEntry, EncodingError> {
    if buf.len() < 8 {
        return Err(EncodingError {
            message: "Buffer too short for centroid_id".to_string(),
        });
    }

    let centroid_id = u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    *buf = &buf[8..];

    let vector_size = dimensions * 4;
    if buf.len() < vector_size {
        return Err(EncodingError {
            message: format!(
                "Buffer too short for centroid vector: need {} bytes, have {}",
                vector_size,
                buf.len()
            ),
        });
    }

    let vector_buf = &buf[..vector_size];
    *buf = &buf[vector_size..];

    let mut vector_slice = vector_buf;
    let vector: Vec<f32> = decode_fixed_element_array(&mut vector_slice, 4)?;

    Ok(CentroidEntry {
        centroid_id,
        vector,
    })
}

/// CentroidChunk value storing a chunk of cluster centroids.
///
/// Multiple chunks together form the complete centroid index for a collection.
/// On startup, all chunks are scanned and loaded into an in-memory HNSW graph
/// for fast nearest-centroid search during queries.
///
/// ## Value Layout (little-endian)
///
/// ```text
/// ┌───────────────────────────────────────────────────────────────┐
/// │  count:   u16 (number of entries, <= CHUNK_TARGET)            │
/// │  entries: Array<CentroidEntry>                                │
/// │                                                               │
/// │  CentroidEntry                                                │
/// │  ┌──────────────────────────────────────────────────────────┐ │
/// │  │  centroid_id: u64 (stable identifier)                    │ │
/// │  │  vector:      FixedElementArray<f32>                     │ │
/// │  │               (dimensions elements from CollectionMeta)  │ │
/// │  └──────────────────────────────────────────────────────────┘ │
/// └───────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Sizing
///
/// - `CHUNK_TARGET` (default 4096) centroids per chunk
/// - At 1536 dimensions: ~25 MB per chunk (4096 × 1536 × 4 bytes + overhead)
/// - Dimensionality is obtained from `CollectionMeta` at decode time
#[derive(Debug, Clone, PartialEq)]
pub struct CentroidChunkValue {
    /// Centroid entries in this chunk (up to `CHUNK_TARGET` entries).
    pub entries: Vec<CentroidEntry>,
}

impl CentroidChunkValue {
    pub fn new(entries: Vec<CentroidEntry>) -> Self {
        Self { entries }
    }

    /// Returns the number of centroids in this chunk.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if this chunk is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Encode the chunk with known dimensionality.
    pub fn encode_to_bytes(&self, dimensions: usize) -> Bytes {
        let mut buf = BytesMut::new();

        // Count prefix
        let count = self.entries.len();
        if count > u16::MAX as usize {
            panic!("Too many centroids in chunk: {}", count);
        }
        buf.extend_from_slice(&(count as u16).to_le_bytes());

        // Entries
        for entry in &self.entries {
            debug_assert_eq!(
                entry.dimensions(),
                dimensions,
                "Centroid dimension mismatch"
            );
            entry.encode(&mut buf);
        }

        buf.freeze()
    }

    /// Decode the chunk with known dimensionality.
    pub fn decode_from_bytes(buf: &[u8], dimensions: usize) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for centroid chunk count".to_string(),
            });
        }

        let count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        let mut slice = &buf[2..];

        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(decode_centroid_entry(&mut slice, dimensions)?);
        }

        Ok(CentroidChunkValue { entries })
    }

    /// Find a centroid by ID.
    pub fn get_centroid(&self, centroid_id: u64) -> Option<&CentroidEntry> {
        self.entries.iter().find(|e| e.centroid_id == centroid_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_centroid_chunk() {
        // given
        let dimensions = 4;
        let value = CentroidChunkValue::new(vec![
            CentroidEntry::new(1, vec![1.0, 2.0, 3.0, 4.0]),
            CentroidEntry::new(2, vec![5.0, 6.0, 7.0, 8.0]),
        ]);

        // when
        let encoded = value.encode_to_bytes(dimensions);
        let decoded = CentroidChunkValue::decode_from_bytes(&encoded, dimensions).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_handle_empty_chunk() {
        // given
        let dimensions = 128;
        let value = CentroidChunkValue::new(vec![]);

        // when
        let encoded = value.encode_to_bytes(dimensions);
        let decoded = CentroidChunkValue::decode_from_bytes(&encoded, dimensions).unwrap();

        // then
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_handle_high_dimensional_centroids() {
        // given
        let dimensions = 1536;
        let vector: Vec<f32> = (0..dimensions).map(|i| i as f32).collect();
        let value = CentroidChunkValue::new(vec![CentroidEntry::new(42, vector.clone())]);

        // when
        let encoded = value.encode_to_bytes(dimensions);
        let decoded = CentroidChunkValue::decode_from_bytes(&encoded, dimensions).unwrap();

        // then
        assert_eq!(decoded.entries.len(), 1);
        assert_eq!(decoded.entries[0].centroid_id, 42);
        assert_eq!(decoded.entries[0].vector, vector);
    }

    #[test]
    fn should_find_centroid_by_id() {
        // given
        let value = CentroidChunkValue::new(vec![
            CentroidEntry::new(10, vec![1.0, 2.0]),
            CentroidEntry::new(20, vec![3.0, 4.0]),
            CentroidEntry::new(30, vec![5.0, 6.0]),
        ]);

        // when / then
        assert_eq!(value.get_centroid(20).unwrap().centroid_id, 20);
        assert!(value.get_centroid(99).is_none());
    }

    #[test]
    fn should_preserve_centroid_ids() {
        // given
        let dimensions = 2;
        let value = CentroidChunkValue::new(vec![
            CentroidEntry::new(100, vec![1.0, 2.0]),
            CentroidEntry::new(200, vec![3.0, 4.0]),
            CentroidEntry::new(u64::MAX, vec![5.0, 6.0]),
        ]);

        // when
        let encoded = value.encode_to_bytes(dimensions);
        let decoded = CentroidChunkValue::decode_from_bytes(&encoded, dimensions).unwrap();

        // then
        assert_eq!(decoded.entries[0].centroid_id, 100);
        assert_eq!(decoded.entries[1].centroid_id, 200);
        assert_eq!(decoded.entries[2].centroid_id, u64::MAX);
    }
}

//! VectorData value encoding/decoding.
//!
//! Stores the raw embedding vector bytes for a single vector.
//!
//! ## Storage Design
//!
//! Vectors are stored individually (one record per vector) rather than batched
//! together. This enables:
//!
//! - **Efficient point lookups**: Load a single vector without reading neighbors
//! - **Partial loading**: During filtered search, only load vectors that pass filters
//! - **Independent updates**: Upsert/delete individual vectors without rewriting batches
//!
//! ## Separation from Metadata
//!
//! Vector data and metadata are stored in separate records (`VectorData` vs `VectorMeta`)
//! because:
//!
//! - **Different access patterns**: Distance computation needs only the vector bytes;
//!   result formatting needs only the metadata
//! - **Size optimization**: Vectors are large (e.g., 6 KB at 1536 dims); metadata is small
//! - **Metadata-only scans**: Filtering can scan metadata without loading vectors
//!
//! ## Dimensionality
//!
//! The vector length is not stored in the value—it's obtained from `CollectionMeta`.
//! All vectors in a collection must have the same dimensionality. The value is simply
//! `dimensions × 4` bytes of contiguous f32 values.

use super::{EncodingError, decode_fixed_element_array, encode_fixed_element_array};
use bytes::{Bytes, BytesMut};

/// VectorData value storing the raw embedding vector.
///
/// The key for this record is `VectorDataKey { vector_id }`, where `vector_id`
/// is the internal u64 ID (not the user-provided external ID).
///
/// ## Value Layout (little-endian)
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │  vector:  FixedElementArray<f32>                               │
/// │           (dimensions × 4 bytes, no length prefix)             │
/// │                                                                │
/// │  Example at 1536 dimensions:                                   │
/// │  [f32][f32][f32]...[f32]  (1536 elements = 6,144 bytes)        │
/// └────────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Common Dimensionalities
///
/// | Model              | Dimensions | Value Size |
/// |--------------------|------------|------------|
/// | MiniLM-L6          | 384        | 1.5 KB     |
/// | BERT base          | 768        | 3 KB       |
/// | OpenAI ada-002     | 1536       | 6 KB       |
/// | OpenAI text-3-large| 3072       | 12 KB      |
///
/// ## Note on Decoding
///
/// The dimensionality is obtained from `CollectionMeta` at runtime. The decode
/// function validates that the buffer length is divisible by 4 (size of f32).
#[derive(Debug, Clone, PartialEq)]
pub struct VectorDataValue {
    /// Vector components as f32 values in little-endian format.
    ///
    /// Length must equal `CollectionMeta.dimensions`.
    pub vector: Vec<f32>,
}

impl VectorDataValue {
    pub fn new(vector: Vec<f32>) -> Self {
        Self { vector }
    }

    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.vector.len() * 4);
        encode_fixed_element_array(&self.vector, &mut buf);
        buf.freeze()
    }

    /// Decode vector data from bytes.
    ///
    /// Note: The buffer should contain exactly `dimensions * 4` bytes.
    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        let mut slice = buf;
        let vector: Vec<f32> = decode_fixed_element_array(&mut slice, 4)?;
        Ok(VectorDataValue { vector })
    }

    /// Returns the dimensionality of the vector.
    pub fn dimensions(&self) -> usize {
        self.vector.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_vector_data_value() {
        // given
        let value = VectorDataValue::new(vec![1.0, 2.0, 3.0, 4.0]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert_eq!(encoded.len(), 16); // 4 * 4 bytes
    }

    #[test]
    fn should_handle_high_dimensional_vector() {
        // given
        let vector: Vec<f32> = (0..1536).map(|i| i as f32 * 0.001).collect();
        let value = VectorDataValue::new(vector.clone());

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.dimensions(), 1536);
        assert_eq!(decoded.vector, vector);
    }

    #[test]
    fn should_handle_special_float_values() {
        // given
        let value = VectorDataValue::new(vec![
            0.0,
            -0.0,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::MIN,
            f32::MAX,
        ]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.vector[0], 0.0);
        assert_eq!(decoded.vector[1], -0.0);
        assert_eq!(decoded.vector[2], f32::INFINITY);
        assert_eq!(decoded.vector[3], f32::NEG_INFINITY);
        assert_eq!(decoded.vector[4], f32::MIN);
        assert_eq!(decoded.vector[5], f32::MAX);
    }

    #[test]
    fn should_handle_empty_vector() {
        // given
        let value = VectorDataValue::new(vec![]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = VectorDataValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.dimensions(), 0);
    }

    #[test]
    fn should_return_error_for_misaligned_buffer() {
        // given
        let buf = vec![0u8; 5]; // 5 bytes, not divisible by 4

        // when
        let result = VectorDataValue::decode_from_bytes(&buf);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("not divisible"));
    }
}

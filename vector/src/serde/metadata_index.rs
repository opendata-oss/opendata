//! MetadataIndex value encoding/decoding.
//!
//! Inverted index mapping metadata field/value pairs to vector IDs.

use super::EncodingError;
use crate::serde::vector_bitmap::VectorBitmap;
use bytes::{BufMut, Bytes, BytesMut};
use roaring::RoaringTreemap;

/// MetadataIndex value storing vector IDs to include and exclude for a metadata field/value pair.
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataIndexValue {
    pub included: VectorBitmap,
    pub excluded: VectorBitmap,
}

impl MetadataIndexValue {
    pub fn new() -> Self {
        Self {
            included: VectorBitmap::new(),
            excluded: VectorBitmap::new(),
        }
    }

    pub fn from_treemap(vector_ids: RoaringTreemap) -> Self {
        Self {
            included: VectorBitmap::from_treemap(vector_ids),
            excluded: VectorBitmap::new(),
        }
    }

    pub fn include(vector_id: u64) -> Self {
        let mut value = Self::new();
        value.include_vector(vector_id);
        value
    }

    pub fn exclude(vector_id: u64) -> Self {
        let mut value = Self::new();
        value.exclude_vector(vector_id);
        value
    }

    pub fn include_vector(&mut self, vector_id: u64) {
        self.excluded.remove(vector_id);
        self.included.insert(vector_id);
    }

    pub fn exclude_vector(&mut self, vector_id: u64) {
        self.included.remove(vector_id);
        self.excluded.insert(vector_id);
    }

    pub fn make_mutually_exclusive(&mut self) {
        let mut overlap = self.included.clone();
        overlap.intersect_with(&self.excluded);
        debug_assert!(overlap.is_empty());
        self.excluded.difference_with(&overlap);
    }

    pub fn effective_vector_ids(&self) -> RoaringTreemap {
        let mut included = self.included.vector_ids.clone();
        included -= &self.excluded.vector_ids;
        included
    }

    pub fn contains(&self, vector_id: u64) -> bool {
        self.included.contains(vector_id) && !self.excluded.contains(vector_id)
    }

    pub fn len(&self) -> u64 {
        self.effective_vector_ids().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encode_to_bytes(&self) -> Result<Bytes, EncodingError> {
        let mut normalized = self.clone();
        normalized.make_mutually_exclusive();
        let included = normalized.included.encode_to_bytes()?;
        let excluded = normalized.excluded.encode_to_bytes()?;

        let mut buf = BytesMut::with_capacity(8 + included.len() + excluded.len());
        buf.put_u32_le(u32::try_from(included.len()).expect("included bitmap too large"));
        buf.extend_from_slice(&included);
        buf.put_u32_le(u32::try_from(excluded.len()).expect("excluded bitmap too large"));
        buf.extend_from_slice(&excluded);
        Ok(buf.freeze())
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 8 {
            return Err(EncodingError {
                message: "Buffer too short for MetadataIndexValue".to_string(),
            });
        }

        let included_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + included_len + 4 {
            return Err(EncodingError {
                message: "Buffer too short for MetadataIndexValue included bitmap".to_string(),
            });
        }
        let included = VectorBitmap::decode_from_bytes(&buf[4..4 + included_len])?;

        let excluded_len_offset = 4 + included_len;
        let excluded_len = u32::from_le_bytes([
            buf[excluded_len_offset],
            buf[excluded_len_offset + 1],
            buf[excluded_len_offset + 2],
            buf[excluded_len_offset + 3],
        ]) as usize;
        let excluded_offset = excluded_len_offset + 4;
        if buf.len() < excluded_offset + excluded_len {
            return Err(EncodingError {
                message: "Buffer too short for MetadataIndexValue excluded bitmap".to_string(),
            });
        }
        let excluded =
            VectorBitmap::decode_from_bytes(&buf[excluded_offset..excluded_offset + excluded_len])?;

        let mut value = Self { included, excluded };
        value.make_mutually_exclusive();
        Ok(value)
    }
}

impl Default for MetadataIndexValue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_metadata_index_value() {
        // given
        let mut value = MetadataIndexValue::new();
        value.include_vector(1);
        value.include_vector(2);
        value.exclude_vector(3);

        // when
        let encoded = value.encode_to_bytes().unwrap();
        let decoded = MetadataIndexValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert!(decoded.included.contains(1));
        assert!(decoded.included.contains(2));
        assert!(decoded.excluded.contains(3));
    }

    #[test]
    fn should_compute_effective_vector_ids() {
        // given
        let mut value = MetadataIndexValue::new();
        value.include_vector(1);
        value.include_vector(2);
        value.exclude_vector(2);

        // when
        let effective = value.effective_vector_ids();

        // then
        assert!(effective.contains(1));
        assert!(!effective.contains(2));
    }
}

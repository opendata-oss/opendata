//! IdDictionary value encoding/decoding.
//!
//! Maps user-provided external IDs to system-assigned internal vector IDs.
//!
//! ## Why Two ID Spaces?
//!
//! Users provide **external IDs**—arbitrary strings up to 64 bytes—to identify
//! their vectors. The system maps these to **internal IDs** (u64) because:
//!
//! 1. **Efficient bitmaps**: Fixed-width u64 keys enable RoaringTreemap operations
//! 2. **Better compression**: Monotonically increasing IDs cluster well in bitmaps
//! 3. **Lifecycle management**: System controls ID allocation and reuse
//!
//! ## Upsert Behavior
//!
//! When inserting a vector with an existing external ID:
//!
//! 1. Look up existing internal ID from `IdDictionary`
//! 2. Delete old vector: add to deleted bitmap, tombstone data/metadata
//! 3. Allocate new internal ID (from `SeqBlock`)
//! 4. Write new vector with new internal ID
//! 5. Update `IdDictionary` to point to new internal ID
//!
//! This "delete old + insert new" approach avoids expensive read-modify-write
//! cycles to update every posting list and metadata index entry.
//!
//! ## Delete Operation
//!
//! Deleting a vector requires atomic operations via `WriteBatch`:
//! 1. Add vector ID to deleted bitmap (centroid_id = 0 posting list)
//! 2. Tombstone `VectorData` record
//! 3. Tombstone `VectorMeta` record
//! 4. Tombstone `IdDictionary` entry
//!
//! Metadata index cleanup happens during LIRE maintenance.

use super::{Decode, Encode, EncodingError};
use bytes::{Bytes, BytesMut};

/// IdDictionary value storing the internal vector ID for an external ID.
///
/// The key for this record is `IdDictionaryKey { external_id }`, which uses
/// `TerminatedBytes` encoding to preserve lexicographic ordering of external IDs.
///
/// ## Value Layout (little-endian)
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │  vector_id:  u64  (8 bytes, little-endian)                     │
/// └────────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Usage
///
/// - **Insert**: Look up external ID → if exists, upsert; else allocate new internal ID
/// - **Get by external ID**: Point lookup to resolve external → internal mapping
/// - **Get by internal ID**: Use `VectorMeta` which stores the reverse mapping
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdDictionaryValue {
    /// Internal vector ID (system-assigned, monotonically increasing).
    ///
    /// This ID is used in `VectorData`, `VectorMeta`, posting lists, and
    /// metadata indexes. It's allocated from `SeqBlock` using block-based
    /// allocation for crash safety.
    pub vector_id: u64,
}

impl IdDictionaryValue {
    pub fn new(vector_id: u64) -> Self {
        Self { vector_id }
    }

    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(8);
        self.vector_id.encode(&mut buf);
        buf.freeze()
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 8 {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for IdDictionaryValue: need 8 bytes, have {}",
                    buf.len()
                ),
            });
        }
        let mut slice = buf;
        let vector_id = u64::decode(&mut slice)?;
        Ok(IdDictionaryValue { vector_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_id_dictionary_value() {
        // given
        let value = IdDictionaryValue::new(12345);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = IdDictionaryValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert_eq!(encoded.len(), 8);
    }

    #[test]
    fn should_encode_and_decode_max_value() {
        // given
        let value = IdDictionaryValue::new(u64::MAX);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = IdDictionaryValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_return_error_for_short_buffer() {
        // given
        let short_buf = vec![0u8; 4]; // Only 4 bytes, need 8

        // when
        let result = IdDictionaryValue::decode_from_bytes(&short_buf);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Buffer too short"));
    }
}

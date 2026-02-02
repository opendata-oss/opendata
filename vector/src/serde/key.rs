//! Key encoding/decoding for vector database records.
//!
//! All keys use big-endian encoding for lexicographic ordering.

use super::{EncodingError, FieldValue, KEY_VERSION, RecordKey, RecordType, record_type_from_tag};
use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::KeyPrefix;
use common::serde::terminated_bytes;

/// CollectionMeta key - singleton record storing collection schema.
///
/// Key layout: `[version | tag]` (2 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionMetaKey;

impl RecordKey for CollectionMetaKey {
    const RECORD_TYPE: RecordType = RecordType::CollectionMeta;
}

impl CollectionMetaKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for CollectionMetaKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        Ok(CollectionMetaKey)
    }
}

/// Deletions key - singleton record storing deleted vector IDs bitmap.
///
/// Key layout: `[version | tag]` (2 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionsKey;

impl RecordKey for DeletionsKey {
    const RECORD_TYPE: RecordType = RecordType::Deletions;
}

impl DeletionsKey {
    pub fn new() -> Self {
        Self
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for DeletionsKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        Ok(DeletionsKey)
    }
}

impl Default for DeletionsKey {
    fn default() -> Self {
        Self::new()
    }
}

/// CentroidChunk key - stores a chunk of cluster centroids.
///
/// Key layout: `[version | tag | chunk_id:u32-BE]` (6 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentroidChunkKey {
    pub chunk_id: u32,
}

impl RecordKey for CentroidChunkKey {
    const RECORD_TYPE: RecordType = RecordType::CentroidChunk;
}

impl CentroidChunkKey {
    pub fn new(chunk_id: u32) -> Self {
        Self { chunk_id }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(6);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.put_u32(self.chunk_id); // Big-endian
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 6 {
            return Err(EncodingError {
                message: "Buffer too short for CentroidChunkKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        let chunk_id = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        Ok(CentroidChunkKey { chunk_id })
    }

    /// Returns a range covering all centroid chunk keys.
    pub fn all_chunks_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// PostingList key - maps centroid ID to vector IDs.
///
/// Key layout: `[version | tag | centroid_id:u32-BE]` (6 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingListKey {
    pub centroid_id: u32,
}

impl RecordKey for PostingListKey {
    const RECORD_TYPE: RecordType = RecordType::PostingList;
}

impl PostingListKey {
    pub fn new(centroid_id: u32) -> Self {
        Self { centroid_id }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(6);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.put_u32(self.centroid_id); // Big-endian
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 6 {
            return Err(EncodingError {
                message: "Buffer too short for PostingListKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        let centroid_id = u32::from_be_bytes([buf[2], buf[3], buf[4], buf[5]]);
        Ok(PostingListKey { centroid_id })
    }

    /// Returns a range covering all posting list keys.
    pub fn all_posting_lists_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// IdDictionary key - maps external string IDs to internal u64 vector IDs.
///
/// Key layout: `[version | tag | external_id:TerminatedBytes]` (variable)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdDictionaryKey {
    pub external_id: String,
}

impl RecordKey for IdDictionaryKey {
    const RECORD_TYPE: RecordType = RecordType::IdDictionary;
}

impl IdDictionaryKey {
    pub fn new(external_id: impl Into<String>) -> Self {
        Self {
            external_id: external_id.into(),
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        terminated_bytes::serialize(self.external_id.as_bytes(), &mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 3 {
            // At minimum: version + tag + terminator
            return Err(EncodingError {
                message: "Buffer too short for IdDictionaryKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;

        let mut slice = &buf[2..];
        let external_id_bytes =
            terminated_bytes::deserialize(&mut slice).map_err(|e| EncodingError {
                message: format!("Failed to decode external_id: {}", e),
            })?;

        let external_id =
            String::from_utf8(external_id_bytes.to_vec()).map_err(|e| EncodingError {
                message: format!("Invalid UTF-8 in external_id: {}", e),
            })?;

        Ok(IdDictionaryKey { external_id })
    }

    /// Returns a range covering all ID dictionary keys.
    pub fn all_ids_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }

    /// Returns a range covering all IDs with the given prefix.
    ///
    /// Note: This creates a range over the serialized key format. The prefix
    /// is serialized using TerminatedBytes encoding, so the range will include
    /// all IDs that start with the given string prefix.
    pub fn prefix_range(prefix: &str) -> BytesRange {
        let mut buf = BytesMut::new();
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        terminated_bytes::serialize(prefix.as_bytes(), &mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// VectorData key - stores raw vector bytes.
///
/// Key layout: `[version | tag | vector_id:u64-BE]` (10 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorDataKey {
    pub vector_id: u64,
}

impl RecordKey for VectorDataKey {
    const RECORD_TYPE: RecordType = RecordType::VectorData;
}

impl VectorDataKey {
    pub fn new(vector_id: u64) -> Self {
        Self { vector_id }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(10);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.put_u64(self.vector_id); // Big-endian
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 10 {
            return Err(EncodingError {
                message: "Buffer too short for VectorDataKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        let vector_id = u64::from_be_bytes([
            buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
        ]);
        Ok(VectorDataKey { vector_id })
    }

    /// Returns a range covering all vector data keys.
    pub fn all_vectors_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// VectorMeta key - stores vector metadata including external ID.
///
/// Key layout: `[version | tag | vector_id:u64-BE]` (10 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorMetaKey {
    pub vector_id: u64,
}

impl RecordKey for VectorMetaKey {
    const RECORD_TYPE: RecordType = RecordType::VectorMeta;
}

impl VectorMetaKey {
    pub fn new(vector_id: u64) -> Self {
        Self { vector_id }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(10);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.put_u64(self.vector_id); // Big-endian
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 10 {
            return Err(EncodingError {
                message: "Buffer too short for VectorMetaKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        let vector_id = u64::from_be_bytes([
            buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
        ]);
        Ok(VectorMetaKey { vector_id })
    }

    /// Returns a range covering all vector metadata keys.
    pub fn all_metadata_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// MetadataIndex key - inverted index mapping metadata values to vector IDs.
///
/// Key layout: `[version | tag | field:TerminatedBytes | value:FieldValue]` (variable)
#[derive(Debug, Clone, PartialEq)]
pub struct MetadataIndexKey {
    pub field: String,
    pub value: FieldValue,
}

impl RecordKey for MetadataIndexKey {
    const RECORD_TYPE: RecordType = RecordType::MetadataIndex;
}

impl MetadataIndexKey {
    pub fn new(field: impl Into<String>, value: FieldValue) -> Self {
        Self {
            field: field.into(),
            value,
        }
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        terminated_bytes::serialize(self.field.as_bytes(), &mut buf);
        self.value.encode_sortable(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 4 {
            // Minimum: version + tag + field terminator + value type
            return Err(EncodingError {
                message: "Buffer too short for MetadataIndexKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;

        let mut slice = &buf[2..];

        let field_bytes = terminated_bytes::deserialize(&mut slice).map_err(|e| EncodingError {
            message: format!("Failed to decode field: {}", e),
        })?;

        let field = String::from_utf8(field_bytes.to_vec()).map_err(|e| EncodingError {
            message: format!("Invalid UTF-8 in field: {}", e),
        })?;

        let value = FieldValue::decode_sortable(&mut slice)?;

        Ok(MetadataIndexKey { field, value })
    }

    /// Returns a range covering all metadata index keys for a specific field.
    pub fn field_range(field: &str) -> BytesRange {
        let mut buf = BytesMut::new();
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        terminated_bytes::serialize(field.as_bytes(), &mut buf);
        BytesRange::prefix(buf.freeze())
    }

    /// Returns a range covering all metadata index keys.
    pub fn all_indexes_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// SeqBlock key - singleton record storing sequence allocation state.
///
/// Key layout: `[version | tag]` (2 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqBlockKey;

impl RecordKey for SeqBlockKey {
    const RECORD_TYPE: RecordType = RecordType::SeqBlock;
}

impl SeqBlockKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(2);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for SeqBlockKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        Ok(SeqBlockKey)
    }
}

/// Validates the key prefix (version and record tag).
fn validate_key_prefix<T: RecordKey>(buf: &[u8]) -> Result<(), EncodingError> {
    let prefix = KeyPrefix::from_bytes_versioned(buf, KEY_VERSION)?;
    let record_type = record_type_from_tag(prefix.tag())?;

    if record_type != T::RECORD_TYPE {
        return Err(EncodingError {
            message: format!(
                "Invalid record type: expected {:?}, got {:?}",
                T::RECORD_TYPE,
                record_type
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_collection_meta_key() {
        // given
        let key = CollectionMetaKey;

        // when
        let encoded = key.encode();
        let decoded = CollectionMetaKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 2);
    }

    #[test]
    fn should_encode_and_decode_centroid_chunk_key() {
        // given
        let key = CentroidChunkKey::new(42);

        // when
        let encoded = key.encode();
        let decoded = CentroidChunkKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 6);
    }

    #[test]
    fn should_preserve_centroid_chunk_key_ordering() {
        // given
        let key1 = CentroidChunkKey::new(1);
        let key2 = CentroidChunkKey::new(2);
        let key3 = CentroidChunkKey::new(100);

        // when
        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        // then
        assert!(encoded1 < encoded2);
        assert!(encoded2 < encoded3);
    }

    #[test]
    fn should_encode_and_decode_posting_list_key() {
        // given
        let key = PostingListKey::new(123);

        // when
        let encoded = key.encode();
        let decoded = PostingListKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_deletions_key() {
        // given
        let key = DeletionsKey::new();

        // when
        let encoded = key.encode();
        let decoded = DeletionsKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_id_dictionary_key() {
        // given
        let key = IdDictionaryKey::new("my-vector-id");

        // when
        let encoded = key.encode();
        let decoded = IdDictionaryKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_preserve_id_dictionary_key_ordering() {
        // given
        let key1 = IdDictionaryKey::new("aaa");
        let key2 = IdDictionaryKey::new("aab");
        let key3 = IdDictionaryKey::new("bbb");

        // when
        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        // then
        assert!(encoded1 < encoded2);
        assert!(encoded2 < encoded3);
    }

    #[test]
    fn should_encode_and_decode_vector_data_key() {
        // given
        let key = VectorDataKey::new(0xDEADBEEF_CAFEBABE);

        // when
        let encoded = key.encode();
        let decoded = VectorDataKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 10);
    }

    #[test]
    fn should_preserve_vector_data_key_ordering() {
        // given
        let key1 = VectorDataKey::new(1);
        let key2 = VectorDataKey::new(2);
        let key3 = VectorDataKey::new(u64::MAX);

        // when
        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        // then
        assert!(encoded1 < encoded2);
        assert!(encoded2 < encoded3);
    }

    #[test]
    fn should_encode_and_decode_vector_meta_key() {
        // given
        let key = VectorMetaKey::new(12345);

        // when
        let encoded = key.encode();
        let decoded = VectorMetaKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_metadata_index_key_string() {
        // given
        let key = MetadataIndexKey::new("category", FieldValue::String("shoes".to_string()));

        // when
        let encoded = key.encode();
        let decoded = MetadataIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_metadata_index_key_int64() {
        // given
        let key = MetadataIndexKey::new("price", FieldValue::Int64(99));

        // when
        let encoded = key.encode();
        let decoded = MetadataIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_metadata_index_key_float64() {
        // given
        let key = MetadataIndexKey::new("score", FieldValue::Float64(1.23));

        // when
        let encoded = key.encode();
        let decoded = MetadataIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_encode_and_decode_metadata_index_key_bool() {
        // given
        let key = MetadataIndexKey::new("active", FieldValue::Bool(true));

        // when
        let encoded = key.encode();
        let decoded = MetadataIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_preserve_int64_ordering_in_metadata_index() {
        // given
        let key_neg = MetadataIndexKey::new("price", FieldValue::Int64(-100));
        let key_zero = MetadataIndexKey::new("price", FieldValue::Int64(0));
        let key_pos = MetadataIndexKey::new("price", FieldValue::Int64(100));

        // when
        let encoded_neg = key_neg.encode();
        let encoded_zero = key_zero.encode();
        let encoded_pos = key_pos.encode();

        // then
        assert!(encoded_neg < encoded_zero);
        assert!(encoded_zero < encoded_pos);
    }

    #[test]
    fn should_preserve_float64_ordering_in_metadata_index() {
        // given
        let key_neg = MetadataIndexKey::new("score", FieldValue::Float64(-1.0));
        let key_zero = MetadataIndexKey::new("score", FieldValue::Float64(0.0));
        let key_pos = MetadataIndexKey::new("score", FieldValue::Float64(1.0));

        // when
        let encoded_neg = key_neg.encode();
        let encoded_zero = key_zero.encode();
        let encoded_pos = key_pos.encode();

        // then
        assert!(encoded_neg < encoded_zero);
        assert!(encoded_zero < encoded_pos);
    }

    #[test]
    fn should_encode_and_decode_seq_block_key() {
        // given
        let key = SeqBlockKey;

        // when
        let encoded = key.encode();
        let decoded = SeqBlockKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 2);
    }

    #[test]
    fn should_reject_wrong_record_type() {
        // given
        let collection_meta_key = CollectionMetaKey;
        let encoded = collection_meta_key.encode();

        // when
        let result = SeqBlockKey::decode(&encoded);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Invalid record type"));
    }

    #[test]
    fn should_reject_wrong_version() {
        // given
        let mut buf = BytesMut::new();
        buf.put_u8(0x99); // Wrong version
        buf.put_u8(RecordType::CollectionMeta.tag().as_byte());

        // when
        let result = CollectionMetaKey::decode(&buf);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("version"));
    }
}

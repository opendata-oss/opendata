//! Key encoding/decoding for vector database records.
//!
//! All keys use big-endian encoding for lexicographic ordering.

use super::{
    Decode, Encode, EncodingError, FieldValue, KEY_VERSION, RecordKey, RecordType, SUBSYSTEM,
};
use crate::serde::vector_id::LEAF_LEVEL;
use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
#[allow(unused_imports)]
use bytes::{BufMut, Bytes, BytesMut};
use common::BytesRange;
use common::serde::key_prefix::KeyPrefix;
use common::serde::terminated_bytes;
use std::ops::Bound::{Excluded, Included};

/// CollectionMeta key - singleton record storing collection schema.
///
/// Key layout: `[subsystem | version | tag]` (3 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionMetaKey;

impl RecordKey for CollectionMetaKey {
    const RECORD_TYPE: RecordType = RecordType::CollectionMeta;
}

impl CollectionMetaKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(3);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 3 {
            return Err(EncodingError {
                message: "Buffer too short for CollectionMetaKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        Ok(CollectionMetaKey)
    }
}

/// Centroids key - singleton record storing centroid tree metadata.
///
/// Key layout: `[subsystem | version | tag]` (3 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentroidsKey;

impl RecordKey for CentroidsKey {
    const RECORD_TYPE: RecordType = RecordType::Centroids;
}

impl CentroidsKey {
    pub fn new() -> Self {
        Self
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(3);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 3 {
            return Err(EncodingError {
                message: "Buffer too short for CentroidsKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        Ok(CentroidsKey)
    }
}

impl Default for CentroidsKey {
    fn default() -> Self {
        Self::new()
    }
}

/// PostingList key - maps centroid ID to vector IDs.
///
/// Key layout: `[subsystem | version | tag | centroid_id:u64-BE]` (11 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostingListKey {
    pub(crate) centroid_id: VectorId,
}

impl RecordKey for PostingListKey {
    const RECORD_TYPE: RecordType = RecordType::PostingList;
}

impl PostingListKey {
    pub(crate) fn new(centroid_id: VectorId) -> Self {
        assert!(centroid_id.is_centroid() || centroid_id.is_root());
        Self { centroid_id }
    }

    pub(crate) fn encode_prefix_for_level(level: u8) -> Bytes {
        let mut buf = BytesMut::with_capacity(4);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        VectorId::encode_level_prefix(&mut buf, level);
        buf.freeze()
    }

    pub(crate) fn inner_level_bytes_range() -> BytesRange {
        let start_buf = Self::encode_prefix_for_level(LEAF_LEVEL + 1);
        let end_buf = Self::new(ROOT_VECTOR_ID).encode();
        BytesRange::new(Included(start_buf), Excluded(end_buf))
    }

    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(11);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        self.centroid_id.encode(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 11 {
            return Err(EncodingError {
                message: "Buffer too short for PostingListKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        let mut suffix = &buf[3..];
        let centroid_id = VectorId::decode(&mut suffix)?;
        Ok(PostingListKey { centroid_id })
    }

    /// Returns a range covering all posting list keys.
    pub fn all_posting_lists_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(3);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// IdDictionary key - maps external string IDs to internal u64 vector IDs.
///
/// Key layout: `[subsystem | version | tag | external_id:TerminatedBytes]` (variable)
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
        if buf.len() < 4 {
            // At minimum: subsystem + version + tag + terminator
            return Err(EncodingError {
                message: "Buffer too short for IdDictionaryKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;

        let mut slice = &buf[3..];
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
        let mut buf = BytesMut::with_capacity(3);
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
/// Key layout: `[subsystem | version | tag | vector_id:u64-BE]` (11 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorDataKey {
    pub(crate) vector_id: VectorId,
}

impl RecordKey for VectorDataKey {
    const RECORD_TYPE: RecordType = RecordType::VectorData;
}

impl VectorDataKey {
    pub(crate) fn new(vector_id: VectorId) -> Self {
        assert!(vector_id.is_data_vector());
        Self { vector_id }
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(11);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        self.vector_id.encode(&mut buf);
        buf.freeze()
    }

    #[allow(dead_code)]
    pub(crate) fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 11 {
            return Err(EncodingError {
                message: "Buffer too short for VectorDataKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        let vector_id = VectorId::decode(&mut &buf[3..])?;
        Ok(VectorDataKey { vector_id })
    }

    /// Returns a range covering all vector data keys.
    #[allow(dead_code)]
    pub(crate) fn all_vectors_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(3);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// MetadataIndex key - inverted index mapping metadata values to vector IDs.
///
/// Key layout: `[subsystem | version | tag | field:TerminatedBytes | value:FieldValue]` (variable)
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
        if buf.len() < 5 {
            // Minimum: subsystem + version + tag + field terminator + value type
            return Err(EncodingError {
                message: "Buffer too short for MetadataIndexKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;

        let mut slice = &buf[3..];

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
        let mut buf = BytesMut::with_capacity(3);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// SeqBlock key - singleton record storing sequence allocation state.
///
/// Key layout: `[subsystem | version | tag]` (3 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqBlockKey;

impl RecordKey for SeqBlockKey {
    const RECORD_TYPE: RecordType = RecordType::SeqBlock;
}

impl SeqBlockKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(3);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 3 {
            return Err(EncodingError {
                message: "Buffer too short for SeqBlockKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        Ok(SeqBlockKey)
    }
}

/// CentroidSeqBlock key - singleton record storing centroid sequence allocation state.
///
/// Key layout: `[subsystem | version | tag]` (3 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentroidSeqBlockKey;

impl RecordKey for CentroidSeqBlockKey {
    const RECORD_TYPE: RecordType = RecordType::CentroidSeqBlock;
}

impl CentroidSeqBlockKey {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(3);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        buf.freeze()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 3 {
            return Err(EncodingError {
                message: "Buffer too short for CentroidSeqBlockKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        Ok(CentroidSeqBlockKey)
    }
}

/// CentroidStats key - per-centroid vector count for rebalance triggers.
///
/// Key layout: `[subsystem | version | tag | level:u8 | centroid_id:u64-BE]` (12 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CentroidStatsKey {
    pub(crate) centroid_id: VectorId,
}

impl RecordKey for CentroidStatsKey {
    const RECORD_TYPE: RecordType = RecordType::CentroidStats;
}

impl CentroidStatsKey {
    pub(crate) fn new(centroid_id: VectorId) -> Self {
        assert!(centroid_id.is_centroid());
        Self { centroid_id }
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(12);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        self.centroid_id.encode(&mut buf);
        buf.freeze()
    }

    pub(crate) fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 11 {
            return Err(EncodingError {
                message: "Buffer too short for CentroidStatsKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        let centroid_id = VectorId::decode(&mut &buf[3..])?;
        Ok(CentroidStatsKey { centroid_id })
    }
}

/// CentroidInfo key - per-centroid metadata for the centroid tree.
///
/// Key layout: `[subsystem | version | tag | centroid_id:u64-BE]` (11 bytes)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CentroidInfoKey {
    pub(crate) centroid_id: VectorId,
}

impl RecordKey for CentroidInfoKey {
    const RECORD_TYPE: RecordType = RecordType::CentroidInfo;
}

impl CentroidInfoKey {
    pub(crate) fn new(centroid_id: VectorId) -> Self {
        assert!(centroid_id.is_centroid());
        Self { centroid_id }
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(11);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        self.centroid_id.encode(&mut buf);
        buf.freeze()
    }

    pub(crate) fn decode(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 11 {
            return Err(EncodingError {
                message: "Buffer too short for CentroidInfoKey".to_string(),
            });
        }
        validate_key_prefix::<Self>(buf)?;
        let centroid_id = VectorId::decode(&mut &buf[3..])?;
        Ok(CentroidInfoKey { centroid_id })
    }

    /// Returns a range covering all centroid info keys.
    #[allow(dead_code)]
    pub(crate) fn all_centroid_infos_range() -> BytesRange {
        let mut buf = BytesMut::with_capacity(3);
        Self::RECORD_TYPE.prefix().write_to(&mut buf);
        BytesRange::prefix(buf.freeze())
    }
}

/// Validates the key prefix (version and record tag).
fn validate_key_prefix<T: RecordKey>(buf: &[u8]) -> Result<(), EncodingError> {
    let prefix = KeyPrefix::from_bytes_with_validation(buf, SUBSYSTEM, KEY_VERSION)?;
    let record_type = RecordType::from_prefix(prefix)?;

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
        assert_eq!(encoded.len(), 3);
    }

    #[test]
    fn should_encode_and_decode_posting_list_key() {
        // given
        let key = PostingListKey::new(VectorId::centroid_id(1, 123));

        // when
        let encoded = key.encode();
        let decoded = PostingListKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_construct_correct_inner_level_range() {
        // given/when:
        let max_num: u64 = 0x00FF_FFFF_FFFF_FFFF;
        let min_num: u64 = 1;
        let range = PostingListKey::inner_level_bytes_range();

        // then: assert leaf centroids don't fall in range
        for num in [max_num, 12345, min_num] {
            let vid = VectorId::centroid_id(1, num);
            let key = PostingListKey::new(vid);
            assert!(!range.contains(&key.encode()));
        }
        // then: assert root does not fall in range
        let key = PostingListKey::new(ROOT_VECTOR_ID);
        assert!(!range.contains(&key.encode()));
        // then: assert inner centroids fall in range
        for l in 2..10 {
            for num in [max_num, 12345, min_num] {
                let vid = VectorId::centroid_id(l, num);
                let key = PostingListKey::new(vid);
                assert!(range.contains(&key.encode()));
            }
        }
    }

    #[test]
    fn should_encode_and_decode_centroids_key() {
        // given
        let key = CentroidsKey::new();

        // when
        let encoded = key.encode();
        let decoded = CentroidsKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 3);
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
        let key = VectorDataKey::new(VectorId::data_vector_id(0x00DE_ADBE_EFCA_FEBA));

        // when
        let encoded = key.encode();
        let decoded = VectorDataKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 11);
    }

    #[test]
    fn should_preserve_vector_data_key_ordering() {
        // given
        let key1 = VectorDataKey::new(VectorId::data_vector_id(1));
        let key2 = VectorDataKey::new(VectorId::data_vector_id(2));
        let key3 = VectorDataKey::new(VectorId::data_vector_id(0x00FF_FFFF_FFFF_FFFF));

        // when
        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        // then
        assert!(encoded1 < encoded2);
        assert!(encoded2 < encoded3);
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
        assert_eq!(encoded.len(), 3);
    }

    #[test]
    fn should_encode_and_decode_centroid_seq_block_key() {
        // given
        let key = CentroidSeqBlockKey;

        // when
        let encoded = key.encode();
        let decoded = CentroidSeqBlockKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 3);
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
    fn should_encode_and_decode_centroid_stats_key() {
        // given
        let key = CentroidStatsKey::new(VectorId::centroid_id(3, 42));

        // when
        let encoded = key.encode();
        let decoded = CentroidStatsKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 11);
    }

    #[test]
    fn should_preserve_centroid_stats_key_ordering() {
        // given
        let key1 = CentroidStatsKey::new(VectorId::centroid_id(1, 1));
        let key2 = CentroidStatsKey::new(VectorId::centroid_id(1, 2));
        let key3 = CentroidStatsKey::new(VectorId::centroid_id(2, 1));

        // when
        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        // then
        assert!(encoded1 < encoded2);
        assert!(encoded2 < encoded3);
    }

    #[test]
    fn should_encode_and_decode_centroid_info_key() {
        // given
        let key = CentroidInfoKey::new(VectorId::centroid_id(1, 123));

        // when
        let encoded = key.encode();
        let decoded = CentroidInfoKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), 11);
    }

    #[test]
    fn should_preserve_centroid_info_key_ordering() {
        // given
        let key1 = CentroidInfoKey::new(VectorId::centroid_id(1, 1));
        let key2 = CentroidInfoKey::new(VectorId::centroid_id(1, 2));
        let key3 = CentroidInfoKey::new(VectorId::centroid_id(1, 0x00FF_FFFF_FFFF_FFFF));

        // when
        let encoded1 = key1.encode();
        let encoded2 = key2.encode();
        let encoded3 = key3.encode();

        // then
        assert!(encoded1 < encoded2);
        assert!(encoded2 < encoded3);
    }

    #[test]
    fn should_reject_wrong_version() {
        // given
        let mut buf = BytesMut::new();
        buf.put_u8(crate::serde::SUBSYSTEM);
        buf.put_u8(0x99); // Wrong version
        buf.put_u8(RecordType::CollectionMeta.tag().as_byte());

        // when
        let result = CollectionMetaKey::decode(&buf);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("version"));
    }
}

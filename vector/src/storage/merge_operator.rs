//! Merge operator for vector database that handles merging of posting lists and metadata indexes.
//!
//! Routes merge operations to the appropriate merge function based on the
//! record type encoded in the key.

use bytes::Bytes;
use common::serde::key_prefix::RecordTag;
use roaring::RoaringTreemap;
use std::io::Cursor;

use crate::serde::posting_list::PostingListValue;
use crate::serde::{EncodingError, RecordType};

/// Merge operator for vector database that handles merging of different record types.
///
/// Currently supports:
/// - PostingList: Unions RoaringTreemaps for centroid assignments
/// - MetadataIndex: Unions RoaringTreemaps for metadata filtering
pub struct VectorDbMergeOperator;

impl common::storage::MergeOperator for VectorDbMergeOperator {
    fn merge(&self, key: &Bytes, existing_value: Option<Bytes>, new_value: Bytes) -> Bytes {
        // If no existing value, just return the new value
        let Some(existing) = existing_value else {
            return new_value;
        };

        // Decode record type from key
        if key.len() < 2 {
            panic!("Invalid key: key length is less than 2 bytes");
        }

        let record_tag =
            RecordTag::from_byte(key[1]).expect("Failed to decode record tag from key");

        let record_type_id = record_tag.record_type();
        let record_type = RecordType::from_id(record_type_id)
            .expect("Failed to get record type from record tag");

        match record_type {
            RecordType::PostingList | RecordType::MetadataIndex => {
                // Both PostingList and MetadataIndex use RoaringTreemap and merge via union
                merge_roaring_treemap(existing, new_value).expect("Failed to merge RoaringTreemap")
            }
            _ => {
                // For other record types (IdDictionary, VectorData, VectorMeta, etc.),
                // just use new value. These should use Put, not Merge, but handle gracefully
                new_value
            }
        }
    }
}

/// Merge two RoaringTreemap values by unioning them.
///
/// Used for:
/// - PostingList: Union vector IDs assigned to a centroid
/// - MetadataIndex: Union vector IDs matching a metadata filter
fn merge_roaring_treemap(existing: Bytes, new_value: Bytes) -> Result<Bytes, EncodingError> {
    // Deserialize both bitmaps
    let existing_bitmap = RoaringTreemap::deserialize_from(Cursor::new(existing.as_ref()))
        .map_err(|e| EncodingError {
            message: format!("Failed to deserialize existing RoaringTreemap: {}", e),
        })?;

    let new_bitmap =
        RoaringTreemap::deserialize_from(Cursor::new(new_value.as_ref())).map_err(|e| {
            EncodingError {
                message: format!("Failed to deserialize new RoaringTreemap: {}", e),
            }
        })?;

    // Union the bitmaps
    let merged = existing_bitmap | new_bitmap;

    // Serialize result using PostingListValue
    let posting_value = PostingListValue::from_treemap(merged);
    posting_value.encode_to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::FieldValue;
    use crate::serde::key::{IdDictionaryKey, MetadataIndexKey, PostingListKey};
    use crate::serde::metadata_index::MetadataIndexValue;
    use common::storage::MergeOperator;
    use rstest::rstest;

    /// Helper to create a test key for PostingList
    fn create_posting_list_key() -> Bytes {
        PostingListKey::new(1).encode()
    }

    /// Helper to create a test key for MetadataIndex
    fn create_metadata_index_key() -> Bytes {
        MetadataIndexKey::new("category", FieldValue::String("shoes".to_string())).encode()
    }

    /// Helper to create a test key for other record types (e.g., IdDictionary)
    fn create_other_record_type_key() -> Bytes {
        IdDictionaryKey::new("vec-1").encode()
    }

    #[rstest]
    #[case(
        vec![1, 2, 3],
        vec![4, 5, 6],
        vec![1, 2, 3, 4, 5, 6],
        "non-overlapping vector IDs"
    )]
    #[case(
        vec![1, 2, 3],
        vec![2, 3, 4],
        vec![1, 2, 3, 4],
        "overlapping vector IDs (union with duplicates)"
    )]
    #[case(
        vec![],
        vec![1, 2, 3],
        vec![1, 2, 3],
        "existing empty, new has IDs"
    )]
    #[case(
        vec![1, 2, 3],
        vec![],
        vec![1, 2, 3],
        "existing has IDs, new empty"
    )]
    #[case(
        vec![],
        vec![],
        vec![],
        "both empty"
    )]
    fn should_merge_posting_list(
        #[case] existing_ids: Vec<u64>,
        #[case] new_ids: Vec<u64>,
        #[case] expected_ids: Vec<u64>,
        #[case] description: &str,
    ) {
        // given
        let mut existing_bitmap = RoaringTreemap::new();
        for id in existing_ids {
            existing_bitmap.insert(id);
        }
        let existing_value = PostingListValue::from_treemap(existing_bitmap)
            .encode_to_bytes()
            .unwrap();

        let mut new_bitmap = RoaringTreemap::new();
        for id in new_ids {
            new_bitmap.insert(id);
        }
        let new_value = PostingListValue::from_treemap(new_bitmap)
            .encode_to_bytes()
            .unwrap();

        // when
        let merged = merge_roaring_treemap(existing_value, new_value).unwrap();
        let decoded = PostingListValue::decode_from_bytes(&merged).unwrap();

        // then
        let mut expected_bitmap = RoaringTreemap::new();
        for id in expected_ids {
            expected_bitmap.insert(id);
        }
        assert_eq!(
            decoded.vector_ids, expected_bitmap,
            "Failed test case: {}",
            description
        );
    }

    #[rstest]
    #[case(
        vec![1, 2, 3],
        vec![4, 5, 6],
        vec![1, 2, 3, 4, 5, 6],
        "non-overlapping vector IDs in metadata index"
    )]
    #[case(
        vec![1, 2, 3],
        vec![2, 3, 4],
        vec![1, 2, 3, 4],
        "overlapping vector IDs in metadata index (union)"
    )]
    fn should_merge_metadata_index(
        #[case] existing_ids: Vec<u64>,
        #[case] new_ids: Vec<u64>,
        #[case] expected_ids: Vec<u64>,
        #[case] description: &str,
    ) {
        // given
        let mut existing_bitmap = RoaringTreemap::new();
        for id in existing_ids {
            existing_bitmap.insert(id);
        }
        let existing_value = MetadataIndexValue::from_treemap(existing_bitmap)
            .encode_to_bytes()
            .unwrap();

        let mut new_bitmap = RoaringTreemap::new();
        for id in new_ids {
            new_bitmap.insert(id);
        }
        let new_value = MetadataIndexValue::from_treemap(new_bitmap)
            .encode_to_bytes()
            .unwrap();

        // when
        let merged = merge_roaring_treemap(existing_value, new_value).unwrap();
        let decoded = MetadataIndexValue::decode_from_bytes(&merged).unwrap();

        // then
        let mut expected_bitmap = RoaringTreemap::new();
        for id in expected_ids {
            expected_bitmap.insert(id);
        }
        assert_eq!(
            decoded.vector_ids, expected_bitmap,
            "Failed test case: {}",
            description
        );
    }

    #[rstest]
    #[case(RecordType::PostingList, create_posting_list_key, "PostingList")]
    #[case(RecordType::MetadataIndex, create_metadata_index_key, "MetadataIndex")]
    fn should_route_to_correct_merge_function(
        #[case] record_type: RecordType,
        #[case] key_fn: fn() -> Bytes,
        #[case] description: &str,
    ) {
        // given
        let operator = VectorDbMergeOperator;
        let key = key_fn();

        let mut existing_bitmap = RoaringTreemap::new();
        existing_bitmap.insert(1);
        existing_bitmap.insert(2);
        let existing_value = PostingListValue::from_treemap(existing_bitmap)
            .encode_to_bytes()
            .unwrap();

        let mut new_bitmap = RoaringTreemap::new();
        new_bitmap.insert(3);
        new_bitmap.insert(4);
        let new_value = PostingListValue::from_treemap(new_bitmap)
            .encode_to_bytes()
            .unwrap();

        // when
        let merged = operator.merge(&key, Some(existing_value.clone()), new_value.clone());

        // then - verify the merge actually happened (union)
        let decoded = PostingListValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(
            decoded.vector_ids.len(),
            4,
            "{} merge should union values",
            description
        );
    }

    #[test]
    fn should_return_new_value_when_no_existing_value() {
        // given
        let operator = VectorDbMergeOperator;
        let key = create_posting_list_key();
        let new_value = Bytes::from(b"new_value".to_vec());

        // when
        let result = operator.merge(&key, None, new_value.clone());

        // then
        assert_eq!(result, new_value);
    }

    #[test]
    fn should_return_new_value_for_other_record_types() {
        // given
        let operator = VectorDbMergeOperator;
        let key = create_other_record_type_key();
        let existing_value = Bytes::from(b"existing".to_vec());
        let new_value = Bytes::from(b"new_value".to_vec());

        // when
        let result = operator.merge(&key, Some(existing_value), new_value.clone());

        // then - should return new_value without merging
        assert_eq!(result, new_value);
    }
}

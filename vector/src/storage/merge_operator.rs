//! Merge operator for vector database that handles merging of posting lists, deletions, and metadata indexes.
//!
//! Routes merge operations to the appropriate merge function based on the
//! record type encoded in the key.

use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::metadata_index::MetadataIndexValue;
use crate::serde::posting_list::merge_batch_posting_list;
use crate::serde::{EncodingError, KEY_VERSION, RecordType, SUBSYSTEM};
use bytes::Bytes;
use common::serde::key_prefix::KeyPrefix;
use slatedb::MergeOperatorError;

/// Merge operator for vector database that handles merging of different record types.
///
/// Currently supports:
/// - Deletions: Unions RoaringTreemaps for deleted vector tracking
/// - PostingList: Deduplicates by id, keeping only the last update per id
/// - MetadataIndex: Merges included/excluded bitmaps with newer writes taking precedence
pub struct VectorDbMergeOperator {
    dimensions: usize,
}

impl VectorDbMergeOperator {
    pub fn new(dimensions: usize) -> Self {
        Self { dimensions }
    }
}

impl slatedb::MergeOperator for VectorDbMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        // Delegate to merge_batch with a single-element slice.
        self.merge_batch(key, existing_value, std::slice::from_ref(&value))
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        let prefix =
            KeyPrefix::from_bytes_with_validation(key, SUBSYSTEM, KEY_VERSION).map_err(|e| {
                MergeOperatorError::Callback {
                    message: e.to_string(),
                }
            })?;
        let record_type =
            RecordType::from_prefix(prefix).map_err(|e| MergeOperatorError::Callback {
                message: e.to_string(),
            })?;

        match record_type {
            RecordType::MetadataIndex => merge_batch_metadata_index(existing_value, operands)
                .map_err(|e| MergeOperatorError::Callback {
                    message: e.to_string(),
                }),
            RecordType::PostingList => Ok(merge_batch_posting_list(
                existing_value,
                operands,
                self.dimensions,
            )),
            RecordType::CentroidStats => Ok(merge_batch_centroid_stats(existing_value, operands)),
            _ => {
                // For other record types (IdDictionary, VectorData, VectorMeta, etc.),
                // just take the last operand. These should use Put, not Merge, but handle
                // gracefully.
                if let Some(last) = operands.last() {
                    Ok(last.clone())
                } else if let Some(existing) = existing_value {
                    Ok(existing)
                } else {
                    Ok(Bytes::new())
                }
            }
        }
    }
}

fn merge_metadata_index_pair(
    newer: MetadataIndexValue,
    older: MetadataIndexValue,
) -> MetadataIndexValue {
    let mut included = newer.included.clone();
    let mut old_included = older.included.clone();
    old_included.difference_with(&newer.excluded);
    included.union_with(&old_included);

    let mut excluded = newer.excluded.clone();
    let mut old_excluded = older.excluded.clone();
    old_excluded.difference_with(&newer.included);
    excluded.union_with(&old_excluded);

    let mut merged = MetadataIndexValue { included, excluded };
    merged.make_mutually_exclusive();
    merged
}

fn merge_batch_metadata_index(
    existing: Option<Bytes>,
    operands: &[Bytes],
) -> Result<Bytes, EncodingError> {
    let mut merged = if let Some(existing) = existing {
        MetadataIndexValue::decode_from_bytes(&existing)?
    } else {
        MetadataIndexValue::new()
    };

    for operand in operands {
        let newer = MetadataIndexValue::decode_from_bytes(operand)?;
        merged = merge_metadata_index_pair(newer, merged);
    }

    merged.encode_to_bytes()
}

/// Batch merge CentroidStats values by summing all i32 deltas at once.
fn merge_batch_centroid_stats(existing: Option<Bytes>, operands: &[Bytes]) -> Bytes {
    let mut total = if let Some(existing) = existing {
        CentroidStatsValue::decode_from_bytes(&existing)
            .expect("Failed to decode existing CentroidStatsValue")
            .num_vectors
    } else {
        0
    };

    for operand in operands {
        total += CentroidStatsValue::decode_from_bytes(operand)
            .expect("Failed to decode operand CentroidStatsValue")
            .num_vectors;
    }

    CentroidStatsValue::new(total).encode_to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::FieldValue;
    use crate::serde::key::{CentroidStatsKey, IdDictionaryKey, MetadataIndexKey, PostingListKey};
    use crate::serde::metadata_index::MetadataIndexValue;
    use crate::serde::posting_list::{PostingListValue, PostingUpdate};
    use crate::serde::vector_id::VectorId;
    use roaring::RoaringTreemap;
    use rstest::rstest;
    use slatedb::MergeOperator;

    /// Helper to create a test key for PostingList
    fn create_posting_list_key() -> Bytes {
        PostingListKey::new(VectorId::centroid_id(1, 1)).encode()
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
        let merged = merge_batch_metadata_index(Some(existing_value), &[new_value]).unwrap();
        let decoded = MetadataIndexValue::decode_from_bytes(&merged).unwrap();

        // then
        let mut expected_bitmap = RoaringTreemap::new();
        for id in expected_ids {
            expected_bitmap.insert(id);
        }
        assert_eq!(
            decoded.effective_vector_ids(),
            expected_bitmap,
            "Failed test case: {}",
            description
        );
    }

    #[test]
    fn should_route_metadata_index_to_roaring_treemap_merge() {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = create_metadata_index_key();

        let mut existing_bitmap = RoaringTreemap::new();
        existing_bitmap.insert(1);
        existing_bitmap.insert(2);
        let existing_value = MetadataIndexValue::from_treemap(existing_bitmap)
            .encode_to_bytes()
            .unwrap();

        let mut new_bitmap = RoaringTreemap::new();
        new_bitmap.insert(3);
        new_bitmap.insert(4);
        let new_value = MetadataIndexValue::from_treemap(new_bitmap)
            .encode_to_bytes()
            .unwrap();

        // when
        let merged = operator
            .merge_batch(&key, Some(existing_value), &[new_value])
            .unwrap();

        // then - verify the merge actually happened (union)
        let decoded = MetadataIndexValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.len(), 4);
    }

    #[test]
    fn should_route_posting_list_to_deduplication_merge() {
        // given
        let operator = VectorDbMergeOperator::new(2);
        let key = create_posting_list_key();

        let existing_postings = vec![PostingUpdate::append(
            VectorId::data_vector_id(1),
            vec![1.0, 2.0],
        )];
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_postings = vec![PostingUpdate::append(
            VectorId::data_vector_id(2),
            vec![3.0, 4.0],
        )];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = operator
            .merge_batch(&key, Some(existing_value), &[new_value])
            .unwrap();

        // then - verify the merge produced deduplicated result
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();
        assert_eq!(decoded.len(), 2);
    }

    #[test]
    fn should_return_new_value_when_no_existing_value() {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = create_posting_list_key();
        let new_value = Bytes::from(b"new_value".to_vec());

        // when
        let result = operator
            .merge_batch(&key, None, std::slice::from_ref(&new_value))
            .unwrap();

        // then
        assert_eq!(result, new_value);
    }

    #[rstest]
    #[case(10, 5, 15, "positive + positive")]
    #[case(10, -3, 7, "positive + negative")]
    #[case(-5, -3, -8, "negative + negative")]
    #[case(0, 5, 5, "zero + positive")]
    #[case(5, 0, 5, "positive + zero")]
    #[case(0, 0, 0, "zero + zero")]
    fn should_merge_centroid_stats(
        #[case] existing_count: i32,
        #[case] new_count: i32,
        #[case] expected_count: i32,
        #[case] description: &str,
    ) {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = CentroidStatsKey::new(VectorId::centroid_id(1, 1)).encode();
        let existing_value = CentroidStatsValue::new(existing_count).encode_to_bytes();
        let new_value = CentroidStatsValue::new(new_count).encode_to_bytes();

        // when
        let merged = operator
            .merge_batch(&key, Some(existing_value), &[new_value])
            .unwrap();

        // then
        let decoded = CentroidStatsValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(
            decoded.num_vectors, expected_count,
            "Failed test case: {}",
            description
        );
    }

    #[test]
    fn should_batch_merge_centroid_stats_no_existing() {
        // given
        let op0 = CentroidStatsValue::new(3).encode_to_bytes();
        let op1 = CentroidStatsValue::new(5).encode_to_bytes();
        let op2 = CentroidStatsValue::new(-2).encode_to_bytes();

        // when
        let merged = merge_batch_centroid_stats(None, &[op0, op1, op2]);

        // then
        let decoded = CentroidStatsValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.num_vectors, 6);
    }

    #[test]
    fn should_batch_merge_centroid_stats_with_existing() {
        // given
        let existing = CentroidStatsValue::new(10).encode_to_bytes();
        let op0 = CentroidStatsValue::new(3).encode_to_bytes();
        let op1 = CentroidStatsValue::new(-7).encode_to_bytes();

        // when
        let merged = merge_batch_centroid_stats(Some(existing), &[op0, op1]);

        // then
        let decoded = CentroidStatsValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.num_vectors, 6);
    }

    #[test]
    fn should_route_merge_batch_centroid_stats() {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = CentroidStatsKey::new(VectorId::centroid_id(1, 1)).encode();
        let existing = CentroidStatsValue::new(10).encode_to_bytes();
        let op0 = CentroidStatsValue::new(5).encode_to_bytes();
        let op1 = CentroidStatsValue::new(-3).encode_to_bytes();

        // when
        let merged = operator
            .merge_batch(&key, Some(existing), &[op0, op1])
            .unwrap();

        // then
        let decoded = CentroidStatsValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.num_vectors, 12);
    }

    #[test]
    fn should_return_new_value_for_other_record_types() {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = create_other_record_type_key();
        let existing_value = Bytes::from(b"existing".to_vec());
        let new_value = Bytes::from(b"new_value".to_vec());

        // when
        let result = operator
            .merge_batch(&key, Some(existing_value), std::slice::from_ref(&new_value))
            .unwrap();

        // then - should return new_value without merging
        assert_eq!(result, new_value);
    }

    #[test]
    fn should_route_merge_batch_metadata_index_to_batch_treemap() {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = create_metadata_index_key();

        let existing = MetadataIndexValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(1);
            bm
        })
        .encode_to_bytes()
        .unwrap();
        let op0 = MetadataIndexValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(2);
            bm
        })
        .encode_to_bytes()
        .unwrap();
        let op1 = MetadataIndexValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(3);
            bm
        })
        .encode_to_bytes()
        .unwrap();

        // when
        let merged = operator
            .merge_batch(&key, Some(existing), &[op0, op1])
            .unwrap();

        // then
        let mut expected = RoaringTreemap::new();
        for id in [1, 2, 3] {
            expected.insert(id);
        }
        let decoded = MetadataIndexValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.effective_vector_ids(), expected);
    }

    #[test]
    fn should_merge_metadata_index_with_newer_exclusions_taking_precedence() {
        // given
        let older = MetadataIndexValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(1);
            bm.insert(2);
            bm
        });
        let mut newer = MetadataIndexValue::new();
        newer.exclude_vector(2);
        newer.include_vector(3);

        // when
        let merged = merge_batch_metadata_index(
            Some(older.encode_to_bytes().unwrap()),
            &[newer.encode_to_bytes().unwrap()],
        )
        .unwrap();
        let decoded = MetadataIndexValue::decode_from_bytes(&merged).unwrap();

        // then
        let mut expected = RoaringTreemap::new();
        expected.insert(1);
        expected.insert(3);
        assert_eq!(decoded.effective_vector_ids(), expected);
        assert!(decoded.excluded.contains(2));
    }
}

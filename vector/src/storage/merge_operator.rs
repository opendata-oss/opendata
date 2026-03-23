//! Merge operator for vector database that handles merging of posting lists, deletions, and metadata indexes.
//!
//! Routes merge operations to the appropriate merge function based on the
//! record type encoded in the key.

use crate::serde::centroid_chunk::CentroidChunkValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::posting_list::merge_batch_posting_list;
use crate::serde::{EncodingError, KEY_VERSION, RecordType, SUBSYSTEM};
use bytes::Bytes;
use common::serde::key_prefix::KeyPrefix;
use common::storage::default_merge_batch;
use roaring::RoaringTreemap;
use std::io::Cursor;

/// Merge operator for vector database that handles merging of different record types.
///
/// Currently supports:
/// - Deletions: Unions RoaringTreemaps for deleted vector tracking
/// - PostingList: Deduplicates by id, keeping only the last update per id
/// - MetadataIndex: Unions RoaringTreemaps for metadata filtering
pub struct VectorDbMergeOperator {
    dimensions: usize,
}

impl VectorDbMergeOperator {
    pub fn new(dimensions: usize) -> Self {
        Self { dimensions }
    }
}

impl common::storage::MergeOperator for VectorDbMergeOperator {
    fn merge(&self, key: &Bytes, existing_value: Option<Bytes>, new_value: Bytes) -> Bytes {
        self.merge_batch(key, existing_value, &[new_value])
    }

    fn merge_batch(&self, key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
        let prefix = KeyPrefix::from_bytes_with_validation(key, SUBSYSTEM, KEY_VERSION)
            .expect("Failed to decode key prefix");
        let record_type =
            RecordType::from_prefix(prefix).expect("Failed to get record type from record tag");

        match record_type {
            RecordType::Deletions | RecordType::MetadataIndex => {
                // Deletions and MetadataIndex use RoaringTreemap and merge via union
                merge_batch_roaring_treemap(existing_value, operands)
                    .expect("Failed to batch merge RoaringTreemap")
            }
            RecordType::PostingList => {
                merge_batch_posting_list(existing_value, operands, self.dimensions)
            }
            RecordType::CentroidStats => merge_batch_centroid_stats(existing_value, operands),
            RecordType::CentroidChunk => {
                merge_batch_centroid_chunk(existing_value, operands, self.dimensions)
            }
            _ => {
                // For other record types (IdDictionary, VectorData, VectorMeta, etc.), just use new value
                // for each pairwise merge. These should use Put, not Merge, but handle gracefully
                default_merge_batch(key, existing_value, operands, |_k, _e, v| v)
            }
        }
    }
}

/// Batch merge RoaringTreemap values by unioning all treemaps at once.
///
/// Used for:
/// - Deletions: Union deleted vector IDs
/// - MetadataIndex: Union vector IDs matching a metadata filter
fn merge_batch_roaring_treemap(
    existing: Option<Bytes>,
    operands: &[Bytes],
) -> Result<Bytes, EncodingError> {
    let mut merged = if let Some(existing) = existing {
        RoaringTreemap::deserialize_from(Cursor::new(existing.as_ref())).map_err(|e| {
            EncodingError {
                message: format!("Failed to deserialize existing RoaringTreemap: {}", e),
            }
        })?
    } else {
        RoaringTreemap::new()
    };

    for operand in operands {
        let bitmap =
            RoaringTreemap::deserialize_from(Cursor::new(operand.as_ref())).map_err(|e| {
                EncodingError {
                    message: format!("Failed to deserialize operand RoaringTreemap: {}", e),
                }
            })?;
        merged |= bitmap;
    }

    let mut buf = Vec::new();
    merged.serialize_into(&mut buf).map_err(|e| EncodingError {
        message: format!("Failed to serialize merged RoaringTreemap: {}", e),
    })?;
    Ok(Bytes::from(buf))
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

/// Batch merge CentroidChunk values by appending entries from all operands.
fn merge_batch_centroid_chunk(
    existing: Option<Bytes>,
    operands: &[Bytes],
    dimensions: usize,
) -> Bytes {
    let mut entries = if let Some(existing) = existing {
        CentroidChunkValue::decode_from_bytes(&existing, dimensions)
            .expect("Failed to decode existing CentroidChunkValue")
            .entries
    } else {
        Vec::new()
    };

    for operand in operands {
        let chunk = CentroidChunkValue::decode_from_bytes(operand, dimensions)
            .expect("Failed to decode operand CentroidChunkValue");
        entries.extend(chunk.entries);
    }

    CentroidChunkValue::new(entries).encode_to_bytes(dimensions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::FieldValue;
    use crate::serde::centroid_chunk::CentroidEntry;
    use crate::serde::deletions::DeletionsValue;
    use crate::serde::key::{
        CentroidChunkKey, CentroidStatsKey, DeletionsKey, IdDictionaryKey, MetadataIndexKey,
        PostingListKey,
    };
    use crate::serde::metadata_index::MetadataIndexValue;
    use crate::serde::posting_list::{PostingListValue, PostingUpdate};
    use common::storage::MergeOperator;
    use rstest::rstest;

    /// Helper to create a test key for Deletions
    fn create_deletions_key() -> Bytes {
        DeletionsKey::new().encode()
    }

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
    fn should_merge_deletions(
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
        let existing_value = DeletionsValue::from_treemap(existing_bitmap)
            .encode_to_bytes()
            .unwrap();

        let mut new_bitmap = RoaringTreemap::new();
        for id in new_ids {
            new_bitmap.insert(id);
        }
        let new_value = DeletionsValue::from_treemap(new_bitmap)
            .encode_to_bytes()
            .unwrap();

        // when
        let merged = merge_batch_roaring_treemap(Some(existing_value), &[new_value]).unwrap();
        let decoded = DeletionsValue::decode_from_bytes(&merged).unwrap();

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
        let merged = merge_batch_roaring_treemap(Some(existing_value), &[new_value]).unwrap();
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

    #[test]
    fn should_route_deletions_to_roaring_treemap_merge() {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = create_deletions_key();

        let mut existing_bitmap = RoaringTreemap::new();
        existing_bitmap.insert(1);
        existing_bitmap.insert(2);
        let existing_value = DeletionsValue::from_treemap(existing_bitmap)
            .encode_to_bytes()
            .unwrap();

        let mut new_bitmap = RoaringTreemap::new();
        new_bitmap.insert(3);
        new_bitmap.insert(4);
        let new_value = DeletionsValue::from_treemap(new_bitmap)
            .encode_to_bytes()
            .unwrap();

        // when
        let merged = operator.merge(&key, Some(existing_value), new_value);

        // then - verify the merge actually happened (union)
        let decoded = DeletionsValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.vector_ids.len(), 4);
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
        let merged = operator.merge(&key, Some(existing_value), new_value);

        // then - verify the merge actually happened (union)
        let decoded = MetadataIndexValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.vector_ids.len(), 4);
    }

    #[test]
    fn should_route_posting_list_to_deduplication_merge() {
        // given
        let operator = VectorDbMergeOperator::new(2);
        let key = create_posting_list_key();

        let existing_postings = vec![PostingUpdate::append(1, vec![1.0, 2.0])];
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_postings = vec![PostingUpdate::append(2, vec![3.0, 4.0])];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = operator.merge(&key, Some(existing_value), new_value);

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
        let result = operator.merge(&key, None, new_value.clone());

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
        let key = CentroidStatsKey::new(1).encode();
        let existing_value = CentroidStatsValue::new(existing_count).encode_to_bytes();
        let new_value = CentroidStatsValue::new(new_count).encode_to_bytes();

        // when
        let merged = operator.merge(&key, Some(existing_value), new_value);

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
        let key = CentroidStatsKey::new(1).encode();
        let existing = CentroidStatsValue::new(10).encode_to_bytes();
        let op0 = CentroidStatsValue::new(5).encode_to_bytes();
        let op1 = CentroidStatsValue::new(-3).encode_to_bytes();

        // when
        let merged = operator.merge_batch(&key, Some(existing), &[op0, op1]);

        // then
        let decoded = CentroidStatsValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.num_vectors, 12);
    }

    #[test]
    fn should_batch_merge_centroid_chunk_no_existing() {
        // given
        let dimensions = 2;
        let op0 = CentroidChunkValue::new(vec![CentroidEntry::new(1, vec![1.0, 2.0])])
            .encode_to_bytes(dimensions);
        let op1 = CentroidChunkValue::new(vec![
            CentroidEntry::new(2, vec![3.0, 4.0]),
            CentroidEntry::new(3, vec![5.0, 6.0]),
        ])
        .encode_to_bytes(dimensions);

        // when
        let merged = merge_batch_centroid_chunk(None, &[op0, op1], dimensions);

        // then
        let decoded = CentroidChunkValue::decode_from_bytes(&merged, dimensions).unwrap();
        assert_eq!(decoded.entries.len(), 3);
        assert_eq!(decoded.entries[0].centroid_id, 1);
        assert_eq!(decoded.entries[1].centroid_id, 2);
        assert_eq!(decoded.entries[2].centroid_id, 3);
        assert_eq!(decoded.entries[0].vector, vec![1.0, 2.0]);
        assert_eq!(decoded.entries[1].vector, vec![3.0, 4.0]);
        assert_eq!(decoded.entries[2].vector, vec![5.0, 6.0]);
    }

    #[test]
    fn should_batch_merge_centroid_chunk_with_existing() {
        // given
        let dimensions = 2;
        let existing = CentroidChunkValue::new(vec![CentroidEntry::new(1, vec![1.0, 2.0])])
            .encode_to_bytes(dimensions);
        let op0 = CentroidChunkValue::new(vec![CentroidEntry::new(2, vec![3.0, 4.0])])
            .encode_to_bytes(dimensions);
        let op1 = CentroidChunkValue::new(vec![CentroidEntry::new(3, vec![5.0, 6.0])])
            .encode_to_bytes(dimensions);

        // when
        let merged = merge_batch_centroid_chunk(Some(existing), &[op0, op1], dimensions);

        // then
        let decoded = CentroidChunkValue::decode_from_bytes(&merged, dimensions).unwrap();
        assert_eq!(decoded.entries.len(), 3);
        assert_eq!(decoded.entries[0].centroid_id, 1);
        assert_eq!(decoded.entries[1].centroid_id, 2);
        assert_eq!(decoded.entries[2].centroid_id, 3);
        assert_eq!(decoded.entries[0].vector, vec![1.0, 2.0]);
        assert_eq!(decoded.entries[1].vector, vec![3.0, 4.0]);
        assert_eq!(decoded.entries[2].vector, vec![5.0, 6.0]);
    }

    #[test]
    fn should_route_merge_batch_centroid_chunk() {
        // given
        let dimensions = 2;
        let operator = VectorDbMergeOperator::new(dimensions);
        let key = CentroidChunkKey::new(1).encode();
        let existing = CentroidChunkValue::new(vec![CentroidEntry::new(1, vec![1.0, 2.0])])
            .encode_to_bytes(dimensions);
        let op0 = CentroidChunkValue::new(vec![CentroidEntry::new(2, vec![3.0, 4.0])])
            .encode_to_bytes(dimensions);

        // when
        let merged = operator.merge_batch(&key, Some(existing), &[op0]);

        // then
        let decoded = CentroidChunkValue::decode_from_bytes(&merged, dimensions).unwrap();
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].centroid_id, 1);
        assert_eq!(decoded.entries[1].centroid_id, 2);
        assert_eq!(decoded.entries[0].vector, vec![1.0, 2.0]);
        assert_eq!(decoded.entries[1].vector, vec![3.0, 4.0]);
    }

    #[test]
    fn should_return_new_value_for_other_record_types() {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = create_other_record_type_key();
        let existing_value = Bytes::from(b"existing".to_vec());
        let new_value = Bytes::from(b"new_value".to_vec());

        // when
        let result = operator.merge(&key, Some(existing_value), new_value.clone());

        // then - should return new_value without merging
        assert_eq!(result, new_value);
    }

    #[test]
    fn should_batch_merge_deletions() {
        // given
        let op0 = DeletionsValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(1);
            bm.insert(2);
            bm
        })
        .encode_to_bytes()
        .unwrap();
        let op1 = DeletionsValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(2);
            bm.insert(3);
            bm
        })
        .encode_to_bytes()
        .unwrap();
        let op2 = DeletionsValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(4);
            bm.insert(5);
            bm
        })
        .encode_to_bytes()
        .unwrap();

        // when - no existing value
        let merged = merge_batch_roaring_treemap(None, &[op0, op1, op2]).unwrap();
        let decoded = DeletionsValue::decode_from_bytes(&merged).unwrap();

        // then - union of all bitmaps
        let mut expected = RoaringTreemap::new();
        for id in [1, 2, 3, 4, 5] {
            expected.insert(id);
        }
        assert_eq!(decoded.vector_ids, expected);
    }

    #[test]
    fn should_batch_merge_deletions_with_existing() {
        // given
        let existing = DeletionsValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(10);
            bm
        })
        .encode_to_bytes()
        .unwrap();
        let op0 = DeletionsValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(1);
            bm.insert(10);
            bm
        })
        .encode_to_bytes()
        .unwrap();

        // when
        let merged = merge_batch_roaring_treemap(Some(existing), &[op0]).unwrap();
        let decoded = DeletionsValue::decode_from_bytes(&merged).unwrap();

        // then
        let mut expected = RoaringTreemap::new();
        for id in [1, 10] {
            expected.insert(id);
        }
        assert_eq!(decoded.vector_ids, expected);
    }

    #[test]
    fn should_route_merge_batch_deletions_to_batch_treemap() {
        // given
        let operator = VectorDbMergeOperator::new(3);
        let key = create_deletions_key();

        let existing = DeletionsValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(1);
            bm
        })
        .encode_to_bytes()
        .unwrap();
        let op0 = DeletionsValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(2);
            bm
        })
        .encode_to_bytes()
        .unwrap();
        let op1 = DeletionsValue::from_treemap({
            let mut bm = RoaringTreemap::new();
            bm.insert(3);
            bm
        })
        .encode_to_bytes()
        .unwrap();

        // when
        let merged = operator.merge_batch(&key, Some(existing), &[op0, op1]);

        // then
        let mut expected = RoaringTreemap::new();
        for id in [1, 2, 3] {
            expected.insert(id);
        }
        let decoded = DeletionsValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.vector_ids, expected);
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
        let merged = operator.merge_batch(&key, Some(existing), &[op0, op1]);

        // then
        let mut expected = RoaringTreemap::new();
        for id in [1, 2, 3] {
            expected.insert(id);
        }
        let decoded = MetadataIndexValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.vector_ids, expected);
    }
}

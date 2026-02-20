//! Merge operator for vector database that handles merging of posting lists, deletions, and metadata indexes.
//!
//! Routes merge operations to the appropriate merge function based on the
//! record type encoded in the key.

use bytes::Bytes;
use common::serde::key_prefix::KeyPrefix;
use roaring::RoaringTreemap;
use std::io::Cursor;

use crate::serde::centroid_chunk::CentroidChunkValue;
use crate::serde::centroid_stats::CentroidStatsValue;
use crate::serde::posting_list::{merge_batch_posting_list, merge_posting_list};
use crate::serde::vector_indexed_metadata::{
    VectorIndexedMetadataMergeValue, VectorIndexedMetadataValue,
};
use crate::serde::{EncodingError, KEY_VERSION, RecordType};

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
        let prefix =
            KeyPrefix::from_bytes_versioned(key, KEY_VERSION).expect("Failed to decode key prefix");
        let record_tag = prefix.tag();
        let record_type_id = record_tag.record_type();
        let record_type =
            RecordType::from_id(record_type_id).expect("Failed to get record type from record tag");

        // VectorIndexedMetadata uses different formats for put (value) vs merge (delta),
        // so we must handle the None case explicitly rather than returning new_value as-is.
        if matches!(record_type, RecordType::VectorIndexedMetadata) {
            return merge_vector_indexed_metadata(existing_value, new_value);
        }

        // For all other record types, the merge format matches the value format
        let Some(existing) = existing_value else {
            return new_value;
        };

        match record_type {
            RecordType::Deletions | RecordType::MetadataIndex => {
                // Deletions and MetadataIndex use RoaringTreemap and merge via union
                merge_roaring_treemap(existing, new_value).expect("Failed to merge RoaringTreemap")
            }
            RecordType::PostingList => {
                // PostingList deduplicates by id, keeping only the last update per id
                merge_posting_list(existing, new_value, self.dimensions)
            }
            RecordType::CentroidStats => {
                // CentroidStats sums i32 deltas
                merge_centroid_stats(existing, new_value)
            }
            RecordType::CentroidChunk => {
                // CentroidChunk appends new entries to existing chunk
                merge_centroid_chunk(existing, new_value, self.dimensions)
            }
            _ => {
                // For other record types (IdDictionary, VectorData, VectorMeta, etc.),
                // just use new value. These should use Put, not Merge, but handle gracefully
                new_value
            }
        }
    }

    fn merge_batch(&self, key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
        let prefix =
            KeyPrefix::from_bytes_versioned(key, KEY_VERSION).expect("Failed to decode key prefix");
        let record_type = RecordType::from_id(prefix.tag().record_type())
            .expect("Failed to get record type from record tag");

        match record_type {
            RecordType::PostingList => {
                merge_batch_posting_list(existing_value, operands, self.dimensions)
            }
            _ => {
                // Default pairwise for all other record types
                let mut result = existing_value;
                for operand in operands {
                    result = Some(self.merge(key, result, operand.clone()));
                }
                result.expect("merge_batch called with no existing value and no operands")
            }
        }
    }
}

/// Merge two RoaringTreemap values by unioning them.
///
/// Used for:
/// - Deletions: Union deleted vector IDs
/// - MetadataIndex: Union vector IDs matching a metadata filter
#[allow(dead_code)]
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

    // Serialize result
    let mut buf = Vec::new();
    merged.serialize_into(&mut buf).map_err(|e| EncodingError {
        message: format!("Failed to serialize merged RoaringTreemap: {}", e),
    })?;
    Ok(Bytes::from(buf))
}

/// Merge two CentroidChunk values by appending entries from new to existing.
fn merge_centroid_chunk(existing: Bytes, new_value: Bytes, dimensions: usize) -> Bytes {
    let existing_chunk = CentroidChunkValue::decode_from_bytes(&existing, dimensions)
        .expect("Failed to decode existing CentroidChunkValue");
    let new_chunk = CentroidChunkValue::decode_from_bytes(&new_value, dimensions)
        .expect("Failed to decode new CentroidChunkValue");
    let mut combined_entries = existing_chunk.entries;
    combined_entries.extend(new_chunk.entries);
    CentroidChunkValue::new(combined_entries).encode_to_bytes(dimensions)
}

/// Merge a VectorIndexedMetadata value with either a merge delta or a full replacement.
///
/// The `new_value` can be one of two formats:
/// - **Merge delta** (VectorIndexedMetadataMergeValue): `num_removals:u32 + removals + num_additions:u32 + additions`
/// - **Full value** (VectorIndexedMetadataValue): `count:u32 + centroid_ids`
///
/// This dual-format handling is needed because SlateDB's batch processing may pass
/// Put values as merge operands when both Put and Merge operations for the same key
/// appear in the same batch. The two formats are unambiguously distinguishable by size:
/// Put = `4 + C*8` bytes, Merge = `4 + R*8 + 4 + A*8` bytes. These can never be equal
/// for the same first u32 value (since that would require `4 + A*8 = 0`).
fn merge_vector_indexed_metadata(existing: Option<Bytes>, new_value: Bytes) -> Bytes {
    // Detect whether new_value is a full value (Put format) or a merge delta.
    let is_put_format = if new_value.len() >= 4 {
        let first_u32 =
            u32::from_le_bytes([new_value[0], new_value[1], new_value[2], new_value[3]]) as usize;
        new_value.len() == 4 + first_u32 * 8
    } else {
        false
    };

    if is_put_format {
        // Full value replacement — the Put supersedes any existing value.
        new_value
    } else {
        // Merge delta — apply removals/additions to existing value.
        let mut current = match existing {
            Some(bytes) => VectorIndexedMetadataValue::decode_from_bytes(&bytes)
                .expect("Failed to decode existing VectorIndexedMetadataValue"),
            None => VectorIndexedMetadataValue::new(vec![]),
        };
        let delta = VectorIndexedMetadataMergeValue::decode_from_bytes(&new_value)
            .expect("Failed to decode VectorIndexedMetadataMergeValue");
        current.apply_merge(&delta);
        current.encode_to_bytes()
    }
}

/// Merge two CentroidStats values by summing their i32 deltas.
fn merge_centroid_stats(existing: Bytes, new_value: Bytes) -> Bytes {
    let existing_stats = CentroidStatsValue::decode_from_bytes(&existing)
        .expect("Failed to decode existing CentroidStatsValue");
    let new_stats = CentroidStatsValue::decode_from_bytes(&new_value)
        .expect("Failed to decode new CentroidStatsValue");
    let merged = CentroidStatsValue::new(existing_stats.num_vectors + new_stats.num_vectors);
    merged.encode_to_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::FieldValue;
    use crate::serde::deletions::DeletionsValue;
    use crate::serde::key::{
        CentroidStatsKey, DeletionsKey, IdDictionaryKey, MetadataIndexKey, PostingListKey,
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
        let merged = merge_roaring_treemap(existing_value, new_value).unwrap();
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

    // ---- VectorIndexedMetadata merge tests ----

    use crate::serde::key::VectorIndexedMetadataKey;

    /// Helper to create a test key for VectorIndexedMetadata
    fn create_vector_indexed_metadata_key() -> Bytes {
        VectorIndexedMetadataKey::new(1).encode()
    }

    #[test]
    fn should_merge_vector_indexed_metadata_delta_with_existing() {
        // given — existing value with centroids [1, 2, 3], merge removes 2 and adds 4
        let operator = VectorDbMergeOperator::new(3);
        let key = create_vector_indexed_metadata_key();
        let existing = VectorIndexedMetadataValue::new(vec![1, 2, 3]).encode_to_bytes();
        let delta = VectorIndexedMetadataMergeValue::new(vec![2], vec![4]).encode_to_bytes();

        // when
        let merged = operator.merge(&key, Some(existing), delta);

        // then
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.centroid_ids, vec![1, 3, 4]);
    }

    #[test]
    fn should_merge_vector_indexed_metadata_delta_without_existing() {
        // given — no existing value, merge adds centroids [5, 6]
        let operator = VectorDbMergeOperator::new(3);
        let key = create_vector_indexed_metadata_key();
        let delta = VectorIndexedMetadataMergeValue::new(vec![], vec![5, 6]).encode_to_bytes();

        // when
        let merged = operator.merge(&key, None, delta);

        // then
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.centroid_ids, vec![5, 6]);
    }

    #[test]
    fn should_treat_put_value_as_full_replacement_in_merge() {
        // given — existing value with centroids [1, 2, 3], but new_value is a Put (full value)
        // This happens when SlateDB's batch processing passes a Put as a merge operand.
        let operator = VectorDbMergeOperator::new(3);
        let key = create_vector_indexed_metadata_key();
        let existing = VectorIndexedMetadataValue::new(vec![1, 2, 3]).encode_to_bytes();
        let put_value = VectorIndexedMetadataValue::new(vec![10, 20]).encode_to_bytes();

        // when
        let merged = operator.merge(&key, Some(existing), put_value.clone());

        // then — the Put value should completely replace the existing value
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.centroid_ids, vec![10, 20]);
        assert_eq!(merged, put_value);
    }

    #[test]
    fn should_treat_put_value_as_full_replacement_without_existing() {
        // given — no existing value, new_value is a Put
        let operator = VectorDbMergeOperator::new(3);
        let key = create_vector_indexed_metadata_key();
        let put_value = VectorIndexedMetadataValue::new(vec![7, 8, 9]).encode_to_bytes();

        // when
        let merged = operator.merge(&key, None, put_value.clone());

        // then
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.centroid_ids, vec![7, 8, 9]);
        assert_eq!(merged, put_value);
    }

    #[test]
    fn should_handle_empty_put_value_in_merge() {
        // given — empty Put value (0 centroids) passed as merge operand
        let operator = VectorDbMergeOperator::new(3);
        let key = create_vector_indexed_metadata_key();
        let existing = VectorIndexedMetadataValue::new(vec![1, 2]).encode_to_bytes();
        let empty_put = VectorIndexedMetadataValue::new(vec![]).encode_to_bytes();

        // when
        let merged = operator.merge(&key, Some(existing), empty_put);

        // then — empty Put replaces existing
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.centroid_ids, Vec::<u64>::new());
    }

    #[test]
    fn should_route_posting_list_to_batch_merge() {
        // given - 2 operands with overlapping IDs
        let operator = VectorDbMergeOperator::new(2);
        let key = create_posting_list_key();

        let op0 = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(2, vec![3.0, 4.0]),
        ])
        .unwrap()
        .encode_to_bytes();
        let op1 = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(2, vec![30.0, 40.0]),
            PostingUpdate::append(3, vec![5.0, 6.0]),
        ])
        .unwrap()
        .encode_to_bytes();

        // when
        let merged = operator.merge_batch(&key, None, &[op0, op1]);

        // then
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();
        let entries: Vec<_> = decoded.iter().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].id(), 1);
        assert_eq!(entries[1].id(), 2);
        assert_eq!(entries[1].vector().unwrap(), &[30.0, 40.0]); // newer wins
        assert_eq!(entries[2].id(), 3);
    }

    #[test]
    fn should_handle_single_centroid_put_value_in_merge() {
        // given — single-centroid Put value (4 + 1*8 = 12 bytes)
        let operator = VectorDbMergeOperator::new(3);
        let key = create_vector_indexed_metadata_key();
        let existing = VectorIndexedMetadataValue::new(vec![1, 2, 3]).encode_to_bytes();
        let put_value = VectorIndexedMetadataValue::new(vec![42]).encode_to_bytes();

        // when
        let merged = operator.merge(&key, Some(existing), put_value);

        // then
        let decoded = VectorIndexedMetadataValue::decode_from_bytes(&merged).unwrap();
        assert_eq!(decoded.centroid_ids, vec![42]);
    }
}

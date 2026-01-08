#![allow(dead_code)]

use bytes::Bytes;

use crate::model::RecordTag;
use crate::serde::bucket_list::BucketListValue;
use crate::serde::inverted_index::InvertedIndexValue;
use crate::serde::timeseries::merge_time_series;
use crate::serde::{EncodingError, RecordType};

/// Merge operator for OpenTSDB that handles merging of different record types.
///
/// Routes merge operations to the appropriate merge function based on the
/// record type encoded in the key.
pub(crate) struct OpenTsdbMergeOperator;

impl common::storage::MergeOperator for OpenTsdbMergeOperator {
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

        let record_type = record_tag
            .record_type()
            .expect("Failed to get record type from record tag");

        match record_type {
            RecordType::InvertedIndex => {
                merge_inverted_index(existing, new_value).expect("Failed to merge inverted index")
            }
            RecordType::TimeSeries => {
                merge_time_series(existing, new_value).expect("Failed to merge time series")
            }
            RecordType::BucketList => {
                merge_bucket_list(existing, new_value).expect("Failed to merge bucket list")
            }
            _ => {
                // For other record types (SeriesDictionary, ForwardIndex), just use new value
                // These should use Put, not Merge, but handle gracefully
                new_value
            }
        }
    }
}

/// Merge inverted index posting lists by unioning RoaringBitmaps.
fn merge_inverted_index(existing: Bytes, new_value: Bytes) -> Result<Bytes, EncodingError> {
    let existing_bitmap = InvertedIndexValue::decode(existing.as_ref())?.postings;
    let new_bitmap = InvertedIndexValue::decode(new_value.as_ref())?.postings;

    let merged = existing_bitmap | new_bitmap;
    (InvertedIndexValue { postings: merged }).encode()
}

/// Merge bucket lists by taking the union of buckets and sorting.
/// Used by the merge operator to handle concurrent updates to the BucketsList.
fn merge_bucket_list(existing: Bytes, new_value: Bytes) -> Result<Bytes, EncodingError> {
    let mut base_buckets = BucketListValue::decode(existing.as_ref())?.buckets;
    let other_buckets = BucketListValue::decode(new_value.as_ref())?.buckets;

    // Add buckets from other that aren't in base
    for bucket in other_buckets {
        if !base_buckets.contains(&bucket) {
            base_buckets.push(bucket);
        }
    }

    // Sort by start time
    base_buckets.sort_by_key(|b| b.1);

    Ok(BucketListValue {
        buckets: base_buckets,
    }
    .encode())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Sample;
    use crate::serde::key::{BucketListKey, InvertedIndexKey, TimeSeriesKey};
    use crate::serde::timeseries::TimeSeriesValue;
    use bytes::Bytes;
    use common::storage::MergeOperator;
    use roaring::RoaringBitmap;
    use rstest::rstest;

    /// Helper to create a test key for InvertedIndex
    fn create_inverted_index_key() -> Bytes {
        InvertedIndexKey {
            time_bucket: 1000,
            bucket_size: 1,
            attribute: "env".to_string(),
            value: "prod".to_string(),
        }
        .encode()
    }

    /// Helper to create a test key for TimeSeries
    fn create_time_series_key() -> Bytes {
        TimeSeriesKey {
            time_bucket: 1000,
            bucket_size: 1,
            series_id: 42,
        }
        .encode()
    }

    /// Helper to create a test key for BucketList
    fn create_bucket_list_key() -> Bytes {
        BucketListKey.encode()
    }

    /// Helper to create a test key for other record types (e.g., SeriesDictionary)
    fn create_other_record_type_key() -> Bytes {
        use crate::serde::key::SeriesDictionaryKey;
        SeriesDictionaryKey {
            time_bucket: 1000,
            bucket_size: 1,
            series_fingerprint: 123,
        }
        .encode()
    }

    #[rstest]
    #[case(
        vec![1, 2, 3],
        vec![4, 5, 6],
        vec![1, 2, 3, 4, 5, 6],
        "non-overlapping series IDs"
    )]
    #[case(
        vec![1, 2, 3],
        vec![2, 3, 4],
        vec![1, 2, 3, 4],
        "overlapping series IDs (union with duplicates)"
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
    fn should_merge_inverted_index(
        #[case] existing_ids: Vec<u32>,
        #[case] new_ids: Vec<u32>,
        #[case] expected_ids: Vec<u32>,
        #[case] description: &str,
    ) {
        // given
        let mut existing_bitmap = RoaringBitmap::new();
        for id in existing_ids {
            existing_bitmap.insert(id);
        }
        let existing_value = InvertedIndexValue {
            postings: existing_bitmap,
        }
        .encode()
        .unwrap();

        let mut new_bitmap = RoaringBitmap::new();
        for id in new_ids {
            new_bitmap.insert(id);
        }
        let new_value = InvertedIndexValue {
            postings: new_bitmap,
        }
        .encode()
        .unwrap();

        // when
        let merged = merge_inverted_index(existing_value, new_value).unwrap();
        let decoded = InvertedIndexValue::decode(merged.as_ref()).unwrap();

        // then
        let mut expected_bitmap = RoaringBitmap::new();
        for id in expected_ids {
            expected_bitmap.insert(id);
        }
        assert_eq!(
            decoded.postings, expected_bitmap,
            "Failed test case: {}",
            description
        );
    }

    #[rstest]
    #[case(
        vec![(1, 100), (2, 200)],
        vec![(3, 300), (4, 400)],
        vec![(1, 100), (2, 200), (3, 300), (4, 400)],
        "non-overlapping buckets"
    )]
    #[case(
        vec![(1, 100), (2, 200), (3, 300)],
        vec![(2, 200), (3, 300), (4, 400)],
        vec![(1, 100), (2, 200), (3, 300), (4, 400)],
        "overlapping buckets (deduplication)"
    )]
    #[case(
        vec![],
        vec![(1, 100), (2, 200)],
        vec![(1, 100), (2, 200)],
        "existing empty, new has buckets"
    )]
    #[case(
        vec![(1, 100), (2, 200)],
        vec![],
        vec![(1, 100), (2, 200)],
        "existing has buckets, new empty"
    )]
    #[case(
        vec![],
        vec![],
        vec![],
        "both empty"
    )]
    #[case(
        vec![(2, 200), (1, 100), (4, 400)],
        vec![(3, 300)],
        vec![(1, 100), (2, 200), (3, 300), (4, 400)],
        "unsorted buckets should be sorted by start time"
    )]
    fn should_merge_bucket_list(
        #[case] existing_buckets: Vec<(u8, u32)>,
        #[case] new_buckets: Vec<(u8, u32)>,
        #[case] expected_buckets: Vec<(u8, u32)>,
        #[case] description: &str,
    ) {
        // given
        let existing_value = BucketListValue {
            buckets: existing_buckets,
        }
        .encode();

        let new_value = BucketListValue {
            buckets: new_buckets,
        }
        .encode();

        // when
        let merged = merge_bucket_list(existing_value, new_value).unwrap();
        let decoded = BucketListValue::decode(merged.as_ref()).unwrap();

        // then
        assert_eq!(
            decoded.buckets, expected_buckets,
            "Failed test case: {}",
            description
        );
    }

    #[rstest]
    #[case(
        vec![(1000, 10.0), (2000, 20.0)],
        vec![(3000, 30.0), (4000, 40.0)],
        vec![(1000, 10.0), (2000, 20.0), (3000, 30.0), (4000, 40.0)],
        "non-overlapping timestamps"
    )]
    #[case(
        vec![(1000, 10.0), (2000, 20.0), (3000, 30.0)],
        vec![(2000, 200.0), (3000, 300.0), (4000, 40.0)],
        vec![(1000, 10.0), (2000, 200.0), (3000, 300.0), (4000, 40.0)],
        "overlapping timestamps (last write wins)"
    )]
    #[case(
        vec![],
        vec![(1000, 10.0), (2000, 20.0)],
        vec![(1000, 10.0), (2000, 20.0)],
        "existing empty, new has samples"
    )]
    #[case(
        vec![(1000, 10.0), (2000, 20.0)],
        vec![],
        vec![(1000, 10.0), (2000, 20.0)],
        "existing has samples, new empty"
    )]
    #[case(
        vec![],
        vec![],
        vec![],
        "both empty"
    )]
    fn should_merge_time_series(
        #[case] existing_samples: Vec<(i64, f64)>,
        #[case] new_samples: Vec<(i64, f64)>,
        #[case] expected_samples: Vec<(i64, f64)>,
        #[case] description: &str,
    ) {
        // given
        let existing_points: Vec<Sample> = existing_samples
            .iter()
            .map(|(ts, val)| Sample {
                timestamp_ms: *ts,
                value: *val,
            })
            .collect();
        let existing_value = TimeSeriesValue {
            points: existing_points,
        }
        .encode()
        .unwrap();

        let new_points: Vec<Sample> = new_samples
            .iter()
            .map(|(ts, val)| Sample {
                timestamp_ms: *ts,
                value: *val,
            })
            .collect();
        let new_value = TimeSeriesValue { points: new_points }.encode().unwrap();

        // when
        let merged = merge_time_series(existing_value, new_value).unwrap();
        let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();

        // then
        let expected_points: Vec<Sample> = expected_samples
            .iter()
            .map(|(ts, val)| Sample {
                timestamp_ms: *ts,
                value: *val,
            })
            .collect();
        assert_eq!(
            decoded.points, expected_points,
            "Failed test case: {}",
            description
        );
    }

    #[rstest]
    #[case(RecordType::InvertedIndex, create_inverted_index_key, "InvertedIndex")]
    #[case(RecordType::TimeSeries, create_time_series_key, "TimeSeries")]
    #[case(RecordType::BucketList, create_bucket_list_key, "BucketList")]
    fn should_route_to_correct_merge_function(
        #[case] record_type: RecordType,
        #[case] key_fn: fn() -> Bytes,
        #[case] description: &str,
    ) {
        // given
        let operator = OpenTsdbMergeOperator;
        let key = key_fn();

        // Create test values based on record type
        let (existing_value, new_value) = match record_type {
            RecordType::InvertedIndex => {
                let existing = InvertedIndexValue {
                    postings: {
                        let mut bm = RoaringBitmap::new();
                        bm.insert(1);
                        bm.insert(2);
                        bm
                    },
                }
                .encode()
                .unwrap();
                let new = InvertedIndexValue {
                    postings: {
                        let mut bm = RoaringBitmap::new();
                        bm.insert(3);
                        bm.insert(4);
                        bm
                    },
                }
                .encode()
                .unwrap();
                (existing, new)
            }
            RecordType::TimeSeries => {
                let existing = TimeSeriesValue {
                    points: vec![
                        Sample {
                            timestamp_ms: 1000,
                            value: 10.0,
                        },
                        Sample {
                            timestamp_ms: 2000,
                            value: 20.0,
                        },
                    ],
                }
                .encode()
                .unwrap();
                let new = TimeSeriesValue {
                    points: vec![
                        Sample {
                            timestamp_ms: 3000,
                            value: 30.0,
                        },
                        Sample {
                            timestamp_ms: 4000,
                            value: 40.0,
                        },
                    ],
                }
                .encode()
                .unwrap();
                (existing, new)
            }
            RecordType::BucketList => {
                let existing = BucketListValue {
                    buckets: vec![(1, 100), (2, 200)],
                }
                .encode();
                let new = BucketListValue {
                    buckets: vec![(3, 300), (4, 400)],
                }
                .encode();
                (existing, new)
            }
            _ => unreachable!(),
        };

        // when
        let merged = operator.merge(&key, Some(existing_value.clone()), new_value.clone());

        // then - verify the merge actually happened (not just returning new_value)
        // For InvertedIndex, check it's a union
        if record_type == RecordType::InvertedIndex {
            let decoded = InvertedIndexValue::decode(merged.as_ref()).unwrap();
            assert_eq!(
                decoded.postings.len(),
                4,
                "{} merge should union values",
                description
            );
        }
        // For TimeSeries, check samples are merged
        else if record_type == RecordType::TimeSeries {
            let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();
            assert_eq!(
                decoded.points.len(),
                4,
                "{} merge should combine samples",
                description
            );
        }
        // For BucketList, check buckets are merged
        else if record_type == RecordType::BucketList {
            let decoded = BucketListValue::decode(merged.as_ref()).unwrap();
            assert_eq!(
                decoded.buckets.len(),
                4,
                "{} merge should union buckets",
                description
            );
        }
    }

    #[test]
    fn should_return_new_value_when_no_existing_value() {
        // given
        let operator = OpenTsdbMergeOperator;
        let key = create_inverted_index_key();
        let new_value = Bytes::from(b"new_value".to_vec());

        // when
        let result = operator.merge(&key, None, new_value.clone());

        // then
        assert_eq!(result, new_value);
    }

    #[test]
    fn should_return_new_value_for_other_record_types() {
        // given
        let operator = OpenTsdbMergeOperator;
        let key = create_other_record_type_key();
        let existing_value = Bytes::from(b"existing".to_vec());
        let new_value = Bytes::from(b"new_value".to_vec());

        // when
        let result = operator.merge(&key, Some(existing_value), new_value.clone());

        // then - should return new_value without merging
        assert_eq!(result, new_value);
    }
}

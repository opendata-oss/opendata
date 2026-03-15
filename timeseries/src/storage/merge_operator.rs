use bytes::Bytes;

use crate::model::RecordTag;
use crate::serde::bucket_list::BucketListValue;
use crate::serde::inverted_index::InvertedIndexValue;
use crate::serde::timeseries::merge_batch_time_series;
use crate::serde::{EncodingError, RecordType, record_type_from_tag};
use common::storage::default_merge_batch;

/// Merge operator for OpenTSDB that handles merging of different record types.
///
/// Routes merge operations to the appropriate merge function based on the
/// record type encoded in the key.
pub(crate) struct OpenTsdbMergeOperator;

impl common::storage::MergeOperator for OpenTsdbMergeOperator {
    fn merge(&self, key: &Bytes, existing_value: Option<Bytes>, new_value: Bytes) -> Bytes {
        self.merge_batch(key, existing_value, &[new_value])
    }

    fn merge_batch(&self, key: &Bytes, existing_value: Option<Bytes>, operands: &[Bytes]) -> Bytes {
        // Decode record type from key
        if key.len() < 2 {
            panic!("Invalid key: key length is less than 2 bytes");
        }

        let record_tag =
            RecordTag::from_byte(key[1]).expect("Failed to decode record tag from key");

        let record_type =
            record_type_from_tag(record_tag).expect("Failed to get record type from record tag");

        match record_type {
            RecordType::InvertedIndex => merge_batch_inverted_index(existing_value, operands)
                .expect("Failed to batch merge inverted index"),
            RecordType::TimeSeries => merge_batch_time_series(existing_value, operands)
                .expect("Failed to batch merge time series"),
            RecordType::BucketList => merge_batch_bucket_list(existing_value, operands)
                .expect("Failed to batch merge bucket list"),
            _ => {
                // For other record types (SeriesDictionary, ForwardIndex), just use new value for each pairwise merge.
                // These should use Put, not Merge, but handle gracefully
                default_merge_batch(key, existing_value, operands, |_k, _e, v| v)
            }
        }
    }
}

/// Batch merge inverted index posting lists by unioning all RoaringBitmaps at once.
///
/// Decodes every bitmap once and unions them into a single result, avoiding
/// repeated serialize/deserialize cycles of the default pairwise merge.
fn merge_batch_inverted_index(
    existing: Option<Bytes>,
    operands: &[Bytes],
) -> Result<Bytes, EncodingError> {
    let mut merged = if let Some(existing) = existing {
        InvertedIndexValue::decode(existing.as_ref())?.postings
    } else {
        roaring::RoaringBitmap::new()
    };

    for operand in operands {
        let bitmap = InvertedIndexValue::decode(operand.as_ref())?.postings;
        merged |= bitmap;
    }

    (InvertedIndexValue { postings: merged }).encode()
}

/// Batch merge bucket lists by collecting all unique buckets and sorting once.
///
/// Decodes every bucket list once, collects unique buckets, and produces a single
/// sorted result, avoiding repeated decode/encode cycles.
fn merge_batch_bucket_list(
    existing: Option<Bytes>,
    operands: &[Bytes],
) -> Result<Bytes, EncodingError> {
    let mut buckets = if let Some(existing) = existing {
        BucketListValue::decode(existing.as_ref())?.buckets
    } else {
        Vec::new()
    };

    for operand in operands {
        let other_buckets = BucketListValue::decode(operand.as_ref())?.buckets;
        for bucket in other_buckets {
            if !buckets.contains(&bucket) {
                buckets.push(bucket);
            }
        }
    }

    // Sort by start time
    buckets.sort_by_key(|b| b.1);

    Ok(BucketListValue { buckets }.encode())
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
        let merged = merge_batch_inverted_index(Some(existing_value), &[new_value]).unwrap();
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
        let merged = merge_batch_bucket_list(Some(existing_value), &[new_value]).unwrap();
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
        let merged = merge_batch_time_series(Some(existing_value), &[new_value]).unwrap();
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
        let mut bm = RoaringBitmap::new();
        bm.insert(1);
        bm.insert(2);
        let new_value = InvertedIndexValue {
            postings: bm.clone(),
        }
        .encode()
        .unwrap();

        // when
        let result = operator.merge(&key, None, new_value);

        // then
        let decoded = InvertedIndexValue::decode(result.as_ref()).unwrap();
        assert_eq!(decoded.postings, bm);
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

    #[test]
    fn should_batch_merge_inverted_index() {
        // given
        let op0 = InvertedIndexValue {
            postings: {
                let mut bm = RoaringBitmap::new();
                bm.insert(1);
                bm.insert(2);
                bm
            },
        }
        .encode()
        .unwrap();
        let op1 = InvertedIndexValue {
            postings: {
                let mut bm = RoaringBitmap::new();
                bm.insert(2);
                bm.insert(3);
                bm
            },
        }
        .encode()
        .unwrap();
        let op2 = InvertedIndexValue {
            postings: {
                let mut bm = RoaringBitmap::new();
                bm.insert(4);
                bm.insert(5);
                bm
            },
        }
        .encode()
        .unwrap();

        // when - no existing value
        let merged =
            merge_batch_inverted_index(None, &[op0.clone(), op1.clone(), op2.clone()]).unwrap();
        let decoded = InvertedIndexValue::decode(merged.as_ref()).unwrap();

        // then - union of all bitmaps
        let mut expected = RoaringBitmap::new();
        for id in [1, 2, 3, 4, 5] {
            expected.insert(id);
        }
        assert_eq!(decoded.postings, expected);
    }

    #[test]
    fn should_batch_merge_inverted_index_with_existing() {
        // given
        let existing = InvertedIndexValue {
            postings: {
                let mut bm = RoaringBitmap::new();
                bm.insert(10);
                bm
            },
        }
        .encode()
        .unwrap();
        let op0 = InvertedIndexValue {
            postings: {
                let mut bm = RoaringBitmap::new();
                bm.insert(1);
                bm.insert(10);
                bm
            },
        }
        .encode()
        .unwrap();

        // when
        let merged = merge_batch_inverted_index(Some(existing), &[op0]).unwrap();
        let decoded = InvertedIndexValue::decode(merged.as_ref()).unwrap();

        // then
        let mut expected = RoaringBitmap::new();
        for id in [1, 10] {
            expected.insert(id);
        }
        assert_eq!(decoded.postings, expected);
    }

    #[test]
    fn should_batch_merge_bucket_list() {
        // given
        let op0 = BucketListValue {
            buckets: vec![(1, 100), (2, 200)],
        }
        .encode();
        let op1 = BucketListValue {
            buckets: vec![(2, 200), (3, 300)],
        }
        .encode();
        let op2 = BucketListValue {
            buckets: vec![(4, 400)],
        }
        .encode();

        // when - no existing value
        let merged = merge_batch_bucket_list(None, &[op0, op1, op2]).unwrap();
        let decoded = BucketListValue::decode(merged.as_ref()).unwrap();

        // then - unique buckets sorted by start time
        assert_eq!(
            decoded.buckets,
            vec![(1, 100), (2, 200), (3, 300), (4, 400)]
        );
    }

    #[test]
    fn should_batch_merge_bucket_list_with_existing() {
        // given
        let existing = BucketListValue {
            buckets: vec![(1, 100)],
        }
        .encode();
        let op0 = BucketListValue {
            buckets: vec![(2, 200)],
        }
        .encode();
        let op1 = BucketListValue {
            buckets: vec![(1, 100), (3, 300)],
        }
        .encode();

        // when
        let merged = merge_batch_bucket_list(Some(existing), &[op0, op1]).unwrap();
        let decoded = BucketListValue::decode(merged.as_ref()).unwrap();

        // then
        assert_eq!(decoded.buckets, vec![(1, 100), (2, 200), (3, 300)]);
    }

    #[test]
    fn should_batch_merge_time_series() {
        // given
        let op0 = TimeSeriesValue {
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
        let op1 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 200.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 30.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op2 = TimeSeriesValue {
            points: vec![Sample {
                timestamp_ms: 4000,
                value: 40.0,
            }],
        }
        .encode()
        .unwrap();

        // when - no existing value
        let merged =
            crate::serde::timeseries::merge_batch_time_series(None, &[op0, op1, op2]).unwrap();
        let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();

        // then - merged with last write wins on timestamp 2000
        let expected = vec![
            Sample {
                timestamp_ms: 1000,
                value: 10.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 200.0,
            },
            Sample {
                timestamp_ms: 3000,
                value: 30.0,
            },
            Sample {
                timestamp_ms: 4000,
                value: 40.0,
            },
        ];
        assert_eq!(decoded.points, expected);
    }

    #[test]
    fn should_batch_merge_time_series_with_existing() {
        // given
        let existing = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 1000,
                    value: 1.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 3.0,
                },
            ],
        }
        .encode()
        .unwrap();
        let op0 = TimeSeriesValue {
            points: vec![
                Sample {
                    timestamp_ms: 2000,
                    value: 2.0,
                },
                Sample {
                    timestamp_ms: 3000,
                    value: 300.0,
                },
            ],
        }
        .encode()
        .unwrap();

        // when
        let merged =
            crate::serde::timeseries::merge_batch_time_series(Some(existing), &[op0]).unwrap();
        let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();

        // then - timestamp 3000 takes operand value (newer)
        let expected = vec![
            Sample {
                timestamp_ms: 1000,
                value: 1.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 2.0,
            },
            Sample {
                timestamp_ms: 3000,
                value: 300.0,
            },
        ];
        assert_eq!(decoded.points, expected);
    }

    #[rstest]
    #[case(RecordType::InvertedIndex, create_inverted_index_key, "InvertedIndex")]
    #[case(RecordType::TimeSeries, create_time_series_key, "TimeSeries")]
    #[case(RecordType::BucketList, create_bucket_list_key, "BucketList")]
    fn should_route_merge_batch_to_correct_function(
        #[case] record_type: RecordType,
        #[case] key_fn: fn() -> Bytes,
        #[case] description: &str,
    ) {
        // given
        let operator = OpenTsdbMergeOperator;
        let key = key_fn();

        let (existing_value, op0, op1) = match record_type {
            RecordType::InvertedIndex => {
                let existing = InvertedIndexValue {
                    postings: {
                        let mut bm = RoaringBitmap::new();
                        bm.insert(1);
                        bm
                    },
                }
                .encode()
                .unwrap();
                let o0 = InvertedIndexValue {
                    postings: {
                        let mut bm = RoaringBitmap::new();
                        bm.insert(2);
                        bm
                    },
                }
                .encode()
                .unwrap();
                let o1 = InvertedIndexValue {
                    postings: {
                        let mut bm = RoaringBitmap::new();
                        bm.insert(3);
                        bm
                    },
                }
                .encode()
                .unwrap();
                (existing, o0, o1)
            }
            RecordType::TimeSeries => {
                let existing = TimeSeriesValue {
                    points: vec![Sample {
                        timestamp_ms: 1000,
                        value: 10.0,
                    }],
                }
                .encode()
                .unwrap();
                let o0 = TimeSeriesValue {
                    points: vec![Sample {
                        timestamp_ms: 2000,
                        value: 20.0,
                    }],
                }
                .encode()
                .unwrap();
                let o1 = TimeSeriesValue {
                    points: vec![Sample {
                        timestamp_ms: 3000,
                        value: 30.0,
                    }],
                }
                .encode()
                .unwrap();
                (existing, o0, o1)
            }
            RecordType::BucketList => {
                let existing = BucketListValue {
                    buckets: vec![(1, 100)],
                }
                .encode();
                let o0 = BucketListValue {
                    buckets: vec![(2, 200)],
                }
                .encode();
                let o1 = BucketListValue {
                    buckets: vec![(3, 300)],
                }
                .encode();
                (existing, o0, o1)
            }
            _ => unreachable!(),
        };

        // when
        let merged = operator.merge_batch(&key, Some(existing_value), &[op0, op1]);

        // then - verify all three sources were merged
        match record_type {
            RecordType::InvertedIndex => {
                let decoded = InvertedIndexValue::decode(merged.as_ref()).unwrap();
                assert_eq!(
                    decoded.postings.len(),
                    3,
                    "{} batch merge should union all values",
                    description
                );
            }
            RecordType::TimeSeries => {
                let decoded = TimeSeriesValue::decode(merged.as_ref()).unwrap();
                assert_eq!(
                    decoded.points.len(),
                    3,
                    "{} batch merge should combine all samples",
                    description
                );
            }
            RecordType::BucketList => {
                let decoded = BucketListValue::decode(merged.as_ref()).unwrap();
                assert_eq!(
                    decoded.buckets.len(),
                    3,
                    "{} batch merge should union all buckets",
                    description
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn should_merge_batch_return_new_value_for_other_record_types() {
        // given
        let operator = OpenTsdbMergeOperator;
        let key = create_other_record_type_key();
        let existing_value = Bytes::from(b"existing".to_vec());
        let op0 = Bytes::from(b"op0".to_vec());
        let op1 = Bytes::from(b"final".to_vec());

        // when
        let result = operator.merge_batch(&key, Some(existing_value), &[op0, op1.clone()]);

        // then - falls back to pairwise merge; last operand wins for non-mergeable types
        assert_eq!(result, op1);
    }
}

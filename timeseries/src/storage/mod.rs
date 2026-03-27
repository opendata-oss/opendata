use async_trait::async_trait;
use bytes::Bytes;
use common::bytes::{BytesRange, lex_increment};
use common::storage::RecordOp;
use common::{Record, Storage, StorageRead};
use roaring::RoaringBitmap;
use std::ops::Bound::{Excluded, Included, Unbounded};

use crate::index::{InvertedIndex, SeriesSpec};
use crate::model::{Sample, SeriesFingerprint, SeriesId, TimeBucket};
use crate::serde::key::{MetricTimeSeriesKey, TimeSeriesKey};
use crate::serde::timeseries::{TimeSeriesIterator, TimeSeriesValue};
use crate::{
    index::ForwardIndex,
    model::Label,
    query_io::{self, ReadKind},
    serde::{
        TimeBucketScoped,
        bucket_list::BucketListValue,
        dictionary::SeriesDictionaryValue,
        forward_index::ForwardIndexValue,
        inverted_index::InvertedIndexValue,
        key::{BucketListKey, ForwardIndexKey, InvertedIndexKey, SeriesDictionaryKey},
    },
    util::Result,
};

pub(crate) mod merge_operator;

/// Extension trait for StorageRead that provides OpenTSDB-specific loading methods
#[async_trait]
pub(crate) trait OpenTsdbStorageReadExt: StorageRead {
    /// Given a time range, return all the time buckets that contain data for
    /// that range sorted by start time.
    ///
    /// This method examines the actual list of buckets in storage to determine the
    /// candidate buckets (as opposed to computing theoretical buckets from the
    /// start and end times).
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_buckets_in_range(
        &self,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> Result<Vec<TimeBucket>> {
        if let (Some(start), Some(end)) = (start_secs, end_secs)
            && end < start
        {
            return Err("end must be greater than or equal to start".into());
        }

        // Convert to minutes once before filtering
        let start_min = start_secs.map(|s| (s / 60) as u32);
        let end_min = end_secs.map(|e| (e / 60) as u32);

        let key = BucketListKey.encode();
        let record = self.get(key).await?;
        query_io::record_get(ReadKind::BucketList, &record);
        let bucket_list = match record {
            Some(record) => BucketListValue::decode(record.value.as_ref())?,
            None => BucketListValue {
                buckets: Vec::new(),
            },
        };

        let mut filtered_buckets: Vec<TimeBucket> = bucket_list
            .buckets
            .into_iter()
            .map(|(size, start)| TimeBucket { size, start })
            .filter(|bucket| match (start_min, end_min) {
                (None, None) => true,
                (Some(start), None) => {
                    let start_bucket_min = start - start % bucket.size_in_mins();
                    bucket.start >= start_bucket_min
                }
                (None, Some(end)) => {
                    let end_bucket_min = end - end % bucket.size_in_mins();
                    bucket.start <= end_bucket_min
                }
                (Some(start), Some(end)) => {
                    let start_bucket_min = start - start % bucket.size_in_mins();
                    let end_bucket_min = end - end % bucket.size_in_mins();
                    bucket.start >= start_bucket_min && bucket.start <= end_bucket_min
                }
            })
            .collect();

        filtered_buckets.sort_by_key(|bucket| bucket.start);
        Ok(filtered_buckets)
    }

    /// Given a set of sorted, non-overlapping time ranges, return all buckets
    /// that overlap any range. Reads the bucket list once.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_buckets_for_ranges(&self, ranges: &[(i64, i64)]) -> Result<Vec<TimeBucket>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let key = BucketListKey.encode();
        let record = self.get(key).await?;
        query_io::record_get(ReadKind::BucketList, &record);
        let bucket_list = match record {
            Some(record) => BucketListValue::decode(record.value.as_ref())?,
            None => {
                return Ok(Vec::new());
            }
        };

        let mut filtered_buckets: Vec<TimeBucket> = bucket_list
            .buckets
            .into_iter()
            .map(|(size, start)| TimeBucket { size, start })
            .filter(|bucket| {
                let bucket_start_min = bucket.start as i64;
                let bucket_end_min = bucket_start_min + bucket.size_in_mins() as i64;
                // Convert bucket bounds to seconds for comparison
                let bucket_start_secs = bucket_start_min * 60;
                let bucket_end_secs = bucket_end_min * 60;
                // Bucket is half-open [start, end), range is closed [r_start, r_end].
                // Overlap iff bucket_end > r_start (strict: end is exclusive) and
                // bucket_start <= r_end (inclusive: start is inclusive).
                ranges.iter().any(|&(r_start, r_end)| {
                    bucket_end_secs > r_start && bucket_start_secs <= r_end
                })
            })
            .collect();

        filtered_buckets.sort_by_key(|bucket| bucket.start);
        Ok(filtered_buckets)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_forward_index(&self, bucket: TimeBucket) -> Result<ForwardIndex> {
        let range = ForwardIndexKey::bucket_range(&bucket);
        let records = self.scan(range).await?;
        query_io::record_scan(ReadKind::ForwardIndex, &records);

        let forward_index = ForwardIndex::default();
        for record in records {
            let key = ForwardIndexKey::decode(record.key.as_ref())?;
            let value = ForwardIndexValue::decode(record.value.as_ref())?;
            forward_index.series.insert(key.series_id, value.into());
        }

        Ok(forward_index)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_inverted_index(&self, bucket: TimeBucket) -> Result<InvertedIndex> {
        let range = InvertedIndexKey::bucket_range(&bucket);
        let records = self.scan(range).await?;
        query_io::record_scan(ReadKind::InvertedIndex, &records);

        let inverted_index = InvertedIndex::default();
        for record in records {
            let key = InvertedIndexKey::decode(record.key.as_ref())?;
            let value = InvertedIndexValue::decode(record.value.as_ref())?;
            inverted_index.postings.insert(
                Label {
                    name: key.attribute,
                    value: key.value,
                },
                value.postings,
            );
        }

        Ok(inverted_index)
    }

    /// Load only the specified terms from the inverted index.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_inverted_index_terms(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<InvertedIndex> {
        let result = InvertedIndex::default();
        for term in terms {
            let key = InvertedIndexKey {
                time_bucket: bucket.start,
                bucket_size: bucket.size,
                attribute: term.name.clone(),
                value: term.value.clone(),
            }
            .encode();
            let record = self.get(key).await?;
            query_io::record_get(ReadKind::InvertedIndex, &record);
            if let Some(record) = record {
                let value = InvertedIndexValue::decode(record.value.as_ref())?;
                result.postings.insert(term.clone(), value.postings);
            }
        }
        Ok(result)
    }

    /// Load only the specified series from the forward index.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_forward_index_series(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<ForwardIndex> {
        let result = ForwardIndex::default();
        for &series_id in series_ids {
            let key = ForwardIndexKey {
                time_bucket: bucket.start,
                bucket_size: bucket.size,
                series_id,
            }
            .encode();
            let record = self.get(key).await?;
            query_io::record_get(ReadKind::ForwardIndex, &record);
            if let Some(record) = record {
                let value = ForwardIndexValue::decode(record.value.as_ref())?;
                result.series.insert(series_id, value.into());
            }
        }
        Ok(result)
    }

    /// Load the series dictionary using the provided insert function and
    /// return the maximum series ID found, which can be used to
    /// initialize counters
    #[tracing::instrument(level = "trace", skip(self, bucket, insert))]
    async fn load_series_dictionary<F>(&self, bucket: &TimeBucket, mut insert: F) -> Result<u32>
    where
        F: FnMut(SeriesFingerprint, SeriesId) + Send,
    {
        let range = SeriesDictionaryKey::bucket_range(bucket);
        let records = self.scan(range).await?;

        let mut max_series_id = 0;
        for record in records {
            let key = SeriesDictionaryKey::decode(record.key.as_ref())?;
            let value = SeriesDictionaryValue::decode(record.value.as_ref())?;
            insert(key.series_fingerprint, value.series_id);
            max_series_id = std::cmp::max(max_series_id, value.series_id);
        }

        Ok(max_series_id)
    }

    /// Get all unique values for a specific label name within a bucket.
    /// This method scans only the inverted index keys for the specified label,
    /// which is more efficient than loading all inverted index entries.
    ///
    /// Note: We don't need to verify that the decoded `key.attribute` matches
    /// `label_name` after scanning. The `attribute_range` prefix includes a
    /// 2-byte little-endian length prefix before the attribute string (see
    /// `encode_utf8`), which guarantees that only exact attribute matches are
    /// returned. For example, searching for "hostname" (len=8, encoded as
    /// `[0x08, 0x00, ...]`) can never match a key with attribute "host"
    /// (len=4, encoded as `[0x04, 0x00, ...]`) because the length bytes differ.
    /// See `serde::name::tests::should_not_match_shorter_attribute_with_value_that_looks_like_suffix`
    /// for test coverage of this invariant.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        let range = InvertedIndexKey::attribute_range(bucket, label_name);
        let records = self.scan(range).await?;

        let mut values = Vec::new();
        for record in records {
            let key = InvertedIndexKey::decode(record.key.as_ref())?;
            values.push(key.value);
        }

        Ok(values)
    }

    // ── Singleton scan for merge-written keys ──────────────────────
    //
    // For keys written with RecordOp::Merge, a point get() must walk
    // sorted runs sequentially to discover all merge operands. By
    // contrast, a scan forces SlateDB's scan path, which can fan out
    // across sorted runs concurrently. This helper wraps a singleton
    // scan: it builds a range [key, lex_successor(key)) so that only
    // the exact key can match, then verifies the returned record.

    /// Read a merge-written key via singleton scan instead of point get.
    ///
    /// This forces SlateDB's scan path, which handles merge operands
    /// more efficiently than sequential get() for merge-heavy keys.
    /// The `kind` parameter controls instrumentation attribution.
    async fn get_merged_record(&self, key: Bytes, kind: ReadKind) -> Result<Option<Record>> {
        let range = match lex_increment(&key) {
            Some(successor) => BytesRange::new(Included(key.clone()), Excluded(successor)),
            // Effectively impossible for normal Timeseries keys, but handle defensively.
            None => BytesRange::new(Included(key.clone()), Unbounded),
        };
        let records = self.scan(range).await?;
        query_io::record_scan(kind, &records);
        Ok(records.into_iter().find(|r| r.key == key))
    }

    // ── Warm helpers (touch storage to populate block cache) ────────

    /// Load samples for a single series using the metric-prefixed key layout.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_metric_samples(
        &self,
        bucket: &TimeBucket,
        metric_name: &str,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        let key = MetricTimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            metric_name: metric_name.to_string(),
            series_id,
        }
        .encode();

        let record = self
            .get_merged_record(key, ReadKind::SampleMetricScan)
            .await?;
        match record {
            Some(record) => {
                let iter = TimeSeriesIterator::new(record.value.as_ref()).ok_or_else(|| {
                    crate::error::Error::Internal(
                        "Invalid timeseries data in metric-prefixed storage".into(),
                    )
                })?;

                let samples: Vec<Sample> = iter
                    .filter_map(|r| r.ok())
                    .filter(|s| s.timestamp_ms > start_ms && s.timestamp_ms <= end_ms)
                    .collect();
                Ok(samples)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Warm the bucket list by reading it from storage. Returns the list of
    /// buckets and the total bytes read (key + value).
    async fn warm_bucket_list(&self) -> Result<WarmBucketListResult> {
        let key = BucketListKey.encode();
        let record = self.get(key).await?;
        match record {
            Some(record) => {
                let bytes_read = (record.key.len() + record.value.len()) as u64;
                let bucket_list = BucketListValue::decode(record.value.as_ref())?;
                let buckets = bucket_list
                    .buckets
                    .into_iter()
                    .map(|(size, start)| TimeBucket { size, start })
                    .collect();
                Ok(WarmBucketListResult {
                    buckets,
                    bytes_read,
                })
            }
            None => Ok(WarmBucketListResult {
                buckets: Vec::new(),
                bytes_read: 0,
            }),
        }
    }

    /// Warm the forward index for a bucket by scanning all its keys.
    /// Returns the total bytes read (sum of key + value sizes).
    /// Does not decode values — only touches storage to populate cache.
    async fn warm_forward_index_bytes(&self, bucket: TimeBucket) -> Result<u64> {
        let range = ForwardIndexKey::bucket_range(&bucket);
        let records = self.scan(range).await?;
        let mut bytes = 0u64;
        for record in records {
            bytes += (record.key.len() + record.value.len()) as u64;
        }
        Ok(bytes)
    }

    /// Warm the inverted index for a bucket by scanning all its keys.
    /// Returns the total bytes read (sum of key + value sizes).
    /// Does not decode values — only touches storage to populate cache.
    async fn warm_inverted_index_bytes(&self, bucket: TimeBucket) -> Result<u64> {
        let range = InvertedIndexKey::bucket_range(&bucket);
        let records = self.scan(range).await?;
        let mut bytes = 0u64;
        for record in records {
            bytes += (record.key.len() + record.value.len()) as u64;
        }
        Ok(bytes)
    }
}

/// Result of warming the bucket list.
pub(crate) struct WarmBucketListResult {
    pub buckets: Vec<TimeBucket>,
    pub bytes_read: u64,
}

// Implement the trait for all types that implement StorageRead
impl<T: ?Sized + StorageRead> OpenTsdbStorageReadExt for T {}

pub(crate) trait OpenTsdbStorageExt: Storage {
    fn merge_bucket_list(&self, bucket: TimeBucket) -> Result<RecordOp> {
        let key = BucketListKey.encode();
        let value = BucketListValue {
            buckets: vec![(bucket.size, bucket.start)],
        }
        .encode();

        Ok(RecordOp::Merge(Record { key, value }.into()))
    }

    fn insert_series_id(
        &self,
        bucket: TimeBucket,
        fingerprint: SeriesFingerprint,
        id: SeriesId,
    ) -> Result<RecordOp> {
        let key = SeriesDictionaryKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            series_fingerprint: fingerprint,
        }
        .encode();
        let value = SeriesDictionaryValue { series_id: id }.encode();
        Ok(RecordOp::Put(Record { key, value }.into()))
    }

    fn insert_forward_index(
        &self,
        bucket: TimeBucket,
        series_id: SeriesId,
        series_spec: SeriesSpec,
    ) -> Result<RecordOp> {
        let key = ForwardIndexKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            series_id,
        }
        .encode();
        let value = ForwardIndexValue {
            metric_unit: series_spec.unit,
            metric_meta: series_spec.metric_type.into(),
            label_count: series_spec.labels.len() as u16,
            labels: series_spec.labels,
        }
        .encode();
        Ok(RecordOp::Put(Record { key, value }.into()))
    }

    fn merge_inverted_index(
        &self,
        bucket: TimeBucket,
        label: Label,
        postings: RoaringBitmap,
    ) -> Result<RecordOp> {
        let key = InvertedIndexKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            attribute: label.name,
            value: label.value,
        }
        .encode();
        let value = InvertedIndexValue { postings }.encode()?;
        Ok(RecordOp::Merge(Record { key, value }.into()))
    }

    fn merge_samples(
        &self,
        bucket: TimeBucket,
        series_id: SeriesId,
        samples: Vec<Sample>,
    ) -> Result<RecordOp> {
        let key = TimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            series_id,
        }
        .encode();
        let value = TimeSeriesValue { points: samples }.encode()?;
        Ok(RecordOp::Merge(Record { key, value }.into()))
    }

    fn merge_metric_samples(
        &self,
        bucket: TimeBucket,
        metric_name: &str,
        series_id: SeriesId,
        samples: Vec<Sample>,
    ) -> Result<RecordOp> {
        let key = MetricTimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            metric_name: metric_name.to_string(),
            series_id,
        }
        .encode();
        let value = TimeSeriesValue { points: samples }.encode()?;
        Ok(RecordOp::Merge(Record { key, value }.into()))
    }
}

// Implement the trait for all types that implement Storage
impl<T: ?Sized + Storage> OpenTsdbStorageExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::storage::PutRecordOp;
    use common::storage::in_memory::InMemoryStorage;
    use std::sync::Arc;

    /// Create an InMemoryStorage with the given hour-buckets pre-populated.
    /// Each entry in `bucket_starts` is the bucket start in minutes.
    async fn storage_with_buckets(bucket_starts: &[u32]) -> Arc<InMemoryStorage> {
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let buckets: Vec<(u8, u32)> = bucket_starts.iter().map(|&s| (1u8, s)).collect();
        let key = BucketListKey.encode();
        let value = BucketListValue { buckets }.encode();
        storage
            .put(vec![PutRecordOp::new(Record { key, value })])
            .await
            .unwrap();
        storage
    }

    fn starts(buckets: &[TimeBucket]) -> Vec<u32> {
        buckets.iter().map(|b| b.start).collect()
    }

    // ── get_buckets_for_ranges boundary tests ──────────────────────────

    #[tokio::test]
    async fn ranges_point_at_bucket_start() {
        // Buckets: [0, 3600s), [3600s, 7200s)  (hour buckets at min 0 and 60)
        let s = storage_with_buckets(&[0, 60]).await;
        // Point query exactly at second bucket start
        let buckets = s.get_buckets_for_ranges(&[(3600, 3600)]).await.unwrap();
        // Should match only the bucket starting at 3600s (min 60), not the one ending there
        assert_eq!(starts(&buckets), vec![60]);
    }

    #[tokio::test]
    async fn ranges_point_at_epoch() {
        let s = storage_with_buckets(&[0, 60]).await;
        // Point query at t=0
        let buckets = s.get_buckets_for_ranges(&[(0, 0)]).await.unwrap();
        assert_eq!(starts(&buckets), vec![0]);
    }

    #[tokio::test]
    async fn ranges_spanning_two_buckets() {
        let s = storage_with_buckets(&[0, 60, 120]).await;
        // Range [1800, 5400] spans bucket [0,3600) and [3600,7200)
        let buckets = s.get_buckets_for_ranges(&[(1800, 5400)]).await.unwrap();
        assert_eq!(starts(&buckets), vec![0, 60]);
    }

    #[tokio::test]
    async fn ranges_disjoint_skips_middle() {
        // 3 hour-buckets: [0,3600), [3600,7200), [7200,10800)
        let s = storage_with_buckets(&[0, 60, 120]).await;
        // Two disjoint ranges that skip the middle bucket
        let buckets = s
            .get_buckets_for_ranges(&[(100, 200), (8000, 9000)])
            .await
            .unwrap();
        assert_eq!(starts(&buckets), vec![0, 120]);
    }

    #[tokio::test]
    async fn ranges_empty_returns_nothing() {
        let s = storage_with_buckets(&[0, 60]).await;
        let buckets = s.get_buckets_for_ranges(&[]).await.unwrap();
        assert!(buckets.is_empty());
    }

    #[tokio::test]
    async fn ranges_exact_bucket_boundary_excludes_previous() {
        // Range starts exactly at boundary between two buckets.
        // Bucket [0, 3600s) should NOT match range [3600, 7200].
        let s = storage_with_buckets(&[0, 60]).await;
        let buckets = s.get_buckets_for_ranges(&[(3600, 7200)]).await.unwrap();
        assert_eq!(starts(&buckets), vec![60]);
    }

    // ── warm helper tests ───────────────────────────────────────────────

    use crate::serde::forward_index::{ForwardIndexValue, MetricMeta};
    use crate::serde::inverted_index::InvertedIndexValue;

    fn dummy_fi_value() -> ForwardIndexValue {
        ForwardIndexValue {
            metric_unit: None,
            metric_meta: MetricMeta {
                metric_type: 0,
                flags: 0,
            },
            label_count: 1,
            labels: vec![crate::model::Label {
                name: "__name__".to_string(),
                value: "test_metric".to_string(),
            }],
        }
    }

    fn dummy_ii_value() -> InvertedIndexValue {
        InvertedIndexValue {
            postings: roaring::RoaringBitmap::from_iter([1u32, 2, 3]),
        }
    }

    /// Populate a storage with forward/inverted index entries for a bucket.
    async fn storage_with_metadata(bucket_start: u32) -> Arc<InMemoryStorage> {
        let storage = storage_with_buckets(&[bucket_start]).await;
        let bucket = TimeBucket::hour(bucket_start);
        let mut ops = Vec::new();

        // Insert 3 forward index entries.
        for series_id in 1u32..=3 {
            let key = ForwardIndexKey {
                time_bucket: bucket.start,
                bucket_size: bucket.size,
                series_id,
            }
            .encode();
            let value = dummy_fi_value().encode();
            ops.push(PutRecordOp::new(Record { key, value }));
        }

        // Insert 2 inverted index entries.
        for (attr, val) in [("__name__", "test_metric"), ("env", "prod")] {
            let key = InvertedIndexKey {
                time_bucket: bucket.start,
                bucket_size: bucket.size,
                attribute: attr.to_string(),
                value: val.to_string(),
            }
            .encode();
            let value = dummy_ii_value().encode().unwrap();
            ops.push(PutRecordOp::new(Record { key, value }));
        }

        storage.put(ops).await.unwrap();
        storage
    }

    #[tokio::test]
    async fn warm_bucket_list_returns_buckets_and_bytes() {
        let s = storage_with_buckets(&[0, 60, 120]).await;
        let result = s.warm_bucket_list().await.unwrap();
        assert_eq!(result.buckets.len(), 3);
        assert!(result.bytes_read > 0);
        let starts: Vec<u32> = result.buckets.iter().map(|b| b.start).collect();
        assert_eq!(starts, vec![0, 60, 120]);
    }

    #[tokio::test]
    async fn warm_bucket_list_returns_empty_when_no_buckets() {
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let result = storage.warm_bucket_list().await.unwrap();
        assert!(result.buckets.is_empty());
        assert_eq!(result.bytes_read, 0);
    }

    #[tokio::test]
    async fn warm_forward_index_bytes_returns_total_bytes() {
        let s = storage_with_metadata(60).await;
        let bucket = TimeBucket::hour(60);
        let bytes = s.warm_forward_index_bytes(bucket).await.unwrap();
        // 3 forward index entries, each with key + value > 0 bytes
        assert!(bytes > 0, "expected non-zero bytes, got {bytes}");
    }

    #[tokio::test]
    async fn warm_forward_index_bytes_zero_for_empty_bucket() {
        let s = storage_with_buckets(&[60]).await;
        // Bucket exists in list but has no forward index entries
        let bucket = TimeBucket::hour(60);
        let bytes = s.warm_forward_index_bytes(bucket).await.unwrap();
        assert_eq!(bytes, 0);
    }

    #[tokio::test]
    async fn warm_inverted_index_bytes_returns_total_bytes() {
        let s = storage_with_metadata(60).await;
        let bucket = TimeBucket::hour(60);
        let bytes = s.warm_inverted_index_bytes(bucket).await.unwrap();
        // 2 inverted index entries
        assert!(bytes > 0, "expected non-zero bytes, got {bytes}");
    }

    #[tokio::test]
    async fn warm_inverted_index_bytes_zero_for_empty_bucket() {
        let s = storage_with_buckets(&[60]).await;
        let bucket = TimeBucket::hour(60);
        let bytes = s.warm_inverted_index_bytes(bucket).await.unwrap();
        assert_eq!(bytes, 0);
    }

    // ── metric-prefixed sample storage tests ────────────────────────────

    #[tokio::test]
    async fn should_merge_and_read_metric_prefixed_samples() {
        // given
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);
        let samples = vec![
            Sample {
                timestamp_ms: 1000,
                value: 1.0,
            },
            Sample {
                timestamp_ms: 2000,
                value: 2.0,
            },
        ];

        // when - write via merge_metric_samples
        let op = storage
            .merge_metric_samples(bucket, "http_requests_total", 42, samples)
            .unwrap();
        match op {
            common::storage::RecordOp::Merge(record) => {
                storage.merge(vec![record]).await.unwrap();
            }
            _ => panic!("expected Merge op"),
        }

        // then - read back via get_metric_samples
        let result = storage
            .get_metric_samples(&bucket, "http_requests_total", 42, i64::MIN, i64::MAX)
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].timestamp_ms, 1000);
        assert_eq!(result[1].timestamp_ms, 2000);
    }

    #[tokio::test]
    async fn should_return_empty_for_nonexistent_metric_prefixed_sample() {
        // given
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);

        // when
        let result = storage
            .get_metric_samples(&bucket, "nonexistent_metric", 1, i64::MIN, i64::MAX)
            .await
            .unwrap();

        // then
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn should_filter_metric_prefixed_samples_by_time_range() {
        // given
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);
        let samples = vec![
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
                value: 3.0,
            },
        ];
        let op = storage
            .merge_metric_samples(bucket, "cpu_usage", 10, samples)
            .unwrap();
        match op {
            common::storage::RecordOp::Merge(record) => {
                storage.merge(vec![record]).await.unwrap();
            }
            _ => panic!("expected Merge op"),
        }

        // when - query with range (1000, 2500] (exclusive start, inclusive end)
        let result = storage
            .get_metric_samples(&bucket, "cpu_usage", 10, 1000, 2500)
            .await
            .unwrap();

        // then - only timestamp 2000 is in (1000, 2500]
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].timestamp_ms, 2000);
    }

    // ── get_merged_record (singleton scan) tests ────────────────────

    #[tokio::test]
    async fn singleton_scan_returns_exact_merged_record() {
        // given: two merge writes for the same metric/series key
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);

        // First merge: samples at t=1000
        let op1 = storage
            .merge_metric_samples(
                bucket,
                "http_total",
                7,
                vec![Sample {
                    timestamp_ms: 1000,
                    value: 1.0,
                }],
            )
            .unwrap();
        // Second merge: samples at t=2000
        let op2 = storage
            .merge_metric_samples(
                bucket,
                "http_total",
                7,
                vec![Sample {
                    timestamp_ms: 2000,
                    value: 2.0,
                }],
            )
            .unwrap();
        for op in [op1, op2] {
            match op {
                RecordOp::Merge(record) => {
                    storage.merge(vec![record]).await.unwrap();
                }
                _ => panic!("expected Merge op"),
            }
        }

        // when: read via singleton scan
        let key = MetricTimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            metric_name: "http_total".to_string(),
            series_id: 7,
        }
        .encode();
        let record = storage
            .get_merged_record(key, ReadKind::SampleMetricScan)
            .await
            .unwrap();

        // then: record exists and contains merged samples
        assert!(record.is_some(), "singleton scan should return the record");
        let record = record.unwrap();
        let iter = TimeSeriesIterator::new(record.value.as_ref()).unwrap();
        let samples: Vec<Sample> = iter.filter_map(|r| r.ok()).collect();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].timestamp_ms, 1000);
        assert_eq!(samples[1].timestamp_ms, 2000);
    }

    #[tokio::test]
    async fn singleton_scan_does_not_match_neighboring_keys() {
        // given: two metric/series keys that are lexicographically adjacent
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);

        // Write series_id=7 and series_id=8 for the same metric
        for (series_id, value) in [(7, 1.0), (8, 2.0)] {
            let op = storage
                .merge_metric_samples(
                    bucket,
                    "cpu",
                    series_id,
                    vec![Sample {
                        timestamp_ms: 1000,
                        value,
                    }],
                )
                .unwrap();
            match op {
                RecordOp::Merge(record) => {
                    storage.merge(vec![record]).await.unwrap();
                }
                _ => panic!("expected Merge op"),
            }
        }

        // when: singleton scan for series_id=7
        let key_7 = MetricTimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            metric_name: "cpu".to_string(),
            series_id: 7,
        }
        .encode();
        let record = storage
            .get_merged_record(key_7, ReadKind::SampleMetricScan)
            .await
            .unwrap();

        // then: only series 7 returned, not series 8
        let record = record.unwrap();
        let iter = TimeSeriesIterator::new(record.value.as_ref()).unwrap();
        let samples: Vec<Sample> = iter.filter_map(|r| r.ok()).collect();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 1.0, "should be series 7, not 8");
    }

    #[tokio::test]
    async fn singleton_scan_missing_key_returns_none() {
        // given: empty storage
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);

        // when: singleton scan for a key that doesn't exist
        let key = MetricTimeSeriesKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            metric_name: "nonexistent".to_string(),
            series_id: 1,
        }
        .encode();
        let record = storage
            .get_merged_record(key, ReadKind::SampleMetricScan)
            .await
            .unwrap();

        // then: None
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn singleton_scan_records_scan_instrumentation() {
        use crate::query_io::{self, QueryIoCollector};

        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);

        // Write one sample
        let op = storage
            .merge_metric_samples(
                bucket,
                "req_total",
                1,
                vec![Sample {
                    timestamp_ms: 1000,
                    value: 42.0,
                }],
            )
            .unwrap();
        match op {
            RecordOp::Merge(record) => {
                storage.merge(vec![record]).await.unwrap();
            }
            _ => panic!("expected Merge op"),
        }

        // when: read within a collector scope
        let collector = QueryIoCollector::new();
        let _record = query_io::run_with_collector(&collector, async {
            let key = MetricTimeSeriesKey {
                time_bucket: bucket.start,
                bucket_size: bucket.size,
                metric_name: "req_total".to_string(),
                series_id: 1,
            }
            .encode();
            storage
                .get_merged_record(key, ReadKind::SampleMetricScan)
                .await
        })
        .await
        .unwrap();

        // then: scan counters populated, get counters zero
        let io = collector.snapshot();
        assert_eq!(io.sample_metric_scan_calls, 1, "one scan call");
        assert_eq!(io.sample_metric_scan_records, 1, "one record returned");
        assert!(io.sample_metric_scan_bytes > 0, "bytes recorded");
        assert_eq!(io.sample_metric_get_calls, 0, "no get calls");
        assert_eq!(io.sample_get_calls, 0, "no legacy get calls");

        // Scan should appear in scan totals, not get totals
        assert_eq!(io.storage_scan_calls_total(), 1);
        assert_eq!(io.storage_get_calls_total(), 0);
        assert_eq!(io.storage_read_api_calls_total(), 1);
    }

    #[tokio::test]
    async fn singleton_scan_miss_records_zero_bytes() {
        use crate::query_io::{self, QueryIoCollector};

        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);

        // when: singleton scan on missing key within a collector scope
        let collector = QueryIoCollector::new();
        let record = query_io::run_with_collector(&collector, async {
            let key = MetricTimeSeriesKey {
                time_bucket: bucket.start,
                bucket_size: bucket.size,
                metric_name: "missing".to_string(),
                series_id: 1,
            }
            .encode();
            storage
                .get_merged_record(key, ReadKind::SampleMetricScan)
                .await
        })
        .await
        .unwrap();

        // then: None result, scan call counted but 0 records/bytes
        assert!(record.is_none());
        let io = collector.snapshot();
        assert_eq!(io.sample_metric_scan_calls, 1, "scan call counted");
        assert_eq!(io.sample_metric_scan_records, 0, "no records");
        assert_eq!(io.sample_metric_scan_bytes, 0, "no bytes");
    }

    #[tokio::test]
    async fn legacy_singleton_scan_records_correct_kind() {
        use crate::query_io::{self, QueryIoCollector};

        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let bucket = TimeBucket::hour(60);

        // Write one legacy sample
        let op = storage
            .merge_samples(
                bucket,
                1,
                vec![Sample {
                    timestamp_ms: 1000,
                    value: 10.0,
                }],
            )
            .unwrap();
        match op {
            RecordOp::Merge(record) => {
                storage.merge(vec![record]).await.unwrap();
            }
            _ => panic!("expected Merge op"),
        }

        // when: read with SampleScan kind
        let collector = QueryIoCollector::new();
        let record = query_io::run_with_collector(&collector, async {
            let key = TimeSeriesKey {
                time_bucket: bucket.start,
                bucket_size: bucket.size,
                series_id: 1,
            }
            .encode();
            storage.get_merged_record(key, ReadKind::SampleScan).await
        })
        .await
        .unwrap();

        // then: SampleScan counters populated
        assert!(record.is_some());
        let io = collector.snapshot();
        assert_eq!(io.sample_scan_calls, 1, "one legacy scan");
        assert_eq!(io.sample_scan_records, 1, "one record");
        assert!(io.sample_scan_bytes > 0, "bytes recorded");
        assert_eq!(io.sample_metric_scan_calls, 0, "not metric scan");
        assert_eq!(io.sample_get_calls, 0, "no legacy get");
    }
}

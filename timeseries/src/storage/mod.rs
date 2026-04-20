use async_trait::async_trait;
use common::storage::{PutRecordOp, RecordOp};
use common::{Record, Storage, StorageRead};
use roaring::RoaringBitmap;

use crate::index::{InvertedIndex, SeriesSpec};
use crate::model::{Sample, SeriesFingerprint, SeriesId, TimeBucket};
use crate::serde::key::TimeSeriesKey;
use crate::serde::timeseries::TimeSeriesValue;
use crate::{
    index::ForwardIndex,
    model::Label,
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

    /// Load only the specified terms from the inverted index. Legacy
    /// batch path — kept for the v1 evaluator / pipeline which still
    /// calls it. New callers should use [`Self::get_inverted_index_term`]
    /// and fan out in parallel themselves so each per-term latency is
    /// independently traceable.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_inverted_index_terms(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<InvertedIndex> {
        let result = InvertedIndex::default();
        for term in terms {
            if let Some(postings) = self.get_inverted_index_term(bucket, term).await? {
                result.postings.insert(term.clone(), postings);
            }
        }
        Ok(result)
    }

    /// Load only the specified series from the forward index. Legacy
    /// batch path — see [`Self::get_inverted_index_terms`] for context.
    /// New callers should use [`Self::get_forward_index_one`].
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_forward_index_series(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<ForwardIndex> {
        let result = ForwardIndex::default();
        for &series_id in series_ids {
            if let Some(spec) = self.get_forward_index_one(bucket, series_id).await? {
                result.series.insert(series_id, spec);
            }
        }
        Ok(result)
    }

    /// Fetch a single inverted-index posting for `(bucket, term)`.
    /// Returns `None` when the term isn't present in the bucket.
    ///
    /// Per-term granularity by design: callers (e.g. the query
    /// adapter) parallelise at *their* layer so concurrency budget and
    /// caching can be managed end-to-end. The previous batched variant
    /// looped sequentially and was a silent bottleneck.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<Option<RoaringBitmap>> {
        let key = InvertedIndexKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            attribute: term.name.clone(),
            value: term.value.clone(),
        }
        .encode();
        let rec = self.get(key).await?;
        match rec {
            Some(r) => {
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::InvertedIndexFetch,
                    r.value.len() as u64,
                );
                let value = InvertedIndexValue::decode(r.value.as_ref())?;
                Ok(Some(value.postings))
            }
            None => Ok(None),
        }
    }

    /// Fetch a single forward-index entry for `(bucket, series_id)`.
    /// Returns `None` when the series isn't present in the bucket.
    /// See [`Self::get_inverted_index_term`] for why this is per-key.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<SeriesSpec>> {
        let key = ForwardIndexKey {
            time_bucket: bucket.start,
            bucket_size: bucket.size,
            series_id,
        }
        .encode();
        let rec = self.get(key).await?;
        match rec {
            Some(r) => {
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::ForwardIndexFetch,
                    r.value.len() as u64,
                );
                let value = ForwardIndexValue::decode(r.value.as_ref())?;
                Ok(Some(value.into()))
            }
            None => Ok(None),
        }
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

    /// Check whether `bucket` is already present in the stored `BucketList`.
    ///
    /// Used by the flush path to suppress redundant single-element merges
    /// on a bucket that has already been announced. The `BucketListKey` is
    /// a global singleton that stays hot in SlateDB's block cache (read on
    /// every query and warmed at startup), so this is a cache hit in the
    /// common case.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn bucket_list_contains(&self, bucket: TimeBucket) -> Result<bool> {
        let key = BucketListKey.encode();
        let Some(record) = self.get(key).await? else {
            return Ok(false);
        };
        let list = BucketListValue::decode(record.value.as_ref())?;
        Ok(list.buckets.contains(&(bucket.size, bucket.start)))
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
        metric_name: &str,
        samples: Vec<Sample>,
    ) -> Result<RecordOp> {
        let key = TimeSeriesKey {
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

/// Read the current `BucketList` value and rewrite it as a single `Put`,
/// collapsing the merge chain that accrues across ingestion batches into
/// one flat record. Intended to run once at startup before ingestion
/// begins, so later reads don't have to replay operands scattered across
/// SSTs. Safe only when there are no concurrent writers.
#[tracing::instrument(level = "info", skip_all)]
pub(crate) async fn coalesce_bucket_list(storage: &dyn Storage) -> Result<()> {
    let key = BucketListKey.encode();
    let Some(record) = storage.get(key.clone()).await? else {
        return Ok(());
    };
    storage
        .put(vec![PutRecordOp::new(Record {
            key,
            value: record.value,
        })])
        .await?;
    Ok(())
}

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

    // ── bucket_list_contains tests ─────────────────────────────────────

    #[tokio::test]
    async fn should_return_true_when_bucket_in_list() {
        // given
        let storage = storage_with_buckets(&[0, 60, 120]).await;

        // when
        let present = storage
            .bucket_list_contains(TimeBucket { size: 1, start: 60 })
            .await
            .unwrap();

        // then
        assert!(present);
    }

    #[tokio::test]
    async fn should_return_false_when_bucket_absent() {
        // given
        let storage = storage_with_buckets(&[0, 60]).await;

        // when
        let present = storage
            .bucket_list_contains(TimeBucket {
                size: 1,
                start: 180,
            })
            .await
            .unwrap();

        // then
        assert!(!present);
    }

    #[tokio::test]
    async fn should_return_false_when_bucket_list_key_missing() {
        // given
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));

        // when
        let present = storage
            .bucket_list_contains(TimeBucket { size: 1, start: 0 })
            .await
            .unwrap();

        // then
        assert!(!present);
    }

    #[tokio::test]
    async fn should_distinguish_buckets_with_same_start_but_different_size() {
        // given: only a size=1 bucket at start=60 is present
        let storage = storage_with_buckets(&[60]).await;

        // when
        let size_one = storage
            .bucket_list_contains(TimeBucket { size: 1, start: 60 })
            .await
            .unwrap();
        let size_two = storage
            .bucket_list_contains(TimeBucket { size: 2, start: 60 })
            .await
            .unwrap();

        // then
        assert!(size_one);
        assert!(!size_two);
    }

    // ── coalesce_bucket_list tests ─────────────────────────────────────

    #[tokio::test]
    async fn should_coalesce_bucket_list_preserving_merged_value() {
        // given: several merge operands have accrued on the BucketList key
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let ops = vec![
            storage
                .merge_bucket_list(TimeBucket { size: 1, start: 0 })
                .unwrap(),
            storage
                .merge_bucket_list(TimeBucket { size: 1, start: 60 })
                .unwrap(),
            storage
                .merge_bucket_list(TimeBucket {
                    size: 1,
                    start: 120,
                })
                .unwrap(),
        ];
        storage.apply(ops).await.unwrap();

        // when
        coalesce_bucket_list(storage.as_ref()).await.unwrap();

        // then: readable value still reflects the full merged list
        let buckets = storage.get_buckets_in_range(None, None).await.unwrap();
        assert_eq!(starts(&buckets), vec![0, 60, 120]);
    }

    #[tokio::test]
    async fn should_be_noop_when_bucket_list_absent() {
        // given
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));

        // when
        coalesce_bucket_list(storage.as_ref()).await.unwrap();

        // then: key is still absent
        let record = storage.get(BucketListKey.encode()).await.unwrap();
        assert!(record.is_none());
    }
}

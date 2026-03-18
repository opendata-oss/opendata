use async_trait::async_trait;
use common::storage::RecordOp;
use common::{Record, Storage, StorageRead};
use roaring::RoaringBitmap;

use crate::index::{ForwardIndexBatchStats, ForwardIndexLookup, InvertedIndex, SeriesSpec};
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

// ── Batched forward index loading ──────────────────────────────────────

/// Minimum contiguous run length to use a range scan instead of individual gets.
const MIN_SCAN_RUN_LEN: u32 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ForwardIndexBatchOp {
    Point(SeriesId),
    Range {
        start: SeriesId,
        end_inclusive: SeriesId,
        count: u32,
    },
}

/// A contiguous run of series IDs in the sorted, deduped input.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ForwardIndexRun {
    start: SeriesId,
    end_inclusive: SeriesId,
    count: u32,
}

/// Build contiguous runs from sorted, deduped series IDs.
fn build_forward_index_runs(sorted_ids: &[SeriesId]) -> Vec<ForwardIndexRun> {
    if sorted_ids.is_empty() {
        return Vec::new();
    }
    let mut runs = Vec::new();
    let mut run_start = sorted_ids[0];
    let mut run_end = sorted_ids[0];
    for &id in &sorted_ids[1..] {
        if run_end.checked_add(1) == Some(id) {
            run_end = id;
        } else {
            runs.push(ForwardIndexRun {
                start: run_start,
                end_inclusive: run_end,
                count: run_end - run_start + 1,
            });
            run_start = id;
            run_end = id;
        }
    }
    runs.push(ForwardIndexRun {
        start: run_start,
        end_inclusive: run_end,
        count: run_end - run_start + 1,
    });
    runs
}

/// Populate run-length and gap histograms from the runs.
fn record_run_and_gap_stats(runs: &[ForwardIndexRun], stats: &mut ForwardIndexBatchStats) {
    for run in runs {
        match run.count {
            1 => stats.run_len_1 += 1,
            2..=3 => stats.run_len_2_3 += 1,
            4..=7 => stats.run_len_4_7 += 1,
            8..=15 => stats.run_len_8_15 += 1,
            _ => stats.run_len_16_plus += 1,
        }
    }
    for pair in runs.windows(2) {
        // Runs are sorted and non-overlapping, so next.start > prev.end_inclusive.
        debug_assert!(pair[1].start > pair[0].end_inclusive);
        let gap = pair[1]
            .start
            .saturating_sub(pair[0].end_inclusive)
            .saturating_sub(1);
        match gap {
            0 => {} // adjacent (no gap) — shouldn't happen with distinct runs, but defensive
            1 => stats.gap_1 += 1,
            2..=3 => stats.gap_2_3 += 1,
            4..=7 => stats.gap_4_7 += 1,
            8..=15 => stats.gap_8_15 += 1,
            _ => stats.gap_16_plus += 1,
        }
    }
}

/// Convert runs to batch ops, using range scans only for runs >= min_scan_run_len.
fn plan_ops_from_runs(runs: &[ForwardIndexRun], min_scan_run_len: u32) -> Vec<ForwardIndexBatchOp> {
    let mut ops = Vec::new();
    for run in runs {
        if run.count >= min_scan_run_len {
            ops.push(ForwardIndexBatchOp::Range {
                start: run.start,
                end_inclusive: run.end_inclusive,
                count: run.count,
            });
        } else {
            for id in run.start..=run.end_inclusive {
                ops.push(ForwardIndexBatchOp::Point(id));
            }
        }
    }
    ops
}

/// Forward index loaded from storage, carrying batch loading statistics.
/// Wraps ForwardIndex to keep telemetry out of the logical data structure.
pub(crate) struct LoadedForwardIndex {
    pub(crate) inner: ForwardIndex,
    pub(crate) batch_stats: ForwardIndexBatchStats,
}

impl ForwardIndexLookup for LoadedForwardIndex {
    fn get_spec(&self, series_id: &SeriesId) -> Option<SeriesSpec> {
        self.inner.get_spec(series_id)
    }

    fn all_series(&self) -> Vec<(SeriesId, SeriesSpec)> {
        self.inner.all_series()
    }

    fn batch_stats(&self) -> ForwardIndexBatchStats {
        self.batch_stats.clone()
    }
}

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
            if let Some(record) = self.get(key).await? {
                let value = InvertedIndexValue::decode(record.value.as_ref())?;
                result.postings.insert(term.clone(), value.postings);
            }
        }
        Ok(result)
    }

    /// Load only the specified series from the forward index using batched operations.
    /// Contiguous runs of series IDs are loaded with a single scan() instead of N get() calls.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_forward_index_series(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<LoadedForwardIndex> {
        let total_requested = series_ids.len() as u32;

        // Sort and dedup
        let mut sorted_ids: Vec<SeriesId> = series_ids.to_vec();
        sorted_ids.sort();
        sorted_ids.dedup();
        let unique_count = sorted_ids.len() as u32;

        let runs = build_forward_index_runs(&sorted_ids);
        let result = ForwardIndex::default();
        let mut stats = ForwardIndexBatchStats {
            total_series_requested: total_requested,
            unique_series: unique_count,
            ..Default::default()
        };
        record_run_and_gap_stats(&runs, &mut stats);
        let ops = plan_ops_from_runs(&runs, MIN_SCAN_RUN_LEN);
        stats.batch_ops = ops.len() as u32;

        for op in &ops {
            match op {
                ForwardIndexBatchOp::Point(series_id) => {
                    stats.point_lookups += 1;
                    let key = ForwardIndexKey {
                        time_bucket: bucket.start,
                        bucket_size: bucket.size,
                        series_id: *series_id,
                    }
                    .encode();
                    if let Some(record) = self.get(key).await? {
                        let value = ForwardIndexValue::decode(record.value.as_ref())?;
                        result.series.insert(*series_id, value.into());
                    }
                }
                ForwardIndexBatchOp::Range {
                    start,
                    end_inclusive,
                    count,
                } => {
                    stats.range_scans += 1;
                    stats.range_scan_series += count;
                    stats.scan_span_series += end_inclusive - start + 1;
                    let range = ForwardIndexKey::series_range(bucket, *start, *end_inclusive);
                    let records = self.scan(range).await?;
                    for record in records {
                        let key = ForwardIndexKey::decode(record.key.as_ref())?;
                        let value = ForwardIndexValue::decode(record.value.as_ref())?;
                        result.series.insert(key.series_id, value.into());
                    }
                }
            }
        }

        tracing::trace!(
            total_requested,
            unique_series = unique_count,
            batch_ops = stats.batch_ops,
            point_lookups = stats.point_lookups,
            range_scans = stats.range_scans,
            range_scan_series = stats.range_scan_series,
            scan_span_series = stats.scan_span_series,
            run_len_1 = stats.run_len_1,
            run_len_2_3 = stats.run_len_2_3,
            run_len_4_7 = stats.run_len_4_7,
            run_len_8_15 = stats.run_len_8_15,
            run_len_16_plus = stats.run_len_16_plus,
            gap_1 = stats.gap_1,
            gap_2_3 = stats.gap_2_3,
            gap_4_7 = stats.gap_4_7,
            gap_8_15 = stats.gap_8_15,
            gap_16_plus = stats.gap_16_plus,
            "forward index batch load"
        );

        Ok(LoadedForwardIndex {
            inner: result,
            batch_stats: stats,
        })
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

    // ── build_forward_index_runs tests ─────────────────────────────────

    #[test]
    fn runs_empty() {
        assert!(build_forward_index_runs(&[]).is_empty());
    }

    #[test]
    fn runs_single() {
        assert_eq!(
            build_forward_index_runs(&[42]),
            vec![ForwardIndexRun {
                start: 42,
                end_inclusive: 42,
                count: 1
            }]
        );
    }

    #[test]
    fn runs_contiguous() {
        assert_eq!(
            build_forward_index_runs(&[10, 11, 12]),
            vec![ForwardIndexRun {
                start: 10,
                end_inclusive: 12,
                count: 3
            }]
        );
    }

    #[test]
    fn runs_gap() {
        assert_eq!(
            build_forward_index_runs(&[10, 12, 13]),
            vec![
                ForwardIndexRun {
                    start: 10,
                    end_inclusive: 10,
                    count: 1
                },
                ForwardIndexRun {
                    start: 12,
                    end_inclusive: 13,
                    count: 2
                },
            ]
        );
    }

    #[test]
    fn runs_u32_max_contiguous() {
        assert_eq!(
            build_forward_index_runs(&[u32::MAX - 1, u32::MAX]),
            vec![ForwardIndexRun {
                start: u32::MAX - 1,
                end_inclusive: u32::MAX,
                count: 2
            }]
        );
    }

    // ── plan_ops_from_runs tests (with MIN_SCAN_RUN_LEN threshold) ───

    fn plan(sorted_ids: &[SeriesId]) -> Vec<ForwardIndexBatchOp> {
        let runs = build_forward_index_runs(sorted_ids);
        plan_ops_from_runs(&runs, MIN_SCAN_RUN_LEN)
    }

    #[test]
    fn plan_empty() {
        assert!(plan(&[]).is_empty());
    }

    #[test]
    fn plan_single_point() {
        assert_eq!(plan(&[42]), vec![ForwardIndexBatchOp::Point(42)]);
    }

    #[test]
    fn plan_run_of_3_becomes_points() {
        // Run of 3 < MIN_SCAN_RUN_LEN=8, so each becomes a Point
        assert_eq!(
            plan(&[10, 11, 12]),
            vec![
                ForwardIndexBatchOp::Point(10),
                ForwardIndexBatchOp::Point(11),
                ForwardIndexBatchOp::Point(12),
            ]
        );
    }

    #[test]
    fn plan_run_of_7_becomes_points() {
        assert_eq!(
            plan(&[1, 2, 3, 4, 5, 6, 7]),
            (1..=7).map(ForwardIndexBatchOp::Point).collect::<Vec<_>>()
        );
    }

    #[test]
    fn plan_run_of_8_becomes_range() {
        assert_eq!(
            plan(&[1, 2, 3, 4, 5, 6, 7, 8]),
            vec![ForwardIndexBatchOp::Range {
                start: 1,
                end_inclusive: 8,
                count: 8
            }]
        );
    }

    #[test]
    fn plan_mixed_short_and_long() {
        // [1,2,3] = run of 3 (< 8) -> 3 Points
        // [10..=17] = run of 8 (>= 8) -> 1 Range
        let mut ids: Vec<u32> = vec![1, 2, 3];
        ids.extend(10..=17);
        let ops = plan(&ids);
        assert_eq!(
            ops,
            vec![
                ForwardIndexBatchOp::Point(1),
                ForwardIndexBatchOp::Point(2),
                ForwardIndexBatchOp::Point(3),
                ForwardIndexBatchOp::Range {
                    start: 10,
                    end_inclusive: 17,
                    count: 8
                },
            ]
        );
    }

    #[test]
    fn plan_mixed_long_point_short() {
        // [1..=10] = range, [20] = point, [30,31,32] = 3 points
        let mut ids: Vec<u32> = (1..=10).collect();
        ids.push(20);
        ids.extend(30..=32);
        let ops = plan(&ids);
        assert_eq!(
            ops,
            vec![
                ForwardIndexBatchOp::Range {
                    start: 1,
                    end_inclusive: 10,
                    count: 10
                },
                ForwardIndexBatchOp::Point(20),
                ForwardIndexBatchOp::Point(30),
                ForwardIndexBatchOp::Point(31),
                ForwardIndexBatchOp::Point(32),
            ]
        );
    }

    #[test]
    fn plan_all_isolated() {
        assert_eq!(
            plan(&[1, 5, 100]),
            vec![
                ForwardIndexBatchOp::Point(1),
                ForwardIndexBatchOp::Point(5),
                ForwardIndexBatchOp::Point(100),
            ]
        );
    }

    // ── record_run_and_gap_stats tests ───────────────────────────────

    #[test]
    fn stats_run_histogram() {
        // [1,2,3] = run of 3, [5..=12] = run of 8
        let runs = build_forward_index_runs(&{
            let mut ids: Vec<u32> = vec![1, 2, 3];
            ids.extend(5..=12);
            ids
        });
        let mut stats = ForwardIndexBatchStats::default();
        record_run_and_gap_stats(&runs, &mut stats);
        assert_eq!(stats.run_len_2_3, 1);
        assert_eq!(stats.run_len_8_15, 1);
        assert_eq!(stats.run_len_1, 0);
        assert_eq!(stats.gap_1, 1); // gap between 3 and 5 = 1
    }

    #[test]
    fn stats_all_isolated() {
        let runs = build_forward_index_runs(&[1, 10, 20]);
        let mut stats = ForwardIndexBatchStats::default();
        record_run_and_gap_stats(&runs, &mut stats);
        assert_eq!(stats.run_len_1, 3);
        assert_eq!(stats.gap_8_15, 2); // gaps of 8 and 9
    }

    #[test]
    fn stats_single_long_run() {
        let ids: Vec<u32> = (1..=17).collect();
        let runs = build_forward_index_runs(&ids);
        let mut stats = ForwardIndexBatchStats::default();
        record_run_and_gap_stats(&runs, &mut stats);
        assert_eq!(stats.run_len_16_plus, 1);
        assert_eq!(stats.gap_1, 0);
    }

    #[test]
    fn stats_gap_buckets() {
        // Gaps: 1, 3, 5, 10, 20
        let runs = build_forward_index_runs(&[1, 3, 7, 13, 24, 45]);
        let mut stats = ForwardIndexBatchStats::default();
        record_run_and_gap_stats(&runs, &mut stats);
        assert_eq!(stats.run_len_1, 6);
        assert_eq!(stats.gap_1, 1); // gap=1 (1→3)
        assert_eq!(stats.gap_2_3, 1); // gap=3 (3→7)
        assert_eq!(stats.gap_4_7, 1); // gap=5 (7→13)
        assert_eq!(stats.gap_8_15, 1); // gap=10 (13→24)
        assert_eq!(stats.gap_16_plus, 1); // gap=20 (24→45)
    }

    // ── Integration tests for batched get_forward_index_series ─────────

    fn make_series_spec(label_value: &str) -> SeriesSpec {
        SeriesSpec {
            unit: None,
            metric_type: None,
            labels: vec![crate::model::Label {
                name: "test".to_string(),
                value: label_value.to_string(),
            }],
        }
    }

    async fn storage_with_forward_index(
        series: &[(SeriesId, &str)],
        bucket: &TimeBucket,
    ) -> Arc<InMemoryStorage> {
        let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )));
        let ops: Vec<_> = series
            .iter()
            .map(|&(sid, label)| {
                let spec = make_series_spec(label);
                storage.insert_forward_index(*bucket, sid, spec).unwrap()
            })
            .collect();
        let put_ops: Vec<_> = ops
            .into_iter()
            .map(|op| match op {
                RecordOp::Put(p) => p,
                _ => panic!("expected Put"),
            })
            .collect();
        storage.put(put_ops).await.unwrap();
        storage
    }

    #[tokio::test]
    async fn batched_fi_mixed_ops() {
        let bucket = TimeBucket {
            start: 100,
            size: 1,
        };
        let s = storage_with_forward_index(
            &[(1, "a"), (2, "b"), (3, "c"), (5, "e"), (10, "j")],
            &bucket,
        )
        .await;
        let snapshot = s.snapshot().await.unwrap();
        let result = snapshot
            .get_forward_index_series(&bucket, &[1, 2, 3, 5, 10])
            .await
            .unwrap();
        // All 5 should be present
        assert_eq!(result.inner.series.len(), 5);
        // With MIN_SCAN_RUN_LEN=8: [1,2,3] run of 3 < 8 -> 3 points, [5] point, [10] point = 5 points total
        assert_eq!(result.batch_stats.range_scans, 0);
        assert_eq!(result.batch_stats.point_lookups, 5);
        assert_eq!(result.batch_stats.batch_ops, 5);
    }

    #[tokio::test]
    async fn batched_fi_missing_ids() {
        let bucket = TimeBucket {
            start: 100,
            size: 1,
        };
        let s = storage_with_forward_index(&[(2, "b")], &bucket).await;
        let snapshot = s.snapshot().await.unwrap();
        let result = snapshot
            .get_forward_index_series(&bucket, &[1, 2, 3])
            .await
            .unwrap();
        // Only series 2 exists
        assert_eq!(result.inner.series.len(), 1);
        assert!(result.inner.series.get(&2).is_some());
    }

    #[tokio::test]
    async fn batched_fi_dedup() {
        let bucket = TimeBucket {
            start: 100,
            size: 1,
        };
        let s = storage_with_forward_index(&[(1, "a")], &bucket).await;
        let snapshot = s.snapshot().await.unwrap();
        let result = snapshot
            .get_forward_index_series(&bucket, &[1, 1, 1])
            .await
            .unwrap();
        assert_eq!(result.inner.series.len(), 1);
        assert_eq!(result.batch_stats.unique_series, 1);
        assert_eq!(result.batch_stats.total_series_requested, 3);
        assert_eq!(result.batch_stats.point_lookups, 1);
    }

    #[tokio::test]
    async fn batched_fi_empty_input() {
        let bucket = TimeBucket {
            start: 100,
            size: 1,
        };
        let s = storage_with_forward_index(&[], &bucket).await;
        let snapshot = s.snapshot().await.unwrap();
        let result = snapshot
            .get_forward_index_series(&bucket, &[])
            .await
            .unwrap();
        assert_eq!(result.inner.series.len(), 0);
        assert_eq!(result.batch_stats.batch_ops, 0);
    }

    #[tokio::test]
    async fn batched_fi_dense_run_below_threshold() {
        // Run of 5 < MIN_SCAN_RUN_LEN=8, so all become point lookups
        let bucket = TimeBucket {
            start: 100,
            size: 1,
        };
        let s = storage_with_forward_index(
            &[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")],
            &bucket,
        )
        .await;
        let snapshot = s.snapshot().await.unwrap();
        let result = snapshot
            .get_forward_index_series(&bucket, &[1, 2, 3, 4, 5])
            .await
            .unwrap();
        assert_eq!(result.inner.series.len(), 5);
        assert_eq!(result.batch_stats.range_scans, 0);
        assert_eq!(result.batch_stats.point_lookups, 5);
        assert_eq!(result.batch_stats.unique_series, 5);
        assert_eq!(result.batch_stats.batch_ops, 5);
        assert_eq!(result.batch_stats.range_scan_series, 0);
        // Run histogram: one run of 5 -> run_len_4_7
        assert_eq!(result.batch_stats.run_len_4_7, 1);
    }

    #[tokio::test]
    async fn batched_fi_dense_run_meets_threshold() {
        // Run of 8 >= MIN_SCAN_RUN_LEN=8, so becomes a range scan
        let bucket = TimeBucket {
            start: 100,
            size: 1,
        };
        let series: Vec<(SeriesId, &str)> = (1..=8).map(|i| (i, "x")).collect();
        let s = storage_with_forward_index(&series, &bucket).await;
        let snapshot = s.snapshot().await.unwrap();
        let ids: Vec<u32> = (1..=8).collect();
        let result = snapshot
            .get_forward_index_series(&bucket, &ids)
            .await
            .unwrap();
        assert_eq!(result.inner.series.len(), 8);
        assert_eq!(result.batch_stats.range_scans, 1);
        assert_eq!(result.batch_stats.point_lookups, 0);
        assert_eq!(result.batch_stats.batch_ops, 1);
        assert_eq!(result.batch_stats.range_scan_series, 8);
        assert_eq!(result.batch_stats.scan_span_series, 8);
        assert_eq!(result.batch_stats.run_len_8_15, 1);
    }

    #[tokio::test]
    async fn batched_fi_equivalence_with_individual_gets() {
        // Verify batched results match N individual lookups
        let bucket = TimeBucket {
            start: 100,
            size: 1,
        };
        let s = storage_with_forward_index(
            &[(1, "a"), (2, "b"), (5, "e"), (6, "f"), (10, "j")],
            &bucket,
        )
        .await;
        let snapshot = s.snapshot().await.unwrap();
        let batched = snapshot
            .get_forward_index_series(&bucket, &[1, 2, 5, 6, 10])
            .await
            .unwrap();

        // Individual lookups
        for &sid in &[1u32, 2, 5, 6, 10] {
            let individual = snapshot
                .get_forward_index_series(&bucket, &[sid])
                .await
                .unwrap();
            let batched_spec = batched.inner.get_spec(&sid);
            let individual_spec = individual.inner.get_spec(&sid);
            assert_eq!(
                batched_spec.is_some(),
                individual_spec.is_some(),
                "mismatch for sid={}",
                sid
            );
            if let (Some(b), Some(i)) = (batched_spec, individual_spec) {
                assert_eq!(b.labels, i.labels, "label mismatch for sid={}", sid);
            }
        }
    }
}

use std::sync::Arc;

use bytes::Bytes;
use common::StorageError;
use common::default_scan_options;
use roaring::RoaringBitmap;
use slatedb::{DbRead, WriteBatch};

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

/// Given a time range, return all the time buckets that contain data for
/// that range sorted by start time.
///
/// This method examines the actual list of buckets in storage to determine the
/// candidate buckets (as opposed to computing theoretical buckets from the
/// start and end times).
#[tracing::instrument(level = "trace", skip_all)]
pub(crate) async fn get_buckets_in_range<R>(
    db: &R,
    start_secs: Option<i64>,
    end_secs: Option<i64>,
) -> Result<Vec<TimeBucket>>
where
    R: DbRead + Send + Sync,
{
    if let (Some(start), Some(end)) = (start_secs, end_secs)
        && end < start
    {
        return Err("end must be greater than or equal to start".into());
    }

    // Convert to minutes once before filtering
    let start_min = start_secs.map(|s| (s / 60) as u32);
    let end_min = end_secs.map(|e| (e / 60) as u32);

    let key = BucketListKey.encode();
    let record = db.get(&key).await.map_err(StorageError::from_storage)?;
    let bucket_list = match record {
        Some(record) => BucketListValue::decode(record.as_ref())?,
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
pub(crate) async fn get_buckets_for_ranges<R>(
    db: &R,
    ranges: &[(i64, i64)],
) -> Result<Vec<TimeBucket>>
where
    R: DbRead + Send + Sync,
{
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let key = BucketListKey.encode();
    let record = db.get(&key).await.map_err(StorageError::from_storage)?;
    let bucket_list = match record {
        Some(record) => BucketListValue::decode(record.as_ref())?,
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
            ranges
                .iter()
                .any(|&(r_start, r_end)| bucket_end_secs > r_start && bucket_start_secs <= r_end)
        })
        .collect();

    filtered_buckets.sort_by_key(|bucket| bucket.start);
    Ok(filtered_buckets)
}

#[tracing::instrument(level = "trace", skip_all)]
pub(crate) async fn get_forward_index<R>(db: &R, bucket: TimeBucket) -> Result<ForwardIndex>
where
    R: DbRead + Send + Sync,
{
    let range = ForwardIndexKey::bucket_range(&bucket);
    let mut iter = db
        .scan_with_options(range, &default_scan_options())
        .await
        .map_err(StorageError::from_storage)?;

    let forward_index = ForwardIndex::default();
    while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
        let key = ForwardIndexKey::decode(record.key.as_ref())?;
        let value = ForwardIndexValue::decode(record.value.as_ref())?;
        forward_index.series.insert(key.series_id, value.into());
    }

    Ok(forward_index)
}

#[tracing::instrument(level = "trace", skip_all)]
pub(crate) async fn get_inverted_index<R>(db: &R, bucket: TimeBucket) -> Result<InvertedIndex>
where
    R: DbRead + Send + Sync,
{
    let range = InvertedIndexKey::bucket_range(&bucket);
    let mut iter = db
        .scan_with_options(range, &default_scan_options())
        .await
        .map_err(StorageError::from_storage)?;

    let inverted_index = InvertedIndex::default();
    while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
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
/// calls it. New callers should use [`get_inverted_index_term`]
/// and fan out in parallel themselves so each per-term latency is
/// independently traceable.
#[tracing::instrument(level = "trace", skip_all)]
pub(crate) async fn get_inverted_index_terms<R>(
    db: &R,
    bucket: &TimeBucket,
    terms: &[Label],
) -> Result<InvertedIndex>
where
    R: DbRead + Send + Sync,
{
    let result = InvertedIndex::default();
    for term in terms {
        if let Some(postings) = get_inverted_index_term(db, bucket, term).await? {
            result.postings.insert(term.clone(), postings);
        }
    }
    Ok(result)
}

/// Load only the specified series from the forward index. Legacy
/// batch path — see [`get_inverted_index_terms`] for context.
/// New callers should use [`get_forward_index_one`].
#[tracing::instrument(level = "trace", skip_all)]
pub(crate) async fn get_forward_index_series<R>(
    db: &R,
    bucket: &TimeBucket,
    series_ids: &[SeriesId],
) -> Result<ForwardIndex>
where
    R: DbRead + Send + Sync,
{
    let result = ForwardIndex::default();
    for &series_id in series_ids {
        if let Some(spec) = get_forward_index_one(db, bucket, series_id).await? {
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
pub(crate) async fn get_inverted_index_term<R>(
    db: &R,
    bucket: &TimeBucket,
    term: &Label,
) -> Result<Option<RoaringBitmap>>
where
    R: DbRead + Send + Sync,
{
    let key = InvertedIndexKey {
        time_bucket: bucket.start,
        bucket_size: bucket.size,
        attribute: term.name.clone(),
        value: term.value.clone(),
    }
    .encode();
    let rec = db.get(&key).await.map_err(StorageError::from_storage)?;
    match rec {
        Some(r) => {
            crate::promql::trace::record_bytes(
                crate::promql::trace::IoKind::InvertedIndexFetch,
                r.len() as u64,
            );
            let value = InvertedIndexValue::decode(r.as_ref())?;
            Ok(Some(value.postings))
        }
        None => Ok(None),
    }
}

/// Fetch a single forward-index entry for `(bucket, series_id)`.
/// Returns `None` when the series isn't present in the bucket.
/// See [`get_inverted_index_term`] for why this is per-key.
#[tracing::instrument(level = "trace", skip_all)]
pub(crate) async fn get_forward_index_one<R>(
    db: &R,
    bucket: &TimeBucket,
    series_id: SeriesId,
) -> Result<Option<SeriesSpec>>
where
    R: DbRead + Send + Sync,
{
    let key = ForwardIndexKey {
        time_bucket: bucket.start,
        bucket_size: bucket.size,
        series_id,
    }
    .encode();
    let rec = db.get(&key).await.map_err(StorageError::from_storage)?;
    match rec {
        Some(r) => {
            crate::promql::trace::record_bytes(
                crate::promql::trace::IoKind::ForwardIndexFetch,
                r.len() as u64,
            );
            let value = ForwardIndexValue::decode(r.as_ref())?;
            Ok(Some(value.into()))
        }
        None => Ok(None),
    }
}

/// Load the series dictionary using the provided insert function and
/// return the maximum series ID found, which can be used to
/// initialize counters
#[tracing::instrument(level = "trace", skip(db, bucket, insert))]
pub(crate) async fn load_series_dictionary<R, F>(
    db: &R,
    bucket: &TimeBucket,
    mut insert: F,
) -> Result<u32>
where
    R: DbRead + Send + Sync,
    F: FnMut(SeriesFingerprint, SeriesId) + Send,
{
    let range = SeriesDictionaryKey::bucket_range(bucket);
    let mut iter = db
        .scan_with_options(range, &default_scan_options())
        .await
        .map_err(StorageError::from_storage)?;

    let mut max_series_id = 0;
    while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
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
pub(crate) async fn bucket_list_contains<R>(db: &R, bucket: TimeBucket) -> Result<bool>
where
    R: DbRead + Send + Sync,
{
    let key = BucketListKey.encode();
    let Some(record) = db.get(&key).await.map_err(StorageError::from_storage)? else {
        return Ok(false);
    };
    let list = BucketListValue::decode(record.as_ref())?;
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
pub(crate) async fn get_label_values<R>(
    db: &R,
    bucket: &TimeBucket,
    label_name: &str,
) -> Result<Vec<String>>
where
    R: DbRead + Send + Sync,
{
    let range = InvertedIndexKey::attribute_range(bucket, label_name);
    let mut iter = db
        .scan_with_options(range, &default_scan_options())
        .await
        .map_err(StorageError::from_storage)?;

    let mut values = Vec::new();
    while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
        let key = InvertedIndexKey::decode(record.key.as_ref())?;
        values.push(key.value);
    }

    Ok(values)
}

// ── Batch write helpers (used by the flusher) ────────────────────────

/// A pair of (key, value) bytes that can be merged or put into a SlateDB write batch.
pub(crate) struct KvPair {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

pub(crate) fn merge_bucket_list_kv(bucket: TimeBucket) -> Result<KvPair> {
    let key = BucketListKey.encode();
    let value = BucketListValue {
        buckets: vec![(bucket.size, bucket.start)],
    }
    .encode();

    Ok(KvPair { key, value })
}

pub(crate) fn insert_series_id_kv(
    bucket: TimeBucket,
    fingerprint: SeriesFingerprint,
    id: SeriesId,
) -> Result<KvPair> {
    let key = SeriesDictionaryKey {
        time_bucket: bucket.start,
        bucket_size: bucket.size,
        series_fingerprint: fingerprint,
    }
    .encode();
    let value = SeriesDictionaryValue { series_id: id }.encode();
    Ok(KvPair { key, value })
}

pub(crate) fn insert_forward_index_kv(
    bucket: TimeBucket,
    series_id: SeriesId,
    series_spec: SeriesSpec,
) -> Result<KvPair> {
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
    Ok(KvPair { key, value })
}

pub(crate) fn merge_inverted_index_kv(
    bucket: TimeBucket,
    label: Label,
    postings: RoaringBitmap,
) -> Result<KvPair> {
    let key = InvertedIndexKey {
        time_bucket: bucket.start,
        bucket_size: bucket.size,
        attribute: label.name,
        value: label.value,
    }
    .encode();
    let value = InvertedIndexValue { postings }.encode()?;
    Ok(KvPair { key, value })
}

pub(crate) fn merge_samples_kv(
    bucket: TimeBucket,
    series_id: SeriesId,
    metric_name: &str,
    samples: Vec<Sample>,
) -> Result<KvPair> {
    let key = TimeSeriesKey {
        time_bucket: bucket.start,
        bucket_size: bucket.size,
        metric_name: metric_name.to_string(),
        series_id,
    }
    .encode();
    let value = TimeSeriesValue { points: samples }.encode()?;
    Ok(KvPair { key, value })
}

/// Read the current `BucketList` value and rewrite it as a single `Put`,
/// collapsing the merge chain that accrues across ingestion batches into
/// one flat record. Intended to run once at startup before ingestion
/// begins, so later reads don't have to replay operands scattered across
/// SSTs. Safe only when there are no concurrent writers.
#[tracing::instrument(level = "info", skip_all)]
pub(crate) async fn coalesce_bucket_list(db: &Arc<slatedb::Db>) -> Result<()> {
    let key = BucketListKey.encode();
    let Some(record) = db.get(&key).await.map_err(StorageError::from_storage)? else {
        return Ok(());
    };
    let mut batch = WriteBatch::new();
    batch.put(key, record);
    db.write(batch).await.map_err(StorageError::from_storage)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use slatedb::Db;
    use slatedb::DbBuilder;
    use slatedb::object_store::memory::InMemory;
    use std::sync::Arc;

    async fn new_in_memory_db() -> Arc<Db> {
        let object_store = Arc::new(InMemory::new());
        let db = DbBuilder::new("test", object_store)
            .with_merge_operator(Arc::new(OpenTsdbMergeOperator))
            .build()
            .await
            .unwrap();
        Arc::new(db)
    }

    /// Create an in-memory Db with the given hour-buckets pre-populated.
    /// Each entry in `bucket_starts` is the bucket start in minutes.
    async fn db_with_buckets(bucket_starts: &[u32]) -> Arc<Db> {
        let db = new_in_memory_db().await;
        let buckets: Vec<(u8, u32)> = bucket_starts.iter().map(|&s| (1u8, s)).collect();
        let key = BucketListKey.encode();
        let value = BucketListValue { buckets }.encode();
        db.put(&key, &value).await.unwrap();
        db
    }

    fn starts(buckets: &[TimeBucket]) -> Vec<u32> {
        buckets.iter().map(|b| b.start).collect()
    }

    // ── get_buckets_for_ranges boundary tests ──────────────────────────

    #[tokio::test]
    async fn ranges_point_at_bucket_start() {
        // Buckets: [0, 3600s), [3600s, 7200s)  (hour buckets at min 0 and 60)
        let db = db_with_buckets(&[0, 60]).await;
        // Point query exactly at second bucket start
        let buckets = get_buckets_for_ranges(db.as_ref(), &[(3600, 3600)])
            .await
            .unwrap();
        // Should match only the bucket starting at 3600s (min 60), not the one ending there
        assert_eq!(starts(&buckets), vec![60]);
    }

    #[tokio::test]
    async fn ranges_point_at_epoch() {
        let db = db_with_buckets(&[0, 60]).await;
        // Point query at t=0
        let buckets = get_buckets_for_ranges(db.as_ref(), &[(0, 0)])
            .await
            .unwrap();
        assert_eq!(starts(&buckets), vec![0]);
    }

    #[tokio::test]
    async fn ranges_spanning_two_buckets() {
        let db = db_with_buckets(&[0, 60, 120]).await;
        // Range [1800, 5400] spans bucket [0,3600) and [3600,7200)
        let buckets = get_buckets_for_ranges(db.as_ref(), &[(1800, 5400)])
            .await
            .unwrap();
        assert_eq!(starts(&buckets), vec![0, 60]);
    }

    #[tokio::test]
    async fn ranges_disjoint_skips_middle() {
        // 3 hour-buckets: [0,3600), [3600,7200), [7200,10800)
        let db = db_with_buckets(&[0, 60, 120]).await;
        // Two disjoint ranges that skip the middle bucket
        let buckets = get_buckets_for_ranges(db.as_ref(), &[(100, 200), (8000, 9000)])
            .await
            .unwrap();
        assert_eq!(starts(&buckets), vec![0, 120]);
    }

    #[tokio::test]
    async fn ranges_empty_returns_nothing() {
        let db = db_with_buckets(&[0, 60]).await;
        let buckets = get_buckets_for_ranges(db.as_ref(), &[]).await.unwrap();
        assert!(buckets.is_empty());
    }

    #[tokio::test]
    async fn ranges_exact_bucket_boundary_excludes_previous() {
        // Range starts exactly at boundary between two buckets.
        // Bucket [0, 3600s) should NOT match range [3600, 7200].
        let db = db_with_buckets(&[0, 60]).await;
        let buckets = get_buckets_for_ranges(db.as_ref(), &[(3600, 7200)])
            .await
            .unwrap();
        assert_eq!(starts(&buckets), vec![60]);
    }

    // ── bucket_list_contains tests ─────────────────────────────────────

    #[tokio::test]
    async fn should_return_true_when_bucket_in_list() {
        // given
        let db = db_with_buckets(&[0, 60, 120]).await;

        // when
        let present = bucket_list_contains(db.as_ref(), TimeBucket { size: 1, start: 60 })
            .await
            .unwrap();

        // then
        assert!(present);
    }

    #[tokio::test]
    async fn should_return_false_when_bucket_absent() {
        // given
        let db = db_with_buckets(&[0, 60]).await;

        // when
        let present = bucket_list_contains(
            db.as_ref(),
            TimeBucket {
                size: 1,
                start: 180,
            },
        )
        .await
        .unwrap();

        // then
        assert!(!present);
    }

    #[tokio::test]
    async fn should_return_false_when_bucket_list_key_missing() {
        // given
        let db = new_in_memory_db().await;

        // when
        let present = bucket_list_contains(db.as_ref(), TimeBucket { size: 1, start: 0 })
            .await
            .unwrap();

        // then
        assert!(!present);
    }

    #[tokio::test]
    async fn should_distinguish_buckets_with_same_start_but_different_size() {
        // given: only a size=1 bucket at start=60 is present
        let db = db_with_buckets(&[60]).await;

        // when
        let size_one = bucket_list_contains(db.as_ref(), TimeBucket { size: 1, start: 60 })
            .await
            .unwrap();
        let size_two = bucket_list_contains(db.as_ref(), TimeBucket { size: 2, start: 60 })
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
        let db = new_in_memory_db().await;
        let mut batch = WriteBatch::new();
        for start in [0u32, 60, 120] {
            let kv = merge_bucket_list_kv(TimeBucket { size: 1, start }).unwrap();
            batch.merge(kv.key, kv.value);
        }
        db.write(batch).await.unwrap();

        // when
        coalesce_bucket_list(&db).await.unwrap();

        // then: readable value still reflects the full merged list
        let buckets = get_buckets_in_range(db.as_ref(), None, None).await.unwrap();
        assert_eq!(starts(&buckets), vec![0, 60, 120]);
    }

    #[tokio::test]
    async fn should_be_noop_when_bucket_list_absent() {
        // given
        let db = new_in_memory_db().await;

        // when
        coalesce_bucket_list(&db).await.unwrap();

        // then: key is still absent
        let record = db.get(&BucketListKey.encode()).await.unwrap();
        assert!(record.is_none());
    }
}

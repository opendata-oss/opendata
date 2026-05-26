//! Native SlateDB-backed storage for timeseries.
//!
//! [`Storage`] owns a `slatedb::Db` and implements [`Store`]: every
//! [`StorageRead`] method plus the write path (batch apply, snapshot, flush).
//! Reads are served by the private [`StorageReaderInner`], which is generic
//! over SlateDB's [`DbReadOps`] so a single implementation of the OpenTSDB
//! read methods (bucket list, forward / inverted index, series dictionary)
//! works against all three SlateDB read handles: the writer `Db` itself, a
//! point-in-time [`StorageSnapshot`] (`DbSnapshot`), and the read-only
//! [`StorageReader`] (`DbReader`). The OpenTSDB record-op builders are free
//! functions — pure encoders that touch no storage state.
//!
//! Value types (`Record`, `RecordOp`, `Ttl`, …), the error type, and the
//! `Ttl`/options → SlateDB conversions are all reused from `common::storage`;
//! only the storage *handles* are native here.

use std::sync::Arc;

use bytes::Bytes;
use common::storage::config::SlateDbStorageConfig;
use common::storage::factory::build_split_cache;
use common::storage::metrics_recorder::MetricsRsRecorder;
use common::storage::slate::SlateDbStorage as CommonSlateDbStorage;
use common::storage::{
    CheckpointInfo, MergeOptions, MergeRecordOp, PutOptions, PutRecordOp, Record, RecordOp,
    StorageError, StorageResult, WriteOptions, WriteResult,
};
use common::{BytesRange, Ttl, create_object_store};
use roaring::RoaringBitmap;
use slatedb::config::{
    CheckpointOptions, CheckpointScope, DbReaderOptions, ScanOptions, Settings,
    WriteOptions as SlateDbWriteOptions,
};
use slatedb::object_store::ObjectStore;
use slatedb::{
    Db, DbBuilder, DbIterator, DbReadOps, DbReader, DbSnapshot, IterationOrder, WriteBatch,
};
use tracing::info;
use uuid::Uuid;

use crate::index::{ForwardIndex, InvertedIndex, SeriesSpec};
use crate::model::{Label, Sample, SeriesFingerprint, SeriesId, TimeBucket};
use crate::serde::TimeBucketScoped;
use crate::serde::bucket_list::BucketListValue;
use crate::serde::dictionary::SeriesDictionaryValue;
use crate::serde::forward_index::ForwardIndexValue;
use crate::serde::inverted_index::InvertedIndexValue;
use crate::serde::key::{
    BucketListKey, ForwardIndexKey, InvertedIndexKey, SeriesDictionaryKey, TimeSeriesKey,
};
use crate::serde::timeseries::TimeSeriesValue;
use crate::storage::merge_operator::OpenTsdbMergeOperator;

/// Private accessor that gives the blanket [`StorageRead`] impl access to a
/// handle's [`StorageReaderInner`] without exposing the inner type outside
/// this module.
trait HasReader {
    type Db: DbReadOps + Send + Sync;
    fn reader(&self) -> &StorageReaderInner<Self::Db>;
}

/// Read operations shared by every storage handle.
///
/// Implemented (via a blanket impl forwarding to the private
/// [`StorageReaderInner`]) by [`Storage`], [`StorageSnapshot`], and
/// [`StorageReader`], so per-bucket readers and background tasks can be
/// generic over the underlying SlateDB read handle. See the methods of the
/// same names on `StorageReaderInner` for the full documentation.
#[async_trait::async_trait]
pub(crate) trait StorageRead: Send + Sync {
    /// Retrieves a single value by exact key. Returns `Ok(None)` if absent.
    async fn get(&self, key: Bytes) -> StorageResult<Option<Bytes>>;

    /// Returns an iterator over the given key range.
    async fn scan(&self, range: BytesRange) -> StorageResult<DbIterator>;

    /// Returns the buckets overlapping `[start_secs, end_secs]`, sorted by
    /// start time.
    async fn get_buckets_in_range(
        &self,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> crate::util::Result<Vec<TimeBucket>>;

    /// Returns the buckets overlapping any of the given disjoint ranges.
    async fn get_buckets_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> crate::util::Result<Vec<TimeBucket>>;

    /// Loads the full forward index of `bucket`.
    async fn get_forward_index(&self, bucket: TimeBucket) -> crate::util::Result<ForwardIndex>;

    /// Loads the full inverted index of `bucket`.
    async fn get_inverted_index(&self, bucket: TimeBucket) -> crate::util::Result<InvertedIndex>;

    /// Loads only the given terms from the inverted index (legacy batch path).
    async fn get_inverted_index_terms(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> crate::util::Result<InvertedIndex>;

    /// Fetches a single inverted-index posting for `(bucket, term)`.
    async fn get_inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> crate::util::Result<Option<RoaringBitmap>>;

    /// Loads only the given series from the forward index (legacy batch path).
    async fn get_forward_index_series(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> crate::util::Result<ForwardIndex>;

    /// Fetches a single forward-index entry for `(bucket, series_id)`.
    async fn get_forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> crate::util::Result<Option<SeriesSpec>>;

    /// Loads the series dictionary of `bucket` through `insert` and returns
    /// the maximum series ID found.
    ///
    /// `Self: Sized` keeps the trait object-safe (for [`Store`]); call this
    /// on a concrete handle.
    async fn load_series_dictionary<F>(
        &self,
        bucket: &TimeBucket,
        insert: F,
    ) -> crate::util::Result<u32>
    where
        F: FnMut(SeriesFingerprint, SeriesId) + Send,
        Self: Sized;

    /// Checks whether `bucket` is present in the stored `BucketList`.
    async fn bucket_list_contains(&self, bucket: TimeBucket) -> crate::util::Result<bool>;

    /// Returns all values of `label_name` within `bucket`.
    async fn get_label_values(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> crate::util::Result<Vec<String>>;
}

#[async_trait::async_trait]
impl<H: HasReader + Send + Sync> StorageRead for H {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Bytes>> {
        self.reader().get(key).await
    }

    async fn scan(&self, range: BytesRange) -> StorageResult<DbIterator> {
        self.reader().scan(range).await
    }

    async fn get_buckets_in_range(
        &self,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> crate::util::Result<Vec<TimeBucket>> {
        self.reader()
            .get_buckets_in_range(start_secs, end_secs)
            .await
    }

    async fn get_buckets_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> crate::util::Result<Vec<TimeBucket>> {
        self.reader().get_buckets_for_ranges(ranges).await
    }

    async fn get_forward_index(&self, bucket: TimeBucket) -> crate::util::Result<ForwardIndex> {
        self.reader().get_forward_index(bucket).await
    }

    async fn get_inverted_index(&self, bucket: TimeBucket) -> crate::util::Result<InvertedIndex> {
        self.reader().get_inverted_index(bucket).await
    }

    async fn get_inverted_index_terms(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> crate::util::Result<InvertedIndex> {
        self.reader().get_inverted_index_terms(bucket, terms).await
    }

    async fn get_inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> crate::util::Result<Option<RoaringBitmap>> {
        self.reader().get_inverted_index_term(bucket, term).await
    }

    async fn get_forward_index_series(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> crate::util::Result<ForwardIndex> {
        self.reader()
            .get_forward_index_series(bucket, series_ids)
            .await
    }

    async fn get_forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> crate::util::Result<Option<SeriesSpec>> {
        self.reader().get_forward_index_one(bucket, series_id).await
    }

    async fn load_series_dictionary<F>(
        &self,
        bucket: &TimeBucket,
        insert: F,
    ) -> crate::util::Result<u32>
    where
        F: FnMut(SeriesFingerprint, SeriesId) + Send,
    {
        self.reader().load_series_dictionary(bucket, insert).await
    }

    async fn bucket_list_contains(&self, bucket: TimeBucket) -> crate::util::Result<bool> {
        self.reader().bucket_list_contains(bucket).await
    }

    async fn get_label_values(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> crate::util::Result<Vec<String>> {
        self.reader().get_label_values(bucket, label_name).await
    }
}

/// The full storage surface — every [`StorageRead`] method plus the write
/// path. This is the flusher's dependency: [`Storage`] is the only production
/// implementation, and the trait exists so tests can substitute a failing
/// implementation and verify that errors from each flush phase are
/// propagated — the concrete SlateDB writer offers no way to inject
/// per-operation faults.
#[async_trait::async_trait]
pub(crate) trait Store: StorageRead {
    /// Applies a batch of mixed operations atomically.
    async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult>;

    /// Creates a point-in-time snapshot for consistent reads.
    async fn snapshot(&self) -> StorageResult<StorageSnapshot>;

    /// Flushes pending writes to durable storage.
    async fn flush(&self) -> StorageResult<()>;
}

/// The shared read side of the storage backend.
///
/// Generic over [`DbReadOps`] so the same OpenTSDB read methods serve the
/// writer `Db`, a `DbSnapshot`, and a `DbReader`. Cloning is cheap (one `Arc`).
///
/// Private to this module: callers go through the [`StorageRead`] methods on
/// [`Storage`], [`StorageSnapshot`], and [`StorageReader`], which forward here.
#[derive(Debug)]
struct StorageReaderInner<T: DbReadOps + Send + Sync> {
    db: Arc<T>,
}

impl<T: DbReadOps + Send + Sync> Clone for StorageReaderInner<T> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

impl<T: DbReadOps + Send + Sync> StorageReaderInner<T> {
    /// Retrieves a single value by exact key. Returns `Ok(None)` if absent.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Bytes>> {
        self.db.get(key).await.map_err(StorageError::from_storage)
    }

    /// Returns an iterator over the given key range.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan(&self, range: BytesRange) -> StorageResult<DbIterator> {
        let scan_options = ScanOptions {
            durability_filter: Default::default(),
            dirty: false,
            read_ahead_bytes: 1024 * 1024,
            cache_blocks: true,
            max_fetch_tasks: 4,
            order: IterationOrder::Ascending,
            filter_context: None,
        };
        self.db
            .scan_with_options(range, &scan_options)
            .await
            .map_err(StorageError::from_storage)
    }

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
    ) -> crate::util::Result<Vec<TimeBucket>> {
        if let (Some(start), Some(end)) = (start_secs, end_secs)
            && end < start
        {
            return Err("end must be greater than or equal to start".into());
        }

        // Convert to minutes once before filtering
        let start_min = start_secs.map(|s| (s / 60) as u32);
        let end_min = end_secs.map(|e| (e / 60) as u32);

        let value = self.get(BucketListKey.encode()).await?;
        let bucket_list = match value {
            Some(value) => BucketListValue::decode(value.as_ref())?,
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
    async fn get_buckets_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> crate::util::Result<Vec<TimeBucket>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let value = self.get(BucketListKey.encode()).await?;
        let bucket_list = match value {
            Some(value) => BucketListValue::decode(value.as_ref())?,
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
    async fn get_forward_index(&self, bucket: TimeBucket) -> crate::util::Result<ForwardIndex> {
        let range = ForwardIndexKey::bucket_range(&bucket);
        let mut iter = self.scan(range).await?;

        let forward_index = ForwardIndex::default();
        while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
            let key = ForwardIndexKey::decode(record.key.as_ref())?;
            let value = ForwardIndexValue::decode(record.value.as_ref())?;
            forward_index.series.insert(key.series_id, value.into());
        }
        Ok(forward_index)
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_inverted_index(&self, bucket: TimeBucket) -> crate::util::Result<InvertedIndex> {
        let range = InvertedIndexKey::bucket_range(&bucket);
        let mut iter = self.scan(range).await?;

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
    /// calls it. New callers should use [`Self::get_inverted_index_term`]
    /// and fan out in parallel themselves so each per-term latency is
    /// independently traceable.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_inverted_index_terms(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> crate::util::Result<InvertedIndex> {
        let result = InvertedIndex::default();
        for term in terms {
            if let Some(postings) = self.get_inverted_index_term(bucket, term).await? {
                result.postings.insert(term.clone(), postings);
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
    ) -> crate::util::Result<Option<RoaringBitmap>> {
        let key = InvertedIndexKey {
            bucket: *bucket,
            attribute: term.name.clone(),
            value: term.value.clone(),
        }
        .encode();

        match self.get(key).await? {
            Some(value) => {
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::InvertedIndexFetch,
                    value.len() as u64,
                );
                let inverted_index_value = InvertedIndexValue::decode(value.as_ref())?;
                Ok(Some(inverted_index_value.postings))
            }
            None => Ok(None),
        }
    }

    /// Load only the specified series from the forward index. Legacy
    /// batch path — see [`Self::get_inverted_index_terms`] for context.
    /// New callers should use [`Self::get_forward_index_one`].
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_forward_index_series(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> crate::util::Result<ForwardIndex> {
        let result = ForwardIndex::default();
        for &series_id in series_ids {
            if let Some(spec) = self.get_forward_index_one(bucket, series_id).await? {
                result.series.insert(series_id, spec);
            }
        }
        Ok(result)
    }

    /// Fetch a single forward-index entry for `(bucket, series_id)`.
    /// Returns `None` when the series isn't present in the bucket.
    /// See [`Self::get_inverted_index_term`] for why this is per-key.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get_forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> crate::util::Result<Option<SeriesSpec>> {
        let key = ForwardIndexKey {
            bucket: *bucket,
            series_id,
        }
        .encode();

        match self.get(key).await? {
            Some(value) => {
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::ForwardIndexFetch,
                    value.len() as u64,
                );
                let forward_index_value = ForwardIndexValue::decode(value.as_ref())?;
                Ok(Some(forward_index_value.into()))
            }
            None => Ok(None),
        }
    }

    /// Load the series dictionary using the provided insert function and
    /// return the maximum series ID found, which can be used to
    /// initialize counters
    #[tracing::instrument(level = "trace", skip(self, bucket, insert))]
    async fn load_series_dictionary<F>(
        &self,
        bucket: &TimeBucket,
        mut insert: F,
    ) -> crate::util::Result<u32>
    where
        F: FnMut(SeriesFingerprint, SeriesId) + Send,
    {
        let range = SeriesDictionaryKey::bucket_range(bucket);
        let mut iter = self.scan(range).await?;

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
    async fn bucket_list_contains(&self, bucket: TimeBucket) -> crate::util::Result<bool> {
        let Some(value) = self.get(BucketListKey.encode()).await? else {
            return Ok(false);
        };
        let list = BucketListValue::decode(value.as_ref())?;
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
    async fn get_label_values(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> crate::util::Result<Vec<String>> {
        let range = InvertedIndexKey::attribute_range(bucket, label_name);
        let mut iter = self.scan(range).await?;

        let mut values = Vec::new();
        while let Some(record) = iter.next().await.map_err(StorageError::from_storage)? {
            let key = InvertedIndexKey::decode(record.key.as_ref())?;
            values.push(key.value);
        }

        Ok(values)
    }
}

/// Read-only storage using SlateDB's `DbReader`.
///
/// Provides read-only access without fencing, so multiple readers can coexist
/// with a single writer.
#[derive(Clone)]
pub(crate) struct StorageReader {
    reader: StorageReaderInner<DbReader>,
}

impl StorageReader {
    /// Builds a reader from configuration, wired with the OpenTSDB merge
    /// operator, the metrics recorder, and (when configured) the foyer block
    /// cache. When `checkpoint_id` is set, the reader is pinned to that
    /// checkpoint and does not advance with newer writes.
    pub(crate) async fn try_new(
        slate_config: &SlateDbStorageConfig,
        reader_options: DbReaderOptions,
        checkpoint_id: Option<Uuid>,
    ) -> crate::util::Result<Self> {
        let object_store = create_object_store(&slate_config.object_store)?;
        Self::try_new_with_object_store(slate_config, reader_options, checkpoint_id, object_store)
            .await
    }

    /// Like [`Self::try_new`] but over an explicit object store, so tests can
    /// share an in-memory store between a writer and a reader.
    pub(crate) async fn try_new_with_object_store(
        slate_config: &SlateDbStorageConfig,
        reader_options: DbReaderOptions,
        checkpoint_id: Option<Uuid>,
        object_store: Arc<dyn ObjectStore>,
    ) -> crate::util::Result<Self> {
        let adapter = CommonSlateDbStorage::merge_operator_adapter(Arc::new(OpenTsdbMergeOperator));
        let mut builder = DbReader::builder(slate_config.path.clone(), object_store)
            .with_options(reader_options)
            .with_merge_operator(Arc::new(adapter))
            .with_metrics_recorder(Arc::new(MetricsRsRecorder));

        if let Some(checkpoint_id) = checkpoint_id {
            builder = builder.with_checkpoint_id(checkpoint_id);
        }

        if let Some(cache) =
            build_split_cache(&slate_config.block_cache, &slate_config.meta_cache).await?
        {
            builder = builder.with_db_cache(cache);
        }

        let reader = builder.build().await.map_err(|e| {
            StorageError::Storage(format!("Failed to create SlateDB reader: {}", e))
        })?;

        Ok(StorageReader {
            reader: StorageReaderInner {
                db: Arc::new(reader),
            },
        })
    }

    /// Closes the underlying `DbReader` (which also closes the block cache).
    pub(crate) async fn close(&self) -> StorageResult<()> {
        self.reader
            .db
            .close()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(())
    }
}

impl HasReader for StorageReader {
    type Db = DbReader;
    fn reader(&self) -> &StorageReaderInner<DbReader> {
        &self.reader
    }
}

/// A consistent point-in-time read view of the storage, wrapping a SlateDB
/// `DbSnapshot`. Reads go through [`StorageRead`].
#[derive(Clone)]
pub(crate) struct StorageSnapshot {
    reader: StorageReaderInner<DbSnapshot>,
}

impl HasReader for StorageSnapshot {
    type Db = DbSnapshot;
    fn reader(&self) -> &StorageReaderInner<DbSnapshot> {
        &self.reader
    }
}

/// Read/write SlateDB-backed storage.
///
/// SlateDB is an embedded key-value store built on object storage, providing
/// LSM-tree semantics with cloud-native durability. Reads go through
/// [`StorageRead`]; cloning is cheap (the handles are `Arc`s).
#[derive(Clone)]
pub(crate) struct Storage {
    db: Arc<Db>,
    reader: StorageReaderInner<Db>,
}

impl HasReader for Storage {
    type Db = Db;
    fn reader(&self) -> &StorageReaderInner<Db> {
        &self.reader
    }
}

impl Storage {
    /// Opens the storage from configuration, wired with the OpenTSDB merge
    /// operator, the metrics recorder, and (when configured) the foyer block
    /// cache. Coalesces the bucket list before returning, while the freshly
    /// fenced writer is still the only process that can write.
    pub(crate) async fn try_new(slate_config: &SlateDbStorageConfig) -> crate::util::Result<Self> {
        let object_store = create_object_store(&slate_config.object_store)?;
        Self::try_new_with_object_store(slate_config, object_store).await
    }

    /// Like [`Self::try_new`] but over an explicit object store, so tests can
    /// share an in-memory store between a writer and a reader.
    pub(crate) async fn try_new_with_object_store(
        slate_config: &SlateDbStorageConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> crate::util::Result<Self> {
        let settings = load_settings(slate_config)?;
        info!(
            "create slatedb storage with config: {:?}, settings: {:?}",
            slate_config, settings
        );

        let adapter = CommonSlateDbStorage::merge_operator_adapter(Arc::new(OpenTsdbMergeOperator));
        let mut builder = DbBuilder::new(slate_config.path.clone(), object_store)
            .with_settings(settings)
            .with_merge_operator(Arc::new(adapter))
            .with_metrics_recorder(Arc::new(MetricsRsRecorder));

        if let Some(cache) =
            build_split_cache(&slate_config.block_cache, &slate_config.meta_cache).await?
        {
            builder = builder.with_db_cache(cache);
        }

        let db = Arc::new(
            builder
                .build()
                .await
                .map_err(|e| StorageError::Storage(format!("Failed to create SlateDB: {}", e)))?,
        );

        let storage = Self::from_db(db);
        storage.coalesce_bucket_list().await?;
        Ok(storage)
    }

    fn from_db(db: Arc<Db>) -> Self {
        Self {
            db: db.clone(),
            reader: StorageReaderInner { db },
        }
    }

    /// Read the current `BucketList` value and rewrite it as a single `Put`,
    /// collapsing the merge chain that accrues across ingestion batches into
    /// one flat record, so later reads don't have to replay operands scattered
    /// across SSTs. Safe only when there are no concurrent writers, e.g. at
    /// startup before ingestion begins.
    #[tracing::instrument(level = "info", skip_all)]
    pub(crate) async fn coalesce_bucket_list(&self) -> crate::util::Result<()> {
        let key = BucketListKey.encode();
        let Some(value) = self.reader.get(key.clone()).await? else {
            return Ok(());
        };
        self.put(vec![PutRecordOp::new(Record { key, value })])
            .await?;
        Ok(())
    }

    // ── write path ───────────────────────────────────────────────────

    /// Applies a batch of mixed operations atomically with default options
    /// (`await_durable: false`).
    pub(crate) async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult> {
        self.apply_with_options(ops, WriteOptions::default()).await
    }

    /// Applies a batch of mixed operations atomically with custom options.
    pub(crate) async fn apply_with_options(
        &self,
        records: Vec<RecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            match op {
                RecordOp::Put(op) => {
                    batch.put_with_options(op.record.key, op.record.value, &op.options.into())
                }
                RecordOp::Merge(op) => {
                    batch.merge_with_options(op.record.key, op.record.value, &op.options.into())
                }
                RecordOp::Delete(key) => batch.delete(key),
            }
        }
        self.write_batch(batch, options).await
    }

    /// Writes records with default options (`await_durable: false`).
    pub(crate) async fn put(&self, records: Vec<PutRecordOp>) -> StorageResult<WriteResult> {
        self.put_with_options(records, WriteOptions::default())
            .await
    }

    /// Writes records with custom options controlling durability.
    pub(crate) async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.put_with_options(op.record.key, op.record.value, &op.options.into());
        }
        self.write_batch(batch, options).await
    }

    /// Merges records using the configured merge operator, default options.
    pub(crate) async fn merge(&self, records: Vec<MergeRecordOp>) -> StorageResult<WriteResult> {
        let mut batch = WriteBatch::new();
        for op in records {
            batch.merge_with_options(op.record.key, op.record.value, &op.options.into());
        }
        self.write_batch(batch, WriteOptions::default()).await
    }

    async fn write_batch(
        &self,
        batch: WriteBatch,
        options: WriteOptions,
    ) -> StorageResult<WriteResult> {
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
            ..SlateDbWriteOptions::default()
        };
        let write_handle = self
            .db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(WriteResult {
            seqnum: write_handle.seqnum(),
        })
    }

    // ── lifecycle ────────────────────────────────────────────────────

    /// Creates a point-in-time snapshot for consistent reads.
    pub(crate) async fn snapshot(&self) -> StorageResult<StorageSnapshot> {
        let snapshot = self
            .db
            .snapshot()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(StorageSnapshot {
            reader: StorageReaderInner { db: snapshot },
        })
    }

    /// Flushes pending writes to durable storage.
    pub(crate) async fn flush(&self) -> StorageResult<()> {
        self.db.flush().await.map_err(StorageError::from_storage)?;
        Ok(())
    }

    /// Creates a durable checkpoint covering all data.
    pub(crate) async fn create_checkpoint(&self) -> StorageResult<CheckpointInfo> {
        let result = self
            .db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(CheckpointInfo {
            id: result.id,
            manifest_id: result.manifest_id,
        })
    }

    /// Closes the database (which also closes the block cache).
    pub(crate) async fn close(&self) -> StorageResult<()> {
        self.db.close().await.map_err(StorageError::from_storage)?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Store for Storage {
    async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<WriteResult> {
        Storage::apply(self, ops).await
    }

    async fn snapshot(&self) -> StorageResult<StorageSnapshot> {
        Storage::snapshot(self).await
    }

    async fn flush(&self) -> StorageResult<()> {
        Storage::flush(self).await
    }
}

// ── OpenTSDB op builders ─────────────────────────────────────────────
//
// Pure encoders from domain values to storage record operations; they touch
// no storage state, so they are free functions rather than methods.

pub(crate) fn merge_bucket_list(bucket: TimeBucket, ttl: Ttl) -> crate::util::Result<RecordOp> {
    let key = BucketListKey.encode();
    let value = BucketListValue {
        buckets: vec![(bucket.size, bucket.start)],
    }
    .encode();

    Ok(RecordOp::Merge(MergeRecordOp::new_with_ttl(
        Record { key, value },
        MergeOptions { ttl },
    )))
}

pub(crate) fn insert_series_id(
    bucket: TimeBucket,
    fingerprint: SeriesFingerprint,
    id: SeriesId,
    ttl: Ttl,
) -> crate::util::Result<RecordOp> {
    let key = SeriesDictionaryKey {
        bucket,
        series_fingerprint: fingerprint,
    }
    .encode();
    let value = SeriesDictionaryValue { series_id: id }.encode();
    Ok(RecordOp::Put(PutRecordOp::new_with_options(
        Record { key, value },
        PutOptions { ttl },
    )))
}

pub(crate) fn insert_forward_index(
    bucket: TimeBucket,
    series_id: SeriesId,
    series_spec: SeriesSpec,
    ttl: Ttl,
) -> crate::util::Result<RecordOp> {
    let key = ForwardIndexKey { bucket, series_id }.encode();
    let value = ForwardIndexValue {
        metric_unit: series_spec.unit,
        metric_meta: series_spec.metric_type.into(),
        label_count: series_spec.labels.len() as u16,
        labels: series_spec.labels,
    }
    .encode();
    Ok(RecordOp::Put(PutRecordOp::new_with_options(
        Record { key, value },
        PutOptions { ttl },
    )))
}

pub(crate) fn merge_inverted_index(
    bucket: TimeBucket,
    label: Label,
    postings: RoaringBitmap,
    ttl: Ttl,
) -> crate::util::Result<RecordOp> {
    let key = InvertedIndexKey {
        bucket,
        attribute: label.name,
        value: label.value,
    }
    .encode();
    let value = InvertedIndexValue { postings }.encode()?;
    Ok(RecordOp::Merge(MergeRecordOp::new_with_ttl(
        Record { key, value },
        MergeOptions { ttl },
    )))
}

pub(crate) fn merge_samples(
    bucket: TimeBucket,
    series_id: SeriesId,
    metric_name: &str,
    samples: Vec<Sample>,
    ttl: Ttl,
) -> crate::util::Result<RecordOp> {
    let key = TimeSeriesKey {
        bucket,
        metric_name: metric_name.to_string(),
        series_id,
    }
    .encode();
    let value = TimeSeriesValue { points: samples }.encode()?;
    Ok(RecordOp::Merge(MergeRecordOp::new_with_ttl(
        Record { key, value },
        MergeOptions { ttl },
    )))
}

fn load_settings(slate_config: &SlateDbStorageConfig) -> StorageResult<Settings> {
    match &slate_config.settings_path {
        Some(path) => Settings::from_file(path).map_err(|e| {
            StorageError::Storage(format!(
                "Failed to load SlateDB settings from {}: {}",
                path, e
            ))
        }),
        None => Ok(Settings::load().unwrap_or_default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;
    use slatedb::{MergeOperator as SlateDbMergeOperator, MergeOperatorError};
    use slatedb_common::clock::MockSystemClock;

    fn storage_from_db(db: Db) -> Storage {
        Storage::from_db(Arc::new(db))
    }

    fn reader_from_db_reader(reader: DbReader) -> StorageReader {
        StorageReader {
            reader: StorageReaderInner {
                db: Arc::new(reader),
            },
        }
    }

    #[tokio::test]
    async fn should_read_data_written_by_storage_via_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = storage_from_db(db);

        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
                Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        let reader = DbReader::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage_reader = reader_from_db_reader(reader);

        let value = storage_reader.get(Bytes::from("key1")).await.unwrap();
        assert_eq!(value, Some(Bytes::from("value1")));
        let value = storage_reader.get(Bytes::from("key2")).await.unwrap();
        assert_eq!(value, Some(Bytes::from("value2")));
        let value = storage_reader.get(Bytes::from("key3")).await.unwrap();
        assert!(value.is_none());

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_scan_data_written_by_storage_via_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = storage_from_db(db);

        storage
            .put(vec![
                Record::new(Bytes::from("a"), Bytes::from("1")).into(),
                Record::new(Bytes::from("b"), Bytes::from("2")).into(),
                Record::new(Bytes::from("c"), Bytes::from("3")).into(),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        let reader = DbReader::builder(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage_reader = reader_from_db_reader(reader);

        let mut iter = storage_reader.scan(BytesRange::unbounded()).await.unwrap();
        let mut results = Vec::new();
        while let Some(record) = iter.next().await.unwrap() {
            results.push((record.key, record.value));
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (Bytes::from("a"), Bytes::from("1")));
        assert_eq!(results[1], (Bytes::from("b"), Bytes::from("2")));
        assert_eq!(results[2], (Bytes::from("c"), Bytes::from("3")));

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_set_expire_ts_based_on_ttl() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/ttl_db";
        let clock = Arc::new(MockSystemClock::new());

        let db = DbBuilder::new(path, object_store.clone())
            .with_settings(Settings {
                default_ttl: Some(30_000),
                ..Default::default()
            })
            .with_system_clock(clock.clone())
            .build()
            .await
            .unwrap();
        let storage = storage_from_db(db);

        storage
            .put(vec![
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key1"), Bytes::from("value1")),
                    PutOptions {
                        ttl: Ttl::ExpireAfter(20_000),
                    },
                ),
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key2"), Bytes::from("value2")),
                    PutOptions { ttl: Ttl::Default },
                ),
                PutRecordOp::new_with_options(
                    Record::new(Bytes::from("key3"), Bytes::from("value3")),
                    PutOptions { ttl: Ttl::NoExpiry },
                ),
            ])
            .await
            .unwrap();

        let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
        assert_eq!(kv1.expire_ts, Some(20_000));
        let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
        assert_eq!(kv2.expire_ts, Some(30_000));
        let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
        assert_eq!(kv3.expire_ts, None);

        storage.close().await.unwrap();
    }

    /// Simple merge operator that concatenates existing and new values.
    /// Implements SlateDB's `MergeOperator` directly (test-only).
    struct ConcatMergeOperator;

    impl SlateDbMergeOperator for ConcatMergeOperator {
        fn merge(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            value: Bytes,
        ) -> Result<Bytes, MergeOperatorError> {
            let mut result = existing_value.unwrap_or_default().to_vec();
            result.extend_from_slice(&value);
            Ok(Bytes::from(result))
        }

        fn merge_batch(
            &self,
            _key: &Bytes,
            existing_value: Option<Bytes>,
            operands: &[Bytes],
        ) -> Result<Bytes, MergeOperatorError> {
            if operands.is_empty() && existing_value.is_none() {
                return Err(MergeOperatorError::EmptyBatch);
            }
            let mut result = existing_value.unwrap_or_default().to_vec();
            for operand in operands {
                result.extend_from_slice(operand);
            }
            Ok(Bytes::from(result))
        }
    }

    #[tokio::test]
    async fn should_set_expire_ts_on_merge_records_based_on_ttl() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/merge_ttl_db";
        let clock = Arc::new(MockSystemClock::new());

        let db = DbBuilder::new(path, object_store.clone())
            .with_settings(Settings {
                default_ttl: Some(30_000),
                ..Default::default()
            })
            .with_system_clock(clock.clone())
            .with_merge_operator(Arc::new(ConcatMergeOperator))
            .build()
            .await
            .unwrap();
        let storage = storage_from_db(db);

        storage
            .merge(vec![
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key1"), Bytes::from("v1")),
                    MergeOptions {
                        ttl: Ttl::ExpireAfter(20_000),
                    },
                ),
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key2"), Bytes::from("v2")),
                    MergeOptions { ttl: Ttl::Default },
                ),
                MergeRecordOp::new_with_ttl(
                    Record::new(Bytes::from("key3"), Bytes::from("v3")),
                    MergeOptions { ttl: Ttl::NoExpiry },
                ),
            ])
            .await
            .unwrap();

        let kv1 = storage.db.get_key_value(b"key1").await.unwrap().unwrap();
        assert_eq!(kv1.value, Bytes::from("v1"));
        assert_eq!(kv1.expire_ts, Some(20_000));
        let kv2 = storage.db.get_key_value(b"key2").await.unwrap().unwrap();
        assert_eq!(kv2.value, Bytes::from("v2"));
        assert_eq!(kv2.expire_ts, Some(30_000));
        let kv3 = storage.db.get_key_value(b"key3").await.unwrap().unwrap();
        assert_eq!(kv3.value, Bytes::from("v3"));
        assert_eq!(kv3.expire_ts, None);

        storage.close().await.unwrap();
    }

    async fn reader_can_see(path: &str, object_store: Arc<InMemory>, key: &str) -> bool {
        let reader = DbReader::builder(path, object_store).build().await.unwrap();
        let storage_reader = reader_from_db_reader(reader);
        storage_reader
            .get(Bytes::from(key.to_owned()))
            .await
            .unwrap()
            .is_some()
    }

    #[tokio::test]
    async fn put_defaults_to_not_await_durable() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/put_default_durability";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = storage_from_db(db);

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();

        assert!(!reader_can_see(path, object_store.clone(), "k1").await);
        storage.flush().await.unwrap();
        assert!(reader_can_see(path, object_store.clone(), "k1").await);

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn apply_with_await_durable_true_is_visible_to_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/apply_durable";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = storage_from_db(db);

        storage
            .apply_with_options(
                vec![RecordOp::Put(
                    Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
                )],
                WriteOptions {
                    await_durable: true,
                },
            )
            .await
            .unwrap();

        assert!(reader_can_see(path, object_store.clone(), "k1").await);
        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn snapshot_sees_writes_made_before_it() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/snapshot";

        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = storage_from_db(db);

        storage
            .put(vec![
                Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            ])
            .await
            .unwrap();

        let snapshot = storage.snapshot().await.unwrap();
        let value = snapshot.get(Bytes::from("k1")).await.unwrap();
        assert_eq!(value, Some(Bytes::from("v1")));

        storage.close().await.unwrap();
    }
}

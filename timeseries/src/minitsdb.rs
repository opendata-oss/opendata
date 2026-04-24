use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::StorageError;
use common::coordinator::{Durability, WriteCoordinator, WriteCoordinatorConfig, WriteError};
use slatedb::{Db, DbRead, DbSnapshot};

const WRITE_CHANNEL: &str = "write";

use crate::delta::{TsdbContext, TsdbWriteDelta};
use crate::error::Error;
use crate::flusher::TsdbFlusher;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::model::{Label, Sample, Series, SeriesId, TimeBucket};
use crate::query::BucketQueryReader;
use crate::serde::key::TimeSeriesKey;
use crate::serde::timeseries::TimeSeriesIterator;
use crate::storage::{
    get_forward_index, get_forward_index_one, get_forward_index_series, get_inverted_index,
    get_inverted_index_term, get_inverted_index_terms, get_label_values, load_series_dictionary,
};
use crate::util::Result;

/// A snapshot-like handle used by `MiniQueryReader`. We erase the
/// concrete reader type behind an `Arc<dyn ErasedSnapshot>` (a local
/// trait) so a MiniQueryReader can be constructed from either
/// `Arc<DbSnapshot>` or `Arc<DbReader>` without being generic itself.
///
/// `slatedb::DbRead` is not object-safe (its methods are generic over
/// key types), so we can't store `Arc<dyn DbRead>` directly — we wrap
/// the small set of operations the timeseries extension functions use.
#[async_trait]
pub(crate) trait ErasedSnapshot: Send + Sync {
    async fn get_forward_index_async(
        &self,
        bucket: TimeBucket,
    ) -> Result<crate::index::ForwardIndex>;
    async fn get_forward_index_series_async(
        &self,
        bucket: TimeBucket,
        series_ids: Vec<SeriesId>,
    ) -> Result<crate::index::ForwardIndex>;
    async fn get_inverted_index_async(
        &self,
        bucket: TimeBucket,
    ) -> Result<crate::index::InvertedIndex>;
    async fn get_inverted_index_terms_async(
        &self,
        bucket: TimeBucket,
        terms: Vec<Label>,
    ) -> Result<crate::index::InvertedIndex>;
    async fn get_label_values_async(
        &self,
        bucket: TimeBucket,
        label_name: String,
    ) -> Result<Vec<String>>;
    async fn get_forward_index_one_async(
        &self,
        bucket: TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<crate::index::SeriesSpec>>;
    async fn get_inverted_index_term_async(
        &self,
        bucket: TimeBucket,
        term: Label,
    ) -> Result<Option<roaring::RoaringBitmap>>;
    async fn get_async(&self, key: bytes::Bytes) -> Result<Option<bytes::Bytes>>;
}

#[async_trait]
impl<R> ErasedSnapshot for R
where
    R: DbRead + Send + Sync,
{
    async fn get_forward_index_async(
        &self,
        bucket: TimeBucket,
    ) -> Result<crate::index::ForwardIndex> {
        get_forward_index(self, bucket).await
    }
    async fn get_forward_index_series_async(
        &self,
        bucket: TimeBucket,
        series_ids: Vec<SeriesId>,
    ) -> Result<crate::index::ForwardIndex> {
        get_forward_index_series(self, &bucket, &series_ids).await
    }
    async fn get_inverted_index_async(
        &self,
        bucket: TimeBucket,
    ) -> Result<crate::index::InvertedIndex> {
        get_inverted_index(self, bucket).await
    }
    async fn get_inverted_index_terms_async(
        &self,
        bucket: TimeBucket,
        terms: Vec<Label>,
    ) -> Result<crate::index::InvertedIndex> {
        get_inverted_index_terms(self, &bucket, &terms).await
    }
    async fn get_label_values_async(
        &self,
        bucket: TimeBucket,
        label_name: String,
    ) -> Result<Vec<String>> {
        get_label_values(self, &bucket, &label_name).await
    }
    async fn get_forward_index_one_async(
        &self,
        bucket: TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<crate::index::SeriesSpec>> {
        get_forward_index_one(self, &bucket, series_id).await
    }
    async fn get_inverted_index_term_async(
        &self,
        bucket: TimeBucket,
        term: Label,
    ) -> Result<Option<roaring::RoaringBitmap>> {
        get_inverted_index_term(self, &bucket, &term).await
    }
    async fn get_async(&self, key: bytes::Bytes) -> Result<Option<bytes::Bytes>> {
        self.get(&key)
            .await
            .map_err(|e| StorageError::from_storage(e).into())
    }
}

pub(crate) struct MiniQueryReader {
    bucket: TimeBucket,
    snapshot: Arc<dyn ErasedSnapshot>,
}

impl MiniQueryReader {
    pub(crate) fn new(bucket: TimeBucket, snapshot: Arc<dyn ErasedSnapshot>) -> Self {
        Self { bucket, snapshot }
    }
}

#[async_trait]
impl BucketQueryReader for MiniQueryReader {
    async fn forward_index(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let forward_index = io_trace_async(IoKindLocal::ForwardIndexFetch, async {
            self.snapshot
                .get_forward_index_series_async(self.bucket, series_ids.to_vec())
                .await
        })
        .await?;
        Ok(Box::new(forward_index))
    }

    async fn all_forward_index(
        &self,
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let forward_index = io_trace_async(
            IoKindLocal::ForwardIndexFetch,
            self.snapshot.get_forward_index_async(self.bucket),
        )
        .await?;
        Ok(Box::new(forward_index))
    }

    async fn inverted_index(
        &self,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let inverted_index = io_trace_async(IoKindLocal::InvertedIndexFetch, async {
            self.snapshot
                .get_inverted_index_terms_async(self.bucket, terms.to_vec())
                .await
        })
        .await?;
        Ok(Box::new(inverted_index))
    }

    async fn all_inverted_index(
        &self,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let inverted_index = io_trace_async(
            IoKindLocal::InvertedIndexFetch,
            self.snapshot.get_inverted_index_async(self.bucket),
        )
        .await?;
        Ok(Box::new(inverted_index))
    }

    async fn label_values(&self, label_name: &str) -> Result<Vec<String>> {
        io_trace_async(
            IoKindLocal::LabelValuesFetch,
            self.snapshot
                .get_label_values_async(self.bucket, label_name.to_string()),
        )
        .await
    }

    async fn forward_index_one(
        &self,
        series_id: SeriesId,
    ) -> Result<Option<crate::index::SeriesSpec>> {
        io_trace_async(
            IoKindLocal::ForwardIndexFetch,
            self.snapshot
                .get_forward_index_one_async(self.bucket, series_id),
        )
        .await
    }

    async fn inverted_index_term(&self, term: &Label) -> Result<Option<roaring::RoaringBitmap>> {
        io_trace_async(
            IoKindLocal::InvertedIndexFetch,
            self.snapshot
                .get_inverted_index_term_async(self.bucket, term.clone()),
        )
        .await
    }

    async fn samples(
        &self,
        series_id: SeriesId,
        metric_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        let storage_key = TimeSeriesKey {
            time_bucket: self.bucket.start,
            bucket_size: self.bucket.size,
            metric_name: metric_name.to_string(),
            series_id,
        };
        let record = io_trace_async(
            IoKindLocal::SamplesFetch,
            self.snapshot.get_async(storage_key.encode()),
        )
        .await?;

        match record {
            Some(record) => {
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::SamplesFetch,
                    record.len() as u64,
                );
                let raw_len = record.len() as u64;
                let samples = io_trace_sync(IoKindLocal::Deserialize, || {
                    let iter = TimeSeriesIterator::new(record.as_ref()).ok_or_else(|| {
                        Error::Internal("Invalid timeseries data in storage".into())
                    })?;
                    let samples: Vec<Sample> = iter
                        .filter_map(|r| r.ok())
                        // Filter by time range: timestamp > start_ms && timestamp <= end_ms
                        // Following PromQL lookback window semantics with exclusive start
                        .filter(|s| s.timestamp_ms > start_ms && s.timestamp_ms <= end_ms)
                        .collect();
                    Ok::<Vec<Sample>, Error>(samples)
                })?;
                // Deserialize's bytes are the same bytes the fetch returned —
                // it's decoding that payload. Attribute here so both kinds
                // report throughput.
                crate::promql::trace::record_bytes(
                    crate::promql::trace::IoKind::Deserialize,
                    raw_len,
                );
                Ok(samples)
            }
            None => Ok(Vec::new()),
        }
    }
}

// ─── trace helpers ──────────────────────────────────────────────────

use crate::promql::trace::IoKind as IoKindLocal;

async fn io_trace_async<F: std::future::Future<Output = T>, T>(kind: IoKindLocal, fut: F) -> T {
    crate::promql::trace::record_async(kind, fut).await
}

fn io_trace_sync<T>(kind: IoKindLocal, f: impl FnOnce() -> T) -> T {
    crate::promql::trace::record_sync(kind, f)
}

pub(crate) struct MiniTsdb {
    bucket: TimeBucket,
    write_coordinator: WriteCoordinator<TsdbWriteDelta, TsdbFlusher>,
}

impl MiniTsdb {
    /// Returns a reference to the time bucket
    pub(crate) fn bucket(&self) -> &TimeBucket {
        &self.bucket
    }

    /// Create a query reader for read operations.
    pub(crate) fn query_reader(&self) -> MiniQueryReader {
        let view = self.write_coordinator.view();
        MiniQueryReader {
            bucket: self.bucket,
            snapshot: view.snapshot.clone() as Arc<dyn ErasedSnapshot>,
        }
    }

    pub(crate) async fn load(bucket: TimeBucket, db: Arc<Db>) -> Result<Self> {
        let snapshot = db.snapshot().await.map_err(StorageError::from_storage)?;

        let mut series_dict = HashMap::new();
        let next_series_id =
            load_series_dictionary(snapshot.as_ref(), &bucket, |fingerprint, series_id| {
                series_dict.insert(fingerprint, series_id);
            })
            .await?;

        let context = TsdbContext {
            bucket,
            series_dict: Arc::new(series_dict),
            next_series_id,
        };

        let flusher = TsdbFlusher { db: db.clone() };

        let initial_snapshot: Arc<DbSnapshot> = db
            .snapshot()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut write_coordinator = WriteCoordinator::new(
            WriteCoordinatorConfig::default(),
            vec![WRITE_CHANNEL.to_string()],
            context,
            initial_snapshot,
            flusher,
        );
        write_coordinator.start();

        Ok(Self {
            bucket,
            write_coordinator,
        })
    }

    /// Ingest a batch of series with samples in a single operation.
    ///
    /// If `timeout` is provided, waits up to the given duration for space in the
    /// write queue. Otherwise, fails immediately when the queue is full.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>()
        )
    )]
    pub(crate) async fn ingest_batch(
        &self,
        series_list: &[Series],
        timeout: Option<Duration>,
    ) -> Result<()> {
        let total_samples = series_list.iter().map(|s| s.samples.len()).sum::<usize>();

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Starting MiniTsdb batch ingest"
        );

        let handle = self.write_coordinator.handle(WRITE_CHANNEL);
        let mut write_handle = match timeout {
            Some(t) => handle
                .write_timeout(series_list.to_vec(), t)
                .await
                .map_err(|e| map_write_error(e.discard_inner()))?,
            None => handle
                .try_write(series_list.to_vec())
                .await
                .map_err(|e| map_write_error(e.discard_inner()))?,
        };

        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(map_write_error)?;

        tracing::debug!(
            bucket = ?self.bucket,
            series_count = series_list.len(),
            total_samples = total_samples,
            "Completed MiniTsdb batch ingest"
        );

        Ok(())
    }

    /// Ingest a single series with samples.
    pub(crate) async fn ingest(&self, series: &Series) -> Result<()> {
        self.ingest_batch(std::slice::from_ref(series), None).await
    }

    /// Flush pending data to the storage memtable (not yet durable).
    ///
    /// After this returns, the data is visible to snapshot reads but has not
    /// been persisted to durable storage. Call `db.flush()` afterwards
    /// to make the data durable.
    pub(crate) async fn flush_written(&self) -> Result<()> {
        let handle = self.write_coordinator.handle(WRITE_CHANNEL);
        let mut flush_handle = handle.flush(false).await.map_err(map_write_error)?;

        flush_handle
            .wait(Durability::Written)
            .await
            .map_err(map_write_error)?;

        Ok(())
    }

    /// Gracefully stop the write coordinator, flushing pending data.
    pub(crate) async fn stop(self) -> Result<()> {
        self.write_coordinator
            .stop()
            .await
            .map_err(Error::Internal)?;
        Ok(())
    }
}

fn map_write_error(e: WriteError) -> Error {
    match e {
        WriteError::Backpressure(_) => {
            metrics::counter!(crate::tsdb_metrics::TSDB_BACKPRESSURE).increment(1);
            Error::Backpressure
        }
        WriteError::TimeoutError(_) => Error::Backpressure,
        WriteError::Shutdown => Error::Internal("Write coordinator shut down".to_string()),
        WriteError::ApplyError(_, msg) => Error::Internal(msg),
        WriteError::FlushError(msg) => Error::Storage(msg),
        WriteError::Internal(msg) => Error::Internal(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Sample, Series};
    use slatedb::DbBuilder;
    use slatedb::object_store::memory::InMemory;

    /// Create a MiniTsdb with a custom queue capacity.
    async fn load_with_config(bucket: TimeBucket, db: Arc<Db>, queue_capacity: usize) -> MiniTsdb {
        let snapshot = db.snapshot().await.unwrap();

        let mut series_dict = HashMap::new();
        let next_series_id =
            load_series_dictionary(snapshot.as_ref(), &bucket, |fingerprint, series_id| {
                series_dict.insert(fingerprint, series_id);
            })
            .await
            .unwrap();

        let context = TsdbContext {
            bucket,
            series_dict: Arc::new(series_dict),
            next_series_id,
        };

        let flusher = TsdbFlusher { db: db.clone() };

        let initial_snapshot: Arc<DbSnapshot> = db.snapshot().await.unwrap();

        let config = WriteCoordinatorConfig {
            queue_capacity,
            ..Default::default()
        };

        let mut write_coordinator = WriteCoordinator::new(
            config,
            vec![WRITE_CHANNEL.to_string()],
            context,
            initial_snapshot,
            flusher,
        );
        write_coordinator.start();

        MiniTsdb {
            bucket,
            write_coordinator,
        }
    }

    fn test_series(name: &str, ts: i64, value: f64) -> Series {
        Series::new(
            name,
            vec![Label::new("host", "server1")],
            vec![Sample::new(ts, value)],
        )
    }

    async fn test_db() -> Arc<Db> {
        use crate::storage::merge_operator::OpenTsdbMergeOperator;

        let object_store = Arc::new(InMemory::new());
        let db = DbBuilder::new("test", object_store)
            .with_merge_operator(Arc::new(OpenTsdbMergeOperator))
            .build()
            .await
            .unwrap();
        Arc::new(db)
    }

    #[tokio::test]
    async fn should_succeed_ingest_batch_with_timeout_when_queue_has_space() {
        // given - queue has space (capacity=1)
        let bucket = TimeBucket::hour(60);
        let db = test_db().await;
        let mini = load_with_config(bucket, db, 1).await;

        // when
        let s1 = vec![test_series("cpu", 3_700_000, 1.0)];
        let result = mini
            .ingest_batch(&s1, Some(Duration::from_millis(50)))
            .await;

        // then
        assert!(result.is_ok(), "expected success, got {:?}", result);
    }

    #[tokio::test(start_paused = true)]
    async fn should_fail_ingest_batch_when_timeout_too_short_for_drain() {
        // given - queue_capacity=2, coordinator paused
        let bucket = TimeBucket::hour(60);
        let db = test_db().await;
        let mini = load_with_config(bucket, db, 2).await;

        let pause = mini.write_coordinator.pause_handle(WRITE_CHANNEL);
        pause.pause();

        // fill the queue
        let handle = mini.write_coordinator.handle(WRITE_CHANNEL);
        let _wh1 = handle
            .try_write(vec![test_series("cpu", 3_700_000, 1.0)])
            .await
            .unwrap();
        let _wh2 = handle
            .try_write(vec![test_series("cpu", 3_700_001, 2.0)])
            .await
            .unwrap();

        // verify immediate reject with no timeout
        let s3 = vec![test_series("cpu", 3_700_002, 3.0)];
        let result = mini.ingest_batch(&s3, None).await;
        assert!(
            matches!(result, Err(Error::Backpressure)),
            "expected Backpressure with no timeout, got {:?}",
            result
        );

        // unpause after 200ms
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            pause.unpause();
        });

        // when - 10ms timeout is too short, queue is still full
        let result = mini
            .ingest_batch(&s3, Some(Duration::from_millis(10)))
            .await;

        // then
        assert!(
            matches!(result, Err(Error::Backpressure)),
            "expected Backpressure with short timeout, got {:?}",
            result
        );

        // when - 5s timeout is long enough to wait for the delayed unpause
        let result = mini.ingest_batch(&s3, Some(Duration::from_secs(5))).await;

        // then
        assert!(
            result.is_ok(),
            "expected success with long timeout, got {:?}",
            result
        );
    }
}

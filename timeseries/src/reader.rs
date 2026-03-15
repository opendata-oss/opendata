//! Read-only time series database access.
//!
//! This module provides [`TimeSeriesDbReader`], a read-only view of a time
//! series database. It uses SlateDB's `DbReader` under the hood, which
//! coexists with a production writer without fencing — unlike `Db::open()`,
//! which always fences the previous writer.

use std::collections::HashMap;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use common::StorageRead;
use common::storage::factory::create_storage_read;
use common::{StorageReaderRuntime, StorageSemantics};
use futures::stream::{self, StreamExt};
use moka::future::Cache;

use crate::config::ReaderConfig;
use crate::error::{QueryError, Result};
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::minitsdb::MiniQueryReader;
use crate::model::{
    Label, Labels, QueryOptions, QueryValue, RangeSample, Sample, SeriesId, TimeBucket,
};
use crate::query::{BucketQueryReader, QueryReader};
use crate::storage::OpenTsdbStorageReadExt;
use crate::storage::merge_operator::OpenTsdbMergeOperator;
use crate::tsdb::{
    TsdbReadEngine, eval_query_range_bounds, find_label_values_in_range, find_labels_in_range,
    find_series_in_range,
};

// ── ReaderQueryReader ────────────────────────────────────────────────

/// QueryReader implementation for read-only access.
///
/// Wraps `Arc<MiniQueryReader>` per bucket (unlike `TsdbQueryReader` which
/// uses owned `MiniQueryReader`).
pub(crate) struct ReaderQueryReader {
    mini_readers: HashMap<TimeBucket, Arc<MiniQueryReader>>,
}

impl ReaderQueryReader {
    fn new(readers: Vec<(TimeBucket, Arc<MiniQueryReader>)>) -> Self {
        Self {
            mini_readers: readers.into_iter().collect(),
        }
    }
}

#[async_trait]
impl QueryReader for ReaderQueryReader {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        Ok(self.mini_readers.keys().cloned().collect())
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index(series_ids).await
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.inverted_index(terms).await
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.all_inverted_index().await
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.label_values(label_name).await
    }

    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.samples(series_id, start_ms, end_ms).await
    }
}

// ── TimeSeriesDbReader ───────────────────────────────────────────────

/// A read-only view of a time series database.
///
/// `TimeSeriesDbReader` provides the same read API as [`TimeSeriesDb`](crate::TimeSeriesDb)
/// but without write operations. It uses SlateDB's `DbReader`, which opens the
/// database without fencing — this means it can safely coexist with a production
/// writer on the same storage path.
///
/// # When to Use
///
/// Use `TimeSeriesDbReader` when you need read-only access to production data
/// without risking writer fencing:
///
/// - **Benchmarking**: Run queries against production data from a separate process.
/// - **Ad hoc analysis**: Investigate metrics without affecting the running service.
/// - **Testing**: Verify data written by a production writer.
///
/// # Example
///
/// ```ignore
/// use timeseries::{TimeSeriesDbReader, ReaderConfig};
/// use common::StorageConfig;
///
/// let config = ReaderConfig {
///     storage: StorageConfig::default(),
///     ..Default::default()
/// };
/// let reader = TimeSeriesDbReader::open(config).await?;
///
/// let result = reader.query("rate(http_requests_total[5m])", None).await?;
/// ```
pub struct TimeSeriesDbReader {
    storage: Arc<dyn StorageRead>,
    /// LRU cache for read-only query buckets.
    query_cache: Cache<TimeBucket, Arc<MiniQueryReader>>,
}

impl TimeSeriesDbReader {
    /// Opens a read-only view of the time series database.
    ///
    /// Uses SlateDB's `DbReader` which polls the manifest at `refresh_interval`
    /// to discover new data. Does **not** fence the existing writer.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    pub async fn open(config: ReaderConfig) -> Result<Self> {
        let reader_options = slatedb::config::DbReaderOptions {
            manifest_poll_interval: config.refresh_interval,
            ..Default::default()
        };

        let storage = create_storage_read(
            &config.storage,
            StorageReaderRuntime::new(),
            StorageSemantics::new().with_merge_operator(Arc::new(OpenTsdbMergeOperator)),
            reader_options,
        )
        .await?;
        Ok(Self::from_storage_with_capacity(
            storage,
            config.cache_capacity,
        ))
    }

    /// Creates a TimeSeriesDbReader from an existing storage implementation.
    pub(crate) fn from_storage(storage: Arc<dyn StorageRead>) -> Self {
        Self::from_storage_with_capacity(storage, crate::config::DEFAULT_CACHE_CAPACITY)
    }

    fn from_storage_with_capacity(storage: Arc<dyn StorageRead>, cache_capacity: u64) -> Self {
        let query_cache = Cache::builder().max_capacity(cache_capacity).build();
        Self {
            storage,
            query_cache,
        }
    }

    /// Get a cached bucket reader, loading from storage if needed.
    async fn get_or_load_bucket(&self, bucket: TimeBucket) -> Arc<MiniQueryReader> {
        let storage = self.storage.clone();
        self.query_cache
            .get_with(bucket, async move {
                Arc::new(MiniQueryReader::new(bucket, storage))
            })
            .await
    }

    // ── Public inherent methods ───────────────────────────────────────

    /// Evaluates an instant PromQL query at a single point in time.
    ///
    /// If `time` is `None`, the current wall-clock time is used.
    pub async fn query(
        &self,
        query: &str,
        time: Option<SystemTime>,
    ) -> std::result::Result<QueryValue, QueryError> {
        <Self as TsdbReadEngine>::eval_query(self, query, time, &QueryOptions::default()).await
    }

    /// Evaluates a range PromQL query over a time interval.
    pub async fn query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime>,
        step: Duration,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        eval_query_range_bounds(self, query, range, step, &QueryOptions::default()).await
    }

    /// Returns the set of label-sets matching the given series matchers.
    pub async fn series(
        &self,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        find_series_in_range(self, matchers, range).await
    }

    /// Returns the set of label names matching the given matchers.
    pub async fn labels(
        &self,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        find_labels_in_range(self, matchers, range).await
    }

    /// Returns the set of values for a given label name.
    pub async fn label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> std::result::Result<Vec<String>, QueryError> {
        find_label_values_in_range(self, label_name, matchers, range).await
    }
}

// ── TsdbReadEngine for TimeSeriesDbReader ────────────────────────────

/// Maximum number of buckets to load concurrently.
const BUCKET_LOAD_CONCURRENCY: usize = 8;

#[async_trait]
impl TsdbReadEngine for TimeSeriesDbReader {
    type QR = ReaderQueryReader;

    async fn make_query_reader(&self, start: i64, end: i64) -> Result<ReaderQueryReader> {
        let buckets = self
            .storage
            .get_buckets_in_range(Some(start), Some(end))
            .await?;

        let readers: Vec<_> = stream::iter(buckets)
            .map(|bucket| async move {
                let mini = self.get_or_load_bucket(bucket).await;
                (bucket, mini)
            })
            .buffer_unordered(BUCKET_LOAD_CONCURRENCY)
            .collect()
            .await;

        Ok(ReaderQueryReader::new(readers))
    }

    async fn make_query_reader_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> Result<ReaderQueryReader> {
        let buckets = self.storage.get_buckets_for_ranges(ranges).await?;

        let readers: Vec<_> = stream::iter(buckets)
            .map(|bucket| async move {
                let mini = self.get_or_load_bucket(bucket).await;
                (bucket, mini)
            })
            .buffer_unordered(BUCKET_LOAD_CONCURRENCY)
            .collect()
            .await;

        Ok(ReaderQueryReader::new(readers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Series;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::storage::in_memory::InMemoryStorage;

    fn create_shared_storage() -> Arc<InMemoryStorage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )))
    }

    #[tokio::test]
    async fn reader_sees_written_data() {
        // Write data through internal Tsdb, then verify reader sees it.
        let storage = create_shared_storage();

        let tsdb = crate::tsdb::Tsdb::new(storage.clone());
        let series = vec![
            Series::builder("http_requests_total")
                .label("method", "GET")
                .label("status", "200")
                .sample(1700000000000, 100.0)
                .sample(1700000001000, 101.0)
                .build(),
        ];
        tsdb.ingest_samples(series).await.unwrap();
        tsdb.flush().await.unwrap();

        // Open reader on the same storage
        let reader = TimeSeriesDbReader::from_storage(storage);

        // Query should find the data
        let query_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000001000);
        let result = reader
            .query("http_requests_total", Some(query_time))
            .await
            .unwrap();

        match result {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 101.0);
            }
            _ => panic!("expected Vector result"),
        }
    }

    #[tokio::test]
    async fn reader_series_discovery() {
        let storage = create_shared_storage();

        let tsdb = crate::tsdb::Tsdb::new(storage.clone());
        let series = vec![
            Series::builder("http_requests_total")
                .label("method", "GET")
                .sample(1700000000000, 100.0)
                .build(),
            Series::builder("http_requests_total")
                .label("method", "POST")
                .sample(1700000000000, 50.0)
                .build(),
            Series::builder("cpu_usage")
                .label("host", "server1")
                .sample(1700000000000, 0.75)
                .build(),
        ];
        tsdb.ingest_samples(series).await.unwrap();
        tsdb.flush().await.unwrap();

        let reader = TimeSeriesDbReader::from_storage(storage);

        // Series discovery
        let series = reader
            .series(
                &["{__name__=~\"http_requests_total|cpu_usage\"}"],
                (SystemTime::UNIX_EPOCH + Duration::from_secs(1699999000))
                    ..=(SystemTime::UNIX_EPOCH + Duration::from_secs(1700001000)),
            )
            .await
            .unwrap();
        assert_eq!(series.len(), 3);

        // Label names
        let labels = reader
            .labels(
                None,
                (SystemTime::UNIX_EPOCH + Duration::from_secs(1699999000))
                    ..=(SystemTime::UNIX_EPOCH + Duration::from_secs(1700001000)),
            )
            .await
            .unwrap();
        assert!(labels.contains(&"__name__".to_string()));
        assert!(labels.contains(&"method".to_string()));
        assert!(labels.contains(&"host".to_string()));

        // Label values
        let values = reader
            .label_values(
                "method",
                None,
                (SystemTime::UNIX_EPOCH + Duration::from_secs(1699999000))
                    ..=(SystemTime::UNIX_EPOCH + Duration::from_secs(1700001000)),
            )
            .await
            .unwrap();
        assert!(values.contains(&"GET".to_string()));
        assert!(values.contains(&"POST".to_string()));
    }

    #[tokio::test]
    async fn writer_and_reader_coexist_on_shared_storage() {
        // Verify that a writer and reader can both operate on the same
        // InMemoryStorage without interfering with each other.
        let storage = create_shared_storage();

        let tsdb = crate::tsdb::Tsdb::new(storage.clone());

        // Write initial data
        let series = vec![
            Series::builder("metric_a")
                .label("env", "prod")
                .sample(1700000000000, 1.0)
                .build(),
        ];
        tsdb.ingest_samples(series).await.unwrap();
        tsdb.flush().await.unwrap();

        // Open reader
        let reader = TimeSeriesDbReader::from_storage(storage.clone());

        // Reader sees initial data
        let query_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000000000);
        let result = reader.query("metric_a", Some(query_time)).await.unwrap();
        match &result {
            QueryValue::Vector(samples) => assert_eq!(samples.len(), 1),
            _ => panic!("expected Vector"),
        }

        // Writer can still write more data (no fencing error)
        let more_series = vec![
            Series::builder("metric_a")
                .label("env", "prod")
                .sample(1700000002000, 2.0)
                .build(),
        ];
        tsdb.ingest_samples(more_series).await.unwrap();
        tsdb.flush().await.unwrap();

        // Reader sees newly written data (InMemory storage shares state)
        let query_time2 = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000002000);
        let result2 = reader.query("metric_a", Some(query_time2)).await.unwrap();
        match &result2 {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 2.0);
            }
            _ => panic!("expected Vector"),
        }
    }

    #[tokio::test]
    async fn reader_query_range() {
        let storage = create_shared_storage();

        let tsdb = crate::tsdb::Tsdb::new(storage.clone());
        let series = vec![
            Series::builder("counter")
                .label("job", "test")
                .sample(1700000000000, 100.0)
                .sample(1700000015000, 115.0)
                .sample(1700000030000, 130.0)
                .sample(1700000045000, 145.0)
                .sample(1700000060000, 160.0)
                .build(),
        ];
        tsdb.ingest_samples(series).await.unwrap();
        tsdb.flush().await.unwrap();

        let reader = TimeSeriesDbReader::from_storage(storage);

        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000000);
        let end = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000060);

        let result = reader
            .query_range("counter", start..=end, Duration::from_secs(15))
            .await
            .unwrap();

        assert!(!result.is_empty());
        // Each RangeSample should have multiple data points
        for rs in &result {
            assert!(!rs.samples.is_empty());
        }
    }

    /// Integration test using real SlateDB storage via TimeSeriesDb::open and
    /// TimeSeriesDbReader::open on the same local path. This exercises the
    /// actual DbReader open path and verifies writer + reader coexistence
    /// without fencing.
    #[tokio::test]
    async fn slatedb_writer_and_reader_coexist_no_fencing() {
        use crate::{Config, ReaderConfig, TimeSeriesDb};
        use common::storage::config::{
            LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
        };

        let tmp_dir = tempfile::tempdir().unwrap();
        let storage_config = common::StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp_dir.path().to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
        });

        // 1. Open writer and write data
        let writer = TimeSeriesDb::open(Config {
            storage: storage_config.clone(),
            flush_interval: Duration::from_secs(60),
            retention: None,
        })
        .await
        .unwrap();

        let series = vec![
            Series::builder("http_requests_total")
                .label("method", "GET")
                .label("status", "200")
                .sample(1700000000000, 100.0)
                .sample(1700000001000, 101.0)
                .build(),
        ];
        writer.write(series).await.unwrap();
        writer.flush().await.unwrap();

        // 2. Open reader via the public API (exercises create_storage_read + DbReader)
        let reader = TimeSeriesDbReader::open(ReaderConfig {
            storage: storage_config.clone(),
            refresh_interval: Duration::from_millis(100),
            ..Default::default()
        })
        .await
        .unwrap();

        // 3. Reader sees written data
        let query_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000001000);
        let result = reader
            .query("http_requests_total", Some(query_time))
            .await
            .unwrap();
        match &result {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 101.0);
            }
            _ => panic!("expected Vector, got {:?}", result),
        }

        // 4. Writer can still write after reader opened (no fencing)
        let more_series = vec![
            Series::builder("http_requests_total")
                .label("method", "GET")
                .label("status", "200")
                .sample(1700000002000, 102.0)
                .build(),
        ];
        writer.write(more_series).await.unwrap();
        writer.flush().await.unwrap();

        // 5. Verify writer is still functional by querying through the writer
        let query_time2 = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000002000);
        let writer_result = writer
            .query("http_requests_total", Some(query_time2))
            .await
            .unwrap();
        match &writer_result {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 102.0);
            }
            _ => panic!("expected Vector from writer, got {:?}", writer_result),
        }

        // 6. Verify reader can still query its original snapshot (not fenced).
        //
        // NOTE: Ideally we'd also verify that the reader sees the *new* data
        // (value=102.0 at t=1700000002000) after its manifest_poll_interval
        // elapses.  However, SlateDB's DbReader currently hits a
        // "invalid sequence number ordering during merge" error when it
        // encounters SSTs written after it opened, due to the merge operator
        // seeing ascending sequence numbers.  Once that is resolved upstream
        // this test should be extended to assert refresh visibility.
        let reader_result = reader
            .query("http_requests_total", Some(query_time))
            .await
            .unwrap();
        match &reader_result {
            QueryValue::Vector(samples) => {
                assert_eq!(
                    samples.len(),
                    1,
                    "reader should still work after writer writes more data"
                );
                assert_eq!(samples[0].value, 101.0);
            }
            _ => panic!("expected Vector from reader, got {:?}", reader_result),
        }
    }

    #[tokio::test]
    async fn should_persist_data_after_flush_and_writer_reopen() {
        use crate::{Config, TimeSeriesDb};
        use common::storage::config::{
            LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
        };

        // given
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage_config = common::StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: tmp_dir.path().to_str().unwrap().to_string(),
            }),
            settings_path: None,
            block_cache: None,
        });

        let writer = TimeSeriesDb::open(Config {
            storage: storage_config.clone(),
            flush_interval: Duration::from_secs(60),
            retention: None,
        })
        .await
        .unwrap();

        let series = vec![
            Series::builder("flush_durability_metric")
                .label("env", "test")
                .sample(1700000001000, 7.0)
                .build(),
        ];
        writer.write(series).await.unwrap();

        // when
        writer.flush().await.unwrap();
        drop(writer);

        let reopened = TimeSeriesDb::open(Config {
            storage: storage_config,
            flush_interval: Duration::from_secs(60),
            retention: None,
        })
        .await
        .unwrap();
        let query_time = SystemTime::UNIX_EPOCH + Duration::from_millis(1700000001000);
        let result = reopened
            .query("flush_durability_metric", Some(query_time))
            .await
            .unwrap();

        // then
        match result {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 1);
                assert_eq!(samples[0].value, 7.0);
            }
            _ => panic!("expected Vector result after reopen"),
        }
    }

    /// Writer and reader must return identical timestamps for query_range
    /// with an inclusive end (`..=end`) where end is step-aligned. This
    /// guards against the regression where double-converting through
    /// `range_bounds_to_system_time` shifted the inclusive end by 1ms.
    #[tokio::test]
    async fn writer_and_reader_query_range_parity_at_boundary() {
        use crate::model::QueryOptions;

        let storage = create_shared_storage();
        let tsdb = crate::tsdb::Tsdb::new(storage.clone());

        // 5 samples at 15s intervals: t+0, t+15, t+30, t+45, t+60
        let series = vec![
            Series::builder("gauge")
                .label("job", "test")
                .sample(1700000000000, 1.0)
                .sample(1700000015000, 2.0)
                .sample(1700000030000, 3.0)
                .sample(1700000045000, 4.0)
                .sample(1700000060000, 5.0)
                .build(),
        ];
        tsdb.ingest_samples(series).await.unwrap();
        tsdb.flush().await.unwrap();

        let reader = TimeSeriesDbReader::from_storage(storage.clone());

        // Use Tsdb directly for the writer side (same data, same storage)
        let writer_tsdb = crate::tsdb::Tsdb::new(storage);

        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000000);
        let end = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000060);
        let step = Duration::from_secs(15);

        let reader_result = reader
            .query_range("gauge", start..=end, step)
            .await
            .unwrap();
        let writer_result = writer_tsdb
            .eval_query_range("gauge", start..=end, step, &QueryOptions::default())
            .await
            .unwrap();

        // Extract sorted (timestamp_ms, value) pairs for comparison
        let extract_timestamps = |samples: &[RangeSample]| -> Vec<Vec<i64>> {
            let mut result: Vec<Vec<i64>> = samples
                .iter()
                .map(|rs| rs.samples.iter().map(|(ts, _)| *ts).collect())
                .collect();
            result.sort();
            result
        };

        let reader_ts = extract_timestamps(&reader_result);
        let writer_ts = extract_timestamps(&writer_result);

        assert_eq!(
            reader_ts, writer_ts,
            "reader and writer must produce identical timestamps for the same range query"
        );
        // The final step at t+60s must be present (inclusive end).
        let all_ts: Vec<i64> = reader_result
            .iter()
            .flat_map(|rs| rs.samples.iter().map(|(ts, _)| *ts))
            .collect();
        assert!(
            all_ts.contains(&1700000060000),
            "inclusive end (t+60s) must be included; got timestamps: {:?}",
            all_ts,
        );
    }

    #[tokio::test]
    async fn query_range_rejects_zero_step() {
        let storage = create_shared_storage();

        let tsdb = crate::tsdb::Tsdb::new(storage.clone());
        let series = vec![
            Series::builder("counter")
                .label("job", "test")
                .sample(1700000000000, 100.0)
                .build(),
        ];
        tsdb.ingest_samples(series).await.unwrap();
        tsdb.flush().await.unwrap();

        let reader = TimeSeriesDbReader::from_storage(storage);

        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000000);
        let end = SystemTime::UNIX_EPOCH + Duration::from_secs(1700000060);

        let result = reader
            .query_range("counter", start..=end, Duration::ZERO)
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, QueryError::InvalidQuery(_)),
            "expected InvalidQuery, got {:?}",
            err
        );
    }

    #[test]
    fn reader_config_deserializes_default_cache_capacity() {
        let yaml = r#"
storage:
  type: InMemory
"#;
        let config: crate::config::ReaderConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.cache_capacity, crate::config::DEFAULT_CACHE_CAPACITY);
    }

    #[test]
    fn reader_config_honors_custom_cache_capacity() {
        let yaml = r#"
storage:
  type: InMemory
cache_capacity: 200
"#;
        let config: crate::config::ReaderConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.cache_capacity, 200);
    }

    #[test]
    fn from_storage_uses_default_cache_capacity() {
        let storage = create_shared_storage();
        let reader = TimeSeriesDbReader::from_storage(storage);
        assert_eq!(
            reader.query_cache.policy().max_capacity(),
            Some(crate::config::DEFAULT_CACHE_CAPACITY)
        );
    }

    #[test]
    fn from_storage_with_capacity_honors_custom_value() {
        let storage = create_shared_storage();
        let reader = TimeSeriesDbReader::from_storage_with_capacity(storage, 123);
        assert_eq!(reader.query_cache.policy().max_capacity(), Some(123));
    }
}

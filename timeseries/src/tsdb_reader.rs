use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use common::StorageRead;
use moka::future::Cache;
use promql_parser::parser::EvalStmt;
use tokio::sync::RwLock;

use crate::error::QueryError;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::minitsdb::MiniQueryReader;
use crate::model::{
    Label, Labels, MetricMetadata, QueryOptions, QueryValue, RangeSample, Sample, SeriesId,
    TimeBucket,
};
use crate::query::{BucketQueryReader, QueryReader};
use crate::storage::OpenTsdbStorageReadExt;
use crate::tsdb::{
    discover_label_values, discover_labels, discover_series, evaluate_instant, evaluate_range,
    preload_ranges,
};
use crate::util::Result;

/// Read-only multi-bucket time series database.
///
/// Like `Tsdb`, but backed by `StorageRead` (no writer, no fencing).
/// Used by `TimeSeriesDbReader` for safe, non-fencing read access.
pub(crate) struct TsdbReader {
    storage: Arc<dyn StorageRead>,
    /// LRU cache for read-only query buckets.
    query_cache: Cache<TimeBucket, Arc<MiniQueryReader>>,
    /// Metadata catalog (always empty for reader — metadata is populated during writes).
    pub(crate) metadata_catalog: RwLock<HashMap<String, Vec<MetricMetadata>>>,
}

impl TsdbReader {
    pub(crate) fn new(storage: Arc<dyn StorageRead>) -> Self {
        let query_cache = Cache::builder().max_capacity(50).build();

        Self {
            storage,
            query_cache,
            metadata_catalog: RwLock::new(HashMap::new()),
        }
    }

    /// Create a TsdbReaderQueryReader for a time range.
    async fn query_reader(
        &self,
        start_secs: i64,
        end_secs: i64,
    ) -> Result<TsdbReaderQueryReader> {
        let buckets = self
            .storage
            .get_buckets_in_range(Some(start_secs), Some(end_secs))
            .await?;

        let mut readers = Vec::new();
        for bucket in buckets {
            let mini = self.get_or_load_bucket(bucket).await;
            readers.push((bucket, mini));
        }

        Ok(TsdbReaderQueryReader::new(readers))
    }

    /// Create a TsdbReaderQueryReader for disjoint time ranges.
    async fn query_reader_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> Result<TsdbReaderQueryReader> {
        let buckets = self.storage.get_buckets_for_ranges(ranges).await?;

        let mut readers = Vec::new();
        for bucket in buckets {
            let mini = self.get_or_load_bucket(bucket).await;
            readers.push((bucket, mini));
        }

        Ok(TsdbReaderQueryReader::new(readers))
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

    /// Evaluate an instant PromQL query.
    pub(crate) async fn eval_query(
        &self,
        query: &str,
        time: Option<std::time::SystemTime>,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError> {
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

        let query_time = time.unwrap_or_else(std::time::SystemTime::now);
        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start: query_time,
            end: query_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };

        let query_time_secs = query_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let lookback_start_secs = query_time
            .checked_sub(lookback_delta)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs() as i64;

        let ranges = preload_ranges(&stmt, lookback_start_secs, query_time_secs);
        let reader = self.query_reader_for_ranges(&ranges).await?;

        evaluate_instant(&reader, stmt, query_time).await
    }

    /// Evaluate a range PromQL query.
    pub(crate) async fn eval_query_range(
        &self,
        query: &str,
        range: impl std::ops::RangeBounds<std::time::SystemTime>,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;

        let lookback_delta = opts.lookback_delta;
        let stmt = EvalStmt {
            expr,
            start,
            end,
            interval: step,
            lookback_delta,
        };

        let default_start_secs = start
            .checked_sub(lookback_delta)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs() as i64;
        let default_end_secs = end.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        let ranges = preload_ranges(&stmt, default_start_secs, default_end_secs);
        let reader = self.query_reader_for_ranges(&ranges).await?;

        evaluate_range(&reader, stmt).await
    }

    /// Discover series matching any of the given selectors.
    pub(crate) async fn find_series(
        &self,
        matchers: &[&str],
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let reader = self.query_reader(start_secs, end_secs).await?;
        discover_series(&reader, matchers).await
    }

    /// Discover label names, optionally filtered by matchers.
    pub(crate) async fn find_labels(
        &self,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.query_reader(start_secs, end_secs).await?;
        discover_labels(&reader, matchers).await
    }

    /// Discover values for a specific label.
    pub(crate) async fn find_label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.query_reader(start_secs, end_secs).await?;
        discover_label_values(&reader, label_name, matchers).await
    }

    /// Return metadata for all (or a specific) metric.
    pub(crate) async fn find_metadata(
        &self,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        let catalog = self.metadata_catalog.read().await;

        let entries: Vec<MetricMetadata> = match metric {
            Some(name) => catalog.get(name).cloned().unwrap_or_default(),
            None => catalog.values().flatten().cloned().collect(),
        };

        Ok(entries)
    }
}

/// QueryReader implementation for read-only access.
///
/// Wraps `Arc<MiniQueryReader>` per bucket (instead of owned `MiniQueryReader` in `TsdbQueryReader`).
struct TsdbReaderQueryReader {
    mini_readers: HashMap<TimeBucket, Arc<MiniQueryReader>>,
}

impl TsdbReaderQueryReader {
    fn new(readers: Vec<(TimeBucket, Arc<MiniQueryReader>)>) -> Self {
        Self {
            mini_readers: readers.into_iter().collect(),
        }
    }
}

#[async_trait]
impl QueryReader for TsdbReaderQueryReader {
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

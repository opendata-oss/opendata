use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use common::Storage;
use moka::future::Cache;
use promql_parser::parser::EvalStmt;
use tokio::sync::RwLock;
use tracing::error;

use crate::error::QueryError;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::minitsdb::{MiniQueryReader, MiniTsdb};
use crate::model::{
    InstantSample, Label, Labels, MetricMetadata, QueryOptions, QueryValue, RangeSample, Sample,
    Series, SeriesId, TimeBucket,
};
use crate::promql::evaluator::{Evaluator, ExprResult};
use crate::promql::tsdb_router::{get_matching_series_multi_bucket, preload_ranges};
use crate::query::{BucketQueryReader, QueryReader};
use crate::storage::OpenTsdbStorageReadExt;
use crate::util::Result;

/// Multi-bucket time series database.
///
/// Tsdb manages multiple MiniTsdb instances (one per time bucket) and provides
/// a unified QueryReader interface that merges results across buckets.
pub(crate) struct Tsdb {
    storage: Arc<dyn Storage>,

    // TODO(rohan): weird things can happen if these get out of sync
    //  (e.g. ingest cache purged while query cache is present)
    /// TTI cache (15 min idle) for buckets being actively ingested into.
    /// Also used during queries - checked first before query_cache.
    ingest_cache: Cache<TimeBucket, Arc<MiniTsdb>>,

    /// LRU cache (50 max) for read-only query buckets.
    /// Only populated for buckets NOT in ingest_cache.
    query_cache: Cache<TimeBucket, Arc<MiniTsdb>>,

    // Metadata catalog (keyed by metric name)
    pub(crate) metadata_catalog: RwLock<HashMap<String, Vec<MetricMetadata>>>,
}

impl Tsdb {
    pub(crate) fn new(storage: Arc<dyn Storage>) -> Self {
        // TTI cache: 15 minute idle timeout for ingest buckets
        let ingest_cache = Cache::builder()
            .time_to_idle(Duration::from_secs(15 * 60))
            .build();

        // LRU cache: max 50 buckets for query
        let query_cache = Cache::builder().max_capacity(50).build();

        Self {
            storage,
            ingest_cache,
            query_cache,
            metadata_catalog: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a MiniTsdb for ingestion into a specific bucket.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn get_or_create_for_ingest(
        &self,
        bucket: TimeBucket,
    ) -> Result<Arc<MiniTsdb>> {
        // Try ingest cache first
        if let Some(mini) = self.ingest_cache.get(&bucket).await {
            return Ok(mini);
        }

        // Load from storage and put in ingest cache
        let mini = Arc::new(MiniTsdb::load(bucket, self.storage.clone()).await?);
        self.ingest_cache.insert(bucket, mini.clone()).await;
        Ok(mini)
    }

    /// Get a MiniTsdb for a bucket, checking ingest cache first, then query cache.
    async fn get_bucket(&self, bucket: TimeBucket) -> Result<Arc<MiniTsdb>> {
        // 1. Check ingest cache first (has freshest data)
        if let Some(mini) = self.ingest_cache.get(&bucket).await {
            return Ok(mini);
        }

        // 2. Check query cache
        if let Some(mini) = self.query_cache.get(&bucket).await {
            return Ok(mini);
        }

        // 3. Load from storage into query cache (NOT ingest cache)
        let mini = Arc::new(MiniTsdb::load(bucket, self.storage.clone()).await?);
        self.query_cache.insert(bucket, mini.clone()).await;
        Ok(mini)
    }

    /// Create a QueryReader for a time range.
    /// This discovers all buckets covering the range and returns a TsdbQueryReader
    /// that properly handles bucket-scoped series IDs.
    pub(crate) async fn query_reader(
        &self,
        start_secs: i64,
        end_secs: i64,
    ) -> Result<TsdbQueryReader> {
        // TODO(rohan): its weird that we use a snapshot here and the minitsdbs have a different snapshot
        let snapshot = self.storage.snapshot().await?;

        // Discover buckets that cover the query range
        let buckets = snapshot
            .get_buckets_in_range(Some(start_secs), Some(end_secs))
            .await?;

        // Load MiniTsdbs for each bucket (from cache or storage)
        let mut readers = Vec::new();
        for bucket in buckets {
            let mini = self.get_bucket(bucket).await?;
            let reader = mini.query_reader();
            readers.push((bucket, reader));
        }

        Ok(TsdbQueryReader::new(self.storage.clone(), readers))
    }

    /// Create a QueryReader for a set of disjoint time ranges.
    /// Discovers all buckets overlapping any range and returns a TsdbQueryReader.
    pub(crate) async fn query_reader_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> Result<TsdbQueryReader> {
        let snapshot = self.storage.snapshot().await?;
        let buckets = snapshot.get_buckets_for_ranges(ranges).await?;

        let mut readers = Vec::new();
        for bucket in buckets {
            let mini = self.get_bucket(bucket).await?;
            let reader = mini.query_reader();
            readers.push((bucket, reader));
        }

        Ok(TsdbQueryReader::new(self.storage.clone(), readers))
    }

    /// Flush all dirty buckets in the ingest cache to storage.
    pub(crate) async fn flush(&self) -> Result<()> {
        // Note: moka's iter() returns a clone of the current entries
        for (_, mini) in &self.ingest_cache {
            mini.flush().await?;
        }
        Ok(())
    }

    /// Ingest series into the TSDB.
    /// Each series is split by time bucket based on sample timestamps.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            series_count = series_list.len(),
            total_samples = tracing::field::Empty,
            buckets_touched = tracing::field::Empty
        )
    )]
    pub(crate) async fn ingest_samples(&self, series_list: Vec<Series>) -> Result<()> {
        let mut bucket_series_map: HashMap<TimeBucket, Vec<Series>> = HashMap::new();
        let mut total_samples = 0;

        // First pass: group all series by bucket
        for series in series_list {
            let series_sample_count = series.samples.len();
            total_samples += series_sample_count;

            if let Some(metric_name) = series
                .labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.clone())
            {
                let entry = MetricMetadata {
                    metric_name: metric_name.clone(),
                    metric_type: series.metric_type,
                    description: series.description.clone(),
                    unit: series.unit.clone(),
                };
                let mut catalog = self.metadata_catalog.write().await;
                let entries = catalog.entry(metric_name).or_default();
                if !entries.contains(&entry) {
                    entries.push(entry);
                }
            }

            // Group samples by bucket for this series
            let mut bucket_samples: HashMap<TimeBucket, Vec<Sample>> = HashMap::new();

            for sample in series.samples {
                let bucket = TimeBucket::round_to_hour(
                    std::time::UNIX_EPOCH
                        + std::time::Duration::from_millis(sample.timestamp_ms as u64),
                )?;
                bucket_samples.entry(bucket).or_default().push(sample);
            }

            // Create a series for each bucket and add to bucket_series_map
            for (bucket, samples) in bucket_samples {
                let bucket_series = Series {
                    labels: series.labels.clone(),
                    metric_type: series.metric_type,
                    unit: series.unit.clone(),
                    description: series.description.clone(),
                    samples,
                };
                bucket_series_map
                    .entry(bucket)
                    .or_default()
                    .push(bucket_series);
            }
        }

        let buckets_touched = bucket_series_map.len();

        // Second pass: ingest all series for each bucket in a single batch
        for (bucket, series_list) in bucket_series_map {
            let series_count = series_list.len();
            let samples_count: usize = series_list.iter().map(|s| s.samples.len()).sum();

            tracing::debug!(
                bucket = ?bucket,
                series_count = series_count,
                samples_count = samples_count,
                "Ingesting batch into bucket"
            );

            let mini = match self.get_or_create_for_ingest(bucket).await {
                Ok(mini) => mini,
                Err(err) => {
                    error!("failed to load minitsdb: {:?}: {:?}", bucket, err);
                    return Err(err);
                }
            };
            mini.ingest_batch(&series_list).await?;

            tracing::debug!(
                bucket = ?bucket,
                series_count = series_count,
                samples_count = samples_count,
                "Bucket batch ingestion completed"
            );
        }

        // Record final metrics on the main span
        tracing::Span::current().record("total_samples", total_samples);
        tracing::Span::current().record("buckets_touched", buckets_touched);

        tracing::debug!(
            total_samples = total_samples,
            buckets_touched = buckets_touched,
            "Completed ingesting all samples"
        );

        Ok(())
    }

    /// Evaluate an instant PromQL query, returning typed `InstantSample`s.
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

        let mut evaluator = Evaluator::new(&reader);
        let result = evaluator.evaluate(stmt).await?;

        match result {
            ExprResult::Scalar(value) => {
                let timestamp_ms =
                    query_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
                Ok(QueryValue::Scalar {
                    timestamp_ms,
                    value,
                })
            }
            ExprResult::InstantVector(samples) => Ok(QueryValue::Vector(
                samples
                    .into_iter()
                    .map(|s| InstantSample {
                        labels: Labels::from(s.labels),
                        timestamp_ms: s.timestamp_ms,
                        value: s.value,
                    })
                    .collect(),
            )),
            ExprResult::RangeVector(_) => Err(QueryError::Execution(
                "range vectors not supported for instant queries".to_string(),
            )),
        }
    }

    /// Evaluate a range PromQL query, returning typed `RangeSample`s.
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

        // Step through time, collecting per-series samples
        let mut series_map: HashMap<Labels, Vec<(i64, f64)>> = HashMap::new();
        let mut evaluator = Evaluator::new(&reader);
        let mut current_time = start;

        while current_time <= end {
            let instant_stmt = EvalStmt {
                expr: stmt.expr.clone(),
                start: current_time,
                end: current_time,
                interval: Duration::from_secs(0),
                lookback_delta,
            };

            let result = evaluator.evaluate(instant_stmt).await?;
            let timestamp_ms = current_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

            match result {
                ExprResult::InstantVector(samples) => {
                    for sample in samples {
                        let labels = Labels::from(sample.labels);
                        series_map
                            .entry(labels)
                            .or_default()
                            .push((sample.timestamp_ms, sample.value));
                    }
                }
                ExprResult::Scalar(value) => {
                    let labels = Labels::new(vec![]);
                    series_map
                        .entry(labels)
                        .or_default()
                        .push((timestamp_ms, value));
                }
                ExprResult::RangeVector(_) => {
                    return Err(QueryError::Execution(
                        "range vectors not supported in range query evaluation".to_string(),
                    ));
                }
            }

            current_time += step;
        }

        Ok(series_map
            .into_iter()
            .map(|(labels, samples)| RangeSample { labels, samples })
            .collect())
    }

    /// Discover series matching any of the given selectors.
    pub(crate) async fn find_series(
        &self,
        matchers: &[&str],
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        if matchers.is_empty() {
            return Err(QueryError::InvalidQuery(
                "at least one match[] required".to_string(),
            ));
        }

        let reader = self.query_reader(start_secs, end_secs).await?;
        let buckets = reader.list_buckets().await?;
        if buckets.is_empty() {
            return Ok(vec![]);
        }

        let matches: Vec<String> = matchers.iter().map(|s| s.to_string()).collect();
        let bucket_series_map = get_matching_series_multi_bucket(&reader, &buckets, &matches)
            .await
            .map_err(QueryError::InvalidQuery)?;

        let mut unique_series: HashSet<Labels> = HashSet::new();

        for (bucket, series_ids) in bucket_series_map {
            let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
            if series_ids_vec.is_empty() {
                continue;
            }
            let forward_index = reader.forward_index(&bucket, &series_ids_vec).await?;
            for (_id, spec) in forward_index.all_series() {
                let mut sorted_labels = spec.labels.clone();
                sorted_labels.sort();
                unique_series.insert(Labels::new(sorted_labels));
            }
        }

        let mut result: Vec<Labels> = unique_series.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Discover label names, optionally filtered by matchers.
    pub(crate) async fn find_labels(
        &self,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.query_reader(start_secs, end_secs).await?;
        let buckets = reader.list_buckets().await?;
        if buckets.is_empty() {
            return Ok(vec![]);
        }

        let mut label_names: HashSet<String> = HashSet::new();

        match matchers {
            Some(matches) if !matches.is_empty() => {
                let matches: Vec<String> = matches.iter().map(|s| s.to_string()).collect();
                let bucket_series_map =
                    get_matching_series_multi_bucket(&reader, &buckets, &matches)
                        .await
                        .map_err(QueryError::InvalidQuery)?;

                for (bucket, series_ids) in bucket_series_map {
                    let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
                    if series_ids_vec.is_empty() {
                        continue;
                    }
                    let forward_index = reader.forward_index(&bucket, &series_ids_vec).await?;
                    for (_id, spec) in forward_index.all_series() {
                        for attr in &spec.labels {
                            label_names.insert(attr.name.clone());
                        }
                    }
                }
            }
            _ => {
                for bucket in buckets {
                    let inverted_index = reader.all_inverted_index(&bucket).await?;
                    for attr in inverted_index.all_keys() {
                        label_names.insert(attr.name);
                    }
                }
            }
        }

        let mut result: Vec<String> = label_names.into_iter().collect();
        result.sort();
        Ok(result)
    }

    /// Discover values for a specific label, optionally filtered by matchers.
    pub(crate) async fn find_label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.query_reader(start_secs, end_secs).await?;
        let buckets = reader.list_buckets().await?;
        if buckets.is_empty() {
            return Ok(vec![]);
        }

        let mut values: HashSet<String> = HashSet::new();

        match matchers {
            Some(matches) if !matches.is_empty() => {
                let matches: Vec<String> = matches.iter().map(|s| s.to_string()).collect();
                let bucket_series_map =
                    get_matching_series_multi_bucket(&reader, &buckets, &matches)
                        .await
                        .map_err(QueryError::InvalidQuery)?;

                for (bucket, series_ids) in bucket_series_map {
                    let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
                    if series_ids_vec.is_empty() {
                        continue;
                    }
                    let forward_index = reader.forward_index(&bucket, &series_ids_vec).await?;
                    for (_id, spec) in forward_index.all_series() {
                        for attr in &spec.labels {
                            if attr.name == label_name {
                                values.insert(attr.value.clone());
                            }
                        }
                    }
                }
            }
            _ => {
                for bucket in buckets {
                    let label_vals = reader.label_values(&bucket, label_name).await?;
                    values.extend(label_vals);
                }
            }
        }

        let mut result: Vec<String> = values.into_iter().collect();
        result.sort();
        Ok(result)
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

/// QueryReader implementation that properly handles bucket-scoped series IDs.
pub(crate) struct TsdbQueryReader {
    /// Map from bucket to MiniTsdb for efficient bucket queries
    mini_readers: HashMap<TimeBucket, MiniQueryReader>,
}

impl TsdbQueryReader {
    pub fn new(_storage: Arc<dyn Storage>, mini_tsdbs: Vec<(TimeBucket, MiniQueryReader)>) -> Self {
        let bucket_minis = mini_tsdbs.into_iter().collect();
        Self {
            mini_readers: bucket_minis,
        }
    }
}

#[async_trait]
impl QueryReader for TsdbQueryReader {
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
        // TODO: this should be internal error
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MetricType;
    use crate::promql::evaluator::EvalSample;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::storage::in_memory::InMemoryStorage;

    async fn create_test_storage() -> Arc<dyn Storage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )))
    }

    fn create_sample(
        metric_name: &str,
        label_pairs: Vec<(&str, &str)>,
        timestamp: i64,
        value: f64,
    ) -> Series {
        let mut labels = vec![Label {
            name: "__name__".to_string(),
            value: metric_name.to_string(),
        }];
        for (key, val) in label_pairs {
            labels.push(Label {
                name: key.to_string(),
                value: val.to_string(),
            });
        }
        Series {
            labels,
            unit: None,
            metric_type: Some(MetricType::Gauge),
            description: None,
            samples: vec![Sample {
                timestamp_ms: timestamp,
                value,
            }],
        }
    }

    #[tokio::test]
    async fn should_create_tsdb_with_caches() {
        // given
        let storage = create_test_storage().await;

        // when
        let tsdb = Tsdb::new(storage);

        // then: tsdb is created successfully
        // Sync caches to ensure counts are accurate
        tsdb.ingest_cache.run_pending_tasks().await;
        tsdb.query_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 0);
        assert_eq!(tsdb.query_cache.entry_count(), 0);
    }

    #[tokio::test]
    async fn should_get_or_create_bucket_for_ingest() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket = TimeBucket::hour(1000);

        // when
        let mini1 = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // then: same Arc is returned (cached)
        assert!(Arc::ptr_eq(&mini1, &mini2));
        tsdb.ingest_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 1);
    }

    #[tokio::test]
    async fn should_use_ingest_cache_during_queries() {
        // given: a bucket in the ingest cache
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket = TimeBucket::hour(1000);

        // Put bucket in ingest cache
        let mini_ingest = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // when: getting the same bucket for query
        let mini_query = tsdb.get_bucket(bucket).await.unwrap();

        // then: should return the same instance from ingest cache
        assert!(Arc::ptr_eq(&mini_ingest, &mini_query));
        // Query cache should still be empty
        tsdb.query_cache.run_pending_tasks().await;
        assert_eq!(tsdb.query_cache.entry_count(), 0);
    }

    #[tokio::test]
    async fn should_use_query_cache_for_non_ingest_buckets() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket = TimeBucket::hour(1000);

        // when: getting a bucket not in ingest cache
        let mini1 = tsdb.get_bucket(bucket).await.unwrap();
        let mini2 = tsdb.get_bucket(bucket).await.unwrap();

        // then: same Arc is returned (cached in query cache)
        assert!(Arc::ptr_eq(&mini1, &mini2));
        tsdb.query_cache.run_pending_tasks().await;
        tsdb.ingest_cache.run_pending_tasks().await;
        assert_eq!(tsdb.query_cache.entry_count(), 1);
        assert_eq!(tsdb.ingest_cache.entry_count(), 0);
    }

    #[tokio::test]
    async fn should_ingest_and_query_single_bucket() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Use hour-aligned bucket (60 minutes = 1 hour)
        // Bucket at minute 60 covers minutes 60-119, i.e., seconds 3600-7199
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // Ingest a sample with timestamp in the bucket range (seconds 3600-7199)
        // Using 4000 seconds = 4000000 ms
        let sample = create_sample("http_requests", vec![("env", "prod")], 4000000, 42.0);
        mini.ingest(&sample).await.unwrap();

        // Flush to make data visible
        tsdb.flush().await.unwrap();

        // when: query the data with range covering the bucket (seconds 3600-7200)
        let reader = tsdb.query_reader(3600, 7200).await.unwrap();
        let terms = vec![Label {
            name: "__name__".to_string(),
            value: "http_requests".to_string(),
        }];
        let bucket = TimeBucket::hour(60);
        let index = reader.inverted_index(&bucket, &terms).await.unwrap();
        let series_ids: Vec<_> = index.intersect(terms).iter().collect();

        // then
        assert_eq!(series_ids.len(), 1);
    }

    #[tokio::test]
    async fn should_query_across_multiple_buckets_with_evaluator() {
        use crate::promql::evaluator::Evaluator;
        use crate::test_utils::assertions::assert_approx_eq;
        use promql_parser::parser::EvalStmt;
        use std::time::{Duration, UNIX_EPOCH};

        // given: 4 hour-aligned buckets
        // Bucket layout (each bucket is 1 hour = 60 minutes):
        //   Bucket 60:  minutes 60-119,  seconds 3600-7199,   ms 3,600,000-7,199,999
        //   Bucket 120: minutes 120-179, seconds 7200-10799,  ms 7,200,000-10,799,999
        //   Bucket 180: minutes 180-239, seconds 10800-14399, ms 10,800,000-14,399,999
        //   Bucket 240: minutes 240-299, seconds 14400-17999, ms 14,400,000-17,999,999
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Buckets 1 & 2: will end up in query cache (ingest, flush, then invalidate from ingest cache)
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);

        // Buckets 3 & 4: will stay in ingest cache
        let bucket3 = TimeBucket::hour(180);
        let bucket4 = TimeBucket::hour(240);

        // Ingest data into buckets 1 & 2
        // Sample timestamps should be well within the bucket and reachable by lookback
        // Bucket 60: covers 3,600,000-7,199,999 ms -> sample at 3,900,000 ms (3900s)
        // Bucket 120: covers 7,200,000-10,799,999 ms -> sample at 7,900,000 ms (7900s)
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        mini1
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                3_900_000,
                10.0,
            ))
            .await
            .unwrap();
        mini1
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                3_900_001,
                15.0,
            ))
            .await
            .unwrap();

        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini2
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                7_900_000,
                20.0,
            ))
            .await
            .unwrap();
        mini2
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                7_900_001,
                25.0,
            ))
            .await
            .unwrap();

        // Flush buckets 1 & 2 to storage
        tsdb.flush().await.unwrap();

        // Invalidate buckets 1 & 2 from ingest cache so they'll be loaded from query cache
        tsdb.ingest_cache.invalidate(&bucket1).await;
        tsdb.ingest_cache.invalidate(&bucket2).await;
        tsdb.ingest_cache.run_pending_tasks().await;

        // Ingest data into buckets 3 & 4 (these stay in ingest cache)
        // Bucket 180: covers 10,800,000-14,399,999 ms -> sample at 11,900,000 ms (11900s)
        // Bucket 240: covers 14,400,000-17,999,999 ms -> sample at 15,900,000 ms (15900s)
        let mini3 = tsdb.get_or_create_for_ingest(bucket3).await.unwrap();
        mini3
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                11_900_000,
                30.0,
            ))
            .await
            .unwrap();
        mini3
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                11_900_001,
                35.0,
            ))
            .await
            .unwrap();

        let mini4 = tsdb.get_or_create_for_ingest(bucket4).await.unwrap();
        mini4
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                15_900_000,
                40.0,
            ))
            .await
            .unwrap();
        mini4
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                15_900_001,
                45.0,
            ))
            .await
            .unwrap();

        // Flush buckets 3 & 4 to storage (data is now visible for queries)
        tsdb.flush().await.unwrap();

        // Verify cache state: 2 in ingest cache, 0 in query cache (query cache populated on read)
        tsdb.ingest_cache.run_pending_tasks().await;
        tsdb.query_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 2);
        assert_eq!(tsdb.query_cache.entry_count(), 0);

        // when: query across all 4 buckets using the evaluator
        // Query range: seconds 3600-18000 covers all 4 buckets
        let reader = tsdb.query_reader(3600, 18000).await.unwrap();

        // Verify cache state after query: 2 in ingest cache, 2 in query cache
        tsdb.ingest_cache.run_pending_tasks().await;
        tsdb.query_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 2);
        assert_eq!(tsdb.query_cache.entry_count(), 2);

        // Use the evaluator to run 4 separate instant queries, one per bucket
        let mut evaluator = Evaluator::new(&reader);
        let query = r#"http_requests"#;
        let lookback = Duration::from_secs(1000);

        // Query times: one point in each bucket where we expect to find data
        // Bucket 60:  sample at 3,900,000 ms (3900s) -> query at 4000s
        // Bucket 120: sample at 7,900,000 ms (7900s) -> query at 8000s
        // Bucket 180: sample at 11,900,000 ms (11900s) -> query at 12000s
        // Bucket 240: sample at 15,900,000 ms (15900s) -> query at 16000s
        // Lookback of 1000s ensures samples are within the window
        let query_times_secs = [4000u64, 8000, 12000, 16000];
        let expected_prod_values = [10.0, 20.0, 30.0, 40.0];
        let expected_staging_values = [15.0, 25.0, 35.0, 45.0];

        for (i, &query_time_secs) in query_times_secs.iter().enumerate() {
            let expr = promql_parser::parser::parse(query).unwrap();
            let query_time = UNIX_EPOCH + Duration::from_secs(query_time_secs);
            let stmt = EvalStmt {
                expr,
                start: query_time,
                end: query_time,
                interval: Duration::from_secs(0),
                lookback_delta: lookback,
            };

            let mut results = evaluator
                .evaluate(stmt)
                .await
                .unwrap()
                .expect_instant_vector("Expected instant vector result");
            results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

            assert_eq!(
                results.len(),
                2,
                "query {} at {}s: expected 2 results",
                i,
                query_time_secs
            );

            // env=prod
            assert_eq!(results[0].labels.get("env"), Some(&"prod".to_string()));
            assert_approx_eq(results[0].value, expected_prod_values[i]);

            // env=staging
            assert_eq!(results[1].labels.get("env"), Some(&"staging".to_string()));
            assert_approx_eq(results[1].value, expected_staging_values[i]);
        }
    }

    #[tokio::test]
    async fn should_query_across_multiple_buckets_with_different_series_id_mappings() {
        use crate::promql::evaluator::Evaluator;
        use promql_parser::parser::EvalStmt;
        use std::time::{Duration, UNIX_EPOCH};

        // given: Two time buckets with overlapping series but different series IDs
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        // Bucket 1: hour 60 (covers 3,600,000-7,199,999 ms)
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        // Add series in bucket 1: foo{a="b",x="y"} and foo{a="c",x="z"}
        mini1
            .ingest(&create_sample(
                "foo",
                vec![("a", "b"), ("x", "y")],
                3_900_000,
                1.0,
            ))
            .await
            .unwrap();
        mini1
            .ingest(&create_sample(
                "foo",
                vec![("a", "c"), ("x", "z")],
                3_900_001,
                2.0,
            ))
            .await
            .unwrap();
        // Bucket 2: hour 120 (covers 7,200,000-10,799,999 ms)
        let bucket2 = TimeBucket::hour(120);
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        // Add series in bucket 2: foo{a="c",x="z"} and foo{a="d",x="w"}
        mini2
            .ingest(&create_sample(
                "foo",
                vec![("a", "c"), ("x", "z")],
                7_900_000,
                3.0,
            ))
            .await
            .unwrap();
        mini2
            .ingest(&create_sample(
                "foo",
                vec![("a", "d"), ("x", "w")],
                7_900_001,
                4.0,
            ))
            .await
            .unwrap();
        // Flush to storage
        tsdb.flush().await.unwrap();

        // when: Execute a PromQL query that filters by a="c"
        let reader = tsdb.query_reader(3000, 8000).await.unwrap();
        let mut evaluator = Evaluator::new(&reader);
        // Query for foo{a="c"} at time 8000 seconds (in bucket 2)
        let query = r#"foo{a="c"}"#;
        let query_time = UNIX_EPOCH + Duration::from_secs(8000);
        let lookback = Duration::from_secs(5000);
        let expr = promql_parser::parser::parse(query).unwrap();
        let stmt = EvalStmt {
            expr,
            start: query_time,
            end: query_time,
            interval: Duration::from_secs(0),
            lookback_delta: lookback,
        };
        let results = evaluator
            .evaluate(stmt)
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        // then: we should only get the series foo{a="c",x="z"} with value 3.0
        let expected = vec![EvalSample {
            timestamp_ms: 7_900_000,
            value: 3.0,
            labels: [("a", "c"), ("x", "z"), ("__name__", "foo")]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            drop_name: false,
        }];
        assert_eq!(results, expected);
    }

    // ── Native read method tests ─────────────────────────────────────

    async fn create_tsdb_with_data() -> Tsdb {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Ingest two series into bucket at minute 60 (covers 3,600,000–7,199,999 ms)
        let series = vec![
            create_sample("http_requests", vec![("env", "prod")], 4_000_000, 42.0),
            create_sample("http_requests", vec![("env", "staging")], 4_000_000, 10.0),
        ];
        tsdb.ingest_samples(series).await.unwrap();
        tsdb.flush().await.unwrap();
        tsdb
    }

    #[tokio::test]
    async fn eval_query_should_return_instant_vector() {
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("http_requests", Some(query_time), &opts)
            .await
            .unwrap();
        let mut samples = match result {
            QueryValue::Vector(samples) => samples,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| {
            a.labels
                .metric_name()
                .cmp(b.labels.metric_name())
                .then_with(|| a.labels.get("env").cmp(&b.labels.get("env")))
        });

        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("env"), Some("prod"));
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[1].labels.get("env"), Some("staging"));
        assert_eq!(samples[1].value, 10.0);
    }

    #[tokio::test]
    async fn eval_query_should_respect_lookback_delta() {
        // Sample at t=4000s, query at t=4100s (100s later).
        // Default 5m lookback finds it; 10s lookback should not.
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);

        let wide = QueryOptions::default(); // 5m
        let results = tsdb
            .eval_query("http_requests", Some(query_time), &wide)
            .await
            .unwrap()
            .into_samples();
        assert_eq!(results.len(), 2);

        let narrow = QueryOptions {
            lookback_delta: std::time::Duration::from_secs(10),
        };
        let results = tsdb
            .eval_query("http_requests", Some(query_time), &narrow)
            .await
            .unwrap()
            .into_samples();
        assert_eq!(
            results.len(),
            0,
            "10s lookback should miss samples 100s ago"
        );
    }

    #[tokio::test]
    async fn eval_query_range_should_respect_lookback_delta() {
        // Same idea but for range queries: narrow lookback → no results.
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);
        let end = start;
        let step = std::time::Duration::from_secs(60);

        let wide = QueryOptions::default();
        let results = tsdb
            .eval_query_range("http_requests", start..=end, step, &wide)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        let narrow = QueryOptions {
            lookback_delta: std::time::Duration::from_secs(10),
        };
        let results = tsdb
            .eval_query_range("http_requests", start..=end, step, &narrow)
            .await
            .unwrap();
        assert!(
            results.is_empty(),
            "10s lookback should miss samples 100s ago"
        );
    }

    #[tokio::test]
    async fn eval_query_should_return_scalar() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("1+1", Some(query_time), &opts)
            .await
            .unwrap();

        match result {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => {
                assert_eq!(value, 2.0);
                assert_eq!(
                    timestamp_ms,
                    query_time
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64
                );
            }
            other => panic!("expected Scalar, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn eval_query_should_return_error_for_invalid_query() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let opts = QueryOptions::default();
        let result = tsdb.eval_query("invalid{", None, &opts).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[tokio::test]
    async fn eval_query_range_should_return_range_samples() {
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4000);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4000);
        let step = std::time::Duration::from_secs(60);

        let opts = QueryOptions::default();
        let mut results = tsdb
            .eval_query_range("http_requests", start..=end, step, &opts)
            .await
            .unwrap();
        results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].labels.get("env"), Some("prod"));
        assert!(!results[0].samples.is_empty());
        assert_eq!(results[1].labels.get("env"), Some("staging"));
    }

    #[tokio::test]
    async fn eval_query_range_should_return_scalar() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(160);
        let step = std::time::Duration::from_secs(60);

        let opts = QueryOptions::default();
        let results = tsdb
            .eval_query_range("1+1", start..=end, step, &opts)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].labels.metric_name(), "");
        assert_eq!(results[0].samples.len(), 2); // two steps: 100s and 160s
        assert_eq!(results[0].samples[0].1, 2.0);
        assert_eq!(results[0].samples[1].1, 2.0);
    }

    #[tokio::test]
    async fn eval_query_range_should_return_error_for_invalid_query() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(200);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query_range(
                "invalid{",
                start..=end,
                std::time::Duration::from_secs(60),
                &opts,
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[tokio::test]
    async fn find_series_should_return_matching_series() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_series(&["http_requests"], 3600, 7200)
            .await
            .unwrap();
        results.sort_by(|a, b| a.get("env").cmp(&b.get("env")));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("env"), Some("prod"));
        assert_eq!(results[0].metric_name(), "http_requests");
        assert_eq!(results[1].get("env"), Some("staging"));
    }

    #[tokio::test]
    async fn find_series_should_error_on_empty_matchers() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let result = tsdb.find_series(&[], 0, i64::MAX).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[tokio::test]
    async fn find_series_should_dedup_across_buckets() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2]).await.unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb.find_series(&["cpu"], 0, i64::MAX).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name(), "cpu");
    }

    #[tokio::test]
    async fn find_labels_should_return_all_label_names() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb.find_labels(None, 3600, 7200).await.unwrap();
        results.sort();

        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"env".to_string()));
    }

    #[tokio::test]
    async fn find_labels_should_filter_by_matcher() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_labels(Some(&[r#"http_requests{env="prod"}"#]), 3600, 7200)
            .await
            .unwrap();
        results.sort();

        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"env".to_string()));
    }

    #[tokio::test]
    async fn find_labels_should_return_empty_for_no_data() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let results = tsdb.find_labels(None, 0, 100).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn find_label_values_should_return_values() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_label_values("env", None, 3600, 7200)
            .await
            .unwrap();
        results.sort();

        assert_eq!(results, vec!["prod", "staging"]);
    }

    #[tokio::test]
    async fn find_label_values_should_filter_by_matcher() {
        let tsdb = create_tsdb_with_data().await;

        let results = tsdb
            .find_label_values("env", Some(&[r#"http_requests{env="prod"}"#]), 3600, 7200)
            .await
            .unwrap();

        assert_eq!(results, vec!["prod"]);
    }

    #[tokio::test]
    async fn find_label_values_should_return_empty_for_no_data() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let results = tsdb.find_label_values("env", None, 0, 100).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn find_metadata_should_return_all() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let mut series = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        series.description = Some("CPU usage".to_string());
        series.unit = Some("percent".to_string());
        tsdb.ingest_samples(vec![series]).await.unwrap();

        let results = tsdb.find_metadata(None).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name, "cpu");
        assert_eq!(results[0].metric_type, Some(MetricType::Gauge));
        assert_eq!(results[0].description, Some("CPU usage".to_string()));
        assert_eq!(results[0].unit, Some("percent".to_string()));
    }

    // -----------------------------------------------------------------------
    // Offset / @ modifier tests (bucket preloading)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn eval_query_with_offset_should_load_correct_bucket() {
        // Data at 4000s (bucket hour-60: 3600–7199s).
        // Query at 7600s with offset 1h → effective time = 4000s.
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7600);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query("http_requests offset 1h", Some(query_time), &opts)
            .await
            .unwrap();
        let samples = result.into_samples();
        assert_eq!(samples.len(), 2, "offset 1h should find data at 4000s");
    }

    #[tokio::test]
    async fn eval_query_range_with_offset_crossing_bucket() {
        // Data at 4000s. Range [7600,7660] with offset 1h → effective [4000,4060].
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7600);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7660);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("http_requests offset 1h", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(!results.is_empty(), "offset range query should find data");
        for rs in &results {
            assert!(!rs.samples.is_empty());
        }
    }

    #[tokio::test]
    async fn eval_query_with_offset_before_epoch_should_not_error() {
        // Query at 100s with offset 1h → effective time = -3500s (before epoch).
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query("up offset 1h", Some(query_time), &opts)
            .await
            .unwrap();
        let samples = result.into_samples();
        assert!(
            samples.is_empty(),
            "before-epoch offset should return empty"
        );
    }

    #[tokio::test]
    async fn eval_query_range_with_at_before_epoch_should_not_error() {
        // `@ 0 offset 1h` pins evaluation to t=0, then offset pushes to -3600.
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1000);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2000);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("up @ 0 offset 1h", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(
            results.is_empty() || results.iter().all(|rs| rs.samples.is_empty()),
            "@ 0 offset 1h should return empty matrix"
        );
    }

    #[tokio::test]
    async fn eval_query_range_with_at_end_should_load_correct_bucket() {
        // Data at 4000s. Range [4000,8000]. `@ end()` pins to 8000s.
        // With default 5m lookback, sample at 4000s is within range from 8000.
        // Actually, @ end() pins evaluation to t=8000 for each step, but
        // lookback only covers 5min=300s. Data at 4000s is 4000s before 8000s,
        // so it won't be found by lookback. Let's use a range where @ end()
        // helps: range [3900,4100], data at 4000s, `@ end()` pins to 4100s,
        // lookback 5min covers it.
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(3900);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("http_requests @ end()", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(!results.is_empty(), "@ end() should find data");
        // All steps should see the same sample (pinned to end)
        for rs in &results {
            assert!(!rs.samples.is_empty());
        }
    }

    // -----------------------------------------------------------------------
    // Multi-bucket dedup for labels and label_values
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn find_labels_should_dedup_across_buckets() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2]).await.unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb.find_labels(None, 0, i64::MAX).await.unwrap();

        // Label names should not be duplicated
        let mut sorted = results.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            results.len(),
            sorted.len(),
            "label names should be deduplicated across buckets"
        );
        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"host".to_string()));
    }

    #[tokio::test]
    async fn find_label_values_should_dedup_across_buckets() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2]).await.unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb
            .find_label_values("host", None, 0, i64::MAX)
            .await
            .unwrap();

        assert_eq!(
            results,
            vec!["a"],
            "label values should be deduplicated across buckets"
        );
    }

    #[tokio::test]
    async fn find_metadata_should_filter_by_metric() {
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        tsdb.ingest_samples(vec![
            create_sample("cpu", vec![], 4_000_000, 1.0),
            create_sample("mem", vec![], 4_000_000, 2.0),
        ])
        .await
        .unwrap();

        let results = tsdb.find_metadata(Some("cpu")).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name, "cpu");

        let results = tsdb.find_metadata(Some("nonexistent")).await.unwrap();
        assert!(results.is_empty());
    }
}

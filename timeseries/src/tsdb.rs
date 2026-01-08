#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common::Storage;
use moka::future::Cache;

use crate::index::{ForwardIndex, ForwardIndexLookup, InvertedIndex, InvertedIndexLookup};
use crate::minitsdb::MiniTsdb;
use crate::model::{Label, Sample, Series, SeriesId, TimeBucket};
use crate::query::QueryReader;
use crate::storage::OpenTsdbStorageReadExt;
use crate::util::Result;

/// Multi-bucket time series database.
///
/// Tsdb manages multiple MiniTsdb instances (one per time bucket) and provides
/// a unified QueryReader interface that merges results across buckets.
pub(crate) struct Tsdb {
    storage: Arc<dyn Storage>,

    /// TTI cache (15 min idle) for buckets being actively ingested into.
    /// Also used during queries - checked first before query_cache.
    ingest_cache: Cache<TimeBucket, Arc<MiniTsdb>>,

    /// LRU cache (50 max) for read-only query buckets.
    /// Only populated for buckets NOT in ingest_cache.
    query_cache: Cache<TimeBucket, Arc<MiniTsdb>>,
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
        }
    }

    /// Get or create a MiniTsdb for ingestion into a specific bucket.
    pub(crate) async fn get_or_create_for_ingest(
        &self,
        bucket: TimeBucket,
    ) -> Result<Arc<MiniTsdb>> {
        // Try ingest cache first
        if let Some(mini) = self.ingest_cache.get(&bucket).await {
            return Ok(mini);
        }

        // Load from storage and put in ingest cache
        let snapshot = self.storage.snapshot().await?;
        let mini = Arc::new(MiniTsdb::load(bucket.clone(), snapshot).await?);
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
        let snapshot = self.storage.snapshot().await?;
        let mini = Arc::new(MiniTsdb::load(bucket.clone(), snapshot).await?);
        self.query_cache.insert(bucket, mini.clone()).await;
        Ok(mini)
    }

    /// Create a QueryReader for a time range.
    /// This discovers all buckets covering the range and returns a TsdbQueryReader
    /// that merges results across them.
    pub(crate) async fn query_reader(
        &self,
        start_secs: i64,
        end_secs: i64,
    ) -> Result<TsdbQueryReader> {
        let snapshot = self.storage.snapshot().await?;

        // Discover buckets that cover the query range
        let buckets = snapshot
            .get_buckets_in_range(Some(start_secs), Some(end_secs))
            .await?;

        // Load MiniTsdbs for each bucket (from cache or storage)
        let mut mini_tsdbs = Vec::new();
        for bucket in buckets {
            let mini = self.get_bucket(bucket).await?;
            mini_tsdbs.push(mini);
        }

        Ok(TsdbQueryReader { mini_tsdbs })
    }

    /// Flush all dirty buckets in the ingest cache to storage.
    pub(crate) async fn flush(&self) -> Result<()> {
        // Note: moka's iter() returns a clone of the current entries
        for (_, mini) in &self.ingest_cache {
            mini.flush(self.storage.clone()).await?;
        }
        Ok(())
    }

    /// Ingest series into the TSDB.
    /// Each series is split by time bucket based on sample timestamps.
    pub(crate) async fn ingest_samples(&self, series_list: Vec<Series>) -> Result<()> {
        for series in series_list {
            // Group samples by bucket
            let mut bucket_samples: HashMap<TimeBucket, Vec<Sample>> = HashMap::new();

            for sample in series.samples {
                let bucket = TimeBucket::round_to_hour(
                    std::time::UNIX_EPOCH
                        + std::time::Duration::from_millis(sample.timestamp_ms as u64),
                )?;
                bucket_samples.entry(bucket).or_default().push(sample);
            }

            // Ingest each bucket's samples as a series
            for (bucket, samples) in bucket_samples {
                let bucket_series = Series {
                    labels: series.labels.clone(),
                    metric_type: series.metric_type,
                    unit: series.unit.clone(),
                    description: series.description.clone(),
                    samples,
                };

                let mini = self.get_or_create_for_ingest(bucket).await?;
                mini.ingest(&bucket_series).await?;
            }
        }

        Ok(())
    }
}

/// QueryReader implementation that queries across multiple buckets.
pub(crate) struct TsdbQueryReader {
    mini_tsdbs: Vec<Arc<MiniTsdb>>,
}

#[async_trait]
impl QueryReader for TsdbQueryReader {
    async fn forward_index(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>> {
        // Merge forward indexes from all buckets
        let merged = ForwardIndex::default();

        for mini in &self.mini_tsdbs {
            let reader = mini.query_reader().await;
            let index = reader.forward_index(series_ids).await?;

            // Use ForwardIndexLookup trait to get specs and merge
            for &sid in series_ids {
                if let Some(spec) = index.get_spec(&sid) {
                    merged.series.insert(sid, spec);
                }
            }
        }

        Ok(Box::new(merged))
    }

    async fn all_forward_index(&self) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>> {
        // Merge all forward indexes from all buckets
        let merged = ForwardIndex::default();

        for mini in &self.mini_tsdbs {
            let reader = mini.query_reader().await;
            let index = reader.all_forward_index().await?;

            // Merge all series from this bucket
            for (sid, spec) in index.all_series() {
                merged.series.insert(sid, spec);
            }
        }

        Ok(Box::new(merged))
    }

    async fn inverted_index(
        &self,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>> {
        // Merge inverted indexes from all buckets
        let merged = InvertedIndex::default();

        for mini in &self.mini_tsdbs {
            let reader = mini.query_reader().await;
            let index = reader.inverted_index(terms).await?;

            // Get the bitmap for each term and merge (union)
            for term in terms {
                let bitmap = index.intersect(vec![term.clone()]);
                if !bitmap.is_empty() {
                    let mut entry = merged.postings.entry(term.clone()).or_default();
                    *entry.value_mut() |= bitmap;
                }
            }
        }

        Ok(Box::new(merged))
    }

    async fn all_inverted_index(&self) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>> {
        // Merge all inverted indexes from all buckets
        let merged = InvertedIndex::default();

        for mini in &self.mini_tsdbs {
            let reader = mini.query_reader().await;
            let index = reader.all_inverted_index().await?;

            // Merge all keys from this bucket
            for attr in index.all_keys() {
                let bitmap = index.intersect(vec![attr.clone()]);
                if !bitmap.is_empty() {
                    let mut entry = merged.postings.entry(attr).or_default();
                    *entry.value_mut() |= bitmap;
                }
            }
        }

        Ok(Box::new(merged))
    }

    async fn label_values(&self, label_name: &str) -> Result<Vec<String>> {
        // Collect and deduplicate label values from all buckets
        let mut all_values = std::collections::HashSet::new();

        for mini in &self.mini_tsdbs {
            let reader = mini.query_reader().await;
            let values = reader.label_values(label_name).await?;
            all_values.extend(values);
        }

        Ok(all_values.into_iter().collect())
    }

    async fn samples(
        &self,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        // Collect samples from all buckets and concatenate
        // Since buckets don't overlap in time, we can just append
        let mut all_samples = Vec::new();

        for mini in &self.mini_tsdbs {
            let reader = mini.query_reader().await;
            let samples = reader.samples(series_id, start_ms, end_ms).await?;
            all_samples.extend(samples);
        }

        // Sort by timestamp to ensure correct ordering across buckets
        all_samples.sort_by_key(|s| s.timestamp_ms);

        Ok(all_samples)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MetricType;
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
        let mini1 = tsdb.get_or_create_for_ingest(bucket.clone()).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket.clone()).await.unwrap();

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
        let mini_ingest = tsdb.get_or_create_for_ingest(bucket.clone()).await.unwrap();

        // when: getting the same bucket for query
        let mini_query = tsdb.get_bucket(bucket.clone()).await.unwrap();

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
        let mini1 = tsdb.get_bucket(bucket.clone()).await.unwrap();
        let mini2 = tsdb.get_bucket(bucket.clone()).await.unwrap();

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
        let mini = tsdb.get_or_create_for_ingest(bucket.clone()).await.unwrap();

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
        let index = reader.inverted_index(&terms).await.unwrap();
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
        let mini1 = tsdb
            .get_or_create_for_ingest(bucket1.clone())
            .await
            .unwrap();
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

        let mini2 = tsdb
            .get_or_create_for_ingest(bucket2.clone())
            .await
            .unwrap();
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
        let mini3 = tsdb
            .get_or_create_for_ingest(bucket3.clone())
            .await
            .unwrap();
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

        let mini4 = tsdb
            .get_or_create_for_ingest(bucket4.clone())
            .await
            .unwrap();
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
        let evaluator = Evaluator::new(&reader);
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

            let mut results = evaluator.evaluate(stmt).await.unwrap();
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
}

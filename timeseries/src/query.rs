use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use moka::future::Cache;

use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::model::{Label, Sample};
use crate::model::{SeriesId, TimeBucket};
use crate::util::Result;

/// Trait for read-only queries within a single time bucket.
/// This is the bucket-scoped interface that works with bucket-local series IDs.
#[async_trait]
pub(crate) trait BucketQueryReader: Send + Sync {
    /// Get a view into forward index data for the specified series IDs.
    /// This avoids cloning from head/frozen tiers - only storage data is loaded.
    async fn forward_index(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>>;

    /// Get a view into all forward index data.
    /// Used when no match[] filter is provided to retrieve all series.
    async fn all_forward_index(
        &self,
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>>;

    /// Get a view into inverted index data for the specified terms.
    /// This avoids cloning bitmaps upfront - only storage data is pre-loaded.
    async fn inverted_index(
        &self,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>>;

    /// Get a view into all inverted index data.
    /// Used for labels/label_values queries to access all attribute keys.
    async fn all_inverted_index(
        &self,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>>;

    /// Get all unique values for a specific label name.
    /// This is more efficient than loading all inverted index data when
    /// only values for a single label are needed.
    async fn label_values(&self, label_name: &str) -> Result<Vec<String>>;

    /// Get samples for a series within a time range, merging from all layers.
    /// Returns samples sorted by timestamp with duplicates removed (head takes priority).
    async fn samples(&self, series_id: SeriesId, start_ms: i64, end_ms: i64)
    -> Result<Vec<Sample>>;

    /// Batch-fetch samples for specific series in the bucket using a single range scan.
    /// Only returns data for the specified `series_ids`. The scan reads contiguous
    /// storage data and skips non-matching series, which is more efficient than
    /// N individual point lookups when many series are needed from the same bucket.
    async fn batch_samples(
        &self,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<SeriesId, Vec<Sample>>>;
}

/// Trait for read-only queries that may span multiple time buckets.
/// This is the high-level interface that properly handles bucket-scoped series IDs.
#[async_trait]
pub(crate) trait QueryReader: Send + Sync {
    /// Get available time buckets that this reader contains.
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>>;

    /// Get a view into forward index data for the specified series IDs within a specific bucket.
    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>>;

    /// Get a view into inverted index data for the specified terms within a specific bucket.
    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>>;

    /// Get a view into inverted index data for the specified bucket
    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>>;

    /// Get all unique values for a specific label name within a specific bucket.
    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>>;

    /// Get samples for a series within a time range from a specific bucket.
    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>>;

    /// Batch-fetch samples for specific series in a bucket using a single range scan.
    async fn batch_samples(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<SeriesId, Vec<Sample>>>;
}

// ── SharedIndexCache ─────────────────────────────────────────────────

/// Default maximum number of entries in the shared index cache.
const DEFAULT_INDEX_CACHE_CAPACITY: u64 = 500;

/// A caching wrapper around any `QueryReader` that provides cross-query
/// LRU caching for inverted and forward index lookups using Moka.
///
/// Sits between the per-query `CachedQueryReader` (which caches within a
/// single evaluation) and the underlying reader (which hits storage).
/// Samples are **not** cached here — they are already cached per-query
/// by `CachedQueryReader` with full-bucket fetches.
pub(crate) struct SharedIndexCache<R: QueryReader> {
    inner: R,
    inverted_cache:
        Cache<(TimeBucket, Vec<Label>), Arc<dyn InvertedIndexLookup + Send + Sync + 'static>>,
    forward_cache:
        Cache<(TimeBucket, Vec<SeriesId>), Arc<dyn ForwardIndexLookup + Send + Sync + 'static>>,
}

impl<R: QueryReader> SharedIndexCache<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self::with_capacity(inner, DEFAULT_INDEX_CACHE_CAPACITY)
    }

    pub(crate) fn with_capacity(inner: R, capacity: u64) -> Self {
        Self {
            inner,
            inverted_cache: Cache::builder().max_capacity(capacity).build(),
            forward_cache: Cache::builder().max_capacity(capacity).build(),
        }
    }
}

#[async_trait]
impl<R: QueryReader> QueryReader for SharedIndexCache<R> {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        self.inner.list_buckets().await
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let mut sorted_ids = series_ids.to_vec();
        sorted_ids.sort();
        let key = (*bucket, sorted_ids);

        if let Some(cached) = self.forward_cache.get(&key).await {
            // Return a clone of the Arc wrapped in a Box
            return Ok(Box::new(ArcForwardIndex(cached)));
        }

        let result = self.inner.forward_index(bucket, series_ids).await?;
        let arc: Arc<dyn ForwardIndexLookup + Send + Sync + 'static> = result.into();
        self.forward_cache.insert(key, arc.clone()).await;
        Ok(Box::new(ArcForwardIndex(arc)))
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mut sorted_terms = terms.to_vec();
        sorted_terms.sort();
        let key = (*bucket, sorted_terms);

        if let Some(cached) = self.inverted_cache.get(&key).await {
            return Ok(Box::new(ArcInvertedIndex(cached)));
        }

        let result = self.inner.inverted_index(bucket, terms).await?;
        let arc: Arc<dyn InvertedIndexLookup + Send + Sync + 'static> = result.into();
        self.inverted_cache.insert(key, arc.clone()).await;
        Ok(Box::new(ArcInvertedIndex(arc)))
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        self.inner.all_inverted_index(bucket).await
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        self.inner.label_values(bucket, label_name).await
    }

    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        self.inner.samples(bucket, series_id, start_ms, end_ms).await
    }

    async fn batch_samples(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
        start_ms: i64,
        end_ms: i64,
    ) -> Result<HashMap<SeriesId, Vec<Sample>>> {
        self.inner.batch_samples(bucket, series_ids, start_ms, end_ms).await
    }
}

/// Wrapper that delegates `ForwardIndexLookup` to an `Arc<dyn ForwardIndexLookup>`.
struct ArcForwardIndex(Arc<dyn ForwardIndexLookup + Send + Sync + 'static>);

impl ForwardIndexLookup for ArcForwardIndex {
    fn get_spec(&self, series_id: &SeriesId) -> Option<crate::index::SeriesSpec> {
        self.0.get_spec(series_id)
    }

    fn all_series(&self) -> Vec<(SeriesId, crate::index::SeriesSpec)> {
        self.0.all_series()
    }
}

/// Wrapper that delegates `InvertedIndexLookup` to an `Arc<dyn InvertedIndexLookup>`.
struct ArcInvertedIndex(Arc<dyn InvertedIndexLookup + Send + Sync + 'static>);

impl InvertedIndexLookup for ArcInvertedIndex {
    fn intersect(&self, terms: Vec<Label>) -> roaring::RoaringBitmap {
        self.0.intersect(terms)
    }

    fn all_keys(&self) -> Vec<Label> {
        self.0.all_keys()
    }
}

#[cfg(any(test, feature = "bench-internals"))]
pub(crate) mod test_utils {
    use super::*;
    use crate::index::{ForwardIndex, InvertedIndex, SeriesSpec};
    use crate::model::{MetricType, TimeBucket};
    use std::collections::HashMap;

    /// Type alias for bucket data to reduce complexity
    type BucketData =
        HashMap<TimeBucket, (ForwardIndex, InvertedIndex, HashMap<SeriesId, Vec<Sample>>)>;

    /// A mock QueryReader for testing that holds data in memory.
    /// Supports both single and multi-bucket scenarios.
    /// Use `MockQueryReaderBuilder` to construct instances.
    pub(crate) struct MockQueryReader {
        /// Map from bucket to its data
        bucket_data: BucketData,
    }

    #[async_trait]
    impl QueryReader for MockQueryReader {
        async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
            Ok(self.bucket_data.keys().cloned().collect())
        }

        async fn forward_index(
            &self,
            bucket: &TimeBucket,
            _series_ids: &[SeriesId],
        ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
            if let Some((forward_index, _, _)) = self.bucket_data.get(bucket) {
                Ok(Box::new(forward_index.clone()))
            } else {
                Err(crate::error::Error::InvalidInput(format!(
                    "MockQueryReader does not have bucket {:?}",
                    bucket
                )))
            }
        }

        async fn inverted_index(
            &self,
            bucket: &TimeBucket,
            _terms: &[Label],
        ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
            if let Some((_, inverted_index, _)) = self.bucket_data.get(bucket) {
                Ok(Box::new(inverted_index.clone()))
            } else {
                Err(crate::error::Error::InvalidInput(format!(
                    "MockQueryReader does not have bucket {:?}",
                    bucket
                )))
            }
        }

        async fn all_inverted_index(
            &self,
            bucket: &TimeBucket,
        ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
            if let Some((_, inverted_index, _)) = self.bucket_data.get(bucket) {
                Ok(Box::new(inverted_index.clone()))
            } else {
                Err(crate::error::Error::InvalidInput(format!(
                    "MockQueryReader does not have bucket {:?}",
                    bucket
                )))
            }
        }

        async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
            if let Some((_, inverted_index, _)) = self.bucket_data.get(bucket) {
                let values: Vec<String> = inverted_index
                    .postings
                    .iter()
                    .filter(|entry| entry.key().name == label_name)
                    .map(|entry| entry.key().value.clone())
                    .collect();
                Ok(values)
            } else {
                Err(crate::error::Error::InvalidInput(format!(
                    "MockQueryReader does not have bucket {:?}",
                    bucket
                )))
            }
        }

        async fn samples(
            &self,
            bucket: &TimeBucket,
            series_id: SeriesId,
            start_ms: i64,
            end_ms: i64,
        ) -> Result<Vec<Sample>> {
            if let Some((_, _, samples_map)) = self.bucket_data.get(bucket) {
                let samples = samples_map
                    .get(&series_id)
                    .map(|s| {
                        s.iter()
                            .filter(|sample| {
                                sample.timestamp_ms > start_ms && sample.timestamp_ms <= end_ms
                            })
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(samples)
            } else {
                Err(crate::error::Error::InvalidInput(format!(
                    "MockQueryReader does not have bucket {:?}",
                    bucket
                )))
            }
        }

        async fn batch_samples(
            &self,
            bucket: &TimeBucket,
            series_ids: &[SeriesId],
            start_ms: i64,
            end_ms: i64,
        ) -> Result<HashMap<SeriesId, Vec<Sample>>> {
            if let Some((_, _, samples_map)) = self.bucket_data.get(bucket) {
                let id_set: std::collections::HashSet<SeriesId> =
                    series_ids.iter().copied().collect();
                let mut result = HashMap::new();
                for (&series_id, samples) in samples_map {
                    if !id_set.contains(&series_id) {
                        continue;
                    }
                    let filtered: Vec<Sample> = samples
                        .iter()
                        .filter(|s| s.timestamp_ms > start_ms && s.timestamp_ms <= end_ms)
                        .cloned()
                        .collect();
                    if !filtered.is_empty() {
                        result.insert(series_id, filtered);
                    }
                }
                Ok(result)
            } else {
                Err(crate::error::Error::InvalidInput(format!(
                    "MockQueryReader does not have bucket {:?}",
                    bucket
                )))
            }
        }
    }

    /// Builder for creating MockQueryReader instances from test data.
    /// Convenience wrapper for single-bucket scenarios.
    pub(crate) struct MockQueryReaderBuilder {
        inner: MockMultiBucketQueryReaderBuilder,
        bucket: TimeBucket,
    }

    impl MockQueryReaderBuilder {
        pub(crate) fn new(bucket: TimeBucket) -> Self {
            Self {
                inner: MockMultiBucketQueryReaderBuilder::new(),
                bucket,
            }
        }

        /// Add a sample with labels. If a series with the same labels already exists,
        /// the sample is added to that series. Otherwise, a new series is created.
        pub(crate) fn add_sample(
            &mut self,
            labels: Vec<Label>,
            metric_type: MetricType,
            sample: Sample,
        ) -> &mut Self {
            self.inner
                .add_sample(self.bucket, labels, metric_type, sample);
            self
        }

        pub(crate) fn build(self) -> MockQueryReader {
            self.inner.build()
        }
    }

    /// Builder for creating MockQueryReader instances from test data.
    /// Supports multi-bucket scenarios.
    pub(crate) struct MockMultiBucketQueryReaderBuilder {
        bucket_data: BucketData,
        /// Global series ID counter to ensure unique IDs across all buckets
        next_global_series_id: SeriesId,
        /// Maps label fingerprint to series ID for global deduplication
        global_fingerprint_to_id: HashMap<Vec<Label>, SeriesId>,
    }

    impl MockMultiBucketQueryReaderBuilder {
        pub(crate) fn new() -> Self {
            Self {
                bucket_data: HashMap::new(),
                next_global_series_id: 0,
                global_fingerprint_to_id: HashMap::new(),
            }
        }

        /// Add a sample with labels to a specific bucket. If a series with the same labels already exists globally,
        /// the existing series ID is reused. Otherwise, a new series is created with a global ID.
        pub(crate) fn add_sample(
            &mut self,
            bucket: TimeBucket,
            labels: Vec<Label>,
            metric_type: MetricType,
            sample: Sample,
        ) -> &mut Self {
            // Sort labels for consistent fingerprinting
            let mut sorted_attrs = labels.clone();
            // Sort by canonical Label ordering (name, then value) for fingerprinting
            sorted_attrs.sort();

            // Get or create global series ID
            let series_id = if let Some(&id) = self.global_fingerprint_to_id.get(&sorted_attrs) {
                id
            } else {
                let id = self.next_global_series_id;
                self.next_global_series_id += 1;
                self.global_fingerprint_to_id
                    .insert(sorted_attrs.clone(), id);
                id
            };

            // Get or create bucket data
            let (forward_index, inverted_index, samples_map) =
                self.bucket_data.entry(bucket).or_insert_with(|| {
                    (
                        ForwardIndex::default(),
                        InvertedIndex::default(),
                        HashMap::new(),
                    )
                });

            // Add to forward index for this bucket if not already present
            if !forward_index.series.contains_key(&series_id) {
                forward_index.series.insert(
                    series_id,
                    SeriesSpec {
                        unit: None,
                        metric_type: Some(metric_type),
                        labels: labels.clone(),
                    },
                );

                // Add to inverted index for this bucket
                for label in &labels {
                    inverted_index
                        .postings
                        .entry(label.clone())
                        .or_default()
                        .insert(series_id);
                }
            }

            // Add sample to this bucket
            samples_map.entry(series_id).or_default().push(sample);

            self
        }

        pub(crate) fn build(self) -> MockQueryReader {
            MockQueryReader {
                bucket_data: self.bucket_data,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::model::MetricType;

        fn bucket() -> TimeBucket {
            TimeBucket {
                start: 100,
                size: 1,
            }
        }

        fn build_reader() -> MockQueryReader {
            let mut builder = MockQueryReaderBuilder::new(bucket());
            builder.add_sample(
                vec![
                    Label { name: "__name__".to_string(), value: "cpu".to_string() },
                    Label { name: "host".to_string(), value: "a".to_string() },
                ],
                MetricType::Gauge,
                Sample { timestamp_ms: 1000, value: 1.0 },
            );
            builder.add_sample(
                vec![
                    Label { name: "__name__".to_string(), value: "cpu".to_string() },
                    Label { name: "host".to_string(), value: "b".to_string() },
                ],
                MetricType::Gauge,
                Sample { timestamp_ms: 1000, value: 2.0 },
            );
            builder.build()
        }

        #[tokio::test]
        async fn shared_index_cache_returns_same_results() {
            let reader = build_reader();
            let cached = SharedIndexCache::new(reader);
            let b = bucket();

            let terms = vec![Label { name: "__name__".to_string(), value: "cpu".to_string() }];
            let inv1 = cached.inverted_index(&b, &terms).await.unwrap();
            let inv2 = cached.inverted_index(&b, &terms).await.unwrap();

            let ids1: Vec<SeriesId> = inv1.intersect(terms.clone()).iter().collect();
            let ids2: Vec<SeriesId> = inv2.intersect(terms.clone()).iter().collect();
            assert_eq!(ids1, ids2);
            assert_eq!(ids1.len(), 2);
        }

        #[tokio::test]
        async fn shared_index_cache_normalizes_key_order() {
            let reader = build_reader();
            let cached = SharedIndexCache::new(reader);
            let b = bucket();

            let terms_a = vec![
                Label { name: "__name__".to_string(), value: "cpu".to_string() },
                Label { name: "host".to_string(), value: "a".to_string() },
            ];
            let terms_b = vec![
                Label { name: "host".to_string(), value: "a".to_string() },
                Label { name: "__name__".to_string(), value: "cpu".to_string() },
            ];

            // Both orderings should hit the same cache entry
            let _ = cached.inverted_index(&b, &terms_a).await.unwrap();
            let _ = cached.inverted_index(&b, &terms_b).await.unwrap();
            // No assertion on cache internals, but verifying no error on reordered keys
        }

        #[tokio::test]
        async fn shared_index_cache_forward_index_passthrough() {
            let reader = build_reader();
            let cached = SharedIndexCache::new(reader);
            let b = bucket();

            let fwd = cached.forward_index(&b, &[0, 1]).await.unwrap();
            assert!(fwd.get_spec(&0).is_some());
            assert!(fwd.get_spec(&1).is_some());
            assert!(fwd.get_spec(&999).is_none());
        }

        #[tokio::test]
        async fn shared_index_cache_samples_passthrough() {
            let reader = build_reader();
            let cached = SharedIndexCache::new(reader);
            let b = bucket();

            let samples = cached.samples(&b, 0, 0, 2000).await.unwrap();
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, 1.0);
        }

        #[tokio::test]
        async fn batch_samples_filters_by_series_ids() {
            let reader = build_reader();
            let b = bucket();

            // Only request series 0
            let result = reader.batch_samples(&b, &[0], 0, 2000).await.unwrap();
            assert_eq!(result.len(), 1);
            assert!(result.contains_key(&0));
            assert!(!result.contains_key(&1));
        }

        #[tokio::test]
        async fn batch_samples_filters_by_time_range() {
            let mut builder = MockQueryReaderBuilder::new(bucket());
            builder.add_sample(
                vec![Label { name: "__name__".to_string(), value: "m".to_string() }],
                MetricType::Gauge,
                Sample { timestamp_ms: 1000, value: 1.0 },
            );
            builder.add_sample(
                vec![Label { name: "__name__".to_string(), value: "m".to_string() }],
                MetricType::Gauge,
                Sample { timestamp_ms: 5000, value: 5.0 },
            );
            let reader = builder.build();
            let b = bucket();

            // Time range that only includes the first sample
            let result = reader.batch_samples(&b, &[0], 0, 2000).await.unwrap();
            assert_eq!(result.get(&0).unwrap().len(), 1);
            assert_eq!(result.get(&0).unwrap()[0].value, 1.0);
        }

        #[tokio::test]
        async fn batch_samples_returns_empty_for_no_match() {
            let reader = build_reader();
            let b = bucket();

            let result = reader.batch_samples(&b, &[999], 0, 2000).await.unwrap();
            assert!(result.is_empty());
        }
    }
}

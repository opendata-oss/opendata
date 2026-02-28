use async_trait::async_trait;

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
}

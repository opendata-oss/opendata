#![allow(dead_code)]

use async_trait::async_trait;

use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::model::SeriesId;
use crate::series::{Label, Sample};
use crate::util::Result;

/// Trait for read-only queries across all data tiers.
/// Implementations hide the details of how data is stored and retrieved.
#[async_trait]
pub(crate) trait QueryReader: Send + Sync {
    /// Get a view into forward index data for the specified series IDs.
    /// This avoids cloning from head/frozen tiers - only storage data is loaded.
    async fn forward_index(
        &self,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>>;

    /// Get a view into all forward index data.
    /// Used when no match[] filter is provided to retrieve all series.
    async fn all_forward_index(&self) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>>;

    /// Get a view into inverted index data for the specified terms.
    /// This avoids cloning bitmaps upfront - only storage data is pre-loaded.
    async fn inverted_index(
        &self,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>>;

    /// Get a view into all inverted index data.
    /// Used for labels/label_values queries to access all attribute keys.
    async fn all_inverted_index(&self) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>>;

    /// Get all unique values for a specific label name.
    /// This is more efficient than loading all inverted index data when
    /// only values for a single label are needed.
    async fn label_values(&self, label_name: &str) -> Result<Vec<String>>;

    /// Get samples for a series within a time range, merging from all layers.
    /// Returns samples sorted by timestamp with duplicates removed (head takes priority).
    async fn samples(&self, series_id: SeriesId, start_ms: i64, end_ms: i64)
    -> Result<Vec<Sample>>;
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;
    use crate::index::{ForwardIndex, InvertedIndex};
    use crate::model::{SeriesSpec, TimeBucket};
    use crate::series::MetricType;
    use std::collections::HashMap;

    /// A mock QueryReader for testing that holds data in memory.
    /// Use `MockQueryReaderBuilder` to construct instances.
    pub(crate) struct MockQueryReader {
        bucket: TimeBucket,
        forward_index: ForwardIndex,
        inverted_index: InvertedIndex,
        samples: HashMap<SeriesId, Vec<Sample>>,
    }

    #[async_trait]
    impl QueryReader for MockQueryReader {
        async fn forward_index(
            &self,
            _series_ids: &[SeriesId],
        ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>> {
            Ok(Box::new(self.forward_index.clone()))
        }

        async fn all_forward_index(
            &self,
        ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + '_>> {
            Ok(Box::new(self.forward_index.clone()))
        }

        async fn inverted_index(
            &self,
            _terms: &[Label],
        ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>> {
            Ok(Box::new(self.inverted_index.clone()))
        }

        async fn all_inverted_index(
            &self,
        ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + '_>> {
            Ok(Box::new(self.inverted_index.clone()))
        }

        async fn label_values(&self, label_name: &str) -> Result<Vec<String>> {
            let values: Vec<String> = self
                .inverted_index
                .postings
                .iter()
                .filter(|entry| entry.key().name == label_name)
                .map(|entry| entry.key().value.clone())
                .collect();
            Ok(values)
        }

        async fn samples(
            &self,
            series_id: SeriesId,
            start_ms: i64,
            end_ms: i64,
        ) -> Result<Vec<Sample>> {
            let samples = self
                .samples
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
        }
    }

    /// Builder for creating MockQueryReader instances from test data.
    pub(crate) struct MockQueryReaderBuilder {
        bucket: TimeBucket,
        forward_index: ForwardIndex,
        inverted_index: InvertedIndex,
        samples: HashMap<SeriesId, Vec<Sample>>,
        next_series_id: SeriesId,
        /// Maps fingerprint (sorted attributes) to series ID for deduplication
        fingerprint_to_id: HashMap<Vec<Label>, SeriesId>,
    }

    impl MockQueryReaderBuilder {
        pub(crate) fn new(bucket: TimeBucket) -> Self {
            Self {
                bucket,
                forward_index: ForwardIndex::default(),
                inverted_index: InvertedIndex::default(),
                samples: HashMap::new(),
                next_series_id: 0,
                fingerprint_to_id: HashMap::new(),
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
            // Sort labels for consistent fingerprinting
            let mut sorted_attrs = labels.clone();
            sorted_attrs.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value)));

            // Get or create series ID
            let series_id = if let Some(&id) = self.fingerprint_to_id.get(&sorted_attrs) {
                id
            } else {
                let id = self.next_series_id;
                self.next_series_id += 1;

                // Add to forward index
                self.forward_index.series.insert(
                    id,
                    SeriesSpec {
                        metric_unit: None,
                        metric_type,
                        labels: labels.clone(),
                    },
                );

                // Add to inverted index
                for label in &labels {
                    self.inverted_index
                        .postings
                        .entry(label.clone())
                        .or_default()
                        .insert(id);
                }

                self.fingerprint_to_id.insert(sorted_attrs, id);
                id
            };

            // Add sample
            self.samples.entry(series_id).or_default().push(sample);

            self
        }

        pub(crate) fn build(self) -> MockQueryReader {
            MockQueryReader {
                bucket: self.bucket,
                forward_index: self.forward_index,
                inverted_index: self.inverted_index,
                samples: self.samples,
            }
        }
    }
}

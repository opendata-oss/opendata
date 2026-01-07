#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;

use crate::index::SeriesSpec;
use crate::model::{MetricType, Sample, Series, SeriesFingerprint, SeriesId, TimeBucket};
use crate::{
    error::Error,
    index::{ForwardIndex, InvertedIndex},
    model::Label,
    util::{Fingerprint, Result},
};

/// The delta chunk is the current in-memory segment of OpenTSDB representing
/// the data that has been ingested but not yet flushed to storage.
pub(crate) struct TsdbDeltaBuilder<'a> {
    pub(crate) bucket: TimeBucket,
    pub(crate) forward_index: ForwardIndex,
    pub(crate) inverted_index: InvertedIndex,
    pub(crate) series_dict: &'a DashMap<SeriesFingerprint, SeriesId>,
    pub(crate) series_dict_delta: HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) samples: HashMap<SeriesId, Vec<Sample>>,
    pub(crate) next_series_id: &'a AtomicU32,
}

impl<'a> TsdbDeltaBuilder<'a> {
    pub(crate) fn new(
        bucket: TimeBucket,
        series_dict: &'a DashMap<SeriesFingerprint, SeriesId>,
        next_series_id: &'a AtomicU32,
    ) -> Self {
        Self {
            bucket,
            forward_index: ForwardIndex::default(),
            inverted_index: InvertedIndex::default(),
            series_dict,
            series_dict_delta: HashMap::new(),
            samples: HashMap::new(),
            next_series_id,
        }
    }

    /// Ingest a series with its samples.
    /// Returns an error if any sample timestamp is outside the bucket's time range.
    pub(crate) fn ingest(&mut self, series: &Series) -> Result<()> {
        // Sort labels once before iterating samples
        let mut sorted_labels = series.labels.clone();
        sorted_labels.sort_by(|a, b| a.name.cmp(&b.name));

        for sample in &series.samples {
            self.ingest_sample(
                &sorted_labels,
                &series.unit,
                series.metric_type,
                sample.clone(),
            )?;
        }
        Ok(())
    }

    fn ingest_sample(
        &mut self,
        labels: &[Label],
        unit: &Option<String>,
        metric_type: Option<MetricType>,
        sample: Sample,
    ) -> Result<()> {
        // Validate sample timestamp is within bucket range
        let bucket_start_ms = self.bucket.start as i64 * 60 * 1000;
        let bucket_end_ms =
            (self.bucket.start as i64 + self.bucket.size_in_mins() as i64) * 60 * 1000;
        if sample.timestamp_ms < bucket_start_ms || sample.timestamp_ms >= bucket_end_ms {
            return Err(Error::InvalidInput(format!(
                "Sample timestamp {} is outside bucket range [{}, {})",
                sample.timestamp_ms, bucket_start_ms, bucket_end_ms
            )));
        }

        // Labels are already sorted by ingest() before calling this method
        let fingerprint = labels.fingerprint();

        // Fast path: check local delta first (for samples in the same batch)
        if let Some(&series_id) = self.series_dict_delta.get(&fingerprint) {
            self.samples.entry(series_id).or_default().push(sample);
            return Ok(());
        }

        // Atomic get-or-create in the shared dictionary. This ensures that
        // concurrent ingest() calls will not create duplicate series IDs
        // for the same fingerprint.
        #[cfg(test)]
        fail::fail_point!("delta_before_entry");

        let mut is_new = false;
        let series_id = *self
            .series_dict
            .entry(fingerprint)
            .or_insert_with(|| {
                is_new = true;
                self.next_series_id.fetch_add(1, Ordering::SeqCst)
            })
            .value();

        // Only update indexes if WE created this series
        if is_new {
            self.series_dict_delta.insert(fingerprint, series_id);

            let series_spec = SeriesSpec {
                unit: unit.clone(),
                metric_type,
                labels: labels.to_vec(),
            };

            self.forward_index.series.insert(series_id, series_spec);

            for label in labels {
                self.inverted_index
                    .postings
                    .entry(label.clone())
                    .or_default()
                    .value_mut()
                    .insert(series_id);
            }
        }

        self.samples.entry(series_id).or_default().push(sample);
        Ok(())
    }

    pub(crate) fn build(self) -> TsdbDelta {
        TsdbDelta {
            bucket: self.bucket,
            forward_index: self.forward_index,
            inverted_index: self.inverted_index,
            series_dict: self.series_dict_delta,
            samples: self.samples,
        }
    }
}

pub(crate) struct TsdbDelta {
    pub(crate) bucket: TimeBucket,
    pub(crate) forward_index: ForwardIndex,
    pub(crate) inverted_index: InvertedIndex,
    pub(crate) series_dict: HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) samples: HashMap<SeriesId, Vec<Sample>>,
}

impl TsdbDelta {
    /// Create an empty delta for a bucket
    pub(crate) fn empty(bucket: TimeBucket) -> Self {
        Self {
            bucket,
            forward_index: ForwardIndex::default(),
            inverted_index: InvertedIndex::default(),
            series_dict: HashMap::new(),
            samples: HashMap::new(),
        }
    }

    /// Check if delta has any data
    pub(crate) fn is_empty(&self) -> bool {
        self.samples.is_empty() && self.series_dict.is_empty()
    }

    /// Merge another delta into this one, accumulating all data
    pub(crate) fn merge(&mut self, other: TsdbDelta) {
        // Merge forward index
        self.forward_index.merge(&other.forward_index);

        // Merge inverted index
        self.inverted_index.merge(other.inverted_index);

        // Merge series dict
        for (fingerprint, series_id) in other.series_dict {
            self.series_dict.insert(fingerprint, series_id);
        }

        // Merge samples
        for (series_id, samples) in other.samples {
            self.samples.entry(series_id).or_default().extend(samples);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MetricType;
    use crate::model::Temporality;
    use dashmap::DashMap;
    use std::sync::atomic::AtomicU32;

    fn create_test_bucket() -> TimeBucket {
        TimeBucket::hour(1000)
    }

    fn create_test_labels() -> Vec<Label> {
        vec![
            Label {
                name: "service".to_string(),
                value: "api".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "prod".to_string(),
            },
        ]
    }

    fn create_test_sample() -> Sample {
        // Timestamp must be within bucket range (1000 min = 60,000,000 ms to 1060 min = 63,600,000 ms)
        Sample {
            timestamp_ms: 60_000_001,
            value: 42.5,
        }
    }

    #[test]
    fn should_create_new_series_when_ingesting_first_sample() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut labels = create_test_labels();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let sample = create_test_sample();
        let metric_unit = Some("bytes".to_string());
        let metric_type = MetricType::Gauge;

        // when
        builder
            .ingest_sample(&labels, &metric_unit, Some(metric_type), sample.clone())
            .unwrap();

        // then
        assert_eq!(next_series_id.load(Ordering::SeqCst), 1);
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);

        // Verify sample is stored
        let samples = builder.samples.get(&0).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0], sample);

        // Verify forward index
        let series_spec = builder.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.unit, metric_unit);
        match (series_spec.metric_type, Some(metric_type)) {
            (Some(MetricType::Gauge), Some(MetricType::Gauge)) => {}
            _ => panic!("Metric types don't match"),
        }
        // Labels are already sorted
        assert_eq!(series_spec.labels, labels);

        // Verify inverted index
        for label in &labels {
            let postings = builder.inverted_index.postings.get(label).unwrap();
            assert!(postings.value().contains(0));
        }
    }

    #[test]
    fn should_reuse_series_id_for_samples_with_same_attributes() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut labels = create_test_labels();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        // Timestamps must be within bucket range (60,000,000 to 63,600,000 ms)
        let sample1 = Sample {
            timestamp_ms: 60_000_001,
            value: 10.0,
        };
        let sample2 = Sample {
            timestamp_ms: 60_000_002,
            value: 20.0,
        };
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());

        // when
        builder
            .ingest_sample(&labels, &unit, Some(metric_type), sample1.clone())
            .unwrap();
        builder
            .ingest_sample(&labels, &unit, Some(metric_type), sample2.clone())
            .unwrap();

        // then
        assert_eq!(next_series_id.load(Ordering::SeqCst), 1); // Only one series created
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);

        // Both samples should be under the same series_id
        let samples = builder.samples.get(&0).unwrap();
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0], sample1);
        assert_eq!(samples[1], sample2);
    }

    #[test]
    fn should_create_different_series_id_for_different_attributes() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut attributes1 = vec![Label {
            name: "service".to_string(),
            value: "api".to_string(),
        }];
        attributes1.sort_by(|a, b| a.name.cmp(&b.name));
        let mut attributes2 = vec![Label {
            name: "service".to_string(),
            value: "web".to_string(),
        }];
        attributes2.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());

        // when
        builder
            .ingest_sample(&attributes1, &unit, Some(metric_type), create_test_sample())
            .unwrap();
        builder
            .ingest_sample(&attributes2, &unit, Some(metric_type), create_test_sample())
            .unwrap();

        // then
        assert_eq!(next_series_id.load(Ordering::SeqCst), 2); // Two series created
        assert_eq!(builder.series_dict_delta.len(), 2);
        assert_eq!(builder.samples.len(), 2);
        assert!(builder.samples.contains_key(&0));
        assert!(builder.samples.contains_key(&1));
    }

    #[test]
    fn should_reuse_series_id_from_existing_series_dict() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let mut labels = create_test_labels();
        // Sort labels to match what ingest_sample does
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let fingerprint = labels.fingerprint();
        series_dict.insert(fingerprint, 42); // Existing series_id
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut test_labels = create_test_labels();
        test_labels.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());

        // when
        builder
            .ingest_sample(&test_labels, &unit, Some(metric_type), create_test_sample())
            .unwrap();

        // then
        assert_eq!(next_series_id.load(Ordering::SeqCst), 0); // No new series_id created
        assert_eq!(builder.series_dict_delta.len(), 0); // Not added to delta
        assert_eq!(builder.samples.len(), 1);
        assert!(builder.samples.contains_key(&42)); // Uses existing series_id
    }

    #[test]
    fn should_reuse_series_id_from_delta_dict_when_ingesting_again() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut labels = create_test_labels();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());

        // when
        builder
            .ingest_sample(&labels, &unit, Some(metric_type), create_test_sample())
            .unwrap();
        builder
            .ingest_sample(&labels, &unit, Some(metric_type), create_test_sample())
            .unwrap();

        // then
        assert_eq!(next_series_id.load(Ordering::SeqCst), 1); // Only one series_id created
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);
        assert!(builder.samples.contains_key(&0)); // Reused series_id 0
        assert_eq!(builder.samples.get(&0).unwrap().len(), 2); // Two samples
    }

    #[test]
    fn should_sort_attributes_before_fingerprinting() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut attributes1 = vec![
            Label {
                name: "z_key".to_string(),
                value: "value".to_string(),
            },
            Label {
                name: "a_key".to_string(),
                value: "value".to_string(),
            },
        ];
        attributes1.sort_by(|a, b| a.name.cmp(&b.name));
        let mut attributes2 = vec![
            Label {
                name: "a_key".to_string(),
                value: "value".to_string(),
            },
            Label {
                name: "z_key".to_string(),
                value: "value".to_string(),
            },
        ];
        attributes2.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());

        // when
        builder
            .ingest_sample(&attributes1, &unit, Some(metric_type), create_test_sample())
            .unwrap();
        builder
            .ingest_sample(&attributes2, &unit, Some(metric_type), create_test_sample())
            .unwrap();

        // then
        assert_eq!(next_series_id.load(Ordering::SeqCst), 1); // Same series_id reused
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);
    }

    #[test]
    fn should_store_metric_unit_and_type_in_forward_index() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut labels = create_test_labels();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_unit = Some("requests_per_second".to_string());
        let metric_type = MetricType::Sum {
            monotonic: true,
            temporality: Temporality::Cumulative,
        };

        // when
        builder
            .ingest_sample(
                &labels,
                &metric_unit,
                Some(metric_type),
                create_test_sample(),
            )
            .unwrap();

        // then
        let series_spec = builder.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.unit, metric_unit);
        match series_spec.metric_type {
            Some(MetricType::Sum {
                monotonic,
                temporality,
            }) => {
                assert!(monotonic);
                assert_eq!(temporality, Temporality::Cumulative);
            }
            _ => panic!("Expected Sum metric type"),
        }
    }

    #[test]
    fn should_index_all_labels_in_inverted_index() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut labels = vec![
            Label {
                name: "service".to_string(),
                value: "api".to_string(),
            },
            Label {
                name: "env".to_string(),
                value: "prod".to_string(),
            },
            Label {
                name: "region".to_string(),
                value: "us-east".to_string(),
            },
        ];
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());

        // when
        builder
            .ingest_sample(&labels, &unit, Some(metric_type), create_test_sample())
            .unwrap();

        // then
        assert_eq!(builder.inverted_index.postings.len(), 3);
        for label in &labels {
            let postings = builder.inverted_index.postings.get(label).unwrap();
            assert!(postings.value().contains(0));
            assert_eq!(postings.value().len(), 1);
        }
    }

    #[test]
    fn should_handle_empty_attributes_list() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let labels = Vec::<Label>::new();
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());

        // when
        builder
            .ingest_sample(&labels, &unit, Some(metric_type), create_test_sample())
            .unwrap();

        // then
        assert_eq!(next_series_id.load(Ordering::SeqCst), 1);
        assert_eq!(builder.series_dict_delta.len(), 1);
        assert_eq!(builder.samples.len(), 1);
        assert_eq!(builder.inverted_index.postings.len(), 0); // No attributes to index
        let series_spec = builder.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.labels.len(), 0);
    }

    #[test]
    fn should_handle_none_metric_unit() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut labels = create_test_labels();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_type = MetricType::Gauge;
        let unit: Option<String> = None;

        // when
        builder
            .ingest_sample(&labels, &unit, Some(metric_type), create_test_sample())
            .unwrap();

        // then
        let series_spec = builder.forward_index.series.get(&0).unwrap();
        assert_eq!(series_spec.unit, None);
        match (series_spec.metric_type, Some(metric_type)) {
            (Some(MetricType::Gauge), Some(MetricType::Gauge)) => {}
            _ => panic!("Metric types don't match"),
        }
    }

    #[test]
    fn should_serialize_concurrent_ingests_for_same_fingerprint() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        // given
        let bucket = create_test_bucket();
        let series_dict = Arc::new(DashMap::new());
        let next_series_id = Arc::new(AtomicU32::new(0));
        let labels = create_test_labels();

        // Setup barrier for 2 threads to synchronize at the failpoint
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = barrier.clone();

        fail::cfg_callback("delta_before_entry", move || {
            barrier_clone.wait();
        })
        .unwrap();

        // when: spawn two threads that will race to create the same series
        let series_dict_a = series_dict.clone();
        let next_series_id_a = next_series_id.clone();
        let mut attributes_a = labels.clone();
        attributes_a.sort_by(|a, b| a.name.cmp(&b.name));
        let bucket_a = bucket.clone();
        let unit = Some("bytes".to_string());

        let handle_a = thread::spawn(move || {
            let mut builder = TsdbDeltaBuilder::new(bucket_a, &series_dict_a, &next_series_id_a);
            // Timestamp must be within bucket range (60,000,000 to 63,600,000 ms)
            builder
                .ingest_sample(
                    &attributes_a,
                    &unit,
                    Some(MetricType::Gauge),
                    Sample {
                        timestamp_ms: 60_000_001,
                        value: 42.0,
                    },
                )
                .unwrap();
            builder.build()
        });

        let series_dict_b = series_dict.clone();
        let next_series_id_b = next_series_id.clone();
        let mut attributes_b = labels.clone();
        attributes_b.sort_by(|a, b| a.name.cmp(&b.name));
        let bucket_b = bucket.clone();
        let unit_b = Some("bytes".to_string());

        let handle_b = thread::spawn(move || {
            let mut builder = TsdbDeltaBuilder::new(bucket_b, &series_dict_b, &next_series_id_b);
            // Timestamp must be within bucket range (60,000,000 to 63,600,000 ms)
            builder
                .ingest_sample(
                    &attributes_b,
                    &unit_b,
                    Some(MetricType::Gauge),
                    Sample {
                        timestamp_ms: 60_000_002,
                        value: 43.0,
                    },
                )
                .unwrap();
            builder.build()
        });

        let delta_a = handle_a.join().unwrap();
        let delta_b = handle_b.join().unwrap();

        // Cleanup failpoint
        fail::remove("delta_before_entry");

        // then: only ONE series ID should have been assigned
        assert_eq!(
            next_series_id.load(Ordering::SeqCst),
            1,
            "Both threads should have used the same series ID"
        );

        // The shared series_dict should have exactly one entry
        assert_eq!(series_dict.len(), 1);

        // Both deltas should reference series_id 0
        // Note: only the "winner" will have series_dict_delta populated
        // The "loser" will have an empty series_dict_delta but still have samples
        let total_series_dict_entries = delta_a.series_dict.len() + delta_b.series_dict.len();
        assert_eq!(
            total_series_dict_entries, 1,
            "Only one thread should have created the series entry"
        );

        // Both deltas should have their respective samples for series_id 0
        let samples_a = delta_a
            .samples
            .get(&0)
            .expect("delta_a should have samples for series 0");
        assert_eq!(samples_a.len(), 1);
        assert_eq!(
            samples_a[0].value, 42.0,
            "delta_a should have sample with value 42.0"
        );

        let samples_b = delta_b
            .samples
            .get(&0)
            .expect("delta_b should have samples for series 0");
        assert_eq!(samples_b.len(), 1);
        assert_eq!(
            samples_b[0].value, 43.0,
            "delta_b should have sample with value 43.0"
        );
    }

    #[test]
    fn should_reject_sample_before_bucket_start() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut labels = create_test_labels();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());
        // Timestamp before bucket start (60,000,000 ms)
        let sample = Sample {
            timestamp_ms: 59_999_999,
            value: 42.5,
        };

        // when
        let result = builder.ingest_sample(&labels, &unit, Some(metric_type), sample);

        // then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)));
        assert!(err.to_string().contains("outside bucket range"));
    }

    #[test]
    fn should_reject_sample_at_or_after_bucket_end() {
        // given
        let bucket = create_test_bucket();
        let series_dict = DashMap::new();
        let next_series_id = AtomicU32::new(0);
        let mut builder = TsdbDeltaBuilder::new(bucket, &series_dict, &next_series_id);
        let mut labels = create_test_labels();
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let metric_type = MetricType::Gauge;
        let unit = Some("bytes".to_string());
        // Timestamp at bucket end (63,600,000 ms) - should be rejected (exclusive end)
        let sample = Sample {
            timestamp_ms: 63_600_000,
            value: 42.5,
        };

        // when
        let result = builder.ingest_sample(&labels, &unit, Some(metric_type), sample);

        // then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)));
        assert!(err.to_string().contains("outside bucket range"));
    }
}

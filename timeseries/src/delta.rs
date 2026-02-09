use std::collections::HashMap;
use std::sync::Arc;

use common::coordinator::Delta;

use crate::index::{ForwardIndex, InvertedIndex, SeriesSpec};
use crate::model::{Label, MetricType, Sample, Series, SeriesFingerprint, SeriesId, TimeBucket};
use crate::util::Fingerprint;

/// State that persists across delta freezes.
pub(crate) struct TsdbContext {
    pub(crate) bucket: TimeBucket,
    pub(crate) series_dict: Arc<HashMap<SeriesFingerprint, SeriesId>>,
    pub(crate) next_series_id: u32,
}

/// Frozen (immutable) delta data sent to the flusher.
pub(crate) struct FrozenTsdbDelta {
    pub(crate) bucket: TimeBucket,
    pub(crate) forward_index: ForwardIndex,
    pub(crate) inverted_index: InvertedIndex,
    pub(crate) series_dict_delta: HashMap<SeriesFingerprint, SeriesId>,
    pub(crate) samples: HashMap<SeriesId, Vec<Sample>>,
}

impl FrozenTsdbDelta {
    pub(crate) fn is_empty(&self) -> bool {
        self.samples.is_empty() && self.series_dict_delta.is_empty()
    }
}

/// Delta implementation for the write coordinator.
///
/// Uses the Arc base + HashMap overlay pattern: the base series dictionary
/// is an Arc clone from context (O(1)), and new series are tracked in a
/// local HashMap overlay. Since the write coordinator is single-threaded,
/// no DashMap/AtomicU32 is needed.
pub(crate) struct TsdbWriteDelta {
    bucket: TimeBucket,
    series_dict_base: Arc<HashMap<SeriesFingerprint, SeriesId>>,
    series_dict_delta: HashMap<SeriesFingerprint, SeriesId>,
    forward_index: ForwardIndex,
    inverted_index: InvertedIndex,
    samples: HashMap<SeriesId, Vec<Sample>>,
    next_series_id: u32,
}

impl TsdbWriteDelta {
    fn ingest(&mut self, series: &Series) -> Result<(), String> {
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
    ) -> Result<(), String> {
        // Validate sample timestamp is within bucket range
        let bucket_start_ms = self.bucket.start as i64 * 60 * 1000;
        let bucket_end_ms =
            (self.bucket.start as i64 + self.bucket.size_in_mins() as i64) * 60 * 1000;
        if sample.timestamp_ms < bucket_start_ms || sample.timestamp_ms >= bucket_end_ms {
            return Err(format!(
                "Sample timestamp {} is outside bucket range [{}, {})",
                sample.timestamp_ms, bucket_start_ms, bucket_end_ms
            ));
        }

        let fingerprint = labels.fingerprint();

        // Check delta overlay first (for series created in this delta)
        if let Some(&series_id) = self.series_dict_delta.get(&fingerprint) {
            self.samples.entry(series_id).or_default().push(sample);
            return Ok(());
        }

        // Check base dictionary
        if let Some(&series_id) = self.series_dict_base.get(&fingerprint) {
            self.samples.entry(series_id).or_default().push(sample);
            return Ok(());
        }

        // New series: allocate ID
        let series_id = self.next_series_id;
        self.next_series_id += 1;

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

        self.samples.entry(series_id).or_default().push(sample);
        Ok(())
    }
}

impl Delta for TsdbWriteDelta {
    type Context = TsdbContext;
    type Write = Vec<Series>;
    type Frozen = FrozenTsdbDelta;
    type FrozenView = ();
    type ApplyResult = ();
    type DeltaView = ();

    fn init(context: Self::Context) -> Self {
        Self {
            bucket: context.bucket,
            series_dict_base: context.series_dict.clone(),
            series_dict_delta: HashMap::new(),
            forward_index: ForwardIndex::default(),
            inverted_index: InvertedIndex::default(),
            samples: HashMap::new(),
            next_series_id: context.next_series_id,
        }
    }

    fn apply(&mut self, write: Self::Write) -> Result<Self::ApplyResult, String> {
        for series in &write {
            self.ingest(series)?;
        }
        Ok(())
    }

    fn estimate_size(&self) -> usize {
        // Rough estimate: 16 bytes per sample + index overhead
        let sample_count: usize = self.samples.values().map(|v| v.len()).sum();
        sample_count * 16
            + self.forward_index.series.len() * 128
            + self.inverted_index.postings.len() * 64
    }

    fn freeze(self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        // Merge base + delta into new Arc for the next context
        let merged_dict = if self.series_dict_delta.is_empty() {
            // O(1) when no new series
            self.series_dict_base.clone()
        } else {
            let mut merged = (*self.series_dict_base).clone();
            merged.extend(self.series_dict_delta.iter());
            Arc::new(merged)
        };

        let context = TsdbContext {
            bucket: self.bucket,
            series_dict: merged_dict,
            next_series_id: self.next_series_id,
        };

        let frozen = FrozenTsdbDelta {
            bucket: self.bucket,
            forward_index: self.forward_index,
            inverted_index: self.inverted_index,
            series_dict_delta: self.series_dict_delta,
            samples: self.samples,
        };

        (frozen, (), context)
    }

    fn reader(&self) -> Self::DeltaView {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MetricType;

    fn create_test_bucket() -> TimeBucket {
        TimeBucket::hour(1000)
    }

    fn create_test_context() -> TsdbContext {
        TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        }
    }

    fn create_test_sample() -> Sample {
        // Bucket at minute 1000 covers ms 60_000_000 to 63_600_000
        Sample {
            timestamp_ms: 60_000_001,
            value: 42.5,
        }
    }

    fn create_test_series(name: &str, labels: Vec<(&str, &str)>, sample: Sample) -> Series {
        let label_vec: Vec<Label> = labels.into_iter().map(|(k, v)| Label::new(k, v)).collect();
        let mut series = Series::new(name, label_vec, vec![sample]);
        series.metric_type = Some(MetricType::Gauge);
        series
    }

    #[test]
    fn should_create_new_series_on_first_apply() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let series =
            create_test_series("http_requests", vec![("env", "prod")], create_test_sample());

        // when
        delta.apply(vec![series]).unwrap();

        // then
        assert_eq!(delta.next_series_id, 1);
        assert_eq!(delta.series_dict_delta.len(), 1);
        assert_eq!(delta.samples.len(), 1);
        assert_eq!(delta.samples.get(&0).unwrap().len(), 1);
    }

    #[test]
    fn should_reuse_series_id_for_same_fingerprint() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let sample1 = Sample {
            timestamp_ms: 60_000_001,
            value: 10.0,
        };
        let sample2 = Sample {
            timestamp_ms: 60_000_002,
            value: 20.0,
        };
        let series1 = create_test_series("http_requests", vec![("env", "prod")], sample1);
        let series2 = create_test_series("http_requests", vec![("env", "prod")], sample2);

        // when
        delta.apply(vec![series1]).unwrap();
        delta.apply(vec![series2]).unwrap();

        // then
        assert_eq!(delta.next_series_id, 1); // Only one series created
        assert_eq!(delta.series_dict_delta.len(), 1);
        assert_eq!(delta.samples.get(&0).unwrap().len(), 2);
    }

    #[test]
    fn should_carry_series_dict_across_freeze_cycles() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let series =
            create_test_series("http_requests", vec![("env", "prod")], create_test_sample());
        delta.apply(vec![series]).unwrap();

        // when: freeze and create new delta
        let (frozen, _, new_ctx) = delta.freeze();
        let mut delta2 = TsdbWriteDelta::init(new_ctx);

        // Apply a series with the same fingerprint
        let sample2 = Sample {
            timestamp_ms: 60_000_002,
            value: 99.0,
        };
        let series2 = create_test_series("http_requests", vec![("env", "prod")], sample2);
        delta2.apply(vec![series2]).unwrap();

        // then: should reuse series_id from frozen context
        assert_eq!(delta2.next_series_id, 1); // No new ID allocated
        assert_eq!(delta2.series_dict_delta.len(), 0); // Not in delta overlay
        assert_eq!(delta2.samples.get(&0).unwrap().len(), 1);

        // And the frozen delta has the original series
        assert_eq!(frozen.series_dict_delta.len(), 1);
        assert_eq!(frozen.samples.get(&0).unwrap().len(), 1);
    }

    #[test]
    fn should_reject_sample_outside_bucket_range() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);
        let bad_sample = Sample {
            timestamp_ms: 59_999_999, // Before bucket start
            value: 42.5,
        };
        let series = create_test_series("http_requests", vec![("env", "prod")], bad_sample);

        // when
        let result = delta.apply(vec![series]);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("outside bucket range"));
    }

    #[test]
    fn should_accumulate_samples_across_multiple_applies() {
        // given
        let ctx = create_test_context();
        let mut delta = TsdbWriteDelta::init(ctx);

        // when: apply 3 batches with same series
        for i in 0..3 {
            let sample = Sample {
                timestamp_ms: 60_000_001 + i,
                value: i as f64,
            };
            let series = create_test_series("metric", vec![("k", "v")], sample);
            delta.apply(vec![series]).unwrap();
        }

        // then
        assert_eq!(delta.next_series_id, 1);
        assert_eq!(delta.samples.get(&0).unwrap().len(), 3);
    }

    #[test]
    fn should_skip_merge_on_freeze_when_no_new_series() {
        // given: context with existing series, delta with no new series
        let mut base = HashMap::new();
        base.insert(12345u128, 0u32);
        let ctx = TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(base),
            next_series_id: 1,
        };
        let delta = TsdbWriteDelta::init(ctx);
        let base_ptr = Arc::as_ptr(&delta.series_dict_base);

        // when
        let (_, _, new_ctx) = delta.freeze();

        // then: Arc should be the same pointer (O(1) clone, no merge)
        assert_eq!(Arc::as_ptr(&new_ctx.series_dict), base_ptr);
    }

    #[test]
    fn should_reuse_series_from_base_dict() {
        // given: context with pre-existing series
        let mut labels = vec![
            Label::new("__name__", "http_requests"),
            Label::new("env", "prod"),
        ];
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        let fingerprint = labels.fingerprint();

        let mut base = HashMap::new();
        base.insert(fingerprint, 42u32);
        let ctx = TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(base),
            next_series_id: 43,
        };
        let mut delta = TsdbWriteDelta::init(ctx);
        let series =
            create_test_series("http_requests", vec![("env", "prod")], create_test_sample());

        // when
        delta.apply(vec![series]).unwrap();

        // then
        assert_eq!(delta.next_series_id, 43); // No new ID allocated
        assert_eq!(delta.series_dict_delta.len(), 0); // Not added to delta
        assert!(delta.samples.contains_key(&42)); // Uses existing series_id
    }
}

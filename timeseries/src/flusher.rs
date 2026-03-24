use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use common::coordinator::Flusher;
use common::storage::{Storage, StorageSnapshot};

use crate::config::SampleStorageLayout;
use crate::delta::{FrozenTsdbDelta, TsdbWriteDelta};
use crate::storage::OpenTsdbStorageExt;

/// Flusher implementation for the timeseries write coordinator.
///
/// Converts a `FrozenTsdbDelta` into storage operations and applies them
/// atomically, then returns a new snapshot for readers.
pub(crate) struct TsdbFlusher {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) sample_storage_layout: SampleStorageLayout,
}

#[async_trait]
impl Flusher<TsdbWriteDelta> for TsdbFlusher {
    async fn flush_delta(
        &self,
        frozen: FrozenTsdbDelta,
        _epoch_range: &Range<u64>,
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        if frozen.is_empty() {
            return self.storage.snapshot().await.map_err(|e| e.to_string());
        }

        let mut ops = Vec::new();
        ops.push(
            self.storage
                .merge_bucket_list(frozen.bucket)
                .map_err(|e| e.to_string())?,
        );

        for (fingerprint, series_id) in &frozen.series_dict_delta {
            ops.push(
                self.storage
                    .insert_series_id(frozen.bucket, *fingerprint, *series_id)
                    .map_err(|e| e.to_string())?,
            );
        }

        for entry in frozen.forward_index.series.iter() {
            ops.push(
                self.storage
                    .insert_forward_index(frozen.bucket, *entry.key(), entry.value().clone())
                    .map_err(|e| e.to_string())?,
            );
        }

        for entry in frozen.inverted_index.postings.iter() {
            ops.push(
                self.storage
                    .merge_inverted_index(frozen.bucket, entry.key().clone(), entry.value().clone())
                    .map_err(|e| e.to_string())?,
            );
        }

        for (series_id, series_samples) in frozen.samples {
            let op = match self.sample_storage_layout {
                SampleStorageLayout::LegacySeriesId => self
                    .storage
                    .merge_samples(frozen.bucket, series_id, series_samples.samples),
                SampleStorageLayout::MetricPrefixed => self.storage.merge_metric_samples(
                    frozen.bucket,
                    &series_samples.metric_name,
                    series_id,
                    series_samples.samples,
                ),
            };
            ops.push(op.map_err(|e| e.to_string())?);
        }

        self.storage.apply(ops).await.map_err(|e| e.to_string())?;

        self.storage.snapshot().await.map_err(|e| e.to_string())
    }

    async fn flush_storage(&self) -> Result<(), String> {
        self.storage.flush().await.map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::TsdbContext;
    use crate::model::{Label, MetricType, Sample, Series, TimeBucket};
    use crate::storage::OpenTsdbStorageReadExt;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::coordinator::Delta;
    use common::storage::in_memory::InMemoryStorage;
    use std::collections::HashMap;

    fn create_test_storage() -> Arc<dyn Storage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )))
    }

    fn create_test_bucket() -> TimeBucket {
        TimeBucket::hour(1000)
    }

    fn create_test_sample() -> Sample {
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

    #[tokio::test]
    async fn should_flush_delta_to_storage() {
        // given
        let storage = create_test_storage();
        let flusher = TsdbFlusher {
            storage: storage.clone(),
            sample_storage_layout: SampleStorageLayout::LegacySeriesId,
        };
        let ctx = TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        };
        let mut delta = TsdbWriteDelta::init(ctx);
        let series =
            create_test_series("http_requests", vec![("env", "prod")], create_test_sample());
        delta.apply(vec![series]).unwrap();
        let (frozen, _, _) = delta.freeze();

        // when
        let snapshot = flusher.flush_delta(frozen, &(1..2)).await.unwrap();

        // then
        let buckets = snapshot.get_buckets_in_range(None, None).await.unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0], create_test_bucket());
    }

    #[tokio::test]
    async fn should_skip_empty_delta() {
        // given
        let storage = create_test_storage();
        let flusher = TsdbFlusher {
            storage: storage.clone(),
            sample_storage_layout: SampleStorageLayout::LegacySeriesId,
        };
        let ctx = TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        };
        let delta = TsdbWriteDelta::init(ctx);
        let (frozen, _, _) = delta.freeze();

        // when
        let result = flusher.flush_delta(frozen, &(1..2)).await;

        // then
        assert!(result.is_ok());
        let snapshot = result.unwrap();
        let buckets = snapshot.get_buckets_in_range(None, None).await.unwrap();
        assert_eq!(buckets.len(), 0);
    }

    fn create_failing_storage() -> Arc<common::storage::in_memory::FailingStorage> {
        let inner = create_test_storage();
        common::storage::in_memory::FailingStorage::wrap(inner)
    }

    fn create_non_empty_frozen() -> FrozenTsdbDelta {
        let ctx = TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        };
        let mut delta = TsdbWriteDelta::init(ctx);
        let series =
            create_test_series("http_requests", vec![("env", "prod")], create_test_sample());
        delta.apply(vec![series]).unwrap();
        let (frozen, _, _) = delta.freeze();
        frozen
    }

    #[tokio::test]
    async fn should_propagate_apply_error() {
        // given
        let storage = create_failing_storage();
        let flusher = TsdbFlusher {
            storage: storage.clone(),
            sample_storage_layout: SampleStorageLayout::LegacySeriesId,
        };
        storage.fail_apply(common::StorageError::Storage("test apply error".into()));

        // when
        let result = flusher
            .flush_delta(create_non_empty_frozen(), &(1..2))
            .await;

        // then
        let err = result.err().expect("expected apply error");
        assert!(
            err.contains("test apply error"),
            "expected test apply error message, got: {err}"
        );
    }

    #[tokio::test]
    async fn should_propagate_snapshot_error_after_apply() {
        // given
        let storage = create_failing_storage();
        let flusher = TsdbFlusher {
            storage: storage.clone(),
            sample_storage_layout: SampleStorageLayout::LegacySeriesId,
        };
        // Apply succeeds, but snapshot after apply fails
        storage.fail_snapshot(common::StorageError::Storage("test snapshot error".into()));

        // when
        let result = flusher
            .flush_delta(create_non_empty_frozen(), &(1..2))
            .await;

        // then
        let err = result.err().expect("expected snapshot error");
        assert!(
            err.contains("test snapshot error"),
            "expected test snapshot error message, got: {err}"
        );
    }

    #[tokio::test]
    async fn should_propagate_flush_storage_error() {
        // given
        let storage = create_failing_storage();
        let flusher = TsdbFlusher {
            storage: storage.clone(),
            sample_storage_layout: SampleStorageLayout::LegacySeriesId,
        };
        storage.fail_flush(common::StorageError::Storage("test flush error".into()));

        // when
        let result = flusher.flush_storage().await;

        // then
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("test flush error"),
            "expected test flush error message"
        );
    }

    #[tokio::test]
    async fn should_persist_series_dict_entries() {
        // given
        let storage = create_test_storage();
        let flusher = TsdbFlusher {
            storage: storage.clone(),
            sample_storage_layout: SampleStorageLayout::LegacySeriesId,
        };
        let ctx = TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        };
        let mut delta = TsdbWriteDelta::init(ctx);
        let series1 = create_test_series("metric_a", vec![("env", "prod")], create_test_sample());
        let series2 = create_test_series(
            "metric_b",
            vec![("env", "staging")],
            Sample {
                timestamp_ms: 60_000_002,
                value: 99.0,
            },
        );
        delta.apply(vec![series1, series2]).unwrap();
        let (frozen, _, _) = delta.freeze();

        // when
        let snapshot = flusher.flush_delta(frozen, &(1..3)).await.unwrap();

        // then: verify series dictionary was persisted
        let bucket = create_test_bucket();
        let mut count = 0;
        let _max_id = snapshot
            .load_series_dictionary(&bucket, |_fingerprint, _series_id| {
                count += 1;
            })
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    /// End-to-end test: ingest the same series across multiple deltas/flushes
    /// with MetricPrefixed layout and verify all samples remain visible.
    ///
    /// This is a regression test for a bug where the merge operator did not
    /// handle RecordType::MetricTimeSeries, causing each flush to overwrite
    /// previous samples instead of merging them.
    #[tokio::test]
    async fn should_merge_metric_prefixed_samples_across_multiple_flushes() {
        // given
        let storage = create_test_storage();
        let flusher = TsdbFlusher {
            storage: storage.clone(),
            sample_storage_layout: SampleStorageLayout::MetricPrefixed,
        };
        let bucket = create_test_bucket();

        // Flush 1: samples at t=60_000_001 and t=60_000_002
        let ctx1 = TsdbContext {
            bucket,
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        };
        let mut delta1 = TsdbWriteDelta::init(ctx1);
        delta1
            .apply(vec![create_test_series(
                "http_requests",
                vec![("env", "prod")],
                Sample { timestamp_ms: 60_000_001, value: 1.0 },
            )])
            .unwrap();
        delta1
            .apply(vec![create_test_series(
                "http_requests",
                vec![("env", "prod")],
                Sample { timestamp_ms: 60_000_002, value: 2.0 },
            )])
            .unwrap();
        let (frozen1, _, ctx2) = delta1.freeze();
        flusher.flush_delta(frozen1, &(1..2)).await.unwrap();

        // Flush 2: samples at t=60_000_003 and t=60_000_004
        let mut delta2 = TsdbWriteDelta::init(ctx2);
        delta2
            .apply(vec![create_test_series(
                "http_requests",
                vec![("env", "prod")],
                Sample { timestamp_ms: 60_000_003, value: 3.0 },
            )])
            .unwrap();
        delta2
            .apply(vec![create_test_series(
                "http_requests",
                vec![("env", "prod")],
                Sample { timestamp_ms: 60_000_004, value: 4.0 },
            )])
            .unwrap();
        let (frozen2, _, ctx3) = delta2.freeze();
        flusher.flush_delta(frozen2, &(2..3)).await.unwrap();

        // Flush 3: sample at t=60_000_005
        let mut delta3 = TsdbWriteDelta::init(ctx3);
        delta3
            .apply(vec![create_test_series(
                "http_requests",
                vec![("env", "prod")],
                Sample { timestamp_ms: 60_000_005, value: 5.0 },
            )])
            .unwrap();
        let (frozen3, _, _) = delta3.freeze();
        let snapshot = flusher.flush_delta(frozen3, &(3..4)).await.unwrap();

        // then: all 5 samples from all 3 flushes must be present
        let samples = snapshot
            .get_metric_samples(&bucket, "http_requests", 0, i64::MIN, i64::MAX)
            .await
            .unwrap();
        assert_eq!(
            samples.len(),
            5,
            "Expected 5 samples across 3 flushes, got {}. \
             If only the last flush's samples are present, the merge operator \
             is discarding existing values for MetricTimeSeries keys.",
            samples.len()
        );
        assert_eq!(samples[0].timestamp_ms, 60_000_001);
        assert_eq!(samples[1].timestamp_ms, 60_000_002);
        assert_eq!(samples[2].timestamp_ms, 60_000_003);
        assert_eq!(samples[3].timestamp_ms, 60_000_004);
        assert_eq!(samples[4].timestamp_ms, 60_000_005);
    }
}

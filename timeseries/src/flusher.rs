use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use common::coordinator::Flusher;
use common::storage::{Storage, StorageSnapshot};

use crate::delta::{FrozenTsdbDelta, TsdbWriteDelta};
use crate::storage::{OpenTsdbStorageExt, OpenTsdbStorageReadExt};
use crate::tsdb_metrics;

/// Flusher implementation for the timeseries write coordinator.
///
/// Converts a `FrozenTsdbDelta` into storage operations and applies them
/// atomically, then returns a new snapshot for readers.
pub(crate) struct TsdbFlusher {
    pub(crate) storage: Arc<dyn Storage>,
}

#[async_trait]
impl Flusher<TsdbWriteDelta> for TsdbFlusher {
    async fn flush_delta(
        &mut self,
        frozen: FrozenTsdbDelta,
        _epoch_range: &Range<u64>,
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        if frozen.is_empty() {
            return self.storage.snapshot().await.map_err(|e| e.to_string());
        }

        let new_series_count = frozen.series_dict_delta.len() as u64;
        let start = std::time::Instant::now();

        let mut ops = Vec::new();
        // Suppress the BucketList merge when this bucket is already listed.
        // Without this check, every flush emits an identical single-element
        // merge operand on a singleton hot key that only coalesces at major
        // compaction. `merge_batch_bucket_list` still dedupes, so concurrent
        // first-sightings from two flushers are safe.
        let bucket_announced = self
            .storage
            .bucket_list_contains(frozen.bucket)
            .await
            .map_err(|e| e.to_string())?;
        if !bucket_announced {
            ops.push(
                self.storage
                    .merge_bucket_list(frozen.bucket)
                    .map_err(|e| e.to_string())?,
            );
        }

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
            ops.push(
                self.storage
                    .merge_samples(
                        frozen.bucket,
                        series_id,
                        &series_samples.metric_name,
                        series_samples.points,
                    )
                    .map_err(|e| e.to_string())?,
            );
        }

        let result = self.storage.apply(ops).await.map_err(|e| e.to_string());
        let elapsed = start.elapsed().as_secs_f64();

        match &result {
            Ok(_) => {
                ::metrics::counter!(tsdb_metrics::TSDB_FLUSH_TOTAL, "status" => "success")
                    .increment(1);
                ::metrics::counter!(tsdb_metrics::TSDB_SERIES_CREATED).increment(new_series_count);
            }
            Err(_) => {
                ::metrics::counter!(tsdb_metrics::TSDB_FLUSH_TOTAL, "status" => "error")
                    .increment(1);
            }
        }
        ::metrics::histogram!(tsdb_metrics::TSDB_FLUSH_DURATION_SECONDS).record(elapsed);

        result?;
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
    use crate::serde::bucket_list::BucketListValue;
    use crate::serde::key::BucketListKey;
    use crate::storage::OpenTsdbStorageReadExt;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::Record;
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
        let mut flusher = TsdbFlusher {
            storage: storage.clone(),
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
        let mut flusher = TsdbFlusher {
            storage: storage.clone(),
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
        let mut flusher = TsdbFlusher {
            storage: storage.clone(),
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
        let mut flusher = TsdbFlusher {
            storage: storage.clone(),
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
    async fn should_register_bucket_on_first_flush() {
        // given
        let storage = create_test_storage();
        let mut flusher = TsdbFlusher {
            storage: storage.clone(),
        };
        let ctx = TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        };
        let mut delta = TsdbWriteDelta::init(ctx);
        delta
            .apply(vec![create_test_series(
                "m",
                vec![("env", "prod")],
                create_test_sample(),
            )])
            .unwrap();
        let (frozen, _, _) = delta.freeze();

        // when
        let snapshot = flusher.flush_delta(frozen, &(1..2)).await.unwrap();

        // then
        let buckets = snapshot.get_buckets_in_range(None, None).await.unwrap();
        assert_eq!(buckets, vec![create_test_bucket()]);
    }

    #[tokio::test]
    async fn should_not_duplicate_bucket_across_multiple_flushes() {
        // given: two back-to-back flushes for the same bucket
        let storage = create_test_storage();
        let mut flusher = TsdbFlusher {
            storage: storage.clone(),
        };
        let bucket = create_test_bucket();

        for (i, name) in ["metric_a", "metric_b"].iter().enumerate() {
            let ctx = TsdbContext {
                bucket,
                series_dict: Arc::new(HashMap::new()),
                next_series_id: i as u32,
            };
            let mut delta = TsdbWriteDelta::init(ctx);
            delta
                .apply(vec![create_test_series(
                    name,
                    vec![("env", "prod")],
                    Sample {
                        timestamp_ms: 60_000_000 + i as i64,
                        value: i as f64,
                    },
                )])
                .unwrap();
            let (frozen, _, _) = delta.freeze();

            // when
            flusher.flush_delta(frozen, &(1..2)).await.unwrap();
        }

        // then: bucket appears exactly once
        let snapshot = storage.snapshot().await.unwrap();
        let buckets = snapshot.get_buckets_in_range(None, None).await.unwrap();
        assert_eq!(buckets, vec![bucket]);
    }

    #[tokio::test]
    async fn should_skip_bucket_list_merge_when_bucket_already_present() {
        // given: storage pre-populated with the bucket in the BucketList
        let storage = create_test_storage();
        let bucket = create_test_bucket();
        let pre_existing = BucketListValue {
            buckets: vec![(bucket.size, bucket.start)],
        }
        .encode();
        storage
            .put(vec![common::storage::PutRecordOp::new(Record {
                key: BucketListKey.encode(),
                value: pre_existing,
            })])
            .await
            .unwrap();

        let mut flusher = TsdbFlusher {
            storage: storage.clone(),
        };
        let ctx = TsdbContext {
            bucket,
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        };
        let mut delta = TsdbWriteDelta::init(ctx);
        delta
            .apply(vec![create_test_series(
                "m",
                vec![("env", "prod")],
                create_test_sample(),
            )])
            .unwrap();
        let (frozen, _, _) = delta.freeze();

        // when
        let snapshot = flusher.flush_delta(frozen, &(1..2)).await.unwrap();

        // then: list still has exactly one entry for this bucket
        let buckets = snapshot.get_buckets_in_range(None, None).await.unwrap();
        assert_eq!(buckets, vec![bucket]);
    }

    #[tokio::test]
    async fn should_persist_series_dict_entries() {
        // given
        let storage = create_test_storage();
        let mut flusher = TsdbFlusher {
            storage: storage.clone(),
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
}

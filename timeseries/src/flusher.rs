use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use common::StorageError;
use common::coordinator::Flusher;
use slatedb::{Db, DbSnapshot, WriteBatch};

use crate::delta::{FrozenTsdbDelta, TsdbWriteDelta};
use crate::storage::{
    bucket_list_contains, insert_forward_index_kv, insert_series_id_kv, merge_bucket_list_kv,
    merge_inverted_index_kv, merge_samples_kv,
};
use crate::tsdb_metrics;

/// Flusher implementation for the timeseries write coordinator.
///
/// Converts a `FrozenTsdbDelta` into storage operations and applies them
/// atomically, then returns a new snapshot for readers.
pub(crate) struct TsdbFlusher {
    pub(crate) db: Arc<Db>,
}

#[async_trait]
impl Flusher<TsdbWriteDelta> for TsdbFlusher {
    async fn flush_delta(
        &mut self,
        frozen: FrozenTsdbDelta,
        _epoch_range: &Range<u64>,
    ) -> Result<Arc<DbSnapshot>, String> {
        if frozen.is_empty() {
            return self.db.snapshot().await.map_err(|e| e.to_string());
        }

        let new_series_count = frozen.series_dict_delta.len() as u64;
        let start = std::time::Instant::now();

        let mut batch = WriteBatch::new();
        // Suppress the BucketList merge when this bucket is already listed.
        // Without this check, every flush emits an identical single-element
        // merge operand on a singleton hot key that only coalesces at major
        // compaction. `merge_batch_bucket_list` still dedupes, so concurrent
        // first-sightings from two flushers are safe.
        let bucket_announced = bucket_list_contains(self.db.as_ref(), frozen.bucket)
            .await
            .map_err(|e| e.to_string())?;
        if !bucket_announced {
            let kv = merge_bucket_list_kv(frozen.bucket).map_err(|e| e.to_string())?;
            batch.merge(kv.key, kv.value);
        }

        for (fingerprint, series_id) in &frozen.series_dict_delta {
            let kv = insert_series_id_kv(frozen.bucket, *fingerprint, *series_id)
                .map_err(|e| e.to_string())?;
            batch.put(kv.key, kv.value);
        }

        for entry in frozen.forward_index.series.iter() {
            let kv = insert_forward_index_kv(frozen.bucket, *entry.key(), entry.value().clone())
                .map_err(|e| e.to_string())?;
            batch.put(kv.key, kv.value);
        }

        for entry in frozen.inverted_index.postings.iter() {
            let kv =
                merge_inverted_index_kv(frozen.bucket, entry.key().clone(), entry.value().clone())
                    .map_err(|e| e.to_string())?;
            batch.merge(kv.key, kv.value);
        }

        for (series_id, series_samples) in frozen.samples {
            let kv = merge_samples_kv(
                frozen.bucket,
                series_id,
                &series_samples.metric_name,
                series_samples.points,
            )
            .map_err(|e| e.to_string())?;
            batch.merge(kv.key, kv.value);
        }

        let result = self
            .db
            .write(batch)
            .await
            .map_err(StorageError::from_storage)
            .map_err(|e| e.to_string());
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
        self.db.snapshot().await.map_err(|e| e.to_string())
    }

    async fn flush_storage(&self) -> Result<(), String> {
        self.db.flush().await.map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::TsdbContext;
    use crate::model::{Label, MetricType, Sample, Series, TimeBucket};
    use crate::serde::bucket_list::BucketListValue;
    use crate::serde::key::BucketListKey;
    use crate::storage::get_buckets_in_range;
    use crate::storage::load_series_dictionary;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::coordinator::Delta;
    use common::storage::testing::FailingObjectStore;
    use slatedb::DbBuilder;
    use slatedb::object_store::memory::InMemory;
    use std::collections::HashMap;

    async fn create_test_db() -> Arc<Db> {
        let object_store = Arc::new(InMemory::new());
        let db = DbBuilder::new("test", object_store)
            .with_merge_operator(Arc::new(OpenTsdbMergeOperator))
            .build()
            .await
            .unwrap();
        Arc::new(db)
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
        let db = create_test_db().await;
        let mut flusher = TsdbFlusher { db: db.clone() };
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
        let buckets = get_buckets_in_range(snapshot.as_ref(), None, None)
            .await
            .unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0], create_test_bucket());
    }

    #[tokio::test]
    async fn should_skip_empty_delta() {
        // given
        let db = create_test_db().await;
        let mut flusher = TsdbFlusher { db: db.clone() };
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
        let buckets = get_buckets_in_range(snapshot.as_ref(), None, None)
            .await
            .unwrap();
        assert_eq!(buckets.len(), 0);
    }

    async fn create_failing_db() -> (Arc<Db>, Arc<FailingObjectStore>) {
        let inner: Arc<dyn slatedb::object_store::ObjectStore> = Arc::new(InMemory::new());
        let failing = FailingObjectStore::new(inner);
        let db = common::StorageBuilder::from_object_store("test", failing.clone())
            .await
            .unwrap()
            .with_merge_operator(Arc::new(OpenTsdbMergeOperator))
            .build()
            .await
            .unwrap();
        (db.db, failing)
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
        let (db, failing) = create_failing_db().await;
        let mut flusher = TsdbFlusher { db: db.clone() };
        // Inject a non-retryable Precondition; slatedb would retry any other
        // object_store::Error forever.
        failing.set_fail_put(true);

        // when
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            flusher.flush_delta(create_non_empty_frozen(), &(1..2)),
        )
        .await
        .expect("flush_delta hung past 5s; retry barrier is broken");

        // then
        assert!(result.is_err(), "expected flush to fail with fail_put set");
    }

    /// `db.snapshot()` fails with `SlateDBError::Closed` after the db is
    /// closed. `flush_delta` with an empty frozen delta takes the
    /// snapshot-only shortcut, so closing the db before the call isolates
    /// the failure to the snapshot step.
    #[tokio::test]
    async fn should_propagate_snapshot_error() {
        // given
        let db = create_test_db().await;
        let mut flusher = TsdbFlusher { db: db.clone() };
        // Close the db so the next db.snapshot() returns Closed
        db.close().await.unwrap();

        // when — empty delta means flush_delta jumps straight to snapshot()
        let ctx = TsdbContext {
            bucket: create_test_bucket(),
            series_dict: Arc::new(HashMap::new()),
            next_series_id: 0,
        };
        let delta = TsdbWriteDelta::init(ctx);
        let (frozen, _, _) = delta.freeze();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            flusher.flush_delta(frozen, &(1..2)),
        )
        .await
        .expect("flush_delta hung past 5s");

        // then
        assert!(
            result.is_err(),
            "expected snapshot error after db.close(), got Ok"
        );
    }

    #[tokio::test]
    async fn should_propagate_flush_storage_error() {
        use slatedb::config::WriteOptions;
        // given — write something non-durably so it stays in memtable; then
        // enable put failures so the subsequent flush_storage (which drains the
        // memtable through object-store puts) actually surfaces an error.
        let (db, failing) = create_failing_db().await;
        let flusher = TsdbFlusher { db: db.clone() };
        let mut batch = WriteBatch::new();
        batch.put(b"__probe", b"v");
        db.write_with_options(
            batch,
            &WriteOptions {
                await_durable: false,
            },
        )
        .await
        .unwrap();
        failing.set_fail_put(true);

        // when
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), flusher.flush_storage())
                .await
                .expect("flush_storage hung past 5s; retry barrier is broken");

        // then
        assert!(result.is_err(), "expected flush_storage to fail");
    }

    #[tokio::test]
    async fn should_register_bucket_on_first_flush() {
        // given
        let db = create_test_db().await;
        let mut flusher = TsdbFlusher { db: db.clone() };
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
        let buckets = get_buckets_in_range(snapshot.as_ref(), None, None)
            .await
            .unwrap();
        assert_eq!(buckets, vec![create_test_bucket()]);
    }

    #[tokio::test]
    async fn should_not_duplicate_bucket_across_multiple_flushes() {
        // given: two back-to-back flushes for the same bucket
        let db = create_test_db().await;
        let mut flusher = TsdbFlusher { db: db.clone() };
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
        let snapshot = db.snapshot().await.unwrap();
        let buckets = get_buckets_in_range(snapshot.as_ref(), None, None)
            .await
            .unwrap();
        assert_eq!(buckets, vec![bucket]);
    }

    #[tokio::test]
    async fn should_skip_bucket_list_merge_when_bucket_already_present() {
        // given: storage pre-populated with the bucket in the BucketList
        let db = create_test_db().await;
        let bucket = create_test_bucket();
        let pre_existing = BucketListValue {
            buckets: vec![(bucket.size, bucket.start)],
        }
        .encode();
        db.put(&BucketListKey.encode(), &pre_existing)
            .await
            .unwrap();

        let mut flusher = TsdbFlusher { db: db.clone() };
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
        let buckets = get_buckets_in_range(snapshot.as_ref(), None, None)
            .await
            .unwrap();
        assert_eq!(buckets, vec![bucket]);
    }

    #[tokio::test]
    async fn should_persist_series_dict_entries() {
        // given
        let db = create_test_db().await;
        let mut flusher = TsdbFlusher { db: db.clone() };
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
        let _max_id =
            load_series_dictionary(snapshot.as_ref(), &bucket, |_fingerprint, _series_id| {
                count += 1;
            })
            .await
            .unwrap();
        assert_eq!(count, 2);
    }
}

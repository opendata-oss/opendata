//! Startup cache warmer that pre-populates the block cache by scanning
//! recent time bucket key ranges through the normal StorageRead API.
//!
//! This is a temporary workaround until SlateDB's CacheManager
//! (`set_warm_prefixes()` + `warm_current()`) is available. Delete this
//! module when that lands.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use common::{BytesRange, StorageError, default_scan_options};
use slatedb::DbRead;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::promql::config::CacheWarmerConfig;
use crate::serde::TimeBucketScoped;
use crate::serde::key::{
    BucketListKey, ForwardIndexKey, InvertedIndexKey, SeriesDictionaryKey, TimeSeriesKey,
};
use crate::storage::get_buckets_in_range;

const LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Erased reader trait used by the cache warmer.
///
/// `slatedb::DbRead` is not object-safe (its methods are generic over key
/// types), so we wrap the two operations the warmer needs (`get`, `scan_iter`)
/// behind an object-safe trait. Implemented for the concrete `Arc<Db>` and
/// `Arc<DbReader>` handles the TSDB holds at runtime.
#[async_trait]
pub(crate) trait CacheWarmerReader: Send + Sync {
    async fn get_bytes(&self, key: Bytes) -> crate::util::Result<Option<Bytes>>;

    /// Scan `range` and return the number of records touched, populating any
    /// block cache attached to the underlying `DbRead` as a side effect.
    async fn drain_scan(&self, range: BytesRange) -> crate::util::Result<u64>;

    /// Erased version of `get_buckets_in_range` (needed because `DbRead` is not
    /// object-safe).
    async fn list_buckets_in_range(
        &self,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> crate::util::Result<Vec<crate::model::TimeBucket>>;
}

#[async_trait]
impl<R> CacheWarmerReader for R
where
    R: DbRead + Send + Sync + 'static,
{
    async fn get_bytes(&self, key: Bytes) -> crate::util::Result<Option<Bytes>> {
        self.get(&key)
            .await
            .map_err(|e| StorageError::from_storage(e).into())
    }

    async fn drain_scan(&self, range: BytesRange) -> crate::util::Result<u64> {
        let mut iter = self
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        let mut count = 0u64;
        while let Some(_kv) = iter.next().await.map_err(StorageError::from_storage)? {
            count += 1;
        }
        Ok(count)
    }

    async fn list_buckets_in_range(
        &self,
        start_secs: Option<i64>,
        end_secs: Option<i64>,
    ) -> crate::util::Result<Vec<crate::model::TimeBucket>> {
        get_buckets_in_range(self, start_secs, end_secs).await
    }
}

pub(crate) struct CacheWarmerHandle {
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

impl CacheWarmerHandle {
    pub async fn shutdown(self) {
        self.cancel.cancel();
        if let Err(e) = self.join.await {
            tracing::error!("Cache warmer task panicked: {e}");
        }
    }
}

/// Spawns a one-off cache warming task. Returns a handle that must be
/// shut down before closing the database.
pub(crate) fn start(
    reader: Arc<dyn CacheWarmerReader>,
    config: CacheWarmerConfig,
) -> CacheWarmerHandle {
    let cancel = CancellationToken::new();
    let join = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            match warm(&reader, &config, &cancel).await {
                Ok(stats) => tracing::info!(
                    buckets = stats.buckets,
                    records = stats.records,
                    elapsed = ?stats.elapsed,
                    "Cache warming complete"
                ),
                Err(e) => tracing::warn!("Cache warming failed: {e}"),
            }
        }
    });
    CacheWarmerHandle { cancel, join }
}

struct WarmStats {
    buckets: usize,
    records: u64,
    elapsed: Duration,
}

async fn warm(
    reader: &Arc<dyn CacheWarmerReader>,
    config: &CacheWarmerConfig,
    cancel: &CancellationToken,
) -> crate::util::Result<WarmStats> {
    let start = Instant::now();
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let range_start = now_secs - config.warm_range.as_secs() as i64;

    // Warm the bucket list key
    let _ = reader.get_bytes(BucketListKey.encode()).await?;

    let buckets = reader
        .list_buckets_in_range(Some(range_start), Some(now_secs))
        .await?;
    let total = buckets.len();

    let mut records: u64 = 0;
    let mut last_log = Instant::now();

    for (i, bucket) in buckets.iter().enumerate() {
        if cancel.is_cancelled() {
            tracing::info!("Cache warming cancelled");
            break;
        }

        records += reader
            .drain_scan(ForwardIndexKey::bucket_range(bucket))
            .await?;
        records += reader
            .drain_scan(InvertedIndexKey::bucket_range(bucket))
            .await?;
        records += reader
            .drain_scan(SeriesDictionaryKey::bucket_range(bucket))
            .await?;

        if config.include_samples {
            records += reader
                .drain_scan(TimeSeriesKey::bucket_range(bucket))
                .await?;
        }

        if last_log.elapsed() >= LOG_INTERVAL {
            tracing::info!(
                bucket = i + 1,
                total,
                records,
                elapsed = ?start.elapsed(),
                "Cache warming progress"
            );
            last_log = Instant::now();
        }
    }

    Ok(WarmStats {
        buckets: total,
        records,
        elapsed: start.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Series;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use crate::tsdb::Tsdb;
    use slatedb::DbBuilder;
    use slatedb::object_store::memory::InMemory;

    async fn create_db() -> Arc<slatedb::Db> {
        let object_store = Arc::new(InMemory::new());
        let db = DbBuilder::new("test", object_store)
            .with_merge_operator(Arc::new(OpenTsdbMergeOperator))
            .build()
            .await
            .unwrap();
        Arc::new(db)
    }

    #[tokio::test]
    async fn should_warm_with_data() {
        // given
        let db = create_db().await;
        let tsdb = Tsdb::new(db.clone());
        let series = vec![
            Series::builder("http_requests_total")
                .label("method", "GET")
                .sample(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                    100.0,
                )
                .build(),
        ];
        tsdb.ingest_samples(series, None).await.unwrap();
        tsdb.flush().await.unwrap();

        let config = CacheWarmerConfig {
            warm_range: Duration::from_secs(3600),
            include_samples: true,
        };

        // when
        let cancel = CancellationToken::new();
        let reader: Arc<dyn CacheWarmerReader> = db;
        let stats = warm(&reader, &config, &cancel).await.unwrap();

        // then
        assert!(stats.buckets > 0);
        assert!(stats.records > 0);
    }

    #[tokio::test]
    async fn should_warm_empty_storage() {
        // given
        let db = create_db().await;
        let config = CacheWarmerConfig {
            warm_range: Duration::from_secs(3600),
            include_samples: true,
        };

        // when
        let cancel = CancellationToken::new();
        let reader: Arc<dyn CacheWarmerReader> = db;
        let stats = warm(&reader, &config, &cancel).await.unwrap();

        // then
        assert_eq!(stats.buckets, 0);
        assert_eq!(stats.records, 0);
    }

    #[tokio::test]
    async fn should_stop_on_cancellation() {
        // given
        let db = create_db().await;
        let tsdb = Tsdb::new(db.clone());
        let series = vec![
            Series::builder("metric_a")
                .label("env", "test")
                .sample(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64,
                    1.0,
                )
                .build(),
        ];
        tsdb.ingest_samples(series, None).await.unwrap();
        tsdb.flush().await.unwrap();

        let config = CacheWarmerConfig {
            warm_range: Duration::from_secs(3600),
            include_samples: true,
        };

        // when — cancel before starting
        let cancel = CancellationToken::new();
        cancel.cancel();
        let reader: Arc<dyn CacheWarmerReader> = db;
        let stats = warm(&reader, &config, &cancel).await.unwrap();

        // then — should have found buckets but processed none
        assert_eq!(stats.records, 0);
    }
}

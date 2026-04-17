//! Startup cache warmer that pre-populates the block cache by scanning
//! recent time bucket key ranges through the normal StorageRead API.
//!
//! This is a temporary workaround until SlateDB's CacheManager
//! (`set_warm_prefixes()` + `warm_current()`) is available. Delete this
//! module when that lands.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{BytesRange, StorageRead};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::promql::config::CacheWarmerConfig;
use crate::serde::TimeBucketScoped;
use crate::serde::key::{
    BucketListKey, ForwardIndexKey, InvertedIndexKey, SeriesDictionaryKey, TimeSeriesKey,
};
use crate::storage::OpenTsdbStorageReadExt;

const LOG_INTERVAL: Duration = Duration::from_secs(30);

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
pub(crate) fn start(storage: Arc<dyn StorageRead>, config: CacheWarmerConfig) -> CacheWarmerHandle {
    let cancel = CancellationToken::new();
    let join = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            match warm(&storage, &config, &cancel).await {
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
    storage: &Arc<dyn StorageRead>,
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
    let _ = storage.get(BucketListKey.encode()).await?;

    let buckets = storage
        .get_buckets_in_range(Some(range_start), Some(now_secs))
        .await?;
    let total = buckets.len();

    let mut records: u64 = 0;
    let mut last_log = Instant::now();

    for (i, bucket) in buckets.iter().enumerate() {
        if cancel.is_cancelled() {
            tracing::info!("Cache warming cancelled");
            break;
        }

        records += drain_scan(storage, ForwardIndexKey::bucket_range(bucket)).await?;
        records += drain_scan(storage, InvertedIndexKey::bucket_range(bucket)).await?;
        records += drain_scan(storage, SeriesDictionaryKey::bucket_range(bucket)).await?;

        if config.include_samples {
            records += drain_scan(storage, TimeSeriesKey::bucket_range(bucket)).await?;
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

/// Scan a key range, consuming all records to populate the block cache.
/// Returns the number of records touched.
async fn drain_scan(storage: &Arc<dyn StorageRead>, range: BytesRange) -> crate::util::Result<u64> {
    let mut iter = storage.scan_iter(range).await?;
    let mut count = 0u64;
    while iter.next().await?.is_some() {
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Series;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use crate::tsdb::Tsdb;
    use common::storage::in_memory::InMemoryStorage;

    fn create_storage() -> Arc<InMemoryStorage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )))
    }

    #[tokio::test]
    async fn should_warm_with_data() {
        // given
        let storage = create_storage();
        let tsdb = Tsdb::new(storage.clone());
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
        let stats = warm(&(storage as Arc<dyn StorageRead>), &config, &cancel)
            .await
            .unwrap();

        // then
        assert!(stats.buckets > 0);
        assert!(stats.records > 0);
    }

    #[tokio::test]
    async fn should_warm_empty_storage() {
        // given
        let storage = create_storage();
        let config = CacheWarmerConfig {
            warm_range: Duration::from_secs(3600),
            include_samples: true,
        };

        // when
        let cancel = CancellationToken::new();
        let stats = warm(&(storage as Arc<dyn StorageRead>), &config, &cancel)
            .await
            .unwrap();

        // then
        assert_eq!(stats.buckets, 0);
        assert_eq!(stats.records, 0);
    }

    #[tokio::test]
    async fn should_stop_on_cancellation() {
        // given
        let storage = create_storage();
        let tsdb = Tsdb::new(storage.clone());
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
        let stats = warm(&(storage as Arc<dyn StorageRead>), &config, &cancel)
            .await
            .unwrap();

        // then — should have found buckets but processed none
        assert_eq!(stats.records, 0);
    }
}

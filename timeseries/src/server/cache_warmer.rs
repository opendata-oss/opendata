//! Startup cache warmer that pre-populates the block cache for recent time
//! buckets.
//!
//! It discovers the buckets overlapping the configured recent window and hands
//! them to [`WarmStorage::warm`], which drives SlateDB's cache manager
//! (`warm_sst`) over the SSTs backing those buckets — pulling their filters,
//! index, and (optionally) sample data blocks into the block cache.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::promql::config::CacheWarmerConfig;
use crate::storage::WarmStorage;

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
pub(crate) fn start<R: WarmStorage + 'static>(
    storage: R,
    config: CacheWarmerConfig,
) -> CacheWarmerHandle {
    let cancel = CancellationToken::new();
    let join = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            match warm(&storage, &config, &cancel).await {
                Ok(stats) => tracing::info!(
                    buckets = stats.buckets,
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
    elapsed: Duration,
}

async fn warm<R: WarmStorage>(
    storage: &R,
    config: &CacheWarmerConfig,
    cancel: &CancellationToken,
) -> crate::util::Result<WarmStats> {
    let start = Instant::now();
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let range_start = now_secs - config.warm_range.as_secs() as i64;

    let buckets = storage
        .get_buckets_in_range(Some(range_start), Some(now_secs))
        .await?;
    let total = buckets.len();

    storage
        .warm(buckets, config.include_samples, cancel)
        .await?;

    Ok(WarmStats {
        buckets: total,
        elapsed: start.elapsed(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Series;
    use crate::storage::{Storage, in_memory_storage};
    use crate::tsdb::Tsdb;
    use std::sync::Arc;

    async fn create_storage() -> Arc<Storage> {
        Arc::new(in_memory_storage().await)
    }

    #[tokio::test]
    async fn should_warm_with_data() {
        // given
        let storage = create_storage().await;
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
        let stats = warm(storage.as_ref(), &config, &cancel).await.unwrap();

        // then — the recent bucket is discovered and warming succeeds
        assert!(stats.buckets > 0);
    }

    #[tokio::test]
    async fn should_warm_empty_storage() {
        // given
        let storage = create_storage().await;
        let config = CacheWarmerConfig {
            warm_range: Duration::from_secs(3600),
            include_samples: true,
        };

        // when
        let cancel = CancellationToken::new();
        let stats = warm(storage.as_ref(), &config, &cancel).await.unwrap();

        // then
        assert_eq!(stats.buckets, 0);
    }

    #[tokio::test]
    async fn should_complete_when_cancelled() {
        // given
        let storage = create_storage().await;
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

        // then — warming returns gracefully (the warm short-circuits on the
        // pre-cancelled token) rather than erroring or hanging
        warm(storage.as_ref(), &config, &cancel).await.unwrap();
    }
}

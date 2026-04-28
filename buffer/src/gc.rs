use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime},
};

use futures::{StreamExt, stream};
use slatedb::object_store::{ObjectStore, path::Path};
use tokio_util::sync::CancellationToken;

use crate::{Error, metric_names as m, queue::ManifestStore};

pub(crate) struct GarbageCollector {
    manifest_store: ManifestStore,
    object_store: Arc<dyn ObjectStore>,
    data_path_prefix: String,
    gc_interval: Duration,
    gc_grace_period: Duration,
}

struct ManifestSnapshot {
    locations: HashSet<String>,
    oldest_location_ts: Option<SystemTime>,
}

impl GarbageCollector {
    pub(crate) fn new(
        manifest_path: String,
        data_path_prefix: String,
        gc_interval: Duration,
        gc_grace_period: Duration,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            manifest_store: ManifestStore {
                object_store: object_store.clone(),
                manifest_path,
            },
            object_store,
            data_path_prefix,
            gc_interval,
            gc_grace_period,
        }
    }

    async fn collect_once(&self, now: SystemTime) -> Result<(), Error> {
        let start = std::time::Instant::now();

        // take snapshot of manifest
        let manifest = self.read_manifest_snapshot().await?;

        // list files currently in object store
        let prefix = Path::from(format!("{}/", self.data_path_prefix));
        let mut list_stream = self.object_store.list(Some(&prefix));

        // collect deletion candidates
        let mut to_delete: Vec<Path> = Vec::new();
        while let Some(result) = list_stream.next().await {
            let meta =
                result.map_err(|e| Error::Storage(format!("Failed to list objects: {}", e)))?;

            let path_str = meta.location.to_string();

            // extract ULID timestamp from filename; skip non-.batch / non-ULID files
            let file_ts = match Self::extract_ulid_timestamp(&path_str) {
                Some(ts) => ts,
                None => continue,
            };

            // skip if newer than the oldest manifest entry — could be an in-flight enqueue
            if let Some(oldest_ts) = manifest.oldest_location_ts
                && file_ts >= oldest_ts
            {
                continue;
            }

            // skip if younger than the grace period
            let age = now.duration_since(file_ts).unwrap_or_default();
            if age < self.gc_grace_period {
                continue;
            }

            // skip if referenced in the manifest — still needs to be consumed
            if manifest.locations.contains(&path_str) {
                continue;
            }

            to_delete.push(meta.location);
        }

        // bulk delete all candidates
        let mut deleted: u64 = 0;
        let mut failed: u64 = 0;
        if !to_delete.is_empty() {
            tracing::debug!(count = to_delete.len(), "GC deleting orphaned batch files");
            let locations = stream::iter(to_delete.iter().cloned().map(Ok));
            let mut results = self.object_store.delete_stream(locations.boxed());
            while let Some(result) = results.next().await {
                match result {
                    Ok(_) => deleted += 1,
                    Err(e) => {
                        failed += 1;
                        tracing::warn!(error = %e, "GC failed to delete batch file");
                    }
                }
            }
        }

        metrics::counter!(m::GC_FILES_DELETED).increment(deleted);
        metrics::counter!(m::GC_FILES_FAILED).increment(failed);
        metrics::histogram!(m::GC_DURATION_SECONDS).record(start.elapsed().as_secs_f64());

        Ok(())
    }

    pub(crate) async fn collect(self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    tracing::info!("garbage collector shutting down");
                    return;
                }
                _ = tokio::time::sleep(self.gc_interval) => {
                    if let Err(e) = self.collect_once(SystemTime::now()).await {
                        tracing::warn!(error = %e, "garbage collection cycle failed");
                    }
                }
            }
        }
    }

    fn extract_ulid_timestamp(path: &str) -> Option<SystemTime> {
        let filename = path.rsplit('/').next()?;
        let stem = filename.strip_suffix(".batch")?;
        let ulid = ulid::Ulid::from_string(stem).ok()?;
        Some(ulid.datetime())
    }

    async fn read_manifest_snapshot(&self) -> Result<ManifestSnapshot, Error> {
        let result = self.manifest_store.read().await?;

        let mut locations = HashSet::new();
        let mut oldest_location_ts: Option<SystemTime> = None;

        for entry in result.0.iter() {
            let entry = entry?;
            locations.insert(entry.location.clone());

            if let Some(ts) = Self::extract_ulid_timestamp(&entry.location)
                && oldest_location_ts.is_none_or(|old| ts < old)
            {
                oldest_location_ts = Some(ts);
            }
        }

        Ok(ManifestSnapshot {
            locations,
            oldest_location_ts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CollectorConfig;
    use crate::model::encode_batch;
    use crate::queue::QueueProducer;
    use bytes::Bytes;
    use common::ObjectStoreConfig;
    use slatedb::object_store::PutPayload;
    use slatedb::object_store::memory::InMemory;
    use std::time::Duration;

    const TEST_MANIFEST_PATH: &str = "test/manifest";
    const TEST_DATA_PREFIX: &str = "ingest";

    fn make_config() -> CollectorConfig {
        CollectorConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: TEST_MANIFEST_PATH.to_string(),
            data_path_prefix: TEST_DATA_PREFIX.to_string(),
            gc_interval: Duration::from_secs(1),
            gc_grace_period: Duration::from_secs(0),
        }
    }

    fn make_gc(store: &Arc<dyn ObjectStore>, config: CollectorConfig) -> GarbageCollector {
        GarbageCollector::new(
            config.manifest_path,
            config.data_path_prefix,
            config.gc_interval,
            config.gc_grace_period,
            store.clone(),
        )
    }

    fn make_producer(store: &Arc<dyn ObjectStore>) -> QueueProducer {
        QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone())
    }

    /// Create a batch file path with a ULID generated from the given timestamp.
    fn batch_path_from_ts(ts_ms: u64) -> String {
        let ulid = ulid::Ulid::from_parts(ts_ms, 0);
        format!("{}/{}.batch", TEST_DATA_PREFIX, ulid)
    }

    async fn write_batch_file(store: &Arc<dyn ObjectStore>, location: &str) {
        let entries = &[Bytes::from("data")];
        let payload = encode_batch(entries, crate::CompressionType::None).unwrap();
        let path = Path::from(location);
        store.put(&path, PutPayload::from(payload)).await.unwrap();
    }

    async fn file_exists(store: &Arc<dyn ObjectStore>, location: &str) -> bool {
        store.get(&Path::from(location)).await.is_ok()
    }

    #[tokio::test]
    async fn should_delete_orphaned_batch_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gc = make_gc(&store, make_config());

        // Write an old orphaned batch file (not in manifest)
        let old_path = batch_path_from_ts(1000);
        write_batch_file(&store, &old_path).await;

        // Run GC with "now" far in the future so the file is old enough
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000);
        gc.collect_once(now).await.unwrap();

        assert!(!file_exists(&store, &old_path).await);
    }

    #[tokio::test]
    async fn should_not_delete_referenced_batch_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gc = make_gc(&store, make_config());
        let producer = make_producer(&store);

        // Write a batch file and enqueue it in the manifest
        let path = batch_path_from_ts(1000);
        write_batch_file(&store, &path).await;
        producer.enqueue(path.clone(), vec![]).await.unwrap();

        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000);
        gc.collect_once(now).await.unwrap();

        assert!(file_exists(&store, &path).await);
    }

    #[tokio::test]
    async fn should_not_delete_files_within_grace_period() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = CollectorConfig {
            gc_grace_period: Duration::from_secs(600),
            ..make_config()
        };
        let gc = make_gc(&store, config);

        // Write a batch file with a recent timestamp
        let recent_ts_ms = 900_000;
        let path = batch_path_from_ts(recent_ts_ms);
        write_batch_file(&store, &path).await;

        // "now" is only slightly after the file timestamp — within grace period
        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000);
        gc.collect_once(now).await.unwrap();

        assert!(file_exists(&store, &path).await);
    }

    #[tokio::test]
    async fn should_not_delete_files_newer_than_oldest_manifest_entry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gc = make_gc(&store, make_config());
        let producer = make_producer(&store);

        // Enqueue a batch at ts=5000 (this becomes the oldest manifest entry)
        let manifest_path = batch_path_from_ts(5000);
        write_batch_file(&store, &manifest_path).await;
        producer
            .enqueue(manifest_path.clone(), vec![])
            .await
            .unwrap();

        // Write an orphaned batch at ts=6000 (newer than the oldest manifest entry)
        let newer_path = batch_path_from_ts(6000);
        write_batch_file(&store, &newer_path).await;

        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000);
        gc.collect_once(now).await.unwrap();

        // The newer orphaned file should NOT be deleted
        assert!(file_exists(&store, &newer_path).await);
        // The referenced file should also still exist
        assert!(file_exists(&store, &manifest_path).await);
    }

    #[tokio::test]
    async fn should_skip_non_batch_files() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gc = make_gc(&store, make_config());

        // Write files without .batch suffix under the data prefix
        let txt_path = format!("{}/somefile.txt", TEST_DATA_PREFIX);
        let no_ext_path = format!("{}/01J5T4R3KXBMZ7QV9N2WG8YDHP", TEST_DATA_PREFIX);
        write_batch_file(&store, &txt_path).await;
        write_batch_file(&store, &no_ext_path).await;

        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000);
        gc.collect_once(now).await.unwrap();

        // Both should still exist — GC skips non-.batch / non-ULID files
        assert!(file_exists(&store, &txt_path).await);
        assert!(file_exists(&store, &no_ext_path).await);
    }

    #[tokio::test]
    async fn should_delete_older_orphans_but_keep_referenced() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gc = make_gc(&store, make_config());
        let producer = make_producer(&store);

        // Enqueue a batch at ts=5000 (oldest manifest entry)
        let referenced = batch_path_from_ts(5000);
        write_batch_file(&store, &referenced).await;
        producer.enqueue(referenced.clone(), vec![]).await.unwrap();

        // Orphaned batch at ts=2000 (older than oldest manifest entry)
        let orphan_old = batch_path_from_ts(2000);
        write_batch_file(&store, &orphan_old).await;

        // Orphaned batch at ts=3000 (also older than oldest manifest entry)
        let orphan_mid = batch_path_from_ts(3000);
        write_batch_file(&store, &orphan_mid).await;

        let now = SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000);
        gc.collect_once(now).await.unwrap();

        assert!(!file_exists(&store, &orphan_old).await);
        assert!(!file_exists(&store, &orphan_mid).await);
        assert!(file_exists(&store, &referenced).await);
    }

    #[test]
    fn should_extract_ulid_timestamp() {
        let ts_ms = 1_700_000_000_000u64;
        let ulid = ulid::Ulid::from_parts(ts_ms, 0);
        let path = format!("ingest/{}.batch", ulid);

        let result = GarbageCollector::extract_ulid_timestamp(&path);
        assert!(result.is_some());

        let extracted = result.unwrap();
        let expected = SystemTime::UNIX_EPOCH + Duration::from_millis(ts_ms);
        assert_eq!(extracted, expected);
    }

    #[test]
    fn should_return_none_for_invalid_ulid_path() {
        assert!(GarbageCollector::extract_ulid_timestamp("ingest/not-a-ulid.batch").is_none());
        assert!(GarbageCollector::extract_ulid_timestamp("ingest/file.txt").is_none());
        assert!(GarbageCollector::extract_ulid_timestamp("").is_none());
    }

    #[tokio::test]
    async fn should_shutdown_collect_loop_on_cancel() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = CollectorConfig {
            gc_interval: Duration::from_secs(3600), // very long so it won't fire
            ..make_config()
        };
        let gc = make_gc(&store, config);

        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(gc.collect(shutdown_clone));

        // Cancel immediately
        shutdown.cancel();

        // The collect loop should exit promptly
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("collect loop did not shut down in time")
            .expect("collect task panicked");
    }
}

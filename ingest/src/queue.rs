use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use common::clock::Clock;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectStore, PutMode, PutPayload, UpdateVersion,
};

use crate::error::{Error, Result};
use crate::queue_config::QueueConfig;

fn millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as i64
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ProducerManifest {
    pending: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ConsumerManifest {
    claimed: HashMap<String, i64>,
    done: Vec<String>,
}

enum ManifestWriteError {
    Conflict,
    Fatal(Error),
}

#[derive(Clone)]
struct ManifestStore {
    object_store: Arc<dyn ObjectStore>,
    manifest_path: String,
}

impl ManifestStore {
    async fn read<T>(&self) -> Result<(T, Option<UpdateVersion>)>
    where
        T: serde::de::DeserializeOwned + Default,
    {
        let path = Path::from(self.manifest_path.as_str());
        match self.object_store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let manifest: T = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok((manifest, Some(version)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok((T::default(), None)),
            Err(e) => Err(Error::Storage(e.to_string())),
        }
    }

    async fn write<T>(
        &self,
        manifest: &T,
        version: Option<UpdateVersion>,
    ) -> std::result::Result<(), ManifestWriteError>
    where
        T: serde::Serialize,
    {
        let path = Path::from(self.manifest_path.as_str());
        let json = serde_json::to_vec(manifest)
            .map_err(|e| ManifestWriteError::Fatal(Error::Serialization(e.to_string())))?;

        let put_mode = match version {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };

        match self
            .object_store
            .put_opts(&path, PutPayload::from(json), put_mode.into())
            .await
        {
            Ok(_) => Ok(()),
            Err(ObjectStoreError::Precondition { .. })
            | Err(ObjectStoreError::AlreadyExists { .. }) => Err(ManifestWriteError::Conflict),
            Err(e) => Err(ManifestWriteError::Fatal(Error::Storage(e.to_string()))),
        }
    }
}

struct ConflictCounter {
    write_count: AtomicU64,
    conflict_count: AtomicU64,
}

impl ConflictCounter {
    fn new() -> Self {
        Self {
            write_count: AtomicU64::new(0),
            conflict_count: AtomicU64::new(0),
        }
    }

    fn record_write(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_conflict(&self) {
        self.conflict_count.fetch_add(1, Ordering::Relaxed);
    }

    fn conflict_rate(&self) -> f64 {
        let writes = self.write_count.load(Ordering::Relaxed);
        if writes == 0 {
            return 0.0;
        }
        let conflicts = self.conflict_count.load(Ordering::Relaxed);
        (conflicts as f64 / writes as f64) * 100.0
    }
}

pub struct QueueProducer {
    manifest_store: ManifestStore,
    counter: ConflictCounter,
}

impl QueueProducer {
    pub fn new(config: QueueConfig) -> Result<Self> {
        let object_store = common::storage::factory::create_object_store(&config.object_store)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store)
    }

    pub fn with_object_store(
        config: QueueConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        Ok(Self {
            manifest_store: ManifestStore {
                object_store,
                manifest_path: config.manifest_path,
            },
            counter: ConflictCounter::new(),
        })
    }

    pub async fn enqueue(&self, location: String) -> Result<()> {
        loop {
            let (mut manifest, version): (ProducerManifest, _) =
                self.manifest_store.read().await?;
            manifest.pending.push(location.clone());
            self.counter.record_write();
            match self.manifest_store.write(&manifest, version).await {
                Ok(()) => return Ok(()),
                Err(ManifestWriteError::Conflict) => {
                    self.counter.record_conflict();
                    continue;
                }
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub fn conflict_rate(&self) -> f64 {
        self.counter.conflict_rate()
    }
}

pub struct QueueConsumer {
    producer_store: ManifestStore,
    consumer_store: ManifestStore,
    heartbeat_timeout_ms: i64,
    done_cleanup_threshold: usize,
    clock: Arc<dyn Clock>,
    counter: ConflictCounter,
}

fn consumer_manifest_path(producer_path: &str) -> String {
    match producer_path.rsplit_once('.') {
        Some((base, ext)) => format!("{}.consumer.{}", base, ext),
        None => format!("{}.consumer", producer_path),
    }
}

impl QueueConsumer {
    pub fn new(config: QueueConfig, clock: Arc<dyn Clock>) -> Result<Self> {
        let object_store = common::storage::factory::create_object_store(&config.object_store)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store, clock)
    }

    pub fn with_object_store(
        config: QueueConfig,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let consumer_path = consumer_manifest_path(&config.manifest_path);
        Ok(Self {
            producer_store: ManifestStore {
                object_store: object_store.clone(),
                manifest_path: config.manifest_path,
            },
            consumer_store: ManifestStore {
                object_store,
                manifest_path: consumer_path,
            },
            heartbeat_timeout_ms: config.heartbeat_timeout_ms,
            done_cleanup_threshold: config.done_cleanup_threshold,
            clock,
            counter: ConflictCounter::new(),
        })
    }

    pub async fn claim(&self) -> Result<Option<String>> {
        loop {
            let (producer, _): (ProducerManifest, _) = self.producer_store.read().await?;
            let (mut consumer, version): (ConsumerManifest, _) =
                self.consumer_store.read().await?;
            let now = millis(self.clock.now());

            // Try to re-claim a stale location first
            let cutoff = now - self.heartbeat_timeout_ms;
            let stale = consumer
                .claimed
                .iter()
                .filter(|(_, ts)| **ts < cutoff)
                .min_by_key(|(_, ts)| **ts)
                .map(|(loc, _)| loc.clone());

            let location = if let Some(loc) = stale {
                loc
            } else {
                // Find first pending location not already claimed or done
                match producer.pending.iter().find(|loc| {
                    !consumer.claimed.contains_key(loc.as_str())
                        && !consumer.done.contains(loc)
                }) {
                    Some(loc) => loc.clone(),
                    None => return Ok(None),
                }
            };

            consumer.claimed.insert(location.clone(), now);
            match self.consumer_store.write(&consumer, version).await {
                Ok(()) => return Ok(Some(location)),
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub async fn dequeue(&self, location: &str) -> Result<bool> {
        loop {
            let (mut consumer, version): (ConsumerManifest, _) =
                self.consumer_store.read().await?;
            if consumer.claimed.remove(location).is_none() {
                return Ok(false);
            }
            consumer.done.push(location.to_string());
            match self.consumer_store.write(&consumer, version).await {
                Ok(()) => break,
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }

        // Check if we should cleanup done items
        let (consumer, _): (ConsumerManifest, _) = self.consumer_store.read().await?;
        if consumer.done.len() >= self.done_cleanup_threshold {
            self.cleanup_done().await?;
        }

        Ok(true)
    }

    async fn cleanup_done(&self) -> Result<()> {
        // Read the current done list
        let (consumer, _): (ConsumerManifest, _) = self.consumer_store.read().await?;
        let done_set: std::collections::HashSet<&str> =
            consumer.done.iter().map(|s| s.as_str()).collect();

        // CAS-loop: remove done locations from producer pending
        loop {
            let (mut producer, version): (ProducerManifest, _) =
                self.producer_store.read().await?;
            producer.pending.retain(|loc| !done_set.contains(loc.as_str()));
            self.counter.record_write();
            match self.producer_store.write(&producer, version).await {
                Ok(()) => break,
                Err(ManifestWriteError::Conflict) => {
                    self.counter.record_conflict();
                    continue;
                }
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }

        // CAS-loop: clear the flushed locations from consumer done
        loop {
            let (mut consumer, version): (ConsumerManifest, _) =
                self.consumer_store.read().await?;
            consumer.done.retain(|loc| !done_set.contains(loc.as_str()));
            match self.consumer_store.write(&consumer, version).await {
                Ok(()) => break,
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }

        Ok(())
    }

    pub async fn heartbeat(&self, locations: &[String]) -> Result<()> {
        loop {
            let (mut consumer, version): (ConsumerManifest, _) =
                self.consumer_store.read().await?;
            let now = millis(self.clock.now());
            for location in locations {
                if consumer.claimed.contains_key(location) {
                    consumer.claimed.insert(location.clone(), now);
                }
            }
            match self.consumer_store.write(&consumer, version).await {
                Ok(()) => return Ok(()),
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub async fn len(&self) -> Result<usize> {
        let (producer, _): (ProducerManifest, _) = self.producer_store.read().await?;
        Ok(producer.pending.len())
    }

    pub fn conflict_rate(&self) -> f64 {
        self.counter.conflict_rate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::clock::SystemClock;
    use common::storage::config::ObjectStoreConfig;
    use slatedb::object_store::memory::InMemory;

    fn test_config() -> QueueConfig {
        QueueConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: "test/manifest.json".to_string(),
            heartbeat_timeout_ms: 30_000,
            done_cleanup_threshold: 100,
        }
    }

    async fn read_producer_manifest(store: &Arc<dyn ObjectStore>, path: &str) -> ProducerManifest {
        let path = Path::from(path);
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn read_consumer_manifest(store: &Arc<dyn ObjectStore>, path: &str) -> ConsumerManifest {
        let path = Path::from(path);
        match store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.unwrap();
                serde_json::from_slice(&bytes).unwrap()
            }
            Err(ObjectStoreError::NotFound { .. }) => ConsumerManifest::default(),
            Err(e) => panic!("unexpected error: {}", e),
        }
    }

    #[tokio::test]
    async fn should_enqueue_locations_to_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(test_config(), store.clone()).unwrap();

        producer.enqueue("path/to/file1.json".to_string()).await.unwrap();
        producer.enqueue("path/to/file2.json".to_string()).await.unwrap();

        let manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert_eq!(manifest.pending, vec!["path/to/file1.json", "path/to/file2.json"]);
    }

    #[tokio::test]
    async fn should_merge_with_existing_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Pre-populate manifest
        let existing = ProducerManifest {
            pending: vec!["existing/file.json".to_string()],
        };
        let json = serde_json::to_vec(&existing).unwrap();
        let path = Path::from("test/manifest.json");
        store.put(&path, PutPayload::from(json)).await.unwrap();

        let producer =
            QueueProducer::with_object_store(test_config(), store.clone()).unwrap();
        producer.enqueue("new/file.json".to_string()).await.unwrap();

        let manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert_eq!(
            manifest.pending,
            vec!["existing/file.json", "new/file.json"]
        );
    }

    #[tokio::test]
    async fn should_claim_earliest_pending_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = test_config();
        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("first.json".to_string()).await.unwrap();
        producer.enqueue("second.json".to_string()).await.unwrap();

        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, Some("first.json".to_string()));

        // Producer manifest still has both pending items (read-only from consumer)
        let producer_manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert_eq!(producer_manifest.pending, vec!["first.json", "second.json"]);

        // Consumer manifest tracks the claim
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(consumer_manifest.claimed.contains_key("first.json"));
    }

    #[tokio::test]
    async fn should_return_none_when_nothing_to_claim() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer =
            QueueConsumer::with_object_store(test_config(), store.clone(), Arc::new(SystemClock)).unwrap();

        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, None);
    }

    #[tokio::test]
    async fn should_dequeue_claimed_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = test_config();
        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("to_process.json".to_string()).await.unwrap();
        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, Some("to_process.json".to_string()));

        let removed = consumer.dequeue("to_process.json").await.unwrap();
        assert!(removed);

        // Producer manifest still has the pending item
        let producer_manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert_eq!(producer_manifest.pending, vec!["to_process.json"]);

        // Consumer manifest: claimed is empty, done has the location
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(consumer_manifest.claimed.is_empty());
        assert_eq!(consumer_manifest.done, vec!["to_process.json"]);
    }

    #[tokio::test]
    async fn should_return_false_when_dequeuing_unknown_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer =
            QueueConsumer::with_object_store(test_config(), store.clone(), Arc::new(SystemClock)).unwrap();

        let removed = consumer.dequeue("unknown.json").await.unwrap();
        assert!(!removed);
    }

    #[tokio::test]
    async fn should_handle_concurrent_claims() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = test_config();
        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();

        let n = 5;
        for i in 0..n {
            producer
                .enqueue(format!("file_{}.json", i))
                .await
                .unwrap();
        }

        let mut join_handles = Vec::new();
        for _ in 0..n {
            let consumer =
                QueueConsumer::with_object_store(config.clone(), store.clone(), Arc::new(SystemClock)).unwrap();
            join_handles.push(tokio::spawn(async move {
                consumer.claim().await.unwrap().unwrap()
            }));
        }

        let mut claimed_locations: Vec<String> = Vec::new();
        for jh in join_handles {
            claimed_locations.push(jh.await.unwrap());
        }

        claimed_locations.sort();
        claimed_locations.dedup();
        assert_eq!(claimed_locations.len(), n);

        // Producer manifest still has all pending items
        let producer_manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert_eq!(producer_manifest.pending.len(), n);

        // Consumer manifest has all claimed
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert_eq!(consumer_manifest.claimed.len(), n);
    }

    #[tokio::test]
    async fn should_heartbeat_claimed_locations() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = test_config();
        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        producer.enqueue("b.json".to_string()).await.unwrap();
        consumer.claim().await.unwrap();
        consumer.claim().await.unwrap();

        let before = millis(SystemTime::now());
        consumer
            .heartbeat(&["a.json".to_string(), "b.json".to_string()])
            .await
            .unwrap();

        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(*consumer_manifest.claimed.get("a.json").unwrap() >= before);
        assert!(*consumer_manifest.claimed.get("b.json").unwrap() >= before);
    }

    #[tokio::test]
    async fn should_overwrite_heartbeat_timestamps() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = test_config();
        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        consumer.claim().await.unwrap();

        consumer
            .heartbeat(&["a.json".to_string()])
            .await
            .unwrap();
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        let ts1 = *consumer_manifest.claimed.get("a.json").unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        consumer
            .heartbeat(&["a.json".to_string()])
            .await
            .unwrap();
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        let ts2 = *consumer_manifest.claimed.get("a.json").unwrap();

        assert!(ts2 > ts1);
    }

    #[tokio::test]
    async fn should_skip_unknown_locations_in_heartbeat() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = test_config();
        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        consumer.claim().await.unwrap();

        consumer
            .heartbeat(&["unknown.json".to_string()])
            .await
            .unwrap();

        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(!consumer_manifest.claimed.contains_key("unknown.json"));
        assert_eq!(consumer_manifest.claimed.len(), 1);
    }

    #[tokio::test]
    async fn should_reclaim_stale_location_when_pending_is_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut config = test_config();
        config.heartbeat_timeout_ms = 50;

        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer1 =
            QueueConsumer::with_object_store(config.clone(), store.clone(), Arc::new(SystemClock)).unwrap();
        let consumer2 =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        let claimed = consumer1.claim().await.unwrap();
        assert_eq!(claimed, Some("a.json".to_string()));

        // consumer2 shares the same consumer manifest, so it sees the claim
        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, None);

        // ToDo: use mock clock
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, Some("a.json".to_string()));

        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert_eq!(consumer_manifest.claimed.len(), 1);
        assert!(consumer_manifest.claimed.contains_key("a.json"));
    }

    #[tokio::test]
    async fn should_not_reclaim_location_with_fresh_heartbeat() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut config = test_config();
        config.heartbeat_timeout_ms = 200;

        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer1 =
            QueueConsumer::with_object_store(config.clone(), store.clone(), Arc::new(SystemClock)).unwrap();
        let consumer2 =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        consumer1.claim().await.unwrap();

        // Heartbeat keeps it fresh
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        consumer1
            .heartbeat(&["a.json".to_string()])
            .await
            .unwrap();

        // consumer2 cannot reclaim — heartbeat is fresh
        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, None);
    }

    #[tokio::test]
    async fn should_reclaim_oldest_stale_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut config = test_config();
        config.heartbeat_timeout_ms = 50;

        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config.clone(), store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("old.json".to_string()).await.unwrap();
        consumer.claim().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        producer.enqueue("newer.json".to_string()).await.unwrap();
        consumer.claim().await.unwrap();

        // Wait for both to become stale
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let reclaimer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();
        let claimed = reclaimer.claim().await.unwrap();
        assert_eq!(claimed, Some("old.json".to_string()));
    }

    #[tokio::test]
    async fn should_prefer_stale_claimed_over_pending() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut config = test_config();
        config.heartbeat_timeout_ms = 50;

        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer1 =
            QueueConsumer::with_object_store(config.clone(), store.clone(), Arc::new(SystemClock)).unwrap();
        let consumer2 =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("claimed.json".to_string()).await.unwrap();
        consumer1.claim().await.unwrap();

        // Wait for heartbeat to expire
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        // Enqueue a new pending item
        producer.enqueue("pending.json".to_string()).await.unwrap();

        // consumer2 should prefer the stale claimed item
        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, Some("claimed.json".to_string()));
    }

    #[tokio::test]
    async fn should_accumulate_done_in_consumer_manifest_after_dequeue() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = test_config();
        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        producer.enqueue("b.json".to_string()).await.unwrap();
        consumer.claim().await.unwrap();
        consumer.claim().await.unwrap();

        consumer.dequeue("a.json").await.unwrap();
        consumer.dequeue("b.json").await.unwrap();

        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(consumer_manifest.claimed.is_empty());
        assert_eq!(consumer_manifest.done.len(), 2);
        assert!(consumer_manifest.done.contains(&"a.json".to_string()));
        assert!(consumer_manifest.done.contains(&"b.json".to_string()));

        // Producer manifest still has both in pending (no flush yet)
        let producer_manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert_eq!(producer_manifest.pending.len(), 2);
    }

    #[tokio::test]
    async fn should_cleanup_done_removes_from_producer_when_threshold_reached() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut config = test_config();
        config.done_cleanup_threshold = 2;

        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        producer.enqueue("b.json".to_string()).await.unwrap();
        producer.enqueue("c.json".to_string()).await.unwrap();

        consumer.claim().await.unwrap();
        consumer.claim().await.unwrap();

        // First dequeue — done has 1 item, below threshold
        consumer.dequeue("a.json").await.unwrap();
        let producer_manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert_eq!(producer_manifest.pending.len(), 3); // still all three

        // Second dequeue — done reaches threshold (2), triggers cleanup
        consumer.dequeue("b.json").await.unwrap();
        let producer_manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert_eq!(producer_manifest.pending, vec!["c.json"]);
    }

    #[tokio::test]
    async fn should_cleanup_done_clears_done_list_in_consumer_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut config = test_config();
        config.done_cleanup_threshold = 2;

        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        producer.enqueue("b.json".to_string()).await.unwrap();

        consumer.claim().await.unwrap();
        consumer.claim().await.unwrap();
        consumer.dequeue("a.json").await.unwrap();
        consumer.dequeue("b.json").await.unwrap();

        // After cleanup, consumer done list should be empty
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(consumer_manifest.done.is_empty());

        // Producer pending should also be empty
        let producer_manifest = read_producer_manifest(&store, "test/manifest.json").await;
        assert!(producer_manifest.pending.is_empty());
    }

    #[tokio::test]
    async fn should_not_reclaim_done_locations() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = test_config();
        let producer =
            QueueProducer::with_object_store(config.clone(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        producer.enqueue("a.json".to_string()).await.unwrap();
        consumer.claim().await.unwrap();
        consumer.dequeue("a.json").await.unwrap();

        // "a.json" is still in producer pending but in consumer done — should not be claimed again
        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, None);
    }
}

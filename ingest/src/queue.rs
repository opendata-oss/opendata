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
struct Manifest {
    pending: Vec<String>,
    claimed: HashMap<String, i64>,
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
    async fn read(&self) -> Result<(Manifest, Option<UpdateVersion>)> {
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
                let manifest: Manifest = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok((manifest, Some(version)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok((Manifest::default(), None)),
            Err(e) => Err(Error::Storage(e.to_string())),
        }
    }

    async fn write(
        &self,
        manifest: &Manifest,
        version: Option<UpdateVersion>,
    ) -> std::result::Result<(), ManifestWriteError> {
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
            let (mut manifest, version) = self.manifest_store.read().await?;
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
    manifest_store: ManifestStore,
    heartbeat_timeout_ms: i64,
    clock: Arc<dyn Clock>,
    counter: ConflictCounter,
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
        Ok(Self {
            manifest_store: ManifestStore {
                object_store,
                manifest_path: config.manifest_path,
            },
            heartbeat_timeout_ms: config.heartbeat_timeout_ms,
            clock,
            counter: ConflictCounter::new(),
        })
    }

    pub async fn claim(&self) -> Result<Option<String>> {
        loop {
            let (mut manifest, version) = self.manifest_store.read().await?;
            let now = millis(self.clock.now());

            let cutoff = now - self.heartbeat_timeout_ms;
            let stale = manifest
                .claimed
                .iter()
                .filter(|(_, ts)| **ts < cutoff)
                .min_by_key(|(_, ts)| **ts)
                .map(|(loc, _)| loc.clone());

            let location = if let Some(loc) = stale {
                loc
            } else if !manifest.pending.is_empty() {
                manifest.pending.remove(0)
            } else {
                return Ok(None);
            };

            manifest.claimed.insert(location.clone(), now);
            self.counter.record_write();
            match self.manifest_store.write(&manifest, version).await {
                Ok(()) => return Ok(Some(location)),
                Err(ManifestWriteError::Conflict) => {
                    self.counter.record_conflict();
                    continue;
                }
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub async fn dequeue(&self, location: &str) -> Result<bool> {
        loop {
            let (mut manifest, version) = self.manifest_store.read().await?;
            if manifest.claimed.remove(location).is_none() {
                return Ok(false);
            }
            match self.manifest_store.write(&manifest, version).await {
                Ok(()) => return Ok(true),
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub async fn heartbeat(&self, locations: &[String]) -> Result<()> {
        loop {
            let (mut manifest, version) = self.manifest_store.read().await?;
            let now = millis(self.clock.now());
            for location in locations {
                if manifest.claimed.contains_key(location) {
                    manifest.claimed.insert(location.clone(), now);
                }
            }
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

    pub async fn len(&self) -> Result<usize> {
        let (manifest, _) = self.manifest_store.read().await?;
        Ok(manifest.pending.len() + manifest.claimed.len())
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
        }
    }

    async fn read_manifest(store: &Arc<dyn ObjectStore>, path: &str) -> Manifest {
        let path = Path::from(path);
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn should_enqueue_locations_to_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(test_config(), store.clone()).unwrap();

        producer.enqueue("path/to/file1.json".to_string()).await.unwrap();
        producer.enqueue("path/to/file2.json".to_string()).await.unwrap();

        let manifest = read_manifest(&store, "test/manifest.json").await;
        assert_eq!(manifest.pending, vec!["path/to/file1.json", "path/to/file2.json"]);
        assert!(manifest.claimed.is_empty());
    }

    #[tokio::test]
    async fn should_merge_with_existing_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Pre-populate manifest
        let existing = Manifest {
            pending: vec!["existing/file.json".to_string()],
            claimed: HashMap::new(),
        };
        let json = serde_json::to_vec(&existing).unwrap();
        let path = Path::from("test/manifest.json");
        store.put(&path, PutPayload::from(json)).await.unwrap();

        let producer =
            QueueProducer::with_object_store(test_config(), store.clone()).unwrap();
        producer.enqueue("new/file.json".to_string()).await.unwrap();

        let manifest = read_manifest(&store, "test/manifest.json").await;
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

        let manifest = read_manifest(&store, "test/manifest.json").await;
        assert_eq!(manifest.pending, vec!["second.json"]);
        assert!(manifest.claimed.contains_key("first.json"));
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

        let manifest = read_manifest(&store, "test/manifest.json").await;
        assert!(manifest.pending.is_empty());
        assert!(manifest.claimed.is_empty());
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

        let manifest = read_manifest(&store, "test/manifest.json").await;
        assert!(manifest.pending.is_empty());
        assert_eq!(manifest.claimed.len(), n);
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

        let manifest = read_manifest(&store, "test/manifest.json").await;
        assert!(*manifest.claimed.get("a.json").unwrap() >= before);
        assert!(*manifest.claimed.get("b.json").unwrap() >= before);
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
        let manifest = read_manifest(&store, "test/manifest.json").await;
        let ts1 = *manifest.claimed.get("a.json").unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        consumer
            .heartbeat(&["a.json".to_string()])
            .await
            .unwrap();
        let manifest = read_manifest(&store, "test/manifest.json").await;
        let ts2 = *manifest.claimed.get("a.json").unwrap();

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

        let manifest = read_manifest(&store, "test/manifest.json").await;
        assert!(!manifest.claimed.contains_key("unknown.json"));
        assert_eq!(manifest.claimed.len(), 1);
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

        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, None);

        // ToDo: use mock clock
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, Some("a.json".to_string()));

        let manifest = read_manifest(&store, "test/manifest.json").await;
        assert!(manifest.pending.is_empty());
        assert_eq!(manifest.claimed.len(), 1);
        assert!(manifest.claimed.contains_key("a.json"));
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
}

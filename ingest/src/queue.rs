use std::sync::Arc;

use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectStore, PutMode, PutPayload, UpdateVersion,
};

use crate::error::{Error, Result};
use crate::queue_config::QueueConfig;

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct Manifest {
    pending: Vec<String>,
    claimed: Vec<String>,
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

pub struct QueueProducer {
    manifest_store: ManifestStore,
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
        })
    }

    pub async fn enqueue(&self, location: String) -> Result<()> {
        loop {
            let (mut manifest, version) = self.manifest_store.read().await?;
            manifest.pending.push(location.clone());
            match self.manifest_store.write(&manifest, version).await {
                Ok(()) => return Ok(()),
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }
}

pub struct QueueConsumer {
    manifest_store: ManifestStore,
}

impl QueueConsumer {
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
        })
    }

    pub async fn claim(&self) -> Result<Option<String>> {
        loop {
            let (mut manifest, version) = self.manifest_store.read().await?;
            if manifest.pending.is_empty() {
                return Ok(None);
            }
            let location = manifest.pending.remove(0);
            manifest.claimed.push(location.clone());
            match self.manifest_store.write(&manifest, version).await {
                Ok(()) => return Ok(Some(location)),
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub async fn dequeue(&self, location: &str) -> Result<bool> {
        loop {
            let (mut manifest, version) = self.manifest_store.read().await?;
            let Some(pos) = manifest.claimed.iter().position(|l| l == location) else {
                return Ok(false);
            };
            manifest.claimed.swap_remove(pos);
            match self.manifest_store.write(&manifest, version).await {
                Ok(()) => return Ok(true),
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::storage::config::ObjectStoreConfig;
    use slatedb::object_store::memory::InMemory;

    fn test_config() -> QueueConfig {
        QueueConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: "test/manifest.json".to_string(),
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
            claimed: vec![],
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
            QueueConsumer::with_object_store(config, store.clone()).unwrap();

        producer.enqueue("first.json".to_string()).await.unwrap();
        producer.enqueue("second.json".to_string()).await.unwrap();

        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, Some("first.json".to_string()));

        let manifest = read_manifest(&store, "test/manifest.json").await;
        assert_eq!(manifest.pending, vec!["second.json"]);
        assert_eq!(manifest.claimed, vec!["first.json"]);
    }

    #[tokio::test]
    async fn should_return_none_when_nothing_to_claim() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer =
            QueueConsumer::with_object_store(test_config(), store.clone()).unwrap();

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
            QueueConsumer::with_object_store(config, store.clone()).unwrap();

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
            QueueConsumer::with_object_store(test_config(), store.clone()).unwrap();

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
                QueueConsumer::with_object_store(config.clone(), store.clone()).unwrap();
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
}

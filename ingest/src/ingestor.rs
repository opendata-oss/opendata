use std::sync::Arc;

use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, PutPayload};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::KeyValueEntry;
use crate::queue::QueueProducer;

pub struct Ingestor {
    object_store: Arc<dyn ObjectStore>,
    producer: QueueProducer,
    path_prefix: String,
}

impl Ingestor {
    pub fn new(config: Config, producer: QueueProducer) -> Result<Self> {
        let object_store = common::storage::factory::create_object_store(&config.object_store)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store, producer)
    }

    pub fn with_object_store(
        config: Config,
        object_store: Arc<dyn ObjectStore>,
        producer: QueueProducer,
    ) -> Result<Self> {
        Ok(Self {
            object_store,
            producer,
            path_prefix: config.path_prefix,
        })
    }

    pub async fn ingest(&self, entries: Vec<KeyValueEntry>) -> Result<()> {
        let json = serde_json::to_vec(&entries)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let id = uuid::Uuid::new_v4();
        let path = Path::from(format!("{}/{}.json", self.path_prefix, id));

        self.object_store
            .put(&path, PutPayload::from(json))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        self.producer.enqueue(path.to_string()).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::queue_config::QueueConfig;
    use bytes::Bytes;
    use common::storage::config::ObjectStoreConfig;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;

    fn test_config() -> Config {
        Config {
            object_store: ObjectStoreConfig::InMemory,
            path_prefix: "test-ingest".to_string(),
        }
    }

    fn test_queue_config() -> QueueConfig {
        QueueConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: "test/manifest.json".to_string(),
        }
    }

    #[tokio::test]
    async fn should_ingest_entries_and_enqueue_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(test_queue_config(), store.clone()).unwrap();
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), producer).unwrap();

        let entries = vec![
            KeyValueEntry {
                key: "key1".to_string(),
                value: Bytes::from("value1"),
            },
            KeyValueEntry {
                key: "key2".to_string(),
                value: Bytes::from("value2"),
            },
        ];

        ingestor.ingest(entries).await.unwrap();

        // Verify manifest has the enqueued location
        let path = slatedb::object_store::path::Path::from("test/manifest.json");
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let pending = manifest["pending"].as_array().unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].as_str().unwrap().starts_with("test-ingest/"));
        assert!(pending[0].as_str().unwrap().ends_with(".json"));
    }

    #[tokio::test]
    async fn should_write_valid_json_to_object_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(test_queue_config(), store.clone()).unwrap();
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), producer).unwrap();

        let entries = vec![KeyValueEntry {
            key: "mykey".to_string(),
            value: Bytes::from("myvalue"),
        }];

        ingestor.ingest(entries).await.unwrap();

        // Read back the location from the manifest
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        let data = store
            .get(&manifest_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let location = manifest["pending"][0].as_str().unwrap();

        let path = Path::from(location);
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let parsed: Vec<KeyValueEntry> = serde_json::from_slice(&data).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "mykey");
        assert_eq!(parsed[0].value, Bytes::from("myvalue"));
    }
}

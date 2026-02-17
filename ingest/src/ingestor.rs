use std::sync::Arc;
use std::time::Duration;

use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, PutPayload};
use tokio::sync::Mutex;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::KeyValueEntry;
use crate::queue::QueueProducer;

struct BatchState {
    entries: Vec<KeyValueEntry>,
    size_bytes: usize,
    started_at: Option<tokio::time::Instant>,
}

pub struct Ingestor {
    object_store: Arc<dyn ObjectStore>,
    producer: QueueProducer,
    path_prefix: String,
    batch: Mutex<BatchState>,
    batch_interval: Duration,
    batch_max_bytes: usize,
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
            batch: Mutex::new(BatchState {
                entries: Vec::new(),
                size_bytes: 0,
                started_at: None,
            }),
            batch_interval: Duration::from_millis(config.batch_interval_ms),
            batch_max_bytes: config.batch_max_bytes,
        })
    }

    pub async fn ingest(&self, entries: Vec<KeyValueEntry>) -> Result<()> {
        let incoming_size: usize = entries
            .iter()
            .map(|e| e.key.len() + e.value.len())
            .sum();

        let flush_entries = {
            let mut batch = self.batch.lock().await;
            batch.entries.extend(entries);
            batch.size_bytes += incoming_size;
            if batch.started_at.is_none() {
                batch.started_at = Some(tokio::time::Instant::now());
            }

            let should_flush = batch.size_bytes >= self.batch_max_bytes
                || batch
                    .started_at
                    .map(|s| s.elapsed() >= self.batch_interval)
                    .unwrap_or(false);

            if should_flush {
                let entries = std::mem::take(&mut batch.entries);
                batch.size_bytes = 0;
                batch.started_at = None;
                Some(entries)
            } else {
                None
            }
        };

        if let Some(entries) = flush_entries {
            self.write_batch(entries).await?;
        }

        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        let entries = {
            let mut batch = self.batch.lock().await;
            if batch.entries.is_empty() {
                return Ok(());
            }
            let entries = std::mem::take(&mut batch.entries);
            batch.size_bytes = 0;
            batch.started_at = None;
            entries
        };

        self.write_batch(entries).await
    }

    async fn write_batch(&self, entries: Vec<KeyValueEntry>) -> Result<()> {
        let json =
            serde_json::to_vec(&entries).map_err(|e| Error::Serialization(e.to_string()))?;

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
            batch_interval_ms: 60_000,
            batch_max_bytes: 64 * 1024 * 1024,
        }
    }

    fn test_queue_config() -> QueueConfig {
        QueueConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: "test/manifest.json".to_string(),
            heartbeat_timeout_ms: 30_000,
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
        ingestor.flush().await.unwrap();

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
        ingestor.flush().await.unwrap();

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

    #[tokio::test]
    async fn should_flush_when_batch_size_exceeded() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(test_queue_config(), store.clone()).unwrap();

        let mut config = test_config();
        config.batch_max_bytes = 10; // very small threshold

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), producer).unwrap();

        let entries = vec![KeyValueEntry {
            key: "a-long-key".to_string(),
            value: Bytes::from("a-long-value"),
        }];

        // This single ingest exceeds the 10-byte threshold, so it should auto-flush
        ingestor.ingest(entries).await.unwrap();

        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        let data = store
            .get(&manifest_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let pending = manifest["pending"].as_array().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn should_flush_when_interval_elapsed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(test_queue_config(), store.clone()).unwrap();

        let mut config = test_config();
        config.batch_interval_ms = 50;
        config.batch_max_bytes = 64 * 1024 * 1024;

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), producer).unwrap();

        // First ingest — starts the timer, but doesn't flush
        ingestor
            .ingest(vec![KeyValueEntry {
                key: "k1".to_string(),
                value: Bytes::from("v1"),
            }])
            .await
            .unwrap();

        // Nothing written yet
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        assert!(store.get(&manifest_path).await.is_err());

        // Sleep past the interval
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Second ingest triggers the time check
        ingestor
            .ingest(vec![KeyValueEntry {
                key: "k2".to_string(),
                value: Bytes::from("v2"),
            }])
            .await
            .unwrap();

        let data = store
            .get(&manifest_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let pending = manifest["pending"].as_array().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn should_not_flush_below_thresholds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(test_queue_config(), store.clone()).unwrap();
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), producer).unwrap();

        ingestor
            .ingest(vec![KeyValueEntry {
                key: "k".to_string(),
                value: Bytes::from("v"),
            }])
            .await
            .unwrap();

        // Nothing written to object store yet (below thresholds)
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        assert!(store.get(&manifest_path).await.is_err());

        // Explicit flush writes it
        ingestor.flush().await.unwrap();

        let data = store
            .get(&manifest_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let pending = manifest["pending"].as_array().unwrap();
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn should_batch_multiple_ingests_into_single_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(test_queue_config(), store.clone()).unwrap();
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), producer).unwrap();

        ingestor
            .ingest(vec![KeyValueEntry {
                key: "k1".to_string(),
                value: Bytes::from("v1"),
            }])
            .await
            .unwrap();

        ingestor
            .ingest(vec![KeyValueEntry {
                key: "k2".to_string(),
                value: Bytes::from("v2"),
            }])
            .await
            .unwrap();

        ingestor.flush().await.unwrap();

        // Only one file enqueued
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        let data = store
            .get(&manifest_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let pending = manifest["pending"].as_array().unwrap();
        assert_eq!(pending.len(), 1);

        // The single file contains both entries
        let location = pending[0].as_str().unwrap();
        let path = Path::from(location);
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let parsed: Vec<KeyValueEntry> = serde_json::from_slice(&data).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].key, "k1");
        assert_eq!(parsed[1].key, "k2");
    }
}

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::clock::Clock;
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStore;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::model::KeyValueEntry;
use crate::queue::QueueConsumer;
use crate::queue_config::ConsumerConfig;

pub struct CollectedBatch {
    pub entries: Vec<KeyValueEntry>,
    location: String,
}

impl CollectedBatch {
    pub fn location(&self) -> &str {
        &self.location
    }
}

pub struct Collector {
    consumer: Arc<QueueConsumer>,
    object_store: Arc<dyn ObjectStore>,
    in_flight: Arc<Mutex<HashSet<String>>>,
    shutdown: CancellationToken,
    _heartbeat_handle: tokio::task::JoinHandle<()>,
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl Collector {
    pub fn new(config: ConsumerConfig, clock: Arc<dyn Clock>) -> Result<Self> {
        let object_store = common::storage::factory::create_object_store(&config.object_store)
            .map_err(|e| Error::Storage(e.to_string()))?;
        let heartbeat_interval = Duration::from_millis(config.heartbeat_timeout_ms as u64 / 3);
        let consumer = QueueConsumer::new(config, clock)?;
        Ok(Self::build(consumer, object_store, heartbeat_interval))
    }

    pub fn with_object_store(
        consumer: QueueConsumer,
        object_store: Arc<dyn ObjectStore>,
        heartbeat_interval: Duration,
    ) -> Self {
        Self::build(consumer, object_store, heartbeat_interval)
    }

    fn build(
        consumer: QueueConsumer,
        object_store: Arc<dyn ObjectStore>,
        heartbeat_interval: Duration,
    ) -> Self {
        let consumer = Arc::new(consumer);
        let in_flight = Arc::new(Mutex::new(HashSet::new()));
        let shutdown = CancellationToken::new();

        let heartbeat_handle = {
            let consumer = Arc::clone(&consumer);
            let in_flight = Arc::clone(&in_flight);
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                Self::heartbeat_loop(consumer, in_flight, heartbeat_interval, shutdown).await;
            })
        };

        Self {
            consumer,
            object_store,
            in_flight,
            shutdown,
            _heartbeat_handle: heartbeat_handle,
        }
    }

    async fn heartbeat_loop(
        consumer: Arc<QueueConsumer>,
        in_flight: Arc<Mutex<HashSet<String>>>,
        interval: Duration,
        shutdown: CancellationToken,
    ) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(interval) => {
                    let locations: Vec<String> =
                        in_flight.lock().unwrap().iter().cloned().collect();
                    if !locations.is_empty() {
                        let _ = consumer.heartbeat(&locations).await;
                    }
                }
            }
        }
    }

    pub async fn next_batch(&self) -> Result<Option<CollectedBatch>> {
        let location = match self.consumer.claim().await? {
            Some(loc) => loc,
            None => return Ok(None),
        };

        let path = Path::from(location.as_str());
        let data = self
            .object_store
            .get(&path)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let entries: Vec<KeyValueEntry> =
            serde_json::from_slice(&data).map_err(|e| Error::Serialization(e.to_string()))?;

        self.in_flight.lock().unwrap().insert(location.clone());

        Ok(Some(CollectedBatch { entries, location }))
    }

    pub async fn ack(&self, batch: &CollectedBatch) -> Result<()> {
        self.in_flight.lock().unwrap().remove(&batch.location);
        let dequeued = self.consumer.dequeue(&batch.location).await?;
        if !dequeued {
            return Err(Error::Storage(format!(
                "location not claimed: {}",
                batch.location
            )));
        }
        Ok(())
    }

    pub async fn len(&self) -> Result<usize> {
        self.consumer.len().await
    }

    pub fn conflict_rate(&self) -> f64 {
        self.consumer.conflict_rate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::QueueProducer;
    use crate::queue_config::{ConsumerConfig, ProducerConfig};
    use bytes::Bytes;
    use common::clock::SystemClock;
    use common::storage::config::ObjectStoreConfig;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::PutPayload;

    fn test_producer_config() -> ProducerConfig {
        ProducerConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: "test/manifest.json".to_string(),
        }
    }

    fn test_consumer_config() -> ConsumerConfig {
        ConsumerConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: "test/manifest.json".to_string(),
            heartbeat_timeout_ms: 30_000,
            done_cleanup_threshold: 100,
        }
    }

    fn test_entries() -> Vec<KeyValueEntry> {
        vec![
            KeyValueEntry {
                key: Bytes::from("key1"),
                value: Bytes::from("value1"),
            },
            KeyValueEntry {
                key: Bytes::from("key2"),
                value: Bytes::from("value2"),
            },
        ]
    }

    async fn write_batch(
        store: &Arc<dyn ObjectStore>,
        location: &str,
        entries: &[KeyValueEntry],
    ) {
        let json = serde_json::to_vec(entries).unwrap();
        let path = Path::from(location);
        store.put(&path, PutPayload::from(json)).await.unwrap();
    }

    fn make_collector(
        store: &Arc<dyn ObjectStore>,
        consumer_config: ConsumerConfig,
        heartbeat_interval: Duration,
    ) -> (QueueProducer, Collector) {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let producer =
            QueueProducer::with_object_store(test_producer_config(), store.clone()).unwrap();
        let consumer =
            QueueConsumer::with_object_store(consumer_config, store.clone(), clock).unwrap();
        let collector = Collector::with_object_store(consumer, store.clone(), heartbeat_interval);
        (producer, collector)
    }

    #[tokio::test]
    async fn should_collect_enqueued_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) =
            make_collector(&store, test_consumer_config(), Duration::from_secs(10));

        let entries = test_entries();
        let location = "batches/batch-001.json";
        write_batch(&store, location, &entries).await;
        producer.enqueue(location.to_string()).await.unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.entries.len(), 2);
        assert_eq!(batch.entries[0].key, Bytes::from("key1"));
        assert_eq!(batch.entries[0].value, Bytes::from("value1"));
        assert_eq!(batch.entries[1].key, Bytes::from("key2"));
        assert_eq!(batch.entries[1].value, Bytes::from("value2"));
        assert_eq!(batch.location(), location);
    }

    #[tokio::test]
    async fn should_return_none_when_queue_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_producer, collector) =
            make_collector(&store, test_consumer_config(), Duration::from_secs(10));

        let result = collector.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_ack_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) =
            make_collector(&store, test_consumer_config(), Duration::from_secs(10));

        let entries = test_entries();
        let location = "batches/batch-002.json";
        write_batch(&store, location, &entries).await;
        producer.enqueue(location.to_string()).await.unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        collector.ack(&batch).await.unwrap();

        // After ack, re-claim returns None (location moved to done)
        let next = collector.next_batch().await.unwrap();
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn should_heartbeat_in_flight_batches() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let heartbeat_interval = Duration::from_millis(20);
        let (producer, collector) =
            make_collector(&store, test_consumer_config(), heartbeat_interval);

        let entries = test_entries();
        let location = "batches/batch-003.json";
        write_batch(&store, location, &entries).await;
        producer.enqueue(location.to_string()).await.unwrap();

        let _batch = collector.next_batch().await.unwrap().unwrap();

        // Read initial claim timestamp
        let consumer_manifest_path = Path::from("test/manifest.consumer.json");
        let data = store
            .get(&consumer_manifest_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let ts_before = manifest["claimed"][location].as_i64().unwrap();

        // Wait for the background heartbeat to fire
        tokio::time::sleep(Duration::from_millis(50)).await;

        let data = store
            .get(&consumer_manifest_path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let ts_after = manifest["claimed"][location].as_i64().unwrap();

        assert!(ts_after > ts_before);
    }

    #[tokio::test]
    async fn should_stop_heartbeating_after_ack() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let heartbeat_interval = Duration::from_millis(20);
        let (producer, collector) =
            make_collector(&store, test_consumer_config(), heartbeat_interval);

        let entries = test_entries();
        let location = "batches/batch-004.json";
        write_batch(&store, location, &entries).await;
        producer.enqueue(location.to_string()).await.unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        collector.ack(&batch).await.unwrap();

        assert!(collector.in_flight.lock().unwrap().is_empty());
    }
}

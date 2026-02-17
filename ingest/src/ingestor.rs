use std::sync::Arc;
use std::time::{Duration, SystemTime};

use common::clock::Clock;
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, PutPayload};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::KeyValueEntry;
use crate::queue::QueueProducer;

type Notifier = tokio::sync::watch::Sender<Option<Result<()>>>;

#[derive(Clone)]
pub struct WriteWatcher {
    rx: tokio::sync::watch::Receiver<Option<Result<()>>>,
}

impl WriteWatcher {
    pub fn result(&self) -> Option<Result<()>> {
        self.rx.borrow().clone()
    }

    pub async fn await_durable(&mut self) -> Result<()> {
        self.rx
            .wait_for(|v| v.is_some())
            .await
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?
            .clone()
            .expect("value must be present after wait_for")
    }
}

enum IngestMessage {
    Write {
        entries: Vec<KeyValueEntry>,
        notifier: Notifier,
    },
    Flush {
        done: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

struct Batch {
    entries: Vec<KeyValueEntry>,
    notifiers: Vec<Notifier>,
    size_bytes: usize,
    started_at: Option<SystemTime>,
}

impl Batch {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            notifiers: Vec::new(),
            size_bytes: 0,
            started_at: None,
        }
    }

    fn add(&mut self, entries: Vec<KeyValueEntry>, notifier: Notifier, now: SystemTime) {
        let incoming_size: usize = entries.iter().map(|e| e.key.len() + e.value.len()).sum();
        self.entries.extend(entries);
        self.size_bytes += incoming_size;
        self.notifiers.push(notifier);
        if self.started_at.is_none() {
            self.started_at = Some(now);
        }
    }

    fn take(&mut self) -> (Vec<KeyValueEntry>, Vec<Notifier>) {
        self.size_bytes = 0;
        self.started_at = None;
        (
            std::mem::take(&mut self.entries),
            std::mem::take(&mut self.notifiers),
        )
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

struct BatchWriter {
    object_store: Arc<dyn ObjectStore>,
    producer: QueueProducer,
    path_prefix: String,
    batch_interval: Duration,
    batch_max_bytes: usize,
    batch: Batch,
    clock: Arc<dyn Clock>,
}

impl BatchWriter {
    async fn run(
        &mut self,
        mut rx: mpsc::UnboundedReceiver<IngestMessage>,
        shutdown: CancellationToken,
    ) {
        loop {
            let sleep_duration = match self.batch.started_at {
                Some(started) => (started + self.batch_interval)
                    .duration_since(self.clock.now())
                    .unwrap_or(Duration::ZERO),
                None => self.batch_interval,
            };

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    self.close(&mut rx).await;
                    return;
                },
                msg = rx.recv() => {
                    match msg {
                        Some(IngestMessage::Write { entries, notifier }) => {
                            self.batch.add(entries, notifier, self.clock.now());
                            if self.batch.size_bytes >= self.batch_max_bytes {
                                let _ = self.write_batch().await;
                            }
                        }
                        Some(IngestMessage::Flush { done }) => {
                            let _ = done.send(self.write_batch().await);
                        }
                        None => break,
                    }
                },
                _ = tokio::time::sleep(sleep_duration) => {
                    let _ = self.write_batch().await;
                },
            }
        }
    }

    async fn write_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let (entries, notifiers) = self.batch.take();
        let result = self.write_and_enqueue(entries).await;

        for tx in notifiers {
            let _ = tx.send(Some(result.clone()));
        }

        result
    }

    async fn write_and_enqueue(&self, entries: Vec<KeyValueEntry>) -> Result<()> {
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

    async fn close(&mut self, rx: &mut mpsc::UnboundedReceiver<IngestMessage>) {
        let mut flush_responders = Vec::new();

        while let Ok(msg) = rx.try_recv() {
            match msg {
                IngestMessage::Write { entries, notifier } => {
                    self.batch.add(entries, notifier, self.clock.now());
                }
                IngestMessage::Flush { done } => {
                    flush_responders.push(done);
                }
            }
        }

        let result = self.write_batch().await;

        for done in flush_responders {
            let _ = done.send(result.clone());
        }
    }
}

pub struct Ingestor {
    tx: mpsc::UnboundedSender<IngestMessage>,
    cancellation_token: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl Ingestor {
    pub fn new(config: Config, producer: QueueProducer, clock: Arc<dyn Clock>) -> Result<Self> {
        let object_store = common::storage::factory::create_object_store(&config.object_store)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store, producer, clock)
    }

    pub fn with_object_store(
        config: Config,
        object_store: Arc<dyn ObjectStore>,
        producer: QueueProducer,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();

        let mut writer = BatchWriter {
            object_store,
            producer,
            path_prefix: config.path_prefix,
            batch_interval: Duration::from_millis(config.batch_interval_ms),
            batch_max_bytes: config.batch_max_bytes,
            batch: Batch::new(),
            clock,
        };
        let cancellation_token = shutdown.clone();
        let handle = tokio::spawn(async move { writer.run(rx, shutdown).await });

        Ok(Self { tx, cancellation_token, handle })
    }

    pub fn ingest(&self, entries: Vec<KeyValueEntry>) -> Result<WriteWatcher> {
        let (notifier_tx, notifier_rx) = tokio::sync::watch::channel(None);
        self.tx
            .send(IngestMessage::Write {
                entries,
                notifier: notifier_tx,
            })
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?;
        Ok(WriteWatcher { rx: notifier_rx })
    }

    pub async fn flush(&self) -> Result<()> {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(IngestMessage::Flush { done: done_tx })
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?;
        done_rx
            .await
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?
    }

    pub async fn close(self) -> Result<()> {
        self.cancellation_token.cancel();
        let _ = self.handle.await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::queue_config::QueueConfig;
    use bytes::Bytes;
    use common::clock::SystemClock;
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
            Ingestor::with_object_store(test_config(), store.clone(), producer, Arc::new(SystemClock)).unwrap();

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

        ingestor.ingest(entries).unwrap();
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
            Ingestor::with_object_store(test_config(), store.clone(), producer, Arc::new(SystemClock)).unwrap();

        let entries = vec![KeyValueEntry {
            key: "mykey".to_string(),
            value: Bytes::from("myvalue"),
        }];

        ingestor.ingest(entries).unwrap();
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
            Ingestor::with_object_store(config, store.clone(), producer, Arc::new(SystemClock)).unwrap();

        let entries = vec![KeyValueEntry {
            key: "a-long-key".to_string(),
            value: Bytes::from("a-long-value"),
        }];

        // This single ingest exceeds the 10-byte threshold, so the background
        // task should auto-flush without an explicit flush() call.
        let mut watcher = ingestor.ingest(entries).unwrap();
        watcher.await_durable().await.unwrap();

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
            Ingestor::with_object_store(config, store.clone(), producer, Arc::new(SystemClock)).unwrap();

        let mut watcher = ingestor
            .ingest(vec![KeyValueEntry {
                key: "k1".to_string(),
                value: Bytes::from("v1"),
            }])
            .unwrap();

        // Nothing written yet
        assert!(watcher.result().is_none());
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        assert!(store.get(&manifest_path).await.is_err());

        // Background task should flush when the interval elapses — no second ingest needed
        watcher.await_durable().await.unwrap();

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
            Ingestor::with_object_store(test_config(), store.clone(), producer, Arc::new(SystemClock)).unwrap();

        let watcher = ingestor
            .ingest(vec![KeyValueEntry {
                key: "k".to_string(),
                value: Bytes::from("v"),
            }])
            .unwrap();

        // Watcher not yet resolved
        assert!(watcher.result().is_none());

        // Nothing written to object store yet (below thresholds)
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        assert!(store.get(&manifest_path).await.is_err());

        // Explicit flush writes it and notifies the watcher
        ingestor.flush().await.unwrap();

        assert!(watcher.result().unwrap().is_ok());

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
            Ingestor::with_object_store(test_config(), store.clone(), producer, Arc::new(SystemClock)).unwrap();

        let watcher1 = ingestor
            .ingest(vec![KeyValueEntry {
                key: "k1".to_string(),
                value: Bytes::from("v1"),
            }])
            .unwrap();

        let watcher2 = ingestor
            .ingest(vec![KeyValueEntry {
                key: "k2".to_string(),
                value: Bytes::from("v2"),
            }])
            .unwrap();

        ingestor.flush().await.unwrap();

        // Both watchers resolved
        assert!(watcher1.result().unwrap().is_ok());
        assert!(watcher2.result().unwrap().is_ok());

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

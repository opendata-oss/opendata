use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use common::clock::Clock;
use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, PutPayload};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::config::IngestorConfig;
use crate::error::{Error, Result};
use crate::model::encode_batch;
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
        data: Bytes,
        notifier: Notifier,
    },
    Flush {
        done: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

struct Batch {
    entries: Vec<Bytes>,
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

    fn add(&mut self, data: Bytes, notifier: Notifier, now: SystemTime) {
        self.size_bytes += data.len();
        self.entries.push(data);
        self.notifiers.push(notifier);
        if self.started_at.is_none() {
            self.started_at = Some(now);
        }
    }

    fn take(&mut self) -> (Vec<Bytes>, Vec<Notifier>) {
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
    producer: Arc<QueueProducer>,
    path_prefix: String,
    batch_interval: Duration,
    batch_max_bytes: usize,
    batch: Batch,
    clock: Arc<dyn Clock>,
    pending_bytes: Arc<AtomicUsize>,
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
                        Some(IngestMessage::Write { data, notifier }) => {
                            self.batch.add(data, notifier, self.clock.now());
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
        let flushed_bytes = self.batch.size_bytes;
        let (entries, notifiers) = self.batch.take();
        let result = self.write_and_enqueue(entries).await;
        self.pending_bytes
            .fetch_sub(flushed_bytes, Ordering::Release);

        for tx in notifiers {
            let _ = tx.send(Some(result.clone()));
        }

        result
    }

    async fn write_and_enqueue(&self, entries: Vec<Bytes>) -> Result<()> {
        let payload = encode_batch(&entries);

        let id = ulid::Ulid::new();
        let path = Path::from(format!("{}/{}.batch", self.path_prefix, id));

        self.object_store
            .put(&path, PutPayload::from(payload))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        self.producer
            .enqueue(path.to_string(), Bytes::new())
            .await?;

        Ok(())
    }

    async fn close(&mut self, rx: &mut mpsc::UnboundedReceiver<IngestMessage>) {
        let mut flush_responders = Vec::new();

        while let Ok(msg) = rx.try_recv() {
            match msg {
                IngestMessage::Write { data, notifier } => {
                    self.batch.add(data, notifier, self.clock.now());
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
    producer: Arc<QueueProducer>,
    pending_bytes: Arc<AtomicUsize>,
    max_unflushed_bytes: usize,
}

impl Ingestor {
    pub fn new(config: IngestorConfig, clock: Arc<dyn Clock>) -> Result<Self> {
        let object_store =
            common::storage::factory::create_object_store(&config.object_store_config)
                .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store, clock)
    }

    pub fn with_object_store(
        config: IngestorConfig,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let producer = QueueProducer::with_object_store(
            config.manifest_path.clone(),
            object_store.clone(),
            Arc::clone(&clock),
        );
        let (tx, rx) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let producer = Arc::new(producer);
        let pending_bytes = Arc::new(AtomicUsize::new(0));

        let mut writer = BatchWriter {
            object_store,
            producer: Arc::clone(&producer),
            path_prefix: config.data_path_prefix,
            batch_interval: config.flush_interval,
            batch_max_bytes: config.flush_size_bytes,
            batch: Batch::new(),
            clock,
            pending_bytes: Arc::clone(&pending_bytes),
        };
        let cancellation_token = shutdown.clone();
        let handle = tokio::spawn(async move { writer.run(rx, shutdown).await });

        Ok(Self {
            tx,
            cancellation_token,
            handle,
            producer,
            pending_bytes,
            max_unflushed_bytes: config.max_unflushed_bytes,
        })
    }

    pub async fn ingest(&self, data: Bytes) -> Result<WriteWatcher> {
        let incoming_size = data.len();
        self.maybe_apply_backpressure(incoming_size).await?;
        let (notifier_tx, notifier_rx) = tokio::sync::watch::channel(None);
        self.tx
            .send(IngestMessage::Write {
                data,
                notifier: notifier_tx,
            })
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?;
        self.pending_bytes
            .fetch_add(incoming_size, Ordering::Release);
        Ok(WriteWatcher { rx: notifier_rx })
    }

    async fn maybe_apply_backpressure(&self, incoming_size: usize) -> Result<()> {
        let threshold = self.max_unflushed_bytes;
        if incoming_size > threshold {
            return Err(Error::BackpressureLimitExceeded {
                incoming_size,
                limit: threshold,
            });
        }
        if self.pending_bytes.load(Ordering::Acquire) + incoming_size >= threshold {
            self.flush().await?;
        }
        Ok(())
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

    pub fn conflict_rate(&self) -> f64 {
        self.producer.conflict_rate()
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
    use crate::config::IngestorConfig;
    use crate::model::decode_batch;
    use crate::queue::Manifest;
    use bytes::Bytes;
    use common::clock::SystemClock;
    use common::storage::config::ObjectStoreConfig;
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;

    async fn read_manifest_locations(store: &Arc<dyn ObjectStore>, path: &str) -> Vec<String> {
        let path = slatedb::object_store::path::Path::from(path);
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let manifest = Manifest::from_bytes(data).unwrap();
        manifest.iter().map(|e| e.unwrap().location).collect()
    }

    fn test_config() -> IngestorConfig {
        IngestorConfig {
            object_store_config: ObjectStoreConfig::InMemory,
            data_path_prefix: "test-ingest".to_string(),
            manifest_path: "test/manifest.json".to_string(),
            flush_interval: Duration::from_secs(60),
            flush_size_bytes: 64 * 1024 * 1024,
            max_unflushed_bytes: usize::MAX,
        }
    }

    #[tokio::test]
    async fn should_ingest_entries_and_enqueue_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor.ingest(Bytes::from("data1")).await.unwrap();
        ingestor.ingest(Bytes::from("data2")).await.unwrap();
        ingestor.flush().await.unwrap();

        // Verify manifest has the enqueued location
        let locations = read_manifest_locations(&store, "test/manifest.json").await;
        assert_eq!(locations.len(), 1);
        assert!(locations[0].starts_with("test-ingest/"));
        assert!(locations[0].ends_with(".batch"));
    }

    #[tokio::test]
    async fn should_write_valid_batch_to_object_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor.ingest(Bytes::from("mydata")).await.unwrap();
        ingestor.flush().await.unwrap();

        // Read back the location from the manifest
        let locations = read_manifest_locations(&store, "test/manifest.json").await;
        let path = Path::from(locations[0].as_str());
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let parsed = decode_batch(data).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0], Bytes::from("mydata"));
    }

    #[tokio::test]
    async fn should_flush_when_batch_size_exceeded() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.flush_size_bytes = 10; // very small threshold

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        // This single ingest exceeds the 10-byte threshold, so the background
        // task should auto-flush without an explicit flush() call.
        let mut watcher = ingestor
            .ingest(Bytes::from("some-long-data"))
            .await
            .unwrap();
        watcher.await_durable().await.unwrap();

        let locations = read_manifest_locations(&store, "test/manifest.json").await;
        assert_eq!(locations.len(), 1);
    }

    #[tokio::test]
    async fn should_flush_when_interval_elapsed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.flush_interval = Duration::from_millis(50);
        config.flush_size_bytes = 64 * 1024 * 1024;

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        let mut watcher = ingestor.ingest(Bytes::from("v1")).await.unwrap();

        // Nothing written yet
        assert!(watcher.result().is_none());
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        assert!(store.get(&manifest_path).await.is_err());

        // Background task should flush when the interval elapses — no second ingest needed
        watcher.await_durable().await.unwrap();

        let locations = read_manifest_locations(&store, "test/manifest.json").await;
        assert_eq!(locations.len(), 1);
    }

    #[tokio::test]
    async fn should_not_flush_below_thresholds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        let watcher = ingestor.ingest(Bytes::from("v")).await.unwrap();

        // Watcher not yet resolved
        assert!(watcher.result().is_none());

        // Nothing written to object store yet (below thresholds)
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        assert!(store.get(&manifest_path).await.is_err());

        // Explicit flush writes it and notifies the watcher
        ingestor.flush().await.unwrap();

        assert!(watcher.result().unwrap().is_ok());

        let locations = read_manifest_locations(&store, "test/manifest.json").await;
        assert_eq!(locations.len(), 1);
    }

    #[tokio::test]
    async fn should_batch_multiple_ingests_into_single_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        let watcher1 = ingestor.ingest(Bytes::from("data1")).await.unwrap();
        let watcher2 = ingestor.ingest(Bytes::from("data2")).await.unwrap();

        ingestor.flush().await.unwrap();

        // Both watchers resolved
        assert!(watcher1.result().unwrap().is_ok());
        assert!(watcher2.result().unwrap().is_ok());

        // Only one file enqueued
        let locations = read_manifest_locations(&store, "test/manifest.json").await;
        assert_eq!(locations.len(), 1);

        // The single file contains both entries
        let path = Path::from(locations[0].as_str());
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let parsed = decode_batch(data).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], Bytes::from("data1"));
        assert_eq!(parsed[1], Bytes::from("data2"));
    }

    #[tokio::test]
    async fn should_apply_backpressure_when_threshold_exceeded() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.flush_size_bytes = 64 * 1024 * 1024; // large, won't auto-flush
        config.flush_interval = Duration::from_secs(60); // large, won't auto-flush
        config.max_unflushed_bytes = 30; // threshold larger than one entry but smaller than two

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        // First ingest (22 bytes) — below 30-byte threshold
        ingestor
            .ingest(Bytes::from("abcdefghijklmnopqrstuv"))
            .await
            .unwrap();

        // Nothing flushed yet
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest.json");
        assert!(store.get(&manifest_path).await.is_err());

        // Second ingest (22 bytes) — pending(22) + incoming(22) = 44 >= 30, triggers backpressure flush
        ingestor
            .ingest(Bytes::from("abcdefghijklmnopqrstuv"))
            .await
            .unwrap();

        // First batch was flushed by backpressure without an explicit flush() call
        let locations = read_manifest_locations(&store, "test/manifest.json").await;
        assert_eq!(locations.len(), 1);
    }

    #[tokio::test]
    async fn should_reject_ingest_exceeding_backpressure_limit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.max_unflushed_bytes = 10;

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        // 22 bytes > 10-byte limit
        let result = ingestor.ingest(Bytes::from("abcdefghijklmnopqrstuv")).await;

        assert!(matches!(
            result,
            Err(Error::BackpressureLimitExceeded {
                incoming_size: 22,
                limit: 10
            })
        ));
    }
}

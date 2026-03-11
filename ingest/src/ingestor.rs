use std::sync::Arc;
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
use crate::util::millis;

/// A single entry to be ingested, pairing opaque data with optional metadata.
pub struct IngestEntry {
    pub data: Bytes,
    pub metadata: Bytes,
}

type Notifier = tokio::sync::watch::Sender<Option<Result<()>>>;

#[derive(Clone)]
pub struct DurabilityWatcher {
    rx: tokio::sync::watch::Receiver<Option<Result<()>>>,
}

impl DurabilityWatcher {
    /// Return the outcome of the write if the batch has already been flushed,
    /// or `None` if the flush has not completed yet.
    pub fn result(&self) -> Option<Result<()>> {
        self.rx.borrow().clone()
    }

    /// Wait until the batch containing this write has been durably flushed.
    pub async fn await_durable(&mut self) -> Result<()> {
        self.rx
            .wait_for(|v| v.is_some())
            .await
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?
            .clone()
            .expect("value must be present after wait_for")
    }
}

pub struct WriteHandle {
    pub watcher: DurabilityWatcher,
}

enum IngestMessage {
    Write {
        entries: Vec<IngestEntry>,
        ingestion_time_ms: i64,
        notifier: Notifier,
    },
    Flush {
        result_sender: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

struct Batch {
    entries: Vec<Bytes>,
    metadata: Vec<Bytes>,
    ingestion_time_ms: i64,
    notifiers: Vec<Notifier>,
    size_bytes: usize,
    started_at: Option<SystemTime>,
}

impl Batch {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            metadata: Vec::new(),
            ingestion_time_ms: 0,
            notifiers: Vec::new(),
            size_bytes: 0,
            started_at: None,
        }
    }

    fn add(
        &mut self,
        entries: Vec<IngestEntry>,
        ingestion_time_ms: i64,
        notifier: Notifier,
        now: SystemTime,
    ) {
        for entry in entries {
            self.size_bytes += entry.data.len();
            self.entries.push(entry.data);
            self.metadata.push(entry.metadata);
        }
        if self.ingestion_time_ms == 0 {
            self.ingestion_time_ms = ingestion_time_ms;
        }
        self.notifiers.push(notifier);
        if self.started_at.is_none() {
            self.started_at = Some(now);
        }
    }

    fn take(&mut self) -> (Vec<Bytes>, Vec<Bytes>, i64, Vec<Notifier>) {
        self.size_bytes = 0;
        self.started_at = None;
        (
            std::mem::take(&mut self.entries),
            std::mem::take(&mut self.metadata),
            std::mem::replace(&mut self.ingestion_time_ms, 0),
            std::mem::take(&mut self.notifiers),
        )
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.metadata.iter().all(|m| m.is_empty())
    }
}

struct BatchWriter {
    object_store: Arc<dyn ObjectStore>,
    producer: Arc<QueueProducer>,
    path_prefix: String,
    flush_interval: Duration,
    flush_size_bytes: usize,
    batch: Batch,
    clock: Arc<dyn Clock>,
}

impl BatchWriter {
    async fn run(&mut self, mut rx: mpsc::Receiver<IngestMessage>, shutdown: CancellationToken) {
        loop {
            let sleep_duration = match self.batch.started_at {
                Some(started) => (started + self.flush_interval)
                    .duration_since(self.clock.now())
                    .unwrap_or(Duration::ZERO),
                None => self.flush_interval,
            };

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    self.close(&mut rx).await;
                    return;
                },
                msg = rx.recv() => {
                    match msg {
                        Some(IngestMessage::Write { entries, ingestion_time_ms, notifier }) => {
                            self.batch.add(entries, ingestion_time_ms, notifier, self.clock.now());
                            if self.batch.size_bytes >= self.flush_size_bytes {
                                let _ = self.write_batch().await;
                            }
                        }
                        Some(IngestMessage::Flush { result_sender }) => {
                            let _ = result_sender.send(self.write_batch().await);
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
        let (entries, metadata, ingestion_time_ms, notifiers) = self.batch.take();
        let result = self
            .write_and_enqueue(entries, metadata, ingestion_time_ms)
            .await;

        for tx in notifiers {
            let _ = tx.send(Some(result.clone()));
        }

        result
    }

    async fn write_and_enqueue(
        &self,
        entries: Vec<Bytes>,
        metadata: Vec<Bytes>,
        ingestion_time_ms: i64,
    ) -> Result<()> {
        let non_empty: Vec<Bytes> = entries.into_iter().filter(|e| !e.is_empty()).collect();
        let location = if non_empty.is_empty() {
            String::new()
        } else {
            let payload = encode_batch(&non_empty);
            let id = ulid::Ulid::new();
            let path = Path::from(format!("{}/{}.batch", self.path_prefix, id));
            self.object_store
                .put(&path, PutPayload::from(payload))
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            path.to_string()
        };

        self.producer
            .enqueue(location, metadata, ingestion_time_ms)
            .await?;

        Ok(())
    }

    async fn close(&mut self, rx: &mut mpsc::Receiver<IngestMessage>) {
        let mut flush_result_senders = Vec::new();

        while let Ok(msg) = rx.try_recv() {
            match msg {
                IngestMessage::Write {
                    entries,
                    ingestion_time_ms,
                    notifier,
                } => {
                    self.batch
                        .add(entries, ingestion_time_ms, notifier, self.clock.now());
                }
                IngestMessage::Flush { result_sender } => {
                    flush_result_senders.push(result_sender);
                }
            }
        }

        let result = self.write_batch().await;

        for result_sender in flush_result_senders {
            let _ = result_sender.send(result.clone());
        }
    }
}

pub struct Ingestor {
    tx: mpsc::Sender<IngestMessage>,
    cancellation_token: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
    producer: Arc<QueueProducer>,
    clock: Arc<dyn Clock>,
}

impl Ingestor {
    /// Create a new ingestor from the given configuration and clock.
    pub fn new(config: IngestorConfig, clock: Arc<dyn Clock>) -> Result<Self> {
        let object_store_config = match &config.storage {
            common::StorageConfig::InMemory => common::storage::config::ObjectStoreConfig::InMemory,
            common::StorageConfig::SlateDb(c) => c.object_store.clone(),
        };
        let object_store = common::storage::factory::create_object_store(&object_store_config)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store, clock)
    }

    fn with_object_store(
        config: IngestorConfig,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let producer =
            QueueProducer::with_object_store(config.manifest_path.clone(), object_store.clone());
        let (tx, rx) = mpsc::channel(config.max_buffered_inputs);
        let shutdown = CancellationToken::new();
        let producer = Arc::new(producer);

        let mut writer = BatchWriter {
            object_store,
            producer: Arc::clone(&producer),
            path_prefix: config.data_path_prefix,
            flush_interval: config.flush_interval,
            flush_size_bytes: config.flush_size_bytes,
            batch: Batch::new(),
            clock: Arc::clone(&clock),
        };
        let cancellation_token = shutdown.clone();
        let handle = tokio::spawn(async move { writer.run(rx, shutdown).await });

        Ok(Self {
            tx,
            cancellation_token,
            handle,
            producer,
            clock,
        })
    }

    /// Submit a set of entries for ingestion. Each entry pairs opaque data with metadata.
    ///
    /// Returns a [`WriteHandle`] that can be used to check or await durability.
    /// Applies backpressure when the message buffer is full.
    pub async fn ingest(&self, entries: Vec<IngestEntry>) -> Result<WriteHandle> {
        let ingestion_time_ms = millis(self.clock.now());
        let (notifier_tx, notifier_rx) = tokio::sync::watch::channel(None);
        self.tx
            .send(IngestMessage::Write {
                entries,
                ingestion_time_ms,
                notifier: notifier_tx,
            })
            .await
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?;
        Ok(WriteHandle {
            watcher: DurabilityWatcher { rx: notifier_rx },
        })
    }

    /// Flush the current batch, blocking until all pending entries are durably written.
    pub async fn flush(&self) -> Result<()> {
        let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
        self.tx
            .send(IngestMessage::Flush { result_sender })
            .await
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?;
        result_receiver
            .await
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?
    }

    /// Return the fraction of manifest writes that encountered optimistic-concurrency conflicts.
    pub fn conflict_rate(&self) -> f64 {
        self.producer.conflict_rate()
    }

    /// Shut down the ingestor, flushing any remaining buffered entries before returning.
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
    use crate::queue::{Manifest, QueueEntry};
    use bytes::Bytes;
    use common::StorageConfig;
    use common::clock::{MockClock, SystemClock};
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use std::time::UNIX_EPOCH;

    async fn read_manifest_entries(store: &Arc<dyn ObjectStore>, path: &str) -> Vec<QueueEntry> {
        let path = slatedb::object_store::path::Path::from(path);
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let manifest = Manifest::from_bytes(data).unwrap();
        manifest.iter().map(|e| e.unwrap()).collect()
    }

    fn ie(data: &str) -> IngestEntry {
        IngestEntry {
            data: Bytes::from(data.to_string()),
            metadata: Bytes::new(),
        }
    }

    fn test_config() -> IngestorConfig {
        IngestorConfig {
            storage: StorageConfig::InMemory,
            data_path_prefix: "test-ingest".to_string(),
            manifest_path: "test/manifest".to_string(),
            flush_interval: Duration::from_hours(24),
            flush_size_bytes: 64 * 1024 * 1024,
            max_buffered_inputs: 1000,
        }
    }

    #[tokio::test]
    async fn should_ingest_entries_and_enqueue_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor.ingest(vec![ie("data1")]).await.unwrap();
        ingestor.ingest(vec![ie("data2")]).await.unwrap();
        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert!(entries[0].location.starts_with("test-ingest/"));
        assert!(entries[0].location.ends_with(".batch"));
    }

    #[tokio::test]
    async fn should_write_valid_batch_to_object_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor.ingest(vec![ie("mydata")]).await.unwrap();
        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        let path = Path::from(entries[0].location.as_str());
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let parsed = decode_batch(data).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0], Bytes::from("mydata"));
    }

    #[tokio::test]
    async fn should_flush_when_batch_size_exceeded() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.flush_size_bytes = 10;

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        let mut watcher = ingestor.ingest(vec![ie("some-long-data")]).await.unwrap();
        watcher.watcher.await_durable().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn should_flush_when_interval_elapsed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.flush_interval = Duration::from_millis(50);
        config.flush_size_bytes = 64 * 1024 * 1024;

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        let mut watcher = ingestor.ingest(vec![ie("v1")]).await.unwrap();

        assert!(watcher.watcher.result().is_none());
        let manifest_path = slatedb::object_store::path::Path::from("test/manifest");
        assert!(store.get(&manifest_path).await.is_err());

        watcher.watcher.await_durable().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn should_not_flush_below_thresholds() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        let watcher = ingestor.ingest(vec![ie("v")]).await.unwrap();

        assert!(watcher.watcher.result().is_none());

        let manifest_path = slatedb::object_store::path::Path::from("test/manifest");
        assert!(store.get(&manifest_path).await.is_err());

        ingestor.flush().await.unwrap();

        assert!(watcher.watcher.result().unwrap().is_ok());

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn should_batch_multiple_ingests_into_single_file() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        let watcher1 = ingestor.ingest(vec![ie("data1")]).await.unwrap();
        let watcher2 = ingestor.ingest(vec![ie("data2")]).await.unwrap();

        ingestor.flush().await.unwrap();

        assert!(watcher1.watcher.result().unwrap().is_ok());
        assert!(watcher2.watcher.result().unwrap().is_ok());

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);

        let path = Path::from(entries[0].location.as_str());
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let parsed = decode_batch(data).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], Bytes::from("data1"));
        assert_eq!(parsed[1], Bytes::from("data2"));
    }

    #[tokio::test]
    async fn should_apply_backpressure_when_buffer_full() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut config = test_config();
        config.max_buffered_inputs = 1;

        let ingestor =
            Ingestor::with_object_store(config, store.clone(), Arc::new(SystemClock)).unwrap();

        // First ingest fills the single-slot buffer
        ingestor.ingest(vec![ie("data1")]).await.unwrap();

        // Second ingest succeeds once the background task consumes the first message
        ingestor.ingest(vec![ie("data2")]).await.unwrap();

        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert!(!entries.is_empty());
    }

    #[tokio::test]
    async fn should_record_metadata_and_ingestion_time_in_queue_entry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let fixed_time = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        let clock = Arc::new(MockClock::with_time(fixed_time));

        let ingestor = Ingestor::with_object_store(test_config(), store.clone(), clock).unwrap();

        let metadata = Bytes::from(r#"{"topic":"events"}"#);
        ingestor
            .ingest(vec![IngestEntry {
                data: Bytes::from("payload"),
                metadata: metadata.clone(),
            }])
            .await
            .unwrap();
        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].metadata, vec![metadata]);
        assert_eq!(entries[0].ingestion_time_ms, 1_700_000_000_000);
    }

    #[tokio::test]
    async fn should_flush_remaining_entries_on_close() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor.ingest(vec![ie("unflushed")]).await.unwrap();

        ingestor.close().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);

        let path = Path::from(entries[0].location.as_str());
        let data = store.get(&path).await.unwrap().bytes().await.unwrap();
        let parsed = decode_batch(data).unwrap();
        assert_eq!(parsed, vec![Bytes::from("unflushed")]);
    }

    #[tokio::test]
    async fn should_produce_separate_batches_per_flush() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor.ingest(vec![ie("batch1")]).await.unwrap();
        ingestor.flush().await.unwrap();

        ingestor.ingest(vec![ie("batch2")]).await.unwrap();
        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 2);
        assert_ne!(entries[0].location, entries[1].location);

        let data1 = store
            .get(&Path::from(entries[0].location.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(decode_batch(data1).unwrap(), vec![Bytes::from("batch1")]);

        let data2 = store
            .get(&Path::from(entries[1].location.as_str()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(decode_batch(data2).unwrap(), vec![Bytes::from("batch2")]);
    }

    #[tokio::test]
    async fn should_enqueue_without_batch_when_data_empty_and_metadata_present() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        let metadata = Bytes::from(r#"{"checkpoint":true}"#);
        ingestor
            .ingest(vec![IngestEntry {
                data: Bytes::new(),
                metadata: metadata.clone(),
            }])
            .await
            .unwrap();
        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert!(entries[0].location.is_empty());
        assert_eq!(entries[0].metadata, vec![metadata]);
    }

    #[tokio::test]
    async fn should_skip_enqueue_when_data_and_metadata_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor.ingest(vec![]).await.unwrap();
        ingestor.flush().await.unwrap();

        let manifest_path = slatedb::object_store::path::Path::from("test/manifest");
        assert!(store.get(&manifest_path).await.is_err());
    }
}

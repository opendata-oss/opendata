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
use crate::queue::{Metadata, QueueProducer};
use crate::util::millis;

type Notifier = tokio::sync::watch::Sender<Option<Result<()>>>;

#[derive(Clone)]
pub struct DurabilityWatcher {
    receiver: tokio::sync::watch::Receiver<Option<Result<()>>>,
}

impl DurabilityWatcher {
    /// Return the outcome of the write if the batch has already been flushed,
    /// or `None` if the flush has not completed yet.
    pub fn result(&self) -> Option<Result<()>> {
        self.receiver.borrow().clone()
    }

    /// Wait until the batch containing this write has been durably flushed.
    pub async fn await_durable(&mut self) -> Result<()> {
        self.receiver
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
    Add {
        entries: Vec<Bytes>,
        metadata: Bytes,
        ingestion_time_ms: i64,
        notifier: Notifier,
    },
    Flush {
        result_sender: tokio::sync::oneshot::Sender<Result<()>>,
    },
}

#[derive(Default)]
struct DataAndNotifiers {
    entries: Vec<Bytes>,
    metadata: Vec<Metadata>,
    notifiers: Vec<Notifier>,
}

impl DataAndNotifiers {
    fn add(
        &mut self,
        entries: Vec<Bytes>,
        metadata: Bytes,
        ingestion_time_ms: i64,
        notifier: Notifier,
    ) -> Result<()> {
        let start_index: u32 = self.entries.len().try_into().map_err(|_| {
            Error::InvalidInput(format!(
                "batch entry count {} exceeds u32::MAX",
                self.entries.len()
            ))
        })?;
        self.entries.extend(entries);
        self.metadata.push(Metadata {
            start_index,
            ingestion_time_ms,
            payload: metadata,
        });
        self.notifiers.push(notifier);
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.metadata.is_empty() && self.notifiers.is_empty()
    }
}

struct Batch {
    data_and_notifiers: DataAndNotifiers,
    size_bytes: usize,
    started_at: Option<SystemTime>,
}

impl Batch {
    fn new() -> Self {
        Self {
            data_and_notifiers: DataAndNotifiers::default(),
            size_bytes: 0,
            started_at: None,
        }
    }

    fn add(
        &mut self,
        entries: Vec<Bytes>,
        metadata: Bytes,
        ingestion_time_ms: i64,
        notifier: Notifier,
        now: SystemTime,
    ) -> Result<()> {
        self.size_bytes += entries.iter().map(|e| e.len()).sum::<usize>() + metadata.len();
        self.data_and_notifiers
            .add(entries, metadata, ingestion_time_ms, notifier)?;
        if self.started_at.is_none() {
            self.started_at = Some(now);
        }
        Ok(())
    }

    fn take(&mut self) -> DataAndNotifiers {
        self.size_bytes = 0;
        self.started_at = None;
        std::mem::take(&mut self.data_and_notifiers)
    }

    fn is_empty(&self) -> bool {
        self.data_and_notifiers.is_empty()
    }
}

struct BatchWriterTask {
    object_store: Arc<dyn ObjectStore>,
    producer: Arc<QueueProducer>,
    data_path_prefix: String,
    flush_interval: Duration,
    flush_size_bytes: usize,
    batch: Batch,
    clock: Arc<dyn Clock>,
}

impl BatchWriterTask {
    fn new(
        object_store: Arc<dyn ObjectStore>,
        producer: Arc<QueueProducer>,
        data_path_prefix: String,
        flush_interval: Duration,
        flush_size_bytes: usize,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            object_store,
            producer,
            data_path_prefix,
            flush_interval,
            flush_size_bytes,
            batch: Batch::new(),
            clock,
        }
    }

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
                    return;
                },
                msg = rx.recv() => {
                    match msg {
                        Some(IngestMessage::Add { entries, metadata, ingestion_time_ms, notifier }) => {
                            if let Err(e) = self.batch.add(entries, metadata, ingestion_time_ms, notifier.clone(), self.clock.now()) {
                                let _ = notifier.send(Some(Err(e)));
                            } else if self.batch.size_bytes >= self.flush_size_bytes {
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
        let data_and_notifiers = self.batch.take();
        let result = self
            .write_and_enqueue(data_and_notifiers.entries, data_and_notifiers.metadata)
            .await;

        for notifier in data_and_notifiers.notifiers {
            let _ = notifier.send(Some(result.clone()));
        }

        result
    }

    async fn write_and_enqueue(&self, entries: Vec<Bytes>, metadata: Vec<Metadata>) -> Result<()> {
        let payload = encode_batch(&entries)?;
        let id = ulid::Ulid::new();
        let path = Path::from(format!("{}/{}.batch", self.data_path_prefix, id));
        self.object_store
            .put(&path, PutPayload::from(payload))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        self.producer.enqueue(path.to_string(), metadata).await?;

        Ok(())
    }
}

struct BatchWriter {
    producer: Arc<QueueProducer>,
    sender: mpsc::Sender<IngestMessage>,
    cancellation_token: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl BatchWriter {
    fn new(
        object_store: Arc<dyn ObjectStore>,
        queue_manifest_path: String,
        data_path_prefix: String,
        flush_interval: Duration,
        flush_size_bytes: usize,
        max_buffered_inputs: usize,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(max_buffered_inputs);
        let producer = Arc::new(QueueProducer::with_object_store(
            queue_manifest_path,
            object_store.clone(),
        ));
        let mut task = BatchWriterTask::new(
            object_store,
            producer.clone(),
            data_path_prefix,
            flush_interval,
            flush_size_bytes,
            clock,
        );
        let shutdown = CancellationToken::new();
        let cancellation_token = shutdown.clone();
        let handle = tokio::spawn(async move { task.run(receiver, shutdown).await });
        Self {
            producer,
            sender,
            cancellation_token,
            handle,
        }
    }

    async fn add(
        &self,
        entries: Vec<Bytes>,
        metadata: Bytes,
        ingestion_time_ms: i64,
    ) -> Result<DurabilityWatcher> {
        let (notifier_sender, notifier_receiver) = tokio::sync::watch::channel(None);
        self.sender
            .send(IngestMessage::Add {
                entries,
                metadata,
                ingestion_time_ms,
                notifier: notifier_sender,
            })
            .await
            .map_err(|_| Error::Storage("ingestor shut down".to_string()))?;
        Ok(DurabilityWatcher {
            receiver: notifier_receiver,
        })
    }

    async fn flush(&self) -> Result<()> {
        let (result_sender, result_receiver) = tokio::sync::oneshot::channel();
        self.sender
            .send(IngestMessage::Flush { result_sender })
            .await
            .map_err(|_| Error::Storage("batch writer task not running".to_string()))?;
        result_receiver
            .await
            .map_err(|_| Error::Storage("batch writer task not running".to_string()))?
    }

    fn conflict_rate(&self) -> f64 {
        self.producer.conflict_rate()
    }
    async fn close(self) -> Result<()> {
        self.flush().await?;
        self.cancellation_token.cancel();
        let _ = self.handle.await;
        Ok(())
    }
}

pub struct Ingestor {
    writer: BatchWriter,
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
        let writer = BatchWriter::new(
            object_store,
            config.manifest_path,
            config.data_path_prefix,
            config.flush_interval,
            config.flush_size_bytes,
            config.max_buffered_inputs,
            clock.clone(),
        );
        Ok(Self { writer, clock })
    }

    /// Submit a set of entries and associated metadata for ingestion.
    ///
    /// Returns a [`WriteHandle`] that can be used to check or await durability.
    /// Applies backpressure when the message buffer is full.
    pub async fn ingest(&self, entries: Vec<Bytes>, metadata: Bytes) -> Result<WriteHandle> {
        if entries.is_empty() {
            return Err(Error::InvalidInput("entries must not be empty".to_string()));
        }
        let ingestion_time_ms = millis(self.clock.now());
        let durability_watcher = self
            .writer
            .add(entries, metadata, ingestion_time_ms)
            .await?;
        Ok(WriteHandle {
            watcher: durability_watcher,
        })
    }

    /// Flush the current batch, blocking until all pending entries are durably written.
    pub async fn flush(&self) -> Result<()> {
        self.writer.flush().await
    }

    /// Return the fraction of manifest writes that encountered optimistic-concurrency conflicts.
    pub fn conflict_rate(&self) -> f64 {
        self.writer.conflict_rate()
    }

    /// Shut down the ingestor, flushing any remaining buffered entries before returning.
    pub async fn close(self) -> Result<()> {
        self.writer.close().await
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

        ingestor
            .ingest(vec![Bytes::from("data1")], Bytes::new())
            .await
            .unwrap();
        ingestor
            .ingest(vec![Bytes::from("data2")], Bytes::new())
            .await
            .unwrap();
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

        ingestor
            .ingest(vec![Bytes::from("mydata")], Bytes::new())
            .await
            .unwrap();
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

        let mut watcher = ingestor
            .ingest(vec![Bytes::from("some-long-data")], Bytes::new())
            .await
            .unwrap();
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

        let mut watcher = ingestor
            .ingest(vec![Bytes::from("v1")], Bytes::new())
            .await
            .unwrap();

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

        let watcher = ingestor
            .ingest(vec![Bytes::from("v")], Bytes::new())
            .await
            .unwrap();

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

        let watcher1 = ingestor
            .ingest(vec![Bytes::from("data1")], Bytes::new())
            .await
            .unwrap();
        let watcher2 = ingestor
            .ingest(vec![Bytes::from("data2")], Bytes::new())
            .await
            .unwrap();

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
        ingestor
            .ingest(vec![Bytes::from("data1")], Bytes::new())
            .await
            .unwrap();

        // Second ingest succeeds once the background task consumes the first message
        ingestor
            .ingest(vec![Bytes::from("data2")], Bytes::new())
            .await
            .unwrap();

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
            .ingest(vec![Bytes::from("payload")], metadata.clone())
            .await
            .unwrap();
        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].metadata.len(), 1);
        assert_eq!(entries[0].metadata[0].payload, metadata);
        assert_eq!(entries[0].metadata[0].ingestion_time_ms, 1_700_000_000_000);
    }

    #[tokio::test]
    async fn should_flush_remaining_entries_on_close() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor
            .ingest(vec![Bytes::from("unflushed")], Bytes::new())
            .await
            .unwrap();

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

        ingestor
            .ingest(vec![Bytes::from("batch1")], Bytes::new())
            .await
            .unwrap();
        ingestor.flush().await.unwrap();

        ingestor
            .ingest(vec![Bytes::from("batch2")], Bytes::new())
            .await
            .unwrap();
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
    async fn should_not_flush_empty_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor.flush().await.unwrap();

        let manifest_path = slatedb::object_store::path::Path::from("test/manifest");
        assert!(store.get(&manifest_path).await.is_err());
    }

    #[tokio::test]
    async fn should_preserve_all_empty_entries_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor
            .ingest(vec![Bytes::new(), Bytes::new()], Bytes::from("meta"))
            .await
            .unwrap();
        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert!(!entries[0].location.is_empty());
        assert_eq!(entries[0].metadata.len(), 1);
        assert_eq!(entries[0].metadata[0].payload, Bytes::from("meta"));
        assert_eq!(entries[0].metadata[0].start_index, 0);

        let data = store
            .get(&Path::from(entries[0].location.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let records = decode_batch(data).unwrap();
        assert_eq!(records, vec![Bytes::new(), Bytes::new()]);
    }

    #[tokio::test]
    async fn should_preserve_empty_and_non_empty_entries_in_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        ingestor
            .ingest(
                vec![
                    Bytes::from("a"),
                    Bytes::new(),
                    Bytes::from("b"),
                    Bytes::new(),
                ],
                Bytes::from("meta"),
            )
            .await
            .unwrap();
        ingestor.flush().await.unwrap();

        let entries = read_manifest_entries(&store, "test/manifest").await;
        assert_eq!(entries.len(), 1);
        assert!(!entries[0].location.is_empty());
        assert_eq!(entries[0].metadata.len(), 1);
        assert_eq!(entries[0].metadata[0].payload, Bytes::from("meta"));
        assert_eq!(entries[0].metadata[0].start_index, 0);

        let data = store
            .get(&Path::from(entries[0].location.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let records = decode_batch(data).unwrap();
        assert_eq!(
            records,
            vec![
                Bytes::from("a"),
                Bytes::new(),
                Bytes::from("b"),
                Bytes::new()
            ]
        );
    }

    #[tokio::test]
    async fn should_reject_empty_entries() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let ingestor =
            Ingestor::with_object_store(test_config(), store.clone(), Arc::new(SystemClock))
                .unwrap();

        let result = ingestor.ingest(vec![], Bytes::new()).await;
        assert!(matches!(result, Err(Error::InvalidInput(_))));

        let result = ingestor.ingest(vec![], Bytes::from("meta")).await;
        assert!(matches!(result, Err(Error::InvalidInput(_))));
    }
}

use std::cell::Cell;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::path::Path;

use crate::config::CollectorConfig;
use crate::error::{Error, Result};
use crate::model::decode_batch;
use crate::queue::{QueueConsumer, QueueEntry};

const DEQUEUE_INTERVAL: u64 = 100;

/// A batch of entries read from object storage by the [`Collector`].
pub struct CollectedBatch {
    /// The deserialized opaque byte entries from the data batch.
    pub entries: Vec<Bytes>,
    /// The queue sequence number of this batch.
    pub sequence: u64,
    /// The object storage path of the data batch.
    pub location: String,
}

/// Reads batches of ingested entries from object storage via a queue consumer.
///
/// The collector iterates over entries in the queue manifest in ingestion order,
/// fetches the corresponding data batches from object storage, and makes them
/// available to the caller. Epoch-based fencing ensures only a single active
/// collector processes entries at any time.
pub struct Collector {
    consumer: QueueConsumer,
    object_store: Arc<dyn ObjectStore>,
    ack_count: Cell<u64>,
    last_acked_sequence: Cell<Option<u64>>,
}

impl Collector {
    /// Create a new collector from the given configuration.
    pub fn new(config: CollectorConfig) -> Result<Self> {
        let object_store_config = match &config.storage {
            common::StorageConfig::InMemory => common::storage::config::ObjectStoreConfig::InMemory,
            common::StorageConfig::SlateDb(c) => c.object_store.clone(),
        };
        let object_store = common::storage::factory::create_object_store(&object_store_config)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self::with_object_store(config, object_store))
    }

    fn with_object_store(config: CollectorConfig, object_store: Arc<dyn ObjectStore>) -> Self {
        let consumer = QueueConsumer::with_object_store(config.manifest_path, object_store.clone());
        Self {
            consumer,
            object_store,
            ack_count: Cell::new(0),
            last_acked_sequence: Cell::new(None),
        }
    }

    /// Initialize the consumer by fencing any previous consumer instance.
    ///
    /// If `last_acked_sequence` is `Some(seq)`, the collector resumes after that
    /// sequence — the next call to [`next_batch`](Self::next_batch) will read
    /// sequence `seq + 1`. If `None`, the collector peeks the queue to discover
    /// the first available entry and positions itself just before it.
    pub async fn initialize(&self, last_acked_sequence: Option<u64>) -> Result<()> {
        self.consumer.initialize().await?;
        if let Some(seq) = last_acked_sequence {
            self.last_acked_sequence.set(Some(seq));
            self.flush().await?;
        }
        Ok(())
    }

    /// Read the next data batch from object storage.
    ///
    /// If no batch has been acked yet, peeks the earliest entry in the queue.
    /// Otherwise, reads the entry with sequence `last_acked_sequence + 1`.
    /// Returns `None` if no matching entry is available.
    pub async fn next_batch(&self) -> Result<Option<CollectedBatch>> {
        let queue_entry = match self.last_acked_sequence.get() {
            Some(seq) => self.consumer.read(seq.wrapping_add(1)).await?,
            None => self.consumer.peek().await?,
        };
        match queue_entry {
            Some(entry) => self.fetch_batch(entry).await,
            None => Ok(None),
        }
    }

    async fn fetch_batch(
        &self,
        queue_entry: crate::queue::QueueEntry,
    ) -> Result<Option<CollectedBatch>> {
        let path = Path::from(queue_entry.location.as_str());
        let data = self
            .object_store
            .get(&path)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let entries = decode_batch(data)?;

        Ok(Some(CollectedBatch {
            entries,
            sequence: queue_entry.sequence,
            location: queue_entry.location,
        }))
    }

    /// Acknowledge that the batch with the given sequence number has been processed.
    ///
    /// Acks must be in order — the sequence must immediately follow the last acked
    /// sequence, otherwise an error is returned. To amortize manifest writes, the
    /// collector only calls `dequeue()` on the queue consumer every
    /// 100 acks.
    pub async fn ack(&self, sequence: u64) -> Result<()> {
        if let Some(last) = self.last_acked_sequence.get()
            && sequence != last + 1
        {
            return Err(Error::Storage(format!(
                "out-of-order ack: expected sequence {}, got {}",
                last + 1,
                sequence
            )));
        }
        self.last_acked_sequence.set(Some(sequence));
        let count = self.ack_count.get() + 1;
        self.ack_count.set(count);
        if count.is_multiple_of(DEQUEUE_INTERVAL) {
            let dequeued = self.consumer.dequeue(sequence).await?;
            delete_dequeued_batches(self.object_store.clone(), dequeued);
        }
        Ok(())
    }

    /// Flush any pending acks by dequeueing up to the last acked sequence.
    pub async fn flush(&self) -> Result<()> {
        if let Some(seq) = self.last_acked_sequence.get() {
            let dequeued = self.consumer.dequeue(seq).await?;
            delete_dequeued_batches(self.object_store.clone(), dequeued);
        }
        Ok(())
    }

    /// Flush pending acks and consume the collector.
    pub async fn close(self) -> Result<()> {
        self.flush().await
    }

    /// Return the number of entries in the queue as of the last manifest read or write.
    pub fn len(&self) -> usize {
        self.consumer.len()
    }

    /// Return `true` if the queue had no entries as of the last manifest read or write.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the percentage of manifest writes that encountered a conflict.
    pub fn conflict_rate(&self) -> f64 {
        self.consumer.conflict_rate()
    }
}

fn delete_dequeued_batches(object_store: Arc<dyn ObjectStore>, entries: Vec<QueueEntry>) {
    if entries.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let locations = stream::iter(entries.iter().map(|e| Ok(Path::from(e.location.as_str()))));
        let mut results = object_store.delete_stream(locations.boxed());
        let mut i = 0;
        while let Some(result) = results.next().await {
            if let Err(e) = result {
                tracing::warn!(
                    path = entries[i].location,
                    error = %e,
                    "failed to delete ingested data batch"
                );
            }
            i += 1;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CollectorConfig;
    use crate::model::encode_batch;
    use crate::queue::QueueProducer;
    use bytes::Bytes;
    use common::StorageConfig;
    use slatedb::object_store::PutPayload;
    use slatedb::object_store::memory::InMemory;

    const TEST_MANIFEST_PATH: &str = "test/manifest";

    fn test_collector_config() -> CollectorConfig {
        CollectorConfig {
            storage: StorageConfig::InMemory,
            manifest_path: TEST_MANIFEST_PATH.to_string(),
        }
    }

    fn test_entries() -> Vec<Bytes> {
        vec![Bytes::from("data1"), Bytes::from("data2")]
    }

    async fn write_batch(store: &Arc<dyn ObjectStore>, location: &str, entries: &[Bytes]) {
        let payload = encode_batch(entries);
        let path = Path::from(location);
        store.put(&path, PutPayload::from(payload)).await.unwrap();
    }

    fn make_collector(
        store: &Arc<dyn ObjectStore>,
        config: CollectorConfig,
    ) -> (QueueProducer, Collector) {
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        let collector = Collector::with_object_store(config, store.clone());
        (producer, collector)
    }

    #[tokio::test]
    async fn should_collect_enqueued_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        let location = "batches/batch-001";
        write_batch(&store, location, &entries).await;
        producer
            .enqueue(location.to_string(), vec![])
            .await
            .unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.entries.len(), 2);
        assert_eq!(batch.entries[0], Bytes::from("data1"));
        assert_eq!(batch.entries[1], Bytes::from("data2"));
        assert_eq!(batch.location, location);
    }

    #[tokio::test]
    async fn should_return_none_when_queue_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let result = collector.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_ack_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        let location = "batches/batch-002";
        write_batch(&store, location, &entries).await;
        producer
            .enqueue(location.to_string(), vec![])
            .await
            .unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        collector.ack(batch.sequence).await.unwrap();
        collector.flush().await.unwrap();

        // After flush, entry is dequeued
        let next = collector.next_batch().await.unwrap();
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn should_next_batch_return_batch_after_last_acked() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        write_batch(&store, "batches/second", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("batches/second".to_string(), vec![])
            .await
            .unwrap();

        let first = collector.next_batch().await.unwrap().unwrap();
        collector.ack(first.sequence).await.unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.location, "batches/second");
        assert_eq!(batch.sequence, 1);
        assert_eq!(batch.entries.len(), 2);
    }

    #[tokio::test]
    async fn should_next_batch_return_none_when_no_more_entries() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();

        let first = collector.next_batch().await.unwrap().unwrap();
        collector.ack(first.sequence).await.unwrap();

        let result = collector.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_resume_from_last_acked_sequence() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(Some(0)).await.unwrap();

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        write_batch(&store, "batches/second", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("batches/second".to_string(), vec![])
            .await
            .unwrap();

        // Initialized with last_acked_sequence=0, so next_batch reads sequence 1
        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.location, "batches/second");
        assert_eq!(batch.sequence, 1);
    }

    #[tokio::test]
    async fn should_reject_out_of_order_ack() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        write_batch(&store, "batches/second", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("batches/second".to_string(), vec![])
            .await
            .unwrap();

        let first = collector.next_batch().await.unwrap().unwrap();
        collector.ack(first.sequence).await.unwrap();

        // Acking sequence 5 when last acked was 0 should fail
        let result = collector.ack(5).await;
        assert!(matches!(result, Err(Error::Storage(msg)) if msg.contains("out-of-order ack")));
    }

    #[tokio::test]
    async fn should_batch_dequeue_calls() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        // Enqueue DEQUEUE_INTERVAL + 1 entries
        let count = DEQUEUE_INTERVAL + 1;
        for i in 0..count {
            let location = format!("batches/batch-{:04}", i);
            write_batch(&store, &location, &entries).await;
            producer.enqueue(location, vec![]).await.unwrap();
        }

        // Ack all entries; dequeue fires at DEQUEUE_INTERVAL
        for i in 0..count {
            let batch = collector.next_batch().await.unwrap().unwrap();
            assert_eq!(batch.sequence, i);
            collector.ack(batch.sequence).await.unwrap();
        }

        // After DEQUEUE_INTERVAL acks, entries up to that point are dequeued.
        // The last entry (index DEQUEUE_INTERVAL) was acked but not yet dequeued.
        assert_eq!(collector.len(), 1);

        // Flush to dequeue the remaining entry.
        collector.flush().await.unwrap();

        let result = collector.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_flush_pending_acks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        write_batch(&store, "batches/second", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("batches/second".to_string(), vec![])
            .await
            .unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        collector.ack(batch.sequence).await.unwrap();

        // Without flush, the entry is still in the manifest.
        // Verify by checking queue length hasn't changed.
        assert_eq!(collector.len(), 2);

        // After flush, dequeue removes the acked entry
        collector.flush().await.unwrap();
        assert_eq!(collector.len(), 1);
    }

    #[tokio::test]
    async fn should_close_flush_and_consume() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        collector.ack(batch.sequence).await.unwrap();
        collector.close().await.unwrap();

        // After close, entries should be dequeued
        let (_, collector2) = make_collector(&store, test_collector_config());
        collector2.initialize(None).await.unwrap();
        let result = collector2.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_fence_previous_collector() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector1) = make_collector(&store, test_collector_config());
        collector1.initialize(None).await.unwrap();

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();

        // Second collector fences the first
        let (_, collector2) = make_collector(&store, test_collector_config());
        collector2.initialize(None).await.unwrap();

        // First collector should get a Fenced error
        let result = collector1.next_batch().await;
        assert!(matches!(result, Err(Error::Fenced)));
    }

    #[tokio::test]
    async fn should_iterate_multiple_sequential_batches() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        let locations = ["batches/a", "batches/b", "batches/c"];
        for loc in &locations {
            write_batch(&store, loc, &entries).await;
            producer.enqueue(loc.to_string(), vec![]).await.unwrap();
        }

        for (i, expected_loc) in locations.iter().enumerate() {
            let batch = collector.next_batch().await.unwrap().unwrap();
            assert_eq!(batch.sequence, i as u64);
            assert_eq!(batch.location, *expected_loc);
            collector.ack(batch.sequence).await.unwrap();
        }

        let result = collector.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_initialize_none_with_pre_existing_entries() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, _) = make_collector(&store, test_collector_config());

        // Enqueue and dequeue placeholder entries to advance the sequence counter
        let entries = test_entries();
        for i in 0..5 {
            let loc = format!("batches/placeholder-{}", i);
            write_batch(&store, &loc, &entries).await;
            producer.enqueue(loc, vec![]).await.unwrap();
        }
        // Use a temporary collector to dequeue placeholders
        let (_, tmp_collector) = make_collector(&store, test_collector_config());
        tmp_collector.initialize(None).await.unwrap();
        for _ in 0..5 {
            let batch = tmp_collector.next_batch().await.unwrap().unwrap();
            tmp_collector.ack(batch.sequence).await.unwrap();
        }
        tmp_collector.close().await.unwrap();

        // Now enqueue the real entry — it gets sequence 5
        write_batch(&store, "batches/pre-existing", &entries).await;
        producer
            .enqueue("batches/pre-existing".to_string(), vec![])
            .await
            .unwrap();

        // New collector with initialize(None) should find this entry
        let (_, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.location, "batches/pre-existing");
        assert_eq!(batch.sequence, 5);
    }

    #[tokio::test]
    async fn should_initialize_with_sequence_dequeue_already_processed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, _) = make_collector(&store, test_collector_config());

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        write_batch(&store, "batches/second", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("batches/second".to_string(), vec![])
            .await
            .unwrap();

        // Simulate restart: new collector resumes after sequence 0
        let (_, collector) = make_collector(&store, test_collector_config());
        collector.initialize(Some(0)).await.unwrap();

        // The flush in initialize should have dequeued entries through sequence 0
        assert_eq!(collector.len(), 1);

        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.location, "batches/second");
        assert_eq!(batch.sequence, 1);
    }

    async fn assert_batch_deleted(store: &Arc<dyn ObjectStore>, location: &str) {
        // Yield to let the spawned deletion task run.
        tokio::task::yield_now().await;
        let path = Path::from(location);
        let result = store.get(&path).await;
        assert!(
            result.is_err(),
            "expected batch file to be deleted: {location}"
        );
    }

    #[tokio::test]
    async fn should_delete_batch_file_after_flush() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        let location = "batches/delete-me";
        write_batch(&store, location, &entries).await;
        producer
            .enqueue(location.to_string(), vec![])
            .await
            .unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        collector.ack(batch.sequence).await.unwrap();
        collector.flush().await.unwrap();

        assert_batch_deleted(&store, location).await;
    }

    #[tokio::test]
    async fn should_delete_batch_files_after_dequeue_interval() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        let mut locations = Vec::new();
        for i in 0..DEQUEUE_INTERVAL {
            let location = format!("batches/interval-{:04}", i);
            write_batch(&store, &location, &entries).await;
            producer.enqueue(location.clone(), vec![]).await.unwrap();
            locations.push(location);
        }

        for _ in 0..DEQUEUE_INTERVAL {
            let batch = collector.next_batch().await.unwrap().unwrap();
            collector.ack(batch.sequence).await.unwrap();
        }

        // The DEQUEUE_INTERVAL-th ack triggers dequeue + deletion.
        for location in &locations {
            assert_batch_deleted(&store, location).await;
        }
    }

    #[tokio::test]
    async fn should_delete_batch_files_on_close() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, collector) = make_collector(&store, test_collector_config());
        collector.initialize(None).await.unwrap();

        let entries = test_entries();
        let location = "batches/close-me";
        write_batch(&store, location, &entries).await;
        producer
            .enqueue(location.to_string(), vec![])
            .await
            .unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        collector.ack(batch.sequence).await.unwrap();
        collector.close().await.unwrap();

        assert_batch_deleted(&store, location).await;
    }
}

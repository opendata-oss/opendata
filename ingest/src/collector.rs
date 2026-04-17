use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::path::Path;
use tokio_util::sync::CancellationToken;

use crate::config::CollectorConfig;
use crate::error::{Error, Result};
use crate::gc::GarbageCollector;
use crate::metric_names as m;
use crate::model::decode_batch;
use crate::queue::{Metadata, QueueConsumer};

const DEQUEUE_INTERVAL: u64 = 100;

/// A batch of entries read from object storage by the [`Collector`].
pub struct CollectedBatch {
    /// The deserialized opaque byte entries from the data batch.
    pub entries: Vec<Bytes>,
    /// The queue sequence number of this batch.
    pub sequence: u64,
    /// The object storage path of the data batch.
    pub location: String,
    /// Metadata ranges attached by the ingestor(s) that contributed to this batch.
    pub metadata: Vec<Metadata>,
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
    gc_shutdown: CancellationToken,
    gc_handle: tokio::task::JoinHandle<()>,
    ack_count: u64,
    last_acked_sequence: Option<u64>,
    last_fetched_sequence: Option<u64>,
}

impl Collector {
    /// Create a new collector from the given configuration.
    ///
    /// Initializes the queue consumer (fencing any previous instance) and spawns
    /// the garbage collector. If `last_acked_sequence` is `Some(seq)`, the
    /// collector resumes after that sequence; if `None`, it discovers the first
    /// available entry.
    pub async fn new(config: CollectorConfig, last_acked_sequence: Option<u64>) -> Result<Self> {
        let object_store = common::storage::factory::create_object_store(&config.object_store)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store, last_acked_sequence).await
    }

    pub async fn with_object_store(
        config: CollectorConfig,
        object_store: Arc<dyn ObjectStore>,
        last_acked_sequence: Option<u64>,
    ) -> Result<Self> {
        crate::metric_names::describe_collector_metrics();
        let consumer =
            QueueConsumer::with_object_store(config.manifest_path.clone(), object_store.clone());

        // Fence previous consumers and position the cursor before spawning GC.
        consumer.initialize().await?;
        if let Some(seq) = last_acked_sequence {
            consumer.dequeue(seq).await?;
        }

        let gc_shutdown = CancellationToken::new();
        let gc = GarbageCollector::new(
            config.manifest_path,
            config.data_path_prefix,
            config.gc_interval,
            config.gc_grace_period,
            object_store.clone(),
        );
        let gc_handle = tokio::spawn(gc.collect(gc_shutdown.clone()));

        Ok(Self {
            consumer,
            object_store,
            ack_count: 0,
            last_acked_sequence,
            gc_shutdown,
            gc_handle,
            last_fetched_sequence: last_acked_sequence,
        })
    }

    /// Read the next data batch from object storage.
    ///
    /// If no batch has been fetched yet, peeks the earliest entry in the queue.
    /// Otherwise, reads the entry with sequence `last_fetched_sequence + 1`.
    /// Returns `None` if no matching entry is available.
    pub async fn next_batch(&mut self) -> Result<Option<CollectedBatch>> {
        let queue_entry = match self.last_fetched_sequence {
            None => self.consumer.peek().await?,
            Some(seq) => self.consumer.read(seq.wrapping_add(1)).await?,
        };
        metrics::gauge!(m::QUEUE_LENGTH).set(self.consumer.len() as f64);
        match queue_entry {
            Some(entry) => {
                let sequence = entry.sequence;
                let batch = self.fetch_batch(entry).await?;
                self.last_fetched_sequence = Some(sequence);
                if let Some(ref b) = batch
                    && let Some(last_meta) = b.metadata.last()
                {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    let lag_s = (now_ms - last_meta.ingestion_time_ms) as f64 / 1000.0;
                    metrics::gauge!(m::COLLECTOR_LAG_SECONDS).set(lag_s.max(0.0));
                }
                Ok(batch)
            }
            None => Ok(None),
        }
    }

    async fn fetch_batch(
        &self,
        queue_entry: crate::queue::QueueEntry,
    ) -> Result<Option<CollectedBatch>> {
        let start = Instant::now();
        let path = Path::from(queue_entry.location.as_str());
        let data = self
            .object_store
            .get(&path)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let data_len = data.len() as u64;
        let entries = decode_batch(data)?;

        metrics::counter!(m::BATCHES_COLLECTED).increment(1);
        metrics::counter!(m::ENTRIES_COLLECTED).increment(entries.len() as u64);
        metrics::counter!(m::BYTES_COLLECTED).increment(data_len);
        metrics::histogram!(m::FETCH_DURATION_SECONDS).record(start.elapsed().as_secs_f64());

        Ok(Some(CollectedBatch {
            entries,
            sequence: queue_entry.sequence,
            location: queue_entry.location,
            metadata: queue_entry.metadata,
        }))
    }

    /// Acknowledge that the batch with the given sequence number has been processed.
    ///
    /// Acks must be in order — the sequence must immediately follow the last acked
    /// sequence, otherwise an error is returned. To amortize manifest writes, the
    /// collector only calls `dequeue()` on the queue consumer every
    /// 100 acks.
    pub async fn ack(&mut self, sequence: u64) -> Result<()> {
        if let Some(last) = self.last_acked_sequence
            && sequence != last + 1
        {
            return Err(Error::Storage(format!(
                "out-of-order ack: expected sequence {}, got {}",
                last + 1,
                sequence
            )));
        }
        self.last_acked_sequence = Some(sequence);
        self.ack_count += 1;
        metrics::counter!(m::ACKS).increment(1);
        if self.ack_count.is_multiple_of(DEQUEUE_INTERVAL) {
            self.consumer.dequeue(sequence).await?;
        }
        Ok(())
    }

    /// Flush any pending acks by dequeueing up to the last acked sequence.
    pub async fn flush(&mut self) -> Result<()> {
        if let Some(seq) = self.last_acked_sequence {
            self.consumer.dequeue(seq).await?;
        }
        Ok(())
    }

    /// Flush pending acks, shut down the garbage collector, and consume the collector.
    pub async fn close(mut self) -> Result<()> {
        self.flush().await?;
        self.gc_shutdown.cancel();
        let _ = self.gc_handle.await;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CollectorConfig;
    use crate::model::{CompressionType, encode_batch};
    use crate::queue::{Metadata, QueueProducer};
    use bytes::Bytes;
    use common::ObjectStoreConfig;
    use slatedb::object_store::PutPayload;
    use slatedb::object_store::memory::InMemory;
    use std::time::Duration;

    const TEST_MANIFEST_PATH: &str = "test/manifest";

    fn test_collector_config() -> CollectorConfig {
        CollectorConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: TEST_MANIFEST_PATH.to_string(),
            data_path_prefix: "ingest".to_string(),
            gc_interval: Duration::from_secs(300),
            gc_grace_period: Duration::from_secs(600),
        }
    }

    fn test_entries() -> Vec<Bytes> {
        vec![Bytes::from("data1"), Bytes::from("data2")]
    }

    async fn write_batch(store: &Arc<dyn ObjectStore>, location: &str, entries: &[Bytes]) {
        let payload = encode_batch(entries, CompressionType::None).unwrap();
        let path = Path::from(location);
        store.put(&path, PutPayload::from(payload)).await.unwrap();
    }

    async fn make_collector(
        store: &Arc<dyn ObjectStore>,
        config: CollectorConfig,
        last_acked_sequence: Option<u64>,
    ) -> (QueueProducer, Collector) {
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        let collector = Collector::with_object_store(config, store.clone(), last_acked_sequence)
            .await
            .unwrap();
        (producer, collector)
    }

    #[tokio::test]
    async fn should_collect_enqueued_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
    async fn should_collect_metadata_from_queue_entry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

        let entries = test_entries();
        let location = "batches/batch-meta";
        write_batch(&store, location, &entries).await;

        let metadata = vec![Metadata {
            start_index: 0,
            ingestion_time_ms: 1_700_000_000_000,
            payload: Bytes::from(r#"{"topic":"events"}"#),
        }];
        producer
            .enqueue(location.to_string(), metadata)
            .await
            .unwrap();

        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.metadata.len(), 1);
        assert_eq!(batch.metadata[0].start_index, 0);
        assert_eq!(batch.metadata[0].ingestion_time_ms, 1_700_000_000_000);
        assert_eq!(
            batch.metadata[0].payload,
            Bytes::from(r#"{"topic":"events"}"#)
        );
    }

    #[tokio::test]
    async fn should_return_none_when_queue_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_producer, mut collector) =
            make_collector(&store, test_collector_config(), None).await;

        let result = collector.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_ack_batch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
    async fn should_next_batch_advance_before_previous_batch_is_acked() {
        // given
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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

        // when
        let first = collector.next_batch().await.unwrap().unwrap();
        let second = collector.next_batch().await.unwrap().unwrap();

        // then
        assert_eq!(first.location, "batches/first");
        assert_eq!(first.sequence, 0);
        assert_eq!(second.location, "batches/second");
        assert_eq!(second.sequence, 1);
    }

    #[tokio::test]
    async fn should_next_batch_return_none_when_no_more_entries() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
        let (producer, mut collector) =
            make_collector(&store, test_collector_config(), Some(0)).await;

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
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
        let (_, mut collector2) = make_collector(&store, test_collector_config(), None).await;
        let result = collector2.next_batch().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_fence_previous_collector() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector1) =
            make_collector(&store, test_collector_config(), None).await;

        let entries = test_entries();
        write_batch(&store, "batches/first", &entries).await;
        producer
            .enqueue("batches/first".to_string(), vec![])
            .await
            .unwrap();

        // Second collector fences the first
        let (_, _collector2) = make_collector(&store, test_collector_config(), None).await;

        // First collector should get a Fenced error
        let result = collector1.next_batch().await;
        assert!(matches!(result, Err(Error::Fenced)));
    }

    #[tokio::test]
    async fn should_iterate_multiple_sequential_batches() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;

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
        let (producer, _) = make_collector(&store, test_collector_config(), None).await;

        // Enqueue and dequeue placeholder entries to advance the sequence counter
        let entries = test_entries();
        for i in 0..5 {
            let loc = format!("batches/placeholder-{}", i);
            write_batch(&store, &loc, &entries).await;
            producer.enqueue(loc, vec![]).await.unwrap();
        }
        // Use a temporary collector to dequeue placeholders
        let (_, mut tmp_collector) = make_collector(&store, test_collector_config(), None).await;
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
        let (_, mut collector) = make_collector(&store, test_collector_config(), None).await;

        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.location, "batches/pre-existing");
        assert_eq!(batch.sequence, 5);
    }

    #[tokio::test]
    async fn should_initialize_with_sequence_dequeue_already_processed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, _) = make_collector(&store, test_collector_config(), None).await;

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
        let (_, mut collector) = make_collector(&store, test_collector_config(), Some(0)).await;

        // The flush in initialize should have dequeued entries through sequence 0
        assert_eq!(collector.len(), 1);

        let batch = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.location, "batches/second");
        assert_eq!(batch.sequence, 1);
    }
}

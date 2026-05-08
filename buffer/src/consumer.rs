use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::path::Path;
use tokio_util::sync::CancellationToken;

use crate::config::ConsumerConfig;
use crate::error::{Error, Result};
use crate::gc::GarbageCollector;
use crate::metric_names as m;
use crate::model::decode_batch;
use crate::queue::{Metadata, QueueConsumer};

const DEQUEUE_INTERVAL: u64 = 100;

/// A batch of entries read from object storage by the [`Consumer`].
pub struct ConsumedBatch {
    /// The deserialized opaque byte entries from the data batch.
    pub entries: Vec<Bytes>,
    /// The queue sequence number of this batch.
    pub sequence: u64,
    /// The object storage path of the data batch.
    pub location: String,
    /// Metadata ranges attached by the buffer(s) that contributed to this batch.
    pub metadata: Vec<Metadata>,
}

/// Lightweight pointer to one manifest entry. Carries everything a
/// caller needs to fetch the corresponding data batch from object
/// storage without re-reading the manifest.
///
/// Returned by [`Consumer::next_descriptors`]. See RFC 0003.
#[derive(Debug, Clone, PartialEq)]
pub struct BatchDescriptor {
    /// The queue sequence number of this batch.
    pub sequence: u64,
    /// The object storage path of the data batch.
    pub location: String,
    /// Per-range metadata items attached to this batch by producers.
    pub metadata: Vec<Metadata>,
    /// Object size in bytes when known. Reserved for the runtime's
    /// byte-budget accounting (see `opendata-contrib` RFC 0002). The
    /// current Buffer manifest format does not carry per-entry object
    /// size, so v1 always emits `None`. A future manifest extension
    /// may populate this; runtime callers handle `None` with a
    /// pessimistic reservation.
    pub object_bytes: Option<u64>,
}

/// Cloneable, concurrency-safe handle for fetching data batches from
/// object storage given a [`BatchDescriptor`].
///
/// The handle holds an `Arc<dyn ObjectStore>` and the manifest path
/// (used as a metric label only). It carries no manifest cursor and
/// no mutable state, so it is safe to clone into many fetch worker
/// tasks while the owning [`Consumer`] keeps `&mut`-only access to
/// manifest operations (`next_descriptors`, `ack`, `ack_through`,
/// `flush`).
///
/// See RFC 0003 ("Concurrency Model") for the full contract.
#[derive(Clone)]
pub struct ConsumerFetchHandle {
    object_store: Arc<dyn ObjectStore>,
    manifest_path: String,
}

impl std::fmt::Debug for ConsumerFetchHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerFetchHandle")
            .field("manifest_path", &self.manifest_path)
            .finish_non_exhaustive()
    }
}

impl ConsumerFetchHandle {
    /// Fetch and decode the batch identified by `descriptor`. Stateless
    /// — does not consult or mutate manifest state. Safe to call
    /// concurrently from multiple tasks (against the same handle clone
    /// or distinct clones); two calls against the same descriptor
    /// produce identical [`ConsumedBatch`] values.
    ///
    /// The handle does not detect epoch fencing on its own; a fenced
    /// consumer's [`Consumer::next_descriptors`] is what surfaces
    /// `Error::Fenced`. Fetches against descriptors handed out before
    /// fencing succeed against object storage as long as the data
    /// object is still present (i.e. before GC runs).
    pub async fn fetch(&self, descriptor: BatchDescriptor) -> Result<ConsumedBatch> {
        let _ = &self.manifest_path; // reserved for future per-source label
        let start = Instant::now();
        let path = Path::from(descriptor.location.as_str());
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

        Ok(ConsumedBatch {
            entries,
            sequence: descriptor.sequence,
            location: descriptor.location,
            metadata: descriptor.metadata,
        })
    }
}

/// Reads batches of ingested entries from object storage via a queue consumer.
///
/// The consumer iterates over entries in the queue manifest in ingestion order,
/// fetches the corresponding data batches from object storage, and makes them
/// available to the caller. Epoch-based fencing ensures only a single active
/// consumer processes entries at any time.
pub struct Consumer {
    consumer: QueueConsumer,
    object_store: Arc<dyn ObjectStore>,
    manifest_path: String,
    gc_shutdown: CancellationToken,
    gc_handle: tokio::task::JoinHandle<()>,
    ack_count: u64,
    last_acked_sequence: Option<u64>,
    last_fetched_sequence: Option<u64>,
    /// Read-ahead cursor: highest sequence handed out by
    /// [`Consumer::next_descriptors`]. Distinct from
    /// `last_fetched_sequence` because callers may receive
    /// descriptors before the corresponding fetch resolves.
    /// Initialized from `last_acked_sequence` on construction.
    last_handed_out_sequence: Option<u64>,
}

impl Consumer {
    /// Create a new consumer from the given configuration.
    ///
    /// Initializes the queue consumer (fencing any previous instance) and spawns
    /// the garbage collector. If `last_acked_sequence` is `Some(seq)`, the
    /// consumer resumes after that sequence; if `None`, it discovers the first
    /// available entry.
    pub async fn new(config: ConsumerConfig, last_acked_sequence: Option<u64>) -> Result<Self> {
        let object_store = common::storage::factory::create_object_store(&config.object_store)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store, last_acked_sequence).await
    }

    pub async fn with_object_store(
        config: ConsumerConfig,
        object_store: Arc<dyn ObjectStore>,
        last_acked_sequence: Option<u64>,
    ) -> Result<Self> {
        crate::metric_names::describe_consumer_metrics();
        let manifest_path = config.manifest_path.clone();
        let consumer =
            QueueConsumer::with_object_store(manifest_path.clone(), object_store.clone());

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
            manifest_path,
            ack_count: 0,
            last_acked_sequence,
            gc_shutdown,
            gc_handle,
            last_fetched_sequence: last_acked_sequence,
            last_handed_out_sequence: last_acked_sequence,
        })
    }

    /// Read the next data batch from object storage.
    ///
    /// Compatibility wrapper over [`Consumer::next_descriptors`] +
    /// [`Consumer::fetch_descriptor`]. Behavior is preserved from
    /// pre-RFC-0003: peeks the next manifest entry past the last
    /// handed-out / fetched sequence, fetches the corresponding object,
    /// and returns it. Returns `None` if no matching entry is
    /// available.
    pub async fn next_batch(&mut self) -> Result<Option<ConsumedBatch>> {
        let mut descriptors = self.next_descriptors(1).await?;
        match descriptors.pop() {
            Some(d) => Ok(Some(self.fetch_descriptor(d).await?)),
            None => Ok(None),
        }
    }

    /// Read the manifest once and return up to `max` contiguous
    /// [`BatchDescriptor`]s past the consumer's read-ahead cursor.
    ///
    /// Does not perform any object-store GET. Does not mutate the
    /// durable ack frontier. Advances the in-memory read-ahead cursor
    /// (`last_handed_out_sequence`) by the number of descriptors
    /// returned.
    ///
    /// Returns an empty `Vec` if no new entries are available;
    /// returns `Err(Error::Fenced)` if the consumer's epoch no longer
    /// matches the manifest's.
    ///
    /// **Caller contract**: once a descriptor is returned, the caller
    /// is responsible for either fetching and processing it or
    /// accepting that it will be re-handed-out only via process
    /// restart (`Consumer::new` with a `last_acked_sequence` argument).
    /// Lost descriptors are not reissued within a process. See RFC
    /// 0003 "Descriptor Handout Contract" for the full rules.
    pub async fn next_descriptors(&mut self, max: usize) -> Result<Vec<BatchDescriptor>> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let entries = self
            .consumer
            .descriptors_after(self.last_handed_out_sequence, max)
            .await?;
        metrics::gauge!(m::QUEUE_LENGTH).set(self.consumer.len() as f64);
        if let Some(last) = entries.last() {
            self.last_handed_out_sequence = Some(last.sequence);
        }
        let count = entries.len() as u64;
        if count > 0 {
            metrics::counter!(m::DESCRIPTORS_HANDED_OUT).increment(count);
        }
        Ok(entries
            .into_iter()
            .map(|e| BatchDescriptor {
                sequence: e.sequence,
                location: e.location,
                metadata: e.metadata,
                // Reserved field; the current manifest format does not
                // carry per-entry object size. See RFC 0003.
                object_bytes: None,
            })
            .collect())
    }

    /// Construct a cloneable fetch handle. O(1); each call returns a
    /// fresh handle, and the handle itself implements `Clone` for
    /// further duplication into worker tasks.
    pub fn fetch_handle(&self) -> ConsumerFetchHandle {
        ConsumerFetchHandle {
            object_store: self.object_store.clone(),
            manifest_path: self.manifest_path.clone(),
        }
    }

    /// Fetch and decode a single batch via the consumer's serial
    /// wrapper. Equivalent to `self.fetch_handle().fetch(descriptor)`,
    /// but additionally maintains the consumer's serial lag cursor
    /// (`last_fetched_sequence` + the `consumer_lag_seconds` gauge)
    /// used by the legacy `next_batch` path.
    ///
    /// Parallel-fetch callers should use [`Consumer::fetch_handle`]
    /// directly; the runtime owns its own per-stage latency
    /// histograms (RFC 0002).
    pub async fn fetch_descriptor(&mut self, descriptor: BatchDescriptor) -> Result<ConsumedBatch> {
        let handle = self.fetch_handle();
        let batch = handle.fetch(descriptor).await?;
        self.last_fetched_sequence = match self.last_fetched_sequence {
            Some(prev) => Some(prev.max(batch.sequence)),
            None => Some(batch.sequence),
        };
        if let Some(last_meta) = batch.metadata.last() {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let lag_s = (now_ms - last_meta.ingestion_time_ms) as f64 / 1000.0;
            metrics::gauge!(m::CONSUMER_LAG_SECONDS).set(lag_s.max(0.0));
        }
        Ok(batch)
    }

    /// Acknowledge that the batch with the given sequence number has been processed.
    ///
    /// Acks must be in order — the sequence must immediately follow the last acked
    /// sequence, otherwise an error is returned. To amortize manifest writes, the
    /// consumer only calls `dequeue()` on the queue consumer every
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

    /// Advance the durable ack frontier through (and including)
    /// `sequence`. Performs `dequeue(sequence)` against the manifest
    /// **first**, then updates in-memory state on success.
    ///
    /// On error (storage, fence), `last_acked_sequence` and the
    /// `buffer.acks` counter remain at their pre-call values. A retry
    /// against the same sequence is safe.
    ///
    /// Errors if `sequence <= last_acked_sequence` (the frontier is
    /// monotonic).
    pub async fn ack_through(&mut self, sequence: u64) -> Result<()> {
        if let Some(last) = self.last_acked_sequence
            && sequence <= last
        {
            return Err(Error::Storage(format!(
                "non-monotonic ack_through: last_acked={last}, requested={sequence}"
            )));
        }
        let count_advanced = match self.last_acked_sequence {
            None => sequence + 1,
            Some(last) => sequence - last,
        };

        // Durable dequeue first. Bubbles up Fenced and Storage errors
        // without touching local state.
        self.consumer.dequeue(sequence).await?;

        // Only mutate in-memory state after dequeue succeeds.
        self.last_acked_sequence = Some(sequence);
        self.ack_count = self.ack_count.wrapping_add(count_advanced);
        metrics::counter!(m::ACKS).increment(count_advanced);
        Ok(())
    }

    /// Flush any pending acks by dequeueing up to the last acked sequence.
    pub async fn flush(&mut self) -> Result<()> {
        if let Some(seq) = self.last_acked_sequence {
            self.consumer.dequeue(seq).await?;
        }
        Ok(())
    }

    /// Flush pending acks, shut down the garbage collector, and consume the handle.
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
    use crate::config::ConsumerConfig;
    use crate::model::{CompressionType, encode_batch};
    use crate::queue::{Metadata, QueueProducer};
    use bytes::Bytes;
    use common::ObjectStoreConfig;
    use slatedb::object_store::PutPayload;
    use slatedb::object_store::memory::InMemory;
    use std::time::Duration;

    const TEST_MANIFEST_PATH: &str = "test/manifest";

    fn test_collector_config() -> ConsumerConfig {
        ConsumerConfig {
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
        config: ConsumerConfig,
        last_acked_sequence: Option<u64>,
    ) -> (QueueProducer, Consumer) {
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        let collector = Consumer::with_object_store(config, store.clone(), last_acked_sequence)
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

    // ========================================================================
    // RFC 0003 read-ahead API tests.
    // ========================================================================

    async fn enqueue_n(
        store: &Arc<dyn ObjectStore>,
        producer: &QueueProducer,
        n: usize,
    ) -> Vec<String> {
        let entries = test_entries();
        let mut locations = Vec::with_capacity(n);
        for i in 0..n {
            let loc = format!("batches/seq-{i:06}");
            write_batch(store, &loc, &entries).await;
            producer.enqueue(loc.clone(), vec![]).await.unwrap();
            locations.push(loc);
        }
        locations
    }

    #[tokio::test]
    async fn should_next_descriptors_max_zero_returns_empty_without_manifest_read() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 3).await;

        // max=0 returns empty and does not advance the cursor.
        let v = collector.next_descriptors(0).await.unwrap();
        assert!(v.is_empty());

        // Subsequent next_descriptors still sees all three.
        let v = collector.next_descriptors(10).await.unwrap();
        assert_eq!(v.len(), 3);
    }

    #[tokio::test]
    async fn should_next_descriptors_returns_contiguous_run_when_available() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 5).await;

        let v = collector.next_descriptors(3).await.unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].sequence, 0);
        assert_eq!(v[1].sequence, 1);
        assert_eq!(v[2].sequence, 2);
        // object_bytes is reserved (always None in v1).
        assert!(v.iter().all(|d| d.object_bytes.is_none()));

        // Cursor advanced; next call returns sequences 3 and 4 only.
        let v2 = collector.next_descriptors(10).await.unwrap();
        assert_eq!(v2.len(), 2);
        assert_eq!(v2[0].sequence, 3);
        assert_eq!(v2[1].sequence, 4);

        // Empty when nothing new.
        let v3 = collector.next_descriptors(10).await.unwrap();
        assert!(v3.is_empty());
    }

    #[tokio::test]
    async fn should_next_descriptors_returns_fewer_than_max_on_short_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 2).await;

        let v = collector.next_descriptors(10).await.unwrap();
        assert_eq!(v.len(), 2);
    }

    #[tokio::test]
    async fn should_fetch_handle_return_same_consumed_batch_as_fetch_descriptor() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 2).await;

        let descriptors = collector.next_descriptors(2).await.unwrap();
        let handle = collector.fetch_handle();
        let from_handle = handle.fetch(descriptors[0].clone()).await.unwrap();
        let from_wrapper = collector
            .fetch_descriptor(descriptors[1].clone())
            .await
            .unwrap();

        assert_eq!(from_handle.sequence, descriptors[0].sequence);
        assert_eq!(from_handle.location, descriptors[0].location);
        assert_eq!(from_handle.entries.len(), 2);
        assert_eq!(from_wrapper.sequence, descriptors[1].sequence);
        assert_eq!(from_wrapper.location, descriptors[1].location);
        assert_eq!(from_wrapper.entries.len(), 2);
    }

    #[tokio::test]
    async fn should_fetch_handle_be_clone_send_sync_static() {
        // Compile-time check: handle is Clone + Send + Sync + 'static.
        fn assert_send_sync_static<T: Send + Sync + 'static>() {}
        assert_send_sync_static::<ConsumerFetchHandle>();

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 4).await;
        let descriptors = collector.next_descriptors(4).await.unwrap();

        // Two clones of the same handle, run from spawned tasks.
        let handle = collector.fetch_handle();
        let h1 = handle.clone();
        let h2 = handle.clone();
        let d0 = descriptors[0].clone();
        let d1 = descriptors[1].clone();
        let t1 = tokio::spawn(async move { h1.fetch(d0).await.unwrap() });
        let t2 = tokio::spawn(async move { h2.fetch(d1).await.unwrap() });
        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();
        assert_eq!(r1.sequence, 0);
        assert_eq!(r2.sequence, 1);

        // Owner can still drive next_descriptors and ack_through after
        // the spawned tasks completed (no &mut conflict because the
        // handle holds an Arc, not a borrow).
        collector.ack_through(1).await.unwrap();
    }

    #[tokio::test]
    async fn should_ack_through_advance_frontier_in_one_call() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 5).await;

        let _v = collector.next_descriptors(5).await.unwrap();

        // Advance through sequence 3 in one call.
        collector.ack_through(3).await.unwrap();
        // Manifest has 1 entry remaining (sequence 4).
        assert_eq!(collector.len(), 1);
    }

    #[tokio::test]
    async fn should_ack_through_reject_non_monotonic_input() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 5).await;
        collector.next_descriptors(5).await.unwrap();

        collector.ack_through(2).await.unwrap();
        let err = collector.ack_through(2).await.unwrap_err();
        match err {
            Error::Storage(msg) => assert!(msg.contains("non-monotonic ack_through")),
            other => panic!("expected Error::Storage, got {other:?}"),
        }
        // The bookkeeping did not advance.
        assert_eq!(collector.last_acked_sequence, Some(2));
    }

    #[tokio::test]
    async fn should_ack_through_leave_state_unchanged_on_fence() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector1) =
            make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 5).await;
        collector1.next_descriptors(5).await.unwrap();
        collector1.ack_through(0).await.unwrap();
        assert_eq!(collector1.last_acked_sequence, Some(0));

        // Fence collector1 by spinning up a second consumer.
        let (_, _collector2) = make_collector(&store, test_collector_config(), None).await;

        // ack_through against the fenced consumer must error and leave
        // last_acked_sequence at 0.
        let err = collector1.ack_through(2).await.unwrap_err();
        assert!(matches!(err, Error::Fenced));
        assert_eq!(collector1.last_acked_sequence, Some(0));
    }

    #[tokio::test]
    async fn should_next_descriptors_surface_fence() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector1) =
            make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 3).await;

        let (_, _collector2) = make_collector(&store, test_collector_config(), None).await;

        let result = collector1.next_descriptors(10).await;
        assert!(matches!(result, Err(Error::Fenced)));
    }

    #[tokio::test]
    async fn should_next_batch_be_equivalent_to_descriptor_plus_fetch() {
        // Equivalence: next_batch == next_descriptors(1) + fetch_descriptor.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        let entries = test_entries();
        let location = "batches/equiv";
        write_batch(&store, location, &entries).await;
        producer
            .enqueue(location.to_string(), vec![])
            .await
            .unwrap();

        let via_wrapper = collector.next_batch().await.unwrap().unwrap();
        assert_eq!(via_wrapper.sequence, 0);
        assert_eq!(via_wrapper.location, location);
        assert_eq!(via_wrapper.entries.len(), 2);
    }

    #[tokio::test]
    async fn should_descriptor_handout_contract_not_reissue_lost_descriptors() {
        // Per RFC 0003: once next_descriptors returns a descriptor,
        // it is not handed out again within the same process.
        // Recovery is restart from the durable ack frontier.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (producer, mut collector) = make_collector(&store, test_collector_config(), None).await;
        enqueue_n(&store, &producer, 3).await;

        // Hand out 2 descriptors; "lose" them (drop without acking).
        let lost = collector.next_descriptors(2).await.unwrap();
        assert_eq!(lost.len(), 2);
        drop(lost);

        // The same consumer's next_descriptors moves on past them.
        let next = collector.next_descriptors(10).await.unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].sequence, 2);

        // Recovery: a fresh consumer initialized from the last durable
        // ack (None here, since nothing was acked) re-emits the
        // unacked range from the start.
        collector.close().await.unwrap();
        let (_, mut recovered) = make_collector(&store, test_collector_config(), None).await;
        let again = recovered.next_descriptors(10).await.unwrap();
        assert_eq!(again.len(), 3);
        assert_eq!(again[0].sequence, 0);
    }
}

//! Background buffer consumer that reads vector batches from a buffer queue
//! and ingests them into [`VectorDb`].
//!
//! The wire format on the buffer is a `prost`-encoded
//! [`super::proto::WriteRequest`], identical to what the HTTP write handler
//! accepts under `application/protobuf`. Each entry is preceded by a 2-byte
//! metadata payload `[version=1, payload_encoding=1]` so future format
//! changes can be detected.
//!
//! Per-entry decoding errors are logged and skipped; they do not stop the
//! poll loop. Entries that successfully decode are written via
//! [`VectorDb::write`].

use std::sync::Arc;

use buffer::{ConsumedBatch, Consumer, Metadata};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::request::WriteRequest;
use crate::VectorDb;
use crate::model::BufferConsumerConfig;

const METADATA_VERSION: u8 = 1;
const PAYLOAD_ENCODING_VECTOR_PROTO: u8 = 1;
const METADATA_LEN: usize = 2;
const PREFETCH_BATCH_BUFFER: usize = 1;
const ACK_SEQUENCE_BUFFER: usize = 1;
const MAX_ERROR_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Debug)]
struct BufferMetadata {
    payload_encoding: u8,
}

impl BufferMetadata {
    fn decode(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < METADATA_LEN {
            return Err(format!(
                "metadata too short: expected {} bytes, got {}",
                METADATA_LEN,
                payload.len()
            ));
        }
        if payload[0] != METADATA_VERSION {
            return Err(format!("unknown metadata version: {}", payload[0]));
        }
        Ok(Self {
            payload_encoding: payload[1],
        })
    }
}

/// Handle returned by [`BufferConsumer::run`] that enables graceful shutdown.
///
/// Dropping this handle does **not** stop the consumer — call
/// [`shutdown`](Self::shutdown) to cancel the poll loop and flush pending
/// acks before the collector is released.
pub(crate) struct ConsumerHandle {
    cancel: CancellationToken,
    join: JoinHandle<()>,
}

impl ConsumerHandle {
    /// Signal the consumer to stop and wait for it to flush pending acks.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        if let Err(e) = self.join.await {
            tracing::error!("Buffer consumer task panicked: {e}");
        }
    }
}

pub(crate) struct BufferConsumer {
    db: Arc<VectorDb>,
    config: BufferConsumerConfig,
}

impl BufferConsumer {
    pub fn new(db: Arc<VectorDb>, config: BufferConsumerConfig) -> Self {
        Self { db, config }
    }

    /// Initialize the buffer collector and spawn the poll loop as a background task.
    pub async fn run(self: Arc<Self>) -> Result<ConsumerHandle, buffer::Error> {
        let collector_config = buffer::ConsumerConfig {
            object_store: self.config.object_store.clone(),
            manifest_path: self.config.manifest_path.clone(),
            data_path_prefix: self.config.data_path_prefix.clone(),
            gc_interval: self.config.gc_interval,
            gc_grace_period: self.config.gc_grace_period,
        };
        let collector = Consumer::new(collector_config, None).await?;
        self.start(collector).await
    }

    /// Like [`run`](Self::run), but uses a pre-built object store for the collector.
    /// Useful in tests where the buffer producer and consumer must share an in-memory store.
    #[cfg(test)]
    pub async fn run_with_object_store(
        self: Arc<Self>,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    ) -> Result<ConsumerHandle, buffer::Error> {
        let collector_config = buffer::ConsumerConfig {
            object_store: self.config.object_store.clone(),
            manifest_path: self.config.manifest_path.clone(),
            data_path_prefix: self.config.data_path_prefix.clone(),
            gc_interval: self.config.gc_interval,
            gc_grace_period: self.config.gc_grace_period,
        };
        let collector = Consumer::with_object_store(collector_config, object_store, None).await?;
        self.start(collector).await
    }

    async fn start(self: &Arc<Self>, collector: Consumer) -> Result<ConsumerHandle, buffer::Error> {
        tracing::info!(
            manifest_path = %self.config.manifest_path,
            poll_interval = ?self.config.poll_interval,
            "Vector buffer consumer started"
        );

        let db = self.db.clone();
        let poll_interval = self.config.poll_interval;
        let cancel = CancellationToken::new();

        let join = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                poll_loop(db, poll_interval, collector, cancel).await;
            }
        });

        Ok(ConsumerHandle { cancel, join })
    }
}

// TODO: improve error handling — distinguish Fenced (fatal) from transient Storage errors,
// add backoff for repeated failures, and retry acks before giving up.
async fn poll_loop(
    db: Arc<VectorDb>,
    poll_interval: std::time::Duration,
    collector: Consumer,
    cancel: CancellationToken,
) {
    let (batch_tx, mut batch_rx) = mpsc::channel(PREFETCH_BATCH_BUFFER);
    let (ack_tx, ack_rx) = mpsc::channel(ACK_SEQUENCE_BUFFER);
    let collector_join = tokio::spawn(collector_loop(
        collector,
        poll_interval,
        cancel.clone(),
        batch_tx,
        ack_rx,
    ));

    loop {
        let batch = tokio::select! {
            _ = cancel.cancelled() => break,
            maybe_batch = batch_rx.recv() => match maybe_batch {
                Some(batch) => batch,
                None => break,
            }
        };

        let sequence = batch.sequence;
        process_batch(&db, &batch).await;

        if let Err(e) = ack_tx.send(sequence).await {
            tracing::error!(sequence, "Failed to send ack to collector task: {e}");
            break;
        }

        if cancel.is_cancelled() {
            break;
        }
    }

    tracing::info!("Vector buffer consumer shutting down, flushing pending acks...");
    drop(ack_tx);
    if let Err(e) = collector_join.await {
        tracing::error!("Consumer task panicked: {e}");
    }
}

async fn collector_loop(
    mut collector: Consumer,
    poll_interval: std::time::Duration,
    cancel: CancellationToken,
    batch_tx: mpsc::Sender<ConsumedBatch>,
    mut ack_rx: mpsc::Receiver<u64>,
) {
    let mut error_backoff = poll_interval;

    loop {
        if cancel.is_cancelled() {
            break;
        }

        match ack_rx.try_recv() {
            Ok(sequence) => {
                ack_sequence(&mut collector, sequence).await;
                continue;
            }
            Err(mpsc::error::TryRecvError::Disconnected) => break,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        let mut wait_delay = None;

        if batch_tx.capacity() > 0 {
            match collector.next_batch().await {
                Ok(Some(batch)) => {
                    error_backoff = poll_interval;
                    if let Err(e) = batch_tx.send(batch).await {
                        tracing::error!("Failed to send batch to consumer task: {e}");
                        break;
                    }
                    continue;
                }
                Ok(None) => {
                    error_backoff = poll_interval;
                    wait_delay = Some(poll_interval);
                }
                Err(e) => {
                    tracing::error!(retry_in = ?error_backoff, "Failed to read next batch: {e}");
                    wait_delay = Some(error_backoff);
                    error_backoff = error_backoff
                        .checked_mul(2)
                        .unwrap_or(MAX_ERROR_RETRY_BACKOFF)
                        .min(MAX_ERROR_RETRY_BACKOFF);
                }
            }
        }

        match wait_for_work(&cancel, &mut ack_rx, &batch_tx, wait_delay, &mut collector).await {
            WaitOutcome::Wake => {}
            WaitOutcome::Stop => break,
        }
    }

    drain_pending_acks(&mut collector, &mut ack_rx).await;

    if let Err(e) = collector.close().await {
        tracing::error!("Failed to flush collector on shutdown: {e}");
    }
}

enum WaitOutcome {
    Wake,
    Stop,
}

async fn wait_for_work(
    cancel: &CancellationToken,
    ack_rx: &mut mpsc::Receiver<u64>,
    batch_tx: &mpsc::Sender<ConsumedBatch>,
    delay: Option<std::time::Duration>,
    collector: &mut Consumer,
) -> WaitOutcome {
    let wait_for_capacity = batch_tx.capacity() == 0;
    let mut timer = delay.map(|delay| Box::pin(sleep(delay)));
    let timer_enabled = timer.is_some();

    tokio::select! {
        _ = cancel.cancelled() => WaitOutcome::Stop,
        ack = ack_rx.recv() => match ack {
            Some(sequence) => {
                ack_sequence(collector, sequence).await;
                WaitOutcome::Wake
            }
            None => WaitOutcome::Stop,
        },
        permit = batch_tx.reserve(), if wait_for_capacity => match permit {
            Ok(permit) => {
                drop(permit);
                WaitOutcome::Wake
            }
            Err(_) => WaitOutcome::Stop,
        },
        _ = async {
            if let Some(timer) = timer.as_mut() {
                timer.await;
            }
        }, if timer_enabled => WaitOutcome::Wake,
    }
}

async fn drain_pending_acks(collector: &mut Consumer, ack_rx: &mut mpsc::Receiver<u64>) {
    while let Ok(sequence) = ack_rx.try_recv() {
        ack_sequence(collector, sequence).await;
    }
}

async fn ack_sequence(collector: &mut Consumer, sequence: u64) {
    if let Err(e) = collector.ack(sequence).await {
        tracing::error!(sequence, "Failed to ack batch: {e}");
    }
}

async fn process_batch(db: &VectorDb, batch: &ConsumedBatch) {
    for (idx, entry) in batch.entries.iter().enumerate() {
        let metadata = find_metadata_for_entry(&batch.metadata, idx as u32);
        if let Err(reason) = process_entry(db, entry, metadata).await {
            tracing::warn!(
                sequence = batch.sequence,
                entry_index = idx,
                "Skipping entry: {reason}"
            );
        }
    }
}

async fn process_entry(
    db: &VectorDb,
    entry: &Bytes,
    metadata: Option<&Metadata>,
) -> Result<(), String> {
    let meta = metadata.ok_or("no metadata for entry")?;
    let parsed = BufferMetadata::decode(&meta.payload)?;

    if parsed.payload_encoding != PAYLOAD_ENCODING_VECTOR_PROTO {
        return Err(format!(
            "unsupported payload encoding: {}",
            parsed.payload_encoding
        ));
    }

    let request = WriteRequest::from_protobuf(entry.as_ref())
        .map_err(|e| format!("vector proto decode failed: {e}"))?;

    if request.upsert_vectors.is_empty() {
        return Ok(());
    }

    db.write(request.upsert_vectors)
        .await
        .map_err(|e| format!("vector db write failed: {e}"))?;

    Ok(())
}

/// Find the metadata that applies to a given entry index.
///
/// Metadata ranges are sorted by `start_index`. The applicable range for
/// entry `idx` is the last metadata whose `start_index <= idx`.
fn find_metadata_for_entry(metadata: &[Metadata], idx: u32) -> Option<&Metadata> {
    let pos = metadata.partition_point(|m| m.start_index <= idx);
    if pos > 0 {
        Some(&metadata[pos - 1])
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_decode_valid_metadata() {
        // given
        let payload = vec![1, 1];

        // when
        let meta = BufferMetadata::decode(&payload).unwrap();

        // then
        assert_eq!(meta.payload_encoding, PAYLOAD_ENCODING_VECTOR_PROTO);
    }

    #[test]
    fn should_decode_metadata_with_trailing_bytes() {
        // given
        let payload = vec![1, 1, 0, 0];

        // when
        let meta = BufferMetadata::decode(&payload).unwrap();

        // then
        assert_eq!(meta.payload_encoding, PAYLOAD_ENCODING_VECTOR_PROTO);
    }

    #[test]
    fn should_reject_unknown_version() {
        // given
        let payload = vec![99, 1];

        // when
        let result = BufferMetadata::decode(&payload);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown metadata version"));
    }

    #[test]
    fn should_reject_short_metadata() {
        // given
        let payload = vec![1];

        // when
        let result = BufferMetadata::decode(&payload);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("metadata too short"));
    }

    #[test]
    fn should_find_metadata_for_entry() {
        // given
        let metadata = vec![
            Metadata {
                start_index: 0,
                ingestion_time_ms: 1000,
                payload: Bytes::from_static(&[1, 1]),
            },
            Metadata {
                start_index: 3,
                ingestion_time_ms: 2000,
                payload: Bytes::from_static(&[1, 1]),
            },
        ];

        // when/then
        let m = find_metadata_for_entry(&metadata, 0).unwrap();
        assert_eq!(m.start_index, 0);

        // when/then
        let m = find_metadata_for_entry(&metadata, 2).unwrap();
        assert_eq!(m.start_index, 0);

        // when/then
        let m = find_metadata_for_entry(&metadata, 3).unwrap();
        assert_eq!(m.start_index, 3);

        // when/then
        let m = find_metadata_for_entry(&metadata, 5).unwrap();
        assert_eq!(m.start_index, 3);
    }

    /// End-to-end: Producer → BufferConsumer → VectorDb → search.
    #[tokio::test]
    async fn should_ingest_via_consumer_and_search_back() {
        use std::collections::HashMap;
        use std::time::Duration;

        use common::ObjectStoreConfig;
        use common::clock::SystemClock;
        use prost::Message;
        use slatedb::object_store::ObjectStore;
        use slatedb::object_store::memory::InMemory;

        use crate::VectorDbRead;
        use crate::model::{Config, DistanceMetric, Query};
        use crate::server::proto;

        // given
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest = "ingest/manifest".to_string();
        let data_prefix = "ingest".to_string();

        // Build a WriteRequest with two vectors
        let proto_request = proto::WriteRequest {
            upsert_vectors: vec![
                proto::Vector {
                    id: "vec-1".to_string(),
                    attributes: HashMap::from([(
                        "vector".to_string(),
                        proto::AttributeValueMessage::new(proto::AttributeValueProto::VectorValue(
                            proto::VectorValueProto {
                                values: vec![1.0, 0.0, 0.0],
                            },
                        )),
                    )]),
                },
                proto::Vector {
                    id: "vec-2".to_string(),
                    attributes: HashMap::from([(
                        "vector".to_string(),
                        proto::AttributeValueMessage::new(proto::AttributeValueProto::VectorValue(
                            proto::VectorValueProto {
                                values: vec![0.0, 1.0, 0.0],
                            },
                        )),
                    )]),
                },
            ],
        };
        let entry_bytes = proto_request.encode_to_vec();
        let metadata_bytes = Bytes::from_static(&[METADATA_VERSION, PAYLOAD_ENCODING_VECTOR_PROTO]);

        // Produce via Producer
        let producer_config = buffer::ProducerConfig {
            object_store: ObjectStoreConfig::InMemory,
            data_path_prefix: data_prefix.clone(),
            manifest_path: manifest.clone(),
            flush_interval: Duration::from_millis(10),
            flush_size_bytes: 64 * 1024 * 1024,
            max_buffered_inputs: 1000,
            batch_compression: buffer::CompressionType::None,
        };
        let producer = buffer::Producer::with_object_store(
            producer_config,
            obj_store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();
        producer
            .produce(vec![Bytes::from(entry_bytes)], metadata_bytes)
            .await
            .unwrap();
        producer.close().await.unwrap();

        // Open a vector db (in-memory storage)
        let db = Arc::new(
            VectorDb::open(Config {
                dimensions: 3,
                distance_metric: DistanceMetric::L2,
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        // Start the consumer against the shared object store
        let consumer_config = BufferConsumerConfig {
            object_store: ObjectStoreConfig::InMemory,
            manifest_path: manifest,
            data_path_prefix: data_prefix,
            poll_interval: Duration::from_millis(10),
            gc_interval: Duration::from_secs(300),
            gc_grace_period: Duration::from_secs(600),
        };
        let consumer = Arc::new(BufferConsumer::new(db.clone(), consumer_config));
        let handle = consumer.run_with_object_store(obj_store).await.unwrap();

        // Wait for the consumer to process and write
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            db.flush().await.unwrap();
            let results = db
                .search(&Query::new(vec![1.0, 0.0, 0.0]).with_limit(2))
                .await
                .unwrap();
            if results.iter().any(|r| r.vector.id == "vec-1") {
                break;
            }
            if std::time::Instant::now() >= deadline {
                handle.shutdown().await;
                panic!("consumer did not ingest the produced batch in time");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        handle.shutdown().await;
    }
}

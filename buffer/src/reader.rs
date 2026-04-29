//! Read-only entry point into a Buffer.
//!
//! [`BufferReader`] opens a manifest object directly via the object store,
//! parses it with the same binary format the [`Consumer`](crate::Consumer)
//! uses, and reads referenced batch objects. It performs **no** writes,
//! never increments the manifest's epoch, and never starts the garbage
//! collector. Use this when an inspection tool, audit, or debugger needs
//! to look at a Buffer manifest without disturbing the active consumer.
//!
//! [`Consumer::new`](crate::Consumer) (and the `initialize` flow it
//! requires) is *not* read-only — it bumps the manifest epoch on
//! initialization and fences any other consumer. Tools that point at a
//! production manifest must use [`BufferReader`] instead.

use std::sync::Arc;

use bytes::Bytes;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::path::Path;

use crate::error::{Error, Result};
use crate::model::decode_batch;
use crate::queue::{ManifestView, parse_manifest};

/// Read-only access to a Buffer manifest and its referenced batch objects.
#[derive(Clone)]
pub struct BufferReader {
    object_store: Arc<dyn ObjectStore>,
    manifest_path: String,
}

impl BufferReader {
    /// Construct a reader for the manifest at `manifest_path`, served by
    /// `object_store`.
    pub fn new(object_store: Arc<dyn ObjectStore>, manifest_path: String) -> Self {
        Self {
            object_store,
            manifest_path,
        }
    }

    /// Path of the manifest object this reader is bound to.
    pub fn manifest_path(&self) -> &str {
        &self.manifest_path
    }

    /// Fetch and parse the manifest from object storage.
    ///
    /// Returns the manifest view at the time of the read; concurrent
    /// producers may have appended further entries by the time the
    /// caller acts on the view.
    pub async fn read_manifest(&self) -> Result<ManifestView> {
        let path = Path::from(self.manifest_path.as_str());
        let result = self
            .object_store
            .get(&path)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        parse_manifest(bytes)
    }

    /// Fetch and decode a batch object at the given object-store path.
    ///
    /// Returns the entry payloads in the same order they were written by
    /// the producer. The caller is responsible for interpreting each
    /// entry's bytes; per-entry metadata envelopes come from the
    /// manifest, not the batch.
    pub async fn read_batch(&self, location: &str) -> Result<Vec<Bytes>> {
        let path = Path::from(location);
        let result = self
            .object_store
            .get(&path)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        decode_batch(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use common::ObjectStoreConfig;
    use common::clock::SystemClock;
    use slatedb::object_store::ObjectStore;
    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::path::Path;

    use crate::config::ProducerConfig;
    use crate::model::CompressionType;
    use crate::producer::Producer;

    use super::BufferReader;

    fn test_config(manifest_path: &str) -> ProducerConfig {
        ProducerConfig {
            object_store: ObjectStoreConfig::InMemory,
            data_path_prefix: "test/data".to_string(),
            manifest_path: manifest_path.to_string(),
            flush_interval: Duration::from_secs(24 * 60 * 60),
            flush_size_bytes: 64 * 1024 * 1024,
            max_buffered_inputs: 1000,
            batch_compression: CompressionType::None,
        }
    }

    async fn produce_and_flush(producer: &Producer, payload: &[u8], metadata: Bytes) {
        producer
            .produce(vec![Bytes::copy_from_slice(payload)], metadata)
            .await
            .expect("produce");
        producer.flush().await.expect("flush");
    }

    async fn manifest_etag(store: &Arc<dyn ObjectStore>, path: &str) -> Option<String> {
        match store.get(&Path::from(path)).await {
            Ok(result) => result.meta.e_tag.clone(),
            Err(_) => None,
        }
    }

    #[tokio::test]
    async fn read_manifest_round_trips_producer_writes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_path = "ingest/test/manifest";
        let producer = Producer::with_object_store(
            test_config(manifest_path),
            Arc::clone(&store),
            Arc::new(SystemClock),
        )
        .expect("producer");
        let reader = BufferReader::new(Arc::clone(&store), manifest_path.to_string());

        let metadata = Bytes::from_static(&[1, 1, 1, 0]);
        produce_and_flush(&producer, b"row-0", metadata.clone()).await;

        let view = reader.read_manifest().await.expect("read manifest");
        assert_eq!(view.entries().len(), 1);
        let entry = &view.entries()[0];
        assert_eq!(entry.metadata.len(), 1);
        assert_eq!(&entry.metadata[0].payload[..], &[1, 1, 1, 0]);
        assert!(view.next_sequence > 0);

        let payloads = reader
            .read_batch(&entry.location)
            .await
            .expect("read batch");
        assert_eq!(payloads, vec![Bytes::from_static(b"row-0")]);

        producer.close().await.expect("close producer");
    }

    /// Pin down the read-only contract: invoking BufferReader against a
    /// manifest must not change the manifest object's ETag and must not
    /// advance its epoch. This is the property that lets us point this
    /// tool at a live production manifest without fencing the active
    /// consumer.
    #[tokio::test]
    async fn read_manifest_does_not_mutate_object() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest_path = "ingest/test/manifest-readonly";
        let producer = Producer::with_object_store(
            test_config(manifest_path),
            Arc::clone(&store),
            Arc::new(SystemClock),
        )
        .expect("producer");
        let reader = BufferReader::new(Arc::clone(&store), manifest_path.to_string());

        let metadata = Bytes::from_static(&[1, 1, 1, 0]);
        produce_and_flush(&producer, b"row-0", metadata.clone()).await;

        let etag_before = manifest_etag(&store, manifest_path)
            .await
            .expect("manifest written");
        let view_before = reader.read_manifest().await.expect("read manifest");
        let epoch_before = view_before.epoch;

        // Multiple read passes — including a batch fetch — must not touch
        // the manifest object.
        let _view_again = reader.read_manifest().await.expect("read manifest 2");
        let _payloads = reader
            .read_batch(&view_before.entries()[0].location)
            .await
            .expect("read batch");

        let etag_after = manifest_etag(&store, manifest_path)
            .await
            .expect("manifest still present");
        let epoch_after = reader.read_manifest().await.expect("final read").epoch;

        assert_eq!(
            etag_before, etag_after,
            "BufferReader must not modify the manifest object's ETag",
        );
        assert_eq!(
            epoch_before, epoch_after,
            "BufferReader must not advance the manifest epoch",
        );

        producer.close().await.expect("close producer");
    }
}

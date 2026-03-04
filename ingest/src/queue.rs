#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use common::clock::Clock;
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectStore, PutMode, PutPayload, UpdateVersion,
};

use crate::error::{Error, Result};
use crate::queue_config::ConsumerConfig;

fn millis(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_millis() as i64
}

const PRODUCER_MANIFEST_VERSION: u16 = 1;
const FOOTER_SIZE: usize = 14; // 4 bytes entry_count + 8 bytes next_sequence + 2 bytes version

#[derive(Debug, Clone)]
struct ProducerEntry {
    sequence: u64,
    location: String,
    ingestion_time_ms: i64,
    metadata: Bytes,
}

#[derive(Debug, Clone)]
struct ProducerManifest {
    data: Bytes,
    appended: BytesMut,
    appended_count: usize,
    next_sequence: u64,
}

impl ProducerManifest {
    /// Create an empty manifest with a valid footer.
    fn empty() -> Self {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u32_le(0);
        buf.put_u64_le(0);
        buf.put_u16_le(PRODUCER_MANIFEST_VERSION);
        Self {
            data: buf.freeze(),
            appended: BytesMut::new(),
            appended_count: 0,
            next_sequence: 0,
        }
    }

    /// Wrap raw binary data as a producer manifest, validating the footer.
    fn from_bytes(data: Bytes) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::Serialization(
                "producer manifest data must not be empty".to_string(),
            ));
        }
        if data.len() < FOOTER_SIZE {
            return Err(Error::Serialization(
                "producer manifest too short for footer".to_string(),
            ));
        }
        let version_start = data.len() - 2;
        let version = u16::from_le_bytes(data[version_start..].try_into().unwrap());
        if version != PRODUCER_MANIFEST_VERSION {
            return Err(Error::Serialization(format!(
                "unsupported producer manifest version: {}",
                version
            )));
        }
        let next_seq_start = data.len() - 10;
        let next_sequence =
            u64::from_le_bytes(data[next_seq_start..next_seq_start + 8].try_into().unwrap());
        Ok(Self {
            data,
            appended: BytesMut::new(),
            appended_count: 0,
            next_sequence,
        })
    }

    /// Build a manifest from a slice of entries (for full rebuild, e.g. cleanup).
    fn from_entries(entries: &[ProducerEntry]) -> Self {
        let next_sequence = entries.iter().map(|e| e.sequence + 1).max().unwrap_or(0);
        let mut buf = BytesMut::new();
        for entry in entries {
            Self::encode_entry(&mut buf, entry);
        }
        buf.put_u32_le(entries.len() as u32);
        buf.put_u64_le(next_sequence);
        buf.put_u16_le(PRODUCER_MANIFEST_VERSION);
        Self {
            data: buf.freeze(),
            appended: BytesMut::new(),
            appended_count: 0,
            next_sequence,
        }
    }

    /// Number of entries (read from the footer, O(1)).
    fn entries_count(&self) -> usize {
        let base = self.existing_entries_count();
        base + self.appended_count
    }

    /// Whether the manifest contains no entries.
    fn is_empty(&self) -> bool {
        self.entries_count() == 0
    }

    /// Iterate over decoded entries.
    fn iter(&self) -> ProducerManifestIter<'_> {
        let base_count = self.existing_entries_count();
        let entries_end = if self.data.is_empty() {
            0
        } else {
            self.data.len() - FOOTER_SIZE
        };
        ProducerManifestIter {
            data: &self.data,
            offset: 0,
            remaining: base_count,
            entries_end,
            appended: &self.appended,
            appended_offset: 0,
            appended_remaining: self.appended_count,
        }
    }

    fn existing_entries_count(&self) -> usize {
        if self.data.is_empty() {
            0
        } else {
            let footer_start = self.data.len() - FOOTER_SIZE;
            u32::from_le_bytes(
                self.data[footer_start..footer_start + 4]
                    .try_into()
                    .unwrap(),
            ) as usize
        }
    }

    /// Append a single entry without copying existing data.
    /// The entry is encoded and stored internally; bytes are merged in `to_bytes()`.
    /// The entry's sequence number is overwritten with the manifest's next sequence.
    fn append(&mut self, entry: &ProducerEntry) {
        let sequenced = ProducerEntry {
            sequence: self.next_sequence,
            ..entry.clone()
        };
        Self::encode_entry(&mut self.appended, &sequenced);
        self.next_sequence += 1;
        self.appended_count += 1;
    }

    /// Remove all entries with sequence <= `through_sequence`, returning them.
    fn dequeue(&mut self, through_sequence: u64) -> Vec<ProducerEntry> {
        let next_seq = self.next_sequence;
        let all: Vec<ProducerEntry> = self.iter().map(|e| e.unwrap()).collect();
        let (removed, remaining): (Vec<_>, Vec<_>) = all
            .into_iter()
            .partition(|e| e.sequence <= through_sequence);
        *self = Self::from_entries(&remaining);
        self.next_sequence = next_seq;
        // Update the data bytes to reflect the preserved next_sequence
        let mut buf = BytesMut::from(self.data.as_ref());
        let ns_start = buf.len() - 10;
        buf[ns_start..ns_start + 8].copy_from_slice(&next_seq.to_le_bytes());
        self.data = buf.freeze();
        removed
    }

    /// Serialize the manifest to bytes for writing to object storage.
    /// When an entry was appended, this merges existing data with the appended
    /// entry and writes a new footer.
    fn to_bytes(&self) -> Bytes {
        if self.appended.is_empty() {
            return self.data.clone();
        }
        let (prefix, base_count) = if self.data.is_empty() {
            (&[] as &[u8], 0u32)
        } else {
            let footer_start = self.data.len() - FOOTER_SIZE;
            let count = u32::from_le_bytes(
                self.data[footer_start..footer_start + 4]
                    .try_into()
                    .unwrap(),
            );
            (&self.data[..footer_start], count)
        };
        let mut buf = BytesMut::with_capacity(prefix.len() + self.appended.len() + FOOTER_SIZE);
        buf.extend_from_slice(prefix);
        buf.extend_from_slice(&self.appended);
        buf.put_u32_le(base_count + self.appended_count as u32);
        buf.put_u64_le(self.next_sequence);
        buf.put_u16_le(PRODUCER_MANIFEST_VERSION);
        buf.freeze()
    }

    fn encode_entry(buf: &mut BytesMut, entry: &ProducerEntry) {
        let entry_len = 8 + 2 + entry.location.len() + 8 + entry.metadata.len();
        buf.put_u32_le(entry_len as u32);
        buf.put_u64_le(entry.sequence);
        buf.put_u16_le(entry.location.len() as u16);
        buf.extend_from_slice(entry.location.as_bytes());
        buf.put_i64_le(entry.ingestion_time_ms);
        buf.extend_from_slice(&entry.metadata);
    }
}

/// Decode a single entry from binary data at the given offset.
fn decode_entry(data: &[u8], offset: &mut usize, end: usize) -> Result<ProducerEntry> {
    if *offset + 4 > end {
        return Err(Error::Serialization(
            "producer manifest truncated: not enough bytes for entry_len".to_string(),
        ));
    }

    let entry_len = u32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap()) as usize;
    *offset += 4;

    if *offset + entry_len > end {
        return Err(Error::Serialization(
            "producer manifest truncated: entry extends beyond data".to_string(),
        ));
    }

    let sequence = u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;

    let location_len = u16::from_le_bytes(data[*offset..*offset + 2].try_into().unwrap()) as usize;
    *offset += 2;

    let location = String::from_utf8(data[*offset..*offset + location_len].to_vec())
        .map_err(|e| Error::Serialization(e.to_string()))?;
    *offset += location_len;

    let ingestion_time_ms = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;

    let metadata_len = entry_len - 8 - 2 - location_len - 8;
    let metadata = Bytes::copy_from_slice(&data[*offset..*offset + metadata_len]);
    *offset += metadata_len;

    Ok(ProducerEntry {
        sequence,
        location,
        ingestion_time_ms,
        metadata,
    })
}

struct ProducerManifestIter<'a> {
    data: &'a [u8],
    offset: usize,
    remaining: usize,
    entries_end: usize,
    appended: &'a [u8],
    appended_offset: usize,
    appended_remaining: usize,
}

impl<'a> Iterator for ProducerManifestIter<'a> {
    type Item = Result<ProducerEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining > 0 {
            self.remaining -= 1;
            Some(decode_entry(self.data, &mut self.offset, self.entries_end))
        } else if self.appended_remaining > 0 {
            self.appended_remaining -= 1;
            Some(decode_entry(
                self.appended,
                &mut self.appended_offset,
                self.appended.len(),
            ))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct ConsumerManifest {
    claimed: HashMap<String, i64>,
    done: Vec<String>,
}

enum ManifestWriteError {
    Conflict,
    Fatal(Error),
}

#[derive(Clone)]
struct ManifestStore {
    object_store: Arc<dyn ObjectStore>,
    manifest_path: String,
}

impl ManifestStore {
    async fn read<T>(&self) -> Result<(T, Option<UpdateVersion>)>
    where
        T: serde::de::DeserializeOwned + Default,
    {
        let path = Path::from(self.manifest_path.as_str());
        match self.object_store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let manifest: T = serde_json::from_slice(&bytes)
                    .map_err(|e| Error::Serialization(e.to_string()))?;
                Ok((manifest, Some(version)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok((T::default(), None)),
            Err(e) => Err(Error::Storage(e.to_string())),
        }
    }

    async fn write<T>(
        &self,
        manifest: &T,
        version: Option<UpdateVersion>,
    ) -> std::result::Result<(), ManifestWriteError>
    where
        T: serde::Serialize,
    {
        let path = Path::from(self.manifest_path.as_str());
        let json = serde_json::to_vec(manifest)
            .map_err(|e| ManifestWriteError::Fatal(Error::Serialization(e.to_string())))?;

        let put_mode = match version {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };

        match self
            .object_store
            .put_opts(&path, PutPayload::from(json), put_mode.into())
            .await
        {
            Ok(_) => Ok(()),
            Err(ObjectStoreError::Precondition { .. })
            | Err(ObjectStoreError::AlreadyExists { .. }) => Err(ManifestWriteError::Conflict),
            Err(e) => Err(ManifestWriteError::Fatal(Error::Storage(e.to_string()))),
        }
    }

    async fn read_producer_manifest(&self) -> Result<(ProducerManifest, Option<UpdateVersion>)> {
        let path = Path::from(self.manifest_path.as_str());
        match self.object_store.get(&path).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|e| Error::Storage(e.to_string()))?;
                let manifest = ProducerManifest::from_bytes(bytes)?;
                Ok((manifest, Some(version)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok((ProducerManifest::empty(), None)),
            Err(e) => Err(Error::Storage(e.to_string())),
        }
    }

    async fn write_producer_manifest(
        &self,
        manifest: &ProducerManifest,
        version: Option<UpdateVersion>,
    ) -> std::result::Result<(), ManifestWriteError> {
        let path = Path::from(self.manifest_path.as_str());
        let put_mode = match version {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        let data = manifest.to_bytes();

        match self
            .object_store
            .put_opts(&path, PutPayload::from(data.to_vec()), put_mode.into())
            .await
        {
            Ok(_) => Ok(()),
            Err(ObjectStoreError::Precondition { .. })
            | Err(ObjectStoreError::AlreadyExists { .. }) => Err(ManifestWriteError::Conflict),
            Err(e) => Err(ManifestWriteError::Fatal(Error::Storage(e.to_string()))),
        }
    }
}

struct ConflictCounter {
    write_count: AtomicU64,
    conflict_count: AtomicU64,
}

impl ConflictCounter {
    fn new() -> Self {
        Self {
            write_count: AtomicU64::new(0),
            conflict_count: AtomicU64::new(0),
        }
    }

    fn record_write(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_conflict(&self) {
        self.conflict_count.fetch_add(1, Ordering::Relaxed);
    }

    fn conflict_rate(&self) -> f64 {
        let writes = self.write_count.load(Ordering::Relaxed);
        if writes == 0 {
            return 0.0;
        }
        let conflicts = self.conflict_count.load(Ordering::Relaxed);
        (conflicts as f64 / writes as f64) * 100.0
    }
}

pub struct QueueProducer {
    manifest_store: ManifestStore,
    clock: Arc<dyn Clock>,
    counter: ConflictCounter,
}

impl QueueProducer {
    pub fn with_object_store(
        manifest_path: String,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            manifest_store: ManifestStore {
                object_store,
                manifest_path,
            },
            clock,
            counter: ConflictCounter::new(),
        }
    }

    pub async fn enqueue(&self, location: String, metadata: Bytes) -> Result<()> {
        let entry = ProducerEntry {
            sequence: 0,
            location,
            ingestion_time_ms: millis(self.clock.now()),
            metadata,
        };
        loop {
            let (mut manifest, version) = self.manifest_store.read_producer_manifest().await?;
            manifest.append(&entry);
            self.counter.record_write();
            match self
                .manifest_store
                .write_producer_manifest(&manifest, version)
                .await
            {
                Ok(()) => return Ok(()),
                Err(ManifestWriteError::Conflict) => {
                    self.counter.record_conflict();
                    continue;
                }
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub fn conflict_rate(&self) -> f64 {
        self.counter.conflict_rate()
    }
}

pub struct QueueConsumer {
    producer_store: ManifestStore,
    consumer_store: ManifestStore,
    heartbeat_timeout_ms: i64,
    done_cleanup_threshold: usize,
    clock: Arc<dyn Clock>,
    counter: ConflictCounter,
    producer_len: AtomicU64,
}

fn consumer_manifest_path(producer_path: &str) -> String {
    format!("{}.consumer.json", producer_path)
}

impl QueueConsumer {
    pub fn with_object_store(
        config: ConsumerConfig,
        object_store: Arc<dyn ObjectStore>,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let consumer_path = consumer_manifest_path(&config.manifest_path);
        Ok(Self {
            producer_store: ManifestStore {
                object_store: object_store.clone(),
                manifest_path: config.manifest_path,
            },
            consumer_store: ManifestStore {
                object_store,
                manifest_path: consumer_path,
            },
            heartbeat_timeout_ms: config.heartbeat_timeout_ms,
            done_cleanup_threshold: config.done_cleanup_threshold,
            clock,
            counter: ConflictCounter::new(),
            producer_len: AtomicU64::new(0),
        })
    }

    pub async fn claim(&self) -> Result<Option<String>> {
        loop {
            let (producer, _) = self.read_producer_manifest().await?;
            let (mut consumer, version): (ConsumerManifest, _) = self.consumer_store.read().await?;
            let now = millis(self.clock.now());

            // Try to re-claim a stale location first
            let cutoff = now - self.heartbeat_timeout_ms;
            let stale = consumer
                .claimed
                .iter()
                .filter(|(_, ts)| **ts < cutoff)
                .min_by_key(|(_, ts)| **ts)
                .map(|(loc, _)| loc.clone());

            let location = if let Some(loc) = stale {
                loc
            } else {
                // Find first pending location not already claimed or done
                let mut found = None;
                for entry in producer.iter() {
                    let entry = entry?;
                    if !consumer.claimed.contains_key(entry.location.as_str())
                        && !consumer.done.contains(&entry.location)
                    {
                        found = Some(entry.location);
                        break;
                    }
                }
                match found {
                    Some(loc) => loc,
                    None => return Ok(None),
                }
            };

            consumer.claimed.insert(location.clone(), now);
            match self.consumer_store.write(&consumer, version).await {
                Ok(()) => return Ok(Some(location)),
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub async fn dequeue(&self, location: &str) -> Result<bool> {
        loop {
            let (mut consumer, version): (ConsumerManifest, _) = self.consumer_store.read().await?;
            if consumer.claimed.remove(location).is_none() {
                return Ok(false);
            }
            consumer.done.push(location.to_string());
            match self.consumer_store.write(&consumer, version).await {
                Ok(()) => break,
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }

        // Check if we should cleanup done items
        let (consumer, _): (ConsumerManifest, _) = self.consumer_store.read().await?;
        if consumer.done.len() >= self.done_cleanup_threshold {
            self.cleanup_done().await?;
        }

        Ok(true)
    }

    async fn cleanup_done(&self) -> Result<()> {
        // Read the current done list
        let (consumer, _): (ConsumerManifest, _) = self.consumer_store.read().await?;
        let done_set: std::collections::HashSet<&str> =
            consumer.done.iter().map(|s| s.as_str()).collect();

        // CAS-loop: remove done locations from producer manifest (iterate, filter, rebuild)
        loop {
            let (producer, version) = self.read_producer_manifest().await?;
            let kept: Vec<ProducerEntry> = producer
                .iter()
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .filter(|entry| !done_set.contains(entry.location.as_str()))
                .collect();
            let producer = ProducerManifest::from_entries(&kept);
            self.counter.record_write();
            match self
                .producer_store
                .write_producer_manifest(&producer, version)
                .await
            {
                Ok(()) => break,
                Err(ManifestWriteError::Conflict) => {
                    self.counter.record_conflict();
                    continue;
                }
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }

        // CAS-loop: clear the flushed locations from consumer done
        loop {
            let (mut consumer, version): (ConsumerManifest, _) = self.consumer_store.read().await?;
            consumer.done.retain(|loc| !done_set.contains(loc.as_str()));
            match self.consumer_store.write(&consumer, version).await {
                Ok(()) => break,
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }

        Ok(())
    }

    pub async fn heartbeat(&self, locations: &[String]) -> Result<()> {
        loop {
            let (mut consumer, version): (ConsumerManifest, _) = self.consumer_store.read().await?;
            let now = millis(self.clock.now());
            for location in locations {
                if consumer.claimed.contains_key(location) {
                    consumer.claimed.insert(location.clone(), now);
                }
            }
            match self.consumer_store.write(&consumer, version).await {
                Ok(()) => return Ok(()),
                Err(ManifestWriteError::Conflict) => continue,
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    pub fn len(&self) -> usize {
        self.producer_len.load(Ordering::Relaxed) as usize
    }

    async fn read_producer_manifest(&self) -> Result<(ProducerManifest, Option<UpdateVersion>)> {
        let result = self.producer_store.read_producer_manifest().await?;
        self.producer_len
            .store(result.0.entries_count() as u64, Ordering::Relaxed);
        Ok(result)
    }

    pub fn conflict_rate(&self) -> f64 {
        self.counter.conflict_rate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::clock::SystemClock;
    use slatedb::object_store::memory::InMemory;

    const TEST_MANIFEST_PATH: &str = "test/manifest";

    fn test_consumer_config() -> ConsumerConfig {
        ConsumerConfig {
            manifest_path: TEST_MANIFEST_PATH.to_string(),
            heartbeat_timeout_ms: 30_000,
            done_cleanup_threshold: 100,
        }
    }

    async fn read_producer_manifest(store: &Arc<dyn ObjectStore>, path: &str) -> ProducerManifest {
        let path = Path::from(path);
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        ProducerManifest::from_bytes(bytes).unwrap()
    }

    async fn read_consumer_manifest(store: &Arc<dyn ObjectStore>, path: &str) -> ConsumerManifest {
        let path = Path::from(path);
        match store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.unwrap();
                serde_json::from_slice(&bytes).unwrap()
            }
            Err(ObjectStoreError::NotFound { .. }) => ConsumerManifest::default(),
            Err(e) => panic!("unexpected error: {}", e),
        }
    }

    #[tokio::test]
    async fn should_enqueue_locations_to_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );

        producer
            .enqueue("path/to/file1.json".to_string(), Bytes::new())
            .await
            .unwrap();
        producer
            .enqueue("path/to/file2.json".to_string(), Bytes::new())
            .await
            .unwrap();

        let manifest = read_producer_manifest(&store, "test/manifest").await;
        let locations: Vec<String> = manifest.iter().map(|e| e.unwrap().location).collect();
        assert_eq!(locations, vec!["path/to/file1.json", "path/to/file2.json"]);
    }

    #[tokio::test]
    async fn should_merge_with_existing_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Pre-populate manifest with binary format
        let existing = ProducerManifest::from_entries(&[ProducerEntry {
            sequence: 0,
            location: "existing/file.json".to_string(),
            ingestion_time_ms: 1000,
            metadata: Bytes::new(),
        }]);
        let path = Path::from("test/manifest");
        store
            .put(&path, PutPayload::from(existing.to_bytes().to_vec()))
            .await
            .unwrap();

        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        producer
            .enqueue("new/file.json".to_string(), Bytes::new())
            .await
            .unwrap();

        let manifest = read_producer_manifest(&store, "test/manifest").await;
        let locations: Vec<String> = manifest.iter().map(|e| e.unwrap().location).collect();
        assert_eq!(locations, vec!["existing/file.json", "new/file.json"]);
    }

    #[tokio::test]
    async fn should_claim_earliest_pending_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        producer
            .enqueue("first.json".to_string(), Bytes::new())
            .await
            .unwrap();
        producer
            .enqueue("second.json".to_string(), Bytes::new())
            .await
            .unwrap();

        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, Some("first.json".to_string()));

        // Producer manifest still has both pending items (read-only from consumer)
        let producer_manifest = read_producer_manifest(&store, "test/manifest").await;
        let locations: Vec<String> = producer_manifest
            .iter()
            .map(|e| e.unwrap().location)
            .collect();
        assert_eq!(locations, vec!["first.json", "second.json"]);

        // Consumer manifest tracks the claim
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(consumer_manifest.claimed.contains_key("first.json"));
    }

    #[tokio::test]
    async fn should_return_none_when_nothing_to_claim() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, None);
    }

    #[tokio::test]
    async fn should_dequeue_claimed_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        producer
            .enqueue("to_process.json".to_string(), Bytes::new())
            .await
            .unwrap();
        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, Some("to_process.json".to_string()));

        let removed = consumer.dequeue("to_process.json").await.unwrap();
        assert!(removed);

        // Producer manifest still has the pending item
        let producer_manifest = read_producer_manifest(&store, "test/manifest").await;
        let locations: Vec<String> = producer_manifest
            .iter()
            .map(|e| e.unwrap().location)
            .collect();
        assert_eq!(locations, vec!["to_process.json"]);

        // Consumer manifest: claimed is empty, done has the location
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(consumer_manifest.claimed.is_empty());
        assert_eq!(consumer_manifest.done, vec!["to_process.json"]);
    }

    #[tokio::test]
    async fn should_return_false_when_dequeuing_unknown_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        let removed = consumer.dequeue("unknown.json").await.unwrap();
        assert!(!removed);
    }

    #[tokio::test]
    async fn should_handle_concurrent_claims() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );

        let n = 5;
        for i in 0..n {
            producer
                .enqueue(format!("file_{}.json", i), Bytes::new())
                .await
                .unwrap();
        }

        let mut join_handles = Vec::new();
        for _ in 0..n {
            let consumer = QueueConsumer::with_object_store(
                test_consumer_config(),
                store.clone(),
                Arc::new(SystemClock),
            )
            .unwrap();
            join_handles.push(tokio::spawn(async move {
                consumer.claim().await.unwrap().unwrap()
            }));
        }

        let mut claimed_locations: Vec<String> = Vec::new();
        for jh in join_handles {
            claimed_locations.push(jh.await.unwrap());
        }

        claimed_locations.sort();
        claimed_locations.dedup();
        assert_eq!(claimed_locations.len(), n);

        // Producer manifest still has all pending items
        let producer_manifest = read_producer_manifest(&store, "test/manifest").await;
        assert_eq!(producer_manifest.entries_count(), n);

        // Consumer manifest has all claimed
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert_eq!(consumer_manifest.claimed.len(), n);
    }

    #[tokio::test]
    async fn should_heartbeat_claimed_locations() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        producer
            .enqueue("b.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer.claim().await.unwrap();
        consumer.claim().await.unwrap();

        let before = millis(SystemTime::now());
        consumer
            .heartbeat(&["a.json".to_string(), "b.json".to_string()])
            .await
            .unwrap();

        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(*consumer_manifest.claimed.get("a.json").unwrap() >= before);
        assert!(*consumer_manifest.claimed.get("b.json").unwrap() >= before);
    }

    #[tokio::test]
    async fn should_overwrite_heartbeat_timestamps() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer.claim().await.unwrap();

        consumer.heartbeat(&["a.json".to_string()]).await.unwrap();
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        let ts1 = *consumer_manifest.claimed.get("a.json").unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        consumer.heartbeat(&["a.json".to_string()]).await.unwrap();
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        let ts2 = *consumer_manifest.claimed.get("a.json").unwrap();

        assert!(ts2 > ts1);
    }

    #[tokio::test]
    async fn should_skip_unknown_locations_in_heartbeat() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer.claim().await.unwrap();

        consumer
            .heartbeat(&["unknown.json".to_string()])
            .await
            .unwrap();

        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(!consumer_manifest.claimed.contains_key("unknown.json"));
        assert_eq!(consumer_manifest.claimed.len(), 1);
    }

    #[tokio::test]
    async fn should_reclaim_stale_location_when_pending_is_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut consumer_config = test_consumer_config();
        consumer_config.heartbeat_timeout_ms = 50;

        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer1 = QueueConsumer::with_object_store(
            consumer_config.clone(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();
        let consumer2 =
            QueueConsumer::with_object_store(consumer_config, store.clone(), Arc::new(SystemClock))
                .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        let claimed = consumer1.claim().await.unwrap();
        assert_eq!(claimed, Some("a.json".to_string()));

        // consumer2 shares the same consumer manifest, so it sees the claim
        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, None);

        // ToDo: use mock clock
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, Some("a.json".to_string()));

        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert_eq!(consumer_manifest.claimed.len(), 1);
        assert!(consumer_manifest.claimed.contains_key("a.json"));
    }

    #[tokio::test]
    async fn should_not_reclaim_location_with_fresh_heartbeat() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut consumer_config = test_consumer_config();
        consumer_config.heartbeat_timeout_ms = 200;

        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer1 = QueueConsumer::with_object_store(
            consumer_config.clone(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();
        let consumer2 =
            QueueConsumer::with_object_store(consumer_config, store.clone(), Arc::new(SystemClock))
                .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer1.claim().await.unwrap();

        // Heartbeat keeps it fresh
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        consumer1.heartbeat(&["a.json".to_string()]).await.unwrap();

        // consumer2 cannot reclaim — heartbeat is fresh
        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, None);
    }

    #[tokio::test]
    async fn should_reclaim_oldest_stale_location() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut consumer_config = test_consumer_config();
        consumer_config.heartbeat_timeout_ms = 50;

        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer = QueueConsumer::with_object_store(
            consumer_config.clone(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        producer
            .enqueue("old.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer.claim().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        producer
            .enqueue("newer.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer.claim().await.unwrap();

        // Wait for both to become stale
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        let reclaimer =
            QueueConsumer::with_object_store(consumer_config, store.clone(), Arc::new(SystemClock))
                .unwrap();
        let claimed = reclaimer.claim().await.unwrap();
        assert_eq!(claimed, Some("old.json".to_string()));
    }

    #[tokio::test]
    async fn should_prefer_stale_claimed_over_pending() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut consumer_config = test_consumer_config();
        consumer_config.heartbeat_timeout_ms = 50;

        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer1 = QueueConsumer::with_object_store(
            consumer_config.clone(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();
        let consumer2 =
            QueueConsumer::with_object_store(consumer_config, store.clone(), Arc::new(SystemClock))
                .unwrap();

        producer
            .enqueue("claimed.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer1.claim().await.unwrap();

        // Wait for heartbeat to expire
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        // Enqueue a new pending item
        producer
            .enqueue("pending.json".to_string(), Bytes::new())
            .await
            .unwrap();

        // consumer2 should prefer the stale claimed item
        let claimed = consumer2.claim().await.unwrap();
        assert_eq!(claimed, Some("claimed.json".to_string()));
    }

    #[tokio::test]
    async fn should_accumulate_done_in_consumer_manifest_after_dequeue() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        producer
            .enqueue("b.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer.claim().await.unwrap();
        consumer.claim().await.unwrap();

        consumer.dequeue("a.json").await.unwrap();
        consumer.dequeue("b.json").await.unwrap();

        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(consumer_manifest.claimed.is_empty());
        assert_eq!(consumer_manifest.done.len(), 2);
        assert!(consumer_manifest.done.contains(&"a.json".to_string()));
        assert!(consumer_manifest.done.contains(&"b.json".to_string()));

        // Producer manifest still has both in pending (no flush yet)
        let producer_manifest = read_producer_manifest(&store, "test/manifest").await;
        assert_eq!(producer_manifest.entries_count(), 2);
    }

    #[tokio::test]
    async fn should_cleanup_done_removes_from_producer_when_threshold_reached() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut consumer_config = test_consumer_config();
        consumer_config.done_cleanup_threshold = 2;

        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer =
            QueueConsumer::with_object_store(consumer_config, store.clone(), Arc::new(SystemClock))
                .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        producer
            .enqueue("b.json".to_string(), Bytes::new())
            .await
            .unwrap();
        producer
            .enqueue("c.json".to_string(), Bytes::new())
            .await
            .unwrap();

        consumer.claim().await.unwrap();
        consumer.claim().await.unwrap();

        // First dequeue — done has 1 item, below threshold
        consumer.dequeue("a.json").await.unwrap();
        let producer_manifest = read_producer_manifest(&store, "test/manifest").await;
        assert_eq!(producer_manifest.entries_count(), 3); // still all three

        // Second dequeue — done reaches threshold (2), triggers cleanup
        consumer.dequeue("b.json").await.unwrap();
        let producer_manifest = read_producer_manifest(&store, "test/manifest").await;
        let locations: Vec<String> = producer_manifest
            .iter()
            .map(|e| e.unwrap().location)
            .collect();
        assert_eq!(locations, vec!["c.json"]);
    }

    #[tokio::test]
    async fn should_cleanup_done_clears_done_list_in_consumer_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut consumer_config = test_consumer_config();
        consumer_config.done_cleanup_threshold = 2;

        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer =
            QueueConsumer::with_object_store(consumer_config, store.clone(), Arc::new(SystemClock))
                .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        producer
            .enqueue("b.json".to_string(), Bytes::new())
            .await
            .unwrap();

        consumer.claim().await.unwrap();
        consumer.claim().await.unwrap();
        consumer.dequeue("a.json").await.unwrap();
        consumer.dequeue("b.json").await.unwrap();

        // After cleanup, consumer done list should be empty
        let consumer_manifest = read_consumer_manifest(&store, "test/manifest.consumer.json").await;
        assert!(consumer_manifest.done.is_empty());

        // Producer pending should also be empty
        let producer_manifest = read_producer_manifest(&store, "test/manifest").await;
        assert!(producer_manifest.is_empty());
    }

    #[tokio::test]
    async fn should_not_reclaim_done_locations() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer = QueueProducer::with_object_store(
            TEST_MANIFEST_PATH.to_string(),
            store.clone(),
            Arc::new(SystemClock),
        );
        let consumer = QueueConsumer::with_object_store(
            test_consumer_config(),
            store.clone(),
            Arc::new(SystemClock),
        )
        .unwrap();

        producer
            .enqueue("a.json".to_string(), Bytes::new())
            .await
            .unwrap();
        consumer.claim().await.unwrap();
        consumer.dequeue("a.json").await.unwrap();

        // "a.json" is still in producer pending but in consumer done — should not be claimed again
        let claimed = consumer.claim().await.unwrap();
        assert_eq!(claimed, None);
    }

    // ---- Unit-test helpers ----

    fn entry(location: &str, time_ms: i64, metadata: &[u8]) -> ProducerEntry {
        ProducerEntry {
            sequence: 0,
            location: location.to_string(),
            ingestion_time_ms: time_ms,
            metadata: Bytes::from(metadata.to_vec()),
        }
    }

    fn entry_seq(seq: u64, location: &str, time_ms: i64, metadata: &[u8]) -> ProducerEntry {
        ProducerEntry {
            sequence: seq,
            location: location.to_string(),
            ingestion_time_ms: time_ms,
            metadata: Bytes::from(metadata.to_vec()),
        }
    }

    fn collect_locations(manifest: &ProducerManifest) -> Vec<String> {
        manifest.iter().map(|e| e.unwrap().location).collect()
    }

    #[test]
    fn should_create_empty_manifest() {
        let m = ProducerManifest::empty();

        assert_eq!(m.entries_count(), 0);
        assert!(m.is_empty());
        assert_eq!(m.iter().count(), 0);

        let bytes = m.to_bytes();
        assert_eq!(bytes.len(), FOOTER_SIZE);
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(bytes[4..12].try_into().unwrap()), 0);
        assert_eq!(
            u16::from_le_bytes(bytes[12..14].try_into().unwrap()),
            PRODUCER_MANIFEST_VERSION
        );
    }

    #[test]
    fn should_parse_valid_manifest_bytes() {
        let entries = vec![entry_seq(0, "a", 1, b"x"), entry_seq(1, "b", 2, b"y")];
        let data = ProducerManifest::from_entries(&entries).to_bytes();

        let m = ProducerManifest::from_bytes(data).unwrap();

        assert_eq!(m.entries_count(), 2);
    }

    #[test]
    fn should_parse_footer_only_bytes() {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u32_le(0);
        buf.put_u64_le(42);
        buf.put_u16_le(PRODUCER_MANIFEST_VERSION);

        let m = ProducerManifest::from_bytes(buf.freeze()).unwrap();

        assert_eq!(m.entries_count(), 0);

        // Appended entry should get sequence 42
        let mut m = m;
        m.append(&entry("loc", 1, b""));
        let entries: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 42);
    }

    #[test]
    fn should_reject_empty_bytes() {
        let err = ProducerManifest::from_bytes(Bytes::new()).unwrap_err();

        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn should_reject_bytes_too_short_for_footer() {
        let err = ProducerManifest::from_bytes(Bytes::from_static(&[0; 13])).unwrap_err();

        assert!(err.to_string().contains("too short for footer"));
    }

    #[test]
    fn should_reject_wrong_version() {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u32_le(0);
        buf.put_u64_le(0);
        buf.put_u16_le(99);

        let err = ProducerManifest::from_bytes(buf.freeze()).unwrap_err();

        assert!(err.to_string().contains("unsupported"));
        assert!(err.to_string().contains("99"));
    }

    #[test]
    fn should_reject_version_zero() {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u32_le(0);
        buf.put_u64_le(0);
        buf.put_u16_le(0);

        let err = ProducerManifest::from_bytes(buf.freeze()).unwrap_err();

        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn should_make_appended_entry_accessible_via_iter() {
        let mut m = ProducerManifest::empty();

        m.append(&entry("loc", 42, b"meta"));

        let entries: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[0].location, "loc");
        assert_eq!(entries[0].ingestion_time_ms, 42);
        assert_eq!(entries[0].metadata, &b"meta"[..]);
    }

    #[test]
    fn should_append_to_existing_base_entries() {
        let base = ProducerManifest::from_entries(&[entry_seq(0, "base", 1, b"")]);
        let data = base.to_bytes();
        let mut m = ProducerManifest::from_bytes(data).unwrap();

        m.append(&entry("appended", 2, b""));

        assert_eq!(m.entries_count(), 2);
        assert_eq!(collect_locations(&m), vec!["base", "appended"]);
        let entries: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[test]
    fn should_preserve_append_order() {
        let mut m = ProducerManifest::empty();

        m.append(&entry("a", 1, b""));
        m.append(&entry("b", 2, b""));
        m.append(&entry("c", 3, b""));

        assert_eq!(collect_locations(&m), vec!["a", "b", "c"]);
        let entries: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
        assert_eq!(entries[2].sequence, 2);
    }

    #[test]
    fn should_create_empty_manifest_from_empty_slice() {
        let mut m = ProducerManifest::from_entries(&[]);

        assert_eq!(m.entries_count(), 0);
        assert!(m.is_empty());

        m.append(&entry("loc", 1, b""));
        let entries: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 0);
    }

    #[test]
    fn should_create_manifest_from_multiple_entries() {
        let entries = vec![
            entry_seq(0, "x", 10, b"m1"),
            entry_seq(1, "y", 20, b"m2"),
            entry_seq(2, "z", 30, b"m3"),
        ];

        let m = ProducerManifest::from_entries(&entries);

        assert_eq!(m.entries_count(), 3);
        assert_eq!(m.next_sequence, 3);
        let decoded: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].sequence, 0);
        assert_eq!(decoded[0].location, "x");
        assert_eq!(decoded[0].ingestion_time_ms, 10);
        assert_eq!(decoded[0].metadata, &b"m1"[..]);
        assert_eq!(decoded[1].sequence, 1);
        assert_eq!(decoded[1].location, "y");
        assert_eq!(decoded[1].ingestion_time_ms, 20);
        assert_eq!(decoded[1].metadata, &b"m2"[..]);
        assert_eq!(decoded[2].sequence, 2);
        assert_eq!(decoded[2].location, "z");
        assert_eq!(decoded[2].ingestion_time_ms, 30);
        assert_eq!(decoded[2].metadata, &b"m3"[..]);
    }

    #[test]
    fn should_handle_entry_with_empty_location() {
        let m = ProducerManifest::from_entries(&[entry_seq(0, "", 0, b"")]);

        let decoded: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].location, "");
        assert_eq!(decoded[0].ingestion_time_ms, 0);
        assert!(decoded[0].metadata.is_empty());
    }

    #[test]
    fn should_handle_entry_with_large_metadata() {
        let big_meta = vec![0xAB_u8; 1024];

        let m = ProducerManifest::from_entries(&[entry_seq(0, "loc", 1, &big_meta)]);

        let decoded: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].metadata.len(), 1024);
        assert_eq!(&decoded[0].metadata[..], &big_meta[..]);
    }

    #[test]
    fn should_handle_negative_ingestion_time() {
        let m = ProducerManifest::from_entries(&[entry_seq(0, "loc", -1000, b"")]);

        let decoded: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].ingestion_time_ms, -1000);
    }

    #[test]
    fn should_return_footer_for_empty_manifest() {
        let m = ProducerManifest::empty();

        let bytes = m.to_bytes();

        assert_eq!(bytes.len(), FOOTER_SIZE);
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(bytes[4..12].try_into().unwrap()), 0);
        assert_eq!(
            u16::from_le_bytes(bytes[12..14].try_into().unwrap()),
            PRODUCER_MANIFEST_VERSION
        );
    }

    #[test]
    fn should_merge_base_and_appended() {
        let base = ProducerManifest::from_entries(&[entry_seq(0, "base", 1, b"")]);
        let mut m = ProducerManifest::from_bytes(base.to_bytes()).unwrap();
        m.append(&entry("appended", 2, b""));

        let serialized = m.to_bytes();
        let reparsed = ProducerManifest::from_bytes(serialized).unwrap();

        assert_eq!(reparsed.entries_count(), 2);
        assert_eq!(collect_locations(&reparsed), vec!["base", "appended"]);
        let entries: Vec<ProducerEntry> = reparsed.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[test]
    fn should_write_correct_footer_count() {
        let base =
            ProducerManifest::from_entries(&[entry_seq(0, "a", 1, b""), entry_seq(1, "b", 2, b"")]);
        let mut m = ProducerManifest::from_bytes(base.to_bytes()).unwrap();
        m.append(&entry("c", 3, b""));
        m.append(&entry("d", 4, b""));
        m.append(&entry("e", 5, b""));

        let bytes = m.to_bytes();

        let footer_start = bytes.len() - FOOTER_SIZE;
        let count = u32::from_le_bytes(bytes[footer_start..footer_start + 4].try_into().unwrap());
        let next_seq = u64::from_le_bytes(
            bytes[footer_start + 4..footer_start + 12]
                .try_into()
                .unwrap(),
        );
        let version = u16::from_le_bytes(bytes[footer_start + 12..].try_into().unwrap());
        assert_eq!(count, 5);
        assert_eq!(next_seq, 5);
        assert_eq!(version, PRODUCER_MANIFEST_VERSION);
    }

    #[test]
    fn should_round_trip_from_entries_to_bytes_from_bytes() {
        let entries = vec![entry_seq(0, "a", 10, b"m1"), entry_seq(1, "b", 20, b"m2")];
        let original = ProducerManifest::from_entries(&entries);

        let reparsed = ProducerManifest::from_bytes(original.to_bytes()).unwrap();

        assert_eq!(reparsed.entries_count(), 2);
        let decoded: Vec<ProducerEntry> = reparsed.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].sequence, 0);
        assert_eq!(decoded[0].location, "a");
        assert_eq!(decoded[0].ingestion_time_ms, 10);
        assert_eq!(decoded[0].metadata, &b"m1"[..]);
        assert_eq!(decoded[1].sequence, 1);
        assert_eq!(decoded[1].location, "b");
        assert_eq!(decoded[1].ingestion_time_ms, 20);
        assert_eq!(decoded[1].metadata, &b"m2"[..]);
    }

    #[test]
    fn should_round_trip_append_serialize_reparse() {
        let mut m = ProducerManifest::empty();
        m.append(&entry("x", 100, b"data"));
        m.append(&entry("y", 200, b"more"));

        let reparsed = ProducerManifest::from_bytes(m.to_bytes()).unwrap();

        assert_eq!(reparsed.entries_count(), 2);
        assert_eq!(collect_locations(&reparsed), vec!["x", "y"]);
    }

    #[test]
    fn should_chain_serialize_reparse_append() {
        let original = ProducerManifest::from_entries(&[entry_seq(0, "a", 1, b"")]);
        let mut m = ProducerManifest::from_bytes(original.to_bytes()).unwrap();
        m.append(&entry("b", 2, b""));

        let mut m2 = ProducerManifest::from_bytes(m.to_bytes()).unwrap();
        m2.append(&entry("c", 3, b""));

        let final_m = ProducerManifest::from_bytes(m2.to_bytes()).unwrap();

        assert_eq!(final_m.entries_count(), 3);
        assert_eq!(collect_locations(&final_m), vec!["a", "b", "c"]);
        let entries: Vec<ProducerEntry> = final_m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[2].sequence, 2);
    }

    #[test]
    fn should_dequeue_entries_through_sequence() {
        let mut m = ProducerManifest::empty();
        for _ in 0..5 {
            m.append(&entry("loc", 1, b""));
        }

        let removed = m.dequeue(2);

        assert_eq!(removed.len(), 3);
        assert_eq!(removed[0].sequence, 0);
        assert_eq!(removed[1].sequence, 1);
        assert_eq!(removed[2].sequence, 2);
        assert_eq!(m.entries_count(), 2);
        let remaining: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(remaining[0].sequence, 3);
        assert_eq!(remaining[1].sequence, 4);
        assert_eq!(m.next_sequence, 5);
    }

    #[test]
    fn should_dequeue_all_entries() {
        let mut m = ProducerManifest::empty();
        for _ in 0..3 {
            m.append(&entry("loc", 1, b""));
        }

        let removed = m.dequeue(2);

        assert_eq!(removed.len(), 3);
        assert!(m.is_empty());
        assert_eq!(m.next_sequence, 3);
    }

    #[test]
    fn should_dequeue_nothing_when_sequence_below_first() {
        let entries = vec![
            entry_seq(5, "a", 1, b""),
            entry_seq(6, "b", 2, b""),
            entry_seq(7, "c", 3, b""),
        ];
        let mut m = ProducerManifest::from_entries(&entries);

        let removed = m.dequeue(3);

        assert!(removed.is_empty());
        assert_eq!(m.entries_count(), 3);
    }

    #[test]
    fn should_append_after_dequeue() {
        let mut m = ProducerManifest::empty();
        for _ in 0..3 {
            m.append(&entry("loc", 1, b""));
        }

        m.dequeue(0);

        assert_eq!(m.entries_count(), 2);
        let remaining: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(remaining[0].sequence, 1);
        assert_eq!(remaining[1].sequence, 2);

        m.append(&entry("new", 4, b""));
        let all: Vec<ProducerEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(all.len(), 3);
        assert_eq!(all[2].sequence, 3);
    }
}

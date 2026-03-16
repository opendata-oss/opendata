use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{BufMut, Bytes, BytesMut};
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    Error as ObjectStoreError, ObjectStore, PutMode, PutPayload, UpdateVersion,
};

use crate::error::{Error, Result};

const MANIFEST_VERSION: u16 = 1;
const UNINITIALIZED_EPOCH: u64 = u64::MAX;
const ENTRY_LEN_SIZE: usize = 4;
const LOCATION_LEN_SIZE: usize = 2;
const INGESTION_TIME_MS_SIZE: usize = 8;
const METADATA_LEN_SIZE: usize = 4;
const START_INDEX_SIZE: usize = 4;
const METADATA_COUNT_SIZE: usize = 4;
const ENTRIES_COUNT_SIZE: usize = 4;
const SEQUENCE_SIZE: usize = 8;
const EPOCH_SIZE: usize = 8;
const VERSION_SIZE: usize = 2;
const FOOTER_SIZE: usize = ENTRIES_COUNT_SIZE + SEQUENCE_SIZE + EPOCH_SIZE + VERSION_SIZE;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Metadata {
    pub(crate) start_index: u32,
    pub(crate) ingestion_time_ms: i64,
    pub(crate) payload: Bytes,
}

#[derive(Debug, Clone)]
pub(crate) struct QueueEntry {
    pub(crate) sequence: u64,
    pub(crate) location: String,
    pub(crate) metadata: Vec<Metadata>,
}

impl QueueEntry {
    fn new(location: String, metadata: Vec<Metadata>) -> Result<Self> {
        if location.len() > u16::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "location length {} exceeds u16::MAX",
                location.len()
            )));
        }
        if metadata.len() > u32::MAX as usize {
            return Err(Error::InvalidInput(format!(
                "metadata count {} exceeds u32::MAX",
                metadata.len()
            )));
        }
        Ok(Self {
            sequence: 0,
            location,
            metadata,
        })
    }

    fn clone_with_sequence(&self, sequence: u64) -> Self {
        Self {
            sequence,
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Manifest {
    data: Bytes,
    appended: BytesMut,
    appended_count: usize,
    next_sequence: u64,
    epoch: u64,
}

impl Manifest {
    /// Create an empty manifest with a valid footer.
    fn empty() -> Self {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u32_le(0);
        buf.put_u64_le(0);
        buf.put_u64_le(0);
        buf.put_u16_le(MANIFEST_VERSION);
        Self {
            data: buf.freeze(),
            appended: BytesMut::new(),
            appended_count: 0,
            next_sequence: 0,
            epoch: 0,
        }
    }

    /// Wrap raw binary data as a queue manifest, validating the footer.
    pub(crate) fn from_bytes(data: Bytes) -> Result<Self> {
        if data.is_empty() {
            return Err(Error::Serialization(
                "queue manifest data must not be empty".to_string(),
            ));
        }
        if data.len() < FOOTER_SIZE {
            return Err(Error::Serialization(
                "queue manifest too short for footer".to_string(),
            ));
        }
        let version_start = data.len() - VERSION_SIZE;
        let version = u16::from_le_bytes(data[version_start..].try_into().unwrap());
        if version != MANIFEST_VERSION {
            return Err(Error::Serialization(format!(
                "unsupported queue manifest version: {}",
                version
            )));
        }
        let epoch_start = data.len() - VERSION_SIZE - EPOCH_SIZE;
        let epoch = u64::from_le_bytes(
            data[epoch_start..epoch_start + EPOCH_SIZE]
                .try_into()
                .unwrap(),
        );
        let next_seq_start = data.len() - VERSION_SIZE - EPOCH_SIZE - SEQUENCE_SIZE;
        let next_sequence = u64::from_le_bytes(
            data[next_seq_start..next_seq_start + SEQUENCE_SIZE]
                .try_into()
                .unwrap(),
        );
        Ok(Self {
            data,
            appended: BytesMut::new(),
            appended_count: 0,
            next_sequence,
            epoch,
        })
    }

    /// Build a manifest from a slice of entries.
    #[cfg(test)]
    fn from_entries(entries: &[QueueEntry]) -> Self {
        let next_sequence = entries.iter().map(|e| e.sequence + 1).max().unwrap_or(0);
        let mut buf = BytesMut::new();
        for entry in entries {
            Self::encode_entry(&mut buf, entry).unwrap();
        }
        buf.put_u32_le(entries.len() as u32);
        buf.put_u64_le(next_sequence);
        buf.put_u64_le(0);
        buf.put_u16_le(MANIFEST_VERSION);
        Self {
            data: buf.freeze(),
            appended: BytesMut::new(),
            appended_count: 0,
            next_sequence,
            epoch: 0,
        }
    }

    /// Number of entries (read from the footer, O(1)).
    fn entries_count(&self) -> usize {
        let base = self.existing_entries_count();
        base + self.appended_count
    }

    /// Whether the manifest contains no entries.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.entries_count() == 0
    }

    /// Return a borrowing iterator that lazily deserializes entries.
    pub(crate) fn iter(&self) -> ManifestIter<'_> {
        let base_count = self.existing_entries_count();
        let entries_end = if self.data.is_empty() {
            0
        } else {
            self.data.len() - FOOTER_SIZE
        };
        ManifestIter {
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
                self.data[footer_start..footer_start + ENTRIES_COUNT_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize
        }
    }

    /// Append a single entry without copying existing data.
    /// The entry is encoded and stored internally; bytes are merged in `to_bytes()`.
    /// The entry's sequence number is overwritten with the manifest's next sequence.
    fn append(&mut self, entry: &QueueEntry) -> Result<()> {
        let sequenced = entry.clone_with_sequence(self.next_sequence);
        Self::encode_entry(&mut self.appended, &sequenced)?;
        self.next_sequence += 1;
        self.appended_count += 1;
        Ok(())
    }

    /// Remove all entries with sequence <= `through_sequence`, returning them.
    ///
    /// Optimized to avoid deserializing/re-serializing remaining entries: only the
    /// removed entries are fully decoded, while remaining entries are byte-copied.
    fn dequeue(&mut self, through_sequence: u64) -> Result<Vec<QueueEntry>> {
        let next_seq = self.next_sequence;
        let epoch = self.epoch;

        let base_count = self.existing_entries_count();
        let entries_end = if self.data.is_empty() {
            0
        } else {
            self.data.len() - FOOTER_SIZE
        };

        let (mut removed, remaining_base_start, remaining_base_count) =
            split_entries(&self.data, base_count, entries_end, through_sequence)?;

        let appended_end = self.appended.len();
        let (appended_removed, remaining_appended_start, remaining_appended_count) = split_entries(
            &self.appended,
            self.appended_count,
            appended_end,
            through_sequence,
        )?;
        removed.extend(appended_removed);

        let remaining_base_bytes = &self.data[remaining_base_start..entries_end];
        let remaining_appended_bytes = &self.appended[remaining_appended_start..appended_end];
        let total_remaining = remaining_base_count + remaining_appended_count;

        let mut buf = BytesMut::with_capacity(
            remaining_base_bytes.len() + remaining_appended_bytes.len() + FOOTER_SIZE,
        );
        buf.extend_from_slice(remaining_base_bytes);
        buf.extend_from_slice(remaining_appended_bytes);
        buf.put_u32_le(total_remaining);
        buf.put_u64_le(next_seq);
        buf.put_u64_le(epoch);
        buf.put_u16_le(MANIFEST_VERSION);

        self.data = buf.freeze();
        self.appended = BytesMut::new();
        self.appended_count = 0;
        self.next_sequence = next_seq;
        self.epoch = epoch;

        Ok(removed)
    }

    /// Set the epoch and patch the data bytes in place.
    fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
        let mut buf = BytesMut::from(self.data.as_ref());
        let epoch_start = buf.len() - VERSION_SIZE - EPOCH_SIZE;
        buf[epoch_start..epoch_start + EPOCH_SIZE].copy_from_slice(&epoch.to_le_bytes());
        self.data = buf.freeze();
    }

    /// Serialize the manifest to bytes for writing to object storage.
    /// When an entry was appended, this merges existing data with the appended
    /// entry and writes a new footer.
    fn to_bytes(&self) -> Result<Bytes> {
        if self.appended.is_empty() {
            return Ok(self.data.clone());
        }
        let (prefix, base_count) = if self.data.is_empty() {
            (&[] as &[u8], 0u32)
        } else {
            let footer_start = self.data.len() - FOOTER_SIZE;
            let count = u32::from_le_bytes(
                self.data[footer_start..footer_start + ENTRIES_COUNT_SIZE]
                    .try_into()
                    .unwrap(),
            );
            (&self.data[..footer_start], count)
        };
        let total_count: u32 = base_count
            .checked_add(self.appended_count as u32)
            .ok_or_else(|| {
                Error::Serialization(format!(
                    "total entry count consisting of {} existing entries + {} appended entries exceeds u32::MAX",
                    base_count, self.appended_count
                ))
            })?;
        let mut buf = BytesMut::with_capacity(prefix.len() + self.appended.len() + FOOTER_SIZE);
        buf.extend_from_slice(prefix);
        buf.extend_from_slice(&self.appended);
        buf.put_u32_le(total_count);
        buf.put_u64_le(self.next_sequence);
        buf.put_u64_le(self.epoch);
        buf.put_u16_le(MANIFEST_VERSION);
        Ok(buf.freeze())
    }

    fn encode_entry(buf: &mut BytesMut, entry: &QueueEntry) -> Result<()> {
        debug_assert!(entry.location.len() <= u16::MAX as usize);
        let metadata_size: usize = METADATA_COUNT_SIZE
            + entry
                .metadata
                .iter()
                .map(|m| {
                    START_INDEX_SIZE + INGESTION_TIME_MS_SIZE + METADATA_LEN_SIZE + m.payload.len()
                })
                .sum::<usize>();
        let entry_body_len =
            SEQUENCE_SIZE + LOCATION_LEN_SIZE + entry.location.len() + metadata_size;
        debug_assert!(entry_body_len <= u32::MAX as usize);
        buf.put_u32_le(entry_body_len as u32);
        buf.put_u64_le(entry.sequence);
        buf.put_u16_le(entry.location.len() as u16);
        buf.extend_from_slice(entry.location.as_bytes());
        debug_assert!(entry.metadata.len() <= u32::MAX as usize);
        buf.put_u32_le(entry.metadata.len() as u32);
        for m in &entry.metadata {
            if m.payload.len() > u32::MAX as usize {
                return Err(Error::InvalidInput(format!(
                    "metadata payload size {} exceeds u32::MAX",
                    m.payload.len()
                )));
            }
            buf.put_u32_le(m.start_index);
            buf.put_i64_le(m.ingestion_time_ms);
            buf.put_u32_le(m.payload.len() as u32);
            buf.extend_from_slice(&m.payload);
        }
        Ok(())
    }
}

/// Walk entries in `data[0..end]`, splitting at `through_sequence`.
/// Entries with sequence <= through_sequence are fully decoded and returned.
/// Returns (removed_entries, remaining_start_offset, remaining_count).
fn split_entries(
    data: &[u8],
    count: usize,
    end: usize,
    through_sequence: u64,
) -> Result<(Vec<QueueEntry>, usize, u32)> {
    let mut removed = Vec::new();
    let mut offset = 0usize;

    for i in 0..count {
        let entry_start = offset;
        let entry = decode_entry(data, &mut offset, end)?;

        if entry.sequence <= through_sequence {
            removed.push(entry);
        } else {
            return Ok((removed, entry_start, (count - i) as u32));
        }
    }

    Ok((removed, end, 0))
}

/// Decode a single entry from binary data at the given offset.
fn decode_entry(data: &[u8], offset: &mut usize, end: usize) -> Result<QueueEntry> {
    if *offset + ENTRY_LEN_SIZE > end {
        return Err(Error::Serialization(
            "queue entry corrupt: size of entry length field does not fit in entry".to_string(),
        ));
    }

    let entry_len =
        u32::from_le_bytes(data[*offset..*offset + ENTRY_LEN_SIZE].try_into().unwrap()) as usize;
    *offset += ENTRY_LEN_SIZE;

    if *offset + entry_len > end {
        return Err(Error::Serialization(
            "queue entry corrupt: entry has less bytes than set in the entry length".to_string(),
        ));
    }

    let entry_end = *offset + entry_len;

    let sequence = u64::from_le_bytes(data[*offset..*offset + SEQUENCE_SIZE].try_into().unwrap());
    *offset += SEQUENCE_SIZE;

    let location_len = u16::from_le_bytes(
        data[*offset..*offset + LOCATION_LEN_SIZE]
            .try_into()
            .unwrap(),
    ) as usize;
    *offset += LOCATION_LEN_SIZE;

    let min_entry_len = SEQUENCE_SIZE + LOCATION_LEN_SIZE + location_len + METADATA_COUNT_SIZE;
    if entry_len < min_entry_len {
        return Err(Error::Serialization(format!(
            "queue entry corrupt: entry length {} is less than minimum entry length {} for the length of the location {}",
            entry_len, min_entry_len, location_len
        )));
    }

    let location = String::from_utf8(data[*offset..*offset + location_len].to_vec())
        .map_err(|e| Error::Serialization(e.to_string()))?;
    *offset += location_len;

    let metadata_count = u32::from_le_bytes(
        data[*offset..*offset + METADATA_COUNT_SIZE]
            .try_into()
            .unwrap(),
    ) as usize;
    *offset += METADATA_COUNT_SIZE;

    let mut metadata = Vec::with_capacity(metadata_count);
    for _ in 0..metadata_count {
        if *offset + START_INDEX_SIZE > end {
            return Err(Error::Serialization(
                "queue entry corrupt: size of start index field does not fit in entry".to_string(),
            ));
        }
        let start_index = u32::from_le_bytes(
            data[*offset..*offset + START_INDEX_SIZE]
                .try_into()
                .unwrap(),
        );
        *offset += START_INDEX_SIZE;

        if *offset + INGESTION_TIME_MS_SIZE > end {
            return Err(Error::Serialization(
                "queue entry corrupt: size of ingestion time field does not fit in entry"
                    .to_string(),
            ));
        }
        let ingestion_time_ms = i64::from_le_bytes(
            data[*offset..*offset + INGESTION_TIME_MS_SIZE]
                .try_into()
                .unwrap(),
        );
        *offset += INGESTION_TIME_MS_SIZE;

        if *offset + METADATA_LEN_SIZE > end {
            return Err(Error::Serialization(
                "queue entry corrupt: size of metadata length field does not fit in entry"
                    .to_string(),
            ));
        }
        let m_len = u32::from_le_bytes(
            data[*offset..*offset + METADATA_LEN_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;
        *offset += METADATA_LEN_SIZE;

        if *offset + m_len > end {
            return Err(Error::Serialization(
                "queue entry corrupt: metadata has less bytes than set in the metadata length"
                    .to_string(),
            ));
        }
        metadata.push(Metadata {
            start_index,
            ingestion_time_ms,
            payload: Bytes::copy_from_slice(&data[*offset..*offset + m_len]),
        });
        *offset += m_len;
    }

    *offset = entry_end;

    Ok(QueueEntry {
        sequence,
        location,
        metadata,
    })
}

/// Borrowing iterator over manifest entries. Lazily deserializes each entry.
pub(crate) struct ManifestIter<'a> {
    data: &'a [u8],
    offset: usize,
    remaining: usize,
    entries_end: usize,
    appended: &'a [u8],
    appended_offset: usize,
    appended_remaining: usize,
}

impl Iterator for ManifestIter<'_> {
    type Item = Result<QueueEntry>;

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
        } else if self.offset != self.entries_end {
            let err = Some(Err(Error::Serialization(format!(
                "base entries did not consume all bytes: offset {} != entries_end {}",
                self.offset, self.entries_end
            ))));
            self.offset = self.entries_end;
            err
        } else {
            None
        }
    }
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
    async fn read(&self) -> Result<(Manifest, Option<UpdateVersion>)> {
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
                let manifest = Manifest::from_bytes(bytes)?;
                Ok((manifest, Some(version)))
            }
            Err(ObjectStoreError::NotFound { .. }) => Ok((Manifest::empty(), None)),
            Err(e) => Err(Error::Storage(e.to_string())),
        }
    }

    async fn write(
        &self,
        manifest: &Manifest,
        version: Option<UpdateVersion>,
    ) -> std::result::Result<(), ManifestWriteError> {
        let path = Path::from(self.manifest_path.as_str());
        let put_mode = match version {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        let data = manifest.to_bytes().map_err(ManifestWriteError::Fatal)?;

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
        let rate = (conflicts as f64 / writes as f64) * 100.0;
        rate.min(100.0)
    }
}

/// A producer that appends entries to a shared manifest in object storage.
///
/// Writes use optimistic concurrency: the manifest is read, modified locally,
/// and written back with a conditional put. On conflict the operation is
/// retried automatically until it succeeds.
pub struct QueueProducer {
    manifest_store: ManifestStore,
    counter: ConflictCounter,
}

impl QueueProducer {
    /// Create a new producer backed by the given [`ObjectStore`].
    pub fn with_object_store(manifest_path: String, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            manifest_store: ManifestStore {
                object_store,
                manifest_path,
            },
            counter: ConflictCounter::new(),
        }
    }

    /// Append an entry to the queue with the given `location` and `metadata`.
    ///
    /// Returns [`Error::InvalidInput`] if `location` exceeds 65 535 bytes or
    /// `metadata` exceeds 2³²−1 items. The write is retried automatically on
    /// optimistic-concurrency conflicts.
    pub async fn enqueue(&self, location: String, metadata: Vec<Metadata>) -> Result<()> {
        let entry = QueueEntry::new(location, metadata)?;
        loop {
            let (mut manifest, version) = self.manifest_store.read().await?;
            manifest.append(&entry)?;
            self.counter.record_write();
            match self.manifest_store.write(&manifest, version).await {
                Ok(()) => return Ok(()),
                Err(ManifestWriteError::Conflict) => {
                    self.counter.record_conflict();
                    continue;
                }
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    /// Return the percentage of manifest writes that encountered a conflict.
    pub fn conflict_rate(&self) -> f64 {
        self.counter.conflict_rate()
    }
}

/// A consumer that reads and dequeues entries from a shared manifest in
/// object storage.
///
/// Single-consumer semantics are enforced through an epoch stored in the
/// manifest. Calling [`QueueConsumer::initialize`] increments the epoch,
/// fencing any previous consumer instance. Every subsequent read or dequeue
/// checks that the local epoch still matches the manifest, returning
/// [`Error::Fenced`] if another consumer has taken over.
pub struct QueueConsumer {
    manifest_store: ManifestStore,
    epoch: AtomicU64,
    counter: ConflictCounter,
    queue_len: AtomicU64,
}

impl QueueConsumer {
    /// Create a new consumer backed by the given [`ObjectStore`].
    ///
    /// The consumer is not active until [`QueueConsumer::initialize`] is called.
    pub fn with_object_store(manifest_path: String, object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            manifest_store: ManifestStore {
                object_store,
                manifest_path,
            },
            epoch: AtomicU64::new(UNINITIALIZED_EPOCH),
            counter: ConflictCounter::new(),
            queue_len: AtomicU64::new(0),
        }
    }

    /// Initialize the consumer by incrementing the epoch in the queue manifest.
    /// This fences any previous consumer that was using the old epoch.
    pub async fn initialize(&self) -> Result<()> {
        loop {
            let (mut manifest, version) = self.read_manifest().await?;
            let mut new_epoch = manifest.epoch.wrapping_add(1);
            if new_epoch == UNINITIALIZED_EPOCH {
                new_epoch = new_epoch.wrapping_add(1);
            }
            manifest.set_epoch(new_epoch);
            match self.write_manifest(&manifest, version).await {
                Ok(()) => {
                    self.epoch.store(new_epoch, Ordering::Relaxed);
                    return Ok(());
                }
                Err(ManifestWriteError::Conflict) => {
                    self.counter.record_conflict();
                    continue;
                }
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    /// Return the first entry in the queue without dequeueing it.
    /// Returns `Fenced` if the consumer's epoch does not match the manifest's epoch.
    pub(crate) async fn peek(&self) -> Result<Option<QueueEntry>> {
        let (manifest, _) = self.read_manifest().await?;
        if manifest.epoch != self.epoch.load(Ordering::Relaxed) {
            return Err(Error::Fenced);
        }
        manifest.iter().next().transpose()
    }

    /// Return the entry with the given sequence number, or None if not found.
    /// Returns `Fenced` if the consumer's epoch does not match the manifest's epoch.
    pub(crate) async fn read(&self, sequence: u64) -> Result<Option<QueueEntry>> {
        let (manifest, _) = self.read_manifest().await?;
        if manifest.epoch != self.epoch.load(Ordering::Relaxed) {
            return Err(Error::Fenced);
        }
        manifest
            .iter()
            .find(|e| matches!(e, Ok(e) if e.sequence == sequence))
            .transpose()
    }

    /// Remove all entries with sequence <= `through_sequence`, returning the removed entries.
    /// Returns `Fenced` if the consumer's epoch does not match the manifest's epoch.
    pub(crate) async fn dequeue(&self, through_sequence: u64) -> Result<Vec<QueueEntry>> {
        loop {
            let (mut manifest, version) = self.read_manifest().await?;
            if manifest.epoch != self.epoch.load(Ordering::Relaxed) {
                return Err(Error::Fenced);
            }
            let removed = manifest.dequeue(through_sequence)?;
            match self.write_manifest(&manifest, version).await {
                Ok(()) => return Ok(removed),
                Err(ManifestWriteError::Conflict) => {
                    self.counter.record_conflict();
                    continue;
                }
                Err(ManifestWriteError::Fatal(e)) => return Err(e),
            }
        }
    }

    /// Return the number of entries in the queue as of the last manifest read or write.
    pub fn len(&self) -> usize {
        self.queue_len.load(Ordering::Relaxed) as usize
    }

    async fn read_manifest(&self) -> Result<(Manifest, Option<UpdateVersion>)> {
        let result = self.manifest_store.read().await?;
        self.queue_len
            .store(result.0.entries_count() as u64, Ordering::Relaxed);
        Ok(result)
    }

    async fn write_manifest(
        &self,
        manifest: &Manifest,
        version: Option<UpdateVersion>,
    ) -> std::result::Result<(), ManifestWriteError> {
        self.counter.record_write();
        let result = self.manifest_store.write(manifest, version).await;
        if result.is_ok() {
            self.queue_len
                .store(manifest.entries_count() as u64, Ordering::Relaxed);
        }
        result
    }

    /// Return the percentage of manifest writes that encountered a conflict.
    pub fn conflict_rate(&self) -> f64 {
        self.counter.conflict_rate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    const TEST_MANIFEST_PATH: &str = "test/manifest";

    async fn read_producer_manifest(store: &Arc<dyn ObjectStore>, path: &str) -> Manifest {
        let path = Path::from(path);
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        Manifest::from_bytes(bytes).unwrap()
    }

    #[tokio::test]
    async fn should_initialize_consumer_and_increment_epoch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        consumer.initialize().await.unwrap();

        let manifest = read_producer_manifest(&store, TEST_MANIFEST_PATH).await;
        assert_eq!(manifest.epoch, 1);
    }

    #[tokio::test]
    async fn should_peek_none_when_queue_is_empty() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer.initialize().await.unwrap();

        let result = consumer.peek().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_read_entry_by_sequence() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        producer
            .enqueue("a.batch".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("b.batch".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("c.batch".to_string(), vec![])
            .await
            .unwrap();

        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer.initialize().await.unwrap();

        let entry = consumer.read(1).await.unwrap().unwrap();
        assert_eq!(entry.location, "b.batch");
        assert_eq!(entry.sequence, 1);
    }

    #[tokio::test]
    async fn should_read_none_for_missing_sequence() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        producer
            .enqueue("a.batch".to_string(), vec![])
            .await
            .unwrap();

        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer.initialize().await.unwrap();

        let result = consumer.read(99).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_fence_old_consumer_on_peek() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer_a =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer_a.initialize().await.unwrap();

        let consumer_b =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer_b.initialize().await.unwrap();

        let result = consumer_a.peek().await;
        assert!(matches!(result, Err(Error::Fenced)));
    }

    #[tokio::test]
    async fn should_fence_old_consumer_on_dequeue() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let consumer_a =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer_a.initialize().await.unwrap();

        let consumer_b =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer_b.initialize().await.unwrap();

        let result = consumer_a.dequeue(0).await;
        assert!(matches!(result, Err(Error::Fenced)));
    }

    #[tokio::test]
    async fn should_fence_uninitialized_consumer() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        producer
            .enqueue("a.batch".to_string(), vec![])
            .await
            .unwrap();

        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        let result = consumer.peek().await;
        assert!(matches!(result, Err(Error::Fenced)));
    }

    #[tokio::test]
    async fn should_wrap_epoch_to_zero_at_max() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let mut manifest = Manifest::empty();
        manifest.set_epoch(u64::MAX - 1);
        let path = Path::from(TEST_MANIFEST_PATH);
        store
            .put(
                &path,
                PutPayload::from(manifest.to_bytes().unwrap().to_vec()),
            )
            .await
            .unwrap();

        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer.initialize().await.unwrap();

        let manifest = read_producer_manifest(&store, TEST_MANIFEST_PATH).await;
        assert_eq!(manifest.epoch, 0);
    }

    #[tokio::test]
    async fn should_peek_first_entry_with_valid_epoch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        producer
            .enqueue("a.batch".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("b.batch".to_string(), vec![])
            .await
            .unwrap();

        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer.initialize().await.unwrap();

        let entry = consumer.peek().await.unwrap().unwrap();
        assert_eq!(entry.location, "a.batch");
    }

    #[tokio::test]
    async fn should_dequeue_entries_with_valid_epoch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        producer
            .enqueue("a.batch".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("b.batch".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("c.batch".to_string(), vec![])
            .await
            .unwrap();

        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer.initialize().await.unwrap();

        let removed = consumer.dequeue(1).await.unwrap();
        assert_eq!(removed.len(), 2);
        assert_eq!(removed[0].location, "a.batch");
        assert_eq!(removed[1].location, "b.batch");

        let next = consumer.peek().await.unwrap().unwrap();
        assert_eq!(next.location, "c.batch");
    }

    #[tokio::test]
    async fn should_enqueue_after_consumer_dequeue() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        producer
            .enqueue("a.batch".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("b.batch".to_string(), vec![])
            .await
            .unwrap();

        let consumer =
            QueueConsumer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        consumer.initialize().await.unwrap();

        consumer.dequeue(1).await.unwrap();

        producer
            .enqueue("c.batch".to_string(), vec![])
            .await
            .unwrap();

        let next = consumer.peek().await.unwrap().unwrap();
        assert_eq!(next.location, "c.batch");
        assert_eq!(next.sequence, 2);
    }

    #[tokio::test]
    async fn should_enqueue_locations_to_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        producer
            .enqueue("path/to/file1.batch".to_string(), vec![])
            .await
            .unwrap();
        producer
            .enqueue("path/to/file2.batch".to_string(), vec![])
            .await
            .unwrap();

        let manifest = read_producer_manifest(&store, TEST_MANIFEST_PATH).await;
        let locations: Vec<String> = manifest.iter().map(|e| e.unwrap().location).collect();
        assert_eq!(
            locations,
            vec!["path/to/file1.batch", "path/to/file2.batch"]
        );
    }

    #[tokio::test]
    async fn should_merge_with_existing_manifest() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let existing = Manifest::from_entries(&[QueueEntry {
            sequence: 0,
            location: "existing/file.batch".to_string(),
            metadata: vec![],
        }]);
        let path = Path::from(TEST_MANIFEST_PATH);
        store
            .put(
                &path,
                PutPayload::from(existing.to_bytes().unwrap().to_vec()),
            )
            .await
            .unwrap();

        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());
        producer
            .enqueue("new/file.batch".to_string(), vec![])
            .await
            .unwrap();

        let manifest = read_producer_manifest(&store, "test/manifest").await;
        let locations: Vec<String> = manifest.iter().map(|e| e.unwrap().location).collect();
        assert_eq!(locations, vec!["existing/file.batch", "new/file.batch"]);
    }

    fn entry(location: &str, metadata: Vec<Metadata>) -> QueueEntry {
        QueueEntry::new(location.to_string(), metadata).unwrap()
    }

    fn entry_seq(seq: u64, location: &str, metadata: Vec<Metadata>) -> QueueEntry {
        QueueEntry {
            sequence: seq,
            location: location.to_string(),
            metadata,
        }
    }

    fn meta(start_index: u32, time_ms: i64, data: &str) -> Metadata {
        Metadata {
            start_index,
            ingestion_time_ms: time_ms,
            payload: Bytes::from(data.to_string()),
        }
    }

    fn collect_locations(manifest: &Manifest) -> Vec<String> {
        manifest.iter().map(|e| e.unwrap().location).collect()
    }

    #[test]
    fn should_create_empty_manifest() {
        let m = Manifest::empty();

        assert_eq!(m.entries_count(), 0);
        assert!(m.is_empty());
        assert_eq!(m.epoch, 0);

        let bytes = m.to_bytes().unwrap();
        assert_eq!(bytes.len(), FOOTER_SIZE);
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(bytes[4..12].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(bytes[12..20].try_into().unwrap()), 0);
        assert_eq!(
            u16::from_le_bytes(bytes[20..22].try_into().unwrap()),
            MANIFEST_VERSION
        );
    }

    #[test]
    fn should_parse_valid_manifest_bytes() {
        let entries = vec![
            entry_seq(0, "a", vec![meta(0, 1, "x")]),
            entry_seq(1, "b", vec![meta(0, 2, "y")]),
        ];
        let data = Manifest::from_entries(&entries).to_bytes().unwrap();

        let m = Manifest::from_bytes(data).unwrap();

        assert_eq!(m.entries_count(), 2);
    }

    #[test]
    fn should_parse_footer_only_bytes() {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u32_le(0);
        buf.put_u64_le(42);
        buf.put_u64_le(0);
        buf.put_u16_le(MANIFEST_VERSION);

        let m = Manifest::from_bytes(buf.freeze()).unwrap();

        assert_eq!(m.entries_count(), 0);
        assert_eq!(m.epoch, 0);

        let mut m = m;
        m.append(&entry("loc", vec![])).unwrap();
        let entries: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 42);
    }

    #[test]
    fn should_reject_empty_bytes() {
        let err = Manifest::from_bytes(Bytes::new()).unwrap_err();

        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn should_reject_bytes_too_short_for_footer() {
        let err = Manifest::from_bytes(Bytes::from_static(&[0; 21])).unwrap_err();

        assert!(err.to_string().contains("too short for footer"));
    }

    #[test]
    fn should_reject_wrong_version() {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u32_le(0);
        buf.put_u64_le(0);
        buf.put_u64_le(0);
        buf.put_u16_le(99);

        let err = Manifest::from_bytes(buf.freeze()).unwrap_err();

        assert!(err.to_string().contains("unsupported"));
        assert!(err.to_string().contains("99"));
    }

    #[test]
    fn should_reject_version_zero() {
        let mut buf = BytesMut::with_capacity(FOOTER_SIZE);
        buf.put_u32_le(0);
        buf.put_u64_le(0);
        buf.put_u64_le(0);
        buf.put_u16_le(0);

        let err = Manifest::from_bytes(buf.freeze()).unwrap_err();

        assert!(err.to_string().contains("unsupported"));
    }

    #[test]
    fn should_make_appended_entry_accessible_via_iter() {
        let mut m = Manifest::empty();

        m.append(&entry("loc", vec![meta(0, 42, "meta")])).unwrap();

        let entries: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[0].location, "loc");
        assert_eq!(entries[0].metadata, vec![meta(0, 42, "meta")]);
    }

    #[test]
    fn should_append_to_existing_base_entries() {
        let base = Manifest::from_entries(&[entry_seq(0, "base", vec![])]);
        let data = base.to_bytes().unwrap();
        let mut m = Manifest::from_bytes(data).unwrap();

        m.append(&entry("appended", vec![])).unwrap();

        assert_eq!(m.entries_count(), 2);
        assert_eq!(collect_locations(&m), vec!["base", "appended"]);
        let entries: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[test]
    fn should_preserve_append_order() {
        let mut m = Manifest::empty();

        m.append(&entry("a", vec![])).unwrap();
        m.append(&entry("b", vec![])).unwrap();
        m.append(&entry("c", vec![])).unwrap();

        assert_eq!(collect_locations(&m), vec!["a", "b", "c"]);
        let entries: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
        assert_eq!(entries[2].sequence, 2);
    }

    #[test]
    fn should_handle_entry_with_empty_location() {
        let m = Manifest::from_entries(&[entry_seq(0, "", vec![])]);

        let decoded: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].location, "");
        assert!(decoded[0].metadata.is_empty());
    }

    #[test]
    fn should_handle_entry_with_large_metadata() {
        let big_meta = Bytes::from(vec![0xAB_u8; 1024]);

        let m = Manifest::from_entries(&[entry_seq(
            0,
            "loc",
            vec![Metadata {
                start_index: 0,
                ingestion_time_ms: 1,
                payload: big_meta.clone(),
            }],
        )]);

        let decoded: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].metadata.len(), 1);
        assert_eq!(decoded[0].metadata[0].payload, big_meta);
    }

    #[test]
    fn should_handle_negative_ingestion_time() {
        let m = Manifest::from_entries(&[entry_seq(0, "loc", vec![meta(0, -1000, "")])]);

        let decoded: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].metadata[0].ingestion_time_ms, -1000);
    }

    #[test]
    fn should_return_footer_for_empty_manifest() {
        let m = Manifest::empty();

        let bytes = m.to_bytes().unwrap();

        assert_eq!(bytes.len(), FOOTER_SIZE);
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(bytes[4..12].try_into().unwrap()), 0);
        assert_eq!(u64::from_le_bytes(bytes[12..20].try_into().unwrap()), 0);
        assert_eq!(
            u16::from_le_bytes(bytes[20..22].try_into().unwrap()),
            MANIFEST_VERSION
        );
    }

    #[test]
    fn should_merge_base_and_appended() {
        let base = Manifest::from_entries(&[entry_seq(0, "base", vec![])]);
        let mut m = Manifest::from_bytes(base.to_bytes().unwrap()).unwrap();
        m.append(&entry("appended", vec![])).unwrap();

        let serialized = m.to_bytes().unwrap();
        let reparsed = Manifest::from_bytes(serialized).unwrap();

        assert_eq!(reparsed.entries_count(), 2);
        assert_eq!(collect_locations(&reparsed), vec!["base", "appended"]);
        let entries: Vec<QueueEntry> = reparsed.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[0].sequence, 0);
        assert_eq!(entries[1].sequence, 1);
    }

    #[test]
    fn should_write_correct_footer_count() {
        let base = Manifest::from_entries(&[entry_seq(0, "a", vec![]), entry_seq(1, "b", vec![])]);
        let mut m = Manifest::from_bytes(base.to_bytes().unwrap()).unwrap();
        m.append(&entry("c", vec![])).unwrap();
        m.append(&entry("d", vec![])).unwrap();
        m.append(&entry("e", vec![])).unwrap();

        let bytes = m.to_bytes().unwrap();

        let footer_start = bytes.len() - FOOTER_SIZE;
        let count = u32::from_le_bytes(bytes[footer_start..footer_start + 4].try_into().unwrap());
        let next_seq = u64::from_le_bytes(
            bytes[footer_start + 4..footer_start + 12]
                .try_into()
                .unwrap(),
        );
        let epoch = u64::from_le_bytes(
            bytes[footer_start + 12..footer_start + 20]
                .try_into()
                .unwrap(),
        );
        let version = u16::from_le_bytes(bytes[footer_start + 20..].try_into().unwrap());
        assert_eq!(count, 5);
        assert_eq!(next_seq, 5);
        assert_eq!(epoch, 0);
        assert_eq!(version, MANIFEST_VERSION);
    }

    #[test]
    fn should_round_trip_from_entries_to_bytes_from_bytes() {
        let entries = vec![
            entry_seq(0, "a", vec![meta(0, 10, "m1")]),
            entry_seq(1, "b", vec![meta(0, 20, "m2")]),
        ];
        let original = Manifest::from_entries(&entries);

        let reparsed = Manifest::from_bytes(original.to_bytes().unwrap()).unwrap();

        assert_eq!(reparsed.entries_count(), 2);
        let decoded: Vec<QueueEntry> = reparsed.iter().map(|e| e.unwrap()).collect();
        assert_eq!(decoded[0].sequence, 0);
        assert_eq!(decoded[0].location, "a");
        assert_eq!(decoded[0].metadata, vec![meta(0, 10, "m1")]);
        assert_eq!(decoded[1].sequence, 1);
        assert_eq!(decoded[1].location, "b");
        assert_eq!(decoded[1].metadata, vec![meta(0, 20, "m2")]);
    }

    #[test]
    fn should_round_trip_append_serialize_reparse() {
        let mut m = Manifest::empty();
        m.append(&entry("x", vec![meta(0, 100, "data")])).unwrap();
        m.append(&entry("y", vec![meta(0, 200, "more")])).unwrap();

        let reparsed = Manifest::from_bytes(m.to_bytes().unwrap()).unwrap();

        assert_eq!(reparsed.entries_count(), 2);
        assert_eq!(collect_locations(&reparsed), vec!["x", "y"]);
    }

    #[test]
    fn should_chain_serialize_reparse_append() {
        let original = Manifest::from_entries(&[entry_seq(0, "a", vec![])]);
        let mut m = Manifest::from_bytes(original.to_bytes().unwrap()).unwrap();
        m.append(&entry("b", vec![])).unwrap();

        let mut m2 = Manifest::from_bytes(m.to_bytes().unwrap()).unwrap();
        m2.append(&entry("c", vec![])).unwrap();

        let final_m = Manifest::from_bytes(m2.to_bytes().unwrap()).unwrap();

        assert_eq!(final_m.entries_count(), 3);
        assert_eq!(collect_locations(&final_m), vec!["a", "b", "c"]);
        let entries: Vec<QueueEntry> = final_m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(entries[2].sequence, 2);
    }

    #[test]
    fn should_dequeue_entries_through_sequence() {
        let mut m = Manifest::empty();
        for _ in 0..5 {
            m.append(&entry("loc", vec![])).unwrap();
        }

        let removed = m.dequeue(2).unwrap();

        assert_eq!(removed.len(), 3);
        assert_eq!(removed[0].sequence, 0);
        assert_eq!(removed[1].sequence, 1);
        assert_eq!(removed[2].sequence, 2);
        assert_eq!(m.entries_count(), 2);
        let remaining: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(remaining[0].sequence, 3);
        assert_eq!(remaining[1].sequence, 4);
        assert_eq!(m.next_sequence, 5);
    }

    #[test]
    fn should_dequeue_all_entries() {
        let mut m = Manifest::empty();
        for _ in 0..3 {
            m.append(&entry("loc", vec![])).unwrap();
        }

        let removed = m.dequeue(2).unwrap();

        assert_eq!(removed.len(), 3);
        assert!(m.is_empty());
        assert_eq!(m.next_sequence, 3);
    }

    #[test]
    fn should_dequeue_nothing_when_sequence_below_first() {
        let entries = vec![
            entry_seq(5, "a", vec![]),
            entry_seq(6, "b", vec![]),
            entry_seq(7, "c", vec![]),
        ];
        let mut m = Manifest::from_entries(&entries);

        let removed = m.dequeue(3).unwrap();

        assert!(removed.is_empty());
        assert_eq!(m.entries_count(), 3);
    }

    #[test]
    fn should_append_after_dequeue() {
        let mut m = Manifest::empty();
        for _ in 0..3 {
            m.append(&entry("loc", vec![])).unwrap();
        }

        m.dequeue(0).unwrap();

        assert_eq!(m.entries_count(), 2);
        let remaining: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(remaining[0].sequence, 1);
        assert_eq!(remaining[1].sequence, 2);

        m.append(&entry("new", vec![])).unwrap();
        let all: Vec<QueueEntry> = m.iter().map(|e| e.unwrap()).collect();
        assert_eq!(all.len(), 3);
        assert_eq!(all[2].sequence, 3);
    }

    /// Serialize a single entry into raw bytes (without footer).
    fn encode_entry_bytes(entry: &QueueEntry) -> Vec<u8> {
        let mut buf = BytesMut::new();
        Manifest::encode_entry(&mut buf, entry).unwrap();
        buf.to_vec()
    }

    /// Wrap raw entry bytes with a manifest footer (entry_count=1) so that
    /// `Manifest::from_bytes` + `.iter()` exercises `decode_entry`.
    fn manifest_from_raw_entry(entry_bytes: &[u8]) -> Manifest {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(entry_bytes);
        buf.put_u32_le(1); // entry_count
        buf.put_u64_le(1); // next_sequence
        buf.put_u64_le(0); // epoch
        buf.put_u16_le(MANIFEST_VERSION);
        Manifest::from_bytes(buf.freeze()).unwrap()
    }

    /// Offset of metadata_count inside the encoded entry (for location "a").
    /// Layout: entry_len(4) + sequence(8) + location_len(2) + location(1)
    const METADATA_COUNT_OFFSET: usize = ENTRY_LEN_SIZE + SEQUENCE_SIZE + LOCATION_LEN_SIZE + 1;

    /// Build an entry with no metadata, then corrupt metadata_count to `count`
    /// and extend the buffer with `extra_bytes` after metadata_count to simulate
    /// partial metadata. Also patches entry_len to match the new total size.
    fn corrupt_metadata_entry(count: u32, extra_bytes: &[u8]) -> Vec<u8> {
        let e = QueueEntry {
            sequence: 1,
            location: "a".to_string(),
            metadata: vec![],
        };
        let mut raw = encode_entry_bytes(&e);
        // Overwrite metadata_count
        raw[METADATA_COUNT_OFFSET..METADATA_COUNT_OFFSET + 4].copy_from_slice(&count.to_le_bytes());
        // Append extra bytes (partial metadata fields)
        raw.extend_from_slice(extra_bytes);
        // Patch entry_len to cover the full buffer after the 4-byte prefix
        let new_entry_len = (raw.len() - ENTRY_LEN_SIZE) as u32;
        raw[..ENTRY_LEN_SIZE].copy_from_slice(&new_entry_len.to_le_bytes());
        raw
    }

    #[test]
    fn should_reject_trailing_bytes_before_footer() {
        // Build a valid entry, then add garbage bytes before the footer.
        let mut raw = encode_entry_bytes(&entry_seq(0, "loc", vec![]));
        raw.extend_from_slice(&[0xFFu8; 5]); // trailing garbage

        let manifest = manifest_from_raw_entry(&raw);
        let items: Vec<Result<QueueEntry>> = manifest.iter().collect();
        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok());
        let err = items[1].as_ref().unwrap_err();
        assert!(
            err.to_string().contains("did not consume all bytes"),
            "got: {}",
            err
        );
    }

    #[test]
    fn should_reject_entry_with_entry_len_below_minimum() {
        // entry_len too small: less than SEQUENCE_SIZE + LOCATION_LEN_SIZE + METADATA_COUNT_SIZE (14)
        let bad_entry_len = (SEQUENCE_SIZE + LOCATION_LEN_SIZE + METADATA_COUNT_SIZE - 1) as u32;
        let mut raw = Vec::new();
        raw.extend_from_slice(&bad_entry_len.to_le_bytes());
        raw.extend_from_slice(&[0u8; 13]); // enough raw bytes to not truncate

        let manifest = manifest_from_raw_entry(&raw);
        let err = manifest.iter().next().unwrap().unwrap_err();
        assert!(err.to_string().contains(
            "entry length 13 is less than minimum entry length 14 for the length of the location 0"
        ));
    }

    #[test]
    fn should_reject_entry_with_entry_len_below_minimum_for_location() {
        let location = "abc";
        // entry_len covers fixed fields but not the full location
        let bad_entry_len =
            (SEQUENCE_SIZE + LOCATION_LEN_SIZE + METADATA_COUNT_SIZE + location.len() - 1) as u32;
        let mut raw = Vec::new();
        raw.extend_from_slice(&bad_entry_len.to_le_bytes());
        raw.extend_from_slice(&0u64.to_le_bytes()); // sequence
        raw.extend_from_slice(&(location.len() as u16).to_le_bytes()); // location_len
        raw.extend_from_slice(&[0u8; 20]); // padding so entry doesn't extend beyond data

        let manifest = manifest_from_raw_entry(&raw);
        let err = manifest.iter().next().unwrap().unwrap_err();
        assert!(
            err.to_string()
                .contains("entry length 16 is less than minimum entry length 17"),
            "got: {}",
            err
        );
    }

    #[test]
    fn should_reject_entry_truncated_before_entry_len() {
        // Data too short to even read entry_len (need 4 bytes, provide 2)
        let manifest = manifest_from_raw_entry(&[0u8; 2]);
        let err = manifest.iter().next().unwrap().unwrap_err();
        assert!(
            matches!(&err, Error::Serialization(msg) if msg.contains("entry length field does not fit")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn should_reject_entry_truncated_before_metadata_start_index() {
        // metadata_count = 1 but no metadata bytes at all
        let manifest = manifest_from_raw_entry(&corrupt_metadata_entry(1, &[]));
        let err = manifest.iter().next().unwrap().unwrap_err();
        assert!(
            matches!(&err, Error::Serialization(msg) if msg.contains("start index field does not fit")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn should_reject_entry_truncated_before_metadata_ingestion_time() {
        // metadata_count = 1, only start_index present (4 bytes)
        let manifest = manifest_from_raw_entry(&corrupt_metadata_entry(1, &0u32.to_le_bytes()));
        let err = manifest.iter().next().unwrap().unwrap_err();
        assert!(
            matches!(&err, Error::Serialization(msg) if msg.contains("ingestion time field does not fit")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn should_reject_entry_truncated_before_metadata_length() {
        // metadata_count = 1, start_index + ingestion_time present, but no m_len
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes()); // start_index
        extra.extend_from_slice(&0i64.to_le_bytes()); // ingestion_time_ms
        let manifest = manifest_from_raw_entry(&corrupt_metadata_entry(1, &extra));
        let err = manifest.iter().next().unwrap().unwrap_err();
        assert!(
            matches!(&err, Error::Serialization(msg) if msg.contains("metadata length field does not fit")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn should_reject_entry_truncated_before_metadata_payload() {
        // metadata_count = 1, all fixed fields present, m_len says 10 but only 2 bytes follow
        let mut extra = Vec::new();
        extra.extend_from_slice(&0u32.to_le_bytes()); // start_index
        extra.extend_from_slice(&0i64.to_le_bytes()); // ingestion_time_ms
        extra.extend_from_slice(&10u32.to_le_bytes()); // m_len = 10
        extra.extend_from_slice(&[0xAB, 0xCD]); // only 2 payload bytes
        let manifest = manifest_from_raw_entry(&corrupt_metadata_entry(1, &extra));
        let err = manifest.iter().next().unwrap().unwrap_err();
        assert!(
            matches!(&err, Error::Serialization(msg) if msg.contains("metadata has less bytes than set")),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn should_reject_location_exceeding_u16_max() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let producer =
            QueueProducer::with_object_store(TEST_MANIFEST_PATH.to_string(), store.clone());

        let long_location = "x".repeat(u16::MAX as usize + 1);
        let result = producer.enqueue(long_location, vec![]).await;
        assert!(matches!(result, Err(Error::InvalidInput(msg)) if msg.contains("location length")));
    }
}

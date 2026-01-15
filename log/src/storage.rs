//! Log-specific storage wrappers.
//!
//! This module provides [`LogStorage`] and [`LogStorageRead`] which wrap
//! the underlying storage traits with log-specific operations like key listing
//! and segment metadata access.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use common::{Storage, StorageIterator, StorageRead};

use common::SeqBlock;

use crate::error::{Error, Result};
use crate::listing::LogKeyIterator;
use crate::model::LogEntry;
use crate::segment::LogSegment;
use crate::serde::{LogEntryKey, SEQ_BLOCK_KEY, SegmentId, SegmentMeta, SegmentMetaKey};

/// Read-only log storage operations.
///
/// Wraps `Arc<dyn StorageRead>` with log-specific read operations
/// for segments and key listings.
#[derive(Clone)]
pub(crate) struct LogStorageRead {
    storage: Arc<dyn StorageRead>,
}

impl LogStorageRead {
    /// Creates a new read-only log storage wrapper.
    pub(crate) fn new(storage: Arc<dyn StorageRead>) -> Self {
        Self { storage }
    }

    /// Gets a single record by key.
    #[cfg(test)]
    pub(crate) async fn get(&self, key: Bytes) -> Result<Option<common::Record>> {
        self.storage
            .get(key)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Gets the current sequence block from storage.
    ///
    /// Returns `None` if no block has been written yet.
    pub(crate) async fn get_seq_block(&self) -> Result<Option<SeqBlock>> {
        let record = self
            .storage
            .get(Bytes::from_static(&SEQ_BLOCK_KEY))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        match record {
            Some(r) => Ok(Some(SeqBlock::deserialize(&r.value)?)),
            None => Ok(None),
        }
    }

    /// Returns an iterator over keys present in the given segment range.
    ///
    /// Keys are deduplicated and returned in lexicographic order.
    /// The segment range is half-open: [start, end).
    pub(crate) async fn list_keys(
        &self,
        segment_range: Range<SegmentId>,
    ) -> Result<LogKeyIterator> {
        LogKeyIterator::open(Arc::clone(&self.storage), segment_range).await
    }

    /// Scans segment metadata records within the given segment ID range.
    ///
    /// Returns segments ordered by segment ID.
    pub(crate) async fn scan_segments(&self, range: Range<SegmentId>) -> Result<Vec<LogSegment>> {
        let scan_range = SegmentMetaKey::scan_range(range);
        let mut iter = self.storage.scan_iter(scan_range).await?;

        let mut segments = Vec::new();
        while let Some(record) = iter.next().await? {
            let key = SegmentMetaKey::deserialize(&record.key)?;
            let meta = SegmentMeta::deserialize(&record.value)?;
            segments.push(LogSegment::new(key.segment_id, meta));
        }

        Ok(segments)
    }

    /// Scans log entries for a key within a segment and sequence range.
    ///
    /// Returns an iterator that yields `LogEntry` values in sequence order.
    pub(crate) async fn scan_entries(
        &self,
        segment: &LogSegment,
        key: &Bytes,
        seq_range: Range<u64>,
    ) -> Result<SegmentIterator> {
        let scan_range = LogEntryKey::scan_range(segment, key, seq_range.clone());
        let inner = self.storage.scan_iter(scan_range).await?;
        Ok(SegmentIterator::new(
            inner,
            seq_range,
            segment.meta().start_seq,
        ))
    }
}

/// Iterator over log entries within a single segment.
///
/// Wraps a `StorageIterator` and handles range validation and `LogEntry`
/// deserialization.
pub(crate) struct SegmentIterator {
    inner: Box<dyn StorageIterator + Send>,
    seq_range: Range<u64>,
    segment_start_seq: u64,
}

impl SegmentIterator {
    fn new(
        inner: Box<dyn StorageIterator + Send>,
        seq_range: Range<u64>,
        segment_start_seq: u64,
    ) -> Self {
        Self {
            inner,
            seq_range,
            segment_start_seq,
        }
    }

    /// Returns the next log entry within the sequence range, or None if exhausted.
    pub(crate) async fn next(&mut self) -> Result<Option<LogEntry>> {
        loop {
            let Some(record) = self
                .inner
                .next()
                .await
                .map_err(|e| Error::Storage(e.to_string()))?
            else {
                return Ok(None);
            };

            let entry_key = LogEntryKey::deserialize(&record.key, self.segment_start_seq)?;

            // Skip entries outside our sequence range
            if entry_key.sequence < self.seq_range.start {
                continue;
            }
            if entry_key.sequence >= self.seq_range.end {
                return Ok(None);
            }

            return Ok(Some(LogEntry {
                key: entry_key.key,
                sequence: entry_key.sequence,
                value: record.value,
            }));
        }
    }
}

/// Read-write log storage operations.
///
/// Wraps `Arc<dyn Storage>` with log-specific operations.
#[derive(Clone)]
pub(crate) struct LogStorage {
    storage: Arc<dyn Storage>,
}

impl LogStorage {
    /// Creates a new log storage wrapper.
    pub(crate) fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }

    /// Creates a new log storage with an in-memory backend.
    #[cfg(test)]
    pub(crate) fn in_memory() -> Self {
        use common::storage::in_memory::InMemoryStorage;
        Self::new(Arc::new(InMemoryStorage::new()))
    }

    /// Returns a read-only view of this storage.
    pub(crate) fn as_read(&self) -> LogStorageRead {
        LogStorageRead::new(Arc::clone(&self.storage) as Arc<dyn StorageRead>)
    }

    /// Writes records to storage with options.
    pub(crate) async fn put_with_options(
        &self,
        records: Vec<common::Record>,
        options: common::WriteOptions,
    ) -> Result<()> {
        self.storage
            .put_with_options(records, options)
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    /// Writes a SeqBlock record to storage.
    #[cfg(test)]
    pub(crate) async fn write_seq_block(&self, block: &common::SeqBlock) -> Result<()> {
        use crate::serde::{KEY_VERSION, RecordType};
        let key = Bytes::from(vec![KEY_VERSION, RecordType::SeqBlock.id()]);
        let value = block.serialize();
        self.storage
            .put(vec![common::Record::new(key, value)])
            .await?;
        Ok(())
    }

    /// Writes a segment metadata record to storage.
    #[cfg(test)]
    pub(crate) async fn write_segment(&self, segment: &LogSegment) -> Result<()> {
        let key = SegmentMetaKey::new(segment.id()).serialize();
        let value = segment.meta().serialize();
        self.storage
            .put(vec![common::Record::new(key, value)])
            .await?;
        Ok(())
    }

    /// Writes a log entry record to storage.
    ///
    /// This is a low-level API primarily for testing. Production code should
    /// use the higher-level `Log::append` method.
    #[cfg(test)]
    pub(crate) async fn write_entry(&self, segment: &LogSegment, entry: &LogEntry) -> Result<()> {
        let entry_key = LogEntryKey::new(segment.id(), entry.key.clone(), entry.sequence);
        let record = common::Record {
            key: entry_key.serialize(segment.meta().start_seq),
            value: entry.value.clone(),
        };
        self.storage.put(vec![record]).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::SegmentMeta;

    #[tokio::test]
    async fn should_get_record_when_present() {
        // given
        let storage = LogStorage::in_memory();
        let key = Bytes::from("test-key");
        let value = Bytes::from("test-value");
        storage
            .storage
            .put(vec![common::Record::new(key.clone(), value.clone())])
            .await
            .unwrap();

        // when
        let result = storage.as_read().get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, value);
    }

    #[tokio::test]
    async fn should_return_none_when_record_absent() {
        // given
        let storage = LogStorage::in_memory();

        // when
        let result = storage.as_read().get(Bytes::from("missing")).await.unwrap();

        // then
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn should_scan_segments_in_order() {
        // given
        let storage = LogStorage::in_memory();
        let seg0 = LogSegment::new(0, SegmentMeta::new(0, 100));
        let seg1 = LogSegment::new(1, SegmentMeta::new(100, 200));
        let seg2 = LogSegment::new(2, SegmentMeta::new(200, 300));
        storage.write_segment(&seg0).await.unwrap();
        storage.write_segment(&seg2).await.unwrap(); // write out of order
        storage.write_segment(&seg1).await.unwrap();

        // when
        let segments = storage.as_read().scan_segments(0..u32::MAX).await.unwrap();

        // then
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
        assert_eq!(segments[2].id(), 2);
    }

    #[tokio::test]
    async fn should_scan_segments_with_range() {
        // given
        let storage = LogStorage::in_memory();
        for i in 0u32..5 {
            let seg = LogSegment::new(i, SegmentMeta::new(i as u64 * 100, i as i64 * 100));
            storage.write_segment(&seg).await.unwrap();
        }

        // when
        let segments = storage.as_read().scan_segments(1..4).await.unwrap();

        // then
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id(), 1);
        assert_eq!(segments[1].id(), 2);
        assert_eq!(segments[2].id(), 3);
    }

    #[tokio::test]
    async fn should_scan_entries_for_key() {
        // given
        let storage = LogStorage::in_memory();
        let segment = LogSegment::new(0, SegmentMeta::new(0, 100));
        storage.write_segment(&segment).await.unwrap();

        let key = Bytes::from("key1");
        for seq in 0..5 {
            let entry = LogEntry {
                key: key.clone(),
                sequence: seq,
                value: Bytes::from(format!("value-{}", seq)),
            };
            storage.write_entry(&segment, &entry).await.unwrap();
        }

        // when
        let mut iter = storage
            .as_read()
            .scan_entries(&segment, &key, 0..u64::MAX)
            .await
            .unwrap();

        // then
        let mut entries = Vec::new();
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }
        assert_eq!(entries.len(), 5);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.sequence, i as u64);
        }
    }

    #[tokio::test]
    async fn should_filter_entries_by_sequence_range() {
        // given
        let storage = LogStorage::in_memory();
        let segment = LogSegment::new(0, SegmentMeta::new(0, 100));
        storage.write_segment(&segment).await.unwrap();

        let key = Bytes::from("key1");
        for seq in 0..10 {
            let entry = LogEntry {
                key: key.clone(),
                sequence: seq,
                value: Bytes::from(format!("value-{}", seq)),
            };
            storage.write_entry(&segment, &entry).await.unwrap();
        }

        // when - scan only sequences 3..7
        let mut iter = storage
            .as_read()
            .scan_entries(&segment, &key, 3..7)
            .await
            .unwrap();

        // then
        let mut entries = Vec::new();
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].sequence, 3);
        assert_eq!(entries[3].sequence, 6);
    }

    #[tokio::test]
    async fn should_return_empty_iterator_for_unknown_key() {
        // given
        let storage = LogStorage::in_memory();
        let segment = LogSegment::new(0, SegmentMeta::new(0, 100));
        storage.write_segment(&segment).await.unwrap();

        // when
        let mut iter = storage
            .as_read()
            .scan_entries(&segment, &Bytes::from("unknown"), 0..u64::MAX)
            .await
            .unwrap();

        // then
        assert!(iter.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_write_and_read_seq_block() {
        // given
        let storage = LogStorage::in_memory();
        let block = common::SeqBlock::new(1000, 500);

        // when
        storage.write_seq_block(&block).await.unwrap();

        // then
        use crate::serde::{KEY_VERSION, RecordType};
        let key = Bytes::from(vec![KEY_VERSION, RecordType::SeqBlock.id()]);
        let record = storage.as_read().get(key).await.unwrap().unwrap();
        let read_block = common::SeqBlock::deserialize(&record.value).unwrap();
        assert_eq!(read_block.base_sequence, 1000);
        assert_eq!(read_block.block_size, 500);
    }

    #[tokio::test]
    async fn should_put_with_options() {
        // given
        let storage = LogStorage::in_memory();
        let records = vec![
            common::Record::new(Bytes::from("k1"), Bytes::from("v1")),
            common::Record::new(Bytes::from("k2"), Bytes::from("v2")),
        ];

        // when
        storage
            .put_with_options(records, common::WriteOptions::default())
            .await
            .unwrap();

        // then
        let r1 = storage.as_read().get(Bytes::from("k1")).await.unwrap();
        let r2 = storage.as_read().get(Bytes::from("k2")).await.unwrap();
        assert_eq!(r1.unwrap().value, Bytes::from("v1"));
        assert_eq!(r2.unwrap().value, Bytes::from("v2"));
    }

    #[tokio::test]
    async fn should_clone_log_storage_read() {
        // given
        let storage = LogStorage::in_memory();
        storage
            .storage
            .put(vec![common::Record::new(
                Bytes::from("key"),
                Bytes::from("value"),
            )])
            .await
            .unwrap();

        let read = storage.as_read();

        // when
        let cloned = read.clone();

        // then - both should see the same data
        let r1 = read.get(Bytes::from("key")).await.unwrap();
        let r2 = cloned.get(Bytes::from("key")).await.unwrap();
        assert_eq!(r1.unwrap().value, r2.unwrap().value);
    }
}

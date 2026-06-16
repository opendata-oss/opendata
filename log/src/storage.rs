//! Log-specific storage extensions.
//!
//! This module provides [`LogStorageRead`], an extension trait that adds
//! log-specific read operations (segment scanning, entry scanning, key
//! listing) to any [`StorageRead`] implementation, and test helpers for
//! writing log domain types to storage.

use async_trait::async_trait;
use std::collections::BTreeSet;
use std::ops::Range;

use crate::error::{Error, Result};
use crate::model::{LogEntry, SegmentId};
use crate::segment::LogSegment;
use crate::serde::{ListingEntryKey, LogEntryKey, SegmentMeta, SegmentMetaKey};
use bytes::Bytes;
#[cfg(test)]
use common::Storage;
use common::{StorageIterator, StorageRead};

/// Extension trait adding log-specific read operations to any [`StorageRead`].
///
/// Provides default implementations for scanning segments, entries, and listing keys.
/// Automatically available on all `StorageRead` types via a blanket impl.
#[async_trait]
pub(crate) trait LogStorageRead: StorageRead {
    /// Returns the keys present in a single segment.
    ///
    /// Listing records are interleaved with log entries across segment
    /// boundaries in the key layout, so cross-segment listing scans aren't
    /// safe at the storage layer. Callers wanting to enumerate keys across
    /// multiple segments stitch per-segment results together using the
    /// segment cache. See [`crate::reader::LogReadView::list_keys`].
    async fn list_keys_in_segment(&self, segment_id: SegmentId) -> Result<BTreeSet<Bytes>> {
        let scan_range = ListingEntryKey::scan_range_for_segment(segment_id);
        let mut iter = self
            .scan_iter(scan_range)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut keys = BTreeSet::new();
        while let Some(record) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let entry_key = ListingEntryKey::deserialize(&record.key)?;
            keys.insert(entry_key.key);
        }

        Ok(keys)
    }

    /// Scans segment metadata records within the given segment ID range.
    ///
    /// Returns segments ordered by segment ID.
    async fn scan_segments(&self, range: Range<SegmentId>) -> Result<Vec<LogSegment>> {
        let scan_range = SegmentMetaKey::scan_range(range);
        let mut iter = self.scan_iter(scan_range).await?;

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
    ///
    /// Issues a prefix scan over the `(segment, key)` prefix rather than a
    /// bounded range so that backends with prefix-aware SST filters can skip
    /// tables for this `(segment, key)`: slatedb's `BloomFilterPolicy` with
    /// `LogKeyPrefixExtractor` skips SSTs that contain no entries for the key,
    /// and the sequence-range filter skips SSTs whose entries for the key all
    /// sit below the scan's resume cursor. The cursor is relativized to this
    /// segment (the filter stores relative sequences — see
    /// [`crate::filter_sequence`]) and passed as the filter context.
    ///
    /// The seq range is still enforced client-side by [`SegmentIterator`],
    /// which early-exits once `sequence >= seq_range.end`; filter pruning only
    /// drops whole SSTs, so the iterator remains the source of exactness.
    async fn scan_entries(
        &self,
        segment: &LogSegment,
        key: &Bytes,
        seq_range: Range<u64>,
    ) -> Result<SegmentIterator> {
        let prefix = LogEntryKey::scan_prefix(segment, key);
        let relative_cursor = seq_range.start.saturating_sub(segment.meta().start_seq);
        let filter_context = Some(crate::filter_sequence::sequence_filter_context(
            relative_cursor,
        ));
        let inner = self.scan_prefix_iter(prefix, filter_context).await?;
        Ok(SegmentIterator::new(
            inner,
            seq_range,
            segment.meta().start_seq,
        ))
    }

    /// Counts log entries for a key within a segment and sequence range by
    /// scanning every record and tallying. Use this as a fallback when no
    /// [`SlateReadHandle`](common::SlateReadHandle) is available.
    async fn count_entries(
        &self,
        segment: &LogSegment,
        key: &Bytes,
        seq_range: Range<u64>,
    ) -> Result<u64> {
        let byte_range = LogEntryKey::scan_range(segment, key, seq_range);
        let mut iter = self.scan_iter(byte_range).await?;
        let mut total = 0u64;
        while iter.next().await?.is_some() {
            total = total.saturating_add(1);
        }
        Ok(total)
    }
}

impl<T: StorageRead + ?Sized> LogStorageRead for T {}

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

/// Extension trait adding log-specific test write helpers to any [`Storage`].
///
/// These helpers construct storage records from log domain types (segments,
/// entries, sequence blocks) and are used by tests to set up storage state
/// without going through the full write coordinator.
#[cfg(test)]
#[async_trait]
pub(crate) trait LogStorageWrite: Storage {
    /// Writes a segment metadata record to storage.
    async fn write_segment(&self, segment: &LogSegment) -> Result<()> {
        let key = SegmentMetaKey::new(segment.id()).serialize();
        let value = segment.meta().serialize();
        self.put(vec![common::Record::new(key, value).into()])
            .await?;
        Ok(())
    }

    /// Writes a log entry record to storage.
    ///
    /// This is a low-level API primarily for testing. Production code should
    /// use the higher-level `LogDb::append` method.
    async fn write_entry(&self, segment: &LogSegment, entry: &LogEntry) -> Result<()> {
        let entry_key = LogEntryKey::new(segment.id(), entry.key.clone(), entry.sequence);
        let record = common::Record {
            key: entry_key.serialize(segment.meta().start_seq),
            value: entry.value.clone(),
        };
        self.put(vec![record.into()]).await?;
        Ok(())
    }

    /// Writes a SeqBlock record to storage at the canonical [`crate::serde::SEQ_BLOCK_KEY`].
    async fn write_seq_block(&self, block: &common::SeqBlock) -> Result<()> {
        let key = Bytes::from_static(&crate::serde::SEQ_BLOCK_KEY);
        let value = block.serialize();
        self.put(vec![common::Record::new(key, value).into()])
            .await?;
        Ok(())
    }
}

#[cfg(test)]
impl<T: Storage + ?Sized> LogStorageWrite for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::SegmentMeta;
    use opendata_macros::storage_test;

    #[storage_test]
    async fn should_get_record_when_present(storage: Arc<dyn Storage>) {
        // given
        let key = Bytes::from("test-key");
        let value = Bytes::from("test-value");
        storage
            .put(vec![common::Record::new(key.clone(), value.clone()).into()])
            .await
            .unwrap();

        // when
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, value);
    }

    #[storage_test]
    async fn should_return_none_when_record_absent(storage: Arc<dyn Storage>) {
        // when
        let result = storage.get(Bytes::from("missing")).await.unwrap();

        // then
        assert!(result.is_none());
    }

    #[storage_test]
    async fn should_scan_segments_in_order(storage: Arc<dyn Storage>) {
        // given — user segments start at id 1
        let seg1 = LogSegment::new(1, SegmentMeta::new(0, 100));
        let seg2 = LogSegment::new(2, SegmentMeta::new(100, 200));
        let seg3 = LogSegment::new(3, SegmentMeta::new(200, 300));
        storage.write_segment(&seg1).await.unwrap();
        storage.write_segment(&seg3).await.unwrap(); // write out of order
        storage.write_segment(&seg2).await.unwrap();

        // when
        let segments = storage.scan_segments(1..u32::MAX).await.unwrap();

        // then
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id(), 1);
        assert_eq!(segments[1].id(), 2);
        assert_eq!(segments[2].id(), 3);
    }

    #[storage_test]
    async fn should_scan_segments_with_range(storage: Arc<dyn Storage>) {
        // given
        for i in 1u32..=5 {
            let seg = LogSegment::new(i, SegmentMeta::new(i as u64 * 100, i as i64 * 100));
            storage.write_segment(&seg).await.unwrap();
        }

        // when
        let segments = storage.scan_segments(2..5).await.unwrap();

        // then
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id(), 2);
        assert_eq!(segments[1].id(), 3);
        assert_eq!(segments[2].id(), 4);
    }

    #[storage_test]
    async fn should_scan_entries_for_key(storage: Arc<dyn Storage>) {
        // given
        let segment = LogSegment::new(1, SegmentMeta::new(0, 100));
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

    #[storage_test]
    async fn should_filter_entries_by_sequence_range(storage: Arc<dyn Storage>) {
        // given
        let segment = LogSegment::new(1, SegmentMeta::new(0, 100));
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
        let mut iter = storage.scan_entries(&segment, &key, 3..7).await.unwrap();

        // then
        let mut entries = Vec::new();
        while let Some(entry) = iter.next().await.unwrap() {
            entries.push(entry);
        }
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].sequence, 3);
        assert_eq!(entries[3].sequence, 6);
    }

    #[storage_test]
    async fn should_return_empty_iterator_for_unknown_key(storage: Arc<dyn Storage>) {
        // given
        let segment = LogSegment::new(1, SegmentMeta::new(0, 100));
        storage.write_segment(&segment).await.unwrap();

        // when
        let mut iter = storage
            .scan_entries(&segment, &Bytes::from("unknown"), 0..u64::MAX)
            .await
            .unwrap();

        // then
        assert!(iter.next().await.unwrap().is_none());
    }

    #[storage_test]
    async fn should_write_and_read_seq_block(storage: Arc<dyn Storage>) {
        // given
        let block = common::SeqBlock::new(1000, 500);

        // when
        storage.write_seq_block(&block).await.unwrap();

        // then
        let key = Bytes::from_static(&crate::serde::SEQ_BLOCK_KEY);
        let record = storage.get(key).await.unwrap().unwrap();
        let read_block = common::SeqBlock::deserialize(&record.value).unwrap();
        assert_eq!(read_block.base_sequence, 1000);
        assert_eq!(read_block.block_size, 500);
    }

    #[storage_test]
    async fn should_put_with_options(storage: Arc<dyn Storage>) {
        // given
        let records = vec![
            common::Record::new(Bytes::from("k1"), Bytes::from("v1")).into(),
            common::Record::new(Bytes::from("k2"), Bytes::from("v2")).into(),
        ];

        // when
        storage
            .put_with_options(records, common::WriteOptions::default())
            .await
            .unwrap();

        // then
        let r1 = storage.get(Bytes::from("k1")).await.unwrap();
        let r2 = storage.get(Bytes::from("k2")).await.unwrap();
        assert_eq!(r1.unwrap().value, Bytes::from("v1"));
        assert_eq!(r2.unwrap().value, Bytes::from("v2"));
    }
}

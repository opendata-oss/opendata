#![allow(dead_code)]

//! Log segment management.
//!
//! This module provides the [`LogSegment`] abstraction for logical partitioning
//! of log data. Segments are sequential, non-overlapping ranges of sequence numbers
//! that enable efficient seeking, retention management, and cross-key operations.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use common::{Record, Storage, StorageRead};
use tokio::sync::RwLock;

use crate::error::Result;
use crate::serde::{SegmentId, SegmentMeta, SegmentMetaKey};

/// Read-only operations for segments.
#[async_trait]
pub(crate) trait SegmentRead: Send + Sync {
    /// Returns the underlying storage.
    fn storage(&self) -> Arc<dyn StorageRead>;

    /// Returns the latest segment, if any exist.
    async fn latest(&self) -> Result<Option<LogSegment>>;

    /// Returns all segments ordered by start sequence.
    async fn all(&self) -> Result<Vec<LogSegment>>;

    /// Finds segments covering the given sequence range.
    async fn find_covering(&self, range: std::ops::Range<u64>) -> Result<Vec<LogSegment>>;
}

/// A logical segment of the log.
///
/// `LogSegment` is a first-class object representing a segment in the log.
/// Segments group entries across all keys within a range of sequence numbers.
///
/// Currently tracks only ID and metadata, but designed to eventually hold
/// additional state such as key listings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogSegment {
    id: SegmentId,
    meta: SegmentMeta,
}

impl LogSegment {
    /// Creates a new log segment.
    pub(crate) fn new(id: SegmentId, meta: SegmentMeta) -> Self {
        Self { id, meta }
    }

    /// Returns the segment's unique identifier.
    pub fn id(&self) -> SegmentId {
        self.id
    }

    /// Returns the segment's metadata.
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }
}

/// In-memory cache of segments loaded from storage.
///
/// Used by both [`SegmentReader`] and [`SegmentStore`] to provide
/// fast access to segment metadata without repeated storage reads.
///
/// Keyed by `start_seq` to optimize `find_covering` queries.
struct SegmentCache {
    /// Segments keyed by their starting sequence number.
    segments: RwLock<BTreeMap<u64, LogSegment>>,
}

impl SegmentCache {
    /// Creates a new cache by loading all segments from storage.
    async fn open(storage: &dyn StorageRead) -> Result<Self> {
        let range = SegmentMetaKey::scan_range(..);
        let records = storage.scan(range).await?;

        let mut segments = BTreeMap::new();
        for record in records {
            let key = SegmentMetaKey::deserialize(&record.key)?;
            let meta = SegmentMeta::deserialize(&record.value)?;
            let start_seq = meta.start_seq;
            let segment = LogSegment::new(key.segment_id, meta);
            segments.insert(start_seq, segment);
        }

        Ok(Self {
            segments: RwLock::new(segments),
        })
    }

    /// Creates an empty cache for testing.
    #[cfg(test)]
    fn new() -> Self {
        Self {
            segments: RwLock::new(BTreeMap::new()),
        }
    }

    /// Returns the latest segment, if any exist.
    async fn latest(&self) -> Option<LogSegment> {
        let segments = self.segments.read().await;
        segments.values().next_back().cloned()
    }

    /// Returns all segments ordered by start sequence.
    async fn all(&self) -> Vec<LogSegment> {
        let segments = self.segments.read().await;
        segments.values().cloned().collect()
    }

    /// Finds segments covering the given sequence range.
    async fn find_covering(&self, range: std::ops::Range<u64>) -> Vec<LogSegment> {
        let segments = self.segments.read().await;

        if range.start >= range.end {
            return Vec::new();
        }

        // Find the first segment that could contain range.start.
        // This is the segment with the largest start_seq <= range.start.
        let mut result = Vec::new();
        let mut iter = segments.range(..=range.start).rev();

        if let Some((_, first_seg)) = iter.next() {
            result.push(first_seg.clone());
        }

        // Add all segments starting within our query range
        for (_, seg) in segments.range(range.start.saturating_add(1)..range.end) {
            result.push(seg.clone());
        }

        result
    }

    /// Adds a segment to the cache.
    async fn insert(&self, segment: LogSegment) {
        let mut segments = self.segments.write().await;
        segments.insert(segment.meta.start_seq, segment);
    }

    /// Refreshes the cache by loading segments from storage.
    ///
    /// If `after_segment_id` is `Some(id)`, only loads segments with id > `id` and appends them.
    /// If `after_segment_id` is `None`, reloads all segments.
    async fn refresh(
        &self,
        storage: &dyn StorageRead,
        after_segment_id: Option<SegmentId>,
    ) -> Result<()> {
        let range = match after_segment_id {
            Some(id) => SegmentMetaKey::scan_range(id.saturating_add(1)..),
            None => SegmentMetaKey::scan_range(..),
        };
        let records = storage.scan(range).await?;

        let mut segments = self.segments.write().await;
        if after_segment_id.is_none() {
            segments.clear();
        }

        for record in records {
            let key = SegmentMetaKey::deserialize(&record.key)?;
            let meta = SegmentMeta::deserialize(&record.value)?;
            let start_seq = meta.start_seq;
            let segment = LogSegment::new(key.segment_id, meta);
            segments.insert(start_seq, segment);
        }

        Ok(())
    }
}

/// Read-only access to segments.
///
/// `SegmentReader` provides read-only operations for discovering and
/// loading segments from storage. Used by `LogReader` and other components
/// that don't need write access.
pub(crate) struct SegmentReader {
    storage: Arc<dyn StorageRead>,
    cache: SegmentCache,
}

impl SegmentReader {
    /// Opens a segment reader, loading all segments into cache.
    pub(crate) async fn open(storage: Arc<dyn StorageRead>) -> Result<Self> {
        let cache = SegmentCache::open(&*storage).await?;
        Ok(Self { storage, cache })
    }

    /// Creates a new segment reader for testing.
    #[cfg(test)]
    pub(crate) fn new(storage: Arc<dyn StorageRead>) -> Self {
        Self {
            storage,
            cache: SegmentCache::new(),
        }
    }

    /// Refreshes the segment cache by loading new segments from storage.
    ///
    /// Only scans for segments newer than the latest cached segment.
    // TODO: Add periodic refresh to scan for new segments from storage.
    pub(crate) async fn refresh(&self) -> Result<()> {
        let after = self.cache.latest().await.map(|s| s.id);
        self.cache.refresh(&*self.storage, after).await
    }
}

#[async_trait]
impl SegmentRead for SegmentReader {
    fn storage(&self) -> Arc<dyn StorageRead> {
        Arc::clone(&self.storage)
    }

    async fn latest(&self) -> Result<Option<LogSegment>> {
        Ok(self.cache.latest().await)
    }

    async fn all(&self) -> Result<Vec<LogSegment>> {
        Ok(self.cache.all().await)
    }

    async fn find_covering(&self, range: std::ops::Range<u64>) -> Result<Vec<LogSegment>> {
        Ok(self.cache.find_covering(range).await)
    }
}

/// Manages segment persistence and lifecycle.
///
/// `SegmentStore` handles creating new segments and tracking the active segment.
/// For read-only access, use [`SegmentReader`].
pub(crate) struct SegmentStore {
    storage: Arc<dyn Storage>,
    cache: SegmentCache,
    config: crate::config::SegmentConfig,
}

impl SegmentStore {
    /// Opens the segment store, loading all existing segments into cache.
    pub(crate) async fn open(
        storage: Arc<dyn Storage>,
        config: crate::config::SegmentConfig,
    ) -> Result<Self> {
        let cache = SegmentCache::open(&*storage).await?;
        Ok(Self {
            storage,
            cache,
            config,
        })
    }

    /// Creates a new segment store for testing.
    #[cfg(test)]
    pub(crate) fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            cache: SegmentCache::new(),
            config: crate::config::SegmentConfig::default(),
        }
    }

    /// Writes a new segment.
    ///
    /// The new segment ID is derived from the latest existing segment.
    /// Returns the newly created segment.
    pub(crate) async fn write(&self, meta: SegmentMeta) -> Result<LogSegment> {
        let segment_id = match self.cache.latest().await {
            Some(latest) => latest.id + 1,
            None => 0,
        };

        let key = SegmentMetaKey::new(segment_id).serialize();
        let value = meta.serialize();
        self.storage.put(vec![Record::new(key, value)]).await?;

        let segment = LogSegment::new(segment_id, meta);
        self.cache.insert(segment.clone()).await;

        Ok(segment)
    }

    /// Checks if the current segment should be rolled based on the seal interval.
    ///
    /// Returns `true` if:
    /// - A seal interval is configured, AND
    /// - Either no segment exists, OR the current segment has exceeded the seal interval
    ///
    /// The `current_time_ms` parameter is the current wall-clock time in milliseconds
    /// since the Unix epoch.
    pub(crate) async fn should_roll(&self, current_time_ms: i64) -> bool {
        let Some(seal_interval) = self.config.seal_interval else {
            return false;
        };

        let Some(latest) = self.cache.latest().await else {
            // No segments exist yet
            return true;
        };

        let seal_interval_ms = seal_interval.as_millis() as i64;
        let segment_age_ms = current_time_ms - latest.meta().start_time_ms;
        segment_age_ms >= seal_interval_ms
    }

    /// Forces creation of a new segment, sealing the current one.
    ///
    /// This unconditionally creates a new segment regardless of seal interval.
    /// Used for testing and manual segment management.
    ///
    /// # Parameters
    /// - `current_time_ms`: Current wall-clock time in milliseconds since Unix epoch
    /// - `start_seq`: The starting sequence number for the new segment
    ///
    /// # Returns
    /// The newly created segment.
    pub(crate) async fn seal_current(
        &self,
        current_time_ms: i64,
        start_seq: u64,
    ) -> Result<LogSegment> {
        self.write(SegmentMeta::new(start_seq, current_time_ms))
            .await
    }

    /// Ensures a segment is available for writing, rolling if necessary.
    ///
    /// If no segment exists or the current segment has exceeded the seal interval,
    /// a new segment is created. This method should be used when appending entries
    /// to ensure writes go to the appropriate segment.
    ///
    /// # Parameters
    /// - `current_time_ms`: Current wall-clock time in milliseconds since Unix epoch
    /// - `start_seq`: The starting sequence number for a new segment if one is created
    ///
    /// # Returns
    /// The segment that should receive new writes.
    pub(crate) async fn ensure_latest(
        &self,
        current_time_ms: i64,
        start_seq: u64,
    ) -> Result<LogSegment> {
        if self.should_roll(current_time_ms).await {
            return self
                .write(SegmentMeta::new(start_seq, current_time_ms))
                .await;
        }

        // should_roll returned false. Either:
        // 1. seal_interval is None - use latest or create first segment
        // 2. seal_interval is Some but segment is young - use latest (which must exist)
        match self.cache.latest().await {
            Some(segment) => Ok(segment),
            None => {
                self.write(SegmentMeta::new(start_seq, current_time_ms))
                    .await
            }
        }
    }
}

#[async_trait]
impl SegmentRead for SegmentStore {
    fn storage(&self) -> Arc<dyn StorageRead> {
        Arc::clone(&self.storage) as Arc<dyn StorageRead>
    }

    async fn latest(&self) -> Result<Option<LogSegment>> {
        Ok(self.cache.latest().await)
    }

    async fn all(&self) -> Result<Vec<LogSegment>> {
        Ok(self.cache.all().await)
    }

    async fn find_covering(&self, range: std::ops::Range<u64>) -> Result<Vec<LogSegment>> {
        Ok(self.cache.find_covering(range).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SegmentConfig;
    use common::storage::in_memory::InMemoryStorage;

    #[tokio::test]
    async fn should_return_none_when_no_segments_exist() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();

        // when
        let latest = store.latest().await.unwrap();

        // then
        assert!(latest.is_none());
    }

    #[tokio::test]
    async fn should_write_first_segment_with_id_zero() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();
        let meta = SegmentMeta::new(0, 1000);

        // when
        let segment = store.write(meta.clone()).await.unwrap();

        // then
        assert_eq!(segment.id(), 0);
        assert_eq!(segment.meta(), &meta);
    }

    #[tokio::test]
    async fn should_increment_segment_id_on_subsequent_writes() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();

        // when
        let seg0 = store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        let seg1 = store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        let seg2 = store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        // then
        assert_eq!(seg0.id(), 0);
        assert_eq!(seg1.id(), 1);
        assert_eq!(seg2.id(), 2);
    }

    #[tokio::test]
    async fn should_return_latest_segment() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();

        // when
        let latest = store.latest().await.unwrap();

        // then
        assert_eq!(latest.unwrap().id(), 1);
    }

    #[tokio::test]
    async fn should_scan_all_segments() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        // when
        let segments = store.all().await.unwrap();

        // then
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
        assert_eq!(segments[2].id(), 2);
    }

    #[tokio::test]
    async fn should_persist_segments_to_storage() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();

        // when - reopen from same storage
        let store2 = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();
        let segments = store2.all().await.unwrap();

        // then
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[0].meta().start_seq, 0);
        assert_eq!(segments[1].id(), 1);
        assert_eq!(segments[1].meta().start_seq, 100);
    }

    #[tokio::test]
    async fn should_load_segments_on_reader_open() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();

        // when
        let reader = SegmentReader::open(storage).await.unwrap();
        let segments = reader.all().await.unwrap();

        // then
        assert_eq!(segments.len(), 2);
    }

    #[tokio::test]
    async fn should_find_segments_by_seq_range_all() {
        // given: segments at seq 0, 100, 200
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query all sequences
        let segments = reader.find_covering(0..u64::MAX).await.unwrap();

        // then: all segments match
        assert_eq!(segments.len(), 3);
    }

    #[tokio::test]
    async fn should_find_segments_by_seq_range_single() {
        // given: segments at seq 0, 100, 200
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query seq 50..60 (within first segment)
        let segments = reader.find_covering(50..60).await.unwrap();

        // then: only first segment matches
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id(), 0);
    }

    #[tokio::test]
    async fn should_find_segments_by_seq_range_spanning() {
        // given: segments at seq 0, 100, 200
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query seq 50..150 (spans first and second segment)
        let segments = reader.find_covering(50..150).await.unwrap();

        // then: first two segments match
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
    }

    #[tokio::test]
    async fn should_find_segments_by_seq_range_unbounded_end() {
        // given: segments at seq 0, 100, 200
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query seq 150.. (from middle of second segment to end)
        let segments = reader.find_covering(150..u64::MAX).await.unwrap();

        // then: second and third segments match
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 1);
        assert_eq!(segments[1].id(), 2);
    }

    #[tokio::test]
    async fn should_find_no_segments_when_range_before_all() {
        // given: segments starting at seq 100
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(100, 1000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query seq 0..50 (before any segment data)
        let segments = reader.find_covering(0..50).await.unwrap();

        // then: no segments match (segment starts at 100)
        assert_eq!(segments.len(), 0);
    }

    #[tokio::test]
    async fn should_find_no_segments_when_storage_empty() {
        // given: no segments
        let storage = Arc::new(InMemoryStorage::new());
        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query any range
        let segments = reader.find_covering(0..u64::MAX).await.unwrap();

        // then: no segments
        assert_eq!(segments.len(), 0);
    }

    #[tokio::test]
    async fn should_find_last_segment_when_range_after_all() {
        // given: segments at seq 0, 100, 200
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query seq 500..600 (after all segment starts)
        let segments = reader.find_covering(500..600).await.unwrap();

        // then: last segment matches (it could contain seqs 200+)
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id(), 2);
    }

    #[tokio::test]
    async fn should_find_segment_when_query_starts_at_boundary() {
        // given: segments at seq 0, 100, 200
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query starting exactly at segment boundary
        let segments = reader.find_covering(100..150).await.unwrap();

        // then: only the segment starting at 100 matches
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id(), 1);
    }

    #[tokio::test]
    async fn should_find_segments_with_unbounded_start() {
        // given: segments at seq 0, 100, 200
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();

        // when: query with unbounded start ..150
        let segments = reader.find_covering(0..150).await.unwrap();

        // then: first two segments match
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
    }

    #[tokio::test]
    async fn should_find_covering_via_segment_store() {
        // given: segments at seq 0, 100, 200
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();

        // when: query via store (not reader)
        let segments = store.find_covering(50..150).await.unwrap();

        // then: first two segments match
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
    }

    #[tokio::test]
    async fn should_roll_returns_false_when_no_seal_interval() {
        // given: store without seal interval
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();

        // when
        let should_roll = store.should_roll(1000).await;

        // then
        assert!(!should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_true_when_no_segments_exist() {
        // given: store with seal interval but no segments
        let storage = Arc::new(InMemoryStorage::new());
        let config = SegmentConfig {
            seal_interval: Some(std::time::Duration::from_secs(3600)),
        };
        let store = SegmentStore::open(storage, config).await.unwrap();

        // when
        let should_roll = store.should_roll(1000).await;

        // then: should roll to create first segment
        assert!(should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_false_when_within_interval() {
        // given: segment created at time 1000, seal interval 1 hour
        let storage = Arc::new(InMemoryStorage::new());
        let config = SegmentConfig {
            seal_interval: Some(std::time::Duration::from_secs(3600)),
        };
        let store = SegmentStore::open(storage, config).await.unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();

        // when: current time is 1000 + 30 minutes
        let current_time_ms = 1000 + 30 * 60 * 1000;
        let should_roll = store.should_roll(current_time_ms).await;

        // then
        assert!(!should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_true_when_interval_exceeded() {
        // given: segment created at time 1000, seal interval 1 hour
        let storage = Arc::new(InMemoryStorage::new());
        let config = SegmentConfig {
            seal_interval: Some(std::time::Duration::from_secs(3600)),
        };
        let store = SegmentStore::open(storage, config).await.unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();

        // when: current time is 1000 + 2 hours
        let current_time_ms = 1000 + 2 * 60 * 60 * 1000;
        let should_roll = store.should_roll(current_time_ms).await;

        // then
        assert!(should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_true_when_exactly_at_interval() {
        // given: segment created at time 1000, seal interval 1 hour
        let storage = Arc::new(InMemoryStorage::new());
        let config = SegmentConfig {
            seal_interval: Some(std::time::Duration::from_secs(3600)),
        };
        let store = SegmentStore::open(storage, config).await.unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();

        // when: current time is exactly at the interval boundary
        let current_time_ms = 1000 + 60 * 60 * 1000;
        let should_roll = store.should_roll(current_time_ms).await;

        // then: at boundary should roll
        assert!(should_roll);
    }

    #[tokio::test]
    async fn ensure_latest_creates_first_segment_when_none_exist_without_seal_interval() {
        // given: no segments, no seal interval
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();

        // when
        let segment = store.ensure_latest(1000, 0).await.unwrap();

        // then: creates segment 0
        assert_eq!(segment.id(), 0);
        assert_eq!(segment.meta().start_seq, 0);
        assert_eq!(segment.meta().start_time_ms, 1000);
    }

    #[tokio::test]
    async fn ensure_latest_creates_first_segment_when_none_exist_with_seal_interval() {
        // given: no segments, seal interval configured
        let storage = Arc::new(InMemoryStorage::new());
        let config = SegmentConfig {
            seal_interval: Some(std::time::Duration::from_secs(3600)),
        };
        let store = SegmentStore::open(storage, config).await.unwrap();

        // when
        let segment = store.ensure_latest(1000, 0).await.unwrap();

        // then: creates segment 0
        assert_eq!(segment.id(), 0);
        assert_eq!(segment.meta().start_seq, 0);
        assert_eq!(segment.meta().start_time_ms, 1000);
    }

    #[tokio::test]
    async fn ensure_latest_returns_existing_segment_when_within_interval() {
        // given: segment at time 1000, seal interval 1 hour
        let storage = Arc::new(InMemoryStorage::new());
        let config = SegmentConfig {
            seal_interval: Some(std::time::Duration::from_secs(3600)),
        };
        let store = SegmentStore::open(storage, config).await.unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();

        // when: request at 30 minutes later
        let current_time_ms = 1000 + 30 * 60 * 1000;
        let segment = store.ensure_latest(current_time_ms, 100).await.unwrap();

        // then: returns existing segment (not rolled)
        assert_eq!(segment.id(), 0);
        assert_eq!(segment.meta().start_seq, 0);
    }

    #[tokio::test]
    async fn ensure_latest_rolls_to_new_segment_when_interval_exceeded() {
        // given: segment at time 1000, seal interval 1 hour
        let storage = Arc::new(InMemoryStorage::new());
        let config = SegmentConfig {
            seal_interval: Some(std::time::Duration::from_secs(3600)),
        };
        let store = SegmentStore::open(storage, config).await.unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();

        // when: request at 2 hours later
        let current_time_ms = 1000 + 2 * 60 * 60 * 1000;
        let segment = store.ensure_latest(current_time_ms, 100).await.unwrap();

        // then: creates new segment
        assert_eq!(segment.id(), 1);
        assert_eq!(segment.meta().start_seq, 100);
        assert_eq!(segment.meta().start_time_ms, current_time_ms);
    }

    #[tokio::test]
    async fn ensure_latest_returns_existing_segment_without_seal_interval() {
        // given: segment exists, no seal interval
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage, Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();

        // when: request at any time
        let segment = store.ensure_latest(999999999, 100).await.unwrap();

        // then: returns existing segment (never rolls without seal_interval)
        assert_eq!(segment.id(), 0);
        assert_eq!(segment.meta().start_seq, 0);
    }

    #[tokio::test]
    async fn refresh_should_load_new_segments_after_id() {
        // given: reader with segment 0 cached
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();

        let reader = SegmentReader::open(storage.clone()).await.unwrap();
        assert_eq!(reader.all().await.unwrap().len(), 1);

        // when: new segments are written and reader refreshes
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();
        store.write(SegmentMeta::new(200, 3000)).await.unwrap();
        reader.refresh().await.unwrap();

        // then: reader sees all three segments
        let segments = reader.all().await.unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
        assert_eq!(segments[2].id(), 2);
    }

    #[tokio::test]
    async fn refresh_should_load_all_when_cache_empty() {
        // given: reader with empty cache, segments in storage
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();

        let reader = SegmentReader::new(storage.clone());
        assert_eq!(reader.all().await.unwrap().len(), 0);

        // when: refresh with empty cache
        reader.refresh().await.unwrap();

        // then: all segments loaded
        let segments = reader.all().await.unwrap();
        assert_eq!(segments.len(), 2);
    }

    #[tokio::test]
    async fn refresh_should_be_idempotent_when_no_new_segments() {
        // given: reader with all segments cached
        let storage = Arc::new(InMemoryStorage::new());
        let store = SegmentStore::open(storage.clone(), Default::default())
            .await
            .unwrap();
        store.write(SegmentMeta::new(0, 1000)).await.unwrap();
        store.write(SegmentMeta::new(100, 2000)).await.unwrap();

        let reader = SegmentReader::open(storage).await.unwrap();
        assert_eq!(reader.all().await.unwrap().len(), 2);

        // when: refresh with no new segments
        reader.refresh().await.unwrap();

        // then: same segments, no duplicates
        let segments = reader.all().await.unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
    }
}

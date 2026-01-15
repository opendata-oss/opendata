#![allow(dead_code)]

//! Log segment management.
//!
//! This module provides the [`LogSegment`] abstraction for logical partitioning
//! of log data. Segments are sequential, non-overlapping ranges of sequence numbers
//! that enable efficient seeking, retention management, and cross-key operations.

use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;

use common::Record;

use crate::config::SegmentConfig;
use crate::error::Result;
use crate::serde::{SegmentId, SegmentMeta, SegmentMetaKey};

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

/// Delta representing segment state changes.
///
/// Produced by [`SegmentCache::build_delta`] and consumed by [`SegmentCache::apply_delta`].
#[derive(Debug, Clone)]
pub(crate) struct SegmentDelta {
    /// The segment for this write.
    segment: LogSegment,
    /// Whether this is a newly created segment.
    is_new: bool,
}

impl SegmentDelta {
    /// Creates a new segment delta.
    pub(crate) fn new(segment: LogSegment, is_new: bool) -> Self {
        Self { segment, is_new }
    }

    /// Returns the segment.
    pub(crate) fn segment(&self) -> &LogSegment {
        &self.segment
    }

    /// Returns whether this is a newly created segment.
    pub(crate) fn is_new(&self) -> bool {
        self.is_new
    }
}

/// In-memory cache of segments loaded from storage.
///
/// Provides fast access to segment metadata without repeated storage reads.
/// Also implements the delta pattern for batched writes via `build_delta`
/// and `apply_delta`.
///
/// Keyed by `start_seq` to optimize `find_covering` queries.
pub(crate) struct SegmentCache {
    /// Segments keyed by their starting sequence number.
    segments: BTreeMap<u64, LogSegment>,
    /// Configuration for segment management.
    config: SegmentConfig,
}

impl SegmentCache {
    /// Creates a new cache by loading all segments from storage.
    pub(crate) async fn open(
        storage: &crate::storage::LogStorageRead,
        config: SegmentConfig,
    ) -> Result<Self> {
        let loaded = storage.scan_segments(0..u32::MAX).await?;

        let mut segments = BTreeMap::new();
        for segment in loaded {
            segments.insert(segment.meta.start_seq, segment);
        }

        Ok(Self { segments, config })
    }

    /// Creates an empty cache for testing.
    #[cfg(test)]
    fn new() -> Self {
        Self {
            segments: BTreeMap::new(),
            config: SegmentConfig::default(),
        }
    }

    /// Returns the latest segment, if any exist.
    pub(crate) fn latest(&self) -> Option<LogSegment> {
        self.segments.values().next_back().cloned()
    }

    /// Returns all segments ordered by start sequence.
    pub(crate) fn all(&self) -> Vec<LogSegment> {
        self.segments.values().cloned().collect()
    }

    /// Finds segments covering the given sequence range.
    pub(crate) fn find_covering(&self, range: &Range<u64>) -> Vec<LogSegment> {
        if range.start >= range.end {
            return Vec::new();
        }

        // Find the first segment that could contain range.start.
        // This is the segment with the largest start_seq <= range.start.
        let mut result = Vec::new();
        let mut iter = self.segments.range(..=range.start).rev();

        if let Some((_, first_seg)) = iter.next() {
            result.push(first_seg.clone());
        }

        // Add all segments starting within our query range
        for (_, seg) in self
            .segments
            .range(range.start.saturating_add(1)..range.end)
        {
            result.push(seg.clone());
        }

        result
    }

    /// Adds a segment to the cache.
    pub(crate) fn insert(&mut self, segment: LogSegment) {
        self.segments.insert(segment.meta.start_seq, segment);
    }

    /// Refreshes the cache by loading segments from storage.
    ///
    /// If `after_segment_id` is `Some(id)`, only loads segments with id > `id` and appends them.
    /// If `after_segment_id` is `None`, reloads all segments.
    pub(crate) async fn refresh(
        &mut self,
        storage: &crate::storage::LogStorageRead,
        after_segment_id: Option<SegmentId>,
    ) -> Result<()> {
        let loaded = match after_segment_id {
            Some(id) => {
                storage
                    .scan_segments(id.saturating_add(1)..u32::MAX)
                    .await?
            }
            None => storage.scan_segments(0..u32::MAX).await?,
        };

        if after_segment_id.is_none() {
            self.segments.clear();
        }

        for segment in loaded {
            self.segments.insert(segment.meta.start_seq, segment);
        }

        Ok(())
    }

    /// Builds a delta for segment state, adding a segment record if needed.
    ///
    /// Determines whether to use an existing segment or create a new one based on
    /// the seal interval configuration. If a new segment is needed, adds the segment
    /// metadata record to `records`.
    ///
    /// Does NOT update the cache - call `apply_delta()` after the storage write succeeds.
    ///
    /// # Parameters
    /// - `current_time_ms`: Current wall-clock time in milliseconds since Unix epoch
    /// - `start_seq`: The starting sequence number for a new segment if one is created
    /// - `records`: Mutable vec to append the segment meta record to (if new segment)
    ///
    /// # Returns
    /// A `SegmentDelta` containing the segment to use and whether it's new.
    pub(crate) fn build_delta(
        &self,
        current_time_ms: i64,
        start_seq: u64,
        records: &mut Vec<Record>,
    ) -> SegmentDelta {
        let latest = self.latest();
        let needs_new_segment =
            Self::should_roll(self.config.seal_interval, current_time_ms, latest.as_ref());

        if needs_new_segment {
            let segment_id = latest.map(|s| s.id + 1).unwrap_or(0);
            let meta = SegmentMeta::new(start_seq, current_time_ms);
            let key = SegmentMetaKey::new(segment_id).serialize();
            let value = meta.serialize();
            records.push(Record::new(key, value));

            let segment = LogSegment::new(segment_id, meta);
            SegmentDelta::new(segment, true)
        } else {
            // Safe to unwrap: should_roll returns true if latest is None
            let segment = latest.unwrap();
            SegmentDelta::new(segment, false)
        }
    }

    /// Applies a delta to update the segment cache.
    ///
    /// Call this after the storage write succeeds. Only updates the cache
    /// if the delta indicates a new segment was created.
    pub(crate) fn apply_delta(&mut self, delta: SegmentDelta) {
        if delta.is_new {
            self.insert(delta.segment);
        }
    }

    /// Checks if a new segment should be created based on seal interval.
    fn should_roll(
        seal_interval: Option<Duration>,
        current_time_ms: i64,
        latest: Option<&LogSegment>,
    ) -> bool {
        // No latest segment means we need to create the first one
        let Some(latest) = latest else {
            return true;
        };

        // No seal interval means we never roll (use existing segment)
        let Some(seal_interval) = seal_interval else {
            return false;
        };

        let seal_interval_ms = seal_interval.as_millis() as i64;
        let segment_age_ms = current_time_ms - latest.meta().start_time_ms;
        segment_age_ms >= seal_interval_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LogStorage;

    // Helper to create a segment and write it to storage + cache
    async fn write_segment(
        storage: &LogStorage,
        cache: &mut SegmentCache,
        meta: SegmentMeta,
    ) -> LogSegment {
        let segment_id = match cache.latest() {
            Some(latest) => latest.id + 1,
            None => 0,
        };
        let segment = LogSegment::new(segment_id, meta);
        storage.write_segment(&segment).await.unwrap();
        cache.insert(segment.clone());
        segment
    }

    #[tokio::test]
    async fn should_return_none_when_no_segments_exist() {
        // given
        let storage = LogStorage::in_memory();
        let cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();

        // when
        let latest = cache.latest();

        // then
        assert!(latest.is_none());
    }

    #[tokio::test]
    async fn should_write_first_segment_with_id_zero() {
        // given
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        let meta = SegmentMeta::new(0, 1000);

        // when
        let segment = write_segment(&storage, &mut cache, meta.clone()).await;

        // then
        assert_eq!(segment.id(), 0);
        assert_eq!(segment.meta(), &meta);
    }

    #[tokio::test]
    async fn should_increment_segment_id_on_subsequent_writes() {
        // given
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();

        // when
        let seg0 = write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        let seg1 = write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        let seg2 = write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // then
        assert_eq!(seg0.id(), 0);
        assert_eq!(seg1.id(), 1);
        assert_eq!(seg2.id(), 2);
    }

    #[tokio::test]
    async fn should_return_latest_segment() {
        // given
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;

        // when
        let latest = cache.latest();

        // then
        assert_eq!(latest.unwrap().id(), 1);
    }

    #[tokio::test]
    async fn should_scan_all_segments() {
        // given
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // when
        let segments = cache.all();

        // then
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
        assert_eq!(segments[2].id(), 2);
    }

    #[tokio::test]
    async fn should_persist_segments_to_storage() {
        // given
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;

        // when - reopen cache from same storage
        let cache2 = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        let segments = cache2.all();

        // then
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[0].meta().start_seq, 0);
        assert_eq!(segments[1].id(), 1);
        assert_eq!(segments[1].meta().start_seq, 100);
    }

    #[tokio::test]
    async fn should_find_segments_by_seq_range_all() {
        // given: segments at seq 0, 100, 200
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // when: query all sequences
        let segments = cache.find_covering(&(0..u64::MAX));

        // then: all segments match
        assert_eq!(segments.len(), 3);
    }

    #[tokio::test]
    async fn should_find_segments_by_seq_range_single() {
        // given: segments at seq 0, 100, 200
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // when: query seq 50..60 (within first segment)
        let segments = cache.find_covering(&(50..60));

        // then: only first segment matches
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id(), 0);
    }

    #[tokio::test]
    async fn should_find_segments_by_seq_range_spanning() {
        // given: segments at seq 0, 100, 200
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // when: query seq 50..150 (spans first and second segment)
        let segments = cache.find_covering(&(50..150));

        // then: first two segments match
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
    }

    #[tokio::test]
    async fn should_find_segments_by_seq_range_unbounded_end() {
        // given: segments at seq 0, 100, 200
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // when: query seq 150.. (from middle of second segment to end)
        let segments = cache.find_covering(&(150..u64::MAX));

        // then: second and third segments match
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 1);
        assert_eq!(segments[1].id(), 2);
    }

    #[tokio::test]
    async fn should_find_no_segments_when_range_before_all() {
        // given: segments starting at seq 100
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 1000)).await;

        // when: query seq 0..50 (before any segment data)
        let segments = cache.find_covering(&(0..50));

        // then: no segments match (segment starts at 100)
        assert_eq!(segments.len(), 0);
    }

    #[tokio::test]
    async fn should_find_no_segments_when_storage_empty() {
        // given: no segments
        let storage = LogStorage::in_memory();
        let cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();

        // when: query any range
        let segments = cache.find_covering(&(0..u64::MAX));

        // then: no segments
        assert_eq!(segments.len(), 0);
    }

    #[tokio::test]
    async fn should_find_last_segment_when_range_after_all() {
        // given: segments at seq 0, 100, 200
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // when: query seq 500..600 (after all segment starts)
        let segments = cache.find_covering(&(500..600));

        // then: last segment matches (it could contain seqs 200+)
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id(), 2);
    }

    #[tokio::test]
    async fn should_find_segment_when_query_starts_at_boundary() {
        // given: segments at seq 0, 100, 200
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // when: query starting exactly at segment boundary
        let segments = cache.find_covering(&(100..150));

        // then: only the segment starting at 100 matches
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].id(), 1);
    }

    #[tokio::test]
    async fn should_find_segments_with_unbounded_start() {
        // given: segments at seq 0, 100, 200
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(100, 2000)).await;
        write_segment(&storage, &mut cache, SegmentMeta::new(200, 3000)).await;

        // when: query with unbounded start ..150
        let segments = cache.find_covering(&(0..150));

        // then: first two segments match
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].id(), 0);
        assert_eq!(segments[1].id(), 1);
    }

    #[tokio::test]
    async fn should_roll_returns_false_when_no_seal_interval() {
        // given: no seal interval, no latest segment
        let should_roll = SegmentCache::should_roll(None, 1000, None);

        // then: should roll (no segment exists)
        assert!(should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_true_when_no_segments_exist() {
        // given: seal interval configured but no segments
        let seal_interval = Some(Duration::from_secs(3600));

        // when
        let should_roll = SegmentCache::should_roll(seal_interval, 1000, None);

        // then: should roll to create first segment
        assert!(should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_false_when_within_interval() {
        // given: segment created at time 1000, seal interval 1 hour
        let seal_interval = Some(Duration::from_secs(3600));
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));

        // when: current time is 1000 + 30 minutes
        let current_time_ms = 1000 + 30 * 60 * 1000;
        let should_roll = SegmentCache::should_roll(seal_interval, current_time_ms, Some(&segment));

        // then
        assert!(!should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_true_when_interval_exceeded() {
        // given: segment created at time 1000, seal interval 1 hour
        let seal_interval = Some(Duration::from_secs(3600));
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));

        // when: current time is 1000 + 2 hours
        let current_time_ms = 1000 + 2 * 60 * 60 * 1000;
        let should_roll = SegmentCache::should_roll(seal_interval, current_time_ms, Some(&segment));

        // then
        assert!(should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_true_when_exactly_at_interval() {
        // given: segment created at time 1000, seal interval 1 hour
        let seal_interval = Some(Duration::from_secs(3600));
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));

        // when: current time is exactly at the interval boundary
        let current_time_ms = 1000 + 60 * 60 * 1000;
        let should_roll = SegmentCache::should_roll(seal_interval, current_time_ms, Some(&segment));

        // then: at boundary should roll
        assert!(should_roll);
    }

    #[tokio::test]
    async fn should_roll_returns_false_without_seal_interval_when_segment_exists() {
        // given: no seal interval, segment exists
        let segment = LogSegment::new(0, SegmentMeta::new(0, 1000));

        // when
        let should_roll = SegmentCache::should_roll(None, 999999999, Some(&segment));

        // then: never rolls without seal_interval when segment exists
        assert!(!should_roll);
    }

    #[tokio::test]
    async fn build_delta_creates_first_segment_when_none_exist() {
        // given
        let storage = LogStorage::in_memory();
        let cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        let mut records = Vec::new();

        // when
        let delta = cache.build_delta(1000, 0, &mut records);

        // then
        assert!(delta.is_new());
        assert_eq!(delta.segment().id(), 0);
        assert_eq!(delta.segment().meta().start_seq, 0);
        assert_eq!(delta.segment().meta().start_time_ms, 1000);
        assert_eq!(records.len(), 1); // segment meta record added
    }

    #[tokio::test]
    async fn build_delta_returns_existing_segment_when_within_interval() {
        // given: segment exists, within seal interval
        let storage = LogStorage::in_memory();
        let config = SegmentConfig {
            seal_interval: Some(Duration::from_secs(3600)),
        };
        let mut cache = SegmentCache::open(&storage.as_read(), config)
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        let mut records = Vec::new();

        // when: request 30 minutes later
        let current_time_ms = 1000 + 30 * 60 * 1000;
        let delta = cache.build_delta(current_time_ms, 100, &mut records);

        // then: returns existing segment, no new record
        assert!(!delta.is_new());
        assert_eq!(delta.segment().id(), 0);
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn build_delta_creates_new_segment_when_interval_exceeded() {
        // given: segment at time 1000, seal interval 1 hour
        let storage = LogStorage::in_memory();
        let config = SegmentConfig {
            seal_interval: Some(Duration::from_secs(3600)),
        };
        let mut cache = SegmentCache::open(&storage.as_read(), config)
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        let mut records = Vec::new();

        // when: request 2 hours later
        let current_time_ms = 1000 + 2 * 60 * 60 * 1000;
        let delta = cache.build_delta(current_time_ms, 100, &mut records);

        // then: creates new segment
        assert!(delta.is_new());
        assert_eq!(delta.segment().id(), 1);
        assert_eq!(delta.segment().meta().start_seq, 100);
        assert_eq!(records.len(), 1);
    }

    #[tokio::test]
    async fn build_delta_does_not_update_cache() {
        // given
        let storage = LogStorage::in_memory();
        let cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        let mut records = Vec::new();

        // when
        let _delta = cache.build_delta(1000, 0, &mut records);

        // then: cache is not updated
        assert!(cache.latest().is_none());
    }

    #[tokio::test]
    async fn apply_delta_updates_cache_for_new_segment() {
        // given
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        let mut records = Vec::new();
        let delta = cache.build_delta(1000, 0, &mut records);

        // when
        cache.apply_delta(delta);

        // then: cache is updated
        let latest = cache.latest().unwrap();
        assert_eq!(latest.id(), 0);
        assert_eq!(latest.meta().start_seq, 0);
    }

    #[tokio::test]
    async fn apply_delta_is_noop_for_existing_segment() {
        // given: segment exists
        let storage = LogStorage::in_memory();
        let mut cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        write_segment(&storage, &mut cache, SegmentMeta::new(0, 1000)).await;
        let mut records = Vec::new();

        // when: build delta returns existing segment
        let delta = cache.build_delta(2000, 100, &mut records);
        assert!(!delta.is_new());

        // apply should be a no-op
        cache.apply_delta(delta);

        // then: still only one segment
        let all = cache.all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id(), 0);
    }

    #[tokio::test]
    async fn build_delta_creates_correct_segment_meta_record() {
        // given
        let storage = LogStorage::in_memory();
        let cache = SegmentCache::open(&storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        let mut records = Vec::new();

        // when
        let delta = cache.build_delta(5000, 42, &mut records);

        // then: verify the record can be deserialized
        assert_eq!(records.len(), 1);
        let key = SegmentMetaKey::deserialize(&records[0].key).unwrap();
        let meta = SegmentMeta::deserialize(&records[0].value).unwrap();
        assert_eq!(key.segment_id, delta.segment().id());
        assert_eq!(meta.start_seq, 42);
        assert_eq!(meta.start_time_ms, 5000);
    }
}

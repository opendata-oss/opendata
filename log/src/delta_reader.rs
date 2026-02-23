use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::sync::Arc;

use crate::storage::LogStorageRead;
use bytes::Bytes;

use crate::delta_writer::LogDelta;
use crate::error::Result;
use crate::listing::LogKeyIterator;
use crate::model::{LogEntry, Segment, SegmentId, Sequence};
use crate::reader::LogIterator;
use crate::segment::{LogSegment, SegmentSnapshot};

/// Broadcast payload sent to subscribers after a freeze.
///
/// Contains segment metadata and a read-optimized index of log entries
/// grouped by key. Shared via `Arc` (see [`Delta::FrozenView`]) so that
/// broadcasting to subscribers is cheap.
pub(crate) struct FrozenLogDeltaView {
    pub new_segments: Vec<LogSegment>,
    pub entries: HashMap<Bytes, Vec<LogEntry>>,
    /// Maps segment ID → keys that are new to that segment in this delta.
    pub segment_keys: HashMap<SegmentId, BTreeSet<Bytes>>,
}

impl FrozenLogDeltaView {
    pub fn new(
        new_segments: Vec<LogSegment>,
        entries: HashMap<Bytes, Vec<LogEntry>>,
        segment_keys: HashMap<SegmentId, BTreeSet<Bytes>>,
    ) -> Self {
        Self {
            new_segments,
            entries,
            segment_keys,
        }
    }
}

/// A point-in-time read view of the log.
///
/// Combines the storage snapshot, segment cache, and any frozen deltas
/// that haven't been flushed yet. Query methods merge results across
/// all three sources.
type View = common::coordinator::View<LogDelta>;

/// Iterator over log entries from frozen (unflushed) deltas.
///
/// Walks frozen deltas in oldest-first order, yielding entries for a given
/// key within the sequence range. Each frozen delta contains a
/// `HashMap<Bytes, Vec<LogEntry>>` index; entries within each delta are
/// already sorted by sequence number.
pub(crate) struct FrozenViewLogIterator {
    view: Arc<View>,
    key: Bytes,
    seq_range: Range<Sequence>,
    /// Index into `view.frozen`, counting from oldest (len-1) to newest (0).
    frozen_idx: usize,
    /// Position within the current delta's filtered entry slice.
    entry_idx: usize,
}

impl FrozenViewLogIterator {
    pub(crate) fn new(view: Arc<View>, key: Bytes, seq_range: Range<Sequence>) -> Self {
        Self {
            view,
            key,
            seq_range,
            frozen_idx: 0,
            entry_idx: 0,
        }
    }

    /// Returns the next log entry from frozen deltas, or None if exhausted.
    pub(crate) fn next(&mut self) -> Option<LogEntry> {
        let frozen = &self.view.frozen;
        let num_frozen = frozen.len();

        loop {
            if self.frozen_idx >= num_frozen {
                return None;
            }

            // frozen is newest-first; iterate oldest-first
            let delta_idx = num_frozen - 1 - self.frozen_idx;
            let delta = &frozen[delta_idx].val;

            if let Some(entries) = delta.entries.get(&self.key) {
                // Find the slice within seq_range using binary search
                let start = entries.partition_point(|e| e.sequence < self.seq_range.start);
                let end = entries.partition_point(|e| e.sequence < self.seq_range.end);
                let slice = &entries[start..end];

                if self.entry_idx < slice.len() {
                    let entry = slice[self.entry_idx].clone();
                    self.entry_idx += 1;
                    return Some(entry);
                }
            }

            // Current delta exhausted, move to next
            self.frozen_idx += 1;
            self.entry_idx = 0;
        }
    }
}

/// Tracks flushed and unflushed segments for the coordinator.
///
/// Flushed segments come from storage and are stored in a [`SegmentSnapshot`].
/// Unflushed segments come from frozen (not yet flushed) deltas and are tracked
/// in a separate snapshot.
pub(crate) struct SegmentView {
    /// Segments that have been flushed to storage.
    flushed: SegmentSnapshot,
    /// Segments from frozen (unflushed) deltas.
    unflushed: SegmentSnapshot,
}

impl SegmentView {
    /// Creates a new segment view from the initial coordinator view.
    pub(crate) async fn open(view: &View) -> Result<Self> {
        let flushed = SegmentSnapshot::open(&*view.snapshot).await?;
        let unflushed = Self::collect_unflushed(view);
        Ok(Self { flushed, unflushed })
    }

    /// Finds segments covering the given sequence range, merging flushed
    /// and unflushed segments.
    pub(crate) fn find_covering(&self, range: &Range<u64>) -> Vec<LogSegment> {
        if range.start >= range.end {
            return Vec::new();
        }
        // Merge candidates from both sources and prune.
        let mut candidates = self.flushed.find_covering(range);
        candidates.extend(self.unflushed.find_covering(range));

        // Keep from the covering segment (largest start_seq <= range.start) onward.
        let first_after = candidates.partition_point(|s| s.meta().start_seq <= range.start);
        if first_after > 1 {
            candidates.drain(..first_after - 1);
        }
        candidates
    }

    /// Updates segment tracking from a new coordinator view.
    pub(crate) fn update(&mut self, view: &View) {
        // Move segments from the last flushed delta into the snapshot.
        if let Some(flushed) = &view.last_flushed_delta {
            for segment in &flushed.val.new_segments {
                self.flushed.insert(segment.clone());
            }
        }

        self.unflushed = Self::collect_unflushed(view);
    }

    /// Rebuilds the unflushed snapshot from the current frozen deltas.
    fn collect_unflushed(view: &View) -> SegmentSnapshot {
        let mut segments = BTreeMap::new();
        for frozen in &view.frozen {
            for seg in &frozen.val.new_segments {
                segments.insert(seg.meta().start_seq, seg.clone());
            }
        }
        SegmentSnapshot::from_segments(segments)
    }
}

/// A read view backed by the write coordinator.
///
/// Used by [`LogDb`](crate::LogDb) for reads that merge storage snapshot
/// data with frozen (unflushed) deltas from the coordinator.
pub(crate) struct DeltaReaderView {
    view: Arc<View>,
    segments: SegmentView,
}

impl DeltaReaderView {
    pub(crate) async fn open(view: Arc<View>) -> Result<Self> {
        let segments = SegmentView::open(&view).await?;
        Ok(Self { view, segments })
    }

    /// Updates the view from a coordinator broadcast.
    pub(crate) fn update_view(&mut self, view: Arc<View>) {
        self.segments.update(&view);
        self.view = view;
    }

    pub(crate) fn scan_entries(&self, key: Bytes, seq_range: Range<Sequence>) -> LogIterator {
        let segments = self.segments.find_covering(&seq_range);
        LogIterator::from_view(segments, Arc::clone(&self.view), key, seq_range)
    }

    pub(crate) async fn list_keys(
        &self,
        segment_range: Range<SegmentId>,
    ) -> Result<LogKeyIterator> {
        let mut keys = self.view.snapshot.list_keys(segment_range.clone()).await?;
        collect_frozen_keys(&self.view, &segment_range, &mut keys);
        Ok(LogKeyIterator::from_keys(keys))
    }

    pub(crate) fn list_segments(&self, seq_range: Range<Sequence>) -> Vec<Segment> {
        self.segments
            .find_covering(&seq_range)
            .into_iter()
            .map(|s| s.into())
            .collect()
    }
}

/// Collects keys from frozen (unflushed) deltas that belong to segments
/// within the given range, inserting them directly into `keys`.
///
/// Each frozen delta tracks an explicit `segment_keys` mapping (populated at
/// delta freeze time), so we simply look up matching segment IDs directly.
fn collect_frozen_keys(view: &View, segment_range: &Range<SegmentId>, keys: &mut BTreeSet<Bytes>) {
    for frozen in &view.frozen {
        for (seg_id, seg_keys) in &frozen.val.segment_keys {
            if segment_range.contains(seg_id) {
                keys.extend(seg_keys.iter().cloned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::Storage;
    use common::coordinator::EpochStamped;
    use common::storage::in_memory::InMemoryStorage;

    fn entry(key: &str, seq: u64, value: &str) -> LogEntry {
        LogEntry {
            key: Bytes::from(key.to_string()),
            sequence: seq,
            value: Bytes::from(value.to_string()),
        }
    }

    fn frozen_delta(entries: Vec<LogEntry>) -> Arc<FrozenLogDeltaView> {
        let mut map: HashMap<Bytes, Vec<LogEntry>> = HashMap::new();
        for e in entries {
            map.entry(e.key.clone()).or_default().push(e);
        }
        Arc::new(FrozenLogDeltaView::new(vec![], map, HashMap::new()))
    }

    /// Creates a View with the given frozen deltas (newest-first, matching
    /// the coordinator convention).
    async fn make_view(frozen_newest_first: Vec<Arc<FrozenLogDeltaView>>) -> Arc<View> {
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let snapshot = storage.snapshot().await.unwrap();
        Arc::new(View {
            current: (),
            frozen: frozen_newest_first
                .into_iter()
                .enumerate()
                .map(|(i, v)| EpochStamped {
                    val: v,
                    epoch_range: i as u64..(i + 1) as u64,
                })
                .collect(),
            snapshot,
            last_flushed_delta: None,
        })
    }

    #[tokio::test]
    async fn frozen_iter_returns_none_when_no_frozen_deltas() {
        let view = make_view(vec![]).await;
        let mut iter = FrozenViewLogIterator::new(view, Bytes::from("key"), 0..u64::MAX);
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn frozen_iter_yields_entries_from_single_delta() {
        let delta = frozen_delta(vec![entry("key", 10, "v10"), entry("key", 11, "v11")]);
        let view = make_view(vec![delta]).await;

        let mut iter = FrozenViewLogIterator::new(view, Bytes::from("key"), 0..u64::MAX);

        let e = iter.next().unwrap();
        assert_eq!(e.sequence, 10);
        assert_eq!(e.value, Bytes::from("v10"));

        let e = iter.next().unwrap();
        assert_eq!(e.sequence, 11);
        assert_eq!(e.value, Bytes::from("v11"));

        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn frozen_iter_yields_entries_across_multiple_deltas_in_sequence_order() {
        // Frozen vec is newest-first: delta_b (newer) then delta_a (older)
        let delta_a = frozen_delta(vec![entry("key", 5, "v5"), entry("key", 6, "v6")]);
        let delta_b = frozen_delta(vec![entry("key", 10, "v10"), entry("key", 11, "v11")]);
        let view = make_view(vec![delta_b, delta_a]).await;

        let mut iter = FrozenViewLogIterator::new(view, Bytes::from("key"), 0..u64::MAX);

        // Should yield oldest-first: delta_a entries, then delta_b entries
        assert_eq!(iter.next().unwrap().sequence, 5);
        assert_eq!(iter.next().unwrap().sequence, 6);
        assert_eq!(iter.next().unwrap().sequence, 10);
        assert_eq!(iter.next().unwrap().sequence, 11);
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn frozen_iter_filters_by_key() {
        let delta = frozen_delta(vec![
            entry("key-a", 1, "a1"),
            entry("key-b", 2, "b2"),
            entry("key-a", 3, "a3"),
        ]);
        let view = make_view(vec![delta]).await;

        let mut iter = FrozenViewLogIterator::new(view, Bytes::from("key-a"), 0..u64::MAX);

        assert_eq!(iter.next().unwrap().sequence, 1);
        assert_eq!(iter.next().unwrap().sequence, 3);
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn frozen_iter_filters_by_seq_range() {
        let delta = frozen_delta(vec![
            entry("key", 5, "v5"),
            entry("key", 10, "v10"),
            entry("key", 15, "v15"),
            entry("key", 20, "v20"),
        ]);
        let view = make_view(vec![delta]).await;

        let mut iter = FrozenViewLogIterator::new(view, Bytes::from("key"), 10..20);

        assert_eq!(iter.next().unwrap().sequence, 10);
        assert_eq!(iter.next().unwrap().sequence, 15);
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn frozen_iter_returns_none_when_no_matching_entries() {
        let delta = frozen_delta(vec![entry("other", 1, "v1")]);
        let view = make_view(vec![delta]).await;

        let mut iter = FrozenViewLogIterator::new(view, Bytes::from("key"), 0..u64::MAX);
        assert!(iter.next().is_none());
    }

    mod coordinator_view {
        use super::*;
        use crate::segment::LogSegment;
        use crate::serde::SegmentMeta;

        fn frozen_delta_with_segments(
            segments: Vec<LogSegment>,
            entries: Vec<LogEntry>,
        ) -> Arc<FrozenLogDeltaView> {
            let mut map: HashMap<Bytes, Vec<LogEntry>> = HashMap::new();
            for e in &entries {
                map.entry(e.key.clone()).or_default().push(e.clone());
            }
            // Build segment_keys: assign all entry keys to each segment.
            // In tests, entries typically belong to the segments provided.
            let mut segment_keys: HashMap<SegmentId, BTreeSet<Bytes>> = HashMap::new();
            for seg in &segments {
                let keys: BTreeSet<Bytes> = entries.iter().map(|e| e.key.clone()).collect();
                if !keys.is_empty() {
                    segment_keys.insert(seg.id(), keys);
                }
            }
            Arc::new(FrozenLogDeltaView::new(segments, map, segment_keys))
        }

        #[tokio::test]
        async fn returns_keys_from_frozen_deltas() {
            // Delta with segment 0 and two keys
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            let delta = frozen_delta_with_segments(
                vec![seg0.clone()],
                vec![entry("key-a", 0, "v0"), entry("key-b", 1, "v1")],
            );
            let view = make_view(vec![delta]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let mut iter = delta_view.list_keys(0..1).await.unwrap();
            let k1 = iter.next().await.unwrap().unwrap();
            let k2 = iter.next().await.unwrap().unwrap();
            assert!(iter.next().await.unwrap().is_none());

            // Keys should be in lexicographic order
            assert_eq!(k1.key, Bytes::from("key-a"));
            assert_eq!(k2.key, Bytes::from("key-b"));
        }

        #[tokio::test]
        async fn keys_from_multiple_frozen_deltas_are_merged_and_deduplicated() {
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            // Older delta: key-a, key-b in segment 0
            let delta_a = frozen_delta_with_segments(
                vec![seg0.clone()],
                vec![entry("key-a", 0, "v0"), entry("key-b", 1, "v1")],
            );
            // Newer delta: key-b (dup), key-c in segment 0 (no new segments,
            // but segment_keys explicitly maps them to segment 0)
            let delta_b = {
                let entries = vec![entry("key-b", 2, "v2"), entry("key-c", 3, "v3")];
                let mut map: HashMap<Bytes, Vec<LogEntry>> = HashMap::new();
                for e in &entries {
                    map.entry(e.key.clone()).or_default().push(e.clone());
                }
                let mut segment_keys: HashMap<SegmentId, BTreeSet<Bytes>> = HashMap::new();
                segment_keys.insert(0, entries.iter().map(|e| e.key.clone()).collect());
                Arc::new(FrozenLogDeltaView::new(vec![], map, segment_keys))
            };
            // newest-first: delta_b, delta_a
            let view = make_view(vec![delta_b, delta_a]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let mut iter = delta_view.list_keys(0..1).await.unwrap();
            let mut keys = vec![];
            while let Some(k) = iter.next().await.unwrap() {
                keys.push(k.key);
            }
            assert_eq!(
                keys,
                vec![
                    Bytes::from("key-a"),
                    Bytes::from("key-b"),
                    Bytes::from("key-c"),
                ]
            );
        }

        #[tokio::test]
        async fn segment_range_filters_frozen_keys() {
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            let seg1 = LogSegment::new(1, SegmentMeta::new(100, 2000));
            // Older delta: key-a in segment 0
            let delta_a =
                frozen_delta_with_segments(vec![seg0.clone()], vec![entry("key-a", 0, "v0")]);
            // Newer delta: key-b in segment 1
            let delta_b =
                frozen_delta_with_segments(vec![seg1.clone()], vec![entry("key-b", 100, "v100")]);
            // newest-first
            let view = make_view(vec![delta_b, delta_a]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            // Only query segment 1
            let mut iter = delta_view.list_keys(1..2).await.unwrap();
            let k = iter.next().await.unwrap().unwrap();
            assert_eq!(k.key, Bytes::from("key-b"));
            assert!(iter.next().await.unwrap().is_none());
        }

        /// Creates a View whose snapshot comes from the given storage,
        /// with the given frozen deltas (newest-first).
        async fn make_view_with_storage(
            storage: Arc<dyn Storage>,
            frozen_newest_first: Vec<Arc<FrozenLogDeltaView>>,
        ) -> Arc<View> {
            let snapshot = storage.snapshot().await.unwrap();
            Arc::new(View {
                current: (),
                frozen: frozen_newest_first
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| EpochStamped {
                        val: v,
                        epoch_range: i as u64..(i + 1) as u64,
                    })
                    .collect(),
                snapshot,
                last_flushed_delta: None,
            })
        }

        /// Writes a listing entry into storage for the given segment and key.
        async fn write_listing_entry(storage: &dyn Storage, segment_id: u32, key: &[u8]) {
            use crate::serde::{ListingEntryKey, ListingEntryValue};
            let storage_key =
                ListingEntryKey::new(segment_id, Bytes::copy_from_slice(key)).serialize();
            let value = ListingEntryValue::new().serialize();
            storage
                .put(vec![common::Record::new(storage_key, value)])
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn frozen_keys_not_in_snapshot_are_included() {
            // Storage has key-a in segment 0
            let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
            write_listing_entry(&*storage, 0, b"key-a").await;

            // Frozen delta adds key-b (not in storage) in segment 0
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            let delta =
                frozen_delta_with_segments(vec![seg0.clone()], vec![entry("key-b", 1, "v1")]);
            let view = make_view_with_storage(Arc::clone(&storage), vec![delta]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let mut iter = delta_view.list_keys(0..1).await.unwrap();
            let mut keys = vec![];
            while let Some(k) = iter.next().await.unwrap() {
                keys.push(k.key);
            }
            // Both storage key and frozen key should appear
            assert_eq!(keys, vec![Bytes::from("key-a"), Bytes::from("key-b"),]);
        }

        #[tokio::test]
        async fn frozen_and_storage_keys_are_deduplicated() {
            // Storage has key-a in segment 0
            let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
            write_listing_entry(&*storage, 0, b"key-a").await;

            // Frozen delta also has key-a (same key, new entry) in segment 0
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            let delta =
                frozen_delta_with_segments(vec![seg0.clone()], vec![entry("key-a", 1, "v1")]);
            let view = make_view_with_storage(Arc::clone(&storage), vec![delta]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let mut iter = delta_view.list_keys(0..1).await.unwrap();
            let mut keys = vec![];
            while let Some(k) = iter.next().await.unwrap() {
                keys.push(k.key);
            }
            // key-a appears only once despite being in both sources
            assert_eq!(keys, vec![Bytes::from("key-a")]);
        }

        #[tokio::test]
        async fn no_frozen_deltas_returns_storage_keys() {
            // Storage has keys in segment 0
            let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
            write_listing_entry(&*storage, 0, b"key-a").await;
            write_listing_entry(&*storage, 0, b"key-b").await;

            // View with empty frozen list
            let view = make_view_with_storage(Arc::clone(&storage), vec![]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let mut iter = delta_view.list_keys(0..1).await.unwrap();
            let mut keys = vec![];
            while let Some(k) = iter.next().await.unwrap() {
                keys.push(k.key);
            }
            assert_eq!(keys, vec![Bytes::from("key-a"), Bytes::from("key-b"),]);
        }

        #[tokio::test]
        async fn empty_segment_range_returns_no_keys() {
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            let delta = frozen_delta_with_segments(vec![seg0], vec![entry("key-a", 0, "v0")]);
            let view = make_view(vec![delta]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let mut iter = delta_view.list_keys(0..0).await.unwrap();
            assert!(iter.next().await.unwrap().is_none());
        }

        #[tokio::test]
        async fn keys_returned_in_lexicographic_order() {
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            let delta = frozen_delta_with_segments(
                vec![seg0],
                vec![
                    entry("zebra", 0, "v0"),
                    entry("apple", 1, "v1"),
                    entry("mango", 2, "v2"),
                ],
            );
            let view = make_view(vec![delta]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let mut iter = delta_view.list_keys(0..1).await.unwrap();
            let mut keys = vec![];
            while let Some(k) = iter.next().await.unwrap() {
                keys.push(k.key);
            }
            assert_eq!(
                keys,
                vec![
                    Bytes::from("apple"),
                    Bytes::from("mango"),
                    Bytes::from("zebra"),
                ]
            );
        }

        #[tokio::test]
        async fn list_segments_includes_frozen_segments() {
            // Frozen delta introduces segment 0
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            let delta = frozen_delta_with_segments(vec![seg0], vec![entry("key-a", 0, "v0")]);
            let view = make_view(vec![delta]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let segments = delta_view.list_segments(0..u64::MAX);
            assert_eq!(segments.len(), 1);
            assert_eq!(segments[0].id, 0);
            assert_eq!(segments[0].start_seq, 0);
        }

        #[tokio::test]
        async fn list_segments_merges_cached_and_frozen() {
            // Storage has segment 0
            let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
            {
                use crate::serde::SegmentMetaKey;
                let key = SegmentMetaKey::new(0).serialize();
                let value = SegmentMeta::new(0, 1000).serialize();
                storage
                    .put(vec![common::Record::new(key, value)])
                    .await
                    .unwrap();
            }

            // Frozen delta introduces segment 1
            let seg1 = LogSegment::new(1, SegmentMeta::new(100, 2000));
            let delta = frozen_delta_with_segments(vec![seg1], vec![entry("key-a", 100, "v100")]);
            let view = make_view_with_storage(Arc::clone(&storage), vec![delta]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            let segments = delta_view.list_segments(0..u64::MAX);
            assert_eq!(segments.len(), 2);
            assert_eq!(segments[0].id, 0);
            assert_eq!(segments[0].start_seq, 0);
            assert_eq!(segments[1].id, 1);
            assert_eq!(segments[1].start_seq, 100);
        }

        #[tokio::test]
        async fn list_segments_filters_frozen_by_seq_range() {
            let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
            let seg1 = LogSegment::new(1, SegmentMeta::new(100, 2000));
            let delta_a = frozen_delta_with_segments(vec![seg0], vec![entry("key-a", 0, "v0")]);
            let delta_b = frozen_delta_with_segments(vec![seg1], vec![entry("key-a", 100, "v100")]);
            // newest-first
            let view = make_view(vec![delta_b, delta_a]).await;

            let delta_view = DeltaReaderView::open(view).await.unwrap();

            // Query only sequences in segment 1's range
            let segments = delta_view.list_segments(100..200);
            assert_eq!(segments.len(), 1);
            assert_eq!(segments[0].id, 1);
            assert_eq!(segments[0].start_seq, 100);
        }
    }
}

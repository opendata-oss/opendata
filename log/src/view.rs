use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use common::StorageRead;

use crate::config::SegmentConfig;
use crate::delta::LogDelta;
use crate::error::Result;
use crate::listing::LogKeyIterator;
use crate::model::{LogEntry, Segment, SegmentId, Sequence};
use crate::reader::LogIterator;
use crate::segment::{LogSegment, SegmentCache};
use crate::storage::LogStorageRead;

/// Broadcast payload sent to subscribers after a freeze.
///
/// Contains segment metadata and a read-optimized index of log entries
/// grouped by key. Shared via `Arc` (see [`Delta::FrozenView`]) so that
/// broadcasting to subscribers is cheap.
pub(crate) struct FrozenLogDeltaView {
    pub new_segments: Vec<LogSegment>,
    pub entries: HashMap<Bytes, Vec<LogEntry>>,
}

impl FrozenLogDeltaView {
    pub fn new(new_segments: Vec<LogSegment>, entries: HashMap<Bytes, Vec<LogEntry>>) -> Self {
        Self {
            new_segments,
            entries,
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
pub(crate) struct FrozenViewIterator {
    view: Arc<View>,
    key: Bytes,
    seq_range: Range<Sequence>,
    /// Index into `view.frozen`, counting from oldest (len-1) to newest (0).
    frozen_idx: usize,
    /// Position within the current delta's filtered entry slice.
    entry_idx: usize,
}

impl FrozenViewIterator {
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

/// The source of data for reads.
///
/// Before the first coordinator broadcast, we read from the storage snapshot
/// taken at open time. After the first broadcast, we read from the
/// coordinator's [`View`], which includes both the storage snapshot and any
/// frozen (unflushed) deltas.
enum ViewSource {
    Initialized(Arc<dyn StorageRead>),
    View(Arc<View>),
}

pub(crate) struct LogView {
    source: ViewSource,
    segments: SegmentCache,
}

impl LogView {
    pub(crate) async fn open(
        storage: Arc<dyn StorageRead>,
        segment_config: SegmentConfig,
    ) -> Result<Self> {
        let log_storage = LogStorageRead::new(storage.clone());
        let segments = SegmentCache::open(&log_storage, segment_config).await?;
        Ok(Self {
            source: ViewSource::Initialized(storage),
            segments,
        })
    }

    /// Updates the view from a coordinator broadcast.
    pub(crate) fn update(&mut self, view: Arc<View>) {
        // Apply new segments from the last flushed delta
        if let Some(flushed) = &view.last_flushed_delta {
            for segment in &flushed.val.new_segments {
                self.segments.insert(segment.clone());
            }
        }

        self.source = ViewSource::View(view);
    }

    pub(crate) fn scan_entries(&self, key: Bytes, seq_range: Range<Sequence>) -> LogIterator {
        match &self.source {
            ViewSource::Initialized(storage) => {
                let log_storage = LogStorageRead::new(storage.clone());
                LogIterator::from_storage(log_storage, &self.segments, key, seq_range)
            }
            ViewSource::View(view) => {
                LogIterator::from_view(&self.segments, Arc::clone(view), key, seq_range)
            }
        }
    }

    pub(crate) fn list_keys(&self, _segment_range: Range<SegmentId>) -> LogKeyIterator {
        todo!()
    }

    pub(crate) fn list_segments(&self, _seq_range: Range<Sequence>) -> Vec<Segment> {
        todo!()
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
        Arc::new(FrozenLogDeltaView::new(vec![], map))
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
        let mut iter = FrozenViewIterator::new(view, Bytes::from("key"), 0..u64::MAX);
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn frozen_iter_yields_entries_from_single_delta() {
        let delta = frozen_delta(vec![entry("key", 10, "v10"), entry("key", 11, "v11")]);
        let view = make_view(vec![delta]).await;

        let mut iter = FrozenViewIterator::new(view, Bytes::from("key"), 0..u64::MAX);

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

        let mut iter = FrozenViewIterator::new(view, Bytes::from("key"), 0..u64::MAX);

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

        let mut iter = FrozenViewIterator::new(view, Bytes::from("key-a"), 0..u64::MAX);

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

        let mut iter = FrozenViewIterator::new(view, Bytes::from("key"), 10..20);

        assert_eq!(iter.next().unwrap().sequence, 10);
        assert_eq!(iter.next().unwrap().sequence, 15);
        assert!(iter.next().is_none());
    }

    #[tokio::test]
    async fn frozen_iter_returns_none_when_no_matching_entries() {
        let delta = frozen_delta(vec![entry("other", 1, "v1")]);
        let view = make_view(vec![delta]).await;

        let mut iter = FrozenViewIterator::new(view, Bytes::from("key"), 0..u64::MAX);
        assert!(iter.next().is_none());
    }
}

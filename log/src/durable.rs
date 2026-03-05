//! View tracking for subscriber tasks.
//!
//! The [`ViewTracker`] buffers pending writes and produces read-view snapshots
//! when a watermark advances. In read-durable mode the watermark comes from
//! storage durability notifications; in the default (flushed) mode the caller
//! advances the watermark immediately after each push.

use std::collections::VecDeque;
use std::sync::Arc;

use common::storage::StorageSnapshot;

use crate::segment::{LogSegment, SegmentCache};

/// A pending write waiting for durable confirmation.
pub(crate) struct PendingEntry {
    pub seqnum: u64,
    pub epoch: u64,
    pub snapshot: Arc<dyn StorageSnapshot>,
    pub segment: Option<LogSegment>,
}

/// Buffers pending writes and produces read views when a watermark advances.
///
/// Used by both the flushed and durable subscriber tasks. The flushed path
/// calls `advance(seqnum)` immediately after each `push`, making it a
/// pass-through. The durable path advances only when the storage engine
/// confirms durability.
pub(crate) struct ViewTracker {
    pending: VecDeque<PendingEntry>,
    segments: SegmentCache,
}

impl ViewTracker {
    /// Creates a new tracker with an initial segment cache.
    pub(crate) fn new(segments: SegmentCache) -> Self {
        Self {
            pending: VecDeque::new(),
            segments,
        }
    }

    /// Appends a pending entry to the queue.
    pub(crate) fn push(&mut self, entry: PendingEntry) {
        self.pending.push_back(entry);
    }

    /// Drains entries where `seqnum <= durable_seq`, inserting any new segments
    /// into the internal segment cache. Returns the `(epoch, snapshot)` of the
    /// last drained entry, if any.
    pub(crate) fn advance(&mut self, durable_seq: u64) -> Option<(u64, Arc<dyn StorageSnapshot>)> {
        let mut last: Option<(u64, Arc<dyn StorageSnapshot>)> = None;

        while self
            .pending
            .front()
            .is_some_and(|e| e.seqnum <= durable_seq)
        {
            let entry = self.pending.pop_front().unwrap();
            if let Some(segment) = entry.segment {
                self.segments.insert(segment);
            }
            last = Some((entry.epoch, entry.snapshot));
        }

        last
    }

    /// Returns a reference to the accumulated segment cache.
    pub(crate) fn segments(&self) -> &SegmentCache {
        &self.segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SegmentConfig;
    use crate::serde::SegmentMeta;
    use common::Storage;
    use common::storage::in_memory::InMemoryStorage;

    async fn make_snapshot() -> Arc<dyn StorageSnapshot> {
        let storage = InMemoryStorage::new();
        storage.snapshot().await.unwrap()
    }

    fn empty_cache() -> SegmentCache {
        // Open with no storage — produces an empty cache. We use a fresh
        // in-memory storage for this.
        SegmentCache::new_empty(SegmentConfig::default())
    }

    #[tokio::test]
    async fn push_then_advance_returns_correct_epoch_and_snapshot() {
        let mut tracker = ViewTracker::new(empty_cache());
        let snap = make_snapshot().await;

        tracker.push(PendingEntry {
            seqnum: 1,
            epoch: 10,
            snapshot: snap.clone(),
            segment: None,
        });

        let result = tracker.advance(1);
        assert!(result.is_some());
        let (epoch, _) = result.unwrap();
        assert_eq!(epoch, 10);
    }

    #[tokio::test]
    async fn advance_does_nothing_when_durable_seq_below_pending() {
        let mut tracker = ViewTracker::new(empty_cache());
        let snap = make_snapshot().await;

        tracker.push(PendingEntry {
            seqnum: 5,
            epoch: 1,
            snapshot: snap,
            segment: None,
        });

        let result = tracker.advance(3);
        assert!(result.is_none());
        // Entry still pending
        assert_eq!(tracker.pending.len(), 1);
    }

    #[tokio::test]
    async fn partial_advance_leaves_remaining_entries() {
        let mut tracker = ViewTracker::new(empty_cache());

        tracker.push(PendingEntry {
            seqnum: 1,
            epoch: 1,
            snapshot: make_snapshot().await,
            segment: None,
        });
        tracker.push(PendingEntry {
            seqnum: 3,
            epoch: 2,
            snapshot: make_snapshot().await,
            segment: None,
        });
        tracker.push(PendingEntry {
            seqnum: 5,
            epoch: 3,
            snapshot: make_snapshot().await,
            segment: None,
        });

        let result = tracker.advance(3);
        assert!(result.is_some());
        let (epoch, _) = result.unwrap();
        assert_eq!(epoch, 2); // last drained entry had epoch 2
        assert_eq!(tracker.pending.len(), 1); // seqnum=5 still pending
    }

    #[tokio::test]
    async fn segments_accumulated_across_entries() {
        let mut tracker = ViewTracker::new(empty_cache());

        let seg0 = LogSegment::new(0, SegmentMeta::new(0, 1000));
        let seg1 = LogSegment::new(1, SegmentMeta::new(100, 2000));

        tracker.push(PendingEntry {
            seqnum: 1,
            epoch: 1,
            snapshot: make_snapshot().await,
            segment: Some(seg0.clone()),
        });
        tracker.push(PendingEntry {
            seqnum: 2,
            epoch: 2,
            snapshot: make_snapshot().await,
            segment: Some(seg1.clone()),
        });
        tracker.push(PendingEntry {
            seqnum: 3,
            epoch: 3,
            snapshot: make_snapshot().await,
            segment: None,
        });

        tracker.advance(3);

        let all = tracker.segments().all();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id(), 0);
        assert_eq!(all[1].id(), 1);
    }

    #[tokio::test]
    async fn advance_on_empty_tracker_returns_none() {
        let mut tracker = ViewTracker::new(empty_cache());
        assert!(tracker.advance(100).is_none());
    }
}

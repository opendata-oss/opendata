//! View tracking for subscriber tasks.
//!
//! The [`ViewTracker`] buffers pending writes and produces read-view snapshots
//! when a watermark advances. In read-durable mode the watermark comes from
//! storage durability notifications; in the default (written) mode the caller
//! advances the watermark immediately after each push.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::model::SegmentId;
use crate::reader::ReadHandle;

/// A write-aligned read view entry.
pub(crate) struct ViewEntry {
    pub seqnum: u64,
    pub epoch: u64,
    pub snapshot: Arc<ReadHandle>,
    pub last_segment_id: Option<SegmentId>,
}

/// Buffers pending writes and produces read views when a watermark advances.
///
/// Used by both the written and durable subscriber tasks. The written path
/// calls `advance(seqnum)` immediately after each `push`, making it a
/// pass-through. The durable path advances only when the storage engine
/// confirms durability.
pub(crate) struct ViewTracker {
    pending: VecDeque<ViewEntry>,
}

impl ViewTracker {
    /// Creates a new, empty tracker.
    pub(crate) fn new() -> Self {
        Self {
            pending: VecDeque::new(),
        }
    }

    /// Appends a pending entry to the queue.
    pub(crate) fn push(&mut self, entry: ViewEntry) {
        self.pending.push_back(entry);
    }

    /// Drains entries where `seqnum <= durable_seq`. Returns the last drained
    /// entry, if any.
    pub(crate) fn advance(&mut self, durable_seq: u64) -> Option<ViewEntry> {
        let mut last: Option<ViewEntry> = None;

        while self
            .pending
            .front()
            .is_some_and(|e| e.seqnum <= durable_seq)
        {
            last = Some(self.pending.pop_front().unwrap());
        }

        last
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::DbBuilder;
    use slatedb::object_store::memory::InMemory;

    async fn make_snapshot() -> Arc<ReadHandle> {
        let object_store = Arc::new(InMemory::new());
        let db = DbBuilder::new("/test/view_tracker", object_store)
            .build()
            .await
            .unwrap();
        Arc::new(ReadHandle::Snapshot(db.snapshot().await.unwrap()))
    }

    #[tokio::test]
    async fn push_then_advance_returns_correct_epoch_and_snapshot() {
        let mut tracker = ViewTracker::new();
        let snap = make_snapshot().await;

        tracker.push(ViewEntry {
            seqnum: 1,
            epoch: 10,
            snapshot: snap.clone(),
            last_segment_id: Some(7),
        });

        let result = tracker.advance(1);
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.epoch, 10);
        assert_eq!(result.last_segment_id, Some(7));
    }

    #[tokio::test]
    async fn advance_does_nothing_when_durable_seq_below_pending() {
        let mut tracker = ViewTracker::new();
        let snap = make_snapshot().await;

        tracker.push(ViewEntry {
            seqnum: 5,
            epoch: 1,
            snapshot: snap,
            last_segment_id: Some(9),
        });

        let result = tracker.advance(3);
        assert!(result.is_none());
        // Entry still pending
        assert_eq!(tracker.pending.len(), 1);
    }

    #[tokio::test]
    async fn partial_advance_leaves_remaining_entries() {
        let mut tracker = ViewTracker::new();

        tracker.push(ViewEntry {
            seqnum: 1,
            epoch: 1,
            snapshot: make_snapshot().await,
            last_segment_id: Some(0),
        });
        tracker.push(ViewEntry {
            seqnum: 3,
            epoch: 2,
            snapshot: make_snapshot().await,
            last_segment_id: Some(1),
        });
        tracker.push(ViewEntry {
            seqnum: 5,
            epoch: 3,
            snapshot: make_snapshot().await,
            last_segment_id: Some(2),
        });

        let result = tracker.advance(3);
        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.epoch, 2); // last drained entry had epoch 2
        assert_eq!(result.last_segment_id, Some(1));
        assert_eq!(tracker.pending.len(), 1); // seqnum=5 still pending
    }

    #[tokio::test]
    async fn advance_on_empty_tracker_returns_none() {
        let mut tracker = ViewTracker::new();
        assert!(tracker.advance(100).is_none());
    }
}

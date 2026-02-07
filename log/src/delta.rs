//! Write coordinator delta and flusher for the log.
//!
//! This module implements the [`Delta`] and [`Flusher`] traits from
//! the write coordinator, providing the log-specific write batching
//! and flush logic.

use crate::listing::ListingCache;
use crate::model::{AppendResult, Record as UserRecord};
use crate::segment::{LogSegment, SegmentCache};
use crate::serde::{LogEntryBuilder, SegmentMeta, SegmentMetaKey};
use crate::storage::LogStorage;
use async_trait::async_trait;
use bytes::Bytes;
use common::coordinator::{Delta, Flusher};
use common::storage::StorageSnapshot;
use common::{Record, WriteOptions};
use std::ops::Range;
use std::sync::Arc;

/// The write type for the log coordinator.
///
/// Bundles user records with a timestamp since [`Delta::apply`] doesn't
/// have clock access.
pub(crate) struct LogWrite {
    pub records: Vec<UserRecord>,
    pub timestamp_ms: i64,
    pub force_seal: bool,
}

/// Context that persists across deltas.
///
/// Owns the mutable caches needed during delta application. Returned
/// to the write coordinator on [`Delta::freeze`] so that the next
/// delta can continue where this one left off.
pub(crate) struct LogContext {
    pub sequence_allocator: common::SequenceAllocator,
    pub segment_cache: SegmentCache,
    pub listing_cache: ListingCache,
}

/// Accumulates writes for a batch before flushing to storage.
pub(crate) struct LogDelta {
    context: LogContext,
    records: Vec<Record>,
    new_segments: Vec<LogSegment>,
}

/// Frozen (immutable) snapshot of a delta, ready for flushing.
pub(crate) struct FrozenLogDelta {
    pub records: Vec<Record>,
}

/// Broadcast payload sent to subscribers after a flush.
#[derive(Clone)]
pub(crate) struct FrozenLogDeltaView {
    pub new_segments: Vec<LogSegment>,
}

/// Flushes frozen deltas to storage.
pub(crate) struct LogFlusher {
    storage: LogStorage,
}

impl LogFlusher {
    pub(crate) fn new(storage: LogStorage) -> Self {
        Self { storage }
    }
}

impl Delta for LogDelta {
    type Context = LogContext;
    type Write = LogWrite;
    // Read of latest writes not yet supported
    type DeltaView = ();
    type Frozen = FrozenLogDelta;
    type FrozenView = FrozenLogDeltaView;
    type ApplyResult = AppendResult;

    fn init(context: Self::Context) -> Self {
        Self {
            context,
            records: Vec::new(),
            new_segments: Vec::new(),
        }
    }

    /// Apply a write to the delta. Returns the [`AppendResult`] with the
    /// base sequence number assigned to the batch.
    fn apply(&mut self, write: Self::Write) -> Result<AppendResult, String> {
        let count = write.records.len() as u64;

        let base_seq = if count > 0 {
            // 1. Allocate sequences
            let (base_seq, maybe_record) = self.context.sequence_allocator.allocate(count);
            if let Some(r) = maybe_record {
                self.records.push(r);
            }

            // 2. Build segment delta
            let seg_delta = self.context.segment_cache.build_delta(
                write.timestamp_ms,
                base_seq,
                &mut self.records,
            );

            // 3. Build listing delta
            let keys: Vec<Bytes> = write.records.iter().map(|r| r.key.clone()).collect();
            let listing_delta =
                self.context
                    .listing_cache
                    .build_delta(&seg_delta, &keys, &mut self.records);

            // 4. Build log entry records
            LogEntryBuilder::build(
                seg_delta.segment(),
                base_seq,
                &write.records,
                &mut self.records,
            );

            // 5. Apply subsystem deltas to context caches
            let is_new = seg_delta.is_new();
            let segment = seg_delta.segment().clone();
            self.context.segment_cache.apply_delta(seg_delta);
            if is_new {
                self.new_segments.push(segment);
            }
            self.context.listing_cache.apply_delta(listing_delta);

            base_seq
        } else {
            self.context.sequence_allocator.peek_next_sequence()
        };

        // Handle force_seal: create a new segment
        if write.force_seal {
            let next_seq = self.context.sequence_allocator.peek_next_sequence();
            let segment_id = match self.context.segment_cache.latest() {
                Some(latest) => latest.id() + 1,
                None => 0,
            };
            let meta = SegmentMeta::new(next_seq, write.timestamp_ms);
            let segment = LogSegment::new(segment_id, meta.clone());

            // Write segment metadata record
            let key = SegmentMetaKey::new(segment_id).serialize();
            let value = meta.serialize();
            self.records.push(Record::new(key, value));

            // Update cache and track
            self.context.segment_cache.insert(segment.clone());
            self.new_segments.push(segment);
        }

        Ok(AppendResult {
            start_sequence: base_seq,
        })
    }

    fn estimate_size(&self) -> usize {
        self.records
            .iter()
            .map(|r| r.key.len() + r.value.len())
            .sum()
    }

    fn freeze(self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        let frozen_read = FrozenLogDeltaView {
            new_segments: self.new_segments,
        };
        let frozen = FrozenLogDelta {
            records: self.records,
        };
        (frozen, frozen_read, self.context)
    }

    fn reader(&self) -> Self::DeltaView {}
}

#[async_trait]
impl Flusher<LogDelta> for LogFlusher {
    async fn flush_delta(
        &self,
        frozen: FrozenLogDelta,
        _epoch_range: &Range<u64>,
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        let options = WriteOptions {
            await_durable: false,
        };
        self.storage
            .put_with_options(frozen.records, options)
            .await
            .map_err(|e| e.to_string())?;

        let snapshot = self.storage.snapshot().await.map_err(|e| e.to_string())?;
        Ok(snapshot)
    }

    async fn flush_storage(&self) -> Result<(), String> {
        self.storage.flush().await.map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::SEQ_BLOCK_KEY;
    use common::SequenceAllocator;
    use std::sync::Arc;

    /// Creates a LogContext with fresh in-memory state.
    async fn test_context() -> LogContext {
        use crate::config::SegmentConfig;
        use common::storage::in_memory::InMemoryStorage;

        let storage: Arc<dyn common::Storage> = Arc::new(InMemoryStorage::new());
        let seq_key = Bytes::from_static(&SEQ_BLOCK_KEY);
        let sequence_allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();

        let log_storage = LogStorage::new(storage);
        let segment_cache = SegmentCache::open(&log_storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        let listing_cache = ListingCache::new();

        LogContext {
            sequence_allocator,
            segment_cache,
            listing_cache,
        }
    }

    fn make_write(keys: &[&str], timestamp_ms: i64) -> LogWrite {
        LogWrite {
            records: keys
                .iter()
                .map(|k| UserRecord {
                    key: Bytes::from(k.to_string()),
                    value: Bytes::from(format!("value-{}", k)),
                })
                .collect(),
            timestamp_ms,
            force_seal: false,
        }
    }

    #[tokio::test]
    async fn should_apply_single_write() {
        // given
        let ctx = test_context().await;
        let mut delta = LogDelta::init(ctx);

        // when
        delta.apply(make_write(&["key1"], 1000)).unwrap();

        // then - records accumulated (seq block + segment meta + listing + entry)
        assert!(!delta.records.is_empty());
        // Sequence should have advanced
        assert_eq!(delta.context.sequence_allocator.peek_next_sequence(), 1);
    }

    #[tokio::test]
    async fn should_apply_multiple_writes() {
        // given
        let ctx = test_context().await;
        let mut delta = LogDelta::init(ctx);

        // when
        delta.apply(make_write(&["key1", "key2"], 1000)).unwrap();
        let records_after_first = delta.records.len();
        delta.apply(make_write(&["key3"], 1000)).unwrap();

        // then - records accumulate across writes
        assert!(delta.records.len() > records_after_first);
        // Sequences are sequential: first write gets 0,1; second gets 2
        assert_eq!(delta.context.sequence_allocator.peek_next_sequence(), 3);
    }

    #[tokio::test]
    async fn should_track_new_segments() {
        // given
        let ctx = test_context().await;
        let mut delta = LogDelta::init(ctx);

        // when - first write triggers segment creation
        delta.apply(make_write(&["key1"], 1000)).unwrap();

        // then
        let (_frozen, frozen_read, _ctx) = delta.freeze();
        assert_eq!(frozen_read.new_segments.len(), 1);
        assert_eq!(frozen_read.new_segments[0].id(), 0);
    }

    #[tokio::test]
    async fn should_freeze_and_return_context() {
        // given
        let ctx = test_context().await;
        let mut delta = LogDelta::init(ctx);
        delta.apply(make_write(&["key1", "key2"], 1000)).unwrap();

        // when
        let (frozen, _, returned_ctx) = delta.freeze();

        // then - frozen has records
        assert!(!frozen.records.is_empty());
        // Context has updated state
        assert_eq!(returned_ctx.sequence_allocator.peek_next_sequence(), 2);
    }

    #[tokio::test]
    async fn should_estimate_size() {
        // given
        let ctx = test_context().await;
        let mut delta = LogDelta::init(ctx);
        delta.apply(make_write(&["k1", "k2"], 1000)).unwrap();

        // when
        let size = delta.estimate_size();

        // then - should be positive (sum of key+value bytes for all records)
        assert!(size > 0);
    }

    #[tokio::test]
    async fn should_flush_writes_to_storage() {
        // given
        use common::storage::in_memory::InMemoryStorage;

        let storage: Arc<dyn common::Storage> = Arc::new(InMemoryStorage::new());
        let log_storage = LogStorage::new(Arc::clone(&storage));
        let flusher = LogFlusher::new(log_storage.clone());

        let seq_key = Bytes::from_static(&SEQ_BLOCK_KEY);
        let sequence_allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();

        use crate::config::SegmentConfig;
        let segment_cache = SegmentCache::open(&log_storage.as_read(), SegmentConfig::default())
            .await
            .unwrap();
        let listing_cache = ListingCache::new();

        let ctx = LogContext {
            sequence_allocator,
            segment_cache,
            listing_cache,
        };
        let mut delta = LogDelta::init(ctx);
        delta.apply(make_write(&["mykey"], 1000)).unwrap();
        let (frozen, _, _ctx) = delta.freeze();

        // when
        let snapshot = flusher.flush_delta(frozen, &(1..2)).await.unwrap();

        // then - records are readable from snapshot
        let result = snapshot
            .get(Bytes::from_static(&SEQ_BLOCK_KEY))
            .await
            .unwrap();
        assert!(result.is_some());
    }
}

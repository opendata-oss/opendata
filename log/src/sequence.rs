//! Sequence number allocation for the Log crate.
//!
//! This module provides [`SequenceAllocator`] for allocating monotonically
//! increasing sequence numbers with the delta pattern for batched writes.
//!
//! # Key Format
//!
//! The log uses a 2-byte key for the SeqBlock record:
//!
//! ```text
//! | version (0x01) | record_type (0x02) |
//! ```
//!
//! # Delta Pattern
//!
//! ```ignore
//! let mut records = Vec::new();
//!
//! // Build delta - may add a block record if new block needed
//! let seq_delta = seq_allocator.build_delta(count, &mut records);
//!
//! // ... write records atomically ...
//!
//! // Apply delta to update allocator state
//! seq_allocator.apply_delta(seq_delta);
//! ```

use bytes::Bytes;
use common::{DEFAULT_BLOCK_SIZE, Record, SeqBlock};

use crate::error::Result;
use crate::serde::SEQ_BLOCK_KEY;
use crate::storage::LogStorageRead;

/// Returns the storage key for the SeqBlock record.
fn seq_block_key() -> Bytes {
    Bytes::from_static(&SEQ_BLOCK_KEY)
}

/// Delta representing a sequence allocation.
///
/// Produced by [`SequenceAllocator::build_delta`] and consumed by
/// [`SequenceAllocator::apply_delta`].
#[derive(Debug, Clone)]
pub(crate) struct SequenceDelta {
    /// The base sequence number of the allocation.
    base_sequence: u64,
    /// The number of sequences allocated.
    count: u64,
    /// New block allocation, if one was needed.
    new_block: Option<SeqBlock>,
}

impl SequenceDelta {
    /// Returns the base sequence number.
    pub(crate) fn base_sequence(&self) -> u64 {
        self.base_sequence
    }

    /// Returns the count of allocated sequences.
    #[cfg(test)]
    pub(crate) fn count(&self) -> u64 {
        self.count
    }
}

/// Sequence allocator with delta pattern for batched writes.
///
/// Separates state changes from storage writes:
///
/// 1. `build_delta()` calculates what sequences to allocate and what
///    records to write (if a new block is needed)
/// 2. The caller writes all records atomically
/// 3. `apply_delta()` updates the internal state
///
/// This allows sequence allocation to be batched with other writes.
pub(crate) struct SequenceAllocator {
    /// Current block (if any).
    current_block: Option<SeqBlock>,
    /// Next sequence number to allocate.
    next_sequence: u64,
}

impl SequenceAllocator {
    /// Opens a new sequence allocator, reading current state from storage.
    pub(crate) async fn open(storage: &LogStorageRead) -> Result<Self> {
        let current_block = storage.get_seq_block().await?;

        // After recovery, we start allocating from the next block
        // (sequences in the current block may have been lost on crash)
        let next_sequence = current_block.as_ref().map(|b| b.next_base()).unwrap_or(0);

        Ok(Self {
            current_block,
            next_sequence,
        })
    }

    /// Returns the next sequence number that would be allocated.
    #[cfg(test)]
    pub(crate) fn peek_next_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Builds a delta for allocating the given number of sequences.
    ///
    /// If a new block is needed (current block exhausted or doesn't exist),
    /// adds a SeqBlock record to `records`.
    ///
    /// Does NOT update internal state - call `apply_delta()` after the
    /// storage write succeeds.
    pub(crate) fn build_delta(&self, count: u64, records: &mut Vec<Record>) -> SequenceDelta {
        let remaining = self.remaining();
        let base_sequence = self.next_sequence;

        if remaining >= count {
            // Current block can satisfy the request
            SequenceDelta {
                base_sequence,
                count,
                new_block: None,
            }
        } else {
            // Need a new block
            let from_new_block = count - remaining;
            let new_base = self
                .current_block
                .as_ref()
                .map(|b| b.next_base())
                .unwrap_or(0);
            let block_size = from_new_block.max(DEFAULT_BLOCK_SIZE);
            let new_block = SeqBlock::new(new_base, block_size);

            // Add block record
            let key = seq_block_key();
            let value = new_block.serialize();
            records.push(Record::new(key, value));

            // Base is from remaining current block (if any) or new block
            let actual_base = if remaining > 0 {
                base_sequence
            } else {
                new_block.base_sequence
            };

            SequenceDelta {
                base_sequence: actual_base,
                count,
                new_block: Some(new_block),
            }
        }
    }

    /// Applies a delta to update internal state.
    ///
    /// Call this after the storage write succeeds.
    pub(crate) fn apply_delta(&mut self, delta: SequenceDelta) {
        self.next_sequence = delta.base_sequence + delta.count;
        if delta.new_block.is_some() {
            self.current_block = delta.new_block;
        }
    }

    /// Returns the number of sequences remaining in the current block.
    fn remaining(&self) -> u64 {
        match &self.current_block {
            None => 0,
            Some(block) => block.next_base().saturating_sub(self.next_sequence),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LogStorage;

    #[tokio::test]
    async fn should_allocate_from_new_block_on_first_allocation() {
        // given
        let storage = LogStorage::in_memory();
        let allocator = SequenceAllocator::open(&storage.as_read()).await.unwrap();
        let mut records = Vec::new();

        // when
        let delta = allocator.build_delta(10, &mut records);

        // then
        assert_eq!(delta.base_sequence(), 0);
        assert_eq!(delta.count(), 10);
        assert!(delta.new_block.is_some());
        assert_eq!(records.len(), 1); // block record added
    }

    #[tokio::test]
    async fn should_allocate_from_current_block_when_sufficient() {
        // given
        let storage = LogStorage::in_memory();
        let mut allocator = SequenceAllocator::open(&storage.as_read()).await.unwrap();
        let mut records1 = Vec::new();

        // First allocation creates a block
        let delta1 = allocator.build_delta(10, &mut records1);
        allocator.apply_delta(delta1);

        // when - second allocation within same block
        let mut records2 = Vec::new();
        let delta2 = allocator.build_delta(20, &mut records2);

        // then
        assert_eq!(delta2.base_sequence(), 10);
        assert_eq!(delta2.count(), 20);
        assert!(delta2.new_block.is_none()); // no new block needed
        assert_eq!(records2.len(), 0); // no records added
    }

    #[tokio::test]
    async fn should_allocate_new_block_when_exhausted() {
        // given
        let storage = LogStorage::in_memory();
        let mut allocator = SequenceAllocator::open(&storage.as_read()).await.unwrap();
        let mut records1 = Vec::new();

        // Allocate the entire first block
        let delta1 = allocator.build_delta(DEFAULT_BLOCK_SIZE, &mut records1);
        allocator.apply_delta(delta1);

        // when - need more sequences
        let mut records2 = Vec::new();
        let delta2 = allocator.build_delta(10, &mut records2);

        // then
        assert_eq!(delta2.base_sequence(), DEFAULT_BLOCK_SIZE);
        assert!(delta2.new_block.is_some());
        assert_eq!(records2.len(), 1);
    }

    #[tokio::test]
    async fn should_span_blocks_when_allocation_exceeds_remaining() {
        // given
        let storage = LogStorage::in_memory();
        let mut allocator = SequenceAllocator::open(&storage.as_read()).await.unwrap();
        let mut records1 = Vec::new();

        // Allocate most of first block
        let delta1 = allocator.build_delta(DEFAULT_BLOCK_SIZE - 10, &mut records1);
        allocator.apply_delta(delta1);

        // when - request more than remaining
        let mut records2 = Vec::new();
        let delta2 = allocator.build_delta(25, &mut records2);

        // then - should start at remaining position
        assert_eq!(delta2.base_sequence(), DEFAULT_BLOCK_SIZE - 10);
        assert_eq!(delta2.count(), 25);
        assert!(delta2.new_block.is_some());
    }

    #[tokio::test]
    async fn should_update_state_on_apply_delta() {
        // given
        let storage = LogStorage::in_memory();
        let mut allocator = SequenceAllocator::open(&storage.as_read()).await.unwrap();
        let mut records = Vec::new();

        let delta = allocator.build_delta(10, &mut records);

        // when
        allocator.apply_delta(delta);

        // then
        assert_eq!(allocator.peek_next_sequence(), 10);
    }

    #[tokio::test]
    async fn should_recover_from_storage() {
        // given
        let storage = LogStorage::in_memory();

        // Write a block to storage
        let block = SeqBlock::new(1000, 500);
        storage.write_seq_block(&block).await.unwrap();

        // when
        let allocator = SequenceAllocator::open(&storage.as_read()).await.unwrap();

        // then - should start from next block
        assert_eq!(allocator.peek_next_sequence(), 1500);
    }

    #[tokio::test]
    async fn should_allocate_larger_block_when_needed() {
        // given
        let storage = LogStorage::in_memory();
        let allocator = SequenceAllocator::open(&storage.as_read()).await.unwrap();
        let mut records = Vec::new();

        // when - request more than default block size
        let large_count = DEFAULT_BLOCK_SIZE * 2;
        let delta = allocator.build_delta(large_count, &mut records);

        // then
        assert!(delta.new_block.is_some());
        assert_eq!(delta.new_block.as_ref().unwrap().block_size, large_count);
    }
}

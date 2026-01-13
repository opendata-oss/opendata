//! Sequence number allocation and persistence.
//!
//! This module provides components for allocating monotonically increasing
//! sequence numbers with crash recovery. It consists of two layers:
//!
//! 1. **[`SeqBlockStore`]**: Lower-level component for persisting sequence block
//!    allocations to storage.
//! 2. **[`SequenceAllocator`]**: Higher-level component for allocating individual
//!    sequence numbers from blocks.
//!
//! # Design
//!
//! Block-based allocation reduces write amplification by pre-allocating ranges
//! of sequence numbers instead of persisting after every allocation. The storage
//! layer tracks allocations via [`SeqBlock`] records:
//!
//! - `base_sequence`: Starting sequence number of the allocated block
//! - `block_size`: Number of sequence numbers in the block
//!
//! On crash recovery, the next block starts at `base_sequence + block_size`,
//! ensuring monotonicity even if some allocated sequences were unused.
//!
//! # Usage
//!
//! Each system provides its own key format for storing the SeqBlock record:
//!
//! ```ignore
//! use bytes::Bytes;
//! use common::sequence::{SeqBlockStore, SequenceAllocator};
//!
//! // Domain-specific key (e.g., Log uses [0x01, 0x02])
//! const MY_SEQ_BLOCK_KEY: &[u8] = &[0x01, 0x02];
//!
//! let key = Bytes::from_static(MY_SEQ_BLOCK_KEY);
//! let block_store = SeqBlockStore::new(storage, key);
//! let allocator = SequenceAllocator::new(block_store);
//! allocator.initialize().await?;
//!
//! let seq = allocator.allocate_one().await?;
//! ```

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;

use crate::serde::DeserializeError;
use crate::serde::seq_block::SeqBlock;
use crate::{Record, Storage, StorageError};

/// Default block size for sequence allocation.
pub const DEFAULT_BLOCK_SIZE: u64 = 4096;

/// Error type for sequence allocation operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SequenceError {
    /// Storage operation failed
    Storage(StorageError),
    /// Deserialization failed
    Deserialize(DeserializeError),
}

impl std::error::Error for SequenceError {}

impl std::fmt::Display for SequenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SequenceError::Storage(e) => write!(f, "storage error: {}", e),
            SequenceError::Deserialize(e) => write!(f, "deserialize error: {}", e),
        }
    }
}

impl From<StorageError> for SequenceError {
    fn from(err: StorageError) -> Self {
        SequenceError::Storage(err)
    }
}

impl From<DeserializeError> for SequenceError {
    fn from(err: DeserializeError) -> Self {
        SequenceError::Deserialize(err)
    }
}

/// Result type alias for sequence allocation operations.
pub type SequenceResult<T> = std::result::Result<T, SequenceError>;

/// Persists and retrieves sequence block allocations.
///
/// This is a generic store that works with any storage backend and any key format.
/// The key is provided at construction time and must be a valid serialized key
/// for the domain using this store.
///
/// # Usage
///
/// Call [`initialize`](Self::initialize) once at startup to load the last
/// allocated block from storage. Then call [`allocate`](Self::allocate) to
/// get new blocks as needed.
///
/// # Thread Safety
///
/// This struct uses a tokio mutex internally to ensure safe concurrent access.
/// The mutex is held across storage writes to prevent racing allocations from
/// producing overlapping sequence blocks.
pub struct SeqBlockStore {
    storage: Arc<dyn Storage>,
    key: Bytes,
    last_block: Mutex<Option<SeqBlock>>,
}

impl SeqBlockStore {
    /// Creates a new sequence block store.
    ///
    /// # Arguments
    /// * `storage` - The storage backend to persist blocks to
    /// * `key` - The serialized key to use for storing the block record
    pub fn new(storage: Arc<dyn Storage>, key: Bytes) -> Self {
        Self {
            storage,
            key,
            last_block: Mutex::new(None),
        }
    }

    /// Initializes the store by reading the last allocated block from storage.
    ///
    /// This must be called once at startup before calling [`allocate`](Self::allocate).
    pub async fn initialize(&self) -> SequenceResult<()> {
        let block = match self.storage.get(self.key.clone()).await? {
            Some(record) => Some(SeqBlock::deserialize(&record.value)?),
            None => None,
        };
        *self.last_block.lock().await = block;
        Ok(())
    }

    /// Returns the last allocated block, if any.
    pub async fn last_block(&self) -> Option<SeqBlock> {
        self.last_block.lock().await.clone()
    }

    /// Allocates a new sequence block.
    ///
    /// The new block starts immediately after the previous block (or at 0 for
    /// the first allocation). The block size is the maximum of `min_count` and
    /// [`DEFAULT_BLOCK_SIZE`]. The block is persisted to storage before returning.
    pub async fn allocate(&self, min_count: u64) -> SequenceResult<SeqBlock> {
        let mut last_block = self.last_block.lock().await;

        let base_sequence = match &*last_block {
            Some(block) => block.next_base(),
            None => 0,
        };

        let block_size = min_count.max(DEFAULT_BLOCK_SIZE);
        let new_block = SeqBlock::new(base_sequence, block_size);

        let value: Bytes = new_block.serialize();
        self.storage
            .put(vec![Record::new(self.key.clone(), value)])
            .await?;

        *last_block = Some(new_block.clone());
        Ok(new_block)
    }
}

/// Allocates sequence numbers from pre-allocated blocks.
///
/// This allocator manages the lifecycle of sequence number allocation:
/// - Initialize from storage on startup
/// - Allocate individual sequence numbers
/// - Persist new blocks when the current block is exhausted
///
/// # Thread Safety
///
/// This struct is thread-safe and can be shared across tasks.
pub struct SequenceAllocator {
    block_store: SeqBlockStore,
    block: Mutex<AllocatedSeqBlock>,
}

struct AllocatedSeqBlock {
    current_block: Option<SeqBlock>,
    next_sequence: u64,
}

impl AllocatedSeqBlock {
    fn remaining(&self) -> u64 {
        match &self.current_block {
            None => 0,
            Some(block) => block.next_base().saturating_sub(self.next_sequence),
        }
    }
}

impl SequenceAllocator {
    /// Creates a new allocator using the given block store.
    ///
    /// # Initialization
    ///
    /// The allocator must be initialized before use by calling [`initialize`].
    /// This reads the current block allocation from storage and prepares
    /// the allocator for sequence allocation.
    pub fn new(block_store: SeqBlockStore) -> Self {
        Self {
            block_store,
            block: Mutex::new(AllocatedSeqBlock {
                current_block: None,
                next_sequence: 0,
            }),
        }
    }

    /// Initializes the allocator from storage.
    ///
    /// This should be called once during startup. It initializes the block
    /// store, which reads the last allocated block from storage.
    ///
    /// After initialization, [`allocate`] can be called.
    pub async fn initialize(&self) -> SequenceResult<()> {
        self.block_store.initialize().await
    }

    /// Returns the next sequence number that would be allocated.
    ///
    /// This does not consume any sequences; it just peeks at the current state.
    pub async fn peek_next_sequence(&self) -> u64 {
        let block = self.block.lock().await;
        block.next_sequence
    }

    /// Allocates a single sequence number.
    ///
    /// Convenience method equivalent to `allocate(1)`.
    pub async fn allocate_one(&self) -> SequenceResult<u64> {
        self.allocate(1).await
    }

    /// Allocates a contiguous range of sequence numbers.
    ///
    /// Returns the first sequence number in the allocated range. The caller
    /// can use sequences `base..base+count`.
    ///
    /// If the current block doesn't have enough sequences remaining, the
    /// remaining sequences are used and a new block is allocated for the rest.
    /// Since blocks are contiguous (each starts where the previous ends), the
    /// returned sequence range is always contiguous.
    ///
    /// # Errors
    ///
    /// Returns an error if block allocation fails (e.g., storage error).
    pub async fn allocate(&self, count: u64) -> SequenceResult<u64> {
        let mut block = self.block.lock().await;

        let remaining = block.remaining();

        // If current block can satisfy the request, use it
        if remaining >= count {
            let base_sequence = block.next_sequence;
            block.next_sequence += count;
            return Ok(base_sequence);
        }

        // Need a new block. Use remaining sequences from current block first.
        let from_new_block = count - remaining;
        let new_block = self.block_store.allocate(from_new_block).await?;

        // Base sequence is either from current block (if any remaining) or new block
        let base_sequence = if remaining > 0 {
            block.next_sequence
        } else {
            new_block.base_sequence
        };

        block.next_sequence = new_block.base_sequence + from_new_block;
        block.current_block = Some(new_block);

        Ok(base_sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::in_memory::InMemoryStorage;

    fn test_key() -> Bytes {
        Bytes::from_static(&[0x01, 0x02])
    }

    #[tokio::test]
    async fn should_return_none_when_no_block_allocated() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SeqBlockStore::new(storage, test_key());
        store.initialize().await.unwrap();

        // when
        let block = store.allocate(1).await.unwrap();

        // then
        assert_eq!(block.base_sequence, 0);
        assert_eq!(block.block_size, DEFAULT_BLOCK_SIZE);
    }

    #[tokio::test]
    async fn should_allocate_larger_block_when_requested() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SeqBlockStore::new(storage, test_key());
        store.initialize().await.unwrap();

        // when
        let large_count = DEFAULT_BLOCK_SIZE * 2;
        let block = store.allocate(large_count).await.unwrap();

        // then
        assert_eq!(block.base_sequence, 0);
        assert_eq!(block.block_size, large_count);
    }

    #[tokio::test]
    async fn should_allocate_sequential_blocks() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SeqBlockStore::new(storage, test_key());
        store.initialize().await.unwrap();

        // when
        let block1 = store.allocate(1).await.unwrap();
        let block2 = store.allocate(1).await.unwrap();
        let block3 = store.allocate(1).await.unwrap();

        // then
        assert_eq!(block1.base_sequence, 0);
        assert_eq!(block2.base_sequence, DEFAULT_BLOCK_SIZE);
        assert_eq!(block3.base_sequence, DEFAULT_BLOCK_SIZE * 2);
    }

    #[tokio::test]
    async fn should_recover_from_storage_on_initialize() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());

        // First instance allocates some blocks
        let store1 = SeqBlockStore::new(Arc::clone(&storage), test_key());
        store1.initialize().await.unwrap();
        store1.allocate(1).await.unwrap();
        store1.allocate(1).await.unwrap();

        // Second instance should recover
        let store2 = SeqBlockStore::new(Arc::clone(&storage), test_key());
        store2.initialize().await.unwrap();

        // when
        let block = store2.allocate(1).await.unwrap();

        // then - should start after the previous blocks
        assert_eq!(block.base_sequence, DEFAULT_BLOCK_SIZE * 2);
    }

    #[tokio::test]
    async fn should_allocate_sequential_sequence_numbers() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let block_store = SeqBlockStore::new(storage, test_key());
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await.unwrap();

        // when
        let seq1 = allocator.allocate_one().await.unwrap();
        let seq2 = allocator.allocate_one().await.unwrap();
        let seq3 = allocator.allocate_one().await.unwrap();

        // then
        assert_eq!(seq1, 0);
        assert_eq!(seq2, 1);
        assert_eq!(seq3, 2);
    }

    #[tokio::test]
    async fn should_allocate_batch_of_sequences() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let block_store = SeqBlockStore::new(storage, test_key());
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await.unwrap();

        // when
        let seq1 = allocator.allocate(10).await.unwrap();
        let seq2 = allocator.allocate(5).await.unwrap();

        // then
        assert_eq!(seq1, 0);
        assert_eq!(seq2, 10);
    }

    #[tokio::test]
    async fn should_span_blocks_when_batch_exceeds_remaining() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let block_store = SeqBlockStore::new(storage, test_key());
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await.unwrap();

        // Allocate most of the first block
        allocator.allocate(DEFAULT_BLOCK_SIZE - 10).await.unwrap();

        // when - allocate more than remaining (10 left, request 25)
        let seq = allocator.allocate(25).await.unwrap();

        // then - should get contiguous sequences starting at remaining position
        assert_eq!(seq, DEFAULT_BLOCK_SIZE - 10);
    }

    #[tokio::test]
    async fn should_allocate_new_block_when_exhausted() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let block_store = SeqBlockStore::new(storage, test_key());
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await.unwrap();

        // when - allocate entire first block plus one more
        allocator.allocate(DEFAULT_BLOCK_SIZE).await.unwrap();
        let seq = allocator.allocate_one().await.unwrap();

        // then - should be first sequence of second block
        assert_eq!(seq, DEFAULT_BLOCK_SIZE);
    }

    #[tokio::test]
    async fn should_recover_sequence_allocation_across_instances() {
        // given
        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());

        // First allocator uses some sequences
        let block_store1 = SeqBlockStore::new(Arc::clone(&storage), test_key());
        let allocator1 = SequenceAllocator::new(block_store1);
        allocator1.initialize().await.unwrap();
        allocator1.allocate(10).await.unwrap();

        // Second allocator should start from next block (sequences 0-9 may be lost, but monotonicity preserved)
        let block_store2 = SeqBlockStore::new(Arc::clone(&storage), test_key());
        let allocator2 = SequenceAllocator::new(block_store2);
        allocator2.initialize().await.unwrap();

        // when
        let seq = allocator2.allocate_one().await.unwrap();

        // then - should start at next block's base
        assert_eq!(seq, DEFAULT_BLOCK_SIZE);
    }

    #[tokio::test]
    async fn should_allocate_larger_than_default_block_size() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let block_store = SeqBlockStore::new(storage, test_key());
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await.unwrap();

        // when - request more than DEFAULT_BLOCK_SIZE
        let large_count = DEFAULT_BLOCK_SIZE * 2;
        let seq = allocator.allocate(large_count).await.unwrap();

        // then
        assert_eq!(seq, 0);

        // and subsequent allocation should continue from there
        let next_seq = allocator.allocate_one().await.unwrap();
        assert_eq!(next_seq, large_count);
    }

    #[tokio::test]
    async fn should_allocate_exactly_remaining() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let block_store = SeqBlockStore::new(storage, test_key());
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await.unwrap();

        // Use some sequences
        allocator.allocate(100).await.unwrap();
        let remaining = DEFAULT_BLOCK_SIZE - 100;

        // when - allocate exactly the remaining amount
        let seq = allocator.allocate(remaining).await.unwrap();

        // then - should use up exactly the current block
        assert_eq!(seq, 100);

        // and next allocation should come from new block
        let next_seq = allocator.allocate_one().await.unwrap();
        assert_eq!(next_seq, DEFAULT_BLOCK_SIZE);
    }

    #[tokio::test]
    async fn should_handle_large_batch_spanning_from_partial_block() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let block_store = SeqBlockStore::new(storage, test_key());
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await.unwrap();

        // Use most of first block, leaving 100
        allocator.allocate(DEFAULT_BLOCK_SIZE - 100).await.unwrap();

        // when - request much more than remaining (spans into new block)
        let large_request = DEFAULT_BLOCK_SIZE + 500;
        let seq = allocator.allocate(large_request).await.unwrap();

        // then - should start at the remaining position
        assert_eq!(seq, DEFAULT_BLOCK_SIZE - 100);

        // and next allocation continues after the large batch
        let next_seq = allocator.allocate_one().await.unwrap();
        assert_eq!(next_seq, DEFAULT_BLOCK_SIZE - 100 + large_request);
    }

    #[tokio::test]
    async fn should_return_last_block() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let store = SeqBlockStore::new(storage, test_key());
        store.initialize().await.unwrap();

        // when - no allocation yet
        let before = store.last_block().await;

        // then
        assert!(before.is_none());

        // when - after allocation
        let allocated = store.allocate(1).await.unwrap();
        let after = store.last_block().await;

        // then
        assert_eq!(after, Some(allocated));
    }

    #[tokio::test]
    async fn should_peek_next_sequence_without_consuming() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let block_store = SeqBlockStore::new(storage, test_key());
        let allocator = SequenceAllocator::new(block_store);
        allocator.initialize().await.unwrap();

        // Allocate some sequences
        allocator.allocate(10).await.unwrap();

        // when
        let peeked = allocator.peek_next_sequence().await;
        let allocated = allocator.allocate_one().await.unwrap();

        // then - peeked should match what was allocated
        assert_eq!(peeked, allocated);
        assert_eq!(peeked, 10);
    }
}

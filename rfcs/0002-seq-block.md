# RFC 0002: Block-Based Sequence Allocation

**Status**: Draft

**Authors**:
- [Almog Gavra](https://github.com/agavra)
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC defines a generic mechanism for allocating monotonically increasing IDs (sequence
numbers) with crash recovery support. The design reduces write amplification by pre-allocating
blocks of sequence numbers instead of persisting the counter after every allocation. On crash
recovery, the next block starts after the previously allocated range, ensuring monotonicity even
if some allocated sequences were unused.

This is a shared primitive used by multiple OpenData systems (Log, Vector) for assigning unique,
ordered identifiers to records.

## Motivation

Multiple OpenData systems need monotonically increasing sequence numbers:

- **Log**: Each log entry is assigned a sequence number for ordering and offset-based reads
- **Vector**: Internal vector IDs are assigned for efficient bitmap operations in posting lists

Naive approaches have significant drawbacks:

1. **Per-ID persistence**: Writing the sequence counter to storage after every allocation causes
   severe write amplification. For high-throughput workloads, this becomes a bottleneck.

2. **In-memory only**: Keeping the counter only in memory loses state on crash, breaking
   monotonicity guarantees that downstream components depend on.

Block-based allocation provides a middle ground: batch persistence with crash recovery.

## Goals

- Define the `SeqBlock` value format for storing allocation state
- Define the block allocation algorithm with crash recovery semantics
- Provide a generic allocator implementation usable by any OpenData system
- Support configurable block sizes to balance write amplification vs. sequence space efficiency

## Non-Goals

- Domain-specific key formats (each system defines its own storage key)
- Distributed allocation across multiple nodes (left for future work)
- Gap-free sequence assignment (gaps from crash recovery are acceptable)

## Design

### SeqBlock Value Format

The `SeqBlock` record stores the current allocation state as two big-endian u64 values:

```text
| base_sequence (u64 BE) | block_size (u64 BE) |
```

- `base_sequence`: Starting sequence number of the allocated block
- `block_size`: Number of sequence numbers in the block

The allocated range is `[base_sequence, base_sequence + block_size)`.

Total size: 16 bytes.

### Storage Key Format

Each system provides its own key format. The `SeqBlock` value type is shared, but keys are
domain-specific. For example:

| System | Key Format | Key Bytes |
|--------|------------|-----------|
| Log    | `[version, record_type]` | `[0x01, 0x02]` |
| Vector | `[version, record_type]` | `[0x01, 0x08]` |

This allows each system to integrate with its existing key versioning scheme.

### Allocation Algorithm

The allocator operates in two layers:

**Layer 1: SeqBlockStore (Block Persistence)**

```rust
pub struct SeqBlockStore {
    storage: Arc<dyn Storage>,
    key: Bytes,
    last_block: Mutex<Option<SeqBlock>>,
}
```

- `initialize()`: Reads the last allocated block from storage on startup
- `allocate(min_count)`: Allocates a new block with size `max(min_count, DEFAULT_BLOCK_SIZE)`

**Layer 2: SequenceAllocator (Sequence Distribution)**

```rust
pub struct SequenceAllocator {
    block_store: SeqBlockStore,
    block: Mutex<AllocatedSeqBlock>,
}
```

- `allocate(count)`: Returns contiguous sequence numbers from the current block
- `allocate_one()`: Convenience method for single-sequence allocation
- `peek_next_sequence()`: Returns the next sequence without consuming it

**Allocation Flow:**

1. On startup, `initialize()` reads the last `SeqBlock` from storage (if present)
2. On first allocation, if no block exists, allocate block `[0, DEFAULT_BLOCK_SIZE)`
3. Subsequent allocations return the next sequence from the current block
4. When the current block is exhausted, allocate a new block starting at `previous.next_base()`
5. Each new block is persisted to storage before returning

**Default Block Size**: 4096 sequences. This balances:
- Write amplification (larger = fewer storage writes)
- Sequence space efficiency (smaller = fewer wasted sequences on crash)

### Crash Recovery

On crash recovery:

1. Read the `SeqBlock` record to find the last allocated range `[base, base + size)`
2. Allocate a new block starting at `base + size`
3. Any sequences allocated but not used before crash are skipped (gaps in sequence space)

This ensures monotonicity: new sequences are always greater than any previously allocated
sequence, even if the previous allocator crashed mid-block.

### Usage Example

```rust
use bytes::Bytes;
use common::sequence::{SeqBlockStore, SequenceAllocator};
use common::Storage;
use std::sync::Arc;

// Domain-specific key
const MY_SEQ_BLOCK_KEY: &[u8] = &[0x01, 0x42];

async fn example(storage: Arc<dyn Storage>) {
    let key = Bytes::from_static(MY_SEQ_BLOCK_KEY);
    let block_store = SeqBlockStore::new(storage, key);
    let allocator = SequenceAllocator::new(block_store);

    allocator.initialize().await.unwrap();

    // Allocate single sequence
    let seq = allocator.allocate_one().await.unwrap();

    // Allocate batch of sequences
    let base = allocator.allocate(100).await.unwrap();
    // Use sequences base..base+100
}
```

### Thread Safety

Both `SeqBlockStore` and `SequenceAllocator` are thread-safe. They use tokio mutexes internally
to ensure:

- No concurrent allocations produce overlapping sequence ranges
- Block persistence is atomic (mutex held across storage write)

### API Surface

```rust
// In common crate

/// Block of allocated sequence numbers
pub struct SeqBlock {
    pub base_sequence: u64,
    pub block_size: u64,
}

/// Low-level block persistence
pub struct SeqBlockStore { /* ... */ }

impl SeqBlockStore {
    pub fn new(storage: Arc<dyn Storage>, key: Bytes) -> Self;
    pub async fn initialize(&self) -> SequenceResult<()>;
    pub async fn allocate(&self, min_count: u64) -> SequenceResult<SeqBlock>;
    pub async fn last_block(&self) -> Option<SeqBlock>;
}

/// High-level sequence allocation
pub struct SequenceAllocator { /* ... */ }

impl SequenceAllocator {
    pub fn new(block_store: SeqBlockStore) -> Self;
    pub async fn initialize(&self) -> SequenceResult<()>;
    pub async fn allocate(&self, count: u64) -> SequenceResult<u64>;
    pub async fn allocate_one(&self) -> SequenceResult<u64>;
    pub async fn peek_next_sequence(&self) -> u64;
}

/// Default block size
pub const DEFAULT_BLOCK_SIZE: u64 = 4096;
```

## Alternatives

### Per-ID Persistence

Writing the sequence counter after every allocation ensures no gaps but causes severe write
amplification. For 1M allocations, this would be 1M storage writes instead of ~244 block writes
(with default 4096 block size).

**Rejected**: Write amplification is unacceptable for high-throughput workloads.

## Updates

| Date       | Description |
|------------|-------------|
| 2025-01-13 | Initial draft |

//! SeqBlock value encoding/decoding.
//!
//! Stores sequence allocation state for internal vector ID generation.
//!
//! ## Block-Based ID Allocation
//!
//! Internal vector IDs are allocated from a monotonically increasing counter.
//! Rather than persisting the counter after every insert (expensive), the writer
//! pre-allocates a **block** of IDs and records the allocation in a `SeqBlock` record.
//!
//! ## Allocation Procedure
//!
//! 1. **On initialization**: Read `SeqBlock` to get last allocated range `[base, base+size)`
//! 2. **Allocate new block**: Write new `SeqBlock` with `base = old_base + old_size`
//! 3. **During operation**: Assign IDs from current block, incrementing after each insert
//! 4. **Block exhausted**: Allocate new block and write updated `SeqBlock`
//!
//! ## Crash Recovery
//!
//! On crash recovery:
//! 1. Read the `SeqBlock` record
//! 2. Allocate a fresh block starting after the previous range
//! 3. Any IDs allocated but not used before crash are skipped (creates gaps)
//!
//! Monotonicity is preserved—IDs never decrease. Gaps in the ID space are acceptable
//! since RoaringTreemap handles sparse sets efficiently.
//!
//! ## Block Sizing
//!
//! Block size balances:
//! - **Larger blocks**: Fewer `SeqBlock` writes (less write amplification)
//! - **Smaller blocks**: Fewer wasted IDs on crash (tighter ID space)
//!
//! The block size is an implementation detail, not exposed via configuration.

// Re-export SeqBlock from common crate
pub use common::serde::seq_block::SeqBlock;

// For backwards compatibility during transition
#[deprecated(note = "Use SeqBlock instead")]
pub type SeqBlockValue = SeqBlock;

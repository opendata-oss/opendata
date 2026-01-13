//! Sequence number allocation for the Log crate.
//!
//! This module provides the log-specific configuration for sequence allocation.
//! It uses the generic [`SeqBlockStore`] and [`SequenceAllocator`] from the
//! `common` crate with log-specific key encoding.
//!
//! # Key Format
//!
//! The log uses a 2-byte key for the SeqBlock record:
//!
//! ```text
//! | version (0x01) | record_type (0x02) |
//! ```
//!
//! This matches the log's key versioning scheme where `0x02` is the SeqBlock
//! record type discriminator.

use std::sync::Arc;

use bytes::Bytes;
use common::Storage;
use common::sequence::{SeqBlockStore, SequenceAllocator};

use crate::serde::{KEY_VERSION, RecordType};

/// Creates a sequence allocator configured for the log's key format.
///
/// The allocator uses the log's SeqBlock key format:
/// `[KEY_VERSION (0x01), RecordType::SeqBlock (0x02)]`
pub fn create_sequence_allocator(storage: Arc<dyn Storage>) -> SequenceAllocator {
    let key = Bytes::from(vec![KEY_VERSION, RecordType::SeqBlock.id()]);
    let block_store = SeqBlockStore::new(storage, key);
    SequenceAllocator::new(block_store)
}

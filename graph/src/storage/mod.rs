mod catalog;
pub(crate) mod merge_operator;
mod reader;
mod writer;

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use common::storage::Storage;
use common::SequenceAllocator;
use tokio::runtime::Handle;
use tracing::info;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::serde::keys::{MetadataKey, SequenceKey};
use crate::serde::{MetadataSubType, SequenceKind};

use catalog::Catalog;
pub(crate) use merge_operator::GraphMergeOperator;

/// Graph storage adapter that implements Grafeo's `GraphStore` + `GraphStoreMut`
/// traits over OpenData's `Storage` (SlateDB) abstraction.
///
/// All trait methods are synchronous (per Grafeo's trait design), but SlateDB
/// operations are async. The bridge uses `tokio::task::block_in_place` + `block_on`
/// to execute one async operation per trait method call.
pub struct SlateGraphStore {
    storage: Arc<dyn Storage>,
    rt: Handle,
    catalog: parking_lot::RwLock<Catalog>,
    node_seq: Mutex<SequenceAllocator>,
    edge_seq: Mutex<SequenceAllocator>,
    current_epoch: AtomicU64,
    node_count: AtomicI64,
    edge_count: AtomicI64,
    backward_edges: bool,
}

impl SlateGraphStore {
    /// Opens or initializes a graph store over the given storage backend.
    ///
    /// Loads catalog, metadata counters, and sequence allocators from existing
    /// data, or initializes them for a fresh database.
    pub async fn new(storage: Arc<dyn Storage>, config: &Config) -> Result<Self> {
        let rt = Handle::current();

        // Load sequence allocators
        let node_seq_key = SequenceKey::encode(SequenceKind::NodeId);
        let node_seq = SequenceAllocator::load(storage.as_ref(), node_seq_key).await?;

        let edge_seq_key = SequenceKey::encode(SequenceKind::EdgeId);
        let edge_seq = SequenceAllocator::load(storage.as_ref(), edge_seq_key).await?;

        // Load metadata counters
        let node_count = load_i64_metadata(storage.as_ref(), MetadataSubType::NodeCount).await?;
        let edge_count = load_i64_metadata(storage.as_ref(), MetadataSubType::EdgeCount).await?;
        let epoch = load_u64_metadata(storage.as_ref(), MetadataSubType::CurrentEpoch).await?;

        // Load catalog from storage
        let catalog = Catalog::load(storage.as_ref()).await?;

        info!(
            node_count,
            edge_count,
            epoch,
            labels = catalog.label_count(),
            edge_types = catalog.edge_type_count(),
            "graph store loaded"
        );

        Ok(Self {
            storage,
            rt,
            catalog: parking_lot::RwLock::new(catalog),
            node_seq: Mutex::new(node_seq),
            edge_seq: Mutex::new(edge_seq),
            current_epoch: AtomicU64::new(epoch),
            node_count: AtomicI64::new(node_count),
            edge_count: AtomicI64::new(edge_count),
            backward_edges: config.backward_edges,
        })
    }

    /// Executes an async storage operation synchronously using block_in_place.
    fn exec<F, T>(&self, f: F) -> Result<T>
    where
        F: std::future::Future<Output = common::storage::StorageResult<T>>,
    {
        tokio::task::block_in_place(|| self.rt.block_on(f)).map_err(Error::from)
    }

    /// Returns the underlying storage (for tests and diagnostics).
    #[cfg(test)]
    pub(crate) fn storage(&self) -> &dyn Storage {
        self.storage.as_ref()
    }
}

/// Loads an i64 metadata counter from storage, defaulting to 0 if absent.
async fn load_i64_metadata(storage: &dyn Storage, sub_type: MetadataSubType) -> Result<i64> {
    let key = MetadataKey { sub_type }.encode();
    match storage.get(key).await? {
        Some(record) => {
            if record.value.len() >= 8 {
                Ok(i64::from_le_bytes(record.value[..8].try_into().unwrap()))
            } else {
                Ok(0)
            }
        }
        None => Ok(0),
    }
}

/// Loads a u64 metadata value from storage, defaulting to 0 if absent.
async fn load_u64_metadata(storage: &dyn Storage, sub_type: MetadataSubType) -> Result<u64> {
    let key = MetadataKey { sub_type }.encode();
    match storage.get(key).await? {
        Some(record) => {
            if record.value.len() >= 8 {
                Ok(u64::from_le_bytes(record.value[..8].try_into().unwrap()))
            } else {
                Ok(0)
            }
        }
        None => Ok(0),
    }
}

/// Encodes an i64 as little-endian bytes for metadata storage.
pub(crate) fn encode_i64_le(val: i64) -> Bytes {
    Bytes::copy_from_slice(&val.to_le_bytes())
}

/// Encodes a u64 as little-endian bytes for metadata storage.
pub(crate) fn encode_u64_le(val: u64) -> Bytes {
    Bytes::copy_from_slice(&val.to_le_bytes())
}

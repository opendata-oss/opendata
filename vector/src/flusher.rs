//! Flusher implementation for VectorDb.
//!
//! This module contains the `VectorDbFlusher` which applies accumulated
//! RecordOps to storage atomically and updates the shared snapshot for queries.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use async_trait::async_trait;
use common::coordinator::{BroadcastDelta, Flusher};
use common::storage::{Storage, StorageSnapshot};

use crate::delta::{VectorDbImmutableDelta, VectorDbWriteDelta};

/// Flusher implementation for VectorDb.
///
/// Applies the accumulated RecordOps to storage atomically and updates
/// the shared snapshot for queries.
pub struct VectorDbFlusher {
    /// Storage backend.
    pub storage: Arc<dyn Storage>,
    /// Shared snapshot for queries (updated after each flush).
    /// Uses std::sync::Mutex - callers should lock briefly and clone.
    pub snapshot: Arc<Mutex<Arc<dyn StorageSnapshot>>>,
}

#[async_trait]
impl Flusher<VectorDbWriteDelta> for VectorDbFlusher {
    async fn flush_delta(
        &self,
        frozen: VectorDbImmutableDelta,
        _epoch_range: &Range<u64>,
    ) -> Result<BroadcastDelta<VectorDbWriteDelta>, String> {
        // Apply all ops atomically
        self.storage
            .apply(frozen.ops.clone())
            .await
            .map_err(|e| e.to_string())?;

        // Get new snapshot
        let snapshot = self.storage.snapshot().await.map_err(|e| e.to_string())?;

        // Update shared snapshot (lock briefly)
        *self.snapshot.lock().unwrap() = Arc::clone(&snapshot);

        // StorageSnapshot extends StorageRead, so we can return it directly
        Ok(BroadcastDelta {
            snapshot,
            broadcast: (),
        })
    }

    async fn flush_storage(&self) -> std::result::Result<(), String> {
        self.storage.flush().await.map_err(|e| e.to_string())
    }
}

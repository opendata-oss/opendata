//! Flusher implementation for VectorDb.
//!
//! This module contains the `VectorDbFlusher` which applies accumulated
//! RecordOps to storage atomically and updates the shared snapshot for queries.

use std::ops::Range;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use common::coordinator::Flusher;
use common::storage::{Storage, StorageSnapshot};

use crate::delta::{VectorDbImmutableDelta, VectorDbWriteDelta};

/// Flusher implementation for VectorDb.
///
/// Applies the accumulated RecordOps to storage atomically and updates
/// the shared snapshot for queries.
pub struct VectorDbFlusher {
    /// Storage backend.
    pub storage: Arc<dyn Storage>,
}

#[async_trait]
impl Flusher<VectorDbWriteDelta> for VectorDbFlusher {
    async fn flush_delta(
        &self,
        frozen: VectorDbImmutableDelta,
        _epoch_range: &Range<u64>,
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        // Apply all ops atomically
        self.storage
            .apply(frozen.ops.clone())
            .await
            .map_err(|e| e.to_string())?;

        // Get new snapshot
        let snapshot = self.storage.snapshot().await.map_err(|e| e.to_string())?;

        // StorageSnapshot extends StorageRead, so we can return it directly
        Ok(snapshot)
    }

    async fn flush_storage(&self) -> std::result::Result<(), String> {
        self.storage.flush().await.map_err(|e| e.to_string())
    }
}

use crate::batched::delta::{VectorDbDeltaView, VectorDbWriteDelta};
use crate::batched::indexer::indexer::Indexer;
use async_trait::async_trait;
use common::Storage;
use common::coordinator::Flusher;
use common::storage::StorageSnapshot;
use std::ops::Range;
use std::sync::Arc;

pub(crate) struct VectorDbFlusher {
    storage: Arc<dyn Storage>,
    last_snapshot: Arc<dyn StorageSnapshot>,
    indexer: Indexer,
}

impl VectorDbFlusher {
    pub(crate) fn new(
        storage: Arc<dyn Storage>,
        initial_snapshot: Arc<dyn StorageSnapshot>,
        indexer: Indexer,
    ) -> Self {
        Self {
            storage,
            last_snapshot: initial_snapshot,
            indexer,
        }
    }
}

#[async_trait]
impl Flusher<VectorDbWriteDelta> for VectorDbFlusher {
    async fn flush_delta(
        &mut self,
        frozen: Arc<VectorDbDeltaView>,
        _epoch_range: &Range<u64>,
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        // do indexing work
        let updates = self
            .indexer
            .update_index(frozen.writes.clone(), self.last_snapshot.clone())
            .await
            .map_err(|e| e.to_string())?;
        self.storage
            .apply(updates)
            .await
            .map_err(|e| e.to_string())?;

        // Get new snapshot
        let snapshot = self.storage.snapshot().await.map_err(|e| e.to_string())?;

        self.last_snapshot = snapshot.clone();
        Ok(snapshot)
    }

    async fn flush_storage(&self) -> Result<(), String> {
        self.storage.flush().await.map_err(|e| e.to_string())
    }
}

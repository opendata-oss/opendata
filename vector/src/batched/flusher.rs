use std::ops::Range;
use std::sync::Arc;
use async_trait::async_trait;
use common::coordinator::Flusher;
use common::{Storage, StorageRead};
use common::storage::StorageSnapshot;
use crate::batched::delta::{VectorDbDeltaView, VectorDbWriteDelta};
use crate::batched::indexer::indexer::Indexer;

struct VectorDbFlusher {
    storage: Arc<dyn Storage>,
    last_snapshot: Arc<dyn StorageRead>,
    indexer: Indexer,
}

#[async_trait]
impl Flusher<VectorDbWriteDelta> for VectorDbFlusher {
    async fn flush_delta(
        &mut self,
        frozen: Arc<VectorDbDeltaView>,
        _epoch_range: &Range<u64>
    ) -> Result<Arc<dyn StorageSnapshot>, String> {
        // do indexing work
        let updates = self.indexer.update_index(
            frozen.writes.clone(),
            self.last_snapshot.clone(),
        ).await.map_err(|e| e.to_string())?;
        self.storage.apply(updates).await.map_err(|e| e.to_string())?;

        // Get new snapshot
        let snapshot = self
            .storage
            .snapshot()
            .await
            .map_err(|e| e.to_string())?;

        // StorageSnapshot extends StorageRead, so we can return it directly
        Ok(snapshot)
    }

    async fn flush_storage(&self) -> Result<(), String> {
        self.storage.flush().await.map_err(|e| e.to_string())
    }
}
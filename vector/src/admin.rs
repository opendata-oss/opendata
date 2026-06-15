use crate::db::VectorDb;
use crate::error::{Error, Result};
use crate::model::Config;
use crate::serde::vector_id::VectorId;
use crate::write::indexer::tree::Indexer;
use crate::write::indexer::tree::validator::validate as validate_tree_index;
use common::Storage;
use common::StorageBuilder;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Administrative entry point for vector index maintenance.
///
/// This API intentionally exposes only coarse-grained maintenance operations
/// and keeps the tree indexer itself internal.
pub struct VectorDbAdmin {
    config: Config,
    storage: Arc<dyn Storage>,
}

impl VectorDbAdmin {
    /// Open a vector database for administrative operations.
    pub async fn open(config: Config) -> Result<Self> {
        let builder = StorageBuilder::new(&config.storage)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create storage: {e}")))?;
        // Build storage with the shared vector wiring (per-segment routing
        // extractor, merge operator, FTS compaction filter). The extractor name
        // is persisted in the manifest and validated on open, so an admin client
        // opening a segmented database must match it.
        //
        // The writer's custom compaction *scheduler* (which forces a major FTS
        // compaction once deletes cross the threshold) is deliberately NOT
        // installed here — we pass `None`. It is driven by an in-memory delete
        // tracker that only the writer's flusher updates; with no flusher the
        // admin client has no signal to act on, so it keeps SlateDB's default
        // size-tiered scheduler. The filter still runs correctly on whatever
        // compactions size-tiered schedules.
        let storage = crate::storage::build_vector_storage(&config, builder, None).await?;
        Ok(Self { config, storage })
    }

    /// Run one index-maintenance round with no new vector writes.
    pub async fn index_once(&self) -> Result<()> {
        let snapshot = self.snapshot().await?;
        let mut indexer = self.load_indexer(snapshot.clone()).await?;
        let result = indexer.update_index(vec![], 1, snapshot, 0).await?;
        if !result.ops.is_empty() {
            self.storage
                .apply(result.ops)
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
            self.storage
                .flush()
                .await
                .map_err(|e| Error::Storage(e.to_string()))?;
        }
        Ok(())
    }

    /// Validate the persisted centroid tree against the reconstructed in-memory state.
    pub async fn validate_index(&self) -> Result<()> {
        let snapshot = self.snapshot().await?;
        let indexer = self.load_indexer(snapshot.clone()).await?;
        validate_tree_index(snapshot, indexer.state(), self.config.dimensions as usize).await
    }

    /// Render the current persisted tree state level by level.
    pub async fn print_tree(&self) -> Result<String> {
        let snapshot = self.snapshot().await?;
        let state =
            VectorDb::load_indexer_state(self.storage.clone(), snapshot, &self.config, 0).await?;

        let mut out = String::new();
        let depth = state.centroids_meta().depth as u16;
        out.push_str(&format!("root count={}\n", state.root_centroid_count()));

        let mut by_level: BTreeMap<u16, Vec<(VectorId, u64)>> = BTreeMap::new();
        for (&centroid_id, centroid) in state.centroids() {
            let count = state
                .centroid_counts()
                .get(&(centroid.level))
                .and_then(|counts| counts.get(&centroid_id))
                .copied()
                .unwrap_or(0);
            by_level
                .entry(centroid.level as u16)
                .or_default()
                .push((centroid_id, count));
        }

        for level in (0..depth).rev() {
            out.push_str(&format!("level {level}\n"));
            let mut entries = by_level.remove(&level).unwrap_or_default();
            entries.sort_by_key(|(centroid_id, _)| *centroid_id);
            for (centroid_id, count) in entries {
                out.push_str(&format!("  centroid {centroid_id} count={count}\n"));
            }
        }

        Ok(out)
    }

    async fn snapshot(&self) -> Result<Arc<dyn common::storage::StorageSnapshot>> {
        self.storage
            .snapshot()
            .await
            .map_err(|e| Error::Storage(format!("Failed to create snapshot: {e}")))
    }

    async fn load_indexer(
        &self,
        snapshot: Arc<dyn common::storage::StorageSnapshot>,
    ) -> Result<Indexer> {
        let state =
            VectorDb::load_indexer_state(self.storage.clone(), snapshot, &self.config, 0).await?;
        Ok(Indexer::new(VectorDb::indexer_opts(&self.config), state))
    }
}

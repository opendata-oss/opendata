use crate::db::VectorDb;
use crate::error::{Error, Result};
use crate::model::Config;
use crate::storage::merge_operator::VectorDbMergeOperator;
use crate::write::indexer::tree::Indexer;
use crate::write::indexer::tree::validator::validate as validate_tree_index;
use common::Storage;
use common::{StorageBuilder, StorageSemantics};
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
        let merge_op = VectorDbMergeOperator::new(config.dimensions as usize);
        let storage = StorageBuilder::new(&config.storage)
            .await
            .map_err(|e| Error::Storage(format!("Failed to create storage: {e}")))?
            .with_semantics(StorageSemantics::new().with_merge_operator(Arc::new(merge_op)))
            .build()
            .await
            .map_err(|e| Error::Storage(format!("Failed to create storage: {e}")))?;
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

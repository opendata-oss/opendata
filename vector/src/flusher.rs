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

#[cfg(test)]
mod tests {
    use super::*;
    use common::coordinator::Flusher;
    use common::storage::RecordOp;
    use common::storage::in_memory::{FailingStorage, InMemoryStorage};

    fn create_failing_storage() -> Arc<FailingStorage> {
        let inner: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        FailingStorage::wrap(inner)
    }

    fn create_non_empty_frozen() -> VectorDbImmutableDelta {
        VectorDbImmutableDelta {
            ops: vec![RecordOp::Put(
                common::Record::new(
                    bytes::Bytes::from("test-key"),
                    bytes::Bytes::from("test-value"),
                )
                .into(),
            )],
        }
    }

    #[tokio::test]
    async fn should_propagate_apply_error() {
        // given
        let storage = create_failing_storage();
        let flusher = VectorDbFlusher {
            storage: storage.clone(),
        };
        storage.fail_apply(common::StorageError::Storage("test apply error".into()));

        // when
        let result = flusher
            .flush_delta(create_non_empty_frozen(), &(1..2))
            .await;

        // then
        let err = result.err().expect("expected apply error");
        assert!(
            err.contains("test apply error"),
            "expected test apply error message, got: {err}"
        );
    }

    #[tokio::test]
    async fn should_propagate_snapshot_error_after_apply() {
        // given
        let storage = create_failing_storage();
        let flusher = VectorDbFlusher {
            storage: storage.clone(),
        };
        // Apply succeeds, but snapshot after apply fails
        storage.fail_snapshot(common::StorageError::Storage("test snapshot error".into()));

        // when
        let result = flusher
            .flush_delta(create_non_empty_frozen(), &(1..2))
            .await;

        // then
        let err = result.err().expect("expected snapshot error");
        assert!(
            err.contains("test snapshot error"),
            "expected test snapshot error message, got: {err}"
        );
    }

    #[tokio::test]
    async fn should_propagate_flush_storage_error() {
        // given
        let storage = create_failing_storage();
        let flusher = VectorDbFlusher {
            storage: storage.clone(),
        };
        storage.fail_flush(common::StorageError::Storage("test flush error".into()));

        // when
        let result = flusher.flush_storage().await;

        // then
        assert!(result.is_err());
        assert!(
            result.unwrap_err().contains("test flush error"),
            "expected test flush error message"
        );
    }
}

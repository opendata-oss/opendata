//! In-memory delta for buffering vector writes before flush.
//!
//! This module implements the delta pattern for accumulating vector writes
//! in memory before they are atomically flushed to storage. Similar to the
//! timeseries delta, but adapted for vector data with external ID tracking.
//!
//! ## Key Design
//!
//! - Delta is keyed by **external_id** (String), not internal ID
//! - ID dictionary lookups happen at flush time, not during write
//! - No ID allocation during write() - deferred to flush time
//! - Later writes to the same external_id override earlier ones
//!
//! ## WriteCoordinator Integration
//!
//! The `VectorDbWriteDelta` implements the `Delta` trait for use with the
//! WriteCoordinator. Async operations (ID lookup, allocation, centroid
//! assignment) are performed by VectorDb::write() before sending to the
//! coordinator. The delta receives `PreparedVectorWrite` instances with
//! all necessary IDs pre-computed.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Error, Result};
use async_trait::async_trait;
use common::storage::{RecordOp, Storage, StorageRead, StorageSnapshot};
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

use crate::model::AttributeValue;
use crate::serde::posting_list::PostingUpdate;
use crate::storage::VectorDbStorageExt;
use crate::write_coordinator::{Delta, Flusher};

// ============================================================================
// WriteCoordinator Integration Types
// ============================================================================

/// A fully-prepared vector write ready for the coordinator.
///
/// All async operations (ID lookup, allocation, centroid assignment)
/// are done before this struct is created. The delta can then process
/// these writes synchronously.
#[derive(Debug, Clone)]
pub struct PreparedVectorWrite {
    /// User-provided external ID.
    pub external_id: String,
    /// Newly allocated internal ID for this vector.
    pub new_internal_id: u64,
    /// Previous internal ID if this is an upsert (looked up from ID dictionary).
    pub old_internal_id: Option<u64>,
    /// Centroid this vector is assigned to (via HNSW search).
    pub centroid_id: u32,
    /// Vector embedding values.
    pub values: Vec<f32>,
    /// All attributes including the vector field.
    pub attributes: Vec<(String, AttributeValue)>,
}

/// Image containing state needed to initialize a new delta.
///
/// This is passed to `Delta::init()` when creating a fresh delta.
#[derive(Clone)]
pub struct VectorDbImage {
    /// Storage for building RecordOps (used by VectorDbStorageExt trait methods).
    pub storage: Arc<dyn Storage>,
    /// Vector dimensions for encoding.
    pub dimensions: usize,
}

/// Immutable delta containing all RecordOps ready to be flushed.
///
/// This is the result of `Delta::freeze()` and contains the finalized
/// operations to apply atomically to storage.
pub struct VectorDbImmutableDelta {
    /// All RecordOps accumulated and finalized from the delta.
    pub ops: Vec<RecordOp>,
}

/// Mutable delta that accumulates writes and builds RecordOps.
///
/// Implements the `Delta` trait for use with WriteCoordinator.
#[derive(Clone)]
pub struct VectorDbWriteDelta {
    /// Storage reference for building RecordOps.
    storage: Arc<dyn Storage>,
    /// Vector dimensions for encoding.
    dimensions: usize,
    /// Accumulated RecordOps (ID dictionary, vector data).
    ops: Vec<RecordOp>,
    /// Posting list updates grouped by centroid (merged in freeze).
    posting_updates: HashMap<u32, Vec<PostingUpdate>>,
    /// Deleted vector IDs (merged in freeze).
    deleted_vectors: RoaringTreemap,
}

impl Delta for VectorDbWriteDelta {
    type Image = VectorDbImage;
    type Write = PreparedVectorWrite;
    type ImmutableDelta = VectorDbImmutableDelta;

    fn init(image: Self::Image) -> Self {
        Self {
            storage: image.storage,
            dimensions: image.dimensions,
            ops: Vec::new(),
            posting_updates: HashMap::new(),
            deleted_vectors: RoaringTreemap::new(),
        }
    }

    fn apply(&mut self, writes: Vec<Self::Write>) -> Result<(), Arc<Error>> {
        for write in writes {
            // Build ops synchronously from prepared data

            // 1. Update IdDictionary
            if write.old_internal_id.is_some() {
                self.ops.push(
                    self.storage
                        .delete_id_dictionary(&write.external_id)
                        .map_err(Arc::new)?,
                );
            }
            self.ops.push(
                self.storage
                    .put_id_dictionary(&write.external_id, write.new_internal_id)
                    .map_err(Arc::new)?,
            );

            // 2. Handle old vector deletion (if upsert)
            if let Some(old_id) = write.old_internal_id {
                self.deleted_vectors.insert(old_id);
                self.ops.push(
                    self.storage
                        .delete_vector_data(old_id)
                        .map_err(Arc::new)?,
                );
            }

            // 3. Write new vector data
            self.ops.push(
                self.storage
                    .put_vector_data(write.new_internal_id, &write.external_id, &write.attributes)
                    .map_err(Arc::new)?,
            );

            // 4. Accumulate posting list update
            self.posting_updates
                .entry(write.centroid_id)
                .or_default()
                .push(PostingUpdate::append(write.new_internal_id, write.values));
        }
        Ok(())
    }

    fn estimate_size(&self) -> usize {
        // Rough estimate: 100 bytes per op, 50 bytes per posting update, 8 bytes per deletion
        self.ops.len() * 100
            + self
                .posting_updates
                .values()
                .map(|v| v.len())
                .sum::<usize>()
                * 50
            + self.deleted_vectors.len() as usize * 8
    }

    fn freeze(&self) -> (Self::ImmutableDelta, Self::Image) {
        let mut ops = self.ops.clone();

        // Finalize posting list merges
        for (centroid_id, updates) in &self.posting_updates {
            if let Ok(op) = self
                .storage
                .merge_posting_list(*centroid_id, updates.clone())
            {
                ops.push(op);
            }
        }

        // Finalize deleted vectors merge
        if !self.deleted_vectors.is_empty()
            && let Ok(op) = self
                .storage
                .merge_deleted_vectors(self.deleted_vectors.clone())
        {
            ops.push(op);
        }

        (
            VectorDbImmutableDelta { ops },
            VectorDbImage {
                storage: Arc::clone(&self.storage),
                dimensions: self.dimensions,
            },
        )
    }
}

/// Flusher implementation for VectorDb.
///
/// Applies the accumulated RecordOps to storage atomically and updates
/// the shared snapshot for queries.
pub struct VectorDbFlusher {
    /// Storage backend.
    pub storage: Arc<dyn Storage>,
    /// Shared snapshot for queries (updated after each flush).
    pub snapshot: Arc<RwLock<Arc<dyn StorageSnapshot>>>,
}

#[async_trait]
impl Flusher<VectorDbWriteDelta> for VectorDbFlusher {
    async fn flush(&self, delta: &VectorDbImmutableDelta) -> Result<Arc<dyn StorageRead>> {
        // Apply all ops atomically
        self.storage.apply(delta.ops.clone()).await?;

        // Get new snapshot
        let snapshot = self.storage.snapshot().await?;

        // Update shared snapshot
        *self.snapshot.write().await = Arc::clone(&snapshot);

        // StorageSnapshot extends StorageRead, so we can return it directly
        Ok(snapshot)
    }
}

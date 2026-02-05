//! In-memory delta for buffering vector writes before flush.
//!
//! This module implements the delta pattern for accumulating vector writes
//! in memory before they are atomically flushed to storage.
//!
//! ## Key Design
//!
//! - The `VectorDbImage` contains shared state: the in-memory ID dictionary
//!   (DashMap), the centroid graph for assignment, and a sync-safe ID allocator
//! - The delta handles all write logic in `apply()`: ID allocation, dictionary
//!   lookup for upsert detection, centroid assignment, and dictionary updates
//! - The write path just validates and enqueues
//!
//! ## WriteCoordinator Integration
//!
//! The `VectorDbWriteDelta` implements the `Delta` trait for use with the
//! WriteCoordinator. The delta receives `VectorWrite` instances and handles
//! ID allocation, dictionary updates, and centroid assignment.

use std::collections::HashMap;
use std::sync::Arc;

use crate::hnsw::CentroidGraph;
use crate::model::AttributeValue;
use crate::serde::posting_list::PostingUpdate;
use crate::storage::record;
use common::SequenceAllocator;
use common::coordinator::Delta;
use common::storage::RecordOp;
use dashmap::DashMap;
use roaring::RoaringTreemap;

// ============================================================================
// WriteCoordinator Integration Types
// ============================================================================

/// A vector write ready for the coordinator.
///
/// The write path validates and enqueues this struct.
/// The delta handles ID allocation, dictionary lookup, centroid assignment, and updates.
#[derive(Debug, Clone)]
pub struct VectorWrite {
    /// User-provided external ID.
    pub external_id: String,
    /// Vector embedding values.
    pub values: Vec<f32>,
    /// All attributes including the vector field.
    pub attributes: Vec<(String, AttributeValue)>,
}

/// Image containing shared state for the delta.
///
/// This is passed to `Delta::init()` when creating a fresh delta. The image
/// contains references to shared in-memory structures that persist across
/// delta lifecycles.
pub struct VectorDbDeltaContext {
    /// Vector dimensions for encoding.
    pub dimensions: usize,
    /// In-memory ID dictionary mapping external_id -> internal_id.
    /// Updated by the delta during apply().
    pub dictionary: Arc<DashMap<String, u64>>,
    /// In-memory centroid graph for assignment (immutable after initialization).
    pub centroid_graph: Arc<dyn CentroidGraph>,
    /// Synchronous ID allocator for internal ID generation.
    pub id_allocator: SequenceAllocator,
}

/// Immutable delta containing all RecordOps ready to be flushed.
///
/// This is the result of `Delta::freeze()` and contains the finalized
/// operations to apply atomically to storage.
#[derive(Clone)]
pub struct VectorDbImmutableDelta {
    /// All RecordOps accumulated and finalized from the delta.
    pub ops: Vec<RecordOp>,
}

/// Mutable delta that accumulates writes and builds RecordOps.
///
/// Implements the `Delta` trait for use with WriteCoordinator.
pub struct VectorDbWriteDelta {
    /// Reference to the shared image.
    ctx: VectorDbDeltaContext,
    /// Accumulated RecordOps (ID dictionary, vector data).
    ops: Vec<RecordOp>,
    /// Posting list updates grouped by centroid (merged in freeze).
    posting_updates: HashMap<u32, Vec<PostingUpdate>>,
    /// Deleted vector IDs (merged in freeze).
    deleted_vectors: RoaringTreemap,
}

impl VectorDbWriteDelta {
    /// Assign a vector to its nearest centroid using the HNSW graph.
    fn assign_to_centroid(&self, vector: &[f32]) -> u32 {
        self.ctx
            .centroid_graph
            .search(vector, 1)
            .first()
            .copied()
            .unwrap_or(1)
    }
}

impl Delta for VectorDbWriteDelta {
    type Context = VectorDbDeltaContext;
    type Write = Vec<VectorWrite>;
    type Frozen = VectorDbImmutableDelta;

    fn init(context: VectorDbDeltaContext) -> Self {
        Self {
            ctx: context,
            ops: Vec::new(),
            posting_updates: HashMap::new(),
            deleted_vectors: RoaringTreemap::new(),
        }
    }

    fn apply(&mut self, vector_writes: Self::Write) -> Result<(), String> {
        for write in vector_writes {
            // 1. Allocate new internal ID
            let (new_internal_id, seq_alloc_put) = self.ctx.id_allocator.allocate_one();
            if let Some(seq_alloc_put) = seq_alloc_put {
                self.ops.push(RecordOp::Put(seq_alloc_put));
            }

            // 2. Check dictionary for existing mapping (upsert detection)
            let old_internal_id = self.ctx.dictionary.get(&write.external_id).map(|r| *r);

            // 3. Assign to centroid using the graph
            let centroid_id = self.assign_to_centroid(&write.values);

            // 4. Update ID dictionary (in-memory)
            self.ctx
                .dictionary
                .insert(write.external_id.clone(), new_internal_id);

            // 5. Build storage ops for ID dictionary
            if old_internal_id.is_some() {
                self.ops
                    .push(record::delete_id_dictionary(&write.external_id));
            }
            self.ops.push(record::put_id_dictionary(
                &write.external_id,
                new_internal_id,
            ));

            // 6. Handle old vector deletion (if upsert)
            if let Some(old_id) = old_internal_id {
                self.deleted_vectors.insert(old_id);
                self.ops.push(record::delete_vector_data(old_id));
            }

            // 7. Write new vector data
            self.ops.push(record::put_vector_data(
                new_internal_id,
                &write.external_id,
                &write.attributes,
            ));

            // 8. Accumulate posting list update
            self.posting_updates
                .entry(centroid_id)
                .or_default()
                .push(PostingUpdate::append(new_internal_id, write.values));
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

    fn freeze(self) -> (Self::Frozen, Self::Context) {
        let mut ops = self.ops;

        // Finalize posting list merges
        for (centroid_id, updates) in self.posting_updates {
            if let Ok(op) = record::merge_posting_list(centroid_id, updates) {
                ops.push(op);
            }
        }

        // Finalize deleted vectors merge
        if !self.deleted_vectors.is_empty()
            && let Ok(op) = record::merge_deleted_vectors(self.deleted_vectors)
        {
            ops.push(op);
        }

        (VectorDbImmutableDelta { ops }, self.ctx)
    }
}

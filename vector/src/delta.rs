//! In-memory delta for buffering vector writes before flush.
//!
//! This module implements the delta pattern for accumulating vector writes
//! in memory before they are atomically flushed to storage.
//!
//! ## Key Design
//!
//! - The `VectorDbDeltaContext` contains shared state: the in-memory ID dictionary
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

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tracing::debug;

use crate::hnsw::CentroidGraph;
use crate::lire::commands::RebalanceCommand;
use crate::lire::rebalancer::IndexRebalanceOp;
use crate::model::AttributeValue;
use crate::serde::centroid_chunk::CentroidEntry;
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

pub(crate) enum VectorDbWrite {
    Write(Vec<VectorWrite>),
    #[allow(dead_code)]
    Rebalance(RebalanceCommand),
}

/// A vector write ready for the coordinator.
///
/// The write path validates and enqueues this struct.
/// The delta handles ID allocation, dictionary lookup, centroid assignment, and updates.
#[derive(Debug, Clone)]
pub(crate) struct VectorWrite {
    /// User-provided external ID.
    pub(crate) external_id: String,
    /// Vector embedding values.
    pub(crate) values: Vec<f32>,
    /// All attributes including the vector field.
    pub(crate) attributes: Vec<(String, AttributeValue)>,
}

/// Configuration options for the delta.
pub(crate) struct VectorDbDeltaOpts {
    /// Vector dimensions for encoding.
    #[allow(dead_code)]
    pub(crate) dimensions: usize,
    /// Number of vectors in a centroid's posting list that triggers a split.
    pub(crate) split_threshold_vectors: u64,
    /// Target number of centroid entries per chunk.
    #[allow(dead_code)]
    pub(crate) chunk_target: usize,
}

/// Image containing shared state for the delta.
///
/// This is passed to `Delta::init()` when creating a fresh delta. The image
/// contains references to shared in-memory structures that persist across
/// delta lifecycles.
pub(crate) struct VectorDbDeltaContext {
    /// Configuration options.
    pub(crate) opts: VectorDbDeltaOpts,
    /// In-memory ID dictionary mapping external_id -> internal_id.
    /// Updated by the delta during apply().
    pub(crate) dictionary: Arc<DashMap<String, u64>>,
    /// In-memory centroid graph for assignment (immutable after initialization).
    pub(crate) centroid_graph: Arc<dyn CentroidGraph>,
    /// Synchronous ID allocator for internal ID generation.
    pub(crate) id_allocator: SequenceAllocator,
    /// Channel for sending rebalance ops to index rebalancer
    pub(crate) rebalancer_tx: tokio::sync::mpsc::UnboundedSender<IndexRebalanceOp>,
    /// In-memory centroid vector counts, loaded from storage at startup and
    /// updated on each write. Used by the current delta to observe counts
    /// and schedule splits/merges.
    pub(crate) centroid_counts: HashMap<u64, u64>,
    /// The current centroid chunk being appended to.
    #[allow(dead_code)]
    pub(crate) current_chunk_id: u32,
    /// Number of centroid entries in the current chunk.
    #[allow(dead_code)]
    pub(crate) current_chunk_count: usize,
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
pub(crate) struct VectorDbWriteDelta {
    /// Reference to the shared image.
    pub(crate) ctx: VectorDbDeltaContext,
    /// Accumulated RecordOps (ID dictionary, vector data).
    pub(crate) ops: Vec<RecordOp>,
    /// Shared view of the delta's current state, readable by concurrent readers.
    pub(crate) view: Arc<std::sync::RwLock<VectorDbDeltaView>>,
    /// Centroids that already have an in-flight split op sent to the rebalancer.
    /// Prevents duplicate split requests within a single delta lifecycle.
    pub(crate) pending_splits: HashSet<u64>,
}

impl VectorDbWriteDelta {
    /// Assign a vector to its nearest centroid using the HNSW graph.
    fn assign_to_centroid(&self, vector: &[f32]) -> u64 {
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
    type Write = VectorDbWrite;
    type DeltaView = Arc<std::sync::RwLock<VectorDbDeltaView>>;
    type Frozen = VectorDbImmutableDelta;
    type FrozenView = Arc<VectorDbDeltaView>;
    type ApplyResult = Arc<dyn Any + Send + Sync + 'static>;

    fn init(context: VectorDbDeltaContext) -> Self {
        Self {
            ctx: context,
            ops: Vec::new(),
            view: Arc::new(std::sync::RwLock::new(VectorDbDeltaView::new())),
            pending_splits: HashSet::new(),
        }
    }

    fn apply(
        &mut self,
        write: Self::Write,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        match write {
            VectorDbWrite::Write(writes) => self.apply_write(writes),
            VectorDbWrite::Rebalance(cmd) => self.apply_rebalance_cmd(cmd),
        }
    }

    fn estimate_size(&self) -> usize {
        let view = self.view.read().expect("lock poisoned");
        // Rough estimate: 100 bytes per op, 50 bytes per posting update, 8 bytes per deletion
        self.ops.len() * 100
            + view
                .posting_updates
                .values()
                .map(|v| v.len())
                .sum::<usize>()
                * 50
            + view.deleted_centroids.len() as usize * 8
    }

    fn freeze(self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        let mut ops = self.ops;
        let view = self.view.read().expect("lock poisoned").clone();

        // Finalize posting list merges and centroid stats deltas
        for (centroid_id, updates) in &view.posting_updates {
            let count = updates.len() as i32;
            if let Ok(op) = record::merge_posting_list(*centroid_id, updates.clone()) {
                ops.push(op);
            }
            ops.push(record::merge_centroid_stats(*centroid_id, count));
        }

        // Finalize deleted vectors merge
        if !view.deleted_centroids.is_empty() {
            let op = record::merge_deleted_vectors(view.deleted_centroids.clone())
                .expect("failure to construct deleted vectors row");
            ops.push(op);
        }

        (VectorDbImmutableDelta { ops }, Arc::new(view), self.ctx)
    }

    fn reader(&self) -> Self::DeltaView {
        self.view.clone()
    }
}

impl VectorDbWriteDelta {
    fn apply_write(
        &mut self,
        vector_writes: Vec<VectorWrite>,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        let mut view = self.view.write().expect("lock poisoned");
        let mut touched_centroids = HashSet::new();

        for write in vector_writes {
            // Allocate new internal ID
            let (new_internal_id, seq_alloc_put) = self.ctx.id_allocator.allocate_one();
            if let Some(seq_alloc_put) = seq_alloc_put {
                self.ops.push(RecordOp::Put(seq_alloc_put.into()));
            }

            // Check dictionary for existing mapping (upsert detection)
            let old_internal_id = self.ctx.dictionary.get(&write.external_id).map(|r| *r);

            // Assign to centroid using the graph
            let centroid_id = self.assign_to_centroid(&write.values);

            // Update ID dictionary (in-memory)
            self.ctx
                .dictionary
                .insert(write.external_id.clone(), new_internal_id);

            // Build storage ops for ID dictionary
            self.ops.push(record::put_id_dictionary(
                &write.external_id,
                new_internal_id,
            ));

            // Handle old vector deletion (if upsert)
            if let Some(old_id) = old_internal_id {
                self.ops.push(record::delete_vector_data(old_id));
            }

            // Write new vector data
            self.ops.push(record::put_vector_data(
                new_internal_id,
                &write.external_id,
                &write.attributes,
            ));

            // Accumulate posting list update
            view.add_to_posting(centroid_id, new_internal_id, write.values);
            *self.ctx.centroid_counts.entry(centroid_id).or_default() += 1;

            touched_centroids.insert(centroid_id);
        }

        drop(view);

        // Check if any touched centroid exceeded the split threshold
        self.maybe_schedule_splits(touched_centroids);

        Ok(Arc::new(()))
    }

    fn maybe_schedule_splits(&mut self, touched_centroids: HashSet<u64>) {
        for centroid_id in touched_centroids {
            let count = self
                .ctx
                .centroid_counts
                .get(&centroid_id)
                .copied()
                .unwrap_or(0);
            if count >= self.ctx.opts.split_threshold_vectors
                && !self.pending_splits.contains(&centroid_id)
            {
                debug!(
                    centroid_id,
                    count,
                    threshold = self.ctx.opts.split_threshold_vectors,
                    "scheduling split for centroid"
                );
                self.pending_splits.insert(centroid_id);
                let centroid_vec = self
                    .ctx
                    .centroid_graph
                    .get_centroid_vector(centroid_id)
                    .expect("unexpected missing centroid");
                let _ = self.ctx.rebalancer_tx.send(IndexRebalanceOp::ExecuteSplit {
                    centroid: CentroidEntry::new(centroid_id, centroid_vec),
                });
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct VectorDbDeltaView {
    pub(crate) posting_updates: HashMap<u64, Vec<PostingUpdate>>,
    pub(crate) deleted_centroids: RoaringTreemap,
}

impl VectorDbDeltaView {
    fn new() -> Self {
        Self {
            posting_updates: HashMap::new(),
            deleted_centroids: RoaringTreemap::new(),
        }
    }

    pub(crate) fn add_to_posting(&mut self, centroid_id: u64, vector_id: u64, vector: Vec<f32>) {
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::append(vector_id, vector));
    }

    #[allow(dead_code)]
    pub(crate) fn delete_from_posting(&mut self, centroid_id: u64, vector_id: u64) {
        self.posting_updates
            .entry(centroid_id)
            .or_default()
            .push(PostingUpdate::delete(vector_id));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::CentroidGraph;
    use crate::model::AttributeValue;
    use crate::serde::centroid_chunk::CentroidEntry;
    use crate::serde::key::{CentroidStatsKey, IdDictionaryKey, PostingListKey, VectorDataKey};
    use bytes::{Buf, Bytes};
    use common::SequenceAllocator;
    use common::coordinator::Delta;
    use common::storage::RecordOp;
    use common::storage::in_memory::InMemoryStorage;

    /// Mock CentroidGraph that always returns a fixed centroid ID.
    struct MockCentroidGraph {
        centroid_id: u64,
    }

    impl MockCentroidGraph {
        fn new(centroid_id: u64) -> Self {
            Self { centroid_id }
        }
    }

    impl CentroidGraph for MockCentroidGraph {
        fn search(&self, _query: &[f32], _k: usize) -> Vec<u64> {
            vec![self.centroid_id]
        }

        fn add_centroid(&self, _entry: &CentroidEntry) -> anyhow::Result<()> {
            Ok(())
        }

        fn remove_centroid(&self, _centroid_id: u64) -> anyhow::Result<()> {
            Ok(())
        }

        fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
            if centroid_id == self.centroid_id {
                Some(vec![0.0; 3])
            } else {
                None
            }
        }

        fn len(&self) -> usize {
            1
        }
    }

    /// Create a test context with the given centroid ID for assignment.
    async fn create_test_context(centroid_id: u64) -> VectorDbDeltaContext {
        create_test_context_with_threshold(centroid_id, 10_000)
            .await
            .0
    }

    /// Create a test context with the given centroid ID and split threshold.
    /// Returns the context and the rebalancer receiver for inspecting sent ops.
    async fn create_test_context_with_threshold(
        centroid_id: u64,
        split_threshold: u64,
    ) -> (
        VectorDbDeltaContext,
        tokio::sync::mpsc::UnboundedReceiver<IndexRebalanceOp>,
    ) {
        let storage: Arc<dyn common::Storage> = Arc::new(InMemoryStorage::new());
        let key = Bytes::from_static(&[0x01, 0x02]);
        let id_allocator = SequenceAllocator::load(storage.as_ref(), key)
            .await
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let ctx = VectorDbDeltaContext {
            opts: VectorDbDeltaOpts {
                dimensions: 3,
                split_threshold_vectors: split_threshold,
                chunk_target: 4096,
            },
            dictionary: Arc::new(DashMap::new()),
            centroid_graph: Arc::new(MockCentroidGraph::new(centroid_id)),
            id_allocator,
            rebalancer_tx: tx,
            centroid_counts: HashMap::new(),
            current_chunk_id: 0,
            current_chunk_count: 0,
        };
        (ctx, rx)
    }

    /// Create a simple vector write for testing.
    fn create_vector_write(external_id: &str, values: Vec<f32>) -> VectorWrite {
        VectorWrite {
            external_id: external_id.to_string(),
            values: values.clone(),
            attributes: vec![
                ("vector".to_string(), AttributeValue::Vector(values)),
                (
                    "category".to_string(),
                    AttributeValue::String("test".to_string()),
                ),
            ],
        }
    }

    /// Helper to check if an op is a Put for a specific key prefix.
    fn is_put_with_key_prefix(op: &RecordOp, prefix: &[u8]) -> bool {
        match op {
            RecordOp::Put(record) => record.record.key.starts_with(prefix),
            _ => false,
        }
    }

    /// Helper to check if an op is a Merge for a specific key prefix.
    fn is_merge_with_key_prefix(op: &RecordOp, prefix: &[u8]) -> bool {
        match op {
            RecordOp::Merge(record) => record.record.key.starts_with(prefix),
            _ => false,
        }
    }

    #[tokio::test]
    async fn should_add_vectors() {
        // given
        let ctx = create_test_context(1).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        let write = create_vector_write("vec-1", vec![1.0, 2.0, 3.0]);

        // when
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have ops for ID dictionary put and vector data put
        let id_dict_key = IdDictionaryKey::new("vec-1").encode();
        let vector_data_key_prefix = VectorDataKey::new(0).encode();

        // Find ID dictionary put
        let has_id_dict_put = frozen.ops.iter().any(|op| match op {
            RecordOp::Put(record) => record.record.key == id_dict_key,
            _ => false,
        });
        assert!(has_id_dict_put, "should have ID dictionary put op");

        // Find vector data put (key starts with vector data prefix)
        let has_vector_data_put = frozen
            .ops
            .iter()
            .any(|op| is_put_with_key_prefix(op, &vector_data_key_prefix[..2]));
        assert!(has_vector_data_put, "should have vector data put op");
    }

    #[tokio::test]
    async fn should_assign_vectors_to_postings() {
        // given
        let centroid_id = 42u64;
        let ctx = create_test_context(centroid_id).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        let write = create_vector_write("vec-1", vec![1.0, 2.0, 3.0]);

        // when
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have a merge op for the posting list of centroid 42
        let posting_key = PostingListKey::new(centroid_id).encode();
        let has_posting_merge = frozen.ops.iter().any(|op| match op {
            RecordOp::Merge(record) => record.record.key == posting_key,
            _ => false,
        });
        assert!(
            has_posting_merge,
            "should have posting list merge op for centroid {}",
            centroid_id
        );
    }

    #[tokio::test]
    async fn should_update_dictionary_on_insert() {
        // given
        let ctx = create_test_context(1).await;
        let dictionary = Arc::clone(&ctx.dictionary);
        let mut delta = VectorDbWriteDelta::init(ctx);

        let write = create_vector_write("vec-1", vec![1.0, 2.0, 3.0]);

        // when
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();

        // then - dictionary should be updated in memory
        assert!(dictionary.contains_key("vec-1"));
        let internal_id = *dictionary.get("vec-1").unwrap();
        assert_eq!(internal_id, 0, "first allocated ID should be 0");
    }

    #[tokio::test]
    async fn should_add_vectors_on_update() {
        // given
        let ctx = create_test_context(1).await;
        let mut delta = VectorDbWriteDelta::init(ctx);
        let write = create_vector_write("vec-1", vec![1.0, 2.0, 3.0]);
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();
        let write = create_vector_write("vec-1", vec![4.0, 5.0, 6.0]);
        let first_id = *delta.ctx.dictionary.get("vec-1").unwrap();

        // when:
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();
        let (frozen, _view, ctx) = delta.freeze();

        // then - should have put for new ID dictionary entry only
        let id_dict_key = IdDictionaryKey::new("vec-1").encode();
        let id_dict_puts: Vec<_> = frozen
            .ops
            .clone()
            .into_iter()
            .filter(|op| match op {
                RecordOp::Put(record) => record.record.key == id_dict_key,
                _ => false,
            })
            .collect();
        assert!(!id_dict_puts.is_empty());
        let RecordOp::Put(record) = id_dict_puts.last().unwrap() else {
            panic!("should have ID dictionary put op");
        };
        let new_id = record.record.value.clone().get_u64_le();
        assert!(new_id > first_id);
        // Dictionary should have new internal ID
        let new_id_dict = *ctx.dictionary.get("vec-1").unwrap();
        assert_eq!(new_id_dict, new_id);
    }

    #[tokio::test]
    async fn should_assign_vectors_to_postings_on_update() {
        // given
        let centroid_id = 5u64;
        let ctx = create_test_context(centroid_id).await;

        // Pre-populate dictionary to simulate existing vector
        ctx.dictionary.insert("vec-1".to_string(), 100);

        let mut delta = VectorDbWriteDelta::init(ctx);

        let write = create_vector_write("vec-1", vec![4.0, 5.0, 6.0]);

        // when
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have posting list merge for the new vector
        let posting_key = PostingListKey::new(centroid_id).encode();
        let has_posting_merge = frozen.ops.iter().any(|op| match op {
            RecordOp::Merge(record) => record.record.key == posting_key,
            _ => false,
        });
        assert!(
            has_posting_merge,
            "should have posting list merge op on update"
        );
    }

    #[tokio::test]
    async fn should_delete_old_vector_data_on_update() {
        // given
        let ctx = create_test_context(1).await;
        let mut delta = VectorDbWriteDelta::init(ctx);
        let write = create_vector_write("vec-1", vec![4.0, 5.0, 6.0]);
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();
        let old_internal_id = *delta.ctx.dictionary.get("vec-1").unwrap();

        // when
        let write = create_vector_write("vec-1", vec![4.0, 5.0, 6.0]);
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have delete op for old vector data
        let old_vector_key = VectorDataKey::new(old_internal_id).encode();
        let has_vector_delete = frozen.ops.iter().any(|op| match op {
            RecordOp::Delete(key) => *key == old_vector_key,
            _ => false,
        });
        assert!(has_vector_delete, "should have vector data delete op");
    }

    #[tokio::test]
    async fn should_handle_multiple_vectors_in_single_apply() {
        // given
        let ctx = create_test_context(1).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        let writes = vec![
            create_vector_write("vec-1", vec![1.0, 0.0, 0.0]),
            create_vector_write("vec-2", vec![0.0, 1.0, 0.0]),
            create_vector_write("vec-3", vec![0.0, 0.0, 1.0]),
        ];

        // when
        delta.apply(VectorDbWrite::Write(writes)).unwrap();
        let (frozen, _view, ctx) = delta.freeze();

        // then - should have 3 vectors in dictionary
        assert_eq!(ctx.dictionary.len(), 3);
        assert!(ctx.dictionary.contains_key("vec-1"));
        assert!(ctx.dictionary.contains_key("vec-2"));
        assert!(ctx.dictionary.contains_key("vec-3"));

        // Should have ID dictionary puts for each
        let id_dict_puts = frozen
            .ops
            .iter()
            .filter(|op| is_put_with_key_prefix(op, &IdDictionaryKey::new("").encode()[..2]))
            .count();
        assert_eq!(id_dict_puts, 3, "should have 3 ID dictionary put ops");

        // Should have vector data puts for each
        let vector_data_puts = frozen
            .ops
            .iter()
            .filter(|op| is_put_with_key_prefix(op, &VectorDataKey::new(0).encode()[..2]))
            .count();
        assert_eq!(vector_data_puts, 3, "should have 3 vector data put ops");
    }

    #[tokio::test]
    async fn should_allocate_sequential_internal_ids() {
        // given
        let ctx = create_test_context(1).await;
        let dictionary = Arc::clone(&ctx.dictionary);
        let mut delta = VectorDbWriteDelta::init(ctx);

        let writes = vec![
            create_vector_write("vec-1", vec![1.0, 0.0, 0.0]),
            create_vector_write("vec-2", vec![0.0, 1.0, 0.0]),
            create_vector_write("vec-3", vec![0.0, 0.0, 1.0]),
        ];

        // when
        delta.apply(VectorDbWrite::Write(writes)).unwrap();

        // then - internal IDs should be sequential starting from 0
        let id1 = *dictionary.get("vec-1").unwrap();
        let id2 = *dictionary.get("vec-2").unwrap();
        let id3 = *dictionary.get("vec-3").unwrap();

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
    }

    #[tokio::test]
    async fn should_group_postings_by_centroid() {
        // given - create a mock that returns different centroids based on query
        struct MultiCentroidGraph;

        impl CentroidGraph for MultiCentroidGraph {
            fn search(&self, query: &[f32], _k: usize) -> Vec<u64> {
                // Return centroid based on which dimension has highest value
                if query[0] > query[1] && query[0] > query[2] {
                    vec![1]
                } else if query[1] > query[2] {
                    vec![2]
                } else {
                    vec![3]
                }
            }

            fn add_centroid(&self, _entry: &CentroidEntry) -> anyhow::Result<()> {
                Ok(())
            }

            fn remove_centroid(&self, _centroid_id: u64) -> anyhow::Result<()> {
                Ok(())
            }

            fn get_centroid_vector(&self, _centroid_id: u64) -> Option<Vec<f32>> {
                None
            }

            fn len(&self) -> usize {
                3
            }
        }

        let storage: Arc<dyn common::Storage> = Arc::new(InMemoryStorage::new());
        let key = Bytes::from_static(&[0x01, 0x02]);
        let id_allocator = SequenceAllocator::load(storage.as_ref(), key)
            .await
            .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let ctx = VectorDbDeltaContext {
            opts: VectorDbDeltaOpts {
                dimensions: 3,
                split_threshold_vectors: 10_000,
                chunk_target: 4096,
            },
            dictionary: Arc::new(DashMap::new()),
            centroid_graph: Arc::new(MultiCentroidGraph),
            id_allocator,
            rebalancer_tx: tx,
            centroid_counts: HashMap::new(),
            current_chunk_id: 0,
            current_chunk_count: 0,
        };

        let mut delta = VectorDbWriteDelta::init(ctx);

        let writes = vec![
            create_vector_write("vec-1", vec![1.0, 0.0, 0.0]), // -> centroid 1
            create_vector_write("vec-2", vec![0.0, 1.0, 0.0]), // -> centroid 2
            create_vector_write("vec-3", vec![0.0, 0.0, 1.0]), // -> centroid 3
            create_vector_write("vec-4", vec![0.9, 0.1, 0.0]), // -> centroid 1
        ];

        // when
        delta.apply(VectorDbWrite::Write(writes)).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have posting list merges for centroids 1, 2, and 3
        let posting_merges: Vec<_> = frozen
            .ops
            .iter()
            .filter(|op| is_merge_with_key_prefix(op, &PostingListKey::new(0).encode()[..2]))
            .collect();

        assert_eq!(
            posting_merges.len(),
            3,
            "should have 3 posting list merge ops"
        );
    }

    #[tokio::test]
    async fn should_update_centroid_counts_per_centroid() {
        // given - create a mock that routes vectors to different centroids
        struct MultiCentroidGraph;

        impl CentroidGraph for MultiCentroidGraph {
            fn search(&self, query: &[f32], _k: usize) -> Vec<u64> {
                if query[0] > query[1] && query[0] > query[2] {
                    vec![1]
                } else if query[1] > query[2] {
                    vec![2]
                } else {
                    vec![3]
                }
            }

            fn add_centroid(&self, _entry: &CentroidEntry) -> anyhow::Result<()> {
                Ok(())
            }

            fn remove_centroid(&self, _centroid_id: u64) -> anyhow::Result<()> {
                Ok(())
            }

            fn get_centroid_vector(&self, _centroid_id: u64) -> Option<Vec<f32>> {
                None
            }

            fn len(&self) -> usize {
                3
            }
        }

        let storage: Arc<dyn common::Storage> = Arc::new(InMemoryStorage::new());
        let key = Bytes::from_static(&[0x01, 0x02]);
        let id_allocator = SequenceAllocator::load(storage.as_ref(), key)
            .await
            .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();

        let ctx = VectorDbDeltaContext {
            opts: VectorDbDeltaOpts {
                dimensions: 3,
                split_threshold_vectors: 10_000,
                chunk_target: 4096,
            },
            dictionary: Arc::new(DashMap::new()),
            centroid_graph: Arc::new(MultiCentroidGraph),
            id_allocator,
            rebalancer_tx: tx,
            centroid_counts: HashMap::new(),
            current_chunk_id: 0,
            current_chunk_count: 0,
        };

        let mut delta = VectorDbWriteDelta::init(ctx);

        let writes = vec![
            create_vector_write("vec-1", vec![1.0, 0.0, 0.0]), // -> centroid 1
            create_vector_write("vec-2", vec![0.0, 1.0, 0.0]), // -> centroid 2
            create_vector_write("vec-3", vec![0.0, 0.0, 1.0]), // -> centroid 3
            create_vector_write("vec-4", vec![0.9, 0.1, 0.0]), // -> centroid 1
        ];

        // when
        delta.apply(VectorDbWrite::Write(writes)).unwrap();
        let (_frozen, _view, ctx) = delta.freeze();

        // then - only centroids that received postings should have counts
        assert_eq!(ctx.centroid_counts.get(&1), Some(&2)); // vec-1, vec-4
        assert_eq!(ctx.centroid_counts.get(&2), Some(&1)); // vec-2
        assert_eq!(ctx.centroid_counts.get(&3), Some(&1)); // vec-3
        assert_eq!(ctx.centroid_counts.len(), 3);
    }

    #[tokio::test]
    async fn should_emit_centroid_stats_on_freeze() {
        // given
        let centroid_id = 42u64;
        let ctx = create_test_context(centroid_id).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        let writes = vec![
            create_vector_write("vec-1", vec![1.0, 2.0, 3.0]),
            create_vector_write("vec-2", vec![4.0, 5.0, 6.0]),
        ];

        // when
        delta.apply(VectorDbWrite::Write(writes)).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have a centroid stats merge op with delta = 2
        let stats_key = CentroidStatsKey::new(centroid_id).encode();
        let stats_merge = frozen.ops.iter().find(|op| match op {
            RecordOp::Merge(record) => record.record.key == stats_key,
            _ => false,
        });
        assert!(
            stats_merge.is_some(),
            "should have centroid stats merge op for centroid {}",
            centroid_id
        );

        // Verify the delta value is 2
        if let Some(RecordOp::Merge(record)) = stats_merge {
            let value = crate::serde::centroid_stats::CentroidStatsValue::decode_from_bytes(
                &record.record.value,
            )
            .unwrap();
            assert_eq!(value.num_vectors, 2, "should have delta of 2 for 2 vectors");
        }
    }

    #[tokio::test]
    async fn should_estimate_size_correctly() {
        // given
        let ctx = create_test_context(1).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        // Initial size should be 0
        assert_eq!(delta.estimate_size(), 0);

        // when - add a vector
        let write = create_vector_write("vec-1", vec![1.0, 2.0, 3.0]);
        delta.apply(VectorDbWrite::Write(vec![write])).unwrap();

        // then - size should be non-zero
        let size = delta.estimate_size();
        assert!(size > 0, "size should be non-zero after adding vector");
    }

    #[tokio::test]
    async fn should_expose_posting_updates_via_reader() {
        // given
        let centroid_id = 7u64;
        let ctx = create_test_context(centroid_id).await;
        let mut delta = VectorDbWriteDelta::init(ctx);
        let reader = delta.reader();

        // when - insert a new vector and upsert an existing one
        let writes = vec![
            create_vector_write("vec-2", vec![1.0, 0.0, 0.0]),
            create_vector_write("vec-1", vec![0.0, 1.0, 0.0]),
        ];
        delta.apply(VectorDbWrite::Write(writes)).unwrap();

        // then - reader should see posting updates for both vectors
        let view = reader.read().expect("lock poisoned");
        let postings = view
            .posting_updates
            .get(&centroid_id)
            .expect("should have postings for centroid");
        assert_eq!(
            postings.len(),
            2,
            "should have posting updates for both vectors"
        );
    }

    #[tokio::test]
    async fn should_send_split_when_threshold_exceeded() {
        // given - threshold of 3, centroid 5
        let centroid_id = 5u64;
        let (ctx, mut rx) = create_test_context_with_threshold(centroid_id, 3).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        // when - write 4 vectors (exceeds threshold of 3)
        let writes = vec![
            create_vector_write("v1", vec![1.0, 0.0, 0.0]),
            create_vector_write("v2", vec![0.0, 1.0, 0.0]),
            create_vector_write("v3", vec![0.0, 0.0, 1.0]),
            create_vector_write("v4", vec![1.0, 1.0, 0.0]),
        ];
        delta.apply(VectorDbWrite::Write(writes)).unwrap();

        // then - should have received exactly one ExecuteSplit for centroid 5
        let op = rx.try_recv().expect("should have received a split op");
        match op {
            IndexRebalanceOp::ExecuteSplit { centroid } => {
                assert_eq!(centroid.centroid_id, centroid_id);
            }
            _ => panic!("expected ExecuteSplit, got {:?}", op),
        }
        assert!(
            rx.try_recv().is_err(),
            "should not have sent duplicate split ops"
        );
    }

    #[tokio::test]
    async fn should_not_send_duplicate_split_for_same_centroid() {
        // given - threshold of 2, centroid 1
        let centroid_id = 1u64;
        let (ctx, mut rx) = create_test_context_with_threshold(centroid_id, 2).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        // when - first batch exceeds threshold
        let writes1 = vec![
            create_vector_write("v1", vec![1.0, 0.0, 0.0]),
            create_vector_write("v2", vec![0.0, 1.0, 0.0]),
        ];
        delta.apply(VectorDbWrite::Write(writes1)).unwrap();

        // drain the first split op
        let _ = rx.try_recv().expect("should have first split op");

        // when - second batch also goes to the same centroid
        let writes2 = vec![
            create_vector_write("v3", vec![0.0, 0.0, 1.0]),
            create_vector_write("v4", vec![1.0, 1.0, 0.0]),
        ];
        delta.apply(VectorDbWrite::Write(writes2)).unwrap();

        // then - should not have sent a second split op (already pending)
        assert!(
            rx.try_recv().is_err(),
            "should not send duplicate split for same centroid"
        );
    }

    #[tokio::test]
    async fn should_not_send_split_when_below_threshold() {
        // given - threshold of 5, centroid 1
        let centroid_id = 1u64;
        let (ctx, mut rx) = create_test_context_with_threshold(centroid_id, 5).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        // when - write 3 vectors (below threshold of 5)
        let writes = vec![
            create_vector_write("v1", vec![1.0, 0.0, 0.0]),
            create_vector_write("v2", vec![0.0, 1.0, 0.0]),
            create_vector_write("v3", vec![0.0, 0.0, 1.0]),
        ];
        delta.apply(VectorDbWrite::Write(writes)).unwrap();

        // then - no split ops should have been sent
        assert!(
            rx.try_recv().is_err(),
            "should not send split when below threshold"
        );
    }
}

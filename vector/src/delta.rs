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
    type DeltaView = ();
    type Frozen = VectorDbImmutableDelta;
    type FrozenView = ();
    type ApplyResult = ();

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

    fn freeze(self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
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

        (VectorDbImmutableDelta { ops }, (), self.ctx)
    }

    fn reader(&self) -> Self::DeltaView {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hnsw::CentroidGraph;
    use crate::model::AttributeValue;
    use crate::serde::key::{DeletionsKey, IdDictionaryKey, PostingListKey, VectorDataKey};
    use bytes::Bytes;
    use common::SequenceAllocator;
    use common::coordinator::Delta;
    use common::storage::RecordOp;
    use common::storage::in_memory::InMemoryStorage;
    use std::sync::Arc;

    /// Mock CentroidGraph that always returns a fixed centroid ID.
    struct MockCentroidGraph {
        centroid_id: u32,
    }

    impl MockCentroidGraph {
        fn new(centroid_id: u32) -> Self {
            Self { centroid_id }
        }
    }

    impl CentroidGraph for MockCentroidGraph {
        fn search(&self, _query: &[f32], _k: usize) -> Vec<u32> {
            vec![self.centroid_id]
        }

        fn len(&self) -> usize {
            1
        }
    }

    /// Create a test context with the given centroid ID for assignment.
    async fn create_test_context(centroid_id: u32) -> VectorDbDeltaContext {
        let storage: Arc<dyn common::Storage> = Arc::new(InMemoryStorage::new());
        let key = Bytes::from_static(&[0x01, 0x02]);
        let id_allocator = SequenceAllocator::load(storage.as_ref(), key)
            .await
            .unwrap();

        VectorDbDeltaContext {
            dimensions: 3,
            dictionary: Arc::new(DashMap::new()),
            centroid_graph: Arc::new(MockCentroidGraph::new(centroid_id)),
            id_allocator,
        }
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
            RecordOp::Put(record) => record.key.starts_with(prefix),
            _ => false,
        }
    }

    /// Helper to check if an op is a Merge for a specific key prefix.
    fn is_merge_with_key_prefix(op: &RecordOp, prefix: &[u8]) -> bool {
        match op {
            RecordOp::Merge(record) => record.key.starts_with(prefix),
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
        delta.apply(vec![write]).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have ops for ID dictionary put and vector data put
        let id_dict_key = IdDictionaryKey::new("vec-1").encode();
        let vector_data_key_prefix = VectorDataKey::new(0).encode();

        // Find ID dictionary put
        let has_id_dict_put = frozen.ops.iter().any(|op| match op {
            RecordOp::Put(record) => record.key == id_dict_key,
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
        let centroid_id = 42u32;
        let ctx = create_test_context(centroid_id).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        let write = create_vector_write("vec-1", vec![1.0, 2.0, 3.0]);

        // when
        delta.apply(vec![write]).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have a merge op for the posting list of centroid 42
        let posting_key = PostingListKey::new(centroid_id).encode();
        let has_posting_merge = frozen.ops.iter().any(|op| match op {
            RecordOp::Merge(record) => record.key == posting_key,
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
        delta.apply(vec![write]).unwrap();

        // then - dictionary should be updated in memory
        assert!(dictionary.contains_key("vec-1"));
        let internal_id = *dictionary.get("vec-1").unwrap();
        assert_eq!(internal_id, 0, "first allocated ID should be 0");
    }

    #[tokio::test]
    async fn should_add_vectors_on_update() {
        // given
        let ctx = create_test_context(1).await;

        // Pre-populate dictionary to simulate existing vector
        ctx.dictionary.insert("vec-1".to_string(), 100);

        let mut delta = VectorDbWriteDelta::init(ctx);

        let write = create_vector_write("vec-1", vec![4.0, 5.0, 6.0]);

        // when
        delta.apply(vec![write]).unwrap();
        let (frozen, _view, ctx) = delta.freeze();

        // then - should have delete for old ID dictionary entry and put for new
        let id_dict_key = IdDictionaryKey::new("vec-1").encode();

        let has_id_dict_delete = frozen.ops.iter().any(|op| match op {
            RecordOp::Delete(key) => *key == id_dict_key,
            _ => false,
        });
        assert!(has_id_dict_delete, "should have ID dictionary delete op");

        let has_id_dict_put = frozen.ops.iter().any(|op| match op {
            RecordOp::Put(record) => record.key == id_dict_key,
            _ => false,
        });
        assert!(has_id_dict_put, "should have ID dictionary put op");

        // Dictionary should have new internal ID
        let new_internal_id = *ctx.dictionary.get("vec-1").unwrap();
        assert_ne!(new_internal_id, 100, "internal ID should be updated");
    }

    #[tokio::test]
    async fn should_assign_vectors_to_postings_on_update() {
        // given
        let centroid_id = 5u32;
        let ctx = create_test_context(centroid_id).await;

        // Pre-populate dictionary to simulate existing vector
        ctx.dictionary.insert("vec-1".to_string(), 100);

        let mut delta = VectorDbWriteDelta::init(ctx);

        let write = create_vector_write("vec-1", vec![4.0, 5.0, 6.0]);

        // when
        delta.apply(vec![write]).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have posting list merge for the new vector
        let posting_key = PostingListKey::new(centroid_id).encode();
        let has_posting_merge = frozen.ops.iter().any(|op| match op {
            RecordOp::Merge(record) => record.key == posting_key,
            _ => false,
        });
        assert!(
            has_posting_merge,
            "should have posting list merge op on update"
        );
    }

    #[tokio::test]
    async fn should_mark_old_vector_internal_id_deleted_on_update() {
        // given
        let ctx = create_test_context(1).await;
        let old_internal_id = 100u64;

        // Pre-populate dictionary to simulate existing vector
        ctx.dictionary.insert("vec-1".to_string(), old_internal_id);

        let mut delta = VectorDbWriteDelta::init(ctx);

        let write = create_vector_write("vec-1", vec![4.0, 5.0, 6.0]);

        // when
        delta.apply(vec![write]).unwrap();
        let (frozen, _view, _ctx) = delta.freeze();

        // then - should have delete op for old vector data
        let old_vector_key = VectorDataKey::new(old_internal_id).encode();
        let has_vector_delete = frozen.ops.iter().any(|op| match op {
            RecordOp::Delete(key) => *key == old_vector_key,
            _ => false,
        });
        assert!(has_vector_delete, "should have vector data delete op");

        // Should have merge op for deleted vectors bitmap
        let deletions_key = DeletionsKey::new().encode();
        let has_deletions_merge = frozen.ops.iter().any(|op| match op {
            RecordOp::Merge(record) => record.key == deletions_key,
            _ => false,
        });
        assert!(
            has_deletions_merge,
            "should have deletions merge op to mark old ID as deleted"
        );
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
        delta.apply(writes).unwrap();
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
        delta.apply(writes).unwrap();

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
            fn search(&self, query: &[f32], _k: usize) -> Vec<u32> {
                // Return centroid based on which dimension has highest value
                if query[0] > query[1] && query[0] > query[2] {
                    vec![1]
                } else if query[1] > query[2] {
                    vec![2]
                } else {
                    vec![3]
                }
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

        let ctx = VectorDbDeltaContext {
            dimensions: 3,
            dictionary: Arc::new(DashMap::new()),
            centroid_graph: Arc::new(MultiCentroidGraph),
            id_allocator,
        };

        let mut delta = VectorDbWriteDelta::init(ctx);

        let writes = vec![
            create_vector_write("vec-1", vec![1.0, 0.0, 0.0]), // -> centroid 1
            create_vector_write("vec-2", vec![0.0, 1.0, 0.0]), // -> centroid 2
            create_vector_write("vec-3", vec![0.0, 0.0, 1.0]), // -> centroid 3
            create_vector_write("vec-4", vec![0.9, 0.1, 0.0]), // -> centroid 1
        ];

        // when
        delta.apply(writes).unwrap();
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
    async fn should_estimate_size_correctly() {
        // given
        let ctx = create_test_context(1).await;
        let mut delta = VectorDbWriteDelta::init(ctx);

        // Initial size should be 0
        assert_eq!(delta.estimate_size(), 0);

        // when - add a vector
        let write = create_vector_write("vec-1", vec![1.0, 2.0, 3.0]);
        delta.apply(vec![write]).unwrap();

        // then - size should be non-zero
        let size = delta.estimate_size();
        assert!(size > 0, "size should be non-zero after adding vector");
    }
}

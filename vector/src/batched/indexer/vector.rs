use crate::Error::Internal;
use crate::Result;
use crate::batched::indexer::IndexerOpts;
use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::split::ReassignVector;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::delta::VectorWrite;
use crate::model::VECTOR_FIELD_NAME;
use crate::serde::FieldValue;
use crate::serde::vector_data::VectorDataValue;
use common::StorageRead;
use futures::future::BoxFuture;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;
use tracing::debug;

/// An upsert where we need to resolve the old vector data from storage.
struct ResolvedUpsert {
    write: VectorWrite,
    old: (u64, VectorDataValue),
}

pub(crate) struct WriteVectors {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    writes: Vec<VectorWrite>,
}

impl WriteVectors {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        writes: Vec<VectorWrite>,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            writes,
        }
    }

    pub(crate) async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<()> {
        if self.writes.is_empty() {
            return Ok(());
        }
        let view = VectorIndexView::new(delta, state, self.snapshot.clone());

        // compact so last write for each external id wins
        let writes = Self::compact_writes(self.writes);

        // Partition into inserts (new vectors) vs upserts (existing external_id).
        // Centroid assignment is computed separately on the blocking pool so it can run
        // in parallel with the upsert storage reads.
        let centroid_graph = view.centroid_graph();
        let assignment_inputs: Vec<_> = writes
            .iter()
            .map(|write| (write.external_id.clone(), write.values.clone()))
            .collect();
        let assignment_handle =
            task::spawn_blocking(move || Self::assign_centroids(assignment_inputs, centroid_graph));

        let mut inserts = Vec::with_capacity(writes.len());
        let mut upsert_futures: Vec<BoxFuture<'static, Result<ResolvedUpsert>>> = Vec::new();

        for w in writes {
            let Some(old_vector_id) = view.vector_id(&w.external_id) else {
                inserts.push(w);
                continue;
            };
            // Upsert — need to read old vector data from storage
            let data_fut = view.vector_data(old_vector_id, self.opts.dimensions);
            upsert_futures.push(Box::pin(async move {
                let old_data = data_fut.await?.ok_or_else(|| {
                    Internal(format!("missing vector data for id {}", old_vector_id))
                })?;
                Ok(ResolvedUpsert {
                    write: w,
                    old: (old_vector_id, old_data),
                })
            }));
        }

        // Resolve upserts concurrently (bounded by AsyncBatchDriver) while the
        // centroid assignments run on the blocking pool.
        let (centroid_assignments, upsert_results) =
            tokio::join!(assignment_handle, AsyncBatchDriver::execute(upsert_futures));
        let centroid_assignments = centroid_assignments
            .map_err(|e| Internal(format!("centroid assignment task failed: {e}")))??;
        let mut upserts = Vec::with_capacity(upsert_results.len());
        for result in upsert_results {
            upserts.push(result?);
        }
        drop(view);

        // Apply inserts to delta (no old data to clean up)
        for insert in inserts {
            let centroid = *centroid_assignments
                .get(&insert.external_id)
                .ok_or_else(|| {
                    Internal(format!(
                        "missing centroid assignment for external_id={}",
                        insert.external_id
                    ))
                })?;
            let vector_id = delta.add_vector(&insert.external_id, &insert.attributes);
            delta.add_to_posting(centroid, vector_id, insert.values.clone());
            for (attr_name, attr_value) in &insert.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.opts.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta.add_to_inverted_index(attr_name.clone(), field_value, vector_id);
            }
        }

        // Apply upserts to delta
        for upsert in upserts {
            let centroid = *centroid_assignments
                .get(&upsert.write.external_id)
                .ok_or_else(|| {
                    Internal(format!(
                        "missing centroid assignment for external_id={}",
                        upsert.write.external_id
                    ))
                })?;
            let (old_vector_id, _old_vector_data) = upsert.old;
            delta.delete_vector(old_vector_id);
            // todo: delete from old postings and inverted index
            let vector_id = delta.add_vector(&upsert.write.external_id, &upsert.write.attributes);
            delta.add_to_posting(centroid, vector_id, upsert.write.values.clone());
            for (attr_name, attr_value) in &upsert.write.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.opts.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta.add_to_inverted_index(attr_name.clone(), field_value, vector_id);
            }
        }
        Ok(())
    }

    fn assign_centroids(
        writes: Vec<(String, Vec<f32>)>,
        centroid_graph: Arc<crate::batched::indexer::state::DirtyCentroidGraph>,
    ) -> Result<HashMap<String, u64>> {
        let assignments: Vec<_> = writes
            .into_par_iter()
            .map(|(external_id, values)| {
                centroid_graph
                    .search(&values, 1)
                    .first()
                    .copied()
                    .ok_or_else(|| Internal("no centroids found".to_string()))
                    .map(|centroid| (external_id, centroid))
            })
            .collect();

        let mut centroid_assignments = HashMap::with_capacity(assignments.len());
        for assignment in assignments {
            let (external_id, centroid) = assignment?;
            centroid_assignments.insert(external_id, centroid);
        }

        Ok(centroid_assignments)
    }

    fn compact_writes(updates: Vec<VectorWrite>) -> Vec<VectorWrite> {
        let mut compacted = HashMap::with_capacity(updates.len());
        for update in updates {
            compacted.insert(update.external_id.clone(), update);
        }
        compacted.into_values().collect()
    }
}

struct VerifiedVectorReassignment {
    reassignment: ReassignVector,
    centroid: u64,
}

struct ResolvedVectorReassignment {
    reassignment: ReassignVector,
    data: VectorDataValue,
    centroid: u64,
}

pub(crate) struct ReassignVectors {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    reassignments: Vec<ReassignVector>,
}

impl ReassignVectors {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        reassignments: Vec<ReassignVector>,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            reassignments,
        }
    }

    pub(crate) async fn execute(
        mut self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<()> {
        if self.reassignments.is_empty() {
            return Ok(());
        }
        let view = VectorIndexView::new(delta, state, self.snapshot);
        let centroid_graph = view.centroid_graph();

        // update current centroid in case centroid was moved as part of a split/merge
        self.reassignments.iter_mut().for_each(|r| {
            if let Some(p) = view.last_written_posting(r.vector_id) {
                r.current_centroid = p;
            }
        });

        // determine which vectors actually need a new assignment
        let reassignments: Vec<_> = self
            .reassignments
            .into_par_iter()
            .filter_map(|r| {
                let &closest_centroid = centroid_graph
                    .search(&r.vector, 1)
                    .first()
                    .expect("no centroids");
                if closest_centroid == r.current_centroid {
                    None
                } else {
                    Some(VerifiedVectorReassignment {
                        reassignment: r,
                        centroid: closest_centroid,
                    })
                }
            })
            .collect();

        // pull the old vector data so we can update inverted indexes
        let mut to_resolve = Vec::with_capacity(reassignments.len());
        for r in reassignments {
            let data_fut = view.vector_data(r.reassignment.vector_id, self.opts.dimensions);
            to_resolve.push(Box::pin(async move {
                Ok(ResolvedVectorReassignment {
                    reassignment: r.reassignment,
                    data: data_fut.await?.expect("missing vector data"),
                    centroid: r.centroid,
                })
            })
                as BoxFuture<Result<ResolvedVectorReassignment>>);
        }
        let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
        let mut resolved = Vec::with_capacity(resolve_results.len());
        for result in resolve_results {
            resolved.push(result?);
        }
        drop(view);

        // execute the reassignments
        for r in resolved {
            debug!("old data: {:?}", r.data);
            delta.remove_from_posting(r.reassignment.current_centroid, r.reassignment.vector_id);
            delta.add_to_posting(r.centroid, r.reassignment.vector_id, r.reassignment.vector);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batched::indexer::test_utils::IndexerOpTestHarness;
    use crate::model::AttributeValue;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::vector_data::Field;
    use crate::storage::VectorDbStorageReadExt;
    use std::collections::HashSet;

    const DIMS: usize = 3;
    const CENTROID_ID: u64 = 0;

    fn create_opts() -> Arc<IndexerOpts> {
        Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            merge_threshold_vectors: 0,
            split_threshold_vectors: usize::MAX,
            split_search_neighbourhood: 4,
            indexed_fields: HashSet::from(["category".to_string()]),
            chunk_target: 4096,
        })
    }

    fn make_write(id: &str, values: Vec<f32>, category: &str) -> VectorWrite {
        VectorWrite {
            external_id: id.to_string(),
            values: values.clone(),
            attributes: vec![
                ("vector".to_string(), AttributeValue::Vector(values)),
                (
                    "category".to_string(),
                    AttributeValue::String(category.to_string()),
                ),
                (
                    "description".to_string(),
                    AttributeValue::String(format!("{id} description")),
                ),
            ],
        }
    }

    fn make_data(id: &str, values: Vec<f32>, category: &str) -> VectorDataValue {
        let fields = vec![
            Field::new("vector".to_string(), FieldValue::Vector(values)),
            Field::new(
                "category".to_string(),
                FieldValue::String(category.to_string()),
            ),
            Field::new(
                "description".to_string(),
                FieldValue::String(format!("{id} description")),
            ),
        ];
        VectorDataValue::new(id.to_string(), fields)
    }

    #[tokio::test]
    async fn should_update_delta_correctly_from_write_vectors() {
        // given — empty state, 3 new vectors with different categories
        let mut h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let opts = create_opts();
        let writes = vec![
            make_write("a", vec![1.0, 0.0, 0.0], "shoes"),
            make_write("b", vec![0.0, 1.0, 0.0], "shoes"),
            make_write("c", vec![0.0, 0.0, 1.0], "boots"),
        ];

        // when
        h.write_and_apply(&opts, writes).await;

        // then:
        // storage should have dictionary + vector data for each vector
        let expected = vec![
            ("a", vec![1.0, 0.0, 0.0], "shoes"),
            ("b", vec![0.0, 1.0, 0.0], "shoes"),
            ("c", vec![0.0, 0.0, 1.0], "boots"),
        ];
        let mut expected_postings: HashMap<u64, Vec<f32>> = HashMap::new();
        let mut internal_ids = HashMap::new();
        for (ext_id, values, category) in &expected {
            let internal_id = h
                .storage
                .lookup_internal_id(ext_id)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("missing dictionary entry for {ext_id}"));
            internal_ids.insert(ext_id.to_string(), internal_id);
            let data = h
                .storage
                .get_vector_data(internal_id, DIMS)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("missing vector data for {ext_id}"));
            assert_eq!(data, make_data(ext_id, values.clone(), category));
            expected_postings.insert(internal_id, values.clone());
        }
        // posting list should contain exactly the 3 vectors with correct IDs and values
        let posting = h.storage.get_posting_list(CENTROID_ID, DIMS).await.unwrap();
        assert_eq!(posting.len(), 3);
        for p in posting.iter() {
            let expected_vec = expected_postings
                .get(&p.id())
                .unwrap_or_else(|| panic!("unexpected vector id {} in posting list", p.id()));
            assert_eq!(
                p.vector(),
                Some(expected_vec.as_slice()),
                "vector mismatch for posting id {}",
                p.id()
            );
        }
        // inverted index should have correct entries
        let shoes_bitmap = h
            .storage
            .get_metadata_index("category", FieldValue::String("shoes".to_string()))
            .await
            .unwrap();
        assert_eq!(shoes_bitmap.len(), 2);
        assert!(shoes_bitmap.contains(internal_ids["a"]));
        assert!(shoes_bitmap.contains(internal_ids["b"]));
        let boots_bitmap = h
            .storage
            .get_metadata_index("category", FieldValue::String("boots".to_string()))
            .await
            .unwrap();
        assert_eq!(boots_bitmap.len(), 1);
        assert!(boots_bitmap.contains(internal_ids["c"]));
        // non-indexed field "description" should have no inverted index entries
        for id in ["a", "b", "c"] {
            let bitmap = h
                .storage
                .get_metadata_index(
                    "description",
                    FieldValue::String(format!("{id} description")),
                )
                .await
                .unwrap();
            assert!(
                bitmap.is_empty(),
                "non-indexed field 'description' should have no inverted index entries for {id}"
            );
        }
    }

    #[tokio::test]
    async fn should_update_delta_correctly_from_write_vectors_on_update() {
        // given — insert 2 vectors first
        let mut h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let opts = create_opts();
        h.write_and_apply(
            &opts,
            vec![
                make_write("a", vec![1.0, 0.0, 0.0], "shoes"),
                make_write("b", vec![0.0, 1.0, 0.0], "shoes"),
            ],
        )
        .await;
        let old_id_a = h.storage.lookup_internal_id("a").await.unwrap().unwrap();
        let old_id_b = h.storage.lookup_internal_id("b").await.unwrap().unwrap();

        // when — upsert both with new values
        h.write_and_apply(
            &opts,
            vec![
                make_write("a", vec![2.0, 0.0, 0.0], "shoes"),
                make_write("b", vec![0.0, 2.0, 0.0], "shoes"),
            ],
        )
        .await;

        // then — old vector data should be deleted from storage
        assert!(
            h.storage
                .get_vector_data(old_id_a, DIMS)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            h.storage
                .get_vector_data(old_id_b, DIMS)
                .await
                .unwrap()
                .is_none()
        );
        // then — dictionary lookups should resolve to new vector data with updated values
        let new_id_a = h.storage.lookup_internal_id("a").await.unwrap().unwrap();
        let new_id_b = h.storage.lookup_internal_id("b").await.unwrap().unwrap();
        assert_ne!(old_id_a, new_id_a);
        assert_ne!(old_id_b, new_id_b);
        let data_a = h
            .storage
            .get_vector_data(new_id_a, DIMS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data_a, make_data("a", vec![2.0, 0.0, 0.0], "shoes"));
        let data_b = h
            .storage
            .get_vector_data(new_id_b, DIMS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data_b, make_data("b", vec![0.0, 2.0, 0.0], "shoes"));
    }

    #[tokio::test]
    async fn should_compact_duplicate_writes_from_write_vectors() {
        // given — 3 writes with the same external_id
        let mut h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let opts = create_opts();
        let writes = vec![
            make_write("x", vec![1.0, 0.0, 0.0], "shoes"),
            make_write("x", vec![2.0, 0.0, 0.0], "clothes"),
            make_write("x", vec![3.0, 0.0, 0.0], "furniture"),
        ];

        // when
        h.write_and_apply(&opts, writes).await;

        // then — storage should have the last write's values
        let internal_id = h.storage.lookup_internal_id("x").await.unwrap().unwrap();
        let data = h
            .storage
            .get_vector_data(internal_id, DIMS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data, make_data("x", vec![3.0, 0.0, 0.0], "furniture"));
        // then — posting list should have exactly 1 entry
        let posting = h.storage.get_posting_list(CENTROID_ID, DIMS).await.unwrap();
        assert_eq!(posting.len(), 1);
    }

    // ---- ReassignVectors tests ----

    #[tokio::test]
    async fn should_reassign_vectors_after_split() {
        // given — all vectors initially at centroid 0 (single centroid db).
        // Then centroid 0 is "split" into two new centroids via the delta,
        // and vectors should be reassigned to their nearest new centroid.
        let mut h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let opts = create_opts();
        let writes = vec![
            make_write("a", vec![0.9, 0.1, 0.0], "shoes"),
            make_write("b", vec![0.1, 0.9, 0.0], "shoes"),
            make_write("c", vec![0.2, 0.8, 0.0], "boots"),
        ];
        h.write_and_apply(&opts, writes).await;
        let id_a = h.storage.lookup_internal_id("a").await.unwrap().unwrap();
        let id_b = h.storage.lookup_internal_id("b").await.unwrap().unwrap();
        let id_c = h.storage.lookup_internal_id("c").await.unwrap().unwrap();
        // Simulate a split: delete centroid 0, add two new centroids via the delta
        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        delta.delete_centroids(vec![CENTROID_ID]);
        let c_a = delta.add_centroid(vec![1.0, 0.0, 0.0]);
        let c_b = delta.add_centroid(vec![0.0, 1.0, 0.0]);

        // All vectors claim current_centroid=0 (the deleted centroid)
        let reassignments = vec![
            ReassignVector {
                vector_id: id_a,
                vector: vec![0.9, 0.1, 0.0],
                current_centroid: CENTROID_ID,
            },
            ReassignVector {
                vector_id: id_b,
                vector: vec![0.1, 0.9, 0.0],
                current_centroid: CENTROID_ID,
            },
            ReassignVector {
                vector_id: id_c,
                vector: vec![0.2, 0.8, 0.0],
                current_centroid: CENTROID_ID,
            },
        ];

        // when
        let rv = ReassignVectors::new(&opts, &(snapshot as Arc<dyn StorageRead>), reassignments);
        rv.execute(&h.state, &mut delta).await.unwrap();
        let ops = delta.freeze(&mut h.state);
        h.storage.apply(ops).await.unwrap();

        // then — "a" (near [1,0,0]) should be at c_a, "b" and "c" (near [0,1,0]) at c_b
        let posting_a = h
            .storage
            .get_posting_list(c_a.centroid_id, DIMS)
            .await
            .unwrap();
        let ids_a: HashSet<u64> = posting_a.iter().map(|p| p.id()).collect();
        assert!(ids_a.contains(&id_a), "c_a should contain vector a");
        assert!(!ids_a.contains(&id_b), "c_a should not contain vector b");
        assert!(!ids_a.contains(&id_c), "c_a should not contain vector c");
        let posting_b = h
            .storage
            .get_posting_list(c_b.centroid_id, DIMS)
            .await
            .unwrap();
        let ids_b: HashSet<u64> = posting_b.iter().map(|p| p.id()).collect();
        assert!(ids_b.contains(&id_b), "c_b should contain vector b");
        assert!(ids_b.contains(&id_c), "c_b should contain vector c");
        assert!(!ids_b.contains(&id_a), "c_b should not contain vector a");
    }

    #[tokio::test]
    async fn should_skip_reassignment_when_already_at_closest_centroid() {
        // given — vector "a" is at centroid 0, which is already its nearest centroid
        let mut h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let opts = create_opts();
        let writes = vec![make_write("a", vec![0.9, 0.1, 0.0], "shoes")];
        h.write_and_apply(&opts, writes).await;
        let id_a = h.storage.lookup_internal_id("a").await.unwrap().unwrap();

        let reassignments = vec![ReassignVector {
            vector_id: id_a,
            vector: vec![0.9, 0.1, 0.0],
            current_centroid: CENTROID_ID,
        }];

        // when
        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let rv = ReassignVectors::new(&opts, &(snapshot as Arc<dyn StorageRead>), reassignments);
        rv.execute(&h.state, &mut delta).await.unwrap();
        let ops = delta.freeze(&mut h.state);

        // then — no posting merges written (vector stays put)
        let posting_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, common::storage::RecordOp::Merge(_)))
            .collect();
        assert!(
            posting_ops.is_empty(),
            "no posting merges should be written when vector stays put"
        );
    }

    #[tokio::test]
    async fn should_use_delta_posting_as_true_current_centroid() {
        // This tests the case where a reassignment is queued for a vector from a
        // neighbouring centroid, but that neighbour was ALSO split in the same round.
        // The vector was already moved to a new centroid by the earlier split, so
        // current_centroid in the ReassignVector is stale. The reassigner should
        // check the delta's current_posting to get the true current centroid.
        let mut h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let opts = create_opts();
        // Write vector "a" to centroid 0
        let writes = vec![make_write("a", vec![0.9, 0.1, 0.0], "shoes")];
        h.write_and_apply(&opts, writes).await;
        let id_a = h.storage.lookup_internal_id("a").await.unwrap().unwrap();
        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        delta.delete_centroids(vec![CENTROID_ID]);
        let c0 = delta.add_centroid(vec![1.0, 0.0, 0.0]); // "a" is nearest to this
        let c1 = delta.add_centroid(vec![0.0, 1.0, 0.0]);
        // The earlier split already moved "a" to c0 in this delta
        delta.add_to_posting(c0.centroid_id, id_a, vec![0.9, 0.1, 0.0]);

        // Now a neighbour reassignment comes in with stale current_centroid=CENTROID_ID
        let reassignments = vec![ReassignVector {
            vector_id: id_a,
            vector: vec![0.9, 0.1, 0.0],
            current_centroid: CENTROID_ID, // stale — "a" is actually at c0 now
        }];

        // when
        let rv = ReassignVectors::new(&opts, &(snapshot as Arc<dyn StorageRead>), reassignments);
        rv.execute(&h.state, &mut delta).await.unwrap();
        let ops = delta.freeze(&mut h.state);
        h.storage.apply(ops).await.unwrap();

        // then — "a" should remain at c0 (its nearest centroid). The reassigner
        // should have corrected current_centroid from the delta's posting and
        // seen that c0 is already the closest, so no move happens.
        let posting_c0 = h
            .storage
            .get_posting_list(c0.centroid_id, DIMS)
            .await
            .unwrap();
        let ids_c0: HashSet<u64> = posting_c0.iter().map(|p| p.id()).collect();
        assert!(ids_c0.contains(&id_a), "c0 should still contain vector a");
        // c1 should not have "a"
        let posting_c1 = h
            .storage
            .get_posting_list(c1.centroid_id, DIMS)
            .await
            .unwrap();
        let ids_c1: HashSet<u64> = posting_c1.iter().map(|p| p.id()).collect();
        assert!(!ids_c1.contains(&id_a), "c1 should not contain vector a");
    }
}

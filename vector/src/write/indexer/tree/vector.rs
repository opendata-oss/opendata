use crate::Error::Internal;
use crate::Result;
use crate::model::VECTOR_FIELD_NAME;
use crate::serde::FieldValue;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::vector_data::VectorDataValue;
use crate::serde::vector_id::VectorId;
use crate::write::delta::VectorWrite;
use crate::write::indexer::drivers::AsyncBatchDriver;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::centroids::{
    LeveledCentroidIndex, TreeLevel, batch_search_centroids, batch_search_centroids_in_level,
};
use crate::write::indexer::tree::split::ReassignVector;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use common::StorageRead;
use futures::future::BoxFuture;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::trace;

/// An upsert where we need to resolve the old vector data from storage.
struct ResolvedUpsert {
    write: VectorWrite,
    old: (VectorId, VectorDataValue),
}

pub(crate) struct WriteVectors {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
    writes: Vec<VectorWrite>,
}

impl WriteVectors {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        writes: Vec<VectorWrite>,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            snapshot_epoch,
            writes,
        }
    }

    /// Returns (inserts, updates) counts.
    pub(crate) async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<(usize, usize)> {
        if self.writes.is_empty() {
            return Ok((0, 0));
        }
        let (inserts, upserts, centroid_assignments) = {
            let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);

            // compact so last write for each external id wins
            let writes = Self::compact_writes(self.writes);

            // Partition into inserts (new vectors) vs upserts (existing external_id).
            // Centroid assignment is computed separately on the blocking pool so it can run
            // in parallel with the upsert storage reads.
            let centroid_index =
                view.centroid_index(self.opts.dimensions, self.opts.distance_metric);
            let assignment_inputs: Vec<_> = writes
                .iter()
                .map(|write| (write.external_id.clone(), write.values.as_slice()))
                .collect();
            let assignments_fut = Self::assign_centroids(assignment_inputs, &centroid_index);

            let mut inserts = Vec::with_capacity(writes.len());
            let mut upsert_futures: Vec<BoxFuture<'static, Result<ResolvedUpsert>>> = Vec::new();

            for w in writes.clone() {
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
                tokio::join!(assignments_fut, AsyncBatchDriver::execute(upsert_futures));
            let centroid_assignments = centroid_assignments?;
            let mut upserts = Vec::with_capacity(upsert_results.len());
            for result in upsert_results {
                upserts.push(result?);
            }
            (inserts, upserts, centroid_assignments)
        };

        let insert_count = inserts.len();
        let update_count = upserts.len();

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
            let vector_id = delta
                .forward_index
                .add_vector(&insert.external_id, &insert.attributes);
            delta
                .search_index
                .add_to_posting(centroid, vector_id, insert.values.clone());
            for (attr_name, attr_value) in &insert.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.opts.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta
                    .search_index
                    .add_to_inverted_index(attr_name.clone(), field_value, vector_id);
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
            delta.forward_index.delete_vector(old_vector_id);
            // todo: delete from old postings and inverted index
            let vector_id = delta
                .forward_index
                .add_vector(&upsert.write.external_id, &upsert.write.attributes);
            delta
                .search_index
                .add_to_posting(centroid, vector_id, upsert.write.values.clone());
            for (attr_name, attr_value) in &upsert.write.attributes {
                if attr_name == VECTOR_FIELD_NAME {
                    continue;
                }
                if !self.opts.indexed_fields.contains(attr_name) {
                    continue;
                }
                let field_value: FieldValue = attr_value.clone().into();
                delta
                    .search_index
                    .add_to_inverted_index(attr_name.clone(), field_value, vector_id);
            }
        }
        Ok((insert_count, update_count))
    }

    async fn assign_centroids(
        writes: Vec<(String, &[f32])>,
        centroid_index: &LeveledCentroidIndex<'_>,
    ) -> Result<HashMap<String, VectorId>> {
        let search_result = batch_search_centroids(centroid_index, 1, writes).await?;
        let mut centroid_assignments = HashMap::with_capacity(search_result.len());
        for (external_id, centroids) in search_result {
            let Some(centroid) = centroids.first() else {
                return Err(Internal("no centroids found".to_string()));
            };
            centroid_assignments.insert(external_id, centroid.id());
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
    new_centroid: VectorId,
}

struct ResolvedVectorReassignment {
    reassignment: ReassignVector,
    data: VectorDataValue,
    new_centroid: VectorId,
}

struct ResolvedCentroidReassignment {
    reassignment: ReassignVector,
    data: CentroidInfoValue,
    new_centroid: VectorId,
}

pub(crate) struct ReassignVectors {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
    reassignments: Vec<ReassignVector>,
    level: TreeLevel,
}

impl ReassignVectors {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        reassignments: Vec<ReassignVector>,
        level: TreeLevel,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            snapshot_epoch,
            reassignments,
            level,
        }
    }

    /// Returns the number of vectors actually reassigned.
    pub(crate) async fn execute(
        mut self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<usize> {
        if self.reassignments.is_empty() {
            return Ok(0);
        }
        // sanity check we don't have any double reassignments
        let ids = self
            .reassignments
            .iter()
            .map(|r| r.vector_id)
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), self.reassignments.len());
        let reassignments = {
            let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);
            let centroid_index =
                view.centroid_index(self.opts.dimensions, self.opts.distance_metric);

            // update current centroid in case centroid was moved as part of a split/merge
            self.reassignments.iter_mut().for_each(|r| {
                assert_eq!(r.vector_id.level() + 1, self.level.level());
                assert_eq!(r.level, self.level);
                if let Some(p) = view.last_written_posting(r.vector_id) {
                    r.current_centroid = p;
                }
            });

            let ann_search_batch: Vec<_> = self
                .reassignments
                .iter()
                .map(|r| (r.vector_id, r.vector.as_slice()))
                .collect();
            let assignments =
                batch_search_centroids_in_level(&centroid_index, 1, ann_search_batch, self.level)
                    .await?;

            // determine which vectors actually need a new assignment
            let reassignments: Vec<_> = self
                .reassignments
                .into_iter()
                .filter_map(|r| {
                    let closest_centroid = assignments
                        .get(&r.vector_id)
                        .expect("no centroids")
                        .first()
                        .expect("no centroids")
                        .id();
                    if closest_centroid == r.current_centroid {
                        None
                    } else {
                        Some(VerifiedVectorReassignment {
                            reassignment: r,
                            new_centroid: closest_centroid,
                        })
                    }
                })
                .collect();
            reassignments
        };

        if self.level.is_leaf() {
            Self::execute_vector_reassignments(
                &self.opts,
                &self.snapshot,
                self.snapshot_epoch,
                state,
                delta,
                reassignments,
            )
            .await
        } else {
            Self::execute_centroid_reassignments(
                &self.snapshot,
                self.snapshot_epoch,
                state,
                delta,
                reassignments,
            )
            .await
        }
    }

    async fn execute_centroid_reassignments(
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
        reassignments: Vec<VerifiedVectorReassignment>,
    ) -> Result<usize> {
        let resolved = {
            let view = VectorIndexView::new(delta, state, snapshot, snapshot_epoch);
            reassignments
                .into_iter()
                .map(|r| ResolvedCentroidReassignment {
                    data: view
                        .centroid(r.reassignment.vector_id)
                        .cloned()
                        .expect("unexpected missing centroid"),
                    reassignment: r.reassignment,
                    new_centroid: r.new_centroid,
                })
                .collect::<Vec<_>>()
        };
        let nreassigned = resolved.len();
        for mut r in resolved {
            delta
                .search_index
                .remove_from_posting(r.reassignment.current_centroid, r.reassignment.vector_id);
            delta.search_index.add_to_posting(
                r.new_centroid,
                r.reassignment.vector_id,
                r.reassignment.vector,
            );
            r.data.parent_vector_id = r.new_centroid;
            delta
                .search_index
                .update_centroid(r.reassignment.vector_id, r.data);
        }
        Ok(nreassigned)
    }

    async fn execute_vector_reassignments(
        opts: &IndexerOpts,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
        reassignments: Vec<VerifiedVectorReassignment>,
    ) -> Result<usize> {
        let resolved = {
            let view = VectorIndexView::new(delta, state, snapshot, snapshot_epoch);
            // pull the old vector data so we can update inverted indexes
            let mut to_resolve = Vec::with_capacity(reassignments.len());
            for r in reassignments {
                let data_fut = view.vector_data(r.reassignment.vector_id, opts.dimensions);
                to_resolve.push(Box::pin(async move {
                    Ok(ResolvedVectorReassignment {
                        reassignment: r.reassignment,
                        // TODO: this will panic on update - we need to delete vector from old posting
                        data: data_fut.await?.expect("missing vector data"),
                        new_centroid: r.new_centroid,
                    })
                })
                    as BoxFuture<Result<ResolvedVectorReassignment>>);
            }
            let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
            let mut resolved = Vec::with_capacity(resolve_results.len());
            for result in resolve_results {
                resolved.push(result?);
            }
            resolved
        };
        let nreassigned = resolved.len();
        for r in resolved {
            trace!("old data: {:?}", r.data);
            delta
                .search_index
                .remove_from_posting(r.reassignment.current_centroid, r.reassignment.vector_id);
            delta.search_index.add_to_posting(
                r.new_centroid,
                r.reassignment.vector_id,
                r.reassignment.vector,
            );
        }
        Ok(nreassigned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AttributeValue, MetadataFieldSpec};
    use crate::serde::FieldType;
    use crate::serde::FieldValue;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::vector_data::Field;
    use crate::serde::vector_id::ROOT_VECTOR_ID;
    use crate::storage::VectorDbStorageReadExt;
    use crate::write::indexer::tree::centroids::{TreeDepth, TreeLevel};
    use crate::write::indexer::tree::posting_list::{Posting, PostingList};
    use crate::write::indexer::tree::test_utils::{
        IndexerOpTestHarness, IndexerOpTestHarnessBuilder,
    };
    use std::collections::{HashMap, HashSet};

    const DIMS: usize = 3;
    const CENTROID_ID_NUM: u64 = 1;

    fn centroid_id(id: u64) -> VectorId {
        VectorId::centroid_id(1, id)
    }

    fn create_opts() -> Arc<IndexerOpts> {
        Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            root_threshold_vectors: usize::MAX,
            merge_threshold_vectors: 0,
            split_threshold_vectors: usize::MAX,
            split_search_neighbourhood: 4,
            indexed_fields: HashSet::from(["category".to_string()]),
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
            Field::new("vector", FieldValue::Vector(values)),
            Field::new("category", FieldValue::String(category.to_string())),
            Field::new(
                "description",
                FieldValue::String(format!("{id} description")),
            ),
        ];
        VectorDataValue::new(id, fields)
    }

    async fn make_single_centroid_harness() -> IndexerOpTestHarness {
        IndexerOpTestHarnessBuilder::new(vec![
            MetadataFieldSpec::new("category", FieldType::String, true),
            MetadataFieldSpec::new("description", FieldType::String, false),
        ])
        .with_leaf_centroid(centroid_id(CENTROID_ID_NUM), vec![0.0; DIMS], vec![])
        .build(DIMS)
        .await
    }

    #[tokio::test]
    async fn should_update_delta_correctly_from_write_vectors() {
        // given
        let mut h = make_single_centroid_harness().await;
        let opts = create_opts();
        let writes = vec![
            make_write("a", vec![1.0, 0.0, 0.0], "shoes"),
            make_write("b", vec![0.0, 1.0, 0.0], "shoes"),
            make_write("c", vec![0.0, 0.0, 1.0], "boots"),
        ];

        // when
        h.write_and_apply(&opts, writes).await;

        // then
        let expected = vec![
            ("a", vec![1.0, 0.0, 0.0], "shoes"),
            ("b", vec![0.0, 1.0, 0.0], "shoes"),
            ("c", vec![0.0, 0.0, 1.0], "boots"),
        ];
        let mut expected_postings: HashMap<VectorId, Vec<f32>> = HashMap::new();
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
        let posting = PostingList::from_value(
            h.storage
                .get_posting_list(centroid_id(CENTROID_ID_NUM), DIMS)
                .await
                .unwrap(),
        );
        assert_eq!(posting.len(), 3);
        for p in posting.iter() {
            let posting_id = p.id();
            let expected_vec = expected_postings
                .get(&posting_id)
                .unwrap_or_else(|| panic!("unexpected vector id {} in posting list", posting_id));
            assert_eq!(p.vector(), expected_vec.as_slice());
        }
        let shoes_bitmap = h
            .storage
            .get_metadata_index("category", FieldValue::String("shoes".to_string()))
            .await
            .unwrap();
        assert_eq!(shoes_bitmap.len(), 2);
        assert!(shoes_bitmap.contains(internal_ids["a"].id()));
        assert!(shoes_bitmap.contains(internal_ids["b"].id()));
        let boots_bitmap = h
            .storage
            .get_metadata_index("category", FieldValue::String("boots".to_string()))
            .await
            .unwrap();
        assert_eq!(boots_bitmap.len(), 1);
        assert!(boots_bitmap.contains(internal_ids["c"].id()));
        for id in ["a", "b", "c"] {
            let bitmap = h
                .storage
                .get_metadata_index(
                    "description",
                    FieldValue::String(format!("{id} description")),
                )
                .await
                .unwrap();
            assert!(bitmap.is_empty());
        }
    }

    #[tokio::test]
    async fn should_update_delta_correctly_from_write_vectors_on_update() {
        // given
        let mut h = make_single_centroid_harness().await;
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

        // when
        h.write_and_apply(
            &opts,
            vec![
                make_write("a", vec![2.0, 0.0, 0.0], "shoes"),
                make_write("b", vec![0.0, 2.0, 0.0], "shoes"),
            ],
        )
        .await;

        // then
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
        // given
        let mut h = make_single_centroid_harness().await;
        let opts = create_opts();
        let writes = vec![
            make_write("x", vec![1.0, 0.0, 0.0], "shoes"),
            make_write("x", vec![2.0, 0.0, 0.0], "clothes"),
            make_write("x", vec![3.0, 0.0, 0.0], "furniture"),
        ];

        // when
        h.write_and_apply(&opts, writes).await;

        // then
        let internal_id = h.storage.lookup_internal_id("x").await.unwrap().unwrap();
        let data = h
            .storage
            .get_vector_data(internal_id, DIMS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(data, make_data("x", vec![3.0, 0.0, 0.0], "furniture"));
        let posting = PostingList::from_value(
            h.storage
                .get_posting_list(centroid_id(CENTROID_ID_NUM), DIMS)
                .await
                .unwrap(),
        );
        assert_eq!(posting.len(), 1);
    }

    #[tokio::test]
    async fn should_reassign_vectors_after_split() {
        // given
        let mut h = make_single_centroid_harness().await;
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
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let old_centroid = centroid_id(CENTROID_ID_NUM);
        delta.search_index.delete_centroids(vec![old_centroid]);
        delta.search_index.remove_from_root(old_centroid);
        let (c_a, c_a_info) =
            delta
                .search_index
                .add_centroid(level, vec![1.0, 0.0, 0.0], ROOT_VECTOR_ID);
        delta.search_index.add_to_root(c_a, c_a_info.vector.clone());
        let (c_b, c_b_info) =
            delta
                .search_index
                .add_centroid(level, vec![0.0, 1.0, 0.0], ROOT_VECTOR_ID);
        delta.search_index.add_to_root(c_b, c_b_info.vector.clone());

        let reassignments = vec![
            ReassignVector::new(id_a, vec![0.9, 0.1, 0.0], old_centroid, level),
            ReassignVector::new(id_b, vec![0.1, 0.9, 0.0], old_centroid, level),
            ReassignVector::new(id_c, vec![0.2, 0.8, 0.0], old_centroid, level),
        ];

        // when
        let rv = ReassignVectors::new(&opts, &snapshot, 1, reassignments, level);
        rv.execute(&h.state, &mut delta).await.unwrap();
        h.apply_delta(delta).await;

        // then
        let posting_a =
            PostingList::from_value(h.storage.get_posting_list(c_a, DIMS).await.unwrap());
        let ids_a: HashSet<VectorId> = posting_a.iter().map(|p: &Posting| p.id()).collect();
        assert!(ids_a.contains(&id_a));
        assert!(!ids_a.contains(&id_b));
        assert!(!ids_a.contains(&id_c));
        let posting_b =
            PostingList::from_value(h.storage.get_posting_list(c_b, DIMS).await.unwrap());
        let ids_b: HashSet<VectorId> = posting_b.iter().map(|p: &Posting| p.id()).collect();
        assert!(ids_b.contains(&id_b));
        assert!(ids_b.contains(&id_c));
        assert!(!ids_b.contains(&id_a));
    }

    #[tokio::test]
    async fn should_skip_reassignment_when_already_at_closest_centroid() {
        // given
        let mut h = make_single_centroid_harness().await;
        let opts = create_opts();
        h.write_and_apply(&opts, vec![make_write("a", vec![0.9, 0.1, 0.0], "shoes")])
            .await;
        let id_a = h.storage.lookup_internal_id("a").await.unwrap().unwrap();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let reassignments = vec![ReassignVector::new(
            id_a,
            vec![0.9, 0.1, 0.0],
            centroid_id(CENTROID_ID_NUM),
            level,
        )];

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let rv = ReassignVectors::new(&opts, &snapshot, 1, reassignments, level);
        rv.execute(&h.state, &mut delta).await.unwrap();
        let ops = delta.freeze(1, &mut h.state);

        // then
        let posting_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, common::storage::RecordOp::Merge(_)))
            .collect();
        assert!(posting_ops.is_empty());
    }

    #[tokio::test]
    async fn should_use_delta_posting_as_true_current_centroid() {
        // given
        let mut h = make_single_centroid_harness().await;
        let opts = create_opts();
        h.write_and_apply(&opts, vec![make_write("a", vec![0.9, 0.1, 0.0], "shoes")])
            .await;
        let id_a = h.storage.lookup_internal_id("a").await.unwrap().unwrap();
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let old_centroid = centroid_id(CENTROID_ID_NUM);
        delta.search_index.delete_centroids(vec![old_centroid]);
        delta.search_index.remove_from_root(old_centroid);
        let (c0, c0_info) =
            delta
                .search_index
                .add_centroid(level, vec![1.0, 0.0, 0.0], ROOT_VECTOR_ID);
        delta.search_index.add_to_root(c0, c0_info.vector.clone());
        let (c1, c1_info) =
            delta
                .search_index
                .add_centroid(level, vec![0.0, 1.0, 0.0], ROOT_VECTOR_ID);
        delta.search_index.add_to_root(c1, c1_info.vector.clone());
        delta
            .search_index
            .add_to_posting(c0, id_a, vec![0.9, 0.1, 0.0]);

        let reassignments = vec![ReassignVector::new(
            id_a,
            vec![0.9, 0.1, 0.0],
            old_centroid,
            level,
        )];

        // when
        let rv = ReassignVectors::new(&opts, &snapshot, 1, reassignments, level);
        rv.execute(&h.state, &mut delta).await.unwrap();
        h.apply_delta(delta).await;

        // then
        let posting_c0 =
            PostingList::from_value(h.storage.get_posting_list(c0, DIMS).await.unwrap());
        let ids_c0: HashSet<VectorId> = posting_c0.iter().map(|p: &Posting| p.id()).collect();
        assert!(ids_c0.contains(&id_a));
        let posting_c1 =
            PostingList::from_value(h.storage.get_posting_list(c1, DIMS).await.unwrap());
        let ids_c1: HashSet<VectorId> = posting_c1.iter().map(|p: &Posting| p.id()).collect();
        assert!(!ids_c1.contains(&id_a));
    }
}

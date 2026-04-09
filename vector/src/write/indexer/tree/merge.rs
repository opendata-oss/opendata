use crate::Result;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::vector_id::VectorId;
use crate::write::indexer::drivers::AsyncBatchDriver;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::centroids::TreeLevel;
use crate::write::indexer::tree::posting_list::PostingList;
use crate::write::indexer::tree::split::ReassignVector;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use common::StorageRead;
use futures::future::BoxFuture;
use std::sync::Arc;

struct MergeCentroid {
    c: VectorId,
    c_info: CentroidInfoValue,
    postings: Arc<PostingList>,
}

pub(crate) struct MergeCentroids {
    opts: Arc<IndexerOpts>,
    level: TreeLevel,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
}

impl MergeCentroids {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        level: TreeLevel,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Self {
        Self {
            opts: opts.clone(),
            level,
            snapshot: snapshot.clone(),
            snapshot_epoch,
        }
    }

    /// Returns (reassignment_vectors, merge_count).
    pub(crate) async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<(Vec<ReassignVector>, usize)> {
        let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);

        // find all centroids that need to be merged at the specified level
        let counts = view.centroid_counts(self.level);
        let to_merge = counts
            .iter()
            .filter(|(_k, v)| **v < self.opts.merge_threshold_vectors as u64)
            .map(|(&k, _v)| k)
            .collect::<Vec<_>>();
        if to_merge.is_empty() {
            return Ok((vec![], 0));
        }
        // Don't merge if all centroids would be merged — there are no targets left
        if to_merge.len() >= counts.len() {
            return Ok((vec![], 0));
        }

        // read postings of all mergees
        let mut to_resolve = Vec::with_capacity(to_merge.len());
        for c in to_merge {
            let posting_fut = view.posting_list(c, self.opts.dimensions);
            let c_info = view
                .centroid(c)
                .unwrap_or_else(|| {
                    panic!("merge@{}: unexpected missing centroid {}", self.level, c)
                })
                .clone();
            to_resolve.push(Box::pin(async move {
                Ok(MergeCentroid {
                    c,
                    c_info,
                    postings: posting_fut.get().await?,
                })
            }) as BoxFuture<'static, Result<MergeCentroid>>)
        }
        let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
        let mut resolved = Vec::with_capacity(resolve_results.len());
        for r in resolve_results {
            resolved.push(r?);
        }

        // execute merges
        let merge_count = resolved.len();
        let total_moved = resolved.iter().map(|m| m.postings.len()).sum();
        let mut reassignments = Vec::with_capacity(total_moved);
        for merge in resolved {
            delta.search_index.delete_centroids(vec![merge.c]);
            if merge.c_info.parent_vector_id.is_centroid() {
                assert_eq!(
                    self.level.next_level_up().level(),
                    merge.c_info.parent_vector_id.level(),
                    "unexpected parent for c({}): {}",
                    merge.c,
                    merge.c_info.parent_vector_id
                );
                delta
                    .search_index
                    .remove_from_posting(merge.c_info.parent_vector_id, merge.c);
            } else {
                assert!(merge.c_info.parent_vector_id.is_root());
                assert!(
                    self.level.next_level_up().is_root(),
                    "unexpected root parent for c({})",
                    merge.c,
                );
                delta.search_index.remove_from_root(merge.c);
            }
            for p in merge.postings.iter() {
                reassignments.push(ReassignVector::new(
                    p.id(),
                    p.vector().to_vec(),
                    merge.c,
                    self.level,
                ));
            }
        }

        // return reassign set
        Ok((reassignments, merge_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::vector_id::{ROOT_LEVEL, ROOT_VECTOR_ID};
    use crate::storage::VectorDbStorageReadExt;
    use crate::write::indexer::tree::centroids::{TreeDepth, TreeLevel};
    use crate::write::indexer::tree::posting_list::PostingList;
    use crate::write::indexer::tree::test_utils::{
        IndexerOpTestHarness, IndexerOpTestHarnessBuilder, TestDataVector,
    };
    use std::collections::HashSet;

    const DIMS: usize = 2;
    const MERGE_THRESHOLD: usize = 3;

    const CENTROID_A: VectorId = VectorId::centroid_id(1, 100);
    const CENTROID_B: VectorId = VectorId::centroid_id(1, 101);
    const CENTROID_C: VectorId = VectorId::centroid_id(1, 102);
    const CENTROID_D: VectorId = VectorId::centroid_id(1, 103);
    const L2_CENTROID_A: VectorId = VectorId::centroid_id(2, 200);
    const L2_CENTROID_B: VectorId = VectorId::centroid_id(2, 201);

    fn create_opts() -> Arc<IndexerOpts> {
        Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            root_threshold_vectors: usize::MAX,
            merge_threshold_vectors: MERGE_THRESHOLD,
            split_threshold_vectors: usize::MAX,
            split_search_neighbourhood: 4,
            indexed_fields: HashSet::new(),
        })
    }

    async fn make_one_level_tree(
        centroids: Vec<(VectorId, Vec<f32>, Vec<TestDataVector>)>,
    ) -> IndexerOpTestHarness {
        let mut builder = IndexerOpTestHarnessBuilder::new(vec![]);
        for (id, vector, data_vectors) in centroids {
            builder = builder.with_leaf_centroid(id, vector, data_vectors);
        }
        builder.build(DIMS).await
    }

    async fn make_multilevel_tree() -> IndexerOpTestHarness {
        IndexerOpTestHarnessBuilder::new(vec![])
            .with_leaf_centroid(
                CENTROID_A,
                vec![1.0, 0.0],
                vec![TestDataVector::new("a1", vec![0.9, 0.1], vec![])],
            )
            .with_leaf_centroid(
                CENTROID_B,
                vec![0.0, 1.0],
                vec![
                    TestDataVector::new("b1", vec![0.1, 0.9], vec![]),
                    TestDataVector::new("b2", vec![0.2, 0.8], vec![]),
                ],
            )
            .with_leaf_centroid(
                CENTROID_C,
                vec![-1.0, 0.0],
                vec![
                    TestDataVector::new("c1", vec![-0.9, 0.1], vec![]),
                    TestDataVector::new("c2", vec![-0.8, 0.2], vec![]),
                    TestDataVector::new("c3", vec![-0.95, 0.05], vec![]),
                    TestDataVector::new("c4", vec![-0.85, 0.15], vec![]),
                ],
            )
            .with_leaf_centroid(
                CENTROID_D,
                vec![0.0, -1.0],
                vec![
                    TestDataVector::new("d1", vec![0.0, -0.95], vec![]),
                    TestDataVector::new("d2", vec![0.05, -0.85], vec![]),
                    TestDataVector::new("d3", vec![0.75, -0.1], vec![]),
                    TestDataVector::new("d4", vec![0.85, 0.0], vec![]),
                    TestDataVector::new("d5", vec![0.95, 0.1], vec![]),
                ],
            )
            .with_centroid(
                L2_CENTROID_A,
                vec![1.0, 1.0],
                vec![CENTROID_A, CENTROID_B, CENTROID_C],
            )
            .with_centroid(L2_CENTROID_B, vec![-1.0, -1.0], vec![CENTROID_D])
            .build(DIMS)
            .await
    }

    async fn make_multilevel_tree_with_all_leaf_centroids_below_threshold() -> IndexerOpTestHarness
    {
        IndexerOpTestHarnessBuilder::new(vec![])
            .with_leaf_centroid(
                CENTROID_A,
                vec![1.0, 0.0],
                vec![TestDataVector::new("a1", vec![0.9, 0.1], vec![])],
            )
            .with_leaf_centroid(
                CENTROID_B,
                vec![0.0, 1.0],
                vec![
                    TestDataVector::new("b1", vec![0.1, 0.9], vec![]),
                    TestDataVector::new("b2", vec![0.2, 0.8], vec![]),
                ],
            )
            .with_leaf_centroid(
                CENTROID_C,
                vec![-1.0, 0.0],
                vec![
                    TestDataVector::new("c1", vec![-0.9, 0.1], vec![]),
                    TestDataVector::new("c2", vec![-0.8, 0.2], vec![]),
                ],
            )
            .with_leaf_centroid(
                CENTROID_D,
                vec![0.0, -1.0],
                vec![TestDataVector::new("d1", vec![0.0, -0.95], vec![])],
            )
            .with_centroid(
                L2_CENTROID_A,
                vec![1.0, 1.0],
                vec![CENTROID_A, CENTROID_B, CENTROID_C],
            )
            .with_centroid(L2_CENTROID_B, vec![-1.0, -1.0], vec![CENTROID_D])
            .build(DIMS)
            .await
    }

    #[tokio::test]
    async fn should_merge_centroids_below_threshold_when_parent_is_root() {
        // given
        let mut h = make_one_level_tree(vec![
            (
                CENTROID_A,
                vec![1.0, 0.0],
                vec![TestDataVector::new("a1", vec![0.9, 0.1], vec![])],
            ),
            (
                CENTROID_B,
                vec![0.0, 1.0],
                vec![
                    TestDataVector::new("b1", vec![0.1, 0.9], vec![]),
                    TestDataVector::new("b2", vec![0.2, 0.8], vec![]),
                ],
            ),
            (
                CENTROID_C,
                vec![-1.0, 0.0],
                vec![
                    TestDataVector::new("c1", vec![-0.9, 0.1], vec![]),
                    TestDataVector::new("c2", vec![-0.8, 0.2], vec![]),
                    TestDataVector::new("c3", vec![-0.95, 0.05], vec![]),
                    TestDataVector::new("c4", vec![-0.85, 0.15], vec![]),
                ],
            ),
        ])
        .await;
        let opts = create_opts();
        let id_a1 = h.storage.lookup_internal_id("a1").await.unwrap().unwrap();
        let id_b1 = h.storage.lookup_internal_id("b1").await.unwrap().unwrap();
        let id_b2 = h.storage.lookup_internal_id("b2").await.unwrap().unwrap();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let merge = MergeCentroids::new(&opts, level, &snapshot, 1);
        let (reassignments, merge_count) = merge.execute(&h.state, &mut delta).await.unwrap();
        h.apply_delta(delta).await;

        // then
        assert_eq!(merge_count, 2);
        assert!(
            h.storage
                .get_centroid_info(CENTROID_A)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            h.storage
                .get_centroid_info(CENTROID_B)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            h.storage
                .get_centroid_info(CENTROID_C)
                .await
                .unwrap()
                .is_some()
        );
        // assert centroids removed from root
        let root = PostingList::from_value(h.storage.get_root_posting_list(DIMS).await.unwrap());
        let root_ids: HashSet<_> = root.iter().map(|p| p.id()).collect();
        assert_eq!(root_ids, HashSet::from([CENTROID_C]));
        // assert all vectors added to reassign set
        let reassign_ids: HashSet<VectorId> = reassignments.iter().map(|r| r.vector_id).collect();
        assert_eq!(reassign_ids.len(), 3);
        assert!(reassign_ids.contains(&id_a1));
        assert!(reassign_ids.contains(&id_b1));
        assert!(reassign_ids.contains(&id_b2));
        let a1_r = reassignments.iter().find(|r| r.vector_id == id_a1).unwrap();
        assert_eq!(a1_r.current_centroid, CENTROID_A);
        assert_eq!(a1_r.level, level);
        let b1_r = reassignments.iter().find(|r| r.vector_id == id_b1).unwrap();
        assert_eq!(b1_r.current_centroid, CENTROID_B);
        let b2_r = reassignments.iter().find(|r| r.vector_id == id_b2).unwrap();
        assert_eq!(b2_r.current_centroid, CENTROID_B);
        assert_eq!(ROOT_VECTOR_ID.level(), 0xFF);
    }

    #[tokio::test]
    async fn should_not_merge_when_all_centroids_below_threshold() {
        // given
        let h = make_one_level_tree(vec![
            (
                CENTROID_A,
                vec![1.0, 0.0],
                vec![TestDataVector::new("a1", vec![0.9, 0.1], vec![])],
            ),
            (
                CENTROID_B,
                vec![0.0, 1.0],
                vec![TestDataVector::new("b1", vec![0.1, 0.9], vec![])],
            ),
        ])
        .await;
        let opts = create_opts();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let merge = MergeCentroids::new(&opts, level, &snapshot, 1);
        let (reassignments, merge_count) = merge.execute(&h.state, &mut delta).await.unwrap();

        // then
        assert_eq!(merge_count, 0);
        assert!(reassignments.is_empty());
    }

    #[tokio::test]
    async fn should_not_merge_centroids_at_or_above_threshold() {
        // given
        let h = make_one_level_tree(vec![
            (
                CENTROID_A,
                vec![1.0, 0.0],
                vec![
                    TestDataVector::new("a1", vec![0.9, 0.1], vec![]),
                    TestDataVector::new("a2", vec![0.8, 0.2], vec![]),
                    TestDataVector::new("a3", vec![0.95, 0.05], vec![]),
                ],
            ),
            (
                CENTROID_B,
                vec![0.0, 1.0],
                vec![
                    TestDataVector::new("b1", vec![0.1, 0.9], vec![]),
                    TestDataVector::new("b2", vec![0.2, 0.8], vec![]),
                    TestDataVector::new("b3", vec![0.05, 0.95], vec![]),
                ],
            ),
        ])
        .await;
        let opts = create_opts();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let merge = MergeCentroids::new(&opts, level, &snapshot, 1);
        let (reassignments, merge_count) = merge.execute(&h.state, &mut delta).await.unwrap();

        // then
        assert_eq!(merge_count, 0);
        assert!(reassignments.is_empty());
    }

    #[tokio::test]
    async fn should_not_remove_all_centroids_from_level() {
        // given
        let mut h = make_multilevel_tree_with_all_leaf_centroids_below_threshold().await;
        let opts = create_opts();
        let level = TreeLevel::inner(1, TreeDepth::of(4));

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let (reassignments, merge_count) = MergeCentroids::new(&opts, level, &snapshot, 1)
            .execute(&h.state, &mut delta)
            .await
            .unwrap();
        h.apply_delta(delta).await;

        // then
        assert_eq!(merge_count, 0);
        assert!(reassignments.is_empty());
        assert!(
            h.storage
                .get_centroid_info(CENTROID_A)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            h.storage
                .get_centroid_info(CENTROID_B)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            h.storage
                .get_centroid_info(CENTROID_C)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            h.storage
                .get_centroid_info(CENTROID_D)
                .await
                .unwrap()
                .is_some()
        );
        let parent_a_posting = PostingList::from_value(
            h.storage
                .get_posting_list(L2_CENTROID_A, DIMS)
                .await
                .unwrap(),
        );
        let parent_b_posting = PostingList::from_value(
            h.storage
                .get_posting_list(L2_CENTROID_B, DIMS)
                .await
                .unwrap(),
        );
        let parent_a_ids: HashSet<_> = parent_a_posting.iter().map(|p| p.id()).collect();
        let parent_b_ids: HashSet<_> = parent_b_posting.iter().map(|p| p.id()).collect();
        assert_eq!(
            parent_a_ids,
            HashSet::from([CENTROID_A, CENTROID_B, CENTROID_C])
        );
        assert_eq!(parent_b_ids, HashSet::from([CENTROID_D]));
    }

    #[tokio::test]
    async fn should_remove_centroids_from_parent_when_parent_is_inner_level() {
        // given
        let parent = L2_CENTROID_A;
        let c = CENTROID_C;
        let mut h = make_multilevel_tree().await;
        let opts = create_opts();
        let level = TreeLevel::inner(1, TreeDepth::of(4));

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let (reassignments, merge_count) = MergeCentroids::new(&opts, level, &snapshot, 1)
            .execute(&h.state, &mut delta)
            .await
            .unwrap();
        h.apply_delta(delta).await;

        // then
        assert_eq!(merge_count, 2);
        assert_eq!(reassignments.len(), 3);
        let parent_posting =
            PostingList::from_value(h.storage.get_posting_list(parent, DIMS).await.unwrap());
        let parent_ids: HashSet<_> = parent_posting.iter().map(|p| p.id()).collect();
        assert_eq!(parent_ids, HashSet::from([c]));
    }

    #[tokio::test]
    async fn should_only_merge_centroids_from_level() {
        // given
        let mut h = make_multilevel_tree().await;
        let opts = create_opts();
        let level = TreeLevel::inner(1, TreeDepth::of(4));

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let (reassignments, merge_count) = MergeCentroids::new(&opts, level, &snapshot, 1)
            .execute(&h.state, &mut delta)
            .await
            .unwrap();
        h.apply_delta(delta).await;

        // then
        assert_eq!(merge_count, 2);
        assert_eq!(reassignments.len(), 3);
        assert!(
            h.storage
                .get_centroid_info(CENTROID_A)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            h.storage
                .get_centroid_info(CENTROID_B)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            h.storage
                .get_centroid_info(L2_CENTROID_A)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            h.storage
                .get_centroid_info(L2_CENTROID_B)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            h.storage
                .get_centroid_info(CENTROID_C)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            h.storage
                .get_centroid_info(CENTROID_D)
                .await
                .unwrap()
                .is_some()
        );
        let root = PostingList::from_value(h.storage.get_root_posting_list(DIMS).await.unwrap());
        let root_ids: HashSet<_> = root.iter().map(|p| p.id()).collect();
        assert_eq!(root_ids, HashSet::from([L2_CENTROID_A, L2_CENTROID_B]));
        assert_eq!(ROOT_LEVEL, 0xFF);
    }
}

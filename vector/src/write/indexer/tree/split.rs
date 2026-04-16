use crate::math::kmeans::Clustering;
use crate::math::{distance, heuristics, kmeans};
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::vector_id::VectorId;
use crate::write::indexer::drivers::AsyncBatchDriver;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::centroids::{TreeLevel, batch_search_centroids_in_level};
use crate::write::indexer::tree::posting_list::{Posting, PostingList};
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::{DistanceMetric, Result};
use common::StorageRead;
use futures::future::BoxFuture;
use log::debug;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::task::spawn_blocking;

const MAX_SPLITS: usize = usize::MAX;

#[derive(Clone, Debug)]
pub(crate) struct ReassignVector {
    pub(crate) vector_id: VectorId,
    pub(crate) vector: Vec<f32>,
    pub(crate) current_centroid: VectorId,
    // the level of the centroids that this should be reassigned from/to
    pub(crate) level: TreeLevel,
}

impl ReassignVector {
    pub(crate) fn new(
        vector_id: VectorId,
        vector: Vec<f32>,
        current_centroid: VectorId,
        level: TreeLevel,
    ) -> Self {
        assert_eq!(vector_id.level() + 1, current_centroid.level());
        assert_eq!(level.level(), current_centroid.level());
        Self {
            vector_id,
            vector,
            current_centroid,
            level,
        }
    }
}

pub(crate) struct SplitPostings {
    centroid_vec: Vec<f32>,
    postings: PostingList,
}

impl SplitPostings {
    pub(crate) fn new(centroid_vec: Vec<f32>, postings: PostingList) -> Self {
        Self {
            centroid_vec,
            postings,
        }
    }

    pub(crate) fn centroid_vec(&self) -> &[f32] {
        &self.centroid_vec
    }

    pub(crate) fn postings(self) -> PostingList {
        self.postings
    }
}

struct SplitResult {
    c: VectorId,
    c_info: CentroidInfoValue,
    c_0: SplitPostings,
    c_1: SplitPostings,
    reassign_vectors: Vec<ReassignVector>,
    candidates_evaluated: usize,
    candidates_returned: usize,
}

#[derive(Debug)]
pub(crate) enum SplitError {
    ImbalancedClusters { c: VectorId, count: u64 },
}

#[derive(Debug)]
pub(crate) struct SplitSummary {
    #[allow(dead_code)]
    pub(crate) c: VectorId,
    #[allow(dead_code)]
    pub(crate) new_centroids: Vec<(VectorId, CentroidInfoValue)>,
}

#[derive(Debug)]
pub(crate) struct SplitCentroidsResult {
    pub(crate) splits: Vec<SplitSummary>,
    pub(crate) imbalanced: Vec<(VectorId, u64)>,
    pub(crate) reassignments: Vec<ReassignVector>,
    pub(crate) candidates_evaluated: usize,
    pub(crate) candidates_returned: usize,
}

pub(crate) struct SplitCentroids {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
    level: TreeLevel,
    max_splits: usize,
    clustering: fn(distance: DistanceMetric) -> Box<dyn Clustering + Send>,
}

impl SplitCentroids {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        level: TreeLevel,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Self {
        Self::new_with_max_splits_and_clustering(
            opts,
            level,
            snapshot,
            snapshot_epoch,
            MAX_SPLITS,
            kmeans::for_metric,
        )
    }

    fn new_with_max_splits_and_clustering(
        opts: &Arc<IndexerOpts>,
        level: TreeLevel,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        max_splits: usize,
        clustering: fn(distance_metric: DistanceMetric) -> Box<dyn Clustering + Send>,
    ) -> Self {
        Self {
            opts: opts.clone(),
            level,
            snapshot: snapshot.clone(),
            snapshot_epoch,
            max_splits,
            clustering,
        }
    }

    pub(crate) async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<SplitCentroidsResult> {
        let (splits, postings) = {
            let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);

            // compute the centroids that need to be split, biggest first, capped at MAX_SPLITS
            let counts = view.centroid_counts(self.level);
            let mut to_split: Vec<_> = counts
                .into_iter()
                .filter(|(_k, v)| *v >= self.opts.split_threshold_vectors as u64)
                .collect();
            to_split.sort_by_key(|b| std::cmp::Reverse(b.1));
            to_split.truncate(self.max_splits);
            if to_split.is_empty() {
                return Ok(SplitCentroidsResult {
                    splits: vec![],
                    imbalanced: Vec::new(),
                    reassignments: Vec::new(),
                    candidates_evaluated: 0,
                    candidates_returned: 0,
                });
            }
            // initialize the set of postings to fetch with the split centroids
            let mut postings_to_retrive =
                HashSet::with_capacity(to_split.len() * (1 + self.opts.split_search_neighbourhood));
            postings_to_retrive.extend(to_split.iter().map(|(c, _count)| *c));

            // resolve centroids to full info
            let to_split: Vec<_> = to_split
                .into_iter()
                .map(|(c, count)| {
                    (
                        c,
                        count,
                        view.centroid(c)
                            .unwrap_or_else(|| {
                                panic!("unexpected missing centroid {} at level: {}", c, self.level)
                            })
                            .clone(),
                    )
                })
                .collect();

            // collect each centroids neighbours
            let centroid_index =
                view.centroid_index(self.opts.dimensions, self.opts.distance_metric);
            let neighbours_by_centroid = if self.opts.split_search_neighbourhood > 0 {
                batch_search_centroids_in_level(
                    &centroid_index,
                    self.opts.split_search_neighbourhood + 1,
                    to_split
                        .iter()
                        .map(|(c, _count, c_info)| (*c, c_info.vector.as_slice()))
                        .collect(),
                    self.level,
                )
                .await?
            } else {
                HashMap::new()
            };

            // initialize split tasks
            let mut splits = Vec::with_capacity(to_split.len());
            for (c, count, c_info) in to_split {
                let neighbours = neighbours_by_centroid
                    .get(&c)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|posting| posting.id())
                    .collect::<Vec<_>>();
                postings_to_retrive.extend(neighbours.clone());
                splits.push(SplitCentroid {
                    c,
                    count,
                    c_info,
                    neighbours,
                    dimensions: self.opts.dimensions,
                    distance_metric: self.opts.distance_metric,
                    level: self.level,
                    clustering: (self.clustering)(self.opts.distance_metric),
                })
            }

            // find all relevant postings (centroids and neighbours)
            let mut posting_reads = Vec::with_capacity(postings_to_retrive.len());
            for c in postings_to_retrive {
                let read_fut = view.posting_list(c, self.opts.dimensions);
                posting_reads.push(
                    Box::pin(async move { read_fut.get().await.map(|p| (c, p)) })
                        as BoxFuture<'static, Result<(VectorId, Arc<PostingList>)>>,
                );
            }
            let results = AsyncBatchDriver::execute(posting_reads).await;
            let mut postings = HashMap::new();
            for r in results {
                let (c, c_postings) = r?;
                postings.insert(c, c_postings);
            }
            let postings = Arc::new(postings);
            (splits, postings)
        };

        // execute splits. spawn a blocking task to avoid tying up runtime as this is compute heavy
        let results: Vec<_> = spawn_blocking(move || {
            splits
                .into_par_iter()
                .map(|split| split.execute(postings.clone()))
                .collect()
        })
        .await
        .expect("unexpected join error");

        let indexed_data_by_vector = if self.level.is_leaf() {
            let vector_ids: HashSet<_> = results
                .iter()
                .filter_map(|result| result.as_ref().ok())
                .flat_map(|result| {
                    result
                        .c_0
                        .postings
                        .iter()
                        .chain(result.c_1.postings.iter())
                        .map(|p| p.id())
                        .collect::<Vec<_>>()
                })
                .collect();
            let view = VectorIndexView::new(delta, state, &self.snapshot, self.snapshot_epoch);
            let mut reads = Vec::with_capacity(vector_ids.len());
            for vector_id in vector_ids {
                let fut = view.vector_index_data(vector_id);
                reads.push(Box::pin(async move {
                    Ok((
                        vector_id,
                        fut.await?.unwrap_or_else(|| {
                            panic!("missing vector index data for {}", vector_id)
                        }),
                    ))
                })
                    as BoxFuture<
                        'static,
                        Result<(
                            VectorId,
                            crate::serde::vector_index_data::VectorIndexDataValue,
                        )>,
                    >);
            }
            let results = AsyncBatchDriver::execute(reads).await;
            let mut indexed_data = HashMap::with_capacity(results.len());
            for result in results {
                let (vector_id, index_data) = result?;
                indexed_data.insert(vector_id, index_data);
            }
            indexed_data
        } else {
            HashMap::new()
        };

        // update delta
        let mut total_candidates_evaluated = 0usize;
        let mut total_candidates_returned = 0usize;
        let mut imbalanced = Vec::new();
        let mut reassignments = HashMap::new();
        let mut splits = Vec::with_capacity(results.len());
        for result in results {
            let result = match result {
                Ok(result) => result,
                Err(SplitError::ImbalancedClusters { c, count }) => {
                    imbalanced.push((c, count));
                    continue;
                }
            };
            total_candidates_evaluated += result.candidates_evaluated;
            total_candidates_returned += result.candidates_returned;
            // track reassignments
            reassignments.extend(
                result
                    .reassign_vectors
                    .into_iter()
                    .map(|r| (r.vector_id, r)),
            );

            // delete old centroid and its posting entries
            delta.search_index.delete_centroids(vec![result.c]);
            if result.c_info.parent_vector_id.is_centroid() {
                assert_eq!(
                    self.level.next_level_up().level(),
                    result.c_info.parent_vector_id.level()
                );
                assert!(!self.level.next_level_up().is_root());
                delta
                    .search_index
                    .remove_from_posting(result.c_info.parent_vector_id, result.c);
            } else {
                result.c_info.parent_vector_id.is_root();
                assert!(self.level.next_level_up().is_root());
                delta.search_index.remove_from_root(result.c);
            }

            // create new centroids with postings
            let mut new_centroids = Vec::with_capacity(2);
            for new_centroid in [result.c_0, result.c_1] {
                // add it to the old centroid's parent for now, and then reassign if parent not root
                let (c_id, entry) = delta.search_index.add_centroid(
                    self.level,
                    new_centroid.centroid_vec().to_vec(),
                    result.c_info.parent_vector_id,
                );
                if result.c_info.parent_vector_id.is_centroid() {
                    delta.search_index.add_to_posting(
                        result.c_info.parent_vector_id,
                        c_id,
                        entry.vector.clone(),
                    );
                    reassignments.insert(
                        c_id,
                        ReassignVector::new(
                            c_id,
                            entry.vector.clone(),
                            result.c_info.parent_vector_id,
                            self.level.next_level_up(),
                        ),
                    );
                } else {
                    delta.search_index.add_to_root(c_id, entry.vector.clone());
                }
                // add all posting entries for the new centroid
                for p in new_centroid.postings() {
                    delta
                        .search_index
                        .add_to_posting(c_id, p.id(), p.vector().to_vec());
                    if self.level.is_leaf() {
                        let index_data = indexed_data_by_vector.get(&p.id()).unwrap_or_else(|| {
                            panic!("missing prefetched vector index data for {}", p.id())
                        });
                        delta.forward_index.update_vector_index_data(
                            p.id(),
                            vec![c_id],
                            index_data.indexed_fields.clone(),
                        );
                    } else {
                        // if this is a non-leaf level, then the posting vectors are centroids,
                        // and their parent ref needs to be updated
                        delta.search_index.update_centroid(
                            p.id(),
                            CentroidInfoValue::new(
                                self.level.next_level_down().level(),
                                p.vector().to_vec(),
                                c_id,
                            ),
                        )
                    }
                }
                new_centroids.push((c_id, entry));
            }
            debug!(
                "split: delete centroid {}/{}, add new centroids: {:?}",
                self.level,
                result.c,
                new_centroids
                    .iter()
                    .map(|(c_id, c)| (c.level, *c_id))
                    .collect::<Vec<_>>(),
            );
            splits.push(SplitSummary {
                c: result.c,
                new_centroids,
            })
        }

        // return reassign set
        let reassignments: Vec<_> = reassignments.values().cloned().collect();
        Ok(SplitCentroidsResult {
            splits,
            imbalanced,
            reassignments,
            candidates_evaluated: total_candidates_evaluated,
            candidates_returned: total_candidates_returned,
        })
    }
}

struct SplitCentroid {
    c: VectorId,
    count: u64,
    c_info: CentroidInfoValue,
    neighbours: Vec<VectorId>,
    distance_metric: DistanceMetric,
    dimensions: usize,
    level: TreeLevel,
    clustering: Box<dyn Clustering + Send>,
}

impl SplitCentroid {
    fn execute(
        self,
        postings: Arc<HashMap<VectorId, Arc<PostingList>>>,
    ) -> std::result::Result<SplitResult, SplitError> {
        let c_postings = postings
            .get(&self.c)
            .expect("unexpected missing postings for c");
        assert!(
            c_postings.len() >= 2,
            "tried to split centroid {} with less than 2 postings",
            self.c
        );
        let c_vectors: Vec<(VectorId, Vec<f32>)> = c_postings
            .iter()
            .map(|p| (p.id(), p.vector().to_vec()))
            .collect();

        // Run two_means clustering to find new centroids
        let c_vector_refs: Vec<(u64, &[f32])> = c_vectors
            .iter()
            .map(|(id, v)| (id.id(), v.as_slice()))
            .collect();
        // todo: why does 2means need the vector IDs?
        let (c0_vector, c1_vector) = self.clustering.two_means(&c_vector_refs, self.dimensions);
        // Assign each vector to closer centroid
        let mut c0_postings = PostingList::with_capacity(c_vectors.len());
        let mut c1_postings = PostingList::with_capacity(c_vectors.len());
        for (id, vector) in &c_vectors {
            let d0 = distance::compute_distance(vector, &c0_vector, self.distance_metric);
            let d1 = distance::compute_distance(vector, &c1_vector, self.distance_metric);

            if d0 <= d1 {
                c0_postings.push(Posting::new(*id, vector.clone()));
            } else {
                c1_postings.push(Posting::new(*id, vector.clone()));
            }
        }

        if c0_postings.is_empty() || c1_postings.is_empty() {
            return Err(SplitError::ImbalancedClusters {
                c: self.c,
                count: self.count,
            });
        }

        // Compute reassignments
        // Count all candidates evaluated before heuristic filtering
        let split_candidates = c_postings.len();
        let neighbour_candidates: usize = self
            .neighbours
            .iter()
            .filter_map(|n| postings.get(n))
            .map(|p| p.len())
            .sum();
        let candidates_evaluated = split_candidates + neighbour_candidates;

        let mut reassignments = Vec::with_capacity(c0_postings.len() + c1_postings.len());
        reassignments.extend(self.compute_split_reassignments(
            self.c,
            c_postings.as_ref(),
            &self.c_info.vector,
            &c0_vector,
            &c1_vector,
            self.level,
        ));
        reassignments.extend(self.compute_neighbour_reassignments(
            &self.c_info.vector,
            &c0_vector,
            &c1_vector,
            postings.as_ref(),
            self.level,
        ));

        let candidates_returned = reassignments.len();
        Ok(SplitResult {
            c: self.c,
            c_info: self.c_info,
            c_0: SplitPostings::new(c0_vector, c0_postings),
            c_1: SplitPostings::new(c1_vector, c1_postings),
            reassign_vectors: reassignments,
            candidates_evaluated,
            candidates_returned,
        })
    }

    fn compute_split_reassignments(
        &self,
        centroid_id: VectorId,
        postings: &PostingList,
        c_vector: &[f32],
        c0_vector: &[f32],
        c1_vector: &[f32],
        level: TreeLevel,
    ) -> Vec<ReassignVector> {
        // use the heuristic for c's vectors from the spfresh paper to cheaply determine if
        // a vector may need reassignment. If it may, then check in the centroid graph for
        // its nearest centroid. If the nearest centroid is not c0 or c1, then include in
        // the reassignment set.
        let mut reassignments = Vec::with_capacity(postings.len());
        for p in postings.iter() {
            if !heuristics::split_heuristic(
                p.vector(),
                c_vector,
                c0_vector,
                c1_vector,
                self.distance_metric,
            ) {
                continue;
            }
            reassignments.push(ReassignVector::new(
                p.id(),
                p.vector().to_vec(),
                centroid_id,
                level,
            ));
        }
        reassignments
    }

    fn compute_neighbour_reassignments(
        &self,
        c_vector: &[f32],
        c0_vector: &[f32],
        c1_vector: &[f32],
        postings: &HashMap<VectorId, Arc<PostingList>>,
        level: TreeLevel,
    ) -> Vec<ReassignVector> {
        let mut reassignments: Vec<ReassignVector> = Vec::new();

        // Process each neighbour's postings to find reassignments
        for neighbour_id in &self.neighbours {
            let neighbour_postings = postings
                .get(neighbour_id)
                .expect("missing postings for neighbour");

            for p in neighbour_postings.iter() {
                // use the heuristic for neighbour vectors from the spfresh paper to cheaply
                // determine if a vector may need reassignment. If it may, then check in the
                // centroid graph for its nearest centroid. If the nearest centroid is changed,
                // then add to reassignment set.
                if !heuristics::neighbour_split_heuristic(
                    p.vector(),
                    c_vector,
                    c0_vector,
                    c1_vector,
                    self.distance_metric,
                ) {
                    continue;
                }
                let vector = p.vector().to_vec();

                reassignments.push(ReassignVector::new(p.id(), vector, *neighbour_id, level));
            }
        }
        reassignments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::math::kmeans::Clustering;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::vector_id::ROOT_VECTOR_ID;
    use crate::storage::VectorDbStorageReadExt;
    use crate::write::indexer::tree::centroids::{TreeDepth, TreeLevel};
    use crate::write::indexer::tree::test_utils::{
        IndexerOpTestHarness, IndexerOpTestHarnessBuilder, TestDataVector,
    };
    use std::collections::HashSet;

    const DIMS: usize = 2;
    const CENTROID_ID: VectorId = VectorId::centroid_id(1, 1);
    const SPLIT_THRESHOLD: usize = 4;

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
            merge_threshold_vectors: 0,
            split_threshold_vectors: SPLIT_THRESHOLD,
            split_search_neighbourhood: 4,
            indexed_fields: HashSet::new(),
        })
    }

    fn data_id(id: u64) -> VectorId {
        VectorId::data_vector_id(id)
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

    struct ImbalancedClustering;

    impl Clustering for ImbalancedClustering {
        fn two_means(&self, vectors: &[(u64, &[f32])], _dimensions: usize) -> (Vec<f32>, Vec<f32>) {
            let center = vectors[0].1.to_vec();
            (center.clone(), center)
        }
    }

    fn imbalanced_clustering(_distance_metric: DistanceMetric) -> Box<dyn Clustering + Send> {
        Box::new(ImbalancedClustering)
    }

    #[tokio::test]
    async fn should_split_centroid_and_create_new_centroids_in_storage_when_parent_root() {
        // given
        let mut h = make_one_level_tree(vec![
            (
                CENTROID_A,
                vec![0.5, 0.5],
                vec![
                    TestDataVector::new("a1", vec![0.9, 0.1], vec![]),
                    TestDataVector::new("a2", vec![0.8, 0.2], vec![]),
                    TestDataVector::new("a3", vec![0.95, 0.05], vec![]),
                    TestDataVector::new("a4", vec![0.1, 0.9], vec![]),
                    TestDataVector::new("a5", vec![0.2, 0.8], vec![]),
                    TestDataVector::new("a6", vec![0.05, 0.95], vec![]),
                ],
            ),
            (
                CENTROID_B,
                vec![-1.0, 0.0],
                vec![
                    TestDataVector::new("b1", vec![-0.8, 0.4], vec![]),
                    TestDataVector::new("b2", vec![-0.9, 0.1], vec![]),
                ],
            ),
        ])
        .await;
        let opts = create_opts();
        let mut a_ids = HashSet::new();
        for ext_id in ["a1", "a2", "a3", "a4", "a5", "a6"] {
            a_ids.insert(h.storage.lookup_internal_id(ext_id).await.unwrap().unwrap());
        }
        let id_b1 = h.storage.lookup_internal_id("b1").await.unwrap().unwrap();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new(&opts, level, &snapshot, 1);
        let result = split.execute(&h.state, &mut delta).await.unwrap();
        assert_eq!(result.splits.len(), 1);
        let summary = &result.splits[0];
        assert_eq!(summary.c, CENTROID_A);
        assert_eq!(summary.new_centroids.len(), 2);
        h.apply_delta(delta).await;

        // then
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
                .is_some()
        );
        let root = PostingList::from_value(h.storage.get_root_posting_list(DIMS).await.unwrap());
        let root_ids: HashSet<_> = root.iter().map(|p| p.id()).collect();
        assert!(root_ids.contains(&CENTROID_B));
        let mut all_posted_ids = HashSet::new();
        for (new_centroid_id, _info) in &summary.new_centroids {
            assert!(
                h.storage
                    .get_centroid_info(*new_centroid_id)
                    .await
                    .unwrap()
                    .is_some()
            );
            let posting = PostingList::from_value(
                h.storage
                    .get_posting_list(*new_centroid_id, DIMS)
                    .await
                    .unwrap(),
            );
            assert_eq!(posting.len(), 3);
            assert_eq!(
                h.storage
                    .get_centroid_stats(*new_centroid_id)
                    .await
                    .unwrap()
                    .num_vectors,
                3
            );
            for p in posting.iter() {
                assert!(a_ids.contains(&p.id()));
                all_posted_ids.insert(p.id());
            }
            assert!(root_ids.contains(new_centroid_id));
        }
        assert_eq!(all_posted_ids, a_ids);
        let reassign_ids: HashSet<VectorId> =
            result.reassignments.iter().map(|r| r.vector_id).collect();
        assert!(reassign_ids.contains(&id_b1));
    }

    #[tokio::test]
    async fn should_split_single_centroid_with_no_neighbours() {
        // given
        let mut h = make_one_level_tree(vec![(
            CENTROID_ID,
            vec![0.0, 0.0],
            vec![
                TestDataVector::new("a", vec![0.9, 0.1], vec![]),
                TestDataVector::new("b", vec![0.8, 0.2], vec![]),
                TestDataVector::new("c", vec![0.1, 0.9], vec![]),
                TestDataVector::new("d", vec![0.2, 0.8], vec![]),
            ],
        )])
        .await;
        let opts = create_opts();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new(&opts, level, &snapshot, 1);
        let result = split.execute(&h.state, &mut delta).await.unwrap();
        assert_eq!(result.splits.len(), 1);
        h.apply_delta(delta).await;

        // then
        assert!(
            h.storage
                .get_centroid_info(CENTROID_ID)
                .await
                .unwrap()
                .is_none()
        );
        let root = PostingList::from_value(h.storage.get_root_posting_list(DIMS).await.unwrap());
        assert_eq!(root.len(), 2);
        let mut total = 0;
        for p in root.iter() {
            total += h
                .storage
                .get_posting_list(p.id(), DIMS)
                .await
                .unwrap()
                .len();
        }
        assert_eq!(total, 4);
        assert_eq!(result.reassignments.len(), 4);
        assert!(
            result
                .reassignments
                .iter()
                .all(|r| r.current_centroid == CENTROID_ID)
        );
    }

    #[tokio::test]
    async fn should_search_neighbour_postings_during_split() {
        // given
        let h = make_one_level_tree(vec![
            (
                CENTROID_A,
                vec![0.5, 0.5],
                vec![
                    TestDataVector::new("a1", vec![0.9, 0.1], vec![]),
                    TestDataVector::new("a2", vec![0.8, 0.2], vec![]),
                    TestDataVector::new("a3", vec![0.6, 0.9], vec![]),
                    TestDataVector::new("a4", vec![0.5, 0.8], vec![]),
                ],
            ),
            (
                CENTROID_B,
                vec![0.0, 1.0],
                vec![TestDataVector::new("b1", vec![0.05, 0.95], vec![])],
            ),
        ])
        .await;
        let opts = Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            root_threshold_vectors: usize::MAX,
            merge_threshold_vectors: 0,
            split_threshold_vectors: SPLIT_THRESHOLD,
            split_search_neighbourhood: 1,
            indexed_fields: HashSet::new(),
        });
        let id_b1 = h.storage.lookup_internal_id("b1").await.unwrap().unwrap();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new(&opts, level, &snapshot, 1);
        let result = split.execute(&h.state, &mut delta).await.unwrap();

        // then
        assert_eq!(result.splits.len(), 1);
        let b1_reassign = result
            .reassignments
            .iter()
            .find(|r| r.vector_id == id_b1)
            .unwrap();
        assert_eq!(b1_reassign.current_centroid, CENTROID_B);
    }

    #[tokio::test]
    async fn should_split_centroid_and_create_new_centroids_in_storage_when_parent_is_inner_level()
    {
        // given
        let mut h = make_multilevel_tree().await;
        let opts = create_opts();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let parent = L2_CENTROID_B;
        let target = CENTROID_D;

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new_with_max_splits_and_clustering(
            &opts,
            level,
            &snapshot,
            1,
            1,
            kmeans::for_metric,
        );
        let result = split.execute(&h.state, &mut delta).await.unwrap();
        assert_eq!(result.splits.len(), 1);
        assert_eq!(result.splits[0].c, target);
        h.apply_delta(delta).await;

        // then
        assert!(h.storage.get_centroid_info(target).await.unwrap().is_none());
        let parent_posting =
            PostingList::from_value(h.storage.get_posting_list(parent, DIMS).await.unwrap());
        let parent_ids: HashSet<_> = parent_posting.iter().map(|p| p.id()).collect();
        assert_eq!(parent_ids.len(), 2);
        let split_summary = &result.splits[0];
        let new_centroid_ids: HashSet<_> = split_summary
            .new_centroids
            .iter()
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(parent_ids, new_centroid_ids);
        for (new_centroid_id, _) in &split_summary.new_centroids {
            let info = h
                .storage
                .get_centroid_info(*new_centroid_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(info.parent_vector_id, parent);
            assert_eq!(info.level, 1);
        }
        let reassign_ids: HashSet<_> = result.reassignments.iter().map(|r| r.vector_id).collect();
        assert!(new_centroid_ids.is_subset(&reassign_ids));
    }

    #[tokio::test]
    async fn should_ignore_split_when_clustering_is_imbalanced() {
        // given
        let h = make_multilevel_tree().await;
        let opts = create_opts();
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let target = CENTROID_D;
        let parent = L2_CENTROID_B;

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new_with_max_splits_and_clustering(
            &opts,
            level,
            &snapshot,
            1,
            1,
            imbalanced_clustering,
        );
        let result = split.execute(&h.state, &mut delta).await.unwrap();

        // then
        assert!(result.splits.is_empty());
        assert_eq!(result.imbalanced, vec![(target, 5)]);
        assert!(result.reassignments.is_empty());
        let parent_posting =
            PostingList::from_value(h.storage.get_posting_list(parent, DIMS).await.unwrap());
        let parent_ids: HashSet<_> = parent_posting.iter().map(|p| p.id()).collect();
        assert_eq!(parent_ids, HashSet::from([target]));
        assert!(h.storage.get_centroid_info(target).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn should_only_split_centroids_over_threshold() {
        // given
        let mut h = make_one_level_tree(vec![
            (
                CENTROID_A,
                vec![1.0, 0.0],
                vec![
                    TestDataVector::new("a1", vec![0.9, 0.1], vec![]),
                    TestDataVector::new("a2", vec![0.8, 0.2], vec![]),
                    TestDataVector::new("a3", vec![0.95, 0.05], vec![]),
                    TestDataVector::new("a4", vec![0.85, 0.15], vec![]),
                    TestDataVector::new("a5", vec![0.92, 0.08], vec![]),
                    TestDataVector::new("a6", vec![0.88, 0.12], vec![]),
                ],
            ),
            (
                CENTROID_B,
                vec![0.0, 1.0],
                vec![
                    TestDataVector::new("b1", vec![0.1, 0.9], vec![]),
                    TestDataVector::new("b2", vec![0.2, 0.8], vec![]),
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
        let split = SplitCentroids::new(&opts, level, &snapshot, 1);
        let result = split.execute(&h.state, &mut delta).await.unwrap();
        assert_eq!(result.splits.len(), 1);
        assert_eq!(result.splits[0].c, CENTROID_A);
        h.apply_delta(delta).await;

        // then
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
                .is_some()
        );
        let posting_b =
            PostingList::from_value(h.storage.get_posting_list(CENTROID_B, DIMS).await.unwrap());
        assert_eq!(posting_b.len(), 2);
    }

    #[tokio::test]
    async fn should_only_split_centroids_from_level() {
        // given
        let mut h = make_multilevel_tree().await;
        let opts = Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            root_threshold_vectors: usize::MAX,
            merge_threshold_vectors: 0,
            split_threshold_vectors: 3,
            split_search_neighbourhood: 4,
            indexed_fields: HashSet::new(),
        });
        let level = TreeLevel::inner(2, TreeDepth::of(4));
        let p0 = L2_CENTROID_A;
        let p1 = L2_CENTROID_B;
        let a = CENTROID_A;
        let b = CENTROID_B;
        let c = CENTROID_C;
        let d = CENTROID_D;

        // when
        let snapshot = h.snapshot().await;
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new_with_max_splits_and_clustering(
            &opts,
            level,
            &snapshot,
            1,
            1,
            kmeans::for_metric,
        );
        let result = split.execute(&h.state, &mut delta).await.unwrap();
        assert_eq!(result.splits.len(), 1);
        assert_eq!(result.splits[0].c, p0);
        h.apply_delta(delta).await;

        // then
        assert!(h.storage.get_centroid_info(p0).await.unwrap().is_none());
        assert!(h.storage.get_centroid_info(p1).await.unwrap().is_some());
        assert!(h.storage.get_centroid_info(a).await.unwrap().is_some());
        assert!(h.storage.get_centroid_info(b).await.unwrap().is_some());
        assert!(h.storage.get_centroid_info(c).await.unwrap().is_some());
        assert!(h.storage.get_centroid_info(d).await.unwrap().is_some());
        let root = PostingList::from_value(h.storage.get_root_posting_list(DIMS).await.unwrap());
        let root_ids: HashSet<_> = root.iter().map(|p| p.id()).collect();
        assert_eq!(root_ids.len(), 3);
        assert!(root_ids.contains(&p1));
        let split_ids: HashSet<_> = result.splits[0]
            .new_centroids
            .iter()
            .map(|(id, _)| *id)
            .collect();
        assert!(split_ids.is_subset(&root_ids));
    }

    #[tokio::test]
    async fn should_respect_max_splits() {
        // given
        let mut h = make_one_level_tree(vec![
            (
                CENTROID_A,
                vec![1.0, 0.0],
                vec![
                    TestDataVector::new("a1", vec![0.9, 0.1], vec![]),
                    TestDataVector::new("a2", vec![0.8, 0.2], vec![]),
                    TestDataVector::new("a3", vec![0.95, 0.05], vec![]),
                    TestDataVector::new("a4", vec![0.85, 0.15], vec![]),
                ],
            ),
            (
                CENTROID_B,
                vec![0.0, 1.0],
                vec![
                    TestDataVector::new("b1", vec![0.1, 0.9], vec![]),
                    TestDataVector::new("b2", vec![0.2, 0.8], vec![]),
                    TestDataVector::new("b3", vec![0.05, 0.95], vec![]),
                    TestDataVector::new("b4", vec![0.15, 0.85], vec![]),
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
        let split = SplitCentroids::new_with_max_splits_and_clustering(
            &opts,
            level,
            &snapshot,
            1,
            1,
            kmeans::for_metric,
        );
        let result = split.execute(&h.state, &mut delta).await.unwrap();
        assert_eq!(result.splits.len(), 1);
        h.apply_delta(delta).await;

        // then
        let a_deleted = h
            .storage
            .get_centroid_info(CENTROID_A)
            .await
            .unwrap()
            .is_none();
        let b_deleted = h
            .storage
            .get_centroid_info(CENTROID_B)
            .await
            .unwrap()
            .is_none();
        assert!(a_deleted ^ b_deleted);
    }

    #[tokio::test]
    async fn split_centroid_should_partition_vectors_into_two_clusters() {
        // given
        let h = IndexerOpTestHarnessBuilder::new(vec![])
            .with_leaf_centroid(CENTROID_ID, vec![0.0, 0.0], vec![])
            .build(DIMS)
            .await;
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let mut posting = PostingList::with_capacity(4);
        posting.push(Posting::new(data_id(1), vec![0.9, 0.1]));
        posting.push(Posting::new(data_id(2), vec![0.8, 0.2]));
        posting.push(Posting::new(data_id(3), vec![0.1, 0.9]));
        posting.push(Posting::new(data_id(4), vec![0.2, 0.8]));
        let postings = HashMap::from([(CENTROID_ID, Arc::new(posting))]);

        let split = SplitCentroid {
            c: CENTROID_ID,
            count: 4,
            c_info: CentroidInfoValue::new(1, vec![0.0, 0.0], ROOT_VECTOR_ID),
            neighbours: vec![],
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            level,
            clustering: kmeans::for_metric(DistanceMetric::L2),
        };

        // when
        let result = split.execute(Arc::new(postings)).unwrap();

        // then
        assert_eq!(result.c, CENTROID_ID);
        let c0_ids: HashSet<VectorId> = result.c_0.postings().iter().map(|p| p.id()).collect();
        let c1_ids: HashSet<VectorId> = result.c_1.postings().iter().map(|p| p.id()).collect();
        assert_eq!(c0_ids.len() + c1_ids.len(), 4);
        assert!(c0_ids.is_disjoint(&c1_ids));
        let cluster_a = if c0_ids.contains(&data_id(1)) {
            &c0_ids
        } else {
            &c1_ids
        };
        let cluster_b = if c0_ids.contains(&data_id(3)) {
            &c0_ids
        } else {
            &c1_ids
        };
        assert!(cluster_a.contains(&data_id(1)) && cluster_a.contains(&data_id(2)));
        assert!(cluster_b.contains(&data_id(3)) && cluster_b.contains(&data_id(4)));
    }

    #[tokio::test]
    async fn should_return_split_reassignments_for_vectors_passing_heuristic() {
        // given
        let h = IndexerOpTestHarnessBuilder::new(vec![])
            .with_leaf_centroid(CENTROID_ID, vec![0.0, 0.0], vec![])
            .build(DIMS)
            .await;
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let c_vector = vec![0.0, 0.0];
        let c0_vector = vec![1.0, 0.0];
        let c1_vector = vec![0.0, 1.0];
        let mut postings = PostingList::with_capacity(2);
        postings.push(Posting::new(data_id(10), vec![0.1, 0.1]));
        postings.push(Posting::new(data_id(11), vec![0.9, 0.1]));

        let split = SplitCentroid {
            c: CENTROID_ID,
            count: 2,
            c_info: CentroidInfoValue::new(1, vec![0.0, 0.0], ROOT_VECTOR_ID),
            neighbours: vec![],
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            level,
            clustering: kmeans::for_metric(DistanceMetric::L2),
        };

        // when
        let reassignments = split.compute_split_reassignments(
            CENTROID_ID,
            &postings,
            &c_vector,
            &c0_vector,
            &c1_vector,
            level,
        );

        // then
        assert_eq!(reassignments.len(), 1);
        assert_eq!(reassignments[0].vector_id, data_id(10));
        assert_eq!(reassignments[0].current_centroid, CENTROID_ID);
    }

    #[tokio::test]
    async fn should_return_neighbour_reassignments_for_vectors_passing_heuristic() {
        // given
        let h = IndexerOpTestHarnessBuilder::new(vec![])
            .with_leaf_centroid(CENTROID_ID, vec![0.0, 0.0], vec![])
            .build(DIMS)
            .await;
        let depth = TreeDepth::of(h.state.centroids_meta().depth);
        let level = TreeLevel::leaf(depth);
        let c_vector = vec![0.5, 0.5];
        let c0_vector = vec![1.0, 0.0];
        let c1_vector = vec![0.0, 1.0];
        let neighbour_id = VectorId::centroid_id(1, 99);
        let mut neighbour_postings = PostingList::with_capacity(2);
        neighbour_postings.push(Posting::new(data_id(20), vec![0.9, 0.1]));
        neighbour_postings.push(Posting::new(data_id(21), vec![0.5, 0.5]));
        let postings = HashMap::from([(neighbour_id, Arc::new(neighbour_postings))]);

        let split = SplitCentroid {
            c: CENTROID_ID,
            count: 2,
            c_info: CentroidInfoValue::new(1, vec![0.0, 0.0], ROOT_VECTOR_ID),
            neighbours: vec![neighbour_id],
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            level,
            clustering: kmeans::for_metric(DistanceMetric::L2),
        };

        // when
        let reassignments = split
            .compute_neighbour_reassignments(&c_vector, &c0_vector, &c1_vector, &postings, level);

        // then
        assert_eq!(reassignments.len(), 1);
        assert_eq!(reassignments[0].vector_id, data_id(20));
        assert_eq!(reassignments[0].current_centroid, neighbour_id);
    }
}

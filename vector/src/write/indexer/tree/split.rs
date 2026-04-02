use crate::math::{distance, heuristics, kmeans};
use crate::serde::centroid_info::CentroidInfoValue;
use crate::write::indexer::drivers::AsyncBatchDriver;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::centroids::batch_search_centroids_up_to_level;
use crate::write::indexer::tree::posting_list::{Posting, PostingList};
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::{DistanceMetric, Result};
use common::StorageRead;
use futures::future::BoxFuture;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use log::debug;
use tokio::task::spawn_blocking;

const MAX_SPLITS: usize = usize::MAX;

#[derive(Clone, Debug)]
pub(crate) struct ReassignVector {
    pub(crate) vector_id: u64,
    pub(crate) vector: Vec<f32>,
    pub(crate) current_centroid: u64,
    pub(crate) level: u16,
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
    c: u64,
    c_info: CentroidInfoValue,
    c_0: SplitPostings,
    c_1: SplitPostings,
    reassign_vectors: Vec<ReassignVector>,
    candidates_evaluated: usize,
    candidates_returned: usize,
}

#[derive(Debug)]
pub(crate) struct SplitSummary {
    #[allow(dead_code)]
    pub(crate) c: u64,
    #[allow(dead_code)]
    pub(crate) new_centroids: Vec<(u64, CentroidInfoValue)>,
}

#[derive(Debug)]
pub(crate) struct SplitCentroidsResult {
    pub(crate) splits: Vec<SplitSummary>,
    pub(crate) reassignments: Vec<ReassignVector>,
    pub(crate) candidates_evaluated: usize,
    pub(crate) candidates_returned: usize,
}

pub(crate) struct SplitCentroids {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
    level: u16,
    depth: u16,
    max_splits: usize,
}

impl SplitCentroids {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        level: u16,
        depth: u16,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Self {
        Self::new_with_max_splits(opts, level, depth, snapshot, snapshot_epoch, MAX_SPLITS)
    }

    fn new_with_max_splits(
        opts: &Arc<IndexerOpts>,
        level: u16,
        depth: u16,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        max_splits: usize,
    ) -> Self {
        Self {
            opts: opts.clone(),
            level,
            depth,
            snapshot: snapshot.clone(),
            snapshot_epoch,
            max_splits,
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
            to_split.sort_by(|a, b| b.1.cmp(&a.1));
            to_split.truncate(self.max_splits);
            let to_split: Vec<u64> = to_split.into_iter().map(|(k, _v)| k).collect();
            if to_split.is_empty() {
                return Ok(SplitCentroidsResult {
                    splits: vec![],
                    reassignments: Vec::new(),
                    candidates_evaluated: 0,
                    candidates_returned: 0,
                });
            }
            // initialize the set of postings to fetch with the split centroids
            let mut postings_to_retrive =
                HashSet::with_capacity(to_split.len() * (1 + self.opts.split_search_neighbourhood));
            postings_to_retrive.extend(to_split.clone());

            // resolve centroids to full info
            let to_split: Vec<_> = to_split
                .into_iter()
                .map(|c| {
                    (
                        c,
                        view.centroid(c)
                            .expect(&format!("unexpected missing centroid {} at level: {}", c, self.level))
                            .clone(),
                    )
                })
                .collect();

            // collect each centroids neighbours
            let centroid_index =
                view.centroid_index(self.opts.dimensions, self.opts.distance_metric);
            let neighbours_by_centroid = if self.opts.split_search_neighbourhood > 0 {
                batch_search_centroids_up_to_level(
                    &centroid_index,
                    self.opts.split_search_neighbourhood + 1,
                    to_split
                        .iter()
                        .map(|(c, c_info)| (*c, c_info.vector.as_slice()))
                        .collect(),
                    self.level,
                )
                .await?
            } else {
                HashMap::new()
            };

            // initialize split tasks
            let mut splits = Vec::with_capacity(to_split.len());
            for (c, c_info) in to_split {
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
                    c_info,
                    neighbours,
                    dimensions: self.opts.dimensions,
                    distance_metric: self.opts.distance_metric,
                    level: self.level,
                })
            }

            // find all relevant postings (centroids and neighbours)
            let mut posting_reads = Vec::with_capacity(postings_to_retrive.len());
            for c in postings_to_retrive {
                let read_fut = view.posting_list(c, self.opts.dimensions);
                posting_reads.push(
                    Box::pin(async move { read_fut.get().await.map(|p| (c, p)) })
                        as BoxFuture<'static, Result<(u64, Arc<PostingList>)>>,
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

        // update delta
        let mut total_candidates_evaluated = 0usize;
        let mut total_candidates_returned = 0usize;
        let mut reassignments = HashMap::new();
        let mut splits = Vec::with_capacity(results.len());
        for result in results {
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
            delta
                .search_index
                .delete_centroids(self.level, vec![result.c]);
            if let Some(parent) = result.c_info.parent_vector_id {
                assert!(self.level + 1 < self.depth);
                delta
                    .search_index
                    .remove_from_posting(self.level + 1, parent, result.c);
            } else {
                assert_eq!(self.level + 1, self.depth);
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
                if let Some(parent) = result.c_info.parent_vector_id {
                    delta.search_index.add_to_posting(
                        self.level + 1,
                        parent,
                        c_id,
                        entry.vector.clone(),
                    );
                    reassignments.insert(
                        c_id,
                        ReassignVector {
                            vector_id: c_id,
                            vector: entry.vector.clone(),
                            current_centroid: parent,
                            level: self.level + 1,
                        },
                    );
                } else {
                    delta.search_index.add_to_root(c_id, entry.vector.clone());
                }
                // add all posting entries for the new centroid
                for p in new_centroid.postings() {
                    delta.search_index.add_to_posting(
                        self.level,
                        c_id,
                        p.id(),
                        p.vector().to_vec(),
                    );
                    if self.level > 0 {
                        delta.search_index.update_centroid(
                            p.id(),
                            CentroidInfoValue {
                                level: (self.level - 1) as u8,
                                vector: p.vector().to_vec(),
                                parent_vector_id: Some(c_id),
                            }
                        )
                    }
                }
                new_centroids.push((c_id, entry));
            }
            debug!("split: delete centroid {}/{}, add new centroids: {:?}",
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
            reassignments,
            candidates_evaluated: total_candidates_evaluated,
            candidates_returned: total_candidates_returned,
        })
    }
}

struct SplitCentroid {
    c: u64,
    c_info: CentroidInfoValue,
    neighbours: Vec<u64>,
    distance_metric: DistanceMetric,
    dimensions: usize,
    level: u16,
}

impl SplitCentroid {
    fn execute(self, postings: Arc<HashMap<u64, Arc<PostingList>>>) -> SplitResult {
        let c_postings = postings
            .get(&self.c)
            .expect("unexpected missing postings for c");
        assert!(
            c_postings.len() >= 2,
            "tried to split centroid {} with less than 2 postings",
            self.c
        );
        let c_vectors: Vec<(u64, Vec<f32>)> = c_postings
            .iter()
            .map(|p| (p.id(), p.vector().to_vec()))
            .collect();

        // Run two_means clustering to find new centroids
        let c_vector_refs: Vec<(u64, &[f32])> = c_vectors
            .iter()
            .map(|(id, v)| (*id, v.as_slice()))
            .collect();
        let clustering = kmeans::for_metric(self.distance_metric);
        let (c0_vector, c1_vector) = clustering.two_means(&c_vector_refs, self.dimensions);
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
        SplitResult {
            c: self.c,
            c_info: self.c_info,
            c_0: SplitPostings::new(c0_vector, c0_postings),
            c_1: SplitPostings::new(c1_vector, c1_postings),
            reassign_vectors: reassignments,
            candidates_evaluated,
            candidates_returned,
        }
    }

    fn compute_split_reassignments(
        &self,
        centroid_id: u64,
        postings: &PostingList,
        c_vector: &[f32],
        c0_vector: &[f32],
        c1_vector: &[f32],
        level: u16,
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
            reassignments.push(ReassignVector {
                vector_id: p.id(),
                vector: p.vector().to_vec(),
                current_centroid: centroid_id,
                level,
            })
        }
        reassignments
    }

    fn compute_neighbour_reassignments(
        &self,
        c_vector: &[f32],
        c0_vector: &[f32],
        c1_vector: &[f32],
        postings: &HashMap<u64, Arc<PostingList>>,
        level: u16,
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

                reassignments.push(ReassignVector {
                    vector_id: p.id(),
                    vector,
                    current_centroid: *neighbour_id,
                    level,
                });
            }
        }
        reassignments
    }
}

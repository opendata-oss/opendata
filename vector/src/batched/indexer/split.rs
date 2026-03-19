use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::indexer::IndexerOpts;
use crate::batched::indexer::state::{
    DirtyCentroidGraph, VectorIndexDelta, VectorIndexState, VectorIndexView,
};
use crate::lire::commands::SplitPostings;
use crate::lire::{heuristics, kmeans};
use crate::serde::posting_list::{Posting, PostingList};
use crate::{DistanceMetric, Result, distance};
use common::StorageRead;
use futures::future::BoxFuture;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::task::spawn_blocking;

const MAX_SPLITS: usize = usize::MAX;

#[derive(Clone)]
pub(crate) struct ReassignVector {
    pub(crate) vector_id: u64,
    pub(crate) vector: Vec<f32>,
    pub(crate) current_centroid: u64,
}

struct SplitResult {
    c: u64,
    c_0: SplitPostings,
    c_1: SplitPostings,
    reassign_vectors: Vec<ReassignVector>,
}

pub(crate) struct SplitCentroidsResult {
    pub(crate) splits: usize,
    pub(crate) reassignments: Vec<ReassignVector>,
}

pub(crate) struct SplitCentroids {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
}

impl SplitCentroids {
    pub(crate) fn new(opts: &Arc<IndexerOpts>, snapshot: &Arc<dyn StorageRead>) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
        }
    }

    pub(crate) async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<SplitCentroidsResult> {
        let view = VectorIndexView::new(delta, state, self.snapshot.clone());

        // find all centroids that need to be split, resolve their neighbours, and build up
        // a set of all postings that need to be fetched
        let counts = view.centroid_counts();
        // compute the centroids that need to be split, biggest first, capped at MAX_SPLITS
        let mut to_split: Vec<_> = counts
            .into_iter()
            .filter(|(_k, v)| *v >= self.opts.split_threshold_vectors as u64)
            .collect();
        to_split.sort_by(|a, b| b.1.cmp(&a.1));
        to_split.truncate(MAX_SPLITS);
        let to_split: Vec<u64> = to_split.into_iter().map(|(k, _v)| k).collect();
        if to_split.is_empty() {
            return Ok(SplitCentroidsResult {
                splits: 0,
                reassignments: Vec::new(),
            });
        }

        // initialize the set of postings to fetch with the split centroids
        let mut postings_to_retrive =
            HashSet::with_capacity(to_split.len() * self.opts.split_search_neighbourhood);
        postings_to_retrive.extend(to_split.clone());
        let mut splits = Vec::with_capacity(to_split.len());
        let centroid_graph = view.centroid_graph();
        // collect each centroids neighbours and add to postings to fetch, and initialize splits
        for c in to_split {
            let c_vec = centroid_graph
                .centroid(c)
                .expect("unexpected missing centroid");
            let neighbours =
                centroid_graph.search(&c_vec.vector, self.opts.split_search_neighbourhood);
            postings_to_retrive.extend(neighbours.clone());
            splits.push(SplitCentroid {
                c,
                neighbours,
                centroid_graph: centroid_graph.clone(),
                dimensions: self.opts.dimensions,
                distance_metric: self.opts.distance_metric,
            })
        }

        // find all relevant postings (centroids and neighbours)
        let mut posting_reads = Vec::with_capacity(postings_to_retrive.len());
        for c in postings_to_retrive {
            let read_fut = view.posting_list(c, self.opts.dimensions)?;
            posting_reads.push(Box::pin(async move { read_fut.await.map(|p| (c, p)) })
                as BoxFuture<'static, Result<(u64, PostingList)>>);
        }
        let results = AsyncBatchDriver::execute(posting_reads).await;
        let mut postings = HashMap::new();
        for r in results {
            let (c, c_postings) = r?;
            postings.insert(c, c_postings);
        }
        let postings = Arc::new(postings);

        // execute splits. spawn a blocking task to avoid tying up runtime as this is compute heavy
        let nsplits = splits.len();
        let results: Vec<_> = spawn_blocking(move || {
            splits
                .into_par_iter()
                .map(|split| split.execute(postings.clone()))
                .collect()
        })
        .await
        .expect("unexpected jo");

        // update delta
        let mut reassignments = HashMap::new();
        for result in results {
            // track reassignments
            reassignments.extend(
                result
                    .reassign_vectors
                    .into_iter()
                    .map(|r| (r.vector_id, r)),
            );

            // delete old centroid
            delta.delete_centroids(vec![result.c]);

            // create new centroids with postings
            for new_centroid in [result.c_0, result.c_1] {
                let entry = delta.add_centroid(new_centroid.centroid_vec().to_vec());
                for p in new_centroid.postings() {
                    delta.remove_from_posting(result.c, p.id());
                    delta.add_to_posting(entry.centroid_id, p.id(), p.vector().to_vec());
                }
            }
        }

        // return reassign set
        let reassignments: Vec<_> = reassignments.values().cloned().collect();
        Ok(SplitCentroidsResult {
            splits: nsplits,
            reassignments,
        })
    }
}

struct SplitCentroid {
    c: u64,
    neighbours: Vec<u64>,
    centroid_graph: Arc<DirtyCentroidGraph>,
    distance_metric: DistanceMetric,
    dimensions: usize,
}

impl SplitCentroid {
    fn execute(self, postings: Arc<HashMap<u64, PostingList>>) -> SplitResult {
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
        let mut c0_postings = Vec::new();
        let mut c1_postings = Vec::new();
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
        let mut reassignments = Vec::with_capacity(c0_postings.len() + c1_postings.len());
        let c_vector = self
            .centroid_graph
            .centroid(self.c)
            .expect("unexpected missing centroid");
        reassignments.extend(self.compute_split_reassignments(
            self.c,
            &c_postings,
            &c_vector.vector,
            &c0_vector,
            &c1_vector,
        ));
        reassignments.extend(self.compute_neighbour_reassignments(
            &c_vector.vector,
            &c0_vector,
            &c1_vector,
            postings.as_ref(),
        ));

        SplitResult {
            c: self.c,
            c_0: SplitPostings::new(c0_vector, c0_postings),
            c_1: SplitPostings::new(c1_vector, c1_postings),
            reassign_vectors: reassignments,
        }
    }

    fn compute_split_reassignments(
        &self,
        centroid_id: u64,
        postings: &[Posting],
        c_vector: &[f32],
        c0_vector: &[f32],
        c1_vector: &[f32],
    ) -> Vec<ReassignVector> {
        // use the heuristic for c's vectors from the spfresh paper to cheaply determine if
        // a vector may need reassignment. If it may, then check in the centroid graph for
        // its nearest centroid. If the nearest centroid is not c0 or c1, then include in
        // the reassignment set.
        let mut reassignments = Vec::with_capacity(postings.len());
        for p in postings {
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
            })
        }
        reassignments
    }

    fn compute_neighbour_reassignments(
        &self,
        c_vector: &[f32],
        c0_vector: &[f32],
        c1_vector: &[f32],
        postings: &HashMap<u64, PostingList>,
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
                });
            }
        }
        reassignments
    }
}

use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::indexer::IndexerOpts;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::lire::commands::SplitPostings;
use crate::lire::kmeans;
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
    new_centroids: Vec<SplitPostings>,
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
                old_centroid_vector: c_vec.vector.clone(),
                dimensions: self.opts.dimensions,
                distance_metric: self.opts.distance_metric,
                merge_threshold_vectors: self.opts.merge_threshold_vectors,
                split_threshold_vectors: self.opts.split_threshold_vectors,
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
            for new_centroid in result.new_centroids {
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
    old_centroid_vector: Vec<f32>,
    distance_metric: DistanceMetric,
    dimensions: usize,
    merge_threshold_vectors: usize,
    split_threshold_vectors: usize,
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

        // Run k-means clustering to find new centroids
        let c_vector_refs: Vec<(u64, &[f32])> = c_vectors
            .iter()
            .map(|(id, v)| (*id, v.as_slice()))
            .collect();
        let clustering = kmeans::for_metric(self.distance_metric);
        let ncentroids = self.num_split_centroids(c_postings.len());
        let centroid_vectors = clustering.k_means(&c_vector_refs, self.dimensions, ncentroids);

        let mut centroid_postings: Vec<Vec<Posting>> =
            (0..centroid_vectors.len()).map(|_| Vec::new()).collect();
        for (id, vector) in &c_vectors {
            let (closest_idx, _distance) = centroid_vectors
                .iter()
                .enumerate()
                .map(|(idx, centroid)| {
                    (
                        idx,
                        distance::compute_distance(vector, centroid, self.distance_metric),
                    )
                })
                .min_by(|a, b| a.1.cmp(&b.1))
                .expect("split must produce at least one centroid");
            centroid_postings[closest_idx].push(Posting::new(*id, vector.clone()));
        }

        let reassignments =
            self.compute_reassignments(&c_postings, &centroid_vectors, postings.as_ref());

        let new_centroids = centroid_vectors
            .into_iter()
            .zip(centroid_postings)
            .map(|(centroid_vec, postings)| SplitPostings::new(centroid_vec, postings))
            .collect();

        SplitResult {
            c: self.c,
            new_centroids,
            reassign_vectors: reassignments,
        }
    }

    fn num_split_centroids(&self, posting_count: usize) -> usize {
        let target = (self.merge_threshold_vectors + self.split_threshold_vectors)
            .div_ceil(2)
            .max(1);
        posting_count.div_ceil(target).max(2).min(posting_count)
    }

    fn compute_reassignments(
        &self,
        c_postings: &[Posting],
        new_centroid_vectors: &[Vec<f32>],
        postings: &HashMap<u64, PostingList>,
    ) -> Vec<ReassignVector> {
        let mut reassignments: Vec<ReassignVector> = Vec::new();

        for p in c_postings {
            if self.should_reassign_old_posting_vector(p.vector(), new_centroid_vectors) {
                reassignments.push(ReassignVector {
                    vector_id: p.id(),
                    vector: p.vector().to_vec(),
                    current_centroid: self.c,
                });
            }
        }

        for neighbour_id in &self.neighbours {
            let neighbour_postings = postings
                .get(neighbour_id)
                .expect("missing postings for neighbour");

            for p in neighbour_postings.iter() {
                if self.should_reassign_neighbour_posting_vector(p.vector(), new_centroid_vectors) {
                    reassignments.push(ReassignVector {
                        vector_id: p.id(),
                        vector: p.vector().to_vec(),
                        current_centroid: *neighbour_id,
                    });
                }
            }
        }
        reassignments
    }

    fn should_reassign_old_posting_vector(
        &self,
        vector: &[f32],
        new_centroid_vectors: &[Vec<f32>],
    ) -> bool {
        let d_old =
            distance::compute_distance(vector, &self.old_centroid_vector, self.distance_metric);
        let d_new_best = self.min_new_centroid_distance(vector, new_centroid_vectors);
        d_old <= d_new_best
    }

    fn should_reassign_neighbour_posting_vector(
        &self,
        vector: &[f32],
        new_centroid_vectors: &[Vec<f32>],
    ) -> bool {
        let d_old =
            distance::compute_distance(vector, &self.old_centroid_vector, self.distance_metric);
        let d_new_best = self.min_new_centroid_distance(vector, new_centroid_vectors);
        d_new_best <= d_old
    }

    fn min_new_centroid_distance(
        &self,
        vector: &[f32],
        new_centroid_vectors: &[Vec<f32>],
    ) -> distance::VectorDistance {
        new_centroid_vectors
            .iter()
            .map(|centroid| distance::compute_distance(vector, centroid, self.distance_metric))
            .min()
            .expect("split must produce at least one centroid")
    }
}

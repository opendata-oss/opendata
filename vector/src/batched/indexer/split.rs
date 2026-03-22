use crate::batched::indexer::IndexerOpts;
use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::state::{
    DirtyCentroidGraph, VectorIndexDelta, VectorIndexState, VectorIndexView,
};
use crate::lire::commands::SplitPostings;
use crate::lire::{heuristics, kmeans};
use crate::serde::centroid_chunk::CentroidEntry;
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

#[derive(Clone, Debug)]
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

#[derive(Debug)]
pub(crate) struct SplitSummary {
    #[allow(dead_code)]
    pub(crate) c: u64,
    #[allow(dead_code)]
    pub(crate) new_centroids: Vec<CentroidEntry>,
}

#[derive(Debug)]
pub(crate) struct SplitCentroidsResult {
    pub(crate) splits: Vec<SplitSummary>,
    pub(crate) reassignments: Vec<ReassignVector>,
}

pub(crate) struct SplitCentroids {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
    max_splits: usize,
}

impl SplitCentroids {
    pub(crate) fn new(opts: &Arc<IndexerOpts>, snapshot: &Arc<dyn StorageRead>) -> Self {
        Self::new_with_max_splits(opts, snapshot, MAX_SPLITS)
    }

    fn new_with_max_splits(
        opts: &Arc<IndexerOpts>,
        snapshot: &Arc<dyn StorageRead>,
        max_splits: usize,
    ) -> Self {
        Self {
            opts: opts.clone(),
            snapshot: snapshot.clone(),
            max_splits,
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
        to_split.truncate(self.max_splits);
        let to_split: Vec<u64> = to_split.into_iter().map(|(k, _v)| k).collect();
        if to_split.is_empty() {
            return Ok(SplitCentroidsResult {
                splits: vec![],
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
            let neighbours = centroid_graph
                .search(&c_vec.vector, self.opts.split_search_neighbourhood + 1)
                .into_iter()
                .filter(|&neighbour| c != neighbour)
                .collect::<Vec<_>>();
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
        let results: Vec<_> = spawn_blocking(move || {
            splits
                .into_par_iter()
                .map(|split| split.execute(postings.clone()))
                .collect()
        })
        .await
        .expect("unexpected join error");

        // update delta
        let mut reassignments = HashMap::new();
        let mut splits = Vec::with_capacity(results.len());
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
            let mut new_centroids = Vec::with_capacity(2);
            for new_centroid in [result.c_0, result.c_1] {
                let entry = delta.add_centroid(new_centroid.centroid_vec().to_vec());
                for p in new_centroid.postings() {
                    delta.remove_from_posting(result.c, p.id());
                    delta.add_to_posting(entry.centroid_id, p.id(), p.vector().to_vec());
                }
                new_centroids.push(entry);
            }
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
            c_postings,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batched::indexer::IndexerOpts;
    use crate::batched::indexer::test_utils::IndexerOpTestHarness;
    use crate::serde::centroid_chunk::CentroidEntry;
    use crate::storage::VectorDbStorageReadExt;
    use common::StorageRead;

    const DIMS: usize = 2;
    const CENTROID_ID: u64 = 0;
    const SPLIT_THRESHOLD: usize = 4;

    fn create_opts() -> Arc<IndexerOpts> {
        Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            merge_threshold_vectors: 0,
            split_threshold_vectors: SPLIT_THRESHOLD,
            split_search_neighbourhood: 4,
            indexed_fields: HashSet::new(),
            chunk_target: 4096,
        })
    }

    const CENTROID_A: u64 = 100;
    const CENTROID_B: u64 = 101;

    #[tokio::test]
    async fn should_split_centroid_and_create_new_centroids_in_storage() {
        // given — centroid A (at [0.5, 0.5]) has 6 vectors triggering a split.
        // centroid B (at [-1, 0]) is a neighbour with 2 vectors.
        let mut h = IndexerOpTestHarness::with_centroids(
            vec![
                CentroidEntry::new(CENTROID_A, vec![0.5, 0.5]),
                CentroidEntry::new(CENTROID_B, vec![-1.0, 0.0]),
            ],
            DIMS,
        )
        .await;
        let opts = create_opts(); // split_threshold = 4
        // Vectors near A in two clear sub-clusters
        // Cluster near [1, 0]: a1, a2, a3
        // Cluster near [0, 1]: a4, a5, a6
        // Vectors at B (near [-1, 0]): b1, b2 (below threshold, won't split)
        // b1 at [-0.8, 0.4] — after A splits, the new centroid near [0,1]
        // is closer to b1 than the old A centroid [0.5,0.5] was, so b1
        // should pass the neighbour heuristic.
        let writes = vec![
            h.make_write("a1", vec![0.9, 0.1]),
            h.make_write("a2", vec![0.8, 0.2]),
            h.make_write("a3", vec![0.95, 0.05]),
            h.make_write("a4", vec![0.1, 0.9]),
            h.make_write("a5", vec![0.2, 0.8]),
            h.make_write("a6", vec![0.05, 0.95]),
            h.make_write("b1", vec![-0.8, 0.4]),
            h.make_write("b2", vec![-0.9, 0.1]),
        ];
        h.write_and_apply(&opts, writes).await;
        // Collect internal IDs for A's vectors and b1
        let mut a_ids = HashSet::new();
        for ext_id in ["a1", "a2", "a3", "a4", "a5", "a6"] {
            a_ids.insert(h.storage.lookup_internal_id(ext_id).await.unwrap().unwrap());
        }
        let id_b1 = h.storage.lookup_internal_id("b1").await.unwrap().unwrap();

        // when — run SplitCentroids
        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new(&opts, &(snapshot.clone() as Arc<dyn StorageRead>));
        let result = split.execute(&h.state, &mut delta).await.unwrap();

        // then — only A should split (6 vectors >= threshold 4), not B (2 vectors)
        assert_eq!(result.splits.len(), 1);
        let summary = &result.splits[0];
        assert_eq!(summary.c, CENTROID_A, "should have split centroid A");
        assert_eq!(summary.new_centroids.len(), 2);
        let ops = delta.freeze(&mut h.state);
        h.storage.apply(ops).await.unwrap();
        // then — old centroid A should be in deletions bitmap, B should not
        let deletions = h.storage.get_deleted_vectors().await.unwrap();
        assert!(deletions.contains(CENTROID_A), "A should be deleted");
        assert!(!deletions.contains(CENTROID_B), "B should not be deleted");
        // then — the two new centroids from the summary should exist in storage
        // and each should have exactly 3 of A's 6 vectors
        let mut all_posted_ids = HashSet::new();
        for new_centroid in &summary.new_centroids {
            let scan = h.storage.scan_all_centroids(DIMS).await.unwrap();
            assert!(
                scan.entries
                    .iter()
                    .any(|e| e.centroid_id == new_centroid.centroid_id),
                "new centroid {} should be in storage chunks",
                new_centroid.centroid_id
            );
            let posting = h
                .storage
                .get_posting_list(new_centroid.centroid_id, DIMS)
                .await
                .unwrap();
            assert_eq!(
                posting.len(),
                3,
                "each new centroid should have exactly 3 vectors"
            );
            for p in posting.iter() {
                assert!(
                    a_ids.contains(&p.id()),
                    "posting id {} should be one of A's vectors",
                    p.id()
                );
                all_posted_ids.insert(p.id());
            }
        }
        assert_eq!(
            all_posted_ids, a_ids,
            "all 6 of A's vectors should be distributed across the two new centroids"
        );
        let reassign_ids: HashSet<u64> = result.reassignments.iter().map(|r| r.vector_id).collect();
        assert!(
            reassign_ids.contains(&id_b1),
            "b1 should be in reassignment set from neighbour heuristic"
        );
    }

    #[tokio::test]
    async fn should_split_single_centroid_with_no_neighbours() {
        let mut h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let opts = create_opts();
        let writes = vec![
            h.make_write("a", vec![0.9, 0.1]),
            h.make_write("b", vec![0.8, 0.2]),
            h.make_write("c", vec![0.1, 0.9]),
            h.make_write("d", vec![0.2, 0.8]),
        ];
        h.write_and_apply(&opts, writes).await;

        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new(&opts, &(snapshot as Arc<dyn StorageRead>));
        let result = split.execute(&h.state, &mut delta).await.unwrap();

        assert_eq!(result.splits.len(), 1);
        let ops = delta.freeze(&mut h.state);
        h.storage.apply(ops).await.unwrap();
        let deletions = h.storage.get_deleted_vectors().await.unwrap();
        assert!(deletions.contains(CENTROID_ID));
        let scan = h.storage.scan_all_centroids(DIMS).await.unwrap();
        assert_eq!(scan.entries.len(), 2);
        let mut total = 0;
        for entry in &scan.entries {
            total += h
                .storage
                .get_posting_list(entry.centroid_id, DIMS)
                .await
                .unwrap()
                .len();
        }
        assert_eq!(total, 4);
        assert_eq!(result.reassignments.len(), 0);
    }

    #[tokio::test]
    async fn should_search_neighbour_postings_during_split() {
        let mut h = IndexerOpTestHarness::with_centroids(
            vec![
                CentroidEntry::new(CENTROID_A, vec![0.5, 0.5]),
                CentroidEntry::new(CENTROID_B, vec![0.0, 1.0]),
            ],
            DIMS,
        )
        .await;
        let opts = Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            merge_threshold_vectors: 0,
            split_threshold_vectors: SPLIT_THRESHOLD,
            split_search_neighbourhood: 1,
            indexed_fields: HashSet::new(),
            chunk_target: 4096,
        });
        let writes = vec![
            h.make_write("a1", vec![0.9, 0.1]),
            h.make_write("a2", vec![0.8, 0.2]),
            h.make_write("a3", vec![0.6, 0.9]),
            h.make_write("a4", vec![0.5, 0.8]),
            h.make_write("b1", vec![0.05, 0.95]),
        ];
        h.write_and_apply(&opts, writes).await;
        let id_b1 = h.storage.lookup_internal_id("b1").await.unwrap().unwrap();

        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new(&opts, &(snapshot as Arc<dyn StorageRead>));
        let result = split.execute(&h.state, &mut delta).await.unwrap();

        assert_eq!(result.splits.len(), 1);
        let reassign_ids: HashSet<u64> = result.reassignments.iter().map(|r| r.vector_id).collect();
        assert!(
            reassign_ids.contains(&id_b1),
            "b1 should be reassigned via neighbour search"
        );
        let b1_reassign = result
            .reassignments
            .iter()
            .find(|r| r.vector_id == id_b1)
            .unwrap();
        assert_eq!(b1_reassign.current_centroid, CENTROID_B);
    }

    #[tokio::test]
    async fn should_only_split_centroids_over_threshold() {
        let mut h = IndexerOpTestHarness::with_centroids(
            vec![
                CentroidEntry::new(CENTROID_A, vec![1.0, 0.0]),
                CentroidEntry::new(CENTROID_B, vec![0.0, 1.0]),
            ],
            DIMS,
        )
        .await;
        let opts = create_opts();
        let writes = vec![
            h.make_write("a1", vec![0.9, 0.1]),
            h.make_write("a2", vec![0.8, 0.2]),
            h.make_write("a3", vec![0.95, 0.05]),
            h.make_write("a4", vec![0.85, 0.15]),
            h.make_write("a5", vec![0.92, 0.08]),
            h.make_write("a6", vec![0.88, 0.12]),
            h.make_write("b1", vec![0.1, 0.9]),
            h.make_write("b2", vec![0.2, 0.8]),
        ];
        h.write_and_apply(&opts, writes).await;

        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let split = SplitCentroids::new(&opts, &(snapshot as Arc<dyn StorageRead>));
        let result = split.execute(&h.state, &mut delta).await.unwrap();

        assert_eq!(
            result.splits.len(),
            1,
            "only the centroid over threshold should split"
        );
        let ops = delta.freeze(&mut h.state);
        h.storage.apply(ops).await.unwrap();
        let deletions = h.storage.get_deleted_vectors().await.unwrap();
        assert!(deletions.contains(CENTROID_A), "A should be deleted");
        assert!(!deletions.contains(CENTROID_B), "B should not be deleted");
        let posting_b = h.storage.get_posting_list(CENTROID_B, DIMS).await.unwrap();
        assert_eq!(posting_b.len(), 2, "B's postings should be untouched");
    }

    #[tokio::test]
    async fn should_respect_max_splits() {
        let mut h = IndexerOpTestHarness::with_centroids(
            vec![
                CentroidEntry::new(CENTROID_A, vec![1.0, 0.0]),
                CentroidEntry::new(CENTROID_B, vec![0.0, 1.0]),
            ],
            DIMS,
        )
        .await;
        let opts = create_opts();
        let writes = vec![
            h.make_write("a1", vec![0.9, 0.1]),
            h.make_write("a2", vec![0.8, 0.2]),
            h.make_write("a3", vec![0.95, 0.05]),
            h.make_write("a4", vec![0.85, 0.15]),
            h.make_write("b1", vec![0.1, 0.9]),
            h.make_write("b2", vec![0.2, 0.8]),
            h.make_write("b3", vec![0.05, 0.95]),
            h.make_write("b4", vec![0.15, 0.85]),
        ];
        h.write_and_apply(&opts, writes).await;

        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let split =
            SplitCentroids::new_with_max_splits(&opts, &(snapshot as Arc<dyn StorageRead>), 1);
        let result = split.execute(&h.state, &mut delta).await.unwrap();

        assert_eq!(result.splits.len(), 1, "should respect max_splits=1");
        let ops = delta.freeze(&mut h.state);
        h.storage.apply(ops).await.unwrap();
        let deletions = h.storage.get_deleted_vectors().await.unwrap();
        let a_deleted = deletions.contains(CENTROID_A);
        let b_deleted = deletions.contains(CENTROID_B);
        assert!(
            a_deleted ^ b_deleted,
            "exactly one centroid should be deleted, got A={a_deleted} B={b_deleted}"
        );
    }

    // ---- SplitCentroid (unit) ----

    #[tokio::test]
    async fn split_centroid_should_partition_vectors_into_two_clusters() {
        let h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let delta = VectorIndexDelta::new(&h.state);
        let snapshot = h.storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &h.state, snapshot);
        let centroid_graph = view.centroid_graph();

        let postings: HashMap<u64, PostingList> = HashMap::from([(
            CENTROID_ID,
            vec![
                Posting::new(1, vec![0.9, 0.1]),
                Posting::new(2, vec![0.8, 0.2]),
                Posting::new(3, vec![0.1, 0.9]),
                Posting::new(4, vec![0.2, 0.8]),
            ],
        )]);

        let split = SplitCentroid {
            c: CENTROID_ID,
            neighbours: vec![],
            centroid_graph,
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
        };
        let result = split.execute(Arc::new(postings));

        assert_eq!(result.c, CENTROID_ID);
        let c0_ids: HashSet<u64> = result.c_0.postings().iter().map(|p| p.id()).collect();
        let c1_ids: HashSet<u64> = result.c_1.postings().iter().map(|p| p.id()).collect();
        assert_eq!(c0_ids.len() + c1_ids.len(), 4);
        assert!(c0_ids.is_disjoint(&c1_ids));
        let cluster_a = if c0_ids.contains(&1) {
            &c0_ids
        } else {
            &c1_ids
        };
        let cluster_b = if c0_ids.contains(&3) {
            &c0_ids
        } else {
            &c1_ids
        };
        assert!(cluster_a.contains(&1) && cluster_a.contains(&2));
        assert!(cluster_b.contains(&3) && cluster_b.contains(&4));
    }

    #[tokio::test]
    async fn should_return_split_reassignments_for_vectors_passing_heuristic() {
        let h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let delta = VectorIndexDelta::new(&h.state);
        let snapshot = h.storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &h.state, snapshot);
        let centroid_graph = view.centroid_graph();

        let c_vector = vec![0.0, 0.0];
        let c0_vector = vec![1.0, 0.0];
        let c1_vector = vec![0.0, 1.0];
        let passing = Posting::new(10, vec![0.1, 0.1]);
        let failing = Posting::new(11, vec![0.9, 0.1]);

        let split = SplitCentroid {
            c: CENTROID_ID,
            neighbours: vec![],
            centroid_graph,
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
        };
        let reassignments = split.compute_split_reassignments(
            CENTROID_ID,
            &[passing, failing],
            &c_vector,
            &c0_vector,
            &c1_vector,
        );

        assert_eq!(reassignments.len(), 1);
        assert_eq!(reassignments[0].vector_id, 10);
        assert_eq!(reassignments[0].current_centroid, CENTROID_ID);
    }

    #[tokio::test]
    async fn should_return_neighbour_reassignments_for_vectors_passing_heuristic() {
        let h = IndexerOpTestHarness::with_single_centroid(CENTROID_ID, DIMS).await;
        let delta = VectorIndexDelta::new(&h.state);
        let snapshot = h.storage.snapshot().await.unwrap();
        let view = VectorIndexView::new(&delta, &h.state, snapshot);
        let centroid_graph = view.centroid_graph();

        let c_vector = vec![0.5, 0.5];
        let c0_vector = vec![1.0, 0.0];
        let c1_vector = vec![0.0, 1.0];
        let neighbour_id: u64 = 99;
        let passing = Posting::new(20, vec![0.9, 0.1]);
        let failing = Posting::new(21, vec![0.5, 0.5]);

        let mut postings: HashMap<u64, PostingList> = HashMap::new();
        postings.insert(neighbour_id, vec![passing, failing]);

        let split = SplitCentroid {
            c: CENTROID_ID,
            neighbours: vec![neighbour_id],
            centroid_graph,
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
        };
        let reassignments =
            split.compute_neighbour_reassignments(&c_vector, &c0_vector, &c1_vector, &postings);

        assert_eq!(reassignments.len(), 1);
        assert_eq!(reassignments[0].vector_id, 20);
        assert_eq!(reassignments[0].current_centroid, neighbour_id);
    }
}

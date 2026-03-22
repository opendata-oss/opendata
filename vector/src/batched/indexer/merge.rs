use crate::Result;
use crate::batched::indexer::IndexerOpts;
use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::split::ReassignVector;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::serde::posting_list::PostingList;
use common::StorageRead;
use futures::future::BoxFuture;
use std::sync::Arc;

struct MergeCentroid {
    c: u64,
    postings: PostingList,
}

pub(crate) struct MergeCentroids {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>,
}

impl MergeCentroids {
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
    ) -> Result<Vec<ReassignVector>> {
        let view = VectorIndexView::new(delta, state, self.snapshot.clone());

        // find all centroids that need to be merged
        let counts = view.centroid_counts();
        // compute the centroids that need to be split
        let to_merge = counts
            .iter()
            .filter(|(_k, v)| **v < self.opts.merge_threshold_vectors as u64)
            .map(|(&k, _v)| k)
            .collect::<Vec<_>>();
        if to_merge.is_empty() {
            return Ok(vec![]);
        }
        // Don't merge if all centroids would be merged — there are no targets left
        if to_merge.len() >= counts.len() {
            return Ok(vec![]);
        }

        // read postings of all mergees
        let mut to_resolve = Vec::with_capacity(to_merge.len());
        for c in to_merge {
            let posting_fut = view.posting_list(c, self.opts.dimensions)?;
            to_resolve.push(Box::pin(async move {
                Ok(MergeCentroid {
                    c,
                    postings: posting_fut.await?,
                })
            }) as BoxFuture<'static, Result<MergeCentroid>>)
        }
        let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
        let mut resolved = Vec::with_capacity(resolve_results.len());
        for r in resolve_results {
            resolved.push(r?);
        }

        // execute merges
        let total_moved = resolved.iter().map(|m| m.postings.len()).sum();
        let mut reassignments = Vec::with_capacity(total_moved);
        for merge in resolved {
            delta.delete_centroids(vec![merge.c]);
            for p in merge.postings {
                reassignments.push(ReassignVector {
                    vector_id: p.id(),
                    vector: p.vector().to_vec(),
                    current_centroid: merge.c,
                })
            }
        }

        // return reassign set
        Ok(reassignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batched::indexer::IndexerOpts;
    use crate::batched::indexer::test_utils::IndexerOpTestHarness;
    use crate::serde::centroid_chunk::CentroidEntry;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::storage::VectorDbStorageReadExt;
    use common::StorageRead;
    use std::collections::HashSet;

    const DIMS: usize = 2;
    const MERGE_THRESHOLD: usize = 3;

    const CENTROID_A: u64 = 100;
    const CENTROID_B: u64 = 101;
    const CENTROID_C: u64 = 102;

    fn create_opts() -> Arc<IndexerOpts> {
        Arc::new(IndexerOpts {
            dimensions: DIMS,
            distance_metric: DistanceMetric::L2,
            merge_threshold_vectors: MERGE_THRESHOLD,
            split_threshold_vectors: usize::MAX,
            split_search_neighbourhood: 4,
            indexed_fields: HashSet::new(),
            chunk_target: 4096,
        })
    }

    #[tokio::test]
    async fn should_merge_centroids_below_threshold() {
        // given — 3 centroids: A has 1 vector (below threshold=3), B has 2 (below),
        // C has 4 (above). A and B should be merged, C should not.
        let mut h = IndexerOpTestHarness::with_centroids(
            vec![
                CentroidEntry::new(CENTROID_A, vec![1.0, 0.0]),
                CentroidEntry::new(CENTROID_B, vec![0.0, 1.0]),
                CentroidEntry::new(CENTROID_C, vec![-1.0, 0.0]),
            ],
            DIMS,
        )
        .await;
        let opts = create_opts();

        let writes = vec![
            // 1 vector at A (below threshold)
            h.make_write("a1", vec![0.9, 0.1]),
            // 2 vectors at B (below threshold)
            h.make_write("b1", vec![0.1, 0.9]),
            h.make_write("b2", vec![0.2, 0.8]),
            // 4 vectors at C (above threshold)
            h.make_write("c1", vec![-0.9, 0.1]),
            h.make_write("c2", vec![-0.8, 0.2]),
            h.make_write("c3", vec![-0.95, 0.05]),
            h.make_write("c4", vec![-0.85, 0.15]),
        ];
        h.write_and_apply(&opts, writes).await;

        // collect IDs for vectors at A and B
        let id_a1 = h.storage.lookup_internal_id("a1").await.unwrap().unwrap();
        let id_b1 = h.storage.lookup_internal_id("b1").await.unwrap().unwrap();
        let id_b2 = h.storage.lookup_internal_id("b2").await.unwrap().unwrap();

        // when
        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let merge = MergeCentroids::new(&opts, &(snapshot as Arc<dyn StorageRead>));
        let reassignments = merge.execute(&h.state, &mut delta).await.unwrap();
        let ops = delta.freeze(&mut h.state);
        h.storage.apply(ops).await.unwrap();

        // then — A and B should be deleted
        let deletions = h.storage.get_deleted_vectors().await.unwrap();
        assert!(deletions.contains(CENTROID_A), "A should be deleted");
        assert!(deletions.contains(CENTROID_B), "B should be deleted");
        assert!(!deletions.contains(CENTROID_C), "C should not be deleted");

        // then — all 3 vectors from A and B should be in the reassignment set
        let reassign_ids: HashSet<u64> = reassignments.iter().map(|r| r.vector_id).collect();
        assert_eq!(reassign_ids.len(), 3);
        assert!(reassign_ids.contains(&id_a1));
        assert!(reassign_ids.contains(&id_b1));
        assert!(reassign_ids.contains(&id_b2));

        // then — each reassignment should reference its original centroid
        let a1_r = reassignments.iter().find(|r| r.vector_id == id_a1).unwrap();
        assert_eq!(a1_r.current_centroid, CENTROID_A);
        let b1_r = reassignments.iter().find(|r| r.vector_id == id_b1).unwrap();
        assert_eq!(b1_r.current_centroid, CENTROID_B);
        let b2_r = reassignments.iter().find(|r| r.vector_id == id_b2).unwrap();
        assert_eq!(b2_r.current_centroid, CENTROID_B);
    }

    #[tokio::test]
    async fn should_not_merge_when_all_centroids_below_threshold() {
        // given — 2 centroids both below threshold. Merging would leave no targets,
        // so the guard should prevent any merges.
        let mut h = IndexerOpTestHarness::with_centroids(
            vec![
                CentroidEntry::new(CENTROID_A, vec![1.0, 0.0]),
                CentroidEntry::new(CENTROID_B, vec![0.0, 1.0]),
            ],
            DIMS,
        )
        .await;
        let opts = create_opts(); // threshold = 3

        let writes = vec![
            h.make_write("a1", vec![0.9, 0.1]),
            h.make_write("b1", vec![0.1, 0.9]),
        ];
        h.write_and_apply(&opts, writes).await;

        // when
        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let merge = MergeCentroids::new(&opts, &(snapshot as Arc<dyn StorageRead>));
        let reassignments = merge.execute(&h.state, &mut delta).await.unwrap();

        // then — no merges, no reassignments
        assert!(
            reassignments.is_empty(),
            "should not merge when all centroids are below threshold"
        );
    }

    #[tokio::test]
    async fn should_not_merge_centroids_at_or_above_threshold() {
        // given — 2 centroids both at or above threshold
        let mut h = IndexerOpTestHarness::with_centroids(
            vec![
                CentroidEntry::new(CENTROID_A, vec![1.0, 0.0]),
                CentroidEntry::new(CENTROID_B, vec![0.0, 1.0]),
            ],
            DIMS,
        )
        .await;
        let opts = create_opts(); // threshold = 3

        let writes = vec![
            h.make_write("a1", vec![0.9, 0.1]),
            h.make_write("a2", vec![0.8, 0.2]),
            h.make_write("a3", vec![0.95, 0.05]),
            h.make_write("b1", vec![0.1, 0.9]),
            h.make_write("b2", vec![0.2, 0.8]),
            h.make_write("b3", vec![0.05, 0.95]),
        ];
        h.write_and_apply(&opts, writes).await;

        // when
        let snapshot = h.storage.snapshot().await.unwrap();
        let mut delta = VectorIndexDelta::new(&h.state);
        let merge = MergeCentroids::new(&opts, &(snapshot as Arc<dyn StorageRead>));
        let reassignments = merge.execute(&h.state, &mut delta).await.unwrap();

        // then — no merges
        assert!(
            reassignments.is_empty(),
            "should not merge centroids at or above threshold"
        );
    }
}

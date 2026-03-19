use std::collections::HashSet;
use std::sync::Arc;
use futures::future::BoxFuture;
use common::StorageRead;
use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::split::ReassignVector;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::DistanceMetric;
use crate::serde::posting_list::PostingList;
use crate::Result;

struct MergeCentroid {
    c_from: u64,
    c_to: u64,
}

struct ResolvedMergeCentroid {
    c_from: u64,
    c_to: u64,
    c_from_postings: PostingList
}

struct MergeCentroids {
    dimensions: usize,
    distance_metric: DistanceMetric,
    merge_threshold_vectors: usize,
    split_threshold_vectors: usize,
    search_neighbourhood: usize,
    snapshot: Arc<dyn StorageRead>
}

impl MergeCentroids {
    async fn execute(
        self,
        delta: &mut VectorIndexDelta,
        state: &VectorIndexState
    ) -> Result<Vec<ReassignVector>> {
        let view = VectorIndexView::new(
            delta,
            state,
            self.snapshot.clone()
        );

        // find all centroids that need to be split, resolve their neighbours, and build up
        // a set of all postings that need to be fetched
        let counts = view.centroid_counts();
        // compute the centroids that need to be split
        let to_merge = counts
            .iter()
            .filter(|(_k, v)| **v < self.merge_threshold_vectors as u64)
            .map(|(k, v)| (k, v))
            .collect::<Vec<_>>();

        // find neighbouring centroids that can be merge partners
        let centroid_graph = view.centroid_graph();
        let mut mergees = HashSet::with_capacity(to_merge.len());
        let mut merges = Vec::with_capacity(to_merge.len());
        for (&c, &count) in to_merge {
            let v = centroid_graph.centroid(c).expect("unexpected missing centroid");
            for neighbour in centroid_graph.search(&v.vector, 16) {
                let neighbour_count = counts.get(&neighbour).copied().unwrap_or(u64::MAX);
                if neighbour != c
                    && !mergees.contains(&neighbour)
                    && count.saturating_add(neighbour_count) < self.split_threshold_vectors as u64 {
                    mergees.insert(neighbour);
                    mergees.insert(c);
                    // merge the smaller centroid into the larger centroid
                    let merge = if count < neighbour_count {
                        MergeCentroid { c_from: c, c_to: neighbour }
                    } else {
                        MergeCentroid { c_from: neighbour, c_to: c }
                    };
                    merges.push(merge)
                }
            }
        }

        // read postings of all c_from
        let mut to_resolve = Vec::with_capacity(mergees.len());
        for merge in merges {
            let posting_fut = view.posting_list(merge.c_from, self.dimensions)?;
            to_resolve.push(
                Box::pin(async move {
                    Ok(
                        ResolvedMergeCentroid {
                            c_from: merge.c_from,
                            c_to: merge.c_to,
                            c_from_postings: posting_fut.await?,
                        }
                    )
                }) as BoxFuture<'static, Result<ResolvedMergeCentroid>>
            )
        }
        let resolve_results = AsyncBatchDriver::execute(to_resolve).await;
        let mut resolved = Vec::with_capacity(resolve_results.len());
        for r in resolve_results {
            resolved.push(r?);
        }

        // execute merges
        let total_moved = resolved.iter().map(|m| m.c_from_postings.len()).sum();
        let mut reassignments = Vec::with_capacity(total_moved);
        for merge in resolved {
            delta.delete_centroids(vec![merge.c_from]);
            for p in merge.c_from_postings {
                reassignments.push(
                    ReassignVector {
                        vector_id: p.id(),
                        vector: p.vector().to_vec(),
                        current_centroid: merge.c_from,
                    }
                )
            }
        }

        // return reassign set
        Ok(reassignments)
    }
}
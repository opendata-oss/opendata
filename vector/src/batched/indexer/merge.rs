use std::sync::Arc;
use futures::future::BoxFuture;
use common::StorageRead;
use crate::batched::indexer::drivers::AsyncBatchDriver;
use crate::batched::indexer::indexer::IndexerOpts;
use crate::batched::indexer::split::ReassignVector;
use crate::batched::indexer::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::serde::posting_list::PostingList;
use crate::Result;

struct MergeCentroid {
    c: u64,
    postings: PostingList
}

pub(crate) struct MergeCentroids {
    opts: Arc<IndexerOpts>,
    snapshot: Arc<dyn StorageRead>
}

impl MergeCentroids {
    pub(crate) fn new(opts: &Arc<IndexerOpts>, snapshot: &Arc<dyn StorageRead>) -> Self {
        Self { opts: opts.clone(), snapshot: snapshot.clone() }
    }

    pub(crate) async fn execute(
        self,
        state: &VectorIndexState,
        delta: &mut VectorIndexDelta,
    ) -> Result<Vec<ReassignVector>> {
        let view = VectorIndexView::new(
            delta,
            state,
            self.snapshot.clone()
        );

        // find all centroids that need to be merged
        let counts = view.centroid_counts();
        // compute the centroids that need to be split
        let to_merge = counts
            .iter()
            .filter(|(_k, v)| **v < self.opts.merge_threshold_vectors as u64)
            .map(|(&k, _v)| k)
            .collect::<Vec<_>>();

        // read postings of all mergees
        let mut to_resolve = Vec::with_capacity(to_merge.len());
        for c in to_merge {
            let posting_fut = view.posting_list(c, self.opts.dimensions)?;
            to_resolve.push(
                Box::pin(async move {
                    Ok(
                        MergeCentroid {
                            c,
                            postings: posting_fut.await?,
                        }
                    )
                }) as BoxFuture<'static, Result<MergeCentroid>>
            )
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
                reassignments.push(
                    ReassignVector {
                        vector_id: p.id(),
                        vector: p.vector().to_vec(),
                        current_centroid: merge.c,
                    }
                )
            }
        }

        // return reassign set
        Ok(reassignments)
    }
}
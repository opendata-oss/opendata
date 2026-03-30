use crate::Result;
use crate::serde::centroid_info::CentroidInfoEntry;
use crate::serde::posting_list::PostingList;
use crate::write::indexer::drivers::AsyncBatchDriver;
use crate::write::indexer::tree::IndexerOpts;
use crate::write::indexer::tree::split::ReassignVector;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use common::StorageRead;
use futures::future::BoxFuture;
use std::sync::Arc;

struct MergeCentroid {
    c: u64,
    c_info: CentroidInfoEntry,
    postings: PostingList,
}

pub(crate) struct MergeCentroids {
    opts: Arc<IndexerOpts>,
    level: u16,
    depth: u16,
    snapshot: Arc<dyn StorageRead>,
    snapshot_epoch: u64,
}

impl MergeCentroids {
    pub(crate) fn new(
        opts: &Arc<IndexerOpts>,
        level: u16,
        depth: u16,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Self {
        Self {
            opts: opts.clone(),
            level,
            depth,
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
            let posting_fut = view.posting_list(c, self.opts.dimensions)?;
            let c_info = view
                .centroid(c)
                .expect("unexpected missing centroid")
                .clone();
            to_resolve.push(Box::pin(async move {
                Ok(MergeCentroid {
                    c,
                    c_info,
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
        let merge_count = resolved.len();
        let total_moved = resolved.iter().map(|m| m.postings.len()).sum();
        let mut reassignments = Vec::with_capacity(total_moved);
        for merge in resolved {
            delta
                .search_index
                .delete_centroids(self.level, vec![merge.c]);
            if let Some(parent) = merge.c_info.parent_vector_id {
                assert!(self.level + 1 < self.depth);
                delta
                    .search_index
                    .remove_from_posting(self.level + 1, parent, merge.c);
            } else {
                assert_eq!(self.level + 1, self.depth);
                delta.search_index.remove_from_root(merge.c);
            }
            for p in merge.postings {
                reassignments.push(ReassignVector {
                    level: self.level,
                    vector_id: p.id(),
                    vector: p.vector().to_vec(),
                    current_centroid: merge.c,
                })
            }
        }

        // return reassign set
        Ok((reassignments, merge_count))
    }
}

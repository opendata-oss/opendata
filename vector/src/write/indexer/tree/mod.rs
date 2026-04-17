//! Batch indexer for opendata-vector. This module provides [`Indexer`], which does indexing
//! for batches of writes to the vector db. The indexer maintains:
//! - forward indexes for vector lookup by id
//! - inverted indexes for search filtered on vector attributes
//! - a tree-based ANN index for vector search. See [`centroids`] for details on the tree structure

use crate::DistanceMetric;
use crate::Result;
use crate::serde::vector_id::{LEAF_LEVEL, VectorId};
use crate::write::delta::VectorWrite;
use crate::write::indexer::tree::centroids::{
    AllCentroidsCache, CentroidCache, TreeDepth, TreeLevel,
};
use crate::write::indexer::tree::merge::MergeCentroids;
use crate::write::indexer::tree::root::SplitRoot;
use crate::write::indexer::tree::split::{ReassignVector, SplitCentroids};
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState, VectorIndexView};
use crate::write::indexer::tree::vector::{ReassignVectors, WriteVectors};
use common::StorageRead;
use common::storage::RecordOp;
#[cfg(debug_assertions)]
use common::storage::StorageSnapshot;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, debug_span, error, info, warn};

pub(crate) mod centroids;
mod merge;
pub(crate) mod posting_list;
mod root;
mod split;
pub(crate) mod state;
#[cfg(test)]
pub(crate) mod test_utils;
pub(crate) mod validator;
mod vector;

#[derive(Debug, Default)]
pub(crate) struct IndexerStats {
    pub(crate) inserts: usize,
    pub(crate) updates: usize,
    pub(crate) merges: HashMap<TreeLevel, usize>,
    pub(crate) splits: HashMap<TreeLevel, usize>,
    pub(crate) reassignments: HashMap<TreeLevel, usize>,
    pub(crate) largest_centroids: HashMap<TreeLevel, Vec<(VectorId, u64)>>,
    pub(crate) smallest_centroids: HashMap<TreeLevel, Vec<(VectorId, u64)>>,
}

#[derive(Debug)]
pub(crate) struct IndexerOpts {
    pub(crate) dimensions: usize,
    pub(crate) distance_metric: DistanceMetric,
    pub(crate) root_threshold_vectors: usize,
    pub(crate) merge_threshold_vectors: usize,
    pub(crate) split_threshold_vectors: usize,
    pub(crate) split_search_neighbourhood: usize,
    pub(crate) indexed_fields: HashSet<String>,
}

pub(crate) struct IndexUpdateResults {
    pub(crate) ops: Vec<RecordOp>,
    pub(crate) centroid_cache: Arc<AllCentroidsCache>,
    pub(crate) leaf_centroids: usize,
    pub(crate) centroid_tree_depth: TreeDepth,
}

pub(crate) struct Indexer {
    opts: Arc<IndexerOpts>,
    state: VectorIndexState,
}

impl Indexer {
    pub(crate) fn new(opts: IndexerOpts, state: VectorIndexState) -> Self {
        info!("create indexer with opts: {:?}", opts);
        Self {
            opts: Arc::new(opts),
            state,
        }
    }

    pub(crate) fn state(&self) -> &VectorIndexState {
        &self.state
    }

    fn leaf_centroid_count(&self) -> usize {
        self.state
            .centroids()
            .values()
            .filter(|centroid| centroid.level == 1)
            .count()
    }

    fn query_centroid_cache(&self) -> Arc<AllCentroidsCache> {
        Arc::new(self.state.centroid_cache())
    }

    #[allow(clippy::type_complexity)]
    fn compute_centroid_size_stats(
        &self,
    ) -> (
        HashMap<TreeLevel, Vec<(VectorId, u64)>>,
        HashMap<TreeLevel, Vec<(VectorId, u64)>>,
    ) {
        let depth = TreeDepth::of(self.state.centroids_meta().depth);
        let mut largest = HashMap::new();
        let mut smallest = HashMap::new();

        for level in LEAF_LEVEL..=depth.max_inner_level() {
            let tree_level = TreeLevel::inner(level, depth);
            let mut counts = self
                .state
                .centroid_counts()
                .get(&level)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();

            counts.sort_by(|(a_id, a_count), (b_id, b_count)| {
                b_count.cmp(a_count).then_with(|| a_id.cmp(b_id))
            });
            let top = counts.iter().take(10).copied().collect::<Vec<_>>();
            largest.insert(tree_level, top);

            counts.sort_by(|(a_id, a_count), (b_id, b_count)| {
                a_count.cmp(b_count).then_with(|| a_id.cmp(b_id))
            });
            let bottom = counts.iter().take(10).copied().collect::<Vec<_>>();
            smallest.insert(tree_level, bottom);
        }

        (largest, smallest)
    }

    pub(crate) async fn update_index(
        &mut self,
        updates: Vec<VectorWrite>,
        update_epoch: u64,
        snapshot: Arc<dyn StorageRead>,
        snapshot_epoch: u64,
    ) -> Result<IndexUpdateResults> {
        let update_span = debug_span!(
            "update_index",
            update_epoch,
            snapshot_epoch,
            write_count = updates.len(),
            depth = self.state.centroids_meta().depth as u16
        );
        let update_start = Instant::now();
        let mut stats = IndexerStats::default();
        let mut delta = VectorIndexDelta::new(&self.state);

        let write = WriteVectors::new(&self.opts, &snapshot, snapshot_epoch, updates);
        let write_start = Instant::now();
        let (inserts, updates) = write.execute(&self.state, &mut delta).await?;
        debug!(
            parent: &update_span,
            op = "write_vectors",
            elapsed_ms = elapsed_ms(write_start),
            inserts,
            updates,
            "completed"
        );
        stats.inserts = inserts;
        stats.updates = updates;

        let depth = TreeDepth::of(self.state.centroids_meta().depth);
        let mut next_level = TreeLevel::leaf(depth);
        loop {
            let level = next_level;
            if level.is_root() {
                break;
            }
            next_level = level.next_level_up();
            let merge = MergeCentroids::new(&self.opts, level, &snapshot, snapshot_epoch);
            let merge_start = Instant::now();
            let (reassigns, merge_count) = merge.execute(&self.state, &mut delta).await?;
            debug!(
                parent: &update_span,
                op = "merge_centroids",
                level = &format!("{}", level),
                elapsed_ms = elapsed_ms(merge_start),
                merge_count,
                reassignment_count = reassigns.len(),
                "completed"
            );
            *stats.merges.entry(level).or_default() += merge_count;

            let reassign_after_merge_start = Instant::now();
            let reassign_after_merge_count = self
                .reassign_vectors(&snapshot, snapshot_epoch, &mut delta, reassigns)
                .await?;
            *stats.reassignments.entry(level).or_default() += reassign_after_merge_count;
            debug!(
                parent: &update_span,
                op = "reassign_vectors_after_merge",
                level = &format!("{}", level),
                elapsed_ms = elapsed_ms(reassign_after_merge_start),
                reassigned_count = reassign_after_merge_count,
                "completed"
            );

            let mut split_round = 0usize;
            loop {
                let view = VectorIndexView::new(&delta, &self.state, &snapshot, snapshot_epoch);
                if split_round >= 1000 {
                    let counts = view.centroid_counts(level);
                    let mut counts = counts
                        .into_iter()
                        .map(|(cid, c)| (c, cid))
                        .collect::<Vec<_>>();
                    counts.sort();
                    let ncounts = counts.len();
                    let back = ncounts - 100;
                    error!(
                        msg = "splits seem to be in infinite loop",
                        level = &format!("{}", level),
                        ncentroids = ncounts,
                        counts_back = &format!("{:?}", &counts[back..]),
                        counts_front = &format!("{:?}", &counts[..100]),
                    );
                    panic!("split infinite loop");
                }
                let split = SplitCentroids::new(&self.opts, level, &snapshot, snapshot_epoch);
                let split_start = Instant::now();
                let result = split.execute(&self.state, &mut delta).await?;
                if !result.imbalanced.is_empty() {
                    warn!(
                        parent: &update_span,
                        op = "split_centroids_imbalanced",
                        level = &format!("{}", level),
                        split_round,
                        imbalanced = ?result.imbalanced,
                        "kmeans produced imbalanced clusters"
                    );
                }
                debug!(
                    parent: &update_span,
                    op = "split_centroids",
                    level = &format!("{}", level),
                    split_round,
                    elapsed_ms = elapsed_ms(split_start),
                    split_count = result.splits.len(),
                    reassignment_count = result.reassignments.len(),
                    reassign_candidates_evaluated = result.candidates_evaluated,
                    reassign_candidates_returned = result.candidates_returned,
                    "completed"
                );
                *stats.splits.entry(level).or_default() += result.splits.len();
                if result.splits.is_empty() {
                    break;
                }
                let reassign_after_split_start = Instant::now();
                let reassign_after_split_count = self
                    .reassign_vectors(&snapshot, snapshot_epoch, &mut delta, result.reassignments)
                    .await?;
                *stats.reassignments.entry(level).or_default() += reassign_after_split_count;
                debug!(
                    parent: &update_span,
                    op = "reassign_vectors_after_split",
                    level = &format!("{}", level),
                    split_round,
                    elapsed_ms = elapsed_ms(reassign_after_split_start),
                    reassigned_count = reassign_after_split_count,
                    "completed"
                );
                split_round += 1;
            }
        }

        let split_root = SplitRoot::new(&self.opts, &snapshot, snapshot_epoch);
        let split_root_start = Instant::now();
        split_root.execute(&mut delta, &self.state).await?;
        debug!(
            parent: &update_span,
            op = "split_root",
            elapsed_ms = elapsed_ms(split_root_start),
            "completed"
        );

        let freeze_start = Instant::now();
        let ops = delta.freeze(update_epoch, &mut self.state);
        debug!(
            parent: &update_span,
            op = "freeze_delta",
            update_epoch,
            elapsed_ms = elapsed_ms(freeze_start),
            record_op_count = ops.len(),
            "completed"
        );
        let (largest_centroids, smallest_centroids) = self.compute_centroid_size_stats();
        stats.largest_centroids = largest_centroids;
        stats.smallest_centroids = smallest_centroids;
        info!(
            parent: &update_span,
            elapsed_ms = elapsed_ms(update_start),
            leaf_centroid_count = self.leaf_centroid_count(),
            stats = format!("{:?}", stats),
            "completed"
        );
        let root = self.state.centroid_cache().root(u64::MAX).unwrap();
        info!(
            "root: {:?}",
            root.iter().map(|p| p.id()).collect::<Vec<_>>()
        );
        let root_sz = self.state.centroid_cache().root(u64::MAX).unwrap().len();
        info!("ROOT SIZE: {}", root_sz);
        Ok(IndexUpdateResults {
            ops,
            centroid_cache: self.query_centroid_cache(),
            leaf_centroids: self.leaf_centroid_count(),
            centroid_tree_depth: TreeDepth::of(self.state.centroids_meta().depth),
        })
    }

    #[cfg(debug_assertions)]
    pub(crate) async fn validate(&self, snapshot: Arc<dyn StorageSnapshot>) {
        validator::validate(snapshot, &self.state, self.opts.dimensions)
            .await
            .expect("validation failed");
    }

    async fn reassign_vectors(
        &self,
        snapshot: &Arc<dyn StorageRead>,
        snapshot_epoch: u64,
        delta: &mut VectorIndexDelta,
        reassigns: Vec<ReassignVector>,
    ) -> Result<usize> {
        let total_reassign_count = reassigns.len();
        let reassign_start = Instant::now();
        let mut total_reassigned = 0;
        let mut reassigns_by_level: HashMap<TreeLevel, Vec<ReassignVector>> = HashMap::new();
        for r in reassigns {
            let entry = reassigns_by_level.entry(r.level).or_default();
            entry.push(r);
        }
        let depth = TreeDepth::of(self.state.centroids_meta().depth);
        let mut next_level = TreeLevel::leaf(depth);
        loop {
            let level = next_level;
            if level.is_root() {
                break;
            }
            next_level = next_level.next_level_up();
            let Some(reassigns) = reassigns_by_level.remove(&level) else {
                continue;
            };
            let level_reassign_count = reassigns.len();
            let reassign =
                ReassignVectors::new(&self.opts, snapshot, snapshot_epoch, reassigns, level);
            let level_start = Instant::now();
            let reassigned = reassign.execute(&self.state, delta).await?;
            total_reassigned += reassigned;
            debug!(
                level = &format!("{}", level),
                snapshot_epoch,
                requested_reassignments = level_reassign_count,
                reassigned_count = reassigned,
                elapsed_ms = elapsed_ms(level_start),
                op = "reassign_vectors_level",
                "completed"
            );
        }
        assert!(reassigns_by_level.is_empty());
        debug!(
            snapshot_epoch,
            requested_reassignments = total_reassign_count,
            reassigned_count = total_reassigned,
            elapsed_ms = elapsed_ms(reassign_start),
            op = "reassign_vectors",
            "completed"
        );
        Ok(total_reassigned)
    }
}

fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

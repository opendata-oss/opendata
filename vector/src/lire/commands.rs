#![allow(unused)]

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Write};
use std::sync::Arc;
use log::info;
use tracing::{debug, warn};

use crate::delta::{VectorDbDeltaView, VectorDbWriteDelta};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::posting_list::{PostingList, PostingUpdate};
use crate::storage::VectorDbStorageReadExt;
use crate::storage::record;
use common::coordinator::Delta;
use common::storage::RecordOp;
use crate::lire::commands::RebalanceCommand::SplitSweep;

/// Commands sent by [`crate::lire::rebalancer::IndexRebalancer`] to [`VectorDbWriteDelta`]
/// via [`common::coordinator::WriteCoordinator`] to execute steps of rebalance operations.
pub(crate) enum RebalanceCommand {
    Split(SplitCommand),
    SplitSweep(SplitSweepCommand),
    SplitReassign(SplitReassignCommand),
    Merge(MergeCommand),
    MergeSweep(MergeSweepCommand),
    MergeReassign(MergeReassignCommand),
    FinishSplit(FinishSplitCommand),
    FinishMerge(FinishMergeCommand),
}

impl Debug for RebalanceCommand {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RebalanceCommand::Split(_) => write!(f, "RebalanceCommand::Split"),
            SplitSweep(_) => write!(f, "RebalanceCommand::SplitSweep"),
            RebalanceCommand::SplitReassign(_) => write!(f, "RebalanceCommand::SplitReassign"),
            RebalanceCommand::Merge(_) => write!(f, "RebalanceCommand::Merge"),
            RebalanceCommand::MergeSweep(_) => write!(f, "RebalanceCommand::MergeSweep"),
            RebalanceCommand::MergeReassign(_) => write!(f, "RebalanceCommand::MergeReassign"),
            RebalanceCommand::FinishSplit(_) => write!(f, "RebalanceCommand::FinishSplit"),
            RebalanceCommand::FinishMerge(_) => write!(f, "RebalanceCommand::FinishMerge"),
        };
        Ok(())
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub(crate) struct CentroidPostings {
    centroid_id: u64,
    postings: PostingList,
}

impl CentroidPostings {
    pub(crate) fn new(centroid_id: u64, postings: PostingList) -> Self {
        Self {
            centroid_id,
            postings,
        }
    }

    pub(crate) fn centroid_id(&self) -> u64 {
        self.centroid_id
    }

    pub(crate) fn postings(self) -> PostingList {
        self.postings
    }

    pub(crate) fn postings_ref(&self) -> &PostingList {
        &self.postings
    }
}

/// Instructs the write coordinator to split a given centroid c into 2 new centroids c0 and c1.
/// After this command is executed, c is removed from the centroid graph and c0/c1 are added.
/// The task state transitions to SWEEP.
#[derive(Debug)]
pub(crate) struct SplitCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The id of the centroid being split. After processing this command, c will be removed from
    /// the graph.
    c: u64,
    /// The centroid c0 being created from c, with its initial postings.
    c0: SplitPostings,
    /// The centroid c1 being created from c, with its initial postings.
    c1: SplitPostings,
}

impl SplitCommand {
    pub(crate) fn new(task_id: u64, c: u64, c0: SplitPostings, c1: SplitPostings) -> Self {
        Self { task_id, c, c0, c1 }
    }
}

#[derive(Clone)]
pub(crate) struct SplitCommandResult {
    pub(crate) c0_id: u64,
    pub(crate) c1_id: u64,
}

/// Sent by the index rebalancer after the SplitCommand is flushed to storage. The index rebalancer
/// scans c's postings for vectors written between computing and applying the original split, and
/// updates c0 and c1 postings with these updates. After this command is applied, the task
/// transitions to REASSIGN
#[derive(Debug)]
pub(crate) struct SplitSweepCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The original centroid that was split (needed for metadata merge deltas).
    c: u64,
    /// New postings to be added to c0
    c0: CentroidPostings,
    /// New postings to be added to c1
    c1: CentroidPostings,
}

impl SplitSweepCommand {
    pub(crate) fn new(task_id: u64, c: u64, c0: CentroidPostings, c1: CentroidPostings) -> Self {
        Self { task_id, c, c0, c1 }
    }
}

#[derive(Debug)]
pub(crate) struct VectorReassignment {
    pub(crate) target_centroid_ids: Vec<u64>,
    pub(crate) source_centroid_id: u64,
    pub(crate) vector_id: u64,
    pub(crate) vector: Vec<f32>,
}

impl VectorReassignment {
    pub(crate) fn new(
        target_centroid_ids: Vec<u64>,
        source_centroid_id: u64,
        vector_id: u64,
        vector: Vec<f32>,
    ) -> Self {
        Self {
            target_centroid_ids,
            source_centroid_id,
            vector_id,
            vector,
        }
    }
}

/// Sent by the index rebalancer after the SplitSweepCommand is applied (does not need to block on
/// flush). After the SplitSweepCommand is applied, the rebalancer computes a list of vectors that
/// need to be reassigned, and specifies them in this command. The write coordinator executes the
/// reassignments by adding a `PostingUpdate` with type `Append` to the target posting list, and
/// a `PostingUpdate` with type `Delete` in the source posting list. After this command is applied,
/// the task is complete and its state is deleted. Centroid c is deleted from the set of centroids.
#[derive(Debug)]
pub(crate) struct SplitReassignCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The list of vectors that must be reassigned.
    reassignments: Vec<VectorReassignment>,
}

impl SplitReassignCommand {
    pub(crate) fn new(task_id: u64, reassignments: Vec<VectorReassignment>) -> Self {
        Self {
            task_id,
            reassignments,
        }
    }
}

/// Instructs the write coordinator to initiate a merge of centroid c_from into centroid c_to. After
/// this command is applied, the write coordinator transitions c_from to DRAINING and the task
/// is created and its state set to SWEEP.
#[derive(Debug)]
pub(crate) struct MergeCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The id of the centroid that is the target of the merge.
    c_to: u64,
    /// The id and postings of the centroid that is to be merged into c_to.
    c_from: CentroidPostings,
}

impl MergeCommand {
    pub(crate) fn new(task_id: u64, c_to: u64, c_from: CentroidPostings) -> Self {
        Self {
            task_id,
            c_to,
            c_from,
        }
    }
}

/// Sent by the index rebalancer after the MergeCommand is durably flushed (so that the rebalancer
/// can read from the latest snapshot). The index rebalancer scans c_from's postings for vectors
/// written between computing and applying the original merge, and updates c_to's postings with
/// these updates. After this command is applied, the task transitions to REASSIGN
#[derive(Debug)]
pub(crate) struct MergeSweepCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The original centroid that was merged away (needed for metadata merge deltas).
    c_from: u64,
    /// The postings to be added to centroid c_to
    c_to_postings: CentroidPostings,
}

impl MergeSweepCommand {
    pub(crate) fn new(task_id: u64, c_from: u64, c_to_postings: CentroidPostings) -> Self {
        Self {
            task_id,
            c_from,
            c_to_postings,
        }
    }
}

/// Sent by the index rebalancer after the MergeSweepCommand is applied (does not need to block on
/// flush). After the MergeSweepCommand is applied, the index rebalancer computes reassignments
/// for all of c_other's postings that need it, and sends these to the write coordinator to
/// execute. The write coordinator executes the reassignments by adding a `PostingUpdate` with type
/// `Append` to the target posting list, and a `PostingUpdate` with type `Delete` in the source
/// posting list.After this command is applied, the task and c are deleted.
#[derive(Debug)]
pub(crate) struct MergeReassignCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    reassignments: Vec<VectorReassignment>,
}

impl MergeReassignCommand {
    pub(crate) fn new(task_id: u64, reassignments: Vec<VectorReassignment>) -> Self {
        Self {
            task_id,
            reassignments,
        }
    }
}

/// Sent by the index rebalancer after a split operation completes (or exits early).
/// Cleans up rebalance_participants for the centroids involved in the split.
#[derive(Debug)]
pub(crate) struct FinishSplitCommand {
    pub(crate) task_id: u64,
    pub(crate) c0: Option<u64>,
    pub(crate) c1: Option<u64>,
}

impl FinishSplitCommand {
    pub(crate) fn new(task_id: u64, c0: Option<u64>, c1: Option<u64>) -> Self {
        Self { task_id, c0, c1 }
    }
}

/// Sent by the index rebalancer after a merge operation completes (or exits early).
/// Cleans up rebalance_participants for the centroids involved in the merge.
#[derive(Debug)]
pub(crate) struct FinishMergeCommand {
    pub(crate) task_id: u64,
    c_to: u64,
}

impl FinishMergeCommand {
    pub(crate) fn new(task_id: u64, c_to: u64) -> Self {
        Self { task_id, c_to }
    }
}

/// Deduplicate and accumulate postings from a `CentroidPostings` into the view.
/// Filters out vector IDs that already have a posting update for the given centroid.
fn dedup_and_accumulate(view: &mut VectorDbDeltaView, cp: CentroidPostings) -> u64 {
    let centroid_id = cp.centroid_id();
    let existing_ids: HashSet<u64> = view
        .posting_updates
        .get(&centroid_id)
        .map(|updates| updates.iter().map(|u| u.id()).collect())
        .unwrap_or_default();
    let mut count = 0u64;
    for posting in cp.postings() {
        let (id, v) = posting.unpack();
        if !existing_ids.contains(&id) {
            view.add_to_posting(centroid_id, id, v);
            count += 1;
        }
    }
    count
}

impl VectorDbWriteDelta {
    pub(crate) fn apply_rebalance_cmd(
        &mut self,
        cmd: RebalanceCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        debug!("Applying rebalance command: {:?}", cmd);
        match cmd {
            RebalanceCommand::Split(cmd) => self.apply_split_cmd(cmd),
            RebalanceCommand::SplitSweep(cmd) => self.apply_split_sweep_cmd(cmd),
            RebalanceCommand::SplitReassign(cmd) => self.apply_split_reassign_cmd(cmd),
            RebalanceCommand::Merge(cmd) => self.apply_merge_cmd(cmd),
            RebalanceCommand::MergeSweep(cmd) => self.apply_merge_sweep_cmd(cmd),
            RebalanceCommand::MergeReassign(cmd) => self.apply_merge_reassign_cmd(cmd),
            RebalanceCommand::FinishSplit(cmd) => self.apply_finish_split(cmd),
            RebalanceCommand::FinishMerge(cmd) => self.apply_finish_merge(cmd),
        }
    }

    pub(crate) fn apply_split_cmd(
        &mut self,
        cmd: SplitCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        let (c0_id, seq_alloc_put) = self.ctx.id_allocator.allocate_one();
        if let Some(seq_alloc_put) = seq_alloc_put {
            self.ops.push(RecordOp::Put(seq_alloc_put));
        }
        let (c1_id, seq_alloc_put) = self.ctx.id_allocator.allocate_one();
        if let Some(seq_alloc_put) = seq_alloc_put {
            self.ops.push(RecordOp::Put(seq_alloc_put));
        }
        let c0 = CentroidEntry::new(c0_id, cmd.c0.centroid_vec);
        let c0_postings = cmd.c0.postings;
        let c1 = CentroidEntry::new(c1_id, cmd.c1.centroid_vec);
        let c1_postings = cmd.c1.postings;

        // Write new centroids to centroid chunks
        let mut chunk_batches: HashMap<u32, Vec<CentroidEntry>> = HashMap::new();
        for entry in [&c0, &c1] {
            if self.ctx.current_chunk_count >= self.ctx.opts.chunk_target {
                self.ctx.current_chunk_id += 1;
                self.ctx.current_chunk_count = 0;
            }
            chunk_batches
                .entry(self.ctx.current_chunk_id)
                .or_default()
                .push(entry.clone());
            self.ctx.current_chunk_count += 1;
        }
        for (chunk_id, entries) in chunk_batches {
            self.ops.push(record::merge_centroid_chunk(
                chunk_id,
                entries,
                self.ctx.opts.dimensions,
            ));
        }

        // 1. Remove c from the centroid graph
        self.ctx
            .centroid_graph
            .remove_centroid(cmd.c)
            .map_err(|e| e.to_string())?;

        // 2. Add c0 and c1 to centroid graph
        self.ctx
            .centroid_graph
            .add_centroid(&c0)
            .map_err(|e| e.to_string())?;
        self.ctx
            .centroid_graph
            .add_centroid(&c1)
            .map_err(|e| e.to_string())?;

        // Collect vector IDs before consuming postings (needed for metadata merges)
        let c0_vector_ids: Vec<u64> = c0_postings.iter().map(|p| p.id()).collect();
        let c1_vector_ids: Vec<u64> = c1_postings.iter().map(|p| p.id()).collect();

        let mut view = self.view.write().expect("lock poisoned");

        // Mark the old centroid as deleted in the deletions bitmap
        view.deleted_centroids.insert(cmd.c);

        // 4. Accumulate PostingUpdates for c0
        let c0_count = c0_postings.len() as u64;
        for p in c0_postings {
            let (id, vector) = p.unpack();
            view.add_to_posting(c0_id, id, vector);
        }

        // 5. Accumulate PostingUpdates for c1
        let c1_count = c1_postings.len() as u64;
        for p in c1_postings {
            let (id, vector) = p.unpack();
            view.add_to_posting(c1_id, id, vector);
        }

        drop(view);

        // Update VectorIndexedMetadata: replace old centroid with new child
        if self.ctx.opts.max_cluster_replication > 1 {
            for &vid in &c0_vector_ids {
                self.ops.push(record::merge_vector_indexed_metadata(
                    vid,
                    &[cmd.c],
                    &[c0_id],
                ));
            }
            for &vid in &c1_vector_ids {
                self.ops.push(record::merge_vector_indexed_metadata(
                    vid,
                    &[cmd.c],
                    &[c1_id],
                ));
            }
        }

        // 6. Update centroid_counts: remove c, add c0 count, add c1 count
        self.ctx.rebalancer.drop_centroid(cmd.c);
        self.ctx
            .rebalancer
            .register_participants(cmd.task_id, &[c0_id, c1_id]);
        self.ctx
            .rebalancer
            .update_counts(&[(c0_id, c0_count as i32), (c1_id, c1_count as i32)]);

        Ok(Arc::new(SplitCommandResult { c0_id, c1_id }))
    }

    pub(crate) fn apply_split_sweep_cmd(
        &mut self,
        cmd: SplitSweepCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        let c0_centroid_id = cmd.c0.centroid_id();
        let c1_centroid_id = cmd.c1.centroid_id();

        // Collect vector IDs before consuming postings (needed for metadata merges)
        let c0_vector_ids: Vec<u64> = cmd.c0.postings_ref().iter().map(|p| p.id()).collect();
        let c1_vector_ids: Vec<u64> = cmd.c1.postings_ref().iter().map(|p| p.id()).collect();

        let mut view = self.view.write().expect("lock poisoned");

        let c0_added = dedup_and_accumulate(&mut view, cmd.c0);
        let c1_added = dedup_and_accumulate(&mut view, cmd.c1);

        drop(view);

        self.ctx.rebalancer.update_counts(&[
            (c0_centroid_id, c0_added as i32),
            (c1_centroid_id, c1_added as i32),
        ]);

        // Update VectorIndexedMetadata for swept vectors
        if self.ctx.opts.max_cluster_replication > 1 {
            for &vid in &c0_vector_ids {
                self.ops.push(record::merge_vector_indexed_metadata(
                    vid,
                    &[cmd.c],
                    &[c0_centroid_id],
                ));
            }
            for &vid in &c1_vector_ids {
                self.ops.push(record::merge_vector_indexed_metadata(
                    vid,
                    &[cmd.c],
                    &[c1_centroid_id],
                ));
            }
        }

        Ok(Arc::new(()))
    }

    pub(crate) fn apply_split_reassign_cmd(
        &mut self,
        cmd: SplitReassignCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        self.apply_reassignments(cmd.reassignments)
    }

    pub(crate) fn apply_finish_split(
        &mut self,
        cmd: FinishSplitCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        self.ctx
            .rebalancer
            .finish_split(cmd.task_id, cmd.c0, cmd.c1);
        Ok(Arc::new(()))
    }

    fn apply_reassignments(
        &mut self,
        reassignments: Vec<VectorReassignment>,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        // Read VectorIndexedMetadata for all reassigned vectors so we can clean up
        // all replicated postings (not just the source centroid).
        let metadata_map: HashMap<u64, Vec<u64>> = if self.ctx.opts.max_cluster_replication > 1 {
            let view = self.ctx.rebalancer.view();
            let vector_ids: Vec<u64> = reassignments.iter().map(|r| r.vector_id).collect();

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let futs: Vec<_> = vector_ids
                        .iter()
                        .map(|&vid| {
                            let snap = view.snapshot.clone();
                            async move {
                                let meta = snap.get_vector_indexed_metadata(vid).await;
                                (vid, meta)
                            }
                        })
                        .collect();
                    let results = futures::future::join_all(futs).await;
                    results
                        .into_iter()
                        .filter_map(|(vid, r)| r.ok().flatten().map(|m| (vid, m.centroid_ids)))
                        .collect()
                })
            })
        } else {
            HashMap::new()
        };

        let mut view = self.view.write().expect("lock poisoned");

        for reassignment in reassignments {
            let source_exists = self
                .ctx
                .centroid_graph
                .get_centroid_vector(reassignment.source_centroid_id)
                .is_some();
            if !source_exists {
                // vector was from a centroid involved in a racing split/merge. skip
                continue;
            }

            // Validate target centroid(s). If any target no longer exists, recompute
            // the full assignment using the centroid graph.
            let target_ids = if reassignment
                .target_centroid_ids
                .iter()
                .all(|&tid| self.ctx.centroid_graph.get_centroid_vector(tid).is_some())
            {
                reassignment.target_centroid_ids.clone()
            } else {
                // One or more targets are stale (concurrent split/merge). Recompute.
                crate::distance::assign_to_centroids(
                    &reassignment.vector,
                    self.ctx.centroid_graph.as_ref(),
                    self.ctx.opts.max_cluster_replication,
                    self.ctx.opts.distance_metric,
                )
            };

            if target_ids.is_empty() {
                continue;
            }

            let target_set: HashSet<u64> = target_ids.iter().copied().collect();

            // Delete from all old centroids that are NOT in the new target set.
            if let Some(old_centroids) = metadata_map.get(&reassignment.vector_id) {
                for &old_cid in old_centroids {
                    if !target_set.contains(&old_cid)
                        && self
                            .ctx
                            .centroid_graph
                            .get_centroid_vector(old_cid)
                            .is_some()
                    {
                        view.delete_from_posting(old_cid, reassignment.vector_id);
                        self.ctx.rebalancer.update_counts(&[(old_cid, -1)]);
                    }
                }
                // Also delete from source if not already in metadata (handles in-delta
                // staleness where source was created by a split in the same delta)
                if !old_centroids.contains(&reassignment.source_centroid_id)
                    && !target_set.contains(&reassignment.source_centroid_id)
                {
                    view.delete_from_posting(
                        reassignment.source_centroid_id,
                        reassignment.vector_id,
                    );
                    self.ctx
                        .rebalancer
                        .update_counts(&[(reassignment.source_centroid_id, -1)]);
                }
            } else if !target_set.contains(&reassignment.source_centroid_id) {
                view.delete_from_posting(reassignment.source_centroid_id, reassignment.vector_id);
                self.ctx
                    .rebalancer
                    .update_counts(&[(reassignment.source_centroid_id, -1)]);
            }

            // Append to each target centroid's posting list (skip if already present
            // from old centroids to avoid duplicates).
            let old_centroids = metadata_map.get(&reassignment.vector_id);
            for &tid in &target_ids {
                let already_assigned = old_centroids
                    .map(|oc| oc.contains(&tid))
                    .unwrap_or(tid == reassignment.source_centroid_id);
                if !already_assigned {
                    view.add_to_posting(tid, reassignment.vector_id, reassignment.vector.clone());
                    self.ctx.rebalancer.update_counts(&[(tid, 1)]);
                }
            }

            // Update VectorIndexedMetadata to contain the new targets
            if self.ctx.opts.max_cluster_replication > 1 {
                self.ops.push(record::put_vector_indexed_metadata(
                    reassignment.vector_id,
                    &target_ids,
                ));
            }
        }

        Ok(Arc::new(()))
    }

    pub(crate) fn apply_merge_cmd(
        &mut self,
        cmd: MergeCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        let c_from_id = cmd.c_from.centroid_id();

        // Collect vector IDs before consuming postings (needed for metadata merges)
        let c_from_vector_ids: Vec<u64> =
            cmd.c_from.postings_ref().iter().map(|p| p.id()).collect();

        let c_from_postings = cmd.c_from.postings();

        // 1. Remove c_other from centroid graph
        self.ctx
            .centroid_graph
            .remove_centroid(c_from_id)
            .map_err(|e| e.to_string())?;

        let mut view = self.view.write().expect("lock poisoned");

        // 2. Mark c_other as deleted
        view.deleted_centroids.insert(c_from_id);

        // 3. Move all postings from c_other into c
        let c_other_count = c_from_postings.len() as u64;
        for p in c_from_postings {
            let (id, vector) = p.unpack();
            view.add_to_posting(cmd.c_to, id, vector);
        }

        drop(view);

        // 4. Update centroid_counts: add c_other's count to c, remove c_other
        self.ctx
            .rebalancer
            .update_counts(&[(cmd.c_to, c_other_count as i32)]);
        self.ctx.rebalancer.drop_centroid(c_from_id);

        // Update VectorIndexedMetadata: replace c_from with c_to
        if self.ctx.opts.max_cluster_replication > 1 {
            for &vid in &c_from_vector_ids {
                self.ops.push(record::merge_vector_indexed_metadata(
                    vid,
                    &[c_from_id],
                    &[cmd.c_to],
                ));
            }
        }

        Ok(Arc::new(()))
    }

    pub(crate) fn apply_merge_sweep_cmd(
        &mut self,
        cmd: MergeSweepCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        let c_to_id = cmd.c_to_postings.centroid_id();

        // Collect vector IDs before consuming postings (needed for metadata merges)
        let swept_vector_ids: Vec<u64> = cmd
            .c_to_postings
            .postings_ref()
            .iter()
            .map(|p| p.id())
            .collect();

        let mut view = self.view.write().expect("lock poisoned");

        let added = dedup_and_accumulate(&mut view, cmd.c_to_postings);

        drop(view);

        self.ctx
            .rebalancer
            .update_counts(&[(c_to_id, added as i32)]);

        // Update VectorIndexedMetadata for swept vectors
        if self.ctx.opts.max_cluster_replication > 1 {
            for &vid in &swept_vector_ids {
                self.ops.push(record::merge_vector_indexed_metadata(
                    vid,
                    &[cmd.c_from],
                    &[c_to_id],
                ));
            }
        }

        Ok(Arc::new(()))
    }

    pub(crate) fn apply_merge_reassign_cmd(
        &mut self,
        cmd: MergeReassignCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        self.apply_reassignments(cmd.reassignments)
    }

    pub(crate) fn apply_finish_merge(
        &mut self,
        cmd: FinishMergeCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        self.ctx.rebalancer.finish_merge(cmd.task_id, cmd.c_to);
        Ok(Arc::new(()))
    }
}

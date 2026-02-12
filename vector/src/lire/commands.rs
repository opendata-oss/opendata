#![allow(unused)]

use crate::delta::VectorDbWriteDelta;
use crate::serde::centroid_chunk::CentroidEntry;
use common::coordinator::Delta;
use std::any::Any;
use std::sync::Arc;

/// Commands sent by [`crate::lire::rebalancer::IndexRebalancer`] to [`VectorDbWriteDelta`]
/// via [`common::coordinator::WriteCoordinator`] to execute steps of rebalance operations.
pub(crate) enum RebalanceCommand {
    Split(SplitCommand),
    SplitSweep(SplitSweepCommand),
    SplitReassign(SplitReassignCommand),
    Merge(MergeCommand),
    MergeSweep(MergeSweepCommand),
    MergeReassign(MergeReassignCommand),
}

pub(crate) type Postings = Vec<(u64, Vec<f32>)>;

pub(crate) struct CentroidPostings {
    c: CentroidEntry,
    vectors: Vec<Postings>,
}

/// Instructs the write coordinator to split a given centroid c into 2 new centroids c0 and c1.
/// After this command is executed, the task is initialized and its state set to SWEEP
pub(crate) struct SplitCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The centroid being split. After processing this command, c will be marked DRAINING
    c: CentroidEntry,
    /// The centroids being created from c. After processing this command, c0 and c1 will be
    /// marked ACTIVE and their postings initialized with the vectors from c specified in
    /// the provided postings.
    c0: CentroidPostings,
    c1: CentroidPostings,
}

/// Sent by the index rebalancer after the SplitCommand is flushed to storage. The index rebalancer
/// scans c's postings for vectors written between computing and applying the original split, and
/// updates c0 and c1 postings with these updates. After this command is applied, the task
/// transitions to REASSIGN
pub(crate) struct SplitSweepCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// New postings to be added to c0
    c0: CentroidPostings,
    /// New postings to be added to c1
    c1: CentroidPostings,
}

pub(crate) struct VectorReassignment {
    target_centroid_id: u64,
    source_centroid_id: u64,
    vector_id: u64,
}

/// Sent by the index rebalancer after the SplitSweepCommand is applied (does not need to block on
/// flush). After the SplitSweepCommand is applied, the rebalancer computes a list of vectors that
/// need to be reassigned, and specifies them in this command. The write coordinator executes the
/// reassignments by adding a `PostingUpdate` with type `Append` to the target posting list, and
/// a `PostingUpdate` with type `Delete` in the source posting list. After this command is applied,
/// the task is complete and its state is deleted. Centroid c is deleted from the set of centroids.
pub(crate) struct SplitReassignCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The list of vectors that must be reassigned.
    reassignments: Vec<VectorReassignment>,
}

/// Instructs the write coordinator to initiate a merge of centroid c_other into centroid c. After
/// this command is applied, the write coordinator transitions c_other to DRAINING and the task
/// is created and its state set to SWEEP.
pub(crate) struct MergeCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The id of the centroid that is the target of the merge.
    c: u64,
    /// The id and postings of the centroid that is to be merged into c.
    c_other: CentroidPostings,
}

/// Sent by the index rebalancer after the MergeCommand is durably flushed (so that the rebalancer
/// can read from the latest snapshot). The index rebalancer scans c_other's postings for vectors
/// written between computing and applying the original merge, and updates c's postings with
/// these updates. After this command is applied, the task transitions to REASSIGN
pub(crate) struct MergeSweepCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    /// The postings to be added to centroid c
    c_other: CentroidPostings,
}

/// Sent by the index rebalancer after the MergeSweepCommand is applied (does not need to block on
/// flush). After the MergeSweepCommand is applied, the index rebalancer computes reassignments
/// for all of c_other's postings that need it, and sends these to the write coordinator to
/// execute. The write coordinator executes the reassignments by adding a `PostingUpdate` with type
/// `Append` to the target posting list, and a `PostingUpdate` with type `Delete` in the source
/// posting list.After this command is applied, the task and c are deleted.
pub(crate) struct MergeReassignCommand {
    /// Unique task ID associated with this task
    task_id: u64,
    reassignments: Vec<VectorReassignment>,
}

impl VectorDbWriteDelta {
    pub(crate) fn apply_rebalance_cmd(
        &mut self,
        cmd: RebalanceCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        match cmd {
            RebalanceCommand::Split(cmd) => self.apply_split_cmd(cmd),
            RebalanceCommand::SplitSweep(cmd) => self.apply_split_sweep_cmd(cmd),
            RebalanceCommand::SplitReassign(cmd) => self.apply_split_reassign_cmd(cmd),
            RebalanceCommand::Merge(cmd) => self.apply_merge_cmd(cmd),
            RebalanceCommand::MergeSweep(cmd) => self.apply_merge_sweep_cmd(cmd),
            RebalanceCommand::MergeReassign(cmd) => self.apply_merge_reassign_cmd(cmd),
        }
    }

    pub(crate) fn apply_split_cmd(
        &mut self,
        cmd: SplitCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        todo!()
    }

    pub(crate) fn apply_split_sweep_cmd(
        &mut self,
        cmd: SplitSweepCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        todo!()
    }

    pub(crate) fn apply_split_reassign_cmd(
        &mut self,
        cmd: SplitReassignCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        todo!()
    }

    pub(crate) fn apply_merge_cmd(
        &mut self,
        cmd: MergeCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        todo!()
    }

    pub(crate) fn apply_merge_sweep_cmd(
        &mut self,
        cmd: MergeSweepCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        todo!()
    }

    pub(crate) fn apply_merge_reassign_cmd(
        &mut self,
        cmd: MergeReassignCommand,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        todo!()
    }
}

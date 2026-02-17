#![allow(unused)]

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tracing::{debug, info, trace, warn};

use crate::delta::{VectorDbWrite, VectorDbWriteDelta};
use crate::distance;
use crate::hnsw::CentroidGraph;
use crate::lire::commands::{
    CentroidPostings, MergeCommand, MergeFinishCommand, MergeReassignCommand, MergeSweepCommand,
    RebalanceCommand, SplitCommand, SplitCommandResult, SplitFinishCommand, SplitPostings,
    SplitReassignCommand, SplitSweepCommand, VectorReassignment,
};
use crate::lire::heuristics;
use crate::lire::kmeans;
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;
use crate::serde::posting_list::{Posting, PostingList};
use crate::storage::VectorDbStorageReadExt;
use crate::view_reader::ViewReader;
use common::coordinator::{Durability, WriteCoordinatorHandle};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Configuration options for the index rebalancer.
#[derive(Clone)]
pub(crate) struct IndexRebalancerOpts {
    pub(crate) dimensions: usize,
    pub(crate) distance_metric: DistanceMetric,
    pub(crate) split_search_neighbourhood: usize,
    pub(crate) split_threshold_vectors: usize,
    pub(crate) merge_threshold_vectors: usize,
    pub(crate) max_rebalance_tasks: usize,
}

struct RebalanceTask {
    participants: HashSet<u64>,
}

struct RebalanceTasks {
    tasks: Mutex<HashMap<u64, RebalanceTask>>,
}

impl RebalanceTasks {
    fn new() -> Self {
        Self {
            tasks: Mutex::new(HashMap::new()),
        }
    }

    fn register(&self, id: u64, participants: &[u64]) {
        let mut tasks = self.tasks.lock().unwrap();
        let exists = tasks.contains_key(&id);
        if exists {
            drop(tasks);
            panic!("Rebalance task already registered");
        }
        debug!(
            "registering rebalance task {} with participants {:?}",
            id, participants
        );
        tasks.insert(
            id,
            RebalanceTask {
                participants: participants.iter().cloned().collect(),
            },
        );
    }

    fn register_participants(&self, id: u64, participants: &[u64]) {
        let mut tasks = self.tasks.lock().unwrap();
        let exists = tasks.contains_key(&id);
        if !exists {
            drop(tasks);
            panic!("Rebalance task not registered");
        }
        tasks
            .get_mut(&id)
            .expect("unreachable")
            .participants
            .extend(participants);
    }

    fn participants(&self) -> HashSet<u64> {
        let mut tasks = self.tasks.lock().unwrap();
        tasks
            .values()
            .flat_map(|task| task.participants.iter())
            .cloned()
            .collect()
    }

    fn num_tasks(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    fn deregister(&self, id: u64) {
        debug!("deregistering rebalance task {}", id);
        self.tasks.lock().unwrap().remove(&id);
    }
}

/// The index rebalancer executes index rebalance operations to maintain centroids
/// with limited and balanced sizes of postings.
pub(crate) struct IndexRebalancer {
    opts: IndexRebalancerOpts,
    centroid_graph: Arc<dyn CentroidGraph>,
    centroid_counts: HashMap<u64, u64>,
    pending_rebalance: HashSet<u64>,
    tasks: Arc<RebalanceTasks>,
    total_merges: u64,
    total_splits: u64,
    next_task_id: u64,
    // this is an annoying workaround for the circular dependency between wc and rebalancer
    coordinator_handle_rx: Arc<std::sync::OnceLock<WriteCoordinatorHandle<VectorDbWriteDelta>>>,
    coordinator_handle: Option<WriteCoordinatorHandle<VectorDbWriteDelta>>,
}

impl IndexRebalancer {
    pub(crate) fn new(
        opts: IndexRebalancerOpts,
        centroid_graph: Arc<dyn CentroidGraph>,
        centroid_counts: HashMap<u64, u64>,
        coordinator_handle_rx: Arc<std::sync::OnceLock<WriteCoordinatorHandle<VectorDbWriteDelta>>>,
    ) -> Self {
        Self {
            opts,
            centroid_graph,
            centroid_counts,
            pending_rebalance: HashSet::new(),
            tasks: Arc::new(RebalanceTasks::new()),
            next_task_id: 0,
            total_merges: 0,
            total_splits: 0,
            coordinator_handle_rx,
            coordinator_handle: None,
        }
    }

    fn coordinator_handle(&mut self) -> WriteCoordinatorHandle<VectorDbWriteDelta> {
        if let Some(handle) = &self.coordinator_handle {
            return handle.clone();
        }
        self.coordinator_handle = Some(self.coordinator_handle_rx.get().unwrap().clone());
        self.coordinator_handle()
    }

    pub(crate) fn update_counts(&mut self, posting_deltas: &[(u64, i32)]) {
        for (centroid, delta) in posting_deltas {
            let count = self.centroid_counts.entry(*centroid).or_insert(0);
            *count = count.saturating_add_signed(*delta as i64);
            self.maybe_schedule_rebalance(*centroid);
        }
    }

    pub(crate) fn centroid_count(&self, centroid: u64) -> Option<u64> {
        self.centroid_counts.get(&centroid).copied()
    }

    pub(crate) fn drop_centroid(&mut self, centroid: u64) {
        self.centroid_counts.remove(&centroid);
    }

    pub(crate) fn max_centroid_size(&self) -> u64 {
        self.centroid_counts.values().cloned().max().unwrap_or(0)
    }

    pub(crate) fn register_participants(&mut self, id: u64, participants: &[u64]) {
        self.tasks.register_participants(id, participants);
    }

    pub(crate) fn finish_split(&mut self, task_id: u64, c0: Option<u64>, c1: Option<u64>) {
        self.finish_task(task_id, vec![c0, c1].into_iter().flatten().collect());
    }

    pub(crate) fn finish_merge(&mut self, task_id: u64, c_to: u64) {
        self.finish_task(task_id, vec![c_to]);
    }

    fn finish_task(&mut self, task_id: u64, centroids: Vec<u64>) {
        self.tasks.deregister(task_id);
        let pending = std::mem::take(&mut self.pending_rebalance);
        for centroid in pending.into_iter().chain(centroids.into_iter()) {
            if self.tasks.num_tasks() >= self.opts.max_rebalance_tasks {
                self.pending_rebalance.insert(centroid);
            } else {
                self.maybe_schedule_rebalance(centroid)
            }
        }
        debug!(
            "in-flight: {}, total ops: {}",
            self.tasks.num_tasks(),
            self.total_ops_pending_and_running()
        );
    }

    pub(crate) fn total_ops_pending_and_running(&self) -> usize {
        self.tasks.num_tasks() + self.pending_rebalance.len()
    }

    pub(crate) fn maybe_schedule_split(&mut self, candidate_centroid_id: u64) {
        if self.tasks.participants().contains(&candidate_centroid_id) {
            trace!(
                "centroid {} is already actively participating in rebalance. skip",
                candidate_centroid_id
            );
            return;
        }
        let Some(v) = self
            .centroid_graph
            .get_centroid_vector(candidate_centroid_id)
        else {
            warn!(
                "centroid {} not found in graph. was likely merged already",
                candidate_centroid_id
            );
            return;
        };
        if self.tasks.num_tasks() >= self.opts.max_rebalance_tasks {
            debug!(
                "too many tasks running. mark centroid {} pending",
                candidate_centroid_id
            );
            self.pending_rebalance.insert(candidate_centroid_id);
            return;
        }
        debug!("start split for centroid {}", candidate_centroid_id);
        // centroid is ready to be split, execute
        let c = CentroidEntry::new(candidate_centroid_id, v);
        self.next_task_id += 1;
        let task_id = self.next_task_id;
        self.tasks.register(task_id, &[candidate_centroid_id]);
        self.total_splits += 1;
        let mut task = IndexRebalancerTask {
            centroid_graph: self.centroid_graph.clone(),
            coordinator_handle: self.coordinator_handle(),
            opts: self.opts.clone(),
            task_id,
            tasks: self.tasks.clone(),
        };
        tokio::spawn(async move {
            task.handle_split(c).await.expect("splitting failed");
        });
    }

    pub(crate) fn maybe_schedule_merge(&mut self, candidate_centroid_id: u64, count: u64) {
        if self.tasks.participants().contains(&candidate_centroid_id) {
            trace!(
                "centroid {} is already actively participating in rebalance. skip",
                candidate_centroid_id
            );
            return;
        }
        if self.tasks.num_tasks() >= self.opts.max_rebalance_tasks {
            debug!(
                "too many tasks running. mark centroid {} pending",
                candidate_centroid_id
            );
            self.pending_rebalance.insert(candidate_centroid_id);
            return;
        }
        // Find a suitable merge partner from the centroid graph. A partner is suitable
        // if (1) it is not already participating in a merge, and (2) the total count is
        // not over the split threshold
        let v = self
            .centroid_graph
            .get_centroid_vector(candidate_centroid_id)
            .unwrap_or_else(|| panic!("unexpected missing centroid {}", candidate_centroid_id));
        let neighbours = self.centroid_graph.search(&v, 16);
        let partner = neighbours.iter().copied().find_map(|n_id| {
            let n_count = self.centroid_counts.get(&n_id).copied().unwrap_or(u64::MAX);
            if n_id != candidate_centroid_id
                && !self.tasks.participants().contains(&n_id)
                && count
                    .saturating_add(self.centroid_counts.get(&n_id).copied().unwrap_or(u64::MAX))
                    < self.opts.split_threshold_vectors as u64
            {
                Some((n_id, n_count))
            } else {
                None
            }
        });
        let Some((partner_id, partner_count)) = partner else {
            // no suitable partners, just skip it for now
            // TODO: it would be better to keep track of it but not count it against the
            //       the backpressure threshold
            return;
        };
        self.next_task_id += 1;
        let task_id = self.next_task_id;
        let (c_from, c_to) = if count < partner_count {
            (candidate_centroid_id, partner_id)
        } else {
            (partner_id, candidate_centroid_id)
        };
        self.total_merges += 1;
        self.tasks.register(task_id, &[c_from, c_to]);
        let mut task = IndexRebalancerTask {
            centroid_graph: self.centroid_graph.clone(),
            coordinator_handle: self.coordinator_handle(),
            opts: self.opts.clone(),
            task_id,
            tasks: self.tasks.clone(),
        };
        debug!("start merge for centroids {}->{}", c_from, c_to);
        tokio::spawn(async move {
            task.handle_merge(c_from, c_to).await.expect("merge failed");
        });
    }

    pub(crate) fn log_summary(&self) {
        if self.centroid_counts.is_empty() {
            info!("index summary: no centroids");
            return;
        }
        let num_centroids = self.centroid_counts.len();
        let total: u64 = self.centroid_counts.values().sum();
        let min = self.centroid_counts.values().min().copied().unwrap_or(0);
        let max = self.centroid_counts.values().max().copied().unwrap_or(0);
        let avg = total as f64 / num_centroids as f64;
        info!(
            num_centroids,
            total,
            avg = format!("{:.1}", avg),
            min,
            max,
            total_ops_pending_and_running = self.total_ops_pending_and_running(),
            merges = self.total_merges,
            splits = self.total_splits,
            "index summary"
        );
    }

    pub(crate) fn maybe_schedule_rebalance(&mut self, centroid_id: u64) {
        let Some(&count) = self.centroid_counts.get(&centroid_id) else {
            // centroid may have been removed by another merge
            return;
        };
        trace!(
            "evaluate centroid {} with count {} for rebalance",
            centroid_id, count
        );
        if count >= self.opts.split_threshold_vectors as u64 {
            self.maybe_schedule_split(centroid_id);
        } else if count <= self.opts.merge_threshold_vectors as u64 {
            self.maybe_schedule_merge(centroid_id, count);
        }
    }
}

struct IndexRebalancerTask {
    centroid_graph: Arc<dyn CentroidGraph>,
    coordinator_handle: WriteCoordinatorHandle<VectorDbWriteDelta>,
    opts: IndexRebalancerOpts,
    task_id: u64,
    tasks: Arc<RebalanceTasks>,
}

impl Drop for IndexRebalancerTask {
    fn drop(&mut self) {
        self.tasks.deregister(self.task_id);
    }
}

impl IndexRebalancerTask {
    async fn send_finish_split(
        &self,
        c: u64,
        c0: Option<u64>,
        c1: Option<u64>,
    ) -> Result<(), String> {
        let cmd = RebalanceCommand::SplitFinish(SplitFinishCommand::new(self.task_id, c0, c1));
        let mut write_handle = self
            .coordinator_handle
            .write(VectorDbWrite::Rebalance(cmd))
            .await
            .map_err(|e| e.to_string())?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn send_finish_merge(&self, c_to: u64) -> Result<(), String> {
        let cmd = RebalanceCommand::MergeFinish(MergeFinishCommand::new(self.task_id, c_to));
        let mut write_handle = self
            .coordinator_handle
            .write(VectorDbWrite::Rebalance(cmd))
            .await
            .map_err(|e| e.to_string())?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn handle_split(&mut self, centroid: CentroidEntry) -> Result<(), String> {
        let c_id = centroid.centroid_id;
        let c_vector = centroid.vector;

        // =========================================================================
        // SPLIT → SWEEP
        // =========================================================================

        let reader = ViewReader::new(self.coordinator_handle.view());

        let posting_list_value = reader
            .get_posting_list(c_id, self.opts.dimensions)
            .await
            .map_err(|e| e.to_string())?;

        let posting_list: PostingList = posting_list_value;
        let vectors: Vec<(u64, Vec<f32>)> = posting_list
            .iter()
            .map(|p| (p.id(), p.vector().to_vec()))
            .collect();

        // If < 2 vectors, return early (can't split)
        if vectors.len() < 2 {
            warn!(
                c_id,
                num_vectors = vectors.len(),
                "cannot split centroid with fewer than 2 vectors"
            );
            self.send_finish_split(c_id, None, None).await?;
            return Ok(());
        }

        // Run two_means clustering to find new centroids
        let vector_refs: Vec<(u64, &[f32])> =
            vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        let clustering = kmeans::for_metric(self.opts.distance_metric);
        let (c0_vector, c1_vector) = clustering.two_means(&vector_refs, self.opts.dimensions);

        // Assign each vector to closer centroid
        let mut c0_postings = Vec::new();
        let mut c1_postings = Vec::new();

        for (id, vector) in &vectors {
            let d0 = distance::compute_distance(vector, &c0_vector, self.opts.distance_metric);
            let d1 = distance::compute_distance(vector, &c1_vector, self.opts.distance_metric);

            if d0 <= d1 {
                c0_postings.push(Posting::new(*id, vector.clone()));
            } else {
                c1_postings.push(Posting::new(*id, vector.clone()));
            }
        }

        // Track original vector IDs for sweep phase
        let original_vector_ids: HashSet<u64> = vectors.iter().map(|(id, _)| *id).collect();

        // Send SplitCommand (uses centroid_id, not full CentroidEntry)
        let cmd = RebalanceCommand::Split(SplitCommand::new(
            self.task_id,
            c_id,
            SplitPostings::new(c0_vector.clone(), c0_postings.clone()),
            SplitPostings::new(c1_vector.clone(), c1_postings.clone()),
        ));

        let mut write_handle = self
            .coordinator_handle
            .write(VectorDbWrite::Rebalance(cmd))
            .await
            .map_err(|e| e.to_string())?;
        let result = write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| e.to_string())?;
        let SplitCommandResult { c0_id, c1_id } = result
            .downcast_ref::<SplitCommandResult>()
            .unwrap_or_else(|| panic!("invalid type returned from split: {:?}", result.type_id()))
            .clone();

        // =========================================================================
        // Phase 2: SWEEP → REASSIGN
        // =========================================================================

        // Get an updated view
        let reader = ViewReader::new(self.coordinator_handle.view());

        // Read C's posting list from updated view
        let posting_list_value = reader
            .get_posting_list(c_id, self.opts.dimensions)
            .await
            .map_err(|e| e.to_string())?;

        let c_postings: PostingList = posting_list_value;

        // Find vectors not in original set (missed due to racing inserts)
        let mut sweep_c0_postings = Vec::new();
        let mut sweep_c1_postings = Vec::new();

        for p in c_postings.iter() {
            if !original_vector_ids.contains(&p.id()) {
                let d0 =
                    distance::compute_distance(p.vector(), &c0_vector, self.opts.distance_metric);
                let d1 =
                    distance::compute_distance(p.vector(), &c1_vector, self.opts.distance_metric);

                if d0 <= d1 {
                    sweep_c0_postings.push(p.clone());
                    c0_postings.push(p.clone());
                } else {
                    sweep_c1_postings.push(p.clone());
                    c1_postings.push(p.clone());
                }
            }
        }

        // Send SplitSweepCommand
        let sweep_cmd = RebalanceCommand::SplitSweep(SplitSweepCommand::new(
            self.task_id,
            CentroidPostings::new(c0_id, sweep_c0_postings),
            CentroidPostings::new(c1_id, sweep_c1_postings),
        ));

        let mut write_handle = self
            .coordinator_handle
            .write(VectorDbWrite::Rebalance(sweep_cmd))
            .await
            .map_err(|e| e.to_string())?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| e.to_string())?;

        // =========================================================================
        // Phase 3: REASSIGN
        // =========================================================================

        let mut reassignments = Vec::with_capacity(c0_postings.len() + c1_postings.len());
        reassignments.extend(self.compute_split_reassignments(
            c0_id,
            &c0_postings,
            &c_vector,
            c0_id,
            &c0_vector,
            c1_id,
            &c1_vector,
        ));
        reassignments.extend(self.compute_split_reassignments(
            c1_id,
            &c1_postings,
            &c_vector,
            c0_id,
            &c0_vector,
            c1_id,
            &c1_vector,
        ));
        reassignments.extend(
            self.search_split_neighbourhood_for_reassignments(
                &c_vector, c0_id, c1_id, &c0_vector, &c1_vector,
            )
            .await?,
        );

        let reassign_cmd =
            RebalanceCommand::SplitReassign(SplitReassignCommand::new(self.task_id, reassignments));

        let mut write_handle = self
            .coordinator_handle
            .write(VectorDbWrite::Rebalance(reassign_cmd))
            .await
            .map_err(|e| e.to_string())?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| e.to_string())?;

        self.send_finish_split(c_id, Some(c0_id), Some(c1_id))
            .await?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_split_reassignments(
        &self,
        centroid_id: u64,
        postings: &[Posting],
        c_vector: &[f32],
        c0_id: u64,
        c0_vector: &[f32],
        c1_id: u64,
        c1_vector: &[f32],
    ) -> Vec<VectorReassignment> {
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
                self.opts.distance_metric,
            ) {
                continue;
            }
            let nearest = self.centroid_graph.search(p.vector(), 1);
            let Some(nearest_id) = nearest.first() else {
                continue;
            };

            if *nearest_id != c0_id && *nearest_id != c1_id {
                reassignments.push(VectorReassignment::new(
                    *nearest_id,
                    centroid_id,
                    p.id(),
                    p.vector().to_vec(),
                ));
            }
        }
        reassignments
    }

    /// Scan neighbours of the split centroids and reassign vectors that are now closer
    /// to a different centroid.
    async fn search_split_neighbourhood_for_reassignments(
        &self,
        c_vector: &[f32],
        c0_id: u64,
        c1_id: u64,
        c0_vector: &[f32],
        c1_vector: &[f32],
    ) -> Result<Vec<VectorReassignment>, String> {
        let mut reassignments: Vec<VectorReassignment> = Vec::new();

        // 1. Find nearest `split_search_neighbourhood` centroids of the split region
        let neighbours = self
            .centroid_graph
            .search(c_vector, self.opts.split_search_neighbourhood + 2);

        // 2. Get fresh view for reading posting lists
        let reader = ViewReader::new(self.coordinator_handle.view());

        // 3. For each neighbor (excluding c0, c1), check if any of their vectors should move
        for &neighbour_id in &neighbours {
            if neighbour_id == c0_id || neighbour_id == c1_id {
                continue;
            }

            let neighbour_postings_value = reader
                .get_posting_list(neighbour_id, self.opts.dimensions)
                .await
                .map_err(|e| e.to_string())?;

            let neighbour_postings: PostingList = neighbour_postings_value;

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
                    self.opts.distance_metric,
                ) {
                    continue;
                }
                let vector = p.vector().to_vec();

                // Find the closest centroid for this vector using the graph
                let nearest = self.centroid_graph.search(&vector, 1);
                let nearest_id = nearest.first().copied().unwrap_or(neighbour_id);

                if nearest_id != neighbour_id {
                    reassignments.push(VectorReassignment::new(
                        nearest_id,
                        neighbour_id,
                        p.id(),
                        vector,
                    ));
                }
            }
        }

        Ok(reassignments)
    }

    /// Execute a merge operation. `c_from` is the smaller centroid (to be removed) and
    /// `c_to` is the larger centroid (survives). Both were determined by the delta
    /// before dispatching this operation.
    async fn handle_merge(&mut self, c_from: u64, c_to: u64) -> Result<(), String> {
        // =========================================================================
        // Phase 1: MERGE
        // =========================================================================

        let reader = ViewReader::new(self.coordinator_handle.view());

        let posting_list = reader
            .get_posting_list(c_from, self.opts.dimensions)
            .await
            .map_err(|e| e.to_string())?;

        // Safety check: if c has < 1 vector (e.g. drained by concurrent ops), abort.
        if posting_list.is_empty() {
            warn!(
                c_from,
                num_vectors = posting_list.len(),
                "cannot merge empty centroid",
            );
            self.send_finish_merge(c_to).await?;
            return Ok(());
        }

        // Track original vector IDs for sweep phase, and save (id, vector)
        // pairs for reassignment in Phase 3.
        let original_vector_ids: HashSet<u64> = posting_list.iter().map(|p| p.id()).collect();
        let mut moved_vectors: Vec<(u64, Vec<f32>)> = posting_list
            .iter()
            .map(|p| (p.id(), p.vector().to_vec()))
            .collect();

        // Send MergeCommand: merge c_from's postings into c_to
        let cmd = RebalanceCommand::Merge(MergeCommand::new(
            self.task_id,
            c_to,
            CentroidPostings::new(c_from, posting_list),
        ));

        let mut write_handle = self
            .coordinator_handle
            .write(VectorDbWrite::Rebalance(cmd))
            .await
            .map_err(|e| e.to_string())?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| e.to_string())?;

        // =========================================================================
        // Phase 2: SWEEP
        // =========================================================================

        let reader = ViewReader::new(self.coordinator_handle.view());

        // Re-read c's posting list (to catch racing inserts)
        let c_from_postings = reader
            .get_posting_list(c_from, self.opts.dimensions)
            .await
            .map_err(|e| e.to_string())?;

        // Find vectors not in original set
        let mut swept_postings = Vec::new();
        for p in c_from_postings.iter() {
            if !original_vector_ids.contains(&p.id()) {
                swept_postings.push(p.clone());
            }
        }

        // Save swept vectors for Phase 3 reassignment before moving
        for p in &swept_postings {
            moved_vectors.push((p.id(), p.vector().to_vec()));
        }

        // Swept postings go to c_to (the surviving centroid)
        let sweep_cmd = RebalanceCommand::MergeSweep(MergeSweepCommand::new(
            self.task_id,
            CentroidPostings::new(c_to, swept_postings),
        ));

        let mut write_handle = self
            .coordinator_handle
            .write(VectorDbWrite::Rebalance(sweep_cmd))
            .await
            .map_err(|e| e.to_string())?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| e.to_string())?;

        // =========================================================================
        // Phase 3: REASSIGN
        // =========================================================================

        // We already have all vectors that came from c_from (collected in
        // moved_vectors during Phases 1 and 2). Check whether any of them
        // are closer to a different centroid than c_to.
        let mut reassignments = Vec::new();
        for (id, vector) in &moved_vectors {
            let nearest = self.centroid_graph.search(vector, 1);
            let Some(&nearest_id) = nearest.first() else {
                continue;
            };
            if nearest_id != c_to {
                reassignments.push(VectorReassignment::new(
                    nearest_id,
                    c_to,
                    *id,
                    vector.clone(),
                ));
            }
        }

        let reassign_cmd =
            RebalanceCommand::MergeReassign(MergeReassignCommand::new(self.task_id, reassignments));

        let mut write_handle = self
            .coordinator_handle
            .try_write(VectorDbWrite::Rebalance(reassign_cmd))
            .await
            .map_err(|e| e.to_string())?;
        write_handle
            .wait(Durability::Applied)
            .await
            .map_err(|e| e.to_string())?;

        self.send_finish_merge(c_to).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, OnceLock};
    use std::time::Duration;

    use super::*;
    use crate::delta::{VectorDbDeltaContext, VectorDbDeltaOpts, VectorDbWriteDelta};
    use crate::flusher::VectorDbFlusher;
    use crate::hnsw::build_centroid_graph;
    use crate::serde::centroid_chunk::CentroidEntry;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::key::SeqBlockKey;
    use crate::serde::posting_list::PostingUpdate;
    use crate::storage::merge_operator::VectorDbMergeOperator;
    use crate::storage::{VectorDbStorageReadExt, record};
    use common::SequenceAllocator;
    use common::coordinator::{
        Durability, WriteCoordinator, WriteCoordinatorConfig, WriteCoordinatorHandle,
    };
    use common::storage::in_memory::InMemoryStorage;
    use common::storage::{RecordOp, Storage, StorageSnapshot};
    use dashmap::DashMap;

    struct RebalancerTestEnv {
        storage: Arc<dyn Storage>,
        centroid_graph: Arc<dyn CentroidGraph>,
        coordinator: WriteCoordinator<VectorDbWriteDelta, VectorDbFlusher>,
        handle: WriteCoordinatorHandle<VectorDbWriteDelta>,
        initial_centroids: Vec<CentroidEntry>,
        dimensions: usize,
        distance_metric: DistanceMetric,
        tasks: Arc<RebalanceTasks>,
    }

    impl RebalancerTestEnv {
        #[allow(clippy::type_complexity)]
        async fn new(
            dimensions: usize,
            distance_metric: DistanceMetric,
            centroid_vecs: Vec<Vec<f32>>,
            postings: Vec<(usize, Vec<(u64, Vec<f32>)>)>,
        ) -> Self {
            // Create storage with merge operator
            let merge_operator = Arc::new(VectorDbMergeOperator::new(dimensions));
            let storage: Arc<dyn Storage> =
                Arc::new(InMemoryStorage::with_merge_operator(merge_operator));

            // Create SequenceAllocator
            let seq_key = SeqBlockKey.encode();
            let mut allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
                .await
                .unwrap();

            // Allocate centroid IDs and build entries
            let mut initial_ops: Vec<RecordOp> = Vec::new();
            let mut entries = Vec::with_capacity(centroid_vecs.len());
            for centroid_vec in &centroid_vecs {
                let (centroid_id, seq_put) = allocator.allocate_one();
                if let Some(put) = seq_put {
                    initial_ops.push(RecordOp::Put(put.into()));
                }
                entries.push(CentroidEntry::new(centroid_id, centroid_vec.clone()));
            }

            // Write centroid chunk
            initial_ops.push(record::put_centroid_chunk(0, entries.clone(), dimensions));

            // Write posting lists
            let mut centroid_counts: HashMap<u64, u64> = HashMap::new();
            for (centroid_idx, vectors) in &postings {
                let centroid_id = entries[*centroid_idx].centroid_id;
                let updates: Vec<PostingUpdate> = vectors
                    .iter()
                    .map(|(vid, vec)| PostingUpdate::append(*vid, vec.clone()))
                    .collect();
                let count = updates.len() as u64;
                initial_ops.push(record::merge_posting_list(centroid_id, updates).unwrap());
                initial_ops.push(record::merge_centroid_stats(centroid_id, count as i32));
                *centroid_counts.entry(centroid_id).or_default() += count;
            }

            // Apply all initial ops
            storage.apply(initial_ops).await.unwrap();

            // 6. Get initial snapshot
            let initial_snapshot = storage.snapshot().await.unwrap();

            // 7. Build centroid graph
            let centroid_graph: Arc<dyn CentroidGraph> =
                Arc::from(build_centroid_graph(entries.clone(), distance_metric).unwrap());

            // 8. Build delta context
            let chunk_target = 4096;
            let total_centroids = entries.len();
            let current_chunk_id = if total_centroids == 0 {
                0
            } else {
                ((total_centroids - 1) / chunk_target) as u32
            };
            let current_chunk_count = if total_centroids == 0 {
                0
            } else {
                total_centroids - (current_chunk_id as usize * chunk_target)
            };
            let rebalancer = IndexRebalancer::new(
                IndexRebalancerOpts {
                    dimensions,
                    distance_metric,
                    split_search_neighbourhood: 4,
                    split_threshold_vectors: 10_000,
                    merge_threshold_vectors: 0,
                    max_rebalance_tasks: 0,
                },
                centroid_graph.clone(),
                centroid_counts,
                Arc::new(std::sync::OnceLock::new()),
            );
            let tasks = rebalancer.tasks.clone();
            let ctx = VectorDbDeltaContext {
                opts: VectorDbDeltaOpts {
                    dimensions,
                    chunk_target,
                    max_pending_and_running_rebalance_tasks: usize::MAX,
                    split_threshold_vectors: usize::MAX / 2,
                    rebalance_backpressure_resume_threshold: 0,
                },
                dictionary: Arc::new(DashMap::new()),
                centroid_graph: centroid_graph.clone(),
                id_allocator: allocator,
                current_chunk_id,
                current_chunk_count,
                rebalancer,
                pause_handle: Arc::new(OnceLock::new()),
            };

            // 9. Create flusher and coordinator
            let flusher = VectorDbFlusher {
                storage: storage.clone(),
            };
            let config = WriteCoordinatorConfig {
                queue_capacity: 100,
                flush_interval: Duration::from_secs(3600),
                flush_size_threshold: usize::MAX,
            };
            let mut coordinator = WriteCoordinator::new(
                config,
                vec!["rebalance".to_string()],
                ctx,
                initial_snapshot,
                flusher,
            );
            let handle = coordinator.handle("rebalance");
            coordinator.start();

            Self {
                storage,
                centroid_graph,
                coordinator,
                handle,
                initial_centroids: entries,
                dimensions,
                distance_metric,
                tasks,
            }
        }

        fn create_task(&self) -> IndexRebalancerTask {
            let task_id = 1;
            self.tasks.register(task_id, &[]);
            IndexRebalancerTask {
                centroid_graph: self.centroid_graph.clone(),
                coordinator_handle: self.handle.clone(),
                opts: IndexRebalancerOpts {
                    dimensions: self.dimensions,
                    distance_metric: self.distance_metric,
                    split_search_neighbourhood: 4,
                    split_threshold_vectors: 10_000,
                    merge_threshold_vectors: 0,
                    max_rebalance_tasks: 10,
                },
                task_id,
                tasks: self.tasks.clone(),
            }
        }

        async fn flush_and_snapshot(&self) -> Arc<dyn StorageSnapshot> {
            let mut flush_handle = self.handle.flush(false).await.unwrap();
            flush_handle.wait(Durability::Flushed).await.unwrap();
            self.storage.snapshot().await.unwrap()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_split_centroid_with_five_vectors() {
        // given
        let dimensions = 3;
        let distance_metric = DistanceMetric::L2;
        // One centroid at the midpoint
        let centroid_vecs = vec![vec![5.0, 0.0, 0.0]];
        // 5 vectors forming two clear clusters:
        // Cluster A (near origin): 100, 101
        // Cluster B (near [10,0,0]): 102, 103, 104
        let postings = vec![(
            0,
            vec![
                (100, vec![0.0, 0.0, 0.0]),
                (101, vec![0.1, 0.0, 0.0]),
                (102, vec![10.0, 0.0, 0.0]),
                (103, vec![10.1, 0.0, 0.0]),
                (104, vec![10.0, 0.1, 0.0]),
            ],
        )];
        let env =
            RebalancerTestEnv::new(dimensions, distance_metric, centroid_vecs, postings.clone())
                .await;
        let mut task = env.create_task();
        let original_centroid = env.initial_centroids[0].clone();

        // when
        task.handle_split(original_centroid).await.unwrap();

        // then
        // flush and read from storage
        let snapshot = env.flush_and_snapshot().await;
        // Read posting lists for the two new centroids (c0_id=1, c1_id=2)
        // The original centroid_id=0 was allocated first; split allocates 1 and 2
        assert_new_posting(&postings, &snapshot, 1, &[100u64, 101], 3);
        assert_new_posting(&postings, &snapshot, 2, &[102u64, 103, 104], 3);
        // Verify the original centroid is in the deletions bitmap
        let deletions = snapshot.get_deleted_vectors().await.unwrap();
        assert!(
            deletions.contains(0),
            "original centroid_id=0 should be in deletions bitmap"
        );

        // cleanup
        env.coordinator.stop().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_reassign_vector_to_closer_neighbour_after_split() {
        // given
        // Centroid A at [5.0, 0.0] will be split.
        // Centroid B at [5.0, 1.0] is a close neighbour.
        // Vector 104 at [5.0, 0.5] is near old centroid A and closer to B than
        // either new centroid after the split.
        let dimensions = 2;
        let distance_metric = DistanceMetric::L2;
        let centroid_vecs = vec![
            vec![5.0, 0.0], // centroid A (id=0), to be split
            vec![5.0, 1.0], // centroid B (id=1), neighbour
        ];
        let postings = vec![
            (
                0,
                vec![
                    (100, vec![0.0, 0.0]),
                    (101, vec![0.1, 0.0]),
                    (102, vec![10.0, 0.0]),
                    (103, vec![10.1, 0.0]),
                    (104, vec![5.0, 0.5]),
                ],
            ),
            (1, vec![(200, vec![5.0, 1.0])]),
        ];
        let env =
            RebalancerTestEnv::new(dimensions, distance_metric, centroid_vecs, postings.clone())
                .await;
        let mut task = env.create_task();
        let original_centroid = env.initial_centroids[0].clone();

        // when
        task.handle_split(original_centroid).await.unwrap();

        // then
        let snapshot = env.flush_and_snapshot().await;

        // After split: c0_id=2 (near origin cluster), c1_id=3 (near [10,0] cluster).
        // Vector 104 at [5.0, 0.5] triggers split_heuristic (old centroid d=0.5
        // is closer than both new centroids d≈3.3, d≈5.1), then centroid graph
        // search finds B (id=1) at [5.0, 1.0] (d=0.5) as nearest. Since B ≠ c0
        // and B ≠ c1, vector 104 is reassigned from c0 to B.
        assert_new_posting(&postings, &snapshot, 2, &[100u64, 101], dimensions);
        assert_new_posting(&postings, &snapshot, 3, &[102u64, 103], dimensions);
        assert_new_posting(&postings, &snapshot, 1, &[200u64, 104], dimensions);

        // Original centroid should be in deletions
        let deletions = snapshot.get_deleted_vectors().await.unwrap();
        assert!(
            deletions.contains(0),
            "original centroid_id=0 should be in deletions bitmap"
        );

        // cleanup
        env.coordinator.stop().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_reassign_neighbour_vector_to_new_centroid_after_split() {
        // given
        // Centroid A at [5.0, 0.0] will be split into two clusters near origin
        // and [10, 0]. Centroid B at [2.0, 3.0] is a neighbour whose vector 201
        // at [0.5, 0.5] is much closer to the new c0 (~[0.05, 0]) than to B.
        let dimensions = 2;
        let distance_metric = DistanceMetric::L2;
        let centroid_vecs = vec![
            vec![5.0, 0.0], // centroid A (id=0), to be split
            vec![2.0, 3.0], // centroid B (id=1), neighbour
        ];
        let postings = vec![
            (
                0,
                vec![
                    (100, vec![0.0, 0.0]),
                    (101, vec![0.1, 0.0]),
                    (102, vec![10.0, 0.0]),
                    (103, vec![10.1, 0.0]),
                ],
            ),
            (
                1,
                vec![
                    (200, vec![2.0, 3.0]),
                    (201, vec![0.5, 0.5]), // stray: closer to c0 than to B
                ],
            ),
        ];
        let env =
            RebalancerTestEnv::new(dimensions, distance_metric, centroid_vecs, postings.clone())
                .await;
        let mut task = env.create_task();
        let original_centroid = env.initial_centroids[0].clone();

        // when
        task.handle_split(original_centroid).await.unwrap();

        // then
        let snapshot = env.flush_and_snapshot().await;

        // After split: c0_id=2 (~[0.05, 0]), c1_id=3 (~[10.05, 0]).
        // neighbour_split_heuristic fires for vector 201 because c0 (d≈0.67)
        // is closer than old centroid A (d≈4.53). Centroid graph search finds
        // c0 (d≈0.67) as nearest, which ≠ B (id=1), so 201 is reassigned to c0.
        // Vector 200 stays with B because B itself (d=0) is its nearest centroid.
        assert_new_posting(&postings, &snapshot, 2, &[100u64, 101, 201], dimensions);
        assert_new_posting(&postings, &snapshot, 3, &[102u64, 103], dimensions);
        assert_new_posting(&postings, &snapshot, 1, &[200u64], dimensions);

        // Original centroid should be in deletions
        let deletions = snapshot.get_deleted_vectors().await.unwrap();
        assert!(
            deletions.contains(0),
            "original centroid_id=0 should be in deletions bitmap"
        );

        // cleanup
        env.coordinator.stop().await.unwrap();
    }

    #[allow(clippy::type_complexity)]
    async fn assert_new_posting(
        orig_postings: &[(usize, Vec<(u64, Vec<f32>)>)],
        snapshot: &Arc<dyn StorageSnapshot>,
        centroid_id: u64,
        expected_posting_ids: &[u64],
        dimensions: usize,
    ) {
        let postings = snapshot
            .get_posting_list(centroid_id, dimensions)
            .await
            .unwrap();
        let postings: PostingList = postings.into();
        let posting_ids: HashSet<_> = postings.iter().map(|p| p.id()).collect();
        let expected_posting_ids: HashSet<_> = expected_posting_ids.iter().copied().collect();
        assert_eq!(posting_ids, expected_posting_ids);
        let orig_postings: HashMap<_, _> = orig_postings
            .iter()
            .flat_map(|p| p.1.iter())
            .cloned()
            .collect();
        for p in postings {
            assert_eq!(orig_postings[&p.id()], p.vector().to_vec());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_merge_small_centroid_into_neighbour() {
        // given
        // Centroid A at [0,0] with 2 vectors, B at [1,0] with 3 vectors.
        // Trigger merge on A (smaller). A's postings should move to B. A deleted.
        let dimensions = 2;
        let distance_metric = DistanceMetric::L2;
        let centroid_vecs = vec![
            vec![0.0, 0.0], // centroid A (id=0)
            vec![1.0, 0.0], // centroid B (id=1)
        ];
        let postings = vec![
            (0, vec![(100, vec![0.0, 0.1]), (101, vec![0.1, 0.0])]),
            (
                1,
                vec![
                    (200, vec![1.0, 0.0]),
                    (201, vec![1.1, 0.0]),
                    (202, vec![1.0, 0.1]),
                ],
            ),
        ];
        let env =
            RebalancerTestEnv::new(dimensions, distance_metric, centroid_vecs, postings.clone())
                .await;
        let mut task = env.create_task();
        let centroid_a_id = env.initial_centroids[0].centroid_id;
        let centroid_b_id = env.initial_centroids[1].centroid_id;

        // when - merge A (smaller) into B (larger)
        task.handle_merge(centroid_a_id, centroid_b_id)
            .await
            .unwrap();

        // then
        let snapshot = env.flush_and_snapshot().await;

        // Centroid A should be deleted
        let deletions = snapshot.get_deleted_vectors().await.unwrap();
        assert!(
            deletions.contains(centroid_a_id),
            "centroid A should be in deletions bitmap"
        );

        // All of A's vectors should now be in B's posting list
        // B (id=1) survives because it's larger. A's vectors (100, 101) moved to B.
        // Since A's vectors are at [0,0.1] and [0.1,0], they are closest to A's centroid,
        // but A no longer exists. The reassign phase searches the graph and finds B as nearest.
        // So vectors stay in B (source=B after merge, nearest=B since A is removed).
        assert_new_posting(
            &postings,
            &snapshot,
            centroid_b_id,
            &[100, 101, 200, 201, 202],
            dimensions,
        );

        // cleanup
        env.coordinator.stop().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_reassign_merged_vector_to_closer_centroid() {
        // given
        // 3 centroids: A at [0,0] (small, to merge), B at [5,0] (merge partner), C at [10,0]
        // A has a vector at [8,0] that's closer to C than to B.
        // After merge, vector should be reassigned from B to C.
        let dimensions = 2;
        let distance_metric = DistanceMetric::L2;
        let centroid_vecs = vec![
            vec![0.0, 0.0],  // centroid A (id=0) - small, merge target
            vec![5.0, 0.0],  // centroid B (id=1) - merge partner
            vec![10.0, 0.0], // centroid C (id=2) - for reassignment
        ];
        let postings = vec![
            (
                0,
                vec![
                    (100, vec![0.1, 0.0]),
                    (101, vec![8.0, 0.0]), // closer to C than to B
                ],
            ),
            (
                1,
                vec![
                    (200, vec![5.0, 0.0]),
                    (201, vec![5.1, 0.0]),
                    (202, vec![4.9, 0.0]),
                ],
            ),
            (2, vec![(300, vec![10.0, 0.0])]),
        ];
        let env =
            RebalancerTestEnv::new(dimensions, distance_metric, centroid_vecs, postings.clone())
                .await;
        let mut task = env.create_task();
        let centroid_a_id = env.initial_centroids[0].centroid_id;
        let centroid_b_id = env.initial_centroids[1].centroid_id;

        // when - merge A (smaller) into B (larger)
        task.handle_merge(centroid_a_id, centroid_b_id)
            .await
            .unwrap();

        // then
        let snapshot = env.flush_and_snapshot().await;

        // A should be deleted
        let deletions = snapshot.get_deleted_vectors().await.unwrap();
        assert!(
            deletions.contains(centroid_a_id),
            "centroid A should be in deletions bitmap"
        );

        let c_id = env.initial_centroids[2].centroid_id;

        // Vector 100 at [0.1, 0] - closest to B (d=4.9) vs C (d=9.9), stays in B
        // Vector 101 at [8.0, 0] - closest to C (d=2.0) vs B (d=3.0), reassigned to C
        assert_new_posting(
            &postings,
            &snapshot,
            centroid_b_id,
            &[100, 200, 201, 202],
            dimensions,
        );
        assert_new_posting(&postings, &snapshot, c_id, &[101, 300], dimensions);

        // cleanup
        env.coordinator.stop().await.unwrap();
    }

    // ========================================================================
    // IndexRebalancer scheduling tests
    // ========================================================================

    struct MockCentroidGraph {
        centroids: Vec<(u64, Vec<f32>)>,
    }

    impl CentroidGraph for MockCentroidGraph {
        fn search(&self, _query: &[f32], _k: usize) -> Vec<u64> {
            self.centroids.iter().map(|(id, _)| *id).collect()
        }

        fn add_centroid(&self, _entry: &CentroidEntry) -> anyhow::Result<()> {
            Ok(())
        }

        fn remove_centroid(&self, _centroid_id: u64) -> anyhow::Result<()> {
            Ok(())
        }

        fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
            self.centroids
                .iter()
                .find(|(id, _)| *id == centroid_id)
                .map(|(_, v)| v.clone())
        }

        fn len(&self) -> usize {
            self.centroids.len()
        }
    }

    /// Create a test IndexRebalancer with max_rebalance_tasks=0 so scheduling
    /// adds to pending_rebalance (observable via num_in_flight_tasks) without
    /// needing a coordinator.
    fn create_test_rebalancer(
        centroids: Vec<(u64, Vec<f32>)>,
        centroid_counts: HashMap<u64, u64>,
        split_threshold: usize,
    ) -> IndexRebalancer {
        let centroid_graph: Arc<dyn CentroidGraph> = Arc::new(MockCentroidGraph { centroids });
        IndexRebalancer::new(
            IndexRebalancerOpts {
                dimensions: 2,
                distance_metric: DistanceMetric::L2,
                split_search_neighbourhood: 4,
                split_threshold_vectors: split_threshold,
                merge_threshold_vectors: 2,
                max_rebalance_tasks: 0,
            },
            centroid_graph,
            centroid_counts,
            Arc::new(std::sync::OnceLock::new()),
        )
    }

    #[test]
    fn should_schedule_split_when_count_reaches_threshold() {
        // given - split_threshold=5, centroid 1 in graph
        let mut rebalancer = create_test_rebalancer(vec![(1, vec![1.0, 0.0])], HashMap::new(), 5);

        // when - update count to reach threshold
        rebalancer.update_counts(&[(1, 5)]);

        // then - centroid should be pending rebalance
        assert_eq!(rebalancer.total_ops_pending_and_running(), 1);
    }

    #[test]
    fn should_schedule_merge_for_small_centroid() {
        // given - split_threshold=100, centroid 1 starts with count 0
        let mut rebalancer = create_test_rebalancer(vec![(1, vec![1.0, 0.0])], HashMap::new(), 100);

        // when - add a small number of vectors (below split threshold → merge path)
        rebalancer.update_counts(&[(1, 1)]);

        // then - centroid should be pending rebalance (merge)
        assert_eq!(rebalancer.total_ops_pending_and_running(), 1);
    }

    #[test]
    fn should_not_duplicate_schedule_for_same_centroid() {
        // given
        let mut rebalancer = create_test_rebalancer(vec![(1, vec![1.0, 0.0])], HashMap::new(), 5);
        rebalancer.update_counts(&[(1, 5)]);
        assert_eq!(rebalancer.total_ops_pending_and_running(), 1);

        // when - update the same centroid again
        rebalancer.update_counts(&[(1, 1)]);

        // then - still only 1 pending, not 2
        assert_eq!(rebalancer.total_ops_pending_and_running(), 1);
    }

    #[test]
    fn should_not_schedule_split_when_centroid_not_in_graph() {
        // given - only centroid 1 is in the graph
        let mut rebalancer = create_test_rebalancer(vec![(1, vec![1.0, 0.0])], HashMap::new(), 5);

        // when - push count for centroid 99 (not in graph) above split threshold
        rebalancer.update_counts(&[(99, 10)]);

        // then - not scheduled because get_centroid_vector returns None
        assert_eq!(rebalancer.total_ops_pending_and_running(), 0);
    }

    #[test]
    fn should_track_counts_across_multiple_updates() {
        // given - high threshold so scheduling doesn't matter for this test
        let mut rebalancer = create_test_rebalancer(
            vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])],
            HashMap::new(),
            100_000,
        );

        // when
        rebalancer.update_counts(&[(1, 3), (2, 1)]);
        rebalancer.update_counts(&[(1, 2)]);

        // then
        assert_eq!(rebalancer.centroid_count(1), Some(5));
        assert_eq!(rebalancer.centroid_count(2), Some(1));
        assert_eq!(rebalancer.centroid_count(99), None);
    }

    #[test]
    fn should_schedule_independent_centroids_separately() {
        // given - two centroids in graph
        let mut rebalancer = create_test_rebalancer(
            vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])],
            HashMap::new(),
            5,
        );

        // when - both exceed split threshold
        rebalancer.update_counts(&[(1, 5)]);
        rebalancer.update_counts(&[(2, 6)]);

        // then - both should be pending
        assert_eq!(rebalancer.total_ops_pending_and_running(), 2);
    }

    #[test]
    fn should_add_to_pending_when_max_tasks_running() {
        // given - rebalancer with max_rebalance_tasks=1 and a task already running
        let centroid_graph: Arc<dyn CentroidGraph> = Arc::new(MockCentroidGraph {
            centroids: vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])],
        });
        let mut rebalancer = IndexRebalancer::new(
            IndexRebalancerOpts {
                dimensions: 2,
                distance_metric: DistanceMetric::L2,
                split_search_neighbourhood: 4,
                split_threshold_vectors: 5,
                merge_threshold_vectors: 2,
                max_rebalance_tasks: 1,
            },
            centroid_graph,
            HashMap::new(),
            Arc::new(std::sync::OnceLock::new()),
        );

        // simulate a running task by registering directly
        rebalancer.tasks.register(999, &[99]);

        // when - centroid 1 exceeds split threshold
        rebalancer.update_counts(&[(1, 5)]);

        // then - centroid 1 should be pending (not spawned) because max tasks reached
        assert!(
            rebalancer.pending_rebalance.contains(&1),
            "centroid should be in pending_rebalance"
        );
        assert_eq!(
            rebalancer.tasks.num_tasks(),
            1,
            "should still have only the original running task"
        );
        assert_eq!(
            rebalancer.total_ops_pending_and_running(),
            2,
            "should have 1 running + 1 pending"
        );
    }

    #[test]
    fn should_skip_split_when_centroid_already_participating() {
        // given - rebalancer with max_rebalance_tasks=2, and a running task
        // that lists centroid 1 as a participant
        let centroid_graph: Arc<dyn CentroidGraph> = Arc::new(MockCentroidGraph {
            centroids: vec![(1, vec![1.0, 0.0])],
        });
        let mut rebalancer = IndexRebalancer::new(
            IndexRebalancerOpts {
                dimensions: 2,
                distance_metric: DistanceMetric::L2,
                split_search_neighbourhood: 4,
                split_threshold_vectors: 5,
                merge_threshold_vectors: 2,
                max_rebalance_tasks: 2,
            },
            centroid_graph,
            HashMap::new(),
            Arc::new(std::sync::OnceLock::new()),
        );

        // simulate a running task that involves centroid 1
        rebalancer.tasks.register(999, &[1]);

        // when - centroid 1 exceeds split threshold
        rebalancer.update_counts(&[(1, 5)]);

        // then - centroid should NOT be added to pending (skipped entirely)
        assert!(
            !rebalancer.pending_rebalance.contains(&1),
            "centroid should not be pending when already participating"
        );
        assert_eq!(
            rebalancer.total_ops_pending_and_running(),
            1,
            "should only have the original running task"
        );
    }

    #[test]
    fn should_skip_merge_when_centroid_already_participating() {
        // given - rebalancer with capacity for more tasks, but centroid 1
        // is already participating in a running task
        let centroid_graph: Arc<dyn CentroidGraph> = Arc::new(MockCentroidGraph {
            centroids: vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])],
        });
        let mut rebalancer = IndexRebalancer::new(
            IndexRebalancerOpts {
                dimensions: 2,
                distance_metric: DistanceMetric::L2,
                split_search_neighbourhood: 4,
                split_threshold_vectors: 100,
                merge_threshold_vectors: 5,
                max_rebalance_tasks: 2,
            },
            centroid_graph,
            HashMap::new(),
            Arc::new(std::sync::OnceLock::new()),
        );

        // simulate a running task that involves centroid 1
        rebalancer.tasks.register(999, &[1]);

        // when - centroid 1 gets a small count (below merge threshold)
        rebalancer.update_counts(&[(1, 1)]);

        // then - centroid should NOT be added to pending (skipped entirely)
        assert!(
            !rebalancer.pending_rebalance.contains(&1),
            "centroid should not be pending when already participating"
        );
        assert_eq!(
            rebalancer.total_ops_pending_and_running(),
            1,
            "should only have the original running task"
        );
    }

    #[test]
    fn should_drop_centroid_counts() {
        // given
        let mut counts = HashMap::new();
        counts.insert(1, 50u64);
        counts.insert(2, 30u64);
        let mut rebalancer = create_test_rebalancer(
            vec![(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])],
            counts,
            100_000,
        );

        // when
        rebalancer.drop_centroid(1);

        // then
        assert_eq!(rebalancer.centroid_count(1), None);
        assert_eq!(rebalancer.centroid_count(2), Some(30));
    }
}

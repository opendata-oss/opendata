//! HNSW implementation using the usearch library.
//!
//! Mutations (add/remove centroid) are buffered in a log and only applied to
//! the usearch index once they fall below the MVCC retention watermark (i.e.,
//! no outstanding snapshot references them). Searches replay the log on top of
//! the index results so that every epoch sees a consistent view.

use std::collections::{HashMap, HashSet, VecDeque};
use std::cmp::max;
use std::fmt;
use std::sync::{Arc, RwLock};

use tracing::warn;
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

use super::{CentroidGraph, CentroidGraphRead};

/// Initial capacity reserved for the usearch index.
/// Kept artificially high to avoid usearch deadlock issues near capacity limits.
const INITIAL_CAPACITY: usize = 200_000;

// ---------------------------------------------------------------------------
// Mutation log
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum MutationKind {
    Add { vector: Vec<f32> },
    Delete,
}

#[derive(Debug, Clone)]
struct MutationEntry {
    centroid_id: u64,
    epoch: u64,
    kind: MutationKind,
}

/// The net result of replaying the mutation log up to a given epoch.
struct PendingMutations<'a> {
    /// Centroids that were added (and not subsequently deleted) in the log.
    added: HashMap<u64, &'a Vec<f32>>,
    /// Centroids that were deleted in the log (may or may not be in the index).
    deleted: HashSet<u64>,
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// HNSW graph implementation using the usearch library.
///
/// Uses interior mutability for thread-safe mutation behind `Arc<dyn CentroidGraph>`.
/// The usearch `Index` is internally thread-safe for `add`/`remove`/`search`.
pub struct UsearchCentroidGraph {
    inner: Arc<RwLock<UsearchCentroidGraphInner>>,
}

impl fmt::Debug for UsearchCentroidGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.read().unwrap();
        f.debug_struct("UsearchCentroidGraph")
            .field("index_size", &inner.key_to_centroid.len())
            .field("pending_mutations", &inner.mutation_log.len())
            .finish()
    }
}

impl UsearchCentroidGraph {
    /// Build a new HNSW graph from centroids using usearch.
    pub fn build(
        centroids: Vec<CentroidEntry>,
        distance_metric: DistanceMetric,
    ) -> Result<Self> {
        if centroids.is_empty() {
            return Err(Error::InvalidInput(
                "Cannot build HNSW graph with no centroids".to_string(),
            ));
        }

        let dimensions = centroids[0].dimensions();
        for centroid in &centroids {
            if centroid.dimensions() != dimensions {
                return Err(Error::InvalidInput(format!(
                    "Centroid dimension mismatch: expected {}, got {}",
                    dimensions,
                    centroid.dimensions()
                )));
            }
        }

        let metric = match distance_metric {
            DistanceMetric::L2 => MetricKind::L2sq,
            DistanceMetric::DotProduct => MetricKind::IP,
        };

        let options = IndexOptions {
            dimensions,
            metric,
            quantization: ScalarKind::F32,
            connectivity: 16,
            expansion_add: 200,
            expansion_search: 100,
            multi: false,
        };

        let index = Index::new(&options).map_err(|e| Error::Internal(e.to_string()))?;
        index
            .reserve(INITIAL_CAPACITY)
            .map_err(|e| Error::Internal(e.to_string()))?;

        let mut key_to_centroid = HashMap::with_capacity(centroids.len());
        let mut centroid_to_key = HashMap::with_capacity(centroids.len());
        let mut centroid_vectors = HashMap::with_capacity(centroids.len());

        for (key, centroid) in centroids.iter().enumerate() {
            let key = key as u64;
            index
                .add(key, &centroid.vector)
                .map_err(|e| Error::Internal(e.to_string()))?;
            key_to_centroid.insert(key, centroid.centroid_id);
            centroid_to_key.insert(centroid.centroid_id, key);
            centroid_vectors.insert(centroid.centroid_id, centroid.vector.clone());
        }

        let next_key = centroids.len() as u64;

        Ok(Self {
            inner: Arc::new(RwLock::new(UsearchCentroidGraphInner {
                index,
                key_to_centroid,
                centroid_to_key,
                centroid_vectors,
                next_key,
                distance_metric,
                mutation_log: VecDeque::new(),
                snapshots: MVCC::new(),
            })),
        })
    }

    /// Force-apply any pending mutations whose epochs are at or below the
    /// current retention watermark. Useful in tests after dropping snapshots.
    #[cfg(test)]
    fn apply_pending_mutations(&self) {
        self.inner
            .write()
            .expect("lock poisoned")
            .apply_mutations();
    }
}

// ---------------------------------------------------------------------------
// Trait impls – outer types
// ---------------------------------------------------------------------------

impl CentroidGraphRead for UsearchCentroidGraph {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.inner.read().expect("lock poisoned").search(query, k)
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.inner
            .read()
            .expect("lock poisoned")
            .get_centroid_vector(centroid_id)
    }

    fn len(&self) -> usize {
        self.inner.read().expect("lock poisoned").live_count()
    }
}

impl CentroidGraph for UsearchCentroidGraph {
    fn add_centroid(&self, entry: &CentroidEntry, epoch: u64) -> Result<()> {
        self.inner
            .write()
            .expect("lock poisoned")
            .add_centroid(entry, epoch)
    }

    fn remove_centroid(&self, centroid_id: u64, epoch: u64) -> Result<()> {
        self.inner
            .write()
            .expect("lock poisoned")
            .remove_centroid(centroid_id, epoch)
    }

    fn snapshot(&self) -> Result<Arc<dyn CentroidGraphRead>> {
        let (id, epoch) = self.inner.write().expect("lock poisoned").snapshot();
        Ok(Arc::new(UsearchCentroidGraphSnapshot {
            id,
            epoch,
            inner: Arc::clone(&self.inner),
        }))
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

struct UsearchCentroidGraphSnapshot {
    id: Uuid,
    epoch: u64,
    inner: Arc<RwLock<UsearchCentroidGraphInner>>,
}

impl Drop for UsearchCentroidGraphSnapshot {
    fn drop(&mut self) {
        let mut inner = self.inner.write().expect("lock poisoned");
        inner.release_snapshot(self.id);
    }
}

impl CentroidGraphRead for UsearchCentroidGraphSnapshot {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.inner
            .read()
            .expect("lock poisoned")
            .search_at_epoch(query, k, self.epoch)
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.inner
            .read()
            .expect("lock poisoned")
            .get_centroid_vector_at_epoch(centroid_id, self.epoch)
    }

    fn len(&self) -> usize {
        self.inner
            .read()
            .expect("lock poisoned")
            .live_count_at_epoch(self.epoch)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// Inner state
// ---------------------------------------------------------------------------

struct UsearchCentroidGraphInner {
    /// The usearch index. Contains only centroids whose mutations have been
    /// applied (i.e. at epochs ≤ the retention watermark).
    index: Index,
    /// usearch key → centroid_id  (only for centroids in the index)
    key_to_centroid: HashMap<u64, u64>,
    /// centroid_id → usearch key  (only for centroids in the index)
    centroid_to_key: HashMap<u64, u64>,
    /// Vectors for all known centroids — those in the index *and* those with
    /// pending add mutations. Removed only when a delete mutation is applied.
    centroid_vectors: HashMap<u64, Vec<f32>>,
    /// Next usearch key to allocate
    next_key: u64,
    /// Distance metric (needed for explicit distance computation on log entries)
    distance_metric: DistanceMetric,
    /// Ordered mutation log. Entries are processed FIFO and applied to the
    /// index once their epoch falls at or below the retention watermark.
    mutation_log: VecDeque<MutationEntry>,
    snapshots: MVCC,
}

impl UsearchCentroidGraphInner {
    // -------------------------------------------------------------------
    // Mutation log replay
    // -------------------------------------------------------------------

    /// Replay the mutation log up to `epoch` and return the net adds/deletes.
    fn pending_mutations_at_epoch(&self, epoch: u64) -> PendingMutations<'_> {
        let mut added: HashMap<u64, &Vec<f32>> = HashMap::new();
        let mut deleted: HashSet<u64> = HashSet::new();
        for entry in &self.mutation_log {
            if entry.epoch > epoch {
                break;
            }
            match &entry.kind {
                MutationKind::Add { vector } => {
                    added.insert(entry.centroid_id, vector);
                    deleted.remove(&entry.centroid_id);
                }
                MutationKind::Delete => {
                    deleted.insert(entry.centroid_id);
                    added.remove(&entry.centroid_id);
                }
            }
        }
        PendingMutations { added, deleted }
    }

    // -------------------------------------------------------------------
    // Search
    // -------------------------------------------------------------------

    /// Search at the latest (current) epoch.
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.search_at_epoch(query, k, self.snapshots.latest_epoch())
    }

    /// Search at a specific epoch, replaying the mutation log.
    fn search_at_epoch(&self, query: &[f32], k: usize, epoch: u64) -> Vec<u64> {
        let pm = self.pending_mutations_at_epoch(epoch);
        let mut candidates = self.collect_candidates(query, k, &pm);

        if candidates.len() < k {
            // Retry with 2× k from the index to compensate for filtered deletes
            candidates = self.collect_candidates(query, k * 2, &pm);
            candidates.truncate(k);
            if candidates.len() < k {
                warn!(
                    target_k = k,
                    actual = candidates.len(),
                    epoch,
                    "search_at_epoch: fewer centroids than requested after retry"
                );
            }
        }

        candidates.into_iter().map(|(cid, _)| cid).collect()
    }

    /// Run a single search round: query the index for `search_k` results,
    /// filter out pending deletes, merge in pending adds, sort by distance
    /// and return up to the first `search_k` candidates.
    fn collect_candidates(
        &self,
        query: &[f32],
        search_k: usize,
        pm: &PendingMutations<'_>,
    ) -> Vec<(u64, f32)> {
        let results = self
            .index
            .search(query, search_k)
            .expect("unexpected usearch error");

        let mut candidates: Vec<(u64, f32)> = results
            .keys
            .iter()
            .zip(results.distances.iter())
            .filter_map(|(&key, &dist)| {
                self.key_to_centroid.get(&key).map(|cid| (*cid, dist))
            })
            .filter(|(cid, _)| !pm.deleted.contains(cid))
            .collect();

        for (&cid, vector) in &pm.added {
            let dist = compute_distance(vector, query, self.distance_metric);
            candidates.push((cid, dist));
        }

        candidates
            .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates
    }

    // -------------------------------------------------------------------
    // Counts / vectors
    // -------------------------------------------------------------------

    fn live_count(&self) -> usize {
        self.live_count_at_epoch(self.snapshots.latest_epoch())
    }

    fn live_count_at_epoch(&self, epoch: u64) -> usize {
        let pm = self.pending_mutations_at_epoch(epoch);
        let index_visible = self
            .key_to_centroid
            .values()
            .filter(|cid| !pm.deleted.contains(cid))
            .count();
        index_visible + pm.added.len()
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.get_centroid_vector_at_epoch(centroid_id, self.snapshots.latest_epoch())
    }

    fn get_centroid_vector_at_epoch(&self, centroid_id: u64, epoch: u64) -> Option<Vec<f32>> {
        let pm = self.pending_mutations_at_epoch(epoch);

        // Pending add?
        if let Some(vector) = pm.added.get(&centroid_id) {
            return Some((*vector).clone());
        }
        // In the index and not pending-deleted?
        if !pm.deleted.contains(&centroid_id) && self.centroid_to_key.contains_key(&centroid_id)
        {
            return self.centroid_vectors.get(&centroid_id).cloned();
        }
        None
    }

    // -------------------------------------------------------------------
    // Mutations
    // -------------------------------------------------------------------

    fn add_centroid(&mut self, entry: &CentroidEntry, epoch: u64) -> Result<()> {
        self.snapshots.update_latest_epoch(epoch);
        self.centroid_vectors
            .insert(entry.centroid_id, entry.vector.clone());
        self.mutation_log.push_back(MutationEntry {
            centroid_id: entry.centroid_id,
            epoch,
            kind: MutationKind::Add {
                vector: entry.vector.clone(),
            },
        });
        self.apply_mutations();
        Ok(())
    }

    fn remove_centroid(&mut self, centroid_id: u64, epoch: u64) -> Result<()> {
        self.snapshots.update_latest_epoch(epoch);
        if !self.centroid_vectors.contains_key(&centroid_id) {
            return Err(Error::Internal(format!(
                "Centroid {} not found in graph",
                centroid_id
            )));
        }
        self.mutation_log.push_back(MutationEntry {
            centroid_id,
            epoch,
            kind: MutationKind::Delete,
        });
        self.apply_mutations();
        Ok(())
    }

    /// Apply mutations whose epoch is at or below the retention watermark
    /// to the usearch index.
    fn apply_mutations(&mut self) {
        let watermark = self.snapshots.retention_watermark();
        while let Some(front) = self.mutation_log.front() {
            if front.epoch > watermark {
                break;
            }
            let entry = self.mutation_log.pop_front().unwrap();
            match entry.kind {
                MutationKind::Add { vector } => {
                    let key = self.next_key;
                    self.next_key += 1;
                    self.index
                        .add(key, &vector)
                        .expect("unexpected usearch add error");
                    self.key_to_centroid.insert(key, entry.centroid_id);
                    self.centroid_to_key.insert(entry.centroid_id, key);
                    // centroid_vectors already populated in add_centroid()
                }
                MutationKind::Delete => {
                    if let Some(key) = self.centroid_to_key.remove(&entry.centroid_id) {
                        self.key_to_centroid.remove(&key);
                        self.index
                            .remove(key)
                            .expect("unexpected usearch remove error");
                    }
                    self.centroid_vectors.remove(&entry.centroid_id);
                }
            }
        }
    }

    // -------------------------------------------------------------------
    // Snapshots
    // -------------------------------------------------------------------

    fn snapshot(&mut self) -> (Uuid, u64) {
        self.snapshots.reserve_snapshot()
    }

    fn release_snapshot(&mut self, id: Uuid) {
        self.snapshots.release_snapshot(id);
    }
}

// ---------------------------------------------------------------------------
// Distance computation
// ---------------------------------------------------------------------------

/// Compute distance between two vectors using the given metric, matching
/// the distance values returned by usearch.
fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 => a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum(),
        DistanceMetric::DotProduct => {
            1.0 - a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
        }
    }
}

// ---------------------------------------------------------------------------
// MVCC bookkeeping
// ---------------------------------------------------------------------------

struct MVCC {
    latest_epoch: u64,
    snapshots: HashMap<Uuid, u64>,
}

impl MVCC {
    fn new() -> Self {
        Self {
            latest_epoch: 0,
            snapshots: HashMap::new(),
        }
    }

    fn latest_epoch(&self) -> u64 {
        self.latest_epoch
    }

    fn update_latest_epoch(&mut self, epoch: u64) {
        self.latest_epoch = max(epoch, self.latest_epoch);
    }

    fn reserve_snapshot(&mut self) -> (Uuid, u64) {
        let id = Uuid::new_v4();
        self.snapshots.insert(id, self.latest_epoch);
        (id, self.latest_epoch)
    }

    fn release_snapshot(&mut self, id: Uuid) {
        self.snapshots.remove(&id);
    }

    /// The lowest epoch still referenced by an outstanding snapshot.
    /// Mutations at or below this epoch can safely be applied to the index.
    fn retention_watermark(&self) -> u64 {
        self.snapshots
            .values()
            .min()
            .cloned()
            .unwrap_or(self.latest_epoch)
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_and_search_l2_graph() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        let results = graph.search(&[0.9, 0.1, 0.1], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 1);
    }

    #[test]
    fn should_return_multiple_neighbors() {
        let centroids = vec![
            CentroidEntry::new(1, vec![0.0]),
            CentroidEntry::new(2, vec![1.0]),
            CentroidEntry::new(3, vec![2.0]),
            CentroidEntry::new(4, vec![3.0]),
            CentroidEntry::new(5, vec![4.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        let results = graph.search(&[2.1], 3);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], 3);
    }

    #[test]
    fn should_reject_empty_centroids() {
        let result = UsearchCentroidGraph::build(vec![], DistanceMetric::L2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot build HNSW graph with no centroids"));
    }

    #[test]
    fn should_reject_mismatched_dimensions() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 2.0]),
            CentroidEntry::new(2, vec![3.0, 4.0, 5.0]),
        ];
        let result = UsearchCentroidGraph::build(centroids, DistanceMetric::L2);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Centroid dimension mismatch"));
    }

    #[test]
    fn should_handle_k_larger_than_centroid_count() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0]),
            CentroidEntry::new(2, vec![2.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        let results = graph.search(&[1.5], 10);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn should_add_centroid_and_find_it() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        assert_eq!(graph.len(), 2);

        graph
            .add_centroid(&CentroidEntry::new(3, vec![0.0, 0.0, 1.0]), 2)
            .unwrap();

        assert_eq!(graph.len(), 3);
        let results = graph.search(&[0.0, 0.0, 0.9], 1);
        assert_eq!(results[0], 3);
    }

    #[test]
    fn should_remove_centroid() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        graph.remove_centroid(2, 2).unwrap();

        assert_eq!(graph.len(), 2);
        let results = graph.search(&[0.0, 0.9, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert!(!results.contains(&2), "removed centroid should not appear");
    }

    #[test]
    fn should_search_after_add_and_remove() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
            CentroidEntry::new(3, vec![-1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        graph.remove_centroid(1, 2).unwrap();
        graph
            .add_centroid(&CentroidEntry::new(4, vec![0.5, 0.5]), 2)
            .unwrap();

        assert_eq!(graph.len(), 3);
        let results = graph.search(&[0.5, 0.5], 1);
        assert_eq!(results[0], 4);
    }

    #[test]
    fn should_get_centroid_vector() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        assert_eq!(graph.get_centroid_vector(1), Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(graph.get_centroid_vector(2), Some(vec![0.0, 1.0, 0.0]));
        assert_eq!(graph.get_centroid_vector(99), None);
    }

    // ========================================================================
    // Epoch-based snapshot read tests
    // ========================================================================

    #[test]
    fn search_at_epoch_should_exclude_centroids_added_after_epoch() {
        // given - build with 2 centroids, snapshot at epoch 0, add a third at epoch 5
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph
            .add_centroid(&CentroidEntry::new(3, vec![0.0, 0.0, 1.0]), 5)
            .unwrap();

        // snapshot at epoch 0 should NOT see centroid 3
        let results = snapshot.search(&[0.0, 0.0, 0.9], 3);
        assert!(!results.contains(&3));
        assert_eq!(results.len(), 2);

        // current graph should see centroid 3
        let results = graph.search(&[0.0, 0.0, 0.9], 3);
        assert_eq!(results[0], 3);
    }

    #[test]
    fn search_at_epoch_should_include_deleted_centroid_at_earlier_epoch() {
        // given - build 3 centroids, snapshot at epoch 0, delete centroid 2 at epoch 5
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph.remove_centroid(2, 5).unwrap();

        // snapshot at epoch 0 should still see centroid 2 (deletion is in log,
        // not yet applied; epoch 5 > snapshot epoch 0)
        let results = snapshot.search(&[0.0, 0.9, 0.0], 3);
        assert!(results.contains(&2));

        // current graph should NOT see centroid 2
        let results = graph.search(&[0.0, 0.9, 0.0], 3);
        assert!(!results.contains(&2));
    }

    #[test]
    fn drop_snapshot_should_apply_mutations() {
        // given - snapshot holds watermark, preventing delete from being applied
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph.remove_centroid(2, 3).unwrap();

        // snapshot still sees centroid 2
        let results = snapshot.search(&[0.0, 0.9], 2);
        assert!(results.contains(&2));

        // when - drop snapshot, force apply
        drop(snapshot);
        graph.apply_pending_mutations();

        // then - mutation applied: centroid 2 removed from index and vectors
        assert_eq!(graph.get_centroid_vector(2), None);
        let inner = graph.inner.read().unwrap();
        assert!(inner.mutation_log.is_empty());
        assert!(!inner.centroid_to_key.contains_key(&2));
    }

    #[test]
    fn drop_snapshot_should_not_interfere_with_other_snapshot() {
        // given - two snapshots at different epochs
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0])];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snap1 = graph.snapshot().unwrap(); // epoch 0
        graph
            .add_centroid(&CentroidEntry::new(2, vec![0.0, 1.0]), 5)
            .unwrap();
        let snap2 = graph.snapshot().unwrap(); // epoch 5
        graph.remove_centroid(2, 8).unwrap();

        // when - drop snap1, force apply
        drop(snap1);
        graph.apply_pending_mutations();

        // then - only mutations up to watermark (5) are applied;
        // the delete at epoch 8 stays in the log
        {
            let inner = graph.inner.read().unwrap();
            assert_eq!(inner.mutation_log.len(), 1);
            assert_eq!(inner.mutation_log[0].centroid_id, 2);
        }

        // snap2 (epoch 5) should still see centroid 2
        // (add at 5 was applied to index; delete at 8 > 5 not visible)
        let results = snap2.search(&[0.0, 0.9], 2);
        assert!(results.contains(&2));

        drop(snap2);
    }

    #[test]
    fn get_centroid_vector_at_epoch_should_find_deleted_centroid() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph.remove_centroid(2, 5).unwrap();

        // current: centroid 2 is deleted
        assert_eq!(graph.get_centroid_vector(2), None);
        // snapshot at epoch 0: centroid 2 still visible
        assert_eq!(snapshot.get_centroid_vector(2), Some(vec![0.0, 1.0]));

        drop(snapshot);
    }

    #[test]
    fn mutations_applied_immediately_when_no_snapshots() {
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // No snapshots → mutations applied directly to index
        graph
            .add_centroid(&CentroidEntry::new(3, vec![0.5, 0.5]), 1)
            .unwrap();
        graph.remove_centroid(2, 2).unwrap();

        let inner = graph.inner.read().unwrap();
        assert!(inner.mutation_log.is_empty());
        assert_eq!(inner.key_to_centroid.len(), 2); // 1 and 3
        assert!(inner.centroid_to_key.contains_key(&3));
        assert!(!inner.centroid_to_key.contains_key(&2));
    }

    #[test]
    fn add_then_delete_within_log_should_cancel_out() {
        // given - snapshot pins watermark at 0
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0])];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap(); // epoch 0

        graph
            .add_centroid(&CentroidEntry::new(2, vec![0.0, 1.0]), 5)
            .unwrap();
        graph.remove_centroid(2, 8).unwrap();

        // Current view (epoch 8): centroid 2 was added then deleted → not visible
        assert_eq!(graph.len(), 1);
        assert_eq!(graph.get_centroid_vector(2), None);

        // Snapshot at epoch 0: centroid 2 not yet added → not visible
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot.get_centroid_vector(2), None);

        drop(snapshot);

        // After applying: add+delete cancel, centroid 2 not in index
        graph.apply_pending_mutations();
        let inner = graph.inner.read().unwrap();
        assert!(inner.mutation_log.is_empty());
        assert!(!inner.centroid_to_key.contains_key(&2));
    }
}

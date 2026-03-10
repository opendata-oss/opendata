//! HNSW implementation using the usearch library.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::cmp::max;
use std::fmt;
use std::sync::{Arc, RwLock};

use usearch::{Index, IndexOptions, MetricKind, ScalarKind};
use uuid::Uuid;
use crate::error::{Error, Result};

use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

use super::{CentroidGraph, CentroidGraphRead};

/// Initial capacity reserved for the usearch index.
/// Kept artificially high to avoid usearch deadlock issues near capacity limits.
const INITIAL_CAPACITY: usize = 200_000;

/// Entry in the delete log for epoch-based snapshot reads.
/// When a centroid is removed, it is immediately hard-deleted from the usearch
/// index but recorded here so that snapshot reads at earlier epochs can still
/// find it.
#[derive(Debug, Clone)]
struct DeleteLogEntry {
    centroid_id: u64,
    added_epoch: u64,
    deleted_epoch: u64,
    vector: Vec<f32>,
}

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
            .field("num_centroids", &inner.key_to_centroid.len())
            .finish()
    }
}

impl UsearchCentroidGraph {
    /// Build a new HNSW graph from centroids using usearch.
    ///
    /// # Arguments
    /// * `centroids` - Vector of centroid entries with their IDs and vectors
    /// * `distance_metric` - Distance metric to use for similarity computation
    ///
    /// # Returns
    /// A UsearchCentroidGraph ready for searching
    pub fn build(
        centroids: Vec<CentroidEntry>,
        distance_metric: DistanceMetric,
    ) -> Result<Self> {
        if centroids.is_empty() {
            return Err(Error::InvalidInput(
                "Cannot build HNSW graph with no centroids".to_string(),
            ));
        }

        // Validate all centroids have the same dimensionality
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

        // Convert distance metric to usearch MetricKind
        let metric = match distance_metric {
            DistanceMetric::L2 => MetricKind::L2sq,
            DistanceMetric::DotProduct => MetricKind::IP,
        };

        // Create index options
        let options = IndexOptions {
            dimensions,
            metric,
            quantization: ScalarKind::F32,
            connectivity: 16,      // M parameter
            expansion_add: 200,    // ef_construction
            expansion_search: 100, // ef_search default
            multi: false,
        };

        // Create index
        let index = Index::new(&options).map_err(|e| Error::Internal(e.to_string()))?;

        // Reserve 200K capacity upfront
        index
            .reserve(INITIAL_CAPACITY)
            .map_err(|e| Error::Internal(e.to_string()))?;

        // Build mappings and insert. Initial centroids are recorded at epoch 0.
        let mut key_to_centroid = HashMap::with_capacity(centroids.len());
        let mut centroid_to_key = HashMap::with_capacity(centroids.len());
        let mut centroid_vectors = HashMap::with_capacity(centroids.len());

        for (key, centroid) in centroids.iter().enumerate() {
            let key = key as u64;
            index
                .add(key, &centroid.vector)
                .map_err(|e| Error::Internal(e.to_string()))?;
            key_to_centroid.insert(key, (centroid.centroid_id, 0u64));
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
                delete_log: VecDeque::new(),
                snapshots: MVCC::new(),
            })),
        })
    }

    #[cfg(test)]
    fn clean_delete_log(&self) {
        self.inner.write().expect("lock poisoned").clean_delete_log();
    }
}

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
        let (id, epoch) = self.inner
            .write()
            .expect("lock poisoned")
            .snapshot();
        Ok(Arc::new(
            UsearchCentroidGraphSnapshot {
                id,
                epoch,
                inner: Arc::clone(&self.inner),
            }
        ))
    }
}

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
        self.inner.read().expect("lock poisoned").search_at_epoch(query, k, self.epoch)
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.inner.read().expect("lock poisoned").get_centroid_vector_at_epoch(centroid_id, self.epoch)
    }

    fn len(&self) -> usize {
        self.inner.read().expect("lock poisoned").live_count_at_epoch(self.epoch)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Inner state for UsearchCentroidGraph, protected by a single RwLock.
struct UsearchCentroidGraphInner {
    /// The usearch index (thread-safe internally)
    index: Index,
    /// Map from usearch key to (centroid_id, added_epoch)
    key_to_centroid: HashMap<u64, (u64, u64)>,
    /// Reverse map from centroid_id to usearch key (for O(1) removal)
    centroid_to_key: HashMap<u64, u64>,
    /// Centroid vectors indexed by centroid_id (only live centroids)
    centroid_vectors: HashMap<u64, Vec<f32>>,
    /// Next usearch key to allocate
    next_key: u64,
    /// Distance metric for computing distances in delete log lookups
    distance_metric: DistanceMetric,
    /// Log of recently deleted centroids, ordered by deletion epoch.
    /// Entries are kept until all snapshots that might reference them are released.
    delete_log: VecDeque<DeleteLogEntry>,
    snapshots: MVCC,
}

impl UsearchCentroidGraphInner {
    /// Search at the current epoch using vanilla (unfiltered) search.
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        let results = self.index.search(query, k).expect("unexpected usearch error");
        results
            .keys
            .iter()
            .filter_map(|&key| self.key_to_centroid.get(&key).map(|(cid, _)| *cid))
            .collect()
    }

    /// Search at a specific epoch. Uses filtered search to exclude centroids
    /// added after the epoch, then walks the delete log to recover centroids
    /// that were deleted after the epoch but were visible at it.
    fn search_at_epoch(&self, query: &[f32], k: usize, epoch: u64) -> Vec<u64> {
        // Step 1: filtered search excluding centroids added after the epoch
        let results = self.index.filtered_search(query, k, |key| {
            self.key_to_centroid
                .get(&key)
                .map_or(false, |(_, added_epoch)| *added_epoch <= epoch)
        }).expect("unexpected usearch error");

        let mut candidates: Vec<(u64, f32)> = results
            .keys
            .iter()
            .zip(results.distances.iter())
            .filter_map(|(&key, &dist)| {
                self.key_to_centroid.get(&key).map(|(cid, _)| (*cid, dist))
            })
            .collect();

        // Step 2: walk delete log for centroids that were visible at this epoch
        // but have since been deleted
        for entry in &self.delete_log {
            if entry.deleted_epoch > epoch && entry.added_epoch <= epoch {
                let dist = compute_distance(
                    &entry.vector,
                    query,
                    self.distance_metric,
                );
                // Include if we have fewer than k results, or this beats the worst
                let dominated = candidates.len() >= k
                    && candidates.last().map_or(false, |(_, d)| dist >= *d);
                if !dominated {
                    candidates.push((entry.centroid_id, dist));
                    candidates.sort_by(|a, b| {
                        a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    candidates.truncate(k);
                }
            }
        }

        candidates.into_iter().map(|(cid, _)| cid).collect()
    }

    fn live_count(&self) -> usize {
        self.key_to_centroid.len()
    }

    fn live_count_at_epoch(&self, epoch: u64) -> usize {
        let live = self
            .key_to_centroid
            .values()
            .filter(|(_, added)| *added <= epoch)
            .count();
        let deleted_visible = self
            .delete_log
            .iter()
            .filter(|e| e.added_epoch <= epoch && e.deleted_epoch > epoch)
            .count();
        live + deleted_visible
    }

    fn add_centroid(&mut self, entry: &CentroidEntry, epoch: u64) -> Result<()> {
        self.snapshots.update_latest_epoch(epoch);
        let key = self.next_key;
        self.next_key += 1;

        self.index
            .add(key, &entry.vector)
            .map_err(|e| Error::Internal(e.to_string()))?;

        self.key_to_centroid.insert(key, (entry.centroid_id, epoch));
        self.centroid_to_key.insert(entry.centroid_id, key);
        self.centroid_vectors
            .insert(entry.centroid_id, entry.vector.clone());

        self.clean_delete_log();

        Ok(())
    }

    fn remove_centroid(&mut self, centroid_id: u64, epoch: u64) -> Result<()> {
        self.snapshots.update_latest_epoch(epoch);

        let key = self
            .centroid_to_key
            .remove(&centroid_id)
            .ok_or_else(|| {
                Error::Internal(format!("Centroid {} not found in graph", centroid_id))
            })?;

        let (_, added_epoch) = self
            .key_to_centroid
            .remove(&key)
            .expect("key_to_centroid inconsistency");

        let vector = self
            .centroid_vectors
            .remove(&centroid_id)
            .expect("centroid_vectors inconsistency");

        self.index
            .remove(key)
            .map_err(|e| Error::Internal(e.to_string()))?;

        self.delete_log.push_back(DeleteLogEntry {
            centroid_id,
            added_epoch,
            deleted_epoch: epoch,
            vector,
        });

        self.clean_delete_log();

        Ok(())
    }

    /// Remove delete log entries that are no longer needed by any snapshot.
    fn clean_delete_log(&mut self) {
        let watermark = self.snapshots.retention_watermark();
        while let Some(front) = self.delete_log.front() {
            if front.deleted_epoch <= watermark {
                self.delete_log.pop_front();
            } else {
                break;
            }
        }
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.centroid_vectors.get(&centroid_id).cloned()
    }

    fn get_centroid_vector_at_epoch(&self, centroid_id: u64, epoch: u64) -> Option<Vec<f32>> {
        // Check live centroids
        if let Some(&key) = self.centroid_to_key.get(&centroid_id) {
            if let Some((_, added_epoch)) = self.key_to_centroid.get(&key) {
                if *added_epoch <= epoch {
                    return self.centroid_vectors.get(&centroid_id).cloned();
                }
            }
        }
        // Check delete log
        self.delete_log
            .iter()
            .find(|e| {
                e.centroid_id == centroid_id
                    && e.added_epoch <= epoch
                    && e.deleted_epoch > epoch
            })
            .map(|e| e.vector.clone())
    }

    fn snapshot(&mut self) -> (Uuid, u64) {
        self.snapshots.reserve_snapshot()
    }

    fn release_snapshot(&mut self, id: Uuid) {
        self.snapshots.release_snapshot(id);
    }
}

/// Compute distance between two vectors using the given metric, matching
/// the distance values returned by usearch.
fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        // L2sq: sum of squared differences (matches MetricKind::L2sq)
        DistanceMetric::L2 => a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum(),
        // IP: 1 - dot_product (matches MetricKind::IP)
        DistanceMetric::DotProduct => {
            1.0 - a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
        }
    }
}

struct MVCC {
    /// Last written epoch
    latest_epoch: u64,
    /// Set of outstanding snapshot epochs
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
        let id = uuid::Uuid::new_v4();
        self.snapshots.insert(id, self.latest_epoch);
        (id, self.latest_epoch)
    }

    fn release_snapshot(&mut self, id: Uuid) {
        self.snapshots.remove(&id);
    }

    fn retention_watermark(&self) -> u64 {
        self.snapshots.values().min().cloned().unwrap_or(self.latest_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_and_search_l2_graph() {
        // given - 3 centroids
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];

        // when
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let query = vec![0.9, 0.1, 0.1];
        let results = graph.search(&query, 1);

        // then
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 1);
    }

    #[test]
    fn should_return_multiple_neighbors() {
        // given - 5 centroids in a line
        let centroids = vec![
            CentroidEntry::new(1, vec![0.0]),
            CentroidEntry::new(2, vec![1.0]),
            CentroidEntry::new(3, vec![2.0]),
            CentroidEntry::new(4, vec![3.0]),
            CentroidEntry::new(5, vec![4.0]),
        ];

        // when
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let query = vec![2.1];
        let results = graph.search(&query, 3);

        // then
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], 3); // Closest
    }

    #[test]
    fn should_reject_empty_centroids() {
        // given
        let centroids: Vec<CentroidEntry> = vec![];

        // when
        let result = UsearchCentroidGraph::build(centroids, DistanceMetric::L2);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot build HNSW graph with no centroids")
        );
    }

    #[test]
    fn should_reject_mismatched_dimensions() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 2.0]),
            CentroidEntry::new(2, vec![3.0, 4.0, 5.0]), // Wrong dimensions
        ];

        // when
        let result = UsearchCentroidGraph::build(centroids, DistanceMetric::L2);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Centroid dimension mismatch")
        );
    }

    #[test]
    fn should_handle_k_larger_than_centroid_count() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0]),
            CentroidEntry::new(2, vec![2.0]),
        ];

        // when
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let results = graph.search(&[1.5], 10);

        // then
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn should_add_centroid_and_find_it() {
        // given - start with 2 centroids
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        assert_eq!(graph.len(), 2);

        // when - add a third centroid
        let new_entry = CentroidEntry::new(3, vec![0.0, 0.0, 1.0]);
        graph.add_centroid(&new_entry, 2).unwrap();

        // then - graph has 3 centroids and can find the new one
        assert_eq!(graph.len(), 3);
        let results = graph.search(&[0.0, 0.0, 0.9], 1);
        assert_eq!(results[0], 3);
    }

    #[test]
    fn should_remove_centroid() {
        // given - 3 centroids
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when - remove centroid 2
        graph.remove_centroid(2, 2).unwrap();

        // then - graph has 2 centroids and search near [0, 1, 0] returns 1 or 3
        assert_eq!(graph.len(), 2);
        let results = graph.search(&[0.0, 0.9, 0.0], 2);
        assert_eq!(results.len(), 2);
        assert!(!results.contains(&2), "removed centroid should not appear");
    }

    #[test]
    fn should_search_after_add_and_remove() {
        // given - 3 centroids
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
            CentroidEntry::new(3, vec![-1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when - remove centroid 1, add centroid 4
        graph.remove_centroid(1, 2).unwrap();
        graph
            .add_centroid(&CentroidEntry::new(4, vec![0.5, 0.5]), 2)
            .unwrap();

        // then
        assert_eq!(graph.len(), 3);
        let results = graph.search(&[0.5, 0.5], 1);
        assert_eq!(results[0], 4);
    }

    #[test]
    fn should_get_centroid_vector() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when/then
        assert_eq!(graph.get_centroid_vector(1), Some(vec![1.0, 0.0, 0.0]));
        assert_eq!(graph.get_centroid_vector(2), Some(vec![0.0, 1.0, 0.0]));
        assert_eq!(graph.get_centroid_vector(99), None);
    }

    // ========================================================================
    // Epoch-based snapshot read tests
    // ========================================================================

    #[test]
    fn search_at_epoch_should_exclude_centroids_added_after_epoch() {
        // given - build with 2 centroids at epoch 0, add a third at epoch 5
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph
            .add_centroid(&CentroidEntry::new(3, vec![0.0, 0.0, 1.0]), 5)
            .unwrap();

        // when - search at epoch 0 (before centroid 3 was added)
        let results = snapshot.search(&[0.0, 0.0, 0.9], 3);

        // then - centroid 3 should NOT be visible
        assert!(!results.contains(&3));
        assert_eq!(results.len(), 2);
        let results = graph.search(&[0.0, 0.0, 0.9], 3);
        // centroid 3 should be visible in graph
        assert_eq!(results[0], 3);
    }

    #[test]
    fn search_at_epoch_should_include_deleted_centroid_at_earlier_epoch() {
        // given - build and delete centroid 2 at epoch 5
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph.remove_centroid(2, 5).unwrap();

        // when - search at epoch 0 (before deletion)
        let results = snapshot.search(&[0.0, 0.9, 0.0], 3);

        // then - centroid 2 should still be visible via delete log
        assert!(results.contains(&2));
        let results = graph.search(&[0.0, 0.9, 0.0], 3);
        assert!(!results.contains(&2));
    }

    #[test]
    fn drop_snapshot_should_clean_delete_log() {
        // given - build at epoch 0, delete at epoch 3
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph.remove_centroid(2, 3).unwrap();

        // snapshot can still find centroid 2 via delete log
        let results = snapshot.search(&[0.0, 0.9], 2);
        assert!(results.contains(&2));

        // when - drop the snapshot and force cleanup
        drop(snapshot);
        graph.clean_delete_log();

        // then - centroid 2 is gone from the graph and delete log is cleaned
        assert_eq!(graph.get_centroid_vector(2), None);
        let inner = graph.inner.read().unwrap();
        assert!(inner.delete_log.is_empty());
        assert!(!inner.centroid_to_key.contains_key(&2));
    }

    #[test]
    fn drop_snapshot_should_not_interfere_with_other_snapshot() {
        // given - add centroid at epoch 5, delete at epoch 8
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0])];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snap1 = graph.snapshot().unwrap();
        graph
            .add_centroid(&CentroidEntry::new(2, vec![0.0, 1.0]), 5)
            .unwrap();
        let snap2 = graph.snapshot().unwrap();
        graph.remove_centroid(2, 8).unwrap();

        // when - drop snap1 and force cleanup
        drop(snap1);
        graph.clean_delete_log();

        // then - delete log entry is NOT cleaned (snap2 at epoch 5 still needs it)
        {
            let inner = graph.inner.read().unwrap();
            assert_eq!(inner.delete_log.len(), 1);
            assert_eq!(inner.delete_log[0].centroid_id, 2);
        }

        // snap2 should still see centroid 2 (added at 5 <= snap epoch 5, deleted at 8 > 5)
        let results = snap2.search(&[0.0, 0.9], 2);
        assert!(results.contains(&2));

        drop(snap2);
    }

    #[test]
    fn get_centroid_vector_at_epoch_should_find_deleted_centroid() {
        // given - build and delete centroid 2
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph.remove_centroid(2, 5).unwrap();

        // then - current view: centroid 2 gone; snapshot: centroid 2 present
        assert_eq!(graph.get_centroid_vector(2), None);
        assert_eq!(snapshot.get_centroid_vector(2), Some(vec![0.0, 1.0]));

        drop(snapshot);
    }
}

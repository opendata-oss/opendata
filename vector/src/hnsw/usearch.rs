//! HNSW implementation using the usearch library.

use std::cmp::max;
use std::collections::HashMap;
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

/// Tracks when a centroid was added and optionally deleted, for epoch-based
/// snapshot visibility.
#[derive(Debug, Clone)]
struct CentroidMutation {
    added_epoch: u64,
    deleted_epoch: Option<u64>,
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
        epoch: u64,
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

        // Build mappings and insert
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
                mutations: HashMap::new(),
                cleaned_watermark: 0,
                snapshots: MVCC::new(),
            })),
        })
    }

    #[cfg(test)]
    fn clean_mutations(&self) {
        self.inner.write().expect("lock poisoned").clean_mutations();
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
    /// Map from usearch key to centroid_id
    key_to_centroid: HashMap<u64, u64>,
    /// Reverse map from centroid_id to usearch key (for O(1) removal)
    centroid_to_key: HashMap<u64, u64>,
    /// Centroid vectors indexed by centroid_id
    centroid_vectors: HashMap<u64, Vec<f32>>,
    /// Next usearch key to allocate
    next_key: u64,
    /// Tracks recent mutations for epoch-based snapshot reads.
    /// Keyed by centroid_id.
    mutations: HashMap<u64, CentroidMutation>,
    /// The watermark up to which mutations have actually been cleaned.
    cleaned_watermark: u64,
    snapshots: MVCC
}

impl UsearchCentroidGraphInner {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.search_at_epoch(query, k, self.snapshots.latest_epoch())
    }

    fn search_at_epoch(&self, query: &[f32], k: usize, epoch: u64) -> Vec<u64> {
        let results = self.index.filtered_search(query, k, |key| {
            self.key_to_centroid
                .get(&key)
                .map_or(false, |cid| self.is_visible_at_epoch(*cid, epoch))
        }).expect("unexpected usearch error");
        results
            .keys
            .iter()
            .filter_map(|&key| self.key_to_centroid.get(&key).copied())
            .collect()
    }

    /// A centroid is visible at an epoch if:
    /// - It has no mutation entry (predates tracking, always visible), OR
    /// - It was added at or before the epoch AND not deleted as of the epoch
    fn is_visible_at_epoch(&self, centroid_id: u64, epoch: u64) -> bool {
        // should only be called for centroids that are not hard-deleted
        assert!(self.key_to_centroid.get(&centroid_id).is_some());
        match self.mutations.get(&centroid_id) {
            None => true,
            Some(m) => {
                m.added_epoch <= epoch
                    && m.deleted_epoch.map_or(true, |deleted| deleted > epoch)
            }
        }
    }

    /// Returns true if the centroid has been soft-deleted (has a deleted_epoch set).
    fn is_deleted(&self, centroid_id: u64) -> bool {
        self.mutations
            .get(&centroid_id)
            .is_some_and(|m| m.deleted_epoch.is_some())
    }

    fn live_count(&self) -> usize {
        self.live_count_at_epoch(self.snapshots.latest_epoch())
    }

    fn live_count_at_epoch(&self, epoch: u64) -> usize {
        self.key_to_centroid
            .values()
            .filter(|cid| self.is_visible_at_epoch(**cid, epoch))
            .count()
    }

    fn add_centroid(&mut self, entry: &CentroidEntry, epoch: u64) -> Result<()> {
        self.snapshots.update_latest_epoch(epoch);
        let key = self.next_key;
        self.next_key += 1;

        self.index
            .add(key, &entry.vector)
            .map_err(|e| Error::Internal(e.to_string()))?;

        self.key_to_centroid.insert(key, entry.centroid_id);
        self.centroid_to_key.insert(entry.centroid_id, key);
        self.centroid_vectors
            .insert(entry.centroid_id, entry.vector.clone());
        self.mutations.insert(
            entry.centroid_id,
            CentroidMutation {
                added_epoch: epoch,
                deleted_epoch: None,
            },
        );

        self.clean_mutations();

        Ok(())
    }

    fn remove_centroid(&mut self, centroid_id: u64, epoch: u64) -> Result<()> {
        self.snapshots.update_latest_epoch(epoch);
        if !self.centroid_to_key.contains_key(&centroid_id) {
            return Err(Error::Internal(format!(
                "Centroid {} not found in graph",
                centroid_id
            )));
        }

        // Soft delete: record the deletion epoch but keep the centroid in the
        // usearch index so snapshot reads at earlier epochs can still find it.
        match self.mutations.get_mut(&centroid_id) {
            Some(m) => {
                m.deleted_epoch = Some(epoch);
            }
            None => {
                self.mutations.insert(
                    centroid_id,
                    CentroidMutation {
                        added_epoch: 0,
                        deleted_epoch: Some(epoch),
                    },
                );
            }
        }

        self.clean_mutations();

        Ok(())
    }

    fn clean_mutations(&mut self) {
        let watermark = self.snapshots.retention_watermark();
        if watermark <= self.cleaned_watermark {
            return;
        }

        // Collect centroid_ids whose mutations are fully below the watermark.
        let to_remove: Vec<u64> = self
            .mutations
            .iter()
            .filter(|(_, m)| {
                m.added_epoch < watermark && m.deleted_epoch.map_or(true, |d| d < watermark)
            })
            .map(|(cid, _)| *cid)
            .collect();

        for centroid_id in to_remove {
            let mutation = self.mutations.remove(&centroid_id).unwrap();
            // If the centroid was deleted, hard-remove it from the index now.
            if mutation.deleted_epoch.is_some() {
                self.delete_centroid(centroid_id);
            }
        }

        self.cleaned_watermark = watermark;
    }

    fn delete_centroid(&mut self, centroid_id: u64) {
        let key = self.centroid_to_key.remove(&centroid_id).expect("unexpected missing centroid");
        self.key_to_centroid.remove(&key);
        self.centroid_vectors.remove(&centroid_id);
        self.index.remove(key).expect("unexpected error removing centroid");
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.get_centroid_vector_at_epoch(centroid_id, self.snapshots.latest_epoch())
    }

    fn get_centroid_vector_at_epoch(&self, centroid_id: u64, epoch: u64) -> Option<Vec<f32>> {
        let v = self.centroid_vectors.get(&centroid_id);
        match v {
            Some(_) if self.is_visible_at_epoch(centroid_id, epoch) => v.cloned(),
            _ => None,
        }
    }

    fn snapshot(&mut self) -> (Uuid, u64) {
        self.snapshots.reserve_snapshot()
    }

    fn release_snapshot(&mut self, id: Uuid) {
        self.snapshots.release_snapshot(id);
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
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 0).unwrap();
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
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 0).unwrap();
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
        let result = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 0);

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
        let result = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 0);

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
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 0).unwrap();
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
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 1).unwrap();
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
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 1).unwrap();

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
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 1).unwrap();

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
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 0).unwrap();

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
        // given - build with 2 centroids at epoch 1, add a third at epoch 5
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 1).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph
            .add_centroid(&CentroidEntry::new(3, vec![0.0, 0.0, 1.0]), 5)
            .unwrap();

        // when - search at epoch 3 (before centroid 3 was added)
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
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 1).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph.remove_centroid(2, 5).unwrap();

        // when - search at epoch 3 (before deletion)
        let results = snapshot.search(&[0.0, 0.9, 0.0], 3);

        // then - centroid 2 should still be visible
        assert!(results.contains(&2));
        let results = graph.search(&[0.0, 0.9, 0.0], 3);
        assert!(!results.contains(&2));
    }

    #[test]
    fn drop_snapshot_should_hard_remove_deleted_centroids() {
        // given - build at epoch 1, delete at epoch 3
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 1).unwrap();
        let c2_key = graph.inner.read().unwrap().centroid_to_key[&2];
        let snapshot = graph.snapshot().unwrap();
        graph.remove_centroid(2, 3).unwrap();
        let results = snapshot.search(&[0.0, 0.9], 2);
        assert!(results.contains(&2));

        // when - drop the snapshot and force cleanup
        drop(snapshot);
        graph.clean_mutations();

        // then - centroid 2 is hard-removed; its vector is gone
        assert_eq!(graph.get_centroid_vector(2), None);
        let inner = graph.inner.read().unwrap();
        assert!(!inner.mutations.contains_key(&2));
        assert!(!inner.centroid_to_key.contains_key(&2));
        assert!(!inner.key_to_centroid.contains_key(&c2_key));
        let mut tmp = [0.0; 2];
        let found = inner.index.get(c2_key, &mut tmp).unwrap();
        assert_eq!(found, 0);
    }

    #[test]
    fn drop_snapshot_should_clean_live_entries_below_watermark() {
        // given - build at epoch 1
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 1).unwrap();
        let snapshot = graph.snapshot().unwrap();
        graph.add_centroid(&CentroidEntry::new(3, vec![0.0, 0.0]), 3).unwrap();

        // when - advance watermark past the add epoch and clean
        drop(snapshot);
        graph.clean_mutations();

        // then - mutations are cleaned but centroids remain in the index
        let inner = graph.inner.read().unwrap();
        assert!(inner.mutations.is_empty());
        assert_eq!(inner.key_to_centroid.len(), 3);
    }

    #[test]
    fn update_retention_watermark_should_not_clean_entries_at_or_above_watermark() {
        // given - add centroid at epoch 5, delete at epoch 8
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0])];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2, 1).unwrap();
        graph
            .add_centroid(&CentroidEntry::new(2, vec![0.0, 1.0]), 5)
            .unwrap();
        let snap1 = graph.snapshot().unwrap();
        graph.remove_centroid(2, 8).unwrap();
        let snap2 = graph.snapshot().unwrap();

        // when
        drop(snap1);
        graph.clean_mutations();

        // then - centroid 2's mutation is NOT cleaned
        let inner = graph.inner.read().unwrap();
        assert!(inner.mutations.contains_key(&2));
        assert_eq!(inner.mutations.len(), 1);
        drop(snap2);
    }
}

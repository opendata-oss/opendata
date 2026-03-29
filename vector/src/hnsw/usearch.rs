//! HNSW implementation using the usearch library.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::RwLock;

use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use crate::error::{Error, Result};

use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

use super::CentroidGraph;

/// Initial capacity reserved for the usearch index.
/// Kept artificially high to avoid usearch deadlock issues near capacity limits.
const INITIAL_CAPACITY: usize = 200_000;

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
    /// Distance metric used for computing distances
    distance_metric: DistanceMetric,
}

/// HNSW graph implementation using the usearch library.
///
/// Uses interior mutability for thread-safe mutation behind `Arc<dyn CentroidGraph>`.
/// The usearch `Index` is internally thread-safe for `add`/`remove`/`search`.
pub struct UsearchCentroidGraph {
    inner: RwLock<UsearchCentroidGraphInner>,
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
    pub fn build(centroids: Vec<CentroidEntry>, distance_metric: DistanceMetric) -> Result<Self> {
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
            inner: RwLock::new(UsearchCentroidGraphInner {
                index,
                key_to_centroid,
                centroid_to_key,
                centroid_vectors,
                next_key,
                distance_metric,
            }),
        })
    }
}

impl CentroidGraph for UsearchCentroidGraph {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.inner.read().expect("lock poisoned").search(query, k)
    }

    fn search_with_include_exclude(
        &self,
        query: &[f32],
        k: usize,
        include: &[&CentroidEntry],
        exclude: &HashSet<u64>,
    ) -> Vec<u64> {
        self.inner
            .read()
            .expect("lock poisoned")
            .search_with_include_exclude(query, k, include, exclude)
    }

    fn add_centroid(&self, entry: &CentroidEntry) -> Result<()> {
        self.inner
            .write()
            .expect("lock poisoned")
            .add_centroid(entry)
    }

    fn remove_centroid(&self, centroid_id: u64) -> Result<()> {
        self.inner
            .write()
            .expect("lock poisoned")
            .remove_centroid(centroid_id)
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.inner
            .read()
            .expect("lock poisoned")
            .get_centroid_vector(centroid_id)
    }

    fn all_centroid_ids(&self) -> Vec<u64> {
        self.inner.read().expect("lock poisoned").all_centroid_ids()
    }

    fn len(&self) -> usize {
        self.inner.read().expect("lock poisoned").len()
    }
}

impl UsearchCentroidGraphInner {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        let k = k.min(self.key_to_centroid.len());
        if k == 0 {
            return Vec::new();
        }

        // Search usearch index — request more than k to account for removed entries
        let search_k = (k + 10).min(self.key_to_centroid.len() + 10);
        let results = match self.index.search(query, search_k) {
            Ok(matches) => matches,
            Err(_) => return Vec::new(),
        };

        // Map keys back to centroid_ids, filtering out removed entries
        results
            .keys
            .iter()
            .filter_map(|&key| self.key_to_centroid.get(&key).copied())
            .take(k)
            .collect()
    }

    fn search_with_include_exclude(
        &self,
        query: &[f32],
        k: usize,
        include: &[&CentroidEntry],
        exclude: &HashSet<u64>,
    ) -> Vec<u64> {
        let graph_size = self.key_to_centroid.len();
        let search_k = (k + 10).min(graph_size + 10);

        let mut candidates: Vec<(u64, f32)> = Vec::with_capacity(k + include.len());

        if graph_size > 0 && search_k > 0 {
            // Build set of usearch keys to exclude
            let excluded_keys: HashSet<u64> = exclude
                .iter()
                .filter_map(|centroid_id| self.centroid_to_key.get(centroid_id).copied())
                .collect();

            let results = if excluded_keys.is_empty() {
                self.index.search(query, search_k)
            } else {
                self.index
                    .filtered_search(query, search_k, |key| !excluded_keys.contains(&key))
            };

            if let Ok(results) = results {
                for (&key, &dist) in results.keys.iter().zip(results.distances.iter()) {
                    if let Some(&centroid_id) = self.key_to_centroid.get(&key) {
                        candidates.push((centroid_id, dist));
                    }
                }
            }
        }

        // Compute distances for include centroids (not in the graph)
        for entry in include {
            let dist = self.compute_distance(query, &entry.vector);
            candidates.push((entry.centroid_id, dist));
        }

        // Sort by distance ascending and take top k
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.into_iter().take(k).map(|(id, _)| id).collect()
    }

    fn compute_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.distance_metric {
            DistanceMetric::L2 => a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum(),
            DistanceMetric::DotProduct => {
                1.0 - a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
            }
        }
    }

    fn add_centroid(&mut self, entry: &CentroidEntry) -> Result<()> {
        let key = self.next_key;
        self.next_key += 1;

        self.index
            .add(key, &entry.vector)
            .map_err(|e| Error::Internal(e.to_string()))?;

        self.key_to_centroid.insert(key, entry.centroid_id);
        self.centroid_to_key.insert(entry.centroid_id, key);
        self.centroid_vectors
            .insert(entry.centroid_id, entry.vector.clone());

        Ok(())
    }

    fn remove_centroid(&mut self, centroid_id: u64) -> Result<()> {
        let key = self.centroid_to_key.remove(&centroid_id).ok_or_else(|| {
            Error::Internal(format!("Centroid {} not found in graph", centroid_id))
        })?;

        self.key_to_centroid.remove(&key);
        self.centroid_vectors.remove(&centroid_id);
        self.index
            .remove(key)
            .map_err(|e| Error::Internal(e.to_string()))?;

        Ok(())
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.centroid_vectors.get(&centroid_id).cloned()
    }

    fn all_centroid_ids(&self) -> Vec<u64> {
        self.centroid_vectors.keys().copied().collect()
    }

    fn len(&self) -> usize {
        self.key_to_centroid.len()
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
        graph.add_centroid(&new_entry).unwrap();

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
        graph.remove_centroid(2).unwrap();

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
        graph.remove_centroid(1).unwrap();
        graph
            .add_centroid(&CentroidEntry::new(4, vec![0.5, 0.5]))
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

    #[test]
    fn should_track_vectors_on_add_and_remove() {
        // given
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0])];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when - add centroid
        graph
            .add_centroid(&CentroidEntry::new(2, vec![0.0, 1.0]))
            .unwrap();
        assert_eq!(graph.get_centroid_vector(2), Some(vec![0.0, 1.0]));

        // when - remove centroid
        graph.remove_centroid(2).unwrap();
        assert_eq!(graph.get_centroid_vector(2), None);
    }

    // ---- search_with_include_exclude ----

    #[test]
    fn should_exclude_centroids_from_search() {
        // given - 3 centroids, query near centroid 1
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let exclude = HashSet::from([1]);

        // when - search near centroid 1 but exclude it
        let results = graph.search_with_include_exclude(&[0.9, 0.1, 0.0], 1, &[], &exclude);

        // then - should return centroid 2 or 3, not 1
        assert_eq!(results.len(), 1);
        assert_ne!(results[0], 1, "excluded centroid should not appear");
    }

    #[test]
    fn should_include_centroids_not_in_graph() {
        // given - graph has centroids 1 and 2, include adds centroid 99 closer to query
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let include_entry = CentroidEntry::new(99, vec![0.5, 0.5]);

        // when - search near [0.5, 0.5], include centroid 99 which is right there
        let results =
            graph.search_with_include_exclude(&[0.5, 0.5], 1, &[&include_entry], &HashSet::new());

        // then - centroid 99 should be the closest
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 99);
    }

    #[test]
    fn should_combine_include_and_exclude() {
        // given - 3 centroids in graph, exclude 1, include 99
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let exclude = HashSet::from([1]);
        let include_entry = CentroidEntry::new(99, vec![0.9, 0.1, 0.0]);

        // when - query near [1,0,0], exclude centroid 1 (the nearest graph centroid),
        // but include 99 which is also near [1,0,0]
        let results =
            graph.search_with_include_exclude(&[1.0, 0.0, 0.0], 2, &[&include_entry], &exclude);

        // then - should include 99 and one of 2/3, but not 1
        assert_eq!(results.len(), 2);
        assert!(results.contains(&99));
        assert!(!results.contains(&1));
    }

    #[test]
    fn should_fall_back_to_normal_search_with_empty_include_exclude() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when - no include, no exclude
        let results = graph.search_with_include_exclude(&[0.9, 0.1], 1, &[], &HashSet::new());

        // then - same as regular search
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 1);
    }

    #[test]
    fn should_rank_include_and_graph_results_by_distance() {
        // given - graph centroid at [1,0], include centroid at [0,1]
        let centroids = vec![CentroidEntry::new(1, vec![1.0, 0.0])];
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let include_entry = CentroidEntry::new(99, vec![0.0, 1.0]);

        // when - query at [0.1, 0.9] — closer to include centroid 99
        let results =
            graph.search_with_include_exclude(&[0.1, 0.9], 2, &[&include_entry], &HashSet::new());

        // then - 99 should be first (closer), 1 second
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], 99);
        assert_eq!(results[1], 1);

        // when - query at [0.9, 0.1] — closer to graph centroid 1
        let results =
            graph.search_with_include_exclude(&[0.9, 0.1], 2, &[&include_entry], &HashSet::new());

        // then - 1 should be first (closer), 99 second
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], 1);
        assert_eq!(results[1], 99);
    }
}

//! HNSW implementation using the usearch library.

use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;

use anyhow::Result;
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

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
            return Err(anyhow::anyhow!("Cannot build HNSW graph with no centroids"));
        }

        // Validate all centroids have the same dimensionality
        let dimensions = centroids[0].dimensions();
        for centroid in &centroids {
            if centroid.dimensions() != dimensions {
                return Err(anyhow::anyhow!(
                    "Centroid dimension mismatch: expected {}, got {}",
                    dimensions,
                    centroid.dimensions()
                ));
            }
        }

        // Convert distance metric to usearch MetricKind
        let metric = match distance_metric {
            DistanceMetric::L2 => MetricKind::L2sq,
            DistanceMetric::Cosine => MetricKind::Cos,
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
        let index = Index::new(&options)?;

        // Reserve 200K capacity upfront
        index.reserve(INITIAL_CAPACITY)?;

        // Build mappings and insert
        let mut key_to_centroid = HashMap::with_capacity(centroids.len());
        let mut centroid_to_key = HashMap::with_capacity(centroids.len());
        let mut centroid_vectors = HashMap::with_capacity(centroids.len());

        for (key, centroid) in centroids.iter().enumerate() {
            let key = key as u64;
            index.add(key, &centroid.vector)?;
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
            }),
        })
    }
}

impl CentroidGraph for UsearchCentroidGraph {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.inner.read().expect("lock poisoned").search(query, k)
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

    fn add_centroid(&mut self, entry: &CentroidEntry) -> Result<()> {
        let key = self.next_key;
        self.next_key += 1;

        self.index.add(key, &entry.vector)?;

        self.key_to_centroid.insert(key, entry.centroid_id);
        self.centroid_to_key.insert(entry.centroid_id, key);
        self.centroid_vectors
            .insert(entry.centroid_id, entry.vector.clone());

        Ok(())
    }

    fn remove_centroid(&mut self, centroid_id: u64) -> Result<()> {
        let key = self
            .centroid_to_key
            .remove(&centroid_id)
            .ok_or_else(|| anyhow::anyhow!("Centroid {} not found in graph", centroid_id))?;

        self.key_to_centroid.remove(&key);
        self.centroid_vectors.remove(&centroid_id);
        self.index.remove(key)?;

        Ok(())
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.centroid_vectors.get(&centroid_id).cloned()
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
    fn should_build_and_search_cosine_graph() {
        // given - 3 centroids
        let centroids = vec![
            CentroidEntry::new(10, vec![1.0, 0.0]),
            CentroidEntry::new(20, vec![0.0, 1.0]),
            CentroidEntry::new(30, vec![1.0, 1.0]),
        ];

        // when
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::Cosine).unwrap();
        let query = vec![0.9, 0.1];
        let results = graph.search(&query, 1);

        // then
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 10);
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
}

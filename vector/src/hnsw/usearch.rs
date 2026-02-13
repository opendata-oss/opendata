//! HNSW implementation using the usearch library.

use std::fmt;

use anyhow::Result;
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

use super::CentroidGraph;

/// HNSW graph implementation using the usearch library.
pub struct UsearchCentroidGraph {
    /// The usearch index
    index: Index,
    /// Map from usearch key (0, 1, 2...) to centroid_id
    key_to_centroid: Vec<u64>,
}

impl fmt::Debug for UsearchCentroidGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UsearchCentroidGraph")
            .field("num_centroids", &self.key_to_centroid.len())
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

        // Build mapping from key to centroid_id
        let key_to_centroid: Vec<u64> = centroids.iter().map(|c| c.centroid_id).collect();

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
            connectivity: 16,     // M parameter
            expansion_add: 200,   // ef_construction
            expansion_search: 30, // ef_search default
            multi: false,
        };

        // Create index
        let index = Index::new(&options)?;

        // Reserve capacity
        index.reserve(centroids.len())?;

        // Insert centroids (usearch uses u64 keys: 0, 1, 2...)
        for (key, centroid) in centroids.iter().enumerate() {
            index.add(key as u64, &centroid.vector)?;
        }

        Ok(Self {
            index,
            key_to_centroid,
        })
    }
}

impl CentroidGraph for UsearchCentroidGraph {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        let k = k.min(self.key_to_centroid.len());
        if k == 0 {
            return Vec::new();
        }

        // Search usearch index
        let results = match self.index.search(query, k) {
            Ok(matches) => matches,
            Err(_) => return Vec::new(),
        };

        // Map keys back to centroid_ids
        results
            .keys
            .iter()
            .map(|&key| self.key_to_centroid[key as usize])
            .collect()
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
}

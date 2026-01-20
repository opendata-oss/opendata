//! HNSW graph wrapper for centroid search.
//!
//! This module wraps the hnsw_rs library to provide a simple interface for
//! building and searching an HNSW graph over cluster centroids.

use anyhow::Result;
use hnsw_rs::prelude::*;

use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

/// HNSW graph wrapper for centroid search.
///
/// The graph stores centroids and enables fast approximate nearest neighbor search
/// to find relevant clusters during query execution.
pub struct CentroidGraph {
    /// The HNSW graph (type varies by distance metric)
    hnsw: HnswWrapper,
    /// Map from HNSW point index (0, 1, 2...) to centroid_id
    point_to_centroid: Vec<u32>,
}

/// Wrapper enum to hold different HNSW types based on distance metric.
enum HnswWrapper {
    L2(Hnsw<'static, f32, DistL2>),
    Cosine(Hnsw<'static, f32, DistCosine>),
    DotProduct(Hnsw<'static, f32, DistDot>),
}

impl CentroidGraph {
    /// Build a new HNSW graph from centroids.
    ///
    /// # Arguments
    /// * `centroids` - Vector of centroid entries with their IDs and vectors
    /// * `distance_metric` - Distance metric to use for similarity computation
    ///
    /// # Returns
    /// A CentroidGraph ready for searching
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

        // Build mapping from point index to centroid_id
        let point_to_centroid: Vec<u32> = centroids.iter().map(|c| c.centroid_id).collect();

        // Build HNSW graph based on distance metric
        let hnsw = match distance_metric {
            DistanceMetric::L2 => {
                let hnsw = build_hnsw_l2(&centroids)?;
                HnswWrapper::L2(hnsw)
            }
            DistanceMetric::Cosine => {
                let hnsw = build_hnsw_cosine(&centroids)?;
                HnswWrapper::Cosine(hnsw)
            }
            DistanceMetric::DotProduct => {
                let hnsw = build_hnsw_dot(&centroids)?;
                HnswWrapper::DotProduct(hnsw)
            }
        };

        Ok(Self {
            hnsw,
            point_to_centroid,
        })
    }

    /// Search for k nearest centroids to a query vector.
    ///
    /// # Arguments
    /// * `query` - Query vector
    /// * `k` - Number of nearest centroids to return
    ///
    /// # Returns
    /// Vector of centroid_ids sorted by similarity (closest first)
    pub fn search(&self, query: &[f32], k: usize) -> Vec<u32> {
        let k = k.min(self.point_to_centroid.len());
        if k == 0 {
            return Vec::new();
        }

        // Search HNSW graph (returns point indices with distances)
        let neighbors = match &self.hnsw {
            HnswWrapper::L2(hnsw) => hnsw.search(query, k, 30),
            HnswWrapper::Cosine(hnsw) => hnsw.search(query, k, 30),
            HnswWrapper::DotProduct(hnsw) => hnsw.search(query, k, 30),
        };

        // Map point indices back to centroid_ids
        neighbors
            .iter()
            .map(|neighbor| {
                let point_idx = neighbor.d_id;
                self.point_to_centroid[point_idx]
            })
            .collect()
    }

    /// Returns the number of centroids in the graph.
    pub fn len(&self) -> usize {
        self.point_to_centroid.len()
    }

    /// Returns true if the graph has no centroids.
    pub fn is_empty(&self) -> bool {
        self.point_to_centroid.is_empty()
    }
}

/// Build HNSW graph for L2 distance metric.
fn build_hnsw_l2(centroids: &[CentroidEntry]) -> Result<Hnsw<'static, f32, DistL2>> {
    let nb_points = centroids.len();
    let _dimensions = centroids[0].dimensions();

    // HNSW parameters: M=16, ef_construction=200, max_nb_connection=48
    let hnsw = Hnsw::<f32, DistL2>::new(16, nb_points, 48, 200, DistL2);

    // Insert centroids (HNSW uses sequential point IDs: 0, 1, 2...)
    for (point_idx, centroid) in centroids.iter().enumerate() {
        hnsw.insert((&centroid.vector, point_idx));
    }

    Ok(hnsw)
}

/// Build HNSW graph for Cosine similarity metric.
fn build_hnsw_cosine(centroids: &[CentroidEntry]) -> Result<Hnsw<'static, f32, DistCosine>> {
    let nb_points = centroids.len();
    let _dimensions = centroids[0].dimensions();

    let hnsw = Hnsw::<f32, DistCosine>::new(16, nb_points, 48, 200, DistCosine);

    for (point_idx, centroid) in centroids.iter().enumerate() {
        hnsw.insert((&centroid.vector, point_idx));
    }

    Ok(hnsw)
}

/// Build HNSW graph for Dot Product metric.
fn build_hnsw_dot(centroids: &[CentroidEntry]) -> Result<Hnsw<'static, f32, DistDot>> {
    let nb_points = centroids.len();
    let _dimensions = centroids[0].dimensions();

    let hnsw = Hnsw::<f32, DistDot>::new(16, nb_points, 48, 200, DistDot);

    for (point_idx, centroid) in centroids.iter().enumerate() {
        hnsw.insert((&centroid.vector, point_idx));
    }

    Ok(hnsw)
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

        // when - build graph
        let graph = CentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // then - should find nearest centroid
        let query = vec![0.9, 0.1, 0.1]; // Close to centroid 1
        let results = graph.search(&query, 1);
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

        // when - build graph
        let graph = CentroidGraph::build(centroids, DistanceMetric::Cosine).unwrap();

        // then - should find nearest centroid
        let query = vec![0.9, 0.1]; // Close to centroid 10
        let results = graph.search(&query, 1);
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

        // when - build graph and search for k=3
        let graph = CentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let query = vec![2.1]; // Close to centroid 3
        let results = graph.search(&query, 3);

        // then - should return 3 nearest centroids
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], 3); // Closest
    }

    #[test]
    fn should_reject_empty_centroids() {
        // given - empty centroids
        let centroids = vec![];

        // when - attempt to build graph
        let result = CentroidGraph::build(centroids, DistanceMetric::L2);

        // then - should fail
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(
                e.to_string()
                    .contains("Cannot build HNSW graph with no centroids")
            );
        }
    }

    #[test]
    fn should_reject_mismatched_dimensions() {
        // given - centroids with different dimensions
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 2.0]),
            CentroidEntry::new(2, vec![3.0, 4.0, 5.0]), // Wrong dimensions
        ];

        // when - attempt to build graph
        let result = CentroidGraph::build(centroids, DistanceMetric::L2);

        // then - should fail
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Centroid dimension mismatch"));
        }
    }

    #[test]
    fn should_handle_k_larger_than_centroid_count() {
        // given - 2 centroids
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0]),
            CentroidEntry::new(2, vec![2.0]),
        ];

        // when - build graph and search for k=10
        let graph = CentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let results = graph.search(&[1.5], 10);

        // then - should return all 2 centroids
        assert_eq!(results.len(), 2);
    }
}

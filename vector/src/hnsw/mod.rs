//! HNSW graph implementations for centroid search.
//!
//! This module provides a trait-based abstraction for HNSW indexes with
//! implementations backed by usearch and hnsw_rs libraries.

mod hnsw;
mod usearch;

use anyhow::Result;

use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

// Re-export implementations
pub use hnsw::HnswRsCentroidGraph;
pub use usearch::UsearchCentroidGraph;

/// Trait for HNSW-based centroid graph implementations.
///
/// The graph stores centroids and enables fast approximate nearest neighbor search
/// to find relevant clusters during query execution.
pub trait CentroidGraph: Send + Sync {
    /// Search for k nearest centroids to a query vector.
    ///
    /// # Arguments
    /// * `query` - Query vector
    /// * `k` - Number of nearest centroids to return
    ///
    /// # Returns
    /// Vector of centroid_ids sorted by similarity (closest first)
    fn search(&self, query: &[f32], k: usize) -> Vec<u32>;

    /// Returns the number of centroids in the graph.
    fn len(&self) -> usize;

    /// Returns true if the graph has no centroids.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Build a centroid graph using the default implementation (usearch).
pub fn build_centroid_graph(
    centroids: Vec<CentroidEntry>,
    distance_metric: DistanceMetric,
) -> Result<Box<dyn CentroidGraph>> {
    let graph = UsearchCentroidGraph::build(centroids, distance_metric)?;
    Ok(Box::new(graph))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to run tests against both implementations.
    fn test_with_both_impls<F>(test_fn: F)
    where
        F: Fn(Box<dyn CentroidGraph>),
    {
        // Test with usearch
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let usearch_graph =
            UsearchCentroidGraph::build(centroids.clone(), DistanceMetric::L2).unwrap();
        test_fn(Box::new(usearch_graph));

        // Test with hnsw_rs
        let hnsw_graph = HnswRsCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        test_fn(Box::new(hnsw_graph));
    }

    #[test]
    fn should_work_through_trait_interface() {
        test_with_both_impls(|graph| {
            assert_eq!(graph.len(), 3);
            assert!(!graph.is_empty());

            let results = graph.search(&[0.9, 0.1, 0.1], 1);
            assert_eq!(results.len(), 1);
            assert_eq!(results[0], 1);
        });
    }

    #[test]
    fn should_build_with_default_function() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
        ];

        // when
        let graph = build_centroid_graph(centroids, DistanceMetric::L2).unwrap();

        // then
        assert_eq!(graph.len(), 2);

        let results = graph.search(&[0.9, 0.1], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 1);
    }
}

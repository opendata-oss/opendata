//! Centroid graph implementations for centroid search.
//!
//! This module provides a trait-based abstraction for centroid indexes with
//! implementations backed by the usearch library and exhaustive raw-vector search.

mod exhaustive;
mod usearch;

use crate::error::Result;
use std::collections::HashSet;

use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

// Re-export implementations
pub use exhaustive::ExhaustiveCentroidGraph;
pub use usearch::UsearchCentroidGraph;

/// Trait for centroid graph implementations.
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
    fn search(&self, query: &[f32], k: usize) -> Vec<u64>;

    /// Search for k nearest centroids to a query vector. Exclude centroids with ids
    /// from an exclude set. After search, factor in centroids from an include set.
    /// The centroids from the include set are not part of the current graph, so must
    /// be factored in after the graph search.
    ///
    /// # Arguments
    /// * `query` - Query vector
    /// * `k` - Number of nearest centroids to return
    /// * `include` - New centroids to be considered after searching the graph
    /// * `exclude` - Centroids to be excluded from the graph search
    ///
    /// # Returns
    /// Vector of centroid_ids sorted by similarity (closest first)
    fn search_with_include_exclude(
        &self,
        _query: &[f32],
        _k: usize,
        _include: &[&CentroidEntry],
        _exclude: &HashSet<u64>,
    ) -> Vec<u64> {
        unimplemented!();
    }

    /// Add a centroid to the graph.
    ///
    /// Uses interior mutability since the graph is behind `Arc<dyn CentroidGraph>`.
    fn add_centroid(&self, entry: &CentroidEntry) -> Result<()>;

    /// Remove a centroid from the graph by its ID.
    ///
    /// Uses interior mutability since the graph is behind `Arc<dyn CentroidGraph>`.
    fn remove_centroid(&self, centroid_id: u64) -> Result<()>;

    /// Get the vector for a centroid by its ID.
    ///
    /// Returns `None` if the centroid is not in the graph.
    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>>;

    /// Return all live centroid IDs currently present in the graph.
    fn all_centroid_ids(&self) -> Vec<u64>;

    /// Returns the number of centroids in the graph.
    fn len(&self) -> usize;

    /// Returns true if the graph has no centroids.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Build a centroid graph using the default implementation (exhaustive).
pub fn build_centroid_graph(
    centroids: Vec<CentroidEntry>,
    distance_metric: DistanceMetric,
) -> Result<Box<dyn CentroidGraph>> {
    let graph = ExhaustiveCentroidGraph::build(centroids, distance_metric)?;
    Ok(Box::new(graph))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_work_through_trait_interface() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph: Box<dyn CentroidGraph> =
            Box::new(UsearchCentroidGraph::build(centroids, DistanceMetric::L2).unwrap());

        // when / then
        assert_eq!(graph.len(), 3);
        assert!(!graph.is_empty());

        let results = graph.search(&[0.9, 0.1, 0.1], 1);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], 1);
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

//! HNSW graph implementations for centroid search.
//!
//! This module provides a trait-based abstraction for HNSW indexes with
//! an implementation backed by the usearch library.

mod usearch;

use std::sync::Arc;
use crate::error::Result;

use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

// Re-export implementations
pub use usearch::UsearchCentroidGraph;

/// Reader trait for HNSW-based centroid graph implementations.
///
pub trait CentroidGraphRead: Send + Sync {
    /// Search for k nearest centroids to a query vector.
    ///
    /// # Arguments
    /// * `query` - Query vector
    /// * `k` - Number of nearest centroids to return
    ///
    /// # Returns
    /// Vector of centroid_ids sorted by similarity (closest first)
    fn search(&self, query: &[f32], k: usize) -> Vec<u64>;

    /// Get the vector for a centroid by its ID.
    ///
    /// Returns `None` if the centroid is not in the graph.
    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>>;

    /// Returns the number of centroids in the graph.
    fn len(&self) -> usize;

    /// Returns true if the graph has no centroids.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Trait for HNSW-based centroid graph implementations.
///
/// The graph stores centroids and enables fast approximate nearest neighbor search
/// to find relevant clusters during query execution.
pub trait CentroidGraph: CentroidGraphRead + Send + Sync {
    /// Add a centroid to the graph, recording the mutation at the given epoch.
    ///
    /// Uses interior mutability since the graph is behind `Arc<dyn CentroidGraph>`.
    fn add_centroid(&self, entry: &CentroidEntry, epoch: u64) -> Result<()>;

    /// Remove a centroid from the graph by its ID at the given epoch.
    ///
    /// This is a soft delete: the centroid remains in the underlying index so that
    /// snapshot reads at earlier epochs can still find it. The centroid is recorded
    /// as deleted at the given epoch and excluded from searches at that epoch or later.
    fn remove_centroid(&self, centroid_id: u64, epoch: u64) -> Result<()>;

    /// Take a snapshot of the centroid graph
    fn snapshot(&self) -> Result<Arc<dyn CentroidGraphRead>>;
}

/// Build a centroid graph using the default implementation (usearch).
///
/// All initial centroids are recorded as added at the given `epoch`.
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

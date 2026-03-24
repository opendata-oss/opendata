//! Exhaustive centroid graph implementation backed by raw centroid vectors.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::RwLock;

use crate::distance::{VectorDistance, compute_distance};
use crate::error::{Error, Result};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

use super::CentroidGraph;

struct ExhaustiveCentroidGraphInner {
    distance_metric: DistanceMetric,
    dimensions: usize,
    centroid_ids: Vec<u64>,
    centroid_vectors: Vec<f32>,
    centroid_to_idx: HashMap<u64, usize>,
}

/// Centroid graph implementation that stores raw centroid vectors and searches
/// them exhaustively.
pub struct ExhaustiveCentroidGraph {
    inner: RwLock<ExhaustiveCentroidGraphInner>,
}

impl fmt::Debug for ExhaustiveCentroidGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.read().unwrap();
        f.debug_struct("ExhaustiveCentroidGraph")
            .field("num_centroids", &inner.centroid_ids.len())
            .field("dimensions", &inner.dimensions)
            .field("distance_metric", &inner.distance_metric)
            .finish()
    }
}

impl ExhaustiveCentroidGraph {
    /// Build a new exhaustive centroid graph from raw centroid vectors.
    pub fn build(centroids: Vec<CentroidEntry>, distance_metric: DistanceMetric) -> Result<Self> {
        let dimensions = validate_centroid_dimensions(&centroids)?;
        let centroid_to_idx = build_centroid_index(&centroids)?;
        let (centroid_ids, centroid_vectors) = flatten_centroids(&centroids, dimensions);

        Ok(Self {
            inner: RwLock::new(ExhaustiveCentroidGraphInner {
                distance_metric,
                dimensions,
                centroid_ids,
                centroid_vectors,
                centroid_to_idx,
            }),
        })
    }
}

impl CentroidGraph for ExhaustiveCentroidGraph {
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

impl ExhaustiveCentroidGraphInner {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.search_with_include_exclude(query, k, &[], &HashSet::new())
    }

    fn search_with_include_exclude(
        &self,
        query: &[f32],
        k: usize,
        include: &[&CentroidEntry],
        exclude: &HashSet<u64>,
    ) -> Vec<u64> {
        if k == 0 || query.len() != self.dimensions {
            return Vec::new();
        }

        if k == 1 {
            return self
                .best_matching_centroid(query, include, exclude)
                .into_iter()
                .collect();
        }

        let mut scored = self.score_graph_centroids(query, exclude);
        scored.extend(include.iter().map(|entry| {
            (
                entry.centroid_id,
                compute_distance(query, &entry.vector, self.distance_metric),
            )
        }));
        scored.sort_unstable_by(|left, right| {
            left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0))
        });

        scored
            .into_iter()
            .take(k)
            .map(|(centroid_id, _)| centroid_id)
            .collect()
    }

    fn add_centroid(&mut self, entry: &CentroidEntry) -> Result<()> {
        if entry.dimensions() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Centroid dimension mismatch: expected {}, got {}",
                self.dimensions,
                entry.dimensions()
            )));
        }

        if self.centroid_to_idx.contains_key(&entry.centroid_id) {
            return Err(Error::InvalidInput(format!(
                "Centroid {} already exists in graph",
                entry.centroid_id
            )));
        }

        let idx = self.centroid_ids.len();
        self.centroid_ids.push(entry.centroid_id);
        self.centroid_vectors.extend_from_slice(&entry.vector);
        self.centroid_to_idx.insert(entry.centroid_id, idx);
        Ok(())
    }

    fn remove_centroid(&mut self, centroid_id: u64) -> Result<()> {
        let idx = self.centroid_to_idx.remove(&centroid_id).ok_or_else(|| {
            Error::Internal(format!("Centroid {} not found in graph", centroid_id))
        })?;
        let last_idx = self.centroid_ids.len() - 1;
        let removed = self.centroid_ids.swap_remove(idx);
        debug_assert_eq!(removed, centroid_id);

        if idx != last_idx {
            let moved_centroid_id = self.centroid_ids[idx];
            self.move_vector_chunk(last_idx, idx);
            self.centroid_to_idx.insert(moved_centroid_id, idx);
        }
        self.truncate_last_vector_chunk();

        Ok(())
    }

    fn get_centroid_vector(&self, centroid_id: u64) -> Option<Vec<f32>> {
        self.centroid_to_idx
            .get(&centroid_id)
            .map(|&idx| self.centroid_vector(idx).to_vec())
    }

    fn all_centroid_ids(&self) -> Vec<u64> {
        self.centroid_ids.clone()
    }

    fn len(&self) -> usize {
        self.centroid_ids.len()
    }

    fn score_graph_centroids(
        &self,
        query: &[f32],
        exclude: &HashSet<u64>,
    ) -> Vec<(u64, VectorDistance)> {
        let mut scored = Vec::with_capacity(self.centroid_ids.len().saturating_sub(exclude.len()));

        for (idx, &centroid_id) in self.centroid_ids.iter().enumerate() {
            if exclude.contains(&centroid_id) {
                continue;
            }

            scored.push((
                centroid_id,
                compute_distance(query, self.centroid_vector(idx), self.distance_metric),
            ));
        }

        scored
    }

    fn best_matching_centroid(
        &self,
        query: &[f32],
        include: &[&CentroidEntry],
        exclude: &HashSet<u64>,
    ) -> Option<u64> {
        let mut best: Option<(u64, VectorDistance)> = None;

        for (idx, &centroid_id) in self.centroid_ids.iter().enumerate() {
            if exclude.contains(&centroid_id) {
                continue;
            }

            let distance = compute_distance(query, self.centroid_vector(idx), self.distance_metric);
            Self::update_best_candidate(&mut best, centroid_id, distance);
        }

        for entry in include {
            let distance = compute_distance(query, &entry.vector, self.distance_metric);
            Self::update_best_candidate(&mut best, entry.centroid_id, distance);
        }

        best.map(|(centroid_id, _)| centroid_id)
    }

    fn update_best_candidate(
        best: &mut Option<(u64, VectorDistance)>,
        centroid_id: u64,
        distance: VectorDistance,
    ) {
        match best {
            Some((best_centroid_id, best_distance)) => {
                if matches!(
                    (distance, centroid_id).cmp(&(*best_distance, *best_centroid_id)),
                    Ordering::Less
                ) {
                    *best = Some((centroid_id, distance));
                }
            }
            None => *best = Some((centroid_id, distance)),
        }
    }

    fn centroid_vector(&self, idx: usize) -> &[f32] {
        let start = idx * self.dimensions;
        let end = start + self.dimensions;
        &self.centroid_vectors[start..end]
    }

    fn move_vector_chunk(&mut self, src_idx: usize, dst_idx: usize) {
        if self.dimensions == 0 || src_idx == dst_idx {
            return;
        }

        let src_start = src_idx * self.dimensions;
        let src_end = src_start + self.dimensions;
        let dst_start = dst_idx * self.dimensions;
        self.centroid_vectors
            .copy_within(src_start..src_end, dst_start);
    }

    fn truncate_last_vector_chunk(&mut self) {
        if self.dimensions == 0 {
            return;
        }

        let new_len = self.centroid_vectors.len() - self.dimensions;
        self.centroid_vectors.truncate(new_len);
    }
}

fn validate_centroid_dimensions(centroids: &[CentroidEntry]) -> Result<usize> {
    if centroids.is_empty() {
        return Err(Error::InvalidInput(
            "Cannot build centroid graph with no centroids".to_string(),
        ));
    }

    let dimensions = centroids[0].dimensions();
    for centroid in centroids {
        if centroid.dimensions() != dimensions {
            return Err(Error::InvalidInput(format!(
                "Centroid dimension mismatch: expected {}, got {}",
                dimensions,
                centroid.dimensions()
            )));
        }
    }

    Ok(dimensions)
}

fn build_centroid_index(centroids: &[CentroidEntry]) -> Result<HashMap<u64, usize>> {
    let mut centroid_to_idx = HashMap::with_capacity(centroids.len());

    for (idx, centroid) in centroids.iter().enumerate() {
        if centroid_to_idx.insert(centroid.centroid_id, idx).is_some() {
            return Err(Error::InvalidInput(format!(
                "Duplicate centroid id: {}",
                centroid.centroid_id
            )));
        }
    }

    Ok(centroid_to_idx)
}

fn flatten_centroids(centroids: &[CentroidEntry], dimensions: usize) -> (Vec<u64>, Vec<f32>) {
    let mut centroid_ids = Vec::with_capacity(centroids.len());
    let mut centroid_vectors = Vec::with_capacity(centroids.len() * dimensions);

    for centroid in centroids {
        centroid_ids.push(centroid.centroid_id);
        centroid_vectors.extend_from_slice(&centroid.vector);
    }

    (centroid_ids, centroid_vectors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_and_search_l2_graph() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];

        // when
        let graph = ExhaustiveCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let results = graph.search(&[0.9, 0.1, 0.1], 1);

        // then
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn should_rank_dot_product_neighbors() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![3.0, 0.0]),
            CentroidEntry::new(2, vec![2.0, 1.0]),
            CentroidEntry::new(3, vec![0.0, 3.0]),
        ];

        // when
        let graph = ExhaustiveCentroidGraph::build(centroids, DistanceMetric::DotProduct).unwrap();
        let results = graph.search(&[1.0, 0.0], 2);

        // then
        assert_eq!(results, vec![1, 2]);
    }

    #[test]
    fn should_reject_empty_centroids() {
        // given
        let centroids = vec![];

        // when
        let result = ExhaustiveCentroidGraph::build(centroids, DistanceMetric::L2);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot build centroid graph with no centroids")
        );
    }

    #[test]
    fn should_reject_mismatched_dimensions() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 2.0]),
            CentroidEntry::new(2, vec![3.0, 4.0, 5.0]),
        ];

        // when
        let result = ExhaustiveCentroidGraph::build(centroids, DistanceMetric::L2);

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
    fn should_reject_duplicate_centroid_ids() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(1, vec![0.0, 1.0]),
        ];

        // when
        let result = ExhaustiveCentroidGraph::build(centroids, DistanceMetric::L2);

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Duplicate centroid id")
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
        let graph = ExhaustiveCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let results = graph.search(&[1.5], 10);

        // then
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn should_add_centroid_and_find_it() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
        ];
        let graph = ExhaustiveCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when
        graph
            .add_centroid(&CentroidEntry::new(3, vec![0.0, 0.0, 1.0]))
            .unwrap();
        let results = graph.search(&[0.0, 0.0, 0.9], 1);

        // then
        assert_eq!(graph.len(), 3);
        assert_eq!(results, vec![3]);
    }

    #[test]
    fn should_remove_centroid() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
            CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
        ];
        let graph = ExhaustiveCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when
        graph.remove_centroid(2).unwrap();
        let results = graph.search(&[0.0, 0.9, 0.0], 2);

        // then
        assert_eq!(graph.len(), 2);
        assert_eq!(results.len(), 2);
        assert!(!results.contains(&2));
    }

    #[test]
    fn should_get_centroid_vector() {
        // given
        let graph = ExhaustiveCentroidGraph::build(
            vec![
                CentroidEntry::new(1, vec![1.0, 0.0]),
                CentroidEntry::new(2, vec![0.0, 1.0]),
            ],
            DistanceMetric::L2,
        )
        .unwrap();

        // when / then
        assert_eq!(graph.get_centroid_vector(1), Some(vec![1.0, 0.0]));
        assert_eq!(graph.get_centroid_vector(2), Some(vec![0.0, 1.0]));
        assert_eq!(graph.get_centroid_vector(99), None);
    }

    #[test]
    fn should_return_empty_results_for_mismatched_query_dimensions() {
        // given
        let graph = ExhaustiveCentroidGraph::build(
            vec![
                CentroidEntry::new(1, vec![1.0, 0.0]),
                CentroidEntry::new(2, vec![0.0, 1.0]),
            ],
            DistanceMetric::L2,
        )
        .unwrap();

        // when
        let results = graph.search(&[1.0, 0.0, 0.0], 1);

        // then
        assert!(results.is_empty());
    }

    #[test]
    fn should_break_k_one_ties_by_centroid_id() {
        // given
        let graph = ExhaustiveCentroidGraph::build(
            vec![
                CentroidEntry::new(7, vec![1.0, 0.0]),
                CentroidEntry::new(3, vec![-1.0, 0.0]),
            ],
            DistanceMetric::L2,
        )
        .unwrap();

        // when
        let results = graph.search(&[0.0, 0.0], 1);

        // then
        assert_eq!(results, vec![3]);
    }

    #[test]
    fn should_exclude_centroids_from_search() {
        // given
        let graph = ExhaustiveCentroidGraph::build(
            vec![
                CentroidEntry::new(1, vec![1.0, 0.0, 0.0]),
                CentroidEntry::new(2, vec![0.0, 1.0, 0.0]),
                CentroidEntry::new(3, vec![0.0, 0.0, 1.0]),
            ],
            DistanceMetric::L2,
        )
        .unwrap();
        let exclude = HashSet::from([1]);

        // when
        let results = graph.search_with_include_exclude(&[0.9, 0.1, 0.0], 1, &[], &exclude);

        // then
        assert_eq!(results.len(), 1);
        assert_ne!(results[0], 1);
    }

    #[test]
    fn should_include_centroids_not_in_graph() {
        // given
        let graph = ExhaustiveCentroidGraph::build(
            vec![
                CentroidEntry::new(1, vec![1.0, 0.0]),
                CentroidEntry::new(2, vec![0.0, 1.0]),
            ],
            DistanceMetric::L2,
        )
        .unwrap();
        let include = CentroidEntry::new(99, vec![0.5, 0.5]);

        // when
        let results =
            graph.search_with_include_exclude(&[0.5, 0.5], 1, &[&include], &HashSet::new());

        // then
        assert_eq!(results, vec![99]);
    }

    #[test]
    fn should_return_all_live_centroid_ids() {
        // given
        let graph = ExhaustiveCentroidGraph::build(
            vec![
                CentroidEntry::new(1, vec![1.0, 0.0]),
                CentroidEntry::new(2, vec![0.0, 1.0]),
            ],
            DistanceMetric::L2,
        )
        .unwrap();

        // when
        let mut ids = graph.all_centroid_ids();
        ids.sort_unstable();

        // then
        assert_eq!(ids, vec![1, 2]);
    }
}

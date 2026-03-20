//! SPTAG-inspired centroid graph implementation.
//!
//! SPFresh maintains its head index as an explicit neighborhood graph and,
//! during updates, refines the inserted node plus affected neighbors instead of
//! relying on opaque dynamic HNSW mutations or full rebuilds. This module adapts
//! that idea to the `CentroidGraph` abstraction used by `vector`.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::sync::RwLock;

use crate::distance;
use crate::error::{Error, Result};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

use super::CentroidGraph;

const DEFAULT_NEIGHBORHOOD_SIZE: usize = 16;
const DEFAULT_ENTRY_POINT_COUNT: usize = 8;
const DEFAULT_SEARCH_BEAM: usize = 64;
const DEFAULT_EXACT_SEARCH_THRESHOLD: usize = 256;

#[derive(Debug, Clone)]
struct GraphNode {
    vector: Vec<f32>,
    neighbors: Vec<u64>,
}

#[derive(Debug, Clone, Copy)]
struct GraphParams {
    neighborhood_size: usize,
    entry_point_count: usize,
    search_beam: usize,
    exact_search_threshold: usize,
}

impl Default for GraphParams {
    fn default() -> Self {
        Self {
            neighborhood_size: DEFAULT_NEIGHBORHOOD_SIZE,
            entry_point_count: DEFAULT_ENTRY_POINT_COUNT,
            search_beam: DEFAULT_SEARCH_BEAM,
            exact_search_threshold: DEFAULT_EXACT_SEARCH_THRESHOLD,
        }
    }
}

#[derive(Debug)]
struct SptagCentroidGraphInner {
    dimensions: usize,
    distance_metric: DistanceMetric,
    params: GraphParams,
    nodes: HashMap<u64, GraphNode>,
    entry_points: Vec<u64>,
}

/// Centroid graph backed by an explicit k-nearest-neighbor graph.
///
/// Build time constructs an exact k-NN graph for centroids, while incremental
/// add/remove operations repair only the changed local neighborhood. Search uses
/// a beam traversal seeded by diverse entry points, similar in spirit to the
/// SPTAG head index in SPFresh.
pub struct SptagCentroidGraph {
    inner: RwLock<SptagCentroidGraphInner>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ScoredCandidate {
    id: u64,
    distance: f32,
}

impl Eq for ScoredCandidate {}

impl PartialOrd for ScoredCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl fmt::Debug for SptagCentroidGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.read().expect("lock poisoned");
        f.debug_struct("SptagCentroidGraph")
            .field("num_centroids", &inner.nodes.len())
            .field("entry_points", &inner.entry_points)
            .finish()
    }
}

impl SptagCentroidGraph {
    pub fn build(centroids: Vec<CentroidEntry>, distance_metric: DistanceMetric) -> Result<Self> {
        if centroids.is_empty() {
            return Err(Error::InvalidInput(
                "Cannot build centroid graph with no centroids".to_string(),
            ));
        }

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

        let mut nodes = HashMap::with_capacity(centroids.len());
        for centroid in centroids {
            nodes.insert(
                centroid.centroid_id,
                GraphNode {
                    vector: centroid.vector,
                    neighbors: Vec::new(),
                },
            );
        }

        let mut inner = SptagCentroidGraphInner {
            dimensions,
            distance_metric,
            params: GraphParams::default(),
            nodes,
            entry_points: Vec::new(),
        };
        inner.rebuild_all_neighbors();
        inner.refresh_entry_points();

        Ok(Self {
            inner: RwLock::new(inner),
        })
    }
}

impl CentroidGraph for SptagCentroidGraph {
    fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.inner
            .read()
            .expect("lock poisoned")
            .search_scored(query, k, &HashSet::new())
            .into_iter()
            .map(|candidate| candidate.id)
            .collect()
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
            .nodes
            .get(&centroid_id)
            .map(|node| node.vector.clone())
    }

    fn all_centroid_ids(&self) -> Vec<u64> {
        self.inner.read().expect("lock poisoned").sorted_ids()
    }

    fn len(&self) -> usize {
        self.inner.read().expect("lock poisoned").nodes.len()
    }
}

impl SptagCentroidGraphInner {
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

        let search_k = k
            .saturating_add(include.len())
            .max(self.params.neighborhood_size)
            .min(self.live_count(exclude));

        let mut merged = HashMap::with_capacity(search_k.saturating_add(include.len()));
        for candidate in self.search_scored(query, search_k, exclude) {
            merged.insert(candidate.id, candidate.distance);
        }

        for entry in include {
            if entry.dimensions() != self.dimensions {
                continue;
            }
            let distance = self.distance(query, &entry.vector);
            merged
                .entry(entry.centroid_id)
                .and_modify(|current| {
                    if distance < *current {
                        *current = distance;
                    }
                })
                .or_insert(distance);
        }

        let mut merged: Vec<_> = merged.into_iter().collect();
        merged.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        merged.into_iter().take(k).map(|(id, _)| id).collect()
    }

    fn add_centroid(&mut self, entry: &CentroidEntry) -> Result<()> {
        if entry.dimensions() != self.dimensions {
            return Err(Error::InvalidInput(format!(
                "Centroid dimension mismatch: expected {}, got {}",
                self.dimensions,
                entry.dimensions()
            )));
        }
        if self.nodes.contains_key(&entry.centroid_id) {
            return Err(Error::Internal(format!(
                "Centroid {} already exists in graph",
                entry.centroid_id
            )));
        }

        self.nodes.insert(
            entry.centroid_id,
            GraphNode {
                vector: entry.vector.clone(),
                neighbors: Vec::new(),
            },
        );

        let mut affected: HashSet<u64> = HashSet::new();
        affected.insert(entry.centroid_id);

        let nearest = self.exact_top_k_ids_for(entry.centroid_id, self.params.neighborhood_size);
        if let Some(node) = self.nodes.get_mut(&entry.centroid_id) {
            node.neighbors = nearest.clone();
        }

        for neighbor_id in nearest {
            affected.insert(neighbor_id);
            if let Some(node) = self.nodes.get(&neighbor_id) {
                affected.extend(node.neighbors.iter().copied());
            }
        }

        self.rebuild_local_neighbors(&affected);
        self.refresh_entry_points();
        Ok(())
    }

    fn remove_centroid(&mut self, centroid_id: u64) -> Result<()> {
        let removed = self.nodes.remove(&centroid_id).ok_or_else(|| {
            Error::Internal(format!("Centroid {} not found in graph", centroid_id))
        })?;

        let mut affected: HashSet<u64> = removed.neighbors.into_iter().collect();
        for node in self.nodes.values_mut() {
            node.neighbors
                .retain(|neighbor_id| *neighbor_id != centroid_id);
        }

        let snapshot: Vec<u64> = affected.iter().copied().collect();
        for neighbor_id in snapshot {
            if let Some(node) = self.nodes.get(&neighbor_id) {
                affected.extend(node.neighbors.iter().copied());
            }
        }

        self.rebuild_local_neighbors(&affected);
        self.refresh_entry_points();
        Ok(())
    }

    fn rebuild_all_neighbors(&mut self) {
        let ids = self.sorted_ids();
        for centroid_id in &ids {
            let neighbors = self.exact_top_k_ids_for(*centroid_id, self.params.neighborhood_size);
            if let Some(node) = self.nodes.get_mut(centroid_id) {
                node.neighbors = neighbors;
            }
        }
        self.make_reverse_links(&ids);
    }

    fn rebuild_local_neighbors(&mut self, affected: &HashSet<u64>) {
        if self.nodes.is_empty() || affected.is_empty() {
            return;
        }

        let mut ids: Vec<u64> = affected
            .iter()
            .copied()
            .filter(|centroid_id| self.nodes.contains_key(centroid_id))
            .collect();
        ids.sort_unstable();
        ids.dedup();

        for centroid_id in &ids {
            let neighbors = self.exact_top_k_ids_for(*centroid_id, self.params.neighborhood_size);
            if let Some(node) = self.nodes.get_mut(centroid_id) {
                node.neighbors = neighbors;
            }
        }
        self.make_reverse_links(&ids);
    }

    fn make_reverse_links(&mut self, source_ids: &[u64]) {
        let edges: Vec<(u64, u64)> = source_ids
            .iter()
            .flat_map(|source_id| {
                self.nodes.get(source_id).into_iter().flat_map(move |node| {
                    node.neighbors
                        .iter()
                        .map(move |&neighbor| (neighbor, *source_id))
                })
            })
            .collect();

        for (target_id, candidate_id) in edges {
            self.insert_neighbor(target_id, candidate_id);
        }
    }

    fn insert_neighbor(&mut self, centroid_id: u64, candidate_id: u64) {
        if centroid_id == candidate_id
            || !self.nodes.contains_key(&centroid_id)
            || !self.nodes.contains_key(&candidate_id)
        {
            return;
        }

        let mut candidate_ids = self
            .nodes
            .get(&centroid_id)
            .map(|node| node.neighbors.clone())
            .unwrap_or_default();
        candidate_ids.push(candidate_id);

        let neighbors = self.select_best_neighbors(centroid_id, candidate_ids);
        if let Some(node) = self.nodes.get_mut(&centroid_id) {
            node.neighbors = neighbors;
        }
    }

    fn select_best_neighbors(&self, centroid_id: u64, candidate_ids: Vec<u64>) -> Vec<u64> {
        let Some(node) = self.nodes.get(&centroid_id) else {
            return Vec::new();
        };

        let mut seen = HashSet::with_capacity(candidate_ids.len());
        let mut scored = Vec::with_capacity(candidate_ids.len());
        for candidate_id in candidate_ids {
            if candidate_id == centroid_id || !seen.insert(candidate_id) {
                continue;
            }
            let Some(candidate) = self.nodes.get(&candidate_id) else {
                continue;
            };
            scored.push((candidate_id, self.distance(&node.vector, &candidate.vector)));
        }

        scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        scored
            .into_iter()
            .take(self.params.neighborhood_size)
            .map(|(candidate_id, _)| candidate_id)
            .collect()
    }

    fn exact_top_k_ids_for(&self, centroid_id: u64, k: usize) -> Vec<u64> {
        let Some(node) = self.nodes.get(&centroid_id) else {
            return Vec::new();
        };

        let mut scored = Vec::with_capacity(self.nodes.len().saturating_sub(1));
        for (&candidate_id, candidate) in &self.nodes {
            if candidate_id == centroid_id {
                continue;
            }
            scored.push((candidate_id, self.distance(&node.vector, &candidate.vector)));
        }

        scored.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        scored
            .into_iter()
            .take(k)
            .map(|(candidate_id, _)| candidate_id)
            .collect()
    }

    fn search_scored(
        &self,
        query: &[f32],
        k: usize,
        exclude: &HashSet<u64>,
    ) -> Vec<ScoredCandidate> {
        if k == 0 || query.len() != self.dimensions {
            return Vec::new();
        }

        let live_count = self.live_count(exclude);
        if live_count == 0 {
            return Vec::new();
        }

        if live_count <= self.params.exact_search_threshold {
            return self.exact_search(query, k, exclude);
        }

        let beam = self
            .params
            .search_beam
            .max(k.saturating_mul(4))
            .min(live_count);
        let mut seed_scores = self.seed_scores(query, exclude);
        if seed_scores.is_empty() {
            return self.exact_search(query, k, exclude);
        }
        seed_scores.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.id.cmp(&b.id))
        });

        let mut visited = HashSet::with_capacity(beam.saturating_mul(2));
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        for candidate in seed_scores
            .into_iter()
            .take(self.params.entry_point_count.max(1))
        {
            if !visited.insert(candidate.id) {
                continue;
            }
            candidates.push(Reverse(candidate));
            results.push(candidate);
        }

        while let Some(Reverse(candidate)) = candidates.pop() {
            if results.len() >= beam {
                let worst = results
                    .peek()
                    .expect("beam is non-empty when len >= beam")
                    .distance;
                if candidate.distance > worst {
                    break;
                }
            }

            let neighbors = self
                .nodes
                .get(&candidate.id)
                .map(|node| node.neighbors.clone())
                .unwrap_or_default();

            for neighbor_id in neighbors {
                if exclude.contains(&neighbor_id) || !visited.insert(neighbor_id) {
                    continue;
                }

                let Some(neighbor) = self.nodes.get(&neighbor_id) else {
                    continue;
                };

                let scored = ScoredCandidate {
                    id: neighbor_id,
                    distance: self.distance(query, &neighbor.vector),
                };

                let should_add = results.len() < beam
                    || scored.distance
                        < results
                            .peek()
                            .expect("beam is non-empty when comparing against worst")
                            .distance;
                if should_add {
                    candidates.push(Reverse(scored));
                    results.push(scored);
                    if results.len() > beam {
                        results.pop();
                    }
                }
            }
        }

        let mut output = results.into_sorted_vec();
        if output.is_empty() {
            return self.exact_search(query, k, exclude);
        }
        output.truncate(k.min(output.len()));
        output
    }

    fn exact_search(
        &self,
        query: &[f32],
        k: usize,
        exclude: &HashSet<u64>,
    ) -> Vec<ScoredCandidate> {
        let mut scored = Vec::with_capacity(self.nodes.len());
        for (&centroid_id, node) in &self.nodes {
            if exclude.contains(&centroid_id) {
                continue;
            }
            scored.push(ScoredCandidate {
                id: centroid_id,
                distance: self.distance(query, &node.vector),
            });
        }
        scored.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.id.cmp(&b.id))
        });
        scored.truncate(k.min(scored.len()));
        scored
    }

    fn seed_scores(&self, query: &[f32], exclude: &HashSet<u64>) -> Vec<ScoredCandidate> {
        let mut seeds: Vec<u64> = self
            .entry_points
            .iter()
            .copied()
            .filter(|centroid_id| {
                !exclude.contains(centroid_id) && self.nodes.contains_key(centroid_id)
            })
            .collect();

        if seeds.is_empty() {
            seeds = self
                .sorted_ids()
                .into_iter()
                .filter(|centroid_id| !exclude.contains(centroid_id))
                .take(self.params.entry_point_count)
                .collect();
        }

        seeds
            .into_iter()
            .filter_map(|centroid_id| {
                self.nodes.get(&centroid_id).map(|node| ScoredCandidate {
                    id: centroid_id,
                    distance: self.distance(query, &node.vector),
                })
            })
            .collect()
    }

    fn refresh_entry_points(&mut self) {
        let ids = self.sorted_ids();
        if ids.is_empty() {
            self.entry_points.clear();
            return;
        }

        let max_entries = self.params.entry_point_count.min(ids.len());
        if max_entries == ids.len() {
            self.entry_points = ids;
            return;
        }

        let mut selected = Vec::with_capacity(max_entries);
        let mut selected_set = HashSet::with_capacity(max_entries);

        let first = ids[0];
        selected.push(first);
        selected_set.insert(first);

        while selected.len() < max_entries {
            let mut best: Option<(u64, f32)> = None;

            for &candidate_id in &ids {
                if selected_set.contains(&candidate_id) {
                    continue;
                }

                let candidate_vector = &self
                    .nodes
                    .get(&candidate_id)
                    .expect("candidate exists")
                    .vector;
                let min_distance = selected
                    .iter()
                    .filter_map(|selected_id| self.nodes.get(selected_id))
                    .map(|selected_node| self.distance(candidate_vector, &selected_node.vector))
                    .fold(f32::INFINITY, f32::min);

                match best {
                    Some((_, current_best)) if min_distance <= current_best => {}
                    _ => best = Some((candidate_id, min_distance)),
                }
            }

            let Some((next_id, _)) = best else {
                break;
            };

            selected.push(next_id);
            selected_set.insert(next_id);
        }

        self.entry_points = selected;
    }

    fn sorted_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.nodes.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    fn live_count(&self, exclude: &HashSet<u64>) -> usize {
        self.nodes
            .keys()
            .filter(|centroid_id| !exclude.contains(centroid_id))
            .count()
    }

    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        distance::raw_distance(a, b, self.distance_metric)
    }
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
        let graph = SptagCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let results = graph.search(&[0.9, 0.1, 0.1], 1);

        // then
        assert_eq!(results, vec![1]);
    }

    #[test]
    fn should_build_and_search_dot_product_graph() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![1.0, 0.0]),
            CentroidEntry::new(2, vec![0.0, 1.0]),
            CentroidEntry::new(3, vec![0.8, 0.8]),
        ];

        // when
        let graph = SptagCentroidGraph::build(centroids, DistanceMetric::DotProduct).unwrap();
        let results = graph.search(&[0.9, 0.7], 2);

        // then
        assert_eq!(results[0], 3);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn should_add_centroid_and_find_it() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![0.0, 0.0]),
            CentroidEntry::new(2, vec![10.0, 10.0]),
        ];
        let graph = SptagCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when
        graph
            .add_centroid(&CentroidEntry::new(3, vec![0.1, 0.2]))
            .unwrap();
        let results = graph.search(&[0.1, 0.1], 2);

        // then
        assert_eq!(results[0], 3);
        assert_eq!(graph.len(), 3);
    }

    #[test]
    fn should_remove_centroid_and_repair_local_graph() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![0.0, 0.0]),
            CentroidEntry::new(2, vec![0.5, 0.0]),
            CentroidEntry::new(3, vec![1.0, 0.0]),
            CentroidEntry::new(4, vec![3.0, 0.0]),
        ];
        let graph = SptagCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();

        // when
        graph.remove_centroid(2).unwrap();
        let results = graph.search(&[0.9, 0.0], 2);

        // then
        assert_eq!(results[0], 3);
        assert!(!results.contains(&2));
        assert_eq!(graph.len(), 3);
    }

    #[test]
    fn should_merge_graph_results_with_include_and_exclude() {
        // given
        let centroids = vec![
            CentroidEntry::new(1, vec![0.0, 0.0]),
            CentroidEntry::new(2, vec![5.0, 5.0]),
            CentroidEntry::new(3, vec![10.0, 10.0]),
        ];
        let graph = SptagCentroidGraph::build(centroids, DistanceMetric::L2).unwrap();
        let include = CentroidEntry::new(99, vec![0.1, 0.1]);
        let exclude = HashSet::from([1_u64]);

        // when
        let results = graph.search_with_include_exclude(&[0.0, 0.0], 2, &[&include], &exclude);

        // then
        assert_eq!(results[0], 99);
        assert!(!results.contains(&1));
    }
}

//! SPTAG-inspired centroid graph implementation.
//!
//! This follows the SPFresh/SPTAG head-index structure more closely than the
//! prior graph-only version:
//! - a BKT-style tree is used to seed search
//! - a relative-neighborhood graph is used for refinement and traversal
//! - inserts use a `RefineNode`-style local repair path
//! - removals are tombstoned and periodically trigger tree/graph rebuilds
//!
//! The internal state is intentionally stored in flat vectors so the index can
//! later be serialized externally without another structural redesign.

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::sync::RwLock;

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

use crate::error::{Error, Result};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::serde::collection_meta::DistanceMetric;

use super::CentroidGraph;

const DEFAULT_TREE_NUMBER: usize = 1;
const DEFAULT_BKT_KMEANS_K: usize = 32;
const DEFAULT_BKT_LEAF_SIZE: usize = 8;
const DEFAULT_BKT_SAMPLES: usize = 1000;
const DEFAULT_NEIGHBORHOOD_SIZE: usize = 32;
const DEFAULT_CEF: usize = 96;
const DEFAULT_ADD_CEF: usize = 48;
const DEFAULT_MAX_CHECK: usize = 512;
const DEFAULT_INITIAL_DYNAMIC_PIVOTS: usize = 16;
const DEFAULT_OTHER_DYNAMIC_PIVOTS: usize = 4;
const DEFAULT_ADD_COUNT_FOR_REBUILD: usize = 64;
const DEFAULT_DELETE_RATIO_FOR_REBUILD: f32 = 0.4;
const DEFAULT_EXACT_SEARCH_THRESHOLD: usize = 64;
const DEFAULT_GRAPH_BUILD_CANDIDATES: usize = 128;
const INVALID_INDEX: usize = usize::MAX;

#[derive(Debug, Clone)]
struct GraphNode {
    centroid_id: u64,
    deleted: bool,
}

#[derive(Debug, Clone, Copy)]
struct BktNode {
    center_idx: usize,
    child_start: usize,
    child_end: usize,
    collapsed_leaf: bool,
}

impl BktNode {
    fn leaf(center_idx: usize) -> Self {
        Self {
            center_idx,
            child_start: 0,
            child_end: 0,
            collapsed_leaf: false,
        }
    }

    fn is_leaf(&self) -> bool {
        self.child_start == self.child_end
    }
}

#[derive(Debug, Clone, Copy)]
struct BktRoot {
    child_start: usize,
    child_end: usize,
}

#[derive(Debug, Clone, Copy)]
struct SptagParams {
    tree_number: usize,
    bkt_kmeans_k: usize,
    bkt_leaf_size: usize,
    bkt_samples: usize,
    neighborhood_size: usize,
    cef: usize,
    add_cef: usize,
    max_check: usize,
    initial_dynamic_pivots: usize,
    other_dynamic_pivots: usize,
    add_count_for_rebuild: usize,
    delete_ratio_for_rebuild: f32,
    exact_search_threshold: usize,
    graph_build_candidates: usize,
}

impl Default for SptagParams {
    fn default() -> Self {
        Self {
            tree_number: DEFAULT_TREE_NUMBER,
            bkt_kmeans_k: DEFAULT_BKT_KMEANS_K,
            bkt_leaf_size: DEFAULT_BKT_LEAF_SIZE,
            bkt_samples: DEFAULT_BKT_SAMPLES,
            neighborhood_size: DEFAULT_NEIGHBORHOOD_SIZE,
            cef: DEFAULT_CEF,
            add_cef: DEFAULT_ADD_CEF,
            max_check: DEFAULT_MAX_CHECK,
            initial_dynamic_pivots: DEFAULT_INITIAL_DYNAMIC_PIVOTS,
            other_dynamic_pivots: DEFAULT_OTHER_DYNAMIC_PIVOTS,
            add_count_for_rebuild: DEFAULT_ADD_COUNT_FOR_REBUILD,
            delete_ratio_for_rebuild: DEFAULT_DELETE_RATIO_FOR_REBUILD,
            exact_search_threshold: DEFAULT_EXACT_SEARCH_THRESHOLD,
            graph_build_candidates: DEFAULT_GRAPH_BUILD_CANDIDATES,
        }
    }
}

#[derive(Debug)]
struct SptagCentroidGraphInner {
    dimensions: usize,
    distance_metric: DistanceMetric,
    params: SptagParams,
    nodes: Vec<GraphNode>,
    vectors: Vec<f32>,
    centroid_to_index: HashMap<u64, usize>,
    graph_neighbors: Vec<usize>,
    tree_roots: Vec<BktRoot>,
    tree_nodes: Vec<BktNode>,
    tree_leaf_backlinks: Vec<usize>,
    bkt_balance_factor: Option<f32>,
    pending_tree_mutations: usize,
    deleted_count: usize,
}

/// Centroid graph backed by a BKT tree plus relative-neighborhood graph.
pub struct SptagCentroidGraph {
    inner: RwLock<SptagCentroidGraphInner>,
}

thread_local! {
    static SEARCH_WORKSPACE: RefCell<SearchWorkspace> = RefCell::new(SearchWorkspace::default());
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct ScoredCandidate {
    idx: usize,
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
            .then_with(|| self.idx.cmp(&other.idx))
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TreeCandidate {
    node_idx: usize,
    distance: f32,
}

impl Eq for TreeCandidate {}

impl PartialOrd for TreeCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TreeCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.node_idx.cmp(&other.node_idx))
    }
}

#[derive(Default)]
struct SearchWorkspace {
    epoch: u32,
    visited: Vec<u32>,
    tree_queue: BinaryHeap<Reverse<TreeCandidate>>,
    graph_queue: BinaryHeap<Reverse<ScoredCandidate>>,
    result_bound: BinaryHeap<ScoredCandidate>,
    top_results: BinaryHeap<ScoredCandidate>,
}

impl SearchWorkspace {
    fn reset(&mut self, size: usize) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.epoch = 1;
            self.visited.fill(0);
        }
        if self.visited.len() < size {
            self.visited.resize(size, 0);
        }
        self.tree_queue.clear();
        self.graph_queue.clear();
        self.result_bound.clear();
        self.top_results.clear();
    }

    fn check_and_visit(&mut self, idx: usize) -> bool {
        if self.visited[idx] == self.epoch {
            true
        } else {
            self.visited[idx] = self.epoch;
            false
        }
    }
}

#[derive(Clone, Copy)]
enum SearchExclusions<'a> {
    None,
    ExternalIds(&'a HashSet<u64>),
    SingleIndex(usize),
}

#[derive(Debug)]
struct PartitionCluster {
    indices: Vec<usize>,
    center_idx: usize,
}

struct KmeansAssignment {
    counts: Vec<usize>,
    weighted_counts: Vec<f32>,
    center_sums: Vec<Vec<f32>>,
    cluster_indices: Vec<usize>,
    cluster_dists: Vec<f32>,
    labels: Vec<usize>,
    total_distance: f32,
}

struct KmeansClusteringResult {
    count_std: f32,
    clusters: Vec<PartitionCluster>,
}

impl fmt::Debug for SptagCentroidGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.read().expect("lock poisoned");
        f.debug_struct("SptagCentroidGraph")
            .field("num_centroids", &inner.live_count())
            .field("tree_roots", &inner.tree_roots.len())
            .field("tree_nodes", &inner.tree_nodes.len())
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

        let mut nodes = Vec::with_capacity(centroids.len());
        let mut vectors = Vec::with_capacity(centroids.len().saturating_mul(dimensions));
        let mut centroid_to_index = HashMap::with_capacity(centroids.len());
        for centroid in centroids {
            let idx = nodes.len();
            centroid_to_index.insert(centroid.centroid_id, idx);
            vectors.extend_from_slice(&centroid.vector);
            nodes.push(GraphNode {
                centroid_id: centroid.centroid_id,
                deleted: false,
            });
        }

        let params = SptagParams::default();
        let graph_neighbors =
            vec![INVALID_INDEX; nodes.len().saturating_mul(params.neighborhood_size)];

        let mut inner = SptagCentroidGraphInner {
            dimensions,
            distance_metric,
            params,
            nodes,
            vectors,
            centroid_to_index,
            graph_neighbors,
            tree_roots: Vec::new(),
            tree_nodes: Vec::new(),
            tree_leaf_backlinks: Vec::new(),
            bkt_balance_factor: None,
            pending_tree_mutations: 0,
            deleted_count: 0,
        };
        inner.rebuild_tree();
        inner.rebuild_graph();

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
            .search_ids(query, k, &HashSet::new())
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
        let inner = self.inner.read().expect("lock poisoned");
        let idx = *inner.centroid_to_index.get(&centroid_id)?;
        let node = inner.nodes.get(idx)?;
        if node.deleted {
            None
        } else {
            Some(inner.vector(idx).to_vec())
        }
    }

    fn all_centroid_ids(&self) -> Vec<u64> {
        self.inner.read().expect("lock poisoned").all_centroid_ids()
    }

    fn len(&self) -> usize {
        self.inner.read().expect("lock poisoned").live_count()
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

        if include.is_empty() {
            return self.search_ids(query, k, exclude);
        }

        let search_k = k.saturating_add(include.len());
        let mut merged = HashMap::with_capacity(search_k.saturating_add(include.len()));
        for candidate in self.search_scored(query, search_k, SearchExclusions::ExternalIds(exclude))
        {
            merged.insert(self.nodes[candidate.idx].centroid_id, candidate.distance);
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
        if self.centroid_to_index.contains_key(&entry.centroid_id) {
            return Err(Error::Internal(format!(
                "Centroid {} already exists in graph",
                entry.centroid_id
            )));
        }

        let idx = self.nodes.len();
        self.vectors.extend_from_slice(&entry.vector);
        self.nodes.push(GraphNode {
            centroid_id: entry.centroid_id,
            deleted: false,
        });
        self.centroid_to_index.insert(entry.centroid_id, idx);
        self.graph_neighbors.extend(std::iter::repeat_n(
            INVALID_INDEX,
            self.params.neighborhood_size,
        ));
        self.tree_leaf_backlinks.push(INVALID_INDEX);
        self.pending_tree_mutations = self.pending_tree_mutations.saturating_add(1);

        self.refine_node(idx, true, self.params.add_cef);
        self.maybe_rebuild_after_mutation(false);
        Ok(())
    }

    fn remove_centroid(&mut self, centroid_id: u64) -> Result<()> {
        let idx = *self.centroid_to_index.get(&centroid_id).ok_or_else(|| {
            Error::Internal(format!("Centroid {} not found in graph", centroid_id))
        })?;
        let node = self.nodes.get_mut(idx).expect("index exists");
        if node.deleted {
            return Err(Error::Internal(format!(
                "Centroid {} not found in graph",
                centroid_id
            )));
        }

        node.deleted = true;
        self.clear_graph_row(idx);
        self.deleted_count = self.deleted_count.saturating_add(1);
        self.pending_tree_mutations = self.pending_tree_mutations.saturating_add(1);

        self.maybe_rebuild_after_mutation(true);
        Ok(())
    }

    fn search_ids(&self, query: &[f32], k: usize, exclude: &HashSet<u64>) -> Vec<u64> {
        let exclusions = if exclude.is_empty() {
            SearchExclusions::None
        } else {
            SearchExclusions::ExternalIds(exclude)
        };
        self.search_scored(query, k, exclusions)
            .into_iter()
            .map(|candidate| self.nodes[candidate.idx].centroid_id)
            .collect()
    }

    fn search_scored(
        &self,
        query: &[f32],
        k: usize,
        exclusions: SearchExclusions<'_>,
    ) -> Vec<ScoredCandidate> {
        if k == 0 || query.len() != self.dimensions {
            return Vec::new();
        }

        let live_count = self.live_count_with_exclusions(exclusions);
        if live_count == 0 {
            return Vec::new();
        }

        if live_count <= self.params.exact_search_threshold || self.tree_roots.is_empty() {
            return self.exact_search(query, k, exclusions);
        }

        let final_capacity = k.min(live_count);
        let query_capacity = self.params.cef.max(final_capacity).min(live_count);
        let reservoir_capacity = query_capacity
            .max((self.params.max_check / 16).max(1))
            .min(live_count);
        let max_check = self.params.max_check.min(live_count.max(1));
        let initial_pivots = self.params.initial_dynamic_pivots.min(live_count);
        let other_pivots = self.params.other_dynamic_pivots.min(live_count);

        SEARCH_WORKSPACE.with(|workspace| {
            let mut workspace = workspace.borrow_mut();
            workspace.reset(self.nodes.len());
            self.init_search_trees(query, &mut workspace.tree_queue);

            let mut checked_leaves = 0usize;
            self.search_trees(
                query,
                &mut workspace,
                exclusions,
                &mut checked_leaves,
                initial_pivots,
            );

            while let Some(Reverse(candidate)) = workspace.graph_queue.pop() {
                let worst_final = workspace
                    .top_results
                    .peek()
                    .map(|c| c.distance)
                    .unwrap_or(f32::INFINITY);
                if workspace.top_results.len() >= query_capacity && candidate.distance > worst_final
                {
                    break;
                }

                self.push_candidate_results(
                    query,
                    candidate,
                    exclusions,
                    &mut workspace.top_results,
                    query_capacity,
                );

                if checked_leaves >= max_check {
                    continue;
                }

                for &neighbor_idx in self.graph_row(candidate.idx) {
                    if neighbor_idx == INVALID_INDEX || workspace.check_and_visit(neighbor_idx) {
                        continue;
                    }
                    checked_leaves = checked_leaves.saturating_add(1);

                    let scored = ScoredCandidate {
                        idx: neighbor_idx,
                        distance: self.distance(query, self.vector(neighbor_idx)),
                    };

                    if Self::push_bound_candidate(
                        &mut workspace.result_bound,
                        scored,
                        reservoir_capacity,
                    ) {
                        workspace.graph_queue.push(Reverse(scored));
                    }

                    if checked_leaves >= max_check {
                        break;
                    }
                }

                let best_graph = workspace.graph_queue.peek().map(|entry| entry.0.distance);
                let best_tree = workspace.tree_queue.peek().map(|entry| entry.0.distance);
                if let (Some(best_graph), Some(best_tree)) = (best_graph, best_tree) {
                    if best_graph > best_tree {
                        let limit = (other_pivots + checked_leaves).min(max_check);
                        self.search_trees(
                            query,
                            &mut workspace,
                            exclusions,
                            &mut checked_leaves,
                            limit,
                        );
                    }
                }
            }

            let mut results = std::mem::take(&mut workspace.top_results).into_sorted_vec();
            results.truncate(final_capacity);
            results
        })
    }

    fn init_search_trees(
        &self,
        query: &[f32],
        tree_queue: &mut BinaryHeap<Reverse<TreeCandidate>>,
    ) {
        for root in &self.tree_roots {
            for node_idx in root.child_start..root.child_end {
                let node = self.tree_nodes[node_idx];
                tree_queue.push(Reverse(TreeCandidate {
                    node_idx,
                    distance: self.distance(query, self.vector(node.center_idx)),
                }));
            }
        }
    }

    fn search_trees(
        &self,
        query: &[f32],
        workspace: &mut SearchWorkspace,
        exclusions: SearchExclusions<'_>,
        checked_leaves: &mut usize,
        limit: usize,
    ) {
        while let Some(Reverse(candidate)) = workspace.tree_queue.pop() {
            let tree_node = self.tree_nodes[candidate.node_idx];
            if tree_node.collapsed_leaf {
                if !workspace.check_and_visit(tree_node.center_idx) {
                    *checked_leaves = checked_leaves.saturating_add(1);
                    workspace.graph_queue.push(Reverse(ScoredCandidate {
                        idx: tree_node.center_idx,
                        distance: candidate.distance,
                    }));
                }
                if *checked_leaves >= limit {
                    break;
                }
                continue;
            }

            if tree_node.is_leaf() {
                if !workspace.check_and_visit(tree_node.center_idx) {
                    *checked_leaves = checked_leaves.saturating_add(1);
                    if !self.is_excluded(tree_node.center_idx, exclusions) {
                        workspace.graph_queue.push(Reverse(ScoredCandidate {
                            idx: tree_node.center_idx,
                            distance: candidate.distance,
                        }));
                    }
                }
                if *checked_leaves >= limit {
                    break;
                }
                continue;
            }

            if !workspace.check_and_visit(tree_node.center_idx) {
                if !self.is_excluded(tree_node.center_idx, exclusions) {
                    workspace.graph_queue.push(Reverse(ScoredCandidate {
                        idx: tree_node.center_idx,
                        distance: candidate.distance,
                    }));
                }
            }

            for child_idx in tree_node.child_start..tree_node.child_end {
                let child = self.tree_nodes[child_idx];
                workspace.tree_queue.push(Reverse(TreeCandidate {
                    node_idx: child_idx,
                    distance: self.distance(query, self.vector(child.center_idx)),
                }));
            }
        }
    }

    fn refine_node(&mut self, node_idx: usize, update_neighbors: bool, cef: usize) {
        if !self.is_live_idx(node_idx) {
            return;
        }

        let results = self.search_scored(
            self.vector(node_idx),
            cef.saturating_add(1),
            SearchExclusions::SingleIndex(node_idx),
        );

        let neighbors = self.rng_prune_from_candidates(node_idx, results.into_iter());
        self.set_graph_neighbors(node_idx, &neighbors);

        if update_neighbors {
            for neighbor_idx in neighbors {
                self.insert_neighbor(neighbor_idx, node_idx);
            }
        }
    }

    fn rebuild_graph(&mut self) {
        self.graph_neighbors.fill(INVALID_INDEX);

        let live_indices = self.live_indices();
        let candidate_count = self
            .params
            .graph_build_candidates
            .max(self.params.neighborhood_size.saturating_mul(2));

        for &idx in &live_indices {
            let candidates = self.exact_search_by_index(idx, candidate_count);
            let neighbors = self.rng_prune_from_candidates(idx, candidates.into_iter());
            self.set_graph_neighbors(idx, &neighbors);
        }

        for &idx in &live_indices {
            let neighbors = self.graph_neighbors_vec(idx);
            for neighbor_idx in neighbors {
                self.insert_neighbor(neighbor_idx, idx);
            }
        }
    }

    fn rebuild_tree(&mut self) {
        self.tree_roots.clear();
        self.tree_nodes.clear();
        self.tree_leaf_backlinks.clear();
        self.tree_leaf_backlinks
            .resize(self.nodes.len(), INVALID_INDEX);

        let live_indices = self.live_indices();
        if live_indices.is_empty() {
            return;
        }

        if self.bkt_balance_factor.is_none() {
            self.bkt_balance_factor = Some(self.dynamic_factor_select(&live_indices));
        }

        for tree_idx in 0..self.params.tree_number.max(1) {
            let mut order = live_indices.clone();
            if !order.is_empty() {
                let mut rng = self.partition_rng(&order, tree_idx as u64);
                order.shuffle(&mut rng);
            }

            let root_start = self.tree_nodes.len();
            self.append_root_children(&order);
            let root_end = self.tree_nodes.len();
            self.tree_roots.push(BktRoot {
                child_start: root_start,
                child_end: root_end,
            });
        }

        self.pending_tree_mutations = 0;
    }

    fn append_root_children(&mut self, indices: &[usize]) {
        if indices.is_empty() {
            return;
        }

        if indices.len() <= self.params.bkt_leaf_size {
            let node_idx = self.tree_nodes.len();
            self.tree_nodes.push(BktNode::leaf(indices[0]));
            self.fill_tree_node(node_idx, indices.to_vec());
            return;
        }

        let clusters = self.partition_cluster(indices);
        if clusters.is_empty() {
            let center_idx = self.choose_center_for_indices(indices);
            let node_idx = self.tree_nodes.len();
            self.tree_nodes.push(BktNode {
                center_idx,
                child_start: 0,
                child_end: 0,
                collapsed_leaf: false,
            });
            self.collapse_tree_node(
                node_idx,
                indices
                    .iter()
                    .copied()
                    .filter(|idx| *idx != center_idx)
                    .collect(),
            );
            return;
        }

        for cluster in clusters {
            let node_idx = self.tree_nodes.len();
            self.tree_nodes.push(BktNode {
                center_idx: cluster.center_idx,
                child_start: 0,
                child_end: 0,
                collapsed_leaf: false,
            });
            self.fill_tree_node(node_idx, cluster.indices);
        }
    }

    fn fill_tree_node(&mut self, node_idx: usize, indices: Vec<usize>) {
        let center_idx = self.tree_nodes[node_idx].center_idx;
        let remaining: Vec<usize> = indices
            .into_iter()
            .filter(|idx| *idx != center_idx)
            .collect();
        if remaining.is_empty() {
            self.tree_nodes[node_idx].child_start = 0;
            self.tree_nodes[node_idx].child_end = 0;
            return;
        }

        if remaining.len() <= self.params.bkt_leaf_size {
            let child_start = self.tree_nodes.len();
            for idx in remaining {
                self.tree_nodes.push(BktNode::leaf(idx));
            }
            let child_end = self.tree_nodes.len();
            self.tree_nodes[node_idx].child_start = child_start;
            self.tree_nodes[node_idx].child_end = child_end;
            return;
        }

        let child_clusters = self.partition_cluster(&remaining);
        if child_clusters.is_empty() {
            self.collapse_tree_node(node_idx, remaining);
            return;
        }
        let child_start = self.tree_nodes.len();
        let mut child_specs = Vec::with_capacity(child_clusters.len());
        for cluster in child_clusters {
            let child_idx = self.tree_nodes.len();
            self.tree_nodes.push(BktNode {
                center_idx: cluster.center_idx,
                child_start: 0,
                child_end: 0,
                collapsed_leaf: false,
            });
            child_specs.push((child_idx, cluster.indices));
        }
        let child_end = self.tree_nodes.len();
        self.tree_nodes[node_idx].child_start = child_start;
        self.tree_nodes[node_idx].child_end = child_end;

        for (child_idx, cluster) in child_specs {
            self.fill_tree_node(child_idx, cluster);
        }
    }

    fn partition_cluster(&self, indices: &[usize]) -> Vec<PartitionCluster> {
        if indices.len() <= self.params.bkt_leaf_size {
            return indices
                .iter()
                .copied()
                .map(|idx| PartitionCluster {
                    indices: vec![idx],
                    center_idx: idx,
                })
                .collect();
        }

        let cluster_count = ((indices.len() / self.params.bkt_leaf_size) + 1)
            .max(2)
            .min(self.params.bkt_kmeans_k)
            .min(indices.len());
        let balance_factor = self.bkt_balance_factor.unwrap_or(100.0);
        let clustering = self.kmeans_clustering(indices, cluster_count, balance_factor);
        if clustering.clusters.len() > 1 {
            return clustering.clusters;
        }

        Vec::new()
    }

    fn collapse_tree_node(&mut self, node_idx: usize, members: Vec<usize>) {
        let center_idx = self.tree_nodes[node_idx].center_idx;
        let child_start = self.tree_nodes.len();
        for member_idx in members {
            self.tree_nodes.push(BktNode::leaf(member_idx));
        }
        let child_end = self.tree_nodes.len();
        self.tree_nodes[node_idx].child_start = child_start;
        self.tree_nodes[node_idx].child_end = child_end;
        self.tree_nodes[node_idx].collapsed_leaf = true;
        if center_idx < self.tree_leaf_backlinks.len() {
            self.tree_leaf_backlinks[center_idx] = node_idx;
        }
    }

    fn dynamic_factor_select(&self, indices: &[usize]) -> f32 {
        if indices.len() <= self.params.bkt_leaf_size {
            return 100.0;
        }

        let cluster_count = ((indices.len() / self.params.bkt_leaf_size) + 1)
            .max(2)
            .min(self.params.bkt_kmeans_k)
            .min(indices.len());

        let mut best_lambda_factor = 100.0;
        let mut best_count_std = f32::INFINITY;
        for lambda_factor in [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0] {
            let clustering = self.kmeans_clustering(indices, cluster_count, lambda_factor);
            if clustering.count_std < best_count_std {
                best_count_std = clustering.count_std;
                best_lambda_factor = lambda_factor;
            }
        }
        best_lambda_factor
    }

    fn kmeans_clustering(
        &self,
        indices: &[usize],
        cluster_count: usize,
        lambda_factor: f32,
    ) -> KmeansClusteringResult {
        let sample_len = self
            .params
            .bkt_samples
            .min(indices.len())
            .max(cluster_count)
            .min(indices.len());
        let mut order = indices.to_vec();
        let mut rng = self.partition_rng(indices, lambda_factor.to_bits() as u64);
        let (mut centers, mut counts, adjusted_lambda) =
            self.init_kmeans_centers(&order, cluster_count, sample_len, &mut rng);
        let original_lambda = 1.0f32 / lambda_factor.max(f32::EPSILON) / sample_len.max(1) as f32;
        let mut min_cluster_dist = f32::INFINITY;
        let mut no_improvement = 0usize;

        for _ in 0..100 {
            order.shuffle(&mut rng);
            let assignment = self.kmeans_assign(
                &order[..sample_len],
                &centers,
                &counts,
                adjusted_lambda.min(original_lambda),
                true,
            );
            counts = assignment.counts.clone();
            if assignment.total_distance < min_cluster_dist {
                min_cluster_dist = assignment.total_distance;
                no_improvement = 0;
            } else {
                no_improvement = no_improvement.saturating_add(1);
            }

            let (next_centers, diff) = self.refine_centers(&centers, &assignment);
            centers = next_centers;
            if diff < 1e-3 || no_improvement >= 5 {
                break;
            }
        }

        let zero_counts = vec![0usize; cluster_count];
        let nearest_assignment = self.kmeans_assign(&order, &centers, &zero_counts, 0.0, false);
        for (cluster_idx, &center_idx) in nearest_assignment.cluster_indices.iter().enumerate() {
            if center_idx != INVALID_INDEX {
                centers[cluster_idx] = self.vector(center_idx).to_vec();
            }
        }

        let final_assignment = self.kmeans_assign(&order, &centers, &zero_counts, 0.0, false);
        let count_avg = order.len() as f32 / cluster_count.max(1) as f32;
        let count_std = if count_avg == 0.0 {
            0.0
        } else {
            let variance = final_assignment
                .counts
                .iter()
                .map(|&count| {
                    let delta = count as f32 - count_avg;
                    delta * delta
                })
                .sum::<f32>()
                / cluster_count.max(1) as f32;
            variance.sqrt() / count_avg
        };

        KmeansClusteringResult {
            count_std,
            clusters: self.build_partition_clusters(&order, &final_assignment),
        }
    }

    fn init_kmeans_centers(
        &self,
        order: &[usize],
        cluster_count: usize,
        sample_len: usize,
        rng: &mut StdRng,
    ) -> (Vec<Vec<f32>>, Vec<usize>, f32) {
        let mut best_centers = Vec::new();
        let mut best_counts = vec![0usize; cluster_count];
        let mut best_lambda = 0.0f32;
        let mut best_distance = f32::INFINITY;

        for _ in 0..3 {
            let mut seeded = order.to_vec();
            seeded.shuffle(rng);
            let centers: Vec<Vec<f32>> = seeded
                .iter()
                .take(cluster_count)
                .map(|&idx| self.vector(idx).to_vec())
                .collect();
            let zero_counts = vec![0usize; cluster_count];
            let assignment =
                self.kmeans_assign(&order[..sample_len], &centers, &zero_counts, 0.0, true);
            if assignment.total_distance < best_distance {
                best_distance = assignment.total_distance;
                best_lambda = self.refine_lambda(&assignment, sample_len);
                best_counts = assignment.counts.clone();
                best_centers = centers;
            }
        }

        if best_centers.is_empty() {
            best_centers = order
                .iter()
                .take(cluster_count)
                .map(|&idx| self.vector(idx).to_vec())
                .collect();
        }

        (best_centers, best_counts, best_lambda)
    }

    fn kmeans_assign(
        &self,
        order: &[usize],
        centers: &[Vec<f32>],
        balance_counts: &[usize],
        lambda: f32,
        update_centers: bool,
    ) -> KmeansAssignment {
        let cluster_count = centers.len();
        let mut counts = vec![0usize; cluster_count];
        let mut weighted_counts = vec![0.0f32; cluster_count];
        let mut center_sums = vec![vec![0.0f32; self.dimensions]; cluster_count];
        let mut cluster_indices = vec![INVALID_INDEX; cluster_count];
        let mut cluster_dists = if update_centers {
            vec![f32::NEG_INFINITY; cluster_count]
        } else {
            vec![f32::INFINITY; cluster_count]
        };
        let mut labels = Vec::with_capacity(order.len());
        let mut total_distance = 0.0f32;

        for &idx in order {
            let mut best_cluster = 0usize;
            let mut best_distance = f32::INFINITY;
            for (cluster_idx, center) in centers.iter().enumerate() {
                let distance = self.distance(self.vector(idx), center)
                    + lambda * balance_counts.get(cluster_idx).copied().unwrap_or(0) as f32;
                if distance < best_distance {
                    best_distance = distance;
                    best_cluster = cluster_idx;
                }
            }

            labels.push(best_cluster);
            counts[best_cluster] = counts[best_cluster].saturating_add(1);
            weighted_counts[best_cluster] += best_distance;
            total_distance += best_distance;

            if update_centers {
                for (dim, value) in self.vector(idx).iter().enumerate() {
                    center_sums[best_cluster][dim] += value;
                }
                if best_distance > cluster_dists[best_cluster] {
                    cluster_dists[best_cluster] = best_distance;
                    cluster_indices[best_cluster] = idx;
                }
            } else if best_distance <= cluster_dists[best_cluster] {
                cluster_dists[best_cluster] = best_distance;
                cluster_indices[best_cluster] = idx;
            }
        }

        KmeansAssignment {
            counts,
            weighted_counts,
            center_sums,
            cluster_indices,
            cluster_dists,
            labels,
            total_distance,
        }
    }

    fn refine_lambda(&self, assignment: &KmeansAssignment, size: usize) -> f32 {
        let mut max_cluster = INVALID_INDEX;
        let mut max_count = 0usize;
        for (cluster_idx, &count) in assignment.counts.iter().enumerate() {
            if count > max_count && count > 0 {
                max_count = count;
                max_cluster = cluster_idx;
            }
        }

        if max_cluster == INVALID_INDEX || size == 0 {
            return 0.0;
        }

        let avg_distance = assignment.weighted_counts[max_cluster] / max_count as f32;
        ((assignment.cluster_dists[max_cluster] - avg_distance) / size as f32).max(0.0)
    }

    fn refine_centers(
        &self,
        centers: &[Vec<f32>],
        assignment: &KmeansAssignment,
    ) -> (Vec<Vec<f32>>, f32) {
        let mut max_cluster = INVALID_INDEX;
        let mut max_count = 0usize;
        for (cluster_idx, (&count, &sample_idx)) in assignment
            .counts
            .iter()
            .zip(assignment.cluster_indices.iter())
            .enumerate()
        {
            if count == 0 || sample_idx == INVALID_INDEX {
                continue;
            }
            if count > max_count
                && self.l2_distance(self.vector(sample_idx), &centers[cluster_idx]) > 1e-6
            {
                max_count = count;
                max_cluster = cluster_idx;
            }
        }

        let mut next_centers = Vec::with_capacity(centers.len());
        let mut diff = 0.0f32;
        for (cluster_idx, center) in centers.iter().enumerate() {
            let next_center = if assignment.counts[cluster_idx] == 0 {
                if max_cluster != INVALID_INDEX {
                    self.vector(assignment.cluster_indices[max_cluster])
                        .to_vec()
                } else {
                    center.clone()
                }
            } else {
                assignment.center_sums[cluster_idx]
                    .iter()
                    .map(|value| *value / assignment.counts[cluster_idx] as f32)
                    .collect()
            };
            diff += self.l2_distance(&next_center, center);
            next_centers.push(next_center);
        }

        (next_centers, diff)
    }

    fn build_partition_clusters(
        &self,
        order: &[usize],
        assignment: &KmeansAssignment,
    ) -> Vec<PartitionCluster> {
        let mut clustered = vec![Vec::new(); assignment.counts.len()];
        for (&label, &idx) in assignment.labels.iter().zip(order.iter()) {
            clustered[label].push(idx);
        }

        let mut clusters = Vec::with_capacity(clustered.len());
        for (cluster_idx, mut members) in clustered.into_iter().enumerate() {
            if members.is_empty() {
                continue;
            }

            let mut center_idx = assignment.cluster_indices[cluster_idx];
            if center_idx == INVALID_INDEX || !members.contains(&center_idx) {
                center_idx = self.choose_center_for_indices(&members);
            }
            if let Some(pos) = members.iter().position(|&idx| idx == center_idx) {
                let center = members.swap_remove(pos);
                members.push(center);
            }

            clusters.push(PartitionCluster {
                indices: members,
                center_idx,
            });
        }
        clusters
    }

    fn choose_center_for_indices(&self, indices: &[usize]) -> usize {
        if indices.len() == 1 {
            return indices[0];
        }

        let mut mean = vec![0.0f32; self.dimensions];
        for &idx in indices {
            for (dim, value) in self.vector(idx).iter().enumerate() {
                mean[dim] += value;
            }
        }
        let count = indices.len() as f32;
        for value in &mut mean {
            *value /= count;
        }

        indices
            .iter()
            .copied()
            .min_by(|&a, &b| {
                let da = self.distance(&mean, self.vector(a));
                let db = self.distance(&mean, self.vector(b));
                da.total_cmp(&db).then_with(|| a.cmp(&b))
            })
            .unwrap_or(indices[0])
    }

    fn partition_rng(&self, indices: &[usize], salt: u64) -> StdRng {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ salt ^ (indices.len() as u64).rotate_left(17);
        for &idx in indices.iter().take(32) {
            seed ^= self.nodes[idx]
                .centroid_id
                .wrapping_add(0x9e37_79b9_7f4a_7c15u64);
            seed = seed.rotate_left(13).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        }
        StdRng::seed_from_u64(seed)
    }

    fn l2_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        let len = a.len().min(b.len());
        let mut sum0 = 0.0f32;
        let mut sum1 = 0.0f32;
        let mut sum2 = 0.0f32;
        let mut sum3 = 0.0f32;
        let mut i = 0usize;

        while i + 4 <= len {
            let delta0 = a[i] - b[i];
            let delta1 = a[i + 1] - b[i + 1];
            let delta2 = a[i + 2] - b[i + 2];
            let delta3 = a[i + 3] - b[i + 3];
            sum0 += delta0 * delta0;
            sum1 += delta1 * delta1;
            sum2 += delta2 * delta2;
            sum3 += delta3 * delta3;
            i += 4;
        }

        let mut sum = sum0 + sum1 + sum2 + sum3;
        while i < len {
            let delta = a[i] - b[i];
            sum += delta * delta;
            i += 1;
        }

        sum
    }

    fn push_candidate_results(
        &self,
        query: &[f32],
        candidate: ScoredCandidate,
        exclusions: SearchExclusions<'_>,
        top_results: &mut BinaryHeap<ScoredCandidate>,
        capacity: usize,
    ) {
        let tree_idx = self.tree_leaf_backlinks[candidate.idx];
        if tree_idx == INVALID_INDEX {
            if !self.is_excluded(candidate.idx, exclusions) {
                Self::push_top_result(top_results, candidate, capacity);
            }
            return;
        }

        if !self.is_excluded(candidate.idx, exclusions) {
            Self::push_top_result(top_results, candidate, capacity);
        }

        let tree_node = self.tree_nodes[tree_idx];
        for child_idx in tree_node.child_start..tree_node.child_end {
            let member_idx = self.tree_nodes[child_idx].center_idx;
            if self.is_excluded(member_idx, exclusions) {
                continue;
            }
            Self::push_top_result(
                top_results,
                ScoredCandidate {
                    idx: member_idx,
                    distance: self.distance(query, self.vector(member_idx)),
                },
                capacity,
            );
        }
    }

    fn exact_search_by_index(&self, idx: usize, k: usize) -> Vec<ScoredCandidate> {
        let mut results = Vec::with_capacity(self.live_count().saturating_sub(1));
        for candidate_idx in self.live_indices() {
            if candidate_idx == idx {
                continue;
            }
            results.push(ScoredCandidate {
                idx: candidate_idx,
                distance: self.distance(self.vector(idx), self.vector(candidate_idx)),
            });
        }
        results.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.idx.cmp(&b.idx))
        });
        results.truncate(k.min(results.len()));
        results
    }

    fn exact_search(
        &self,
        query: &[f32],
        k: usize,
        exclusions: SearchExclusions<'_>,
    ) -> Vec<ScoredCandidate> {
        let mut scored = Vec::with_capacity(self.live_count());
        for idx in self.live_indices() {
            if self.is_excluded(idx, exclusions) {
                continue;
            }
            scored.push(ScoredCandidate {
                idx,
                distance: self.distance(query, self.vector(idx)),
            });
        }
        scored.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.idx.cmp(&b.idx))
        });
        scored.truncate(k.min(scored.len()));
        scored
    }

    fn rng_prune_from_candidates<I>(&self, node_idx: usize, candidates: I) -> Vec<usize>
    where
        I: IntoIterator<Item = ScoredCandidate>,
    {
        let mut selected: Vec<usize> = Vec::with_capacity(self.params.neighborhood_size);
        for candidate in candidates {
            if candidate.idx == node_idx || !self.is_live_idx(candidate.idx) {
                continue;
            }

            let good = selected.iter().all(|&selected_idx| {
                self.distance(self.vector(selected_idx), self.vector(candidate.idx))
                    >= candidate.distance
            });

            if good {
                selected.push(candidate.idx);
                if selected.len() >= self.params.neighborhood_size {
                    break;
                }
            }
        }
        selected
    }

    fn graph_row_bounds(&self, node_idx: usize) -> (usize, usize) {
        let start = node_idx.saturating_mul(self.params.neighborhood_size);
        let end = start + self.params.neighborhood_size;
        (start, end)
    }

    fn vector_bounds(&self, node_idx: usize) -> (usize, usize) {
        let start = node_idx.saturating_mul(self.dimensions);
        let end = start + self.dimensions;
        (start, end)
    }

    fn vector(&self, node_idx: usize) -> &[f32] {
        let (start, end) = self.vector_bounds(node_idx);
        &self.vectors[start..end]
    }

    fn graph_row(&self, node_idx: usize) -> &[usize] {
        let (start, end) = self.graph_row_bounds(node_idx);
        &self.graph_neighbors[start..end]
    }

    fn graph_row_mut(&mut self, node_idx: usize) -> &mut [usize] {
        let (start, end) = self.graph_row_bounds(node_idx);
        &mut self.graph_neighbors[start..end]
    }

    fn clear_graph_row(&mut self, node_idx: usize) {
        self.graph_row_mut(node_idx).fill(INVALID_INDEX);
    }

    fn set_graph_neighbors(&mut self, node_idx: usize, neighbors: &[usize]) {
        let row = self.graph_row_mut(node_idx);
        row.fill(INVALID_INDEX);
        let len = neighbors.len().min(row.len());
        row[..len].copy_from_slice(&neighbors[..len]);
    }

    fn graph_neighbors_vec(&self, node_idx: usize) -> Vec<usize> {
        self.graph_row(node_idx)
            .iter()
            .copied()
            .take_while(|idx| *idx != INVALID_INDEX)
            .collect()
    }

    fn insert_neighbor(&mut self, node_idx: usize, candidate_idx: usize) {
        if node_idx == candidate_idx
            || !self.is_live_idx(node_idx)
            || !self.is_live_idx(candidate_idx)
        {
            return;
        }

        if self.graph_row(node_idx).contains(&candidate_idx) {
            return;
        }

        let existing_neighbors = self.graph_neighbors_vec(node_idx);
        let mut candidates = Vec::with_capacity(existing_neighbors.len() + 1);
        for neighbor_idx in existing_neighbors {
            if self.is_live_idx(neighbor_idx) {
                candidates.push(ScoredCandidate {
                    idx: neighbor_idx,
                    distance: self.distance(self.vector(node_idx), self.vector(neighbor_idx)),
                });
            }
        }
        candidates.push(ScoredCandidate {
            idx: candidate_idx,
            distance: self.distance(self.vector(node_idx), self.vector(candidate_idx)),
        });
        candidates.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| a.idx.cmp(&b.idx))
        });
        let neighbors = self.rng_prune_from_candidates(node_idx, candidates);
        self.set_graph_neighbors(node_idx, &neighbors);
    }

    fn maybe_rebuild_after_mutation(&mut self, deletion_mutation: bool) {
        let live_count = self.live_count();
        if live_count == 0 {
            self.tree_roots.clear();
            self.tree_nodes.clear();
            return;
        }

        let delete_ratio = self.deleted_count as f32 / self.nodes.len().max(1) as f32;
        if deletion_mutation && delete_ratio >= self.params.delete_ratio_for_rebuild {
            self.compact_and_rebuild();
            return;
        }

        if self.pending_tree_mutations >= self.params.add_count_for_rebuild {
            self.rebuild_tree();
        }
    }

    fn compact_and_rebuild(&mut self) {
        let live_entries: Vec<_> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| !node.deleted)
            .map(|(idx, node)| CentroidEntry::new(node.centroid_id, self.vector(idx).to_vec()))
            .collect();

        self.nodes.clear();
        self.vectors.clear();
        self.centroid_to_index.clear();
        self.graph_neighbors.clear();
        self.tree_roots.clear();
        self.tree_nodes.clear();
        self.tree_leaf_backlinks.clear();
        self.pending_tree_mutations = 0;
        self.deleted_count = 0;

        for entry in live_entries {
            let idx = self.nodes.len();
            self.centroid_to_index.insert(entry.centroid_id, idx);
            self.vectors.extend_from_slice(&entry.vector);
            self.nodes.push(GraphNode {
                centroid_id: entry.centroid_id,
                deleted: false,
            });
            self.graph_neighbors.extend(std::iter::repeat_n(
                INVALID_INDEX,
                self.params.neighborhood_size,
            ));
            self.tree_leaf_backlinks.push(INVALID_INDEX);
        }

        self.rebuild_tree();
        self.rebuild_graph();
    }

    fn is_live_idx(&self, idx: usize) -> bool {
        self.nodes.get(idx).is_some_and(|node| !node.deleted)
    }

    fn all_centroid_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self
            .nodes
            .iter()
            .filter(|node| !node.deleted)
            .map(|node| node.centroid_id)
            .collect();
        ids.sort_unstable();
        ids
    }

    fn live_indices(&self) -> Vec<usize> {
        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(idx, node)| (!node.deleted).then_some(idx))
            .collect()
    }

    fn live_count(&self) -> usize {
        self.nodes.len().saturating_sub(self.deleted_count)
    }

    fn live_count_with_exclusions(&self, exclusions: SearchExclusions<'_>) -> usize {
        if matches!(exclusions, SearchExclusions::None) {
            return self.live_count();
        }

        self.nodes
            .iter()
            .enumerate()
            .filter(|(idx, node)| !node.deleted && !self.is_excluded(*idx, exclusions))
            .count()
    }

    fn is_excluded(&self, idx: usize, exclusions: SearchExclusions<'_>) -> bool {
        let Some(node) = self.nodes.get(idx) else {
            return true;
        };
        if node.deleted {
            return true;
        }

        match exclusions {
            SearchExclusions::None => false,
            SearchExclusions::ExternalIds(ids) => ids.contains(&node.centroid_id),
            SearchExclusions::SingleIndex(excluded_idx) => idx == excluded_idx,
        }
    }

    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.distance_metric {
            DistanceMetric::L2 => self.l2_distance(a, b),
            DistanceMetric::DotProduct => self.dot_distance(a, b),
        }
    }

    fn dot_distance(&self, a: &[f32], b: &[f32]) -> f32 {
        let len = a.len().min(b.len());
        let mut sum0 = 0.0f32;
        let mut sum1 = 0.0f32;
        let mut sum2 = 0.0f32;
        let mut sum3 = 0.0f32;
        let mut i = 0usize;

        while i + 4 <= len {
            sum0 += a[i] * b[i];
            sum1 += a[i + 1] * b[i + 1];
            sum2 += a[i + 2] * b[i + 2];
            sum3 += a[i + 3] * b[i + 3];
            i += 4;
        }

        let mut sum = sum0 + sum1 + sum2 + sum3;
        while i < len {
            sum += a[i] * b[i];
            i += 1;
        }

        -sum
    }

    fn push_top_result(
        top_results: &mut BinaryHeap<ScoredCandidate>,
        candidate: ScoredCandidate,
        capacity: usize,
    ) {
        if capacity == 0 {
            return;
        }

        if top_results.len() < capacity {
            top_results.push(candidate);
            return;
        }

        let worst = top_results.peek().expect("heap is non-empty");
        if candidate.distance < worst.distance {
            top_results.pop();
            top_results.push(candidate);
        }
    }

    fn push_bound_candidate(
        reservoir: &mut BinaryHeap<ScoredCandidate>,
        candidate: ScoredCandidate,
        capacity: usize,
    ) -> bool {
        if capacity == 0 {
            return false;
        }

        if reservoir.len() < capacity {
            reservoir.push(candidate);
            return true;
        }

        let worst = reservoir.peek().expect("heap is non-empty");
        if candidate.distance < worst.distance {
            reservoir.pop();
            reservoir.push(candidate);
            true
        } else {
            false
        }
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
    fn should_remove_centroid_and_exclude_it_from_search() {
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

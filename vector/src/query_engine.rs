use crate::error::{Error, Result};
use crate::hnsw::CentroidGraph;
use crate::model::{FieldSelection, Filter, Query, SearchResult};
use crate::serde::collection_meta::DistanceMetric;
use crate::serde::posting_list::PostingList;
use crate::serde::vector_data::VectorDataValue;
use crate::storage::VectorDbStorageReadExt;
use crate::{Attribute, Vector, distance};
use common::storage::StorageRead;
use roaring::RoaringTreemap;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;
use tracing::debug;

/// The subset of configuration needed by the query engine.
#[derive(Debug, Clone)]
pub(crate) struct QueryEngineOptions {
    pub(crate) dimensions: u16,
    pub(crate) distance_metric: DistanceMetric,
    pub(crate) query_pruning_factor: Option<f32>,
}

/// Read-only query engine for the vector database.
///
/// `QueryEngine` holds the immutable state needed to execute search queries:
/// a centroid graph for ANN routing, a storage reader for reading posting
/// lists and vector data, and the options for dimensions/distance metric.
///
/// Each query on `VectorDb` creates a short-lived `QueryEngine` from the current
/// snapshot. `QueryEngine` is also used by `VectorDbReader` for read-only access.
pub(crate) struct QueryEngine {
    options: QueryEngineOptions,
    centroid_graph: Arc<dyn CentroidGraph>,
    storage: Arc<dyn StorageRead>,
}

impl QueryEngine {
    pub(crate) fn new(
        options: QueryEngineOptions,
        centroid_graph: Arc<dyn CentroidGraph>,
        storage: Arc<dyn StorageRead>,
    ) -> Self {
        Self {
            options,
            centroid_graph,
            storage,
        }
    }

    pub(crate) async fn get(&self, id: &str) -> Result<Option<Vector>> {
        // 1. Lookup internal ID from IdDictionary
        let Some(internal_id) = self.storage.lookup_internal_id(id).await? else {
            return Ok(None);
        };

        // 2. Load vector data
        //
        // No deletion check needed: the IdDictionary always points to the latest
        // internal ID. On upsert, the old VectorData is hard-deleted and the dictionary
        // is updated to the new internal ID. The deletions bitmap is only used to filter
        // stale posting list entries during search.
        let Some(vector_data) = self
            .storage
            .get_vector_data(internal_id, self.options.dimensions as usize)
            .await?
        else {
            return Ok(None);
        };

        Ok(Some(Self::vector_data_to_vector(&vector_data)))
    }

    fn vector_data_to_vector(vector_data: &VectorDataValue) -> Vector {
        let attributes = vector_data
            .fields()
            .map(|field| Attribute::new(field.field_name.clone(), field.value.clone().into()))
            .collect();
        Vector {
            id: vector_data.external_id().to_string(),
            attributes,
        }
    }

    pub(crate) async fn search(&self, query: &Query) -> Result<Vec<SearchResult>> {
        let nprobe = query.limit.clamp(10, 100);
        self.search_with_nprobe(query, nprobe).await
    }

    pub(crate) async fn search_exact_nprobe(
        &self,
        query: &Query,
        nprobe: usize,
    ) -> Result<Vec<SearchResult>> {
        if query.vector.len() != self.options.dimensions as usize {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.options.dimensions,
                query.vector.len()
            )));
        }

        // Brute-force: compute distance from query to every live centroid
        let all_centroid_ids = self.centroid_graph.all_centroid_ids();
        let mut scored: Vec<(u64, distance::VectorDistance)> = all_centroid_ids
            .iter()
            .filter_map(|&cid| {
                let cv = self.centroid_graph.get_centroid_vector(cid)?;
                let d =
                    distance::compute_distance(&query.vector, &cv, self.options.distance_metric);
                Some((cid, d))
            })
            .collect();
        scored.sort_by(|a, b| a.1.cmp(&b.1));
        let centroid_ids: Vec<u64> = scored.iter().take(nprobe).map(|(id, _)| *id).collect();

        if centroid_ids.is_empty() {
            return Ok(Vec::new());
        }

        let centroid_ids = self.prune_centroids(&centroid_ids, &query.vector);

        let mut sorted_lists = self.load_and_score(&centroid_ids, &query.vector).await?;
        if sorted_lists.is_empty() {
            return Ok(Vec::new());
        }

        // Apply metadata filter
        if let Some(filter) = &query.filter {
            Self::apply_filter(&mut sorted_lists, filter, self.storage.as_ref()).await?;
        }

        let mut results = self.resolve_top_k(sorted_lists, query.limit).await?;
        Self::apply_field_selection(&mut results, &query.include_fields);
        Ok(results)
    }

    pub(crate) async fn search_with_nprobe(
        &self,
        query: &Query,
        nprobe: usize,
    ) -> Result<Vec<SearchResult>> {
        // 1. Validate query dimensions
        if query.vector.len() != self.options.dimensions as usize {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.options.dimensions,
                query.vector.len()
            )));
        }

        // 2. Search HNSW for nearest centroids
        let num_centroids = nprobe;
        let centroid_ids = self.centroid_graph.search(&query.vector, num_centroids);
        debug!(
            "searched for {} centroids, found: {}",
            num_centroids,
            centroid_ids.len()
        );

        if centroid_ids.is_empty() {
            return Ok(Vec::new());
        }

        // 3. Dynamic pruning: skip posting lists whose centroids are far from query
        let original_ncentroids = centroid_ids.len();
        let centroid_ids = self.prune_centroids(&centroid_ids, &query.vector);
        debug!(
            "query: {:?}, before pruning: {} centroids, after dynamic pruning: {} centroids",
            query.vector,
            original_ncentroids,
            centroid_ids.len()
        );

        // 4. Load posting lists and score candidates
        let mut sorted_lists = self.load_and_score(&centroid_ids, &query.vector).await?;

        if sorted_lists.is_empty() {
            return Ok(Vec::new());
        }

        // 5. Apply metadata filter (if provided)
        if let Some(filter) = &query.filter {
            Self::apply_filter(&mut sorted_lists, filter, self.storage.as_ref()).await?;
        }

        // 6. K-way merge and resolve top-k forward index lookups
        let mut results = self.resolve_top_k(sorted_lists, query.limit).await?;
        Self::apply_field_selection(&mut results, &query.include_fields);
        Ok(results)
    }

    /// Apply query-aware dynamic pruning
    /// (SPANN §3.2: https://arxiv.org/pdf/2111.08566).
    ///
    /// Given candidate centroid IDs (already sorted closest-first), computes
    /// the raw distance from the query to each centroid and keeps only those
    /// satisfying `dist(q, c) <= (1 + epsilon) * dist(q, closest)`.
    ///
    /// Returns the pruned centroid IDs. If pruning is disabled (`None`), returns
    /// the input unchanged.
    pub(crate) fn prune_centroids(&self, centroid_ids: &[u64], query: &[f32]) -> Vec<u64> {
        let epsilon = match self.options.query_pruning_factor {
            Some(e) => e,
            None => return centroid_ids.to_vec(),
        };

        let metric = self.options.distance_metric;

        // Compute raw distance (lower = closer) from query to each centroid.
        let mut scored: Vec<(u64, f32)> = centroid_ids
            .iter()
            .filter_map(|&cid| {
                let cv = self.centroid_graph.get_centroid_vector(cid)?;
                let d = distance::raw_distance(query, &cv, metric);
                Some((cid, d))
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        if scored.is_empty() {
            return Vec::new();
        }

        let closest_dist = scored[0].1;
        // Skip pruning when closest distance is non-positive (can happen with
        // DotProduct metric) since the multiplicative threshold is meaningless.
        if closest_dist < 0.0 {
            return scored.into_iter().map(|(id, _)| id).collect();
        }

        let threshold = match metric {
            // raw_distance uses squared L2, so preserve epsilon semantics in
            // Euclidean space by squaring the multiplicative factor.
            DistanceMetric::L2 => (1.0 + epsilon).powi(2) * closest_dist,
            DistanceMetric::DotProduct => (1.0 + epsilon) * closest_dist,
        };
        scored
            .into_iter()
            .take_while(|&(_, d)| d <= threshold)
            .map(|(id, _)| id)
            .collect()
    }

    /// Spawn a task per centroid to load its posting list and score all candidates
    /// against the query vector. Returns per-centroid sorted candidate lists.
    async fn load_and_score(
        &self,
        centroid_ids: &[u64],
        query: &[f32],
    ) -> Result<Vec<Vec<ScoredCandidate>>> {
        let dimensions = self.options.dimensions as usize;
        let metric = self.options.distance_metric;
        let query_vec: Vec<f32> = query.to_vec();

        let mut handles = Vec::with_capacity(centroid_ids.len());
        for &cid in centroid_ids {
            let snap = self.storage.clone();
            let q = query_vec.clone();
            handles.push(tokio::spawn(async move {
                let posting_list: PostingList =
                    snap.get_posting_list(cid, dimensions).await?.into();
                let mut scored: Vec<ScoredCandidate> = posting_list
                    .iter()
                    .map(|posting| {
                        let d = distance::compute_distance(&q, posting.vector(), metric);
                        ScoredCandidate {
                            internal_id: posting.id(),
                            distance: d,
                        }
                    })
                    .collect();
                scored.sort_unstable_by(|a, b| a.distance.cmp(&b.distance));
                Ok::<_, Error>(scored)
            }));
        }

        let results = futures::future::join_all(handles).await;
        let mut sorted_lists: Vec<Vec<ScoredCandidate>> = Vec::with_capacity(results.len());
        for result in results {
            let scored =
                result.map_err(|e| Error::Internal(format!("task join error: {}", e)))??;
            if !scored.is_empty() {
                sorted_lists.push(scored);
            }
        }

        Ok(sorted_lists)
    }

    /// K-way merge the per-centroid sorted lists and resolve top-k forward
    /// index lookups. Only merges as far into the lists as needed to produce
    /// k results, deduplicating by `internal_id` along the way.
    async fn resolve_top_k(
        &self,
        sorted_lists: Vec<Vec<ScoredCandidate>>,
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        let dimensions = self.options.dimensions as usize;

        // Seed the min-heap with the first element of each sorted list.
        let mut heap = BinaryHeap::new();
        for list in sorted_lists {
            let mut iter = list.into_iter();
            if let Some(first) = iter.next() {
                heap.push(Reverse(MergeEntry(first, iter)));
            }
        }

        let mut results = Vec::with_capacity(k);
        let mut seen = HashSet::new();

        // Pop candidates from the heap in score order, batch-resolve k at a
        // time, and stop as soon as we have k results.
        loop {
            // Drain up to k unique candidates from the merge heap.
            let mut batch = Vec::with_capacity(k - results.len());
            while batch.len() < k - results.len() {
                let Some(Reverse(MergeEntry(candidate, mut iter))) = heap.pop() else {
                    break;
                };
                if let Some(next) = iter.next() {
                    heap.push(Reverse(MergeEntry(next, iter)));
                }
                if seen.insert(candidate.internal_id) {
                    batch.push(candidate);
                }
            }

            if batch.is_empty() {
                break;
            }

            // Resolve forward index lookups for the batch concurrently.
            let futures: Vec<_> = batch
                .iter()
                .map(|sr| self.storage.get_vector_data(sr.internal_id, dimensions))
                .collect();
            let loaded = futures::future::join_all(futures).await;

            for (sr, vector_data) in batch.iter().zip(loaded) {
                let Some(vector_data) = vector_data? else {
                    continue;
                };
                results.push(SearchResult {
                    score: sr.distance.score(),
                    vector: Self::vector_data_to_vector(&vector_data),
                });

                if results.len() == k {
                    return Ok(results);
                }
            }
        }

        Ok(results)
    }

    /// Apply field projection to search results.
    fn apply_field_selection(results: &mut [SearchResult], selection: &FieldSelection) {
        match selection {
            FieldSelection::All => {}
            FieldSelection::None => {
                for result in results.iter_mut() {
                    result.vector.attributes.clear();
                }
            }
            FieldSelection::Fields(names) => {
                for result in results.iter_mut() {
                    result
                        .vector
                        .attributes
                        .retain(|attr| names.contains(&attr.name));
                }
            }
        }
    }

    /// Apply a metadata filter to scored candidate lists.
    ///
    /// Collects all candidate IDs into a bitmap, evaluates the filter against
    /// the inverted index, then retains only candidates that pass the filter.
    async fn apply_filter(
        sorted_lists: &mut [Vec<ScoredCandidate>],
        filter: &Filter,
        storage: &dyn StorageRead,
    ) -> Result<()> {
        // Collect all candidate internal IDs
        let mut candidates = RoaringTreemap::new();
        for list in sorted_lists.iter() {
            for candidate in list {
                candidates.insert(candidate.internal_id);
            }
        }

        // Evaluate filter to get allowed IDs
        let allowed = Self::evaluate_filter(filter, &candidates, storage).await?;

        // Filter each list
        for list in sorted_lists.iter_mut() {
            list.retain(|c| allowed.contains(c.internal_id));
        }

        Ok(())
    }

    /// Recursively evaluate a filter against the metadata inverted index.
    ///
    /// Returns a bitmap of vector IDs that match the filter.
    /// For Neq, the `candidates` bitmap is used as the universe for complement.
    fn evaluate_filter<'a>(
        filter: &'a Filter,
        candidates: &'a RoaringTreemap,
        storage: &'a dyn StorageRead,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<RoaringTreemap>> + Send + 'a>>
    {
        Box::pin(async move {
            match filter {
                Filter::Eq(field, value) => {
                    let field_value: crate::serde::FieldValue = value.clone().into();
                    let bitmap = storage.get_metadata_index(field, field_value).await?;
                    Ok(bitmap.vector_ids)
                }
                Filter::Neq(field, value) => {
                    let field_value: crate::serde::FieldValue = value.clone().into();
                    let bitmap = storage.get_metadata_index(field, field_value).await?;
                    let mut result = candidates.clone();
                    result -= &bitmap.vector_ids;
                    Ok(result)
                }
                Filter::In(field, values) => {
                    let mut result = RoaringTreemap::new();
                    for value in values {
                        let field_value: crate::serde::FieldValue = value.clone().into();
                        let bitmap = storage.get_metadata_index(field, field_value).await?;
                        result |= &bitmap.vector_ids;
                    }
                    Ok(result)
                }
                Filter::And(filters) => {
                    if filters.is_empty() {
                        return Ok(candidates.clone());
                    }
                    let mut result =
                        Self::evaluate_filter(&filters[0], candidates, storage).await?;
                    for f in &filters[1..] {
                        let sub = Self::evaluate_filter(f, candidates, storage).await?;
                        result &= &sub;
                    }
                    Ok(result)
                }
                Filter::Or(filters) => {
                    let mut result = RoaringTreemap::new();
                    for f in filters {
                        let sub = Self::evaluate_filter(f, candidates, storage).await?;
                        result |= &sub;
                    }
                    Ok(result)
                }
            }
        })
    }
}

struct ScoredCandidate {
    internal_id: u64,
    distance: distance::VectorDistance,
}

/// Entry for k-way merge of sorted scored candidate lists.
struct MergeEntry(ScoredCandidate, std::vec::IntoIter<ScoredCandidate>);

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.distance == other.0.distance
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.distance.cmp(&other.0.distance)
    }
}

#[cfg(test)]
mod tests {
    use crate::AttributeValue;
    use crate::db::{VectorDb, VectorDbRead};
    use crate::model::{Config, Query, VECTOR_FIELD_NAME, Vector};
    use crate::serde::collection_meta::DistanceMetric;
    use common::{StorageBuilder, StorageConfig};

    fn create_config(dimensions: u16, metric: DistanceMetric) -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions,
            distance_metric: metric,
            metadata_fields: vec![],
            ..Default::default()
        }
    }

    // --- Prune tests ---

    #[tokio::test]
    async fn should_prune_centroids_beyond_epsilon_threshold() {
        // given - 4 centroids at known L2 distances from query [0,0,0]:
        //   c0 = [1,0,0]  -> dist = 1.0
        //   c1 = [1.4,0,0] -> dist = 1.4
        //   c2 = [2,0,0]  -> dist = 2.0
        //   c3 = [5,0,0]  -> dist = 5.0
        // epsilon = 0.5 => threshold = 1.5 * 1.0 = 1.5
        // Expected: c0 (1.0) and c1 (1.4) survive; c2 (2.0) and c3 (5.0) pruned.
        let config = Config {
            query_pruning_factor: Some(0.5),
            ..create_config(3, DistanceMetric::L2)
        };
        let centroids = vec![
            vec![1.0, 0.0, 0.0],
            vec![1.4, 0.0, 0.0],
            vec![2.0, 0.0, 0.0],
            vec![5.0, 0.0, 0.0],
        ];
        let db = {
            let sb = StorageBuilder::new(&config.storage).await.unwrap();
            VectorDb::open_with_centroids(config, centroids, sb)
        }
        .await
        .unwrap();

        let query = [0.0, 0.0, 0.0];
        let all_ids: Vec<u64> = (0..4).collect();

        // when
        let pruned = db.query_engine().prune_centroids(&all_ids, &query);

        // then
        assert_eq!(pruned.len(), 2);
        assert_eq!(pruned[0], 0); // closest
        assert_eq!(pruned[1], 1); // within threshold
    }

    #[tokio::test]
    async fn should_skip_pruning_when_epsilon_is_none() {
        // given - no pruning configured
        let config = create_config(3, DistanceMetric::L2);
        let centroids = vec![vec![1.0, 0.0, 0.0], vec![100.0, 0.0, 0.0]];
        let db = {
            let sb = StorageBuilder::new(&config.storage).await.unwrap();
            VectorDb::open_with_centroids(config, centroids, sb)
        }
        .await
        .unwrap();

        let query = [0.0, 0.0, 0.0];
        let all_ids: Vec<u64> = (0..2).collect();

        // when
        let result = db.query_engine().prune_centroids(&all_ids, &query);

        // then - all centroids returned regardless of distance
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn should_prune_centroids_returns_sorted_by_distance() {
        // given - pass centroid IDs in non-distance order
        let config = Config {
            query_pruning_factor: Some(10.0), // large epsilon, keeps all
            ..create_config(3, DistanceMetric::L2)
        };
        let centroids = vec![
            vec![5.0, 0.0, 0.0], // c0: far
            vec![1.0, 0.0, 0.0], // c1: close
            vec![3.0, 0.0, 0.0], // c2: medium
        ];
        let db = {
            let sb = StorageBuilder::new(&config.storage).await.unwrap();
            VectorDb::open_with_centroids(config, centroids, sb)
        }
        .await
        .unwrap();

        let query = [0.0, 0.0, 0.0];
        // pass IDs in reverse distance order
        let ids: Vec<u64> = vec![0, 2, 1];

        // when
        let result = db.query_engine().prune_centroids(&ids, &query);

        // then - sorted by ascending distance: c1 (1.0), c2 (3.0), c0 (5.0)
        assert_eq!(result, vec![1, 2, 0]);
    }

    // --- Search tests ---

    #[tokio::test]
    async fn should_query_vectors() {
        // given - 4 clusters of 25 vectors each with 128 dimensions
        let config = create_config(128, DistanceMetric::L2);
        let cluster_centers = [
            vec![1.0; 128],  // Cluster 0: all ones
            vec![-1.0; 128], // Cluster 1: all negative ones
            {
                let mut v = vec![0.0; 128];
                v[0] = 10.0;
                v
            }, // Cluster 2: sparse
            {
                let mut v = vec![0.0; 128];
                for i in (0..128).step_by(2) {
                    v[i] = 1.0;
                }
                v
            }, // Cluster 3: alternating
        ];

        let centroids: Vec<Vec<f32>> = cluster_centers.to_vec();
        let db = {
            let sb = StorageBuilder::new(&config.storage).await.unwrap();
            VectorDb::open_with_centroids(config, centroids, sb)
        }
        .await
        .unwrap();

        let mut all_vectors = Vec::new();
        for (cluster_id, center) in cluster_centers.iter().enumerate() {
            for i in 0..25 {
                let mut v = center.clone();
                v[0] += (i as f32) * 0.01;
                all_vectors.push(Vector::new(format!("vec-{}-{}", cluster_id, i), v));
            }
        }
        db.write(all_vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - search for vector similar to cluster 0
        let q = Query::new(vec![1.0; 128]).with_limit(10);
        let results = db.query_engine().search(&q).await.unwrap();

        // then - should find vectors from cluster 0
        assert_eq!(results.len(), 10);
        for result in &results {
            assert!(
                result.vector.id.starts_with("vec-0-"),
                "Expected cluster 0 vector, got: {}",
                result.vector.id
            );
        }
        // Verify results are sorted by score (L2 distance, lower = better)
        for i in 1..results.len() {
            assert!(
                results[i - 1].score <= results[i].score,
                "Results not sorted by score"
            );
        }
    }

    #[tokio::test]
    async fn should_handle_deleted_vectors_in_search() {
        // given - create database with centroids
        let config = create_config(3, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        // Write initial vector
        let vector1 = Vector::new("vec-1", vec![1.0, 0.0, 0.0]);
        db.write(vec![vector1]).await.unwrap();
        db.flush().await.unwrap();

        // when - upsert the same vector (old version should be marked as deleted)
        let vector2 = Vector::new("vec-1", vec![0.9, 0.1, 0.0]);
        db.write(vec![vector2]).await.unwrap();
        db.flush().await.unwrap();

        // when - search
        let results = db
            .query_engine()
            .search(&Query::new(vec![1.0, 0.0, 0.0]).with_limit(10))
            .await
            .unwrap();

        // then - should only return the new version (not the deleted one)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].vector.id, "vec-1");
        let vector = results[0]
            .vector
            .attributes
            .iter()
            .find(|f| f.name == "vector")
            .unwrap()
            .value
            .clone();
        let crate::model::AttributeValue::Vector(vector) = vector.clone() else {
            panic!("unexpected attr type");
        };
        assert_eq!(vector, vec![0.9, 0.1, 0.0]);
    }

    #[tokio::test]
    async fn should_search_with_l2_distance_metric() {
        // given - create database with L2 metric
        let config = create_config(3, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        let vectors = vec![
            Vector::new("close", vec![0.1, 0.1, 0.1]),
            Vector::new("medium", vec![1.0, 1.0, 1.0]),
            Vector::new("far", vec![5.0, 5.0, 5.0]),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - search for vectors near origin
        let results = db
            .query_engine()
            .search(&Query::new(vec![0.0, 0.0, 0.0]).with_limit(3))
            .await
            .unwrap();

        // then - should be sorted by L2 distance (lower = better)
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].vector.id, "close");
        assert_eq!(results[1].vector.id, "medium");
        assert_eq!(results[2].vector.id, "far");

        // Verify scores are increasing (L2 distance)
        for i in 1..results.len() {
            assert!(
                results[i - 1].score <= results[i].score,
                "L2 distances not sorted correctly"
            );
        }
    }

    #[tokio::test]
    async fn should_reject_query_with_wrong_dimensions() {
        // given - database with 3 dimensions
        let config = create_config(3, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        // when - query with wrong dimensions
        let result = db
            .query_engine()
            .search(&Query::new(vec![1.0, 2.0]).with_limit(10))
            .await;

        // then - should fail
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Query dimension mismatch")
        );
    }

    #[tokio::test]
    async fn should_return_empty_results_when_no_vectors() {
        // given - database with centroids but no vectors
        let config = create_config(3, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        // when
        let results = db
            .query_engine()
            .search(&Query::new(vec![1.0, 0.0, 0.0]).with_limit(10))
            .await
            .unwrap();

        // then
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn should_limit_results_to_k() {
        // given - database with many vectors
        let config = create_config(2, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        let vectors: Vec<Vector> = (0..20)
            .map(|i| Vector::new(format!("vec-{}", i), vec![1.0 + (i as f32) * 0.01, 0.0]))
            .collect();
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - search for k=5
        let results = db
            .query_engine()
            .search(&Query::new(vec![1.0, 0.0]).with_limit(5))
            .await
            .unwrap();

        // then
        assert_eq!(results.len(), 5);
    }

    // --- Filter tests ---

    use crate::model::{Filter, MetadataFieldSpec};
    use crate::serde::FieldType;

    fn create_filterable_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            metadata_fields: vec![
                MetadataFieldSpec::new("category", FieldType::String, true),
                MetadataFieldSpec::new("price", FieldType::Int64, true),
                MetadataFieldSpec::new("active", FieldType::Bool, true),
            ],
            ..Default::default()
        }
    }

    async fn setup_filterable_db() -> VectorDb {
        let config = create_filterable_config();
        let db = VectorDb::open(config).await.unwrap();

        let vectors = vec![
            Vector::builder("shoes-1", vec![1.0, 0.0, 0.0])
                .attribute("category", "shoes")
                .attribute("price", 50i64)
                .attribute("active", true)
                .build(),
            Vector::builder("shoes-2", vec![0.9, 0.1, 0.0])
                .attribute("category", "shoes")
                .attribute("price", 100i64)
                .attribute("active", false)
                .build(),
            Vector::builder("boots-1", vec![0.0, 1.0, 0.0])
                .attribute("category", "boots")
                .attribute("price", 150i64)
                .attribute("active", true)
                .build(),
            Vector::builder("hats-1", vec![0.0, 0.0, 1.0])
                .attribute("category", "hats")
                .attribute("price", 25i64)
                .attribute("active", true)
                .build(),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();
        db
    }

    /// Collect result IDs into a sorted Vec for deterministic comparison.
    fn result_ids(results: &[crate::model::SearchResult]) -> Vec<String> {
        let mut ids: Vec<String> = results.iter().map(|r| r.vector.id.clone()).collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn should_filter_with_eq() {
        let db = setup_filterable_db().await;

        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(10)
            .with_filter(Filter::eq("category", "shoes"));
        let results = db.search(&q).await.unwrap();

        assert_eq!(result_ids(&results), vec!["shoes-1", "shoes-2"]);
    }

    #[tokio::test]
    async fn should_filter_with_neq() {
        let db = setup_filterable_db().await;

        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(10)
            .with_filter(Filter::neq("category", "shoes"));
        let results = db.search(&q).await.unwrap();

        assert_eq!(result_ids(&results), vec!["boots-1", "hats-1"]);
    }

    #[tokio::test]
    async fn should_filter_with_in() {
        let db = setup_filterable_db().await;

        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(10)
            .with_filter(Filter::in_set(
                "category",
                vec!["shoes".into(), "boots".into()],
            ));
        let results = db.search(&q).await.unwrap();

        assert_eq!(result_ids(&results), vec!["boots-1", "shoes-1", "shoes-2"]);
    }

    #[tokio::test]
    async fn should_filter_with_and() {
        let db = setup_filterable_db().await;

        // category=shoes AND active=true -> only shoes-1
        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(10)
            .with_filter(Filter::and(vec![
                Filter::eq("category", "shoes"),
                Filter::eq("active", true),
            ]));
        let results = db.search(&q).await.unwrap();

        assert_eq!(result_ids(&results), vec!["shoes-1"]);
    }

    #[tokio::test]
    async fn should_filter_with_or() {
        let db = setup_filterable_db().await;

        // category=hats OR category=boots -> boots-1 and hats-1
        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(10)
            .with_filter(Filter::or(vec![
                Filter::eq("category", "hats"),
                Filter::eq("category", "boots"),
            ]));
        let results = db.search(&q).await.unwrap();

        assert_eq!(result_ids(&results), vec!["boots-1", "hats-1"]);
    }

    #[tokio::test]
    async fn should_filter_with_int_eq() {
        let db = setup_filterable_db().await;

        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(10)
            .with_filter(Filter::eq("price", 50i64));
        let results = db.search(&q).await.unwrap();

        assert_eq!(result_ids(&results), vec!["shoes-1"]);
    }

    #[tokio::test]
    async fn should_return_empty_when_filter_matches_nothing() {
        let db = setup_filterable_db().await;

        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(10)
            .with_filter(Filter::eq("category", "sandals"));
        let results = db.search(&q).await.unwrap();

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn should_search_without_filter() {
        let db = setup_filterable_db().await;

        // No filter - should return all 4 vectors
        let q = Query::new(vec![1.0, 0.0, 0.0]).with_limit(10);
        let results = db.search(&q).await.unwrap();

        assert_eq!(
            result_ids(&results),
            vec!["boots-1", "hats-1", "shoes-1", "shoes-2"]
        );
    }

    #[tokio::test]
    async fn should_filter_with_nested_and_or() {
        let db = setup_filterable_db().await;

        // (category=shoes AND active=true) OR category=hats -> shoes-1 and hats-1
        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(10)
            .with_filter(Filter::or(vec![
                Filter::and(vec![
                    Filter::eq("category", "shoes"),
                    Filter::eq("active", true),
                ]),
                Filter::eq("category", "hats"),
            ]));
        let results = db.search(&q).await.unwrap();

        assert_eq!(result_ids(&results), vec!["hats-1", "shoes-1"]);
    }

    #[tokio::test]
    async fn should_respect_limit_with_filter() {
        let db = setup_filterable_db().await;

        // Filter matches 3 results but limit is 1
        let q = Query::new(vec![1.0, 0.0, 0.0])
            .with_limit(1)
            .with_filter(Filter::neq("category", "hats"));
        let results = db.search(&q).await.unwrap();

        assert_eq!(results.len(), 1);
        // Closest to [1,0,0] among shoes-1, shoes-2, boots-1 is shoes-1
        assert_eq!(results[0].vector.id, "shoes-1");
    }

    // --- Search tests ---

    #[tokio::test]
    async fn should_search_across_multiple_centroids() {
        // given - database with 3 centroids
        let config = create_config(2, DistanceMetric::L2);
        let centroids = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]];
        let db = {
            let sb = StorageBuilder::new(&config.storage).await.unwrap();
            VectorDb::open_with_centroids(config, centroids, sb)
        }
        .await
        .unwrap();

        let vectors = vec![
            Vector::new("c1-1", vec![0.9, 0.0]),
            Vector::new("c1-2", vec![1.1, 0.0]),
            Vector::new("c2-1", vec![0.0, 0.9]),
            Vector::new("c2-2", vec![0.0, 1.1]),
            Vector::new("c3-1", vec![-0.9, 0.0]),
            Vector::new("c3-2", vec![-1.1, 0.0]),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - search in between centroids 1 and 2
        let results = db
            .query_engine()
            .search(&Query::new(vec![0.7, 0.7]).with_limit(10))
            .await
            .unwrap();

        // then - should find vectors from multiple centroids
        assert!(!results.is_empty());
    }

    #[tokio::test]
    async fn should_get_vector_by_id() {
        // given
        let config = create_config(3, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        let vectors = vec![
            Vector::builder("vec-1", vec![1.0, 0.0, 0.0])
                .attribute("category", "shoes")
                .attribute("price", 99i64)
                .build(),
            Vector::builder("vec-2", vec![0.0, 1.0, 0.0])
                .attribute("category", "boots")
                .attribute("price", 149i64)
                .build(),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when
        let record = db.get("vec-1").await.unwrap();

        // then
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.id, "vec-1");
        assert_eq!(
            record.attribute("category"),
            Some(&AttributeValue::String("shoes".to_string()))
        );
        assert_eq!(record.attribute("price"), Some(&AttributeValue::Int64(99)));
        match record.attribute(VECTOR_FIELD_NAME) {
            Some(AttributeValue::Vector(v)) => assert_eq!(v, &[1.0, 0.0, 0.0]),
            other => panic!("expected vector field, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_return_none_for_nonexistent_id() {
        // given
        let config = create_config(3, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        // when
        let record = db.get("does-not-exist").await.unwrap();

        // then
        assert!(record.is_none());
    }

    #[tokio::test]
    async fn should_return_none_for_deleted_vector() {
        // given - write a vector, then upsert it (old version becomes deleted)
        let config = create_config(3, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        let vector1 = Vector::builder("vec-1", vec![1.0, 0.0, 0.0])
            .attribute("category", "shoes")
            .attribute("price", 99i64)
            .build();
        db.write(vec![vector1]).await.unwrap();
        db.flush().await.unwrap();

        // when - upsert with new values
        let vector2 = Vector::builder("vec-1", vec![0.0, 1.0, 0.0])
            .attribute("category", "boots")
            .attribute("price", 199i64)
            .build();
        db.write(vec![vector2]).await.unwrap();
        db.flush().await.unwrap();

        // then - get should return the updated version
        let record = db.get("vec-1").await.unwrap();
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.id, "vec-1");
        assert_eq!(
            record.attribute("category"),
            Some(&AttributeValue::String("boots".to_string()))
        );
        assert_eq!(record.attribute("price"), Some(&AttributeValue::Int64(199)));
        match record.attribute(VECTOR_FIELD_NAME) {
            Some(AttributeValue::Vector(v)) => assert_eq!(v, &[0.0, 1.0, 0.0]),
            other => panic!("expected vector field, got: {:?}", other),
        }
    }
}

use crate::error::{Error, Result};
use crate::hnsw::CentroidGraph;
use crate::model::{AttributeValue, SearchResult};
use crate::serde::collection_meta::DistanceMetric;
use crate::serde::posting_list::PostingList;
use crate::storage::VectorDbStorageReadExt;
use crate::{Attribute, Vector, distance};
use common::storage::StorageRead;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
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

        // 3. Construct VectorRecord with all fields
        let attributes = vector_data
            .fields()
            .map(|field| Attribute::new(field.field_name.clone(), field.value.clone().into()))
            .collect();

        Ok(Some(Vector {
            id: vector_data.external_id().to_string(),
            attributes,
        }))
    }

    pub(crate) async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        let nprobe = k.clamp(10, 100);
        self.search_with_nprobe(query, k, nprobe).await
    }

    pub(crate) async fn search_exact_nprobe(
        &self,
        query: &[f32],
        k: usize,
        nprobe: usize,
    ) -> Result<Vec<SearchResult>> {
        if query.len() != self.options.dimensions as usize {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.options.dimensions,
                query.len()
            )));
        }

        // Brute-force: compute distance from query to every centroid
        let num_centroids = self.centroid_graph.len();
        let all_centroid_ids = self.centroid_graph.search(query, num_centroids);
        let mut scored: Vec<(u64, distance::VectorDistance)> = all_centroid_ids
            .iter()
            .filter_map(|&cid| {
                let cv = self.centroid_graph.get_centroid_vector(cid)?;
                let d = distance::compute_distance(query, &cv, self.options.distance_metric);
                Some((cid, d))
            })
            .collect();
        scored.sort_by(|a, b| a.1.cmp(&b.1));
        let centroid_ids: Vec<u64> = scored.iter().take(nprobe).map(|(id, _)| *id).collect();

        if centroid_ids.is_empty() {
            return Ok(Vec::new());
        }

        let centroid_ids = self.prune_centroids(&centroid_ids, query);

        let sorted_lists = self.load_and_score(&centroid_ids, query).await?;
        if sorted_lists.is_empty() {
            return Ok(Vec::new());
        }
        self.resolve_top_k(sorted_lists, k).await
    }

    pub(crate) async fn search_with_nprobe(
        &self,
        query: &[f32],
        k: usize,
        nprobe: usize,
    ) -> Result<Vec<SearchResult>> {
        // 1. Validate query dimensions
        if query.len() != self.options.dimensions as usize {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.options.dimensions,
                query.len()
            )));
        }

        // 2. Search HNSW for nearest centroids
        let num_centroids = nprobe;
        let centroid_ids = self.centroid_graph.search(query, num_centroids);
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
        let centroid_ids = self.prune_centroids(&centroid_ids, query);
        debug!(
            "query: {:?}, before pruning: {} centroids, after dynamic pruning: {} centroids",
            query,
            original_ncentroids,
            centroid_ids.len()
        );

        // 4. Load posting lists and score candidates
        let sorted_lists = self.load_and_score(&centroid_ids, query).await?;

        if sorted_lists.is_empty() {
            return Ok(Vec::new());
        }

        // 5. K-way merge and resolve top-k forward index lookups
        self.resolve_top_k(sorted_lists, k).await
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

        let threshold = (1.0 + epsilon) * closest_dist;
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

                let metadata: HashMap<String, AttributeValue> = vector_data
                    .fields()
                    .map(|field| (field.field_name.clone(), field.value.clone().into()))
                    .collect();

                results.push(SearchResult {
                    internal_id: sr.internal_id,
                    external_id: vector_data.external_id().to_string(),
                    score: sr.distance.score(),
                    attributes: metadata,
                });

                if results.len() == k {
                    return Ok(results);
                }
            }
        }

        Ok(results)
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
    use crate::model::{Config, VECTOR_FIELD_NAME, Vector};
    use crate::serde::collection_meta::DistanceMetric;
    use common::{StorageConfig, StorageRuntime};

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
        let db = VectorDb::open_with_centroids(config, centroids, StorageRuntime::new())
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
        let db = VectorDb::open_with_centroids(config, centroids, StorageRuntime::new())
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
        let db = VectorDb::open_with_centroids(config, centroids, StorageRuntime::new())
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
        let db = VectorDb::open_with_centroids(config, centroids, StorageRuntime::new())
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
        let query = vec![1.0; 128];
        let results = db.query_engine().search(&query, 10).await.unwrap();

        // then - should find vectors from cluster 0
        assert_eq!(results.len(), 10);
        for result in &results {
            assert!(
                result.external_id.starts_with("vec-0-"),
                "Expected cluster 0 vector, got: {}",
                result.external_id
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
            .search(&[1.0, 0.0, 0.0], 10)
            .await
            .unwrap();

        // then - should only return the new version (not the deleted one)
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].external_id, "vec-1");
        let vector = results[0]
            .attributes
            .iter()
            .find(|f| f.0 == "vector")
            .unwrap()
            .1;
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
        let results = db.query_engine().search(&[0.0, 0.0, 0.0], 3).await.unwrap();

        // then - should be sorted by L2 distance (lower = better)
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].external_id, "close");
        assert_eq!(results[1].external_id, "medium");
        assert_eq!(results[2].external_id, "far");

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
        let result = db.query_engine().search(&[1.0, 2.0], 10).await;

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
            .search(&[1.0, 0.0, 0.0], 10)
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
        let results = db.query_engine().search(&[1.0, 0.0], 5).await.unwrap();

        // then
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn should_search_across_multiple_centroids() {
        // given - database with 3 centroids
        let config = create_config(2, DistanceMetric::L2);
        let centroids = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![-1.0, 0.0]];
        let db = VectorDb::open_with_centroids(config, centroids, StorageRuntime::new())
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
        let results = db.query_engine().search(&[0.7, 0.7], 10).await.unwrap();

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

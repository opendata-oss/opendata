mod ann;
mod bm25;
mod collectors;
mod filter;
mod lookup;
mod operator;
mod project;
mod types;

use crate::Vector;
use crate::error::{Error, Result};
use crate::metric_names::QUERY_SEARCH_DURATION_SECONDS;
use crate::model::{Bm25Query, Query, ScoreBy, SearchOptions, SearchResult};
use crate::query_engine::ann::{ANNOperator, ExhaustiveNNOperator};
use crate::query_engine::bm25::BM25Operator;
use crate::query_engine::filter::PreparedFilter;
use crate::query_engine::lookup::LookupOperator;
use crate::query_engine::operator::{BoxedOperator, Operator};
use crate::query_engine::project::ProjectOperator;
use crate::serde::collection_meta::DistanceMetric;
use crate::serde::vector_bitmap::VectorBitmap;
use crate::storage::VectorDbStorageReadExt;
use crate::write::indexer::tree::centroids::LeveledCentroidIndex;
use common::storage::StorageRead;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
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
/// a centroid index for ANN routing, a storage reader for reading posting
/// lists and vector data, and the options for dimensions/distance metric.
///
/// Each query on `VectorDb` creates a short-lived `QueryEngine` from the current
/// snapshot. `QueryEngine` is also used by `VectorDbReader` for read-only access.
pub(crate) struct QueryEngine {
    options: QueryEngineOptions,
    centroid_index: Arc<LeveledCentroidIndex<'static>>,
    storage: Arc<dyn StorageRead>,
    /// FTS deletions bitmap published by the flusher, used by the BM25 query
    /// path to hide deleted/replaced docs without a per-query storage read.
    deletions: Arc<VectorBitmap>,
}

impl QueryEngine {
    pub(crate) fn new(
        options: QueryEngineOptions,
        centroid_index: Arc<LeveledCentroidIndex<'static>>,
        storage: Arc<dyn StorageRead>,
        deletions: Arc<VectorBitmap>,
    ) -> Self {
        Self {
            options,
            centroid_index,
            storage,
            deletions,
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

        Ok(Some(lookup::vector_data_to_vector(&vector_data)))
    }

    pub(crate) async fn search_exact_nprobe(
        &self,
        query: &Query,
        nprobe: usize,
    ) -> Result<Vec<SearchResult>> {
        let ann_vector = match &query.score_by {
            ScoreBy::Ann(v) => v,
            ScoreBy::Bm25(_) => {
                return Err(Error::InvalidInput(
                    "search_exact_nprobe only supports ANN queries".to_string(),
                ));
            }
        };
        let filter = PreparedFilter::build(query.filter.as_ref(), self.storage.as_ref()).await?;
        let source = ExhaustiveNNOperator::new(
            self.storage.clone(),
            self.centroid_index.clone(),
            self.options.clone(),
            ann_vector.clone(),
            filter,
            nprobe,
            query.limit,
        );
        let lookup = LookupOperator::new(
            self.storage.clone(),
            self.options.dimensions as usize,
            Box::new(source),
        );
        let project = ProjectOperator::new(query.include_fields.clone(), Box::new(lookup));
        Ok(project
            .execute()
            .await?
            .into_iter()
            .map(SearchResult::from)
            .collect())
    }

    pub(crate) async fn search_with_options(
        &self,
        query: &Query,
        options: SearchOptions,
    ) -> Result<Vec<SearchResult>> {
        let search_start = Instant::now();
        let result = self.search_with_options_inner(query, options).await;
        metrics::histogram!(QUERY_SEARCH_DURATION_SECONDS)
            .record(search_start.elapsed().as_secs_f64());
        result
    }

    async fn search_with_options_inner(
        &self,
        query: &Query,
        options: SearchOptions,
    ) -> Result<Vec<SearchResult>> {
        let t = Instant::now();
        let plan = self.plan(query, options).await?;
        let results = plan.execute().await?;
        debug!(
            op = "search_with_options",
            elapsed_ms = t.elapsed().as_millis() as u64
        );
        Ok(results)
    }

    async fn plan(
        &self,
        query: &Query,
        options: SearchOptions,
    ) -> Result<BoxedOperator<Vec<SearchResult>>> {
        match &query.score_by {
            ScoreBy::Ann(vector) => self.plan_ann(vector, query, options).await,
            ScoreBy::Bm25(bm25) => self.plan_bm25(bm25, query).await,
        }
    }

    async fn plan_ann(
        &self,
        ann_vector: &[f32],
        query: &Query,
        options: SearchOptions,
    ) -> Result<BoxedOperator<Vec<SearchResult>>> {
        let nprobe = options.nprobe.unwrap_or_else(|| query.limit.clamp(10, 100));
        // Chain: ann -> lookup -> project. The source validates the query,
        // probes the `nprobe` nearest centroids (with dynamic pruning), and
        // ranks the candidates; lookup/project then materialize the results.
        // TODO: defer filter resolution to execution time
        let filter = PreparedFilter::build(query.filter.as_ref(), self.storage.as_ref()).await?;
        let source = ANNOperator::new(
            self.storage.clone(),
            self.centroid_index.clone(),
            self.options.clone(),
            ann_vector.to_vec(),
            filter,
            nprobe,
            query.limit,
        );
        let lookup = LookupOperator::new(
            self.storage.clone(),
            self.options.dimensions as usize,
            Box::new(source),
        );
        let project = ProjectOperator::new(query.include_fields.clone(), Box::new(lookup));
        let plan = LeafOperator::new(project);
        Ok(Box::new(plan))
    }

    /// BM25 search path (RFC-0006 Milestone 0).
    ///
    /// Assembles the operator chain `bm25 -> lookup -> project` and runs it.
    /// The BM25 operator scores and ranks ids (metadata filtering and FTS delete
    /// resolution happen there, before scoring); lookup/project then materialize
    /// the forward-index data and apply field selection.
    async fn plan_bm25(
        &self,
        bm25: &Bm25Query,
        query: &Query,
    ) -> Result<BoxedOperator<Vec<SearchResult>>> {
        // TODO: defer filter resolution to execution time
        let filter = PreparedFilter::build(query.filter.as_ref(), self.storage.as_ref()).await?;
        let source = BM25Operator::new(
            self.storage.clone(),
            bm25.clone(),
            filter,
            self.deletions.clone(),
            query.limit,
        );
        let lookup = LookupOperator::new(
            self.storage.clone(),
            self.options.dimensions as usize,
            Box::new(source),
        );
        let project = ProjectOperator::new(query.include_fields.clone(), Box::new(lookup));
        let plan = LeafOperator::new(project);
        Ok(Box::new(plan))
    }
}

struct LeafOperator<T: Into<SearchResult>, O: Operator<Vec<T>>> {
    child: O,
    phantom: PhantomData<T>,
}

impl<T: Into<SearchResult>, O: Operator<Vec<T>>> LeafOperator<T, O> {
    fn new(child: O) -> Self {
        Self {
            child,
            phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<T: Into<SearchResult> + Send + Sync + 'static, O: Operator<Vec<T>> + Send + Sync + 'static>
    Operator<Vec<SearchResult>> for LeafOperator<T, O>
{
    async fn execute(&self) -> Result<Vec<SearchResult>> {
        let results = self.child.execute().await?;
        Ok(results.into_iter().map(|r| r.into()).collect())
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
        let results = db.search(&q).await.unwrap();

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
        let result = db.search(&Query::new(vec![1.0, 2.0]).with_limit(10)).await;

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

    #[tokio::test]
    async fn should_normalize_vectors_on_write_for_cosine_metric() {
        // given - a database with Cosine metric and non-unit vectors at different angles
        let config = create_config(3, DistanceMetric::Cosine);
        let db = VectorDb::open(config).await.unwrap();

        // Two vectors pointing in the same direction but with different magnitudes
        // should be equidistant from a query in the same direction under cosine.
        let vectors = vec![
            Vector::new("small", vec![1.0, 0.0, 0.0]),
            Vector::new("large", vec![100.0, 0.0, 0.0]),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - search for a vector in the same direction
        let results = db
            .search(&Query::new(vec![5.0, 0.0, 0.0]).with_limit(2))
            .await
            .unwrap();

        // then - both vectors should have the same score (cosine distance = 0)
        assert_eq!(results.len(), 2);
        assert!(
            (results[0].score - results[1].score).abs() < 1e-6,
            "Cosine distances should be equal for same-direction vectors: {} vs {}",
            results[0].score,
            results[1].score
        );

        let record = db.get("large").await.unwrap().unwrap();

        // stored "large" vector should be unchanged
        match record.attribute(VECTOR_FIELD_NAME) {
            Some(AttributeValue::Vector(v)) => assert_eq!(v, &[100.0, 0.0, 0.0]),
            other => panic!("expected vector field, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_not_normalize_vectors_on_write_for_l2_metric() {
        // given - a database with L2 metric and a non-unit vector
        let config = create_config(3, DistanceMetric::L2);
        let db = VectorDb::open(config).await.unwrap();

        let vector = Vector::new("vec", vec![3.0, 4.0, 0.0]);
        db.write(vec![vector]).await.unwrap();
        db.flush().await.unwrap();

        // when
        let record = db.get("vec").await.unwrap().unwrap();

        // then - stored vector should be unchanged
        match record.attribute(VECTOR_FIELD_NAME) {
            Some(AttributeValue::Vector(v)) => assert_eq!(v, &[3.0, 4.0, 0.0]),
            other => panic!("expected vector field, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn should_search_cosine_with_unnormalized_query() {
        // given - Cosine database with normalized stored vectors
        let config = create_config(3, DistanceMetric::Cosine);
        let db = VectorDb::open(config).await.unwrap();

        // Vectors pointing in different directions (will be normalized on write)
        let vectors = vec![
            Vector::new("along-x", vec![10.0, 0.0, 0.0]),
            Vector::new("along-y", vec![0.0, 10.0, 0.0]),
            Vector::new("along-z", vec![0.0, 0.0, 10.0]),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - query with an unnormalized vector pointing mostly along x
        let results = db
            .search(&Query::new(vec![100.0, 1.0, 0.0]).with_limit(3))
            .await
            .unwrap();

        // then - "along-x" should be most similar (smallest angle)
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].vector.id, "along-x");
        assert_eq!(results[1].vector.id, "along-y");
        assert_eq!(results[2].vector.id, "along-z");
    }

    #[tokio::test]
    async fn should_rank_cosine_by_angular_similarity() {
        // given - Cosine database with vectors at different angles and magnitudes
        let config = create_config(2, DistanceMetric::Cosine);
        let db = VectorDb::open(config).await.unwrap();
        let vectors = vec![
            Vector::new("0-deg", vec![5.0, 0.0]),
            Vector::new("below-45", vec![10.0, 9.0]),
            Vector::new("45-deg", vec![1.0, 1.0]),
            Vector::new("90-deg", vec![0.0, 3.0]),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - query along the same "below-45" direction
        let results = db
            .search(&Query::new(vec![50.0, 45.0]).with_limit(4))
            .await
            .unwrap();

        // then - ranked by angle
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].vector.id, "below-45");
        assert_eq!(results[1].vector.id, "45-deg");
        assert_eq!(results[2].vector.id, "0-deg");
        assert_eq!(results[3].vector.id, "90-deg");

        // scores should be increasing (lower cosine distance = more similar)
        for i in 1..results.len() {
            assert!(
                results[i - 1].score <= results[i].score,
                "Cosine distances not sorted correctly: {} > {}",
                results[i - 1].score,
                results[i].score
            );
        }
    }

    #[tokio::test]
    async fn should_search_bm25_end_to_end() {
        // given - a text field and a few documents
        let config = Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            metadata_fields: vec![MetadataFieldSpec::new("body", FieldType::Text, false)],
            ..Default::default()
        };
        let db = VectorDb::open(config).await.unwrap();
        db.write(vec![
            Vector::builder("d1", vec![0.0; 3])
                .attribute("body", "the quick brown fox")
                .build(),
            Vector::builder("d2", vec![0.0; 3])
                .attribute("body", "a fox and another fox")
                .build(),
            Vector::builder("d3", vec![0.0; 3])
                .attribute("body", "completely unrelated text")
                .build(),
        ])
        .await
        .unwrap();
        db.flush().await.unwrap();

        // when - run a BM25 query through the full project -> lookup -> bm25 chain
        let results = db
            .search(&Query::bm25("body", "fox").with_limit(10))
            .await
            .unwrap();

        // then - only matching docs, with the denser match ranked first
        let ids: Vec<&str> = results.iter().map(|r| r.vector.id.as_str()).collect();
        assert_eq!(ids, vec!["d2", "d1"]);
        // scores are non-increasing
        assert!(results[0].score >= results[1].score);
        // the text field is materialized in the result by default
        assert_eq!(
            results[0].vector.attribute("body"),
            Some(&AttributeValue::String("a fox and another fox".to_string()))
        );
    }
}

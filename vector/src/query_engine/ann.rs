//! ANN source operators (`ANNOperator` / `ExhaustiveNNOperator`).
//!
//! Both score a query vector against the candidates in a set of probed
//! centroids and emit ranked [`ScoredVectorId`]s — the leaf of the vector
//! search chain (`{ANNOperator | ExhaustiveNNOperator} -> LookupOperator ->
//! ProjectOperator`). They differ only in how they pick which centroids to
//! probe:
//!
//! - [`ANNOperator`] routes the query through the centroid index and probes the
//!   `nprobe` nearest centroids (the default approximate path).
//! - [`ExhaustiveNNOperator`] scores every centroid and keeps the `nprobe`
//!   nearest (brute-force centroid selection, used by `search_exact_nprobe`).
//!
//! Forward-index lookups and field projection are deferred to the downstream
//! operators. Deleted/replaced vectors are already removed from the centroid
//! posting lists at write time, so — unlike the FTS path — no deletions bitmap
//! is consulted here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common::storage::StorageRead;
use tracing::debug;

use crate::error::{Error, Result};
use crate::math::distance::{self, VectorDistance};
use crate::metric_names::QUERY_VECTORS_SCORED_TOTAL;
use crate::query_engine::collectors::{Collector, TopK};
use crate::query_engine::filter::PreparedFilter;
use crate::query_engine::operator::Operator;
use crate::query_engine::types::{Score, ScoredVectorId};
use crate::serde::collection_meta::DistanceMetric;
use crate::serde::vector_id::VectorId;
use crate::storage::VectorDbStorageReadExt;
use crate::write::indexer::tree::centroids::{LeveledCentroidIndex, search_centroids};
use crate::write::indexer::tree::posting_list::{Posting, PostingList};

use super::QueryEngineOptions;

/// How a source picks which centroids to probe.
#[derive(Clone, Copy)]
enum Probe {
    /// Probe the `nprobe` nearest centroids via the centroid index.
    Nearest,
    /// Score every centroid, then keep the `nprobe` nearest.
    Exhaustive,
}

/// Shared state and scoring core for the two ANN source operators.
///
/// Owns everything needed to score one query independently of the rest of the
/// engine so the enclosing future stays `Send + 'static`.
struct AnnSource {
    storage: Arc<dyn StorageRead>,
    centroid_index: Arc<LeveledCentroidIndex<'static>>,
    options: QueryEngineOptions,
    /// The raw (un-normalized) query vector; normalized per-metric in `run`.
    query_vector: Vec<f32>,
    filter: PreparedFilter,
    nprobe: usize,
    limit: usize,
}

impl AnnSource {
    /// Validate + normalize the query, probe centroids, score their posting
    /// lists, and collect the ranked survivors.
    async fn execute(&self, probe: Probe) -> Result<Vec<ScoredVectorId<VectorDistance>>> {
        // 1. Validate query dimensions.
        if self.query_vector.len() != self.options.dimensions as usize {
            return Err(Error::InvalidInput(format!(
                "Query dimension mismatch: expected {}, got {}",
                self.options.dimensions,
                self.query_vector.len()
            )));
        }

        // 2. Normalize the query vector if the metric requires it.
        let mut query = self.query_vector.clone();
        self.options.distance_metric.normalize_if_needed(&mut query);

        // 3. Pick (and dynamically prune) the centroids to probe.
        let centroid_ids = select_centroids(
            self.centroid_index.as_ref(),
            &self.options,
            &query,
            self.nprobe,
            probe,
        )
        .await?;
        if centroid_ids.is_empty() {
            return Ok(Vec::new());
        }

        // 4. Load the posting lists and score every candidate vector.
        let scored = load_and_score(&self.storage, &self.options, &centroid_ids, &query).await?;

        // 5. Apply the metadata filter and dedup candidates that appear in more
        //    than one probed centroid (boundary replication), keeping each id's
        //    best (most similar) distance.
        let mut best: HashMap<VectorId, VectorDistance> = HashMap::new();
        for (id, dist) in scored {
            if !self.filter.matches(id.id()) {
                continue;
            }
            best.entry(id)
                .and_modify(|existing| {
                    if dist < *existing {
                        *existing = dist;
                    }
                })
                .or_insert(dist);
        }

        // 6. Stream the deduped candidates into the shared top-k collector.
        //    `VectorDistance` is the `Score` type, so it carries the
        //    most-similar-first ordering directly — `TopK` ranks correctly with
        //    no score transform.
        let mut collector: TopK<VectorDistance> = TopK::new(self.limit);
        for (&id, &dist) in &best {
            collector.collect(ScoredVectorId {
                val: id,
                score: dist,
            });
        }
        Ok(collector.finish())
    }
}

/// Approximate nearest-neighbour source: probes the `nprobe` nearest centroids.
pub(crate) struct ANNOperator(AnnSource);

impl ANNOperator {
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        centroid_index: Arc<LeveledCentroidIndex<'static>>,
        options: QueryEngineOptions,
        query_vector: Vec<f32>,
        filter: PreparedFilter,
        nprobe: usize,
        limit: usize,
    ) -> Self {
        Self(AnnSource {
            storage,
            centroid_index,
            options,
            query_vector,
            filter,
            nprobe,
            limit,
        })
    }
}

#[async_trait::async_trait]
impl Operator<Vec<ScoredVectorId<VectorDistance>>> for ANNOperator {
    async fn execute(&self) -> Result<Vec<ScoredVectorId<VectorDistance>>> {
        self.0.execute(Probe::Nearest).await
    }
}

/// Exhaustive nearest-neighbour source: scores every centroid, then keeps the
/// `nprobe` nearest before scoring their posting lists.
pub(crate) struct ExhaustiveNNOperator(AnnSource);

impl ExhaustiveNNOperator {
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        centroid_index: Arc<LeveledCentroidIndex<'static>>,
        options: QueryEngineOptions,
        query_vector: Vec<f32>,
        filter: PreparedFilter,
        nprobe: usize,
        limit: usize,
    ) -> Self {
        Self(AnnSource {
            storage,
            centroid_index,
            options,
            query_vector,
            filter,
            nprobe,
            limit,
        })
    }
}

#[async_trait::async_trait]
impl Operator<Vec<ScoredVectorId<VectorDistance>>> for ExhaustiveNNOperator {
    async fn execute(&self) -> Result<Vec<ScoredVectorId<VectorDistance>>> {
        self.0.execute(Probe::Exhaustive).await
    }
}

/// Select and dynamically prune the centroids to probe for one query.
async fn select_centroids(
    centroid_index: &LeveledCentroidIndex<'static>,
    options: &QueryEngineOptions,
    query: &[f32],
    nprobe: usize,
    probe: Probe,
) -> Result<Vec<VectorId>> {
    let candidates = match probe {
        Probe::Nearest => search_centroids(centroid_index, query, nprobe).await?,
        Probe::Exhaustive => {
            // Score every centroid (closest first), then keep `nprobe`.
            let mut all = search_centroids(centroid_index, query, usize::MAX).await?;
            all.truncate(nprobe);
            all
        }
    };
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    Ok(prune_centroids(options, &candidates, query))
}

/// Apply query-aware dynamic pruning
/// (SPANN §3.2: https://arxiv.org/pdf/2111.08566).
///
/// Given candidate centroids (already sorted closest-first), computes the raw
/// distance from the query to each and keeps only those satisfying
/// `dist(q, c) <= (1 + epsilon) * dist(q, closest)`. With pruning disabled
/// (`None`), returns every candidate's id unchanged.
fn prune_centroids(
    options: &QueryEngineOptions,
    centroid_ids: &[Posting],
    query: &[f32],
) -> Vec<VectorId> {
    let epsilon = match options.query_pruning_factor {
        Some(e) => e,
        None => return centroid_ids.iter().map(|posting| posting.id()).collect(),
    };

    let metric = options.distance_metric;

    // Compute raw distance (lower = closer) from query to each centroid.
    let mut scored: Vec<(VectorId, f32)> = centroid_ids
        .iter()
        .map(|posting| {
            (
                posting.id(),
                distance::raw_distance(query, posting.vector(), metric),
            )
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
        DistanceMetric::L2 | DistanceMetric::Cosine => (1.0 + epsilon).powi(2) * closest_dist,
        DistanceMetric::DotProduct => (1.0 + epsilon) * closest_dist,
    };
    scored
        .into_iter()
        .take_while(|&(_, d)| d <= threshold)
        .map(|(id, _)| id)
        .collect()
}

/// Spawn a task per centroid to load its posting list and score every
/// candidate against the query, returning all `(id, distance)` pairs across the
/// probed centroids (with duplicates — dedup is the caller's job).
async fn load_and_score(
    storage: &Arc<dyn StorageRead>,
    options: &QueryEngineOptions,
    centroid_ids: &[VectorId],
    query: &[f32],
) -> Result<Vec<(VectorId, VectorDistance)>> {
    let dimensions = options.dimensions as usize;
    let metric = options.distance_metric;

    let t = Instant::now();
    let mut handles = Vec::with_capacity(centroid_ids.len());
    for &cid in centroid_ids {
        let snap = storage.clone();
        let query = query.to_vec();
        handles.push(tokio::spawn(async move {
            let posting_list =
                PostingList::from_value(snap.get_posting_list(cid, dimensions).await?, false);
            let scored: Vec<(VectorId, VectorDistance)> = posting_list
                .iter()
                .map(|posting| {
                    (
                        posting.id(),
                        distance::compute_distance(&query, posting.vector(), metric),
                    )
                })
                .collect();
            Ok::<_, Error>(scored)
        }));
    }

    let results = futures::future::join_all(handles).await;
    debug!(
        op = "search/load_and_score/load",
        elapsed_ms = t.elapsed().as_millis() as u64
    );

    let mut all = Vec::new();
    let mut total_scored: u64 = 0;
    for result in results {
        let scored = result.map_err(|e| Error::Internal(format!("task join error: {}", e)))??;
        total_scored += scored.len() as u64;
        all.extend(scored);
    }
    metrics::counter!(QUERY_VECTORS_SCORED_TOTAL).increment(total_scored);

    Ok(all)
}

/// `VectorDistance` already orders most-similar-first and exposes the raw
/// distance via its inherent `score()`, so it is the ANN path's [`Score`] type
/// directly — no wrapper or order inversion needed.
impl Score for VectorDistance {
    fn score(&self) -> f32 {
        // Inherent `VectorDistance::score()`; the private field is not visible
        // from this module.
        VectorDistance::score(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::VectorDb;
    use crate::model::{Config, Filter, MetadataFieldSpec, Vector};
    use crate::serde::FieldType;
    use crate::storage::VectorDbStorageReadExt;
    use common::StorageConfig;

    fn data_id(id: u64) -> VectorId {
        VectorId::data_vector_id(id)
    }

    fn options(metric: DistanceMetric, pruning: Option<f32>) -> QueryEngineOptions {
        QueryEngineOptions {
            dimensions: 3,
            distance_metric: metric,
            query_pruning_factor: pruning,
        }
    }

    // --- prune_centroids ---

    #[test]
    fn should_prune_centroids_beyond_epsilon_threshold() {
        // given - centroids at L2 distances 1.0, 1.4, 2.0, 5.0 from query [0,0,0];
        // epsilon = 0.5 => threshold = 1.5^2 * 1.0 = 2.25 (squared L2 space).
        let opts = options(DistanceMetric::L2, Some(0.5));
        let query = [0.0, 0.0, 0.0];
        let centroids = vec![
            Posting::from_vec(data_id(1), vec![1.0, 0.0, 0.0]),
            Posting::from_vec(data_id(2), vec![1.4, 0.0, 0.0]),
            Posting::from_vec(data_id(3), vec![2.0, 0.0, 0.0]),
            Posting::from_vec(data_id(4), vec![5.0, 0.0, 0.0]),
        ];

        // when
        let pruned = prune_centroids(&opts, &centroids, &query);

        // then - only the two within threshold survive (squared dists 1.0, 1.96)
        assert_eq!(pruned, vec![data_id(1), data_id(2)]);
    }

    #[test]
    fn should_skip_pruning_when_epsilon_is_none() {
        // given - no pruning configured
        let opts = options(DistanceMetric::L2, None);
        let query = [0.0, 0.0, 0.0];
        let centroids = vec![
            Posting::from_vec(data_id(1), vec![1.0, 0.0, 0.0]),
            Posting::from_vec(data_id(2), vec![100.0, 0.0, 0.0]),
        ];

        // when
        let result = prune_centroids(&opts, &centroids, &query);

        // then - all centroids returned regardless of distance
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn should_prune_centroids_returns_sorted_by_distance() {
        // given - large epsilon keeps all; centroids passed in non-distance order
        let opts = options(DistanceMetric::L2, Some(10.0));
        let query = [0.0, 0.0, 0.0];
        let centroids = vec![
            Posting::from_vec(data_id(1), vec![5.0, 0.0, 0.0]), // far
            Posting::from_vec(data_id(3), vec![3.0, 0.0, 0.0]), // medium
            Posting::from_vec(data_id(2), vec![1.0, 0.0, 0.0]), // close
        ];

        // when
        let result = prune_centroids(&opts, &centroids, &query);

        // then - sorted by ascending distance
        assert_eq!(result, vec![data_id(2), data_id(3), data_id(1)]);
    }

    // --- ANNOperator / ExhaustiveNNOperator ---

    fn ann_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            metadata_fields: vec![MetadataFieldSpec::new("category", FieldType::String, true)],
            ..Default::default()
        }
    }

    fn doc(id: &str, values: Vec<f32>, category: &str) -> Vector {
        Vector::builder(id, values)
            .attribute("category", category)
            .build()
    }

    async fn seeded_db() -> VectorDb {
        let db = VectorDb::open(ann_config()).await.unwrap();
        db.write(vec![
            doc("close", vec![0.1, 0.0, 0.0], "a"),
            doc("medium", vec![1.0, 0.0, 0.0], "b"),
            doc("far", vec![5.0, 0.0, 0.0], "a"),
        ])
        .await
        .unwrap();
        db.flush().await.unwrap();
        db
    }

    async fn prepared_filter(db: &VectorDb, filter: Option<&Filter>) -> PreparedFilter {
        PreparedFilter::build(filter, db.query_engine().storage.as_ref())
            .await
            .unwrap()
    }

    /// Resolve scored internal ids back to external doc ids (rank order kept).
    async fn external_ids(db: &VectorDb, scored: &[ScoredVectorId<VectorDistance>]) -> Vec<String> {
        let engine = db.query_engine();
        let dims = engine.options.dimensions as usize;
        let mut ids = Vec::with_capacity(scored.len());
        for entry in scored {
            let vector_data = engine
                .storage
                .get_vector_data(entry.val, dims)
                .await
                .unwrap()
                .unwrap();
            ids.push(vector_data.external_id().to_string());
        }
        ids
    }

    fn ann_op(
        db: &VectorDb,
        query: Vec<f32>,
        filter: PreparedFilter,
        nprobe: usize,
        limit: usize,
    ) -> ANNOperator {
        let engine = db.query_engine();
        ANNOperator::new(
            engine.storage.clone(),
            engine.centroid_index.clone(),
            engine.options.clone(),
            query,
            filter,
            nprobe,
            limit,
        )
    }

    fn exhaustive_op(
        db: &VectorDb,
        query: Vec<f32>,
        filter: PreparedFilter,
        nprobe: usize,
        limit: usize,
    ) -> ExhaustiveNNOperator {
        let engine = db.query_engine();
        ExhaustiveNNOperator::new(
            engine.storage.clone(),
            engine.centroid_index.clone(),
            engine.options.clone(),
            query,
            filter,
            nprobe,
            limit,
        )
    }

    #[tokio::test]
    async fn should_rank_candidates_by_distance() {
        let db = seeded_db().await;
        let filter = prepared_filter(&db, None).await;

        let results = ann_op(&db, vec![0.0, 0.0, 0.0], filter, 100, 10)
            .execute()
            .await
            .unwrap();

        // nearest-first ordering
        assert_eq!(
            external_ids(&db, &results).await,
            vec!["close", "medium", "far"]
        );
        // public score is the raw L2 distance: non-decreasing as rank descends
        for window in results.windows(2) {
            assert!(window[0].score <= window[1].score);
        }
    }

    #[tokio::test]
    async fn should_respect_limit() {
        let db = seeded_db().await;
        let filter = prepared_filter(&db, None).await;

        let results = ann_op(&db, vec![0.0, 0.0, 0.0], filter, 100, 1)
            .execute()
            .await
            .unwrap();

        assert_eq!(external_ids(&db, &results).await, vec!["close"]);
    }

    #[tokio::test]
    async fn should_apply_metadata_filter() {
        let db = seeded_db().await;
        let filter = prepared_filter(&db, Some(&Filter::eq("category", "a"))).await;

        let results = ann_op(&db, vec![0.0, 0.0, 0.0], filter, 100, 10)
            .execute()
            .await
            .unwrap();

        // only category-"a" docs survive, still nearest-first
        assert_eq!(external_ids(&db, &results).await, vec!["close", "far"]);
    }

    #[tokio::test]
    async fn should_rank_candidates_by_distance_exhaustively() {
        let db = seeded_db().await;
        let filter = prepared_filter(&db, None).await;

        let results = exhaustive_op(&db, vec![0.0, 0.0, 0.0], filter, 100, 10)
            .execute()
            .await
            .unwrap();

        assert_eq!(
            external_ids(&db, &results).await,
            vec!["close", "medium", "far"]
        );
    }
}

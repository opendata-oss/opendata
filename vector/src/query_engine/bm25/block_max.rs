//! BlockMaxScore BM25 operator (RFC-0006 Milestone 3).
//!
//! Scores a BM25 query without visiting every document in the union of the
//! query terms' posting lists. The structure is a Rust port of Lucene's
//! `MaxScoreBulkScorer`
//! (<https://github.com/apache/lucene/blob/main/lucene/core/src/java/org/apache/lucene/search/MaxScoreBulkScorer.java>):
//!
//! - Documents are scored in ascending id order, in **outer windows** aligned
//!   to posting-block boundaries so per-block impacts give tight score bounds.
//! - Within each outer window, scorers are partitioned into *essential* and
//!   *non-essential* sets: the non-essential set is the largest
//!   lowest-bounded group whose summed maximum contribution cannot lift a
//!   document into the current top-k. Only essential posting lists generate
//!   candidates; non-essential lists are probed per candidate.
//! - Essential clauses are scored **term-by-term** through a pluggable
//!   [`EssentialScorer`] strategy (see [`super::essential`]): the strategy
//!   plans each outer window's end and turns the essential postings within
//!   it into a doc-ascending candidate list with partial scores. Window
//!   planning and accumulation are the two pieces that depend on doc-id
//!   density, so they live behind the trait; everything else is shared.
//! - Candidates are filtered against the metadata filter and the FTS
//!   deletions bitmap, then non-essential clauses are applied highest-impact
//!   first with progressive pruning of candidates whose partial score plus
//!   the remaining maximum cannot reach the top-k floor.
//!
//! Individual hits are computed in `f32` with exactly the arithmetic of the
//! exhaustive scorer ([`bm25::score`]), so the two scorers return idential
//! documents and scores.
//!
//! Known future work:
//! - Partitioning sorts by each clause's window bound; It may be worth factoring
//!   in posting sizes (e.g. `bound`/`posting size`), which can split better when
//!   bounds and posting-list lengths are uncorrelated (see [`MaxScoreScorer::partition`]).
//! - Outer windows have no minimum size. This can result in window-sizing/partitioning
//!   dominating scoring. Lucene stretches degenerate windows by tracking window size stats
//!   and probing larger window sizes when they are too small.
//! - Essential accumulation is not filter-aware: with a selective metadata
//!   filter (or heavy un-compacted deletes), essential clauses still score
//!   every posting in the window and candidates are filtered at flush.
//! - Lucene's `firstRequiredScorer` refinement (treating top non-essential
//!   clauses as required when a single essential clause cannot reach the
//!   floor alone) is not implemented.

use std::sync::Arc;

use common::storage::StorageRead;

use super::essential::{
    Candidates, DenseWindowScorer, EssentialScorer, EssentialStrategy, MergeScorer, WindowScorer,
};
use super::term_scorer::{NO_MORE_DOCS, TermScorer};
use super::{BM25Score, dedupe_query_terms, load_field_stats, load_term_stats};
use crate::error::Result;
use crate::math::bm25::{self, ScoreContext};
use crate::model::Bm25Query;
use crate::query_engine::collectors::{Collector, TopK};
use crate::query_engine::filter::PreparedFilter;
use crate::query_engine::operator::Operator;
use crate::query_engine::types::{Score, ScoredVectorId};
use crate::serde::key::TermPostingsKey;
use crate::serde::term_postings::TermPostingsView;
use crate::serde::vector_bitmap::VectorBitmap;
use crate::serde::vector_id::VectorId;

/// Scores a BM25 query with BlockMaxScore pruning and produces the ranked
/// `ScoredVectorId`s. Drop-in alternative to the exhaustive
/// [`BM25Operator`](super::BM25Operator). The `strategy` picks how essential
/// clauses are accumulated (see [`super::essential`]).
pub(crate) struct BlockMaxBM25Operator {
    storage: Arc<dyn StorageRead>,
    query: Bm25Query,
    filter: PreparedFilter,
    deletions: Arc<VectorBitmap>,
    limit: usize,
    strategy: EssentialStrategy,
}

impl BlockMaxBM25Operator {
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        query: Bm25Query,
        filter: PreparedFilter,
        deletions: Arc<VectorBitmap>,
        limit: usize,
        strategy: EssentialStrategy,
    ) -> Self {
        Self {
            storage,
            query,
            filter,
            deletions,
            limit,
            strategy,
        }
    }

    /// Load postings + term stats for each query term as lazily-decoded
    /// [`TermScorer`]s, fetching all terms concurrently (a cold N-term query
    /// would otherwise pay 2N serialized storage round trips). Terms with no
    /// documents are omitted.
    async fn open_term_scorers(
        &self,
        field: &str,
        terms: &[String],
        n_docs: u64,
    ) -> Result<Vec<TermScorer>> {
        let loads = terms.iter().map(|term| async move {
            let stats = load_term_stats(self.storage.as_ref(), field, term).await?;
            let n_t = stats.freq.max(0) as u64;
            if n_t == 0 {
                return Ok::<Option<TermScorer>, crate::error::Error>(None);
            }
            let key = TermPostingsKey::new(field, term).encode();
            let Some(record) = self.storage.get(key).await? else {
                return Ok(None);
            };
            let view = TermPostingsView::parse(record.value)?;
            if view.blocks().is_empty() {
                return Ok(None);
            }
            Ok(Some(TermScorer::new(view, bm25::idf(n_docs, n_t))?))
        });
        let scorers: Vec<Option<TermScorer>> = futures::future::try_join_all(loads).await?;
        Ok(scorers.into_iter().flatten().collect())
    }
}

#[async_trait::async_trait]
impl Operator<Vec<ScoredVectorId<BM25Score>>> for BlockMaxBM25Operator {
    async fn execute(&self) -> Result<Vec<ScoredVectorId<BM25Score>>> {
        let collector: TopK<BM25Score> = TopK::new(self.limit);

        let terms = dedupe_query_terms(&self.query.query);
        if terms.is_empty() {
            return Ok(collector.finish());
        }

        let field_stats = load_field_stats(self.storage.as_ref(), &self.query.field).await?;
        let n_docs = field_stats.count.max(0) as u64;
        if n_docs == 0 || field_stats.total_length <= 0 {
            return Ok(collector.finish());
        }
        let avgdl = field_stats.total_length as f32 / n_docs as f32;

        let scorers = self
            .open_term_scorers(&self.query.field, &terms, n_docs)
            .await?;
        if scorers.is_empty() {
            return Ok(collector.finish());
        }
        let ctx = ScoreContext::new(avgdl);

        match self.strategy {
            EssentialStrategy::DenseWindow => {
                self.run(scorers, ctx, DenseWindowScorer::new(), collector)
            }
            EssentialStrategy::SortMerge => self.run(scorers, ctx, MergeScorer::new(), collector),
        }
    }
}

impl BlockMaxBM25Operator {
    fn run<S: EssentialScorer>(
        &self,
        scorers: Vec<TermScorer>,
        ctx: ScoreContext,
        strategy: S,
        collector: TopK<BM25Score>,
    ) -> Result<Vec<ScoredVectorId<BM25Score>>> {
        let mut scorer = MaxScoreScorer::new(
            scorers,
            ctx,
            &self.filter,
            &self.deletions,
            collector,
            strategy,
        );
        scorer.score_all()?;
        let MaxScoreScorer { collector, .. } = scorer;
        Ok(collector.finish())
    }
}

/// The BlockMaxScore driver: owns the scorers, the essential/non-essential
/// partition, the candidate buffer, and the collector; delegates window
/// planning and essential accumulation to the [`EssentialScorer`] strategy.
struct MaxScoreScorer<'a, S: EssentialScorer> {
    /// Scorers, physically partitioned per window (as Lucene's
    /// `partitionScorers` rewrites `allScorers`): `scorers[..first_essential]`
    /// holds the non-essential scorers (ascending window bound),
    /// `scorers[first_essential..]` the essential ones. Rebuilt by
    /// [`partition`](Self::partition).
    scorers: Vec<WindowScorer>,
    first_essential: usize,
    /// `max_score_sums[i]` = upper bound of the summed contribution of
    /// non-essential scorers `scorers[0..=i]` for the current window.
    max_score_sums: Vec<f64>,
    /// Threshold at which the current partition goes stale and the outer
    /// window is re-planned.
    next_min_competitive: f32,
    ctx: ScoreContext,
    filter: &'a PreparedFilter,
    deletions: &'a VectorBitmap,
    collector: TopK<BM25Score>,
    strategy: S,
    candidates: Candidates,
}

impl<'a, S: EssentialScorer> MaxScoreScorer<'a, S> {
    fn new(
        scorers: Vec<TermScorer>,
        ctx: ScoreContext,
        filter: &'a PreparedFilter,
        deletions: &'a VectorBitmap,
        collector: TopK<BM25Score>,
        strategy: S,
    ) -> Self {
        let n = scorers.len();
        let scorers = scorers
            .into_iter()
            .map(|scorer| WindowScorer {
                scorer,
                max_window_score: 0.0,
            })
            .collect();
        Self {
            scorers,
            first_essential: 0,
            max_score_sums: vec![0.0; n],
            next_min_competitive: 0.0,
            ctx,
            filter,
            deletions,
            collector,
            strategy,
            candidates: Candidates::default(),
        }
    }

    /// Current top-k floor as a raw f32, or 0 while the collector is not full
    /// (any score is competitive; BM25 scores are non-negative).
    fn min_competitive(&self) -> f32 {
        self.collector
            .min_competitive_score()
            .map(|s| s.score())
            .unwrap_or(0.0)
    }

    /// Main loop: iterate outer windows over the whole doc id space.
    fn score_all(&mut self) -> Result<()> {
        let mut window_min = self
            .scorers
            .iter()
            .map(|ws| ws.scorer.doc())
            .min()
            .unwrap_or(NO_MORE_DOCS);

        while window_min < NO_MORE_DOCS {
            let mut window_max = self.plan_window(window_min);
            // Window/partition fixpoint: the window depends on which scorers
            // are essential, and essentialness depends on per-window max
            // scores. Usually settles in one iteration.
            let mut competitive = true;
            loop {
                self.update_max_window_scores(window_min, window_max);
                if !self.partition(self.min_competitive()) {
                    // Even matching every term cannot reach the top-k floor
                    // anywhere in this window: skip it entirely.
                    competitive = false;
                    break;
                }
                let new_max = self.plan_window(window_min);
                if new_max >= window_max {
                    break;
                }
                window_max = new_max;
            }
            if !competitive {
                window_min = window_max;
                continue;
            }

            // Bring essential scorers up to the window start.
            for ws in &mut self.scorers[self.first_essential..] {
                if ws.scorer.doc() < window_min {
                    ws.scorer.seek(window_min)?;
                }
            }

            let mut top = self.essential_top();
            while top < window_max {
                self.score_window_chunk(window_max)?;
                top = self.essential_top();
                if self.min_competitive() >= self.next_min_competitive {
                    // The floor rose enough to demote another scorer to
                    // non-essential: re-plan the rest of this window.
                    break;
                }
            }
            // We need to consider both top and window_max here:
            // - if the loop above breaks early because the min competitive score rose, then
            //   top may not be past window_max
            // - otherwise, top may be advanced past non-essential scorers whose items need to
            //   still be scored
            window_min = top.min(window_max);
        }
        Ok(())
    }

    /// Smallest current doc among essential scorers.
    fn essential_top(&self) -> u64 {
        self.scorers[self.first_essential..]
            .iter()
            .map(|ws| ws.scorer.doc())
            .min()
            .unwrap_or(NO_MORE_DOCS)
    }

    /// Pick the outer window end (exclusive) via the strategy.
    fn plan_window(&self, window_min: u64) -> u64 {
        // Window leads are the essential scorers (from the previous
        // partition), so a non-essential clause's boundaries don't shrink
        // every window. `first_essential == scorers.len()` is reachable
        // here — when a window's partition demotes every scorer, score_all
        // skips the window and re-plans from this function — so fall back to
        // the last (highest-bounded) scorer as the lead rather than
        // producing an unbounded window.
        let first_lead = self.first_essential.min(self.scorers.len() - 1);
        self.strategy
            .plan_window(&self.scorers[first_lead..], window_min)
    }

    /// Refresh every scorer's max possible contribution for
    /// `[window_min, window_max)`.
    fn update_max_window_scores(&mut self, window_min: u64, window_max: u64) {
        for ws in &mut self.scorers {
            ws.max_window_score = if ws.scorer.doc() < window_max {
                let from = ws.scorer.doc().max(window_min);
                ws.scorer
                    .max_score_in_range(from, window_max - 1, &self.ctx)
            } else {
                0.0
            };
        }
    }

    /// Split scorers into non-essential/essential for the current window.
    ///
    /// Returns `false` when every scorer is non-essential — no document in
    /// the window can be competitive. Scorers are sorted by ascending window
    /// bound and the non-essential prefix grows while its error-adjusted
    /// bound sum stays below the top-k floor.
    ///
    /// Demotions stop at the first scorer that fails the test: with the
    /// sort keyed on the bound itself, every later scorer has an
    /// equal-or-larger bound against the same accumulated sum (and
    /// [`bm25::sum_upper_bound`] is monotone in both arguments), so it would
    /// fail too. NOTE: Lucene instead sorts by `bound / cost`, which can
    /// produce a better split when bounds and posting-list lengths are
    /// uncorrelated — but then demotions are no longer a prefix of the sort
    /// order and the demoted scorers must be individually compacted into the
    /// prefix as `partitionScorers` does. If cost-aware ordering is
    /// reintroduced, the `partition_*` regression tests cover the corpora
    /// where the orderings disagree.
    fn partition(&mut self, min_competitive: f32) -> bool {
        // Physically reorder so the regions are contiguous slices (the
        // essential suffix is handed to the strategy as `&mut [WindowScorer]`).
        self.scorers
            .sort_by(|a, b| a.max_window_score.total_cmp(&b.max_window_score));

        let n = self.scorers.len();
        self.first_essential = n;
        self.next_min_competitive = f32::INFINITY;
        let mut max_score_sum = 0.0f64;
        for i in 0..n {
            let w = self.scorers[i].max_window_score as f64;
            let new_sum = max_score_sum + w;
            let upper_bound = bm25::sum_upper_bound(new_sum, i + 1) as f32;
            if upper_bound < min_competitive {
                // Matching all non-essential clauses so far still cannot be
                // competitive.
                max_score_sum = new_sum;
                self.max_score_sums[i] = max_score_sum;
            } else {
                self.first_essential = i;
                self.next_min_competitive = upper_bound;
                break;
            }
        }
        self.first_essential < n
    }

    /// Score one chunk of the current window: the strategy populates the
    /// candidate buffer from the essential scorers, then the driver drops
    /// filtered/deleted docs, probes non-essential clauses, and collects.
    fn score_window_chunk(&mut self, window_max: u64) -> Result<()> {
        let Self {
            scorers,
            first_essential,
            strategy,
            ctx,
            candidates,
            filter,
            deletions,
            ..
        } = self;
        candidates.clear();
        strategy.score_window(
            &mut scorers[*first_essential..],
            window_max,
            ctx,
            candidates,
        )?;
        candidates.retain_docs(|doc| filter.matches(doc) && !deletions.contains(doc));

        self.score_non_essential_and_collect()
    }

    /// Apply non-essential clauses to the candidate buffer — highest
    /// max-score first, pruning candidates that can no longer reach the
    /// top-k floor — then offer the survivors to the collector.
    fn score_non_essential_and_collect(&mut self) -> Result<()> {
        let n = self.scorers.len();
        for i in (0..self.first_essential).rev() {
            if self.candidates.is_empty() {
                break;
            }
            let min_competitive = self.min_competitive();
            if min_competitive > 0.0 {
                let max_remaining = self.max_score_sums[i];
                let min_required = min_required_score(min_competitive, max_remaining, n);
                if min_required > 0.0 {
                    self.candidates.retain_at_least(min_required);
                }
            }
            // Leapfrog this clause over the (doc-ascending) candidates.
            let ws = &mut self.scorers[i];
            for c in 0..self.candidates.len() {
                let doc = self.candidates.docs[c];
                if ws.scorer.doc() < doc {
                    ws.scorer.seek(doc)?;
                }
                if ws.scorer.doc() == doc {
                    let hit = ws.scorer.current_hit(&self.ctx);
                    self.candidates.scores[c] += hit as f64;
                }
            }
        }

        for c in 0..self.candidates.len() {
            self.collector.collect(ScoredVectorId {
                val: VectorId::from_raw(self.candidates.docs[c]),
                score: BM25Score(self.candidates.scores[c] as f32),
            });
        }
        Ok(())
    }
}

/// Lowest partial score a candidate needs for `partial + max_remaining` to
/// possibly reach `min_competitive`, conservatively widened for summation
/// rounding (Lucene's ulp-backoff in `MaxScoreBulkScorer#scoreNonEssential`).
fn min_required_score(min_competitive: f32, max_remaining: f64, num_scorers: usize) -> f64 {
    let mut min_required = min_competitive as f64 - max_remaining;
    let ulp = ulp_f32(min_competitive) as f64;
    while min_required > 0.0
        && bm25::sum_upper_bound(min_required + max_remaining, num_scorers) as f32
            >= min_competitive
    {
        min_required -= ulp;
    }
    min_required
}

/// Unit in the last place of a positive finite f32.
#[inline]
fn ulp_f32(x: f32) -> f32 {
    debug_assert!(x.is_finite() && x > 0.0);
    f32::from_bits(x.to_bits() + 1) - x
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::VectorDb;
    use crate::model::{Config, Filter, MetadataFieldSpec, Vector};
    use crate::query_engine::bm25::BM25Operator;
    use crate::serde::FieldType;
    use crate::serde::collection_meta::DistanceMetric;
    use common::StorageConfig;

    fn text_config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            metadata_fields: vec![
                MetadataFieldSpec::new("body", FieldType::Text, false),
                MetadataFieldSpec::new("category", FieldType::String, true),
            ],
            ..Default::default()
        }
    }

    fn doc(id: &str, body: &str, category: &str) -> Vector {
        Vector::builder(id, vec![0.0; 3])
            .attribute("body", body)
            .attribute("category", category)
            .build()
    }

    async fn seeded_db(docs: Vec<Vector>) -> VectorDb {
        let db = VectorDb::open(text_config()).await.unwrap();
        db.write(docs).await.unwrap();
        db.flush().await.unwrap();
        db
    }

    /// Run the BlockMax operator with the given essential-scoring strategy
    /// against the db's current snapshot.
    async fn run_block_max_with(
        strategy: EssentialStrategy,
        db: &VectorDb,
        query: &str,
        filter: Option<&Filter>,
        limit: usize,
    ) -> Vec<ScoredVectorId<BM25Score>> {
        let engine = db.query_engine();
        let filter = PreparedFilter::build(filter, engine.storage.as_ref())
            .await
            .unwrap();
        BlockMaxBM25Operator::new(
            engine.storage.clone(),
            Bm25Query {
                field: "body".to_string(),
                query: query.to_string(),
            },
            filter,
            engine.deletions.clone(),
            limit,
            strategy,
        )
        .execute()
        .await
        .unwrap()
    }

    /// Run the BlockMax operator (dense-window strategy).
    async fn run_block_max(
        db: &VectorDb,
        query: &str,
        filter: Option<&Filter>,
        limit: usize,
    ) -> Vec<ScoredVectorId<BM25Score>> {
        run_block_max_with(EssentialStrategy::DenseWindow, db, query, filter, limit).await
    }

    /// Run the BlockMax operator (sort-merge strategy).
    async fn run_block_max_sparse(
        db: &VectorDb,
        query: &str,
        filter: Option<&Filter>,
        limit: usize,
    ) -> Vec<ScoredVectorId<BM25Score>> {
        run_block_max_with(EssentialStrategy::SortMerge, db, query, filter, limit).await
    }

    /// Run the exhaustive reference operator against the same snapshot.
    async fn run_exhaustive(
        db: &VectorDb,
        query: &str,
        filter: Option<&Filter>,
        limit: usize,
    ) -> Vec<ScoredVectorId<BM25Score>> {
        let engine = db.query_engine();
        let filter = PreparedFilter::build(filter, engine.storage.as_ref())
            .await
            .unwrap();
        BM25Operator::new(
            engine.storage.clone(),
            Bm25Query {
                field: "body".to_string(),
                query: query.to_string(),
            },
            filter,
            engine.deletions.clone(),
            limit,
        )
        .execute()
        .await
        .unwrap()
    }

    /// Assert both scorers return the same ranked documents.
    ///
    /// Doc ids must match exactly in order; scores may differ in the last few
    /// ulps because the block-max path accumulates per-window sums in `f64`.
    fn assert_same_results(
        block_max: &[ScoredVectorId<BM25Score>],
        exhaustive: &[ScoredVectorId<BM25Score>],
        context: &str,
    ) {
        let bm_ids: Vec<u64> = block_max.iter().map(|r| r.val.id()).collect();
        let ex_ids: Vec<u64> = exhaustive.iter().map(|r| r.val.id()).collect();
        assert_eq!(
            bm_ids, ex_ids,
            "{}: block-max docs {:?} != exhaustive docs {:?}",
            context, bm_ids, ex_ids
        );
        for (bm, ex) in block_max.iter().zip(exhaustive) {
            let (b, e) = (bm.score.score(), ex.score.score());
            assert!(
                (b - e).abs() <= 1e-4 * e.abs().max(1.0),
                "{}: score mismatch for doc {}: {} vs {}",
                context,
                bm.val.id(),
                b,
                e
            );
        }
    }

    #[tokio::test]
    async fn should_rank_documents_by_bm25() {
        let db = seeded_db(vec![
            doc("d1", "quick brown fox", "a"),
            doc("d2", "fox fox", "a"),
            doc("d3", "slow green turtle", "b"),
        ])
        .await;

        let results = run_block_max(&db, "fox", None, 10).await;
        let exhaustive = run_exhaustive(&db, "fox", None, 10).await;

        assert_eq!(results.len(), 2);
        assert_same_results(&results, &exhaustive, "single term");
    }

    #[tokio::test]
    async fn should_respect_limit_and_prune() {
        let db = seeded_db(vec![
            doc("d1", "quick brown fox", "a"),
            doc("d2", "fox fox", "a"),
            doc("d3", "another fox here", "a"),
        ])
        .await;

        let results = run_block_max(&db, "fox", None, 1).await;
        let exhaustive = run_exhaustive(&db, "fox", None, 1).await;

        assert_eq!(results.len(), 1);
        assert_same_results(&results, &exhaustive, "limit 1");
    }

    #[tokio::test]
    async fn should_exclude_deleted_documents() {
        let db = seeded_db(vec![
            doc("d1", "quick brown fox", "a"),
            doc("d2", "fox fox", "a"),
        ])
        .await;
        db.delete(vec!["d2"]).await.unwrap();
        db.flush().await.unwrap();

        let results = run_block_max(&db, "fox", None, 10).await;
        let exhaustive = run_exhaustive(&db, "fox", None, 10).await;

        assert_eq!(results.len(), 1);
        assert_same_results(&results, &exhaustive, "after delete");
    }

    #[tokio::test]
    async fn should_apply_metadata_filter() {
        let db = seeded_db(vec![
            doc("d1", "quick fox", "a"),
            doc("d2", "fox fox", "b"),
            doc("d3", "lazy fox", "a"),
        ])
        .await;

        let filter = Filter::eq("category", "a");
        let results = run_block_max(&db, "fox", Some(&filter), 10).await;
        let exhaustive = run_exhaustive(&db, "fox", Some(&filter), 10).await;

        assert_eq!(results.len(), 2);
        assert_same_results(&results, &exhaustive, "filtered");
    }

    #[tokio::test]
    async fn should_return_empty_when_no_term_matches() {
        let db = seeded_db(vec![doc("d1", "quick brown fox", "a")]).await;
        let results = run_block_max(&db, "zebra", None, 10).await;
        assert!(results.is_empty());
    }

    /// Tiny deterministic PRNG so the differential corpus needs no deps.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Build a corpus with a skewed (Zipf-ish) term distribution: term `t<j>`
    /// appears in a document with probability ~1/(j+1), so low-index terms
    /// are stop-word-like and high-index terms are rare. Documents vary in
    /// length so norms vary too.
    fn build_corpus(num_docs: usize, vocab: usize, seed: u64) -> Vec<Vector> {
        let mut rng = Lcg(seed);
        let mut docs = Vec::with_capacity(num_docs);
        for i in 0..num_docs {
            let mut body = String::new();
            for j in 0..vocab {
                // Frequency within the doc also varies.
                if rng.below((j + 2) as u64) == 0 {
                    let occurrences = 1 + rng.below(4);
                    for _ in 0..occurrences {
                        body.push_str(&format!("t{} ", j));
                    }
                }
            }
            // Padding terms unique to the doc stretch the length distribution.
            for p in 0..rng.below(30) {
                body.push_str(&format!("pad{}x{} ", i, p));
            }
            let category = if i % 3 == 0 { "a" } else { "b" };
            docs.push(doc(&format!("d{}", i), body.trim(), category));
        }
        docs
    }

    /// Differential test: BlockMax must return exactly the documents the
    /// exhaustive scorer returns, across query shapes that exercise
    /// single/multi-term, common/rare mixes, filters, and small limits
    /// (small limits raise the pruning floor fastest).
    #[tokio::test]
    async fn block_max_matches_exhaustive_on_random_corpus() {
        // > 4 * 256 docs for the most common terms, so multi-block postings,
        // window advancement, and block skipping all engage.
        let db = seeded_db(build_corpus(1500, 40, 42)).await;

        let queries = [
            "t0",
            "t1 t30",
            "t0 t1 t2",
            "t5 t25 t39",
            "t0 t39",
            "t0 t1 t2 t3 t10 t35",
            "t38 t39",
            "t0 t0 t1", // duplicate query terms collapse
            "zebra t7", // one term missing entirely
        ];
        for query in queries {
            for limit in [1, 10, 100] {
                let bm = run_block_max(&db, query, None, limit).await;
                let ex = run_exhaustive(&db, query, None, limit).await;
                assert_same_results(&bm, &ex, &format!("q=[{}] k={}", query, limit));
                let sparse = run_block_max_sparse(&db, query, None, limit).await;
                assert_same_results(&sparse, &ex, &format!("sparse q=[{}] k={}", query, limit));
            }
            let filter = Filter::eq("category", "a");
            let bm = run_block_max(&db, query, Some(&filter), 10).await;
            let ex = run_exhaustive(&db, query, Some(&filter), 10).await;
            assert_same_results(&bm, &ex, &format!("q=[{}] filtered", query));
            let sparse = run_block_max_sparse(&db, query, Some(&filter), 10).await;
            assert_same_results(&sparse, &ex, &format!("sparse q=[{}] filtered", query));
        }
    }

    /// Same differential check after deleting a third of the corpus, so the
    /// deletions bitmap interacts with pruning and block boundaries.
    #[tokio::test]
    async fn block_max_matches_exhaustive_after_deletes() {
        let db = seeded_db(build_corpus(900, 30, 7)).await;
        let to_delete: Vec<String> = (0..900).step_by(3).map(|i| format!("d{}", i)).collect();
        db.delete(to_delete.iter().map(|s| s.as_str()))
            .await
            .unwrap();
        db.flush().await.unwrap();

        for query in ["t0", "t0 t1", "t2 t20 t29", "t0 t29"] {
            for limit in [1, 10] {
                let ex = run_exhaustive(&db, query, None, limit).await;
                let bm = run_block_max(&db, query, None, limit).await;
                assert_same_results(&bm, &ex, &format!("deleted q=[{}] k={}", query, limit));
                let sparse = run_block_max_sparse(&db, query, None, limit).await;
                assert_same_results(
                    &sparse,
                    &ex,
                    &format!("sparse deleted q=[{}] k={}", query, limit),
                );
            }
        }
    }

    /// Differential check over a heavily sparse id space (~94% of ids
    /// deleted): the regime the sort-merge strategy exists for. Both
    /// strategies must still match the exhaustive scorer exactly.
    #[tokio::test]
    async fn block_max_matches_exhaustive_on_sparse_id_space() {
        let db = seeded_db(build_corpus(1600, 30, 11)).await;
        let to_delete: Vec<String> = (0..1600)
            .filter(|i| i % 16 != 0)
            .map(|i| format!("d{}", i))
            .collect();
        db.delete(to_delete.iter().map(|s| s.as_str()))
            .await
            .unwrap();
        db.flush().await.unwrap();

        for query in ["t0", "t0 t1", "t1 t12 t29", "t0 t29"] {
            for limit in [1, 10] {
                let ex = run_exhaustive(&db, query, None, limit).await;
                let bm = run_block_max(&db, query, None, limit).await;
                assert_same_results(&bm, &ex, &format!("sparse-ids q=[{}] k={}", query, limit));
                let sparse = run_block_max_sparse(&db, query, None, limit).await;
                assert_same_results(
                    &sparse,
                    &ex,
                    &format!("sparse-ids merge q=[{}] k={}", query, limit),
                );
            }
        }
    }

    /// `parse()` validates framing only; a payload corrupted past its header
    /// must surface as a recoverable error from the scorer, not a panic.
    #[test]
    fn corrupt_block_surfaces_as_error_not_panic() {
        use crate::serde::term_postings::{PostingEntry, TermPostingsEncoder};
        let postings = vec![
            PostingEntry {
                id: VectorId::from_raw(8),
                freq: 2,
                norm: 30,
            },
            PostingEntry {
                id: VectorId::from_raw(2),
                freq: 3,
                norm: 20,
            },
        ];
        let encoded = TermPostingsEncoder::from_postings(postings).encode_to_bytes();
        // Corrupt the ids_encoding byte (payload offset 12) of the first
        // postings block (tag 0x00); parse() never reads it.
        let mut bytes = encoded.to_vec();
        let mut offset = 0usize;
        loop {
            let tag = bytes[offset];
            let len = u32::from_le_bytes([
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
            ]) as usize;
            let payload_start = offset + 5;
            if tag == 0x00 {
                bytes[payload_start + 12] = 0xEE;
                break;
            }
            offset = payload_start + len;
        }
        let view = TermPostingsView::parse(bytes::Bytes::from(bytes))
            .expect("parse() accepts the corrupt payload");
        assert!(TermScorer::new(view, 1.0).is_err());
    }

    /// Regression for the partition's essential/non-essential split when the
    /// terms' costs and bounds point in opposite directions: "alpha" is
    /// common (long posting list) with high-impact blocks pushing its bound
    /// above the floor; "bravo" is rare (short posting list) but only in very
    /// long docs, putting its bound below the floor. Under a cost-aware sort
    /// (Lucene's `bound / cost`, the original implementation here) demotions
    /// are not a prefix of the sort order, and a partition that doesn't
    /// physically compact the regions generates candidates from the wrong
    /// posting list. The current bound-keyed sort makes demotions a prefix by
    /// construction; this corpus pins that down and guards any future return
    /// to cost-aware ordering.
    #[tokio::test]
    async fn partition_keeps_competitive_docs_when_cost_and_bound_disagree() {
        let mut docs = Vec::new();
        let mut filler_id = 0usize;
        let mut filler = |n: usize| -> String {
            let mut s = String::new();
            for _ in 0..n {
                s.push_str(&format!("w{} ", filler_id));
                filler_id += 1;
            }
            s
        };
        for i in 0..300 {
            docs.push(doc(
                &format!("d{:04}", i),
                &format!("alpha alpha {}", filler(8)),
                "a",
            ));
        }
        for i in 300..500 {
            docs.push(doc(&format!("d{:04}", i), &filler(10), "a"));
        }
        for i in 500..900 {
            if i == 600 || i == 700 || i == 800 {
                docs.push(doc(
                    &format!("d{:04}", i),
                    &format!("bravo {}", filler(1000)),
                    "a",
                ));
            } else {
                let mut body = "alpha ".repeat(10);
                body.push_str(&filler(2));
                docs.push(doc(&format!("d{:04}", i), &body, "a"));
            }
        }
        let db = seeded_db(docs).await;
        let bm = run_block_max(&db, "alpha bravo", None, 10).await;
        let ex = run_exhaustive(&db, "alpha bravo", None, 10).await;
        eprintln!(
            "block_max: {:?}",
            bm.iter()
                .map(|r| (r.val.id(), r.score.score()))
                .collect::<Vec<_>>()
        );
        eprintln!(
            "exhaustive: {:?}",
            ex.iter()
                .map(|r| (r.val.id(), r.score.score()))
                .collect::<Vec<_>>()
        );
        assert_same_results(&bm, &ex, "partition region swap");
        let sparse = run_block_max_sparse(&db, "alpha bravo", None, 10).await;
        assert_same_results(&sparse, &ex, "partition region swap (sparse)");
    }

    /// Companion regression to
    /// [`partition_keeps_competitive_docs_when_cost_and_bound_disagree`],
    /// with the mis-assignable window holding a doc strictly above the
    /// floor. "common": 600 early freq-1 docs set the top-1 floor; one late
    /// doc has freq 50 (score well above the floor). "rare": short-posting
    /// docs with a bound below the floor, interleaved with the hot doc so
    /// they share the last window. Under cost-aware ordering with a
    /// non-compacting partition, the freq-50 doc was silently dropped from
    /// the top-1.
    #[tokio::test]
    async fn partition_floor_between_term_bounds_keeps_competitive_docs() {
        // Batch 1 sets the floor: 600 freq-1 "common" docs + filler.
        let mut batch1 = Vec::new();
        let mut i = 0usize;
        for _ in 0..600 {
            batch1.push(doc(
                &format!("d{:04}", i),
                &format!("common p{}a p{}b", i, i),
                "a",
            ));
            i += 1;
        }
        for _ in 0..796 {
            batch1.push(doc(
                &format!("d{:04}", i),
                &format!("f{}a f{}b f{}c", i, i, i),
                "a",
            ));
            i += 1;
        }
        // Batch 2 (higher internal ids, one shared block range): 250 freq-1
        // common docs, ONE hot common doc (freq 50, above the floor), and 30
        // long rare docs (freq 1, below the floor) interleaved.
        let mut batch2 = Vec::new();
        for _ in 0..250 {
            batch2.push(doc(
                &format!("d{:04}", i),
                &format!("common p{}a p{}b", i, i),
                "a",
            ));
            i += 1;
        }
        batch2.push(doc(&format!("d{:04}", i), "common ".repeat(50).trim(), "a"));
        i += 1;
        for t in 0..30 {
            let mut b = String::from("rare");
            for p in 0..200 {
                b.push_str(&format!(" q{}x{}", t, p));
            }
            batch2.push(doc(&format!("d{:04}", i), &b, "a"));
            i += 1;
        }

        let db = seeded_db(batch1).await;
        db.write(batch2).await.unwrap();
        db.flush().await.unwrap();
        let bm = run_block_max(&db, "common rare", None, 1).await;
        let ex = run_exhaustive(&db, "common rare", None, 1).await;
        eprintln!(
            "block_max: {:?}",
            bm.iter()
                .map(|r| (r.val.id(), r.score.score()))
                .collect::<Vec<_>>()
        );
        eprintln!(
            "exhaustive: {:?}",
            ex.iter()
                .map(|r| (r.val.id(), r.score.score()))
                .collect::<Vec<_>>()
        );
        assert_same_results(&bm, &ex, "non-prefix demotion");
        let sparse = run_block_max_sparse(&db, "common rare", None, 1).await;
        assert_same_results(&sparse, &ex, "non-prefix demotion (sparse)");
    }

    /// Multiple flushes produce multiple merge operands per posting list
    /// (small tail blocks between operands); the view and windowing must
    /// handle the fragmented block layout.
    #[tokio::test]
    async fn block_max_matches_exhaustive_across_merge_operands() {
        let db = VectorDb::open(text_config()).await.unwrap();
        let mut rng = Lcg(99);
        let mut next_id = 0usize;
        for _ in 0..5 {
            let mut batch = Vec::new();
            for _ in 0..120 {
                let mut body = String::new();
                for j in 0..15 {
                    if rng.below((j + 2) as u64) == 0 {
                        body.push_str(&format!("t{} t{} ", j, j));
                    }
                }
                batch.push(doc(&format!("d{}", next_id), body.trim(), "a"));
                next_id += 1;
            }
            db.write(batch).await.unwrap();
            db.flush().await.unwrap();
        }

        for query in ["t0 t1", "t0 t14", "t3 t7 t11"] {
            let ex = run_exhaustive(&db, query, None, 10).await;
            let bm = run_block_max(&db, query, None, 10).await;
            assert_same_results(&bm, &ex, &format!("operands q=[{}]", query));
            let sparse = run_block_max_sparse(&db, query, None, 10).await;
            assert_same_results(&sparse, &ex, &format!("sparse operands q=[{}]", query));
        }
    }

    /// Traversal over the mixed layout the compaction filter produces:
    /// verbatim 256-entry pairs interleaved with a re-packed survivor pair
    /// of odd count. Seeks and window planning must cross the boundary
    /// between copied and rewritten pairs cleanly.
    #[test]
    fn term_scorer_traverses_filter_rewritten_layout() {
        use crate::serde::term_postings::{PostingEntry, TermPostingsEncoder, TermPostingsView};
        use bytes::BytesMut;

        let posting = |id: u64| PostingEntry {
            id: VectorId::from_raw(id),
            freq: 1,
            norm: 1,
        };
        // Original: 4 blocks [256, 256, 256, 32]; "delete" ids 300 and 400
        // from the second block by emulating the filter's rewrite: copy the
        // clean pairs verbatim and re-encode the survivors of block 1.
        let original: Vec<_> = (0..800u64).map(posting).collect();
        let encoded = TermPostingsEncoder::from_postings(original).encode_to_bytes();
        let view = TermPostingsView::parse(encoded).unwrap();
        assert_eq!(view.blocks().len(), 4);

        let mut rewritten = BytesMut::new();
        rewritten.extend_from_slice(view.pair_bytes(0));
        let survivors: Vec<_> = (256..512u64)
            .filter(|&id| id != 300 && id != 400)
            .map(posting)
            .collect();
        rewritten
            .extend_from_slice(&TermPostingsEncoder::from_postings(survivors).encode_to_bytes());
        rewritten.extend_from_slice(view.pair_bytes(2));
        rewritten.extend_from_slice(view.pair_bytes(3));

        let mixed = TermPostingsView::parse(rewritten.freeze()).unwrap();
        assert_eq!(mixed.blocks().len(), 4);
        assert_eq!(mixed.blocks()[1].count, 254);

        // Seeks across the verbatim/rewritten boundaries.
        let mut scorer = TermScorer::new(mixed, 1.0).unwrap();
        assert_eq!(scorer.seek(255).unwrap(), 255);
        assert_eq!(scorer.seek(300).unwrap(), 301);
        assert_eq!(scorer.seek(400).unwrap(), 401);
        assert_eq!(scorer.seek(511).unwrap(), 511);
        assert_eq!(scorer.seek(512).unwrap(), 512);
        assert_eq!(scorer.seek(799).unwrap(), 799);
        scorer.next().unwrap();
        assert_eq!(scorer.doc(), NO_MORE_DOCS);
    }
}

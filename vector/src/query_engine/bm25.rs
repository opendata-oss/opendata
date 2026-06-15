//! BM25 query operator (RFC-0006 Milestone 0).
//!
//! Iterates the union of per-term posting lists document by document in
//! descending id order, accumulates each document's BM25 score in a single
//! pass, and collects the ranked survivors into a [`TopK`]. Metadata filtering
//! and FTS delete resolution happen before scoring so excluded documents pay no
//! scoring cost. Forward-index lookups are deferred to the lookup operator.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use common::storage::StorageRead;

use crate::error::Result;
use crate::math::bm25;
use crate::model::Bm25Query;
use crate::query_engine::collectors::{Collector, TopK};
use crate::query_engine::filter::PreparedFilter;
use crate::query_engine::operator::Operator;
use crate::query_engine::types::{Score, ScoredVectorId};
use crate::serde::field_stats::FieldStatsValue;
use crate::serde::key::{FieldStatsKey, TermPostingsKey, TermStatsKey};
use crate::serde::term_postings::{PostingEntry, TermPostingsValue};
use crate::serde::term_stats::TermStatsValue;
use crate::serde::vector_bitmap::VectorBitmap;
use crate::serde::vector_id::VectorId;
use crate::text;

/// A BM25 relevance score, where a higher raw value is more relevant.
///
/// Wraps the raw score so it participates in the generic [`Score`] ordering,
/// which ranks better scores *first* (`Ordering::Less`). BM25 is "higher is
/// better", so the natural `f32` order is inverted here; `score` still returns
/// the raw value for the public `SearchResult.score`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BM25Score(pub(crate) f32);

impl Score for BM25Score {
    fn score(&self) -> f32 {
        self.0
    }
}

impl PartialEq for BM25Score {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for BM25Score {}

impl PartialOrd for BM25Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BM25Score {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher relevance ranks first, so invert the natural f32 order.
        other.0.total_cmp(&self.0)
    }
}

/// Scores a BM25 query: walks the union of the query terms' posting lists and
/// produces the ranked `ScoredVectorId`s.
pub(crate) struct BM25Operator {
    storage: Arc<dyn StorageRead>,
    query: Bm25Query,
    filter: PreparedFilter,
    /// In-memory FTS deletions bitmap; the authority for delete resolution on
    /// the BM25 path.
    deletions: Arc<VectorBitmap>,
    limit: usize,
}

impl BM25Operator {
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        query: Bm25Query,
        filter: PreparedFilter,
        deletions: Arc<VectorBitmap>,
        limit: usize,
    ) -> Self {
        Self {
            storage,
            query,
            filter,
            deletions,
            limit,
        }
    }

    /// Load postings + term stats for each query term and wrap them as
    /// [`TermScorer`]s carrying precomputed IDF. Terms with no documents are
    /// omitted.
    async fn open_term_scorers(
        &self,
        field: &str,
        terms: &[String],
        n_docs: u64,
    ) -> Result<Vec<TermScorer>> {
        let mut scorers = Vec::with_capacity(terms.len());
        for term in terms {
            let postings = self.load_term_postings(field, term).await?;
            let stats = self.load_term_stats(field, term).await?;
            let n_t = stats.freq.max(0) as u64;
            if n_t == 0 {
                continue;
            }
            // Block iteration order is already descending by doc id.
            let entries: Vec<PostingEntry> = postings.iter_entries().copied().collect();
            if entries.is_empty() {
                continue;
            }
            scorers.push(TermScorer::new(entries, bm25::idf(n_docs, n_t)));
        }
        Ok(scorers)
    }

    async fn load_field_stats(&self, field: &str) -> Result<FieldStatsValue> {
        let key = FieldStatsKey::new(field).encode();
        match self.storage.get(key).await? {
            Some(record) => Ok(FieldStatsValue::decode_from_bytes(&record.value)?),
            None => Ok(FieldStatsValue::default()),
        }
    }

    async fn load_term_stats(&self, field: &str, term: &str) -> Result<TermStatsValue> {
        let key = TermStatsKey::new(field, term).encode();
        match self.storage.get(key).await? {
            Some(record) => Ok(TermStatsValue::decode_from_bytes(&record.value)?),
            None => Ok(TermStatsValue::default()),
        }
    }

    async fn load_term_postings(&self, field: &str, term: &str) -> Result<TermPostingsValue> {
        let key = TermPostingsKey::new(field, term).encode();
        match self.storage.get(key).await? {
            Some(record) => Ok(TermPostingsValue::decode_from_bytes(&record.value)?),
            None => Ok(TermPostingsValue::default()),
        }
    }
}

#[async_trait::async_trait]
impl Operator<Vec<ScoredVectorId<BM25Score>>> for BM25Operator {
    async fn execute(&self) -> Result<Vec<ScoredVectorId<BM25Score>>> {
        let mut collector: TopK<BM25Score> = TopK::new(self.limit);

        let terms = dedupe_query_terms(&self.query.query);
        if terms.is_empty() {
            return Ok(collector.finish());
        }

        // Per-field corpus stats are required to compute IDF / avgdl.
        let field_stats = self.load_field_stats(&self.query.field).await?;
        let n_docs = field_stats.count.max(0) as u64;
        if n_docs == 0 || field_stats.total_length <= 0 {
            return Ok(collector.finish());
        }
        let avgdl = field_stats.total_length as f32 / n_docs as f32;

        // One scorer per query term, each carrying the term's precomputed IDF.
        let mut scorers = self
            .open_term_scorers(&self.query.field, &terms, n_docs)
            .await?;
        if scorers.is_empty() {
            return Ok(collector.finish());
        }

        // Max-heap of (current head id, scorer index) so all scorers sharing the
        // next-highest id can be drained together.
        let mut head_heap: BinaryHeap<ScorerHead> = BinaryHeap::with_capacity(scorers.len());
        for (idx, scorer) in scorers.iter().enumerate() {
            if let Some(id) = scorer.peek_doc_id() {
                head_heap.push(ScorerHead {
                    id,
                    scorer_idx: idx,
                });
            }
        }

        // Reused per-doc buffer so we don't allocate a fresh Vec each iteration.
        let mut hits: Vec<bm25::Bm25TermEntry> = Vec::with_capacity(scorers.len());

        while let Some(&ScorerHead { id, .. }) = head_heap.peek() {
            // Drain every scorer sharing this id into the per-doc buffer.
            hits.clear();
            while let Some(head) = head_heap.peek().copied() {
                if head.id != id {
                    break;
                }
                head_heap.pop();
                let scorer = &mut scorers[head.scorer_idx];
                let hit = scorer
                    .pop()
                    .expect("scorer head was on heap but pop returned None");
                hits.push(hit);
                if let Some(next) = scorer.peek_doc_id() {
                    head_heap.push(ScorerHead {
                        id: next,
                        scorer_idx: head.scorer_idx,
                    });
                }
            }

            // Apply the metadata filter BEFORE scoring so excluded docs don't
            // pay the BM25 cost.
            if !self.filter.matches(id.id()) {
                continue;
            }

            // `id.id()` is the raw internal id from the posting, the same id
            // space the deletions bitmap indexes (data vectors are level 0).
            // Skip deleted/replaced docs before scoring; this — not the forward
            // index — hides deleted documents.
            if self.deletions.contains(id.id()) {
                continue;
            }

            let score = bm25::score(&hits, avgdl);
            collector.collect(ScoredVectorId {
                val: id,
                score: BM25Score(score),
            });
        }

        Ok(collector.finish())
    }
}

/// A term's postings paired with the term's precomputed IDF.
///
/// Yields entries in descending `doc_id` order — the same order the underlying
/// `TermPostingsValue` blocks are stored in (RFC-0006).
struct TermScorer {
    /// All entries in descending `doc_id` order.
    entries: Vec<PostingEntry>,
    /// Index of the next entry to yield (`pop`).
    cursor: usize,
    /// IDF for this scorer's term in the current corpus.
    idf: f32,
}

impl TermScorer {
    fn new(entries: Vec<PostingEntry>, idf: f32) -> Self {
        Self {
            entries,
            cursor: 0,
            idf,
        }
    }

    fn peek_doc_id(&self) -> Option<VectorId> {
        self.entries.get(self.cursor).map(|entry| entry.id)
    }

    /// Consume the current head and return its BM25 term hit (the posting's
    /// freq/norm paired with this scorer's IDF).
    fn pop(&mut self) -> Option<bm25::Bm25TermEntry> {
        let entry = self.entries.get(self.cursor)?;
        let hit = bm25::Bm25TermEntry {
            freq: entry.freq,
            norm: entry.norm,
            idf: self.idf,
        };
        self.cursor += 1;
        Some(hit)
    }
}

/// Heap entry referring to the current head of a [`TermScorer`] by index.
///
/// Ordered by `doc_id` descending so the max-heap returns the
/// next-highest-id head across all scorers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScorerHead {
    id: VectorId,
    scorer_idx: usize,
}

impl Ord for ScorerHead {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id
            .cmp(&other.id)
            .then_with(|| other.scorer_idx.cmp(&self.scorer_idx))
    }
}

impl PartialOrd for ScorerHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Tokenize a BM25 query string and drop duplicate terms while preserving
/// first-seen order.
fn dedupe_query_terms(query: &str) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut unique = Vec::new();
    for term in text::tokenize(query) {
        if seen.insert(term.clone()) {
            unique.push(term);
        }
    }
    unique
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::VectorDb;
    use crate::model::{Config, Filter, MetadataFieldSpec, Vector};
    use crate::serde::FieldType;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::storage::VectorDbStorageReadExt;
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

    /// Build a `BM25Operator` from the db's storage snapshot and run it.
    async fn run(
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

    /// Resolve scored internal ids back to external doc ids (rank order kept).
    async fn external_ids(db: &VectorDb, scored: &[ScoredVectorId<BM25Score>]) -> Vec<String> {
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

    #[tokio::test]
    async fn should_rank_documents_by_bm25() {
        // given
        let db = seeded_db(vec![
            doc("d1", "quick brown fox", "a"),
            doc("d2", "fox fox", "a"),
            doc("d3", "slow green turtle", "b"),
        ])
        .await;

        // when
        let results = run(&db, "fox", None, 10).await;

        // then - only docs containing "fox", with the denser match ranked first
        assert_eq!(external_ids(&db, &results).await, vec!["d2", "d1"]);
        assert!(results[0].score.score() >= results[1].score.score());
    }

    #[tokio::test]
    async fn should_respect_limit() {
        let db = seeded_db(vec![
            doc("d1", "quick brown fox", "a"),
            doc("d2", "fox fox", "a"),
            doc("d3", "another fox here", "a"),
        ])
        .await;

        let results = run(&db, "fox", None, 1).await;

        assert_eq!(results.len(), 1);
        assert_eq!(external_ids(&db, &results).await, vec!["d2"]);
    }

    #[tokio::test]
    async fn should_exclude_deleted_documents() {
        let db = seeded_db(vec![
            doc("d1", "quick brown fox", "a"),
            doc("d2", "fox fox", "a"),
        ])
        .await;

        // sanity - both match before the delete
        let before = external_ids(&db, &run(&db, "fox", None, 10).await).await;
        assert_eq!(before, vec!["d2", "d1"]);

        // when - delete the top doc and re-snapshot
        db.delete(vec!["d2"]).await.unwrap();
        db.flush().await.unwrap();

        // then - the deleted doc is gone, the other remains
        let after = external_ids(&db, &run(&db, "fox", None, 10).await).await;
        assert_eq!(after, vec!["d1"]);
    }

    #[tokio::test]
    async fn should_apply_metadata_filter() {
        let db = seeded_db(vec![
            doc("d1", "quick fox", "a"),
            doc("d2", "fox fox", "b"),
            doc("d3", "lazy fox", "a"),
        ])
        .await;

        // when - restrict to category = "a"
        let filter = Filter::eq("category", "a");
        let results = run(&db, "fox", Some(&filter), 10).await;
        let mut ids = external_ids(&db, &results).await;
        ids.sort();

        // then - only category-a docs survive scoring
        assert_eq!(ids, vec!["d1", "d3"]);
    }

    #[tokio::test]
    async fn should_return_empty_when_no_term_matches() {
        let db = seeded_db(vec![doc("d1", "quick brown fox", "a")]).await;

        let results = run(&db, "zebra", None, 10).await;

        assert!(results.is_empty());
    }
}

//! `Collector` trait and the `TopK` streaming top-k collector.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::query_engine::types::{Score, ScoredVectorId};
use crate::serde::vector_id::VectorId;

/// Consumes scored documents and produces the final ranked document set.
///
/// Generic over the [`Score`] type `S`, which defines the ranking direction
/// (better scores compare as [`Ordering::Less`]).
pub(crate) trait Collector<S: Score> {
    /// Offer one scored document to the collector.
    fn collect(&mut self, scored: ScoredVectorId<S>);

    /// The raw score a new document must beat to enter the result set, or
    /// `None` while any score is still competitive (the collector is not full).
    ///
    /// Sources can use this to prune work. The scalar BM25 source does not yet,
    /// but BlockMaxScore will, so it is part of the trait contract today.
    #[allow(dead_code)]
    fn min_competitive_score(&self) -> Option<S>;

    /// Consume the collector and return the ranked documents, best first.
    fn finish(self) -> Vec<ScoredVectorId<S>>;
}

/// Streaming top-k collector.
///
/// Keeps at most `limit` documents in a heap ordered so the worst candidate
/// sits at the top and is evicted first. Because [`Score`] orders best-first
/// (better = lesser), "worst" means the *greatest* score, ties broken by the
/// highest doc id. [`finish`](Collector::finish) returns the survivors sorted
/// best first (ascending score), ties broken by ascending doc id.
pub(crate) struct TopK<S: Score> {
    limit: usize,
    heap: BinaryHeap<WorstFirst<S>>,
}

impl<S: Score> TopK<S> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
        }
    }
}

impl<S: Score> Collector<S> for TopK<S> {
    fn collect(&mut self, document: ScoredVectorId<S>) {
        if self.limit == 0 {
            return;
        }
        let ScoredVectorId { val: id, score } = document;

        // When the heap is full, only admit candidates that beat the current
        // worst entry; otherwise just push.
        if self.heap.len() == self.limit {
            if let Some(worst) = self.heap.peek()
                && !is_better(&score, &id, &worst.score, &worst.id)
            {
                return;
            }
            self.heap.pop();
        }
        self.heap.push(WorstFirst { score, id });
    }

    fn min_competitive_score(&self) -> Option<S> {
        if self.heap.len() == self.limit {
            self.heap.peek().map(|worst| worst.score)
        } else {
            None
        }
    }

    fn finish(self) -> Vec<ScoredVectorId<S>> {
        let mut docs: Vec<ScoredVectorId<S>> = self
            .heap
            .into_iter()
            .map(|entry| ScoredVectorId {
                val: entry.id,
                score: entry.score,
            })
            .collect();
        // Best first: ascending score (better = lesser), ties by ascending id.
        docs.sort_by(|a, b| a.score.cmp(&b.score).then_with(|| a.val.cmp(&b.val)));
        docs
    }
}

/// Top-K heap entry ordered so `BinaryHeap::peek()` returns the worst entry —
/// the next to evict.
struct WorstFirst<S: Score> {
    score: S,
    id: VectorId,
}

impl<S: Score> PartialEq for WorstFirst<S> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<S: Score> Eq for WorstFirst<S> {}

impl<S: Score> PartialOrd for WorstFirst<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: Score> Ord for WorstFirst<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; we want the worst candidate at the top, so a
        // candidate that is *worse* must compare *greater*. `Score` orders
        // best-first, so worse means a greater score, or — on a tie — a higher
        // doc id (final results are sorted by ascending doc id on ties).
        self.score
            .cmp(&other.score)
            .then_with(|| self.id.cmp(&other.id))
    }
}

/// Returns `true` when `(new_score, new_doc_id)` would rank ahead of
/// `(other_score, other_doc_id)`: better (lesser) score, ties broken by
/// ascending doc id.
fn is_better<S: Score>(
    new_score: &S,
    new_id: &VectorId,
    other_score: &S,
    other_id: &VectorId,
) -> bool {
    match new_score.cmp(other_score) {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => new_id < other_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engine::bm25::BM25Score;

    // `BM25Score` (higher raw value = better) exercises the full `Score`
    // contract, including its order inversion: a numerically higher score must
    // rank ahead, so these tests read as "higher score wins".
    fn doc(id: u64, score: f32) -> ScoredVectorId<BM25Score> {
        ScoredVectorId {
            val: VectorId::from_raw(id),
            score: BM25Score(score),
        }
    }

    fn collect_all(collector: &mut TopK<BM25Score>, docs: Vec<ScoredVectorId<BM25Score>>) {
        for d in docs {
            collector.collect(d);
        }
    }

    /// Final ordering is best first (highest BM25 score), ties by ascending id.
    fn ranked(collector: TopK<BM25Score>) -> Vec<(u64, f32)> {
        collector
            .finish()
            .into_iter()
            .map(|d| (d.val.id(), d.score.score()))
            .collect()
    }

    #[test]
    fn should_keep_only_top_k_by_score() {
        let mut top = TopK::new(2);
        collect_all(
            &mut top,
            vec![doc(1, 1.0), doc(2, 5.0), doc(3, 3.0), doc(4, 2.0)],
        );
        assert_eq!(ranked(top), vec![(2, 5.0), (3, 3.0)]);
    }

    #[test]
    fn should_break_score_ties_by_ascending_doc_id() {
        let mut top = TopK::new(10);
        collect_all(&mut top, vec![doc(3, 1.0), doc(1, 1.0), doc(2, 1.0)]);
        assert_eq!(ranked(top), vec![(1, 1.0), (2, 1.0), (3, 1.0)]);
    }

    #[test]
    fn should_prefer_lower_doc_id_on_tie_at_capacity() {
        // At capacity, a tied-score candidate with a higher id must not evict a
        // lower id already held.
        let mut top = TopK::new(1);
        collect_all(&mut top, vec![doc(2, 1.0), doc(5, 1.0)]);
        assert_eq!(ranked(top), vec![(2, 1.0)]);

        // ...but a lower id does win the tie.
        let mut top = TopK::new(1);
        collect_all(&mut top, vec![doc(5, 1.0), doc(2, 1.0)]);
        assert_eq!(ranked(top), vec![(2, 1.0)]);
    }

    #[test]
    fn should_collect_nothing_when_limit_zero() {
        let mut top = TopK::new(0);
        collect_all(&mut top, vec![doc(1, 1.0), doc(2, 2.0)]);
        assert!(top.finish().is_empty());
    }

    #[test]
    fn should_report_min_competitive_score_only_when_full() {
        let mut top = TopK::new(2);
        assert_eq!(top.min_competitive_score(), None);
        top.collect(doc(1, 3.0));
        assert_eq!(top.min_competitive_score(), None);
        top.collect(doc(2, 5.0));
        // Full now: the worst (lowest) score is the bar to beat.
        assert_eq!(top.min_competitive_score().map(|s| s.score()), Some(3.0));
        top.collect(doc(3, 4.0)); // evicts score 3.0
        assert_eq!(top.min_competitive_score().map(|s| s.score()), Some(4.0));
    }
}

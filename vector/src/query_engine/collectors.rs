//! `Collector` trait and the `TopK` streaming top-k collector.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::query_engine::types::ScoredVectorId;
use crate::serde::vector_id::VectorId;

/// Consumes scored documents and produces the final ranked document set.
pub(crate) trait Collector {
    /// Offer one scored document to the collector.
    fn collect(&mut self, scored: ScoredVectorId);

    /// The minimum score a new document must beat to enter the result set, or
    /// `None` while any score is still competitive (the collector is not full).
    ///
    /// Drivers can use this to prune work. The scalar BM25 driver does not yet,
    /// but BlockMaxScore will, so it is part of the trait contract today.
    #[allow(dead_code)]
    fn min_competitive_score(&self) -> Option<f32>;

    /// Consume the collector and return the ranked documents, best first.
    fn finish(self) -> Vec<ScoredVectorId>;
}

/// Streaming top-k collector.
///
/// Keeps at most `limit` documents in a min-heap ordered so the worst
/// (lowest-score, then highest-doc-id) candidate sits at the top and is evicted
/// first. [`finish`](Collector::finish) returns them sorted by descending
/// score, ties broken by ascending doc id.
pub(crate) struct TopK {
    limit: usize,
    heap: BinaryHeap<WorstFirst>,
}

impl TopK {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
        }
    }
}

impl Collector for TopK {
    fn collect(&mut self, document: ScoredVectorId) {
        if self.limit == 0 {
            return;
        }
        let ScoredVectorId { val: id, score } = document;

        // When the heap is full, only admit candidates that beat the current
        // worst entry; otherwise just push.
        if self.heap.len() == self.limit {
            if let Some(worst) = self.heap.peek()
                && !is_better(score, id, worst.score, worst.id)
            {
                return;
            }
            self.heap.pop();
        }
        self.heap.push(WorstFirst { score, id });
    }

    fn min_competitive_score(&self) -> Option<f32> {
        if self.heap.len() == self.limit {
            self.heap.peek().map(|worst| worst.score)
        } else {
            None
        }
    }

    fn finish(self) -> Vec<ScoredVectorId> {
        let mut docs: Vec<ScoredVectorId> = self
            .heap
            .into_iter()
            .map(|entry| ScoredVectorId {
                val: entry.id,
                score: entry.score,
            })
            .collect();
        // Descending score, ties broken by ascending doc id.
        docs.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.val.cmp(&b.val))
        });
        docs
    }
}

/// Top-K heap entry ordered so `BinaryHeap::peek()` returns the worst
/// (lowest-score, highest-doc-id) entry — the next to evict.
struct WorstFirst {
    score: f32,
    id: VectorId,
}

impl PartialEq for WorstFirst {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for WorstFirst {}

impl PartialOrd for WorstFirst {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WorstFirst {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; we want the worst candidate at the top, so a
        // candidate that is *worse* must compare *greater*. Worse means lower
        // score, or — on a score tie — higher doc id (final results are sorted
        // by ascending doc id on ties).
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.id.cmp(&other.id))
    }
}

/// Returns `true` when `(new_score, new_doc_id)` would rank ahead of
/// `(other_score, other_doc_id)`: descending score, ties broken by ascending
/// doc id.
fn is_better(new_score: f32, new_id: VectorId, other_score: f32, other_id: VectorId) -> bool {
    match new_score.partial_cmp(&other_score) {
        Some(Ordering::Greater) => true,
        Some(Ordering::Less) => false,
        Some(Ordering::Equal) | None => new_id < other_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(id: u64, score: f32) -> ScoredVectorId {
        ScoredVectorId {
            val: VectorId::from_raw(id),
            score,
        }
    }

    fn collect_all(collector: &mut TopK, docs: Vec<ScoredVectorId>) {
        for d in docs {
            collector.collect(d);
        }
    }

    /// Final ordering is by descending score, with ties broken by ascending id.
    fn ranked(collector: TopK) -> Vec<(u64, f32)> {
        collector
            .finish()
            .into_iter()
            .map(|d| (d.val.id(), d.score))
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
        assert_eq!(top.min_competitive_score(), Some(3.0));
        top.collect(doc(3, 4.0)); // evicts score 3.0
        assert_eq!(top.min_competitive_score(), Some(4.0));
    }
}

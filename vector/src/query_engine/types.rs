//! Shared query types.
//!

use crate::serde::vector_id::VectorId;
use crate::{SearchResult, Vector};

/// A score with a defined "better" direction.
///
/// Implementors order from **best to worst**, so a *better* score compares as
/// [`Ordering::Less`](std::cmp::Ordering::Less). This lets ranking machinery
/// (e.g. [`TopK`](super::collectors::TopK)) stay metric-agnostic — it always
/// keeps the best scores. [`score`](Score::score) returns the raw `f32`
/// surfaced to callers (`SearchResult.score`), independent of ordering
/// direction.
pub(crate) trait Score: Ord + PartialOrd + Eq + PartialEq + Copy {
    fn score(&self) -> f32;
}

/// A single scored document produced by a query source.
///
/// Sources emit these as they score candidates; ordering and top-k selection
/// are the collector's responsibility. `S` is the [`Score`] type and carries
/// the metric's ordering direction.
pub(crate) struct Scored<T, S> {
    pub(crate) val: T,
    pub(crate) score: S,
}

pub(crate) type ScoredVectorId<S> = Scored<VectorId, S>;

pub(crate) type ScoredVector<S> = Scored<Vector, S>;

impl<S: Score> From<ScoredVector<S>> for SearchResult {
    fn from(val: ScoredVector<S>) -> SearchResult {
        SearchResult {
            score: val.score.score(),
            vector: val.val,
        }
    }
}

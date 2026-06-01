//! Shared query-driver types.
//!
//! A *driver* (see `ann.rs` / `bm25.rs`) executes one scoring strategy and
//! pushes [`ScoredVectorId`]s into a [`Collector`](super::collectors::Collector).
//! The collector owns ranking and top-k selection; the driver only produces
//! scored document ids.

use crate::serde::vector_id::VectorId;
use crate::{SearchResult, Vector};

/// A single scored document produced by a query driver.
///
/// Drivers emit these as they score candidates; ordering and top-k selection
/// are the collector's responsibility.
pub(crate) struct Scored<T> {
    pub(crate) val: T,
    pub(crate) score: f32,
}

pub(crate) type ScoredVectorId = Scored<VectorId>;

pub(crate) type ScoredVector = Scored<Vector>;

impl From<ScoredVector> for SearchResult {
    fn from(val: ScoredVector) -> SearchResult {
        SearchResult {
            score: val.score,
            vector: val.val,
        }
    }
}

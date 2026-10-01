//! BM25 query operators (RFC-0006).
//!
//! Two interchangeable scorers produce the ranked `ScoredVectorId`s for a
//! [`Bm25Query`](crate::model::Bm25Query):
//!
//! - [`block_max`]: the default. Skips non-competitive documents with
//!   BlockMaxScore (RFC-0006 Milestone 3).
//! - [`exhaustive`]: scores every document in the union of the query terms'
//!   posting lists (Milestone 0). Kept as the reference implementation and
//!   for benchmarking the pruned path against it.
//!
//! [`term_scorer`] holds the lazily-decoding posting-list iterator the
//! block-max scorer drives. This module owns the pieces both scorers share:
//! the score type, query tokenization, and the term/field statistics loads.

mod block_max;
mod essential;
mod exhaustive;
mod term_scorer;

pub(crate) use block_max::BlockMaxBM25Operator;
pub(crate) use essential::EssentialStrategy;
pub(crate) use exhaustive::BM25Operator;

use std::cmp::Ordering;
use std::collections::HashSet;

use common::storage::StorageRead;

use crate::error::Result;
use crate::query_engine::types::Score;
use crate::serde::field_stats::FieldStatsValue;
use crate::serde::key::{FieldStatsKey, TermStatsKey};
use crate::serde::term_stats::TermStatsValue;
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

/// Load the per-field corpus statistics (document count / total length) that
/// IDF and `avgdl` derive from. Missing stats decode as zeroed defaults.
async fn load_field_stats(storage: &dyn StorageRead, field: &str) -> Result<FieldStatsValue> {
    let key = FieldStatsKey::new(field).encode();
    match storage.get(key).await? {
        Some(record) => Ok(FieldStatsValue::decode_from_bytes(&record.value)?),
        None => Ok(FieldStatsValue::default()),
    }
}

/// Load a term's document-frequency statistics. Missing stats decode as
/// zeroed defaults (the term matches nothing).
async fn load_term_stats(
    storage: &dyn StorageRead,
    field: &str,
    term: &str,
) -> Result<TermStatsValue> {
    let key = TermStatsKey::new(field, term).encode();
    match storage.get(key).await? {
        Some(record) => Ok(TermStatsValue::decode_from_bytes(&record.value)?),
        None => Ok(TermStatsValue::default()),
    }
}

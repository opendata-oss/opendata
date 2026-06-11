//! Prepared query filter (`PreparedFilter`).
//!
//! Lowers a [`Filter`] AST into precomputed metadata bitmaps so the query
//! operators can answer per-item membership with a cheap
//! [`PreparedFilter::matches`] check. Negations are kept as exclusion bitmaps
//! rather than complemented positive sets, so building a filter never has to
//! enumerate the entire corpus universe.

use std::future::Future;
use std::pin::Pin;

use common::storage::StorageRead;
use roaring::RoaringTreemap;

use crate::error::Result;
use crate::model::{AttributeValue, Filter};
use crate::serde::FieldValue;
use crate::storage::VectorDbStorageReadExt;

/// Executable form of a query [`Filter`].
///
/// An item matches when it is present in `positive` (or `positive` is
/// `None`, meaning "no positive constraint") and absent from every bitmap in
/// `negative`. Equivalently, the matching set is `positive ∩ ¬(⋃ negative)`.
pub(crate) struct PreparedFilter {
    /// Items allowed by the positive (union/intersection) part of the
    /// filter. `None` imposes no positive constraint (matches all ids).
    positive: Option<RoaringTreemap>,
    /// Items excluded by negations. An item matches only if it is
    /// absent from every bitmap here.
    negative: Vec<RoaringTreemap>,
}

/// A subtree collapsed to a single concrete bitmap while combining `Or`
/// branches. `Pos(s)` matches ids in `s`; `Neg(s)` matches ids *not* in `s`.
enum Single {
    Pos(RoaringTreemap),
    Neg(RoaringTreemap),
}

impl PreparedFilter {
    /// Build a prepared filter from an optional filter expression. `None`
    /// matches every item.
    pub(crate) async fn build(expr: Option<&Filter>, storage: &dyn StorageRead) -> Result<Self> {
        match expr {
            None => Ok(Self::match_all()),
            Some(filter) => Self::build_expr(filter, storage).await,
        }
    }

    /// Returns `true` if `doc_id` satisfies the filter.
    pub(crate) fn matches(&self, doc_id: u64) -> bool {
        if let Some(positive) = &self.positive
            && !positive.contains(doc_id)
        {
            return false;
        }
        self.negative
            .iter()
            .all(|excluded| !excluded.contains(doc_id))
    }

    /// No positive constraint and no exclusions: matches everything.
    fn match_all() -> Self {
        Self {
            positive: None,
            negative: Vec::new(),
        }
    }

    /// Empty positive set: matches nothing.
    fn match_none() -> Self {
        Self {
            positive: Some(RoaringTreemap::new()),
            negative: Vec::new(),
        }
    }

    fn build_expr<'a>(
        filter: &'a Filter,
        storage: &'a dyn StorageRead,
    ) -> Pin<Box<dyn Future<Output = Result<Self>> + Send + 'a>> {
        Box::pin(async move {
            match filter {
                Filter::Eq(field, value) => Ok(Self {
                    positive: Some(load_eq(field, value, storage).await?),
                    negative: Vec::new(),
                }),
                Filter::In(field, values) => {
                    let mut set = RoaringTreemap::new();
                    for value in values {
                        set |= &load_eq(field, value, storage).await?;
                    }
                    Ok(Self {
                        positive: Some(set),
                        negative: Vec::new(),
                    })
                }
                Filter::Neq(field, value) => Ok(Self {
                    positive: None,
                    negative: vec![load_eq(field, value, storage).await?],
                }),
                Filter::And(filters) => {
                    let mut acc = Self::match_all();
                    for child in filters {
                        acc = acc.and(Self::build_expr(child, storage).await?);
                    }
                    Ok(acc)
                }
                Filter::Or(filters) => {
                    let mut iter = filters.iter();
                    let Some(first) = iter.next() else {
                        // An empty `Or` matches nothing.
                        return Ok(Self::match_none());
                    };
                    let mut acc = Self::build_expr(first, storage).await?;
                    for child in iter {
                        acc = acc.or(Self::build_expr(child, storage).await?);
                    }
                    Ok(acc)
                }
            }
        })
    }

    /// Intersect two prepared filters (`self AND other`).
    ///
    /// `(P1 ∩ ¬N1) ∩ (P2 ∩ ¬N2) = (P1 ∩ P2) ∩ ¬(N1 ∪ N2)`, so positives
    /// intersect and negatives concatenate.
    fn and(self, other: Self) -> Self {
        let Self {
            positive: p1,
            mut negative,
        } = self;
        let Self {
            positive: p2,
            negative: n2,
        } = other;
        let positive = match (p1, p2) {
            (Some(mut a), Some(b)) => {
                a &= &b;
                Some(a)
            }
            (Some(a), None) | (None, Some(a)) => Some(a),
            (None, None) => None,
        };
        negative.extend(n2);
        Self { positive, negative }
    }

    /// Union two prepared filters (`self OR other`).
    ///
    /// Each side is first collapsed to a single concrete bitmap so the union
    /// can be computed with set operations alone — no corpus universe needed.
    fn or(self, other: Self) -> Self {
        match (self.into_single(), other.into_single()) {
            (Single::Pos(mut a), Single::Pos(b)) => {
                a |= &b;
                Self {
                    positive: Some(a),
                    negative: Vec::new(),
                }
            }
            // a ∨ ¬t = ¬(t − a)
            (Single::Pos(a), Single::Neg(mut t)) | (Single::Neg(mut t), Single::Pos(a)) => {
                t -= &a;
                Self {
                    positive: None,
                    negative: vec![t],
                }
            }
            // ¬a ∨ ¬b = ¬(a ∩ b)
            (Single::Neg(mut a), Single::Neg(b)) => {
                a &= &b;
                Self {
                    positive: None,
                    negative: vec![a],
                }
            }
        }
    }

    /// Collapse to a single concrete bitmap by folding the negatives into the
    /// positive: `Some(p)` → `Pos(p − ⋃neg)`, `None` → `Neg(⋃neg)`.
    fn into_single(self) -> Single {
        let mut excluded = RoaringTreemap::new();
        for bitmap in &self.negative {
            excluded |= bitmap;
        }
        match self.positive {
            Some(mut positive) => {
                positive -= &excluded;
                Single::Pos(positive)
            }
            None => Single::Neg(excluded),
        }
    }
}

/// Load the metadata-index bitmap for a single `field = value` pair.
async fn load_eq(
    field: &str,
    value: &AttributeValue,
    storage: &dyn StorageRead,
) -> Result<RoaringTreemap> {
    let field_value: FieldValue = value.clone().into();
    let bitmap = storage.get_metadata_index(field, field_value).await?;
    Ok(bitmap.effective_vector_ids())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A positive leaf (`Eq`/`In`-like): matches exactly `ids`.
    fn pos(ids: &[u64]) -> PreparedFilter {
        PreparedFilter {
            positive: Some(ids.iter().copied().collect()),
            negative: Vec::new(),
        }
    }

    /// A negated leaf (`Neq`-like): matches everything except `ids`.
    fn neg(ids: &[u64]) -> PreparedFilter {
        PreparedFilter {
            positive: None,
            negative: vec![ids.iter().copied().collect()],
        }
    }

    /// Collect which ids in `0..=universe` the filter matches.
    fn matching(filter: &PreparedFilter, universe: u64) -> Vec<u64> {
        (0..=universe).filter(|&id| filter.matches(id)).collect()
    }

    #[test]
    fn should_match_all_when_no_filter() {
        let filter = PreparedFilter::match_all();
        assert_eq!(matching(&filter, 4), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn should_match_nothing_for_empty_positive() {
        let filter = PreparedFilter::match_none();
        assert!(matching(&filter, 4).is_empty());
    }

    #[test]
    fn should_intersect_positives_on_and() {
        // {1,2,3} AND {2,3,4} -> {2,3}
        let filter = pos(&[1, 2, 3]).and(pos(&[2, 3, 4]));
        assert_eq!(matching(&filter, 5), vec![2, 3]);
    }

    #[test]
    fn should_exclude_on_and_with_neg() {
        // {1,2,3} AND NOT {2} -> {1,3}
        let filter = pos(&[1, 2, 3]).and(neg(&[2]));
        assert_eq!(matching(&filter, 5), vec![1, 3]);
    }

    #[test]
    fn should_union_positives_on_or() {
        // {1,2} OR {3,4} -> {1,2,3,4}
        let filter = pos(&[1, 2]).or(pos(&[3, 4]));
        assert_eq!(matching(&filter, 5), vec![1, 2, 3, 4]);
    }

    #[test]
    fn should_handle_neg_under_or() {
        // {1} OR NOT {1,2} == NOT {2}: everything except 2.
        let filter = pos(&[1]).or(neg(&[1, 2]));
        assert_eq!(matching(&filter, 4), vec![0, 1, 3, 4]);
    }

    #[test]
    fn should_match_all_when_pos_covers_neg_under_or() {
        // {1,2} OR NOT {2} == everything (id 2 is rescued by the positive).
        let filter = pos(&[1, 2]).or(neg(&[2]));
        assert_eq!(matching(&filter, 4), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn should_intersect_negs_under_or() {
        // NOT {1} OR NOT {2} == NOT ({1} ∩ {2}) == NOT {} == everything.
        let filter = neg(&[1]).or(neg(&[2]));
        assert_eq!(matching(&filter, 4), vec![0, 1, 2, 3, 4]);

        // NOT {1,2} OR NOT {2,3} == NOT {2}: only id 2 is excluded.
        let filter = neg(&[1, 2]).or(neg(&[2, 3]));
        assert_eq!(matching(&filter, 4), vec![0, 1, 3, 4]);
    }

    #[test]
    fn should_handle_nested_and_or() {
        // ({1,2,3} AND NOT {3}) OR {5} -> {1,2,5}
        let filter = pos(&[1, 2, 3]).and(neg(&[3])).or(pos(&[5]));
        assert_eq!(matching(&filter, 6), vec![1, 2, 5]);
    }
}

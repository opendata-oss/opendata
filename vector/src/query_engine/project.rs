//! Field-projection operator (`ProjectOperator`).
//!
//! Applies the query's [`FieldSelection`] to the vectors produced by its child
//! operator, trimming each result's attributes in place.

use crate::Vector;
use crate::error::Result;
use crate::model::FieldSelection;
use crate::query_engine::operator::{BoxedOperator, Operator};
use crate::query_engine::types::{Score, ScoredVector};

/// Projects (field-selects) the vectors produced by a child operator.
///
/// Generic over the [`Score`] type `S`, which it passes through unchanged.
pub(crate) struct ProjectOperator<S: Score> {
    selection: FieldSelection,
    child: BoxedOperator<Vec<ScoredVector<S>>>,
}

impl<S: Score> ProjectOperator<S> {
    pub(crate) fn new(
        selection: FieldSelection,
        child: BoxedOperator<Vec<ScoredVector<S>>>,
    ) -> Self {
        Self { selection, child }
    }
}

#[async_trait::async_trait]
impl<S: Score + Send + Sync + 'static> Operator<Vec<ScoredVector<S>>> for ProjectOperator<S> {
    async fn execute(&self) -> Result<Vec<ScoredVector<S>>> {
        let mut scored = self.child.execute().await?;
        for vector in &mut scored {
            project_vector(&mut vector.val, &self.selection);
        }
        Ok(scored)
    }
}

/// Trim a single vector's attributes according to `selection`.
fn project_vector(vector: &mut Vector, selection: &FieldSelection) {
    match selection {
        FieldSelection::All => {}
        FieldSelection::None => vector.attributes.clear(),
        FieldSelection::Fields(names) => {
            vector.attributes.retain(|attr| names.contains(&attr.name))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_engine::bm25::BM25Score;
    use crate::{Attribute, AttributeValue};

    /// A child operator that yields a fixed list of scored vectors. Uses
    /// `BM25Score` as an arbitrary concrete [`Score`]; `ProjectOperator` only
    /// touches `val`, so the choice does not affect what is exercised here.
    struct Fixed(Vec<ScoredVector<BM25Score>>);

    #[async_trait::async_trait]
    impl Operator<Vec<ScoredVector<BM25Score>>> for Fixed {
        async fn execute(&self) -> Result<Vec<ScoredVector<BM25Score>>> {
            Ok(self
                .0
                .iter()
                .map(|entry| ScoredVector {
                    val: entry.val.clone(),
                    score: entry.score,
                })
                .collect())
        }
    }

    /// A vector carrying exactly two attributes, `a` and `b`.
    fn vector(id: &str) -> Vector {
        Vector {
            id: id.to_string(),
            attributes: vec![
                Attribute::new("a", AttributeValue::String("x".to_string())),
                Attribute::new("b", AttributeValue::String("y".to_string())),
            ],
        }
    }

    fn sorted_attr_names(vector: &Vector) -> Vec<String> {
        let mut names: Vec<String> = vector.attributes.iter().map(|a| a.name.clone()).collect();
        names.sort();
        names
    }

    async fn project(selection: FieldSelection) -> Vec<ScoredVector<BM25Score>> {
        let child = Fixed(vec![ScoredVector {
            val: vector("d1"),
            score: BM25Score(1.0),
        }]);
        ProjectOperator::new(selection, Box::new(child))
            .execute()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn should_keep_all_fields() {
        let out = project(FieldSelection::All).await;
        assert_eq!(sorted_attr_names(&out[0].val), vec!["a", "b"]);
    }

    #[tokio::test]
    async fn should_clear_all_fields() {
        let out = project(FieldSelection::None).await;
        assert!(out[0].val.attributes.is_empty());
    }

    #[tokio::test]
    async fn should_retain_only_selected_fields() {
        let out = project(FieldSelection::Fields(vec!["a".to_string()])).await;
        assert_eq!(sorted_attr_names(&out[0].val), vec!["a"]);
    }
}

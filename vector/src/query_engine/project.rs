//! Field-projection operator (`ProjectOperator`).
//!
//! Applies the query's [`FieldSelection`] to the vectors produced by its child
//! operator, trimming each result's attributes in place.

use crate::Vector;
use crate::error::Result;
use crate::model::{FieldSelection, SearchResult};
use crate::query_engine::operator::{BoxedOperator, Operator};
use crate::query_engine::types::ScoredVector;

/// Projects (field-selects) the vectors produced by a child operator.
pub(crate) struct ProjectOperator {
    selection: FieldSelection,
    child: BoxedOperator<Vec<ScoredVector>>,
}

impl ProjectOperator {
    pub(crate) fn new(selection: FieldSelection, child: BoxedOperator<Vec<ScoredVector>>) -> Self {
        Self { selection, child }
    }
}

#[async_trait::async_trait]
impl Operator<Vec<ScoredVector>> for ProjectOperator {
    async fn execute(&self) -> Result<Vec<ScoredVector>> {
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

/// Apply field projection to search results in place.
///
/// Shared with the ANN path, which is not yet expressed as an operator chain.
pub(crate) fn apply_field_selection(results: &mut [SearchResult], selection: &FieldSelection) {
    for result in results.iter_mut() {
        project_vector(&mut result.vector, selection);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Attribute, AttributeValue};

    /// A child operator that yields a fixed list of scored vectors.
    struct Fixed(Vec<ScoredVector>);

    #[async_trait::async_trait]
    impl Operator<Vec<ScoredVector>> for Fixed {
        async fn execute(&self) -> Result<Vec<ScoredVector>> {
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

    async fn project(selection: FieldSelection) -> Vec<ScoredVector> {
        let child = Fixed(vec![ScoredVector {
            val: vector("d1"),
            score: 1.0,
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

    #[test]
    fn should_apply_field_selection_to_search_results() {
        let mut results = vec![SearchResult {
            score: 1.0,
            vector: vector("d1"),
        }];

        apply_field_selection(&mut results, &FieldSelection::Fields(vec!["b".to_string()]));

        assert_eq!(sorted_attr_names(&results[0].vector), vec!["b"]);
    }
}

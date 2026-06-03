//! Forward-index lookup operator (`LookupOperator`).
//!
//! Pulls a ranked set of [`ScoredVectorId`]s from its child operator and loads
//! each one's forward-index `VectorDataValue`, producing [`ScoredVector`]s in
//! the same order.

use std::sync::Arc;

use common::storage::StorageRead;
use tracing::warn;

use crate::error::Result;
use crate::query_engine::operator::{BoxedOperator, Operator};
use crate::query_engine::types::{Score, ScoredVector, ScoredVectorId};
use crate::serde::vector_data::VectorDataValue;
use crate::storage::VectorDbStorageReadExt;
use crate::{Attribute, Vector};

/// Resolves ranked vector ids to their forward-index data.
///
/// Generic over the [`Score`] type `S`, which it passes through unchanged from
/// each [`ScoredVectorId`] to the produced [`ScoredVector`].
pub(crate) struct LookupOperator<S: Score> {
    storage: Arc<dyn StorageRead>,
    dimensions: usize,
    child: BoxedOperator<Vec<ScoredVectorId<S>>>,
}

impl<S: Score> LookupOperator<S> {
    pub(crate) fn new(
        storage: Arc<dyn StorageRead>,
        dimensions: usize,
        child: BoxedOperator<Vec<ScoredVectorId<S>>>,
    ) -> Self {
        Self {
            storage,
            dimensions,
            child,
        }
    }
}

#[async_trait::async_trait]
impl<S: Score + Send + Sync + 'static> Operator<Vec<ScoredVector<S>>> for LookupOperator<S> {
    /// Loads the forward-index data for each scored id, preserving order.
    ///
    /// Assumes every id still exists in the forward index — deletes are resolved
    /// upstream by the scoring operator — so a missing entry (e.g. a concurrent
    /// compaction) is logged as a warning and skipped rather than treated as a
    /// normal delete.
    async fn execute(&self) -> Result<Vec<ScoredVector<S>>> {
        let scored = self.child.execute().await?;

        let loaded = futures::future::join_all(
            scored
                .iter()
                .map(|entry| self.storage.get_vector_data(entry.val, self.dimensions)),
        )
        .await;

        let mut results = Vec::with_capacity(scored.len());
        for (entry, vector_data) in scored.into_iter().zip(loaded) {
            let Some(vector_data) = vector_data? else {
                warn!(
                    id = entry.val.id(),
                    "ranked document missing from forward index; skipping"
                );
                continue;
            };
            results.push(ScoredVector {
                val: vector_data_to_vector(&vector_data),
                score: entry.score,
            });
        }
        Ok(results)
    }
}

/// Convert a stored `VectorDataValue` into the public `Vector` shape.
pub(crate) fn vector_data_to_vector(vector_data: &VectorDataValue) -> Vector {
    let attributes = vector_data
        .fields()
        .map(|field| Attribute::new(field.field_name.clone(), field.value.clone().into()))
        .collect();
    Vector {
        id: vector_data.external_id().to_string(),
        attributes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AttributeValue;
    use crate::db::VectorDb;
    use crate::model::{Config, MetadataFieldSpec, Vector};
    use crate::query_engine::bm25::BM25Score;
    use crate::serde::FieldType;
    use crate::serde::collection_meta::DistanceMetric;
    use crate::serde::vector_id::VectorId;
    use common::StorageConfig;

    /// A child operator that yields a fixed list of scored ids. Uses `BM25Score`
    /// as an arbitrary concrete [`Score`]; `LookupOperator` only passes it
    /// through, so the choice does not affect what is exercised here.
    struct Fixed(Vec<ScoredVectorId<BM25Score>>);

    #[async_trait::async_trait]
    impl Operator<Vec<ScoredVectorId<BM25Score>>> for Fixed {
        async fn execute(&self) -> Result<Vec<ScoredVectorId<BM25Score>>> {
            Ok(self
                .0
                .iter()
                .map(|entry| ScoredVectorId {
                    val: entry.val,
                    score: entry.score,
                })
                .collect())
        }
    }

    fn config() -> Config {
        Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            metadata_fields: vec![MetadataFieldSpec::new("category", FieldType::String, true)],
            ..Default::default()
        }
    }

    fn doc(id: &str, category: &str) -> Vector {
        Vector::builder(id, vec![0.0; 3])
            .attribute("category", category)
            .build()
    }

    async fn seeded_db() -> VectorDb {
        let db = VectorDb::open(config()).await.unwrap();
        db.write(vec![doc("d1", "a"), doc("d2", "b")])
            .await
            .unwrap();
        db.flush().await.unwrap();
        db
    }

    async fn internal_id(db: &VectorDb, external: &str) -> VectorId {
        db.query_engine()
            .storage
            .lookup_internal_id(external)
            .await
            .unwrap()
            .unwrap()
    }

    /// Build a `LookupOperator` from the db's storage snapshot, fed `child` ids.
    fn lookup_op(
        db: &VectorDb,
        child: Vec<ScoredVectorId<BM25Score>>,
    ) -> LookupOperator<BM25Score> {
        let engine = db.query_engine();
        LookupOperator::new(
            engine.storage.clone(),
            engine.options.dimensions as usize,
            Box::new(Fixed(child)),
        )
    }

    #[tokio::test]
    async fn should_resolve_ids_to_vectors_in_order() {
        let db = seeded_db().await;
        let d1 = internal_id(&db, "d1").await;
        let d2 = internal_id(&db, "d2").await;

        // child yields d2 (higher score) then d1
        let results = lookup_op(
            &db,
            vec![
                ScoredVectorId {
                    val: d2,
                    score: BM25Score(2.0),
                },
                ScoredVectorId {
                    val: d1,
                    score: BM25Score(1.0),
                },
            ],
        )
        .execute()
        .await
        .unwrap();

        // order + scores preserved, ids resolved, attributes loaded
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].val.id, "d2");
        assert_eq!(results[0].score.score(), 2.0);
        assert_eq!(
            results[0].val.attribute("category"),
            Some(&AttributeValue::String("b".to_string()))
        );
        assert_eq!(results[1].val.id, "d1");
        assert_eq!(results[1].score.score(), 1.0);
    }

    #[tokio::test]
    async fn should_skip_ids_missing_from_forward_index() {
        let db = seeded_db().await;
        let d1 = internal_id(&db, "d1").await;
        let missing = VectorId::data_vector_id(9_999_999);

        let results = lookup_op(
            &db,
            vec![
                ScoredVectorId {
                    val: missing,
                    score: BM25Score(5.0),
                },
                ScoredVectorId {
                    val: d1,
                    score: BM25Score(1.0),
                },
            ],
        )
        .execute()
        .await
        .unwrap();

        // the missing id is dropped; the present one survives
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].val.id, "d1");
    }
}

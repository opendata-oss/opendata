//! In-memory delta for buffering vector writes before flush.
//!
//! This module implements the delta pattern for accumulating vector writes
//! in memory before they are atomically flushed to storage. Similar to the
//! timeseries delta, but adapted for vector data with external ID tracking.
//!
//! ## Key Design
//!
//! - Delta is keyed by **external_id** (String), not internal ID
//! - ID dictionary lookups happen at flush time, not during write
//! - No ID allocation during write() - deferred to flush time
//! - Later writes to the same external_id override earlier ones

use std::collections::HashMap;

use anyhow::Result;

use crate::model::{AttributeValue, Config, Vector, attributes_to_map};

/// Builder for accumulating vector writes into a delta.
///
/// This builder validates vectors against the schema but does NOT allocate
/// internal IDs. ID allocation happens atomically during flush.
pub(crate) struct VectorDbDeltaBuilder<'a> {
    config: &'a Config,
    pending_vectors: HashMap<String, PendingVector>,
}

/// A pending vector write buffered in memory.
#[derive(Debug, Clone)]
pub(crate) struct PendingVector {
    external_id: String,
    values: Vec<f32>,
    metadata: HashMap<String, AttributeValue>,
}

impl<'a> VectorDbDeltaBuilder<'a> {
    /// Creates a new delta builder.
    pub(crate) fn new(config: &'a Config) -> Self {
        Self {
            config,
            pending_vectors: HashMap::new(),
        }
    }

    /// Writes a vector to the delta after validation.
    ///
    /// This validates:
    /// - Vector dimensions match config
    /// - Attributes match schema (if schema is defined)
    ///
    /// Does NOT allocate internal IDs - that happens during flush.
    pub(crate) async fn write(&mut self, vector: Vector) -> Result<()> {
        // Validate dimensions
        if vector.values.len() != self.config.dimensions as usize {
            return Err(anyhow::anyhow!(
                "Vector dimension mismatch: expected {}, got {}",
                self.config.dimensions,
                vector.values.len()
            ));
        }

        // Validate external ID length
        if vector.id.len() > 64 {
            return Err(anyhow::anyhow!(
                "External ID too long: {} bytes (max 64)",
                vector.id.len()
            ));
        }

        // Convert attributes to map for easier validation and storage
        let metadata = attributes_to_map(&vector.attributes);

        // Validate attributes against schema (if schema is defined)
        if !self.config.metadata_fields.is_empty() {
            self.validate_attributes(&metadata)?;
        }

        // Store in pending vectors (keyed by external_id)
        // If the same external_id is written multiple times in this delta,
        // the later write overwrites the earlier one
        self.pending_vectors.insert(
            vector.id.clone(),
            PendingVector {
                external_id: vector.id,
                values: vector.values,
                metadata,
            },
        );

        Ok(())
    }

    /// Validates attributes against the configured schema.
    fn validate_attributes(&self, metadata: &HashMap<String, AttributeValue>) -> Result<()> {
        // Build a map of field name -> expected type for quick lookup
        let schema: HashMap<&str, crate::serde::FieldType> = self
            .config
            .metadata_fields
            .iter()
            .map(|spec| (spec.name.as_str(), spec.field_type))
            .collect();

        // Check each provided attribute
        for (field_name, value) in metadata {
            match schema.get(field_name.as_str()) {
                Some(expected_type) => {
                    // Validate type matches
                    let actual_type = match value {
                        AttributeValue::String(_) => crate::serde::FieldType::String,
                        AttributeValue::Int64(_) => crate::serde::FieldType::Int64,
                        AttributeValue::Float64(_) => crate::serde::FieldType::Float64,
                        AttributeValue::Bool(_) => crate::serde::FieldType::Bool,
                    };

                    if actual_type != *expected_type {
                        return Err(anyhow::anyhow!(
                            "Type mismatch for field '{}': expected {:?}, got {:?}",
                            field_name,
                            expected_type,
                            actual_type
                        ));
                    }
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "Unknown metadata field: '{}'. Valid fields: {:?}",
                        field_name,
                        schema.keys().collect::<Vec<_>>()
                    ));
                }
            }
        }

        Ok(())
    }

    /// Builds the final delta from accumulated writes.
    pub(crate) fn build(self) -> VectorDbDelta {
        VectorDbDelta {
            vectors: self.pending_vectors,
        }
    }
}

/// A delta containing pending vector writes.
///
/// Vectors are keyed by external_id. Internal ID allocation happens
/// during flush, not during delta construction.
#[derive(Debug)]
pub(crate) struct VectorDbDelta {
    /// Pending vectors keyed by external_id.
    pub(crate) vectors: HashMap<String, PendingVector>,
}

impl VectorDbDelta {
    /// Creates an empty delta.
    pub(crate) fn empty() -> Self {
        Self {
            vectors: HashMap::new(),
        }
    }

    /// Checks if the delta has no pending writes.
    pub(crate) fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Merges another delta into this one.
    ///
    /// For vectors with the same external_id, the incoming delta's
    /// vector overwrites this delta's vector (later writes win).
    pub(crate) fn merge(&mut self, other: VectorDbDelta) {
        for (external_id, vector) in other.vectors {
            self.vectors.insert(external_id, vector);
        }
    }
}

// Expose PendingVector for use in db.rs during flush
impl PendingVector {
    pub(crate) fn external_id(&self) -> &str {
        &self.external_id
    }

    pub(crate) fn values(&self) -> &[f32] {
        &self.values
    }

    pub(crate) fn metadata(&self) -> &HashMap<String, AttributeValue> {
        &self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MetadataFieldSpec, Vector};
    use crate::serde::FieldType;
    use crate::serde::collection_meta::DistanceMetric;
    use common::StorageConfig;
    use std::time::Duration;

    async fn create_test_builder() -> VectorDbDeltaBuilder<'static> {
        let config = Config {
            storage: StorageConfig::InMemory,
            dimensions: 3,
            distance_metric: DistanceMetric::Cosine,
            flush_interval: Duration::from_secs(60),
            metadata_fields: vec![
                MetadataFieldSpec::new("category", FieldType::String, true),
                MetadataFieldSpec::new("price", FieldType::Int64, true),
            ],
        };

        // Leak to get 'static lifetime for test
        let config_static = Box::leak(Box::new(config));

        VectorDbDeltaBuilder::new(config_static)
    }

    #[tokio::test]
    async fn should_write_valid_vector_to_delta() {
        // given
        let mut builder = create_test_builder().await;
        let vector = Vector::builder("vec-1", vec![1.0, 2.0, 3.0])
            .attribute("category", "shoes")
            .attribute("price", 99i64)
            .build();

        // when
        let result = builder.write(vector).await;

        // then
        assert!(result.is_ok());
        let delta = builder.build();
        assert_eq!(delta.vectors.len(), 1);
        assert!(delta.vectors.contains_key("vec-1"));
    }

    #[tokio::test]
    async fn should_reject_vector_with_wrong_dimensions() {
        // given
        let mut builder = create_test_builder().await;
        let vector = Vector::new("vec-1", vec![1.0, 2.0]); // Wrong: 2 instead of 3

        // when
        let result = builder.write(vector).await;

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("dimension mismatch")
        );
    }

    #[tokio::test]
    async fn should_reject_vector_with_unknown_field() {
        // given
        let mut builder = create_test_builder().await;
        let vector = Vector::builder("vec-1", vec![1.0, 2.0, 3.0])
            .attribute("unknown_field", "value")
            .build();

        // when
        let result = builder.write(vector).await;

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Unknown metadata"));
    }

    #[tokio::test]
    async fn should_reject_vector_with_wrong_type() {
        // given
        let mut builder = create_test_builder().await;
        let vector = Vector::builder("vec-1", vec![1.0, 2.0, 3.0])
            .attribute("price", "not a number") // Wrong: should be i64
            .build();

        // when
        let result = builder.write(vector).await;

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Type mismatch"));
    }

    #[tokio::test]
    async fn should_override_earlier_write_with_same_id() {
        // given
        let mut builder = create_test_builder().await;
        let vector1 = Vector::builder("vec-1", vec![1.0, 2.0, 3.0])
            .attribute("category", "shoes")
            .attribute("price", 99i64)
            .build();
        let vector2 = Vector::builder("vec-1", vec![4.0, 5.0, 6.0])
            .attribute("category", "boots")
            .attribute("price", 199i64)
            .build();

        // when
        builder.write(vector1).await.unwrap();
        builder.write(vector2).await.unwrap();
        let delta = builder.build();

        // then
        assert_eq!(delta.vectors.len(), 1);
        let pending = delta.vectors.get("vec-1").unwrap();
        assert_eq!(pending.values(), &[4.0, 5.0, 6.0]);
    }

    #[tokio::test]
    async fn should_merge_deltas() {
        // given
        let mut delta1 = VectorDbDelta::empty();
        delta1.vectors.insert(
            "vec-1".to_string(),
            PendingVector {
                external_id: "vec-1".to_string(),
                values: vec![1.0, 2.0, 3.0],
                metadata: HashMap::new(),
            },
        );

        let mut delta2 = VectorDbDelta::empty();
        delta2.vectors.insert(
            "vec-2".to_string(),
            PendingVector {
                external_id: "vec-2".to_string(),
                values: vec![4.0, 5.0, 6.0],
                metadata: HashMap::new(),
            },
        );

        // when
        delta1.merge(delta2);

        // then
        assert_eq!(delta1.vectors.len(), 2);
        assert!(delta1.vectors.contains_key("vec-1"));
        assert!(delta1.vectors.contains_key("vec-2"));
    }

    #[tokio::test]
    async fn should_check_if_delta_is_empty() {
        // given
        let empty = VectorDbDelta::empty();
        let mut non_empty = VectorDbDelta::empty();
        non_empty.vectors.insert(
            "vec-1".to_string(),
            PendingVector {
                external_id: "vec-1".to_string(),
                values: vec![1.0, 2.0, 3.0],
                metadata: HashMap::new(),
            },
        );

        // then
        assert!(empty.is_empty());
        assert!(!non_empty.is_empty());
    }

    #[tokio::test]
    async fn should_reject_external_id_too_long() {
        // given
        let mut builder = create_test_builder().await;
        let long_id = "a".repeat(65); // 65 bytes, exceeds max of 64
        let vector = Vector::new(long_id, vec![1.0, 2.0, 3.0]);

        // when
        let result = builder.write(vector).await;

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too long"));
    }
}

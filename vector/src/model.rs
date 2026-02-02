//! Public API types for the vector database (RFC 0002).
//!
//! This module provides the user-facing types for writing vectors to the database.
//! The API is designed to be simple and ergonomic while enforcing necessary constraints
//! like dimension matching and metadata schema validation.

use std::collections::HashMap;
use std::time::Duration;

use common::StorageConfig;

// Re-export types from serde layer
pub use crate::serde::FieldType;
pub use crate::serde::collection_meta::DistanceMetric;

/// A vector with its identifying ID, embedding values, and metadata.
///
/// # Identity
///
/// A vector is uniquely identified by its `id` within a namespace. The ID is
/// a user-provided string (max 64 bytes UTF-8) that serves as an external
/// identifier. The system internally maps external IDs to compact u64 internal
/// IDs for efficient storage and indexing.
///
/// # Upsert Semantics
///
/// Writing a vector with an existing ID replaces the previous vector. The old
/// vector is marked as deleted and a new internal ID is allocated. This ensures
/// posting lists and metadata indexes are updated correctly without expensive
/// read-modify-write cycles.
///
/// # Embedding Values
///
/// The `values` field contains the embedding vector as f32 values. The length
/// must match the `dimensions` specified in the `Config` when the database
/// was created.
#[derive(Debug, Clone)]
pub struct Vector {
    /// User-provided unique identifier (max 64 bytes UTF-8).
    pub id: String,

    /// The embedding vector (f32 values).
    pub values: Vec<f32>,

    /// Metadata attributes for filtering.
    pub attributes: Vec<Attribute>,
}

impl Vector {
    /// Creates a new vector with no attributes.
    pub fn new(id: impl Into<String>, values: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            values,
            attributes: Vec::new(),
        }
    }

    /// Builder-style construction for vectors with attributes.
    pub fn builder(id: impl Into<String>, values: Vec<f32>) -> VectorBuilder {
        VectorBuilder {
            id: id.into(),
            values,
            attributes: Vec::new(),
        }
    }
}

/// Builder for constructing `Vector` instances with attributes.
#[derive(Debug)]
pub struct VectorBuilder {
    id: String,
    values: Vec<f32>,
    attributes: Vec<Attribute>,
}

impl VectorBuilder {
    /// Adds a metadata attribute to the vector.
    pub fn attribute(mut self, name: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        self.attributes
            .push(Attribute::new(name.into(), value.into()));
        self
    }

    /// Builds the final `Vector`.
    pub fn build(self) -> Vector {
        Vector {
            id: self.id,
            values: self.values,
            attributes: self.attributes,
        }
    }
}

/// A metadata attribute attached to a vector.
///
/// Attributes enable filtered vector search by allowing queries like
/// `{category="shoes", price < 100}`. Each attribute has a name and a
/// typed value.
#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub value: AttributeValue,
}

impl Attribute {
    pub fn new(name: impl Into<String>, value: AttributeValue) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// Supported attribute value types.
///
/// These types align with the metadata field types defined in the storage
/// layer (CollectionMeta). Type mismatches at write time will return an error.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

// Convenience From implementations for AttributeValue
impl From<String> for AttributeValue {
    fn from(s: String) -> Self {
        AttributeValue::String(s)
    }
}

impl From<&str> for AttributeValue {
    fn from(s: &str) -> Self {
        AttributeValue::String(s.to_string())
    }
}

impl From<i64> for AttributeValue {
    fn from(v: i64) -> Self {
        AttributeValue::Int64(v)
    }
}

impl From<f64> for AttributeValue {
    fn from(v: f64) -> Self {
        AttributeValue::Float64(v)
    }
}

impl From<bool> for AttributeValue {
    fn from(v: bool) -> Self {
        AttributeValue::Bool(v)
    }
}

/// Configuration for a VectorDb instance.
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage backend configuration.
    ///
    /// Determines where and how vector data is persisted. See [`StorageConfig`]
    /// for available options including in-memory and SlateDB backends.
    pub storage: StorageConfig,

    /// Vector dimensionality (immutable after creation).
    ///
    /// All vectors written to this database must have exactly this many
    /// f32 values. Common dimensions: 384 (MiniLM), 768 (BERT), 1536 (OpenAI).
    pub dimensions: u16,

    /// Distance metric for similarity computation (immutable after creation).
    pub distance_metric: DistanceMetric,

    /// How often to flush data to durable storage.
    pub flush_interval: Duration,

    /// Metadata field schema.
    ///
    /// Defines the expected attribute names and types. Writes with unknown
    /// attribute names or type mismatches will fail. If empty, any attribute
    /// names are accepted with types inferred from the first write.
    pub metadata_fields: Vec<MetadataFieldSpec>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::InMemory,
            dimensions: 0, // Must be set explicitly
            distance_metric: DistanceMetric::Cosine,
            flush_interval: Duration::from_secs(60),
            metadata_fields: Vec::new(),
        }
    }
}

/// Metadata field specification for schema definition.
#[derive(Debug, Clone)]
pub struct MetadataFieldSpec {
    /// Field name.
    pub name: String,

    /// Expected value type.
    pub field_type: FieldType,

    /// Whether this field should be indexed for filtering.
    /// Indexed fields can be used in query predicates.
    pub indexed: bool,
}

impl MetadataFieldSpec {
    pub fn new(name: impl Into<String>, field_type: FieldType, indexed: bool) -> Self {
        Self {
            name: name.into(),
            field_type,
            indexed,
        }
    }
}

/// Helper to convert AttributeValue to the serde layer's FieldValue.
///
/// This is used internally when encoding metadata for storage.
pub(crate) fn attribute_value_to_field_value(attr: &AttributeValue) -> crate::serde::FieldValue {
    match attr {
        AttributeValue::String(s) => crate::serde::FieldValue::String(s.clone()),
        AttributeValue::Int64(v) => crate::serde::FieldValue::Int64(*v),
        AttributeValue::Float64(v) => crate::serde::FieldValue::Float64(*v),
        AttributeValue::Bool(v) => crate::serde::FieldValue::Bool(*v),
    }
}

/// Helper to convert the serde layer's FieldValue to AttributeValue.
///
/// This is used internally when decoding metadata from storage.
pub(crate) fn field_value_to_attribute_value(field: &crate::serde::FieldValue) -> AttributeValue {
    match field {
        crate::serde::FieldValue::String(s) => AttributeValue::String(s.clone()),
        crate::serde::FieldValue::Int64(v) => AttributeValue::Int64(*v),
        crate::serde::FieldValue::Float64(v) => AttributeValue::Float64(*v),
        crate::serde::FieldValue::Bool(v) => AttributeValue::Bool(*v),
    }
}

/// Helper to build a metadata map from attributes.
pub(crate) fn attributes_to_map(attributes: &[Attribute]) -> HashMap<String, AttributeValue> {
    attributes
        .iter()
        .map(|attr| (attr.name.clone(), attr.value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_vector_with_builder() {
        // given/when
        let vector = Vector::builder("test-id", vec![1.0, 2.0, 3.0])
            .attribute("category", "test")
            .attribute("count", 42i64)
            .attribute("score", 0.95)
            .attribute("enabled", true)
            .build();

        // then
        assert_eq!(vector.id, "test-id");
        assert_eq!(vector.values, vec![1.0, 2.0, 3.0]);
        assert_eq!(vector.attributes.len(), 4);
        assert_eq!(vector.attributes[0].name, "category");
        assert_eq!(
            vector.attributes[0].value,
            AttributeValue::String("test".to_string())
        );
    }

    #[test]
    fn should_create_vector_without_attributes() {
        // given/when
        let vector = Vector::new("test-id", vec![1.0, 2.0, 3.0]);

        // then
        assert_eq!(vector.id, "test-id");
        assert_eq!(vector.values, vec![1.0, 2.0, 3.0]);
        assert!(vector.attributes.is_empty());
    }

    #[test]
    fn should_convert_str_to_attribute_value() {
        // given
        let value: AttributeValue = "test".into();

        // then
        assert_eq!(value, AttributeValue::String("test".to_string()));
    }

    #[test]
    fn should_convert_int_to_attribute_value() {
        // given
        let value: AttributeValue = 42i64.into();

        // then
        assert_eq!(value, AttributeValue::Int64(42));
    }

    #[test]
    fn should_convert_attributes_to_map() {
        // given
        let attributes = vec![
            Attribute::new("name", AttributeValue::String("test".to_string())),
            Attribute::new("count", AttributeValue::Int64(42)),
        ];

        // when
        let map = attributes_to_map(&attributes);

        // then
        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get("name"),
            Some(&AttributeValue::String("test".to_string()))
        );
        assert_eq!(map.get("count"), Some(&AttributeValue::Int64(42)));
    }
}

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

/// Reserved field name for the embedding vector stored as an AttributeValue::Vector.
pub const VECTOR_FIELD_NAME: &str = "vector";

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
/// The embedding vector must be provided as an attribute with name "vector"
/// and type `AttributeValue::Vector`. The length must match the `dimensions`
/// specified in the `Config` when the database was created.
#[derive(Debug, Clone)]
pub struct Vector {
    /// User-provided unique identifier (max 64 bytes UTF-8).
    pub id: String,

    /// Attributes including the embedding vector (under "vector" field) and metadata.
    pub attributes: Vec<Attribute>,
}

impl Vector {
    /// Creates a new vector with just the embedding values (no other attributes).
    ///
    /// The vector values are stored as an attribute with name [`VECTOR_FIELD_NAME`].
    pub fn new(id: impl Into<String>, values: Vec<f32>) -> Self {
        Self {
            id: id.into(),
            attributes: vec![Attribute::new(
                VECTOR_FIELD_NAME,
                AttributeValue::Vector(values),
            )],
        }
    }

    /// Builder-style construction for vectors with attributes.
    ///
    /// The vector values are stored as an attribute with name [`VECTOR_FIELD_NAME`].
    pub fn builder(id: impl Into<String>, values: Vec<f32>) -> VectorBuilder {
        VectorBuilder {
            id: id.into(),
            attributes: vec![Attribute::new(
                VECTOR_FIELD_NAME,
                AttributeValue::Vector(values),
            )],
        }
    }
}

/// Builder for constructing `Vector` instances with attributes.
#[derive(Debug)]
pub struct VectorBuilder {
    id: String,
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
    Vector(Vec<f32>),
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

impl From<crate::serde::FieldValue> for AttributeValue {
    fn from(field: crate::serde::FieldValue) -> Self {
        match field {
            crate::serde::FieldValue::String(s) => AttributeValue::String(s),
            crate::serde::FieldValue::Int64(v) => AttributeValue::Int64(v),
            crate::serde::FieldValue::Float64(v) => AttributeValue::Float64(v),
            crate::serde::FieldValue::Bool(v) => AttributeValue::Bool(v),
            crate::serde::FieldValue::Vector(v) => AttributeValue::Vector(v),
        }
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

    /// Number of vectors in a centroid's posting list that triggers a split.
    pub split_threshold_vectors: usize,

    /// Number of vectors below which a centroid's posting list triggers a merge.
    pub merge_threshold_vectors: usize,

    /// Number of neighboring centroids to scan for reassignment candidates after a split.
    pub split_search_neighbourhood: usize,

    /// The maximum number of centroids that require rebalancing before which backpressure
    /// is applied by pausing ingestion of new vector writes.
    pub max_pending_and_running_rebalance_tasks: usize,

    /// After backpressure is applied, ingestion resumes after the total number of centroids
    /// requiring rebalance drops below this value.
    pub rebalance_backpressure_resume_threshold: usize,

    /// The maximum number of rebalance tasks that the rebalancer will run concurrently.
    pub max_rebalance_tasks: usize,

    /// Target number of centroids per chunk.
    pub chunk_target: u16,

    /// Query-aware dynamic pruning epsilon (ε₂ from SPANN paper).
    ///
    /// When set, a posting list is searched only if its centroid's distance
    /// to the query satisfies `dist(q, c) <= (1 + epsilon) * dist(q, closest)`.
    /// This reduces query latency by skipping distant posting lists while
    /// preserving recall.
    ///
    /// Typical values: 0.1 to 0.5. `None` disables pruning (all nprobe
    /// posting lists are searched).
    pub query_pruning_factor: Option<f32>,

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
            distance_metric: DistanceMetric::L2,
            flush_interval: Duration::from_secs(60),
            split_threshold_vectors: 2_000,
            merge_threshold_vectors: 500,
            split_search_neighbourhood: 16,
            max_pending_and_running_rebalance_tasks: 16,
            rebalance_backpressure_resume_threshold: 8,
            max_rebalance_tasks: 8,
            chunk_target: 4096,
            query_pruning_factor: None,
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

/// A search result with vector, score, and metadata.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal vector ID
    pub internal_id: u64,
    /// External vector ID (user-provided)
    pub external_id: String,
    /// Similarity score (interpretation depends on distance metric)
    ///
    /// - L2: Lower scores = more similar
    /// - Cosine: Higher scores = more similar (range: -1 to 1)
    /// - DotProduct: Higher scores = more similar
    pub score: f32,
    /// Attribute key-value pairs
    pub attributes: HashMap<String, AttributeValue>,
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
        // 5 attributes: vector + category + count + score + enabled
        assert_eq!(vector.attributes.len(), 5);
        // First attribute is "vector"
        assert_eq!(vector.attributes[0].name, "vector");
        assert_eq!(
            vector.attributes[0].value,
            AttributeValue::Vector(vec![1.0, 2.0, 3.0])
        );
        // Second attribute is "category"
        assert_eq!(vector.attributes[1].name, "category");
        assert_eq!(
            vector.attributes[1].value,
            AttributeValue::String("test".to_string())
        );
    }

    #[test]
    fn should_create_vector_without_extra_attributes() {
        // given/when
        let vector = Vector::new("test-id", vec![1.0, 2.0, 3.0]);

        // then
        assert_eq!(vector.id, "test-id");
        // Only the "vector" attribute
        assert_eq!(vector.attributes.len(), 1);
        assert_eq!(vector.attributes[0].name, "vector");
        assert_eq!(
            vector.attributes[0].value,
            AttributeValue::Vector(vec![1.0, 2.0, 3.0])
        );
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

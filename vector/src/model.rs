//! Public API types for the vector database (RFC 0002).
//!
//! This module provides the user-facing types for writing vectors to the database.
//! The API is designed to be simple and ergonomic while enforcing necessary constraints
//! like dimension matching and metadata schema validation.

use std::collections::HashMap;
use std::time::Duration;

#[cfg(feature = "buffer")]
use common::ObjectStoreConfig;
use common::StorageConfig;
use serde::{Deserialize, Serialize};

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

    pub fn attribute(&self, name: &str) -> Option<&AttributeValue> {
        self.attributes
            .iter()
            .filter(|a| a.name == name)
            .map(|a| &a.value)
            .next()
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// How often to flush data to durable storage (in seconds).
    #[serde(with = "duration_secs")]
    pub flush_interval: Duration,

    /// Number of vectors in a centroid's posting list that triggers a split.
    pub split_threshold_vectors: usize,

    /// Number of vectors below which a centroid's posting list triggers a merge.
    pub merge_threshold_vectors: usize,

    /// Number of neighboring centroids to scan for reassignment candidates after a split.
    pub split_search_neighbourhood: usize,

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

    /// Buffer consumer configuration. When `Some`, the server starts a
    /// background task that ingests vectors from an `opendata-buffer` queue.
    #[cfg(feature = "buffer")]
    #[serde(default)]
    pub buffer_consumer: Option<BufferConsumerConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::InMemory,
            dimensions: 0, // Must be set explicitly
            distance_metric: DistanceMetric::L2,
            flush_interval: Duration::from_secs(60),
            split_threshold_vectors: 150,
            merge_threshold_vectors: 50,
            split_search_neighbourhood: 0,
            query_pruning_factor: None,
            metadata_fields: Vec::new(),
            #[cfg(feature = "buffer")]
            buffer_consumer: None,
        }
    }
}

/// Configuration for the embedded buffer consumer task.
///
/// Mirrors [`buffer::ConsumerConfig`], plus a poll interval used by the
/// vector consumer when the queue is empty.
#[cfg(feature = "buffer")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConsumerConfig {
    /// Object store where the buffer queue lives. Must match the producer.
    pub object_store: ObjectStoreConfig,

    /// Path to the queue manifest in object storage. Must match the producer.
    #[serde(default = "default_buffer_manifest_path")]
    pub manifest_path: String,

    /// Path prefix for data batch objects. Must match the producer.
    #[serde(default = "default_buffer_data_path_prefix")]
    pub data_path_prefix: String,

    /// Poll interval when the queue is empty.
    #[serde(default = "default_buffer_poll_interval", with = "duration_secs")]
    pub poll_interval: Duration,

    /// How often the garbage collector runs.
    #[serde(default = "default_buffer_gc_interval", with = "duration_secs")]
    pub gc_interval: Duration,

    /// Minimum age before an unreferenced batch file is deleted.
    #[serde(default = "default_buffer_gc_grace_period", with = "duration_secs")]
    pub gc_grace_period: Duration,
}

#[cfg(feature = "buffer")]
fn default_buffer_manifest_path() -> String {
    "ingest/manifest".to_string()
}

#[cfg(feature = "buffer")]
fn default_buffer_data_path_prefix() -> String {
    "ingest".to_string()
}

#[cfg(feature = "buffer")]
fn default_buffer_poll_interval() -> Duration {
    Duration::from_millis(100)
}

#[cfg(feature = "buffer")]
fn default_buffer_gc_interval() -> Duration {
    Duration::from_secs(300)
}

#[cfg(feature = "buffer")]
fn default_buffer_gc_grace_period() -> Duration {
    Duration::from_secs(600)
}

/// Options for search operations.
#[derive(Debug, Clone, Default)]
pub struct SearchOptions {
    /// Number of centroids to probe during search.
    pub nprobe: Option<usize>,
}

/// Configuration for a read-only vector database client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderConfig {
    /// Storage backend configuration.
    pub storage: StorageConfig,

    /// Vector dimensionality.
    pub dimensions: u16,

    /// Distance metric for similarity computation.
    pub distance_metric: DistanceMetric,

    /// Query-aware dynamic pruning epsilon (SPANN §3.2).
    ///
    /// See [`Config::query_pruning_factor`] for details.
    pub query_pruning_factor: Option<f32>,

    /// Metadata field schema.
    pub metadata_fields: Vec<MetadataFieldSpec>,
}

/// Metadata field specification for schema definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Similarity score (interpretation depends on distance metric)
    ///
    /// - L2: Lower scores = more similar
    /// - DotProduct: Higher scores = more similar
    pub score: f32,
    /// The vector found by search
    pub vector: Vector,
}

/// Specifies which fields to include in query results.
///
/// Controls which attributes are returned in search results,
/// reducing data transfer when only specific fields are needed.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldSelection {
    /// Include all fields (vector and all metadata).
    All,
    /// Include no fields (only IDs and scores).
    None,
    /// Include specific fields by name (e.g., `["category", "price"]`).
    /// Use `"vector"` to include the embedding vector.
    Fields(Vec<String>),
}

impl From<bool> for FieldSelection {
    fn from(include: bool) -> Self {
        if include {
            FieldSelection::All
        } else {
            FieldSelection::None
        }
    }
}

impl From<Vec<&str>> for FieldSelection {
    fn from(fields: Vec<&str>) -> Self {
        FieldSelection::Fields(fields.into_iter().map(String::from).collect())
    }
}

impl From<Vec<String>> for FieldSelection {
    fn from(fields: Vec<String>) -> Self {
        FieldSelection::Fields(fields)
    }
}

/// Parameters for a BM25 full-text-search query (RFC-0006).
#[derive(Debug, Clone)]
pub struct Bm25Query {
    /// Text field to search against. Must be declared as `FieldType::Text`.
    pub field: String,
    /// Query text. Tokenized with the same tokenizer used at write time.
    pub query: String,
}

/// How a query should score documents.
#[derive(Debug, Clone)]
pub enum ScoreBy {
    /// Approximate-nearest-neighbour search on the embedding vector.
    Ann(Vec<f32>),
    /// BM25 full-text search on a `FieldType::Text` field.
    Bm25(Bm25Query),
}

/// Query specification for vector search.
///
/// Constructed using the builder pattern:
///
/// ```ignore
/// let ann = Query::ann(embedding)
///     .with_limit(10)
///     .with_filter(Filter::eq("category", "shoes"));
/// let bm25 = Query::bm25("body", "fox jumps").with_limit(5);
/// ```
#[derive(Debug, Clone)]
pub struct Query {
    /// Method used to score documents (ANN or BM25).
    pub score_by: ScoreBy,
    /// Maximum number of results to return (default: 10).
    pub limit: usize,
    /// Optional metadata filter.
    pub filter: Option<Filter>,
    /// Which fields to include in results (default: All).
    pub include_fields: FieldSelection,
}

impl Query {
    /// Creates a new ANN query with the given vector.
    ///
    /// Alias for [`Query::ann`]; kept for ergonomics so existing call sites
    /// that read as `Query::new(vec)` continue to work.
    pub fn new(vector: Vec<f32>) -> Self {
        Self::ann(vector)
    }

    /// Creates a new ANN query with the given vector.
    pub fn ann(vector: Vec<f32>) -> Self {
        Self {
            score_by: ScoreBy::Ann(vector),
            limit: 10,
            filter: None,
            include_fields: FieldSelection::All,
        }
    }

    /// Creates a new BM25 query for the given text field.
    pub fn bm25(field: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            score_by: ScoreBy::Bm25(Bm25Query {
                field: field.into(),
                query: query.into(),
            }),
            limit: 10,
            filter: None,
            include_fields: FieldSelection::All,
        }
    }

    /// Sets the maximum number of results to return.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Sets the metadata filter.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Controls which fields are included in results.
    ///
    /// Accepts `true`/`false` for all/none, or `Vec<&str>`/`Vec<String>` for specific fields.
    pub fn with_fields(mut self, fields: impl Into<FieldSelection>) -> Self {
        self.include_fields = fields.into();
        self
    }

    /// Returns the ANN query vector, if this query scores by ANN.
    pub fn ann_vector(&self) -> Option<&Vec<f32>> {
        match &self.score_by {
            ScoreBy::Ann(v) => Some(v),
            ScoreBy::Bm25(_) => None,
        }
    }
}

/// Metadata filter for search queries.
///
/// Filters are composed using simple predicates and logical operators.
/// All filters are evaluated against the metadata inverted indexes.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Field equals value.
    Eq(String, AttributeValue),
    /// Field not equals value.
    Neq(String, AttributeValue),
    /// Field is in set of values.
    In(String, Vec<AttributeValue>),
    /// All filters must match (logical AND).
    And(Vec<Filter>),
    /// Any filter must match (logical OR).
    Or(Vec<Filter>),
}

impl Filter {
    /// Creates an equality filter.
    pub fn eq(field: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        Filter::Eq(field.into(), value.into())
    }

    /// Creates a not-equals filter.
    pub fn neq(field: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        Filter::Neq(field.into(), value.into())
    }

    /// Creates an in-set filter.
    pub fn in_set(field: impl Into<String>, values: Vec<AttributeValue>) -> Self {
        Filter::In(field.into(), values)
    }

    /// Combines filters with logical AND.
    pub fn and(filters: Vec<Filter>) -> Self {
        Filter::And(filters)
    }

    /// Combines filters with logical OR.
    pub fn or(filters: Vec<Filter>) -> Self {
        Filter::Or(filters)
    }

    /// Evaluate this filter against an attribute map (CPU-side reference
    /// semantics). Useful for computing filtered ground truth without going
    /// through the index.
    ///
    /// Semantics for well-formed metadata (every row carries the indexed
    /// fields): `Eq` matches when the field is present and equal; `Neq`
    /// matches when the field is absent or not equal; `In` matches when the
    /// field is present and its value is in the set; `And`/`Or` combine.
    pub fn matches(&self, attributes: &HashMap<String, AttributeValue>) -> bool {
        match self {
            Filter::Eq(field, value) => attributes.get(field) == Some(value),
            Filter::Neq(field, value) => attributes.get(field) != Some(value),
            Filter::In(field, values) => attributes.get(field).is_some_and(|v| values.contains(v)),
            Filter::And(filters) => filters.iter().all(|f| f.matches(attributes)),
            Filter::Or(filters) => filters.iter().any(|f| f.matches(attributes)),
        }
    }
}

/// Helper to build a metadata map from attributes.
pub(crate) fn attributes_to_map(attributes: &[Attribute]) -> HashMap<String, AttributeValue> {
    attributes
        .iter()
        .map(|attr| (attr.name.clone(), attr.value.clone()))
        .collect()
}

mod duration_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(d.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
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

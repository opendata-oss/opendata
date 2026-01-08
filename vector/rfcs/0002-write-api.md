# RFC 0002: Vector Write API

**Status**: Draft

**Authors**:

- [Almog Gavra](https://github.com/agavra)

## Summary

This RFC proposes a public write API for OpenData-Vector, modeled after the `TimeSeries` API in the
timeseries module and `Db` in SlateDB. The `VectorDb` struct provides a clean, high-level interface
for ingesting embedding vectors with metadata.

## Motivation

A dedicated write API enables flexible vector ingestion independent of any particular embedding
provider or data format. While many vector databases couple ingestion with specific embedding models
or HTTP protocols (OTEL, OpenAI, etc.), OpenData-Vector aims to provide a minimal, unopinionated
core. Higher-level integrations can be layered on top without constraining the fundamental data
model. This approach mirrors the OpenData design philosophy.

## Goals

- Provide a minimal API for vector ingestion
- Expose batched write and delete operations with atomic semantics
- Abstract over internal SPANN indexing and storage details
- Follow patterns established by the `TimeSeries` API and SlateDB's `Db`

## Non-Goals

- **Query API** - A `VectorDbReader` interface will be defined in a follow-up RFC
- **HTTP endpoints** - REST/gRPC APIs are out of scope
- **Embedding generation** - No built-in embedding models; vectors are pre-computed
- **Schema evolution** - Adding/removing metadata fields after creation is out of scope
- **Streaming ingestion** - Bulk import optimizations are out of scope

## Design

### Data Model

The public data model represents vectors as user-identified embedding arrays with typed metadata
attributes.

#### Vector

A vector is the primary unit of ingestion, combining an ID, embedding values, and optional metadata.

```rust
/// A vector with its identifying ID, embedding values, and metadata.
///
/// # Identity
///
/// A vector is uniquely identified by its `id` within a namespace. The ID is
/// a user-provided u32 that must be unique. Writing to an existing ID or a
/// previously deleted ID is undefined behavior; callers must ensure IDs are
/// never reused.
///
/// # Embedding Values
///
/// The `values` field contains the embedding vector as f32 values. The length
/// must match the `dimensions` specified in the `Config` when the database
/// was created.
#[derive(Debug, Clone)]
pub struct Vector {
    /// User-provided unique identifier.
    pub id: u32,

    /// The embedding vector (f32 values).
    pub values: Vec<f32>,

    /// Metadata attributes for filtering.
    pub attributes: Vec<Attribute>,
}

impl Vector {
    /// Creates a new vector with no attributes.
    pub fn new(id: u32, values: Vec<f32>) -> Self;

    /// Builder-style construction for vectors with attributes.
    pub fn builder(id: u32, values: Vec<f32>) -> VectorBuilder;
}

pub struct VectorBuilder {
    ...
}

impl VectorBuilder {
    pub fn attribute(self, name: impl Into<String>, value: impl Into<AttributeValue>) -> Self;
    pub fn build(self) -> Vector;
}
```

#### Attributes

Attributes are typed key-value pairs that enable filtered vector search.

```rust
/// A metadata attribute attached to a vector.
#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    pub name: String,
    pub value: AttributeValue,
}

impl Attribute {
    pub fn new(name: impl Into<String>, value: impl Into<AttributeValue>) -> Self;
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

// Convenience From implementations
impl From<String> for AttributeValue { ... }
impl From<&str> for AttributeValue { ... }
impl From<i64> for AttributeValue { ... }
impl From<f64> for AttributeValue { ... }
impl From<bool> for AttributeValue { ... }
```

### VectorDb API

#### Construction

```rust
/// A vector database for storing and querying embedding vectors.
///
/// `VectorDb` provides a high-level API for ingesting vectors with metadata.
/// It handles internal details like SPANN indexing, centroid assignment,
/// and metadata index maintenance automatically.
///
/// # Example
///
/// ```rust
/// use opendata_vector::{VectorDb, Config, Vector, DistanceMetric};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = Config {
///         dimensions: 384,
///         distance_metric: DistanceMetric::Cosine,
///         ..Default::default()
///     };
///     let db = VectorDb::open(config).await?;
///
///     let vectors = vec![
///         Vector::builder(1, vec![0.1; 384])
///             .attribute("category", "electronics")
///             .attribute("price", 99i64)
///             .build(),
///         Vector::builder(2, vec![0.2; 384])
///             .attribute("category", "clothing")
///             .attribute("price", 49i64)
///             .build(),
///     ];
///
///     db.write(vectors).await?;
///     Ok(())
/// }
/// ```
pub struct VectorDb {
    // Internal storage - not exposed
}

impl VectorDb {
    /// Open or create a vector database with the given configuration.
    ///
    /// If the database already exists, the configuration must be compatible:
    /// - `dimensions` must match exactly
    /// - `distance_metric` must match exactly
    ///
    /// Other configuration options (like `flush_interval`) can be changed
    /// on subsequent opens.
    pub async fn open(config: Config) -> Result<Self>;
}
```

#### Configuration

```rust
/// Configuration for a VectorDb instance.
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage backend configuration.
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
            storage: StorageConfig::default(),
            dimensions: 0, // Must be set explicitly
            distance_metric: DistanceMetric::Cosine,
            flush_interval: Duration::from_secs(60),
            metadata_fields: Vec::new(),
        }
    }
}

/// Distance metric for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance: sqrt(sum((x - y)^2))
    /// Lower is more similar.
    L2,

    /// Cosine distance: 1 - cosine_similarity
    /// Range [0, 2]. Lower is more similar.
    #[default]
    Cosine,

    /// Dot product distance: -dot(x, y)
    /// More negative is more similar (vectors should be normalized).
    DotProduct,
}

/// Metadata field specification for schema definition.
#[derive(Debug, Clone)]
pub struct MetadataFieldSpec {
    /// Field name.
    pub name: String,

    /// Expected value type.
    pub field_type: MetadataFieldType,

    /// Whether this field should be indexed for filtering.
    /// Indexed fields can be used in query predicates.
    pub indexed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataFieldType {
    String,
    Int64,
    Float64,
    Bool,
}
```

#### Write API

```rust
impl VectorDb {
    /// Write vectors to the database.
    ///
    /// This is the primary write method. It accepts a batch of vectors and
    /// returns when the data has been accepted for ingestion (but not
    /// necessarily flushed to durable storage).
    ///
    /// # Atomicity
    ///
    /// This operation is atomic: either all vectors in the batch are accepted,
    /// or none are. This matches the behavior of `TimeSeries::write()`.
    ///
    /// # ID Uniqueness
    ///
    /// Writing a vector with an ID that already exists or was previously
    /// deleted is undefined behavior. Callers must ensure IDs are never
    /// reused within a namespace.
    ///
    /// # Validation
    ///
    /// The following validations are performed:
    /// - Vector dimensions must match `Config::dimensions`
    /// - Attribute names must be defined in `Config::metadata_fields` (if specified)
    /// - Attribute types must match the schema
    ///
    /// # Example
    ///
    /// ```rust
    /// let vectors = vec![
    ///     Vector::builder(1, embedding_1)
    ///         .attribute("category", "shoes")
    ///         .attribute("price", 79i64)
    ///         .build(),
    ///     Vector::builder(2, embedding_2)
    ///         .attribute("category", "shirts")
    ///         .attribute("price", 45i64)
    ///         .build(),
    /// ];
    ///
    /// db.write(vectors).await?;
    /// ```
    pub async fn write(&self, vectors: Vec<Vector>) -> Result<()>;

    /// Write with custom options.
    ///
    /// Allows control over durability guarantees.
    pub async fn write_with_options(
        &self,
        vectors: Vec<Vector>,
        options: WriteOptions,
    ) -> Result<()>;

    /// Force flush all pending data to durable storage.
    ///
    /// Normally data is flushed according to `flush_interval`, but this
    /// method can be used to ensure durability immediately.
    pub async fn flush(&self) -> Result<()>;
}

/// Options for write operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Wait for data to be flushed to durable storage before returning.
    /// Default: false (return immediately after buffering)
    pub await_durable: bool,
}
```

#### Delete API

```rust
impl VectorDb {
    /// Delete vectors by ID.
    ///
    /// Removes the specified vectors from the database. Deleted vectors
    /// will no longer appear in search results.
    ///
    /// # Atomicity
    ///
    /// This operation is atomic: either all deletions succeed, or none do.
    ///
    /// # Non-Existent IDs
    ///
    /// Deleting a vector ID that doesn't exist is not an error. This enables
    /// idempotent delete operations.
    ///
    /// # Implementation
    ///
    /// Deletion is a soft delete: vector IDs are added to a deleted bitmap
    /// that is checked during search. The actual data is cleaned up during
    /// background LIRE maintenance.
    ///
    /// # ID Reuse
    ///
    /// Deleted IDs cannot be reused. The deleted bitmap persists until LIRE
    /// maintenance, and re-inserting a deleted ID would cause the new vector
    /// to be incorrectly filtered from search results.
    ///
    /// # Example
    ///
    /// ```rust
    /// // Delete specific vectors
    /// db.delete(vec![1, 2, 3]).await?;
    ///
    /// // Idempotent - deleting again is fine
    /// db.delete(vec![1, 2, 3]).await?;
    /// ```
    pub async fn delete(&self, ids: Vec<u32>) -> Result<()>;
}
```

#### Reader Access (Placeholder)

```rust
impl VectorDb {
    /// Get a read-only view of the vector database.
    ///
    /// The reader provides query access without write capabilities.
    /// See RFC-XXXX for the VectorDbReader API.
    pub fn reader(&self) -> VectorDbReader;
}

/// Read-only view of a vector database.
///
/// API to be defined in a future RFC.
pub struct VectorDbReader {
    // ...
}
```

### Write Path

When `write()` is called, the following operations occur:

1. **Validation**: Check that all vectors have the correct dimensions and valid attributes
2. **For each vector**:
    - Write `VectorData` record with the embedding values
    - Write `VectorMeta` record with the attributes
    - Update `MetadataIndex` entries via merge operators for indexed fields
    - Assign to nearest centroid(s) and update `PostingList` via merge operators
3. **Return**: Success after buffering (or after flush if `await_durable`)

### Delete Path

When `delete()` is called:

1. **Add to deleted bitmap**: Vector IDs are added to the special posting list at `centroid_id=0`
2. **Tombstone records**: `VectorData` and `VectorMeta` records are tombstoned
3. **Deferred cleanup**: Metadata index entries and posting list entries are cleaned up during LIRE
   maintenance

### Comparison with TimeSeries API

| Aspect               | TimeSeries                           | VectorDb                            |
|----------------------|--------------------------------------|-------------------------------------|
| Configuration        | `Config`                             | `Config`                            |
| Primary write method | `write(Vec<Series>)`                 | `write(Vec<Vector>)`                |
| Options variant      | `write_with_options()`               | `write_with_options()`              |
| Data unit            | `Series` (labels + samples)          | `Vector` (id + values + attributes) |
| Identification       | Labels (including `__name__`)        | `id: u32`                           |
| Metadata             | `metric_type`, `unit`, `description` | `attributes: Vec<Attribute>`        |
| Write semantics      | Append samples                       | Insert (no duplicates)              |
| Delete method        | N/A                                  | `delete(Vec<u32>)`                  |
| Durability control   | `WriteOptions::await_durable`        | `WriteOptions::await_durable`       |
| Reader access        | `fn reader() -> TimeSeriesReader`    | `fn reader() -> VectorDbReader`     |

## Alternatives

### Upsert Semantics

**Alternative**: Writing a vector with an existing ID overwrites the previous vector and metadata
automatically.

**Rationale for rejection**: Upsert requires finding and removing stale index entries, which is
expensive:

1. **Posting list cleanup**: The old vector may be assigned to different centroids than the new one.
   To remove stale posting list entries, we must either:
   - Store a reverse index (`vector_id → centroid_ids`) adding storage overhead and write
     amplification
   - Store centroid assignments in `VectorMeta`, requiring a read before every write
   - Scan all posting lists (prohibitively expensive)

2. **Metadata index cleanup**: If indexed attribute values change, we must remove the vector from
   old `MetadataIndex` bitmaps and add to new ones. This requires reading old metadata to determine
   which entries to remove.

3. **Cost per upsert**: At minimum, upsert requires O(1) reads + O(k) posting list updates + O(m)
   metadata index updates, where k is the number of centroid assignments (~2) and m is the number
   of changed indexed attributes. This is significantly more expensive than insert.

Note that delete-then-reinsert is also not supported: deleted IDs remain in the deleted bitmap until
LIRE maintenance, so a re-inserted vector would be incorrectly filtered from search results.

**Future consideration**: A future RFC may add upsert support with explicit cost documentation,
potentially via a `VectorCentroids` reverse index record type to track centroid assignments and
enable efficient cleanup of both posting lists and the deleted bitmap.

### Partial Success on Batch Writes

**Alternative**: Accept valid vectors from a batch even if some fail validation.

```rust
pub struct WriteResult {
    pub accepted: usize,
    pub rejected: Vec<(usize, Error)>,
}
```

**Rationale for rejection**: Atomic semantics are simpler to reason about and match the TimeSeries
API. Partial writes complicate error handling and can lead to inconsistent state if the caller
doesn't handle rejections correctly.

**Future consideration**: A separate `write_partial()` method could be added if needed.

### String IDs Instead of u32

**Alternative**: Use string IDs for vectors instead of u32.

**Rationale for rejection**: The storage layer uses u32 for efficient bitmap compression (
RoaringBitmap). String IDs would require an additional mapping layer and reduce storage efficiency.
Applications needing string IDs can maintain their own mapping.

**Trade-offs**: Users must manage ID allocation and uniqueness. For many workloads, this is
acceptable since they already have compact ID schemes or can maintain a simple mapping table.

### Native Delete-by-Filter

**Alternative**: Add a `delete_by_filter()` method that deletes vectors matching a metadata
predicate.

```rust
pub async fn delete_by_filter(&self, filter: Filter) -> Result<usize>;
```

**Rationale for rejection**: This is a query operation that requires the read API (filtering by
metadata). It can be added in the query RFC as a convenience method built on top of the reader.

## Updates

| Date       | Description                                                    |
|------------|----------------------------------------------------------------|
| 2026-01-07 | Initial draft                                                  |

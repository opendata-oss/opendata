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
    pub fn new(id: impl Into<String>, values: Vec<f32>) -> Self;

    /// Builder-style construction for vectors with attributes.
    pub fn builder(id: impl Into<String>, values: Vec<f32>) -> VectorBuilder;
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
///
/// The Vector variant stores embedding vectors and enables treating the vector
/// as a special attribute alongside other metadata fields. This provides a unified
/// data model where all attributes (including the vector) can be stored and retrieved
/// consistently.
///
/// Vector is an API-level abstraction—at the storage layer, embeddings are stored in
/// VectorData records. Metadata types (String, Int64, Float64, Bool) map to storage
/// layer type IDs in VectorMeta (RFC 0001): String=0, Int64=1, Float64=2, Bool=3.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    /// Vector embedding (f32 array). This enables treating the embedding vector as
    /// a special attribute with the reserved field name "vector". Stored in VectorData,
    /// not VectorMeta.
    Vector(Vec<f32>),
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

// Convenience From implementations
impl From<Vec<f32>> for AttributeValue { ... }
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
///         Vector::builder("product-001", vec![0.1; 384])
///             .attribute("category", "electronics")
///             .attribute("price", 99i64)
///             .build(),
///         Vector::builder("product-002", vec![0.2; 384])
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
    /// Vector embedding type. Use the reserved field name "vector" to define
    /// the embedding field in the schema. API-level only; stored in VectorData.
    Vector,
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
    /// # Upsert Semantics
    ///
    /// Writing a vector with an ID that already exists performs an upsert:
    /// the old vector is deleted and replaced with the new one. The system
    /// allocates a new internal ID for the updated vector and marks the old
    /// internal ID as deleted. This ensures index structures are updated
    /// correctly without expensive read-modify-write cycles.
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
    ///     Vector::builder("shoe-123", embedding_1)
    ///         .attribute("category", "shoes")
    ///         .attribute("price", 79i64)
    ///         .build(),
    ///     Vector::builder("shirt-456", embedding_2)
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
    /// Deletion is a soft delete:
    /// 1. The external ID is looked up in the IdDictionary to find the internal ID
    /// 2. The internal ID is added to the deleted bitmap (centroid_id=0)
    /// 3. VectorData and VectorMeta records are tombstoned
    /// 4. The IdDictionary entry is tombstoned
    /// 5. Metadata index cleanup happens during background LIRE maintenance
    ///
    /// # ID Reuse
    ///
    /// Deleted external IDs can be reused. Writing a vector with a previously
    /// deleted ID will succeed and create a new vector with a new internal ID.
    /// The old internal ID remains in the deleted bitmap until LIRE maintenance.
    ///
    /// # Example
    ///
    /// ```rust
    /// // Delete specific vectors
    /// db.delete(vec!["product-001", "product-002", "product-003"]).await?;
    ///
    /// // Idempotent - deleting again is fine
    /// db.delete(vec!["product-001", "product-002", "product-003"]).await?;
    /// ```
    pub async fn delete(&self, ids: Vec<String>) -> Result<()>;
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
    - Look up the external ID in `IdDictionary`
    - If found (upsert case):
        - Delete the old vector: add old internal ID to deleted bitmap (centroid_id=0)
        - Tombstone old `VectorData` and `VectorMeta` records
    - Allocate a new internal ID using block-based sequence allocation (see `SeqBlock` in storage RFC)
    - Write `VectorData` record with the embedding values (keyed by new internal ID)
    - Write `VectorMeta` record with the external ID and attributes (keyed by new internal ID)
    - Update or create `IdDictionary` entry mapping external ID → new internal ID
    - Update `MetadataIndex` entries via merge operators for indexed fields
    - Assign to nearest centroid(s) and update `PostingList` via merge operators
3. **Return**: Success after buffering (or after flush if `await_durable`)

### Delete Path

When `delete()` is called:

1. **Look up internal IDs**: For each external ID, look up the internal ID in `IdDictionary`
2. **Add to deleted bitmap**: Internal IDs are added to the special posting list at `centroid_id=0`
3. **Tombstone records**: `VectorData`, `VectorMeta`, and `IdDictionary` records are tombstoned
4. **Deferred cleanup**: Metadata index entries and posting list entries are cleaned up during LIRE
   maintenance

### Comparison with TimeSeries API

| Aspect               | TimeSeries                           | VectorDb                            |
|----------------------|--------------------------------------|-------------------------------------|
| Configuration        | `Config`                             | `Config`                            |
| Primary write method | `write(Vec<Series>)`                 | `write(Vec<Vector>)`                |
| Options variant      | `write_with_options()`               | `write_with_options()`              |
| Data unit            | `Series` (labels + samples)          | `Vector` (id + values + attributes) |
| Identification       | Labels (including `__name__`)        | `id: String` (external ID)          |
| Metadata             | `metric_type`, `unit`, `description` | `attributes: Vec<Attribute>`        |
| Write semantics      | Append samples                       | Upsert (replace existing)           |
| Delete method        | N/A                                  | `delete(Vec<String>)`               |
| Durability control   | `WriteOptions::await_durable`        | `WriteOptions::await_durable`       |
| Reader access        | `fn reader() -> TimeSeriesReader`    | `fn reader() -> VectorDbReader`     |

## Alternatives

### Upsert Semantics

**Decision**: Upsert semantics are supported by default.

**Implementation**: When writing a vector with an existing external ID, the system:
1. Looks up the old internal ID from `IdDictionary`
2. Marks the old internal ID as deleted (adds to centroid_id=0 bitmap)
3. Tombstones the old `VectorData` and `VectorMeta` records
4. Allocates a new internal ID for the updated vector
5. Writes new records with the new internal ID
6. Updates `IdDictionary` to point external ID → new internal ID

This "delete old + insert new" approach avoids expensive read-modify-write cycles. Stale posting
list entries and metadata index entries are filtered at query time using the deleted bitmap, and
cleaned up during background LIRE maintenance.

**Cost**: Upserts have the same write cost as inserts (no additional reads required). The trade-off
is that deleted internal IDs accumulate and require periodic cleanup, but this is handled
asynchronously by LIRE maintenance without blocking writes.

**Alternative considered**: Require explicit delete-then-insert from callers. Rejected because it
would require callers to issue two separate API calls, losing atomicity and complicating error
handling.

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

**Decision**: String external IDs are used in the public API.

**Implementation**: The system maintains an `IdDictionary` that maps user-provided string external
IDs (max 64 bytes UTF-8) to system-assigned u64 internal IDs. Benefits:

1. **User convenience**: Callers can use natural identifiers (product SKUs, document UUIDs, etc.)
   without maintaining a separate mapping
2. **Storage efficiency**: Internal u64 IDs enable efficient bitmap compression (RoaringTreemap)
   and monotonic ordering for better compression
3. **Decoupled lifecycle**: The system can manage internal ID allocation and reuse independently
   of user-visible identifiers

**Cost**: One additional KV lookup per write (`IdDictionary` read) and one additional record per
vector (`IdDictionary` entry). For workloads with stable vector sets, the amortized cost is low.

**Alternative considered**: Use u32 IDs in the public API. Rejected because it pushes ID management
complexity onto callers, and many applications naturally work with string identifiers (UUIDs,
composite keys, etc.).

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
| 2026-01-14 | Add AttributeValue::Vector and MetadataFieldType::Vector to    |
|            | support treating embeddings as attributes. Vector is API-only; |
|            | stored in VectorData, not VectorMeta.                          |

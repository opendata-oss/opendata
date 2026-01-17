# RFC 0003: Vector Read API

**Status**: Draft

**Authors**:

- [Almog Gavra](https://github.com/agavra)

## Summary

This RFC proposes a read-only API for OpenData-Vector that follows the LogReader pattern
established in the log module. The `VectorDbReader` provides thread-safe vector query operations
including nearest neighbor search, metadata filtering, and point lookups. A `VectorDbRead` trait
defines the common interface shared by both `VectorDb` (read-write) and `VectorDbReader`
(read-only).

This RFC builds on the unified data model introduced in RFC 0002, where the vector embedding is
treated as a special attribute (with the reserved field name `"vector"`) alongside other metadata
fields.

## Motivation

The write API (RFC 0002) established the ingestion path for vectors, but applications need a clean
separation between read and write concerns. A dedicated read API enables:

1. **Separation of concerns**: Readers can be distributed to consumers that should not have write
   access
2. **Thread safety**: Multiple concurrent readers without write contention
3. **Snapshot isolation**: Readers can operate on stable snapshots independent of ongoing writes
4. **API ergonomics**: Clean, composable query interface following established OpenData patterns
5. **Unified data model**: Vectors are treated as attributes, enabling consistent field selection
   and retrieval patterns

The `VectorDbReader` mirrors the `LogReader` design while adapting to vector-specific query
patterns like nearest neighbor search and metadata filtering.

## Goals

- Define a `VectorDbRead` trait for read operations (following `LogRead` pattern)
- Provide `VectorDbReader` struct implementing read-only access
- Support vector search with metadata filtering
- Support point lookups by ID
- Enable direct reader creation via `VectorDbReader::open(config)`
- Maintain thread safety and snapshot semantics
- Treat vectors as attributes for consistent field selection
- Follow OpenData philosophy: minimal, unopinionated core

## Non-Goals

- **Complex query languages**: No SQL-like syntax or DSL; simple predicates only
- **Query planning/optimization**: No automatic query rewriting or cost-based optimization
- **Aggregations**: No count-distinct, group-by, or analytics operations
- **Joins**: No cross-collection queries or vector joins
- **Streaming results**: Iterator-based streaming can be added later if needed
- **Batch operations**: Batch retrieval can be added in a future RFC if needed
- **HTTP/gRPC endpoints**: Transport protocols remain out of scope

## Design

### Data Model

This RFC uses the `AttributeValue` enum defined in RFC 0002, which includes a `Vector` variant:

```rust
pub enum AttributeValue {
    Vector(Vec<f32>),  // API-level only; stored in VectorData
    String(String),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}
```

The vector embedding is stored under the reserved field name `"vector"` in the fields HashMap,
enabling a unified data model where all attributes (including the vector) can be queried and
retrieved consistently. The Vector variant is first as it is the primary data in a vector
database. At the storage layer, vectors are stored in VectorData records while other attributes
are stored in VectorMeta records.

### VectorDbRead Trait

Following the `LogRead` pattern from the log module, we define a trait that can be implemented by
both `VectorDb` and `VectorDbReader`:

```rust
use async_trait::async_trait;

/// Trait for read operations on a vector database.
///
/// This trait defines the common read interface shared by [`VectorDb`] and
/// [`VectorDbReader`]. It provides methods for searching vectors and retrieving by ID.
///
/// # Implementors
///
/// - [`VectorDb`]: The main database interface with both read and write access.
/// - [`VectorDbReader`]: A read-only view of the database.
///
/// # Example
///
/// ```ignore
/// use opendata_vector::{VectorDbRead, Query, Filter};
///
/// async fn search_products(reader: &impl VectorDbRead) -> Result<()> {
///     let query = Query::new(vec![0.1; 384])
///         .with_filter(Filter::eq("category", "shoes"))
///         .with_limit(10);
///
///     let results = reader.search(query).await?;
///     for result in results {
///         println!("{}: score={}", result.external_id, result.score);
///     }
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait VectorDbRead {
    /// Search for nearest neighbors to a query vector.
    ///
    /// This is the primary query method. It performs approximate nearest neighbor search using
    /// the SPANN algorithm with optional metadata filtering.
    ///
    /// # Arguments
    ///
    /// * `query` - Query specification including vector, limit, filters, and field selection
    ///
    /// # Returns
    ///
    /// Vector of [`SearchResult`] sorted by similarity (best first).
    /// The interpretation of scores depends on the distance metric:
    /// - L2: Lower scores are more similar
    /// - Cosine: Higher scores are more similar (range: -1 to 1)
    /// - DotProduct: Higher scores are more similar
    ///
    /// Results are limited to `limit` items unless a distance threshold is applied, which may
    /// return fewer results.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Query dimensions don't match collection dimensions
    /// - Filter references undefined fields
    /// - Storage read fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// let query = Query::new(embedding)
    ///     .with_limit(10)
    ///     .with_filter(Filter::and(vec![
    ///         Filter::eq("category", "electronics"),
    ///         Filter::lt("price", 100i64),
    ///     ]))
    ///     .with_fields(vec!["category", "price"]);  // Omit vector from results
    ///
    /// let results = reader.search(query).await?;
    /// ```
    async fn search(&self, query: Query) -> Result<Vec<SearchResult>>;

    /// Retrieve a vector record by its external ID.
    ///
    /// This is a point lookup operation that retrieves a single record with all its fields.
    /// Returns `None` if the record doesn't exist or has been deleted.
    ///
    /// # Arguments
    ///
    /// * `id` - External ID of the record to retrieve
    ///
    /// # Returns
    ///
    /// `Some(VectorRecord)` if found, `None` if not found or deleted.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(record) = reader.get("product-123").await? {
    ///     if let Some(AttributeValue::Vector(vec)) = record.fields.get("vector") {
    ///         println!("Found vector with {} dimensions", vec.len());
    ///     }
    ///     println!("Category: {:?}", record.fields.get("category"));
    /// }
    /// ```
    async fn get(&self, id: &str) -> Result<Option<VectorRecord>>;
}
```

### VectorDbReader Struct

```rust
use std::sync::Arc;
use tokio::sync::RwLock;
use common::StorageRead;

/// A read-only view of a vector database.
///
/// `VectorDbReader` provides access to all read operations via the [`VectorDbRead`] trait, but
/// not write operations. This is useful for:
///
/// - Consumers that should not have write access
/// - Sharing read access across multiple components
/// - Separating read and write concerns in your application
///
/// # Obtaining a VectorDbReader
///
/// A `VectorDbReader` can be created in two ways:
///
/// 1. From an existing `VectorDb`:
/// ```ignore
/// let reader = db.reader();
/// ```
///
/// 2. Opened directly:
/// ```ignore
/// let reader = VectorDbReader::open(config).await?;
/// ```
///
/// # Thread Safety
///
/// `VectorDbReader` is designed to be cloned and shared across threads.
/// All methods take `&self` and are safe to call concurrently.
///
/// # Snapshot Semantics
///
/// When opened directly via `open()`, a `VectorDbReader` operates on a storage snapshot taken
/// at open time. It will not see writes that occur after opening.
///
/// When obtained from `VectorDb::reader()`, the reader shares the database's snapshot, which is
/// updated after each flush operation.
///
/// # Example
///
/// ```ignore
/// use opendata_vector::{VectorDbReader, VectorDbRead, Query, Filter, Config};
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Open reader directly
///     let config = Config {
///         dimensions: 384,
///         ..Default::default()
///     };
///     let reader = VectorDbReader::open(config).await?;
///
///     // Search with filters
///     let query = Query::new(embedding)
///         .with_limit(10)
///         .with_filter(Filter::eq("category", "shoes"));
///
///     let results = reader.search(query).await?;
///
///     // Point lookup
///     if let Some(record) = reader.get("product-123").await? {
///         println!("Found: {:?}", record.fields);
///     }
///
///     Ok(())
/// }
/// ```
pub struct VectorDbReader {
    config: Config,
    storage: Arc<dyn StorageRead>,
    /// In-memory HNSW graph for centroid search (loaded lazily on first query).
    centroid_graph: RwLock<Option<CentroidGraph>>,
}

impl VectorDbReader {
    /// Opens a read-only view of the vector database with the given configuration.
    ///
    /// This creates a `VectorDbReader` that can query vectors but cannot write. The reader
    /// operates on a storage snapshot taken at open time.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying storage backend and settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use opendata_vector::{VectorDbReader, Config};
    ///
    /// let config = Config {
    ///     dimensions: 384,
    ///     ..Default::default()
    /// };
    /// let reader = VectorDbReader::open(config).await?;
    /// ```
    pub async fn open(config: Config) -> Result<Self> {
        let storage = config.storage.snapshot().await?;

        Ok(Self {
            config,
            storage,
            centroid_graph: RwLock::new(None),
        })
    }

    /// Creates a VectorDbReader from an existing storage snapshot.
    ///
    /// This is used internally by `VectorDb::reader()` to create readers that share the
    /// database's snapshot.
    pub(crate) fn from_snapshot(
        config: Config,
        storage: Arc<dyn StorageRead>,
    ) -> Self {
        Self {
            config,
            storage,
            centroid_graph: RwLock::new(None),
        }
    }
}
```

### Query API

```rust
/// Query specification for vector search.
///
/// Constructed using the builder pattern for ergonomic queries:
///
/// ```ignore
/// let query = Query::new(embedding)
///     .with_limit(10)
///     .with_filter(Filter::eq("category", "shoes"))
///     .with_distance_threshold(0.8)
///     .with_fields(vec!["category", "price"]);
/// ```
#[derive(Debug, Clone)]
pub struct Query {
    /// Query vector (required)
    pub vector: Vec<f32>,

    /// Maximum number of results to return (default: 10)
    pub limit: usize,

    /// Optional metadata filter
    pub filter: Option<Filter>,

    /// Optional distance threshold (exclude results beyond this distance)
    ///
    /// Interpretation depends on distance metric:
    /// - L2: Exclude if distance > threshold
    /// - Cosine/DotProduct: Exclude if score < threshold
    pub distance_threshold: Option<f32>,

    /// Which fields to include in results (default: All)
    pub include_fields: FieldSelection,
}

/// Specifies which fields to include in query results.
///
/// This enum provides flexible control over which attributes are returned,
/// reducing data transfer when only specific fields are needed.
///
/// # Examples
///
/// ```ignore
/// // Include all fields (vector + metadata)
/// query.with_fields(true)
///
/// // Include no fields (just IDs and scores)
/// query.with_fields(false)
///
/// // Include specific fields
/// query.with_fields(vec!["category", "price"])
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum FieldSelection {
    /// Include all fields (vector and all metadata)
    All,

    /// Include no fields (only external_id and score)
    None,

    /// Include specific fields by name (e.g., ["category", "price"])
    /// Use "vector" to include the embedding vector
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

impl Query {
    /// Creates a new query with the given vector.
    ///
    /// Defaults: limit=10, no filter, include all fields
    pub fn new(vector: Vec<f32>) -> Self {
        Self {
            vector,
            limit: 10,
            filter: None,
            distance_threshold: None,
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

    /// Sets a distance threshold.
    ///
    /// For L2 metric: results with distance > threshold are excluded.
    /// For Cosine/DotProduct: results with score < threshold are excluded.
    pub fn with_distance_threshold(mut self, threshold: f32) -> Self {
        self.distance_threshold = Some(threshold);
        self
    }

    /// Controls which fields are included in results.
    ///
    /// Accepts:
    /// - `true` or `false` for all/none
    /// - `Vec<String>` or `Vec<&str>` for specific fields
    ///
    /// Use "vector" as a field name to include the embedding vector.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Include all fields
    /// query.with_fields(true)
    ///
    /// // Include no fields (just IDs and scores)
    /// query.with_fields(false)
    ///
    /// // Include only category and price
    /// query.with_fields(vec!["category", "price"])
    ///
    /// // Include vector and category
    /// query.with_fields(vec!["vector", "category"])
    /// ```
    pub fn with_fields(mut self, fields: impl Into<FieldSelection>) -> Self {
        self.include_fields = fields.into();
        self
    }
}
```

### Filter API

```rust
/// Metadata filter for search queries.
///
/// Filters are composed using simple predicates and logical operators.
/// All filters are evaluated against the metadata inverted indexes.
///
/// # Examples
///
/// ```ignore
/// // Equality
/// Filter::eq("category", "shoes")
///
/// // Comparison
/// Filter::lt("price", 100i64)
/// Filter::gte("rating", 4.5)
///
/// // Logical operators
/// Filter::and(vec![
///     Filter::eq("category", "shoes"),
///     Filter::lt("price", 100i64),
/// ])
///
/// Filter::or(vec![
///     Filter::eq("category", "shoes"),
///     Filter::eq("category", "boots"),
/// ])
///
/// // Negation
/// Filter::not(Filter::eq("on_sale", true))
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Field equals value
    Eq(String, AttributeValue),

    /// Field not equals value
    Neq(String, AttributeValue),

    /// Field less than value (numeric types only: Int64, Float64)
    Lt(String, AttributeValue),

    /// Field less than or equal to value (numeric types only)
    Lte(String, AttributeValue),

    /// Field greater than value (numeric types only)
    Gt(String, AttributeValue),

    /// Field greater than or equal to value (numeric types only)
    Gte(String, AttributeValue),

    /// Field is in set of values
    In(String, Vec<AttributeValue>),

    /// All filters must match (logical AND)
    And(Vec<Filter>),

    /// Any filter must match (logical OR)
    Or(Vec<Filter>),

    /// Filter must not match (logical NOT)
    Not(Box<Filter>),
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

    /// Creates a less-than filter.
    pub fn lt(field: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        Filter::Lt(field.into(), value.into())
    }

    /// Creates a less-than-or-equal filter.
    pub fn lte(field: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        Filter::Lte(field.into(), value.into())
    }

    /// Creates a greater-than filter.
    pub fn gt(field: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        Filter::Gt(field.into(), value.into())
    }

    /// Creates a greater-than-or-equal filter.
    pub fn gte(field: impl Into<String>, value: impl Into<AttributeValue>) -> Self {
        Filter::Gte(field.into(), value.into())
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

    /// Negates a filter.
    pub fn not(filter: Filter) -> Self {
        Filter::Not(Box::new(filter))
    }
}
```

### Result Types

```rust
use std::collections::HashMap;

/// A vector record retrieved by ID.
///
/// All attributes (including the vector embedding) are stored in the `fields` map.
/// The vector is stored under the special field name `"vector"`.
///
/// Returned by `get()` operations.
#[derive(Debug, Clone)]
pub struct VectorRecord {
    /// External vector ID (user-provided)
    pub external_id: String,

    /// All fields including vector and metadata
    ///
    /// The vector embedding is stored under the key `"vector"` as
    /// `AttributeValue::Vector(Vec<f32>)`.
    pub fields: HashMap<String, AttributeValue>,
}

/// A search result with score and fields.
///
/// All attributes (including the vector embedding) are stored in the `fields` map.
/// The vector is stored under the special field name `"vector"`.
///
/// Which fields are included depends on the `include_fields` setting in the query.
///
/// Returned by `search()` operations.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Internal vector ID (system-assigned)
    pub internal_id: u64,

    /// External vector ID (user-provided)
    pub external_id: String,

    /// Similarity score (interpretation depends on distance metric)
    ///
    /// - L2: Lower scores = more similar
    /// - Cosine: Higher scores = more similar (range: -1 to 1)
    /// - DotProduct: Higher scores = more similar
    pub score: f32,

    /// All fields including vector and metadata
    ///
    /// The vector embedding is stored under the key `"vector"` as
    /// `AttributeValue::Vector(Vec<f32>)`.
    ///
    /// Contents depend on the `include_fields` query parameter:
    /// - `FieldSelection::All`: All fields including vector
    /// - `FieldSelection::None`: Empty map
    /// - `FieldSelection::Fields(names)`: Only specified fields
    pub fields: HashMap<String, AttributeValue>,
}
```

### VectorDb Integration

```rust
impl VectorDb {
    /// Get a read-only view of the vector database.
    ///
    /// The reader shares the database's current snapshot and will see the effects of flush
    /// operations. For a stable snapshot independent of writes, use `VectorDbReader::open()`
    /// instead.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let db = VectorDb::open(config).await?;
    /// let reader = db.reader();
    ///
    /// // Reader shares db's snapshot
    /// let results = reader.search(query).await?;
    /// ```
    pub fn reader(&self) -> VectorDbReader {
        let snapshot = self.snapshot.blocking_read().clone();
        VectorDbReader::from_snapshot(self.config.clone(), snapshot)
    }
}
```

## Query Semantics

### Search Query Execution

The `search()` operation implements the SPANN algorithm with metadata filtering:

1. **Validate query dimensions** against configured dimensions
2. **Load centroids** (lazily on first query) and build HNSW graph if needed
3. **Search HNSW** for nearest centroids
   - Expands search by 10-100x (at least 10, at most 100 centroids)
   - This expansion improves recall for approximate nearest neighbor search
4. **Load posting lists** for identified centroids to get candidate vector IDs
5. **Apply metadata filter** (if provided):
   - For each filter predicate, load metadata index entries
   - Compute intersection/union/difference of vector ID sets using RoaringTreemap operations
   - Intersect filtered IDs with posting list candidates
6. **Filter deleted vectors** using the deleted bitmap (centroid_id=0)
7. **Load fields** for remaining candidates based on `include_fields`:
   - `FieldSelection::All`: Load all fields from VectorData and VectorMeta
   - `FieldSelection::None`: Skip loading fields entirely
   - `FieldSelection::Fields(names)`: Load only specified fields
8. **Score candidates** using configured distance metric (L2, Cosine, or DotProduct)
9. **Sort results**:
   - L2: Ascending (lower distances first)
   - Cosine/DotProduct: Descending (higher scores first)
10. **Truncate to limit results**
11. **Apply distance threshold** (if provided):
    - L2: Exclude results where distance > threshold
    - Cosine/DotProduct: Exclude results where score < threshold

### Filter Evaluation

Filters are evaluated using inverted indexes stored in the `MetadataIndex`:

- **Equality (`Eq`)**: Direct lookup in `MetadataIndex[field][value]` returns vector IDs
- **Inequality (`Neq`)**: All vectors except those in `MetadataIndex[field][value]`
- **Range (`Lt`, `Lte`, `Gt`, `Gte`)**:
  - Scan metadata index keys in sorted order (using sortable encoding)
  - For `Lt`/`Lte`: Scan from start until threshold
  - For `Gt`/`Gte`: Scan from threshold to end
- **In-set (`In`)**: Union of `MetadataIndex[field][value]` for each value in the set
- **Logical operators**:
  - `And`: Intersection of child filter results (using RoaringTreemap intersection)
  - `Or`: Union of child filter results (using RoaringTreemap union)
  - `Not`: Complement of child filter result (all vectors except those in result)

Filter evaluation is performed **after** centroid search to minimize the set of candidates that
need filtering. This is efficient when filters are selective.

### Point Lookup Execution

The `get()` operation retrieves a single vector record by external ID:

1. **Lookup internal ID** from `IdDictionary[external_id]`
2. **Check deleted bitmap**: Return `None` if internal ID is in deleted set (centroid_id=0)
3. **Load vector data** from `VectorData[internal_id]`
4. **Load metadata** from `VectorMeta[internal_id]`
5. **Construct VectorRecord** with external_id and fields (including "vector" field)

## Thread Safety

Following the `LogReader` pattern from the log module:

- All methods take `&self` for concurrent access
- Internal state (`centroid_graph`) uses `RwLock` for safe lazy initialization
- Storage snapshots are immutable and thread-safe (`Arc<dyn StorageRead>`)
- Multiple readers can query concurrently without contention
- Readers obtained from `VectorDb::reader()` share the database's snapshot but are independent
  objects

## Performance Considerations

### Metadata Filtering

Filters are applied **after** centroid search to candidate vectors. This means:

- **Efficient for selective filters**: If the filter eliminates many candidates, less vector
  scoring is needed
- **Less efficient for broad filters**: If the filter keeps most candidates, the overhead may not
  justify the benefit
- **Trade-off**: Applying filters before centroid search would require scanning all metadata
  indexes, which is expensive for large collections

Future optimization: For highly selective filters on frequently-queried fields, consider
filter-first strategies.

### Field Selection

The `include_fields` parameter reduces data transfer and deserialization overhead:

- `FieldSelection::All`: Loads all fields (maximum data transfer)
- `FieldSelection::None`: Loads no fields, only IDs and scores (minimum data transfer)
- `FieldSelection::Fields(names)`: Selectively loads specified fields

Performance benefits:
- Skip loading VectorData entirely if "vector" is not requested (saves 4 bytes × dimensions)
- Skip loading VectorMeta entirely if no metadata fields are requested
- Reduce deserialization overhead for large records

This is useful for:
- Applications that only need IDs and scores (e.g., for ranking before a second pass)
- Pipelines that perform a coarse search first, then refine with additional filters
- Queries where the vector embedding is not needed in results

## Examples

### Basic Search

```rust
use opendata_vector::{VectorDbReader, VectorDbRead, Query, Config, AttributeValue};

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config {
        dimensions: 384,
        ..Default::default()
    };
    let reader = VectorDbReader::open(config).await?;

    let embedding = vec![0.1; 384];
    let query = Query::new(embedding).with_limit(10);

    let results = reader.search(query).await?;
    for result in results {
        println!("{}: score={}", result.external_id, result.score);

        // Access the vector if included
        if let Some(AttributeValue::Vector(vec)) = result.fields.get("vector") {
            println!("  Vector dimensions: {}", vec.len());
        }
    }

    Ok(())
}
```

### Search with Filtering

```rust
use opendata_vector::{Filter, Query};

let query = Query::new(embedding)
    .with_limit(10)
    .with_filter(Filter::and(vec![
        Filter::eq("category", "electronics"),
        Filter::lt("price", 500i64),
        Filter::gte("rating", 4.0),
    ]));

let results = reader.search(query).await?;
```

### Complex Filters

```rust
// OR with multiple categories
let query = Query::new(embedding)
    .with_limit(20)
    .with_filter(Filter::or(vec![
        Filter::eq("category", "shoes"),
        Filter::eq("category", "boots"),
        Filter::eq("category", "sandals"),
    ]));

// Negation
let query = Query::new(embedding)
    .with_limit(10)
    .with_filter(Filter::not(Filter::eq("on_sale", true)));

// In-set filter
let categories = vec!["shoes".into(), "boots".into(), "sandals".into()];
let query = Query::new(embedding)
    .with_limit(10)
    .with_filter(Filter::in_set("category", categories));
```

### Distance Threshold

```rust
// Only return vectors within distance threshold
let query = Query::new(embedding)
    .with_limit(100)
    .with_distance_threshold(0.8)  // For cosine similarity
    .with_fields(false);  // Don't need any fields, just IDs and scores

let results = reader.search(query).await?;
println!("Found {} vectors within threshold", results.len());
```

### Field Selection

```rust
use opendata_vector::AttributeValue;

// Include all fields (default)
let query = Query::new(embedding)
    .with_limit(10)
    .with_fields(true);

// Include no fields (just IDs and scores for ranking)
let query = Query::new(embedding)
    .with_limit(100)
    .with_fields(false);

// Include only specific fields
let query = Query::new(embedding)
    .with_limit(10)
    .with_fields(vec!["category", "price"]);  // Omit vector

// Include vector and category only
let query = Query::new(embedding)
    .with_limit(10)
    .with_fields(vec!["vector", "category"]);

let results = reader.search(query).await?;
for result in results {
    // Access fields based on what was requested
    if let Some(AttributeValue::String(cat)) = result.fields.get("category") {
        println!("{}: category={}", result.external_id, cat);
    }
}
```

### Point Lookup

```rust
use opendata_vector::AttributeValue;

// Single lookup
if let Some(record) = reader.get("product-123").await? {
    // Access vector
    if let Some(AttributeValue::Vector(vec)) = record.fields.get("vector") {
        println!("Vector dimensions: {}", vec.len());
    }

    // Access metadata
    if let Some(AttributeValue::String(cat)) = record.fields.get("category") {
        println!("Category: {}", cat);
    }
    if let Some(AttributeValue::Int64(price)) = record.fields.get("price") {
        println!("Price: {}", price);
    }
}
```

### Combining with Write API

```rust
use opendata_vector::{VectorDb, Vector, AttributeValue};

// Write path
let db = VectorDb::open(config).await?;
let vectors = vec![
    Vector::builder("product-001", embedding_1)
        .attribute("category", "shoes")
        .attribute("price", 99i64)
        .attribute("rating", 4.5)
        .build(),
    Vector::builder("product-002", embedding_2)
        .attribute("category", "boots")
        .attribute("price", 129i64)
        .attribute("rating", 4.8)
        .build(),
];
db.write(vectors).await?;
db.flush().await?;

// Read path (shares snapshot)
let reader = db.reader();
let query = Query::new(query_vector)
    .with_limit(10)
    .with_filter(Filter::eq("category", "shoes"));

let results = reader.search(query).await?;
println!("Found {} shoe products", results.len());

// Access fields
for result in results {
    if let Some(AttributeValue::Int64(price)) = result.fields.get("price") {
        println!("{}: ${}", result.external_id, price);
    }
}
```

### Independent Reader

```rust
use tokio;

// Reader independent of writer (stable snapshot)
let reader = VectorDbReader::open(config).await?;

// Spawn task with stable reader
tokio::spawn(async move {
    loop {
        let query = Query::new(query_vector.clone()).with_limit(10);
        let results = reader.search(query).await?;

        // Results remain stable despite concurrent writes to VectorDb
        println!("Found {} results", results.len());

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
});
```

## Alternatives Considered

### Iterator-based Search Results

**Alternative**: Return `SearchIterator` that lazily loads and scores candidates.

```rust
async fn search(&self, query: Query) -> Result<SearchIterator>;
```

**Rationale for rejection**:
- Vector search typically returns small result sets (limit=10-100)
- Materializing all results is simpler and matches user expectations
- Iterator adds complexity for marginal benefit
- SPANN algorithm requires scoring all candidates before sorting
- Can be added later if streaming large result sets becomes important

### Separate Values and Metadata Fields

**Alternative**: Keep separate `values: Vec<f32>` and `metadata: HashMap<String, AttributeValue>`
fields in result types.

**Rationale for rejection**:
- Treating vector as an attribute provides a unified data model
- Simplifies field selection logic (one mechanism for all fields)
- Enables consistent handling in storage and retrieval layers
- Reduces API surface area (one field map instead of two)
- Makes it easier to add new attribute types in the future

### Separate Filter Type for Range Queries

**Alternative**: Dedicated `RangeFilter` type for numeric comparisons:

```rust
pub enum Filter {
    Eq(String, AttributeValue),
    Range(RangeFilter),
    And(Vec<Filter>),
    Or(Vec<Filter>),
}
```

**Rationale for rejection**:
- The `Lt`, `Lte`, `Gt`, `Gte` variants are more ergonomic for simple queries
- Range filters can be composed using `And(vec![Gte(...), Lt(...)])`
- Simpler enum with fewer special cases
- Matches common filter API patterns from other databases

### SearchOptions as Separate Struct

**Alternative**: Separate `SearchOptions` struct similar to `LogRead::scan_with_options()`:

```rust
async fn search(&self, vector: Vec<f32>, limit: usize) -> Result<Vec<SearchResult>>;
async fn search_with_options(
    &self,
    vector: Vec<f32>,
    limit: usize,
    options: SearchOptions
) -> Result<Vec<SearchResult>>;
```

**Rationale for rejection**:
- The `Query` builder pattern is more ergonomic for multi-parameter queries
- All search parameters (vector, limit, filter, thresholds) are logically grouped in one struct
- No need for default/options split when using builders
- Builder pattern allows adding new options without breaking changes

## Updates

| Date       | Description   |
|------------|---------------|
| 2026-01-14 | Initial draft |

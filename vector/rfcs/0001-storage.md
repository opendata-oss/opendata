# RFC 0001: Vector Database Storage

**Status**: Draft

**Authors**:

- [Almog Gavra](https://github.com/agavra)
- [Bruno Cadonna](https://github.com/cadonna)

## Summary

This RFC defines the storage model for a vector database built on
[SlateDB](https://github.com/slatedb/slatedb). Each collection (namespace) maintains a single
SPANN-style index for efficient approximate nearest neighbor (ANN) search, inverted indexes for
metadata filtering, and a forward index for retrieving vector data by ID. The index is maintained
incrementally using LIRE-style rebalancing via SlateDB merge operators.

## Motivation

The vector database stores high-dimensional embedding vectors with associated metadata. The storage
design must support:

1. **Efficient ANN search** — Finding the k most similar vectors to a query requires specialized
   index structures that avoid exhaustive O(n) scans.

2. **Metadata filtering** — Queries often combine vector similarity with predicate filters (e.g.,
   `{category="electronics", price < 100}`). Inverted indexes on metadata fields enable efficient
   pre/post-filtering.

3. **Memory-disk hybrid indexing** — SPANN-style architecture keeps only cluster centroids in memory
   while storing posting lists on disk, enabling billion-scale indexes with bounded memory.

4. **Incremental updates** — LIRE-style rebalancing maintains index quality without expensive global
   rebuilds. SlateDB merge operators enable concurrent updates to posting lists and centroids.

5. **Simple query path** — A single global index per namespace enables fast, straightforward queries.

## Goals

- Define the record key/value encoding scheme
- Define common value encodings for vectors and metadata
- Define indexing record types (centroids, posting lists, metadata indexes)

## Non-Goals (left for future RFCs)

- Compaction/retention policies and LIRE rebalancing mechanics
- Query execution and search algorithm details
- Quantization strategies (PQ, SQ)
- Distributed/sharded deployment

## Design

### Architecture Overview

Each namespace corresponds to a single SlateDB instance with a global SPANN index. Vector data and
index structures are stored as key-value pairs in the LSM tree.

```ascii
┌─────────────────────────────────────────────────────────────┐
│                OpenData Vector (per namespace)              │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │                  Write Path                         │   │
│   │                                                     │   │
│   │   1. Vector written to WAL (durability)             │   │
│   │   2. External ID looked up / allocated internal ID  │   │
│   │   3. Assigned to nearest centroid(s)                │   │
│   │   4. Posting list updated via merge operator        │   │
│   │   5. Vector data + metadata + dictionary written    │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │              Global SPANN Index                     │   │
│   │                                                     │   │
│   │   Centroids:     [chunk 0] [chunk 1] ... [chunk N]  │   │
│   │                  (loaded into HNSW for navigation)  │   │
│   │                                                     │   │
│   │   Posting Lists: centroid_id → RoaringTreemap       │   │
│   │                  (vector IDs per cluster)           │   │
│   │                                                     │   │
│   │   Deleted:       centroid_id=0 → deleted vector IDs │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │                  Vector Storage                     │   │
│   │                                                     │   │
│   │   IdDictionary:  external_id → internal vector_id   │   │
│   │   VectorData:    vector_id → f32[dimensions]        │   │
│   │   VectorMeta:    vector_id → external_id + metadata │   │
│   │   MetadataIndex: (field, value) → vector IDs        │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │              LIRE Maintenance (background)          │   │
│   │                                                     │   │
│   │   - Split oversized posting lists (new centroids)   │   │
│   │   - Merge undersized posting lists                  │   │
│   │   - Reassign boundary vectors                       │   │
│   │   - Clean up deleted vectors                        │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
│   Storage: SlateDB (LSM KV Store)                           │
│   (vectors, posting lists, metadata all as KV pairs)        │
└─────────────────────────────────────────────────────────────┘
```

### Background on the Query Path

This RFC focuses on storage design, but understanding the query path motivates the storage model. A
vector database query has two components:

- **Vector similarity search**: Find vectors closest to a query vector
- **Metadata filtering**: Restrict results to vectors matching predicate filters

A SPANN-style vector database has four storage components:

- **Centroid index**: In-memory structure (e.g., HNSW) over cluster centroids for fast navigation to
  relevant posting lists
- **Posting lists**: On-disk lists mapping each centroid to the vector IDs in its cluster
- **Vector data**: Raw vector bytes keyed by vector ID
- **Metadata index**: Inverted index mapping metadata field/value pairs to vector IDs

To illustrate, here's how the query
`find top-10 similar to query_vec where category="shoes" and price < 50` is served:

1. Search the centroid index to find the k nearest centroids to `query_vec`
2. Load posting lists for those centroids from disk
3. Intersect candidate vector IDs with the metadata filter `{category="shoes", price < 50}` using
   the inverted index
4. Compute exact distances for remaining candidates
5. Return top-10 by distance

### Record Layout

Record keys are built by concatenating big-endian binary tokens. Lexicographical ordering of keys
matches numeric ordering of encoded values.

Key identifiers used in this RFC:

- `vector_id` (u64): Internal vector identifier, system-assigned, unique within the namespace
- `external_id` (string): User-provided identifier, max 64 bytes, maps to internal vector_id
- `centroid_id` (u32): Cluster centroid identifier
- `chunk_id` (u32): Centroid chunk identifier

### External and Internal IDs

Users provide **external IDs**—arbitrary strings up to 64 bytes—to identify their vectors. The
system maintains an `IdDictionary` that maps each external ID to a system-assigned **internal ID**
(u64). Internal IDs are used in all index structures (posting lists, metadata indexes) because:

1. Fixed-width u64 keys enable efficient bitmap operations (RoaringTreemap)
2. Monotonically increasing IDs improve bitmap compression (sequential IDs cluster well)
3. Decoupling external from internal IDs allows the system to manage ID lifecycle

**Upsert Behavior:**

When inserting a vector with an external ID that already exists:

1. Look up the existing internal ID from `IdDictionary`
2. Delete the old vector: add internal ID to deleted bitmap, tombstone `VectorData` and `VectorMeta`
3. Allocate a new internal ID
4. Write new vector data with the new internal ID
5. Update `IdDictionary` to point to the new internal ID

This "delete old + insert new" approach ensures that posting lists and metadata indexes are updated
correctly via merge operators, without requiring expensive read-modify-write cycles to remove the
old internal ID from every index entry.

### Block-Based ID Allocation

Internal vector IDs are allocated from a monotonically increasing counter using block-based
allocation (similar to the Log RFC's sequence allocation). Rather than persisting the counter after
every insert, the writer pre-allocates a block of IDs and records the allocation in a `SeqBlock`
record.

**Allocation procedure:**

1. On initialization, read the `SeqBlock` record to get the last allocated range `[base, base+size)`
2. Allocate a new block starting at `base + size` and write a new `SeqBlock` before processing
3. During normal operation, assign IDs from the current block, incrementing after each insert
4. When the current block is exhausted, allocate a new block and write an updated `SeqBlock`

**Recovery:**

On crash recovery, read the `SeqBlock` and allocate a fresh block starting after the previous range.
Any IDs allocated but not used before the crash are skipped. This may create gaps in the ID space,
but monotonicity is preserved.

**Block sizing:**

Block size is an implementation detail balancing write amplification (larger blocks reduce
`SeqBlock` write frequency) against ID space efficiency (smaller blocks waste fewer IDs on crash).

### Standard Key Prefix

All records use a standard 2-byte prefix: a single `u8` for the record version and another `u8` for
the record tag. The record tag encodes the record type in the high 4 bits.

```
record_tag byte layout:
┌────────────┬────────────┐
│  bits 7-4  │  bits 3-0  │
│ record type│  reserved  │
│   (1-15)   │    (0)     │
└────────────┴────────────┘
```

The lower 4 bits are reserved and must be set to `0`. They exist for forward compatibility
with future schema changes that may require sub-type discrimination.

### Common Encodings

**Value Encodings** (little-endian):

- `Utf8`: `len: u16` followed by `len` bytes of UTF-8 payload.
- `Vector<D>`: `D` elements of `f32`, where `D` is the dimensionality stored in collection metadata.
  Total size is `D * 4` bytes.
- `Array<T>`: `count: u16` followed by `count` serialized elements of type `T`.
- `FixedElementArray<T>`: Serialized elements back-to-back with no count prefix; length derived from
  buffer size divided by element size.
- `RoaringTreemap`: Roaring treemap serialization format for compressed u64 integer sets (64-bit
  extension of Roaring bitmap).

**Key Encodings** (big-endian for lexicographic ordering):

- `TerminatedBytes`: Variable-length bytes with escape sequences and `0x00` terminator for
  lexicographic ordering. Using `0x00` as terminator ensures shorter keys sort before longer
  keys with the same prefix (e.g., `/foo` < `/foo/bar`):
  - `0x00` in payload → `0x01 0x01` (escaped terminator)
  - `0x01` in payload → `0x01 0x02` (escaped escape byte)
  - `0xFF` in payload → `0x01 0x03` (escaped range-end byte)
  - All other bytes unchanged
  - Terminated with `0x00`

**Note on Endianness**: Value schemas use little-endian encoding. Key schemas use big-endian to
maintain lexicographic ordering for range scans.

### Record Type Reference

| ID     | Name             | Description                                             |
|--------|------------------|---------------------------------------------------------|
| `0x00` | *(reserved)*     | Reserved for future use                                 |
| `0x01` | `CollectionMeta` | Global schema: dimensions, distance metric, field specs |
| `0x02` | `CentroidChunk`  | Stores a chunk of cluster centroids for SPANN navigation|
| `0x03` | `PostingList`    | Maps centroid IDs to vector IDs in that cluster         |
| `0x04` | `IdDictionary`   | Maps external string IDs to internal u64 vector IDs     |
| `0x05` | `VectorData`     | Stores raw vector bytes keyed by internal u64 ID        |
| `0x06` | `VectorMeta`     | Stores metadata key-value pairs for each vector         |
| `0x07` | `MetadataIndex`  | Inverted index mapping metadata values to vector IDs    |
| `0x08` | `SeqBlock`       | Stores sequence allocation state for internal ID gen    |

## Record Definitions & Schemas

### `CollectionMeta` (`RecordType::CollectionMeta` = `0x01`)

Stores the global schema and configuration for the vector collection. This is a singleton record
that defines the structure all vectors in the namespace must conform to.

**Key Layout:**

```
┌─────────┬─────────────┐
│ version │ record_tag  │
│ 1 byte  │   1 byte    │
└─────────┴─────────────┘
```

- `version` (u8): Key format version (currently `0x01`)
- `record_tag` (u8): Record type `0x01` in high nibble, reserved `0x0` in low nibble

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                     CollectionMetaValue                        │
├────────────────────────────────────────────────────────────────┤
│  schema_version:    u32                                        │
│  dimensions:        u16                                        │
│  distance_metric:   u8   (0=L2, 1=cosine, 2=dot_product)       │
│  chunk_target:      u16  (centroids per chunk, default 4096)   │
│  metadata_fields:   Array<MetadataFieldSpec>                   │
│                                                                │
│  MetadataFieldSpec                                             │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  name:       Utf8                                        │  │
│  │  field_type: u8  (0=string, 1=int64, 2=float64, 3=bool)  │  │
│  │  indexed:    u8  (0=false, 1=true)                       │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `schema_version`: Monotonically increasing version number, incremented on metadata field changes
- `dimensions`: Fixed dimensionality for all vectors in the collection (immutable after creation)
- `distance_metric`: Distance function used for similarity computation (immutable after creation)
- `chunk_target`: Number of centroids per chunk (immutable after creation, default 4096)
- `metadata_fields`: Schema for metadata fields, including which fields are indexed for filtering

**Schema Evolution:**

The `schema_version` field tracks metadata schema changes only (not structural changes to the
collection). Supported evolutions:

- **Adding metadata fields**: New fields can be appended to `metadata_fields`. Existing vectors
  without the field return null/missing for that field.
- **Enabling indexing**: A field's `indexed` flag can be changed from false to true. A background
  job must rebuild the `MetadataIndex` entries for existing vectors.

Unsupported changes (require creating a new collection):

- Changing `dimensions`, `distance_metric`, or `chunk_target`
- Removing metadata fields or changing their types
- Disabling indexing on a field (index entries would become stale)

### `CentroidChunk` (`RecordType::CentroidChunk` = `0x02`)

Stores a chunk of cluster centroids for the namespace. During search, these centroids are loaded
into memory and indexed (typically with HNSW) for fast navigation. Centroids are split across
multiple records to enable efficient partial loading—only touched chunks are read from disk and
cached.

**Key Layout:**

```
┌─────────┬─────────────┬──────────┐
│ version │ record_tag  │ chunk_id │
│ 1 byte  │   1 byte    │ 4 bytes  │
└─────────┴─────────────┴──────────┘
```

- `chunk_id` (u32): Identifies the chunk within the namespace, starting from 0

**Why Chunking?** All centroid chunks are fully scanned on startup (`scan(version|0x02|*)`) to load
the HNSW graph—the `chunk_id` exists solely to give each chunk a distinct record key. Storing one
centroid per key would explode the number of keys fetched on startup, increasing latency. Future
work could add hierarchy to page centroids in/out by chunk, but that is out of scope here.

**Value Schema:**

```
┌───────────────────────────────────────────────────────────────┐
│                    CentroidChunkValue                         │
├───────────────────────────────────────────────────────────────┤
│  centroid_count:  u32 (<= CHUNK_TARGET)                       │
│  entries:        Array<CentroidEntry>                         │
│                                                               │
│  CentroidEntry                                                │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │  centroid_id: u32 (stable identifier)                    │ │
│  │  vector:      FixedElementArray<f32>                     │ │
│  │               (dimensions elements)                      │ │
│  └──────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
```

**Structure:**

- Each chunk stores up to `CHUNK_TARGET` centroids (≈25 MB per chunk at 1536 dims with default 4096)
- Each centroid persists its own `centroid_id`, allowing chunks to be rewritten or reordered without
  renumbering posting lists
- `CHUNK_TARGET` is stored in `CollectionMeta` and is immutable after collection creation
- Separate records per chunk enable SlateDB block-level caching and partial reads
- Dimensionality obtained from `CollectionMeta`
- Centroid ratio is configurable; typical values range from 0.1-1% of vectors (e.g., 10K-100K
  centroids for 10M vectors). Higher ratios improve recall at the cost of memory.

### `PostingList` (`RecordType::PostingList` = `0x03`)

Maps a centroid ID to the list of vector IDs assigned to that cluster. During search, posting lists
for the nearest centroids are loaded and their vectors evaluated. Boundary vectors (near multiple
centroids) may appear in multiple posting lists to improve recall. The target duplication factor
is 1.5-2x (i.e., each vector appears in 1.5-2 posting lists on average); exact thresholds for
boundary classification are defined in the LIRE rebalancing RFC.

**Key Layout:**

```
┌─────────┬─────────────┬──────────────┐
│ version │ record_tag  │ centroid_id  │
│ 1 byte  │   1 byte    │   4 bytes    │
└─────────┴─────────────┴──────────────┘
```

**Centroid ID Addressing:**

Every centroid carries an explicit `centroid_id` stored alongside its vector inside the chunk payload.
IDs start at 1 and are never reassigned once issued. Chunks simply group centroids for locality; their
position no longer derives the ID.

**Reserved / lifecycle:**

| ID            | Purpose                                      |
|---------------|----------------------------------------------|
| `0x00000000`  | Deleted vectors bitmap                       |
| `0x00000001+` | Cluster centroids (stable IDs stored in chunks) |

The deleted vectors posting list (centroid_id = 0) contains vector IDs that have been deleted but
not yet cleaned up by LIRE maintenance. During search, this bitmap is loaded once and subtracted
from candidate sets.

**Stability & LIRE maintenance:**

- New centroids created during splits receive freshly allocated IDs and are appended to the relevant
  `CentroidChunk` records (chunks may be rewritten to accommodate the new entry).
- When two centroids merge, the “retiring” centroid ID is tombstoned and its posting list drained via
  merge operator, but the identifier is never reused.
- Posting lists are rewritten only for the centroids whose IDs actually change (new split children);
  all other posting list keys stay stable, avoiding cascading renumbering across the tree.

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                     PostingListValue                           │
├────────────────────────────────────────────────────────────────┤
│  vector_ids:  RoaringTreemap                                   │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- Vector IDs stored as a Roaring treemap for compression and fast set operations (u64 support)
- Posting lists are balanced by the hierarchical clustering algorithm (target size configurable,
  e.g., 1000-5000 vectors)
- LIRE maintenance splits oversized postings and merges undersized ones
- The deleted posting list (centroid_id = 0) is used to filter search results without per-vector
  tombstone records

**Merge Operators:**

Posting lists use SlateDB merge operators to avoid read-modify-write amplification:

- **Add vector**: Merge with treemap containing the new vector ID (treemap OR)
- **Remove vector**: Merge with deletion marker (treemap AND-NOT)

**Write Batching:**

Naive use of merge operators hurts read performance—each read must deserialize and merge all
pending entries. Conversely, read-modify-write avoids merge overhead but causes write amplification
(deserialize, modify, reserialize on every insert).

The solution is a two-layer approach: buffer writes in memory and merge them there (operating on
`RoaringTreemap` directly), then flush the merged buffer using SlateDB merge operators (operating on
`Bytes`). For example, 100 vector inserts across 10 centroids produce 10 SlateDB merges instead of
100. Both layers use the same merge logic (treemap OR / AND-NOT).

Trade-offs:

- Requires a custom in-memory buffer (essentially a memtable)
- Writes are not immediately visible during the buffer interval
- For queryable buffers, a separate WAL and atomic flush protocol would be needed (out of scope)

**Delete Operation:**

Deleting a vector requires four atomic operations via `WriteBatch`: (1) add vector ID to the
deleted bitmap (centroid_id = 0), (2) tombstone the vector data, (3) tombstone the vector metadata,
(4) tombstone the `IdDictionary` entry. Metadata index cleanup happens during LIRE maintenance.

### `IdDictionary` (`RecordType::IdDictionary` = `0x04`)

Maps user-provided external IDs to internal vector IDs. Enables arbitrary string identifiers while
maintaining compact u64 internal IDs for efficient bitmap operations.

**Key Layout:**

```
┌─────────┬─────────────┬─────────────────┐
│ version │ record_tag  │   external_id   │
│ 1 byte  │   1 byte    │ TerminatedBytes │
└─────────┴─────────────┴─────────────────┘
```

- `external_id`: User-provided identifier (max 64 bytes)

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                     IdDictionaryValue                          │
├────────────────────────────────────────────────────────────────┤
│  vector_id:  u64  (internal vector identifier)                 │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- External IDs are encoded using `TerminatedBytes` for correct lexicographic ordering
- Value is a single u64 internal vector ID (8 bytes, little-endian)
- On upsert, the dictionary entry is updated to point to the new internal ID (old entry tombstoned,
  new entry written)
- During delete, the dictionary entry is tombstoned along with vector data/metadata

### `VectorData` (`RecordType::VectorData` = `0x05`)

Stores the raw vector bytes for a single vector. Vectors are stored individually to enable efficient
point lookups and partial loading during filtered searches.

**Key Layout:**

```
┌─────────┬─────────────┬────────────┐
│ version │ record_tag  │ vector_id  │
│ 1 byte  │   1 byte    │  8 bytes   │
└─────────┴─────────────┴────────────┘
```

- `vector_id` (u64): Internal identifier for the vector (system-assigned)

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                      VectorDataValue                           │
├────────────────────────────────────────────────────────────────┤
│  vector:  FixedElementArray<f32>                               │
│           (dimensions elements, from CollectionMeta)           │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- Vector stored as contiguous `f32` values in little-endian format
- Dimensionality obtained from `CollectionMeta`
- Total value size: `dimensions * 4` bytes
- Vector ID is system-assigned via block-based allocation (see "Block-Based ID Allocation")

### `VectorMeta` (`RecordType::VectorMeta` = `0x06`)

Stores metadata key-value pairs associated with a vector, including the external ID for reverse
lookup. Metadata is stored separately from vector data to enable efficient metadata-only scans
during filtered queries.

**Key Layout:**

```
┌─────────┬─────────────┬────────────┐
│ version │ record_tag  │ vector_id  │
│ 1 byte  │   1 byte    │  8 bytes   │
└─────────┴─────────────┴────────────┘
```

- `vector_id` (u64): Internal identifier for the vector (system-assigned)

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                      VectorMetaValue                           │
├────────────────────────────────────────────────────────────────┤
│  external_id: Utf8  (max 64 bytes, user-provided identifier)   │
│  fields:      Array<MetadataField>                             │
│                                                                │
│  MetadataField                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  field_name:  Utf8                                       │  │
│  │  value:       MetadataValue                              │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                │
│  MetadataValue (tagged union)                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  tag:    u8  (0=string, 1=int64, 2=float64, 3=bool)      │  │
│  │  value:  Utf8 | i64 | f64 | u8                           │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `external_id` stored for reverse lookup (internal ID → external ID) during query results
- Fields carry their canonical name directly, matching entries in `CollectionMeta.metadata_fields`
- Metadata fields serialized in ascending lexicographic order by `field_name`
- Field schema defined in `CollectionMeta`; unknown field names are rejected at write time

### `MetadataIndex` (`RecordType::MetadataIndex` = `0x07`)

Inverted index mapping metadata field/value pairs to the set of vectors with that value. Enables
efficient filtering during hybrid queries. Only fields marked as `indexed=true` in `CollectionMeta`
have index entries.

**Key Layout:**

```
┌─────────┬─────────────┬─────────────────┬────────────────────┐
│ version │ record_tag  │      field      │    value_term      │
│ 1 byte  │   1 byte    │ TerminatedBytes │ MetadataTerm bytes │
└─────────┴─────────────┴─────────────────┴────────────────────┘
```

**Key Fields:**

- `field`: Metadata field name (terminated bytes encoding for correct lexicographic ordering)
- `value_term`: Type-aware encoding of the metadata value for correct byte-wise comparisons

**MetadataTerm Encoding:**

```
┌──────────────────────────────────┐
│ tag: u8  (0=string, 1=int64,     │
│              2=float64, 3=bool)  │
│ payload:                         │
│   string  → Utf8 bytes           │
│   int64   → sortable i64         │
│   float64 → sortable f64         │
│   bool    → u8 (0 or 1)          │
└──────────────────────────────────┘
```

The leading tag byte disambiguates types during decoding. Numeric types use lexicographically
sortable encodings (sign-bit flip + big-endian) to enable efficient range scans.

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                    MetadataIndexValue                          │
├────────────────────────────────────────────────────────────────┤
│  vector_ids:  RoaringTreemap                                   │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- Roaring treemap provides efficient compression and set intersection for u64 IDs
- During filtered search, treemaps for multiple predicates are intersected before vector evaluation
- SlateDB prefix encoding compresses common field name prefixes
- Updates use merge operators (same pattern as `PostingList`): treemap OR to add, treemap AND-NOT to
  remove

**Known Limitation:** Range predicates (e.g., `price < 100`) require OR-ing treemaps for all matching
values. This works well for low-cardinality fields and exact-match filters but is expensive for
high-cardinality numeric fields. See Future Considerations for planned improvements.

### `SeqBlock` (`RecordType::SeqBlock` = `0x08`)

Stores the sequence allocation state for internal vector ID generation. This is a singleton record
used for block-based sequence allocation (see "Block-Based ID Allocation").

**Key Layout:**

```
┌─────────┬─────────────┐
│ version │ record_tag  │
│ 1 byte  │   1 byte    │
└─────────┴─────────────┘
```

The key contains only the version and record tag—no additional fields. This ensures exactly one
`SeqBlock` record exists per collection.

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                       SeqBlockValue                            │
├────────────────────────────────────────────────────────────────┤
│  base_sequence:  u64  (start of allocated block)               │
│  block_size:     u64  (number of IDs in block)                 │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `base_sequence`: First ID in the currently allocated block
- `block_size`: Number of IDs pre-allocated (implementation-defined, not exposed via config)
- On initialization, if no `SeqBlock` exists, allocate starting from ID 1
- The allocated range is `[base_sequence, base_sequence + block_size)`
- On crash recovery, allocate a fresh block starting at `base_sequence + block_size`

## Query Execution Overview

### Pure ANN Search (no filters)

```
1. Load global centroid index into memory (cached on startup)
2. Search centroid HNSW for k nearest centroids to query vector
3. Load posting lists for those centroids
4. Load deleted bitmap (centroid_id = 0), subtract from candidates
5. For each vector_id in filtered candidates:
   - Load vector data
   - Compute exact distance
6. Return top-k by distance
```

### Filtered Search

```
1. Load deleted bitmap (centroid_id = 0)
2. Estimate filter selectivity (implementation-defined heuristic)
3. If filter is highly selective (< 1% of vectors):
   - Pre-filter: intersect metadata index bitmaps, subtract deleted
   - Load vectors for matching IDs, compute distances
4. If filter is loose (> 50% of vectors):
   - Post-filter: run ANN search, filter results
5. Otherwise:
   - Hybrid: search centroids, intersect posting list with filter bitmap, subtract deleted
6. Return top-k by distance
```

## Alternatives

### Graph-Based Index (HNSW) for Full Dataset

Rejected in favor of SPANN due to: (1) memory overhead (~1.5-2x vector data for edges vs ~0.1-1%
for centroids), (2) poor disk I/O patterns (random pointer-chasing vs sequential reads), and
(3) degraded quality with updates. HNSW is used for the in-memory centroid index only.

### Per-Segment Centroid Index

An earlier design used immutable segments, each with its own centroid index. Rejected due to:
(1) query latency from searching N indexes and merging, (2) complexity of segment-scoped IDs,
and (3) expensive index rebuilds during compaction vs incremental LIRE maintenance.

### Per-Vector Tombstone Records

Rejected because deletions map naturally to the posting list structure—the deleted set is just
another posting list (centroid_id = 0). Loading one bitmap is cheaper than per-vector tombstones.

### Hierarchical Namespace Prefix

Multi-tenant key prefixes (`| collection_id | ...`) rejected in favor of separate SlateDB instances
per collection for isolation, simplicity, and independent scaling.

## Future Considerations

Future RFCs will address:

- **LIRE rebalancing**: Split/merge thresholds, boundary vector reassignment, consistency guarantees
- **Hot buffer**: In-memory HNSW for recent vectors before SPANN assignment
- **Quantization**: Scalar (int8) and product quantization for memory-efficient search
- **Distributed search**: Namespace placement, cross-node aggregation, replication
- **Range index optimization**: The current `MetadataIndex` evaluates range predicates by OR-ing
  bitmaps for matching values, which is expensive for high-cardinality numeric fields (e.g.,
  timestamps, prices). Future work may add bucketed indexes or bit-sliced indexes for efficient
  range scans.
- **Filter selectivity statistics**: Store per-field cardinality, total vector count, and bitmap
  size estimates to enable smarter query planning (pre-filter vs post-filter vs hybrid).
- **Tombstone cleanup**: Deleted vector IDs accumulate in posting lists and metadata indexes
  (the deleted bitmap filters them at query time, but stale IDs waste space and slow scans).
  A cleanup mechanism is needed to periodically rewrite these structures and clear the deleted
  bitmap.

## Updates

| Date       | Description                                                          |
|------------|----------------------------------------------------------------------|
| 2025-01-07 | Initial draft                                                        |

## References

1. **SPANN: Highly-efficient Billion-scale Approximate Nearest Neighbor Search**
   Chen et al., NeurIPS 2021.
   [Paper](https://arxiv.org/abs/2111.08566)
   — Foundational work on disk-based ANN with cluster centroids and posting lists.

2. **SPFresh: Incremental In-Place Update for Billion-Scale Vector Search**
   Zhang et al., SOSP 2023.
   [Paper](https://dl.acm.org/doi/10.1145/3600006.3613166) |
   [PDF](https://www.microsoft.com/en-us/research/wp-content/uploads/2023/08/SPFresh_SOSP.pdf)
   — LIRE rebalancing protocol for maintaining index quality with updates.

3. **Turbopuffer Architecture**
   [Documentation](https://turbopuffer.com/docs/architecture)
   — Production SPANN-style vector database built on object storage.

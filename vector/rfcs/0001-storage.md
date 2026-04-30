# RFC 0001: Vector Database Storage

**Status**: Draft

**Authors**:

- [Rohan Desai](https://github.com/rodesai)
- [Almog Gavra](https://github.com/agavra)
- [Bruno Cadonna](https://github.com/cadonna)

## Summary

This RFC defines the storage model for a vector database built on
[SlateDB](https://github.com/slatedb/slatedb). Each collection (namespace) maintains a single
SPANN-style index for efficient approximate nearest neighbor (ANN) search, inverted indexes for
metadata filtering, and a forward index for retrieving vector data by ID. The index is maintained
incrementally using LIRE-style rebalancing.

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
   rebuilds.

5. **Simple query path** — A single global index per namespace enables fast, straightforward queries.

## Goals

- Define the record key/value encoding scheme
- Define common value encodings for vectors and metadata
- Define indexing record types (centroids, posting lists, metadata indexes)

## Non-Goals (left for future RFCs)

- Compaction/retention policies and LIRE rebalancing mechanics
- Query execution and search algorithm details
- Quantization strategies
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
│   │   1. Validate and write vector to write coordinator │   │
│   │   2. Index batches of vectors during flush:         │   │
│   │     a. Allocate internal ID in dictionary           │   │
│   │     b. Assign to nearest centroid(s)                │   │
│   │     c. Update postings via merge operator           │   │
│   │     d. Update inverted index for indexed fields     │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │              Global SPANN Index                     │   │
│   │                                                     │   │
│   │   Centroids:     centroid tree depth                │   │
│   │   Posting Lists: vector_id →                        │   │
│   │                  [(vector_id, f32[dimensions])]     │   │
│   │   Centroid Info: vector_id →                        │   │
│   │                   level + f32[dimensions]           │   │
│   │                   + parent vector_ids               │   │
│   │   Centroid Stats: vector_id → postings count        │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
│   ┌─────────────────────────────────────────────────────┐   │
│   │                  Vector Storage                     │   │
│   │                                                     │   │
│   │   IdDictionary:    external_id → internal vector_id │   │
│   │   VectorData:      vector_id →                      │   │
│   │                    external_id + metadata           │   │
│   │                      + f32[dimensions]              │   │
│   │   VectorIndexData: vector_id → indexed fields       │   │
│   │                      + centroids                    │   │
│   │   MetadataIndex: (field, value) → vector IDs        │   │
│   │                      + deleted vector IDs           │   │
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
│   ┌─────────────────────────────────────────────────────┐   │
│   │              Compaction Filters (background)        │   │
│   │                                                     │   │
│   │   - Clean up deleted vectors from inverted index    │   │
│   └─────────────────────────────────────────────────────┘   │
│                                                             │
│   Storage: SlateDB (LSM KV Store)                           │
│   (vectors, posting lists, metadata all as KV pairs)        │
└─────────────────────────────────────────────────────────────┘
```

### Background on the Ingest/Query Path

This RFC focuses on storage design, but understanding the ingest/query paths motivates the
storage model.

A vector database query has two capabilities:

- **Vector similarity search**: Find vectors closest to a query vector
- **Metadata filtering**: Restrict results to vectors matching predicate filters

A SPANN-style vector database has the following storage components:

- **ANN index**: Tree of cluster centroids for fast navigation to the referenced vectors are
  centroids at the next inner level.
- **Vector data**: Vector data (id, metadata, and dimensions) keyed by vector ID
- **Metadata index**: Inverted index mapping metadata field/value pairs to vector IDs

#### ANN Index

The heart of the system is the ANN index which supports fast ANN (approximate nearest neighbour)
search. The ANN index is a search tree designed to support fast navigation to vectors near a query,
incremental updates as vectors are ingested, and lazy partial loading at search time to allow
for fast cold queries.

The tree is made up of levels. Each level stores vectors. The vectors stored at a given level
are the centroids of clusters of vectors at the next level. At the top is a single root node. At
the bottom are the indexed data vectors. The level immediately above the data vectors, called
the leaf, holds the centroids whose postings reference the data vectors.

Each level represents a sampling of the full space represented by the data vectors. As vectors
are ingested, the db traverses the tree from the bottom up and runs LIRE at each level
to maintain a high quality search index at each level. It first adds each data vector to its
posting in the leaf centroids, then executes splits/merges of leaf centroids (level 1). These
splits and  merges mutate the postings at level 2. The indexer then executes splits/merges at
level 2, and so on. Eventually, it reaches the root. If the root becomes too large the indexer
splits it by adding a new level of centroids and writing a new root. The details of the indexing
process are detailed in RFC-0005.

The figure below depicts an example 4-level tree:
```ascii
                     root (255.0)                                      Centroid ID:
  Level: Root       ┌────────────────────────────────────┐      2.1000 => Level 2, Id 1000
(~100 centroids)    │2.1000:<vector>,2.1001:<vector>,... │
                    └────────────────────────────────────┘

                     2.1000                                 2.1001
    Level: 2        ┌────────────────────────────────────┐ ┌────────────────────────────────────┐
(~10K centroids)    │1.100:<vector>,1.101:<vector>,...   │ │1.200:<vector>,1.201:<vector>,...   │  ...
                    └────────────────────────────────────┘ └────────────────────────────────────┘

                     1.100                                  1.101
   Level: 1         ┌────────────────────────────────────┐ ┌────────────────────────────────────┐
(~1M centroids)     │0.10:<vector>,0.11:<vector>,...     │ │0.20:<vector>,0.21:<vector>,...     │  ...
                    └────────────────────────────────────┘ └────────────────────────────────────┘


  Level: Data       ┌────────────────────────────────────────────────────────────────────────────────┐
(~100M vectors)     │                   Data Vectors (0.10, 0.11, 0.20, 0.21, ...)                   │
                    └────────────────────────────────────────────────────────────────────────────────┘
```

Search in the index proceeds from the root down. At each level, search finds the B nearest vectors
to the search query, and reads their postings to seed the search at the next level down. B is called
the "beam" width. For a tree with N levels (internal levels [N-2..1]), beam width B, and query with
vector Q for the top K centroids:

1. Load the root and read the postings into set P (each entry is a centroid ID and vector). Let
   `current_level` be N-2.
2. While `current_level > 1`:
   1. Find the B closest centroids in P to Q
   2. Clear P and load the postings for B into P
   3. Let `current_level` be `current_level - 1`
3. Return the K closest centroids in P

#### Query

Queries use the ANN index along with the metadata index to execute filtered ANN search. To
illustrate, here's how the query
`find top-10 similar to query_vec where category="shoes" and price < 50` is served:

1. Search the ANN index to find the k nearest leaf centroids to `query_vec`
2. Load posting lists for those centroids from storage
3. Intersect candidate vector IDs with the metadata filter `{category="shoes", price < 50}` using
   the inverted index
4. Compute exact distances for remaining candidates
5. Return top-10 by distance

#### Ingest

High volume ingest to vector presents a few challenges. Of note, (1) writes typically require
updating large entities like centroid and metadata index posting lists by adding new vectors,
(2) updates require reading some of the old data to know what index state needs to be cleaned up,
and (3) each write can result in multiple downstream ANN index maintenance operations (e.g.
centroid splits/merges, reassignments, etc). The details of the latter are covered in a later RFC.

Vector addresses these challenges by:
1. Decoupling indexing from writes and indexing large batches of writes at a time. This allows
   for driving high i/o parallelism when indexing requires reads, and amortizes some of the
   index maintenance work over many vectors.
1. Doing logical read-modify-writes of large values like centroid postings and inverted indexes as
   SlateDB merges. Further, as another benefit of the aforementioned batching, vector applies
   many such writes together in a single SlateDB `WriteBatch` to reduce merge overhead within
   SlateDB and instead do the merges directly within the batch. Generally, merges done by SlateDB
   are more expensive as they require deserializing to the Vector in-memory model, applying the
   merge, and then reserializing.

**Delete Operation:**

Deleting a vector requires the following atomic operations via `WriteBatch`:
1. tombstone the vector data
1. tombstone the `IdDictionary` entry.
1. mark the vector as deleted in its centroid posting
1. mark the vector as deleted in its metadata index entries

Note that marking the vector deleted in centroid postings and metadata index entries don't 
result in the vector automatically being removed by SlateDB as the merge operator preserves 
these tombstones to ensure they cover references to the vector in earlier rows. The tombstones 
can be safely removed once the row marking the vector as deleted has been merged into the lowest
SR. To handle this we will use a custom compaction filter that removes deleted vectors from 
centroid postings and metadata index entries after a compaction into SR 0.

### Record Layout

Record keys are built by concatenating big-endian binary tokens. Lexicographical ordering of keys
matches numeric ordering of encoded values.

Key identifiers used in this RFC:

- `vector_id` (u64): Internal vector identifier, system-assigned, unique within the namespace
- `external_id` (string): User-provided identifier, max 64 bytes, maps to internal vector_id

### External ID and Vector ID

Users provide **external IDs**—arbitrary strings up to 64 bytes—to identify their vectors. The
system maintains an `IdDictionary` that maps each external ID to a system-assigned **Vector ID**
(u64). Vector IDs are also used for centroid vectors in the ANN index.

Each Vector ID is made up of 2 parts. The upper 8 bits specify the level at which the vector is
present in the ANN tree. Data vectors always have their level byte set to 0. Leaf centroids
have their level byte set to 1, and so on. The root always has its level byte set to `0xFF`. The
lower 56 bits specify an ID number. We allocate sequential ID numbers for data vectors from a
sequence allocator, and sequential ID numbers for centroids from another allocator. Encoding the
vector's level is mostly ergonomic - its very clear from inspecting the id whether we're dealing
with a data vector or centroid, and at what level of the tree. This allows for clearer logging and
invariant checking throughout the indexer. It also allows for having a single ID for vectors thats
usable in bitmaps.

```ascii
Vector ID
┌───────────────┬─────────────────────────────────────────────────────────────────────────┐
│ANN Tree Level │              ID Number (monotonically increasing, 56 bits)              │
│   (8-bits)    │                                                                         │
└───────────────┴─────────────────────────────────────────────────────────────────────────┘
```

Vector IDs are used in all index structures (posting lists, metadata indexes) because:

1. Fixed-width u64 keys enable efficient bitmap operations (RoaringTreemap)
2. Monotonically increasing IDs improve bitmap compression (sequential IDs cluster well)
3. Decoupling external from internal IDs allows the system to manage ID lifecycle

**Upsert Behavior:**

When inserting a vector with an external ID that already exists:

1. Look up the existing vector ID from `IdDictionary`
2. Delete the old vector: tombstone `VectorData` and `VectorIndexData`
3. Clean up the old vector from centroid and metadata index postings
4. Allocate a new vector ID
5. Write new vector data and postings with the new internal ID
6. Update `IdDictionary` to point to the new internal ID

### Block-Based ID Allocation

Each vector IDs is allocated from a monotonically increasing counter using block-based
allocation (see
[RFC-0002: Block-Based Sequence Allocation](https://github.com/opendata-oss/opendata/blob/main/rfcs/0002-seq-block.md))

### Standard Key Prefix

All records use a standard 3-byte prefix: a single `u8` for the subsystem, a `u8` for the key
format version, and a `u8` for the record tag. The record tag encodes the record type in the high
4 bits.

```
key prefix byte layout:
┌───────────┬─────────┬────────────┐
│ subsystem │ version │ record_tag │
│  1 byte   │ 1 byte  │   1 byte   │
└───────────┴─────────┴────────────┘
```

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
- `Array<S=u16,T>`: `count: S` followed by `count` serialized elements of type `T`. Arrays that 
  use the default count size `u16` are written as `Array<T>`.
- `FixedElementArray<T>`: Serialized elements back-to-back with no count prefix;
- `RoaringTreemap`: Roaring treemap serialization format for compressed u64 integer sets (64-bit
  extension of Roaring bitmap).
- `Vector ID`: The encoded u64 vector ID. For vector ID we always encode as big-endian.

**Key Encodings** (big-endian for lexicographic ordering):

- `TerminatedBytes`: Variable-length bytes with escape sequences and `0x00` terminator for
  lexicographic ordering. Using `0x00` as terminator ensures shorter keys sort before longer
  keys with the same prefix (e.g., `/foo` < `/foo/bar`). See the [TerminatedBytes](../../common/src/serde/terminated_bytes.rs) module for more details.

**Note on Endianness**: Value schemas use little-endian encoding. Key schemas use big-endian to
maintain lexicographic ordering for range scans.

### Record Type Reference

| ID     | Name               | Description                                                 |
|--------|--------------------|-------------------------------------------------------------|
| `0x00` | *(reserved)*       | Reserved for future use                                     |
| `0x01` | `CollectionMeta`   | Global schema: dimensions, distance metric, field specs     |
| `0x02` | `PostingList`      | Maps centroid IDs to vector IDs in that cluster             |
| `0x03` | `IdDictionary`     | Maps external string IDs to internal u64 vector IDs         |
| `0x04` | `VectorData`       | Stores vector and metadata kvpairs for each vector          |
| `0x05` | `MetadataIndex`    | Inverted index mapping metadata values to vector IDs        |
| `0x06` | `SeqBlock`         | Stores sequence allocation state for data Vector ID gen     |
| `0x07` | `CentroidStats`    | Stores centroid posting counts                              |
| `0x08` | `Centroids`        | Stores metadata about the ANN tree                          |
| `0x09` | `CentroidInfo`     | Stores metadata about a centroid (vector, parent centroid)  |
| `0x0a` | `CentroidSeqBlock` | Stores sequence allocation state for centroid Vector ID gen |
| `0x0b` | `VectorIndexData`  | Stores centroid + indexed metadata kv pairs for each vector |

## Record Definitions & Schemas

### `CollectionMeta` (`RecordType::CollectionMeta` = `0x01`)

Stores the global schema and configuration for the vector collection. This is a singleton record
that defines the structure all vectors in the namespace must conform to.

**Key Layout:**

```
┌───────────┬─────────┬────────────┐
│ subsystem │ version │ record_tag │
│  1 byte   │ 1 byte  │   1 byte   │
└───────────┴─────────┴────────────┘
```

- `subsystem` (u8): Vector subsystem identifier (currently `0x02`)
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
- `metadata_fields`: Schema for metadata fields, including which fields are indexed for filtering

**Schema Evolution:**

The `schema_version` field tracks metadata schema changes only (not structural changes to the
collection). Supported evolutions:

- **Adding metadata fields**: New fields can be appended to `metadata_fields`. Existing vectors
  without the field return null/missing for that field.
- **Enabling indexing**: A field's `indexed` flag can be changed from false to true. A background
  job must rebuild the `MetadataIndex` entries for existing vectors.

Unsupported changes (require creating a new collection):

- Changing `dimensions` or `distance_metric`
- Removing metadata fields or changing their types
- Disabling indexing on a field (index entries would become stale)

### `PostingList` (`RecordType::PostingList` = `0x02`)

Maps a vector ID `V_centroid` to the list of vector IDs assigned to the cluster represented by
V_centroid. During search, posting lists for the nearest leaf (level=1) centroids are loaded and
their vectors evaluated.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────────┐
│ subsystem │ version │ record_tag │  vector_id   │
│  1 byte   │ 1 byte  │   1 byte   │   8 bytes    │
└───────────┴─────────┴────────────┴──────────────┘
```

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                     PostingListValue                           │
├────────────────────────────────────────────────────────────────┤
│  postings: Array<u32, Posting>                                 │
│  data: FixedElementArray<f32>                                  │
│                                                                │
│  Posting                                                       │
│  ┌──────────────────────────────────────────────────────────┐  │
|  |  id:          u64                                        │  │
│  │  offset:      u32 (0xFFFFFFFF | offset in data)          │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- The postings field is an array of mutations to the posting. The mutations are ordered by 
  vector id.
- Each entry specifies either addition or deletion of a vector. The distinction is made by the 
  value of the offset field. If its set to `u32::MAX` (`0xFFFFFFFF`) then the entry represents a
  delete. Otherwise, the entry represents addition and the value indicates the position of the 
  vector in he dta field.
- Posting lists are balanced by the hierarchical clustering algorithm (target size configurable,
  e.g., 50-150 vectors)
- It's expected that posting lists will be relatively small. Our evaluation of SPFresh has found
  that it is optimal to maintain one centroid for ~100 vectors. We also expect Vector to have a
  high ingestion rate and modest query rate. For query, deserializing posting lists is a hotspot. 
  So we optimize for a structure that can be efficiently updated. At query time, we can access the
  vector data from the raw bytes.

**Merge Operators:**

Posting lists use SlateDB merge operators to avoid read-modify-write amplification:

- New vectors are added to a posting as a new merge value with all elements of type Append
- Vectors removed by LIRE or deletion are removed from a posting as a new merge value with
  elements of type Delete

### `IdDictionary` (`RecordType::IdDictionary` = `0x03`)

Maps user-provided external IDs to internal vector IDs. Enables arbitrary string identifiers while
maintaining compact u64 internal IDs for efficient bitmap operations.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬─────────────────┐
│ subsystem │ version │ record_tag │   external_id   │
│  1 byte   │ 1 byte  │   1 byte   │ TerminatedBytes │
└───────────┴─────────┴────────────┴─────────────────┘
```

- `external_id`: User-provided identifier (max 64 bytes)

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                     IdDictionaryValue                          │
├────────────────────────────────────────────────────────────────┤
│  vector_id:  VectorId  (internal vector identifier)            │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- External IDs are encoded using `TerminatedBytes` for correct lexicographic ordering
- Value is the encoded Vector ID.
- On upsert, the dictionary entry is updated to point to the new internal ID (old entry tombstoned,
  new entry written)
- During delete, the dictionary entry is tombstoned along with vector data/metadata

### `VectorData` (`RecordType::VectorData` = `0x04`)

Stores metadata key-value pairs associated with a vector, including the external ID for reverse
lookup, and the actual vector data.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬────────────┐
│ subsystem │ version │ record_tag │ vector_id  │
│  1 byte   │ 1 byte  │   1 byte   │  8 bytes   │
└───────────┴─────────┴────────────┴────────────┘
```

- `vector_id` (u64): Internal identifier for the vector (system-assigned)

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────────────┐
│                      VectorDataValue                                   │
├────────────────────────────────────────────────────────────────────────┤
│  external_id: Utf8  (max 64 bytes, user-provided identifier)           │
│  fields:      Array<Field>                                             │
│                                                                        │
│  Field                                                                 │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  field_name:  Utf8                                               │  │
│  │  value:       FieldValue                                         │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  FieldValue (tagged union)                                             │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  tag:    u8  (0=string, 1=int64, 2=float64, 3=bool, 255=vector)  │  │
│  │  value:  Utf8 | i64 | f64 | u8 | Vector<D>                       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `external_id` stored for reverse lookup (internal ID → external ID) during query results
- Fields carry their canonical name directly, matching entries in `CollectionMeta.metadata_fields`, except for
  the special field "vector" which always has the tag `0xff` and stores the raw vector data.
- Metadata fields serialized in ascending lexicographic order by `field_name`
- Field schema defined in `CollectionMeta`; unknown field names are rejected at write time

### `VectorIndexData` (`RecordType::VectorIndexData` = `0x05`)

Stores the subset of per-vector state needed to maintain secondary indexes during upserts,
deletes, and reassignments. This record allows the indexer to remove a vector from its old
centroid posting(s) and old metadata index entries without reading the full `VectorData` record.
We store the indexed fields in both `VectorIndexData` and `VectorData`. We opt to duplicate these
fields to allow for efficiently reading the required data at indexing and query times, at the cost
of added storage. We expect the indexed fields to be small relative to the vector and non-indexed
fields.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬────────────┐
│ subsystem │ version │ record_tag │ vector_id  │
│  1 byte   │ 1 byte  │   1 byte   │  8 bytes   │
└───────────┴─────────┴────────────┴────────────┘
```

- `vector_id`: Internal identifier for the data vector. `VectorIndexData` is only valid for level-0
  data vectors.

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────────────┐
│                    VectorIndexDataValue                                │
├────────────────────────────────────────────────────────────────────────┤
│  postings:       Array<VectorId>                                       │
│  indexed_fields: Array<Field>                                          │
│                                                                        │
│  Field                                                                 │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  field_name:  Utf8                                               │  │
│  │  value:       FieldValue                                         │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `postings` stores the centroid IDs of the posting lists that currently contain the vector. In
  the current implementation this array always has a single centroid ID, but it is encoded as
  an array to leave room for future replication / multi-posting assignment.
- Every `postings` entry must be a centroid `VectorId`.
- `indexed_fields` stores only the metadata fields from `VectorData` whose
  `CollectionMeta.metadata_fields[i].indexed` flag is true.
- `indexed_fields` are serialized in ascending lexicographic order by `field_name`.

**Write / Update Semantics:**

- On insert, `VectorIndexData` is written alongside `VectorData`, using the vector's assigned leaf
  centroid in `postings` and the vector's indexed metadata fields in `indexed_fields`.
- On upsert, the old `VectorIndexData` is read first. Its `postings` are used to remove the old
  vector ID from centroid postings, and its `indexed_fields` are used to exclude the old vector ID
  from metadata indexes. A new `VectorIndexData` record is then written for the new vector ID.
- During leaf-centroid splits and vector reassignments, the vector's `postings` are rewritten to
  its new centroid assignment while preserving `indexed_fields`.
- On delete, `VectorIndexData` is tombstoned along with `VectorData` and the `IdDictionary` entry.

### `MetadataIndex` (`RecordType::MetadataIndex` = `0x06`)

Inverted index mapping metadata field/value pairs to the vectors currently matching that value.
Enables efficient filtering during hybrid queries. Only fields marked as `indexed=true` in
`CollectionMeta` have index entries.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬─────────────────┬────────────────────┐
│ subsystem │ version │ record_tag │      field      │    value_term      │
│  1 byte   │ 1 byte  │   1 byte   │ TerminatedBytes │ MetadataTerm bytes │
└───────────┴─────────┴────────────┴─────────────────┴────────────────────┘
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
│  included:  VectorBitmap                                       │
│  excluded:  VectorBitmap                                       │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `included` contains vector IDs that should be treated as present for this field/value pair.
- `excluded` contains vector IDs that should be treated as removed for this field/value pair.
- Both fields use `VectorBitmap`, which wraps a `RoaringTreemap` for compressed storage and
  efficient set operations over `VectorId`s.
- During query execution, the effective result set is `included - excluded`.
- During filtered search, effective bitmaps for multiple predicates are intersected before exact
  vector evaluation.

**Write / Update Semantics:**

- On insert, for each indexed field `(name, value)`, the vector ID is added to the `included`
  bitmap of that metadata index entry.
- On delete, for each indexed field previously associated with the vector, the vector ID is added
  to the `excluded` bitmap of that metadata index entry.
- On upsert, the old vector ID is excluded from the old indexed field/value pairs and the new
  vector ID is included in the new indexed field/value pairs. This allows upserts to be applied
  as append-only merges without first rewriting the full bitmap.
- The vector's indexed field/value pairs are stored in `VectorIndexData` so the indexer can clean
  up the old metadata index entries during upsert or reassignment without reading `VectorData`.

### `SeqBlock` (`RecordType::SeqBlock` = `0x07`)

Stores the sequence allocation state for internal vector ID generation. This is a singleton record
used for block-based sequence allocation (see "Block-Based ID Allocation").

**Key Layout:**

```
┌───────────┬─────────┬────────────┐
│ subsystem │ version │ record_tag │
│  1 byte   │ 1 byte  │   1 byte   │
└───────────┴─────────┴────────────┘
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

### `CentroidStats` (`RecordType::CentroidStats` = `0x08`)

Stores the current posting count for a centroid. The tree indexer uses this count to decide when a
centroid should be split or merged.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────────┐
│ subsystem │ version │ record_tag │ centroid_id  │
│  1 byte   │ 1 byte  │   1 byte   │   8 bytes    │
└───────────┴─────────┴────────────┴──────────────┘
```

- `centroid_id`: Centroid `VectorId`.

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                     CentroidStatsValue                         │
├────────────────────────────────────────────────────────────────┤
│  num_vectors: i32                                              │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `num_vectors` is the current number of entries in the centroid's posting list.
- Counts are deleted when the centroid is deleted.

### `Centroids` (`RecordType::Centroids` = `0x09`)

Singleton metadata describing the current shape of the centroid tree.

**Key Layout:**

```
┌───────────┬─────────┬────────────┐
│ subsystem │ version │ record_tag │
│  1 byte   │ 1 byte  │   1 byte   │
└───────────┴─────────┴────────────┘
```

The key contains only the version and record tag, so there is exactly one `Centroids` record per
collection.

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────┐
│                       CentroidsValue                           │
├────────────────────────────────────────────────────────────────┤
│  depth: u8                                                     │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `depth` is the number of levels in the ANN tree.
- Data vectors are always level `0`.
- Leaf centroids are level `1`.
- Higher centroid levels increase upward.

### `CentroidInfo` (`RecordType::CentroidInfo` = `0x0a`)

Stores the durable metadata for a single centroid.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────────┐
│ subsystem │ version │ record_tag │ centroid_id  │
│  1 byte   │ 1 byte  │   1 byte   │   8 bytes    │
└───────────┴─────────┴────────────┴──────────────┘
```

- `centroid_id`: Centroid `VectorId`. Centroid IDs are globally unique across all centroid levels.

**Value Schema:**

```
┌────────────────────────────────────────────────────────────────────────┐
│                        CentroidInfoValue                               │
├────────────────────────────────────────────────────────────────────────┤
│  level:            u8                                                  │
│  vector:           Array<f32>                                          │
│  parent_vector_id: VectorId                                            │
└────────────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `level` is the centroid's level in the ANN tree.
- `vector` is the centroid vector.
- `parent_vector_id` is either:
  - the centroid's parent centroid at level `level + 1`, or
  - the distinguished root `VectorId` when the centroid is attached directly to the root posting.
- When a centroid is split, merged, or reassigned to a new parent, its `CentroidInfo` is rewritten.
- When a centroid is deleted, its `CentroidInfo` row is deleted atomically with its posting list
  and `CentroidStats`.

### `CentroidSeqBlock` (`RecordType::CentroidSeqBlock` = `0x0b`)

Stores the sequence allocation state for centroid ID generation.

**Key Layout:**

```
┌───────────┬─────────┬────────────┐
│ subsystem │ version │ record_tag │
│  1 byte   │ 1 byte  │   1 byte   │
└───────────┴─────────┴────────────┘
```

The key contains only the version and record tag, so there is exactly one `CentroidSeqBlock`
record per collection.

**Value Schema:**

`CentroidSeqBlock` uses the shared `SeqBlock` value type:

```
┌────────────────────────────────────────────────────────────────┐
│                         SeqBlock                               │
├────────────────────────────────────────────────────────────────┤
│  base_sequence: u64                                            │
│  block_size:    u64                                            │
└────────────────────────────────────────────────────────────────┘
```

**Structure:**

- This sequence block allocates the lower 56-bit numeric portion of centroid `VectorId`s.
- The centroid level is supplied separately by the indexer when constructing a centroid `VectorId`.
- The allocated range is `[base_sequence, base_sequence + block_size)`.
- `SeqBlock` storage and crash-recovery semantics are the same as for data-vector `SeqBlock`, but
  centroid allocation is tracked separately so data-vector IDs and centroid IDs do not contend for
  the same allocator.

## Alternatives

### Deletions Vector

Earlier iterations of the design tracked all deleted vectors in a global deletions bitmap. The
benefit of this approach is that it allows for very cheap upserts/deletes. Deleting a vector
simply requires adding it to the deletions bitmap. On the other hand, this approach pushes cost
/complexity to other places in the system. It's difficult to clear entries from the bitmap. This
would likely require a custom compaction filter in SlateDB. Even then, we'd need some way to clear
the in-memory bitmap once compaction has finished and the db has loaded the latest slatedb manifest.
It's also difficult to use it to clear entries from inverted index postings. We could also do
this from a compaction filter, but the filter would also have to load the bitmap.

To keep things simple, we've opted to instead load the required data at indexing time to let us
clean centroid postings and inverted index bitmaps. We haven't found the added i/os to be a major
bottleneck during indexing since processing large batches lets us drive lots of parallel reads to
local SSD. This approach also adds space amplification as we track deleted vectors in each relevant
bitmap in the inverted index. In practice this should just be a few bits per vector. We can revisit
the global bitmap approach if its a problem.

### Graph-Based Index for Centroids

Rejected in favor of the ANN tree. The graph-based index had 2 problems:
1. It's very expensive to load, as all centroids need to be read and then built into a single
   HNSW graph. This can take 10s of seconds - 10+ minutes for large dbs. The tree allows us to
   load the index lazily/incrementally.
2. HNSW is not robust in the presence of deletions as occurs during LIRE (see section 3.3: 
   https://arxiv.org/pdf/2105.09613). Deletions sever edges in the graph which hurts centroid 
   recall, which in turn hurts query recall.

### Graph-Based Index (HNSW) for Full Dataset

Rejected in favor of SPANN due to: (1) memory overhead (~1.5-2x vector data for edges vs ~0.1-1%
for centroids), (2) poor disk I/O patterns (random pointer-chasing vs sequential reads), and
(3) degraded quality with updates. HNSW is used for the in-memory centroid index only.

### Per-Segment Centroid Index

An earlier design used immutable segments, each with its own centroid index. Rejected due to:
(1) query latency from searching N indexes and merging, (2) complexity of segment-scoped IDs,
and (3) expensive index rebuilds during compaction vs incremental LIRE maintenance.

### Hierarchical Namespace Prefix

Multi-tenant key prefixes (`| collection_id | ...`) rejected in favor of separate SlateDB instances
per collection for isolation, simplicity, and independent scaling.

## Future Considerations

Future RFCs will address:

- **LIRE rebalancing**: Split/merge thresholds, boundary vector reassignment, consistency guarantees
- **Hot buffer**: In-memory HNSW for recent vectors before SPANN assignment
- **Quantization**: Scalar (int8) and/or binary quantization for faster and memory-efficient search
- **Distributed search**: Namespace placement, cross-node aggregation, replication
- **Range index optimization**: The current `MetadataIndex` evaluates range predicates by OR-ing
  bitmaps for matching values, which is expensive for high-cardinality numeric fields (e.g.,
  timestamps, prices). Future work may add bucketed indexes or bit-sliced indexes for efficient
  range scans.
- **Filter selectivity statistics**: Store per-field cardinality, total vector count, and bitmap
  size estimates to enable smarter query planning (pre-filter vs post-filter vs hybrid).

## Updates

| Date       | Description                                                          |
|------------|----------------------------------------------------------------------|
| 2026-01-07 | Initial draft                                                        |
| 2026-01-24 | Store full vectors in postings. Store all attributes in `VectorData` |
|            | and drop `VectorMeta`                                                |
| 2026-04-10 | Update to reflect ANN tree and batched ingest                        |


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

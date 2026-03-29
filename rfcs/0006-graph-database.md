# RFC 0006: Graph Database Storage

**Status**: Draft

**Authors**:

- [Steven Grond](https://github.com/StevenBtw)

## Summary

This RFC defines the storage model for a labeled property graph (LPG) database built on
[SlateDB](https://github.com/slatedb/slatedb). The graph database integrates
[Grafeo](https://github.com/GrafeoDB/grafeo) (v0.5.28) as its query engine, implementing Grafeo's
`GraphStore` and `GraphStoreMut` traits over SlateDB's ordered key-value interface. The design maps
graph primitives: nodes, edges, labels, properties and adjacency, to SlateDB records using the
standard 3-byte key prefix, with additional index structures for label lookups, property searches,
and traversal. Concurrency control is delegated to SlateDB's built-in transaction support with
snapshot isolation.

## Motivation

OpenData provides purpose-built databases for time series, logs, vectors, and key-value workloads,
all sharing SlateDB as a common storage engine. Graph workloads are a natural addition: they appear
in knowledge graphs, social networks, fraud detection, recommendation engines, and infrastructure
dependency mapping. A graph database built on SlateDB inherits the same operational simplicity
(single storage engine, unified tooling) while providing efficient traversal, pattern matching, and
multi-hop queries via Grafeo's query pipeline.

The storage design must support:

1. **Efficient traversal**: Following edges from a node to its neighbors is the fundamental graph
   operation. Adjacency must be stored in a layout that enables fast, direction-aware traversal
   without loading unrelated data.

2. **Point lookups**: Retrieving a node or edge by ID must require a single point lookup via a
   direct key. The query engine issues point lookups during pattern matching and result
   materialization.

3. **Label and property indexing**: GQL queries filter nodes by label (`MATCH (n:Person)`) and by
   property values (`WHERE n.age > 30`). Without indexes, these require full scans.

4. **Concurrent reads and writes**: Readers must see a consistent view while writers mutate the
   graph concurrently. SlateDB's snapshot isolation provides this guarantee without a custom MVCC
   layer.

5. **Schema catalog persistence**: Grafeo maps label names, edge type names, and property key
   names to compact integer IDs for internal processing. This catalog must survive restarts.

6. **Compatibility with Grafeo's storage traits**: The implementation must satisfy the
   `GraphStore` (read) and `GraphStoreMut` (write) trait contracts so that Grafeo's query
   pipeline, optimizer, and execution engine work unmodified.

## Goals

- Define the record key/value encoding scheme for all graph entity types
- Map Grafeo's `GraphStore` and `GraphStoreMut` trait methods to SlateDB operations
- Define the catalog persistence format for labels, edge types, and property keys
- Define adjacency index layout for directional traversal
- Define label and property index layouts for filtered scans
- Define merge operators for atomic counter updates
- Define the concurrency model: delegated to SlateDB transactions

## Non-Goals (left for future RFCs)

- RDF triple store support (SPARQL, triple indexes)
- Query languages beyond GQL (Cypher, Gremlin, GraphQL, SQL/PGQ)
- Graph algorithms plugin (`algos` feature)
- Vector, text, and hybrid search indexes on graph properties
- GWP (gRPC) and Bolt (Neo4j) protocol support
- Write coordination and distributed deployment
- Compaction policies and garbage collection mechanics
- HTTP API design (covered in a future write/read API RFC)

## Dependencies

The graph engine introduces the following external crate dependencies:

### Grafeo Crates

| Crate            | Version | Role                                                                                                                                     |
|------------------|---------|------------------------------------------------------------------------------------------------------------------------------------------|
| `grafeo-core`    | 0.5.28  | Graph storage traits (`GraphStore`, `GraphStoreMut`), core types (`Node`, `Edge`, `NodeId`, `EdgeId`, `Value`, `PropertyKey`)            |
| `grafeo-common`  | 0.5.28  | Shared primitives (`NodeId`, `EdgeId`, `Value` enum); also provides `EpochId` used by the optional `temporal` feature                    |
| `grafeo-engine`  | 0.5.28  | Query engine: GQL parser, cost-based optimizer, push-based vectorized executor. GQL is the primary query interface and is always enabled |

Grafeo is published on crates.io. All three crates are required dependencies. The graph database
always includes the GQL query engine; there is no "storage-only" deployment mode. Additional query
languages (Cypher, SPARQL, etc.) may be added as optional features in the future, but GQL is the
default and mandatory interface.

### Other External Crates

| Crate          | Version | Role                                                                  |
|----------------|---------|-----------------------------------------------------------------------|
| `parking_lot`  | 0.12    | Faster RwLock/Mutex for catalog cache and sequence allocators         |
| `hashbrown`    | 0.14    | HashMap variant used by Grafeo types                                  |
| `arcstr`       | 1.2     | Atomic reference-counted strings (used for label/type/property names) |
| `smallvec`     | 1.13    | Stack-allocated vectors (used for node label lists)                   |

### Precedent

Other OpenData engines depend on external specialized crates: timeseries uses `promql-parser`
(PromQL query parsing) and `tsz` (time series compression), vector uses `usearch` (HNSW
similarity search). The graph engine follows the same pattern, using Grafeo for graph-specific
query parsing and execution. This could be internalized at a later moment, but would require a
significant amount of work to be done correctly (pruning, new integration tests, etc.).

## Design

### Architecture Overview

Each graph database instance corresponds to a single SlateDB instance. Graph entities (nodes,
edges), their properties, adjacency indexes, label indexes, property indexes, and catalog
dictionaries are all stored as key-value pairs in the LSM tree. Grafeo's query engine operates
on a `SlateGraphStore` adapter that translates trait method calls into SlateDB reads and writes.
Concurrency control is delegated to SlateDB's built-in transaction support, the adapter does not
maintain its own MVCC layer.

```ascii
┌──────────────────────────────────────────────────────────────────────┐
│                   OpenData Graph (per database)                      │
│                                                                      │
│   ┌──────────────────────────────────────────────────────────────┐   │
│   │                   Grafeo Query Engine                        │   │
│   │                                                              │   │
│   │   GQL Parser → Binder → Optimizer → Executor                 │   │
│   │                                                              │   │
│   │   Operates on GraphStore / GraphStoreMut trait objects       │   │
│   └──────────────────────────┬───────────────────────────────────┘   │
│                              │                                       │
│   ┌──────────────────────────▼───────────────────────────────────┐   │
│   │               SlateGraphStore Adapter                        │   │
│   │                                                              │   │
│   │   Implements GraphStore (reads) + GraphStoreMut (writes)     │   │
│   │   Translates trait calls → SlateDB get/put/delete/scan       │   │
│   │                                                              │   │
│   │   Read-then-write ops use StorageTransaction (atomicity)     │   │
│   │   Write-only ops use WriteBatch via Storage::apply()         │   │
│   │                                                              │   │
│   │   In-memory components:                                      │   │
│   │   ├─ Catalog cache (label/type/property-key dictionaries)    │   │
│   │   └─ Statistics (cardinality estimates)                      │   │
│   └──────────────────────────┬───────────────────────────────────┘   │
│                              │                                       │
│   ┌──────────────────────────▼───────────────────────────────────┐   │
│   │                    Record Layout                             │   │
│   │                                                              │   │
│   │   NodeRecord    (0x10)  ─ Node existence + labels            │   │
│   │   EdgeRecord    (0x20)  ─ Edge endpoints + type              │   │
│   │   NodeProperty  (0x30)  ─ Per-node property values           │   │
│   │   EdgeProperty  (0x40)  ─ Per-edge property values           │   │
│   │   ForwardAdj    (0x50)  ─ Outgoing adjacency index           │   │
│   │   BackwardAdj   (0x60)  ─ Incoming adjacency index           │   │
│   │   LabelIndex    (0x70)  ─ Label → node ID index              │   │
│   │   PropertyIndex (0x80)  ─ Property value → node ID index     │   │
│   │   Catalog       (0x90)  ─ Name ↔ ID dictionaries             │   │
│   │   Metadata      (0xE0)  ─ Counters (node count, edge count)  │   │
│   │   SeqBlock      (0xF0)  ─ ID allocation state                │   │
│   └──────────────────────────┬───────────────────────────────────┘   │
│                              │                                       │
│   Storage: SlateDB (LSM KV Store)                                    │
│   (all graph data as ordered key-value pairs)                        │
│   (transactions with snapshot isolation for concurrency control)     │
└──────────────────────────────────────────────────────────────────────┘
```

### Background on Grafeo's Storage Traits

Grafeo's `GraphStore` trait defines the read interface: point lookups by ID, individual property
access (with batch/selective variants for projection pushdown), directional traversal
(`Outgoing`/`Incoming`/`Both`), label-filtered scans, property-filtered searches, zone map skip
pruning, and statistics for the cost-based optimizer.

`GraphStoreMut` extends `GraphStore` with mutations: create/delete nodes and edges, set/remove
properties, add/remove labels, and batch edge creation.

#### Core Types

The following Grafeo types are serialized to/from SlateDB records:

| Type                 | Description                        | Size     |
|----------------------|------------------------------------|----------|
| `NodeId(u64)`        | Node identifier                    | 8 bytes  |
| `EdgeId(u64)`        | Edge identifier                    | 8 bytes  |
| `LabelId(u32)`       | Label dictionary ID                | 4 bytes  |
| `EdgeTypeId(u32)`    | Edge type dictionary ID            | 4 bytes  |
| `PropertyKeyId(u32)` | Property key dictionary ID         | 4 bytes  |
| `Value`              | Dynamic property value (see below) | variable |

The `Value` enum represents property values:

```text
Value variants:
├─ Null                         tag 0x00
├─ Bool(bool)                   tag 0x01
├─ Int64(i64)                   tag 0x02
├─ Float64(f64)                 tag 0x03
├─ String(ArcStr)               tag 0x04
├─ Bytes(Arc<[u8]>)             tag 0x05
├─ Timestamp(i64 micros)        tag 0x06
├─ Date(i32 days)               tag 0x07
├─ Time(i64 nanos, Option<i32>) tag 0x08
├─ Duration(months,days,nanos)  tag 0x09
├─ ZonedDatetime(...)           tag 0x0A
├─ List(Arc<[Value]>)           tag 0x0B
├─ Map(Arc<BTreeMap<...>>)      tag 0x0C
├─ Vector(Arc<[f32]>)           tag 0x0D
├─ Path { nodes, edges }        tag 0x0E
├─ GCounter(BTreeMap<…>)        tag 0x0F
└─ OnCounter(BTreeMap<…>)       tag 0x10
```

### Identifiers: External and Internal

Grafeo uses `NodeId(u64)` and `EdgeId(u64)` as internal identifiers. These are system-assigned,
monotonically increasing, and compact, enabling efficient key encoding and bitmap operations.

Users interact with these IDs through query results and parameters. Unlike the vector database,
graph entities do not have separate external/internal ID mappings, the `NodeId`/`EdgeId` values
returned by `create_node`/`create_edge` are the canonical identifiers.

### Block-Based ID Allocation

Node and edge IDs are allocated from monotonically increasing counters using block-based allocation
(reusing the common crate's `SequenceAllocator`). Two independent sequences are maintained: one for
node IDs and one for edge IDs.

**Allocation procedure:**

1. On initialization, read the `SeqBlock` records to get the last allocated ranges
2. Allocate new blocks starting after the previous ranges
3. During normal operation, assign IDs from the current blocks
4. When a block is exhausted, allocate a new block and persist the updated `SeqBlock`

**Recovery:**

On crash recovery, read the `SeqBlock` records and allocate fresh blocks starting after the
previous ranges. Unused IDs from pre-crash blocks are skipped, this creates gaps but preserves
monotonicity.

### Standard Key Prefix

All records use the standard 3-byte prefix per [RFC 0001](../../rfcs/0001-record-key-prefix.md):
a `u8` subsystem byte, a `u8` version byte (currently `0x01`), and a `u8` record tag. The graph
subsystem uses subsystem byte `0x05` (to be registered in RFC 0001 upon acceptance of this RFC).
The record tag is a full byte value; some record types (Catalog, SeqBlock) encode a sub-type
discriminant in the lower 4 bits.

### Common Encodings

Key encodings use the primitives defined in [RFC 0004](../../rfcs/0004-common-encodings.md):
`TerminatedBytes` for variable-length keys, sortable `i64`/`f64` for ordered numeric keys
(sign-bit flip, big-endian). Value fields are little-endian.

### Record Type Reference

| Tag    | Name               | Description                                             |
|--------|--------------------|---------------------------------------------------------|
| `0x10` | `NodeRecord`       | Node existence and label list                           |
| `0x20` | `EdgeRecord`       | Edge endpoints and type                                 |
| `0x30` | `NodeProperty`     | Property key-value pair for a node                      |
| `0x40` | `EdgeProperty`     | Property key-value pair for an edge                     |
| `0x50` | `ForwardAdj`       | Outgoing adjacency: src -> (edge_type, dst, edge_id)    |
| `0x60` | `BackwardAdj`      | Incoming adjacency: dst -> (edge_type, src, edge_id)    |
| `0x70` | `LabelIndex`       | Label -> node ID mapping for label scans                |
| `0x80` | `PropertyIndex`    | Sortable property value -> node ID for filtered search  |
| `0x9_` | `Catalog`          | Name-to-ID dictionaries (6 sub-types)                   |
| `0xE0` | `Metadata`         | Global counters (node count, edge count)                |
| `0xF0` | `SeqBlock`         | Sequence allocation state for node/edge ID generation   |

## Record Definitions & Schemas

### `NodeRecord` (`0x10`)

Stores the existence of a node and its labels. Each node has exactly one `NodeRecord` key. Deletion
removes the key entirely (SlateDB tombstone), rather than using a soft-delete flag.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬──────────┐
│ subsystem │ version │ record_tag  │ node_id  │
│  1 byte   │ 1 byte  │   1 byte    │ 8 bytes  │
└───────────┴─────────┴─────────────┴──────────┘
```

Total: 11 bytes.

- `subsystem` (u8): `0x05` (Graph)
- `version` (u8): Key format version (currently `0x01`)
- `record_tag` (u8): `0x10`
- `node_id` (u64): Big-endian node identifier

**Value Schema:**

```text
┌────────────────────────────────────────────────────────┐
│                    NodeRecordValue                     │
├────────────────────────────────────────────────────────┤
│  label_ids: FixedElementArray<u32 LE>                  │
└────────────────────────────────────────────────────────┘
```

Encoded as a `FixedElementArray<u32>` per
[RFC 0004](../../rfcs/0004-common-encodings.md#fixedelementarrayt): the label count is derived
from `value_length / 4`, so no explicit count prefix is needed.

**Structure:**

- `label_ids` are catalog-assigned `LabelId` values. Label names are resolved via the catalog.
- A node with no labels is valid (empty value).
- If the key does not exist in SlateDB, the node does not exist.

**Point Lookup:**

To retrieve a node by ID: single `get()` on the encoded key.

### `EdgeRecord` (`0x20`)

Stores the existence of an edge, its source and destination nodes, and edge type. Each edge has
exactly one `EdgeRecord` key. Deletion removes the key via SlateDB tombstone.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬──────────┐
│ subsystem │ version │ record_tag  │ edge_id  │
│  1 byte   │ 1 byte  │   1 byte    │ 8 bytes  │
└───────────┴─────────┴─────────────┴──────────┘
```

Total: 11 bytes.

- `edge_id` (u64): Big-endian edge identifier

**Value Schema:**

```text
┌────────────────────────────────────────────────────────┐
│                    EdgeRecordValue                     │
├────────────────────────────────────────────────────────┤
│  src_node_id:  u64 (LE)                                │
│  dst_node_id:  u64 (LE)                                │
│  edge_type_id: u32 (LE)                                │
│  prop_count:   u16 (LE)                                │
└────────────────────────────────────────────────────────┘
```

Total: 22 bytes.

**Structure:**

- `src_node_id` and `dst_node_id` are the endpoints of the directed edge.
- `edge_type_id` is a catalog-assigned `EdgeTypeId`. The type name is resolved via the catalog.
- `prop_count` tracks the number of `EdgeProperty` records for this edge. Currently written as `0`;
  reserved for future use by the query optimizer for property existence checks without scanning.
- Point lookup is a single `get()` on the encoded key.

### `NodeProperty` (`0x30`)

Stores a single property key-value pair for a node. Properties are stored as individual records
rather than as a single serialized map. This design enables:

- **Projection pushdown**: Reading only the requested properties without deserializing a full map.
- **Selective mutation**: Setting or removing a single property without read-modify-write on the
  full property set.
- **Batch property access**: The `get_nodes_properties_selective_batch` trait method benefits from
  targeted key lookups.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬──────────┬─────────────────┐
│ subsystem │ version │ record_tag  │ node_id  │ property_key_id │
│  1 byte   │ 1 byte  │   1 byte    │ 8 bytes  │    4 bytes      │
└───────────┴─────────┴─────────────┴──────────┴─────────────────┘
```

Total: 15 bytes.

- `node_id` (u64): Big-endian node identifier
- `property_key_id` (u32): Big-endian catalog-assigned `PropertyKeyId`

**Value Schema:**

```text
┌────────────────────────────────────────────────────────┐
│                  NodePropertyValue                     │
├────────────────────────────────────────────────────────┤
│  value: Value::serialize() bytes                       │
└────────────────────────────────────────────────────────┘
```

Property values are serialized using Grafeo's native `Value::serialize()` format (see the
"Value Serialization" section below).

**Deletion:**

Removing a property is done by issuing a SlateDB tombstone for the key. After compaction, the
record disappears entirely.

**All Properties for a Node:**

To load all properties for a node (e.g., for `get_node`), perform a prefix scan:
`[0x05, 0x01, 0x30, node_id_be]` — this returns all property records for that node, each keyed by
`property_key_id`. Resolve property names via the catalog.

### `EdgeProperty` (`0x40`)

Identical layout to `NodeProperty`, but for edges.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬──────────┬─────────────────┐
│ subsystem │ version │ record_tag  │ edge_id  │ property_key_id │
│  1 byte   │ 1 byte  │   1 byte    │ 8 bytes  │    4 bytes      │
└───────────┴─────────┴─────────────┴──────────┴─────────────────┘
```

**Value Schema:** Same as `NodePropertyValue`.

### `ForwardAdj` (`0x50`)

Stores outgoing adjacency: for a given source node, records each outgoing edge with its type and
destination. Adjacency is stored as **individual keys** rather than a single serialized list. This
avoids read-modify-write amplification for high-degree nodes, adding or removing an edge is a
single `put` or `delete` without touching other adjacency entries.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬──────────────┬───────────────┬──────────────┬──────────┐
│ subsystem │ version │ record_tag  │ src_node_id  │ edge_type_id  │ dst_node_id  │ edge_id  │
│  1 byte   │ 1 byte  │   1 byte    │   8 bytes    │   4 bytes     │   8 bytes    │ 8 bytes  │
└───────────┴─────────┴─────────────┴──────────────┴───────────────┴──────────────┴──────────┘
```

Total: 31 bytes.

**Value Schema:** Empty (all information is in the key).

**Structure:**

- The key is designed for efficient prefix scans:
  - `[subsystem, version, tag, src_node_id]`, all outgoing edges from a node (implements `neighbors(node, Outgoing)`)
  - `[subsystem, version, tag, src_node_id, edge_type_id]`, outgoing edges of a specific type (type-filtered
    traversal)
  - `[subsystem, version, tag, src_node_id, edge_type_id, dst_node_id]`, check if a specific edge exists
- Edge type is placed before destination to enable type-filtered traversal without loading all
  edge types. In GQL, edge patterns almost always specify a type: `MATCH (a)-[:KNOWS]->(b)` is
  far more common than `MATCH (a)-[]->(b)`. For the untyped case the scan is still bounded to
  one node's adjacency, not a global scan.
- The `edge_id` suffix ensures uniqueness when multiple edges of the same type connect the same
  pair of nodes (multi-edges).

**Traversal:**

To implement `edges_from(node, Outgoing)`:

1. Prefix scan `[0x05, 0x01, 0x50, node_id_be]`
2. Each key encodes `(edge_type_id, dst_node_id, edge_id)`
3. Return `Vec<(NodeId, EdgeId)>` pairs

To implement `out_degree(node)`:

1. Prefix scan `[0x05, 0x01, 0x50, node_id_be]`
2. Count keys

### `BackwardAdj` (`0x60`)

Stores incoming adjacency: for a given destination node, records each incoming edge with its type
and source. This enables efficient `Incoming` and `Both` direction traversal.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬──────────────┬───────────────┬──────────────┬──────────┐
│ subsystem │ version │ record_tag  │ dst_node_id  │ edge_type_id  │ src_node_id  │ edge_id  │
│  1 byte   │ 1 byte  │   1 byte    │   8 bytes    │   4 bytes     │   8 bytes    │ 8 bytes  │
└───────────┴─────────┴─────────────┴──────────────┴───────────────┴──────────────┴──────────┘
```

Mirror of `ForwardAdj` with source and destination swapped.

**Value Schema:** Empty.

**Configuration:**

Backward adjacency is always maintained. While Grafeo allows disabling backward edges via
`Config::backward_edges`, the SlateDB implementation always writes backward adjacency entries.

**Cost and justification:** One extra 31-byte PUT per edge creation; small relative to the total
record set and enables first-class `Incoming`/`Both` traversal without scanning ForwardAdj.

### `LabelIndex` (`0x70`)

Maps a label to the set of nodes that carry it. Stored as individual keys (one per label-node
pair) to avoid read-modify-write on high-cardinality labels.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬───────────┬──────────┐
│ subsystem │ version │ record_tag  │ label_id  │ node_id  │
│  1 byte   │ 1 byte  │   1 byte    │  4 bytes  │ 8 bytes  │
└───────────┴─────────┴─────────────┴───────────┴──────────┘
```

Total: 15 bytes.

**Value Schema:** Empty.

**Label Scan:**

To implement `nodes_by_label(label)`:

1. Resolve `label` → `LabelId` via catalog
2. Prefix scan `[0x05, 0x01, 0x70, label_id_be]`
3. Each key suffix is a `node_id`
4. Return `Vec<NodeId>`

**Why individual keys instead of bitmaps:**

A label like "Person" could apply to millions of nodes, and nodes gain or lose labels
dynamically. Individual keys keep it simple: one PUT to add a label, one DELETE to remove it,
no need to read current state. The trade-off is more keys in the tree, but since they all share
a long common prefix (`[0x05, 0x01, 0x70, label_id]`), SlateDB's prefix compression handles this well.
If set operations become needed for query planning, bucketed bitmaps can be added later as a
compaction-time optimization.

### `PropertyIndex` (`0x80`)

An optional index mapping property values to node IDs. Enables `find_nodes_by_property`,
`find_nodes_by_properties`, and `find_nodes_in_range` without full scans.

Property indexes are maintained automatically for all sortable property types. Only indexed
properties have `PropertyIndex` entries.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬─────────────────┬───────────────────┬──────────┐
│ subsystem │ version │ record_tag  │ property_key_id │    value_term     │ node_id  │
│  1 byte   │ 1 byte  │   1 byte    │    4 bytes      │ SortableValue     │ 8 bytes  │
└───────────┴─────────┴─────────────┴─────────────────┴───────────────────┴──────────┘
```

**SortableValue Encoding:**

The `value_term` is the raw sortable encoding of the property value, without a type tag prefix.
Since each `PropertyIndex` key is scoped to a single `property_key_id`, all values within a prefix
scan are the same type.

```text
Bool    → u8 (0 or 1)              (1 byte)
Int64   → sortable i64             (8 bytes BE)
Float64 → sortable f64             (8 bytes BE)
String  → TerminatedBytes          (variable)
```

Only these four types support sortable encoding; other `Value` types are not indexed.

**Value Schema:** Empty.

**Equality Lookup:**

To implement `find_nodes_by_property(property, value)`:

1. Resolve `property` → `PropertyKeyId` via catalog
2. Encode `value` as `SortableValue`
3. Prefix scan `[0x05, 0x01, 0x80, property_key_id_be, sortable_value]`
4. Each key suffix is a `node_id`

**Range Scan:**

To implement `find_nodes_in_range(property, min, max, min_inclusive, max_inclusive)`:

1. Resolve `property` → `PropertyKeyId`
2. Encode `min` and `max` as `SortableValue`
3. Range scan from `[subsystem, version, tag, property_key_id, min_encoded]` to `[subsystem, version, tag, property_key_id, max_encoded]`
4. Apply inclusivity bounds

### Catalog Records (`0x90` – `0x95`)

The catalog maps human-readable names to compact integer IDs for labels, edge types, and property
keys. Each mapping is stored **bidirectionally**: an ID→name record for reverse lookups and a
name→ID record for forward lookups. On startup, the ID→name records are scanned to populate an
in-memory bidirectional cache. Writes update both the cache and SlateDB atomically.

Six sub-types share the `0x9_` prefix, using the lower 4 bits of the record tag byte for
discrimination:

| Tag  | CatalogKind        | Key Suffix        | Value        |
|------|--------------------|-------------------|--------------|
| 0x90 | LabelById          | id: u32 (BE)      | name (UTF-8) |
| 0x91 | LabelByName        | name (UTF-8)      | id: u32 (LE) |
| 0x92 | EdgeTypeById       | id: u32 (BE)      | name (UTF-8) |
| 0x93 | EdgeTypeByName     | name (UTF-8)      | id: u32 (LE) |
| 0x94 | PropertyKeyById    | id: u32 (BE)      | name (UTF-8) |
| 0x95 | PropertyKeyByName  | name (UTF-8)      | id: u32 (LE) |

#### By-ID Keys (`0x90`, `0x92`, `0x94`)

```text
┌───────────┬─────────┬─────────────┬──────────┐
│ subsystem │ version │ record_tag  │    id    │
│  1 byte   │ 1 byte  │   1 byte    │ 4 bytes  │
└───────────┴─────────┴─────────────┴──────────┘
```

Total: 7 bytes. Value is the name as raw UTF-8 bytes.

#### By-Name Keys (`0x91`, `0x93`, `0x95`)

```text
┌───────────┬─────────┬─────────────┬────────────────────┐
│ subsystem │ version │ record_tag  │      name          │
│  1 byte   │ 1 byte  │   1 byte    │    raw UTF-8       │
└───────────┴─────────┴─────────────┴────────────────────┘
```

Variable length. The name is the last field in the key, so its length is implicit from the key
length; no `TerminatedBytes` encoding is needed. Value is the `u32 (LE)` ID.

**Catalog Loading:**

On startup, prefix scan `[0x05, 0x01, 0x90]`, `[0x05, 0x01, 0x92]`, and `[0x05, 0x01, 0x94]` (the by-ID sub-types)
to populate the in-memory bidirectional maps (name → ID and ID → name). The catalog is expected
to be small (thousands of entries at most), so full loading is practical.

**Why keep the catalog?** Labels, edge types, and property keys are a small stable set (dozens,
not millions) and adjacency keys include the edge type on every entry; a 4-byte integer instead
of a variable-length string saves significant space at scale. The timeseries engine decided
against a dictionary because metric label cardinality is higher and series fingerprinting
provides an alternative compact identifier.

**ID Assignment:**

New catalog entries use incrementing IDs starting from 0. The next available ID for each catalog
type is derived from the count of entries loaded from the by-ID scan. This assumes single-writer
semantics: only one process writes to the catalog at a time. This is consistent with OpenData's
current single-writer model per database instance.

**Write Path:**

When a new name is encountered (e.g., a new label "Person"), two records are written atomically:

1. By-ID: `[0x05, 0x01, 0x90, id_be]` → `"Person"` (UTF-8 bytes)
2. By-Name: `[0x05, 0x01, 0x91, "Person"]` → `id_le` (4 bytes)

### `Metadata` (`0xE0`)

Stores global counters. These are singleton records with fixed well-known keys.

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┬──────────────┐
│ subsystem │ version │ record_tag  │ metadata_key │
│  1 byte   │ 1 byte  │   1 byte    │    1 byte    │
└───────────┴─────────┴─────────────┴──────────────┘
```

**Metadata Keys:**

| Key    | Name           | Value Type | Description                       |
|--------|----------------|------------|-----------------------------------|
| `0x00` | `NodeCount`    | u64 (LE)   | Total live (non-deleted) nodes    |
| `0x01` | `EdgeCount`    | u64 (LE)   | Total live (non-deleted) edges    |

**Merge Operator:**

`NodeCount` and `EdgeCount` use merge operators for atomic increment/decrement. Each mutation
(create or delete) issues a merge with a signed delta (`+1` or `-1`). The merge function sums
all deltas.

### `SeqBlock` (`0xF0`)

Stores sequence allocation state for node and edge ID generation. Two `SeqBlock` records exist:
one for node IDs and one for edge IDs. The sequence kind is encoded in the lower 4 bits of the
record tag byte (same pattern as catalog sub-types).

**Key Layout:**

```text
┌───────────┬─────────┬─────────────┐
│ subsystem │ version │ record_tag  │
│  1 byte   │ 1 byte  │   1 byte    │
└───────────┴─────────┴─────────────┘
```

Total: 3 bytes.

- `0xF0`: Node ID sequence (`SequenceKind::NodeId = 0` in lower bits)
- `0xF1`: Edge ID sequence (`SequenceKind::EdgeId = 1` in lower bits)

**Value Schema:**

```text
┌────────────────────────────────────────────────────────┐
│                     SeqBlockValue                      │
├────────────────────────────────────────────────────────┤
│  base_sequence:  u64 (LE), start of allocated block    │
│  block_size:     u64 (LE), number of IDs in block      │
└────────────────────────────────────────────────────────┘
```

Follows the same allocation pattern as the common crate's `SequenceAllocator`.

### Value Serialization

Property values (`NodeProperty` / `EdgeProperty` values) are serialized using Grafeo's native
[`Value::serialize()` / `Value::deserialize()`](https://github.com/GrafeoDB/grafeo/blob/main/crates/grafeo-common/src/value/serde.rs).
The format is a one-byte type tag followed by the payload; the tag values match the table in the
"Core Types" section above. This delegates the wire format entirely to `grafeo-common`, ensuring
that the storage layer is always compatible with the query engine's type system without maintaining
a separate encoding. The format is not yet stable across Grafeo major versions; the storage layer
pins a specific Grafeo version and any breaking format change would require a key version bump.

The `SortableValue` encoding used in `PropertyIndex` keys (see PropertyIndex section) is a
separate, sort-preserving encoding distinct from `Value::serialize()`. Only Bool, Int64, Float64,
and String values support sortable encoding; other types return `None` and are not indexed.

### Concurrency Model

Concurrency control is delegated to SlateDB's built-in transaction support rather than implementing
a custom MVCC layer. This simplifies the storage design and leverages SlateDB's tested isolation
guarantees.

**Transaction Usage:**

The adapter uses two patterns depending on the operation type:

1. **Write-only operations** (`create_node`, `create_edge`, `set_*_property`): Use `WriteBatch`
   via `Storage::apply()`. All records for a single logical operation (entity record, indexes,
   counter merges) are written atomically in one batch.

   Note: `set_*_property` is write-only when the property is new. When updating an existing
   indexed property, the adapter reads the old value to delete the stale `PropertyIndex` entry
   before writing the new one; this follows the read-then-write pattern below.

2. **Read-then-write operations** (`delete_node`, `delete_edge`, `add_label`, `remove_label`,
   `remove_*_property`): Use `StorageTransaction` for snapshot-isolated atomicity. The operation
   begins a transaction, reads the current state within that transaction's snapshot, buffers
   writes, and commits. This prevents [time-of-check to time-of-use](https://en.wikipedia.org/wiki/Time-of-check_to_time-of-use)
   (TOCTOU) races; for example, reading a node's labels then writing updated labels while another
   writer modifies the same node.

**Conflict Handling:**

SlateDB transactions use snapshot isolation. If two concurrent transactions write to the same key,
the second to commit receives a `TransactionConflict` error. The adapter retries conflicting
operations up to 3 times with a fresh transaction on each attempt.

**Why not custom MVCC?**

An earlier draft included epoch-based MVCC with version chains in entity record keys. Rejected
because SlateDB already provides snapshot isolation (making it redundant), the epoch
read-increment-write was not protected by a transaction (race condition), and version chains
turned every point lookup into a prefix scan with GC overhead. The key layout remains
forward-compatible with an epoch suffix if time-travel queries are needed later.

### Write Path

Creating a node with labels and properties involves the following SlateDB operations:

```text
create_node_with_props(["Person", "Employee"], [("name", "Alice"), ("age", 30)]):

1. Allocate node_id from SequenceAllocator
2. Resolve/create label IDs: "Person" → LabelId(0), "Employee" → LabelId(1)
3. Resolve/create property key IDs: "name" → PropKeyId(0), "age" → PropKeyId(1)

WriteBatch (via Storage::apply):
  PUT [0x05, 0x01, 0x10, node_id]              → NodeRecordValue { labels: [0, 1] }
  PUT [0x05, 0x01, 0x30, node_id, PropKeyId(0)] → Value::serialize(String("Alice"))
  PUT [0x05, 0x01, 0x30, node_id, PropKeyId(1)] → Value::serialize(Int64(30))
  PUT [0x05, 0x01, 0x80, PropKeyId(0), SortableValue("Alice"), node_id] → (empty)
  PUT [0x05, 0x01, 0x80, PropKeyId(1), SortableValue(30), node_id]     → (empty)
  PUT [0x05, 0x01, 0x70, LabelId(0), node_id]  → (empty)
  PUT [0x05, 0x01, 0x70, LabelId(1), node_id]  → (empty)
  MERGE [0x05, 0x01, 0xE0, 0x00]               → +1  (NodeCount)
```

Creating an edge:

```text
create_edge(src=NodeId(1), dst=NodeId(2), "KNOWS"):

1. Allocate edge_id from SequenceAllocator
2. Resolve/create edge type: "KNOWS" → EdgeTypeId(0)

WriteBatch (via Storage::apply):
  PUT [0x05, 0x01, 0x20, edge_id]                                → EdgeRecordValue { src: 1, dst: 2, type: 0 }
  PUT [0x05, 0x01, 0x50, NodeId(1), EdgeTypeId(0), NodeId(2), edge_id] → (empty)
  PUT [0x05, 0x01, 0x60, NodeId(2), EdgeTypeId(0), NodeId(1), edge_id] → (empty)
  MERGE [0x05, 0x01, 0xE0, 0x01]                                 → +1  (EdgeCount)
```

Deleting a node (transactional):

```text
delete_node(NodeId(42)):

StorageTransaction:
  BEGIN
  GET  [0x05, 0x01, 0x10, 42]  → NodeRecordValue { labels: [0, 1] }  (if absent → return false)
  DEL  [0x05, 0x01, 0x10, 42]
  SCAN [0x05, 0x01, 0x30, 42, ...]  → for each (prop_key_id, value):
    DEL  [0x05, 0x01, 0x30, 42, prop_key_id]
    if value is sortable:
      DEL  [0x05, 0x01, 0x80, prop_key_id, SortableValue(value), 42]
  DEL  [0x05, 0x01, 0x70, LabelId(0), 42]
  DEL  [0x05, 0x01, 0x70, LabelId(1), 42]
  MERGE [0x05, 0x01, 0xE0, 0x00]  → -1  (NodeCount)
  COMMIT
```

Edge cascade is handled separately: Grafeo's engine calls `delete_node_edges` before
`delete_node`, which scans ForwardAdj and BackwardAdj to find connected edges and deletes
each via `delete_edge` (removing the EdgeRecord, both adjacency entries, edge properties
and the EdgeCount merge). This keeps the storage adapter's `delete_node` focused on the node
itself while the engine controls cascade semantics. The cascade is not atomic with node
deletion; a crash between the two calls may leave the node intact with some edges already
deleted. This is acceptable under retry semantics.

### Read Path: Assembling a Full Node

Grafeo's `get_node(id)` returns `Node { id, labels, properties }`. Assembling this from SlateDB:

```text
get_node(NodeId(42)):

1. GET [0x05, 0x01, 0x10, 42_be]  → NodeRecordValue { label_ids: [0, 1] }
2. Resolve labels via catalog: LabelId(0) → "Person", LabelId(1) → "Employee"
3. Prefix scan [0x05, 0x01, 0x30, 42_be] → all properties for node 42
4. Decode each Value (via Value::deserialize), resolve property key names via catalog
5. Return Node { id: 42, labels: ["Person", "Employee"], properties: { "name": "Alice", "age": 30 } }
```

The mapping from trait methods to SlateDB operations is straightforward: each trait method maps
to the record type described above (e.g., `get_node` is a direct `get` on `NodeRecord`;
`edges_from` prefix scans `ForwardAdj`/`BackwardAdj`; `nodes_by_label` prefix scans
`LabelIndex`). `node_ids()` is the only expensive operation (full `NodeRecord` scan, O(N)); the
query engine avoids it when label or property predicates are available.

## Alternatives

### Bundled Property Map (Single Key per Entity)

An alternative stores all properties for an entity as a single serialized map:

```text
Key:   [0x05, 0x01, 0x30, node_id]
Value: Map { "name": "Alice", "age": 30, "email": "alice@example.com" }
```

**Rejected because** reading or writing a single property requires loading and deserializing
the entire map (read and write amplification), defeating projection pushdown in Grafeo's
selective batch methods.

### Adjacency Lists as Serialized Arrays

An alternative stores all edges from a node as a single serialized array:

```text
Key:   [0x05, 0x01, 0x50, src_node_id]
Value: [(edge_type_id, dst_node_id, edge_id), ...]
```

**Rejected because** each edge mutation requires read-modify-write on the entire list (expensive
for high-degree nodes), concurrent edge creations conflict on the same key, and type-filtered
traversal requires deserializing the full array instead of a prefix scan.

### Bitmap-Based Label Index

An alternative uses RoaringBitmaps per label (similar to the vector database's metadata index):

```text
Key:   [0x05, 0x01, 0x70, label_id]
Value: RoaringTreemap of node IDs
```

**Deferred because:**

1. **Read-modify-write**: Every node creation/deletion requires loading and mutating the bitmap.
   Merge operators mitigate this but add complexity.
2. **Individual keys scale better for writes**: Graph workloads with high write throughput benefit
   from append-only individual keys.
3. **Can be added later**: A bitmap-based index can coexist with the per-key index as a
   compaction-time optimization.

### Custom MVCC with Epoch-Based Versioning

An earlier draft stored an epoch in entity record keys and used `DELETED` flags for soft-delete:

```text
Key: [0x05, 0x01, 0x10, node_id, epoch]  (19 bytes)
Value: { flags: DELETED, label_ids: [] }
```

**Rejected because:**

1. **Redundant with SlateDB transactions**: SlateDB provides snapshot isolation natively via
   `DbTransaction`. Building a second MVCC layer on top adds complexity without new capability.
2. **Race condition**: The epoch counter (read-increment-write) was not protected by a transaction,
   allowing two concurrent writers to use the same epoch.
3. **Read amplification**: Every point lookup required a prefix scan over the version chain and
   filtering by epoch, instead of a single `get()`.
4. **Garbage collection burden**: Old versions accumulated in the LSM tree and required a
   background GC process (not yet designed) to reclaim space.

## Open Questions

1. **Property index scope**: Should property indexes also cover edge properties, or only node
   properties? Grafeo's trait only exposes `find_nodes_by_property`, but edge property indexes
   could benefit future query patterns.

2. **Statistics refresh strategy**: How often should statistics be recomputed? Options include:
   every N commits, on explicit request, or adaptive (when cardinality drift exceeds a threshold).

3. **Multi-graph support**: Grafeo supports named graphs (independent graph partitions within a
   database). Should the storage layout include a graph namespace prefix in keys, or should each
   named graph map to a separate SlateDB instance?

4. ~~**Write batching granularity**~~: **Resolved.** Each mutation method (e.g.,
   `create_node_with_props`, `create_edge`) issues a single atomic `WriteBatch` containing the
   entity record, all associated index entries (label, adjacency, property index), and counter
   merge operations. This ensures that a node and its indexes are always consistent.

5. ~~**MVCC model**~~: **Resolved.** Concurrency control is delegated to SlateDB's transaction
   support. No custom epoch-based MVCC. See the "Concurrency Model" section.

6. **Single-writer enforcement**: The storage design assumes single-writer semantics but does not
   enforce it at runtime. If a second writer starts against the same SlateDB instance (e.g. zombie
   process after k8s failover), the failure mode is silent: duplicate catalog IDs, overlapping
   node/edge IDs from the SequenceAllocator, and divergent counters. A fencing mechanism (e.g. CAS
   on a writer-epoch key or leveraging SlateDB's manifest fencing) is needed before production
   deployment. See "Write coordination" in Future Considerations.

## Future Considerations

Future RFCs will address:

- **HTTP API**: Read and write endpoints following the patterns established by timeseries and log.
- **GQL query endpoint**: Mapping GQL query text to Grafeo engine execution and streaming results.
- **Graph algorithms**: Integrating Grafeo's `algos` feature (PageRank, shortest path,
  community detection, etc.) as server-side procedures.
- **Change data capture**: Exposing graph mutations as a stream for downstream consumers.
- **Compaction policies**: Policies for cleaning up tombstoned adjacency/index entries.
- **Write coordination**: Integration with the cross-project write coordination RFC for
  multi-writer scenarios.
- **Time-travel queries**: Grafeo 0.5.24+ ships a temporal property versioning API
  (`get_node_property_at_epoch`, `execute_at_epoch`). The key layout is forward-compatible
  with an epoch suffix extension; this is deferred until production use cases justify the
  added key size and GC complexity.

## References

1. **Grafeo**, Graph database engine providing query parsing, optimization, and execution.
   [GitHub](https://github.com/GrafeoDB/grafeo)

2. **SlateDB**, Cloud-native LSM-tree storage engine built on object storage.
   [GitHub](https://github.com/slatedb/slatedb)

3. **GQL (ISO/IEC 39075:2024)**, The Graph Query Language standard.
   [ISO](https://www.iso.org/standard/76120.html)

4. **OpenData RFC 0001: Record Key Prefix**, Standard 3-byte key prefix format.
   [RFC](../../rfcs/0001-record-key-prefix.md)

5. **OpenData RFC 0004: Common Encodings**, Shared encoding primitives (TerminatedBytes, Utf8, etc.).
   [RFC](../../rfcs/0004-common-encodings.md)

## Updates

| Date       | Description                                                                       |
|------------|-----------------------------------------------------------------------------------|
| 2026-03-05 | Initial draft                                                                     |
| 2026-03-08 | Aligned with test implementation: catalog 6 sub-types, metadata keys,             |
|            | EdgeRecordValue field order, SeqBlock reserved bits, Grafeo v0.5.18               |
| 2026-03-14 | Major revision: removed custom MVCC epoch layer, delegated concurrency control    |
|            | to SlateDB transactions. Simplified NodeRecord/EdgeRecord keys (18 to 10 bytes),  |
|            | removed DELETED flags, removed CurrentEpoch metadata. Added StorageTransaction    |
|            | trait to common crate. Read-then-write ops use transactions for atomicity.        |
|            | Updated to Grafeo v0.5.22. Addressed reviewer feedback from cadonna and apurvam.  |
| 2026-03-29 | Adopted 3-byte key prefix (subsystem `0x05`, version, tag) per RFC 0001 update    |
|            | (PR #326). Updated all key layouts, byte sizes, and prefix examples.              |
|            | Updated to Grafeo v0.5.28. NodeRecordValue uses `FixedElementArray<u32>` per      |
|            | RFC 0004. Added Value format link, GCounter/OnCounter variants, PropertyIndex     |
|            | cleanup in delete_node, edge cascade semantics. Addressed reviewer feedback.      |

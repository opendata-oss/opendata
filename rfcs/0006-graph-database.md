# RFC 0006: Graph Database Storage

**Status**: Draft

**Authors**:

- [Steven Grond](https://github.com/StevenBtw)

## Summary

This RFC defines the storage model for a labeled property graph (LPG) database built on
[SlateDB](https://github.com/slatedb/slatedb). The graph database integrates
[Grafeo](https://github.com/GrafeoDB/grafeo) (v0.5.30) as its query engine, implementing Grafeo's
`GraphStore` and `GraphStoreMut` traits over SlateDB's ordered key-value interface.

The design maps graph primitives — nodes, edges, labels, properties, and adjacency, to SlateDB
records using the standard 3-byte key prefix. Two storage layouts are supported: **Individual**
(one KV row per property/adjacency entry) and **Merged** (packed values with merge-operator-based
updates). Concurrency control is delegated to SlateDB's built-in transaction support.

## Motivation

OpenData provides purpose-built databases for time series, logs, vectors, and key-value workloads,
all sharing SlateDB as a common storage engine. Graph workloads are a natural addition: knowledge
graphs, social networks, fraud detection, recommendation engines, and infrastructure dependency
mapping.

The storage design must support:

1. **Efficient traversal**: Direction-aware adjacency without loading unrelated data.
2. **Point lookups**: Single-key retrieval of nodes and edges by ID.
3. **Label and property indexing**: Filtered scans for `MATCH (n:Person)` and `WHERE n.age > 30`.
4. **Concurrent reads and writes**: Via SlateDB's snapshot isolation.
5. **Schema catalog persistence**: Label/type/property name-to-ID mappings that survive restarts.
6. **Compatibility with Grafeo's storage traits**: Unmodified query pipeline and executor.

## Goals

- Define the record key/value encoding scheme for all graph entity types
- Define two storage layout strategies (Individual and Merged) with trade-offs
- Define the catalog persistence format, adjacency index layout, and property index layout
- Define merge operators for counters, property packing, and adjacency batching
- Define the concurrency model delegated to SlateDB transactions

## Non-Goals

- **Query languages beyond GQL**, **RDF**, **graph algorithms**: Available via Grafeo feature flags.
- **GWP/Bolt protocol support**: Transport layers above the storage model.
- **Vector/text search on properties**: Would require new index record types.
- **Write coordination**: See Open Question #6.
- **Compaction policies**: Delegated to SlateDB.

## Dependencies

### Grafeo Crates

| Crate | Version | Role |
|---|---|---|
| `grafeo-core` | 0.5.30 | Storage traits (`GraphStore`, `GraphStoreMut`), core types |
| `grafeo-common` | 0.5.30 | Shared primitives (`NodeId`, `EdgeId`, `Value` enum) |
| `grafeo-engine` | 0.5.30 | GQL parser, cost-based optimizer, push-based executor |

### Other External Crates

| Crate | Version | Role |
|---|---|---|
| `parking_lot` | 0.12 | Faster RwLock/Mutex for catalog and sequence allocators |
| `hashbrown` | 0.16 | HashMap variant used by Grafeo types |
| `arcstr` | 1.2 | Atomic reference-counted strings for label/type/property names |
| `smallvec` | 1.15 | Stack-allocated vectors for node label lists |

## Design

### Architecture Overview

Each graph database instance corresponds to a single SlateDB instance. All graph data, entities,
properties, indexes, catalog, and metadata, are stored as key-value pairs in the LSM tree.
Grafeo's query engine operates on a `GraphStorage` adapter that translates trait method calls into
SlateDB reads and writes.

**Sync-async bridge:** Grafeo's traits are synchronous; SlateDB's API is async. The adapter bridges
via `tokio::task::block_in_place`.

```
┌──────────────────────────────────────────────────────────┐
│              OpenData Graph (per database)               │
│                                                          │
│  Grafeo Query Engine                                     │
│    GQL Parser → Binder → Optimizer → Executor            │
│    Operates on GraphStore / GraphStoreMut trait objects  │
│                          │                               │
│  GraphStorage Adapter                                    │
│    Implements GraphStore (reads) + GraphStoreMut (writes)│
│    In-memory: Catalog cache, Statistics                  │
│                          │                               │
│  Record Layout                                           │
│   NodeRecord(0x10)  EdgeRecord(0x20)  Properties(0x30/40)│
│   Adjacency(0x50/60) LabelIndex(0x70) PropIndex(0x80)    │
│   Catalog(0x90)  Metadata(0xE0)  SeqBlock(0xF0)          │
│                          │                               │
│  SlateDB (LSM KV Store)                                  │
└──────────────────────────────────────────────────────────┘
```

### Storage Layout Strategies

The `StorageLayout` configuration controls how properties and adjacency are stored:

#### Individual (default)

Each property and adjacency entry is a separate KV row. Simple, good for projection pushdown
and selective mutations. Drawback: high key count at scale (see keyspace analysis below).

#### Merged

Properties and adjacency are packed into single keys per entity, updated via SlateDB's merge
operator. Writes issue merge operands (no read-before-write); reads decode a single packed value.

**Trade-offs:**

| Aspect | Individual | Merged |
|---|---|---|
| Single property read | 1 get | 1 get + deserialize blob |
| All properties read | N gets (prefix scan) | 1 get |
| Property write | 1 put | 1 merge (no read) |
| Neighbor traversal (100 edges) | 100-key scan | 1 get per edge type |
| Neighbor traversal (1000 edges) | 1000-key scan | 1 get per edge type |
| Edge creation | 1 put per direction | 1 merge per direction |
| PropertyIndex maintenance | Read old value, delete old entry, put new | Blind write; stale entries filtered on read |

**Keyspace analysis** (100M nodes, 1B edges, avg 2 labels, 10 node props, 4 indexed, 2 edge props):

| Component | Individual | Merged |
|---|---|---|
| NodeRecord | 100M | 100M |
| LabelIndex | 200M | 200M |
| NodeProperty | 1.0B | 100M (packed) |
| PropertyIndex | 400M | 400M |
| EdgeRecord | 1.0B | 1.0B |
| Adjacency (fwd+bwd) | 2.0B | ~200M (per src+type) |
| EdgeProperty | 2.0B | 1.0B (packed) |
| **Total** | **~6.7B** | **~2.0B** |

### Standard Key Prefix

All records use the standard 3-byte prefix per [RFC 0001](../../rfcs/0001-record-key-prefix.md):
subsystem `0x05`, version `0x01`, record tag. Key fields are big-endian; value fields are
little-endian. Sortable encodings (sign-bit flip) per [RFC 0004](../../rfcs/0004-common-encodings.md).

### Record Type Reference

| Tag | Name | Key Suffix | Value | Notes |
|---|---|---|---|---|
| `0x10` | NodeRecord | `node_id:u64` | `label_ids: [u32 LE]*` | 11-byte key |
| `0x20` | EdgeRecord | `edge_id:u64` | `src:u64, dst:u64, type_id:u32, prop_count:u16` (all LE) | 11-byte key, 22-byte value |
| `0x30` | NodeProperty | Individual: `node_id:u64, prop_key_id:u32` / Merged: `node_id:u64` | Individual: `Value::serialize()` / Merged: packed props blob | |
| `0x40` | EdgeProperty | Individual: `edge_id:u64, prop_key_id:u32` / Merged: `edge_id:u64` | Same pattern as NodeProperty | |
| `0x50` | ForwardAdj | Individual: `src:u64, type_id:u32, dst:u64, edge_id:u64` / Merged: `src:u64, type_id:u32` | Individual: empty / Merged: packed adj blob | |
| `0x60` | BackwardAdj | Mirror of ForwardAdj (dst↔src) | | |
| `0x70` | LabelIndex | `label_id:u32, node_id:u64` | empty | 15-byte key |
| `0x80` | PropertyIndex | `prop_key_id:u32, sortable_value:var, node_id:u64` | empty | |
| `0x9x` | Catalog | By-ID: `id:u32` / By-Name: `name:UTF-8` | name or id | 6 sub-types (0x90–0x95) |
| `0xE0` | Metadata | `sub_type:u8` | `i64 LE` | NodeCount(0), EdgeCount(1) |
| `0xF0` | SeqBlock | (tag encodes kind) | `base:u64 LE, size:u64 LE` | Node(0xF0), Edge(0xF1) |

### Merge Operator

`GraphMergeOperator` dispatches by record tag:

| Record Type | Merge Behavior |
|---|---|
| NodeProperty / EdgeProperty (0x30/0x40) | Property merge: operands are `Set(prop_key_id, value)` or `Remove(prop_key_id)`, applied sequentially to a packed blob |
| ForwardAdj / BackwardAdj (0x50/0x60) | Adjacency merge: operands are `Add(peer_id, edge_id)` or `Remove(peer_id, edge_id)`, applied to a packed entry list |
| Metadata (0xE0) | Additive i64 counter merge |
| Everything else | Last-write-wins |

In **Individual** layout, properties and adjacency use `put()` (not merge), so the property/adjacency
branches never fire. The same merge operator works for both layouts.

**Operand formats:**

```
Property Set:    [0x01][prop_key_id:u32 LE][value_bytes...]
Property Remove: [0x02][prop_key_id:u32 LE]
Adj Add:         [0x01][peer_id:u64 LE][edge_id:u64 LE]
Adj Remove:      [0x02][peer_id:u64 LE][edge_id:u64 LE]
```

**PropertyIndex in Merged mode:** Writes use blind puts (no read-before-write) to avoid O(N)
merge-operand resolution. Stale index entries are filtered at read time in
`find_nodes_by_property` / `find_nodes_in_range` by verifying candidates against actual values.
Staleness is bounded by the number of *distinct indexed values* per (node, property) pair —
not by total write count. Setting `age` to 30 then 40 then 30 again produces at most 2 stale
entries, not 3. Compaction-time vacuuming (see Future Considerations) will reclaim these.

### Adjacency Design

Edge type is placed before destination in adjacency keys to enable type-filtered traversal.
In GQL, `MATCH (a)-[:KNOWS]->(b)` is far more common than `MATCH (a)-[]->(b)`.

Backward adjacency is always maintained (one extra write per edge) to enable first-class
`Incoming`/`Both` direction traversal.

### Catalog

Maps human-readable names to compact u32 IDs for labels, edge types, and property keys. Stored
bidirectionally (ID→name + name→ID). Loaded into an in-memory cache on startup by scanning the
by-ID prefixes. New entries use incrementing IDs (single-writer assumption).

### Concurrency Model

Two operation patterns:

1. **Write-only** (`create_node`, `create_edge`, `set_*_property`): `WriteBatch` via
   `Storage::apply()`. All records for one logical operation in a single atomic batch.

2. **Read-then-write** (`delete_node`, `delete_edge`, `add_label`, `remove_label`):
   `StorageTransaction` with snapshot isolation. Retries up to 3 times on conflict with jitter.

Node deletion is fully atomic: the node, all connected edges, their properties, label/property
index entries, and counter adjustments are committed or rolled back as a single unit. Self-loop
edges are deduplicated during cleanup to avoid double-processing.

### Write Path Examples

**Create node with properties (Individual):**
```
WriteBatch:
  PUT NodeRecord(node_id) → label_ids
  PUT NodeProperty(node_id, prop_key_id) → value   (per property)
  PUT PropertyIndex(prop_key_id, sortable, node_id) → empty   (if sortable)
  PUT LabelIndex(label_id, node_id) → empty   (per label)
  MERGE Metadata(NodeCount) → +1
```

**Create edge (Individual):**
```
WriteBatch:
  PUT EdgeRecord(edge_id) → src, dst, type_id
  PUT ForwardAdj(src, type_id, dst, edge_id) → empty
  PUT BackwardAdj(dst, type_id, src, edge_id) → empty
  MERGE Metadata(EdgeCount) → +1
```

**Set property (Merged):**
```
WriteBatch:
  MERGE MergedNodeProps(node_id) → Set(prop_key_id, value)
  PUT PropertyIndex(prop_key_id, sortable, node_id) → empty   (if sortable)
```

**Create edge (Merged):**
```
WriteBatch:
  PUT EdgeRecord(edge_id) → src, dst, type_id
  MERGE MergedForwardAdj(src, type_id) → Add(dst, edge_id)
  MERGE MergedBackwardAdj(dst, type_id) → Add(src, edge_id)
  MERGE Metadata(EdgeCount) → +1
```

### Value Serialization

Property values use Grafeo's `Value::serialize()` (bincode). The storage layer treats serialized
bytes as opaque. `SortableValue` encoding (PropertyIndex keys) is separate: only Bool, Int64,
Float64, and String support sortable encoding.

## Alternatives

### Bundled Property Map
All properties as a single serialized map. **Rejected**: read-modify-write on every property
mutation defeats projection pushdown. (Now available as the Merged layout using merge operator
to avoid the read-modify-write.)

### Adjacency Lists as Serialized Arrays
All edges from a node as a single array. **Rejected**: concurrent edge mutations conflict on the
same key. (Now available as Merged layout with merge operands for conflict-free appends.)

### Bitmap-Based Label Index
RoaringBitmaps per label. **Deferred**: individual keys scale better for writes; bitmaps can be
added as a compaction-time optimization later.

### Custom MVCC with Epoch-Based Versioning
**Rejected**: redundant with SlateDB transactions, race condition on epoch counter, every lookup
becomes a prefix scan over version chains.

## Open Questions

1. **Property index scope**: Should indexes also cover edge properties?
2. **Statistics refresh**: How often to recompute cardinality estimates?
3. **Multi-graph support**: Graph namespace prefix in keys vs. separate SlateDB instances?
4. ~~**Write batching**~~: **Resolved.** Single atomic WriteBatch per mutation.
5. ~~**MVCC model**~~: **Resolved.** Delegated to SlateDB transactions.
6. **Single-writer enforcement**: SlateDB provides manifest-based fencing but the adapter does not
   yet surface fencing errors. Must be resolved before failover-capable deployment.

## Future Considerations

- **HTTP API**: Read/write endpoints following timeseries and log patterns.
- **Graph algorithms**: Grafeo's `algos` feature as server-side procedures.
- **Compaction policies**: Cleaning tombstoned adjacency/index entries; vacuuming stale
  PropertyIndex entries in Merged mode.
- **Time-travel queries**: Key layout is forward-compatible with epoch suffix extension.
- **Statistics granularity**: Only aggregate NodeCount and EdgeCount are persisted as
  Metadata records. Per-label cardinality (`estimate_label_cardinality`) requires a full
  LabelIndex scan, and per-edge-type degree estimation (`estimate_avg_degree`) has no
  usable index at all — the current implementation falls back to `total_edges / total_nodes`
  regardless of edge type.

  These estimates feed Grafeo's cost-based optimizer. Inaccurate degree estimates cause the
  optimizer to misjudge fan-out during traversal planning. For example, in a graph with 1M
  `KNOWS` edges and 100 `OWNS` edges across 10K nodes, the global average degree is ~100.
  The optimizer would plan `MATCH (a)-[:OWNS]->(b)` as if it expands to 100 neighbors per
  node, when the real `OWNS` degree is ~0.01 — potentially choosing a scan-heavy plan over
  an index lookup. Grafeo's own in-memory store avoids this by maintaining sampled per-type
  statistics via `compute_statistics()`, but the KV storage adapter has no equivalent
  sampling infrastructure.

  Adding per-label and per-edge-type Metadata counters (maintained via merge operator on
  label/edge mutations) would eliminate these scans and provide the optimizer with accurate
  per-type estimates. The Metadata sub-type space (`0xE0` + `sub_type:u8`) has room for
  these additional counters.

## References

1. [Grafeo](https://github.com/GrafeoDB/grafeo) — Graph database engine
2. [SlateDB](https://github.com/slatedb/slatedb) — Cloud-native LSM storage engine
3. [GQL (ISO/IEC 39075:2024)](https://www.iso.org/standard/76120.html) — Graph Query Language
4. [RFC 0001: Record Key Prefix](../../rfcs/0001-record-key-prefix.md)
5. [RFC 0004: Common Encodings](../../rfcs/0004-common-encodings.md)

## Appendix A: Layout Performance Comparison

Criterion benchmarks comparing Individual vs Merged layouts on both InMemory and SlateDB
(in-memory object store) backends. All numbers are wall-clock median latency. Nodes have
10 properties (5 indexed); edges have no properties. Run on Windows 11, Rust 1.86 release
profile, `cargo bench --bench layout_comparison`.

| Operation | Individual InMem | Individual SlateDB | Merged InMem | Merged SlateDB |
|---|---|---|---|---|
| Create node + 10 props | 17.0 µs | 229 µs | 18.9 µs | 215 µs |
| Get node (10 props) | 3.7 µs | 20.9 µs | **2.7 µs** | **13.4 µs** |
| Update single property | **0.9 µs** | 25.5 µs | 1.6 µs | 22.6 µs |
| Create 100 edges | 232 µs | 5.26 ms | 300 µs | 5.66 ms |
| Neighbors (100 outgoing) | 10.9 µs | 96.8 µs | **1.2 µs (9×)** | **81.0 µs** |
| Neighbors (1000 outgoing) | 107 µs | 982 µs | **2.6 µs (41×)** | **853 µs** |
| Bulk 100 nodes + 300 edges | 1.07 ms | 23.0 ms | 1.23 ms | 21.6 ms |

**Observations:**

- **Traversal is the dominant win for Merged:** Neighbor lookups at 1000 edges are 41×
  faster in-memory because one packed value replaces a 1000-key prefix scan. On SlateDB
  the gap narrows (~15%) since the scan is I/O-bound, but Merged still wins.
- **Property reads favor Merged:** at 10 properties per node (1 get + decode vs 11 gets).
  The advantage grows with property count.
- **Single-property writes favor Individual:** a direct `put()` avoids merge-operand
  encoding (0.9 µs vs 1.6 µs in-memory). On SlateDB the difference is within noise.
- **Edge creation is comparable.** Merged adds merge-operand overhead (~30% in-memory)
  but on SlateDB both layouts are I/O-bound at ~5.5 ms for 100 edges.
- **Bulk insert is layout-insensitive on SlateDB:** both complete 100 nodes + 300 edges
  in ~22 ms, dominated by WAL/memtable costs.

**Recommendation:** Use Individual for small graphs or write-heavy workloads with few
properties. Use Merged for traversal-heavy workloads, high-property-count entities,
or graphs exceeding ~10M nodes where keyspace reduction (3.3× fewer keys) matters
for compaction and memory pressure.

## Updates

| Date | Description |
|---|---|
| 2026-03-05 | Initial draft |
| 2026-03-08 | Aligned with implementation: catalog 6 sub-types, metadata keys, Grafeo v0.5.18 |
| 2026-03-14 | Removed custom MVCC, delegated to SlateDB transactions. Simplified key layouts. |
| 2026-03-29 | Adopted 3-byte key prefix per RFC 0001. Updated to Grafeo v0.5.30. |
| 2026-03-30 | Bumped Grafeo to v0.5.30. Clarified Value variant indices are bincode-assigned. |
| 2026-04-03 | Added Merged storage layout with merge-operator-based property packing and adjacency batching. Added keyspace analysis. Renamed SlateGraphStore to GraphStorage. Condensed RFC. |
| 2026-04-03 | Added Appendix A with layout benchmark results. Added statistics granularity to Future Considerations. Clarified PropertyIndex staleness bounds. |

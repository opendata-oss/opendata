# RFC 0006: Graph Database Storage

**Status**: Draft

**Authors**:

- [Steven Grond](https://github.com/StevenBtw)

## Summary

This RFC defines the storage model for a labeled property graph (LPG) database built on
[SlateDB](https://github.com/slatedb/slatedb). The graph database integrates
[Grafeo](https://github.com/GrafeoDB/grafeo) (v0.5.40) as its query engine, implementing Grafeo's
`GraphStore` and `GraphStoreMut` traits over SlateDB's ordered key-value interface.

The design maps graph primitives (nodes, edges, labels, properties, and adjacency) to SlateDB
records using the standard 3-byte key prefix. All properties of an entity are packed into one
KV value per entity, and all adjacency entries for a `(source_node, edge_type)` pair are packed
into one KV value per pair, updated via SlateDB's merge operator. Concurrency control is
delegated to SlateDB's built-in transaction support.

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
- Define the packed value format for properties and adjacency, with the merge-operand wire
  format used to update them
- Define the catalog persistence format, adjacency index layout, and property index layout
- Define merge operators for counters, property packing, and adjacency batching
- Define the concurrency model delegated to SlateDB transactions

## Non-Goals

- **Query languages beyond GQL**, **RDF**, **graph algorithms**: Available via Grafeo feature flags.
- **GWP/Bolt protocol support**: Transport layers above the storage model.
- **Vector/text search on properties**: Would require new index record types.
- **Write coordination and single-writer fencing**: See Future Considerations.
- **Compaction policies**: Delegated to SlateDB.

## Dependencies

### Grafeo Crates

| Crate | Version | Role |
|---|---|---|
| `grafeo-core` | 0.5.40 | Storage traits (`GraphStore`, `GraphStoreMut`), core types |
| `grafeo-common` | 0.5.40 | Shared primitives (`NodeId`, `EdgeId`, `Value` enum) |
| `grafeo-engine` | 0.5.40 | GQL parser, cost-based optimizer, push-based executor |

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

### Storage Layout

All node and edge properties for a given entity are packed into a single KV value per
entity, updated via SlateDB's merge operator. All adjacency entries for a given
`(source_node, edge_type)` pair are likewise packed into a single KV value. Writes issue
merge operands without reading the prior value; reads decode one packed value per entity
(properties) or per `(node, edge_type)` pair (adjacency).

The packed layout is the only storage layout. An earlier draft of this RFC considered a
parallel "Individual" layout with one KV row per property and adjacency entry. It was
dropped before merging: the rest of OpenData uses the merge operator extensively and
carrying two layouts through the codebase, tests, and future API design added surface area
without clear payoff.

**Keyspace at 100M nodes, 1B edges, avg 2 labels, 10 node props, 4 indexed, 2 edge props:**

| Component | Keys |
|---|---|
| NodeRecord | 100M |
| LabelIndex | 200M |
| NodeProps (packed) | 100M |
| PropertyIndex | 400M |
| EdgeRecord | 1.0B |
| Adjacency (fwd+bwd, packed per src+type) | ~200M |
| EdgeProps (packed) | 1.0B |
| **Total** | **~2.0B** |

For comparison, a naive one-KV-row-per-property-and-edge layout at the same scale would
produce approximately 6.7B KV rows (~3.3× more), dominated by per-property and per-edge
adjacency records.

### Standard Key Prefix

All records use the standard 3-byte prefix per [RFC 0001](../../rfcs/0001-record-key-prefix.md):
subsystem `0x05` (to be registered in RFC 0001), version `0x01`, record tag. Key fields are
big-endian; value fields are little-endian.

**SortableValue encoding** is used for PropertyIndex keys to enable range scans. Only four
`Value` variants support sortable key encoding:

| Type | Encoding |
|---|---|
| Bool | `0x00` (false) / `0x01` (true) |
| Int64 | 8 bytes big-endian with sign bit flipped (`value ^ 0x8000000000000000`) |
| Float64 | 8 bytes big-endian; if negative flip all bits, else flip sign bit (`IEEE 754 total order`) |
| String | `TerminatedBytes` encoding per [RFC 0004](../../rfcs/0004-common-encodings.md) |

All other `Value` variants (List, Map, Bytes, etc.) are not sortable and are excluded from
PropertyIndex. This encoding preserves lexicographic ordering in SlateDB's sorted key space.

### Record Type Reference

| Tag | Name | Key Suffix | Value | Notes |
|---|---|---|---|---|
| `0x10` | NodeRecord | `node_id:u64` | `label_ids: [u32 LE]*` | 11-byte key |
| `0x20` | EdgeRecord | `edge_id:u64` | `src:u64, dst:u64, type_id:u32` (all LE) | 11-byte key, 20-byte value |
| `0x30` | NodeProps | `node_id:u64` | Packed properties blob (see Value Encoding) | 11-byte key |
| `0x40` | EdgeProps | `edge_id:u64` | Packed properties blob | 11-byte key |
| `0x50` | ForwardAdj | `src:u64, type_id:u32` | Packed adjacency blob | 15-byte key |
| `0x60` | BackwardAdj | `dst:u64, type_id:u32` | Packed adjacency blob (mirror of ForwardAdj) | 15-byte key |
| `0x70` | LabelIndex | `label_id:u32, node_id:u64` | empty | 15-byte key |
| `0x80` | PropertyIndex | `prop_key_id:u32, sortable_value:var, node_id:u64` | empty | |
| `0x9x` | Catalog | By-ID: `id:u32` / By-Name: `name:UTF-8` | name or id | 6 sub-types (0x90 to 0x95) |
| `0xE0` | Metadata | `sub_type:u8` or `sub_type:u8, id:u32 BE` | `i64 LE` | 4-byte key (aggregates) or 8-byte key (per-type). Aggregates: NodeCount(0x00), EdgeCount(0x01). Per-type: LabelNodeCount(0x10, label_id), EdgeTypeCount(0x11, type_id). High nibble of `sub_type` distinguishes the layout: `0x0?` = aggregate (no id), `0x1?` = per-type (u32 id suffix). Future counter kinds claim new high-nibble ranges. |
| `0xF0` | SeqBlock | (tag encodes kind) | `base:u64 LE, size:u64 LE` | Node(0xF0), Edge(0xF1) |

### Value Encoding

Two value formats carry the bulk of the data: the **packed properties blob** (stored at
NodeProps/EdgeProps keys) and the **packed adjacency blob** (stored at ForwardAdj/BackwardAdj
keys). Both are built from a `u32 LE` count followed by a sequence of fixed-header entries.
Both are opaque to SlateDB; updates go through the merge operator.

#### Packed properties blob

Stored at `NodeProps(node_id)` or `EdgeProps(edge_id)`. Holds every property of one entity.

```
[count:u32 LE]
repeat count times:
  [prop_key_id:u32 LE][value_len:u32 LE][value_bytes...]
```

- `prop_key_id` is the catalog-assigned integer ID of the property key.
- `value_bytes` is the Grafeo `Value` serialized via `bincode` using
  [`Value::serialize` / `Value::deserialize`](https://github.com/GrafeoDB/grafeo/blob/main/crates/grafeo-common/src/types/value.rs)
  in `grafeo-common`. The format is bincode-stable within a Grafeo minor version; any
  backwards-incompatible change rides a prefix-version bump per
  [RFC 0001](../../rfcs/0001-record-key-prefix.md). Length-prefixed so the blob can be
  walked without knowing individual value sizes.
- Entries are stored in insertion order, not sorted. Property counts per entity are small in
  practice (LPG schemas typically have tens of properties, not thousands), so linear scan on
  get/set/remove is cheap relative to the single SlateDB get. Sorting would add work on the
  hot write path without meaningfully speeding up the read path.

**Why not bincode-encode the whole map?** bincode serialization of a `HashMap<u32, Value>`
produces a dense blob but requires the full map in memory at encode and decode time and
cannot be updated incrementally by the merge operator. The hand-rolled layout lets the merge
operator apply individual operands without materializing the full map in the common case.

#### Packed adjacency blob

Stored at `ForwardAdj(src, type_id)` or `BackwardAdj(dst, type_id)`. Holds every adjacency
entry for one `(node, edge_type)` pair.

```
[count:u32 LE]
repeat count times:
  [peer_id:u64 LE][edge_id:u64 LE]
```

- Fixed 16-byte entries, which makes the layout effectively `FixedElementArray<(u64, u64)>`
  per [RFC 0004](../../rfcs/0004-common-encodings.md).
- Unsorted; insertion order preserved. `add` deduplicates on exact `(peer_id, edge_id)`
  match. `remove` does a linear scan with retain.

**Why not sort by `peer_id`?** The adjacency blob is append-heavy (edge creation is the
dominant write pattern) and read patterns are iterate-all, not point lookup by peer. A sorted
insert would require an O(N) shift on every edge creation; the current unsorted append is
O(1) amortized.

**Why not delta-encode the peer IDs?** A production-ready delta+bit-packing codec exists in
Grafeo (`grafeo_core::codec::DeltaBitPacked`). It is not used in v1 because:

1. Delta encoding requires sorted input. Sorting has the insertion-cost problem above.
2. Decoding cost is non-trivial for small blobs, and most adjacency blobs in typical LPG
   workloads are small (node-local fan-out averages 10 to 100 in social-network-style graphs).
3. For very high-degree nodes (~100K+ edges of one type from one source) the codec becomes
   attractive, but these are the same nodes that need chunking (see the overflow discussion
   below). Rather than ship delta encoding now and design chunking later, the format stays
   simple until a real workload exposes the cutover.

#### High-degree overflow (known limitation)

A node with N outgoing edges of type T produces a blob of `4 + 16·N` bytes. At 1M edges
this is 16 MB in a single KV value, and every merge operand must resolve by reading and
rewriting the full blob. Merge-operand batching amortizes this across operands within a
batch, but the per-batch cost still grows linearly with blob size.

This RFC ships without chunking. The planned upgrade path, should it become necessary:

1. **Chunked adjacency**: split the blob into fixed-size chunks (e.g. 1024 entries per chunk)
   with a chunk index suffix in the key. Per-merge cost drops from O(total degree) to
   O(chunk size), and high-cardinality nodes no longer produce multi-MB KV values. Grafeo's
   in-memory store uses a 64-edge chunk strategy with hot/cold tiering that can serve as the
   reference implementation.
2. **Delta+bit-pack** inside each cold chunk once the chunk is closed.
3. **Zone-map sidecar** over chunks for skip-scan during traversal.

All three are additive: they can be introduced without changing the NodeProps/EdgeProps
formats or the merge-operand wire format. The key layout for adjacency (`[tag][src][type_id]`)
is also forward-compatible: a chunk suffix can be appended without affecting existing scan
prefixes.

#### Merge operand wire format

All operands are variable-length with a 1-byte tag.

| Operand | Bytes |
|---|---|
| Property Set | `[0x01][prop_key_id:u32 LE][value_bytes...]` |
| Property Remove | `[0x02][prop_key_id:u32 LE]` |
| Adj Add | `[0x01][peer_id:u64 LE][edge_id:u64 LE]` |
| Adj Remove | `[0x02][peer_id:u64 LE][edge_id:u64 LE]` |

Operand tags are scoped per record type, not global: `0x01` means "Set" for properties
and "Add" for adjacency. `GraphMergeOperator` dispatches on the record tag in the key
(`NodeProps`/`EdgeProps` vs `ForwardAdj`/`BackwardAdj`) before decoding the operand, so
the shared tag values are unambiguous at merge time.

Operands are applied sequentially to the existing packed blob. If the blob does not yet
exist, the merge starts from an empty packed value.

### Merge Operator

`GraphMergeOperator` dispatches by record tag:

| Record Type | Merge Behavior |
|---|---|
| NodeProps / EdgeProps (0x30/0x40) | Apply property operands sequentially to the packed properties blob |
| ForwardAdj / BackwardAdj (0x50/0x60) | Apply adjacency operands sequentially to the packed adjacency blob |
| Metadata (0xE0) | Additive `i64 LE` counter merge |
| Everything else | Last-write-wins |

The additive counter merge is used for both aggregate (`NodeCount`, `EdgeCount`) and per-type
(`LabelNodeCount`, `EdgeTypeCount`) metadata entries. Per-type counters give Grafeo's
cost-based optimizer accurate per-label cardinality and per-edge-type average degree
without needing a full LabelIndex or adjacency scan. Without them, `MATCH (a)-[:OWNS]->(b)`
in a graph with 1M `KNOWS` and 100 `OWNS` edges would plan as if `OWNS` fanned out at the
global average (~100) rather than the real ~0.01, likely choosing a scan over an index
lookup. The cost is one extra merge operand per label add/remove and per edge
create/delete — negligible compared to the packed-blob merges already on the write path.

**Missing-counter fallback:** When `estimate_avg_degree` is called for an edge type that
is not yet in the catalog, or whose `EdgeTypeCount` counter has not been materialized
(no edges of that type have been created in the current database), the adapter falls back
to the global average (`edge_count / node_count`). `estimate_label_cardinality` returns
`0` for unknown labels. The fallback avoids the pathological early-read case where a
cold counter would otherwise report `0` and mislead the optimizer more than the global
average does.

**PropertyIndex staleness:** `set_node_property` writes the new PropertyIndex entry without
reading the prior packed blob to delete the stale one. Reading the blob would trigger
O(N) merge-operand resolution in SlateDB, which is pathologically slow under repeated
updates. Instead, stale index entries are filtered at read time in `find_nodes_by_property`
and `find_nodes_in_range` by verifying candidates against the current property value.
Staleness is bounded by the number of *distinct indexed values* per (node, property) pair
minus one (the current value), not by total write count. Setting `age` to 30 then 40 then
30 again produces at most 1 stale entry, not 2: two distinct values yield two live index
entries, one of which always matches the current value.

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

**Create node with labels:**
```
WriteBatch:
  PUT NodeRecord(node_id) → label_ids
  PUT LabelIndex(label_id, node_id) → empty   (per label)
  MERGE Metadata(NodeCount) → +1
  MERGE Metadata(LabelNodeCount, label_id) → +1   (per label)
```

**Set property:**
```
WriteBatch:
  MERGE NodeProps(node_id) → Set(prop_key_id, value)
  PUT PropertyIndex(prop_key_id, sortable, node_id) → empty   (if sortable)
```

**Create edge:**
```
WriteBatch:
  PUT EdgeRecord(edge_id) → src, dst, type_id
  MERGE ForwardAdj(src, type_id) → Add(dst, edge_id)
  MERGE BackwardAdj(dst, type_id) → Add(src, edge_id)
  MERGE Metadata(EdgeCount) → +1
  MERGE Metadata(EdgeTypeCount, type_id) → +1
```

Deletes and `add_label`/`remove_label` issue matching `-1` merges on the corresponding
counters to keep per-type cardinality in sync.

### Value Serialization

Property values use Grafeo's `Value::serialize()` (bincode). The storage layer treats serialized
bytes as opaque; the packed blob formats above just carry the bytes. `SortableValue` encoding
for PropertyIndex keys is defined in [Standard Key Prefix](#standard-key-prefix) above.

## Alternatives

### One KV row per property and adjacency entry ("Individual")

An earlier draft supported this as a parallel layout. **Rejected for v1**: carrying two
layouts through the codebase, tests, and future API design added surface area without
clear payoff. See "Storage Layout" for keyspace cost at scale.

### Bundled property map without merge operator

All properties as a single bincode-serialized `HashMap`. **Rejected**: every mutation would
be a read-modify-write on the full blob, which contends heavily under concurrent writes.
The merge-operator-based packed format used here is morally the same layout but offloads
the merge to SlateDB and avoids the read.

### Bitmap-based label index

RoaringBitmaps per label. **Deferred**: individual keys scale fine for writes and SlateDB's
scan optimizations ([slatedb/slatedb#1380](https://github.com/slatedb/slatedb/pull/1380))
further improve the read path. Bitmaps can be added as a compaction-time optimization later.

### Custom MVCC with epoch-based versioning

**Rejected**: redundant with SlateDB transactions, race condition on epoch counter, every
lookup becomes a prefix scan over version chains.

## Open Questions

None at this time.

## Future Considerations

- **HTTP API**: Read/write endpoints following the timeseries and log patterns.
- **Graph algorithms**: Grafeo's `algos` feature as server-side procedures.
- **Edge property indexing**: Node properties are indexed via `PropertyIndex` (`0x80`);
  edge properties are not. No MVP query pattern needs it. The key layout is
  forward-compatible: a new `EdgePropertyIndex` record tag (e.g. `0x81`) can mirror the
  node-side layout when demand shows up, without touching existing records.
- **Multi-graph support**: Each graph is a separate SlateDB instance, matching how every
  other OpenData subsystem is scoped. Stamping a graph-id prefix across the ~2B keys in
  the scale analysis would compound keyspace cost for the single-graph case. A
  namespace-prefix variant can be added later under a version bump without breaking
  single-graph deployments.
- **High-degree adjacency chunking**: See "High-degree overflow" in Value Encoding for the
  planned chunk + delta-bit-pack + zone-map upgrade path.
- **PropertyIndex vacuuming**: Blind-write updates leave stale entries that are filtered at
  read time. A compaction-time cleanup pass (or a periodic background scrub) will reclaim
  them at scale. Bound is tight (distinct indexed values, not total writes), so this is
  not urgent.
- **Single-writer fencing**: SlateDB provides manifest-based fencing; the adapter does not
  yet surface `ManifestFencedError`. To be specified alongside the write API / deployment
  topology RFC before failover-capable deployment.
- **Sync/async bridge refinement**: The adapter currently bridges Grafeo's sync traits to
  SlateDB's async API via `tokio::task::block_in_place`. If concurrency benchmarks show
  thread-pool contention, moving to `spawn_blocking` on a dedicated pool is the next step.
- **Time-travel queries**: Key layout is forward-compatible with an epoch suffix extension.

## References

1. [Grafeo](https://github.com/GrafeoDB/grafeo): Graph database engine
2. [SlateDB](https://github.com/slatedb/slatedb): Cloud-native LSM storage engine
3. [GQL (ISO/IEC 39075:2024)](https://www.iso.org/standard/76120.html): Graph Query Language
4. [RFC 0001: Record Key Prefix](../../rfcs/0001-record-key-prefix.md)
5. [RFC 0004: Common Encodings](../../rfcs/0004-common-encodings.md)

## Updates

| Date | Description |
|---|---|
| 2026-03-05 | Initial draft |
| 2026-03-08 | Aligned with implementation: catalog 6 sub-types, metadata keys, Grafeo v0.5.18 |
| 2026-03-14 | Removed custom MVCC, delegated to SlateDB transactions. Simplified key layouts. |
| 2026-03-29 | Adopted 3-byte key prefix per RFC 0001. Updated to Grafeo v0.5.30. |
| 2026-03-30 | Bumped Grafeo to v0.5.30. Clarified Value variant indices are bincode-assigned. |
| 2026-04-03 | Introduced merge-operator-based packed storage. Added keyspace analysis. Renamed SlateGraphStore to GraphStorage. Condensed RFC. |
| 2026-04-03 | Defined SortableValue encoding inline. Noted subsystem 0x05 registration in RFC 0001. Removed unused backward_edges config flag. |
| 2026-04-17 | Committed to the packed layout as the sole storage layout. Dropped Individual. Specified byte-level packed props, packed adjacency, and merge operand formats with rationale. Documented high-degree overflow and planned chunking upgrade path. Moved deferred items (staleness counter, per-type stats, fencing, sync/async bridge) from Open Questions into Future Considerations. Bumped Grafeo to v0.5.40. |
| 2026-04-24 | Added per-label / per-edge-type counters (`LabelNodeCount`, `EdgeTypeCount`) with missing-counter fallback. Documented `sub_type` high-nibble convention and record-type-scoped operand tags. Dropped unused `prop_count:u16` from EdgeRecord. Corrected PropertyIndex staleness bound to `distinct_indexed_values - 1`. Linked `Value::serialize` / `Value::deserialize` in `grafeo-common`. Closed Open Questions: edge property indexing deferred; multi-graph = one SlateDB instance per graph. Grafeo v0.5.40. |

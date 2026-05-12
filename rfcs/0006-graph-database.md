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
│   Catalog(0x90-0x95) Metadata(0xE0) SeqBlock(0xF0/F1)    │
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

All records use the standard 3-byte prefix per [RFC 0001](./0001-record-key-prefix.md):
subsystem `0x05` (to be registered in RFC 0001), version `0x01`, record tag.

```
key prefix byte layout:
┌───────────┬─────────┬────────────┐
│ subsystem │ version │ record_tag │
│  1 byte   │ 1 byte  │   1 byte   │
│  (0x05)   │ (0x01)  │            │
└───────────┴─────────┴────────────┘
```

The record tag's high nibble names the record family (`NodeRecord` `0x1?`, `EdgeRecord`
`0x2?`, and so on). The low nibble distinguishes sub-types within a family for `Catalog`
(`0x90`–`0x95`), `Metadata` (`0xE0`), and `SeqBlock` (`0xF0`/`0xF1`); for all other
families the low nibble is `0x0` and reserved for future sub-types. Key fields after the
prefix are **big-endian**; value fields are **little-endian** unless otherwise noted.

### Common Encodings

**Value encodings** (little-endian):

- `FixedElementArray<T>`: Serialized elements back-to-back with no count prefix. The
  number of elements is derived from `value_len / sizeof(T)`. Defined in
  [RFC 0004](./0004-common-encodings.md).
- `LengthPrefixed<T>`: A `u32 LE` count of `T` elements followed by `count` serialized
  elements packed back-to-back.

**Key encodings** (big-endian for lexicographic ordering):

- `TerminatedBytes`: Variable-length bytes with escape sequences and a `0x00` terminator
  for lexicographic ordering. Defined in [RFC 0004](./0004-common-encodings.md).
- `SortableValue`: Used for `PropertyIndex` keys to enable range scans. Only four
  `Value` variants support sortable key encoding:

  | Type    | Width    | Encoding                                                                         |
  |---------|----------|----------------------------------------------------------------------------------|
  | Bool    | 1 byte   | `0x00` (false) / `0x01` (true)                                                   |
  | Int64   | 8 bytes  | Big-endian with sign bit flipped (`value ^ 0x8000000000000000`)                  |
  | Float64 | 8 bytes  | Big-endian; if negative flip all bits, else flip sign bit (IEEE 754 total order) |
  | String  | variable | `TerminatedBytes` per RFC 0004                                                   |

  All other `Value` variants (`List`, `Map`, `Bytes`, ...) are not sortable and are
  excluded from `PropertyIndex`. The encoding preserves lexicographic ordering in
  SlateDB's sorted key space.

### Common Value Formats

Two value formats carry the bulk of the data: the **packed properties blob** (stored at
`NodeProps` and `EdgeProps` keys) and the **packed adjacency blob** (stored at
`ForwardAdj` and `BackwardAdj` keys). Both are built from a `u32 LE` count followed by a
sequence of fixed-header entries. Both are opaque to SlateDB; updates go through the
merge operator.

#### Packed properties blob

Stored at `NodeProps(node_id)` or `EdgeProps(edge_id)`. Holds every property of one
entity.

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
  [RFC 0001](./0001-record-key-prefix.md). Length-prefixed so the blob can be walked
  without knowing individual value sizes.
- Entries are stored in insertion order, not sorted. Property counts per entity are small
  in practice (LPG schemas typically have tens of properties, not thousands), so linear
  scan on get/set/remove is cheap relative to the single SlateDB get. Sorting would add
  work on the hot write path without meaningfully speeding up the read path.

**Why not bincode-encode the whole map?** bincode serialization of a `HashMap<u32, Value>`
produces a dense blob but requires the full map in memory at encode and decode time and
cannot be updated incrementally by the merge operator. The hand-rolled layout lets the
merge operator apply individual operands without materializing the full map in the common
case.

#### Packed adjacency blob

Stored at `ForwardAdj(src, type_id)` or `BackwardAdj(dst, type_id)`. Holds every
adjacency entry for one `(node, edge_type)` pair.

```
[count:u32 LE]
repeat count times:
  [peer_id:u64 LE][edge_id:u64 LE]
```

- Fixed 16-byte entries, which makes the layout effectively
  `FixedElementArray<(u64, u64)>` per [RFC 0004](./0004-common-encodings.md) after the
  count prefix.
- Unsorted; insertion order preserved. `add` deduplicates on exact `(peer_id, edge_id)`
  match. `remove` does a linear scan with retain.

**Why not sort by `peer_id`?** The adjacency blob is append-heavy (edge creation is the
dominant write pattern) and read patterns are iterate-all, not point lookup by peer. A
sorted insert would require an O(N) shift on every edge creation; the current unsorted
append is O(1) amortized.

**Why not delta-encode the peer IDs?** A production-ready delta+bit-packing codec exists
in Grafeo (`grafeo_core::codec::DeltaBitPacked`). It is not used in v1 because:

1. Delta encoding requires sorted input. Sorting has the insertion-cost problem above.
2. Decoding cost is non-trivial for small blobs, and most adjacency blobs in typical LPG
   workloads are small (node-local fan-out averages 10 to 100 in social-network-style
   graphs).
3. For very high-degree nodes (~100K+ edges of one type from one source) the codec
   becomes attractive, but these are the same nodes that need chunking (see the overflow
   discussion below). Rather than ship delta encoding now and design chunking later, the
   format stays simple until a real workload exposes the cutover.

#### High-degree overflow (known limitation)

A node with N outgoing edges of type T produces a blob of `4 + 16·N` bytes. At 1M edges
this is 16 MB in a single KV value, and every merge operand must resolve by reading and
rewriting the full blob. Merge-operand batching amortizes this across operands within a
batch, but the per-batch cost still grows linearly with blob size.

This RFC ships without chunking. The planned upgrade path, should it become necessary:

1. **Chunked adjacency**: split the blob into fixed-size chunks (e.g. 1024 entries per
   chunk) with a chunk index suffix in the key. Per-merge cost drops from O(total degree)
   to O(chunk size), and high-cardinality nodes no longer produce multi-MB KV values.
   Grafeo's in-memory store uses a 64-edge chunk strategy with hot/cold tiering that can
   serve as the reference implementation.
2. **Delta+bit-pack** inside each cold chunk once the chunk is closed.
3. **Zone-map sidecar** over chunks for skip-scan during traversal.

All three are additive: they can be introduced without changing the `NodeProps`/`EdgeProps`
formats or the merge-operand wire format. The key layout for adjacency
(`[tag][src][type_id]`) is also forward-compatible: a chunk suffix can be appended without
affecting existing scan prefixes.

#### Merge operand wire format

All operands are variable-length with a 1-byte tag.

| Operand           | Bytes                                            |
|-------------------|--------------------------------------------------|
| Property Set      | `[0x01][prop_key_id:u32 LE][value_bytes...]`     |
| Property Remove   | `[0x02][prop_key_id:u32 LE]`                     |
| Adj Add           | `[0x01][peer_id:u64 LE][edge_id:u64 LE]`         |
| Adj Remove        | `[0x02][peer_id:u64 LE][edge_id:u64 LE]`         |

Operand tags are scoped per record type, not global: `0x01` means "Set" for properties
and "Add" for adjacency. `GraphMergeOperator` dispatches on the record tag in the key
(`NodeProps`/`EdgeProps` vs `ForwardAdj`/`BackwardAdj`) before decoding the operand, so
the shared tag values are unambiguous at merge time.

Operands are applied sequentially to the existing packed blob. If the blob does not yet
exist, the merge starts from an empty packed value.

### Record Type Reference

| Tag     | Name             | Description                                                       |
|---------|------------------|-------------------------------------------------------------------|
| `0x10`  | `NodeRecord`     | Per-node label assignments                                        |
| `0x20`  | `EdgeRecord`     | Edge endpoints and type                                           |
| `0x30`  | `NodeProps`      | Packed properties blob for a node                                 |
| `0x40`  | `EdgeProps`      | Packed properties blob for an edge                                |
| `0x50`  | `ForwardAdj`     | Packed adjacency blob: `(src, type_id)` → outgoing edges          |
| `0x60`  | `BackwardAdj`    | Packed adjacency blob: `(dst, type_id)` → incoming edges          |
| `0x70`  | `LabelIndex`     | Inverted index: `label_id` → nodes                                |
| `0x80`  | `PropertyIndex`  | Inverted index: `(prop_key_id, sortable_value)` → nodes           |
| `0x90`  | `LabelByName`    | Catalog: label name → `label_id`                                  |
| `0x91`  | `LabelById`      | Catalog: `label_id` → label name                                  |
| `0x92`  | `EdgeTypeByName` | Catalog: edge type name → `type_id`                               |
| `0x93`  | `EdgeTypeById`   | Catalog: `type_id` → edge type name                               |
| `0x94`  | `PropKeyByName`  | Catalog: property key name → `prop_key_id`                        |
| `0x95`  | `PropKeyById`    | Catalog: `prop_key_id` → property key name                        |
| `0xE0`  | `Metadata`       | Persistent counters (aggregate and per-type)                      |
| `0xF0`  | `NodeSeqBlock`   | Block-based ID allocator state for `node_id`                      |
| `0xF1`  | `EdgeSeqBlock`   | Block-based ID allocator state for `edge_id`                      |

### Merge Operator

`GraphMergeOperator` dispatches by record tag:

| Record Type                                  | Merge Behavior                                                          |
|----------------------------------------------|-------------------------------------------------------------------------|
| `NodeProps` / `EdgeProps` (`0x30`/`0x40`)    | Apply property operands sequentially to the packed properties blob     |
| `ForwardAdj` / `BackwardAdj` (`0x50`/`0x60`) | Apply adjacency operands sequentially to the packed adjacency blob     |
| `Metadata` (`0xE0`)                          | Additive `i64 LE` counter merge                                         |
| Everything else                              | Last-write-wins                                                         |

The additive counter merge is used for both aggregate (`NodeCount`, `EdgeCount`) and
per-type (`LabelNodeCount`, `EdgeTypeCount`) metadata entries. Per-type counters give
Grafeo's cost-based optimizer accurate per-label cardinality and per-edge-type average
degree without needing a full `LabelIndex` or adjacency scan. Without them,
`MATCH (a)-[:OWNS]->(b)` in a graph with 1M `KNOWS` and 100 `OWNS` edges would plan as if
`OWNS` fanned out at the global average (~100) rather than the real ~0.01, likely choosing
a scan over an index lookup. The cost is one extra merge operand per label add/remove and
per edge create/delete — negligible compared to the packed-blob merges already on the
write path.

**Missing-counter fallback:** When `estimate_avg_degree` is called for an edge type that
is not yet in the catalog, or whose `EdgeTypeCount` counter has not been materialized
(no edges of that type have been created in the current database), the adapter falls back
to the global average (`edge_count / node_count`). `estimate_label_cardinality` returns
`0` for unknown labels. The fallback avoids the pathological early-read case where a cold
counter would otherwise report `0` and mislead the optimizer more than the global average
does.

**PropertyIndex staleness:** `set_node_property` writes the new `PropertyIndex` entry
without reading the prior packed blob to delete the stale one. Reading the blob would
trigger O(N) merge-operand resolution in SlateDB, which is pathologically slow under
repeated updates. Instead, stale index entries are filtered at read time in
`find_nodes_by_property` and `find_nodes_in_range` by verifying candidates against the
current property value. Staleness is bounded by the number of *distinct indexed values*
per (node, property) pair minus one (the current value), not by total write count.
Setting `age` to 30 then 40 then 30 again produces at most 1 stale entry, not 2: two
distinct values yield two live index entries, one of which always matches the current
value.

### Adjacency Design

Edge type is placed before destination in adjacency keys to enable type-filtered
traversal. In GQL, `MATCH (a)-[:KNOWS]->(b)` is far more common than `MATCH (a)-[]->(b)`.

Backward adjacency is always maintained (one extra write per edge) to enable first-class
`Incoming`/`Both` direction traversal.

### Catalog Semantics

Maps human-readable names to compact `u32` IDs for labels, edge types, and property keys.
Stored bidirectionally (ID→name + name→ID) across six record sub-types; see
[`Catalog` (`0x90` – `0x95`)](#catalog-0x90--0x95) under Record Definitions & Schemas for
the on-disk layout of each sub-type.

The catalog is loaded into an in-memory cache on startup by range-scanning the by-ID
prefixes (`LabelById`, `EdgeTypeById`, `PropKeyById`). New entries use incrementing IDs
allocated under the single-writer assumption; both directions are written in a single
`WriteBatch` so the cache and on-disk state stay in lockstep.

### Concurrency Model

Two operation patterns:

1. **Write-only** (`create_node`, `create_edge`, `set_*_property`): `WriteBatch` via
   `Storage::apply()`. All records for one logical operation in a single atomic batch.
2. **Read-then-write** (`delete_node`, `delete_edge`, `add_label`, `remove_label`):
   `StorageTransaction` with snapshot isolation. Retries up to 3 times on conflict with
   jitter.

Node deletion is fully atomic: the node, all connected edges, their properties,
label/property index entries, and counter adjustments are committed or rolled back as a
single unit. Self-loop edges are deduplicated during cleanup to avoid double-processing.

### Write Path Examples

**Create node with labels:**
```
WriteBatch:
  PUT NodeRecord(node_id) → label_ids
  PUT LabelIndex(label_id, node_id) → empty     (per label)
  MERGE Metadata(NodeCount) → +1
  MERGE Metadata(LabelNodeCount, label_id) → +1 (per label)
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

Property values use Grafeo's `Value::serialize()` (bincode). The storage layer treats
serialized bytes as opaque; the packed blob formats above just carry the bytes.
`SortableValue` encoding for `PropertyIndex` keys is defined in
[Common Encodings](#common-encodings) above.

## Record Definitions & Schemas

### `NodeRecord` (`0x10`)

Stores per-node label assignments. The set of labels for a node — typically two or three —
is the only state that ever needs to be read without also reading the node's properties
(label-only matches in GQL `MATCH (n:Person) RETURN n`), so it is stored separately from
`NodeProps`.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────┐
│ subsystem │ version │ record_tag │ node_id  │
│  1 byte   │ 1 byte  │   1 byte   │ 8 bytes  │
│  (0x05)   │ (0x01)  │   (0x10)   │   (BE)   │
└───────────┴─────────┴────────────┴──────────┘
```

- `node_id` (u64 BE): The node's identifier, allocated from `NodeSeqBlock`.

**Value Schema:**

```
┌──────────────────────────────────────────────────────┐
│                  NodeRecordValue                     │
├──────────────────────────────────────────────────────┤
│  label_ids:   FixedElementArray<u32 LE>              │
└──────────────────────────────────────────────────────┘
```

- `label_ids`: The catalog IDs of every label assigned to this node, packed as a
  `FixedElementArray` per [RFC 0004](./0004-common-encodings.md). No count prefix; the
  array length is `value_len / 4`. Labels are stored in insertion order.

**Write Semantics:**

- `create_node` writes the record with a `PUT`.
- `add_label` / `remove_label` rewrite the entire `label_ids` array via `PUT` under a
  `StorageTransaction`.

### `EdgeRecord` (`0x20`)

Stores the endpoints and type of an edge. Edge IDs are globally unique and allocated from
`EdgeSeqBlock`.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────┐
│ subsystem │ version │ record_tag │ edge_id  │
│  1 byte   │ 1 byte  │   1 byte   │ 8 bytes  │
│  (0x05)   │ (0x01)  │   (0x20)   │   (BE)   │
└───────────┴─────────┴────────────┴──────────┘
```

- `edge_id` (u64 BE): The edge's identifier.

**Value Schema:**

```
┌──────────────────────────────────────────────────────┐
│                  EdgeRecordValue                     │
├──────────────────────────────────────────────────────┤
│  src:      u64 LE                                    │
│  dst:      u64 LE                                    │
│  type_id:  u32 LE                                    │
└──────────────────────────────────────────────────────┘
```

- `src` (u64 LE): Source node ID.
- `dst` (u64 LE): Destination node ID.
- `type_id` (u32 LE): Catalog ID of this edge's type.

**Write Semantics:**

Always written together with `ForwardAdj` and `BackwardAdj` merges in a single
`WriteBatch`; see [Write Path Examples](#write-path-examples).

### `NodeProps` (`0x30`)

Holds every property of one node, packed into a single value updated via merge operands.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────┐
│ subsystem │ version │ record_tag │ node_id  │
│  1 byte   │ 1 byte  │   1 byte   │ 8 bytes  │
│  (0x05)   │ (0x01)  │   (0x30)   │   (BE)   │
└───────────┴─────────┴────────────┴──────────┘
```

- `node_id` (u64 BE): The node's identifier.

**Value Schema:**

The packed properties blob; see
[Packed properties blob](#packed-properties-blob) under Common Value Formats.

**Write Semantics:**

`set_node_property` issues a `Property Set` merge operand. `remove_node_property` issues
`Property Remove`. The merge operator applies operands sequentially to the existing blob
(or starts from empty if the key is absent).

### `EdgeProps` (`0x40`)

Same layout as `NodeProps` but keyed by `edge_id`.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────┐
│ subsystem │ version │ record_tag │ edge_id  │
│  1 byte   │ 1 byte  │   1 byte   │ 8 bytes  │
│  (0x05)   │ (0x01)  │   (0x40)   │   (BE)   │
└───────────┴─────────┴────────────┴──────────┘
```

- `edge_id` (u64 BE): The edge's identifier.

**Value Schema:**

The packed properties blob; see [Packed properties blob](#packed-properties-blob).

**Write Semantics:**

`set_edge_property` / `remove_edge_property` issue `Property Set` / `Property Remove`
merge operands.

### `ForwardAdj` (`0x50`)

Maps `(src_node, type_id)` to every edge originating at `src_node` with type `type_id`.
Edge-type-before-destination ordering enables type-filtered traversal:
`MATCH (a)-[:KNOWS]->(b)` scans only the `KNOWS` adjacency for `a` rather than all of
`a`'s outgoing edges.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────┬──────────┐
│ subsystem │ version │ record_tag │   src    │ type_id  │
│  1 byte   │ 1 byte  │   1 byte   │ 8 bytes  │ 4 bytes  │
│  (0x05)   │ (0x01)  │   (0x50)   │   (BE)   │   (BE)   │
└───────────┴─────────┴────────────┴──────────┴──────────┘
```

- `src` (u64 BE): Source node ID.
- `type_id` (u32 BE): Catalog ID of the edge type.

**Value Schema:**

The packed adjacency blob; see [Packed adjacency blob](#packed-adjacency-blob) under
Common Value Formats. Each entry's `peer_id` is the destination node of the edge.

**Write Semantics:**

`create_edge` issues an `Adj Add` merge operand. `delete_edge` issues `Adj Remove`. Every
write is mirrored to `BackwardAdj`.

### `BackwardAdj` (`0x60`)

Mirror of `ForwardAdj`, keyed by `(dst, type_id)` and tracking incoming edges. Always
maintained alongside `ForwardAdj` to enable first-class `Incoming` and `Both` traversal
directions.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────┬──────────┐
│ subsystem │ version │ record_tag │   dst    │ type_id  │
│  1 byte   │ 1 byte  │   1 byte   │ 8 bytes  │ 4 bytes  │
│  (0x05)   │ (0x01)  │   (0x60)   │   (BE)   │   (BE)   │
└───────────┴─────────┴────────────┴──────────┴──────────┘
```

- `dst` (u64 BE): Destination node ID.
- `type_id` (u32 BE): Catalog ID of the edge type.

**Value Schema:**

The packed adjacency blob, identical in shape to `ForwardAdj`. Each entry's `peer_id` is
the *source* of the edge (the node on the other side of `dst`).

**Write Semantics:**

Same merge-operand discipline as `ForwardAdj`; every edge create/delete writes one operand
to each side.

### `LabelIndex` (`0x70`)

Inverted index from label to the set of nodes carrying that label. Scanning the
`[label_id]` prefix yields all matching node IDs without reading `NodeRecord` values.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬───────────┬──────────┐
│ subsystem │ version │ record_tag │ label_id  │ node_id  │
│  1 byte   │ 1 byte  │   1 byte   │  4 bytes  │ 8 bytes  │
│  (0x05)   │ (0x01)  │   (0x70)   │   (BE)    │   (BE)   │
└───────────┴─────────┴────────────┴───────────┴──────────┘
```

- `label_id` (u32 BE): Catalog ID of the label.
- `node_id` (u64 BE): Node carrying the label.

**Value Schema:**

Empty. The key carries all information.

**Write Semantics:**

`PUT` with an empty value on label add; tombstone on label remove. Written in the same
`WriteBatch` as the `NodeRecord` update so the index and primary record stay consistent.

### `PropertyIndex` (`0x80`)

Inverted index for node property values, supporting equality and range scans. Only the
four `Value` variants with sortable key encodings (`Bool`, `Int64`, `Float64`, `String`)
are indexed; the property's runtime type determines which encoding applies.

**Key Layout:**

```
┌───────────┬─────────┬────────────┬──────────────┬─────────────────┬──────────┐
│ subsystem │ version │ record_tag │ prop_key_id  │ sortable_value  │ node_id  │
│  1 byte   │ 1 byte  │   1 byte   │   4 bytes    │   variable      │ 8 bytes  │
│  (0x05)   │ (0x01)  │   (0x80)   │    (BE)      │  (see below)    │   (BE)   │
└───────────┴─────────┴────────────┴──────────────┴─────────────────┴──────────┘
```

- `prop_key_id` (u32 BE): Catalog ID of the property key.
- `sortable_value`: The property value encoded via `SortableValue` (see
  [Common Encodings](#common-encodings)). For `Bool`/`Int64`/`Float64` the width is fixed
  (1 / 8 / 8 bytes); for `String` the encoding is `TerminatedBytes` (self-delimiting via
  a `0x00` terminator per RFC 0004). The parser determines the width from the property's
  declared sortable type (looked up by `prop_key_id` in the catalog) or from the
  `TerminatedBytes` terminator for strings.
- `node_id` (u64 BE): The node whose property value matches.

**Value Schema:**

Empty.

**Write Semantics:**

`set_node_property` blind-writes the new index entry without reading the prior packed-props
blob; stale entries are filtered at read time. See the
[PropertyIndex staleness](#merge-operator) note under Merge Operator for the staleness
bound.

### `Catalog` (`0x90` – `0x95`)

Maps human-readable names to compact `u32` IDs for labels, edge types, and property keys.
Six sub-tags cover three entity types × two lookup directions:

| Sub-Tag | Name              | Key             | Value             |
|---------|-------------------|-----------------|-------------------|
| `0x90`  | `LabelByName`     | label name      | `label_id`        |
| `0x91`  | `LabelById`       | `label_id`      | label name        |
| `0x92`  | `EdgeTypeByName`  | edge type name  | `type_id`         |
| `0x93`  | `EdgeTypeById`    | `type_id`       | edge type name    |
| `0x94`  | `PropKeyByName`   | property name   | `prop_key_id`     |
| `0x95`  | `PropKeyById`     | `prop_key_id`   | property name     |

**Key Layout (by-name variants `0x90` / `0x92` / `0x94`):**

```
┌───────────┬─────────┬────────────┬────────────────┐
│ subsystem │ version │ record_tag │      name      │
│  1 byte   │ 1 byte  │   1 byte   │  UTF-8 bytes   │
│  (0x05)   │ (0x01)  │            │   (variable)   │
└───────────┴─────────┴────────────┴────────────────┘
```

- `name`: UTF-8 bytes of the entity name. Extends to the end of the key; no terminator is
  needed because no further fields follow.

**Key Layout (by-id variants `0x91` / `0x93` / `0x95`):**

```
┌───────────┬─────────┬────────────┬───────────┐
│ subsystem │ version │ record_tag │    id     │
│  1 byte   │ 1 byte  │   1 byte   │  4 bytes  │
│  (0x05)   │ (0x01)  │            │   (BE)    │
└───────────┴─────────┴────────────┴───────────┘
```

- `id` (u32 BE): The catalog-assigned identifier (`label_id`, `type_id`, or
  `prop_key_id`).

**Value Schemas:**

- By-name (`0x90` / `0x92` / `0x94`): `u32 LE` — the assigned catalog ID.
- By-id (`0x91` / `0x93` / `0x95`): UTF-8 bytes — the entity name (no length prefix;
  value extends to end of buffer).

**Write Semantics:**

Both directions for a given catalog entry are written in a single `WriteBatch` when a new
entry is allocated, so by-name and by-id rows never disagree. New IDs are assigned by
incrementing an in-memory counter under the single-writer assumption; on startup the
in-memory cache is rehydrated by range-scanning each by-id prefix.

### `Metadata` (`0xE0`)

Persistent counters used by Grafeo's cost-based optimizer for cardinality estimation. The
key's `sub_type` byte distinguishes aggregate counters (one per database) from per-type
counters (one per label or edge type). The high nibble of `sub_type` determines the key
layout: `0x0?` = aggregate (4-byte key), `0x1?` = per-type (8-byte key). Future counter
kinds claim new high-nibble ranges.

**Key Layout (aggregate, high nibble `0x0?`):**

```
┌───────────┬─────────┬────────────┬───────────┐
│ subsystem │ version │ record_tag │ sub_type  │
│  1 byte   │ 1 byte  │   1 byte   │  1 byte   │
│  (0x05)   │ (0x01)  │   (0xE0)   │           │
└───────────┴─────────┴────────────┴───────────┘
```

- `sub_type` (u8): `0x00` `NodeCount` | `0x01` `EdgeCount`.

**Key Layout (per-type, high nibble `0x1?`):**

```
┌───────────┬─────────┬────────────┬───────────┬───────────┐
│ subsystem │ version │ record_tag │ sub_type  │    id     │
│  1 byte   │ 1 byte  │   1 byte   │  1 byte   │  4 bytes  │
│  (0x05)   │ (0x01)  │   (0xE0)   │           │   (BE)    │
└───────────┴─────────┴────────────┴───────────┴───────────┘
```

- `sub_type` (u8): `0x10` `LabelNodeCount` (id = `label_id`) | `0x11` `EdgeTypeCount`
  (id = `type_id`).
- `id` (u32 BE): Catalog ID corresponding to the `sub_type`.

**Value Schema:**

```
┌──────────────────────────────────────────────────────┐
│                   MetadataValue                      │
├──────────────────────────────────────────────────────┤
│  count:  i64 LE                                      │
└──────────────────────────────────────────────────────┘
```

**Write Semantics:**

Merged additively (`+1` / `-1` operands) by `GraphMergeOperator` (see
[Merge Operator](#merge-operator)). See the
[Missing-counter fallback](#merge-operator) note for the cold-counter handling rules.

### `SeqBlock` (`0xF0`, `0xF1`)

Block-based ID allocators per [RFC 0002](./0002-seq-block.md). Two separate allocators
keep node and edge ID assignment from contending.

**Key Layout:**

```
┌───────────┬─────────┬────────────┐
│ subsystem │ version │ record_tag │
│  1 byte   │ 1 byte  │   1 byte   │
│  (0x05)   │ (0x01)  │            │
└───────────┴─────────┴────────────┘
```

The key contains only the 3-byte prefix; the record tag itself names the allocator:

| Tag    | Name           | Allocates  |
|--------|----------------|------------|
| `0xF0` | `NodeSeqBlock` | `node_id`  |
| `0xF1` | `EdgeSeqBlock` | `edge_id`  |

There is exactly one record per allocator.

**Value Schema:**

```
┌──────────────────────────────────────────────────────┐
│                   SeqBlockValue                      │
├──────────────────────────────────────────────────────┤
│  base:  u64 LE                                       │
│  size:  u64 LE                                       │
└──────────────────────────────────────────────────────┘
```

- `base` (u64 LE): First ID in the currently allocated block.
- `size` (u64 LE): Number of IDs in the block; the allocated range is
  `[base, base + size)`.

**Write Semantics:**

Allocation and crash-recovery semantics follow [RFC 0002](./0002-seq-block.md). On startup
the writer reads the current record, allocates a new block starting at `base + size`, and
writes the updated record before assigning any IDs.

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
- **High-degree adjacency chunking**: See
  [High-degree overflow](#high-degree-overflow-known-limitation) for the planned
  chunk + delta-bit-pack + zone-map upgrade path.
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
4. [RFC 0001: Record Key Prefix](./0001-record-key-prefix.md)
5. [RFC 0002: Block-Based Sequence Allocation](./0002-seq-block.md)
6. [RFC 0004: Common Encodings](./0004-common-encodings.md)

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
| 2026-05-11 | Restructured to match the vector / timeseries RFC format: each record type now has its own subsection with key-layout and value-schema box diagrams. Enumerated the six `Catalog` sub-tags (`LabelByName` `0x90` through `PropKeyById` `0x95`). Split `SeqBlock` into named `NodeSeqBlock` (`0xF0`) and `EdgeSeqBlock` (`0xF1`) entries. Named `NodeRecord`'s value encoding as `FixedElementArray<u32>` per RFC 0004. |

# Graph Database

A labeled property graph database built on SlateDB, using Grafeo for GQL query execution.

## Architecture

### Key Concepts

- **SlateGraphStore**: Implements Grafeo's `GraphStore`/`GraphStoreMut` traits over OpenData's `Storage` abstraction
- **Sync-Async Bridge**: Grafeo traits are synchronous; `block_in_place` + `block_on` bridges to async SlateDB
- **Catalog**: Bidirectional name/ID dictionaries for labels, edge types, and property keys
- **Merge Operator**: Additive merge for metadata counters (node/edge counts); last-write-wins for everything else

### Record Types (in `src/serde/`)

| Type | Tag | Key Format | Description |
|------|-----|-----------|-------------|
| NodeRecord | `0x10` | `[ver][tag][node_id:u64][epoch:u64]` | MVCC node entity |
| EdgeRecord | `0x20` | `[ver][tag][edge_id:u64][epoch:u64]` | MVCC edge entity |
| NodeProperty | `0x30` | `[ver][tag][node_id:u64][prop_key:terminated]` | Node property |
| EdgeProperty | `0x40` | `[ver][tag][edge_id:u64][prop_key:terminated]` | Edge property |
| ForwardAdj | `0x50` | `[ver][tag][src:u64][type_id:u32][dst:u64]` | Outgoing edge index |
| BackwardAdj | `0x60` | `[ver][tag][dst:u64][type_id:u32][src:u64]` | Incoming edge index |
| LabelIndex | `0x70` | `[ver][tag][label_id:u32][node_id:u64]` | Label membership |
| PropertyIndex | `0x80` | `[ver][tag][prop_id:u32][sortable_value][node_id:u64]` | Value index |
| Catalog | `0x90` | `[ver][tag+kind][id:u32]` or `[ver][tag+kind][name:terminated]` | Name/ID maps |
| Metadata | `0xE0` | `[ver][tag][sub_type:u8]` | Counters (merge operator) |
| Sequence | `0xF0` | `[ver][tag+kind]` | ID allocation blocks |

### Key Modules

- `src/db.rs` - `GraphDb` high-level API (wraps Grafeo's `GrafeoDB`)
- `src/storage/mod.rs` - `SlateGraphStore` struct, async loading, sync-async bridge
- `src/storage/reader.rs` - `GraphStore` trait impl (reads, traversals, index lookups)
- `src/storage/writer.rs` - `GraphStoreMut` trait impl (create/delete nodes/edges, properties, labels)
- `src/storage/catalog.rs` - In-memory catalog with persistent backing
- `src/storage/merge_operator.rs` - Counter merge for metadata records
- `src/serde/keys.rs` - Key encoding/decoding for all record types
- `src/serde/values.rs` - Value encoding (node/edge records, property values, sortable index values)
- `src/server/http.rs` - Axum HTTP server with `/query`, `/healthy`, `/ready`

### Query Path

1. HTTP POST to `/query` with GQL string body
2. `GraphDb::execute()` delegates to `GrafeoDB::execute()`
3. Grafeo parser/planner/optimizer produces execution plan
4. Executor calls `GraphStore` trait methods on `SlateGraphStore`
5. Each trait call uses `block_in_place` to run async `Storage::get/scan/apply`
6. Results returned as `QueryResult { columns, rows }`

### Write Path

1. Grafeo executor calls `GraphStoreMut` methods (e.g., `create_node`)
2. Writer builds a `Vec<RecordOp>` batch (puts + merges)
3. Catalog entries created on first use (label/edge-type/property names)
4. Sequence allocator provides monotonic IDs (block-based, crash-safe)
5. Batch applied atomically via `Storage::apply()`

## Cross-References

- Serde patterns match `common/src/serde/` (key_prefix, terminated_bytes, sortable)
- Uses `common::SequenceAllocator` for node/edge ID generation
- Uses `common::Storage` trait for SlateDB access
- Grafeo crates: `grafeo-engine` (query), `grafeo-core` (traits/types), `grafeo-common` (value types)

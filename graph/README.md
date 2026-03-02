# Graph

A labeled property graph database with GQL queries, built on SlateDB. Uses [Grafeo](https://github.com/grafeo-db/grafeo) for query parsing, planning, and execution.

## Features

- **GQL queries**: ISO/IEC 39075 graph query language via Grafeo
- **Labeled Property Graph**: Nodes with labels, typed edges, and key-value properties
- **SlateDB storage**: Durable, object-store-native persistence with LSM-tree compaction
- **Property indexes**: Sortable value indexes for range queries
- **HTTP API**: JSON query endpoint with health and readiness checks

## Design

Graph data is stored as flat key-value records in SlateDB, using a custom encoding scheme that preserves sort order for efficient range scans. Grafeo provides the query engine; OpenData provides the storage backend via its `GraphStore`/`GraphStoreMut` trait implementation.

### Record Types (in `src/serde/`)

| Type | Tag | Description |
|------|-----|-------------|
| NodeRecord | `0x10` | Node entity with MVCC epoch versioning |
| EdgeRecord | `0x20` | Edge entity with src/dst/type and versioning |
| NodeProperty | `0x30` | Node property (key -> value) |
| EdgeProperty | `0x40` | Edge property (key -> value) |
| ForwardAdj | `0x50` | Forward adjacency index (src -> dst) |
| BackwardAdj | `0x60` | Backward adjacency index (dst -> src) |
| LabelIndex | `0x70` | Label-to-node index |
| PropertyIndex | `0x80` | Sortable property value index for range queries |
| Catalog | `0x90` | Bidirectional name/ID dictionaries (labels, edge types, property keys) |
| Metadata | `0xE0` | Counters (node count, edge count, epoch) via merge operator |
| Sequence | `0xF0` | Block-based ID allocation |

### Query Path

1. Parse GQL query via Grafeo
2. Grafeo's planner/optimizer calls `GraphStore` trait methods
3. `SlateGraphStore` translates to SlateDB `get()`/`scan()` calls
4. Results flow back through Grafeo's executor

## Usage

```rust
use graph::{GraphDb, Config};

let db = GraphDb::open_with_config(&Config::default()).await?;

// Create nodes and edges via GQL
db.execute("CREATE (:Person {name: 'Alice', age: 30})").await?;
db.execute("CREATE (:Person {name: 'Bob', age: 25})").await?;
db.execute("
    MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
    CREATE (a)-[:KNOWS {since: 2024}]->(b)
").await?;

// Query
let result = db.execute("
    MATCH (p:Person)-[:KNOWS]->(friend)
    RETURN p.name, friend.name
").await?;
```

## Roadmap

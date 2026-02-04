# Log

A key-oriented log database built on an LSM tree (SlateDB).

## Design

The log database leans into its LSM representation: log streams are indexed by arbitrary byte keys within a shared global keyspace. The diagram below shows a simplified representation of the key structure within the LSM.

```text
sorted by (key, seq) ─────────────►

Segment 1    ┌───────┬───────┬───────┬───────┬───────┐
(seq 5–9)    │ A:6   │ B:5   │ B:7   │ C:8   │ C:9   │
             └───────┴───────┴───────┴───────┴───────┘

Segment 0    ┌───────┬───────┬───────┬───────┬───────┐
(seq 0–4)    │ A:0   │ A:3   │ B:1   │ C:2   │ C:4   │
             └───────┴───────┴───────┴───────┴───────┘

Each cell: key:sequence
```

Each log entry contains a key, sequence, and value. Entries are stored as `(segment_id, key, sequence)`, where the sequence is a global counter that increases monotonically per key but is not contiguous.

> [!IMPORTANT]
> Compared to the Kafka data model, our "key" is closer to a topic partition. The key identifies the log stream; the value is the payload. We have no separate notion of keys which distinguish entries within the same log stream.

Segments partition the sequence space and scope compaction—entries within a segment are sorted by key, then sequence. The `seal_interval` configuration controls segment boundaries: smaller intervals reduce write amplification at the cost of read locality. Conceptually, each segment represents a window of time across all keys. In the future, we may support size-based segmentation as well.

What that buys us:
- **Many independent logs, zero provisioning** — each `key` is its own log stream; creating new streams is just writing new keys (no pre-allocation or reconfiguration)
- **Automatic locality via compaction** — the LSM continuously rewrites data so entries for the same key become clustered on disk over time
- **Simple mental model** — scaling isn’t “partition management”; it’s just “write and read by key”

Trade-offs:
- **Write amplification** — LSM compaction rewrites data, which increases total bytes written compared to pure append-only layouts. This behavior can be controlled with windowed compaction strategies based on segment configuration.

## Usage

### Writing

```rust
use log::{LogDb, Config, Record};
use bytes::Bytes;

let log = LogDb::open(Config::default()).await?;

log.append(vec![
    Record { key: Bytes::from("orders"), value: Bytes::from(b"...") },
    Record { key: Bytes::from("events"), value: Bytes::from(b"...") },
]).await?;
```

Each record is assigned a sequence number from a global counter. Sequences increase monotonically within each key but are not contiguous.

### Reading

```rust
use log::{LogRead, LogDbReader};

let reader = LogDbReader::open(Config::default()).await?;

// Scan all entries for a key
let mut iter = reader.scan(Bytes::from("orders"), ..).await?;
while let Some(entry) = iter.next().await? {
    println!("seq={} value={:?}", entry.sequence, entry.value);
}

// Scan from a checkpoint
let mut iter = reader.scan(Bytes::from("orders"), checkpoint..).await?;
```

## Roadmap

- [ ] **Range-based queries** — Scan across multiple keys with prefix or range predicates
- [ ] **Reader groups** — Coordinated consumption with offset tracking and rebalancing
- [ ] **Tail following** — Efficiently wait for new entries without polling
- [ ] **Count API** — Fast cardinality estimates using SST metadata
- [ ] **Retention policies** — Segment-based deletion by time or size
- [ ] **Checkpoints** — Point-in-time snapshots for consistent reads (via SlateDB)
- [ ] **Clones** — Lightweight forks of the log for isolation or branching (via SlateDB)

# Log

A key-oriented log database built on an LSM tree (SlateDB).

## Design

The log database leans into its LSM representation: each stream is an arbitrary `key`, and entries are stored in a single global keyspace as `(key, sequence)`.

What that buys us:
- **Many independent logs, zero provisioning** — each `key` is its own log stream; creating new streams is just writing new keys (no pre-allocation or reconfiguration)
- **Automatic locality via compaction** — the LSM continuously rewrites data so entries for the same key become clustered on disk over time
- **Simple mental model** — scaling isn’t “partition management”; it’s just “write and read by key”

Trade-offs:
- **Write amplification** — LSM compaction rewrites data, which increases total bytes written compared to pure append-only layouts.

## Usage

### Writing

```rust
use log::{Log, Config, Record};
use bytes::Bytes;

let log = Log::open(Config::default()).await?;

log.append(vec![
    Record { key: Bytes::from("orders"), value: Bytes::from(b"...") },
    Record { key: Bytes::from("events"), value: Bytes::from(b"...") },
]).await?;
```

Each record is assigned a sequence number from a global counter. Sequences increase monotonically within each key but are not contiguous.

### Reading

```rust
use log::{LogRead, LogReader};

let reader = LogReader::open(Config::default()).await?;

// Scan all entries for a key
let mut iter = reader.scan(Bytes::from("orders"), ..).await?;
while let Some(entry) = iter.next().await? {
    println!("seq={} value={:?}", entry.sequence, entry.value);
}

// Scan from a checkpoint
let mut iter = reader.scan(Bytes::from("orders"), checkpoint..).await?;
```

## Concepts

**Key** — Identifies an independent log stream. Variable-length bytes.

**Sequence** — A global 64-bit counter assigned at write time. Within each key, sequences are guaranteed to increase monotonically, but they are not contiguous—gaps appear when writes to other keys are interleaved.

**Segment** — Logical partitions of the sequence space, created periodically. Used internally for retention and efficient seeking.

## Roadmap

- [ ] **Range-based queries** — Scan across multiple keys with prefix or range predicates
- [ ] **Reader groups** — Coordinated consumption with offset tracking and rebalancing
- [ ] **Tail following** — Efficiently wait for new entries without polling
- [ ] **Count API** — Fast cardinality estimates using SST metadata
- [ ] **Retention policies** — Segment-based deletion by time or size
- [ ] **Checkpoints** — Point-in-time snapshots for consistent reads (via SlateDB)
- [ ] **Clones** — Lightweight forks of the log for isolation or branching (via SlateDB)

# Log

A key-oriented log database built on an LSM tree (SlateDB).

Each **key is its own ordered log stream**. Creating a new stream is just writing
a new key — there is no partition count to provision, no topic to declare, and no
reconfiguration as the number of streams grows. Where a partitioned log (e.g.
Kafka) makes you size the system up front, Log lets you open millions of
independent streams on demand and lean on LSM compaction to keep each one's
entries clustered on disk.

## Quickstart

Prerequisites: `curl` and `jq`

### 1. Run the HTTP server

```bash
cargo run -p opendata-log --features http-server -- --in-memory --port 8080
```

### 2. Check health

```bash
curl -s http://localhost:8080/-/healthy
```

### 3. Append records

Use `application/protobuf+json` (byte fields are base64-encoded):

```bash
curl -s -X POST 'http://localhost:8080/api/v1/log/append' \
  -H 'Content-Type: application/protobuf+json' \
  -d '{"records":[{"key":"b3JkZXJz","value":"b3JkZXItMQ=="},{"key":"b3JkZXJz","value":"b3JkZXItMg=="}],"awaitDurable":true}'
```

### 4. Scan records for a key

```bash
curl -s 'http://localhost:8080/api/v1/log/scan?key=orders' | jq .
```

Other endpoints: `GET /api/v1/log/keys`, `GET /api/v1/log/segments`,
`GET /api/v1/log/count`, and `/-/ready`. See [RFC 0004](rfcs/0004-http-apis.md)
for the full HTTP surface.

## Data Model

A **record** is the unit you append: a `key` and a `value`. The key identifies a
log stream — all records sharing a key form one ordered, append-only log — and
can be any byte sequence. The value is an opaque payload that the log stores but
never interprets.

On append, every record is assigned a **sequence number** drawn from a single
counter shared by all keys. The counter only ever increases, so sequence numbers
reflect global append order across the whole database. Because keys interleave in
that one counter, a single key's sequences are monotonic but **not contiguous** —
gaps appear wherever other keys were appended in between. Sequence numbers are
how you address data: scans and counts take a sequence range.

Records are grouped into **segments**, each covering a contiguous window of the
global sequence space across all keys. Within a segment, entries are sorted by
key and then by sequence, so any entry is located by `(segment_id, key,
sequence)`. Segments are also the unit of compaction and retention; how they are
sealed is covered under [Design](#design).

If you come from Kafka, a record's key and value mean the same thing here — what
LogDb drops is topics and partitions. There is nothing to declare or size up
front; you append to a key and read it back. The global sequence number is the
one offset-like coordinate, ordering every record across all keys, and LogDb
makes reading any single key's log within that stream efficient.

## Design

The log database leans into its LSM representation: log streams are indexed by
arbitrary byte keys within a shared global keyspace. The diagram below shows a
simplified representation of the key structure within the LSM.

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

Storing entries by `(segment_id, key, sequence)` is what makes per-key reads
cheap: a key's records sort together within each segment, so a scan walks a
contiguous run rather than filtering the global stream.

### Segmentation

Segments partition the sequence space and scope compaction — entries within a
segment are sorted by key, then sequence. Conceptually, each segment represents
a window across all keys. Sealing the active segment and starting a new one is
controlled by [`SegmentConfig`](src/config.rs):

- **`seal_interval`** seals after a wall-clock duration elapses.
- **`seal_byte_limit`** seals once the active segment accumulates that many
  bytes of key+value data (a soft target — see the field docs for overshoot
  bounds).

Either threshold can be set; whichever is reached first triggers a seal. When
both are `None` (the default), all entries stay in segment 0 indefinitely.
Smaller segments reduce write amplification at the cost of read locality. See
[RFC 0002](rfcs/0002-logical-segmentation.md).

### Retention & compaction

Retention is enforced at segment granularity: a sealed segment becomes eligible
for reclamation once its end time is older than `now() - retention`. Retention
requires `seal_interval` to be set and to be no larger than the retention
window. Compaction operates per-segment over the LSM; because the log is
append-only there are no updates or deletes to reconcile, so compaction exists
purely to improve read-side key locality. Both are configured via
[`RetentionConfig`](src/config.rs) and [`LogCompactionOptions`](src/config.rs)
and described in [RFC 0005](rfcs/0005-compaction-and-retention.md).

### What this buys us

- **Many independent logs, zero provisioning** — each `key` is its own log
  stream; creating new streams is just writing new keys.
- **Automatic locality via compaction** — the LSM continuously rewrites data so
  entries for the same key cluster on disk over time.
- **Simple mental model** — scaling isn't "partition management"; it's "write
  and read by key".

### Trade-offs

- **Write amplification** — LSM compaction rewrites data, increasing total bytes
  written compared to a pure append-only layout. Segment configuration and
  windowed compaction strategies bound this cost.

## Usage

`LogDb` is the read+write entry point. `LogDbReader` is a read-only view over the
same storage for consumers that should not write; it periodically discovers new
data on a `refresh_interval`. Both implement the `LogRead` trait (`scan`,
`count`, `list_keys`, `list_segments`, `inspect`).

### Writing

```rust
use log::{LogDb, Config, Record};
use bytes::Bytes;

let log = LogDb::open(Config::default()).await?;

log.try_append(vec![
    Record { key: Bytes::from("orders"), value: Bytes::from("order-1") },
    Record { key: Bytes::from("events"), value: Bytes::from("login") },
]).await?;
```

`try_append` assigns each record a sequence number and returns immediately. The
returned `AppendOutput` carries the `start_sequence` of the batch. Use
`append_timeout(records, timeout)` to bound how long the call waits for queue
space under backpressure.

### Durability

Appends are acknowledged before they are durable. There are two ways to relate
to that durability, and they serve different needs.

**Force it with `flush`.** `flush()` blocks until every prior append is durable.
It also clears the write pipeline to get there, so it both *waits for* and
*hastens* durability — use it when you need a hard barrier (e.g. before shutdown,
or a synchronous "commit" point).

```rust
log.flush().await?;
```

**Observe it with the durable sequence.** Often you don't need to *force*
durability — you only need to *know* what has become durable. The durable
sequence is a watermark that advances on its own as storage confirms writes, so
following it lets you learn what's safe without issuing a `flush`. Crucially,
this does **not** clear the pipeline: in-flight batches keep flowing and
coalescing for throughput, while you observe the watermark catch up behind them.

```rust
// `durable_sequence()` is an exclusive upper bound: a value of N means every
// record with sequence < N is durable.
let watermark = log.durable_sequence();

// Or react as it advances, without forcing anything.
let mut rx = log.subscribe_durable();
rx.changed().await?;
```

#### Read visibility

Where the watermark above tracks the *write* side, **read visibility** governs
when a freshly appended record becomes observable to a scan. It is set per-`LogDb`
via [`Config::read_visibility`](src/config.rs), and a scan blocks until the data
it needs has reached the configured level:

- **`ReadVisibility::Memory`** (default) — a scan sees writes as soon as they are
  visible in memory, without waiting for object-store durability. Fast, but a
  record read this way may not yet survive a crash.
- **`ReadVisibility::Remote`** — a scan only sees writes after their durability
  is confirmed by the storage engine. Reads then never observe a record that
  could be lost, at the cost of waiting for the durable watermark to advance.

In other words, `Memory` lets reads run ahead of durability while `Remote` ties
read visibility to the same durability watermark that `flush` and
`durable_sequence` track. See
[RFC 0005](rfcs/0005-compaction-and-retention.md).

### Reading

```rust
use log::{LogRead, LogDbReader, ReaderConfig};
use bytes::Bytes;

let reader = LogDbReader::open(ReaderConfig::default()).await?;

// Scan all entries for a key
let mut iter = reader.scan(Bytes::from("orders"), ..).await?;
while let Some(entry) = iter.next().await? {
    println!("seq={} value={:?}", entry.sequence, entry.value);
}

// Scan from a checkpoint sequence
let mut iter = reader.scan(Bytes::from("orders"), checkpoint..).await?;

// Count entries in a sequence range (exact, e.g. for consumer lag)
let pending = reader.count(Bytes::from("orders"), checkpoint..).await?;
```

`LogDb` implements `LogRead` too, so the same `scan`/`count` calls work directly
on a writer instance.

### Listing

```rust
// All segments, or those overlapping a sequence range
let segments = reader.list_segments(..).await?;

// Keys present in a range of segments
let mut keys = reader.list_keys(0..segments.len() as u64).await?;
while let Some(log_key) = keys.next().await? {
    println!("key={:?}", log_key.key);
}
```

See [RFC 0003](rfcs/0003-listing-apis.md) for the listing model.

## Roadmap

- [x] **Count API** — exact entry counts over a sequence range (`count`), with
  per-segment breakdowns via `inspect`
- [x] **Retention policies** — segment-based deletion by time
  ([RFC 0005](rfcs/0005-compaction-and-retention.md))
- [x] **Size-based segmentation** — seal the active segment by byte size
  (`seal_byte_limit`)
- [ ] **Tail following** — efficiently wait for new entries without polling
  (in progress; [RFC 0007](rfcs/0007-efficient-tail-scans.md))
- [ ] **Range-based queries** — scan across multiple keys with prefix or range
  predicates
- [ ] **Reader groups** — coordinated consumption with offset tracking and
  rebalancing
- [ ] **Checkpoints** — point-in-time snapshots for consistent reads (via
  SlateDB)
- [ ] **Clones** — lightweight forks of the log for isolation or branching (via
  SlateDB)

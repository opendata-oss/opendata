# RFC 0007: Buffer Consumer

**Status**: Draft

## Summary

This RFC defines a durable ingestion path for vector upserts and
deletes via `opendata-buffer`. Producers write Vector protobuf
(`WriteRequest` / `DeleteRequest` from RFC 0004) through the buffer
`Producer` to object storage. An embedded background task in the
vector service consumes batches, decodes each entry, and applies it
to `VectorDb` via `write()` (upsert) or `delete()`. This complements
the direct HTTP endpoints (RFC 0004).

## Motivation

The direct HTTP path (RFC 0004) couples ingestion availability to
the vector service availability. If the service is down or crashes
after acknowledging a request, writes are lost. The 
[OpenData _Buffer_](buffer/rfcs/0001-stateless-buffer.md) decouples 
these: data is durable in object storage
before the service processes it, producers continue writing when the
service is down, and a crashed consumer resumes from the last
checkpoint via epoch-based fencing. Additionally, embedding-write
traffic is often produced in batch jobs that cross availability
zones; routing it through object storage avoids cross-AZ transfer
fees that are acute at this scale.

## Goals

- Define the contract between writer (any language with a buffer
  Producer) and reader (Rust, in `opendata`) -- the 3-byte metadata
  protocol is the key interface, since producer and consumer may
  live in different repositories
- Support both upsert and delete operations through a single buffer
  channel
- Define an embedded buffer consumer in the vector service
- Coexist with the direct HTTP write/delete endpoints
- Disabled by default; enabled via service-level config

## Non-Goals

- **Search-side caching of buffered writes** -- writes become visible
  only after the consumer applies them to `VectorDb`
- **Multi-consumer parallelism** -- single-consumer fencing is
  sufficient initially
- **Exactly-once guarantees** -- at-least-once; duplicate upserts
  are idempotent (last-write-wins by external ID), and duplicate
  deletes are idempotent (unknown IDs are silently skipped)
- **Changes to the buffer crate API**

## Design

### Architecture Overview

```
    ┌────────────────┐
    │  Client / Job  │
    └────────┬───────┘
             │ Vector proto
             │ (WriteRequest / DeleteRequest)
             ▼
    ┌────────────────┐            ┌────────────────────┐
    │ Buffer Producer├───────────▶│   Object Storage   │
    └────────────────┘            │ (data + manifest)  │
                                  └──────────┬─────────┘
                                             │
                              ╔══════════════▼═════════════╗
                              ║ Vector Service             ║
                              ║                            ║
                              ║ ┌────────────────────────┐ ║
                              ║ │ BufferConsumer         │ ║
                              ║ │                        │ ║
                              ║ │  Consumer              │ ║
                              ║ │  -> decode metadata    │ ║
                              ║ │  -> prost decode       │ ║
                              ║ │  -> VectorDb::write    │ ║
                              ║ │     or ::delete        │ ║
                              ║ └────────────────────────┘ ║
                              ╚════════════════════════════╝
```

Both this path and the direct HTTP path converge at
`VectorDb::write()` / `VectorDb::delete()`.

### Metadata Protocol

A 3-byte payload attached to each `Producer::produce()` call:

| Byte | Field              | Values                              |
|------|--------------------|-------------------------------------|
| 0    | `version`          | `1`                                 |
| 1    | `operation`        | `1`=upsert, `2`=delete              |
| 2    | `payload_encoding` | `1`=Vector protobuf (RFC 0004)      |

The consumer MUST reject unknown version or payload_encoding values.
Unknown operation values are skipped per-entry without stopping the
poll loop.

Each `produce()` call carries one metadata range that applies to the
entries appended in that call, so metadata unambiguously describes
its entry even when the buffer batches multiple calls into a single
data file. Upserts and deletes can therefore be interleaved on the
same buffer channel and are applied in the order the consumer
observes them.

**Encoding (any producer):**

```
[METADATA_VERSION, operation, payload_encoding]
// upsert: [1, 1, 1]
// delete: [1, 2, 1]
```

**Rust decoding:**

```rust
struct BufferMetadata {
    operation: u8,
    payload_encoding: u8,
}

impl BufferMetadata {
    fn decode(payload: &[u8]) -> Result<Self> {
        // reject if len < 3 or version != 1
    }
}
```

### Payload Encoding

The protobuf schema reuses the messages defined in RFC 0004 verbatim
so that a single encoder works for both the HTTP path and the
buffer path:

- `operation = 1` (upsert): a `prost`-encoded `WriteRequest` with
  one or more `Vector` entries. Identical to what the HTTP write
  handler accepts under `Content-Type: application/protobuf`.
- `operation = 2` (delete): a `prost`-encoded `DeleteRequest` with
  one or more external IDs. Identical to what the HTTP delete
  handler accepts.

Per-entry decoding errors are logged and skipped; they do not stop
the poll loop or block subsequent batches.

### Producer

The producer side is intentionally generic: any process that can
encode the RFC 0004 protobuf messages and call
`buffer::Producer::produce()` can ingest into the vector service.
Typical producers include:

- Rust client libraries that already construct `WriteRequest` /
  `DeleteRequest` for the HTTP API and reuse the same encoder for
  the buffer.
- Batch indexing jobs that emit large embedding payloads from offline
  pipelines and want durable buffering rather than an open HTTP
  connection to the service.
- Sidecar processes co-located with applications that buffer writes
  during transient service unavailability.

A producer:

1. Constructs a `WriteRequest` or `DeleteRequest`.
2. Encodes it with `prost` to bytes.
3. Calls `producer.produce(vec![entry_bytes], metadata_bytes)` with
   metadata `[1, 1, 1]` for an upsert or `[1, 2, 1]` for a delete.
4. Awaits `WriteHandle::await_durable()` (or the language equivalent)
   before acknowledging the caller, so durability is reported only
   after the buffer has flushed to object storage.

This RFC does not prescribe a specific exporter or sidecar
implementation; it specifies only the wire contract that any such
producer must follow.

### Vector Buffer Consumer

A `BufferConsumer` background task runs inside `VectorServer`,
started from `VectorServer::with_buffer_consumer()` and stopped
during graceful shutdown. It reuses the same `VectorDb` instance as
the HTTP handlers, so writes from both paths share the same
write coordinator, indexer queue, and durability semantics.

**Internal structure:**

The consumer splits work across two cooperating tasks connected by
bounded `mpsc` channels:

- A **collector loop** drives `buffer::Consumer`. It calls
  `next_batch()`, forwards consumed batches over a prefetch channel,
  and applies acks received over an ack channel. It owns the only
  `&mut Consumer`.
- A **poll loop** receives batches from the prefetch channel,
  processes each entry, and sends the batch sequence number back to
  the collector loop for ack.

The prefetch channel has capacity `1`, so the collector fetches at
most one batch ahead of the processor. This bounds memory and lets
fetch and decode/apply overlap without unbounded queueing.

**Per-entry processing:**

1. Locate the metadata range covering this entry index (entries
   without metadata are skipped with a log line).
2. Decode the 3-byte metadata header.
3. Reject unknown `payload_encoding` (logged, entry skipped).
4. Dispatch on `operation`:
   - `1` → decode `WriteRequest`, call `VectorDb::write()`.
   - `2` → decode `DeleteRequest`, call `VectorDb::delete()`.
   - other → log and skip.

**Lifecycle:**

- On startup, `BufferConsumer::run()` constructs a
  `buffer::Consumer` from `BufferConsumerConfig` and spawns the two
  loops. `run()` returns a `ConsumerHandle` carrying a
  `CancellationToken` and a `JoinHandle`.
- On `ConsumerHandle::shutdown()`, the token is cancelled. The
  collector drains any pending acks from the ack channel, then calls
  `Consumer::close()` to flush.
- Errors from `next_batch()` use exponential backoff up to a 5s
  cap, resetting on the next successful poll.

### Configuration

`buffer_consumer: Option<BufferConsumerConfig>` is added to the
top-level `Config`. When absent (the default), no consumer starts.

```rust
#[cfg(feature = "buffer")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConsumerConfig {
    /// Object store where the buffer queue lives. Must match the producer.
    pub object_store: ObjectStoreConfig,

    /// Path to the queue manifest. Must match the producer.
    /// Default: "ingest/manifest"
    #[serde(default = "default_buffer_manifest_path")]
    pub manifest_path: String,

    /// Path prefix for data batch objects. Must match the producer.
    /// Default: "ingest"
    #[serde(default = "default_buffer_data_path_prefix")]
    pub data_path_prefix: String,

    /// Poll interval when the queue is empty. Default: 100ms.
    #[serde(default = "default_buffer_poll_interval", with = "duration_secs")]
    pub poll_interval: Duration,

    /// How often the garbage collector runs. Default: 300s.
    #[serde(default = "default_buffer_gc_interval", with = "duration_secs")]
    pub gc_interval: Duration,

    /// Minimum age before an unreferenced batch file is deleted. Default: 600s.
    #[serde(default = "default_buffer_gc_grace_period", with = "duration_secs")]
    pub gc_grace_period: Duration,
}
```

**Feature gate:** New cargo feature `buffer = ["http-server",
"dep:buffer"]`. The `BufferConsumerConfig` field, the
`with_buffer_consumer()` builder method on `VectorServer`, and the
binary's wiring are all gated behind it.

**Example YAML:**

```yaml
buffer_consumer:
  object_store:
    type: S3
    bucket: my-ingest-bucket
    region: us-east-1
  manifest_path: "ingest/vector/manifest"
  data_path_prefix: "ingest/vector"
  poll_interval: 100ms
  gc_interval: 300s
  gc_grace_period: 600s
```

The buffer object store may differ from the vector service's storage
backend -- the buffer is an intermediary store.

### Coexistence with HTTP Endpoints

The HTTP write/delete handlers and the buffer consumer share the
same `Arc<VectorDb>` instance. There is no separate code path for
applying buffered writes: both call `VectorDb::write()` /
`VectorDb::delete()`, which serializes through the existing write
coordinator. Search visibility, flush behavior, and indexer
backpressure are therefore identical regardless of which path was
used to ingest.

A deployment may use either path exclusively, both simultaneously,
or migrate from one to the other without changing the on-disk state
or the read API.

## Alternatives Considered

- **Separate consumer binary** -- adds operational complexity;
  embedding the consumer next to `VectorDb` removes a network hop
  and lets both ingest paths share the same write coordinator.
- **Direct HTTP only** -- already exists (RFC 0004); the object
  storage path adds durability and producer-side decoupling that
  direct HTTP cannot provide.
- **Separate buffer channels for upsert and delete** -- two
  manifests would force producers to coordinate ordering between
  two queues (an upsert followed immediately by a delete might be
  reordered). A single channel with an `operation` byte preserves
  per-producer ordering and simplifies configuration.
- **JSON metadata** -- wasteful for a fixed 3-byte schema and harder
  to extend cheaply with bit flags later.
- **Reusing the timeseries 4-byte metadata layout** -- the vector
  consumer needs to distinguish upsert from delete on every entry,
  which the timeseries `signal_type` byte does not encode. A
  vector-specific 3-byte layout keeps the dispatch byte first-class
  rather than overloading an unrelated field.

## Updates

| Date       | Description   |
|------------|---------------|
| 2026-05-08 | Initial draft |

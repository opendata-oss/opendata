# RFC 0006: Buffer Consumer

**Status**: Draft

## Summary

This RFC defines a durable ingestion path for OTLP metrics via
`opendata-buffer`. A Go OTel Collector exporter writes OTLP
protobuf through the buffer `Producer` to object storage. An
embedded background task in the timeseries service consumes
batches, converts them via `OtelConverter`, and writes to
`TimeSeriesDb`. This complements the direct OTLP/HTTP endpoint
(RFC 0004).

## Motivation

The direct OTLP/HTTP path (RFC 0004) couples ingestion
availability to TSDB availability. If the service is down or
crashes after acknowledging a request, metrics are lost. The
buffer crate (RFC buffer-0001) decouples these: data is durable
in object storage before TSDB processes it, producers continue
writing when TSDB is down, and a crashed consumer resumes from
the last checkpoint via epoch-based fencing. Additionally,
OTel metrics traffic is high-volume and often crosses
availability zones; routing it through object storage avoids
cross-AZ transfer fees that are acute at this scale.

## Goals

- Define the contract between writer (Go, in
  `opendata-go`) and reader (Rust, in `opendata`) --
  the 4-byte metadata protocol is the key interface since
  the two sides live in different repositories
- Specify the Go OTel Collector exporter contract
- Define an embedded buffer consumer in the timeseries service
- Coexist with the direct OTLP/HTTP endpoint
- Disabled by default; enabled via service-level config

## Non-Goals

- **Logs and traces** — metadata reserves values, but consumers
  are not defined here
- **Multi-consumer parallelism** — single-consumer fencing is
  sufficient initially
- **Exactly-once guarantees** — at-least-once; duplicate writes
  are idempotent (last-write-wins)
- **Changes to the buffer crate API**

## Design

### Architecture Overview

```
    ┌────────────────┐
    │ OTel SDK / App │
    └────────┬───────┘
             └──┐
                │
    ╔═══════════▼══════════╗
    ║ OTel Collector       ║
    ║                      ║            ┌────────────────────┐
    ║ ┌──────────────────┐ ║            │   Object Storage   │
    ║ │     Receiver     │ ║    ┌───────▶ (data + manifest)  │
    ║ └──────────────────┘ ║    │       │                    │
    ║                      ║    │       └──────────┬─────────┘
    ║ ┌──────────────────┐ ║    │                  │
    ║ │OpenData Exporter ├─╫────┘                  │
    ║ └──────────────────┘ ║                       │
    ╚══════════════════════╝                       │
                                                   │
                                    ╔══════════════▼═════════════╗
                                    ║ Timeseries Service         ║
                                    ║                            ║
                                    ║ ┌────────────────────────┐ ║
                                    ║ │ BufferConsumer         │ ║
                                    ║ │                        │ ║
                                    ║ │  Consumer              │ ║
                                    ║ │  -> decode metadata    │ ║
                                    ║ │  -> prost decode       │ ║
                                    ║ │  -> OtelConverter      │ ║
                                    ║ │  -> tsdb.write()       │ ║
                                    ║ └────────────────────────┘ ║
                                    ╚════════════════════════════╝
```

Both this path and the direct OTLP/HTTP path converge at
`OtelConverter::convert()` -> `TimeSeriesDb::write()`.

### Metadata Protocol

A 4-byte payload attached to each `Producer::produce()` call:

| Byte | Field              | Values                          |
|------|--------------------|-------------------------------- |
| 0    | `version`          | `1`                             |
| 1    | `signal_type`      | `1`=metrics, `2`=logs, `3`=traces |
| 2    | `payload_encoding` | `1`=OTLP protobuf               |
| 3    | `reserved`         | `0`                             |

The consumer MUST reject unknown version, signal_type, or
payload_encoding values.

Each `Append()` call produces one entry with one metadata
range, so metadata unambiguously describes its entry even
when the buffer batches multiple calls into a single data
file.

The expectation is that different signal types use separate
buffer channels (distinct manifest paths), but this is not
a hard requirement. A consumer that only handles metrics
simply discards entries with other signal types.

**Go encoding:**

```go
func EncodeMetadata(signalType, encoding uint8) []byte {
    return []byte{MetadataVersion, signalType, encoding, 0}
}
```

**Rust decoding:**

```rust
struct BufferMetadata {
    version: u8,
    signal_type: u8,
    payload_encoding: u8,
}

impl BufferMetadata {
    fn decode(payload: &[u8]) -> Result<Self> {
        // reject if len < 4 or version != 1
    }
}
```

### Go OTel Collector Exporter

Component type `"opendata"`, alpha stability, metrics-only.
Lives in `opendata-go` as a separate repository.

**Configuration** maps directly to `buffer.ProducerConfig`:

```go
type Config struct {
    ObjectStore    ObjectStoreConfig
    DataPathPrefix string
    ManifestPath   string
    FlushInterval  time.Duration
    FlushSizeBytes int
    Compression    string // "none" | "zstd"
}
```

**Export flow:**

1. `ConsumeMetrics` marshals `pmetric.Metrics` to OTLP
   protobuf
2. Calls `writer.Append([][]byte{proto}, metadata)`
   with metadata `[1, 1, 1, 0]`
3. Awaits `WriteHandle.AwaitDurable(ctx)` before returning

On startup, creates object store + writer. On shutdown,
calls `writer.Close(ctx)`.

**Example collector config:**

```yaml
exporters:
  opendata:
    object_store:
      type: aws
      bucket: opendata-ingest
      region: us-east-1
    data_path_prefix: ingest/otel/metrics/data
    manifest_path: ingest/otel/metrics/manifest
    flush_interval: 10s
    flush_size_bytes: 1048576
    compression: zstd

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      exporters: [opendata]
```

### Timeseries Buffer Consumer

A `BufferConsumer` background task runs in
`TimeSeriesHttpServer` (same pattern as the scraper).
Requires read-write mode. Reuses the same `OtelConverter`
instance as the HTTP handler.

**Poll loop:**

1. Initialize consumer
2. Poll `next_batch()`
3. For each entry: decode metadata -> `prost::decode`
   OTLP proto -> `OtelConverter::convert()` ->
   `tsdb.ingest_samples()`
4. Ack batch
5. Sleep `poll_interval` when the queue is empty
6. On cancellation, flush pending acks

### Configuration

Add `buffer_consumer: Option<BufferConsumerConfig>` to
`PrometheusConfig`. When absent (default), no consumer starts.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConsumerConfig {
    /// Must point to same object store as the producer(s).
    pub object_store: ObjectStoreConfig,

    /// Must match the producer's manifest_path.
    /// Default: "ingest/manifest"
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,

    /// Poll interval when queue is empty. Default: 100ms.
    #[serde(default = "default_poll_interval")]
    pub poll_interval: Duration,
}
```

**Feature gate:** No new flag. Gated behind existing `otel`
feature. Adds `buffer` as a dependency of the timeseries
crate.

**Example YAML:**

```yaml
buffer_consumer:
  object_store:
    type: S3
    bucket: my-ingest-bucket
    region: us-east-1
  manifest_path: "ingest/otel/metrics/manifest"
  poll_interval: 100ms
```

The buffer object store may differ from TSDB storage -- the
buffer is an intermediary store.

### Observability

Prefix: `opendata_buffer_consumer`.

| Metric | Type | Description |
|---|---|---|
| `..._batches_processed_total` | Counter | Processed batches |
| `..._entries_processed_total` | Counter | Processed entries |
| `..._entries_skipped_total` | Counter | Skipped (errors) |
| `..._series_ingested_total` | Counter | Series written |
| `..._batch_processing_duration_seconds` | Histogram | Per-batch |
| `..._poll_empty_total` | Counter | Empty polls |

## Alternatives Considered

- **Separate consumer binary** -- adds operational complexity;
  embedded task follows the scraper pattern and is simpler.
- **Direct gRPC/HTTP** -- already exists (RFC 0004); the
  object storage path adds durability that direct RPCs cannot
  provide.
- **JSON metadata** -- wasteful for a fixed 4-byte schema.

## Updates

| Date       | Description   |
|------------|---------------|
| 2026-03-30 | Initial draft |

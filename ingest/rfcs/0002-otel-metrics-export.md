# RFC 0002: OTEL Metrics Export via Stateless Ingest

**Status**: Draft

**Authors**:

- [Almog Gavra](https://github.com/agavra)

## Summary

This RFC proposes a metrics-first integration between OpenData's stateless ingest
module and OpenTelemetry Collector. The integration uses `opendata-ingest` as a
generic durable append queue, adds batch-level compression, and exposes a
UniFFI-based Rust API so a Go exporter can write to it. The Collector exporter
itself lives in a separate repository.

Entries are standard OTLP protobuf `ExportMetricsServiceRequest` bytes stored
opaquely alongside a small versioned metadata payload. Because ingest is still
pre-alpha, this RFC does not preserve backward compatibility with previously
written batch objects.

## Motivation

The stateless ingest crate already provides the right primitive:

- batches opaque entries in memory
- flushes them durably to object storage
- appends batch locations to a manifest-backed queue
- exposes ordered consumption through the collector API

The integration lets a Collector exporter persist OTLP metrics payloads without
translating them into a database-specific representation, keeping the exporter
simple and preserving compatibility with future Rust consumers that decode
standard OTLP messages directly.

Batch-level compression is needed because OTLP payloads can be large and
uncompressed archival is costly. Compressing the entire flushed batch improves
ratio and reduces framing overhead versus per-entry compression.

## Goals

- Metrics-only first integration with OpenTelemetry Collector.
- Keep `opendata-ingest` generic and free of Collector-specific behavior.
- Store standard OTLP protobuf `ExportMetricsServiceRequest` messages as ingest
  entries.
- Add batch-level compression to stateless ingest.
- Define a minimal versioned metadata contract for interpreting ingest batches.
- Add UniFFI-based Rust bindings so Go can call the ingester.
- Preserve a path for future logs and traces via separate ingestors per signal.
- Ensure a Rust consumer can read batches and decode OTLP payloads without any
  Go-specific logic.

## Non-Goals

- Logs or traces export.
- Database-specific conversion from OTLP metrics into OpenData timeseries.
- Full Collector exporter implementation in this repository.
- Custom archive format beyond OTLP protobuf payloads.
- Per-entry compression.

## Design

### Overview

Three layers:

1. **`opendata-ingest`** — generic durable append queue, gains batch compression
2. **Rust bindings (this repo)** — opaque-byte UniFFI API for Go
3. **Collector exporter (separate repo)** — metrics-only v1, serializes
   `pmetric.Metrics` to OTLP protobuf, passes bytes + metadata to Rust

Persisted payload contract:

- one ingest entry = one serialized `ExportMetricsServiceRequest`
- one flushed batch object may contain many requests, compressed as a whole
- manifest metadata describes how to interpret ranges of records

The exporter uses `exporterhelper` with `sending_queue` enabled and
`wait_for_result = false`, so the handoff boundary is acceptance into the
exporter queue.

### Repository Boundary

**OpenData repository:**

- generic stateless ingest implementation + batch compression
- UniFFI binding APIs wrapping the ingest crate
- round-trip tests for compressed OTLP payloads

**Separate Collector exporter repository:**

- exporter factory, configuration, and OTLP metrics serialization
- metadata payload encoding
- Go binding usage

### Stored Payload

Each entry is one serialized OTLP protobuf `ExportMetricsServiceRequest`. The
ingest crate does not inspect the payload.

Future signal mappings:

- metrics: `ExportMetricsServiceRequest`
- logs: `ExportLogsServiceRequest`
- traces: `ExportTraceServiceRequest`

### Multi-Signal Evolution

Future logs and traces use separate ingest streams per signal, not mixed-signal
batches. The layout is one manifest and one data prefix per signal:

- `ingest/otel/metrics/manifest` and `ingest/otel/metrics/data/...`
- `ingest/otel/logs/manifest` and `ingest/otel/logs/data/...`
- `ingest/otel/traces/manifest` and `ingest/otel/traces/data/...`

This gives one `Ingestor` and `Collector` instance per signal with independent
flush sizing and lifecycle. The metadata `signal_type` field serves as validation
and debugging; routing comes from the signal-specific ingestor configuration.

### Metadata Contract

The ingest crate stores an opaque metadata payload per `ingest()` call range in
the queue manifest. For this integration, metadata is a 4-byte versioned
descriptor:

```text
+----------------------+-------------------------+
| field                | encoding                |
+----------------------+-------------------------+
| version              | u8                      |
| signal_type          | u8                      |
| payload_encoding     | u8                      |
| flags                | u8                      |
+----------------------+-------------------------+
```

Enumerations:

- `signal_type`: `1` = metrics, `2` = logs, `3` = traces
- `payload_encoding`: `1` = OTLP protobuf request message

`flags` is reserved. Producers must set it to zero; consumers must reject
unknown non-zero flags.

### Data Batch Compression

Compression is an ingest-level concern applied to the entire batch object, not
individual entries or OTel metadata.

The batch format adds a trailing uncompressed footer:

```text
+--------------------------------------+
| record 0: [len: u32 LE][data]        |
| record 1: [len: u32 LE][data]        |
| ...                                  |
| record N: [len: u32 LE][data]        |
+--------------------------------------+
| footer (7 bytes, fixed):             |
|   version      : u16 LE              |
|   record_count : u32 LE              |
|   compression  : u8                  |
+--------------------------------------+
```

Compression codec values: `0` = none, `1` = zstd, `2+` = reserved.

**Write path:**

1. encode entries into the batch record region
2. compress the record region if configured
3. append uncompressed footer
4. write to object storage
5. append location + caller metadata to queue manifest

**Read path:**

1. read batch object from object storage
2. read footer to discover compression codec
3. separate record region from footer
4. decompress if required
5. decode batch framing into individual entries

Version 1 supports `zstd` and `none`. `zstd` is preferred for protobuf-heavy
telemetry. The implementation uses one fixed zstd level internally. Unknown
codecs are rejected at both configuration time and read time.

### Ingest Crate Changes

Config additions:

```rust
pub enum BatchCompression {
    None,
    Zstd,
}

// IngestorConfig gains:
pub batch_compression: BatchCompression,
```

`CollectorConfig` is unchanged — compression is discovered from the batch footer.

The collector-facing batch type is extended to expose metadata:

```rust
pub struct CollectedMetadata {
    pub start_index: u32,
    pub ingestion_time_ms: i64,
    pub payload: Bytes,
}

pub struct CollectedBatch {
    pub entries: Vec<Bytes>,
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<CollectedMetadata>,
}
```

### UniFFI-Based Rust Bindings

The bindings live in a new `ingest-bindings/` crate (`opendata-ingest-bindings`),
a sibling to `ingest/`. This separation avoids churn in the core crate when the
binding surface changes (e.g., adding UniFFI annotations, `cdylib` target, or
binding-specific type wrappers). The bindings crate depends on `opendata-ingest`
and re-exports a synchronous, byte-oriented API via UniFFI.

Each OpenData subproject that needs foreign-language bindings should follow this
pattern with its own dedicated bindings crate. A future top-level RFC will
formalize shared conventions for UniFFI version pinning, CI patterns, language
targets, and publishing.

The bound type keeps the core name `Ingestor`:

```rust
pub struct Ingestor { ... }

impl Ingestor {
    pub fn new(config: IngestorConfig) -> Result<Self> { ... }

    pub fn ingest(&self, payload: Vec<u8>, metadata: Vec<u8>) -> Result<()> { ... }

    pub fn ingest_many(&self, payloads: Vec<Vec<u8>>, metadata: Vec<u8>) -> Result<()> { ... }

    pub fn flush(&self) -> Result<()> { ... }

    pub fn close(&self) -> Result<()> { ... }
}
```

Internally, the wrapper owns a Tokio runtime and blocks on the async core API:

```rust
runtime.block_on(async {
    ingestor
        .ingest(vec![Bytes::from(payload)], Bytes::from(metadata))
        .await?
        .watcher
        .await_durable()
        .await
})
```

The binding layer must not expose `tokio` internals, object store details,
manifest internals, or OTel protobuf types.

Generated Go bindings are produced from this crate and published for consumption
by the Collector exporter repository.

### Collector Exporter Design

The exporter is metrics-only in v1 with standard `exporterhelper` defaults:

- `sending_queue.enabled = true`, `wait_for_result = false`
- `retry_on_failure.enabled = true`

Export flow:

1. receive `pmetric.Metrics`
2. serialize to OTLP protobuf `ExportMetricsServiceRequest`
3. submit to exporterhelper sending queue (success returned here by default)
4. in queue worker: encode metadata, call Rust binding ingester
5. exporterhelper retries handle transient failures

### Success and Retry Semantics

By default, export success means the request was accepted into the Collector
queue — not that it is durably in object storage. With `wait_for_result = true`
or sending queue disabled, success reflects completion of the Rust ingest call.

Retry ownership: the Collector exporter owns retry/timeout/queue policy; the
Rust ingest crate owns local batching and durability after handoff.

### Rust Consumer Workflow

1. `Collector::initialize(last_acked_sequence)`
2. `next_batch()`
3. decode metadata, verify `signal_type` and `payload_encoding`
4. decompress batch if required, decode entries
5. decode each entry as `ExportMetricsServiceRequest`
6. `ack(sequence)` after durable downstream processing

### Failure Handling

Error cases: invalid metadata version, unsupported signal type or payload
encoding, unsupported compression codec, corrupted compressed object or batch
framing, invalid OTLP protobuf.

- Producers reject unsupported configuration at construction time.
- Consumers return errors on invalid metadata or compression.
- The exporter treats storage/backpressure failures as retryable unless
  clearly permanent.

### Testing

- `should_roundtrip_uncompressed_batch`
- `should_roundtrip_zstd_compressed_batch`
- `should_collect_zstd_compressed_batch`
- `should_expose_metadata_in_collected_batch`
- `should_reject_unknown_metadata_version`
- `should_decode_export_metrics_service_request_after_collect`

## Alternatives

### OTel envelope in OpenData core

Add an OTel-specific adapter crate with a typed envelope. Rejected: standard
OTLP request messages already exist, and a custom envelope adds a compatibility
surface too early.

### Per-entry compression

Compress each OTLP message individually. Rejected: worse compression ratio,
per-message overhead, and duplicated compression metadata per record.

### Collector exporter in this repository

Rejected: Collector component lifecycle differs, the exporter is Go while core
ingest is Rust, and a separate repository keeps the boundary explicit.

### Normalized internal metrics format

Translate to an internal representation before persisting. Rejected: duplicates
conversion logic, couples the exporter to one downstream representation, and
loses standard OTLP bindings.

## Open Questions

- Can an exporter with native bindings be accepted into
  `open-telemetry/opentelemetry-collector-contrib`, or would upstreaming require
  a pure-Go implementation?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-03-27 | Initial draft |

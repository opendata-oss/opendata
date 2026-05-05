# RFC 0003: ClickHouse Ingestor

**Status**: Draft

**Authors**:

- Apurva Mehta

## Summary

This RFC proposes a Rust ingestor that consumes OpenData Buffer batches and
writes them into ClickHouse. The ingestor is generic over the signal type
(OTLP logs first, OTLP metrics and other signals later) but is deliberately
specific to ClickHouse as the sink. Generalization to other sinks
(Postgres, BigQuery, S3-as-Parquet) is explicitly **not** a goal of this
project; the abstraction we want is "many OpenData data sources can write
to ClickHouse via the same optimal pipeline," not "the same runtime can
target any sink."

The ingestor is layered: a `BufferConsumerRuntime` polls Buffer; a
per-entry `MetadataEnvelopeDecoder` parses producer-supplied envelopes; a
`SignalDecoder` produces typed records; a `CommitGroup` coalesces records
across multiple Buffer batches into ClickHouse-sized inserts; an `Adapter`
maps decoded records to ClickHouse insert chunks; a `ClickHouseWriter`
executes the chunks; and an `AckController` advances and flushes the
Buffer ack high-watermark only after a commit group is fully durable.

The first concrete adapter ingests OTLP logs into a `ReplacingMergeTree`
table, with the Responsive controller as the alpha producer.

## Motivation

The Buffer module described in RFC 0001 gives any producer a durable,
ordered, object-store-backed queue. Today the only consumers are the
OpenData databases — Log and Timeseries — and each one ships its own
consumer loop. The next class of consumer we want to support is sinks
that already have strong analytical storage and need a reliable way to
receive data ingested via Buffer.

ClickHouse is the chosen sink because:

- It has a strong query story for both logs and metrics.
- It is operationally proven and easy to run as a managed service.
- Its `ReplacingMergeTree` engine gives convenient at-merge or at-query
  deduplication, which lines up well with Buffer's at-least-once
  acknowledgement model.
- It supports server-side insert deduplication tokens, which combine
  cleanly with Buffer's deterministic sequence numbering.

The reuse story for this project is *across signal types into ClickHouse*,
not across sinks. The same polling/retry/coalesce/ack pipeline should
work for OTLP logs, OTLP metrics, traces, and any future OpenData
signal. Generalizing the same code to non-ClickHouse sinks is not a goal
and would dilute the design — ClickHouse has specific requirements
around insert sizing, token semantics, and replicated dedupe that
non-trivially shape the layering.

The alpha use case is shipping Responsive controller logs to ClickHouse
in production. Today these logs reach Datadog through Fluent Bit. Datadog
stays the operational path during alpha; the new pipeline runs alongside
it and is evaluated for correctness, not yet relied on.

## Goals

- Define a signal-agnostic, ClickHouse-specific ingestor built on
  `buffer::Consumer` that can be extended with new signal decoders and
  adapters without changing the runtime.
- Define the layering: runtime, per-entry metadata envelope decoder,
  signal decoder, commit-group coalescer, adapter, ClickHouse writer,
  ack controller.
- Define ack, flush, and retry semantics that produce a clear, observable
  at-least-once guarantee with no silent replay window.
- Define the alpha OTLP logs adapter, including ClickHouse table shape,
  the dedupe model, and the ORDER BY tradeoff.
- Define the operational surface: configuration, dry-run, metrics, and
  failure handling.
- Set the alpha acceptance criteria so the prototype has a clear stop
  condition.

## Non-Goals

- Sink generalization. Non-ClickHouse sinks (Postgres, BigQuery,
  S3-as-Parquet, observability vendors) are out of scope. The runtime
  may be reused for other signals, but always into ClickHouse.
- Schema management or schema evolution beyond a per-adapter version
  bump. Adapter table DDL is committed in the crate and applied
  out-of-band for the alpha.
- Multi-table fanout from a single Buffer manifest.
- Server-side rollups, aggregation, or transformation that does not fit
  into a stateless row-mapping function.
- Fully declarative schema-by-config. Adapters are named, compiled, and
  tied to a specific signal/encoding. Configuration controls which
  adapter is selected, batch thresholds, dedupe policy, and operational
  flags — not arbitrary DDL.
- Replacing Datadog as the operational logs path. Datadog stays live
  during the alpha.
- Producer-side concerns. The Buffer producer/consumer contract from
  RFC 0001 is taken as-is.

## Background: Buffer Producer/Consumer Contract

This RFC builds on the Buffer contract defined in RFC 0001. The relevant
parts for the ingestor are:

```rust
impl Consumer {
    pub async fn initialize(&self, last_acked_sequence: Option<u64>) -> Result<()>;
    pub async fn next_batch(&self) -> Result<Option<ConsumedBatch>>;
    pub async fn ack(&self, sequence: u64) -> Result<()>;
    pub async fn flush(&self) -> Result<()>;
}

pub struct ConsumedBatch {
    pub entries: Vec<Bytes>,
    pub sequence: u64,
    pub location: String,
    pub metadata: Vec<Metadata>,
}

pub struct Metadata {
    pub start_index: u32,
    pub ingestion_time_ms: i64,
    pub payload: Bytes,
}
```

Key properties the ingestor relies on:

- A single active consumer per manifest, enforced by epoch fencing.
- In-order delivery within a consumer.
- In-order acks. **Important nuance:** `Consumer::ack(n)` advances the
  in-memory acked position but does not by itself durably dequeue the
  manifest. The current consumer batches dequeues internally and only
  flushes them to object storage every N acks. The explicit durability
  boundary is `Consumer::flush()`, which forces the dequeue to be
  written. The ingestor must call `flush()` after `ack(n)` whenever it
  wants the high-watermark to be replay-safe across crashes; otherwise
  up to N-1 acked sequences can be replayed after a restart.
- Per-range metadata: each `Metadata` item describes a contiguous range
  of records within the data batch and carries an opaque `payload` set
  by the producer. **A single Buffer batch can contain multiple ranges
  with different payloads**, because multiple producer `produce()` calls
  may have been coalesced into one flushed batch. The ingestor must
  treat metadata as a per-entry attribute, not a per-batch one.

## Design

### Architecture

The ingestor lives entirely on the sink side of the system:

```text
producer side (out of scope for this RFC):
  controller logs
    -> Fluent Bit
    -> OTEL Collector logs pipeline
    -> opendata-go OpenData exporter
    -> Buffer logs manifest + data prefix

sink side (this RFC):
  Buffer logs manifest + data prefix
    -> BufferConsumerRuntime           (poll, retry, backoff, shutdown)
    -> MetadataEnvelopeDecoder         (per-entry envelope parsing)
    -> SignalDecoder                   (e.g. OtlpLogsDecoder)
    -> CommitGroup                     (coalesce records across batches)
    -> Adapter                         (e.g. OtlpLogsClickHouseAdapter)
       -> Vec<InsertChunk>             (deterministic chunking, per-chunk token)
    -> ClickHouseWriter                (sync inserts, classified retry)
    -> AckController                   (ack high-watermark, flush)
```

The infra/app boundary is meaningful:

- **Infra** (sink-shape-driven, reusable across signals) owns: Buffer
  protocol, object storage I/O, per-entry envelope validation,
  polling, retry classification, commit-group coalescing, and ack
  flushing. None of this depends on what's in the payload.
- **App** (signal-specific) owns: payload decoding, schema assumptions,
  row mapping, dedupe key construction, ClickHouse settings choice, and
  the adapter's deterministic chunking function.

This boundary is what lets us add a metrics adapter or a traces adapter
later without rewriting the consumer loop.

### Execution Overview

The following diagram shows one successful consume-to-write cycle. The
numbered labels correspond to the steps below, in the same spirit as an
execution overview diagram: each component's job is shown at the point
where ownership of the data changes. The Buffer store and ClickHouse are
external systems; the runtime, decoders, commit group, adapter, writer,
and ack controller run inside the `clickhouse-ingestor` process.

```text

                          ╔═══clickhouse-ingestor process═════════════════════════════════════════════════════════════════════╗                                 
                          ║                                                                                                   ║                                 
                          ║                                                                                                   ║                                 
                          ║                                                                                                   ║                                 
                          ║   ┌────────────────┐      ┌────────────────┐      ┌────────────────┐      ┌────────────────────┐  ║                                 
                          ║   │ BufferConsumer │      │MetadataEnvelope│      │ SignalDecoder  │      │    CommitGroup     │  ║                                 
                        ┌─╫───▶    Runtime     ├─(2)──▶    Decoder     ├─(3)──▶OTLP -> records ├─(4)──▶  range low..high   │  ║                                 
                        │ ║   │                │      │                │      │                │      │                    │  ║                                 
╔══Object Storage══╗    │ ║   └────────────────┘      └────────────────┘      └────────────────┘      └──────────┬─────────┘  ║                                 
║                  ║    │ ║                                                                                      │            ║                                 
║    Manifest      ║    │ ║                   ┌───────────────────────────────(5)────────────────────────────────┘            ║                                 
║          +       ├─(1)┘ ║                   │                                                                               ║                                 
║     Batches      ║      ║                   │                                                                               ║         ╔═══════Clickhouse═════╗
║                  ║      ║          ┌────────▼───────┐          ┌──────────────────┐          ┌────────────────┐             ║         ║                      ║
╚═════════▲════════╝      ║          │    Adapter     │          │  InsertChunk[]   │          │ClickHouseWriter│             ║         ║      ClickHouse      ║
          │               ║          │  plan chunks   ├───(6)────▶   rows + token   ├───(7)────▶  sync insert   ├──────────(8)╫─────────▶  ReplacingMergeTree  ║
          │               ║          │                │          │                  │          │                │             ║         ║                      ║
          │               ║          └────────────────┘          └──────────────────┘          └────────┬───────┘             ║         ║                      ║
          │               ║                                                                             │                     ║         ╚══════════════════════╝
          │               ║                                              ┌──────────────(9)─────────────┘                     ║                                 
          │               ║                                              │                                                    ║                                 
          │               ║                                     ┌────────▼───────┐                                            ║                                 
          │               ║                                     │ AckController  │                                            ║                                 
          └───────────────╫────(10)─────────────────────────────┤  ack + flush   │                                            ║                                 
                          ║                                     │                │                                            ║                                 
                          ║                                     └────────────────┘                                            ║                                 
                          ║                                                                                                   ║                                 
                          ║                                                                                                   ║                                 
                          ╚═══════════════════════════════════════════════════════════════════════════════════════════════════╝                               
```

1. `BufferConsumerRuntime` reads the next unacked Buffer batch from the
   object store via `Consumer::next_batch()`. It materializes a
   `RawBufferBatch` containing `RawEntry[]` plus source coordinates
   (`sequence`, `entry_index`, manifest path, data path, ingestion time).
   No durable ack state changes yet.
2. `MetadataEnvelopeDecoder` consumes the `RawBufferBatch`, parses each
   entry's metadata envelope, and outputs `MetadataEnvelope[]` aligned
   with `RawEntry[]`. It validates `(version, signal_type, encoding)`
   against the configured ingestor; a mismatch is fail-closed.
3. `SignalDecoder` consumes `RawEntry[] + MetadataEnvelope[]` and outputs
   decoded signal records. For OTLP logs, one `RawEntry` can become many
   `DecodedLogRecord`s, so the decoder assigns `record_index` and
   completes the source identity
   `(sequence, entry_index, record_index)`.
4. `CommitGroup` consumes decoded records, appends them to the open
   group, and separately advances the input range
   `low_sequence..=high_sequence`. This range advances even when a batch
   decodes to zero rows.
5. When a row, byte, or age threshold trips, the adapter receives the
   drained `CommitGroupBatch`, sorts records deterministically, maps them
   to ClickHouse rows, and plans the chunk sequence.
6. The adapter outputs `InsertChunk[]`. Each chunk contains rows, column
   names, ClickHouse settings, `chunk_index`, and a deterministic
   `insert_deduplication_token`.
7. `ClickHouseWriter` consumes `InsertChunk[]` and performs synchronous
   inserts one chunk at a time. Retryable failures retry the same chunk
   body and token.
8. ClickHouse accepts the insert into the target `ReplacingMergeTree`
   table. In the best case, a replay inside the insert deduplication
   window is dropped here before duplicate rows are materialized.
9. After every chunk for the commit group succeeds, the runtime invokes
   `AckController` with the successful `low_sequence..=high_sequence`
   range.
10. `AckController` acks every sequence in that contiguous range and
    flushes the Buffer manifest under the configured flush policy. This
    is the durable source checkpoint.

### Runtime

`BufferConsumerRuntime` owns the integration with `buffer::Consumer` and
nothing else. It does not interpret payload bytes, know what ClickHouse
is, or own the ack decision (acks live in the `AckController`).

Responsibilities:

- Holds the `Consumer`, the polling loop, and the retry/backoff state
  for transport-level errors (object store timeouts, transient I/O).
- Reads the next `ConsumedBatch` and splits it into a `RawBufferBatch`
  that carries Buffer source coordinates per entry.
- Hands the `RawBufferBatch` to the metadata decoder, then onward
  through the pipeline.
- Handles graceful shutdown by quiescing the polling loop and waiting
  for the in-flight commit group (if any) to complete or fail.

```rust
pub struct RawBufferBatch {
    pub sequence: u64,
    pub manifest_path: String,
    pub data_object_path: String,
    pub entries: Vec<RawEntry>,
}

pub struct RawEntry {
    /// Index within the batch (same index used by the Buffer record block).
    pub entry_index: u32,
    /// The raw bytes of this entry, opaque to the runtime.
    pub raw_bytes: Bytes,
    /// The metadata payload (envelope) that applies to this specific
    /// entry, taken from the per-range Metadata item that covers
    /// entry_index. A single Buffer batch may carry multiple ranges
    /// with different envelopes.
    pub raw_metadata: Bytes,
    /// Wall-clock ingestion time taken from the Metadata range that
    /// covers this entry.
    pub ingestion_time_ms: i64,
}
```

Splitting `ConsumedBatch.entries` into per-entry `RawEntry`s requires
applying the per-range `Metadata` items in `ConsumedBatch.metadata` to
each record index — and is the place where the runtime materializes the
"metadata is per-entry, not per-batch" property. Downstream layers see
only `RawEntry`s and never have to re-derive ranges.

A simplified pseudocode of the loop:

```rust
loop {
    // Wait for either the next Buffer batch or the commit group's
    // max-age deadline. At low volume, max_age is the only thing that
    // forces a partial group to flush.
    let next = tokio::select! {
        batch = consumer.next_batch() => Event::Batch(batch?),
        _    = sleep_until_age_deadline(&commit_group) => Event::Age,
    };

    match next {
        Event::Batch(Some(batch)) => {
            let raw = split_into_raw_entries(batch);
            let envelopes = metadata_decoder.decode_per_entry(&raw)?;
            metadata_decoder.validate_consistent(&envelopes, &configured)?;
            let decoded = signal_decoder.decode(&raw, &envelopes)?;
            // append updates the input high-watermark even if `decoded`
            // is empty.
            commit_group.append(decoded, raw.sequence);
        }
        Event::Batch(None) => sleep(poll_interval).await,
        Event::Age => { /* fall through to flush check */ }
    }

    if commit_group.should_flush() {
        let drained = commit_group.drain();
        if !drained.records.is_empty() {
            let chunks = adapter.plan(drained.clone())?;             // Vec<InsertChunk>
            writer.execute_all(chunks).await?;                       // sync, retry per chunk
        }
        // Even if the group was zero-row, advance the durable
        // high-watermark so Buffer doesn't accumulate an unacked tail.
        // The controller acks every sequence in the contiguous range
        // (Consumer::ack requires strict in-order acks) and flushes
        // per the configured AckFlushPolicy.
        ack_controller
            .on_commit_group_success(
                &mut consumer,
                drained.low_sequence,
                drained.high_sequence,
            )
            .await?;
    }
}
```

The runtime does not know what `decoded` or the chunks look like — it
only moves them through the pipeline and waits for the writer and the
ack controller to confirm durability before pulling more batches that
would push the commit group past its thresholds.

### Metadata Envelope Decoder

The metadata payload that producers put on each entry is currently a
4-byte header defined in `opendata-go/exporter/opendataexporter/metadata.go`:

```text
[version: u8][signal_type: u8][encoding: u8][reserved: u8]
```

with `version = 1`, `signal_type ∈ {1: metrics, 2: logs}`, and
`encoding ∈ {1: OTLP protobuf}` for the producers we have today. Logs
support is added in Phase 1a of the alpha plan.

The decoder parses this header **per `RawEntry`**, not per batch:

```rust
pub struct MetadataEnvelope {
    pub version: u8,
    pub signal_type: SignalType,
    pub encoding: PayloadEncoding,
}

pub trait MetadataEnvelopeDecoder {
    fn decode_per_entry(
        &self,
        batch: &RawBufferBatch,
    ) -> Result<Vec<MetadataEnvelope>, EnvelopeError>;

    fn validate_consistent(
        &self,
        envelopes: &[MetadataEnvelope],
        configured: &ConfiguredEnvelope,
    ) -> Result<(), EnvelopeError>;
}
```

Two-step contract:

1. `decode_per_entry` returns one envelope per entry in the same order
   as `RawBufferBatch.entries`. Producers can write multiple ranges
   with different envelopes into the same Buffer batch (RFC 0001
   `Producer::produce` model), so this step never assumes batch
   uniformity.
2. `validate_consistent` enforces, for the alpha, that every envelope
   in a batch matches the configured `(signal_type, encoding)` of the
   running ingestor. Mismatched envelopes within a batch are a fail-closed
   condition. This is a deliberate alpha simplification — once the alpha
   stabilizes, a future release can route per-entry to per-signal
   pipelines rather than rejecting the whole batch.

Compatibility policy for the alpha:

- Unknown `version` on any entry: fail closed. Do not ack. Emit a loud
  error metric. Retry until configuration or code is updated.
- Mixed signal types within a batch: fail closed.
- Configured signal/encoding does not match the entry's: fail closed.
- Unknown `encoding`: fail closed.

A future release may add a quarantine / dead-letter mode that acks the
offending range and records it elsewhere. That mode is explicitly out of
scope for the alpha; Buffer's strict ordering means the safest default
during prototype work is to halt rather than silently drop.

### Signal Decoder

`SignalDecoder` is a small trait that converts `RawEntry`s into typed,
sink-agnostic decoded records. The trait is the seam between the generic
ingestor and the data shape.

```rust
pub trait SignalDecoder {
    type Output;
    fn decode(
        &self,
        batch: &RawBufferBatch,
        envelopes: &[MetadataEnvelope],
    ) -> Result<Self::Output, DecodeError>;
}
```

`envelopes` has the same length as `batch.entries` and is in
correspondence with them. The metadata decoder has already validated
that they are consistent with the configured signal/encoding before
the signal decoder is called, so the decoder can rely on uniformity
within a single call. Passing the slice rather than a single envelope
keeps the door open for a future per-entry routing mode.

For the alpha there is exactly one implementation, `OtlpLogsDecoder`,
which produces:

```rust
pub struct DecodedLogs {
    pub records: Vec<DecodedLogRecord>,
}

pub struct DecodedLogRecord {
    /// Buffer source coordinates carried forward so the adapter can
    /// emit them as system columns if it chooses.
    pub source: SourceCoordinates,
    /// Decoded OTLP log record.
    pub log: OtlpLogRecord,
}

pub struct SourceCoordinates {
    pub sequence: u64,
    pub entry_index: u32,
    pub record_index: u32,
    pub manifest_path: String,
    pub data_path: String,
    pub ingestion_time_ms: i64,
}
```

A single `RawEntry` may decode into many `DecodedLogRecord`s because an
OTLP `LogsData` payload contains a tree of resource → scope → log records.
`record_index` is a flat index assigned by the decoder so that
`(sequence, entry_index, record_index)` uniquely identifies a logical row.

The decoder is responsible for:

- Length-decoding and protobuf-decoding the payload.
- Flattening the `ResourceLogs` / `ScopeLogs` / `LogRecord` tree into a
  flat list, copying resource and scope attributes into each record so
  the adapter can do simple row mapping.
- Assigning `record_index`.

The decoder is *not* responsible for ClickHouse types, columns, or
serialization. That belongs to the adapter.

### CommitGroup

`CommitGroup` is the coalescing layer between the signal decoder and the
adapter. ClickHouse strongly prefers inserts of 1k–100k rows and at most
roughly one insert query per second per sync writer; one Buffer batch
per insert is too small under typical load and produces a back-pressure
mismatch where the ingestor and ClickHouse fight each other.

The commit group buffers decoded records (with their source coordinates)
across multiple Buffer batches and triggers an insert when any threshold
trips:

- `max_rows_per_commit_group` (default 100,000)
- `max_bytes_per_commit_group` (default 32 MiB)
- `max_age_per_commit_group` (default 1 second)

It also tracks the contiguous range of Buffer sequences it has absorbed
so the `AckController` can advance the high-watermark to the right
sequence on success.

```rust
pub struct CommitGroup<R> {
    records: Vec<R>,
    low_sequence: Option<u64>,
    high_sequence: Option<u64>,
    bytes: usize,
    opened_at: Instant,
    thresholds: CommitGroupThresholds,
}

impl<R> CommitGroup<R> {
    /// Records the successful processing of a Buffer sequence. The
    /// input high-watermark advances regardless of whether the batch
    /// produced rows — an OTLP payload with zero log records, or an
    /// adapter that drops everything, still represents forward
    /// progress that must be ack-able.
    pub fn append(&mut self, records: Vec<R>, sequence: u64);

    pub fn should_flush(&self) -> bool;
    pub fn drain(&mut self) -> CommitGroupBatch<R>;
    pub fn high_watermark(&self) -> Option<u64>;
    pub fn time_until_max_age(&self, now: Instant) -> Option<Duration>;
}

pub struct CommitGroupBatch<R> {
    pub records: Vec<R>,
    pub low_sequence: u64,
    pub high_sequence: u64,
}
```

Two properties worth pinning down:

- **Input progress is independent of output rows.** `append` always
  updates `low_sequence` / `high_sequence` even when `records` is
  empty. `drain()` returns an empty `records` vector when the group
  absorbed only zero-row inputs; the flush path skips the writer (no
  chunks to insert) and goes straight to acking the contiguous range
  `low_sequence..=high_sequence` followed by `flush()`. Buffer never
  accumulates an unacked tail just because the data happened to
  filter to nothing.
- **Max-age requires a timer.** `should_flush()` is checked at append
  time, but at low volume a partially-filled group could otherwise
  sit indefinitely. The runtime drives flushing through a
  `select!` between `consumer.next_batch()` and a sleep until
  `time_until_max_age`. This is a runtime contract, not a CommitGroup
  internal concern.

Commit groups never span a fail-closed condition. If the metadata
decoder, signal decoder, or adapter rejects part of an in-flight commit
group, the group is discarded along with whatever it had absorbed; the
Buffer is not acked; the ingestor halts.

### Adapter

`Adapter` is the seam between signal-shaped data and ClickHouse. Adapters
are scoped to (signal type, encoding, target table) and are compiled in.
The adapter takes a drained `CommitGroupBatch` and produces a
deterministic sequence of `InsertChunk`s.

```rust
pub trait Adapter {
    type Input;

    fn plan(
        &self,
        batch: CommitGroupBatch<Self::Input>,
    ) -> Result<Vec<InsertChunk>, AdapterError>;
}

pub struct InsertChunk {
    pub database: String,
    pub table: String,
    pub columns: Vec<&'static str>,
    pub rows: Vec<Row>,
    pub clickhouse_settings: ClickHouseSettings,
    /// Deterministic, unique-per-chunk token. Must be a pure function of
    /// (low_sequence, high_sequence, chunk_index, configured thresholds).
    /// Replaying the same Buffer sequence range under the same
    /// configuration must produce the same tokens.
    pub idempotency_token: String,
    pub chunk_index: u32,
    pub observability_labels: Vec<(&'static str, String)>,
}
```

Adapter responsibilities (the parts that are *not* the runtime's,
decoder's, commit group's, writer's, or ack controller's):

- Target database and table, including schema version assumptions.
- Column list and per-column type expectations.
- Row mapping from decoded records to `Row`s.
- Choice of which Buffer source coordinates to materialize as system
  columns.
- The `_adapter_version` value, which is owned by the adapter.
- Deterministic chunking: splitting the commit group into one or more
  `InsertChunk`s of an adapter-chosen size, with stable boundaries
  under replay.
- Per-chunk idempotency token construction (format below).
- ClickHouse settings on each chunk (`async_insert = 0`, `insert_quorum`,
  `insert_deduplication_token`).

#### Idempotency Token Format

A single Buffer sequence range can be split into multiple ClickHouse
inserts (because the commit group exceeded a row/byte threshold), and a
commit group can absorb multiple Buffer sequences. Each chunk needs a
distinct token; reusing a token across chunks with different rows would
let ClickHouse drop valid data, because `insert_deduplication_token`
takes priority over the data hash.

The format is:

```text
{manifest_path}:{database}.{table}:{low_sequence}-{high_sequence}:{adapter_version}:{chunking_fingerprint}:{chunk_index}
```

Where:

- `manifest_path` identifies the Buffer stream. It prevents two
  manifests writing to the same ClickHouse target from sharing a token
  namespace.
- `database.table` identifies the ClickHouse target. It prevents two
  adapters writing different targets from the same manifest from sharing
  a token namespace.
- `low_sequence` / `high_sequence` come from the commit group's
  absorbed sequence range.
- `adapter_version` is the same `_adapter_version` value the adapter
  writes into the row; it changes when the row mapping changes.
- `chunking_fingerprint` is a stable hash (e.g. SHA-256 truncated to
  64 bits, hex-encoded) of every input that affects chunk boundaries:
  `(max_chunk_rows, max_chunk_bytes, ordering_rule_id, any other
  inputs that influence which records land in which chunk)`. Two
  configurations that produce identical chunk boundaries must produce
  the same fingerprint; any change that could move a record to a
  different chunk must change the fingerprint.
- `chunk_index` is a zero-based position within the adapter's chunking
  output.

The chunking function must be pure given
`(records, fingerprinted_config)` so a replay produces the same chunk
boundaries and therefore the same tokens.

Why the fingerprint is required: without it, a partial-success-plus-
restart sequence where chunking config changed in between would let two
different chunks share the same token, which silently drops valid data.
With it, post-config-change chunks insert under a new token namespace
and either dedupe at the table level (same `ORDER BY` tuple, version
column resolves the winner) or insert as logically distinct rows. The
table engine, not the token, owns correctness across config changes.

If chunking is non-deterministic for any reason (e.g. concurrent merges
inside the adapter), the alpha guarantee degrades to "duplicates may
remain past merge until query-time `FINAL`." We pick the deterministic
path for alpha and verify it in tests.

### ClickHouse Writer

`ClickHouseWriter` is a thin driver. It owns:

- The HTTP client and connection pool to ClickHouse.
- Per-chunk insert request execution with the settings from
  `InsertChunk`.
- Error classification.
- Writer-level metrics (insert latency, byte volume, classification
  counts).

Insert behavior for alpha:

- **Sync inserts only.** `async_insert = 0` is set on every chunk. The
  writer does not return success for a chunk until ClickHouse has
  acknowledged the insert.
- **All-chunks-or-none for ack.** A commit group's high-watermark is
  advanced only when *every* chunk in the group succeeds. If chunk N
  fails non-retryably while chunks 0..N-1 already inserted, the
  ingestor halts: the chunks that landed are kept (they are
  idempotently re-applicable on restart with the same tokens), the
  ack does not advance, and on restart the same commit group replays
  with the same token shape so the already-applied chunks are
  deduplicated by ClickHouse and the failing chunk is re-attempted.
- **Quorum.** `insert_quorum = 'auto'` is set when the target table is
  on a replicated engine. For a single-node target (local development)
  the setting is unset.
- **Idempotency token.** Each chunk's `insert_deduplication_token` is
  set from the adapter's per-chunk token (see format above). This is a
  fast-path backstop; correctness across longer crash windows is
  provided by the table engine, not by the token.
- **Format.** Inserts use `RowBinary` or `JSONEachRow`, chosen for the
  alpha based on row complexity (Map columns push us toward `JSONEachRow`
  unless we use `RowBinaryWithNamesAndTypes`).

Error classification:

- `5xx`, network errors, and timeouts: retryable. Exponential backoff
  with jitter, capped retry budget per chunk. After budget is exhausted,
  halt.
- `429`: retryable, backoff respects ClickHouse's rate-limit signal.
- `4xx` other than `429`: non-retryable. Halt the consumer and surface
  the error so an operator can inspect.

### Ack and Retry Semantics

The alpha guarantee is **at-least-once delivery** to ClickHouse, with
deduplication at the table level. The `AckController` owns the durable
boundary.

Per RFC 0001, `Consumer::ack(n)` requires strictly in-order, one-at-
a-time acks (acking a sequence that is not immediately after the
last acked sequence returns an error) and updates an in-memory acked
position. The consumer batches the actual manifest dequeue, only
flushing every N acks (currently 100). A bare `ack` is therefore not
durable; a crash between `ack(n)` and the next internal flush replays
sequences ≤ `n`.

After a commit group succeeds, the controller acks every sequence in
the contiguous range the group absorbed and then flushes once at the
end:

```rust
for seq in low_sequence..=high_sequence {
    consumer.ack(seq).await?;
}
consumer.flush().await?;  // under default AckFlushPolicy
```

This is the pair that makes the high-watermark durable. A future
`Consumer::ack_through(high)` API would collapse the loop into a
single call; that is a buffer-crate follow-up and not v0.

```rust
pub struct AckController {
    flush_policy: AckFlushPolicy,
    groups_since_flush: u32,
}

pub enum AckFlushPolicy {
    /// Flush after every commit group. Default. Durable boundary is
    /// per commit group; replay window is at most one commit group.
    EveryCommitGroup,
    /// Flush after N commit groups. Operator opt-in for higher
    /// throughput at the cost of an explicit replay window.
    EveryN(u32),
}

impl AckController {
    /// Called by the runtime, which owns the Consumer, after a commit
    /// group's chunks have all succeeded. The controller decides
    /// whether this is a flush boundary and acks the full contiguous
    /// range the group absorbed. The Consumer is *not* owned by the
    /// controller — manifest mutations require exclusive access and
    /// the runtime is the single owner.
    ///
    /// The current `Consumer::ack` requires strictly in-order, one-at-
    /// a-time acks (acking a sequence not immediately after the last
    /// acked returns an error), so the controller iterates the range:
    ///
    ///     for seq in low_sequence..=high_sequence {
    ///         consumer.ack(seq).await?;
    ///     }
    ///     if self.should_flush_now() {
    ///         consumer.flush().await?;
    ///     }
    ///
    /// A future `Consumer::ack_through(high)` API would let the loop
    /// collapse into one call; that is a buffer-crate change and
    /// post-v0.
    pub fn on_commit_group_success(
        &mut self,
        consumer: &mut Consumer,
        low_sequence: u64,
        high_sequence: u64,
    ) -> impl Future<Output = Result<()>> + '_;
}
```

The runtime owns the `Consumer`. The controller is a stateless-ish
policy helper that the runtime invokes; it never holds a long-lived
`Arc<Consumer>` because the consumer's manifest mutations
(`ack`, `flush`, `dequeue`) are not safe under shared mutable access.
If we ever need to share a consumer across tasks, that is a separate
`ConsumerHandle` design that synchronizes access internally — it is
not the controller's job.

Concretely, per commit group:

- All chunks succeed → ack the contiguous range
  `low_sequence..=high_sequence` → `flush()` (under default policy)
  → forward progress.
- A chunk fails (retryable) → exponential backoff per chunk, no ack
  advance, lag grows.
- A chunk fails (non-retryable) → consumer halts. Operator decides
  whether to skip the offending sequence range (a config-driven escape
  hatch) or fix the underlying problem.
- Process crashes between `ack` and `flush` → on restart, the consumer
  resumes from the last *flushed* high-watermark and replays. The
  `ReplacingMergeTree` table dedupes on the source-coordinate key, and
  ClickHouse's per-chunk `insert_deduplication_token` collapses
  identical re-attempts within the dedup window.

The `time_since_last_successful_ack_seconds` gauge is the primary
alerting signal: if Buffer is producing and the ack-flush is not
advancing, it climbs unbounded.

### Idempotency and Dedupe Model

Two layers of dedupe, with deliberately different roles:

1. **Authoritative: `ReplacingMergeTree(_adapter_version)`.**
   The dedupe key is the table's `ORDER BY` (see "ORDER BY Tradeoff"
   below); the version column is `_adapter_version`. On merge, the
   row with the highest `_adapter_version` for a given key wins.
   Three consequences worth pinning down:
   - A replay of the same logical record after an adapter mapping
     change resolves to the new mapping rather than implementation-
     defined order — *but only when every column in the `ORDER BY`
     tuple is unchanged*. Version-based dedupe operates within a
     single key.
   - The adapter must monotonically increase `_adapter_version`
     across mapping changes; downgrades are not safe.
   - **Adapter version bumps may change non-key columns only.** Any
     change to a column that participates in `ORDER BY` (today:
     `toDate(Timestamp)`, `ServiceName`, plus the `_odb_*` suffix)
     produces a different dedupe key for the same logical record and
     therefore *will not* dedupe through the version column. Such
     changes are a new schema and require a new table plus a backfill
     or rewrite path. The adapter trait should make this constraint
     explicit; the row-mapping function for ORDER BY columns gets a
     dedicated stability test in the alpha test suite.

2. **Backstop: ClickHouse `insert_deduplication_token`.**
   Set per chunk to
   `"{manifest_path}:{database}.{table}:{low_sequence}-{high_sequence}:{adapter_version}:{chunking_fingerprint}:{chunk_index}"`
   (see "Idempotency Token Format" above for the full definition).
   Drops a re-sent identical insert within ClickHouse's deduplication
   window. Useful for tight crash-restart loops where the same chunk
   replays before the dedup window expires; not load-bearing for
   correctness across longer windows. Chunk identity is essential —
   reusing one token across chunks with different rows would let
   ClickHouse drop valid data.

The correctness argument is:

- The only durable source checkpoint is the Buffer ack + flush. If the
  process crashes before that checkpoint, Buffer will replay from the
  last flushed ack boundary.
- A replay of the same commit group under the same adapter version and
  chunking configuration produces the same ordered rows, the same chunk
  boundaries, and the same per-chunk tokens.
- In the best case, where that replay happens while ClickHouse still has
  the original insert in its deduplication window, ClickHouse suppresses
  the duplicate chunk before rows are materialized. In that case plain
  reads have no duplicate rows and do not need `FINAL`.
- If the replay falls outside the insert deduplication window, or the
  chunking fingerprint changes because the chunking configuration
  changed, ClickHouse may materialize duplicate physical rows. This is
  why insert-level dedupe is not the load-bearing guarantee.
- The table-level source-coordinate key is the long-term guarantee. A
  duplicate physical row for the same logical source record has the same
  `ORDER BY` tuple, and `ReplacingMergeTree(_adapter_version)` collapses
  it on background merge or at query time with `FINAL`. If the adapter
  version changed but the `ORDER BY` tuple did not, the higher
  `_adapter_version` row wins.

This gives the same read-time behavior as insert-only dedupe designs in
the common tight-retry case: the duplicate is dropped by ClickHouse's
insert dedupe path and never appears to readers. The additional
guarantee is for long-window replays, explicit operator replays, or
changed chunking config, where insert-only dedupe can admit duplicate
rows but the target table still has enough source identity to collapse
them. The alpha therefore does not promise duplicate-free plain reads in
the cases where the table-level backstop is needed before merges run;
those readers must use `FINAL` or query-time dedupe. The runbook
documents the query patterns that are safe.

### System Column Ownership

System columns surface Buffer source coordinates (and adapter metadata)
into the destination table. They are owned by the layer that produces the
information:

| Column                  | Owned by   | Rationale                                              |
|-------------------------|------------|--------------------------------------------------------|
| `_odb_sequence`         | runtime    | Comes from `ConsumedBatch.sequence`.                   |
| `_odb_entry_index`      | runtime    | Index in the Buffer data batch.                        |
| `_odb_record_index`     | decoder    | Flat index of decoded records within an entry.         |
| `_odb_manifest_path`    | runtime    | Source manifest configuration.                         |
| `_odb_data_path`        | runtime    | `ConsumedBatch.location`.                              |
| `_odb_ingestion_time_ms`| runtime    | From the Buffer per-range `Metadata`.                  |
| `_adapter_version`      | adapter    | Adapter schema version, not a Buffer-level fact.       |

The runtime emits the source coordinates on `RawBufferBatch` and the
decoder extends them to `(sequence, entry_index, record_index)`. The
adapter chooses which of those to materialize as columns. `_adapter_version`
is computed entirely by the adapter and lives outside the runtime trait.

### Logs Alpha Adapter

The alpha adapter targets a single ClickHouse logs table with a shape
chosen to support the controller debugging workload:

```sql
CREATE TABLE logs (
    Timestamp           DateTime64(9)               CODEC(Delta, ZSTD),
    SeverityText        LowCardinality(String),
    SeverityNumber      UInt8,
    ServiceName         LowCardinality(String),
    Body                String                      CODEC(ZSTD),
    ResourceAttributes  Map(LowCardinality(String), String),
    LogAttributes       Map(LowCardinality(String), String),
    TraceId             String                      CODEC(ZSTD),
    SpanId              String                      CODEC(ZSTD),

    _odb_sequence            UInt64,
    _odb_entry_index         UInt32,
    _odb_record_index        UInt32,
    _odb_manifest_path       LowCardinality(String),
    _odb_data_path           String,
    _odb_ingestion_time_ms   Int64,
    _adapter_version         UInt32
)
ENGINE = ReplacingMergeTree(_adapter_version)
PARTITION BY toDate(Timestamp)
ORDER BY (toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index, _odb_record_index)
TTL toDate(Timestamp) + INTERVAL 30 DAY;
```

Notes:

- `ReplacingMergeTree(_adapter_version)` makes the version column the
  tiebreaker on merge; the adapter monotonically increases its version
  across mapping changes.
- `PARTITION BY` is on `Timestamp` for natural retention via TTL and
  efficient partition drops.
- The TTL is a placeholder — the production retention policy is an open
  question.
- `Map(LowCardinality(String), String)` is a pragmatic default for OTLP
  attribute maps. If the alpha shows that frequent attribute keys deserve
  promoted columns, that is a post-alpha materialized-view change.

The row mapping is mechanical: copy timestamp, severity, body, service
name, attributes; populate the system columns from `SourceCoordinates`;
set `_adapter_version` to a constant defined in the adapter.

#### ORDER BY Tradeoff

`ReplacingMergeTree` dedupes on the full `ORDER BY` tuple, which forces
a tradeoff between dedupe identity and query layout:

- A pure source-coordinate key
  `(_odb_sequence, _odb_entry_index, _odb_record_index)` is clean for
  dedupe but bad for log queries: every common filter
  (`ServiceName = ?`, `Timestamp BETWEEN`, `SeverityNumber > ?`) ends
  up scanning unrelated parts.
- A pure query-shaped key like `(ServiceName, Timestamp, SeverityNumber)`
  is good for queries but does not uniquely identify a record, so
  dedupe collapses logically distinct rows that happen to share those
  columns.

The alpha default above hybridizes: a query-useful prefix
`(toDate(Timestamp), ServiceName, ...)` followed by the source-coordinate
suffix that preserves uniqueness. Because every column in the prefix is
a deterministic function of the source row (the timestamp and service
name come from the OTLP record itself, and a replay produces the same
values), the same logical record always lands in the same `ORDER BY`
slot, so dedupe still works.

This shape is a working default, not a final answer. Phase 3 of the
execution plan calls for a benchmark of three representative log query
shapes against this default and against a raw-landing-table-plus-
materialized-view alternative. The alpha ships with whichever shape
the benchmark recommends; the choice and the measurements are folded
back into this RFC at Phase 6.

### Deployment Model

The ingestor is a single Rust binary deployed by Heracles into the
`responsive` namespace as a regular K8s `Deployment`. The image is built
from the `opendata` workspace through Heracles' existing service-build
scaffolding (the same pattern as `opendata-log` and `opendata-timeseries`).

A future split into a separate `opendata-connectors` repo, with its own
release cadence, is anticipated once we have more than one connector.
That split is explicitly post-alpha.

### Configuration

Configuration is layered: a YAML file mounted as a `ConfigMap`, with
`INGESTOR__` env vars overriding individual keys (Figment-style).

```yaml
buffer:
  manifest_path: ingest/otel/logs/manifest
  data_prefix: ingest/otel/logs/data
  object_store:
    type: Aws
    bucket: responsive-prod-opendata-otel-logs-us-west-2
    region: us-west-2

clickhouse:
  endpoint: https://...clickhouse.cloud:8443
  database: responsive
  table: logs
  insert_quorum: auto
  # user/password come from env via Secret

runtime:
  dry_run: true
  poll_interval_ms: 250
  retry_max_attempts: 6
  retry_initial_backoff_ms: 100
  request_timeout_secs: 30

commit_group:
  max_rows: 100000
  max_bytes: 33554432
  max_age_ms: 1000

ack:
  policy: every_commit_group

adapter:
  adapter_version: 1
  max_chunk_rows: 100000
  max_chunk_bytes: 33554432
  apply_deduplication_token: true

metrics_server:
  bind_addr: 0.0.0.0:9090
```

`dry_run = true` runs the full pipeline including decode and adapter,
but skips the ClickHouse insert, the Buffer ack, and the flush. This is
the gate state for Heracles deployment (Phase 4) and the intermediate
state for production rollout (Phase 5).

**Dry-run → live transition requires a process restart.** While in
dry-run, the ingestor's in-process `Consumer` advances its fetch
cursor as batches are read, but the durable acked position on the
manifest does not move (acks are skipped). Hot-flipping `dry_run` to
`false` would resume from the in-process cursor and skip every batch
already decoded since startup. The supported procedure is: change
config in the ConfigMap → roll the deployment.

The ingestor's normal startup path is `Consumer::new(config, None)`,
which initializes from the earliest unacked sequence on the manifest
— i.e. from durable Buffer state, not in-process state. Operator-
supplied `last_acked_sequence` is reserved for explicit replay
scenarios documented in the runbook (Phase 6); the normal startup
never threads in-process state across the restart.

Credentials live only in environment variables sourced from K8s
`Secret`s. They are never in the ConfigMap.

### Operational Metrics

Prometheus metrics, scraped by the existing OTel collector:

- `buffer_batches_read_total` — counter of batches pulled from Buffer.
- `commit_groups_total` — counter of commit groups flushed.
- `rows_planned_total` — counter of rows produced by the adapter.
- `rows_inserted_total` — counter of rows acknowledged by ClickHouse.
- `insert_chunks_total{result}` — labeled by `success` / `failure`.
- `insert_latency_seconds` — histogram, per chunk.
- `commit_group_size_rows` / `commit_group_size_bytes` — histograms.
- `ack_flush_latency_seconds` — histogram, time from `ack` call to
  `flush` completion.
- `retry_count_total{reason}` — counter. The alpha labels retryable
  attempts as `reason="retryable"`.
- `last_decoded_sequence` — gauge. Advances even in dry-run.
- `last_acked_sequence` — gauge. Advances only when not in dry-run.
- `current_lag_sequences` — deferred. The runtime does not currently
  observe the manifest head sequence, so emitting a gauge now would be
  misleading. Add this once the buffer crate exposes a read-only
  head-sequence peek.
- `time_since_last_successful_ack_seconds` — gauge. Primary alerting
  signal in non-dry-run mode.
- `decode_failures_total{stage}` — labeled by `envelope` / `signal` /
  `adapter`.
- `clickhouse_failures_total{classification}` — labeled by writer error
  class (`Retryable` / `NonRetryable`) in the alpha.

`last_decoded_sequence` and `last_acked_sequence` are tracked
separately so dry-run is observable: in dry-run the decoded gauge
advances and the acked gauge stays still by design.
`time_since_last_successful_ack_seconds` is meaningful only outside
dry-run.

### Failure Modes

- **ClickHouse unreachable.** Inserts fail retryable. Lag grows.
  `time_since_last_successful_ack_seconds` climbs and pages. No data is
  lost; on recovery, the ingestor catches up.
- **Decode failure (envelope or signal).** Fail closed: no ack, halt,
  alert. Operator inspects the offending batch with the inspection tool
  and either ships a fix or, in a documented escape-hatch, advances the
  consumer past the bad sequence with a config flag.
- **Unknown signal type.** Treated as decode failure.
- **Non-retryable ClickHouse error.** Halt and alert. Likely a schema
  mismatch.
- **Process crash mid-flight.** On restart, replay the last unacked
  sequence; dedupe handles duplicates.
- **Object store outage.** `buffer::Consumer` retries internally;
  the runtime sees this as elevated `next_batch` latency.

## Alternatives Considered

### Use ClickHouse `S3Queue`

ClickHouse's `S3Queue` table engine streams data from S3 files directly
into a ClickHouse table. Rejected because:

- ODB data files use the OpenData binary batch format plus per-batch
  metadata; `S3Queue` expects known formats (CSV, JSONEachRow, Parquet).
  We would have to write ClickHouse-readable files instead of native
  ODB batches, which gives up the existing producer ecosystem.
- The Buffer manifest is the source of truth for ordering and ack.
  `S3Queue` has its own coordination model.
- We would lose the layered runtime/decoder/adapter design that makes
  this work reusable for sinks other than ClickHouse.

### Build a Custom ClickHouse Table Engine

A custom engine that natively reads ODB manifests would put the Buffer
protocol inside ClickHouse itself. Rejected for alpha — wrong blast
radius. Possibly revisitable as a long-term optimization once the
external ingestor design has stabilized.

### Inline the Sink in the Producer

We could write directly from the OTel Collector's exporter to ClickHouse,
skipping Buffer entirely. Rejected because we explicitly want Buffer's
durability and ordering guarantees, and because shipping logs to multiple
sinks (ClickHouse plus future ones) is materially easier when Buffer is
the fan-out point.

### Per-Signal Binaries Without a Generic Runtime

We could write a logs-specific binary now and worry about reuse later.
Rejected because the runtime is the *point* — the work to factor it out
afterward is much larger than getting the seam right up front, and the
seam is not where the complexity lives anyway.

## Open Questions

- ORDER BY shape for the logs table. Alpha shipped the hybrid default
  `(toDate(Timestamp), ServiceName, _odb_*)`. Production-scale
  benchmarks remain post-alpha validation and may motivate a new table
  plus backfill if query patterns demand a different key.
- Production retention policy for the logs table (the 30-day TTL above
  is a placeholder).
- Concrete log volume cap for alpha (controller logs only is the
  scope; the byte/record-per-second bound is informed by Phase 1c
  throughput).
- Whether to add binary-owned schema bootstrap/migration support, or to
  commit fully to out-of-band DDL. Alpha applied the generated DDL
  manually in ClickHouse Cloud.
- Whether the adapter's row format should be `JSONEachRow` for
  development clarity or `RowBinaryWithNamesAndTypes` for throughput.
  Alpha uses `JSONEachRow`; revisit with measurement if throughput or
  CPU cost makes serialization material.
- Whether to introduce a ClickHouse-side checkpoint table that records
  `(manifest_path, last_sequence)` so a fully-empty Buffer can still
  resume. For the alpha this is unnecessary because Buffer keeps
  unacked entries; revisit if we ever truncate Buffer for cost reasons.
- Whether `AckFlushPolicy::EveryN` should be allowed in production at
  all, given that the operator-visible value is "more throughput in
  exchange for an explicit replay window." Default for alpha is
  `EveryCommitGroup`; revisit if manifest-write rate becomes the
  bottleneck.

## Alpha Acceptance Criteria

- Controller logs are ingested into ClickHouse end-to-end through the
  documented path: Fluent Bit → OTEL Collector → opendata-go logs
  exporter → Buffer → clickhouse-ingestor → ClickHouse.
- A `FINAL` query over a 5-minute window matches Datadog's count for the
  same window within a documented drift tolerance.
- The ingestor can be paused (scale to 0) and resumed without data loss.
- Re-inserting an already processed Buffer sequence range produces no
  duplicate rows under `FINAL` queries. Operator-directed replay from an
  arbitrary sequence remains post-alpha tooling.
- Failure injection (block ClickHouse for several minutes) shows expected
  behavior: lag grows, ack age climbs, no data loss, recovery on its own.
- Heracles-prod deployment is observable via Prometheus and the runbook
  is sufficient for an on-caller other than the author to pause, resume,
  and inspect the system. Replay-from-sequence is documented as a
  post-alpha gap.
- This RFC reflects what was actually shipped.

## Updates

| Date       | Description                                                                                                                                             |
|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2026-04-28 | Initial draft                                                                                                                                            |
| 2026-04-28 | Per-entry metadata envelope; CommitGroup and AckController layers; chunked idempotency token; ReplacingMergeTree(_adapter_version); ORDER BY tradeoff. |
| 2026-04-28 | v0 traps: dry-run→live restart rule; commit-group input progress independent of rows; max-age timer flush; token includes adapter_version + chunking fingerprint; ORDER BY mapping change requires new table; AckController is not an Arc owner. |
| 2026-04-28 | Ack the contiguous range `low..=high` per commit group, not just the high-watermark; clarified normal startup is `Consumer::new(config, None)` (durable Buffer state), not in-process state. |
| 2026-04-29 | Phase 1a shipped: `opendataexporter` v0.4.0 published with OTLP logs signal support (signal_type=2). Phase 1b Part 2 shipped: Sindri prod collector running v0.4.0 image with both `opendata` (metrics) and `opendata/logs` exporters registered; logs bucket `responsive-prod-opendata-otel-logs-us-west-2` provisioned and idle pending Phase 1c source wiring. No design changes — the source/sink architecture, deployment model, and config shape (incl. bucket name in Configuration §) all matched the RFC as written. |
| 2026-04-29 | Phase 1c shipped: Fluent Bit `forward` output (scoped via two-stage `rewrite_tag` to namespace=responsive + container=controller, `keep=true` so Datadog still receives originals) → OTel collector `fluentforwardreceiver` on port 8006 → batch → opendata/logs exporter. Collector image `:a05d291` adds `fluentforwardreceiver` to the OCB build alongside otlpreceiver. Datadog path unaffected. Worth folding into the RFC's "Logs Alpha Adapter" or a new "Source-side wiring" subsection: aws-for-fluent-bit:stable bundles Fluent Bit 1.9.10, whose opentelemetry output is metrics-only — `forward` is the working source-side primitive on this version. Records currently land with kubernetes metadata in `attributes["kubernetes"]` rather than canonical OTLP `resource.attributes["k8s.*"]`, and severity is unset. A `transform` processor on the collector to promote those is Phase 5 polish, not a design change. |
| 2026-04-29 | Phase 4 shipped: Heracles ingestor pod `prod-clickhouse-ingestor` Running on the heracles EKS cluster with `dry_run = true`. Image `ghcr.io/opendata-oss/clickhouse-ingestor:ch-integration-odb-clickhouse-ingestor-59c0364`. Dedicated IRSA role (`heracles-clickhouse-ingestor-service-account-role`) with tight scoping: `GetObject` on the data prefix, `GetObject`/`PutObject`/`DeleteObject` on the manifest, `ListBucket` prefix-scoped to `ingest/otel/logs/*` plus the manifest. Prometheus scrape endpoint and per-batch INFO logging were identified as Phase 5 polish items. |
| 2026-04-30 | Phase 5 alpha shipped live: metrics server added (`/metrics`, `/-/healthy`), central OTel collector scrapes the ingestor, ClickHouse Cloud endpoint configured, `dry_run=false` rollout resumed from the earliest unacked sequence, drained sequences 0..97 into `responsive.logs`, and rows planned/inserted/queried matched 245 with zero decode/ClickHouse/retry failures. RFC reconciled with implementation details learned during review: manifest+table idempotency token namespace, YAML config, no emitted `current_lag_sequences` until Buffer exposes a head-sequence peek, manual alpha DDL application, and ORDER BY benchmark moved to post-alpha validation. |

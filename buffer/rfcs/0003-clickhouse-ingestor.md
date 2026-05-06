# RFC 0003: ClickHouse Ingestor

**Status**: Draft

**Authors**:

- Apurva Mehta

## Summary

This RFC defines a Rust ingestor that consumes OpenData Buffer batches and
writes them into ClickHouse. The ingestor is generic over signal type
(OTLP logs first, OTLP metrics and traces later), but ClickHouse-specific
as a sink. 

The ingestor is layered: a `BufferConsumerRuntime` polls Buffer; a
per-entry `MetadataEnvelopeDecoder` parses producer-supplied envelopes; a
`SignalDecoder` produces typed records; a `CommitGroup` coalesces records
across multiple Buffer batches into ClickHouse-sized inserts; an `Adapter`
maps decoded records to ClickHouse insert chunks; a `ClickHouseWriter`
executes the chunks; and an `AckController` advances and flushes the
Buffer ack high-watermark only after a commit group is fully durable.

The first concrete adapter ingests OTLP logs into a `ReplacingMergeTree`
table.

## Motivation

The Buffer module described in RFC 0001 gives producers a durable,
ordered, object-store-backed queue. Today the OpenData databases, like 
Timeseries, each ship their own consumer loop. We also need consumers for
external analytical stores that can use Buffer as their ingest path.

ClickHouse is a natural choice as our first sink since it is an increasingly
popular OLAP database suitable for a variety of use cases.

For now, we will focus on observability data and reuse across signal types 
into ClickHouse. Signal types refer to telemetry categories 
(0 = Metrics, 1 = Logs, etc.) and
were introduced in [the timeseries buffer consumer](https://github.com/opendata-oss/opendata/blob/ch-integration/odb-clickhouse-ingestor/timeseries/rfcs/0006-buffer-consumer.md).
The same polling, retry, coalesce, and ack pipeline should work for OTLP
logs, OTLP metrics, traces, and future OpenData signals. ClickHouse-specific
requirements around insert sizing, token semantics, and replicated
dedupe drive the layering. 

The first implementation targets OTLP logs. Metrics, traces, and richer
schema management are follow-on work, but the runtime boundaries should
be chosen now so those additions do not require rewriting the consumer
loop.

## Goals

- Define a signal-agnostic, ClickHouse-specific ingestor built on
  `buffer::Consumer` that can be extended with new signal decoders and
  adapters without changing the runtime.
- Define the layering: runtime, per-entry metadata envelope decoder,
  signal decoder, commit-group coalescer, adapter, ClickHouse writer,
  ack controller.
- Define ack, flush, and retry semantics that produce a clear, observable
  at-least-once guarantee with no silent replay window.
- Define the v0 OTLP logs adapter, including ClickHouse table schema,
  the dedupe model, and the ORDER BY tradeoff.
- Define the operational surface: configuration, dry-run, metrics, and
  failure handling.
- Set validation criteria for the v0 implementation.

## Non-Goals

- Sink generalization. Non-ClickHouse sinks (Postgres, BigQuery,
  S3-as-Parquet, observability vendors) are out of scope. The runtime
  may be reused for other payload types, but always into ClickHouse.
- Schema management or schema evolution beyond a per-adapter version
  bump. Adapter table DDL is committed in the crate and applied
  out-of-band for v0.
- Multi-table fanout from a single Buffer manifest.
- Server-side rollups, aggregation, or transformation that does not fit
  into a stateless row-mapping function.
- Fully declarative schema-by-config. Adapters are named, compiled, and
  tied to a specific signal/encoding. Configuration controls which
  adapter is selected, batch thresholds, dedupe policy, and operational
  flags, not arbitrary DDL.
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
- In-order acks. 
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
  telemetry producers
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

The runtime/application boundary is:

- Runtime code owns the Buffer protocol, object storage I/O, per-entry
  envelope validation, polling, retry classification, commit-group
  coalescing, and ack flushing. None of this depends on the payload.
- Signal-specific code owns payload decoding, schema assumptions, row
  mapping, dedupe key construction, ClickHouse settings, and the
  adapter's deterministic chunking function.

This boundary lets us add metrics or traces without rewriting the
consumer loop.

### Execution Overview

The diagram shows one successful consume-to-write cycle. Each number
matches the step below. Buffer storage and ClickHouse are external
systems. The runtime, decoders, commit group, adapter, writer, and ack
controller run inside the `clickhouse-ingestor` process.

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
   It does not change durable ack state.
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
each record index. This is where the runtime materializes the "metadata
is per-entry, not per-batch" property. Downstream layers see only
`RawEntry`s and never have to re-derive ranges.

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

The runtime does not inspect `decoded` or the chunks. It moves them
through the pipeline and waits for the writer and ack controller to
confirm durability before pulling more batches that would push the
commit group past its thresholds.

### Metadata Envelope Decoder

The metadata payload that producers put on each entry is currently a
4-byte header defined in `opendata-go/exporter/opendataexporter/metadata.go`:

```text
[version: u8][signal_type: u8][encoding: u8][reserved: u8]
```

with `version = 1`, `signal_type ∈ {1: metrics, 2: logs}`, and
`encoding ∈ {1: OTLP protobuf}` for the current producers.

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
2. `validate_consistent` enforces, for v0, that every envelope in a batch
   matches the configured `(signal_type, encoding)` of the running
   ingestor. Mismatched envelopes within a batch fail closed. v0 expects
   a homogeneous signal stream; later versions can route per-entry to
   per-signal pipelines.

Compatibility policy for v0:

- Unknown `version` on any entry: fail closed. Do not ack. Emit a loud
  error metric. Retry until configuration or code is updated.
- Mixed signal types within a batch: fail closed.
- Configured signal/encoding does not match the entry's: fail closed.
- Unknown `encoding`: fail closed.

Later versions can add a quarantine or dead-letter mode that acks the
offending range and records it elsewhere. v0 leaves that out. With
strict Buffer ordering, halting is safer than silently dropping data.

### Signal Decoder

`SignalDecoder` converts `RawEntry`s into typed, sink-agnostic decoded
records. This is the boundary between the generic ingestor and the data
model.

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

For v0 there is exactly one implementation, `OtlpLogsDecoder`,
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
OTLP `LogsData` payload contains a tree of resource, scope, and log records.
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
adapter. ClickHouse strongly prefers inserts of 1k to 100k rows and at most
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
    /// produced rows. An OTLP payload with zero log records, or an
    /// adapter that drops everything, still represents input
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

Two properties matter:

- **Input progress is independent of output rows.** `append` always
  updates `low_sequence` / `high_sequence` even when `records` is
  empty. `drain()` returns an empty `records` vector when the group
  absorbed only zero-row inputs; the flush path skips the writer (no
  chunks to insert) and acks the contiguous range
  `low_sequence..=high_sequence` followed by `flush()`. Buffer does not
  accumulate an unacked tail when the data filters to nothing.
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

`Adapter` maps decoded signal records into ClickHouse inserts. Adapters are
scoped to (signal type, encoding, target table) and compiled in. The
adapter takes a drained `CommitGroupBatch` and produces a deterministic
sequence of `InsertChunk`s.

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
    /// configuration produces the same tokens.
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

A single Buffer sequence range may be split into multiple ClickHouse
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
  different chunk changes the fingerprint.
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
inside the adapter), the v0 guarantee degrades to "duplicates may
remain past merge until query-time `FINAL`." We pick the deterministic
path for v0 and verify it in tests.

### ClickHouse Writer

`ClickHouseWriter` is a thin driver. It owns:

- The HTTP client and connection pool to ClickHouse.
- Per-chunk insert request execution with the settings from
  `InsertChunk`.
- Error classification.
- Writer-level metrics (insert latency, byte volume, classification
  counts).

Insert behavior for v0:

- **Sync inserts only.** `async_insert = 0` is set on every chunk. The
  writer does not return success for a chunk until ClickHouse has
  acknowledged the insert.
- **All-chunks-or-none for ack.** A commit group's high-watermark is
  advanced only when *every* chunk in the group succeeds. If chunk N
  fails non-retryably while chunks 0..N-1 already inserted, the
  ingestor halts: the chunks that landed are kept (they are
  idempotently re-applicable on restart with the same tokens), the
  ack does not advance, and on restart the same commit group replays
  with the same token format so the already-applied chunks are
  deduplicated by ClickHouse and the failing chunk is re-attempted.
- **Quorum.** `insert_quorum = 'auto'` is set when the target table is
  on a replicated engine. For a single-node target (local development)
  the setting is unset.
- **Idempotency token.** Each chunk's `insert_deduplication_token` is
  set from the adapter's per-chunk token (see format above). This is a
  fast-path backstop; correctness across longer crash windows is
  provided by the table engine, not by the token.
- **Format.** Inserts use `RowBinary` or `JSONEachRow`, chosen for the
  v0 implementation based on row complexity (Map columns push us toward `JSONEachRow`
  unless we use `RowBinaryWithNamesAndTypes`).

Error classification:

- `5xx`, network errors, and timeouts: retryable. Exponential backoff
  with jitter, capped retry budget per chunk. After budget is exhausted,
  halt.
- `429`: retryable, backoff respects ClickHouse's rate-limit signal.
- `4xx` other than `429`: non-retryable. Halt the consumer and surface
  the error so an operator can inspect.

### Ack and Retry Semantics

The v0 guarantee is **at-least-once delivery** to ClickHouse, with
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
    /// controller; manifest mutations require exclusive access and
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
    /// collapse into one call; that is a future buffer-crate change.
    pub fn on_commit_group_success(
        &mut self,
        consumer: &mut Consumer,
        low_sequence: u64,
        high_sequence: u64,
    ) -> impl Future<Output = Result<()>> + '_;
}
```
Per commit group:

- All chunks succeed: ack the contiguous range
  `low_sequence..=high_sequence`, then `flush()` under the default policy,
  and continue.
- A chunk fails (retryable): exponential backoff per chunk, no ack
  advance, lag grows.
- A chunk fails (non-retryable): consumer halts. Operator decides
  whether to skip the offending sequence range (a config-driven escape
  hatch) or fix the underlying problem.
- Process crashes between `ack` and `flush`: on restart, the consumer
  resumes from the last *flushed* high-watermark and replays. The
  `ReplacingMergeTree` table dedupes on the source-coordinate key, and
  ClickHouse's per-chunk `insert_deduplication_token` collapses
  identical re-attempts within the dedup window.

The `time_since_last_successful_ack_seconds` gauge is the primary
alerting signal: if Buffer is producing and the ack-flush is not
advancing, it climbs unbounded.

### Idempotency and Dedupe Model

The design uses two dedupe layers:

1. **Authoritative: `ReplacingMergeTree(_adapter_version)`.**
   The dedupe key is the table's `ORDER BY` (see "ORDER BY Tradeoff"
   below); the version column is `_adapter_version`. On merge, the
   row with the highest `_adapter_version` for a given key wins.
   Three consequences follow:
   - A replay of the same logical record after an adapter mapping
     change resolves to the new mapping rather than implementation-
     defined order, but only when every column in the `ORDER BY`
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
     dedicated stability test in the v0 test suite.

2. **Backstop: ClickHouse `insert_deduplication_token`.**
   Set per chunk to
   `"{manifest_path}:{database}.{table}:{low_sequence}-{high_sequence}:{adapter_version}:{chunking_fingerprint}:{chunk_index}"`
   (see "Idempotency Token Format" above for the full definition).
   Drops a re-sent identical insert within ClickHouse's deduplication
   window. Useful for tight crash-restart loops where the same chunk
   replays before the dedup window expires; not load-bearing for
   correctness across longer windows. Chunk identity is essential:
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

In the common tight-retry case, ClickHouse drops the duplicate on the
insert path, so readers do not see it. The table-level key handles
long-window replays, operator replays, and changed chunking config, where
insert-only dedupe can admit duplicate rows. v0 does not guarantee
duplicate-free plain reads before merges run in those cases. Readers
that need immediate correctness must use `FINAL` or query-time dedupe.

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

### OTLP Logs Adapter

The v0 logs adapter targets a single ClickHouse logs table with a schema
chosen for log search and debugging workloads:

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
- The TTL is a placeholder. Retention policy should be set by
  operators.
- `Map(LowCardinality(String), String)` is the v0 default for OTLP
  attribute maps. Promoting frequent attribute keys to dedicated columns
  or materialized views is a future optimization.

The row mapping copies timestamp, severity, body, service name, and
attributes; populates system columns from `SourceCoordinates`; and sets
`_adapter_version` to a constant defined in the adapter.

#### ORDER BY Tradeoff

`ReplacingMergeTree` dedupes on the full `ORDER BY` tuple, which forces
a tradeoff between dedupe identity and query layout:

- A pure source-coordinate key
  `(_odb_sequence, _odb_entry_index, _odb_record_index)` is clean for
  dedupe but bad for log queries: every common filter
  (`ServiceName = ?`, `Timestamp BETWEEN`, `SeverityNumber > ?`) ends
  up scanning unrelated parts.
- A pure query-oriented key like `(ServiceName, Timestamp, SeverityNumber)`
  is good for queries but does not uniquely identify a record, so
  dedupe collapses logically distinct rows that happen to share those
  columns.

The v0 default above hybridizes: a query-useful prefix
`(toDate(Timestamp), ServiceName, ...)` followed by the source-coordinate
suffix that preserves uniqueness. Because every column in the prefix is
a deterministic function of the source row (the timestamp and service
name come from the OTLP record itself, and a replay produces the same
values), the same logical record always lands in the same `ORDER BY`
slot, so dedupe still works.

This table/key is the v0 default. Before changing the table layout, benchmark
representative log query patterns against this key and against a raw
landing table plus materialized views.

### Configuration

Configuration is layered: a YAML file with `INGESTOR__` env vars
overriding individual keys (Figment-style).

```yaml
buffer:
  manifest_path: ingest/otel/logs/manifest
  data_prefix: ingest/otel/logs/data
  object_store:
    type: Aws
    bucket: opendata-otel-logs
    region: us-west-2

clickhouse:
  endpoint: https://...clickhouse.cloud:8443
  database: observability
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
but skips the ClickHouse insert, the Buffer ack, and the flush. This lets
operators validate source compatibility and decode behavior before
advancing the durable Buffer checkpoint.

**Dry-run to live transition requires a process restart.** While in
dry-run, the ingestor's in-process `Consumer` advances its fetch
cursor as batches are read, but the durable acked position on the
manifest does not move (acks are skipped). Hot-flipping `dry_run` to
`false` would resume from the in-process cursor and skip every batch
already decoded since startup. The supported procedure is: change
config, then restart the process.

The ingestor's normal startup path is `Consumer::new(config, None)`,
which initializes from the earliest unacked sequence on the manifest
(durable Buffer state, not in-process state). Operator-supplied
`last_acked_sequence` is reserved for explicit replay scenarios
documented in the runbook; the normal startup never threads in-process
state across the restart.

Credentials live only in environment variables sourced from the
operator's secret management mechanism. They are never in the YAML
config file.

### Future: Config-Driven Multi-Source Ingestors

The v0 configuration is intentionally single-source:

```text
one Buffer manifest -> one configured signal decoder -> one adapter -> one ClickHouse table
```

That is enough to validate the runtime, ack semantics, and dedupe model.
The same runtime can later host many independent source pipelines in one
process.

The future configuration should make sources explicit:

```yaml
clickhouse:
  endpoint: https://...clickhouse.cloud:8443
  # user/password still come from env via Secret
  writer_pool:
    max_in_flight_inserts: 4

sources:
  - name: service_logs
    buffer:
      manifest_path: ingest/otel/logs/manifest
      data_prefix: ingest/otel/logs/data
      object_store:
        type: Aws
        bucket: opendata-otel-logs
        region: us-west-2
    envelope:
      version: 1
      signal_type: logs
      encoding: otlp_protobuf
    decoder: otlp_logs
    adapter: otlp_logs
    target:
      database: observability
      table: logs
    adapter_config:
      adapter_version: 1
      max_chunk_rows: 100000
      max_chunk_bytes: 33554432
      apply_deduplication_token: true
    commit_group:
      max_rows: 100000
      max_bytes: 33554432
      max_age_ms: 1000
    ack:
      policy: every_commit_group

  - name: another_logs_stream
    buffer:
      manifest_path: ingest/otel/another-logs/manifest
      data_prefix: ingest/otel/another-logs/data
      object_store_ref: shared_logs_bucket
    envelope:
      version: 1
      signal_type: logs
      encoding: otlp_protobuf
    decoder: otlp_logs
    adapter: otlp_logs
    target:
      database: observability
      table: another_logs
```

Operationally, a multi-source process is a set of independent pipelines:

- Each source gets its own `buffer::Consumer`, `CommitGroup`, adapter
  instance, and `AckController`.
- A source's ack high-watermark is independent of every other source.
  A failure in one source must not advance another source's Buffer ack.
- Sources may share a `ClickHouseWriter` / connection pool, but the
  writer must apply per-source backpressure so one hot stream does not
  starve lower-volume streams.
- Metrics are labeled by `source`, `signal`, `database`, and `table`.
- Process health should distinguish one failed source from a failed
  ingestor. Readiness can fail when a required source stops. Optional
  sources can report degraded metrics and alerts.

This is config-driven selection of compiled adapters, not arbitrary
schema-by-config. The initial registry is a small mapping:

```text
(signal_type=logs, encoding=otlp_protobuf, adapter=otlp_logs)
  -> OtlpLogsDecoder + OtlpLogsClickHouseAdapter
```

Adding a signal requires a decoder, an adapter, and a registry entry.
Schema management can later make field mappings or generated DDL
configurable; that is separate from source-pipeline configuration.

Multiple sources add one dedupe constraint. The v0 table's `ORDER BY`
uses `(toDate(Timestamp), ServiceName, _odb_sequence, _odb_entry_index,
_odb_record_index)` because only one Buffer manifest writes to the table.
If two manifests can write to the same ClickHouse table, the source
identity is not unique unless the table key includes the manifest
identity too. A future config validator must enforce one of these
policies:

- **One manifest per table.** Multiple sources are allowed in one
  process, but each source writes to its own ClickHouse table.
- **Shared table with manifest in the dedupe key.** If multiple source
  manifests write to the same table, the table schema must include
  `_odb_manifest_path` in the `ORDER BY` tuple, e.g.
  `(toDate(Timestamp), ServiceName, _odb_manifest_path,
  _odb_sequence, _odb_entry_index, _odb_record_index)`.

The idempotency token already includes `manifest_path`, so insert-token
namespaces are safe across sources. Shared tables need the same property
in the table key.

Incremental path:

1. Normalize the current v0 config into a single-element `sources`
   list without changing behavior.
2. Add a source registry and run N independent source tasks in one
   process, sharing only the ClickHouse writer pool and metrics server.
3. Add config validation that rejects unsafe shared-table layouts unless
   the table's dedupe key includes `_odb_manifest_path`.
4. Add per-source health, per-source backpressure, and runbook support
   for pausing/resuming one source without restarting the whole process.
5. Add adapter configuration and binary-owned DDL/schema validation once
   the source model is stable.

This future work still does **not** imply multi-table fanout from one
Buffer manifest. Buffer currently has one active consumer per manifest;
fanout from a single manifest would require explicit Buffer consumer
group semantics or separate producer manifests. Until that exists, the
safe scaling unit is "many manifests, many independent source tasks."

### Operational Metrics

Prometheus metrics, scraped by the existing OTel collector:

- `buffer_batches_read_total`: counter of batches pulled from Buffer.
- `commit_groups_total`: counter of commit groups flushed.
- `rows_planned_total`: counter of rows produced by the adapter.
- `rows_inserted_total`: counter of rows acknowledged by ClickHouse.
- `insert_chunks_total{result}`: labeled by `success` / `failure`.
- `insert_latency_seconds`: histogram, per chunk.
- `commit_group_size_rows` / `commit_group_size_bytes`: histograms.
- `ack_flush_latency_seconds`: histogram, time from `ack` call to
  `flush` completion.
- `retry_count_total{reason}`: counter. v0 labels retryable
  attempts as `reason="retryable"`.
- `last_decoded_sequence`: gauge. Advances even in dry-run.
- `last_acked_sequence`: gauge. Advances only when not in dry-run.
- `current_lag_sequences`: deferred. The runtime does not currently
  observe the manifest head sequence, so emitting a gauge now would be
  misleading. Add this once the buffer crate exposes a read-only
  head-sequence peek.
- `time_since_last_successful_ack_seconds`: gauge. Primary alerting
  signal in non-dry-run mode.
- `decode_failures_total{stage}`: labeled by `envelope` / `signal` /
  `adapter`.
- `clickhouse_failures_total{classification}`: labeled by writer error
  class (`Retryable` / `NonRetryable`) in v0.

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
  ODB batches, which breaks compatibility with existing producers.
- The Buffer manifest is the source of truth for ordering and ack.
  `S3Queue` has its own coordination model.
- We would lose the layered runtime/decoder/adapter design that makes
  this work reusable for future signals into ClickHouse.

### Build a Custom ClickHouse Table Engine

A custom engine that natively reads ODB manifests would put the Buffer
protocol inside ClickHouse itself. Rejected for v0 because it has the
wrong blast radius. Possibly revisitable as a long-term optimization
once the external ingestor design has stabilized.

### Inline the Sink in the Producer

We could write directly from the OTel Collector's exporter to ClickHouse,
skipping Buffer. Rejected because Buffer provides durability and ordering,
and because Buffer is the right fan-out point when the same telemetry
needs multiple sinks.

### Copy the ClickHouse Kafka Sink Connector Exactly-Once Design

The [ClickHouse Kafka sink connector][clickhouse-kafka-sink] has an
exactly-once mode for Kafka Connect. Its [design document][kafka-design]
uses three ingredients:

1. Kafka Connect provides at-least-once delivery and does not commit
   source offsets past a `put()` call that has not completed.
2. The connector stores per-topic/partition state in ClickHouse
   `KeeperMap`: the previous Kafka offset range plus whether that range
   is `BEFORE_PROCESSING` or `AFTER_PROCESSING`.
3. Retries are designed to reproduce identical ClickHouse inserts for the
   same topic/partition offset range. The connector sets
   `insert_deduplication_token` from
   `{topic}-{partition}-{minOffset}-{maxOffset}` and relies on
   ClickHouse insert deduplication to suppress duplicate insert blocks
   inside the deduplication window.

The state machine handles new, same, overlapping, and contained Kafka
offset ranges. If the previous state is `AFTER_PROCESSING`, an
overlapping retry can skip the already-inserted prefix and insert only
the new suffix. If the previous state is `BEFORE_PROCESSING`, the
connector may reinsert the same range and depend on ClickHouse insert
dedupe. It also disables internal buffering in exactly-once mode because
buffering changes batch boundaries, which breaks the identical-block
dedupe assumption.

That design is a good fit for Kafka because Kafka offsets are the source
identity and Kafka Connect controls offset commits. It is not the right
fit for ODB:

- Buffer already has its own durable checkpoint: `ack` + `flush` on the
  manifest. Adding KeeperMap would introduce a second checkpoint store
  that must be kept consistent with Buffer.
- The Kafka connector's dedupe is bounded by ClickHouse's insert
  deduplication window. If an insert succeeds, the connector crashes
  before `AFTER_PROCESSING`, and the same Kafka range is retried after
  the dedupe window expires, a plain MergeTree target can admit duplicate
  rows. Our design uses insert tokens for the same best-case fast path,
  but makes long-window replay correctness depend on the table's
  source-coordinate `ReplacingMergeTree` key.
- The Kafka connector reconstructs previous insert block boundaries
  to make ClickHouse's block-level dedupe work. Our runtime
  decouples Buffer ack ranges from ClickHouse chunk boundaries: a commit
  group can coalesce multiple Buffer sequences and split them into
  deterministic chunks, with `chunk_index` and `chunking_fingerprint`
  preserving token safety.
- Kafka's source identity is `(topic, partition, offset)`. ODB's source
  identity for rows is `(manifest_path, sequence, entry_index,
  record_index)`, and `record_index` is decoder-owned because one Buffer
  entry can decode to multiple logical rows.
- The Kafka design optimizes for duplicate-free plain reads when the
  insert dedupe window catches the retry. We get the same behavior in
  that best case. The difference is that ODB keeps a table-level backstop
  for explicit replay, changed chunking config, or retry outside the
  insert dedupe window, at the cost of eventual dedupe unless readers use
  `FINAL` or query-time dedupe before merges run.

The ODB design keeps the useful part, deterministic insert tokens for
tight retries, without copying the Kafka connector's KeeperMap state
machine. Buffer ack remains the source checkpoint, and
`ReplacingMergeTree(_adapter_version)` remains the long-term dedupe
mechanism.

### Per-Signal Binaries Without a Generic Runtime

We could write a logs-specific binary now and factor the runtime out
later. Rejected because the runtime boundary is small and stable. Leaving
it embedded in a logs binary would make later metrics and traces support
more expensive.

[clickhouse-kafka-sink]: https://clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink
[kafka-design]: https://github.com/ClickHouse/clickhouse-kafka-connect/blob/a96f3341cb5a707a4c999dbac37725caeeaebbf1/docs/DESIGN.md

## Future Improvements

v0 keeps the runtime narrow. These follow-ups fit the same layering and
do not require changing the Buffer ack model.

- Per-entry signal routing: v0 expects each ingestor instance to read a
  homogeneous `(signal_type, encoding)` stream. A later version can route
  entries to different signal pipelines instead of failing a mixed batch.
- Dead-letter / quarantine mode: v0 fails closed on unknown metadata,
  unsupported encodings, decode errors, and schema mismatches. A later
  mode can ack offending ranges after recording them elsewhere, but it
  needs an operator-facing data-loss policy.
- Config-driven multi-source runtime: normalize the single-source config
  into a `sources` list, run one independent pipeline per manifest, share
  only the writer pool and metrics server, and enforce shared-table
  dedupe-key validation.
- Replay tooling: normal startup resumes from durable Buffer state.
  Operator-directed replay from an arbitrary sequence needs guardrails so
  it cannot skip data or create permanent duplicates by accident.
- ORDER BY and engine benchmarks: benchmark the v0 logs key
  `(toDate(Timestamp), ServiceName, _odb_*)` against query-oriented keys,
  raw landing tables plus materialized views, plain `MergeTree`, and
  `ReplacingMergeTree`.
- Schema bootstrap and validation: v0 assumes DDL is applied out of
  band. A later version can own table creation, validate that the live
  table matches the adapter's required columns and key, and reject unsafe
  shared-table configurations before consuming.
- Row serialization format: `JSONEachRow` is straightforward for map
  columns and development. `RowBinaryWithNamesAndTypes` may reduce CPU
  and bytes on the wire and needs measurement.
- ClickHouse-side checkpoint: Buffer ack is the source checkpoint. A
  separate ClickHouse checkpoint table is unnecessary while Buffer keeps
  unacked entries, but may help if operators later truncate Buffer
  manifests aggressively for cost reasons.
- Ack flush policy: `EveryCommitGroup` gives the smallest replay window.
  `EveryN` can reduce manifest writes but makes replay windows explicit;
  operators should enable it only after measuring the tradeoff.
- Retention and storage policy: TTL and storage settings should be
  operator config, not adapter constants.

## Validation Criteria

- OTLP logs are ingested end to end through the documented path:
  producer to Buffer to clickhouse-ingestor to ClickHouse.
- Dry-run mode performs Buffer reads, envelope decoding, signal decoding,
  commit-grouping, and adapter planning without ClickHouse inserts,
  Buffer acks, or manifest flushes.
- A clean shutdown drains the in-flight commit group, inserts all chunks,
  acks the contiguous Buffer sequence range, flushes the manifest, and
  exits with a durable high-watermark.
- Restart after a crash replays from the last flushed Buffer ack
  boundary and does not lose rows.
- Re-inserting an already processed Buffer sequence range produces one
  logical row per `(sequence, entry_index, record_index)` under
  `FINAL` queries.
- Replaying the same commit group with the same chunking config produces
  the same ordered rows, chunk boundaries, and dedupe tokens.
- If ClickHouse is unreachable, inserts fail retryably, Buffer ack does
  not advance, lag grows, and the ingestor catches up after recovery.
- Non-retryable decode or ClickHouse errors fail closed: no Buffer ack
  advance, clear metrics/logs, and operator intervention required.
- Metrics expose decoded progress, acked progress, commit-group counts,
  insert chunk results, retry counts, and ClickHouse failure
  classifications.

## Revision History

| Date       | Description |
|------------|-------------|
| 2026-04-28 | Initial draft. |
| 2026-04-28 | Added per-entry metadata envelope, CommitGroup and AckController layers, chunked idempotency tokens, `ReplacingMergeTree(_adapter_version)`, and ORDER BY tradeoff. |
| 2026-05-05 | Added execution overview, dedupe correctness exposition, Kafka sink connector comparison, and future config-driven multi-source path. |

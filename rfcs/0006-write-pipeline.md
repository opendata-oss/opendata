# RFC 0006: Write Pipeline (Source/Sink)

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC proposes a shared write pipeline model for OpenData systems based on `Source` and `Sink` abstractions over a generic payload type `T`.
The model supports direct writes and staged writes with intermediate `Sink`/`Source` pairs (for example, stateless ingest), while keeping protocol handling and storage concerns decoupled.

## Motivation

Multiple OpenData systems need the same end-to-end write pipeline shape:

- A frontend component accepts writes from external protocols.
- A terminal writer persists data into a database.
- Optional intermediate stages provide durability, buffering, replay, or transport.

Without a shared model, each database crate redefines naming, lifecycle, and acknowledgement semantics.
That duplication makes cross-system ingest features harder to implement and reason about.

## Goals

- Define a reusable source/sink pipeline model for all OpenData systems.
- Support push and pull source semantics with explicit acknowledgement.
- Support intermediate composable stages that are both sink and source.
- Keep payload type generic so each database provides its own serialization and domain model.

## Non-Goals

- Standardize one global record schema for all databases.
- Define protocol-specific HTTP/gRPC request parsing.
- Define datastore-specific idempotency logic in detail.

## Design

### Core Abstractions

The pipeline is generic over payload `T`:

- `Source<T>` emits payloads.
- `Sink<T>` accepts payloads.
- Intermediate stages may implement both (`Sink<T> + Source<T>`).

Conceptual direct pipeline:

```text
Source<T> -> Sink<T>
```

Conceptual staged pipeline:

```text
Source<T> -> StageA(Sink<T> + Source<T>) -> StageB(Sink<T> + Source<T>) -> Sink<T>
```

### System Roles in OpenData

This model characterizes OpenData components into three functional roles:

- **End systems**: terminal systems that persist and serve domain data.
  - Example: TimeSeries, Vector.
- **Connecting systems**: intermediate pipeline stages that can accept and emit payloads.
  - Example: Log, Stateless Ingest.
- **Services/adapters**: boundary components that translate external protocols into pipeline payloads (and optionally expose read APIs).
  - Example: HTTP remote-write and OTLP handlers for TimeSeries.

These are roles, not strict product boundaries. A single deployed component may implement multiple roles.
For example, a TimeSeries HTTP server can act as both a service adapter (ingest endpoints) and an end system (terminal sink).

### Push and Pull Sources

Sources come in two operational forms:

- Push source: receives upstream input and immediately invokes a downstream sink.
- Pull source: exposes batches for a downstream consumer to retrieve and acknowledge.

Proposed traits:

```rust
#[async_trait]
pub trait Sink<T>: Send + Sync {
    async fn accept(&self, payload: T) -> Result<SinkAck>;
}

#[async_trait]
pub trait PullSource<T>: Send + Sync {
    async fn next_batch(&self) -> Result<Option<Batch<T>>>;
    async fn ack(&self, batch: &Batch<T>) -> Result<()>;
}
```

Push sources are typically adapters at protocol boundaries and can be represented as handlers that parse wire input and call `Sink<T>::accept`.

### Acknowledgement Semantics

`SinkAck` communicates acceptance level:

- accepted in memory
- durable in underlying storage

The required level is protocol-specific and should be documented by each adapter.
For durability-sensitive protocols, success responses SHOULD map to durable acknowledgement.

### Stateless Ingest as Generic Sink/Source Stage

Stateless ingest (see `ingest/rfcs/0001-stateless-ingest.md`) fits this model as a reusable intermediate stage:

- `StatelessIngestSink<T>` accepts `T`, serializes it, and appends durable batches.
- `StatelessIngestSource<T>` reads queued batches, deserializes to `T`, and emits downstream.

This stage is generic in `T`; each database defines how `T` is serialized/deserialized.

### Payload and Serialization

This RFC does not mandate a single payload type for all systems.

Each database chooses:

- its pipeline payload type `T`,
- its record/batch format for any intermediate storage stage,
- compatibility/versioning strategy for that format.

Examples:

- TimeSeries may use `Vec<Series>`.
- Log may use log entries/batches.
- Vector may use vector write batches.

### Mapping to Database Implementations

Each database crate defines:

- protocol adapters that produce `T`,
- terminal sink that writes `T` to the database,
- optional intermediate stages and codecs.

Database-specific RFCs should reference this RFC and specify only system-specific bindings.

## Alternatives

### Per-database pipeline models

Each database could define its own source/sink contracts.
Rejected because this duplicates semantics and blocks shared ingest components.

### Shared payload type across all databases

A single canonical payload for all systems could be defined.
Rejected because data models differ significantly and would require lossy or over-generalized abstractions.

## Open Questions

- Should `nack` be part of the pull-source contract in addition to timeout-based redelivery?
- Should a shared `Batch` metadata schema (ids, trace fields) be standardized in `common`?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-03-03 | Initial draft |

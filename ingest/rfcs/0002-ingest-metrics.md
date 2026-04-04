# RFC 0002: Ingest Metrics

**Status**: Draft

**Authors**:

- [Almog Gavra](https://github.com/agavra)

## Summary

Add metrics to the ingest crate using
[metrics-rs](https://docs.rs/metrics/latest/metrics/) (v0.24).
Callers choose the exporter by installing a `metrics::Recorder`.

## Motivation

The ingest crate exposes only `ConflictCounter` and `queue_len`,
both internal `AtomicU64` fields with no exporter integration.
Operators cannot tell whether the collector is keeping up, whether
GC deletes are succeeding, or how much manifest contention exists.

RFC 0001 listed observability as TBD.

## Goals

- Counters, gauges, and histograms for the write path (ingestor)
  and read path (collector).
- No dependency on a specific exporter. The `metrics` crate uses
  a recorder facade; when no recorder is installed the macros are
  zero-cost no-ops.

## Non-Goals

- Bundling a specific exporter.
- Per-instance labels. Callers add those at the recorder level.

## Design

### Library choice

metrics-rs provides a facade (`metrics::counter!`,
`metrics::gauge!`, `metrics::histogram!`) and the binary installs
a `Recorder` of its choice. Already used in `bencher`. The
timeseries server installs `metrics-exporter-prometheus` to render
metrics-rs output on `/metrics` alongside storage-engine metrics.

We will likely continue to migrate the remaining crates in opendata
to the `metrics-rs` crate.

### Metrics

#### Ingestor (write path)

Recorded in `BatchWriterTask::write_and_enqueue`.

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `ingest.batches_flushed` | counter | | Batches written to object store |
| `ingest.entries_flushed` | counter | | Entries across all flushed batches |
| `ingest.bytes_flushed` | counter | | Raw bytes (pre-compression) |
| `ingest.bytes_written` | counter | | Bytes written (post-compression) |
| `ingest.flush_duration_seconds` | histogram | | Write + enqueue latency |
| `ingest.manifest_writes` | counter | `role` | Manifest write attempts |
| `ingest.manifest_conflicts` | counter | `role` | Manifest CAS conflicts |

`bytes_flushed` vs `bytes_written` gives compression ratio.

#### Collector (read path)

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `ingest.batches_collected` | counter | | Batches fetched |
| `ingest.entries_collected` | counter | | Entries fetched |
| `ingest.bytes_collected` | counter | | Bytes read from object store |
| `ingest.collector_lag_seconds` | gauge | | Wall clock minus last batch ingestion time |
| `ingest.queue_length` | gauge | | Entries in manifest queue |
| `ingest.acks` | counter | | Acks processed |
| `ingest.gc_files_deleted` | counter | | Batch files deleted by GC |
| `ingest.gc_files_failed` | counter | | Failed GC deletions |
| `ingest.gc_duration_seconds` | histogram | | GC cycle wall time |
| `ingest.fetch_duration_seconds` | histogram | | Batch fetch latency |
| `ingest.manifest_writes` | counter | `role` | Manifest write attempts |
| `ingest.manifest_conflicts` | counter | `role` | Manifest CAS conflicts |

#### Labels

`manifest_writes` and `manifest_conflicts` carry a `role` label
(`"producer"` or `"consumer"`) to distinguish ingestor-side from
collector-side contention. All other metrics are unlabeled;
ingestors and collectors run in separate processes.

#### Lag calculation

`collector_lag_seconds` is set after each `fetch_batch`:

```
lag = (SystemTime::now() - last_metadata.ingestion_time_ms) / 1000
```

Uses `SystemTime::now()` directly, not the injectable `Clock`.
Lag is operational, not correctness-critical.

## Alternatives

### prometheus-client directly

`timeseries` and `log` originally used `prometheus-client` v0.22.
This couples the library to one exporter and requires callers to
pass a `Registry`. metrics-rs decouples recording from export via
the global recorder facade.

### Custom traits (SlateDB RFC 0021 pattern)

SlateDB defines its own `CounterFn` / `GaugeFn` / `HistogramFn`
and `MetricsRecorder` traits. metrics-rs already provides the same
abstraction and is in our dependency tree, so custom traits would
duplicate existing infrastructure.

We can consider going down this path if things like UniFFI becomes
more of a concern (since Slate already provide UniFFI bindings to
its traits).

## Open Questions

None at this time.

## Updates

| Date       | Description   |
|------------|---------------|
| 2026-04-03 | Initial draft |

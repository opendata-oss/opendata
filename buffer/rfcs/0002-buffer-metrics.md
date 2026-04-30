# RFC 0002: Buffer Metrics

**Status**: Draft

**Authors**:

- [Almog Gavra](https://github.com/agavra)

## Summary

Add metrics to the buffer crate using
[metrics-rs](https://docs.rs/metrics/latest/metrics/) (v0.24).
Callers choose the exporter by installing a `metrics::Recorder`.

## Motivation

The buffer crate exposes only `ConflictCounter` and `queue_len`,
both internal `AtomicU64` fields with no exporter integration.
Operators cannot tell whether the reader is keeping up, whether
GC deletes are succeeding, or how much manifest contention exists.

RFC 0001 listed observability as TBD.

## Goals

- Counters, gauges, and histograms for the write path (writer)
  and read path (reader).
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

#### Buffer (write path)

Recorded in `BatchWriterTask::write_and_enqueue`.

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `buffer.batches_flushed` | counter | | Batches written to object store |
| `buffer.entries_flushed` | counter | | Entries across all flushed batches |
| `buffer.bytes_flushed` | counter | | Raw bytes (pre-compression) |
| `buffer.bytes_written` | counter | | Bytes written (post-compression) |
| `buffer.flush_duration_seconds` | histogram | | Write + enqueue latency |
| `buffer.manifest_writes` | counter | `role` | Manifest write attempts |
| `buffer.manifest_conflicts` | counter | `role` | Manifest CAS conflicts |

`bytes_flushed` vs `bytes_written` gives compression ratio.

#### Collector (read path)

| Name | Type | Labels | Description |
|------|------|--------|-------------|
| `buffer.batches_collected` | counter | | Batches fetched |
| `buffer.entries_collected` | counter | | Entries fetched |
| `buffer.bytes_collected` | counter | | Bytes read from object store |
| `buffer.consumer_lag_seconds` | gauge | | Wall clock minus last batch ingestion time |
| `buffer.queue_length` | gauge | | Entries in manifest queue |
| `buffer.acks` | counter | | Acks processed |
| `buffer.gc_files_deleted` | counter | | Batch files deleted by GC |
| `buffer.gc_files_failed` | counter | | Failed GC deletions |
| `buffer.gc_duration_seconds` | histogram | | GC cycle wall time |
| `buffer.fetch_duration_seconds` | histogram | | Batch fetch latency |
| `buffer.manifest_writes` | counter | `role` | Manifest write attempts |
| `buffer.manifest_conflicts` | counter | `role` | Manifest CAS conflicts |

#### Labels

`manifest_writes` and `manifest_conflicts` carry a `role` label
(`"producer"` or `"consumer"`) to distinguish producer-side from
consumer-side contention. All other metrics are unlabeled;
producers and consumers run in separate processes.

#### Lag calculation

`consumer_lag_seconds` is set after each `fetch_batch`:

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

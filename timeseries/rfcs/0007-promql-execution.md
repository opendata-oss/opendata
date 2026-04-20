# RFC 007: PromQL Execution Engine

**Status**: Draft
**Authors**: [Almog Gavra](https://github.com/agavra)

## Summary

This RFC proposes a rewrite of the core query execution pipeline for timeseries to a columnar,
timestamp-major, pull-based operator pipeline with proper logical and physical planning steps.

## Motivation

There are the following problems in the original implementation:

1. **Per-Step Repeated Work:** the implementation loops on every step during the query’s effective
   range and re-runs the evaluator evaluation. This pipeline does not properly amortize repeated
   work like label hashing, metadata lookups and per-series iteration. We papered over some of this
   using intra-query caches, but this fix was imperfect.
2. **Row-Oriented Compu**t**ation:** the data model stored samples keyed by label. This meant that
   each step evaluation would binary search for the closest matching sample (in the step’s
   neighborhood according to lookback window), and perform an indexed array lookup. This not only
   repeats work that is unnecessary for every step of evaluation it also is extremely inefficient
   for memory cache lines.
3. **Concurrency Is Implicit:** there is parallelism encoded within the data fetching using the
   semaphores, but the actual computation is serial and there is no easy way to speed that up.
4. **No Physical Planning Stage:** this not only makes it difficult to hook in optimizations like
   projection pushdown or range coalescing into SlateDB if/when we support that but it also makes it
   harder in the future for us to add things like a fan-out, multi-node query engine.

The new proposed pipeline brings `timeseries` closer to the SOTA implementation for query
evaluation.

## Goals

- One execution model for instant and range queries
- Columnar, step-major data flow from storage to result
- Explicit physical plan with a small and testable operator set
- Pipelined execution with async operators so we can stream a single query and saturate both CPU and
  I/O

## Non-Goals

- Distributed execution
- Cost-based optimizations
- PromQL language extensions
- Native Histograms

## Architecture

```
HTTP / embedded API
        │
        ▼
┌───────────────────┐     parse PromQL → parser::Expr
│    Parse          │
└──────┬────────────┘
       ▼
┌───────────────────┐     AST rewrites: constant fold, label pushdown,
│    Logical Plan   │     CSE
│    + Optimizer    │
└──────┬────────────┘
       ▼
┌───────────────────┐     LogicalPlan → tree of PhysicalOp
│    Physical Plan  │     Bind storage, resolve step grid, compute
│                   │     series schema per op
└──────┬────────────┘
       ▼
┌───────────────────┐     Pull StepBatch streams from the root
│    Executor       │     Each op: next() → Poll<Option<StepBatch>>
└──────┬────────────┘
       ▼
┌───────────────────┐     StepBatch → QueryValue (instant or range)
│    Result Shape   │
└───────────────────┘
```

## Planning Stage

Planning is split into two stages with a hard boundary between them: `Expr → LogicalPlan` is the *
*rewritable** stage, `LogicalPlan → PhysicalPlan` is the compiling stage. Everything that wants to
inspect, modify, or explain a query touches `LogicalPlan`; once we cross into physical, the tree is
opaque and only the executor talks to it.

We introduce a dedicated logical plan instead of reusing the AST module we depend on because it
allows us to attach information such as the query’s timestamps and reinterpret concepts in more
structure form. The conversion is pretty minor, though it also enables us to power `EXPLAIN`
queries.

The physical plan is a tree of `Operators` that are each individually testable and have the same
input/output types (defined below).

## Physical Plan Execution

### Core Data Model

The universal on-the-wire shape between operators is a `StepBatch`: a contiguous range of output
steps for a contiguous chunk of series, with data stored in columnar representation. This data model
gives us two major benefits:

1. It is columnar with a step-major, series-minor format which allows us to quickly compute
   aggregations within a single time-step.
2. It is chunked, allowing us to pipeline and stripe execution across both dimensions (time and
   labels).

```
StepBatch  (step_range = 2..6, series_range = 1..4)
──────────────────────────────────────────────────────────────────

step_timestamps: Arc<[i64]>      series: Arc<SeriesSchema>
┌──────┐                         ┌────────────────┐
│ t₀   │                         │ 0: {job=api}   │
│ t₁   │                         ├────────────────┤
├──────┤ ◄─ step_range.start=2   │ 1: {job=web}   │ ◄─ series_range.start=1
│ t₂   │                         │ 2: {job=db}    │
│ t₃   │                         │ 3: {job=cache} │
│ t₄   │                         ├────────────────┤
│ t₅   │                         │ 4: {job=edge}  │ ◄─ series_range.end=4
├──────┤ ◄─ step_range.end=6     │ 5: {job=lb}    │
│ t₆   │                         └────────────────┘
│ t₇   │                         (fixed at plan time)
└──────┘

           web         db          cache
         ┌──────────┬──────────┬──────────┐
    t₂   │ 12.3  ✓  │  —    ✗  │  7.1  ✓  │
         ├──────────┼──────────┼──────────┤
    t₃   │ 12.9  ✓  │ 44.0  ✓  │  7.4  ✓  │
         ├──────────┼──────────┼──────────┤
    t₄   │ 13.1  ✓  │ 45.2  ✓  │  —    ✗  │
         ├──────────┼──────────┼──────────┤
    t₅   │ 13.4  ✓  │ 46.0  ✓  │  7.8  ✓  │
         └──────────┴──────────┴──────────┘
```

Each batch is a rectangle of `K series × N steps`. Default `N ≈ 64`, `K ≈ 512` giving a ~256 KB
working set that stays in L2. Both step-wise ops (`sum by` across series at one step) and time-wise
ops (`rate` across steps of one series) have cache-friendly traversal. Operators that need the other
axis insert a `Rechunk` breaker.

Here is the actual struct. Note that we us validity bitsets to distinguish between 0/null values.

```rust
struct StepBatch {
    // Time axis — shared by all series in the batch. One allocation, reused
    // across many batches within a query.
    step_timestamps: Arc<[i64]>,          // length = step_count
    step_range: Range<usize>,             // slice into step_timestamps

    // Series axis — schema is fixed at plan time.
    series: Arc<SeriesSchema>,            // labels, fingerprint, stable index
    series_range: Range<usize>,           // slice into series schema

    // Value columns — SoA, float-only in v1.
    values: Vec<f64>,                     // step_count × series_count, row-major by step
    validity: BitSet,                     // same shape; absent = no sample in lookback window
}
```

### Data Source / Storage API

The interaction between the query execution pipeline and storage happens over the `SeriesSource`
trait with two methods:

- `resolve(selector, time_range)` to handle metadata lookups and resolve series info
- `samples(SamplesRequest)` to retrieve the raw samples

The storage is unaware of PromQL concepts such as lookback deltas, offsets, step alignment, etc… all
of that is handled by the leaf operators (`VectorSelector` and `MatrixSelector`).

Storage is accessed both during planning and execution. In the planning phase, `build_physical_plan`
walks the logical tree and, for each selector, calls `resolve(..)` and drains the stream into the
plan-time series roster (`Arc<SeriesSchema>`). This is the *only* place series identity is resolved;
after this, operators index series by `u32` slot. `resolve` runs eagerly and synchronously with
regards to physical-plan construction which means the planning time includes metadata latency.

The execution `VectorSelector` and `MatrixSelector` operators call `samples(..)` with the
pre-resolved series list and the absolute time window they need. Batches come back streamed and the
leaf reshapes them into `StepBatch`es that the rest of the tree polls. No other operator
communicates with the storage layer.

### Concurrency & Parallelism

There are a few areas where we can introduce parallelism and concurrency:

**Inside the storage fetching.** Selectors typically span multiple storage buckets and since buckets
are disjoint keyspaces, fan-out is safe). The implementation uses a buffered stream with max
concurrency 32 for both metadata and sample streams. These constants mirror v1's
`METADATA_STAGE_READAHEAD` / `SAMPLE_STAGE_READAHEAD` so observed concurrency is comparable.

**Pipelining operator execution.** A selector leaf produces I/O-bound batches but downstream ops are
CPU-bound. `ConcurrentOp` decouples them: the child runs on a spawned tokio task and pushes into a
bounded mpsc channel (default bound = 4), giving implicit back-pressure since if the consumer
stalls, the child blocks on `send`. The physical planner inserts one wrapper per selector leaf whose
resolved series count is ≥ 64.

**Vertical sharding.** We defer using the `Coalesce` operator in the initial implementation but the
wiring is stubbed in `parallelism` and `coalesce_max_shards` defaults to `0`. Needs end-to-end
correctness work (per-series independence above the leaf isn't free) before it's turned on.

**No global permit layer.** v1 had a separate `QueryReaderEvalCache` metadata/sample semaphore
throttling real I/O independent of scheduler readahead. v2 collapses that: the cross-bucket
constants above are both scheduler and I/O ceiling. If we later find storage backends that need hard
global throttling, it goes inside the `SeriesSource` implementation, not in the engine.

### Operators

Each operator is a `trait Operator` that pulls from children when they are ready for their next
batch of work (pulling is a blocking operation). Some operators are breaking, which means that any
child operators must complete fully before they complete. Most queries will not have any breaking
operators.

| **Operator**     | **Role**                                                                  | **Breaking** |
|------------------|---------------------------------------------------------------------------|--------------|
| `VectorSelector` | Leaf. Opens storage cursor; one sample per series per step via lookback.  | No           |
| `MatrixSelector` | Leaf. Sliding lookback window per step for rollup functions.              | No           |
| `Rollup`         | Unified range-function driver (rate, increase, `*_over_time`, ...).       | No           |
| `InstantFn`      | Pointwise scalar functions (abs, ln, clamp, histogram_quantile, ...).     | No           |
| `Binary`         | Vector/vector or vector/scalar binop. Pre-computed series matching.       | No           |
| `Aggregate`      | sum/avg/min/max/count/stddev/topk/bottomk/quantile by labels.             | partial      |
| `Subquery`       | Re-grids child onto inner step; feeds outer MatrixSelector semantics.     | Yes          |
| `Rechunk`        | Transposes series-major ↔ step-major when ops need the other axis.        | Yes          |
| `CountValues`    | Data-dependent schema. Drains child, emits with runtime-derived labelset. | Yes          |
| `Concurrent`     | Producer/consumer decoupling with a bounded mpsc channel.                 | No           |
| `Coalesce`       | Fan-in: merges parallel child streams that share schema.                  | No           |

`Aggregate` with `topk`/`bottomk`/`quantile` buffers a whole step before emitting; `sum`/`avg`/etc.
are streaming. Group maps and series matching are computed at plan time and reused for every batch
since they are invariant across steps.

### Caching & Per-Query State

All caching in v2 is intra-query. Each query builds its own state during Plan, consults it during
Execution, and drops it on completion. This keeps the memory story trivial (everything rolls up to
the query's `MemoryReservation`) and sidesteps the cross-query contention problem.

There are two categories of per-query state with different concurrency contracts:

**Frozen State:** The`build_physical_plan`, handed to operators as `Arc<…>`, never mutated during
execution. This has the series roster, which is the resolved `SeriesSchema` produced by draining
`SeriesSource::resolve(..)` streams. Indexed by `series_idx: u32`; labels, fingerprint, and bucket
membership baked in. Operators use the index for dense-array state (group maps, binary-match tables)
compiled once from the roster at plan time.

**Index Cache:** This is a concurrent cache that lives for the duration of the query. it
deduplicates index lookups within the query because `resolve` fan-out (cross-bucket × per-key, up to
1024 in-flight gets) would otherwise issue the same `(bucket, term)` or `(bucket, series_id)` fetch
multiple times from parallel tasks. It caches inverted and forward index fetches.

### Introspection: EXPLAIN and Trace

Every query endpoint accepts two opt-in flags that surface the planner and executor internals without
changing the result shape.

**`?explain=true`** short-circuits evaluation and returns a three-stage dry run: unoptimised logical,
optimised logical, and a pure description of the physical tree the planner would build. No operators
are instantiated and no storage I/O is issued. Add `?pretty=true` to get a DataFusion-style indented
text tree instead of the JSON `ExplainResult`.

**`?trace=true`** runs the query normally and attaches a structured trace to the response. The
physical planner inserts a transparent `TracingOperator` around each concrete operator; per-op poll
time is recorded without the operator knowing. Storage I/O is bucketed separately (`SamplesFetch`,
`Deserialize`, `ForwardIndexFetch`, inverted-index fetch) and attributed to the currently-polling
operator via a thread-local node id. The same spans are emitted to `tracing` whenever `RUST_LOG`
allows, so out-of-band log capture works without the flag.

```
GET /api/v1/query?query=sum(rate(http_requests_total[5m]))&explain=true&pretty=true
GET /api/v1/query_range?query=...&start=...&end=...&step=15s&trace=true
```

## Testing Strategy

Tests are layered to match the architecture: each operator is verified in isolation, the planner is
pinned with snapshots, and full-pipeline behaviour is exercised by scenarios and HTTP e2e tests.

| Layer            | Location                          | What it covers                                                         |
|------------------|-----------------------------------|------------------------------------------------------------------------|
| Operator unit    | `operators/*.rs` `#[cfg(test)]`   | One operator vs. mock child / source; state edges, validity, memory.   |
| Planner golden   | `plan/explain.rs`                 | Snapshots logical → optimised → physical trees for canonical queries.  |
| Stress scenario  | `testing/columnar_stress.rs`      | Multi-bucket synthetic load (~1k–18k series) vs. precomputed `Oracle`. |
| HTTP end-to-end  | `tests/http_server.rs`            | Drives `/api/v1/query{,_range}` through the full stack.                |
| Microbenchmarks  | `benches/v2_engine_micorbench.rs` | Criterion benches on a `WarmRangeQueryHarness`.                        |
| Manual Testing   | `N/A`                             | Ran queries with ?explain=true and ?trace=true                         |

**Known gap.** The `rate()` portion of the range-query stress scenario has a ~1.5× inflation at
bucket boundaries, pointing at cross-bucket counter handling. Instant probes and the count/sum
portions of the same scenario pass. Tracked for a dedicated fix pass rather than blocking this RFC.

## Alternatives Considered

1. Use `Arrow` for the on-the-wire protocol. This was tempting since it would implement some SIMD
   vectorized computations for the aggregations but the dependency is significant and adds real
   compile-time dependency scope for a relatively simple usage of it.
2. Use `DataFusion` for the query planning. There are too many PromQL-specific concepts (lookback
   deltas, offsets, step-aligned rollups) that don’t clearly map to Data Fusion, so the integration
   cost and dependency weight make it less tempting.
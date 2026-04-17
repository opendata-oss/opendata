# RFC 0007: PromQL Execution Engine — Requirements & Architecture

**Status**: Draft
**Authors**: Almog Gavra <almog@responsive.dev>

## Summary

Rewrite the PromQL evaluation path — from HTTP entry through storage access — as a **columnar,
step-major, pull-based operator pipeline**. The current implementation (`promql/evaluator.rs`,
`promql/pipeline.rs`) is a row-oriented AST interpreter that re-walks the tree per step. The
prototype on `full_hot_opt_prototype` demonstrates a 2x-shape fast path that pre-materializes
samples into `StepBlock`s; it is correct in principle but grafted onto the old path, resulting in
duplicate entry points, hardcoded aggregation ops, and no fallback design. This RFC defines the
contracts and shape of a single, unified engine that replaces both.

## Motivation

Concrete problems in the current code (`main`):

1. **Per-step AST re-evaluation.** `tsdb.rs:317` loops `while current_time <= end` and re-runs
   `evaluator.evaluate()` per step. Selector pipeline repeats; label hashing, metadata lookups,
   and per-series iteration all amortize poorly.
2. **Row-oriented data.** `EvalSample { timestamp, value, labels: HashMap<String,String> }` carries
   a heap-allocated label map per sample. Binary ops and aggregations allocate per step.
3. **Unclear pipeline boundary.** Selector caching lives in `evaluator.rs::SelectorCacheKey`,
   orchestration in `pipeline.rs::resolve_selector_candidates`. Fingerprint computation duplicated.
4. **Hot-path prototype is a special case.** `detect_fast_path_shape` matches exactly
   `VectorSelector` or `Aggregate(Sum|Avg|Min|Max|Count)(VectorSelector)`. Everything else falls
   through to the old path. No shared abstractions.
5. **No physical plan.** There is nowhere to hook optimizations (projection pushdown into storage,
   common-subexpression elimination, step-batch coalescing) or distributed rewrite.
6. **Concurrency is implicit.** `metadata_semaphore`/`sample_semaphore` serialize I/O inside a
   query but there is no per-operator parallelism.

The rewrite should make the fast-path behavior the *only* path, and extend it uniformly to every
PromQL construct.

## Goals

- One execution model for instant and range queries, all expression types.
- Columnar, step-batched data flow from storage to result.
- Explicit physical plan with a small, testable operator set.
- Pluggable storage contract that can push down selector, time range, and step hints.
- Per-operator async streaming so a single query can saturate CPU and I/O.
- Memory-accounted; large queries degrade gracefully rather than OOM.

## Non-Goals

- Distributed execution across nodes (design the contract so it's addable later; don't build it).
- Cost-based optimization (rule-based only for now).
- PromQL language extensions.
- Reworking the storage layer itself (SlateDB, inverted/forward indexes, Gorilla decoding).
- **Native histograms.** v1 handles floats only. Mixed float/histogram series and histogram
  aggregation are out of scope and would extend the batch ABI in a follow-up RFC.
- Writer-side metadata propagation or cache warming (warmer was removed in the prototype; revisit
  separately).

## Prior Art

**VictoriaMetrics** — flat step-major columnar. `timeseries{Values[N], Timestamps[N]}` with one
shared timestamp slice per scope. A unified rollup abstraction collapses every windowed function
(`rate`, `*_over_time`, ...) into one two-pointer driver. AST-level optimizer, no plan IR.

**Prometheus stock** — row-major, per-step AST walk. Fully materializes selectors before stepping.
Single-goroutine per query. The anti-example.

**thanos-io/promql-engine** — pull-based Volcano operators over "step vectors". `Series()` is
pre-computed per operator so aggregate state is dense-array indexed, not map-keyed. Separate
exchange operators (`coalesce`, `concurrent`, `remote_exec`).

**DataFusion / DuckDB** — pull-based async streams over typed batches; schema fixed at plan time.
Ref-counted Arrow buffers for zero-copy slicing. Explicit pipeline-breaker vs pipelineable
operator distinction.

**Biggest borrows**: VM's rollup abstraction, thanos-io's pre-computed schema + pull-based
operators, DataFusion's operator-trait shape. **Biggest avoid**: Prometheus' step-major loop
around a recursive evaluator; monolithic `eval.go`-style match tables.

## Architecture Overview

```
HTTP / embedded API
        │
        ▼
┌───────────────────┐     parse PromQL → parser::Expr
│    Parse          │
└──────┬────────────┘
       ▼
┌───────────────────┐     AST rewrites: constant fold, label pushdown,
│    Logical Plan   │     CSE, distributed markers (future)
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

Every stage is a separate module with a crisp contract. Today all of this is tangled inside
`evaluator.rs` + `pipeline.rs`.

## Core Data Model

The universal on-the-wire shape between operators is a **StepBatch**: a contiguous range of
output steps for a contiguous chunk of series, columnar.

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

Histograms are deliberately out of scope for v1 (see Non-Goals). When added, this struct
becomes an enum with a `Float`/`Histogram`/`Mixed` discriminant, or the column layout splits
into parallel float and histogram arrays mirroring Prometheus' `FPoint`/`HPoint` split.

**Shape choice.** Each batch is a rectangle of `K series × N steps`. Default `N ≈ 64`,
`K ≈ 512` giving a ~256 KB working set that stays in L2. Both step-wise ops (`sum by` across
series at one step) and time-wise ops (`rate` across steps of one series) have cache-friendly
traversal. Operators that need the other axis insert a `Rechunk` breaker.

**Series schema is plan-time, with one named exception.** Each operator exposes
`fn series(&self) -> SchemaRef` before `next()` is called. `SchemaRef` is
`Static(Arc<SeriesSchema>)` for every operator except `count_values`, whose output labelset is
derived from runtime sample values (see `promqltest/testdata/aggregators.test:480`).
`count_values` returns `Deferred` — a pipeline-breaker that drains its child, discovers the
output labelset, then emits with a fixed schema for the rest of execution. Downstream operators
treat `Deferred` as "schema known after my child's first batch." This is the only PromQL
construct with value-dependent output labels; `label_replace`/`label_join` are deterministic
from input labels and do not need this mechanism.

This unlocks dense-array aggregation state keyed by `series_idx: u32` rather than
`HashMap<Labels, State>`. Group maps for `sum by (label)` are computed once in the physical plan.

**Timestamps shared.** `step_timestamps` is one `Arc<[i64]>` per query, pointer-copied into every
batch. Mirrors VictoriaMetrics' design; eliminates per-batch timestamp allocation.

**Instant queries are a range of one step.** No separate code path; the executor treats instant as
`steps.len() == 1`. Removes the `InstantVector` vs `RangeVector` duality that bleeds through the
current code.

## Operator Taxonomy

Small, fixed set. Each operator is a `trait Operator` that pulls from children.

| Operator | Role | Breaking |
|---|---|---|
| `VectorSelector` | Leaf. Opens storage cursor; one sample per series per step via lookback. | No |
| `MatrixSelector` | Leaf. Sliding lookback window per step for rollup functions. | No |
| `Rollup` | Unified range-function driver (rate, increase, `*_over_time`, ...). | No |
| `InstantFn` | Pointwise scalar functions (abs, ln, clamp, histogram_quantile, ...). | No |
| `Binary` | Vector/vector or vector/scalar binop. Pre-computed series matching. | No |
| `Aggregate` | sum/avg/min/max/count/stddev/topk/bottomk/quantile by labels. | partial |
| `Subquery` | Re-grids child onto inner step; feeds outer MatrixSelector semantics. | Yes |
| `Rechunk` | Transposes series-major ↔ step-major when ops need the other axis. | Yes |
| `CountValues` | Data-dependent schema. Drains child, emits with runtime-derived labelset. | Yes |
| `Concurrent` | Producer/consumer decoupling with a bounded mpsc channel. | No |
| `Coalesce` | Fan-in: merges parallel child streams that share schema. | No |

`Aggregate` with `topk`/`bottomk`/`quantile` buffers a whole step before emitting; `sum`/`avg`/etc.
are streaming. Group maps and series matching are computed at plan time and reused for every
batch — both are invariant across steps.

Explicitly *not* operators: HTTP marshaling, caching (handled at query-entry boundary),
result shape (`QueryValue`) construction.

## Storage Contract

Replaces the current `QueryReader` trait. **Storage is PromQL-unaware** — it resolves selectors
to series metadata and streams raw samples over an absolute time range. Lookback delta, `@`,
`offset`, step alignment, and rollup semantics all live in the operators (`VectorSelector`,
`MatrixSelector`, `Rollup`). A previous draft pushed step-grid and `func` hints into the source;
that was a soundness hole — `@ start()`/`@ end()` inside subqueries and range queries
(see current special-case handling at `evaluator.rs:5988`) would require re-implementing the
evaluator inside every storage backend. The source's job is bytes → samples.

```rust
trait SeriesSource {
    // Resolve selector → series metadata. Streamed per bucket.
    fn resolve(&self, selector: &VectorSelector, time_range: TimeRange)
        -> impl Stream<Item = Result<ResolvedSeriesChunk>>;

    // Stream raw samples for a resolved series set over an absolute time
    // range. No step, no lookback, no rollup. One series id → dense column.
    fn samples(&self, hint: SamplesRequest) -> impl Stream<Item = Result<SampleBatch>>;
}

struct SamplesRequest {
    series: Arc<[ResolvedSeriesRef]>,
    time_range: TimeRange,                // inclusive-exclusive, absolute ms
    // No step, lookback, @, offset, or func. Those are executor concerns.
}
```

**Contract (caller → source):**
- `series` is the complete set after selector resolution; source does not widen.
- `time_range` is the absolute ms window the caller needs; it already accounts for the maximum
  lookback / offset / subquery shift applied by the downstream operator.

**Contract (source → caller):**
- Output batches carry a `series_range` covering a contiguous slice of the input series.
- Samples are returned in timestamp order per series, with exact timestamps (no re-alignment).
- Stale markers are preserved as `STALE_NAN`; the caller distinguishes them.

**Optional pushdown, v2+.** Once v1 is measured, we may introduce a narrowly-scoped second
method like `samples_rollup(hint, RollupKind, StepGrid)` for cases where the backend can
compute rate/increase/avg_over_time cheaper than the executor (e.g., pre-aggregated downsamples).
This must never replace the raw `samples()` path — it is a best-effort optimization the executor
can fall back from.

The existing `BucketQueryReader` stays as an internal implementation detail of a `SeriesSource`
that bridges to the per-bucket storage model. Cross-bucket stitching happens inside the source,
not in the executor.

## Execution Model

**Pull-based async streams.** Each operator implements:

```rust
trait Operator: Send {
    fn schema(&self) -> &OperatorSchema;   // series + step grid, known before execution
    fn next(&mut self, cx: &mut Context) -> Poll<Option<Result<StepBatch>>>;
}
```

**Single query drives the tree.** The executor pulls from root; inner ops pull from children.
Back-pressure is implicit (downstream stops pulling). For I/O-bound leaves, the optimizer inserts
`Concurrent` wrappers that spawn a task and return batches via a bounded channel.

**Parallelism.** Per-query parallelism comes from two sources:
- `Concurrent` operators — decouple async I/O from evaluation.
- `Coalesce` — run `N` copies of a subplan in parallel over disjoint series chunks (vertical
  sharding within a single query), merging their outputs.

The physical planner decides where to insert these based on series cardinality estimates. No
implicit spawn-per-series; work is explicit.

**Memory accounting covers all per-query state**, not just `StepBatch` allocations. Every
structure whose size scales with series cardinality or step count must reserve before
allocating:

- Series roster (`Arc<SeriesSchema>`) and per-series sample blocks built during Plan.
- Group maps and binary-match tables computed at plan time from series labels.
- Pipeline-breaker buffers (topk, quantile, `Rechunk`, `CountValues`).
- Per-batch value/validity columns (the original scope).

**Reservation enforcement.** All allocating calls go through
`MemoryReservation::try_grow(bytes)`, returning `QueryError::MemoryLimit` on exceed. No
spilling in v1.

## Caching & Per-Query State

Two layers with different concurrency contracts. Do not reuse `QueryReaderEvalCache` as-is —
its `DashMap`-per-axis design pays for cross-query contention it never sees (cache is
re-allocated per query at `tsdb.rs:403,435,482`) and forces lock-striped maps on read paths
where ownership can be explicit.

**1. Per-query state — striped by plan, not by lock.**
Built during the Plan phase, frozen before Execution. No concurrent maps in the hot path.

- **Series roster**: `Arc<SeriesSchema>` indexed by `series_idx: u32`. Contains labels,
  fingerprint, bucket membership. Populated once; read-only thereafter.
- **Sample blocks**: `Arc<[Arc<Vec<Sample>>]>` indexed by `series_idx` (with per-bucket inner
  dim as needed). Loaded during Plan via fan-out across disjoint `(bucket, series_id)` keys —
  writers never collide, so a transient `Vec<(SeriesKey, OnceCell<...>)>` or a collector
  pattern suffices; no `DashMap` required.
- **Execution ownership**: when the planner inserts a `Coalesce` with K children, each child
  receives a disjoint `series_range`. Children never read each other's slices. `Concurrent`
  likewise owns a single child's output channel.

This is the morsel-ownership pattern (DuckDB, DataFusion): work partitions map 1:1 to state
partitions, making locks unnecessary in steady state.

**2. Cross-query cache — long-lived, concurrent, keyed on canonical plan.**
Deferred to v2 but the contract is established now so v1 types don't have to change:

- **Metadata cache**: resolved series sets, forward/inverted index lookups. Shared across
  queries, keyed by `(bucket, CanonicalSelector)`. This is the *only* layer that justifies
  `DashMap` (or a sharded `RwLock<HashMap>`): write contention is real because two queries
  can miss the same key simultaneously. Use `moka`-style async single-flight to avoid
  duplicate backing reads on a miss.
- **Rollup result cache**: post-rollup `StepBatch`es keyed by `(plan_subtree_hash,
  step_range, series_range)`. Enables suffix-stitching for Grafana-style repeat queries.
  Single-flight on miss. Same concurrency story as metadata cache.

`SeriesSource` exposes both caches via a single handle; the Plan phase consults them and
writes results into the per-query frozen state. Execution sees only `Arc`s.

No "hot path fast path" flag. Single plan, single execution model, two cache tiers with
explicit concurrency semantics.

## Concurrency Model

- **Per query**: one async task drives the root. Operators run inline on that task unless wrapped
  in `Concurrent`.
- **Per `Concurrent`**: one spawned tokio task per wrapped subplan, bounded mpsc channel to
  downstream.
- **Global**: existing `-search.max_concurrent_requests` equivalent (not yet present) caps total
  in-flight queries; metadata/sample concurrency limits continue to live inside `SeriesSource`.

## Error Handling & Observability

- **Errors**: structured `QueryError` propagated through the stream; first error short-circuits.
  No panics on bad user input.
- **Annotations**: PromQL non-fatal warnings ride alongside batches in a `Annotations` sidecar
  (same shape as Prometheus).
- **Tracing**: every operator has a `tracing::Span`. One `INFO` event per query with plan
  fingerprint, series cardinality, step count, wall time, memory high-water mark.
- **Metrics**: per-operator latency, rows-produced, memory-reserved, cache hit rate.

## Migration Strategy

- Delete `evaluator.rs` and `pipeline.rs` wholesale at the end. No compatibility layer, no
  `if hot_path` branch.
- Keep `selector.rs` (inverted-index matcher logic) as-is; it is reusable.
- Retain wire types (`QueryValue`, `InstantSample`, `RangeSample`, `Labels`) so the HTTP/embedded
  boundary is unchanged.
- Do **not** reuse `QueryReaderEvalCache`. Its per-query `DashMap`s solve contention that does
  not exist inside a single query. Plan-time ownership (above) replaces it for per-query state;
  the deferred cross-query cache is a separate design.
- `promqltest` golden tests (`promql/promqltest/`) are the correctness oracle — the new engine
  must pass them before the old one is deleted.

## Alternatives Considered

- **Extend the prototype's fast path to more query shapes.** Rejected: fast-path detection becomes
  a growing match table. Same end state as VictoriaMetrics' monolithic `eval.go`.
- **Keep AST interpreter, add step batching only at selector leaves.** Rejected: intermediate
  types stay row-major; binary/aggregate operators remain the bottleneck.
- **Adopt thanos-io/promql-engine's design verbatim (port to Rust).** Rejected: their step-vector
  shape is one timestamp wide; we want batched step ranges to amortize async overhead and fit
  Rust's `Stream` polling model. We also have tighter control over the storage contract than
  they do (they target an external `Querier`).
- **Use DataFusion as the engine, translate PromQL to DataFusion logical plan.** Rejected:
  PromQL-specific semantics (lookback delta, stale markers, `@`/`offset`, step-alignment of
  rollups) don't cleanly embed. Integration cost outweighs reuse.

## Open Questions

1. **Batch shape defaults.** `N=64 × K=512` is a guess anchored in L2 sizing; needs measurement
   across representative workloads (high cardinality vs wide time range).
2. **Subquery scoping.** Subqueries shift the step grid for their inner plan. Does `Subquery`
   produce its own `MemoryReservation` scope (independent budget per inner plan), or share the
   parent's?
3. **Native histograms.** Deliberately out of v1 scope. `ColumnBlock::Float` only. Histogram
   support (including mixed float/histogram series) is a separate RFC that extends the batch ABI.
4. **Cache key canonicalization.** What fingerprint for a plan subtree is cheap and stable across
   logically-equivalent expressions (e.g., `a+b` vs `b+a` under commutative ops)?
5. **Spilling vs rejecting large queries.** v1 rejects on memory limit; is spill worth the
   complexity in v2?
6. **Writer-side queryability.** Today `Tsdb` and `TimeSeriesDbReader` both implement
   `TsdbReadEngine`. Does the new engine need both, or can we unify them behind
   `SeriesSource`?

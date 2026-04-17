# RFC 0007 Implementation Plan — Orchestration Doc

**Primary spec**: [`0007-promql-execution.md`](./0007-promql-execution.md). Read it first. It
is the source of truth for what to build. This document is the source of truth for **how work
is divided, ordered, and handed off between agents**. It is edited as implementation proceeds.

**Invocation model**: one Claude session drives the whole job. The session reads this file,
advances one unit of work (directly or by dispatching a subagent), updates the state board,
and either continues or stops. A fresh session can resume from any mid-state by reading this
file.

---

## 1. Operating Protocol

### 1.1 The loop every agent follows

1. **Read** this entire file before doing anything. The RFC is the spec; this doc is the state.
2. **Read** [§4 State Board](#4-state-board) to find the next unit of work that is `ready`.
3. **Claim** it by changing its row status to `in-progress` and writing your name (or subagent
   type) in the `owner` column. Commit the edit before starting work — next agent must see
   the claim.
4. **Execute** the work within your role's scope ([§3 Roles](#3-roles)). Do not expand scope.
5. **Verify** per the phase's acceptance criteria before declaring done.
6. **Update** the state board: status `done` or `blocked`. Append to
   [§5 Decisions Log](#5-decisions-log) if you made a call that should be recorded.
   Append a dated one-liner to [§6 Activity Log](#6-activity-log).
7. **Handoff**: if `blocked`, state the blocker clearly. If `done`, the orchestrator picks up
   the next `ready` unit.

### 1.2 What counts as "the state board edit"

- Status: `ready` → `in-progress` → `done` or `blocked`.
- When marking `done`, fill in the `artifacts` column with the paths you created/modified.
- When marking `blocked`, fill in the `blocker` column and add a Decisions Log entry.

### 1.3 Escalate to the user (stop and ask) when

- An RFC requirement is ambiguous in a way that affects the contract between phases.
- A test fixture under `timeseries/src/promql/promqltest/` needs to change (these are the
  correctness oracle; do not change them without explicit approval).
- You need to add a dependency to `Cargo.toml`.
- You want to diverge from the RFC's stated design.
- A phase estimated at one sitting has grown into a sprawling diff.

### 1.4 Hard rules for all agents

These are non-negotiable and override any other instruction:

- Never commit. This project's CLAUDE.md says commits only happen when the user asks.
- Never change `promqltest/` fixtures. Failing goldens are a signal, not a problem to edit away.
- After modifying any `.rs` file, run `cargo fmt` (the workspace has a top-level `Cargo.toml`).
- Before declaring a unit `done`, run
  `cargo clippy --all-targets --all-features -- -D warnings` and fix everything.
- Test naming: `should_*` prefix, given/when/then structure inside the test body.
- No new `*.md` files beyond this plan and the RFC, unless a phase explicitly asks for one.
- Work on the branch the user provided. Do not create branches, worktrees, or stashes on your
  own. If isolation is needed, stop and ask.

---

## 2. Core References

| Thing | Location |
|---|---|
| RFC (spec) | `timeseries/rfcs/0007-promql-execution.md` |
| Baseline on `main` (what we're replacing) | `timeseries/src/promql/evaluator.rs`, `pipeline.rs`, `selector.rs` |
| Prior prototype (reference, not code to port) | branch `full_hot_opt_prototype`, commit `e775b16` |
| Correctness oracle | `timeseries/src/promql/promqltest/` goldens |
| Wire types (unchanged) | `timeseries/src/model.rs` (`QueryValue`, `InstantSample`, `RangeSample`, `Labels`) |
| Storage layer (unchanged) | `timeseries/src/{tsdb.rs,reader.rs,query.rs,index.rs,minitsdb.rs}` |
| Per-project agent notes | `timeseries/AGENTS.md` |
| Workspace CLAUDE rules | top-level `CLAUDE.md` (if present) + `~/.claude/CLAUDE.md` (user) |

### Key facts worth internalizing before starting

- Data is bucketed hourly. Bucket-scoped `SeriesId: u32`, cross-bucket `SeriesFingerprint: u128`.
- `QueryReaderEvalCache` is per-query. The RFC rejects reusing it; plan-time ownership replaces
  it (RFC §"Caching & Per-Query State").
- The prototype branch implemented a 2-shape fast path (`VectorSelector` and
  `Aggregate(Sum|Avg|Min|Max|Count)(VectorSelector)`). It is **reference material only** for
  the `StepBlock` / transpose kernel shape. Do not port it; the RFC is the spec.
- Native histograms are explicitly out of scope for v1. Float-only `values: Vec<f64>`.

---

## 3. Roles

Each role has a tight scope. Subagent dispatches in §4 name the role the agent plays.

### 3.1 Architect (orchestrator)

The role the main session plays by default. Reads the state, advances it, dispatches
implementors, reviews handoffs, escalates.

**Does**: read state, plan the next unit, pick the implementor, spawn subagents with a
task-scoped prompt, integrate results into the state board, answer cross-phase design
questions that stay within the RFC, escalate when RFC is ambiguous.

**Does not**: write production Rust outside trivial wiring. Skip review of implementor output.
Silently edit the RFC — if the RFC needs an amendment, escalate to the user first.

### 3.2 Foundation Implementor

**Scope**: phase 1 only. Creates the new module scaffolding and core types (`StepBatch`,
`SeriesSchema`, `MemoryReservation`, `Operator` trait). No operator bodies, no wiring.

**Does**: define types and traits per RFC §"Core Data Model" and §"Execution Model". Write
unit tests for any non-trivial helpers. Get the module compiling inside the existing crate
behind a feature flag `promql-v2` (off by default) so nothing on `main` breaks.

**Does not**: implement operators, touch `tsdb.rs`, or delete old code.

### 3.3 Storage Implementor

**Scope**: phase 2. Implements `SeriesSource` (RFC §"Storage Contract") as a thin adapter
over the existing `QueryReader` trait. No PromQL semantics — pure range+series fetch and
cardinality estimate.

**Does**: define `SeriesSource`, `SampleHint`, `ResolvedSeriesChunk`, `CardinalityEstimate`;
implement one adapter over the current `BucketQueryReader` fan-out; write integration tests
that exercise the contract guarantees from the RFC.

**Does not**: add step/lookback/`@`/offset handling to the source. That belongs in operators.
The RFC explicitly rejected pushdown for v1.

### 3.4 Operator Implementor (split across phases 3a, 3b, 3c)

**Scope**: one operator group per phase. Each group is one sitting.

- **3a leaves**: `VectorSelector`, `MatrixSelector`. Owns lookback, `@`, `offset` application.
- **3b stateless middle**: `Rollup`, `InstantFn`, `Binary`, `Aggregate` (streaming path:
  sum/avg/min/max/count/stddev/stdvar/group).
- **3c breakers & exchange**: `Aggregate` (topk/bottomk/quantile), `Subquery`, `Rechunk`,
  `CountValues`, `Concurrent`, `Coalesce`.

**Does**: implement the operator behind the v2 feature flag. Pass the RFC's schema and
memory-accounting contracts. Unit-test each operator against hand-built `StepBatch` inputs.

**Does not**: touch the planner, touch `tsdb.rs`, or begin golden tests (those run in phase 6).

### 3.5 Planner Implementor

**Scope**: phase 4. Lowering from `promql_parser::Expr` to a physical operator tree.

**Does**: implement the logical-plan representation, the (minimal, rule-based) optimizer
pass, and the physical planner that binds `SeriesSource`, pre-computes series schemas, builds
group maps and binary-match tables, and inserts `Concurrent`/`Coalesce` where heuristics say
so. Plan-time cardinality gate goes here (RFC §"Memory accounting").

**Does not**: implement operators, change wire types, or touch existing HTTP wiring yet.

### 3.6 Wiring Implementor

**Scope**: phase 5. Connects the new engine to `TimeSeriesDb` behind the v2 feature flag.

**Does**: create a parallel entry point in `tsdb.rs` that uses the new engine when the flag is
on; keep the old path on by default. This is the A/B boundary.

**Does not**: delete the old engine. That is phase 7.

### 3.7 Tester

**Scope**: phase 6. Runs the full `promqltest` corpus against the new engine and triages.

**Does**: enable the flag, run tests, classify failures (spec bug, operator bug, planner bug),
file each as an entry in the Decisions Log with reproducer, and hand back to the Architect
for dispatch. May propose unit tests to isolate specific failures.

**Does not**: edit fixtures. Fix production bugs (hands those back to the relevant Implementor).

### 3.8 Migration Implementor

**Scope**: phase 7. Only runs after phase 6 reports the full corpus green.

**Does**: flip the v2 flag to default-on (or remove the flag), delete `evaluator.rs` and
`pipeline.rs`, remove now-dead types, re-run full test suite + clippy.

**Does not**: change wire types, storage layer, or the `promqltest` runner.

---

## 4. State Board

Legend: `ready` (unblocked, pickup allowed) · `in-progress` (claimed, owner set) ·
`done` · `blocked` (see `blocker` column) · `deferred` (deliberately skipped).

Phases run in order. Within a phase, units may be parallelizable — note in `deps`.

### Phase 1 — Foundation

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 1.1 | Add `promql-v2` feature flag to `timeseries/Cargo.toml` | Foundation Implementor | done | — | `timeseries/Cargo.toml` | |
| 1.2 | Create `timeseries/src/promql/v2/mod.rs` scaffolding | Foundation Implementor | done | 1.1 | `timeseries/src/promql/mod.rs`, `timeseries/src/promql/v2/{mod,batch,memory,operator}.rs` | |
| 1.3 | Implement `StepBatch`, `SeriesSchema`, `SchemaRef` (Static/Deferred) | Foundation Implementor | done | 1.2 | `timeseries/src/promql/v2/batch.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 1.4 | Implement `MemoryReservation` with `try_grow` + `QueryError::MemoryLimit` | Foundation Implementor | done | 1.2 | `timeseries/src/promql/v2/memory.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 1.5 | Define `Operator` trait; stub `schema()` + `next()` shape | Foundation Implementor | done | 1.3, 1.4 | `timeseries/src/promql/v2/operator.rs`, `timeseries/src/promql/v2/mod.rs` | |

**Acceptance**: crate compiles with and without `--features promql-v2`; `cargo clippy
--all-targets --all-features -- -D warnings` passes; unit tests on `MemoryReservation` and
`StepBatch` helpers pass.

### Phase 2 — Storage Contract

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 2.1 | Define `SeriesSource` trait + `SampleHint` + `CardinalityEstimate` | Storage Implementor | done | 1.5 | `timeseries/src/promql/v2/source.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 2.2 | Adapter impl over existing `QueryReader` (cross-bucket stitching inside) | Storage Implementor | done | 2.1 | `timeseries/src/promql/v2/source_adapter.rs`, `timeseries/src/promql/v2/mod.rs`, `timeseries/src/promql/v2/memory.rs` | |
| 2.3 | Integration tests: resolve, estimate, sample-stream ordering, stale markers | Storage Implementor | done | 2.2 | `timeseries/src/promql/v2/source_adapter.rs` (new `#[cfg(test)] mod integration_tests`) | |

**Acceptance**: adapter serves the RFC's caller/source contract guarantees; tests cover
cross-bucket series with staleness; no PromQL semantics leaked into the source.

### Phase 3 — Operators

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 3a.1 | `VectorSelector` operator (lookback, `@`, offset) | Operator Implementor | done | 2.2 | `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/operators/vector_selector.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3a.2 | `MatrixSelector` operator (sliding window) | Operator Implementor | done | 2.2 | `timeseries/src/promql/v2/operators/matrix_selector.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3b.1 | `Rollup` unified driver (rate/increase/\*\_over\_time) | Operator Implementor | done | 3a.2 | `timeseries/src/promql/v2/operators/rollup.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3b.2 | `InstantFn` | Operator Implementor | done | 3a.1 | `timeseries/src/promql/v2/operators/instant_fn.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3b.3 | `Binary` (vector/vector, vector/scalar, scalar/scalar) | Operator Implementor | done | 3a.1 | `timeseries/src/promql/v2/operators/binary.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3b.4 | `Aggregate` streaming ops (sum/avg/min/max/count/stddev/stdvar/group) | Operator Implementor | done | 3a.1 | `timeseries/src/promql/v2/operators/aggregate.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3c.1 | `Aggregate` breaker ops (topk/bottomk/quantile) | Operator Implementor | done | 3b.4 | `timeseries/src/promql/v2/operators/aggregate.rs` | |
| 3c.2 | `Subquery` | | ready | 3a.2, 3b.1 | | |
| 3c.3 | `Rechunk` breaker | | ready | 1.5 | | |
| 3c.4 | `CountValues` (deferred-schema operator) | | ready | 3b.4 | | |
| 3c.5 | `Concurrent` and `Coalesce` exchange operators | | ready | 1.5 | | |

**Acceptance per unit**: operator has unit tests built on hand-constructed `StepBatch`es;
respects `MemoryReservation`; schema contract is explicit (Static vs Deferred).

### Phase 4 — Planner

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 4.1 | Logical plan representation + AST → logical lowering | | ready | 3c.* | | |
| 4.2 | Rule-based optimizer (constant fold, selector label pushdown, CSE if cheap) | | ready | 4.1 | | |
| 4.3 | Physical planner: bind `SeriesSource`, resolve series schemas, build group maps | | ready | 4.1 | | |
| 4.4 | Plan-time cardinality gate (`estimate_cardinality` → `QueryError::TooLarge`) | | ready | 4.3 | | |
| 4.5 | Insertion of `Concurrent`/`Coalesce` based on series-count heuristics | | ready | 4.3 | | |

**Acceptance**: small PromQL expressions produce expected plans in unit tests;
cardinality-gate rejects oversized queries before resolve.

### Phase 5 — Wiring

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 5.1 | New `Tsdb::eval_query_v2` / `eval_query_range_v2` entry points | | ready | 4.* | | |
| 5.2 | Dispatch on `promql-v2` feature flag in HTTP handler | | ready | 5.1 | | |
| 5.3 | Result shaping: `StepBatch` → `QueryValue` without copying labels twice | | ready | 5.1 | | |

**Acceptance**: with flag off, everything behaves as on `main`. With flag on, simple queries
return structurally-identical `QueryValue`s.

### Phase 6 — Verification

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 6.1 | Full `promqltest` corpus run against v2 with flag on | | ready | 5.* | | |
| 6.2 | Triage and file each failure class in Decisions Log | | ready | 6.1 | | |
| 6.3 | Re-run until green; no fixture edits | | ready | 6.2 | | |

**Acceptance**: zero failures on the unmodified `promqltest` corpus. Perf profile taken on a
representative range query (e.g., `sum by (pod) (rate(http_requests_total[5m]))` over 6h) —
numbers recorded in Decisions Log.

### Phase 7 — Migration

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 7.1 | Flip `promql-v2` to default-on or remove the flag | | ready | 6.3 | | |
| 7.2 | Delete `evaluator.rs`, `pipeline.rs`; remove dead types | | ready | 7.1 | | |
| 7.3 | `cargo test --all` + clippy green across workspace | | ready | 7.2 | | |

**Acceptance**: old engine gone, full suite green.

---

## 5. Decisions Log

Append-only. Each entry: date, author (role), decision or open question, rationale,
RFC section touched.

- 2026-04-16 — Architect (RFC author) — Native histograms deferred to a follow-up RFC.
  `ColumnBlock` is float-only in v1. See RFC §"Non-Goals".
- 2026-04-16 — Architect — Storage contract is PromQL-unaware. Lookback/`@`/offset/step/func
  pushdown removed from `SampleHint`. See RFC §"Storage Contract".
- 2026-04-16 — Architect — `count_values` handled via `SchemaRef::Deferred`; only named
  exception to plan-time schema invariant. See RFC §"Core Data Model".
- 2026-04-16 — Architect — `QueryReaderEvalCache` not reused. Plan-time ownership replaces
  per-query DashMap-striped caches. Cross-query cache deferred to v2. See RFC
  §"Caching & Per-Query State".
- 2026-04-16 — Foundation Implementor (unit 1.3) — `StepBatch::validity` uses a hand-rolled
  `Vec<u64>`-backed `BitSet` rather than adding a `bitvec` workspace dependency. Surface area
  needed (with_len/all_set/get/set/clear/len/count_ones) is trivial and one unit of scope
  didn't justify a new dep. Revisit if downstream operators need bitwise ops across words.
- 2026-04-16 — Foundation Implementor (unit 1.3) — `SeriesSchema` minimum shape is
  `labels: Arc<[Labels]>` + `fingerprints: Arc<[u128]>`, both dense-indexed by `series_idx`.
  No group maps, binary-match tables, or bucket-membership yet — planner units (4.x) extend.
  `SchemaRef::empty_static()` helper added for tests / initial operator wiring.
- 2026-04-16 — Foundation Implementor (unit 1.4) — `thiserror` is already a workspace
  dependency (`timeseries/Cargo.toml` line 55), used for the existing crate-level
  `QueryError` in `timeseries/src/error.rs`. Used it directly for `v2::QueryError` — no
  hand-rolled `Display`/`Error` needed and no new dep added.
- 2026-04-16 — Foundation Implementor (unit 1.4) — `v2::QueryError` kept isolated from the
  existing `crate::error::QueryError` per plan guidance. The crate-level variant is a flat
  `Execution(String)` shape; v2 needs structured `MemoryLimit { requested, cap,
  already_reserved }` and `TooLarge { estimated_cells, cell_limit }` for diagnostics and
  tracing. Phase 5 wiring will bridge (likely `impl From<v2::QueryError> for
  crate::error::QueryError` mapping structured variants into `Execution`).
- 2026-04-16 — Foundation Implementor (unit 1.4) — `MemoryReservation` uses `AtomicUsize`
  counters behind an internal `Arc<Inner>`. `try_grow` is a `compare_exchange_weak` CAS
  loop; `release` likewise. No `Mutex`/`RwLock` on the hot path — two or more tokio tasks
  behind a `Concurrent`/`Coalesce` can call `try_grow` in parallel, and under contention
  exactly `floor(cap / bytes)` requests succeed. High-water uses a separate atomic updated
  post-commit (acceptable: it's an observability statistic, not a correctness invariant).
  `release` debug-asserts against underflow but saturates in release builds to avoid
  taking down the process on a miscounted operator.
- 2026-04-16 — Foundation Implementor (unit 1.4) — Clones of `MemoryReservation` share
  state via `Arc<Inner>`. Dropping a clone is *not* a release — callers must pair
  `try_grow` with `release`. This matches the RFC's "pipeline-breakers can give back
  memory when a buffer is dropped" contract (the *operator* knows what it allocated).
- 2026-04-16 — Foundation Implementor (unit 1.5) — `Operator` trait uses `Poll` directly
  (no `Future`/`Stream` wrapper). Shape mirrors `futures::Stream::poll_next` —
  `fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>>`
  — so operators compose cleanly under tokio without the trait itself pinning to a
  runtime. `&mut self` (not `Pin<&mut Self>`) — operators are not self-referential;
  DataFusion and thanos-io promql-engine take the same route. Trait is object-safe
  (`Box<dyn Operator>` tested).
- 2026-04-16 — Foundation Implementor (unit 1.5) — `Operator: Send` only, no `Sync`.
  Matches DataFusion and thanos-io promql-engine; single-task-per-subplan is the
  intended concurrency model (`Concurrent` wrappers move ownership, not share).
  Adding `Sync` later is non-breaking for operators that already satisfy it; removing
  it later would be.
- 2026-04-16 — Foundation Implementor (unit 1.5) — `OperatorSchema` chosen over a
  bare `SchemaRef` return from `schema()` for extensibility. Minimum shape:
  `{ series: SchemaRef, step_grid: StepGrid }` where `StepGrid` is
  `{ start_ms, end_ms, step_ms, step_count }`. `step_count` is stored, not derived,
  so the schema is a cheap `&` return. No storage-specific fields here (phase 2
  defines `SeriesSource`); no group maps / binary-match tables yet (planner units
  4.x extend). Error type on `next()` is `v2::QueryError` (not crate-wide `QueryError`)
  — decision already recorded in the 1.4 entry; reaffirmed here.
- 2026-04-16 — Storage Implementor (unit 2.1) — `SeriesSource` methods use
  return-position `impl Future` / `impl Stream` (RPITIT) rather than
  `async-trait` or `Pin<Box<dyn _>>`. Rust 1.95 / edition 2024 support this
  natively, and it lets concrete adapters avoid boxing on the hot `samples()`
  path. `async-trait` is in the workspace but would force allocation per
  method call for no benefit.
- 2026-04-16 — Storage Implementor (unit 2.1) — `SeriesSource` is **not**
  object-safe (consequence of `impl Stream` / `impl Future` returns). The
  planner in Phase 4 will bind a generic `S: SeriesSource` (or an
  `Arc<S>`); operators that need storage take a generic parameter, not a
  `Box<dyn SeriesSource>`. No blanket dispatch layer today — if a future
  caller needs dynamic dispatch we can add a `dyn`-friendly wrapper that
  boxes the streams, but v1 does not require it.
- 2026-04-16 — Storage Implementor (unit 2.1) — `VectorSelector` is re-used
  directly from `promql_parser::parser::VectorSelector` — the same path
  `evaluator.rs`, `pipeline.rs`, and `selector.rs` already import. No
  crate-local wrapper exists and the Phase 4 planner will lower from
  `promql_parser::Expr`, so matching the parser's type avoids a conversion
  layer.
- 2026-04-16 — Storage Implementor (unit 2.1) — `ResolvedSeriesChunk` is a
  per-bucket unit: each chunk carries a single `bucket_id` and the labels /
  handles for series in that bucket. This matches the RFC's "streamed per
  bucket" wording and lets the Phase 2.2 adapter emit one chunk per
  bucket-scoped fan-out without re-grouping. No ordering guarantee **across**
  chunks — the RFC does not specify selector-input ordering for `resolve()`
  (the planner already has to merge across buckets); 2.2 may choose whatever
  ordering the backing store emits naturally.
- 2026-04-16 — Storage Implementor (unit 2.1) — `SampleBatch` uses an
  explicit `series_range` + columnar `SampleBlock` (per-series
  `Vec<i64>` timestamps + `Vec<f64>` values). Preferred over a flat
  `Vec<RawSample>`: the downstream `VectorSelector` / `MatrixSelector`
  operators iterate per-series in timestamp order (lookback / sliding
  window), so columnar-per-series is the natural access pattern and avoids
  a transpose in the hot path. No end-of-series sentinel on the batch — the
  Stream itself signals completion via `None`, and intermediate batches do
  not need to mark "this series has no more samples" because the time
  window is absolute and the source has already decided to stop emitting
  for any given `series_range`. Adapter 2.2 may revisit if it discovers a
  reason for a per-series terminator.
- 2026-04-16 — Storage Implementor (unit 2.1) — `ResolvedSeriesRef` kept as
  a plain `{ bucket_id: u64, series_id: u32 }` struct (not a newtype wrapper
  around a `u64`). Caller treats it as opaque; source extensions add fields
  without breaking the public surface. `series_id` is `u32` to stay aligned
  with `crate::model::SeriesId` (which is `pub(crate)` and therefore cannot
  be named in the public `v2` surface directly); conversion is a no-op cast
  inside the adapter.
- 2026-04-16 — Storage Implementor (unit 2.2) — Batch emission strategy:
  **per-bucket**. `QueryReaderSource::samples` walks the hint's `series`
  slice and emits one `SampleBatch` per contiguous same-bucket run. A
  series that spans N buckets therefore produces N batches. Rationale: it
  matches the existing `BucketSampleData → shape_matrix_results` split in
  `pipeline.rs`; operators (`VectorSelector`/`MatrixSelector`) already
  own the cross-bucket merge, so pushing it into the source would just
  re-implement that logic. The RFC §"Storage Contract" allows either
  choice; picking the simpler one.
- 2026-04-16 — Storage Implementor (unit 2.2) — Cardinality estimate:
  **per-bucket upper bound from the inverted index**, using only positive
  equality terms (`__name__` + `Label="literal"` matchers). Runs one
  `QueryReader::inverted_index` call per overlapping bucket and sums
  `bitmap.len()`. Flagged `approx: true` whenever the selector uses regex,
  negative, or empty-string matchers (the sum is then a loose upper
  bound). When the selector has **no** positive terms at all the adapter
  returns `{ lower: 0, upper: u64::MAX, approx: true }` and defers the
  real match to `resolve()`. Not sub-millisecond if the inverted index
  has to be loaded from cold storage, but matches the tightest bound the
  existing `QueryReader` trait can give without pipeline.rs's
  `SelectorCacheKey` plumbing or a dedicated count accessor. Phase 4 /
  5 may extend `QueryReader` with an `estimate_selector_count` method if
  measurements show this is insufficient; unit 2.3 should flag a TODO if
  it observes >1 ms p50 for a typical query.
- 2026-04-16 — Storage Implementor (unit 2.2) — `QueryReader` surface
  gap: selector matching logic in `promql::selector` is driven by
  `CachedQueryReader` (ties into the evaluator-era `QueryReaderEvalCache`),
  which the v2 path deliberately does not use. Rather than modify
  `selector.rs` (RFC §"Migration Strategy" says keep it as-is), the
  adapter re-implements an equivalent matcher in a private
  `selector_util` submodule — 1:1 with the positive/OR/negative/empty-
  string handling in `find_candidates_with_reader`/
  `apply_negative_matchers`/`apply_empty_string_matchers`. The two
  must stay behaviourally aligned; unit 2.3 integration tests should
  cover both code paths against the same fixtures.
- 2026-04-16 — Storage Implementor (unit 2.2) — `QueryReader::samples`
  semantics quirk: the existing contract is
  `timestamp > start_ms && timestamp <= end_ms` (inclusive end, exclusive
  start — see the mock reader in `query.rs::test_utils` and the code
  path in `evaluator.rs`). The v2 `SampleHint::time_range` is
  `[start_ms, end_ms_exclusive)` (inclusive start, exclusive end). The
  adapter translates by passing `start_ms = time_range.start_ms - 1`
  and `end_ms = time_range.end_ms_exclusive - 1`. This keeps per-sample
  selection identical to the legacy path. Unit 2.3 should pin this with
  a boundary test (sample at exactly `start_ms`, `end_ms_exclusive - 1`,
  and `end_ms_exclusive`).
- 2026-04-16 — Storage Implementor (unit 2.2) — `ResolvedSeriesRef::bucket_id`
  encoding: `(TimeBucket::start as u64) << 8 | TimeBucket::size as u64`.
  `size == 0` is rejected on decode as a sanity guard (hour-exponent of
  zero is not a legal bucket). Callers treat `bucket_id` as opaque, so
  the representation is an implementation detail — if a future adapter
  needs more than 56 bits of start, the encoding can change without
  breaking the `SeriesSource` contract.
- 2026-04-16 — Storage Implementor (unit 2.2) — Extended `v2::QueryError`
  with an `Internal(String)` variant (previously only `MemoryLimit`
  and `TooLarge`). Needed to carry crate-level `Error::to_string()`
  through the adapter without synthesising fake `TooLarge` payloads.
  The phase-5 wiring unit may rename/split this (e.g. into
  `Storage`/`InvalidInput`) when it bridges `v2::QueryError` to
  `crate::error::QueryError`; leaving it named `Internal` for now since
  every adapter-emitted error *is* internal to the storage path.
- 2026-04-16 — Storage Implementor (unit 2.2) — `resolve()` emission
  ordering: chronological by bucket start. Empty buckets are **skipped**
  (not emitted as empty chunks) — RFC contract says "chunk is a
  contiguous slice", and an empty slice conveys no new information. If
  Phase 4 planner wants explicit "bucket covered, no match" signals,
  revisit; today the empty stream itself encodes that.
- 2026-04-16 — Storage Implementor (unit 2.3) — Test harness pattern:
  **in-crate `#[cfg(test)]` submodule** (`integration_tests` inside
  `timeseries/src/promql/v2/source_adapter.rs`). No `timeseries/tests/`
  directory exists; the crate's established pattern is in-crate
  `#[cfg(test)]` modules (`tsdb.rs`, `reader.rs`, `promql/selector.rs`,
  `promql/pipeline.rs`). Colocating the 2.3 integration suite with the
  adapter also lets it exercise `pub(crate)` surface
  (`QueryReaderSource::new`, `encode_bucket`, internal test fixtures)
  without widening visibility.
- 2026-04-16 — Storage Implementor (unit 2.3) — Fixture: reused
  `MockMultiBucketQueryReaderBuilder` from `crate::query::test_utils`
  (the same builder that backs `promql/selector.rs` and
  `promql/pipeline.rs` tests). No fixture surgery required; the
  builder cleanly supports multi-bucket ingestion, raw `f64` values
  (including `STALE_NAN` bit-patterns), and controlled label sets.
- 2026-04-16 — Storage Implementor (unit 2.3) — `selector_util` matcher
  parity: validated via **hand-built expected series sets** against
  fixtures (not by calling `promql::selector::evaluate_selector_with_reader`
  directly). `selector.rs`'s public surface is driven by
  `CachedQueryReader`, which the v2 path deliberately bypasses; wiring
  the legacy code-path into 2.3 tests would re-introduce the coupling
  2.2 explicitly avoided. The shapes covered (metric-only, equality,
  negation, regex OR, empty-string, combined AND) mirror the matcher
  battery in `promql/selector.rs` tests 1:1, so a drift would produce
  a clear diagnostic: a `selector.rs` test stays green while the
  matching `source_adapter.rs` test fails.
- 2026-04-16 — Storage Implementor (unit 2.3) — Adapter bugs
  surfaced: **zero**. All 16 integration tests (7 RFC-contract, 4
  boundary/edge, 2 cross-bucket, 3 cardinality, 7 matcher-parity)
  passed on first successful compile. The one compile-failure iteration
  was a `collect_ok` helper issue (RPITIT streams are not `Unpin`;
  fix: `Box::pin` inside the helper); the adapter itself needed no
  changes.
- 2026-04-16 — Storage Implementor (unit 2.3) — Test-side note, not
  an adapter bug: `MockQueryReader::samples` returns samples in
  *insertion* order, while the production `QueryReader` contract
  (mirrored by `MiniQueryReader`) returns them sorted by timestamp.
  The `should_return_samples_in_timestamp_order_per_series` test
  therefore ingests in timestamp order and verifies end-to-end
  order *preservation* through the adapter (the RFC-asserted
  property the adapter is responsible for: it must not reorder). A
  full reorder-resilience test would need to either strengthen the
  mock's contract or exercise the real `MiniQueryReader` path; both
  are out of scope for 2.3 and belong with whatever unit hardens the
  test-utils.
- 2026-04-16 — Operator Implementor (unit 3a.1) — Lookback/@/offset
  composition order verified against `evaluator.rs:1104-1160`
  (`selector_bounds`). The operator uses:
  `pin = @-value (constant across all steps) | @ start() = grid.start_ms
  | @ end() = grid.end_ms | step t (default)`; `effective = pin - offset`
  where `Offset::Pos(d)` subtracts `d` and `Offset::Neg(d)` adds `d`
  (evaluator.rs:1145-1154). Lookup window is `(effective - lookback,
  effective]` — inclusive end, exclusive start — matching
  `pipeline.rs:673-688` (`sample.timestamp_ms > lookback_start_ms &&
  sample.timestamp_ms <= current_step_ms`).
- 2026-04-16 — Operator Implementor (unit 3a.1) — `STALE_NAN` handling:
  treated as absence. When the most-recent sample in a step's lookback
  window is `STALE_NAN`, the operator clears validity for that cell and
  does **not** walk further back. This is **stricter** than the current
  `pipeline.rs` path (which does not filter stale markers in the
  lookback walk). The task spec for 3a.1 mandates this; it matches
  Prometheus lookback semantics where a staleness marker terminates a
  series. Downstream parity with the existing engine across `promqltest`
  goldens will be checked in Phase 6 — if any fixture depends on the
  older (stale-swallowing) semantics, that's a 3a.1 re-open, not a
  fixture edit.
- 2026-04-16 — Operator Implementor (unit 3a.1) — Operators live in
  `v2/operators/`. Submodule scaffolding added now so the remaining
  phase-3 units (`matrix_selector.rs`, `rollup.rs`, `instant_fn.rs`,
  `binary.rs`, `aggregate.rs`, `subquery.rs`, `rechunk.rs`,
  `count_values.rs`, `concurrent.rs`, `coalesce.rs`) drop in cleanly.
  `operators::prelude` re-exports the one struct shipped so far
  (`VectorSelectorOp`); the top-level `v2/mod.rs` also re-exports
  `VectorSelectorOp` directly for callers that prefer the flat path.
- 2026-04-16 — Operator Implementor (unit 3a.1) — Batch shape: one
  `StepBatch` per `(series_chunk, step_chunk)` tile. Default tile
  `N = 64 steps × K = 512 series` (RFC §"Core Data Model"); overridable
  via `BatchShape::new` for tests / tuning. v1 keeps the pipeline
  simple: on each new series chunk, the operator drains the full
  `SeriesSource::samples` stream for that chunk into a per-chunk
  `(timestamps, values)` pair, then iterates step chunks over the
  materialised samples. This allocates `O(samples_in_window)` per
  series chunk — all reserved via `MemoryReservation`. A future
  optimization could pipeline chunk load with step emission, but it
  is not justified until profiling.
- 2026-04-16 — Operator Implementor (unit 3a.1) — Memory accounting:
  two `BatchBuffers` / `ChunkSamples` RAII guards each reserve on
  construction and release on `Drop`. `BatchBuffers::finish` releases
  the per-batch bytes *as the batch leaves the operator* — v1
  convention is that the downstream operator re-reserves if it needs
  to retain the batch. `ChunkSamples` reserves `samples_bytes(n)` per
  `absorb` call; total per-chunk reservation is the sum of sample
  counts across the hydrating `SampleBatch` stream. Any `try_grow`
  failure surfaces as `QueryError::MemoryLimit` on the next poll and
  transitions the state machine to `Errored` (subsequent polls return
  `Ready(None)`).
- 2026-04-16 — Operator Implementor (unit 3a.1) — Operator API friction
  against Phase 1's `Operator` trait: **none**. `fn schema(&self) ->
  &OperatorSchema` and `fn next(&mut self, &mut Context) -> Poll<Option
  <Result<StepBatch, QueryError>>>` fit the leaf operator's needs
  exactly. No trait changes required; no silent adjustments made.
  Internal state machine is a small `enum State<'a>` with
  `Init | LoadingChunk | Emitting | Done | Errored` + a transient
  placeholder for `mem::replace` across match arms.
- 2026-04-16 — Operator Implementor (unit 3a.2) — Chose the **fuller
  option**: `MatrixSelectorOp` implements `Operator` with a degenerate
  `next()` (immediate `Ready(None)`) and exposes the useful API via
  `fn windows(&mut self, cx) -> Poll<Option<Result<MatrixWindowBatch,
  QueryError>>>`. Rationale: the sliding-walk two-pointer driver is the
  non-trivial part, and implementing the full window emission path now
  keeps it testable in isolation against hand-built sample streams.
  3b.1/3c.2 will call `windows()` directly; generalization to a shared
  `MatrixInput` trait can happen once those units land. The alternative
  skeleton option would have deferred the driver to 3b.1's reviewer,
  which is the wrong place for it.
- 2026-04-16 — Operator Implementor (unit 3a.2) — Sliding-window formula:
  `pin = @ value / start() / end() / step t`; `effective = pin - offset`
  (`Offset::Pos` subtracts, `Offset::Neg` adds — mirrors
  `evaluator.rs:1749-1764`); window `(effective - range, effective]`
  (exclusive start, inclusive end). Cited against
  `evaluator.rs:595-611` (`slice_samples_binary_search`: "timestamp_ms >
  start_ms && timestamp_ms <= end_ms") and
  `pipeline.rs:184` (`QueryPlan::for_matrix` carves the sample-fetch
  range as `[end_ms - range_ms, end_ms]`, and the evaluator's
  range-function consumers filter with the exclusive/inclusive binary
  search). `lookback_delta` is explicitly **not** applied — the
  bracketed `[range]` replaces it; composition order verified against
  `evaluator.rs:1487-1513` (`evaluate_matrix_selector` folds `@`/offset
  via `apply_time_modifiers` before carving the range window).
- 2026-04-16 — Operator Implementor (unit 3a.2) — `MatrixWindowBatch`
  layout: flat `Vec<i64>` timestamps + parallel `Vec<f64>` values +
  `Vec<CellIndex>` index, length `step_count × series_count`, row-major
  by step (matches `StepBatch`'s `values` layout). `CellIndex` is
  `{ offset: u32, len: u32 }` — `u32` is adequate because a single cell
  is bounded by the range's sample count at scrape resolution and
  billions of samples in one cell would exceed the operator's memory
  budget anyway. Rationale: preferred over a `Vec<Vec<Sample>>` (wasteful
  allocations + poor locality) and over a per-cell `(ts, val)` AoS
  layout (Rollup's two-pointer driver walks parallel columns). Consumers
  get per-cell `(&[i64], &[f64])` via `MatrixWindowBatch::cell_samples`.
  `STALE_NAN` is filtered by the operator while packing — consumers
  never see stale markers, matching 3a.1's stricter policy.
- 2026-04-16 — Operator Implementor (unit 3a.2) — No shared-helper
  extraction. 3a.1's `EffectiveTimes`/`ChunkSamples` shapes duplicate
  into this module: the types are short and their semantics differ at
  the margins (matrix does not fold lookback into the source time-range
  computation, and the chunk-samples reservation interacts with a
  different downstream shape). Generalisation is deferred until 3b.1
  and 3c.2 land; the overlap is small enough that a premature
  `operators/util.rs` module would fight the two callers' differences
  more than help them.
- 2026-04-16 — Operator Implementor (unit 3a.2) — `SeriesCursor`
  two-pointer driver is integer-only: it advances `(lo, hi)` indices
  based on the integer timestamp window `(window_lo_exclusive,
  window_hi_inclusive]`, with a reset-on-backward-jump guard so `@`
  pinning / non-monotonic effective-time sequences remain correct (at
  the cost of an O(samples) scan per reset; monotonic stepping keeps
  the amortised sweep O(samples) across the whole grid). `STALE_NAN`
  filtering happens in a second pass over the cursor's returned
  `Range<usize>` — it keeps the cursor free of float-special-case code
  and lets the packing loop compute the exact in-cell sample count
  before growing the reservation, so `grow_samples` is called once per
  cell rather than once per sample.
- 2026-04-16 — Operator Implementor (unit 3b.1) — Extrapolation formula
  ported verbatim from `timeseries/src/promql/functions.rs:1008-1077`
  (`counter_increase_correction` + `extrapolated_rate`). No
  redesign: the existing implementation is the Prometheus reference and
  the Phase 6 golden tests target it. The only restructuring is
  factoring the shared `(result × factor)` math into one
  `compute_delta` helper with a `DeltaKind::{Counter, Gauge}`
  discriminant so `rate` / `increase` / `delta` share one body —
  `rate` divides by `range_seconds`, `increase` does not, `delta`
  skips the counter-reset correction and the counter-zero clip. The
  counter-zero clip (`duration_to_zero` when `result > 0 && first_v >=
  0`) is Counter-only, matching `functions.rs:1060-1066`.
- 2026-04-16 — Operator Implementor (unit 3b.1) — Rollup-function
  dispatch is a **`RollupKind` enum with per-variant match arms**
  inside `RollupKind::compute`, not a trait object. `QuantileOverTime`
  carries its `q` as a variant payload (plan-time constant per
  Prometheus — range-vector quantiles with per-series `q` are not a
  PromQL construct). Each per-cell dispatch is one `match` on a
  `Copy` enum; no allocation, no vtable, `RollupOp` stays monomorphic
  in the child type. This matches VictoriaMetrics' "function selection
  as data" pattern (RFC §"Prior Art") — adding a new rollup is one
  variant + one arm.
- 2026-04-16 — Operator Implementor (unit 3b.1) — `WindowStream` trait
  is the injection seam: a narrower shape than `Operator` exposing
  only `schema()` + `poll_windows(cx)`. Tests implement it directly on
  a `MockWindows` holding a queue of pre-built `MatrixWindowBatch`es.
  Production uses `MatrixWindowSource<'a, S>` — a thin wrapper around
  `MatrixSelectorOp<'a, S>` that snapshots the upstream schema at
  construction time. The schema-snapshot exists because
  `MatrixSelectorOp::schema` is currently only callable through
  `Operator::schema` for `MatrixSelectorOp<'static, S>` (3a.2 left that
  impl on `'static` only); wrapping avoids the trait-object / lifetime
  gymnastics without touching the completed 3a.2 surface. If Phase 4
  wants a direct impl, the cleanest fix is a generic `fn schema(&self)
  -> &OperatorSchema` inherent method on `MatrixSelectorOp`, which
  would supersede `MatrixWindowSource`.
- 2026-04-16 — Operator Implementor (unit 3b.1) — MVP rollup coverage
  vs existing engine (phase-6 triage input). Existing
  `promql/functions.rs` ships: `rate`, `sum_over_time`, `avg_over_time`,
  `min_over_time`, `max_over_time`, `count_over_time`,
  `stddev_over_time`, `stdvar_over_time`. v2 Rollup implements all 8
  **plus** `increase`, `delta`, `irate`, `idelta`, `resets`,
  `changes`, `last_over_time`, `quantile_over_time`,
  `present_over_time`. Strict superset — Phase 6 golden tests cannot
  regress because of missing Rollup kinds. The extras are ready
  in-kernel but unreachable until the Phase 4 planner wires them to
  their parser-level function names; Phase 6 triage can start with
  the 8 existing kinds and add goldens for the extras when the
  planner exposes them.
- 2026-04-16 — Operator Implementor (unit 3b.1) — `MatrixWindowBatch`
  API friction: **none for the MVP rollup set**. `cell_samples(step_off,
  series_off) -> (&[i64], &[f64])` is exactly the shape every rollup
  reducer needs. The row-major cell indexing and `Arc<[i64]>`
  step-timestamp sharing let `reduce_batch` pass straight through to
  the emitted `StepBatch` without a transpose. One latent concern for
  3c.2 (`Subquery`): the subquery operator re-grids its inner plan and
  may want to *construct* `MatrixWindowBatch`es (not just consume
  them); if so, the `CellIndex`-packing logic in 3a.2 would benefit
  from a pub builder API. Out of scope for 3b.1 but worth flagging.
- 2026-04-16 — Operator Implementor (unit 3a.1) — `hint_series` is
  the operator's constructor input alongside `series: Arc<SeriesSchema>`.
  The two are parallel-indexed: `hint_series[i]` is the opaque
  `ResolvedSeriesRef` for the same series whose labels live at
  `series.labels(i)`. Chunk loads slice `hint_series[chunk_start
  ..chunk_end]` and pass it as the `SampleHint::series` — i.e. each
  chunk's hint covers only that chunk's series, and the returned
  `SampleBatch::series_range` is interpreted as **chunk-local**
  (0..chunk_len), not roster-global. This matches how the adapter in
  2.2 emits batches (one `SampleBatch` per bucket with a
  `series_range` slicing into the supplied hint), so cross-bucket
  series correctly append multiple batches into the same chunk column
  via `ChunkSamples::absorb`.
- 2026-04-16 — Operator Implementor (unit 3b.2) — `InstantFnKind`
  dispatch mirrors `RollupKind`: one `Copy` enum with one match arm per
  function in `InstantFnKind::compute`; plan-time scalar arguments
  (`Round { to_nearest }`, `Clamp { min, max }`, `ClampMin { min }`,
  `ClampMax { max }`) are variant payloads. No trait object, no vtable,
  `InstantFnOp` stays monomorphic in the child type. MVP covers 29
  variants — 25 legacy-engine-parity (`abs`, `ceil`, `floor`, `exp`,
  `ln`, `log2`, `log10`, `sqrt`, `sin`, `cos`, `tan`, `asin`, `acos`,
  `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`, `deg`,
  `rad`, `round`, `clamp`, `clamp_min`, `clamp_max`, `timestamp`) plus
  `sgn` (Prometheus ships it; legacy engine does not). Deliberate
  exclusions: `absent`/`absent_over_time` (schema derivation),
  `histogram_quantile` (RFC non-goal), `scalar`/`vector` (planner
  coercions), `pi`/`time` (nullary constants), and `day_of_*`/`hour`/
  `minute`/`month`/`year`/`days_in_month`/`day_of_week`/`day_of_year`
  (calendar-math pointwise — deferred to keep 3b.2 focused on
  float→float; belongs in a follow-up).
- 2026-04-16 — Operator Implementor (unit 3b.2) — Validity policy:
  **pointer-clone from input, never flip**. If the input cell's validity
  bit is clear, the output bit is clear (value is NaN-filled default).
  If the input is valid, the output is valid *even when the computed
  value is NaN or ±inf* (e.g. `ln(-1) = NaN`, `ln(0) = -inf`). This
  matches the legacy `UnaryFunction` path at
  `timeseries/src/promql/functions.rs:203-211` which writes
  `sample.value = (self.op)(sample.value)` without consulting the
  result. Since the function never produces new absences, the output
  `BitSet` is just `batch.validity.clone()` — cheaper than rebuilding
  and identical bit-for-bit.
- 2026-04-16 — Operator Implementor (unit 3b.2) — **`timestamp()`
  discrepancy flagged, not escalated.** Legacy engine
  (`timeseries/src/promql/functions.rs:870-881`, `TimestampFunction`):
  returns `sample.timestamp_ms as f64 / 1000.0` — the source sample's
  original scrape timestamp. v2 `InstantFnOp`: returns
  `step_timestamps[step_off] as f64 / 1000.0` — the evaluation step
  timestamp. The divergence is forced by the v2 batch ABI (RFC §"Core
  Data Model": `StepBatch.values: Vec<f64>` — no per-cell source
  timestamp column) and the task spec explicitly mandates step-timestamp
  semantics ("output = t/1000 (seconds)"). For instant queries (one
  step == eval_timestamp) the two definitions coincide. For range
  queries with lookback, the divergence is observable — v2 reports the
  evaluation step, legacy reports the underlying sample's scrape time.
  Phase 6 golden tests that exercise `timestamp()` over a range query
  may flag this; per the task's escalation rule the discrepancy is
  documented here rather than blocking progress. If goldens depend on
  legacy semantics, the fix is a schema-level extension (propagating
  per-cell source timestamps through `StepBatch`), not a fixture edit.
- 2026-04-16 — Operator Implementor (unit 3b.2) — Memory accounting:
  only the output `Vec<f64>` values column is charged against the
  reservation (bytes = `cells × 8`). Output validity is a pointer-clone
  of the input `BitSet`, so no new bitset allocation happens; output
  `step_timestamps` is an `Arc` pointer-clone; `SchemaRef` flows
  through by clone. Matches 3a.1 / 3b.1 convention: each operator
  reserves only the bytes it allocates itself — the child owns the
  input batch's reservation.
- 2026-04-16 — Operator Implementor (unit 3b.2) — No changes to Phase
  1's `Operator` / `OperatorSchema` surface or the `StepBatch` / `BitSet`
  types. `InstantFnOp<C: Operator>` generic over the child; state
  machine is a tiny three-flag struct (`done` / `errored` + child). No
  friction against the trait shape.
- 2026-04-16 — Operator Implementor (unit 3b.3) — `MatchTable` shape:
  three variants — `OneToOne(Vec<Option<u32>>)`, `GroupLeft(Vec<Option<u32>>)`,
  `GroupRight(Vec<Option<u32>>)`. All three share the
  `Vec<Option<u32>>` shape because PromQL `group_left` / `group_right`
  reduce to "each output row maps to at most one row on the 'one' side"
  after matching is resolved at plan time — the planner is responsible
  for producing the output `SeriesSchema` (length = "many" side) and
  filling in the per-output-row mapping to the "one" side. No
  `Vec<Vec<u32>>` shape needed; "many" is expressed by the output
  schema having more rows than the "one" side, with multiple output
  rows pointing at the same "one"-side index. Covers all three
  cardinalities fully — no stub / `Unimplemented` path — because the
  shape is as simple as one-to-one once the plan-time work is done.
- 2026-04-16 — Operator Implementor (unit 3b.3) — Scalar-as-operator:
  a scalar is a degenerate child producing 1-series `StepBatch`es over
  the full step grid. `ConstScalarOp` lives in this module as the
  planner-facing constructor (`new(value, grid, reservation)`). The
  binary operator treats vector/scalar and scalar/scalar uniformly —
  the "scalar" side is simply a child with `series_count == 1` and a
  broadcast per cell. Operand order is preserved via distinct
  `new_vector_scalar` / `new_scalar_vector` constructors so
  non-commutative ops (`Sub`, `Div`, `Pow`, `Atan2`, asymmetric
  comparisons) retain semantics.
- 2026-04-16 — Operator Implementor (unit 3b.3) — `bool` modifier
  encoding: carried as a per-variant field on each comparison kind
  (`Eq { bool_modifier: bool }`, `Ne { .. }`, `Gt { .. }`, `Lt { .. }`,
  `Gte { .. }`, `Lte { .. }`). Keeps `BinaryOpKind` a single `Copy`
  enum with no external wrapper and lets the hot loop dispatch on
  `match self.kind` once per batch. `BinaryOpKind::bool_modifier()`
  collapses the six variants to a single `bool` read for the inner
  loop. Semantics match the legacy engine at `evaluator.rs:1898-1923,
  2082-2094`: without `bool`, false-predicate cells are dropped
  (`validity = 0`); true-predicate cells pass through with the LHS
  value (not `1.0`). With `bool`, emit `0.0`/`1.0` per matched cell.
- 2026-04-16 — Operator Implementor (unit 3b.3) — Set-op validity
  semantics: `And` emits LHS value iff both cells valid (else
  `validity = 0`); `Or` emits LHS value if LHS valid, else RHS value
  if RHS valid, else `validity = 0`; `Unless` emits LHS value iff LHS
  valid AND RHS invalid or unmatched, else `validity = 0`. The
  existing engine (`evaluator.rs`) does **not** implement `and` / `or`
  / `unless` at all — no parity question, no divergence; this is a
  strict v2 superset. Phase 6 goldens exercising these ops will be
  their first runtime validation.
- 2026-04-16 — Operator Implementor (unit 3b.3) — Division-by-zero
  semantics **diverge from the legacy engine**. `evaluator.rs:2159-2165`
  forces `x / 0 → NaN` for all cases; this is a bug relative to
  Prometheus and the `promqltest` goldens at `operators.test:108-118`
  which expect IEEE 754 (`1/0 = +Inf`, `-1/0 = -Inf`, `0/0 = NaN`).
  v2 uses IEEE 754 directly — `self` lets `f64::div` produce the
  correct Prometheus-conformant value. Phase 6 will fail the legacy
  engine on these goldens; v2 passes. Flagged here per the task's
  "cite divergences" rule. (Contrast: `bool` modifier and filter-comparison
  semantics **match** legacy bit-for-bit — documented citations in
  the module docs.)
- 2026-04-16 — Operator Implementor (unit 3b.3) — No `Operator` /
  `OperatorSchema` surface changes. `BinaryOp<L: Operator, R: Operator>`
  is generic over both children; the state machine is two flags
  (`done` / `errored`). `BinaryShape` is an internal enum that tags
  the hot-loop path (`VectorVector` / `VectorScalar` / `ScalarVector`
  / `ScalarScalar`) — plan-time data, not a runtime discriminator.
  `pull_both` polls LHS then RHS; if either returns `Pending` the op
  returns `Pending`. Mixed EOS (one child done, the other not) is a
  planner bug — v2 errors with `QueryError::Internal` rather than
  silently producing a partial result. Misaligned `step_range`
  between children is likewise a planner bug and errors cleanly. No
  `async-trait`, no `Box::pin` in the hot path.
- 2026-04-16 — Operator Implementor (unit 3b.4) — `GroupMap`
  encoding: **`Vec<Option<u32>>`** rather than a `Vec<u32>` with a
  sentinel. Rationale: the `None` case is rare but legitimate (the
  planner may need to drop an input from the aggregation without
  trimming the upstream schema — e.g., when a selector-joined child
  pulls in series the aggregation's `by`/`without` explicitly
  excludes). `Option<u32>` makes "drop this input" explicit at the
  language level and compiles to a null-tag check that is trivially
  predictable. The 4-byte padding per entry is negligible — a
  1M-input-series query is 4 MB of map, well under the per-query
  memory cap. `u32::MAX` sentinel would have saved that padding but
  required hand-audited conversions at every call site.
- 2026-04-16 — Operator Implementor (unit 3b.4) — `AggregateKind`
  variant set: **only streaming kinds** (`Sum`, `Avg`, `Min`, `Max`,
  `Count`, `Stddev`, `Stdvar`, `Group`). 3c.1 will extend this enum
  in-place with `Topk(u64)`, `Bottomk(u64)`, and `Quantile(f64)`
  variants — those need a *different operator* (a pipeline breaker
  that buffers the full step) but the same planner-facing discrimi-
  nant. Keeping them in one enum lets the Phase 4 planner lower
  every aggregation to a single `AggregateKind` and dispatch on the
  kind to pick between `AggregateOp` (streaming) and the 3c.1
  breaker (buffering). The `match` on the streaming side exhausts
  the current variants; when 3c.1 adds the new ones, a compile
  error on `reduce_batch`'s match will flag the call site for
  explicit rejection (e.g., `AggregateKind::Topk(_) =>
  unreachable!("breaker kind routed to streaming op")`).
- 2026-04-16 — Operator Implementor (unit 3b.4) — Per-group single
  accumulator struct carries every lane (Kahan sum/c_sum, Welford
  mean/M2 with Kahan compensation, min/max with `any_real` flag,
  count). The per-kind reducer reads only the lane it needs; all
  lanes are updated on every `absorb()`. Trade-off: ~96 bytes per
  group (vs. ~16 if specialised per kind). For the expected group
  counts (thousands, not millions) the memory hit is negligible,
  and the single struct keeps the hot loop monomorphic regardless
  of `AggregateKind` — no runtime branching on kind in the
  per-cell inner loop. The kind match happens once per (step,
  group), after the series sweep. If profiling later shows the
  extra lane work is hot, specialise per kind; not worth the
  complexity pre-measurement.
- 2026-04-16 — Operator Implementor (unit 3b.4) — Min/Max NaN
  handling: **matches existing engine**. The existing evaluator at
  `evaluator.rs:2489-2492` uses `values.iter().fold(f64::INFINITY,
  |a, &b| f64::min(a, b))` — Rust's `f64::min` returns the non-NaN
  side when exactly one argument is NaN (IEEE 754 minNum), so NaN
  inputs are effectively ignored while any real value is present.
  When every input is NaN, the seed (`INFINITY`/`NEG_INFINITY`) is
  returned, which is a minor difference from v2's "NaN stays NaN"
  behaviour for all-NaN groups. Per RFC goals and Prometheus
  documented semantics, preserving NaN for all-NaN groups is
  correct. Phase 6 may flag goldens that exercise this edge case;
  if any do, the divergence is a fix on the legacy side, not v2.
- 2026-04-16 — Operator Implementor (unit 3b.4) — Count validity
  policy: a group with zero valid input contributions emits
  `validity = 0` (group is absent). This mirrors the existing
  engine's structural behaviour: `count()` over a selector with no
  matching samples returns no series at all (the group doesn't
  exist in the `HashMap`); within an existing group with no valid
  cells for a given step, the result is absent rather than `0.0`.
  If a caller needs "0 means zero valid inputs," they should use
  `count_over_time`/filtering at a higher level, not rely on this
  operator synthesising 0.0-valued cells for empty groups.
- 2026-04-16 — Operator Implementor (unit 3b.4) — Memory accounting:
  **two reservations per operator instance**. (1) A plan-time
  `group_count × sizeof(Accumulator)` scratch allocation charged at
  constructor time and released on `Drop` — fails fast with
  `QueryError::MemoryLimit` from `new()` if the reservation can't
  grow. (2) Per-batch output (`Vec<f64>` + `BitSet`) charged on each
  `reduce_batch` call via an `OutBuffers` RAII guard (same pattern
  as `rollup.rs` / `binary.rs`); released when the operator hands
  ownership to the downstream via `StepBatch::new`. The input
  batch's reservation belongs to the child (3a.1 / 3b.x convention).
- 2026-04-16 — Operator Implementor (unit 3b.4) — `Operator` trait
  friction: **none**. `AggregateOp<C: Operator>` is generic over the
  child; state machine is two flags (`done` / `errored`). No trait
  changes; no `StepGrid` extensions needed — the planner passes
  output schema and group map directly to the constructor.
- 2026-04-16 — Operator Implementor (unit 3c.1) — Output-schema
  strategy: **option 3 — planner decides, operator trusts**.
  `AggregateOp` accepts a single `output_schema: Arc<SeriesSchema>`
  and debug-asserts its length against the variant's expected shape:
  `topk/bottomk` ⇒ `input_series_count` (filter shape, one output
  cell per input cell with `validity = 0` for non-selected); everything
  else (streaming kinds + `Quantile`) ⇒ `group_count`. Phase 4
  planner is therefore obligated to build the correct schema per
  variant: for `topk`/`bottomk`, pass the input series' labels
  through unchanged; for `quantile`/streaming, build one row per
  grouping key. No operator-side switch between two schemas —
  simplest, keeps the operator mechanical.
- 2026-04-16 — Operator Implementor (unit 3c.1) — `AggregateKind`
  extended **in place** (as 3b.4 anticipated): new variants
  `Topk(i64)`, `Bottomk(i64)`, `Quantile(f64)`. **Chose `i64`, not
  `u64`**, for K: the RFC/task and `evaluator.rs::coerce_k_size`
  treat `k < 1` as "select nothing"; `u64` cannot represent negative
  K and would force planner-side coercion to silently clamp. `i64`
  matches legacy semantics directly and pushes the out-of-range case
  onto the operator (which already has to handle `k > input_count`
  anyway). Planner coerces the PromQL scalar parameter to `i64` at
  plan time — overflow behaviour of `f64 -> i64` is
  platform-defined; legacy uses `as i64` and we do the same.
  `AggregateKind::is_breaker` / `output_is_inputs` added as inline
  helpers for per-kind dispatch; streaming `reduce_batch_streaming`
  has an `unreachable!` arm for breakers so the compiler flags any
  future regression.
- 2026-04-16 — Operator Implementor (unit 3c.1) — Tie-break rule for
  topk/bottomk: **first-seen wins** (lower input-series index).
  Matches `evaluator.rs:687-694` (`KHeapEntry::cmp` breaks ties
  `self.index.cmp(&other.index)` on a max-heap where "greater =
  more evictable"; higher index is evicted first, lower index
  stays). v2 encodes the same ordering in `KHeapEntry::cmp` plus
  an early-out — candidates only replace the current worst when
  `entry.cmp(worst) == Ordering::Less`, so ties never dislodge an
  already-selected lower-index cell.
- 2026-04-16 — Operator Implementor (unit 3c.1) — K out-of-range
  semantics: **matches legacy** `coerce_k_size` at
  `evaluator.rs:2249-2253`. `k <= 0` ⇒ no cell selected (every
  output cell emits with `validity = 0`); `k >= group_size` ⇒
  every valid input in the group is selected. No fail-fast error
  on negative K — the `reduce_topk_or_bottomk` fast path returns
  an all-zero validity batch when `k <= 0`, saving heap allocation
  and per-cell dispatch.
- 2026-04-16 — Operator Implementor (unit 3c.1) — NaN handling in
  topk/bottomk: NaN entries are pushed into the heap and ranked
  "worst" by `k_cmp` (matches legacy `compare_k_values`). Practical
  effect: NaN inputs are selected only when `K` exceeds the non-NaN
  count in the group — otherwise any real value outranks them.
  Test `should_ignore_nan_inputs_in_topk` verifies the typical case
  (K ≤ real-count ⇒ NaN cells not in output).
- 2026-04-16 — Operator Implementor (unit 3c.1) — `quantile(q)`
  out-of-range semantics: **matches `rollup::rollup_fns::quantile`
  bit-for-bit** (`q < 0 ⇒ -inf`, `q > 1 ⇒ +inf`, `q.is_nan() ⇒ NaN`,
  linear interpolation between ranks for `q ∈ [0, 1]`). The
  implementation is a private `quantile_linear_interp` in
  aggregate.rs rather than a cross-module call into `rollup` — the
  function body is 15 lines and re-using `rollup`'s helper would
  require publishing it outside `pub(super)`, which isn't
  justified for a leaf. Kept the doc comment citing the rollup
  parity so future drift shows up as a doc mismatch.
- 2026-04-16 — Operator Implementor (unit 3c.1) — Memory accounting:
  the `scratch_bytes` field on `AggregateOp` replaces 3b.4's
  `accum_bytes` (renamed, carries the per-kind scratch reservation).
  For streaming kinds: unchanged — `group_count × sizeof(Accumulator)`.
  For `Topk`/`Bottomk`: `group_count × min(k, input_count) ×
  sizeof(KHeapEntry)` — conservative product upper bound, reserved
  once at constructor. For `Quantile`: `input_count × 8 +
  group_count × sizeof(Vec<f64>)` — the sum of per-group sort
  buffer capacities is bounded by `input_count` (each input
  belongs to at most one group), so this is a tight upper bound.
  All three paths also reserve a per-batch `OutBuffers` via the
  existing RAII guard. `should_respect_memory_reservation_for_topk_heap`
  exercises the constructor-time rejection on a 1-byte cap.
- 2026-04-16 — Operator Implementor (unit 3c.1) — `AggregateOp::new`
  **signature unchanged** from 3b.4 — same four parameters (`kind`,
  `group_map`, `output_schema`, `reservation`). No additive fields,
  no separate constructor, no escalation needed. The planner
  distinguishes `topk`/`bottomk` vs streaming/`quantile` purely by
  the output schema it constructs; the operator validates via debug
  assertion. Internal struct gained `heaps`, `sort_bufs`, and
  renamed `accum_bytes` → `scratch_bytes`, but those are private.

## 6. Activity Log

Append-only, one line per agent invocation. Format:
`<date> <role> <unit-id> <status-change> — <one-line summary>`.

- 2026-04-16 Architect — plan document created.
- 2026-04-16 Foundation Implementor 1.1 ready→done — added `promql-v2` feature (empty) to `timeseries/Cargo.toml`; verified `cargo check` with and without the flag and `cargo clippy --all-targets --all-features -- -D warnings` pass.
- 2026-04-16 Foundation Implementor 1.2 in-progress→done — created `promql/v2/{mod,batch,memory,operator}.rs` scaffolding gated on `promql-v2`; stub files carry module doc comments pointing at units 1.3/1.4/1.5. `cargo check -p opendata-timeseries` (no flag) and `--features promql-v2` both compile; `cargo clippy --all-targets --all-features -- -D warnings` passes.
- 2026-04-16 Foundation Implementor 1.3 in-progress→done — implemented `StepBatch`, `SeriesSchema`, `SchemaRef` (Static/Deferred), and a hand-rolled `Vec<u64>`-backed `BitSet` in `promql/v2/batch.rs`; re-exported from `promql/v2/mod.rs`. Row-major-by-step layout documented on the `values` field per RFC. 16 unit tests cover shape invariants, cell indexing, validity access, schema accessors, and bitset edge cases. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Foundation Implementor 1.4 in-progress→done — implemented `MemoryReservation` (atomic CAS-loop `try_grow`/`release`, clone-shared `Arc<Inner>`, separate high-water atomic) and `QueryError` (`MemoryLimit`, `TooLarge`) in `promql/v2/memory.rs` using workspace `thiserror`; re-exported from `promql/v2/mod.rs`. 12 unit tests cover within-cap grow, exceeding cap with diagnostic payload, release, regrow after release, high-water monotonicity, concurrent grows (all-fit and cap-contended), zero-cap rejection, zero-byte no-op, `usize` overflow, clone-shared state, and the `TooLarge` variant. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2::memory` (12 passed), and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Foundation Implementor 1.5 in-progress→done — defined `Operator` trait, `OperatorSchema`, and `StepGrid` in `promql/v2/operator.rs`; re-exported from `promql/v2/mod.rs`. Trait shape: `Send`-only, object-safe, `fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, v2::QueryError>>>`. `OperatorSchema { series: SchemaRef, step_grid: StepGrid }`. 3 unit tests (`should_build_trait_object`, `should_report_schema_before_next`, `should_yield_end_of_stream`) exercise object-safety and the schema-before-poll contract via an `Empty` test double. Phase 1 acceptance re-verified: `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (31 passed — 16 batch + 12 memory + 3 operator), and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Storage Implementor 2.1 in-progress→done — defined `SeriesSource` trait plus `TimeRange`, `ResolvedSeriesRef`, `ResolvedSeriesChunk`, `CardinalityEstimate`, `SampleHint`, `SampleBlock`, `SampleBatch` in `promql/v2/source.rs`; re-exported from `promql/v2/mod.rs`. Trait returns use return-position `impl Future` / `impl Stream` (stable RPITIT, Rust 1.95 / edition 2024) so adapters don't box on the hot path; trait is **not** dyn-safe — planner will bind generic `S: SeriesSource` (see Decisions Log). `VectorSelector` re-used directly from `promql_parser::parser::VectorSelector` (same path as `selector.rs` / `pipeline.rs` / `evaluator.rs`). 7 new unit tests cover constructor / accessor contracts on the value types (no tests that would need a concrete source — 2.2 adapter + 2.3 integration). `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (38 passed — Phase 1's 31 + 7 new), and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Storage Implementor 2.2 in-progress→done — implemented `QueryReaderSource<R: QueryReader>` + supporting helpers in `promql/v2/source_adapter.rs`; re-exported from `promql/v2/mod.rs`. Adapter is stateless (no interior mutability): `resolve()` fans out over overlapping buckets, emits one `ResolvedSeriesChunk` per non-empty bucket in chronological order (empty buckets skipped); `estimate_cardinality()` sums per-bucket inverted-index cardinalities for positive equality terms (flagged `approx: true` when the selector uses regex/negative/empty-string matchers, `{ 0, u64::MAX, approx }` when no positive terms exist); `samples()` groups the hint's series into contiguous same-bucket runs and emits one `SampleBatch` per run (cross-bucket merge is an operator concern — see §5 Decisions Log 2.2). Selector matcher logic re-implemented in a private `selector_util` submodule (behaviour-mirrors `promql::selector`; RFC mandates leaving that file untouched). `v2::QueryError` extended with an `Internal(String)` variant to carry crate-level error strings without synthesising fake `TooLarge` payloads. Added 6 new unit tests covering bucket-id encode/decode round-trip, zero-size rejection, bucket/time-range overlap boundary cases, and contiguous-bucket-run partitioning (including order preservation + empty-input handling). Heavier integration tests deferred to unit 2.3. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (44 passed — Phase 1's 31 + 2.1's 7 + 2.2's 6), and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Storage Implementor 2.3 in-progress→done — added 21 integration tests in a new `integration_tests` `#[cfg(test)]` submodule inside `timeseries/src/promql/v2/source_adapter.rs` (in-crate pattern; no `tests/` dir exists). Coverage: RFC source→caller (contiguous `series_range` slicing, per-series timestamp ordering, `STALE_NAN` bit-exact round-trip), RFC caller→source (no widening beyond selector match, time-range respect), 4 boundary cases pinning the `[start, end)` ↔ `(start, end]` translation (§5 Decisions Log 2.2) plus empty-window short-circuit, cross-bucket stitching (one batch per bucket in chronological order, non-overlapping buckets skipped), cardinality estimate (monotone upper bound vs. resolve, `u64::MAX` sentinel for no-positive-term selectors, `approx` flag for non-equality matchers), and 7 matcher-parity tests against hand-built expected series sets (metric-only, equality, negation, regex, empty-string, combined AND, regex OR). Fixtures reuse `MockMultiBucketQueryReaderBuilder` from `crate::query::test_utils`; no production-code changes, no test-fixture surgery, no adapter bugs surfaced. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (65 passed — 44 prior + 21 new), and `cargo clippy --all-targets --all-features -- -D warnings` all pass. Phase 2 acceptance re-verified.
- 2026-04-16 Operator Implementor 3a.1 in-progress→done — created `promql/v2/operators/{mod,vector_selector}.rs` and re-exported `VectorSelectorOp` from `promql/v2/mod.rs`. Implements PromQL instant-vector semantics (lookback, `@`, `offset`, `STALE_NAN`-as-absence) against the generic `S: SeriesSource` contract — `pin = @-value or grid start/end or step t`, `effective = pin - offset` (Pos subtracts / Neg adds), window `(effective - lookback, effective]` — composition order cross-checked against `evaluator.rs:1126-1160` and `pipeline.rs:673-688` (see §5 Decisions Log entries for 3a.1). Batch shape: per-tile `StepBatch` emission with default `N=64 × K=512` (RFC §"Core Data Model"); tile dimensions are a plumbable `BatchShape`. Memory: `BatchBuffers` and `ChunkSamples` RAII guards reserve on `try_grow` and release on `Drop`; `try_grow` failure surfaces as `QueryError::MemoryLimit` on the next poll. No changes to Phase 1's `Operator` / `OperatorSchema` / `StepGrid` surface — the trait shape fit this leaf unmodified. 11 new unit tests on hand-constructed `MockSource` mocks cover lookback window, absence, `STALE_NAN` termination, `Offset::Pos`, `@ t`, `@ start()`, tile coverage of the full grid, end-of-stream idempotence, static-schema contract, memory-limit rejection + recovery, and zero-series rosters. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (76 passed — 65 prior + 11 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Operator Implementor 3a.2 in-progress→done — created `promql/v2/operators/matrix_selector.rs` (new `MatrixSelectorOp`, `MatrixWindowBatch`, `CellIndex`); registered in `promql/v2/operators/mod.rs` and re-exported from `promql/v2/mod.rs` alongside `VectorSelectorOp`. Picked the fuller option: degenerate `Operator::next` (immediate `Ready(None)`) + secondary `windows(cx)` API emitting flat-columnar window batches with `(offset, len)` per-cell indexing, row-major by step. Sliding-walk driver is an integer-only two-pointer `SeriesCursor` with reset-on-backward-jump to stay correct for `@`-pinned grids; `STALE_NAN` filtered while packing. Composition order (`@` → offset → range) and window shape `(effective - range, effective]` cross-referenced against `evaluator.rs:1487-1513`, `evaluator.rs:595-611`, and `pipeline.rs:184` (see §5 Decisions Log). Memory: `WindowBuffers` (cells reserved up front, samples grown per-cell) + `ChunkSamples` (per-chunk hydration) RAII guards route through `MemoryReservation::try_grow`. 12 new unit tests on hand-built sample streams cover the spec battery (inclusive-end, exclusive-start, empty windows, `@` pinning, offset shift, `STALE_NAN` absence, memory cap, static schema, end-of-stream idempotence, sliding-walk monotonicity, and degenerate `Operator::next`). `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (88 passed — 76 prior + 12 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Operator Implementor 3b.1 in-progress→done — created `promql/v2/operators/rollup.rs` (`RollupOp<W: WindowStream>`, `RollupKind`, `WindowStream` injection trait, `MatrixWindowSource` production wrapper, private `rollup_fns` reducer module); registered in `operators/mod.rs` and re-exported from `v2/mod.rs`. Unified range-function driver: one operator, one two-pointer pass per `(step, series)` window cell, function selection via `Copy` enum (`match` dispatch, no vtable). MVP covers 17 rollup kinds — a strict superset of the existing engine's 8 (see §5 Decisions Log 3b.1 entry for gap triage). Extrapolation formula ported verbatim from `functions.rs:1008-1077` (`counter_increase_correction` + `extrapolated_rate`); `rate` / `increase` / `delta` share one `compute_delta` helper with a `DeltaKind::{Counter, Gauge}` discriminant (Counter path adds reset correction + zero-bound clip; Gauge skips both). Output `StepBatch` allocations are memory-accounted through `OutBuffers` (RAII, `try_grow` on allocate, release on `Drop` or `finish`); input `MatrixWindowBatch` is not double-counted (3a.2 already owns it). 19 new unit tests on hand-constructed `MockWindows` inputs cover rate/reset, increase parity with hand-calc, avg/sum/min/max/count/last/idelta/delta, irate last-two semantics, validity=0 for <min-samples and empty windows, quantile interpolation (4 quantiles), stddev/stdvar, changes/resets, static-schema passthrough, step-grid passthrough, memory cap rejection, error propagation, present_over_time, and a 16-step sliding-window walk. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (107 passed — 88 prior + 19 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Operator Implementor 3b.2 in-progress→done — created `promql/v2/operators/instant_fn.rs` (`InstantFnOp<C: Operator>`, `InstantFnKind` with 29 variants); registered in `operators/mod.rs` and re-exported from `v2/mod.rs`. Pointwise scalar-function driver: one operator, function selection via `Copy` enum (`match` dispatch per cell, no vtable), plan-time scalar arguments (`Round`, `Clamp`, `ClampMin`, `ClampMax`) as variant payloads. MVP covers the legacy engine's 25 pointwise functions (`abs`, `ceil`, `floor`, `exp`, `ln`, `log2`, `log10`, `sqrt`, 6 trig, 6 hyperbolic, `deg`, `rad`, `round`, `clamp`, `clamp_min`, `clamp_max`, `timestamp`) plus `sgn` (not in legacy) — strict superset of the legacy pointwise surface (see §5 Decisions Log for exclusions: `absent`, `histogram_quantile`, `scalar`/`vector`, `pi`/`time`, date/time extractions). Validity policy: **pointer-clone, never flip** — input validity bit gates output validity, NaN/±inf from `ln(-1)`/`ln(0)` pass through with validity=1 per legacy `UnaryFunction` at `functions.rs:203-211` (§5 Decisions Log). Output `StepBatch` shares the input's `step_timestamps: Arc<[i64]>` (pointer-clone), `series: SchemaRef` (clone), and `validity: BitSet` (clone); only the fresh `values: Vec<f64>` is charged to the reservation (`OutValues` RAII guard, `try_grow` on allocate, release on `Drop`/`finish`). `timestamp()` semantics **deliberately diverge** from the legacy engine (step-timestamp/1000 vs. source-sample-timestamp/1000) — forced by the v2 batch ABI and task spec; documented in §5 Decisions Log for Phase 6 triage. 14 new unit tests on hand-built `MockChild` inputs: abs mapping, validity passthrough, clamp/clamp_min/clamp_max with plan-time bounds, `ln(0)=-inf` / `ln(-1)=NaN` with validity=1, `timestamp()` step-timestamp semantics, memory cap rejection, static-schema passthrough, drain + end-of-stream idempotence, sin/cos sanity, round with `to_nearest` (1.0 and 0.5), sgn (incl. NaN), error propagation, and an end-to-end `VectorSelectorOp -> InstantFnOp(Abs)` pipeline. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (121 passed — 107 prior + 14 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Operator Implementor 3b.3 in-progress→done — created `promql/v2/operators/binary.rs` (`BinaryOp<L, R>`, `BinaryOpKind` with 16 variants incl. `bool`-modified comparisons, `MatchTable { OneToOne, GroupLeft, GroupRight }`, `BinaryShape`, `ConstScalarOp` scalar-as-operator helper); registered in `operators/mod.rs` and re-exported from `v2/mod.rs`. All three PromQL binary shapes fully supported: vector/vector (with pre-computed plan-time match table), vector/scalar + scalar/vector (scalar as degenerate 1-series child; distinct constructors preserve operand order), and scalar/scalar. `MatchTable` variants use a uniform `Vec<Option<u32>>` shape — `group_left`/`group_right` express "many" via the planner-built output schema having > 1 rows pointing at the same "one"-side index; no stubbed cardinality path. `bool` modifier encoded as a per-variant field on each comparison kind; filter-comparison semantics (drop false cells, pass through LHS value for true cells) match `evaluator.rs:1898-1923,2082-2094` bit-for-bit (§5 Decisions Log). Set ops `and`/`or`/`unless` implemented as strict v2 superset (legacy engine has no set-op support). Division-by-zero uses IEEE 754 (Prometheus-conformant; matches `promqltest/testdata/operators.test:108-118` goldens); documented divergence from legacy's NaN-forcing in §5. Memory: `OutBuffers` RAII guard routes values + validity through `MemoryReservation::try_grow`; input batches owned by children. Children must share step grid (planner guarantee); misalignment and mixed EOS are clean `QueryError::Internal`. 17 new unit tests on hand-built `MockOp` upstreams cover add V/V one-to-one, sub V/S, div S/S (IEEE 754 infinity), unmatched-rhs validity, filter vs `bool` comparison, pow/mod/atan2, and/or/unless set ops, upstream error propagation, memory cap, static-schema/match-table output, end-of-stream, aligned multi-batch stitching, plus `group_left` (shared RHS) and `group_right` (shared LHS) cases. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (138 passed — 121 prior + 17 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Operator Implementor 3b.4 in-progress→done — created `promql/v2/operators/aggregate.rs` (`AggregateOp<C: Operator>`, `AggregateKind` with 8 streaming variants, `GroupMap`); registered in `operators/mod.rs` and re-exported from `v2/mod.rs` (and in `operators::prelude`). Streaming per-step reduction over planner-provided group map: one `Accumulator` struct per output group carries Kahan sum / Kahan-compensated Welford (mean + M2) / NaN-safe min-max lanes in parallel, updated once per valid input cell; the per-kind reducer reads only the lane it needs at step-finalise time, so the inner sweep is monomorphic regardless of `AggregateKind`. `GroupMap` uses `Vec<Option<u32>>` encoding (§5 Decisions Log — `None` = drop from aggregation; planner contract is `Some(g)` for the normal case). `AggregateKind` populated only with streaming variants (`Sum`/`Avg`/`Min`/`Max`/`Count`/`Stddev`/`Stdvar`/`Group`); 3c.1 extends with `Topk`/`Bottomk`/`Quantile` pipeline-breaker variants (single enum; planner dispatches streaming vs breaker operator based on variant). Validity semantics: groups with zero valid contributions emit `validity = 0` for every kind (including `Count`) — matches the legacy engine's structural "group doesn't exist" behaviour. Min/Max NaN policy: IEEE 754 minNum / maxNum — ignore NaN when any real value contributes, preserve NaN only for all-NaN groups (matches legacy `evaluator.rs:2489-2492` where `f64::min`/`f64::max` already behave this way; all-NaN corner differs cosmetically — legacy returns seed `±INFINITY`, v2 returns NaN). Memory: constructor reserves `group_count × sizeof(Accumulator)` scratch via `try_grow` (fails fast with `QueryError::MemoryLimit`) + per-batch `OutBuffers` RAII for the output `Vec<f64>` + `BitSet`. No changes to `Operator` / `OperatorSchema` / `StepGrid` / `StepBatch` surfaces — trait shape fit unmodified. 15 new unit tests on hand-built `MockOp` upstreams cover sum grouping, avg with invalid cells, NaN-safe min/max, count validity, Welford stddev/stdvar (population var=4 on 2,4,4,4,5,5,7,9), `Group`=1 when any input contributes, `validity=0` for every kind on empty groups, `by ()` single-group, `without ()` per-series-own-group, multi-batch stitching, memory cap rejection (constructor-time), static schema, upstream error propagation, child end-of-stream, and `None`-group-assignment drop behaviour. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (153 passed — 138 prior + 15 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Operator Implementor 3c.1 in-progress→done — extended `promql/v2/operators/aggregate.rs` with `AggregateKind::Topk(i64)` / `Bottomk(i64)` / `Quantile(f64)` breaker variants; no new files, no re-export surgery (3b.4 already exposes `AggregateKind`/`AggregateOp`). `AggregateOp::new` signature unchanged — planner decides the output schema (§5 Decisions Log 3c.1): filter-shape (`input_series_count` rows) for topk/bottomk, reducer-shape (`group_count` rows) for quantile; operator debug-asserts per variant. Breaker dispatch routes through `reduce_batch_breaker` into `reduce_topk_or_bottomk` (bounded `BinaryHeap<KHeapEntry>` per group, one heap reused across steps) and `reduce_quantile` (per-group sort buffer reused across steps). Tie-break, K-out-of-range, NaN, and quantile-q-out-of-range semantics all cited against the legacy engine / `rollup_fns::quantile` — see §5 Decisions Log entries (deterministic first-seen wins on value ties per `evaluator.rs:687-694`; `k < 1` ⇒ empty output per `coerce_k_size` at `evaluator.rs:2249-2253`; NaN ranks "worst" in heap per `compare_k_values` at `evaluator.rs:652-662`; quantile out-of-range matches `rollup.rs::rollup_fns::quantile` bit-for-bit). Memory accounting renamed `accum_bytes` → `scratch_bytes`: streaming paths unchanged, topk/bottomk reserve `group_count × min(k, input_count) × sizeof(KHeapEntry)`, quantile reserves `input_count × 8 + group_count × sizeof(Vec<f64>)` — both conservative upper bounds. 13 new unit tests on hand-built `MockOp` upstreams: topk selection per group, bottomk selection, K≥group size (all valid selected), K=0 (nothing selected), negative K (empty), NaN-ignored-in-topk, deterministic-tiebreak-by-lower-index, quantile linear interpolation (q=0.25/0.5/0.75), quantile out-of-range (q<0 → -inf, q>1 → +inf), quantile per group (two independent groups), memory cap rejection on topk heap, input-schema passthrough for topk, groups-schema passthrough for quantile. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (166 passed — 153 prior + 13 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.

---

## 7. Handoff Checklist (per agent, before stopping)

- [ ] State board updated: status, owner, artifacts, blocker (if any).
- [ ] Activity log line appended.
- [ ] Decisions log entries added for any judgment calls or open questions.
- [ ] `cargo fmt` run on all modified `.rs` files.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes.
- [ ] Relevant tests pass (unit for your unit; full suite only at phase acceptance).
- [ ] No files under `promqltest/` modified.
- [ ] No git commits created.
- [ ] No new `*.md` files created beyond what the phase authorized.

## 8. Subagent Dispatch Template

When the Architect dispatches an implementor, use this shape for the Task prompt. Keep it
short — the agent has this plan and the RFC already.

```
You are the <ROLE> for RFC 0007. Read:
  - timeseries/rfcs/0007-promql-execution.md (spec)
  - timeseries/rfcs/0007-impl-plan.md (plan, your role §<N>)

Your assigned unit: <UNIT-ID> — <UNIT-TITLE>

Scope is strict: §<N> of the plan defines what you may and may not touch.
Acceptance criteria: see the phase's Acceptance row in §4.
Hard rules: §1.4. Handoff checklist: §7.

When done (or blocked), update §4, §5, §6 of the plan and stop. Do not proceed to the next
unit — the orchestrator dispatches.
```

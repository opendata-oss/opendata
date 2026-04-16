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
| 3a.1 | `VectorSelector` operator (lookback, `@`, offset) | | ready | 2.2 | | |
| 3a.2 | `MatrixSelector` operator (sliding window) | | ready | 2.2 | | |
| 3b.1 | `Rollup` unified driver (rate/increase/\*\_over\_time) | | ready | 3a.2 | | |
| 3b.2 | `InstantFn` | | ready | 3a.1 | | |
| 3b.3 | `Binary` (vector/vector, vector/scalar, scalar/scalar) | | ready | 3a.1 | | |
| 3b.4 | `Aggregate` streaming ops (sum/avg/min/max/count/stddev/stdvar/group) | | ready | 3a.1 | | |
| 3c.1 | `Aggregate` breaker ops (topk/bottomk/quantile) | | ready | 3b.4 | | |
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

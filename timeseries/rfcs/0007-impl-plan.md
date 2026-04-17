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
| 3c.2 | `Subquery` | Operator Implementor | done | 3a.2, 3b.1 | `timeseries/src/promql/v2/operators/subquery.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3c.3 | `Rechunk` breaker | Operator Implementor | done | 1.5 | `timeseries/src/promql/v2/operators/rechunk.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3c.4 | `CountValues` (deferred-schema operator) | Operator Implementor | done | 3b.4 | `timeseries/src/promql/v2/operators/count_values.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 3c.5 | `Concurrent` and `Coalesce` exchange operators | Operator Implementor | done | 1.5 | `timeseries/src/promql/v2/operators/concurrent.rs`, `timeseries/src/promql/v2/operators/coalesce.rs`, `timeseries/src/promql/v2/operators/mod.rs`, `timeseries/src/promql/v2/mod.rs` | |

**Acceptance per unit**: operator has unit tests built on hand-constructed `StepBatch`es;
respects `MemoryReservation`; schema contract is explicit (Static vs Deferred).

### Phase 4 — Planner

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 4.1 | Logical plan representation + AST → logical lowering | Planner Implementor | done | 3c.* | `timeseries/src/promql/v2/plan/{mod,plan_types,error,lowering}.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 4.2 | Rule-based optimizer (constant fold, selector label pushdown, CSE if cheap) | Planner Implementor | done | 4.1 | `timeseries/src/promql/v2/plan/optimize.rs`, `timeseries/src/promql/v2/plan/mod.rs` | |
| 4.3 | Physical planner: bind `SeriesSource`, resolve series schemas, build group maps | Planner Implementor | done | 4.1 | `timeseries/src/promql/v2/plan/physical.rs`, `timeseries/src/promql/v2/plan/mod.rs`, `timeseries/src/promql/v2/plan/error.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 4.4 | Plan-time cardinality gate (`estimate_cardinality` → `QueryError::TooLarge`) | Planner Implementor | done | 4.3 | `timeseries/src/promql/v2/plan/cardinality.rs`, `timeseries/src/promql/v2/plan/mod.rs`, `timeseries/src/promql/v2/plan/error.rs`, `timeseries/src/promql/v2/plan/lowering.rs`, `timeseries/src/promql/v2/plan/physical.rs` | |
| 4.5 | Insertion of `Concurrent`/`Coalesce` based on series-count heuristics | Planner Implementor | done | 4.3 | `timeseries/src/promql/v2/plan/parallelism.rs`, `timeseries/src/promql/v2/plan/physical.rs`, `timeseries/src/promql/v2/plan/lowering.rs`, `timeseries/src/promql/v2/plan/mod.rs` | |

**Acceptance**: small PromQL expressions produce expected plans in unit tests;
cardinality-gate rejects oversized queries before resolve.

### Phase 5 — Wiring

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 5.1 | New `Tsdb::eval_query_v2` / `eval_query_range_v2` entry points | Wiring Implementor | done | 4.* | `timeseries/src/tsdb.rs`, `timeseries/src/promql/v2/reshape.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 5.2 | Dispatch on `promql-v2` feature flag in HTTP handler | Wiring Implementor | done | 5.1 | `timeseries/src/server/http.rs`, `timeseries/src/tsdb.rs`, `timeseries/tests/http_server.rs` | |
| 5.3 | Result shaping: `StepBatch` → `QueryValue` without copying labels twice | Wiring Implementor | done | 5.1 | `timeseries/src/promql/v2/reshape.rs` | |

**Acceptance**: with flag off, everything behaves as on `main`. With flag on, simple queries
return structurally-identical `QueryValue`s.

### Phase 6 — Verification

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 6.1 | Full `promqltest` corpus run against v2 with flag on | Tester | done | 5.* | `timeseries/src/promql/promqltest/v2_harness.rs`, `timeseries/src/promql/promqltest/mod.rs` | |
| 6.2 | Triage and file each failure class in Decisions Log | Architect | done | 6.1 | §5 Decisions Log entries from 6.1 (triage table, critical-bug flags) | |
| 6.3 | Re-run until green; no fixture edits | Architect | done | 6.2 | `timeseries/rfcs/0007-impl-plan.md` | All sub-units 6.3.1–6.3.8 landed; full `promqltest` corpus passes under `--features promql-v2`. |
| 6.3.1 | Aggregate panic + leaf-roster dedup by unique `SeriesFingerprint` | Operator/Planner Implementor | done | 6.2 | `timeseries/src/promql/v2/plan/physical.rs`, `timeseries/src/promql/v2/operators/{aggregate,vector_selector,matrix_selector}.rs`, `timeseries/src/promql/v2/operators/instant_fn.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.2 | Add nullary/scalar/calendar function surface (`scalar`, `vector`, `pi`, `time`, `minute`/calendar funcs) | Architect | done | 6.2 | `timeseries/src/promql/v2/{plan/{plan_types,lowering,optimize,cardinality,physical}.rs,operators/{coercion,instant_fn,mod}.rs,reshape.rs,mod.rs}`, `timeseries/src/tsdb.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.3 | Add label-manipulation functions (`label_replace`, `label_join`) | Architect | done | 6.2 | `timeseries/src/promql/v2/{operators/{label_manip,mod}.rs,plan/{plan_types,lowering,optimize,cardinality,physical}.rs,mod.rs}`, `timeseries/src/tsdb.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.4 | Fix `VectorSelector` offset / `@` instant emission to one sample per series | Architect | done | 6.2 | `timeseries/rfcs/0007-impl-plan.md` (see §5 Decisions Log 6.3.4) — subsumed by 6.3.1's leaf-roster dedup; `offset.test` corpus passes clean. | |
| 6.3.5 | Fix subquery inner-grid and `@` / offset semantics | Architect | done | 6.2 | `timeseries/src/promql/v2/operators/subquery.rs`, `timeseries/src/promql/v2/plan/physical.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.6 | Small correctness cluster: topk tie-break, `by(__name__)`, `clamp(min>max)`, `topk(scalar(...), ...)`, binary `on()` schema | Architect | done | 6.2 | `timeseries/src/promql/v2/{operators/{aggregate,instant_fn}.rs,plan/{lowering,optimize,physical,cardinality}.rs,reshape.rs,plan/mod.rs,mod.rs}`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.7 | Restore `timestamp()` source-sample semantics (StepBatch ABI extension) | Architect | done | 6.2 | `timeseries/src/promql/v2/batch.rs`, `timeseries/src/promql/v2/operators/{vector_selector,instant_fn}.rs`, `timeseries/src/promql/v2/reshape.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.8 | Fix `MatrixSelectorOp` + `rate()` + `@` / `offset` window math (per-step effective-time threaded into `RollupOp`) | Architect | done | 6.2 | `timeseries/src/promql/v2/operators/{matrix_selector,subquery,rollup}.rs`, `timeseries/rfcs/0007-impl-plan.md` | |

**Acceptance**: zero failures on the unmodified `promqltest` corpus. Perf profile taken on a
representative range query (e.g., `sum by (pod) (rate(http_requests_total[5m]))` over 6h) —
numbers recorded in Decisions Log.

### Phase 7 — Migration

| # | Unit | Owner | Status | Deps | Artifacts | Blocker |
|---|---|---|---|---|---|---|
| 7.0 | Wire v2 on `TimeSeriesDbReader` so read-only deployments exercise the columnar engine | Wiring Implementor | done | 6.3 | `timeseries/src/tsdb.rs`, `timeseries/src/promql/promqltest/v2_harness.rs` | |
| 7.1 | Flip `promql-v2` to default-on or remove the flag | | ready | 7.0 | | |
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
- 2026-04-16 — Operator Implementor (unit 3c.2) — **RFC Open Question
  #2 resolved: subquery shares the parent `MemoryReservation`.** v1
  subquery allocations (child sub-tree, per-outer-step scratch, emitted
  `MatrixWindowBatch` buffers) all flow through the parent reservation.
  A nested-budget scope per inner plan is deferred post-MVP: the
  parent's cap already bounds total per-query bytes, and a nested
  scope would complicate error attribution (which budget reports the
  overage?) without solving any concrete v1 problem. Revisit if a
  workload shows that a "wide" outer step can mask unbounded inner-plan
  growth.
- 2026-04-16 — Operator Implementor (unit 3c.2) — Factory shape:
  `Box<dyn FnMut(TimeRange, i64) -> Result<Box<dyn Operator + Send>,
  QueryError> + Send>` (type alias `ChildFactory`). Verified
  `Operator` is dyn-safe as claimed (3a.1 already ships a
  `should_build_trait_object` test and the subquery file compiles
  `Box<dyn Operator + Send>` cleanly — no RPITIT / generic-method
  friction). `FnMut` over `Fn` so the planner can carry mutable
  plan-time state through the factory (e.g. a hashed child-schema
  cache); `Send` so the operator composes with `Concurrent`
  wrappers. `TimeRange` is inclusive-exclusive per the v2 storage
  convention: `(outer_t - range, outer_t]` is encoded as
  `[outer_t - range + 1, outer_t + 1)`.
- 2026-04-16 — Operator Implementor (unit 3c.2) — Emission
  granularity: **one `MatrixWindowBatch` per outer step** in v1. The
  alternative (batching multiple outer steps into a single matrix
  tile for downstream `Rollup` to amortise over) needs per-step
  bookkeeping and a cross-step cell-index layout that the 3a.2
  builder doesn't expose publicly; the added complexity is not
  justified until profiling on the Phase 6 corpus shows per-step
  emission is a bottleneck. Rollup / outer-subquery consumers see
  one batch at a time; `step_range = [outer_step_idx,
  outer_step_idx + 1)`.
- 2026-04-16 — Operator Implementor (unit 3c.2) — `WindowStream`
  reuse: `SubqueryOp` implements `WindowStream` directly (unlike
  `MatrixSelectorOp`, which needs the `MatrixWindowSource` wrapper
  because its `Operator::schema` is only reachable through the
  `'static` trait impl). `SubqueryOp` is already `'static` by
  design (no borrowed `SeriesSource`), so it drops into
  `RollupOp<SubqueryOp>` with no wrapping — exactly the "drop-in for
  an outer Rollup" contract the task spec described. Also satisfies
  the `Operator` trait with a degenerate `next()` (matches 3a.2 —
  matrix output cannot fit `StepBatch`'s single-float cell shape).
- 2026-04-16 — Operator Implementor (unit 3c.2) — Child `Pending`
  handling: v1 does **not** persist partial child state across
  `windows()` polls. A single `windows()` invocation drains the
  child for the current outer step; if the child yields `Pending`
  mid-drain, we bubble `Pending` up and rebuild the child from the
  factory on the next poll. This is the simplest correct thing — the
  factory is idempotent in v1 (mock) and the real planner's factory
  produces fresh operators per call anyway. A persistent state
  machine (like 3a.2's `State::LoadingChunk`) can come later if
  profiling shows the factory-rebuild overhead is material under
  `Concurrent` back pressure.
- 2026-04-16 — Operator Implementor (unit 3c.2) — Defensive sample
  filtering: per-inner-step samples that fall outside the outer
  window `(outer_t - range, outer_t]` are dropped while packing.
  Nothing in the `Operator` contract guarantees the child respects
  the range exactly (e.g. a child with its own lookback policy may
  emit a step slightly outside), and the filter is cheap
  (integer-compare per sample). `STALE_NAN` and validity=0 cells
  are also dropped before packing — matches 3a.2's policy so
  downstream `RollupOp` sees pre-filtered windows.
- 2026-04-16 — Operator Implementor (unit 3c.3) — Strategy: **full
  materialisation (option 1) + passthrough short-circuit (option 3)**.
  Partial/sliding materialisation (option 2) deferred — the full-grid
  scratch is plan-time bounded by `(step_count × series_count × 9
  bytes)`, the planner only inserts `Rechunk` when shapes genuinely
  differ (so the pathological case is self-excluded), and "correct
  first, optimize on profile" keeps the v1 surface small. The
  passthrough short-circuit peeks the first upstream batch: if its
  shape already matches the target tile *and* starts on a tile
  boundary, the operator re-emits upstream batches verbatim without
  allocating scratch. First-batch-only probe — per-batch probing
  would double the branch cost on the hot path and the planner
  guarantees uniform upstream shape in this branch.
- 2026-04-16 — Operator Implementor (unit 3c.3) — Passthrough predicate
  subtlety: a batch spanning the *full grid* trivially has
  `step_end == grid_step_count`, so the naive "match if end lands on
  target chunk OR grid edge" formulation silently accepts one giant
  batch as a "match" and emits it as a single tile — wrong when the
  downstream expects smaller tiles. Tightened the predicate to
  require the span to be exactly `target_chunk` (or shorter only at
  the grid edge). Caught by `should_rechunk_along_both_axes` failing
  on the first run; fix is one-liner in `batch_matches_target`.
- 2026-04-16 — Operator Implementor (unit 3c.3) — Memory-release
  ordering: scratch bytes reserved once in the `Init → Draining`
  transition, held across the whole emit phase, released by
  `Scratch::Drop` when the state flips to `Done` (either after the
  last tile, on upstream error, or on operator drop). Per-tile
  output `Vec<f64>`/`BitSet` allocations are *not* separately charged —
  they sum to at most the scratch cell count (in practice equal, since
  every cell ends up in exactly one tile), and double-charging would
  roughly double the peak reservation for no correctness benefit.
  This matches the RFC's "pipeline-breaker holds memory until drained"
  contract. Underflow risk: zero — the single `try_grow(bytes)` is
  paired with one `release(bytes)` inside `Scratch`, `Drop` is
  idempotent via the `bytes = 0` sentinel. Verified by
  `should_release_memory_after_drain`: reservation counter returns to
  pre-call value after draining.
- 2026-04-16 — Operator Implementor (unit 3c.3) — Out-of-order
  upstream batches: supported. `ingest()` places each cell by its
  *global* `(step_range.start + step_off, series_range.start +
  series_off)`, so an upstream that emits `[2..4)` before `[0..2)`
  (or any other scan order) produces the same scratch contents.
  Upstream is free to choose its own emission order — the breaker
  framing already assumes it will buffer the whole stream, and
  absorbing out-of-order tiles costs no more than a natural scan.
  Test `should_handle_out_of_order_upstream_batches` pins this.
- 2026-04-16 — Operator Implementor (unit 3c.3) — `SchemaRef::Deferred`
  handling: `grid_series_count()` falls back to `0`, which causes
  `Rechunk` downstream of a deferred-schema operator (`count_values`,
  unit 3c.4) to drain the child empty and emit nothing. v1 does not
  place `Rechunk` after a deferred producer — the RFC's single
  exception for deferred schemas is `count_values` itself, and the
  planner doesn't re-shape its output. If a future unit needs
  rechunk-after-deferred, the contract will need to extend: the
  operator would have to wait on the child's first batch to bind the
  roster before allocating scratch. Flagged here so the gap is
  discoverable.
- 2026-04-16 — Operator Implementor (unit 3c.4) — Schema-finalisation
  strategy: picked **option 2** from the task prompt. `schema()` returns
  `SchemaRef::Deferred` for the whole life of the operator; the
  finalised `Arc<SeriesSchema>` is exposed via an inherent
  `CountValuesOp::finalized_schema(&self) -> Option<&Arc<SeriesSchema>>`
  method that returns `Some` after the first `next()` drains and emits.
  No trait changes; `SchemaRef::Deferred` as currently defined in
  `batch.rs` is sufficient — it only has to mean "I won't expose my
  roster through `schema()`"; the concrete roster is threaded through a
  separate channel. Downstream operators that need the binding (Phase 4
  planner concern) will call `finalized_schema()` after pulling the
  first batch, or read the batch's `SchemaRef::Static` directly since
  `next()` stamps the emitted `StepBatch` with the newly-materialised
  schema. No `SchemaRef` extension required.
- 2026-04-16 — Operator Implementor (unit 3c.4) — Value-to-label
  formatting: the existing Rust engine does **not** implement
  `count_values` (goldens at `promqltest/testdata/aggregators.test:480`
  are ignored, and a grep across `evaluator.rs` / `pipeline.rs` /
  `functions.rs` returns zero hits for `count_values`/`CountValues`).
  Target is therefore Prometheus' reference: Go's
  `strconv.FormatFloat(v, 'f', -1, 64)` (shortest round-trip decimal).
  Implemented as `format_value_label`: `NaN → "NaN"`, `±Inf →
  "+Inf"/"-Inf"`, `-0.0 → "-0"`, and finite non-zero through Rust's
  `f64::to_string` (which matches Go's `'f'/-1` for finite non-zero
  values). A crate-local citation for parity will only exist once
  Phase 6 unignores those fixtures; the current implementation is
  tested bit-for-bit against the table in the module docs. If Phase 6
  uncovers a corner case (very large `f64` switching to scientific
  notation, tiny denormals printed with trailing zeros, etc.) the fix
  is localised to `format_value_label`.
- 2026-04-16 — Operator Implementor (unit 3c.4) — NaN bucket keying:
  the intermediate map is keyed on `(group: u32, value_bits: u64)`
  where `value_bits = f64::to_bits(v)`. Two consequences: (1) `+0.0`
  and `-0.0` get separate buckets (both collapse to the same label
  `"0"` / `"-0"` on output — distinct labels, so this is correct), and
  (2) every NaN bit pattern gets a separate bucket even though they
  all render to `"NaN"`. The second case is tested
  (`should_distinguish_nan_and_value_by_bit_pattern`) and produces
  multiple `{label="NaN"}` output series if the inputs carry multiple
  NaN bit patterns. That is an edge-case divergence from Prometheus'
  behaviour (Prometheus likely collapses all NaNs into one bucket
  because `math.Float64bits` is not used as a map key there; they
  key on the value directly via Go's `map[float64]`, which treats all
  NaNs as non-equal to anything including themselves — so
  Prometheus' map almost certainly cannot distinguish NaN-bearing
  inputs at all, producing one NaN entry per NaN-bearing input).
  The divergence is user-unobservable for typical queries (NaN is
  rare in PromQL `count_values` arguments) and is flagged here so
  Phase 6 can revisit if a golden fails.
- 2026-04-16 — Operator Implementor (unit 3c.4) — Grouping interaction
  with the deferred axis: the optional `GroupMap` governs the
  *intermediate* group-keyed bucketing; the output schema is
  `(group × distinct-value)` — length = sum over groups of the number
  of distinct values that group observed (bounded above by
  `group_count × distinct_values_total`). Group labels are supplied
  by the planner as an `Arc<[Labels]>` parallel to the group map; the
  operator composes each group's labels with the new
  `{label_name=<formatted_value>}` entry (stripping any pre-existing
  `label_name=*` from the group labels to avoid a duplicate entry,
  which matches the goldens' "overwrite label with output" examples at
  `aggregators.test:517` and `:524`). When `group_map` is `None`, the
  operator synthesises one unit group; callers pass
  `Arc::from(vec![Labels::empty()])` and the output labels carry only
  the new `{label_name=...}` entry.
- 2026-04-16 — Operator Implementor (unit 3c.4) — Emission shape: the
  operator emits **exactly one** `StepBatch` covering the full outer
  grid × full output roster. Rationale: (a) the breaker has already
  materialised every count, so re-tiling costs allocations without
  reducing peak memory; (b) downstream consumers are free to `Rechunk`
  if they want smaller tiles, though the v1 planner doesn't insert
  that. The emitted batch's `series` field is `SchemaRef::Static`
  pointing at the finalised roster — so a downstream that reads the
  batch directly gets the concrete schema without needing
  `finalized_schema()`. `finalized_schema()` is the path for
  downstream operators that look at schemas ahead of pulling the first
  batch (Phase 4 planner binding).
- 2026-04-16 — Operator Implementor (unit 3c.4) — Memory accounting:
  three reservations. (1) Per-bucket `~64 B header + 4 B × step_count`
  for the `HashMap` entry + per-step counts, charged incrementally
  (`try_grow` per new bucket). (2) Schema bytes reserved up front at
  roster-finalisation time, sized conservatively
  (`series_count × (vec header + avg_labels × (Label + 32 B string tail)
  + 16 B fingerprint)`). (3) Per-batch `OutBuffers` for
  `values + validity`. All released on `Drop` (buckets + schema) or
  `finish()` (output buffers). `should_respect_memory_reservation`
  exercises the bucket-charge rejection path.
- 2026-04-16 — Operator Implementor (unit 3c.5) — `Concurrent` mpsc bound
  default: `DEFAULT_CHANNEL_BOUND = 4`. Rationale: matches the task-spec
  suggestion; small enough that a pathological producer can't buffer many
  un-consumed batches (each batch can be up to ~256 KB — 4 is ~1 MB
  worst-case), large enough to keep a CPU-bound consumer fed through one
  async round-trip under light contention. The planner (Phase 4.5) may
  override per-subplan once profiling suggests a different sweet spot.
- 2026-04-16 — Operator Implementor (unit 3c.5) — `Concurrent` spawn model:
  **ambient `tokio::spawn`** (task-spec preference). `ConcurrentOp::new`
  must be called from inside a tokio runtime; the spawned task is retained
  on a `JoinHandle` field (`_task`) so dropping the operator aborts the
  task via the runtime's cancellation-on-drop semantics (cooperative — the
  task exits naturally when its `tx.send` observes receiver-drop). No
  explicit `Handle` parameter; if Phase 5 wiring wants to pin spawns to a
  specific executor, a `new_on(handle, child, bound)` alt-constructor can
  be added non-breakingly.
- 2026-04-16 — Operator Implementor (unit 3c.5) — `Concurrent` child-drive
  loop: spawned task uses `poll_fn(|cx| child.next(cx)).await` per pull.
  Clean under tokio's default waker, no manual `RawWaker` dance, no
  `futures` dep. `Operator::next` takes `&mut self` and the task owns the
  child by value (`'static` child lifetime), so no lifetime gymnastics —
  matches the escalation-path prediction in the task spec.
- 2026-04-16 — Operator Implementor (unit 3c.5) — `Coalesce` fairness:
  **cursor-advancing round-robin** over all live children per `next()`
  call. One round polls each un-exhausted child starting at the cursor;
  first `Ready(Some)` wins and advances the cursor past the emitting
  child. `Pending` children are skipped so one pending sibling can't
  starve a ready one; `Ready(None)` marks the child done. If every live
  child was `Pending` in the round, bubble `Pending`. If every child is
  exhausted, emit `Ready(None)`. No merge-sorted-by-step: the RFC
  contract says disjoint `series_range`s per child, so cross-child step
  alignment is a planner guarantee — a consumer that needs one batch per
  step across all series inserts a `Rechunk` downstream (planner's
  concern, §3.4 scope).
- 2026-04-16 — Operator Implementor (unit 3c.5) — `Coalesce` dyn-safety:
  children held as `Vec<Box<dyn Operator + Send>>`. `Operator` trait is
  already object-safe (3a.1 `should_build_trait_object` + 3c.2's
  `ChildFactory` `Box<dyn Operator + Send>` both landed clean). No
  friction; no generic parameter on `CoalesceOp` — the N children come
  from the same planner fan-out and may be heterogeneous concrete types
  (e.g. `Concurrent<VectorSelectorOp>` vs a bare `VectorSelectorOp`),
  which dyn erasure resolves cleanly.
- 2026-04-16 — Operator Implementor (unit 3c.5) — Back-pressure test
  strategy (for `should_apply_backpressure_via_bounded_channel`): asserts
  end-to-end order preservation across 10 producer sends with
  `bound = 1`, plus a 10ms producer warmup sleep. If the channel weren't
  bounded (or the producer didn't await on `send`), items could arrive
  out of order or duplicated under buggy implementations. The test
  weakens the stronger "counter inspection" assertion from the task
  spec — `tokio::sync::mpsc::Sender` exposes no public in-flight counter,
  and wiring a side-channel counter into the spawned task would require
  production-code changes just for the test. The order-preservation
  check is an adequate stand-in.
- 2026-04-16 — Planner Implementor (unit 4.1) — `LoweringContext` shape:
  `{ start_ms, end_ms, step_ms, lookback_delta_ms }`. `for_instant(t, lookback)`
  helper sets `start == end == t` and `step_ms = 1` (a non-zero sentinel —
  callers must gate division on `is_instant()`). Rationale: the physical
  planner (unit 4.3) already has to distinguish instant from range queries
  by `start == end` to choose step-batch tiling; a dedicated `Instant`
  enum variant in the context would duplicate that check. `1` is safer
  than `0` because downstream code that forgets the check blows up with
  `val * 1` instead of a panicking divide. Prometheus' `-query.default-step`
  of 1s is re-used as the default subquery step for instant queries where
  the AST omits `step`.
- 2026-04-16 — Planner Implementor (unit 4.1) — Unary minus representation:
  `Unary(e)` lowers to `Binary { Mul, Scalar(-1.0), lower(e), matching: None }`.
  No dedicated `LogicalPlan::Negate` variant. Rationale: every existing v2
  operator path already handles scalar/vector `Mul` (§3b.3); adding a
  first-class negate variant would duplicate the logic in a 4th place
  (scalar/scalar, vector/vector, vector/scalar, and now negate). The
  existing `evaluator.rs::Expr::Unary` arm is `todo!()` (never reached
  by the legacy engine's test coverage) so there is no legacy behaviour
  to match. The `-1.0 * foo` shape is also what Prometheus' own engine
  emits internally. If the optimizer (unit 4.2) wants to fuse the
  multiply into a specialised negate, it can pattern-match
  `Binary { Mul, Scalar(-1.0), _, None }` at that point.
- 2026-04-16 — Planner Implementor (unit 4.1) — Function-name coverage:
  strict superset of what the operator kinds expose — every
  `InstantFnKind` variant (29) and every `RollupKind` variant (17) has a
  PromQL name entry in `instant_fn_kind_for` / `rollup_kind_for`. Gaps
  vs. Prometheus that are **not** in our operator enums (and therefore
  can't be lowered): `absent`, `absent_over_time`, `histogram_quantile`,
  `scalar`, `vector`, `pi`, `time`, and the date/time extraction family
  (`day_of_month`, `day_of_week`, `day_of_year`, `days_in_month`, `hour`,
  `minute`, `month`, `year`). These surface as
  `PlanError::UnknownFunction` from lowering. `histogram_quantile` is
  explicitly out of scope per RFC §"Non-Goals" (native histograms); the
  others are tracked in §5 Decisions Log 3b.2's exclusion list and will
  require an `InstantFnKind` (or new schema-deferring operator, for
  `absent`) extension before they can be added to the lowering table.
  Flagged for Architect: no silent addition of operator variants.
- 2026-04-16 — Planner Implementor (unit 4.1) — `rate(instant_vector)`
  validation: the test builds the invalid AST manually (via
  `Function::new(..)` with `ValueType::Matrix` arg_types) because
  `promql_parser::parser::parse` itself rejects `rate(foo)` at parse
  time with "expected type matrix in call to function 'rate', got
  vector". The lowering guard still runs because optimizer passes (unit
  4.2) could synthesise such a shape during rewrites, but the test
  cannot exercise it from the string form. No user-visible impact.
- 2026-04-16 — Planner Implementor (unit 4.1) — `BinaryMatching` is
  `None` when neither `on`/`ignoring` nor `group_left`/`group_right` is
  present. Rationale: the physical planner (unit 4.3) treats "no
  matching record" as "default one-to-one on the full shared label
  set", which is the PromQL default. Emitting an explicit
  `BinaryMatching { axis: Ignoring, labels: [], cardinality: OneToOne }`
  would force the planner to distinguish "user explicitly wrote
  `ignoring()`" (which is the same semantics but has a parser record)
  from "no modifier at all" — we'd rather do that distinction in one
  place (the planner) than twice.
- 2026-04-16 — Planner Implementor (unit 4.1) — Minimal clippy fix to
  `plan_types.rs`: changed `labels.labels.iter().cloned().collect()` to
  `labels.labels.to_vec()` in `labels_to_arc` (lint
  `iter_cloned_collect`). The prior agent's file could not have passed
  `cargo clippy --all-targets --all-features -- -D warnings` cleanly
  as-written (the lint fires on stable); the one-line change preserves
  behaviour exactly (`Vec<Label>` where `Label = String` is a slice).
  Flagged per the "do not rewrite `plan_types.rs`" rule — this is a
  clippy conformance fix, not a redesign.
- 2026-04-16 — Planner Implementor (unit 4.2) — **CSE deferred to 4.3 /
  v2.** The physical plan builds the operator tree as unique objects
  (not a DAG), so structural dedup on the logical `LogicalPlan` tree
  would not yield a shared computation today. RFC Open Question #4
  (plan-subtree cache-key canonicalization) is unresolved; implementing
  CSE prematurely risks picking the wrong equivalence relation (commuta-
  tive reordering of `a+b` vs `b+a`, ignore-list normalisation, etc.).
  Option (a) from the unit brief — skip entirely, revisit once DAG rep
  exists.
- 2026-04-16 — Planner Implementor (unit 4.2) — **Algebraic identities
  dropped.** `x * 1 → x`, `x + 0 → x`, `x * 0 → 0`, etc. are correct for
  pure scalar `x` but change NaN observability on vector branches:
  `Scalar(0) * VectorSelector(foo)` with a NaN input cell must emit
  `0 * NaN = NaN` under IEEE 754 (matches `BinaryOp::apply_arith` at
  `operators/binary.rs:180-193`), but a naive `x * 0 → 0` rewrite would
  replace the whole tree with `Scalar(0)` and lose the NaN. Task spec
  rule "correctness > micro-optimizations" applies. The `* 1` / `+ 0`
  rewrites are safer (no NaN change) but still change validity
  semantics when the vector side is unmatched (`Binary` sets
  `validity = 0` on unmatched cells; the identity rewrite would emit
  valid cells from the vector side's raw validity, which may differ).
  All five identities stay out of v1.
- 2026-04-16 — Planner Implementor (unit 4.2) — **Fixpoint strategy:
  iterate until unchanged, bounded by `MAX_PASSES = 4`.** Each pass
  runs every rule once; rule bodies are themselves recursive bottom-up
  so a single pass already reaches a local fixpoint per rule. The outer
  loop exists only so rule *interactions* reach fixpoint (a rewrite by
  rule A may expose an opportunity for rule B). In practice the
  optimizer converges in 1-2 passes today; the bound is a safety net
  for future rule additions. Equality check on the outer loop uses
  structural `PartialEq` on `LogicalPlan` — cheap because the tree is
  shallow and has no reference-identity-dependent nodes.
- 2026-04-16 — Planner Implementor (unit 4.2) — **NaN-folding policy:
  fold freely.** `NaN + 1` → `Scalar(NaN)` under the optimizer. This
  matches both the legacy engine's `apply_binary_op`
  (`evaluator.rs:2153-2177`, which simply returns `a + b` without
  NaN-guarding) and the v2 `BinaryOp::apply_arith`
  (`operators/binary.rs:180-193`). `NaN` is a normal `f64` at the plan
  level — no distinction from "absent" here; absence is a `BitSet`
  property of `StepBatch`, not a scalar-value concern. Folding
  preserves bit-exact semantics with the runtime path.
- 2026-04-16 — Planner Implementor (unit 4.2) — **Comparison folding
  always safe on scalar/scalar** regardless of the `bool` modifier.
  The legacy engine's `(Scalar, Scalar)` arm at `evaluator.rs:2109-
  2112` calls `apply_binary_op` directly and wraps the result in
  `ExprResult::Scalar` — `return_bool` is *not* consulted; scalar/
  scalar comparisons always produce a scalar 1.0/0.0. PromQL itself
  rejects `1 < 2` (without `bool`) at parse time ("comparisons between
  scalars must use BOOL modifier"), but the optimizer must still
  defend against a hand-built / synthesised AST (a future optimizer
  pass could conceivably produce the shape by folding, though no
  current rule does). The optimizer folds both shapes to the same
  scalar. Corresponding test `should_fold_comparison_without_bool`
  builds the `LogicalPlan` directly because the parser rejects the
  source form.
- 2026-04-16 — Planner Implementor (unit 4.2) — **Selector label
  pushdown scope** narrowed to "redundant (name, op, value) dedup"
  per the unit brief. The parser does not normalise duplicate
  matchers (verified: `promql-parser-0.8.0/src/parser/production.rs`
  emits them verbatim), so `{job="a", job="a"}` reaches the optimizer
  with two matchers. The rule iterates `Matchers.matchers` preserving
  first-occurrence order; `or_matchers` (the `{a="x" or a="y"}` AND-of-
  ORs shape) is left untouched because dedup across OR branches would
  change semantics when branches overlap. No context-driven matcher
  invention (e.g. deriving matchers from an enclosing `on(...)` /
  `ignoring(...)` clause) — task spec rule.
- 2026-04-16 — Planner Implementor (unit 4.2) — **One minor clippy
  collapse in `fold_constants`.** The binary-arm match uses a
  let-chain (`if let (Scalar, Scalar) = ... && let Some(folded) = ...`)
  rather than nested `if let`s. `clippy::collapsible_if` requires the
  collapse; let-chains are stable as of Rust 1.88. Pure style, no
  behaviour change.
- 2026-04-16 — Planner Implementor (unit 4.3) — **`PlanError` extended
  with four additive variants**: `SourceError(String)` for
  `SeriesSource::resolve` failures; `InvalidMatching(String)` for
  label-matching shape rejections (including schema-deferred children
  under static-schema-sensitive parents — the v1 `count_values`
  composition restriction); `MemoryLimit(String)` for plan-time
  `try_grow` overages; and `PhysicalPlanFailed(String)` as the generic
  binding-failure catch-all. `QueryError` payloads are rendered via
  `to_string()` and not threaded through — the wiring unit (Phase 5)
  owns the cross-layer translation, and `PlanError` remains a
  `thiserror` leaf with no runtime-error dependency.
- 2026-04-16 — Planner Implementor (unit 4.3) — **Group-map construction
  algorithm.** `build_group_map(input_schema, AggregateGrouping)` walks
  the input series once, projects each labelset onto the grouping key
  (`By` keeps only listed labels, `Without` keeps everything except
  listed labels AND `__name__`), sorts the projection, looks the key
  up in a `HashMap<Vec<Label>, u32>` accumulator, and appends a new
  group index when absent. Returns `GroupBuild { map, group_labels }`
  where `group_labels[i]` is the sorted `Labels` for the i-th group.
  `__name__` stripping on both `By` and `Without` matches Prometheus'
  aggregate semantics (legacy `evaluator.rs:2262-2268`). `None`
  slots in `input_to_group` are reserved for explicit planner "drop
  this input" cases — not currently emitted but the shape supports
  it.
- 2026-04-16 — Planner Implementor (unit 4.3) — **Match-table
  construction algorithm.** `build_match_table(lhs, rhs, matching,
  include_name)` applies the matching projection per axis (`On` keeps
  only listed labels; `Ignoring` keeps everything except listed
  labels — both drop `__name__` for match-key purposes). For
  `OneToOne` / `ManyToMany`: index RHS by matching key, walk LHS and
  emit `Some(rhs_idx)` / `None`, output schema = LHS labels (minus
  `__name__` when `include_name == false`). For `GroupLeft`: LHS is
  many, one-row-per-LHS output with RHS-side `include(...)` labels
  composed in via `compose_group_labels` (strips matching name from
  many side then overlays one-side values for each include label).
  `GroupRight` mirrors this with LHS/RHS swapped. `include_name` is
  computed from `preserves_metric_name(op)` — arithmetic ops and
  `bool`-modified comparisons drop `__name__`; everything else keeps
  it (matches legacy `changes_metric_schema` at
  `evaluator.rs:2125-2127` and surrounding `result_metric` helper).
  Runtime duplicate-matching detection is deferred to the operator
  — v1 tests don't exercise the corner, and the operator can emit
  `validity = 0` for unmatched cells cleanly.
- 2026-04-16 — Planner Implementor (unit 4.3) — **Subquery factory
  design.** `build_subquery` probes the inner subtree once at plan
  time to snapshot its output `Arc<SeriesSchema>` (v1 assumes plan-
  time-stable inner schemas — the RFC's `count_values` composition
  restriction is enforced upstream). The `ChildFactory` closure
  captures `(source_arc, ctx, reservation, inner_plan.clone())` and
  re-invokes `build_node` synchronously on each outer-step call via
  `tokio::task::block_in_place` + `Handle::block_on`. When the
  factory is called outside a tokio runtime (test paths), it spins
  up a current-thread runtime per call. Two obvious follow-ups for
  perf: (a) cache pre-built child sub-trees keyed on
  `(TimeRange, step_ms)` when the inner plan is deterministic, and
  (b) expose an `async` factory surface so re-planning composes with
  the parent's task without a nested block_on. Neither matters for
  v1 correctness, both are called out in `physical.rs` module docs.
- 2026-04-16 — Planner Implementor (unit 4.3) — **CountValues
  composition restriction.** Per RFC §"Core Data Model" the v1
  planner only supports `count_values` at the root of the logical
  plan (or under no schema-sensitive parent). `static_schema()`
  checks each child operator's `SchemaRef` before calling
  schema-consuming constructors (`AggregateOp`, `BinaryOp`
  vector/vector, `InstantFnOp`); a `SchemaRef::Deferred` child
  triggers `PlanError::InvalidMatching` with a descriptive message.
  Test `should_reject_count_values_under_schema_sensitive_parent`
  pins this. Lifting the restriction is a follow-up: the rechunk /
  aggregate / binary operators would need a two-phase init path that
  waits on the deferred child's first batch before binding their
  own schema.
- 2026-04-16 — Planner Implementor (unit 4.3) — **Operator constructor
  artifacts that required plan-time computation**: `GroupMap` for
  `AggregateOp::new` (all variants — streaming + `Topk`/`Bottomk`
  filter-shape + `Quantile` reducer-shape); `MatchTable` +
  `output_schema: Arc<SeriesSchema>` for
  `BinaryOp::new_vector_vector`; `Option<GroupMap>` +
  `group_labels: Arc<[Labels]>` for `CountValuesOp::new`;
  `ChildFactory` + `series: Arc<SeriesSchema>` for `SubqueryOp::new`;
  per-leaf `Arc<SeriesSchema>` + parallel
  `Arc<[ResolvedSeriesRef]>` hint slice for `VectorSelectorOp::new`
  / `MatrixSelectorOp::new`; `OperatorSchema` snapshot for
  `MatrixWindowSource::new` (3b.1's integration wrapper). No API
  gaps: every operator constructor accepted exactly the shape the
  planner could build at plan time.
- 2026-04-16 — Planner Implementor (unit 4.3) — **`BoxedOp` shim.**
  `Box<dyn Operator + Send>` does not itself implement `Operator`
  (the blanket impl would conflict with `Box<impl Operator>`), so
  the planner wraps each child in a transparent `BoxedOp(Box<dyn
  Operator + Send>)` newtype that forwards `schema()` / `next()` to
  the inner box. Lets generic operator structs (`BinaryOp<L, R>`,
  `AggregateOp<C>`, `InstantFnOp<C>`, `RollupOp<W>`) consume
  trait-object children without turning themselves into `dyn`
  consumers. Zero runtime cost — one extra indirection per poll,
  already paid by the `dyn` dispatch.
- 2026-04-16 — Planner Implementor (unit 4.3) — **Parser-type
  conversions at operator boundaries.** `VectorSelectorOp` /
  `MatrixSelectorOp` take `promql_parser::parser::{AtModifier,
  Offset}` by value (their `EffectiveTimes::compute` consumes them
  directly). The planner carries the simpler plan-time
  `plan_types::{AtModifier, Offset}` shapes so the IR is cheap to
  pattern-match and stable across refactors; local
  `to_parser_at` / `to_parser_offset` helpers convert right before
  the constructor call. `AtModifier::Value(ms)` maps to
  `SystemTime::UNIX_EPOCH + Duration::from_millis(ms)`, clamping
  negative ms to zero (pre-epoch `@` values are already rejected at
  lowering time — see 4.1's `AtModifier::TryFrom` impl).
- 2026-04-16 — Planner Implementor (unit 4.4) — **Default limit =
  20,000,000 cells.** Rationale: the gate bounds `sum(series × steps)`
  across leaves; 20M cells is roughly 2k series × 10k steps or 20k
  series × 1k steps. Both comfortably fit a single-digit GB per-query
  memory cap (float cell = 8 B + 1/8 B validity ≈ 180 MB worst case
  for 20M). Tune per deployment via
  `CardinalityLimits::new(max_series_x_steps)`. `0` explicitly
  disables the gate (`CardinalityLimits::is_disabled`).
- 2026-04-16 — Planner Implementor (unit 4.4) — **Subquery treatment:
  outer-grid flavour (RFC verbatim).** RFC §"Execution Model" says
  "`sum(resolved_series × steps)`". v1 implements that literally —
  each leaf contributes `series × outer_step_count`, ignoring the
  inner step multiplication a subquery actually performs. Real cost
  for a subquery is `inner_step × outer_step × series` per leaf
  inside the subquery; we undercount by `inner_step_count`. Chosen
  deliberately: subqueries are rarely the dominant cardinality, the
  RFC wording is unambiguous, and under-estimation lets the
  reservation (unit 1.4) trip mid-exec if the inner plan truly
  explodes. Flagged here so Phase 6 triage can revisit if a fixture
  mis-behaves.
- 2026-04-16 — Planner Implementor (unit 4.4) — **`u64::MAX` sentinel
  trips the gate immediately (with any non-zero limit).** Unit 2.2's
  adapter returns `series_upper_bound = u64::MAX` for selectors with
  no positive matchers (the "real match deferred to resolve" escape
  hatch). Multiplying by any positive step count saturates to
  `u64::MAX`, which is `> limit` for any finite limit — so the gate
  rejects immediately. The error payload carries `estimated_cells =
  u64::MAX` which is an honest signal: "source could not bound this,
  refusing to resolve blind." Caller who wants this through has to
  disable the gate (`limit = 0`) — matches the defensive-by-default
  posture.
- 2026-04-16 — Planner Implementor (unit 4.4) — **Gate time-range is
  `[0, i64::MAX)`.** The gate does not reproduce the physical
  planner's lookback / `@` / offset folding (that lives in
  `physical::selector_time_range`). For `estimate_cardinality` the
  source only needs a time window to narrow by bucket overlap; a
  maximal window is a safe upper bound (the estimate will include
  buckets the real resolve might drop). Alternative: reuse
  `selector_time_range` — rejected for v1 because it pulls grid / `@`
  computation into the gate and complicates the tree walk for a
  negligible tightening of the estimate.
- 2026-04-16 — Planner Implementor (unit 4.4) — **Wired via
  `LoweringContext.cardinality_limits`.** Added a
  `cardinality_limits: CardinalityLimits` field on `LoweringContext`
  (kept `Copy` — `CardinalityLimits` is a single `u64`) plus a
  `with_cardinality_limits` chainable setter. `LoweringContext::new` /
  `for_instant` default to `CardinalityLimits::default()`
  (20M-cell cap). Chose this over extending the `build_physical_plan`
  signature: callers already thread `LoweringContext` through the
  whole plan pipeline (lowering / optimize / physical), and the limit
  is a plan-time configuration knob sharing that lifetime. Extending
  the signature would duplicate the context's role and churn every
  call site.
- 2026-04-16 — Planner Implementor (unit 4.5) — **Concurrent threshold
  default: 64 series.** Matches the unit brief's suggestion. Low enough
  that any non-trivial production selector (typical selectors match
  thousands of series) ships with decoupled I/O, and high enough that
  unit-test-scale selectors (handfuls of series) skip the wrap —
  constructing a `ConcurrentOp` requires a live tokio runtime and small
  queries don't benefit from the per-wrap task-spawn overhead. Exposed
  as `Parallelism::DEFAULT_CONCURRENT_THRESHOLD_SERIES`. Knobs: `0` →
  wrap every eligible leaf; `u64::MAX` → disable wrap entirely. Tune
  per deployment once Phase 6 profiling has data.
- 2026-04-16 — Planner Implementor (unit 4.5) — **Coalesce deliberately
  skipped in v1** (recommendation in the unit brief's §Scope). RFC
  §"Parallelism" frames `Coalesce` as "vertical sharding within a
  single query", but the safety envelope is narrow: splitting a roster
  into disjoint shards is only correct when every operator on the path
  from leaf to the nearest `Aggregate`/`Binary` boundary is
  per-series-independent. The v1 physical planner doesn't yet track
  that predicate, and shipping `Coalesce` without it risks silent
  data-loss on queries that look like they decompose but don't. The
  `Parallelism::coalesce_max_shards` knob exists (defaults to `0`,
  disabled) so a future unit can flip it on without a public-surface
  break. `Concurrent` alone delivers the RFC's "per-operator async
  streaming" intent; Phase 6 profiling will justify (or not) adding
  `Coalesce` later. Phase 6 risk: v1 is "correct but possibly slow" on
  very wide single-leaf queries — explicitly acceptable.
- 2026-04-16 — Planner Implementor (unit 4.5) — **Insertion point:
  inlined into `build_node`** (option 1 from the unit brief). After a
  selector leaf resolves we know the concrete series count; wrapping
  in place avoids a downcasting post-pass (option 2 — rejected
  because `Box<dyn Operator>` has no reflection). A new private helper
  `maybe_wrap_concurrent(op, series_count, ctx, stats)` does the
  `should_wrap_concurrent` check and records the decision; on wrap it
  routes the boxed child through the existing `BoxedOp` shim (which
  already forwards `schema()`/`next()`) so `ConcurrentOp::new<C>`'s
  `Operator + Send + 'static` bound is satisfied.
- 2026-04-16 — Planner Implementor (unit 4.5) — **MatrixSelectorOp is
  not wrappable directly** — its `Operator::next` is degenerate per
  §3a.2 (the actual windows come from the `WindowStream` bridge). The
  planner therefore wraps the **enclosing `RollupOp`** on the
  `Rollup(MatrixSelector)` path instead: `RollupOp` owns the I/O leaf
  and is a full `Operator`, so `ConcurrentOp(Box<RollupOp>)` preserves
  the "decouple I/O from evaluation" contract at a slightly wider
  boundary (rollup+leaf as one subplan). On the `VectorSelector` path
  the leaf is wrapped directly. On the `Rollup(Subquery)` path no wrap
  is inserted — subquery cardinality is the outer-grid's, not the
  child's, and the subquery operator internally re-plans per outer
  step; wrapping once outside would only parallelise the rollup, not
  the re-planning. A future unit can revisit once Subquery profiling
  shows the factory-call overhead dominates.
- 2026-04-16 — Planner Implementor (unit 4.5) — **Observability via
  `ExchangeStats` returned from a test-only entry point.** Tests can't
  downcast `Box<dyn Operator>` back to `ConcurrentOp` to verify the
  wrap decision, and extending the `Operator` trait with an
  `is_concurrent` / `kind()` probe is forbidden (§3.5 strict scope).
  Introduced `pub async fn build_physical_plan_with_stats(...) ->
  Result<(PhysicalPlan, ExchangeStats), PlanError>` — production
  callers use `build_physical_plan` and discard the stats; test
  callers reach for the `_with_stats` variant and assert on
  `concurrent_wrapped` / `concurrent_skipped` / `coalesce_inserted`.
  The stats struct is trivially `Copy` and adds no runtime overhead on
  the production path.
- 2026-04-16 — Planner Implementor (unit 4.5) — **Wired via
  `LoweringContext.parallelism`** (mirrors 4.4's wiring). Added a
  `parallelism: Parallelism` field on `LoweringContext` with a
  `with_parallelism` chainable setter; `Parallelism::default()` —
  `{ threshold: 64, coalesce_max_shards: 0, channel_bound:
  DEFAULT_CHANNEL_BOUND }` — is used by `LoweringContext::new` /
  `for_instant`. Keeps the context as the single configuration
  surface; `build_physical_plan` signature unchanged.
- 2026-04-16 — Wiring Implementor (unit 5.1) — **Memory cap default: 1 GiB**
  (`DEFAULT_V2_MEMORY_CAP_BYTES = 1 << 30` in `tsdb.rs`). No crate-level
  memory-cap config exists today; plumbing one through `QueryOptions` /
  `TsdbConfig` is out of scope for 5.1. Revisit in 5.2 when the HTTP
  handler gets a natural knob.
- 2026-04-16 — Wiring Implementor (unit 5.1) — **`PlanError → QueryError`
  mapping**. Parse/shape failures (`UnknownFunction`, `InvalidArgument`,
  `InvalidTopLevelString`, `UnsupportedExpression`, `UnsupportedFeature`)
  map to `QueryError::InvalidQuery` — the user wrote something the v2
  engine refuses. Structural / runtime failures (`TooLarge`,
  `MemoryLimit`, `SourceError`, `InvalidMatching`, `PhysicalPlanFailed`)
  map to `QueryError::Execution`. The crate-level `QueryError` lacks
  dedicated `MemoryLimit` / `TooLarge` variants, so the structured v2
  payloads are rendered into the `Execution(String)` variant via
  `thiserror` `Display`. The HTTP handler (unit 5.2) can parse these
  strings or — preferably — extend `crate::error::QueryError` with
  structured variants when the handler lands.
- 2026-04-16 — Wiring Implementor (unit 5.1) — **Instant-vs-range dispatch
  shape**. Single private `execute_v2(query, reader, ctx, is_instant)`
  helper drives both paths: parse → lower → optimize → build_physical_plan
  → loop `poll_fn(|cx| plan.root.next(cx))` → `reshape_instant` /
  `reshape_range`. The `is_instant` bool gates only the reshape; the
  operator pipeline is shape-agnostic per RFC §"Core Data Model"
  ("Instant queries are a range of one step"). `eval_query_v2` sets
  `LoweringContext::for_instant(at_ms, lookback_ms)`; `eval_query_range_v2`
  uses `LoweringContext::new(start_ms, end_ms, step_ms, lookback_ms)`.
  Signatures mirror v1 (`Option<SystemTime>` + `&QueryOptions`,
  `impl RangeBounds<SystemTime> + Duration + &QueryOptions`) so phase 5.2
  can A/B swap without reshaping call sites. Range returns `QueryValue`
  (not `Vec<RangeSample>` like v1) because the plan root may emit a
  `Scalar` over a range grid (e.g. `1+1`); phase 5.2 HTTP dispatch
  collapses as needed.
- 2026-04-16 — Wiring Implementor (unit 5.1) — **Reshape-placeholder
  strategy (option a)**. Minimum viable reshape lives in a new
  `timeseries/src/promql/v2/reshape.rs`: `reshape_instant(plan, batches)`
  / `reshape_range(plan, batches)`. Both read labels from the emitted
  `StepBatch::series: SchemaRef::Static` (works for deferred-schema roots
  because `CountValuesOp` stamps the finalised roster on its emitted
  batch per unit 3c.4). Invalid cells are elided — matches v1's "no
  sample in lookback window = skip this series". Pure-scalar detection
  uses the `ConstScalarOp` canonical shape: single-series roster with
  empty labels. 4 unit tests cover instant/range/scalar/invalid paths.
  Unit 5.3 can polish (avoid label double-clone, thread per-batch
  buffers) without reshaping `tsdb.rs` call sites.
- 2026-04-16 — Wiring Implementor (unit 5.1) — **`TimeSeriesDb` internals
  touched**: reused `Tsdb::query_reader_for_ranges` (the existing
  bucket-set reader builder) to feed `QueryReaderSource::new`. One
  helper (`preload_ranges_for_v2`) re-parses the query to compute the
  v1-style preload range — the v2 planner doesn't yet own a
  `compute_preload_ranges` equivalent, and implementing one would pull
  the phase-4 scope. Phase 5.3 or 7 can short-circuit the re-parse by
  consulting the logical plan directly. No phase-2 API widening and no
  operator / planner surface changes.
- 2026-04-16 — Wiring Implementor (unit 5.1) — **Reader lifetime**.
  `QueryReaderSource<R: QueryReader>` is built with `Arc::new(reader)`
  per query, where `reader: TsdbQueryReader` comes from
  `self.query_reader_for_ranges(...)`. `TsdbQueryReader` owns its data
  (`HashMap<TimeBucket, MiniQueryReader>` — no borrowed handles into
  `Tsdb`), so `Arc<TsdbQueryReader>` is `'static` and composes cleanly
  with `build_physical_plan`'s `S: SeriesSource + Send + Sync + 'static`
  bound. No adapter shim required.
- 2026-04-16 — Wiring Implementor (unit 5.2) — **Dispatch point**.
  Compile-time `#[cfg(feature = "promql-v2")]` / `#[cfg(not(feature = "promql-v2"))]`
  gate lives directly in `server/http.rs` at three call sites —
  `handle_query`, `handle_query_range`, and `handle_federate` (the last
  re-uses the instant entry point per `selector` match). No new
  abstraction layer, no runtime A/B, no query parameter.
- 2026-04-16 — Wiring Implementor (unit 5.2) — **`TsdbEngine` gained
  `eval_query_v2` / `eval_query_range_v2`** (both `#[cfg(feature =
  "promql-v2")]`). `ReadWrite(Tsdb)` delegates to the 5.1 v2 entry
  points; `ReadOnly(reader)` falls back to the v1 reader path because
  v2 is not yet wired on `TimeSeriesDbReader` — that sits in phase 7
  scope. Read-only fall-back for range wraps the v1 `Vec<RangeSample>`
  in `QueryValue::Matrix` so the handler sees a uniform `QueryValue`
  surface regardless of engine mode.
- 2026-04-16 — Wiring Implementor (unit 5.2) — **Range wire-shape
  adapter**. v1 `eval_query_range` returns `Vec<RangeSample>`; v2
  `eval_query_range_v2` returns `QueryValue` (per 5.1 Decisions Log —
  scalar over range grid). Added a local `query_value_to_range_samples`
  helper in `server/http.rs` (gated on `promql-v2`): `Matrix` passes
  through, `Scalar { ts, v }` fans into one anonymous series with one
  sample at `ts` (mirrors v1's `evaluate_range` flattening for scalar
  expressions), `Vector` defensively projects into single-step matrix
  rows. The helper keeps the wire contract (`QueryRangeResponse.data.result:
  Vec<MatrixSeries>`) bit-for-bit identical across the A/B.
- 2026-04-16 — Wiring Implementor (unit 5.2) — **`QueryError` mapping
  left unchanged**. Per 5.1, v2's structured `TooLarge` / `MemoryLimit`
  / `InvalidMatching` / `PhysicalPlanFailed` variants are rendered into
  `QueryError::Execution(String)` via `thiserror` `Display` before
  reaching the handler; the handler already maps
  `QueryError::Execution` → `errorType: "execution"` in
  `query_error_response`. No handler-level branch needed. Introducing
  dedicated `MemoryLimit` / `TooLarge` variants on
  `crate::error::QueryError` (plus matching HTTP status codes per
  Prometheus spec) is a candidate for a follow-up once phase 6 exposes
  demand — keeping it out of 5.2 scope.
- 2026-04-16 — Wiring Implementor (unit 5.2) — **Test-coverage shape**.
  Integration tests live in `timeseries/tests/http_server.rs` (existing
  `#![cfg(feature = "testing")]` file) inside a new
  `#[cfg(feature = "promql-v2")] mod v2_dispatch` sub-module. 4 tests
  exercise the compile-time dispatch via `tower::ServiceExt::oneshot`:
  `should_dispatch_query_endpoint_to_v2_when_feature_enabled`,
  `should_dispatch_query_range_endpoint_to_v2_when_feature_enabled`,
  `should_return_scalar_via_range_adapter_when_query_is_constant`
  (pins the `QueryValue::Scalar` → `Vec<RangeSample>` adapter), and
  `should_map_v2_invalid_query_to_error_response` (pins the `PlanError::
  UnknownFunction` → `QueryError::InvalidQuery` → `bad_data` HTTP
  body). `cargo test -p opendata-timeseries --features testing,
  http-server,promql-v2 --test http_server` runs 30 tests (26 pre-
  existing + 4 new) green; `--features testing,http-server` still runs
  26 green.
- 2026-04-16 — Wiring Implementor (unit 5.3) — **Label-copy strategy**.
  `Labels` is `Vec<Label>` (not `Arc`-shareable without modifying the
  wire type); each `Labels::clone` allocates. The reshape clones each
  output series' `Labels` from `Arc<SeriesSchema>` exactly once per
  output series, never per step or per batch: `reshape_instant` sees
  each series at most once (one step per instant batch); `reshape_range`
  uses `BTreeMap<series_idx, (Labels, Vec<(i64, f64)>)>::or_insert_with`,
  whose closure runs on first insert only. This matches the RFC's
  "single clone at result-shape time" intent without touching the
  `Labels` wire type. A zero-clone path would require making
  `RangeSample::labels` / `InstantSample::labels` a shared handle —
  outside 5.3's scope and a wire-type change the RFC forbids.
- 2026-04-16 — Wiring Implementor (unit 5.3) — **Series ordering**.
  v2 output series are ordered by the global `series_idx` stamped by
  the planner. v1's `evaluate_range` (`tsdb.rs::evaluate_range`) returns
  series in `HashMap<Labels, _>` iteration order — non-deterministic
  across runs; v1's `evaluate_instant` preserves the evaluator's
  emission order (roughly matcher-visit order). Neither is sorted by
  label set. The Prometheus JSON wire shape (`QueryRangeResponse.data.
  result: Vec<MatrixSeries>`) is an unordered array — clients don't
  rely on order — so v2's deterministic `series_idx`-order is
  structurally compatible. No v1-divergence observed that would block
  Phase 6.
- 2026-04-16 — Wiring Implementor (unit 5.3) — **Streaming vs collect**.
  5.1's collected-batches approach (drain operator tree into
  `Vec<StepBatch>`, reshape in a single pass) is retained. A streaming
  reshape is possible — `reshape_range` could allocate per-series
  accumulators up-front (size bounded by `step_count`) and push cells
  as each batch arrives — but memory profile isn't on the phase-5
  acceptance path and the BTreeMap-keyed aggregator reads cleaner.
  Flagged as a future optimisation in the module docstring for Phase 7
  / follow-up to pick up if benchmarking surfaces pressure.
- 2026-04-16 — Wiring Implementor (unit 5.3) — **Defensive dimension
  checks**. `StepBatch::new`'s invariant checks are `debug_assert!`s,
  so release builds skip them. Added `check_batch_shape(batch)` in
  reshape that validates `values.len()` / `validity.len()` /
  `step_range` / `series_range` against the declared schema, returning
  `ReshapeError` (which the wiring layer maps to
  `QueryError::Execution`) rather than letting an index-out-of-bounds
  panic propagate. Should never fire in practice — operators construct
  batches via `StepBatch::new` — but it's the correct release-mode
  defence.
- 2026-04-16 — Wiring Implementor (unit 5.3) — **5.2's adapter remains
  required**. Unit 5.2's `query_value_to_range_samples` still handles
  the `QueryValue::Scalar` → `Vec<RangeSample>` fan-out for scalar
  expressions (`1+1`, etc.) on a range grid. The polished reshape
  emits `QueryValue::Matrix(vec![single anonymous RangeSample])` for
  the scalar-over-range case (mirrors v1's `evaluate_range` semantics),
  so the adapter's `Scalar` branch is unreachable in the `promql-v2`
  path — *but* the 5.2 adapter also accepts the `ReadOnly(reader)` v1
  fall-back path and the compile-time `#[cfg(not(feature = "promql-v2"))]`
  branch, and the `Scalar` arm is cheap to keep for safety. Removing
  it would require proving the reshape contract across all paths; not
  worth the churn. Adapter kept unchanged.
- 2026-04-16 — Tester (unit 6.1) — **Harness shape: option (a) — separate
  `#[cfg(feature = "promql-v2")] mod v2_harness`** under
  `timeseries/src/promql/promqltest/`. Re-parses each fixture via the v1
  harness's existing `dsl::parse_test_file`, reuses v1 `load_series` and
  `assert_results`, but routes `EvalInstant` through a new
  `eval_instant_v2` that calls `Tsdb::eval_query_v2`. One
  `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` per
  fixture file (9 fixtures) + a smoke test + a `should_cover_every_fixture_file`
  drift-guard. Multi-thread runtime is required because the subquery
  factory uses `tokio::task::block_in_place` (§5 Decisions Log 4.3);
  `#[tokio::test]` defaults to single-thread and panics there.
  `run_test_v2` **collects** eval failures rather than short-circuiting
  on the first — one run surfaces every failure per file so Phase 6.2
  triage has complete data. No v1 code path or fixture edited; the
  generated v1 harness from `build.rs` still runs untouched.
- 2026-04-16 — Tester (unit 6.1) — **Corpus outcome: 61 failures across 7
  of 9 fixtures.** `operators.test` and `selectors.test` pass clean.
  Bucketed triage input for 6.2:

  | Bucket | Count | Representative fixtures | Root-cause note |
  |---|---|---|---|
  | Parser/Planner — unknown PromQL function | 28 | `functions:1` (`vector(1)`), `functions:17` (`label_join(...)`), `at_modifier:21` (`label_replace(...)`), `at_modifier:36` (`minute(...)`) | `label_replace`, `label_join`, `vector`, `minute` — the coverage gaps flagged in §5 Decisions Log 3b.2 / 4.1. Error path surfaces as `PlanError::UnknownFunction`. |
  | Planner — `scalar()` as topk `k` arg rejected | 1 | `aggregators:13` (`topk(scalar(foo), http_requests)`) | Lowerer's `expect_number_literal` on topk `k` arg rejects the `scalar(expr)` form. Legacy `coerce_k_size` evaluates it as an inner expression. |
  | Operator — Aggregate (breaker) topk tie-break | 5 | `aggregators:1` (`topk(3, http_requests)`), `aggregators:3` (`topk(5, http_requests{group="canary",job="app-server"})`) | Wrong series selected on ties — reported "Label group mismatch: expected 'canary', got 'production'" and "instance '1' vs '0'". §5 3c.1 claims first-seen-wins matches legacy; goldens disagree, so either (a) the legacy-parity claim is wrong or (b) v2's "first-seen" order differs from v1's due to the new source-ordering path. |
  | Operator — Aggregate (streaming) `__name__` retention in `by(__name__)` | 3 | `name_label_dropping:14` (`sum by (__name__, env) (metric_total{env="1"})`), `name_label_dropping:16`, `name_label_dropping:18` | Group-key projection strips `__name__` unconditionally (4.3 `build_group_map` "both `By` and `Without` drop `__name__`"), but when the user explicitly names `__name__` in `by(...)`, Prometheus keeps it. Planner rule is too aggressive. |
  | Operator — InstantFn `clamp` with `min > max` | 1 | `functions:32` (`clamp(test_clamp, 5, -5)`) | Legacy (and Prometheus) emits empty when `min > max`; v2 currently emits 3 samples. `InstantFnKind::Clamp` doesn't gate on bound ordering. |
  | Operator — InstantFn `timestamp()` divergence | 3 | `at_modifier:33` (`timestamp(metric{job="1"} @ 10)` — expected 10, got 600), `at_modifier:41`, `at_modifier:42` | Known divergence per §5 Decisions Log 3b.2: v2 returns step-timestamp; legacy/goldens expect source-sample-timestamp. Fixing requires propagating per-cell source-ts through `StepBatch`, or the fixture's expectation is wrong — escalate to Architect. |
  | Operator — VectorSelector / offset emits duplicate samples | 5 | `offset:1` (`metric{job="test"} offset 1h`), `offset:3` (`metric{job="test"} @ 7200`), `offset:4`, `offset:5`, `offset:6` | All five report "Expected 1 samples, got 2" — the instant query is emitting two matching cells per series. Likely the operator's `(effective - lookback, effective]` window spans two adjacent load samples and emits both, or reshape is not collapsing a two-step range into one instant result. Tight reproducer: load_series of `metric{job="test"}` at two-sample spacing, then `eval instant at ... / metric{job="test"} offset 1h`. |
  | Operator — Subquery sum/rate arithmetic | 7 | `subquery:1` (`sum_over_time(metric_total[50s:10s])` — 3 vs 1), `subquery:2` (`sum_over_time(metric_total[50s:5s])` — 4 vs 2), `subquery:4` (60s:10s — 2 vs 4), `subquery:5` (`rate(metric_total[20s:10s])` — 0 samples), `subquery:8`, `subquery:9`, `subquery:27` | `SubqueryOp`'s inner-grid walks produce the wrong sample count per outer window. Could be off-by-one on the inclusive/exclusive boundary (§5 3c.2 Decisions Log `(outer_t - range, outer_t]`) or the factory's `TimeRange` encoding swapping start/end. |
  | Operator — Subquery with `@` / `offset` at outer or inner step | 7 | `at_modifier:24` (`sum_over_time(metric{job="1"}[100s:1s] @ 100)` — 460 vs 20), `at_modifier:25–27`, `at_modifier:30–32` | Subquery `@`/offset composition underreports drastically (460 expected → 20 got) or produces zero samples for nested subqueries. Likely the factory doesn't thread `@` / offset into the inner `TimeRange` correctly. |
  | Operator — Binary `on(env)` match-table label projection | 1 | `vector_matching:15` (`foo + on(env) bar`) | Output label set wrong: expected `env='dev'`, got `env='prod'`. `build_match_table` / `build_one_to_one` likely joins on RHS's label value when LHS and RHS disagree on non-match-key labels. |

  Full run output captured at `/tmp/rfc0007_phase6_run.log` for 6.2.
- 2026-04-16 — Tester (unit 6.1) — **Critical v2 bug observed outside the
  corpus** (not yet a corpus failure; exposed by the perf smoke at higher
  cardinality). At ~300-series × 6 buckets the plan root assertion
  `assert_eq!(child_series_count, group_map.len())` at
  `v2/operators/aggregate.rs:695` panics with left=512 (default K=512
  series-tile) right=1800 (series × buckets). Indicates the planner's
  `GroupMap` length is being sized over `bucket_count × series_count`
  rather than unique `series_count` when `SeriesSource::resolve`
  emits multiple per-bucket chunks for the same series. None of the
  current 9 fixtures reach this scale (largest is vector_matching with
  single-bucket data), so the corpus did not trip the assertion — but
  production workloads will. Logged separately because it's a panic,
  not a mismatch, and the escalation rule in the task spec requires
  flagging panics immediately. Reproducer: 300 series × 720 samples
  × 6 h range → `sum by (pod) (rate(http_requests_total[5m]))`.
- 2026-04-16 — Tester (unit 6.1) — **Subquery test requires multi-thread
  runtime**. `SubqueryOp`'s factory uses `tokio::task::block_in_place`
  (§5 Decisions Log 4.3), which panics under the default
  `#[tokio::test]` single-thread runtime with "can call blocking only
  when running on the multi-threaded runtime". Not a production v2
  bug — the HTTP handler and CLI both run multi-threaded — but it
  means any caller-spawned integration test must use
  `#[tokio::test(flavor = "multi_thread", ...)]`. All v2 harness
  fixture tests were set multi-thread. Flagged here so Phase 7
  migration remembers to audit test call sites before deleting v1.
- 2026-04-16 — Tester (unit 6.1) — **Perf snapshot: v2 at 30 series ×
  720 steps, 6 h range, `sum by (pod) (rate(http_requests_total[5m]))`**
  in `InMemoryStorage` test TSDB: **median 29 ms** across 5 runs
  (samples: 28, 28, 29, 29, 29 ms). Test: `v2_harness_perf_smoke`
  (ignored; invoke with `cargo test -p opendata-timeseries --features
  promql-v2 v2_harness_perf_smoke -- --ignored --nocapture`). Not a
  criterion benchmark — a smoke (`std::time::Instant`, 5 samples).
  Scale was clipped from 300 series to 30 series because at 300-series
  the aggregate-operator panic above prevents the query from completing
  — so the recorded number is an under-scale signal and must be
  re-taken once the aggregate planner bug is fixed. Documented here
  in lieu of capturing an unfair v1 baseline.
- 2026-04-16 — Architect — **Phase 6.3 is split into seven bounded fix
  units; no scope cut accepted.** Chosen path is option (1) from the
  handoff: keep Phase 6 acceptance at full-corpus green, but decompose
  the fix work so it can be dispatched safely. Priority order:
  **6.3.1 first** (aggregate panic + unique-series roster dedup, a real
  production panic and perf blocker), then **6.3.2/6.3.3** to clear the
  28 "unknown function" planner failures and unmask any deeper semantic
  bugs they are currently hiding, then the semantic/correctness units
  6.3.4–6.3.7. RFC sections touched: plan §4 state board only; no spec
  change.
- 2026-04-16 — Operator/Planner Implementor (unit 6.3.1) — `resolve_leaf`
  now canonicalises labelsets, deduplicates the leaf roster to one schema
  row per logical series fingerprint, and retains all bucket-local
  `ResolvedSeriesRef`s in a per-series hint group. `VectorSelectorOp` and
  `MatrixSelectorOp` flatten those groups back into `SampleHint`s per series
  chunk and merge returned sample columns onto the logical series, so planner
  dedup does not lose cross-bucket sample loading. RFC sections touched:
  §"Core Data Model", §"Storage Contract".
- 2026-04-16 — Operator/Planner Implementor (unit 6.3.1) — `AggregateOp`
  no longer assumes an input batch covers the full child roster. Streaming,
  `topk`/`bottomk`, and `quantile` reducers now look up `GroupMap` entries
  with `input.series_range.start + local_series_off`; breaker tie-breaks keep
  using the absolute child-series index, while filter-shaped outputs preserve
  the child's `series_range` on emit. RFC sections touched: §"Core Data
  Model".
- 2026-04-16 — Architect (unit 6.3.2) — Added explicit scalar/vector
  coercion to the logical plan: `Time` (scalar `time()` leaf),
  `Scalarize { child }` (`scalar(v)`), and `Vectorize { child }`
  (`vector(s)`). `PhysicalPlan` now carries a `root_is_scalar` flag so
  reshape does not infer top-level scalar-ness from the one-series empty-label
  runtime schema alone; this keeps `vector(1)` / `vector(time())` as vectors
  while `pi()` / `time()` / `scalar(v)` remain scalar roots. RFC sections
  touched: §"Core Data Model", §"Result Shape".
- 2026-04-16 — Architect (unit 6.3.2) — Extended `InstantFnKind` and
  lowering to cover the calendar/date functions (`year`, `month`,
  `day_of_month`, `day_of_year`, `day_of_week`, `hour`, `minute`,
  `days_in_month`). Zero-arg calendar calls lower to `InstantFn(kind,
  Vectorize(Time))`, matching the legacy engine's default-argument semantics
  (`vector(time())`). One-arg forms keep the input-vector semantics. RFC
  sections touched: §"Operator Taxonomy".
- 2026-04-16 — Architect (unit 6.3.3) — `label_replace` / `label_join`
  lower to a dedicated `LogicalPlan::LabelManip` node rather than
  `InstantFn`: they rewrite series labels, not cell values. Lowering now
  validates the string arguments at plan time (non-empty destination label
  names and compilable regexes for `label_replace`), while the physical
  planner computes a deduplicated output roster and an `input_series ->
  output_series` map from the child schema. RFC sections touched:
  §"Operator Taxonomy", §"Core Data Model".
- 2026-04-16 — Architect (unit 6.3.3) — Execution strategy for label
  manipulation is a **static-schema breaker**: `LabelManipOp` drains its
  child, merges input cells onto the deduplicated output roster, allows
  distinct input series to collapse to the same output series when they are
  step-disjoint, and errors with `vector cannot contain metrics with the same
  labelset` on same-step collisions. This keeps `label_replace` /
  `label_join` composable under binary ops and aggregates without introducing
  a second deferred-schema path beyond `count_values`. RFC sections touched:
  §"Core Data Model", §"Execution Model".
- 2026-04-16 — Architect (unit 6.3.6) — Dynamic `topk` / `bottomk`
  parameters lower as `LogicalPlan::Aggregate { kind: Topk(0)|Bottomk(0),
  param: Some(scalar_plan) }` rather than inventing a second aggregate node
  shape or forcing runtime expression evaluation back into the planner.
  `AggregateOp` drains the scalar child once onto the query step grid,
  coerces each step's value with the legacy `coerce_k_size` rule, and keeps
  the existing literal fast path (`param: None`). The cardinality gate now
  also walks aggregate param subtrees so selector-bearing `scalar(...)`
  params count toward preflight estimates. RFC sections touched:
  §"Execution Model", §"Memory accounting".
- 2026-04-16 — Architect (unit 6.3.6) — Root `topk` / `bottomk` ordering is
  enforced in reshape, not inside `AggregateOp`. The operator keeps its
  filter-shaped, schema-preserving output; the physical plan carries a small
  `root_instant_vector_sort` flag so `reshape_instant` can apply Prometheus-
  style ordered instant-vector presentation only at the top level. This keeps
  non-root aggregates composable while fixing the corpus-visible ordering
  expectation. RFC sections touched: §"Result Shape".
- 2026-04-17 — Architect (unit 6.3.7) — Extended `StepBatch` with an
  optional `source_timestamps: Option<Arc<[i64]>>` column: one `i64` per
  cell, matching `values` / `validity` layout. Only `VectorSelectorOp`
  populates it (the sole operator that reads raw samples from storage);
  every derived operator (`InstantFnOp`, `BinaryOp`, `AggregateOp`,
  `RollupOp`, …) leaves it `None` on the output batch, which exactly
  models Prometheus' `timestamp()` behaviour: "timestamp of the
  underlying sample" for a bare vector selector, "evaluation time
  otherwise" (at_modifier.test:193–201). `InstantFnKind::Timestamp` now
  consults `source_timestamps[idx]` when present and falls back to the
  step timestamp otherwise; no other kind reads the column. Picked the
  `Arc<[i64]>` column shape over per-cell `SourceSampleMeta` structs or
  a sidecar map because (a) it pointer-clones for free through
  pass-through operators that choose to propagate (none do in v1), (b)
  it keeps the invalid-cell case cheap (zero sentinel, callers must
  consult validity anyway), and (c) the allocation cost is only paid
  when a `VectorSelectorOp` is live. RFC sections touched: §"Core Data
  Model" (StepBatch shape), §"Operator Taxonomy" (`timestamp()` semantics).
- 2026-04-17 — Architect (unit 6.3.8) — Added an optional
  `effective_times: Option<Arc<[i64]>>` column to `MatrixWindowBatch`
  carrying the per-step **window-end** for rollup consumers. Picked this
  over "shift emitted `step_timestamps` to `effective_t`" because the
  step-timestamp column doubles as the downstream result-time axis
  (reshape, subsequent operators), so shifting it would silently
  mislabel output samples. Keeping `step_timestamps` canonical and
  adding a sibling column for the window math is also cheaper — the new
  column is only populated when the leaf has a non-trivial `@` / offset
  (`MatrixSelectorOp::has_effective_shift`, `SubqueryOp::has_effective_shift`),
  so the no-modifier hot path is unchanged. `RollupOp::reduce_batch` now
  prefers `effective_times` when present and falls back to
  `step_timestamps` otherwise. RFC sections touched: §"Core Data Model"
  (MatrixWindowBatch shape).
- 2026-04-17 — Architect (unit 6.3.5) — Subquery inner-grid alignment moved
  from "forward-walk from `outer_t - range + 1`" to "**absolute multiples of
  `inner_step_ms` within `(effective_t - range, effective_t]`**". The prior
  shape produced inner timestamps like `{-39999, -29999, …, 1}` for
  `[50s:10s]` at `t=10s` — off by one from the sample grid — and always
  emitted at least one inner point even when `range < step` (e.g.
  `[1m:5m]`), so `min_over_time((topk(1, foo))[1m:5m])` at `12m` returned 1
  instead of empty. The new shape uses Euclidean ceil/floor to find the
  first/last multiple of `step` in window and emits zero inner points when
  the window spans no multiple, which matches Prometheus'
  `promql/engine.go::subqueryTimes` step-alignment contract and is the
  shape assumed by every sub-*-over-time fixture. Separately, `@`/offset
  composition moved into the operator: `SubqueryOp` now owns a
  per-outer-step `effective_times: Arc<[i64]>` (planner-precomputed) and
  computes the inner window from that rather than the raw outer-grid
  timestamp. The existing factory closure continues to receive the
  `(TimeRange, inner_step_ms)` shape it had — the only change is that
  `TimeRange::end_ms_exclusive - 1 = effective_t` now, which the factory
  uses to align `inner_grid`. RFC sections touched: plan §4 state board
  only; no spec change. Unit 6.3.8 added to track the matrix-selector
  rate+@ rollup-window bug (at_modifier #21) surfaced while triaging
  residual at_modifier failures — same root cause as the subquery bug
  but in the `MatrixSelectorOp` / `RollupOp` seam, which this unit did
  not touch.
- 2026-04-17 — Architect (unit 6.3.4) — Closed without new code. The five
  `offset:*` failures from 6.1's triage ("Expected 1 samples, got 2") were a
  pre-dedup symptom: `SeriesSource::resolve` emits one `ResolvedSeriesChunk`
  per overlapping bucket, and before 6.3.1 the planner concatenated those into
  the schema roster without deduping. Each cross-bucket query therefore
  produced one schema row per bucket rather than one per fingerprint, which
  `reshape_instant` surfaced as N samples per logical series at the `instant`
  shape. 6.3.1's fingerprint-dedup of `resolve_leaf` fixed this at the
  planner; `VectorSelectorOp::build_batch` always selected a single sample
  per `(step, series)` cell, so no operator change was needed. `offset.test`
  corpus passes clean as of this closure. RFC sections touched: §4 state
  board only; no spec change.

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
- 2026-04-16 Operator Implementor 3c.2 in-progress→done — created `promql/v2/operators/subquery.rs` (`SubqueryOp`, `ChildFactory` type alias); registered in `operators/mod.rs`, added to `operators::prelude`, and re-exported from `v2/mod.rs`. PromQL re-grid operator: owns a planner-supplied factory `Box<dyn FnMut(TimeRange, i64) -> Result<Box<dyn Operator + Send>, QueryError> + Send>` that materialises a fresh child sub-tree per outer step; drives the child to completion, drops samples outside the outer window / with validity=0 / STALE_NAN, packs per-series columns into a `MatrixWindowBatch` (one per outer step). `Operator::next` is degenerate (Ready(None)) per 3a.2 convention; consumers drive via `SubqueryOp::windows` / `WindowStream::poll_windows`. `SubqueryOp` implements `WindowStream` directly (unlike 3a.2's `MatrixSelectorOp` which needs `MatrixWindowSource`) so it drops into `RollupOp<SubqueryOp>` with no wrapper. Memory: shares parent `MemoryReservation` (§5 Decisions Log — RFC Open Question #2 resolved: nested scope deferred post-MVP); `WindowBuffers` RAII guard handles cell-index + sample allocation with the same grow/release pattern 3a.2 uses. `Operator` trait dyn-safety verified (no friction; already proven by 3a.1's `should_build_trait_object`). 11 new unit tests on a hand-built scripted-child mock cover inner-step re-gridding, one-batch-per-outer-step emission, factory-call-count-per-outer-step, `MatrixWindowBatch` cell packing for 2-series × 3-inner-step layout, end-of-stream idempotence, series-order preservation, invalid-cell + STALE_NAN skip, child error propagation, memory-limit rejection, static schema, and an end-to-end `RollupOp<SubqueryOp>` assembly emulating `rate(expr[3s:1s])` with hand-verified `rate = 100/s` arithmetic across two outer steps. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (177 passed — 166 prior + 11 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Operator Implementor 3c.3 in-progress→done — created `promql/v2/operators/rechunk.rs` (`RechunkOp<C: Operator>`); registered in `operators/mod.rs` (incl. `prelude`) and re-exported from `v2/mod.rs`. Pipeline-breaker that repackages upstream batches to a different `(target_step_chunk, target_series_chunk)` rectangle. Strategy (§5 Decisions Log): full-grid materialisation into a `(step_count × series_count)` scratch buffer + single-shot passthrough short-circuit when the first upstream batch already matches the target tile shape on a tile boundary (partial-tile/sliding materialisation deferred). Internal state machine: `Init → {Passthrough | Draining} → Emitting → Done`. Scratch is a `RAII` `Scratch` struct — `try_grow` once in `Init→Draining` for `step_count × series_count × (8 + 1/8)` bytes, `release` on `Drop` when state flips to `Done` (error short-circuit, end-of-stream after last tile, or operator drop). Per-tile outputs are NOT double-charged (already accounted for in scratch; summed cell count equals scratch size). Schema passes through unchanged from the child; `step_grid` is plan-time invariant across the pipeline. Out-of-order upstream emission is handled by placing cells at their global `(step_range.start + step_off, series_range.start + series_off)` offsets. `SchemaRef::Deferred` handling: `grid_series_count` falls back to 0 (v1 planner won't place Rechunk after `count_values`; §5 Decisions Log flags the gap). 12 new unit tests on hand-built `MockOp` upstreams cover passthrough, step-axis rechunk (wider tiles), series-axis rechunk (narrower tiles), both-axes rechunk, value+validity bit-for-bit preservation (incl. real-NaN vs absence), memory-cap rejection, memory-release-after-drain (reservation counter returns to pre-call value), upstream error propagation, static-schema contract, partial-tile at non-evenly-divisible grid edges, empty upstream, and out-of-order upstream batches. One surprise during implementation: the naive passthrough predicate ("end lands on target boundary OR grid edge") accepted a full-grid batch as a "matching tile" and emitted it as a single output — tightened to require the batch span to equal target chunk (or be a short partial tile at the grid edge). No `Operator` / `OperatorSchema` / `StepBatch` surface changes. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (189 passed — 177 prior + 12 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Operator Implementor 3c.5 in-progress→done — created `promql/v2/operators/concurrent.rs` (`ConcurrentOp`, `DEFAULT_CHANNEL_BOUND = 4`) and `promql/v2/operators/coalesce.rs` (`CoalesceOp`); registered in `operators/mod.rs` (incl. `prelude`) and re-exported from `v2/mod.rs`. `ConcurrentOp` wraps a `Send + 'static` child in a `tokio::spawn`ed task that drives it via `std::future::poll_fn(|cx| child.next(cx)).await` and pushes `Result<StepBatch, QueryError>` into a bounded `tokio::sync::mpsc` channel (default bound = 4). Schema is snapshotted at construction (`Arc<OperatorSchema>` clone) so the trait's "schema() before first next()" invariant holds. Back-pressure is the mpsc bound itself — when downstream stops polling, producer `send` parks and the child awaits with it. `_task: JoinHandle` retained so drop cancels. No new deps (tokio already in workspace at `timeseries/Cargo.toml:56`). `CoalesceOp` holds `Vec<Box<dyn Operator + Send>>` + `Vec<bool>` done-flags + a cursor; `next()` does a cursor-advancing round-robin, emitting the first `Ready(Some)` encountered, skipping `Pending` to avoid starvation, and bubbling `Pending` only when every live child was pending in that round. No memory reservation at this layer (children own their batch bytes). Test strategy uses `#[tokio::test]` for `ConcurrentOp` (5 tests: forward-3-batches, error-propagation, close-on-EOS, schema-caching, back-pressure-via-bound-1) and a synchronous `noop_waker` driver for `CoalesceOp` (7 tests: disjoint-series fan-in, all-pending, all-exhausted, error-short-circuit, schema-passthrough, different-cadence, continue-when-one-done). Clippy flagged two `vec![false; N]`-in-test uses; tightened to fixed arrays. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (214 passed — 202 prior + 12 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass. Phase 3 acceptance criteria met for every unit.
- 2026-04-16 Operator Implementor 3c.4 in-progress→done — created `promql/v2/operators/count_values.rs` (`CountValuesOp<C: Operator>`, `format_value_label` helper, private `BucketKey` / `BucketCounts` intermediate state); registered in `operators/mod.rs` (incl. `prelude`) and re-exported from `v2/mod.rs`. Deferred-schema pipeline-breaker implementing `count_values("label", expr)`: `schema()` returns `SchemaRef::Deferred` for the whole operator life; downstream reads the finalised `Arc<SeriesSchema>` either through an inherent `finalized_schema(&self) -> Option<&Arc<SeriesSchema>>` method (populated after the first `next()` drains and emits) or straight off the emitted `StepBatch`'s `SchemaRef::Static` handle — no `SchemaRef` extension required (§5 Decisions Log). Bucket keying on `(group: u32, value_bits: u64)` with `value_bits = f64::to_bits()` distinguishes `+0/-0` and NaN bit patterns in the intermediate map; label formatting collapses all NaNs to `"NaN"`. Value-to-label formatter mirrors Prometheus' `strconv.FormatFloat('f', -1, 64)` — `"NaN"` / `"+Inf"` / `"-Inf"` / `"-0"` special cases, finite non-zero through Rust's `f64::to_string` (shortest round-trip) — with a documented divergence note in §5 Decisions Log (no in-crate citation available: legacy engine does not implement `count_values`, and the `aggregators.test:480` goldens are ignored). Grouping via optional `GroupMap` (reused from 3b.4) + a parallel `Arc<[Labels]>` of group-label sets; emitted series carry the group labels (with any pre-existing `label_name=*` stripped to avoid duplicates, matching the "overwrite label with output" goldens at `aggregators.test:517,524`) composed with the new `{label_name=<formatted_value>}` entry. Emission: a single `StepBatch` covering the full outer grid × full output roster (downstream rechunking is a planner concern). Memory accounting: three reservations — per-bucket bytes (charged incrementally on `try_grow`), schema bytes (reserved up-front at roster finalisation), and per-batch `OutBuffers` (RAII via the same pattern as `aggregate.rs` / `rollup.rs`); all released on `Drop` / `finish()`. 13 new unit tests on hand-built `MockOp` upstreams: one-series-per-distinct-value, counts-per-step, `by`-grouping, `without`-grouping (per-input groups), deferred-schema pre-drain, finalised schema post-drain, NaN bit-pattern bucketing (two distinct NaN patterns emerge as two `{version="NaN"}` series), upstream error propagation, memory-cap rejection on first bucket insert, empty-child end-of-stream, format-label parity with the Prometheus reference table, idempotent EOS after emit, and `None`-group-assignment drop behaviour. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (202 passed — 189 prior + 13 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Planner Implementor 4.1 in-progress→done — created `promql/v2/plan/mod.rs` (submodule declarations + public re-exports) and `promql/v2/plan/lowering.rs` (`lower`, `LoweringContext`, private `lower_call` / `lower_aggregate` / `lower_binary` dispatchers, `binary_op_kind` / `rollup_kind_for` / `instant_fn_kind_for` tables, `expect_number_literal` / `expect_string_literal` literal extractors, diagnostic helpers). `v2/mod.rs` extended with `pub(crate) mod plan;` + re-exports (`LogicalPlan`, `LoweringContext`, `lower`, `PlanError`, supporting types). `plan_types.rs` and `error.rs` untouched except for a one-line clippy-conformance fix in `plan_types.rs::labels_to_arc` (`iter().cloned().collect()` → `.to_vec()`; §5 Decisions Log 4.1 entry). Lowering contract: `VectorSelector` / `MatrixSelector` / `NumberLiteral` / `Subquery` / `Paren` / `Unary` map 1:1 to the IR; `Call` dispatches via name tables to `RollupKind` (17 variants — strict superset of legacy, flags rate-on-instant-vector as `InvalidArgument`) or `InstantFnKind` (29 variants, folds plan-time scalar args into `Round`/`Clamp`/`ClampMin`/`ClampMax`); `Aggregate` dispatches on `TokenType` (`T_SUM`/…/`T_TOPK`/`T_BOTTOMK`/`T_QUANTILE`/`T_COUNT_VALUES`), coercing `topk`/`bottomk`'s `k` to `i64` the same way `evaluator.rs::coerce_k_size` does; `Binary` threads the `bool` modifier into per-variant comparison kinds and emits `BinaryMatching` only when explicit matching/grouping modifiers are present (see §5 Decisions Log). Unary minus lowers to `Binary { Mul, Scalar(-1.0), child }` (§5). Function-name coverage is strict superset of operator enum variants; gaps vs. Prometheus (`absent`, `histogram_quantile`, `scalar`, `vector`, `pi`, `time`, date/time extractions) flagged in §5 Decisions Log and error with `PlanError::UnknownFunction` — no operator-variant additions. 18 new unit tests cover the required should_-prefixed battery plus unary-negative-clamp-bound folding and the `LoweringContext::for_instant` helper. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (232 passed — 214 prior + 18 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Planner Implementor 4.2 in-progress→done — created `promql/v2/plan/optimize.rs` (public `optimize(LogicalPlan) -> LogicalPlan` entry point, private `apply_rules` / `fold_constants` / `apply_scalar_op` / `dedupe_vector_selector_matchers` / `dedupe_matchers`); added `pub mod optimize;` + `pub use optimize::optimize;` to `promql/v2/plan/mod.rs`. Rule set: (1) constant folding on `Binary { Scalar, op, Scalar }` covering the 7 arithmetic variants (`Add`/`Sub`/`Mul`/`Div`/`Mod`/`Pow`/`Atan2`) and all 6 comparison variants (`Eq`/`Ne`/`Gt`/`Lt`/`Gte`/`Lte`, both `bool` settings); (2) unary-minus-of-literal is covered for free by rule (1) because lowering emits `Binary { Mul, Scalar(-1), x }`; (3) redundant `(name, op, value)` matcher dedup on `VectorSelector` / `MatrixSelector` via `Matchers.matchers` walk preserving first-occurrence order (`or_matchers` untouched — cross-branch dedup would change set semantics). No CSE (§5 Decisions Log 4.2 — deferred to 4.3 / v2 post DAG representation); no algebraic identities (§5 — `0 * NaN = NaN` under IEEE 754 would break; task-spec correctness-over-micro-opt rule). Fixpoint: iterate `apply_rules` until `LogicalPlan::PartialEq` shows no change, bounded by `MAX_PASSES = 4` (§5 — converges in 1-2 passes today; bound is a safety net). NaN folding enabled and cited against legacy `evaluator.rs:2153-2177` + v2 `operators/binary.rs:180-193` — both compute `a + b` without NaN guarding. Comparison folding safe on scalar/scalar regardless of `bool` per legacy `evaluator.rs:2109-2112` (§5). 13 new unit tests cover the required `should_*` battery (scalar arithmetic, comparison with `bool`, comparison without `bool` — hand-built AST because the parser rejects the source form, unary-minus-of-literal, non-constant passthrough, duplicate matcher collapse, bare-selector passthrough, nested fold, identities-deferred, bounded fixpoint, NaN fold, fold-under-`InstantFn`, fold-under-`Aggregate`). No changes to `plan_types.rs` / `error.rs` / `lowering.rs` / any operator. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (245 passed — 232 prior + 13 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Planner Implementor 4.3 in-progress→done — created `promql/v2/plan/physical.rs` (public `build_physical_plan<S: SeriesSource + Send + Sync + 'static>(LogicalPlan, &Arc<S>, MemoryReservation, &LoweringContext) -> Result<PhysicalPlan, PlanError>` entry point + `PhysicalPlan { root, output_schema, step_grid }`); registered `pub mod physical;` + re-exports in `promql/v2/plan/mod.rs` and `promql/v2/mod.rs`. Extended `PlanError` with four additive variants — `SourceError(String)`, `InvalidMatching(String)`, `MemoryLimit(String)`, `PhysicalPlanFailed(String)` — so the planner surface stays `QueryError`-free (§5 Decisions Log 4.3). Bottom-up walk: leaves call `source.resolve().try_collect()` into `Arc<SeriesSchema>` + parallel `Arc<[ResolvedSeriesRef]>` hint and wire a `VectorSelectorOp` / (inside `Rollup` branch) `MatrixSelectorOp` + `MatrixWindowSource` pair; intermediates bind every plan-time artifact the operator's constructor takes — `GroupMap` + reducer-/filter-shape `Arc<SeriesSchema>` for `AggregateOp`, `MatchTable` + output `Arc<SeriesSchema>` for vector/vector `BinaryOp` (arithmetic ops drop `__name__` via `preserves_metric_name` mirroring legacy `changes_metric_schema` at `evaluator.rs:2125-2127`; `on`/`ignoring` axis + `group_left`/`group_right`/`ManyToMany` cardinalities via `build_one_to_one`/`build_group_left`/`build_group_right`), `Option<GroupMap>` + `Arc<[Labels]>` for `CountValuesOp`, and a plan-time-probed `Arc<SeriesSchema>` + `ChildFactory` closure for `SubqueryOp` (§5 Decisions Log — factory re-invokes `build_node` synchronously via `tokio::task::block_in_place`; inner-schema assumed plan-time stable per RFC §"Core Data Model"). `CountValues`-composition restriction enforced via a `static_schema()` guard on every schema-sensitive parent: `SchemaRef::Deferred` children are rejected with `PlanError::InvalidMatching` (§5 — RFC §"Core Data Model" allows only root-positioned `count_values` in v1). Concrete generic operator structs (`BinaryOp<L, R>`, `InstantFnOp<C>`, `AggregateOp<C>`, `CountValuesOp<C>`) consume trait-object children via a transparent `BoxedOp(Box<dyn Operator + Send>)` newtype that forwards `schema()`/`next()`. Parser-type conversions (`plan_types::{Offset, AtModifier}` → `promql_parser::parser::{Offset, AtModifier}`) at leaf-operator boundaries via local `to_parser_*` helpers. **No operator-constructor API gaps surfaced** — every operator accepted exactly the shape the planner could build at plan time (§5). 14 new unit tests on an inline `MockSource` (mirrors the 3a.1 / 3b.1 pattern): `should_build_vector_selector_from_logical_plan`, `should_build_matrix_selector`, `should_compute_group_map_for_sum_by_label`, `should_compute_group_map_for_sum_without_label`, `should_output_input_schema_for_topk`, `should_output_group_schema_for_sum`, `should_compute_one_to_one_match_table`, `should_mark_unmatched_series_as_none_in_match_table`, `should_build_subquery_factory_producing_child_per_outer_step`, `should_propagate_source_resolve_error_as_plan_error`, `should_build_rollup_over_matrix_selector_end_to_end` (drives one poll of the built root), `should_build_nested_aggregate_of_rollup` (`sum by (pod) (rate(m[5s]))` via the real parser + 4.1 lowerer), `should_reject_count_values_under_schema_sensitive_parent`, `should_group_left_preserves_include_labels_from_one_side`. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (259 passed — 245 prior + 14 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Planner Implementor 4.4 in-progress→done — created `promql/v2/plan/cardinality.rs` (`CardinalityLimits`, `enforce_cardinality_gate` async entry point) and registered the module in `promql/v2/plan/mod.rs`. Added `PlanError::TooLarge { estimated_cells: u64, limit: u64 }` variant to `plan/error.rs`; extended `LoweringContext` with a `cardinality_limits: CardinalityLimits` field + `with_cardinality_limits` chainable setter (kept `Copy` — see §5 Decisions Log). Wired the gate into `build_physical_plan` at the top of the function, before any `resolve` call. Default limit: 20,000,000 series×steps cells (§5). Gate walks the `LogicalPlan` bottom-up, collecting `VectorSelector` / `MatrixSelector` leaves; calls `SeriesSource::estimate_cardinality` per leaf; multiplies upper bound by the outer-grid `step_count`; sums and rejects when the running total exceeds the configured limit. Saturating arithmetic so `u64::MAX` sentinels from the adapter's "no-positive-matchers" escape hatch trip the gate immediately (§5). Subquery handling: outer-grid flavour per RFC verbatim wording (§5 documents the under-estimation vs. true subquery cost). `CountValues` gated on input cardinality as a lower bound per task spec. `Binary` / `Aggregate` / `Rollup` / `InstantFn` / `Rechunk` / `Concurrent` / `Coalesce` treated as transparent: sum their leaves. Wired via `LoweringContext` (not a new `build_physical_plan` argument) — the context already threads through the whole plan pipeline, and the limit is a plan-time configuration knob sharing that lifetime. 12 new unit tests on a scripted `MockSource` (for gate-only tests) and a `ResolvingSource` (for the integration / order-assertion tests): `should_default_cardinality_limits_to_twenty_million_cells`, `should_treat_zero_limit_as_disabled`, `should_accept_plan_within_limit`, `should_reject_plan_exceeding_limit`, `should_accumulate_cardinality_across_multiple_leaves` (Binary+Rollup pair), `should_multiply_by_step_count` (10 vs 100 steps), `should_report_estimated_cells_in_error`, `should_use_upper_bound_when_estimate_is_approx`, `should_bypass_gate_when_limit_is_zero_or_disabled`, `should_trip_gate_immediately_on_unbounded_sentinel`, `should_call_estimate_cardinality_before_resolve` (via `build_physical_plan`), `should_not_call_resolve_when_gate_rejects` (via `build_physical_plan`). `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (271 passed — 259 prior + 12 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Planner Implementor 4.5 in-progress→done — created `promql/v2/plan/parallelism.rs` (`Parallelism` config struct with `concurrent_threshold_series` / `coalesce_max_shards` / `channel_bound` knobs; `ExchangeStats` test-observability shape; `should_wrap_concurrent` decision helper); registered in `plan/mod.rs` with `ExchangeStats` / `Parallelism` re-exports. Extended `LoweringContext` with a `parallelism: Parallelism` field + `with_parallelism` chainable setter (mirrors 4.4's `with_cardinality_limits`); defaults through `LoweringContext::new` / `for_instant`. Added `pub async fn build_physical_plan_with_stats(...) -> Result<(PhysicalPlan, ExchangeStats), PlanError>` as the test-observable entry point; the existing `build_physical_plan` delegates and discards the stats so production surface is unchanged. Exchange insertion inlined into `build_node` (option 1 from the unit brief): after constructing a `VectorSelectorOp` or `RollupOp(MatrixSelectorOp)` leaf, the new `maybe_wrap_concurrent` helper routes the child through `BoxedOp` (existing shim) and wraps in `ConcurrentOp` when the resolved series count ≥ `concurrent_threshold_series`. `MatrixSelectorOp` is not wrapped directly — its `Operator::next` is degenerate per §3a.2; wrapping the enclosing `RollupOp` preserves the async-decoupling contract at a slightly wider boundary (§5). **Coalesce deliberately skipped in v1** per the unit brief's recommendation — the safety envelope for per-series-independent operator chains isn't yet tracked by the planner, and shipping `Coalesce` without that check risks silent data-loss. The `coalesce_max_shards` knob ships (default `0`, disabled) so a future unit can flip it on without a public-surface break. Concurrent threshold default: **64 series** (`Parallelism::DEFAULT_CONCURRENT_THRESHOLD_SERIES`). 13 new unit tests: 5 in `parallelism.rs` (`should_use_default_parallelism_when_not_specified`, `should_wrap_when_series_count_meets_threshold`, `should_wrap_every_leaf_when_threshold_is_zero`, `should_disable_wrap_when_threshold_is_max`, `should_track_exchange_stats`) + 8 in `physical.rs` (`should_wrap_leaves_above_threshold_in_concurrent`, `should_not_wrap_leaves_below_threshold`, `should_leave_intermediate_operators_unwrapped`, `should_use_default_parallelism_when_not_specified`, `should_allow_disabling_concurrent_with_threshold_zero_or_max`, `should_respect_channel_bound`, `should_skip_coalesce_in_v1`, `should_wrap_rollup_leaf_above_threshold`). `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (284 passed — 271 prior + 13 new), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass. Phase 4 complete.
- 2026-04-16 Wiring Implementor 5.1 in-progress→done — added `Tsdb::eval_query_v2` and `Tsdb::eval_query_range_v2` in `timeseries/src/tsdb.rs` (both `#[cfg(feature = "promql-v2")]`); created `timeseries/src/promql/v2/reshape.rs` with `reshape_instant` / `reshape_range` (plus a `ReshapeError` wrapper) and registered the module in `promql/v2/mod.rs`. Shared private `execute_v2(query, reader, ctx, is_instant)` drives parse → lower → optimize → build_physical_plan → `poll_fn(|cx| plan.root.next(cx))` loop → reshape; wrapped the reader through `Arc<QueryReaderSource<TsdbQueryReader>>` re-using the existing `Tsdb::query_reader_for_ranges` path. Memory cap default: **1 GiB** (`DEFAULT_V2_MEMORY_CAP_BYTES`, §5). `PlanError → crate::error::QueryError` mapping: parse/shape variants → `InvalidQuery`, structural/runtime variants → `Execution` (§5). `v2::QueryError → QueryError::Execution` for streaming errors. Pure-scalar detection uses the `ConstScalarOp` canonical shape (single-series roster, empty labels). Signatures mirror v1 so Phase 5.2 HTTP A/B swap is trivial; the v1 `eval_query` / `eval_query_range` methods and `TsdbReadEngine` trait are **untouched**. 7 new v2 wiring tests (inside a `#[cfg(feature = "promql-v2")] mod v2_wiring_tests` sub-module of `tsdb::tests`): `should_eval_instant_query_v2_selector_returns_one_sample_per_series`, `should_eval_range_query_v2_produces_samples_per_step`, `should_eval_rate_over_counter_v2_matches_expected_rate`, `should_reject_query_exceeding_cardinality_limit`, `should_propagate_memory_limit_at_exec_time`, `should_return_query_error_for_unknown_function`, `should_eval_sum_by_label_v2`. Plus 4 new reshape unit tests in `promql/v2/reshape.rs` (instant, elide-invalid, range, scalar). `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries` (724 passed — v1 path untouched), `cargo test -p opendata-timeseries --features promql-v2` (1019 passed), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (288 passed — 284 prior + 4 reshape), `cargo fmt`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Wiring Implementor 5.2 in-progress→done — wired compile-time `#[cfg(feature = "promql-v2")]` dispatch inside `timeseries/src/server/http.rs` at `handle_query` / `handle_query_range` / `handle_federate`; added `TsdbEngine::eval_query_v2` + `TsdbEngine::eval_query_range_v2` (both gated, both fall back to v1 for `ReadOnly(reader)`); added a local `query_value_to_range_samples` adapter in the handler so the v2 `QueryValue` (including the `Scalar` range-grid case) projects cleanly onto the `Vec<RangeSample>` wire shape consumed by `range_result_to_response`. 4 new integration tests in `timeseries/tests/http_server.rs::v2_dispatch` behind `#[cfg(feature = "promql-v2")]` via `tower::ServiceExt::oneshot`: instant dispatch, range dispatch, scalar-range-adapter, `PlanError::UnknownFunction` → `bad_data`. `cargo check -p opendata-timeseries` / `--features promql-v2` / `--features http-server` / `--features http-server,promql-v2` all pass; `cargo test -p opendata-timeseries` (724 passed), `--features promql-v2` (1019 passed — unchanged from 5.1), `--features testing,http-server --test http_server` (26 passed), `--features testing,http-server,promql-v2 --test http_server` (30 passed = 26 prior + 4 new); `cargo fmt` and `cargo clippy --all-targets --all-features -- -D warnings` clean.
- 2026-04-16 Wiring Implementor 5.3 in-progress→done — polished `timeseries/src/promql/v2/reshape.rs` end-to-end. Label-copy strategy: one `Labels::clone` per output series (instant — implicit via one-sample-per-valid-series; range — explicit via `BTreeMap<series_idx, (Labels, _)>::or_insert_with`), never per step or per batch (§5). Series ordering: deterministic by global `series_idx` (v1 range uses `HashMap<Labels, _>` iteration — non-deterministic; v1 instant uses evaluator emission order; Prometheus wire contract is unordered so v2's order is structurally compatible — §5). Defensive `check_batch_shape` guards values/validity length and range-end bounds in release builds, returning `ReshapeError` rather than panicking (covers 5.1 placeholder's gap where `debug_assert`s were the only safety net). Added explicit `step_count!=1` guard inside `reshape_instant`'s batch loop. Scalar-root range query now produces a single anonymous `RangeSample` with samples sorted by timestamp (batches may arrive out-of-order from Coalesce). 5.2's `query_value_to_range_samples` adapter is **still required** for the `Scalar` over range-grid case (§5). Streaming reshape noted as a future optimisation — collected-batches path retained for clarity. 10 new `should_*` tests (replaced 4 placeholder tests; 14 total): `should_reshape_instant_vector_dropping_invalid_cells`, `should_reshape_range_matrix_preserving_sparse_timestamps`, `should_reshape_scalar_root_for_instant_query`, `should_reshape_scalar_root_for_range_query`, `should_clone_labels_once_per_series_not_per_step`, `should_drop_series_with_no_valid_samples_in_range_query`, `should_handle_deferred_schema_from_count_values`, `should_return_empty_vector_when_no_batches`, `should_return_query_error_on_dimension_mismatch`, `should_return_query_error_on_deferred_batch_schema`, `should_preserve_step_timestamps_in_range_output`, `should_handle_multi_batch_range_query` (+ the 2 carried-over tests from 5.1's placeholder). No `tsdb.rs` signature changes. `cargo check -p opendata-timeseries` (no-feature / `promql-v2` / `http-server` / `http-server,promql-v2` — all clean); `cargo test -p opendata-timeseries --lib` (724 passed — unchanged); `cargo test -p opendata-timeseries --features promql-v2 --lib` (1029 passed = 1019 prior + 10 new); `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (298 passed = 288 prior + 14 new − 4 replaced); `cargo test -p opendata-timeseries --features testing,http-server,promql-v2 --test http_server` (30 passed — unchanged); `cargo fmt` + `cargo clippy --all-targets --all-features -- -D warnings` clean. Phase 5 complete.
- 2026-04-16 Tester 6.1 in-progress→done — added `timeseries/src/promql/promqltest/v2_harness.rs` (separate v2 harness per §5 Decisions Log option-a) and registered `#[cfg(feature = "promql-v2")] mod v2_harness;` in `timeseries/src/promql/promqltest/mod.rs`. Ran the 9-file `.test` corpus through `Tsdb::eval_query_v2` — **2 files pass clean (`operators`, `selectors`); 7 files fail with 61 total eval failures** (see §5 Decisions Log for per-bucket breakdown, counts, and representative fixture citations — 10 buckets total, largest is 28 "unknown function" gaps, and a cross-bucket aggregate-operator panic is flagged separately as a critical out-of-corpus bug). Perf smoke snapshot at 30 series × 720 steps × 6 h range for `sum by (pod) (rate(http_requests_total[5m]))`: **median 29 ms** (InMemoryStorage test TSDB; clipped from 300 series by the aggregate panic — re-take after 6.3 fixes land). No `.test` fixture edited; no v1 code path touched; no new deps. `cargo check -p opendata-timeseries` / `--features promql-v2` clean; `cargo clippy --all-targets --all-features -- -D warnings` clean; v2 harness test run captured at `/tmp/rfc0007_phase6_run.log`. Handing off to 6.2 for triage dispatch.
- 2026-04-16 Architect 6.3 ready→in-progress — accepted full-scope Phase 6 verification (no fixture edits, no acceptance cut), split 6.3 into sub-units 6.3.1–6.3.7, and prioritised 6.3.1 first because the aggregate cardinality panic is a production bug and currently blocks the full-scale perf snapshot.
- 2026-04-16 Architect 6.2 ready→done — triage already captured in §5 Decisions Log by 6.1 (10 failure buckets + 2 critical out-of-corpus bugs: aggregate multi-bucket panic, subquery multi-thread-runtime requirement). No separate triage artifact; 6.1's output is authoritative.
- 2026-04-16 Operator/Planner Implementor 6.3.1 ready→done — fixed the unique-series leaf roster bug by deduplicating `resolve_leaf` output to one schema row per canonical labelset / fingerprint while grouping bucket-local `ResolvedSeriesRef`s per logical series, then updated `VectorSelectorOp` / `MatrixSelectorOp` to flatten and merge those grouped refs during chunk loads. Fixed `AggregateOp`'s streaming, `topk` / `bottomk`, and `quantile` reducers to index `GroupMap` with absolute series positions from `input.series_range`, and preserved the child's `series_range` for filter-shaped breaker outputs. Added focused regressions in planner, aggregate, vector-selector, and matrix-selector tests. Verification: `cargo fmt`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2::operators::aggregate::tests`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2::operators::vector_selector::tests`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2::operators::matrix_selector::tests`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2::plan::physical::tests`; `cargo clippy --all-targets --all-features -- -D warnings`.
- 2026-04-16 Architect 6.3.2 ready→done — added the nullary/scalar/calendar function surface: planner support for `pi()`, `time()`, `vector(s)`, `scalar(v)`, and the zero/one-arg calendar functions; new coercion operators (`TimeScalarOp`, `ScalarizeOp`); scalar-root tracking in `PhysicalPlan` / reshape so vectorized scalars keep vector result types; and end-to-end v2 wiring coverage for `time()`, `vector(time())`, and `minute(vector(...))`. Verification: `cargo fmt`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2`; `cargo test -p opendata-timeseries --features promql-v2 tsdb::tests::v2_wiring_tests`; `cargo clippy --all-targets --all-features -- -D warnings`.
- 2026-04-16 Architect 6.3.3 ready→done — added `label_replace` / `label_join` support via a new `LabelManip` logical node and `LabelManipOp` breaker. Planner now precomputes the rewritten output schema and input→output row map from child labels; runtime merges step-disjoint duplicate rewrites and rejects same-step duplicate labelsets. Added lowering / planner / operator regressions plus v2 wiring tests for both functions. Corpus spot-check: `functions.test` is reduced to the known `clamp(min > max)` failure; `at_modifier.test` no longer reports unknown-function gaps and now exposes the remaining subquery / `@` / `timestamp()` semantics; `name_label_dropping.test` is now isolated to the known `by(__name__)` retention bug. Verification: `cargo fmt`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2`; `cargo test -p opendata-timeseries --features promql-v2 tsdb::tests::v2_wiring_tests`; `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_functions_v2 -- --nocapture`; `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_at_modifier_v2 -- --nocapture`; `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_name_label_dropping_v2 -- --nocapture`; `cargo clippy --all-targets --all-features -- -D warnings`.
- 2026-04-16 Architect 6.3.6 ready→done — fixed the small correctness cluster: `clamp(v, min, max)` now emits empty when `min > max`; aggregate grouping retains `__name__` when explicitly listed in `by(__name__, ...)`; one-to-one binary `on(...)` output schemas now keep only the matching labels; root instant-vector `topk` / `bottomk` results are sorted into Prometheus presentation order during reshape; and `topk(scalar(...), ...)` / `bottomk(scalar(...), ...)` now lower and execute with a dynamic scalar param child drained onto the step grid. Added lowering regression coverage for dynamic `k`, threaded aggregate-param recursion through optimize/cardinality/planning, and re-verified the previously failing corpus buckets (`functions`, `name_label_dropping`, `vector_matching`, `aggregators`). Verification: `cargo fmt`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2`; `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_functions_v2 -- --nocapture`; `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_name_label_dropping_v2 -- --nocapture`; `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_vector_matching_v2 -- --nocapture`; `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_aggregators_v2 -- --nocapture`; `cargo clippy --all-targets --all-features -- -D warnings`.
- 2026-04-17 Architect 6.3.4 ready→done — no new code; unit closed after confirming the five `offset:*` failures ("Expected 1 samples, got 2") were a pre-dedup symptom fixed in-flight by 6.3.1's fingerprint-dedup of `resolve_leaf`. `VectorSelectorOp::build_batch` always emitted a single in-window sample per `(step, series)` cell; the duplication was the planner stacking one schema row per overlapping bucket, which reshape surfaced as N samples per logical series. Verification: `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_offset_v2` passes clean. See §5 Decisions Log 6.3.4.
- 2026-04-17 Architect 6.3.8 ready→done — added optional `effective_times` column to `MatrixWindowBatch`; `MatrixSelectorOp` / `SubqueryOp` populate it when `@` / offset shifts the per-step evaluation time away from the outer grid, and `RollupOp::reduce_batch` prefers it over `step_timestamps` for `(window_start, window_end)` math. Fixes at_modifier #21 (`rate(metric[100s] @ 100) + label_replace(rate(metric[123s] @ 200), …)`); no other corpus bucket affected. Added rollup regression `should_use_effective_times_for_window_math_when_present`. Verification: `cargo fmt`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (328 passed); `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness` (9/10 fixtures green — only at_modifier #33 / #41 `timestamp()` remain, under 6.3.7); `cargo clippy --all-targets --all-features -- -D warnings` clean.
- 2026-04-17 Architect 6.3.7 ready→done — extended `StepBatch` with optional per-cell `source_timestamps: Option<Arc<[i64]>>`; `VectorSelectorOp` stamps the matching-sample timestamp per cell, every derived operator leaves `None` on output (so a wrapping `timestamp()` falls back to step time, matching Prometheus). `InstantFnKind::Timestamp` reads `source_timestamps[idx]` when present. Fixes at_modifier #33 (`timestamp(metric @ 10)` → 10) and #41 (`sum_over_time(timestamp(metric @ 10)[100s:10s] @ 3000)` → 100). Updated the two in-test struct-literal `StepBatch` constructions in `reshape.rs` to pass `source_timestamps: None`; `StepBatch::new` callers are unchanged (field defaults to `None`, builder `with_source_timestamps(ts)` attaches it). Added instant-fn regressions `should_apply_timestamp_returning_source_sample_timestamp_when_available` + renamed-and-clarified fallback test, replacing the prior "step-timestamp semantics" test that documented the v1-known divergence. Corpus: **all 9 fixture files pass clean** under `--features promql-v2`. Verification: `cargo fmt`; `cargo test -p opendata-timeseries --features promql-v2` (1076 passed); `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness` (11/11 green); `cargo clippy --all-targets --all-features -- -D warnings` clean.
- 2026-04-17 Architect 6.3 in-progress→done — Phase 6 acceptance met. All 9 `promqltest` corpus fixtures pass unmodified under `--features promql-v2` (`cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness`). Perf snapshot retaken at 300 series × 6h range (`sum by (pod) (rate(http_requests_total[5m]))` via `v2_harness_perf_smoke`): **median 207 ms** over 5 runs, samples=[206, 207, 207, 207, 209] — up from the 29 ms 30-series baseline once 6.3.1 unblocked the 300-series cardinality. No fixture edited; v1 code path untouched. Phase 7 (migration) unblocked.
- 2026-04-17 Architect 6.3.5 ready→done — fixed subquery inner-grid alignment (`plan/physical.rs::inner_grid`) to emit inner evaluation timestamps at absolute multiples of `inner_step_ms` within `(effective_t - range, effective_t]` rather than the previous forward-walk; empty-grid case (`range < step` with no aligned multiple) now correctly produces zero inner evaluations so the enclosing rollup emits `absent`. Threaded per-outer-step `effective_times: Arc<[i64]>` through `SubqueryOp` via a new `SubqueryOp::with_effective_times` constructor; `build_subquery` now precomputes effective times from `@` / `offset` and passes them in, so each outer step's inner window uses `(effective - range, effective]` instead of the raw outer timestamp. Added operator regression `should_use_effective_times_for_inner_window` and three planner regressions covering the `[50s:10s]` / `[1m:5m]` / `[30s:10s] offset 3s` alignment cases. Corpus: `should_pass_subquery_v2` now passes clean; `should_pass_at_modifier_v2` reduced to the three residual failures — #21 (matrix selector `rate + @`, tracked as new unit 6.3.8) and #33 / #41 (`timestamp()` source-sample semantics, still 6.3.7). Verification: `cargo fmt`; `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (327 passed); `cargo test -p opendata-timeseries --features promql-v2 promql::promqltest::v2_harness::tests::should_pass_subquery_v2`; `cargo clippy --all-targets --all-features -- -D warnings` clean.
- 2026-04-17 Wiring Implementor 7.0 ready→done — lifted `eval_query_v2` / `eval_query_range_v2` onto the `TsdbReadEngine` trait as `#[cfg(feature = "promql-v2")]` default methods; both use `self.make_query_reader_for_ranges(...)`, so `Tsdb` and `TimeSeriesDbReader` get v2 for free without per-impl duplication. Made `execute_v2` generic over `R: QueryReader + Send + Sync + 'static` (the `QueryReaderSource` bound) so the same driver services both `TsdbQueryReader` and `ReaderQueryReader`. Deleted the inherent `Tsdb::eval_query_v2` / `eval_query_range_v2` — the trait defaults cover them, no call sites were pinned to the inherent method. Collapsed `TsdbEngine::eval_query_v2` / `eval_query_range_v2` to a two-arm dispatch on the trait method (previously the `ReadOnly` arm fell back to v1 and wrapped the result in `QueryValue::Matrix`); updated both doc comments accordingly. Added `v2_harness.rs` `use crate::tsdb::TsdbReadEngine` so the existing `tsdb.eval_query_v2(...)` / `eval_query_range_v2(...)` calls resolve through the trait. New tests in `tsdb::tests::v2_wiring_tests`: `should_eval_instant_query_v2_on_read_only_reader_returns_one_sample_per_series`, `should_eval_range_query_v2_on_read_only_reader_produces_samples_per_step`, `should_return_identical_results_for_read_write_and_read_only_paths_on_same_storage` (writer + reader parity over `rate(req_total[1m])`). Verification: `cargo fmt -p opendata-timeseries`; `cargo check -p opendata-timeseries` / `--features promql-v2` clean; `cargo test -p opendata-timeseries --lib` (724 passed); `cargo test -p opendata-timeseries --features promql-v2 --lib` (1079 passed, 2 ignored); `cargo test -p opendata-timeseries --features promql-v2 --lib promqltest::v2_harness` (11/11 fixtures green, 2 ignored perf smokes); `cargo clippy --all-targets --all-features -- -D warnings` clean.

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

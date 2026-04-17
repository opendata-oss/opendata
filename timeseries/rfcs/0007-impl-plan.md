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
over the existing `QueryReader` trait. No PromQL semantics — pure range+series fetch.

**Does**: define `SeriesSource`, `SamplesRequest`, `ResolvedSeriesChunk`;
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
so.

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
| 2.1 | Define `SeriesSource` trait + `SamplesRequest` | Storage Implementor | done | 1.5 | `timeseries/src/promql/v2/source.rs`, `timeseries/src/promql/v2/mod.rs` | |
| 2.2 | Adapter impl over existing `QueryReader` (cross-bucket stitching inside) | Storage Implementor | done | 2.1 | `timeseries/src/promql/v2/source_adapter.rs`, `timeseries/src/promql/v2/mod.rs`, `timeseries/src/promql/v2/memory.rs` | |
| 2.3 | Integration tests: resolve, sample-stream ordering, stale markers | Storage Implementor | done | 2.2 | `timeseries/src/promql/v2/source_adapter.rs` (new `#[cfg(test)] mod integration_tests`) | |

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
| 4.4 | Plan-time cardinality gate | Planner Implementor | removed 2026-04-17 (see §5.4) | 4.3 | — | |
| 4.5 | Insertion of `Concurrent`/`Coalesce` based on series-count heuristics | Planner Implementor | done | 4.3 | `timeseries/src/promql/v2/plan/parallelism.rs`, `timeseries/src/promql/v2/plan/physical.rs`, `timeseries/src/promql/v2/plan/lowering.rs`, `timeseries/src/promql/v2/plan/mod.rs` | |

**Acceptance**: small PromQL expressions produce expected plans in unit tests.

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
| 6.3.2 | Add nullary/scalar/calendar function surface (`scalar`, `vector`, `pi`, `time`, `minute`/calendar funcs) | Architect | done | 6.2 | `timeseries/src/promql/v2/{plan/{plan_types,lowering,optimize,physical}.rs,operators/{coercion,instant_fn,mod}.rs,reshape.rs,mod.rs}`, `timeseries/src/tsdb.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.3 | Add label-manipulation functions (`label_replace`, `label_join`) | Architect | done | 6.2 | `timeseries/src/promql/v2/{operators/{label_manip,mod}.rs,plan/{plan_types,lowering,optimize,physical}.rs,mod.rs}`, `timeseries/src/tsdb.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.4 | Fix `VectorSelector` offset / `@` instant emission to one sample per series | Architect | done | 6.2 | `timeseries/rfcs/0007-impl-plan.md` (see §5 Decisions Log 6.3.4) — subsumed by 6.3.1's leaf-roster dedup; `offset.test` corpus passes clean. | |
| 6.3.5 | Fix subquery inner-grid and `@` / offset semantics | Architect | done | 6.2 | `timeseries/src/promql/v2/operators/subquery.rs`, `timeseries/src/promql/v2/plan/physical.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.6 | Small correctness cluster: topk tie-break, `by(__name__)`, `clamp(min>max)`, `topk(scalar(...), ...)`, binary `on()` schema | Architect | done | 6.2 | `timeseries/src/promql/v2/{operators/{aggregate,instant_fn}.rs,plan/{lowering,optimize,physical}.rs,reshape.rs,plan/mod.rs,mod.rs}`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.7 | Restore `timestamp()` source-sample semantics (StepBatch ABI extension) | Architect | done | 6.2 | `timeseries/src/promql/v2/batch.rs`, `timeseries/src/promql/v2/operators/{vector_selector,instant_fn}.rs`, `timeseries/src/promql/v2/reshape.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.8 | Fix `MatrixSelectorOp` + `rate()` + `@` / `offset` window math (per-step effective-time threaded into `RollupOp`) | Architect | done | 6.2 | `timeseries/src/promql/v2/operators/{matrix_selector,subquery,rollup}.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.9 | Stress-test every operator against `>series_chunk` (>512-series) inputs; fix any latent tile-boundary bugs surfaced | Operator Implementor | done | 6.3.8 | `timeseries/src/promql/v2/operators/{aggregate,binary,instant_fn,coercion,rechunk,concurrent,coalesce,label_manip,count_values,rollup}.rs`, `timeseries/src/tsdb.rs`, `timeseries/rfcs/0007-impl-plan.md` | |
| 6.3.10 | Fix `SubqueryOp` reservation leak at the root cause (`plan::physical::resolve_leaf`'s unpaired `try_grow`) — NOT via scope-wrapper delta-release | | ready | 6.3.9 | | Leak: `resolve_leaf` at `physical.rs:360` calls `reservation.try_grow(label_bytes)` without a paired `release`. Harmless for top-level queries (reservation is dropped at query end) but linear in subquery because the factory rebuilds child plans reusing the same reservation. At ~600+ series × many outer steps trips the 1 GiB cap. **Previous attempt** (commits `c893024` + `b7a2ce4`, reverted in `8c4495a` + `596c446`) wrapped `SubqueryOp::build_outer_step_batch` with a `(reserved_after - reserved_before)` delta-release. This caused a production regression ("many aggregations return no data over long ranges") — suspected cause: `reservation.reserved()` is the global counter, and a `ConcurrentOp` running on another tokio worker during the factory's `block_in_place` can legitimately grow/shrink the counter inside the snapshot window, so the delta captures that operator's bytes and releases them, breaking downstream accounting. **Correct approach for this retry**: attach a `ReservedBytes` RAII guard to `ResolvedLeaf` that pairs its `try_grow` with a `release` in `Drop`, and store the guard on the operator that consumes the resolved roster (`VectorSelectorOp` / `MatrixSelectorOp`) so bytes are released when the operator is dropped at factory-scope exit. Do not touch `SubqueryOp`. Reproduction test in `timeseries/src/tsdb.rs::should_eval_sum_over_time_subquery_with_over_512_series` (currently ignored for a separate `samples()` perf reason; re-enable locally to verify). |

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

This section captures decisions that **still constrain future work**.
Implementation-choice rationale (crate picks, minor optimisations, test-harness
layouts) lives in git history — use `git log -- <path>` to retrieve it.

### 5.1 Architectural contracts (still load-bearing)

- **Native histograms deferred.** `ColumnBlock` / `StepBatch` are float-only in
  v1. Adding histograms is a follow-up RFC.
- **Storage contract is PromQL-unaware.** `SamplesRequest` carries only
  `{ series, time_range }`. Lookback / `@` / offset / step / func pushdown
  lives in operators, not in the source.
- **`count_values` is the only deferred-schema escape hatch.** Every other
  operator publishes `SchemaRef::Static` at plan time. Schema-sensitive parents
  reject `SchemaRef::Deferred` children with `PlanError::InvalidMatching`.
- **`Operator` trait: `Poll`-based, `Send`-only (no `Sync`), object-safe.**
  Shape `fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch,
  QueryError>>>`. `&mut self` (not `Pin`) — operators are not self-referential.
- **`SeriesSource` trait: not object-safe.** Uses RPITIT `impl Future` /
  `impl Stream` returns. Planner binds a concrete generic `S: SeriesSource`.
- **`MemoryReservation` clones share `Arc<Inner>` state.** Dropping a clone
  does **not** release bytes — callers must pair `try_grow` with `release`.
  `AtomicUsize` CAS loop; under contention exactly `floor(cap / bytes)` grows
  succeed.
- **`v2::QueryError` ≠ `crate::error::QueryError`.** Structured variants
  (`MemoryLimit { requested, cap, already_reserved }`, `TooLarge { … }`,
  `Internal(String)`) for tracing; wire boundary maps everything onto
  `crate::error::QueryError::Execution`.
- **Pipeline-breaker pattern for cross-batch state.** Any operator that holds
  per-step state across batches MUST buffer a full
  `step_count × output_row_count` grid, absorb every input batch regardless of
  `(step_tile, series_tile)` shape, and emit a single output batch on
  child-EOS. The streaming-aggregate / breaker-aggregate / vector-vector-binary
  paths all follow this template. A new streaming operator that emits per
  input batch without buffering will be silently wrong when the resolved
  series count exceeds the default 512-series tile.

### 5.2 Known divergences from v1 / Prometheus (relevant to migration)

- **Set ops (`and`/`or`/`unless`)**: v2 ships them; v1 didn't. Strict superset,
  no migration concern.
- **Division by zero**: v2 follows IEEE 754 (matches Prometheus and
  `promqltest/testdata/operators.test`). v1 forced NaN.
- **`count_values` format-label**: legacy engine didn't implement it; v2
  matches Prometheus' `strconv.FormatFloat('f', -1, 64)` shortest-round-trip
  with `NaN`/`+Inf`/`-Inf`/`-0` special cases. Corpus fixture was previously
  ignored; unignore post-migration.
- **`timestamp()`**: returns the **matching sample's** timestamp for bare
  vector selector inputs (via `StepBatch::source_timestamps` populated only by
  `VectorSelectorOp`); falls back to the step timestamp for derived inputs.
  Matches Prometheus.

### 5.3 Non-obvious operator semantics

- **Subquery inner grid**: absolute multiples of `inner_step_ms` that fall in
  `(effective_t - range, effective_t]`. Empty when none align (e.g.
  `[1m:5m]`). Per-outer-step `effective_times` fold `@` / `offset`.
- **`MatrixWindowBatch::effective_times`** (optional column): `MatrixSelectorOp`
  / `SubqueryOp` populate it when `@` / offset is non-trivial. `RollupOp`
  prefers it over `step_timestamps` for `(window_start, window_end)` — needed
  for correct `rate()` extrapolation under `@` / offset.
- **`StepBatch::source_timestamps`** (optional column): populated only by
  `VectorSelectorOp`. Every derived operator leaves it `None`. Only
  `InstantFnKind::Timestamp` reads it.
- **Tile-safe operators** (verified by 6.3.9 stress): `InstantFnOp`,
  `ScalarizeOp` / `TimeScalarOp`, `RechunkOp`, `ConcurrentOp`, `CoalesceOp`,
  `LabelManipOp`, `CountValuesOp`, `RollupOp`. Either stateless per-batch or
  already full-grid accumulators.
- **Full-grid breakers** (promoted from per-batch): `AggregateOp` all kinds
  (streaming + `Topk` / `Bottomk` / `Quantile`), `BinaryOp` vector/vector.

### 5.4 Open items needing follow-up

- **`SubqueryOp` reservation leak (unit 6.3.10).** Root cause:
  `plan::physical::resolve_leaf` at `physical.rs:360` calls
  `reservation.try_grow(label_bytes)` without a paired `release`. Harmless
  for top-level queries (reservation drops at query end) but linear in
  subquery because the factory rebuilds child plans reusing the same
  reservation; at ~600+ series × many outer steps trips the 1 GiB cap.
  **First attempt** (`c893024` + `b7a2ce4`) wrapped
  `SubqueryOp::build_outer_step_batch` with a `(reserved_after -
  reserved_before)` delta-release and caused a production regression — many
  aggregations returned no data over long ranges. Suspected cause:
  `reservation.reserved()` is the global counter, and a `ConcurrentOp` on
  another tokio worker during the factory's `block_in_place` can grow /
  shrink the counter inside the snapshot window, so the delta captures
  concurrent bytes and releases them, corrupting downstream accounting.
  **Reverted** in `8c4495a` + `596c446`. Retry must fix at the source:
  add a `ReservedBytes` RAII guard tied to the resolved schema's bytes so
  `Drop` pairs the `try_grow` — no global-counter deltas, no interaction
  with concurrent operators. **Owner for next unit.**
- **Cardinality gate removed (2026-04-17).** v1 had no equivalent and
  `MemoryReservation` (§1.4) catches mid-exec overruns. `SeriesSource` trait
  returns to two methods (`resolve`, `samples`); `LoweringContext` drops
  `cardinality_limits`; `plan/cardinality.rs` deleted. On object storage the
  gate was doubling per-bucket index RTTs without real safety benefit.
- **Hardcoded limits, no runtime config.** Memory cap = 1 GiB. Requires a
  rebuild to change. Phase 7 cleanup.
- **`timestamp()` with scalar child.** `InstantFnKind::Timestamp` falls back to
  step time when `source_timestamps` is absent. Nested `timestamp(timestamp(X))`
  returns step time for the outer call — matches Prometheus but worth a
  regression if Prometheus ever changes that convention.
- **Coalesce planner gate is off by default.** Per-series-independent operator
  chains aren't tracked by the planner, so turning on `Coalesce` could fan out
  cross-series-sensitive operators incorrectly. `coalesce_max_shards` defaults
  to 0.

## 6. Activity Log

Append-only, one line per unit. Full per-unit narration + verification output
lives in the git commits; use `git log --oneline --grep="RFC 0007"` to browse.

- **Phase 1 — Foundation** (`25ff0aa`): `promql-v2` feature flag; `promql/v2/`
  scaffolding; `StepBatch`, `SeriesSchema`, `SchemaRef`, hand-rolled `BitSet`
  (`batch.rs`); `MemoryReservation` + `v2::QueryError` (`memory.rs`);
  `Operator` trait + `OperatorSchema` + `StepGrid` (`operator.rs`). 31 tests.
- **Phase 2 — Storage Contract** (`4e9361f`): `SeriesSource` trait + value
  types (`source.rs`); `QueryReaderSource<R: QueryReader>` adapter
  (`source_adapter.rs`) with per-bucket resolve emission + matcher-parity
  util. 65 tests total.
- **Phase 3 — Operators** (`cf63fe7`, `757f099`): leaves (`VectorSelectorOp`,
  `MatrixSelectorOp`), stateless middle (`RollupOp`, `InstantFnOp`, `BinaryOp`,
  streaming `AggregateOp`), breakers (`AggregateOp` topk/bottomk/quantile,
  `SubqueryOp`, `RechunkOp`, `CountValuesOp`, `ConcurrentOp`, `CoalesceOp`).
  214 tests total.
- **Phase 4 — Planner** (`17f10f9`): IR (`plan_types.rs`), `lower` (4.1),
  `optimize` constant folding + matcher dedup (4.2), `build_physical_plan`
  (4.3), `Parallelism` / `ConcurrentOp` insertion (4.5). Phase 4 complete:
  284 tests. 4.4 `CardinalityLimits` preflight gate removed 2026-04-17 —
  see §5.4.
- **Phase 5 — Wiring** (`d5ef229`): `Tsdb::eval_query_v2` / `eval_query_range_v2`
  (5.1), HTTP dispatch in `server/http.rs` under `#[cfg(feature = "promql-v2")]`
  (5.2), `reshape_instant` / `reshape_range` polish (5.3). 1029 lib tests
  passing with flag on.
- **Phase 6 — Verification** (`1002bf4`): `v2_harness.rs` runs the 9-file
  corpus through `eval_query_v2`. 6.3 sub-units fixed every failure class.
  - 6.3.1 — leaf-roster dedup by fingerprint + aggregate `series_range`
    indexing; unblocked 300-series perf snapshot.
  - 6.3.2 — nullary/scalar/calendar functions (`scalar`, `vector`, `pi`,
    `time`, `minute`, calendar fns) via new `TimeScalarOp` / `ScalarizeOp`.
  - 6.3.3 — `label_replace` / `label_join` via new `LabelManip` IR +
    `LabelManipOp` breaker.
  - 6.3.4 — closed with no new code; subsumed by 6.3.1's dedup.
  - 6.3.5 — subquery inner-grid → absolute-multiples alignment; `@`/offset
    threaded via `SubqueryOp::with_effective_times`.
  - 6.3.6 — `clamp(min>max)` empty, `__name__` retention in `by(…)`, `on(…)`
    schema projection, root `topk`/`bottomk` ordering in reshape,
    `topk(scalar(...), ...)` dynamic-k.
  - 6.3.7 — `StepBatch::source_timestamps` column; `timestamp()` source-sample
    semantics.
  - 6.3.8 — `MatrixWindowBatch::effective_times` column; `RollupOp` prefers it
    for rate+`@` window math.
  - Corpus: 9/9 fixtures green. Perf: 300 series × 6h, median 207 ms (debug) /
    19 ms (release); release A/B v1/v2 = 11.45× with 1e-9 result parity.
- **Phase 7 — Migration** (in progress):
  - 7.0 (`df1a614`) — `eval_query_v2` / `eval_query_range_v2` lifted to
    `TsdbReadEngine` trait; `TsdbEngine::ReadOnly(reader)` now exercises v2
    (previously fell back to v1). Writer/reader parity test on shared storage.
- **Post-acceptance correctness hardening**:
  - `a982d09` — aggregate streaming tile-boundary fix: promote to step-bounded
    breaker (buffer `step_count × group_count` grid, emit on EOS). Surfaced
    by a prod `sum by (applicationid)` dropout. `9aedd7b` carries the
    failing test reproduction committed before the fix.
  - 6.3.9 (`c86bdb0` + `7770c4d` + `ea25db6` / `043175b` + `b7f541a` /
    `383c196` + `4ae6437` + `e20842a`) — audited all 10 operators under
    `>series_chunk` tiling; fixed `BinaryOp` vv and `AggregateOp` breaker
    kinds (topk/bottomk/quantile) using the same full-grid breaker pattern.
    13 new regression tests, 1 `#[ignore]`d subquery stress flagged a
    `SubqueryOp` reservation leak (§5.4).
  - `6796b02` — v1/v2 criterion benches; `52ecac6` — cross-engine result
    parity check in the A/B smoke.
  - 6.3.10 first attempt (`c893024` + `b7a2ce4`) reverted in `8c4495a` +
    `596c446` after user reported aggregations returning no data over
    long ranges in production. Scope-wrapper delta approach interacts
    badly with `ConcurrentOp` on other tokio workers. Retry must fix
    `resolve_leaf`'s unpaired `try_grow` at the source; see §5.4.

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

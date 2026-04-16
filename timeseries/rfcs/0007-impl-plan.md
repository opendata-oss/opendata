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
| 2.1 | Define `SeriesSource` trait + `SampleHint` + `CardinalityEstimate` | | ready | 1.5 | | |
| 2.2 | Adapter impl over existing `QueryReader` (cross-bucket stitching inside) | | ready | 2.1 | | |
| 2.3 | Integration tests: resolve, estimate, sample-stream ordering, stale markers | | ready | 2.2 | | |

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

## 6. Activity Log

Append-only, one line per agent invocation. Format:
`<date> <role> <unit-id> <status-change> — <one-line summary>`.

- 2026-04-16 Architect — plan document created.
- 2026-04-16 Foundation Implementor 1.1 ready→done — added `promql-v2` feature (empty) to `timeseries/Cargo.toml`; verified `cargo check` with and without the flag and `cargo clippy --all-targets --all-features -- -D warnings` pass.
- 2026-04-16 Foundation Implementor 1.2 in-progress→done — created `promql/v2/{mod,batch,memory,operator}.rs` scaffolding gated on `promql-v2`; stub files carry module doc comments pointing at units 1.3/1.4/1.5. `cargo check -p opendata-timeseries` (no flag) and `--features promql-v2` both compile; `cargo clippy --all-targets --all-features -- -D warnings` passes.
- 2026-04-16 Foundation Implementor 1.3 in-progress→done — implemented `StepBatch`, `SeriesSchema`, `SchemaRef` (Static/Deferred), and a hand-rolled `Vec<u64>`-backed `BitSet` in `promql/v2/batch.rs`; re-exported from `promql/v2/mod.rs`. Row-major-by-step layout documented on the `values` field per RFC. 16 unit tests cover shape invariants, cell indexing, validity access, schema accessors, and bitset edge cases. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2`, and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Foundation Implementor 1.4 in-progress→done — implemented `MemoryReservation` (atomic CAS-loop `try_grow`/`release`, clone-shared `Arc<Inner>`, separate high-water atomic) and `QueryError` (`MemoryLimit`, `TooLarge`) in `promql/v2/memory.rs` using workspace `thiserror`; re-exported from `promql/v2/mod.rs`. 12 unit tests cover within-cap grow, exceeding cap with diagnostic payload, release, regrow after release, high-water monotonicity, concurrent grows (all-fit and cap-contended), zero-cap rejection, zero-byte no-op, `usize` overflow, clone-shared state, and the `TooLarge` variant. `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2::memory` (12 passed), and `cargo clippy --all-targets --all-features -- -D warnings` all pass.
- 2026-04-16 Foundation Implementor 1.5 in-progress→done — defined `Operator` trait, `OperatorSchema`, and `StepGrid` in `promql/v2/operator.rs`; re-exported from `promql/v2/mod.rs`. Trait shape: `Send`-only, object-safe, `fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, v2::QueryError>>>`. `OperatorSchema { series: SchemaRef, step_grid: StepGrid }`. 3 unit tests (`should_build_trait_object`, `should_report_schema_before_next`, `should_yield_end_of_stream`) exercise object-safety and the schema-before-poll contract via an `Empty` test double. Phase 1 acceptance re-verified: `cargo check -p opendata-timeseries` (both feature configurations), `cargo test -p opendata-timeseries --features promql-v2 promql::v2` (31 passed — 16 batch + 12 memory + 3 operator), and `cargo clippy --all-targets --all-features -- -D warnings` all pass.

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

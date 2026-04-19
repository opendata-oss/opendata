# RFC 0008 Implementation Plan — Orchestration Doc

**Primary spec**: [`0008-v2-columnar-long-window-stress.md`](./0008-v2-columnar-long-window-stress.md).
Read it first. That RFC is the source of truth for what to build. This file is
the source of truth for how the work is divided, ordered, and tracked.

**Invocation model**: one orchestration agent owns this file and updates it as
work advances. Implementation sub-agents read this file, execute one narrowly
scoped unit, and report back. The orchestrator then updates this markdown file
with status, artifacts, blockers, and activity notes. This avoids concurrent
edits to the shared coordination file while still making the file itself the
communication ledger.

---

## 1. Operating Protocol

### 1.1 Ralph loop

The orchestrator follows this loop every time:

1. Read this entire file and the RFC.
2. Find the next unit in [§4 State Board](#4-state-board) whose status is
   `ready`.
3. Mark it `in-progress`, assign an owner, and record the intended write scope.
4. Dispatch exactly one narrowly scoped sub-agent for that unit unless two
   units have explicitly disjoint write scopes and are marked parallel-safe.
5. Review the returned diff and verification.
6. Update this file: mark `done` or `blocked`, fill in `artifacts`, append any
   non-obvious judgment call to [§5 Decisions Log](#5-decisions-log), and add a
   one-line summary to [§6 Activity Log](#6-activity-log).
7. Continue with the next `ready` unit or stop if blocked.

### 1.2 Communication contract

- This file is the coordination ledger.
- The orchestrator is the only writer to this file.
- Sub-agents must treat this file as read-only context and report their work
  back to the orchestrator.
- If a sub-agent discovers the assigned scope is wrong, it must stop and report
  the problem rather than widening scope on its own.

### 1.3 Escalate to the user when

- The RFC needs a material design change.
- The oracle cannot remain independent of v2 implementation code.
- The new tests need additional dependencies.
- The runtime cost of the non-ignored regression becomes too high for normal
  `cargo test -p timeseries` use.
- A unit's write scope turns out to overlap materially with another in-flight
  unit and the sequencing should change.

### 1.4 Hard rules

- No correctness dependency on v1. Differential checks against v1 are out of
  scope for this plan.
- The oracle must not call into `eval_query_v2`, `plan::*`, `operators::*`, or
  `reshape::*`.
- No edits under `timeseries/src/promql/promqltest/`.
- After modifying any `.rs` file, run `cargo fmt`.
- Before marking a Rust unit `done`, run the unit's targeted tests and then
  `cargo clippy --all-targets --all-features -- -D warnings`.
- Do not add broad, multi-file worker scopes. One worker owns one narrow unit.
- Do not create additional markdown coordination files. This file is the board.
- No commits by sub-agents. The orchestrator decides if and when to commit.

---

## 2. Core References

| Thing | Location |
|---|---|
| RFC (spec) | `timeseries/rfcs/0008-v2-columnar-long-window-stress.md` |
| Existing v2 A/B smoke | `timeseries/src/promql/promqltest/v2_harness.rs` |
| Existing large-roster v2 tests | `timeseries/src/tsdb.rs` |
| Existing long-form orchestration template | `timeseries/rfcs/0007-impl-plan.md` |
| Query options / concurrency knobs | `timeseries/src/model.rs` |
| Project-specific notes | `timeseries/AGENTS.md` |

### Key facts to keep in mind

- Every tested bucket must contain more than `1024` series.
- The per-bucket roster must not be a multiple of `512`; partial tails are part
  of the design.
- The scenario mixes `Persistent` and `HourLocal` series.
- The non-ignored regression and the ignored soak must share the same
  generator/oracle code paths.
- `count` and `sum` use exact algebraic oracles.
- `rate` assertions apply only to safe interior steps where the full lookback
  window is densely populated.

---

## 3. Roles

### 3.1 Orchestrator

Owns sequencing, dispatch, review, and updates to this markdown file.

**Does**:
- select the next `ready` unit
- assign one narrow worker
- review worker output for scope creep
- update the state board, decisions log, and activity log

**Does not**:
- silently widen worker scope
- allow multiple workers to edit the same Rust files in parallel
- treat a partial implementation as "close enough"

### 3.2 Harness Implementor

Owns test-support scaffolding only.

**Scope**:
- test-only support module shape
- shared scenario/config types
- helper exports from `timeseries/src/testing/mod.rs`

**Does not**:
- implement oracle math
- write end-to-end query tests

### 3.3 Scenario Implementor

Owns synthetic workload generation only.

**Scope**:
- `SeriesKind`, `SeriesSpec`, `Scenario`
- deterministic time grid
- sample expansion and ingestion helpers

**Does not**:
- implement query result assertions
- implement oracle logic beyond scenario metadata generation

### 3.4 Oracle Implementor

Owns generator-derived correctness tables only.

**Scope**:
- visibility math
- exact grouped count/sum oracle
- safe-interior rate oracle
- unit tests for oracle arithmetic

**Does not**:
- call the v2 engine
- write end-to-end TSDB tests

### 3.5 Assertion Implementor

Owns query output normalization and comparison helpers only.

**Scope**:
- normalize label/sample ordering
- compare instant outputs against oracle values
- compare range outputs against oracle tables

**Does not**:
- generate scenarios
- tune workload geometry

### 3.6 Regression Test Implementor

Owns the CI-sized end-to-end tests only.

**Scope**:
- regression scenario builder
- instant probe test
- range test across concurrency profiles

**Does not**:
- add the large soak
- change oracle or scenario semantics

### 3.7 Soak Test Implementor

Owns the ignored high-scale test only.

**Scope**:
- soak scenario sizing
- ignored soak entrypoint
- minimal runtime notes needed by the test itself

**Does not**:
- rewrite the regression tests
- add new query shapes

### 3.8 Verification Implementor

Owns final validation only.

**Scope**:
- targeted test runs
- `cargo clippy`
- report runtime or stability problems back to the orchestrator

**Does not**:
- land speculative code changes outside the assigned fix-up unit

---

## 4. State Board

Legend: `ready` · `in-progress` · `done` · `blocked` · `deferred`.

Only the orchestrator edits this board. `Write Scope` is mandatory for every
unit so worker boundaries stay tight.

### Phase 1 — Harness Foundation

| # | Unit | Owner | Status | Deps | Write Scope | Artifacts | Blocker |
|---|---|---|---|---|---|---|---|
| 1.1 | Add test-only support module skeleton for columnar stress helpers | Harness Implementor | done | — | `timeseries/src/testing/{mod.rs,columnar_stress.rs}` | `timeseries/src/testing/mod.rs`, `timeseries/src/testing/columnar_stress.rs` | |
| 1.2 | Define `ScenarioConfig`, `SeriesKind`, `SeriesSpec`, and `Scenario` data structures | Harness Implementor | done | 1.1 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |

**Acceptance**: support module compiles cleanly behind existing test cfgs;
types are documented enough that later workers do not reinterpret geometry.

### Phase 2 — Scenario Generator

| # | Unit | Owner | Status | Deps | Write Scope | Artifacts | Blocker |
|---|---|---|---|---|---|---|---|
| 2.1 | Implement hour-aligned time-grid and bucket geometry helpers | Scenario Implementor | done | 1.2 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |
| 2.2 | Implement deterministic `Persistent` plus `HourLocal` scenario generation | Scenario Implementor | done | 2.1 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |
| 2.3 | Implement sample expansion and `Tsdb` ingestion helpers for both metrics | Scenario Implementor | done | 2.2 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |

**Acceptance**: regression and soak geometries can be constructed from one
shared generator path; per-bucket counts are directly inspectable from the
generated `Scenario`.

### Phase 3 — Oracle

| # | Unit | Owner | Status | Deps | Write Scope | Artifacts | Blocker |
|---|---|---|---|---|---|---|---|
| 3.1 | Implement latest-scrape visibility helper and probe-step selection | Oracle Implementor | done | 2.2 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |
| 3.2 | Implement exact grouped `count` and `sum` oracle tables | Oracle Implementor | done | 3.1 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |
| 3.3 | Implement safe-interior grouped `rate` oracle table with explicit `None` outside assertion range | Oracle Implementor | done | 3.1 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |
| 3.4 | Add unit tests for oracle math and bucket-edge visibility transitions | Oracle Implementor | done | 3.2, 3.3 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |

**Acceptance**: oracle unit tests prove the core invariants without touching
any query engine code.

### Phase 4 — Assertions and Normalization

| # | Unit | Owner | Status | Deps | Write Scope | Artifacts | Blocker |
|---|---|---|---|---|---|---|---|
| 4.1 | Implement normalization helpers for instant and range query outputs | Assertion Implementor | done | 3.4 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |
| 4.2 | Implement instant assertion helpers for raw selector cardinality and grouped vectors | Assertion Implementor | done | 4.1 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |
| 4.3 | Implement range assertion helpers for grouped `count`, `sum`, and `rate` across step tables | Assertion Implementor | done | 4.1, 3.2, 3.3 | `timeseries/src/testing/columnar_stress.rs` | `timeseries/src/testing/columnar_stress.rs` | |

**Acceptance**: normalization removes ordering from the surface area without
weakening exact-value checks.

### Phase 5 — Regression Tests

| # | Unit | Owner | Status | Deps | Write Scope | Artifacts | Blocker |
|---|---|---|---|---|---|---|---|
| 5.1 | Add regression scenario builder (`8-12` buckets, `1281` series per bucket) | Regression Test Implementor | done | 2.3, 3.4 | `timeseries/src/testing/columnar_stress.rs`, `timeseries/src/tsdb.rs` | `timeseries/src/testing/columnar_stress.rs`, `timeseries/src/tsdb.rs` | |
| 5.2 | Add instant probe regression test for raw selector, grouped `count`, and grouped `sum` at bucket boundaries | Regression Test Implementor | done | 5.1, 4.2 | `timeseries/src/tsdb.rs` | `timeseries/src/tsdb.rs` | |
| 5.3 | Add full-range regression test for grouped `count`, grouped `sum`, and grouped `rate` across multiple `QueryOptions` profiles | Regression Test Implementor | done | 5.1, 4.3 | `timeseries/src/tsdb.rs` | `timeseries/src/tsdb.rs` | |

**Acceptance**: non-ignored regression exercises persisted multi-bucket data
and passes under at least three concurrency profiles.

### Phase 6 — Soak Test

| # | Unit | Owner | Status | Deps | Write Scope | Artifacts | Blocker |
|---|---|---|---|---|---|---|---|
| 6.1 | Add soak scenario builder (`24-36` buckets, `1793` series per bucket) reusing the same generator/oracle path | Soak Test Implementor | done | 5.1 | `timeseries/src/testing/columnar_stress.rs`, `timeseries/src/tsdb.rs` | `timeseries/src/testing/columnar_stress.rs`, `timeseries/src/tsdb.rs` | |
| 6.2 | Add ignored soak test entrypoint and minimal invocation notes in the test doc comment | Soak Test Implementor | done | 6.1, 5.3 | `timeseries/src/tsdb.rs` | `timeseries/src/tsdb.rs` | |

**Acceptance**: the soak is a scale-up of the regression harness, not a second
independent implementation.

### Phase 7 — Verification

| # | Unit | Owner | Status | Deps | Write Scope | Artifacts | Blocker |
|---|---|---|---|---|---|---|---|
| 7.1 | Run targeted regression tests and soak compile path; fix any harness-level issues | Verification Implementor | blocked | 6.2 | repo-local verification only | `cargo test -p opendata-timeseries --features promql-v2 should_eval_v2_long_window_stress_regression_instant_probes`, `cargo test -p opendata-timeseries --features promql-v2 should_eval_v2_long_window_stress_regression_range_queries` | Grouped `count by (applicationid) (stress_gauge)` returns empty results on the regression scenario for both instant probes and range queries; raw-selector and oracle unit tests pass, so verification is currently blocked on a v2 query bug rather than a harness bug. |
| 7.2 | Run `cargo clippy --all-targets --all-features -- -D warnings`; fix any final issues | Verification Implementor | done | 7.1 | repo-local verification only | `cargo clippy --all-targets --all-features -- -D warnings` | |

**Acceptance**: targeted tests are green, ignored soak builds, and clippy is
clean.

---

## 5. Decisions Log

This section records load-bearing decisions that later workers must respect.

- **Orchestrator-only board edits.** This file is the shared state ledger, but
  only the orchestrator updates it. Workers report results back; the
  orchestrator records them here.
- **No v1 oracle.** The harness must stand after v1 removal; correctness comes
  from `SeriesSpec` and algebraic oracle tables only.
- **Two-tier workload.** `Persistent` series cover cross-bucket stitching;
  `HourLocal` series cover per-bucket churn and lookback expiry.
- **Exact counts and sums, selective rates.** `count` and `sum` use exact
  equality. `rate` is asserted only for interior windows explicitly marked
  valid by the oracle.
- **One generator path.** Regression and soak scenarios must share the same
  generator/oracle helpers. Scale changes are config, not new logic.
- **No broad workers.** If a proposed unit touches both
  `timeseries/src/testing/columnar_stress.rs` and `timeseries/src/tsdb.rs`,
  that is acceptable only when the state board explicitly names both paths in
  `Write Scope`.
- **Flush all buckets in the base harness.** The first draft left a tail bucket
  unflushed to mix persisted and head/delta coverage, but that introduced a
  second moving part while the grouped-query regression was still unresolved.
  The current regression and soak scenarios flush every bucket so the failure
  surface stays on long-window grouped correctness.

## 6. Activity Log

Append-only. One line per meaningful orchestrator update.

- 2026-04-18: Initial implementation plan drafted for RFC 0008 with orchestrator-owned markdown state and narrow worker scopes.
- 2026-04-18: Implemented the shared columnar stress support module, regression/soak test entrypoints, and verification runs; regression is blocked on empty grouped-query results rather than harness compilation or clippy issues.

---

## 7. Handoff Checklist

Before the orchestrator marks a unit `done`:

- [ ] State board row updated with status, owner, and artifacts.
- [ ] Decisions Log updated if the worker made a non-obvious judgment call.
- [ ] Activity Log line appended.
- [ ] `cargo fmt` run on touched `.rs` files.
- [ ] Unit-specific targeted tests pass.
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes before final verification signoff.
- [ ] No `promqltest` files changed.
- [ ] Worker stayed inside the declared write scope.

## 8. Dispatch Template

When the orchestrator dispatches a worker, use this shape:

```text
You are the <ROLE> for RFC 0008.

Read:
  - timeseries/rfcs/0008-v2-columnar-long-window-stress.md
  - timeseries/rfcs/0008-impl-plan.md

Your assigned unit: <UNIT-ID> — <UNIT-TITLE>

Write scope:
  - <PATHS FROM STATE BOARD>

Hard rules:
  - stay inside write scope
  - do not edit the plan file
  - do not depend on v1
  - run cargo fmt after .rs edits

Acceptance:
  - see the phase acceptance in §4

When done or blocked, report:
  1. files changed
  2. tests run
  3. any blocker or design decision
Stop after your unit. Do not pick up another one.
```

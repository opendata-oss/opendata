# RFC 0008: v2 Columnar Long-Window Stress Test

**Status**: Draft

**Authors**:
- Almog Gavra <almog.gavra@gmail.com>

## Summary

Add a deterministic end-to-end stress test suite for the timeseries v2
columnar PromQL engine. The suite targets the failure modes observed in
production during long-window queries: aggregates partially computed, series
silently dropped, and result instability under large rosters.

The test geometry is intentionally harsher than the current v2 coverage:
every hour bucket must contain more than `1024` series, and long-range queries
must span many such buckets. The oracle must not depend on the legacy v1
engine, because v1 is expected to be removed. Instead, the test workload is
synthetic and structured so the correct results are derivable directly from
the workload specification with simple arithmetic.

## Motivation

Current v2 coverage is strong in several important areas:

- `promqltest` fixtures cover broad PromQL semantics
- per-operator stress tests cover `>512` series tiling
- the current end-to-end large-roster test covers `sum by (...)` on a single
  timestamp
- the current perf smoke covers `300` series over `6h`

That leaves a gap between unit-scale correctness and the production failure
shape:

- long windows crossing many hour buckets
- more than `1024` series in each bucket, not just in total
- the same logical series continuing across buckets
- additional series that exist only within one bucket
- exact correctness checks that survive v1 removal

This RFC closes that gap with a deterministic stress harness designed to catch
the specific regressions we care about:

- series lost at tile boundaries
- partially accumulated aggregate state
- incorrect cross-bucket stitching
- incorrect staleness / lookback behavior near bucket edges
- result instability as metadata and sample reads run under different
  concurrency settings

## Goals

- Add an end-to-end v2 stress harness that uses a real `Tsdb` and real
  ingestion/query entrypoints
- Ensure every tested bucket contains more than `1024` series
- Exercise both large per-bucket rosters and long multi-bucket windows
- Use an implementation-independent oracle derived from the synthetic workload
- Keep the query set small and representative rather than broad
- Make the suite useful after the v1 engine is removed
- Split coverage into a CI-sized regression and a larger ignored soak

## Non-Goals

- Reproducing all PromQL semantics in the oracle
- Broadening the `promqltest` corpus
- Replacing Criterion or other performance benchmarks
- Testing every PromQL operator in this harness
- Depending on the v1 engine for correctness
- Solving the existing subquery reservation issue in this RFC

## Design

### Test Placement

The stress harness should live alongside the existing v2 end-to-end tests in
the timeseries crate, with reusable workload/oracle helpers in a small test
support module.

Recommended shape:

- test entrypoints in `timeseries/src/tsdb.rs` under the existing
  `#[cfg(feature = "promql-v2")]` test module
- reusable scenario/oracle helpers in a dedicated test-only helper, for
  example `timeseries/src/testing/columnar_stress.rs`

The harness must use:

- a real `Tsdb`
- real ingestion via `ingest_samples`
- real flushing via `flush()`
- real query entrypoints: `eval_query_v2` and `eval_query_range_v2`

Most of the synthetic range should be flushed so the query crosses persisted
hour buckets. The final one or two buckets may remain unflushed so the test
also covers mixed persisted plus head/delta reads.

### Workload Geometry

The dataset is built from two cohort types:

1. `Persistent`
   The same logical series is present in every bucket. This stresses
   cross-bucket stitching for long windows.
2. `HourLocal { bucket_idx }`
   A series exists only within one hour bucket. This stresses per-bucket
   roster churn, bucket-local resolution, and staleness/lookback behavior
   immediately after a bucket stops receiving samples.

Each bucket must contain a roster that is deliberately not a multiple of
`512`, so the engine always processes at least one partial tile tail.

Recommended default geometries:

| Suite | Buckets | Persistent | Hour-local per bucket | Total per bucket |
|-------|---------|------------|------------------------|------------------|
| Regression | 8-12 | 1025 | 256 | 1281 |
| Soak (`#[ignore]`) | 24-36 | 1025 | 768 | 1793 |

These numbers are intentional:

- `1025` is the minimum useful "more than 1024" persistent roster
- `1281` and `1793` both cross several `512`-series tiles and end with a
  partial tile
- the persistent roster alone already exceeds the bucket requirement
- the hour-local roster adds substantial bucket churn without making the
  oracle complicated

### Series Shape

The workload uses two metrics:

- `stress_gauge`
- `stress_counter_total`

Every generated logical series has:

```rust
enum SeriesKind {
    Persistent,
    HourLocal { bucket_idx: usize },
}

struct SeriesSpec {
    series_ordinal: usize,
    application_idx: usize,
    kind: SeriesKind,
    start_ms: i64,
    end_ms: i64,
    gauge_weight: u64,
    counter_inc_per_scrape: u64,
}
```

Suggested label schema:

- `__name__`
- `applicationid`
- `series`
- `kind`
- `home_bucket` for `HourLocal`

`Persistent` series must keep the same labelset in every bucket so they are
the same logical series over time. `HourLocal` series must include a
bucket-specific label so each bucket-local roster is unique.

### Time Grid

Use exact hour-aligned timestamps so the workload lines up cleanly with the
storage bucket geometry.

Recommended constants:

```rust
const SCRAPE_INTERVAL_SECS: u64 = 60;
const QUERY_STEP_SECS: u64 = 60;
const LOOKBACK_SECS: u64 = 5 * 60;
const RATE_WINDOW_SECS: u64 = 15 * 60;
const BUCKET_SECS: u64 = 60 * 60;
```

All series should be scraped once per minute while active.

Gauge behavior:

- value at each scrape is a fixed integer `gauge_weight`

Counter behavior:

- value increases by `counter_inc_per_scrape` on every scrape

This makes the expected grouped `sum` and interior `rate` values trivial to
compute exactly.

### Oracle Design

The oracle must be derived from `SeriesSpec`, not from the query engine.

It must not call into:

- `eval_query_v2`
- `plan::*`
- `operators::*`
- `reshape::*`

The oracle implementation should stay intentionally simple, even if that means
using `O(series * steps)` loops. Trustworthiness matters more than elegance
here.

Recommended structures:

```rust
struct Scenario {
    specs: Vec<SeriesSpec>,
    step_timestamps: Vec<i64>,
    probe_timestamps: Vec<i64>,
}

struct Oracle {
    count_by_app: Vec<Vec<u32>>,
    sum_by_app: Vec<Vec<u64>>,
    rate_by_app: Vec<Vec<Option<f64>>>,
}
```

The oracle builder loops over every query step and every `SeriesSpec`,
determining whether that series should contribute at that step.

#### Visibility Rule

For instant-selector-style checks, a series is visible at evaluation timestamp
`t` when the latest scrape timestamp for that series lies inside the standard
lookback window `(t - lookback, t]`.

Because scrapes are regular and deterministic, the oracle can compute the
latest scrape at or before `t` arithmetically:

```rust
fn latest_scrape_at_or_before(spec: &SeriesSpec, t: i64, scrape_ms: i64) -> Option<i64> {
    if t < spec.start_ms {
        return None;
    }
    let capped = t.min(spec.end_ms);
    if capped < spec.start_ms {
        return None;
    }
    let steps = (capped - spec.start_ms) / scrape_ms;
    Some(spec.start_ms + steps * scrape_ms)
}
```

A series contributes to the instant oracle only when that latest scrape exists
and satisfies the lookback rule.

#### Grouped Gauge Oracles

For `count by (applicationid) (stress_gauge)`:

- increment the corresponding application bucket by `1` for every visible
  series

For `sum by (applicationid) (stress_gauge)`:

- add `gauge_weight` for every visible series

Both are exact integer oracles and should use exact equality in assertions.

#### Counter Rate Oracle

The suite should avoid reproducing Prometheus edge extrapolation logic in the
oracle. Instead, it should assert rate results only on safe interior steps
where the full `[15m]` window is densely populated and far from series
start/stop boundaries.

At those interior steps, each visible series contributes exactly:

```rust
counter_inc_per_scrape as f64 / SCRAPE_INTERVAL_SECS as f64
```

So the grouped oracle for:

```promql
sum by (applicationid) (rate(stress_counter_total[15m]))
```

is the sum of those per-series contributions across all series whose `[15m]`
window is fully populated.

The rate oracle therefore remains simple and independent while still covering
the `MatrixSelector` + `Rollup` path over large rosters.

### Query Set

The query set should remain small:

1. Raw selector

```promql
stress_gauge
```

Used for selected instant probes to catch outright series loss.

2. Grouped count

```promql
count by (applicationid) (stress_gauge)
```

Used for instant probes and full range queries.

3. Grouped sum

```promql
sum by (applicationid) (stress_gauge)
```

Used for instant probes and full range queries.

4. Grouped rate

```promql
sum by (applicationid) (rate(stress_counter_total[15m]))
```

Used for full range queries and selected interior instant probes.

This set is sufficient for the target failure modes:

- raw selector catches missing series directly
- grouped count catches dropped series after aggregation
- grouped sum catches partial accumulation
- grouped rate exercises the matrix-selector/rollup path

Subquery stress is intentionally deferred until the subquery reservation leak
is fixed; it can extend this suite later.

### Probe Timestamps

The suite should validate exact results at selected timestamps around bucket
transitions, not just in the middle of the range.

For representative interior buckets, probe:

- `bucket_end - 1m`
- `bucket_end`
- `bucket_end + 1m`
- `bucket_end + lookback + 1m`

And also probe at interior non-boundary times such as bucket midpoints where
all hour-local series are definitely active.

These probes are where `HourLocal` series are expected to:

- still be visible immediately after the bucket ends because of lookback
- disappear once the lookback window has expired

### Assertions

#### Instant Assertions

For each probe timestamp:

- `stress_gauge`
  - assert the exact expected raw selector cardinality
  - optionally assert exact expected labelset equality for the smaller
    regression suite
- `count by (applicationid) (stress_gauge)`
  - assert exact per-application counts
- `sum by (applicationid) (stress_gauge)`
  - assert exact per-application sums

#### Range Assertions

For the full long-window range query:

- `count by (applicationid) (stress_gauge)`
  - compare every output step against `Oracle::count_by_app`
- `sum by (applicationid) (stress_gauge)`
  - compare every output step against `Oracle::sum_by_app`
- `sum by (applicationid) (rate(stress_counter_total[15m]))`
  - compare steps that have `Some(expected)` in `Oracle::rate_by_app`
  - do not assert steps intentionally marked `None`

Result normalization should sort:

- labelsets
- samples within each range series

This removes ordering from the assertion surface without weakening the
correctness guarantee.

### Concurrency Profiles

The same scenario should be run under multiple `QueryOptions` profiles to
ensure result stability under different read concurrency settings:

```rust
[
    QueryOptions {
        metadata_concurrency: 1,
        sample_concurrency: 1,
        ..Default::default()
    },
    QueryOptions::default(),
    QueryOptions {
        metadata_concurrency: 128,
        sample_concurrency: 512,
        ..Default::default()
    },
]
```

The oracle is identical for all profiles. Any result drift is a correctness
bug.

### Proposed Test Entry Points

The suite should begin with two tests:

1. `should_eval_v2_long_window_stress_regression`
   - enabled in normal runs
   - `8-12` buckets
   - `1281` series per bucket

2. `should_eval_v2_long_window_stress_soak`
   - `#[ignore]`
   - `24-36` buckets
   - `1793` series per bucket

Both should use the same generator/oracle code path so the soak is a scale-up,
not a separate design.

## Alternatives

### Use v1 as the Oracle

Rejected. The point of this suite is to remain valuable after v1 is removed.
Even before removal, v1 parity would only provide differential testing, not an
independent correctness source.

### Encode the Workload as Large `promqltest` Fixtures

Rejected. Very large fixtures are hard to review, hard to evolve, and do not
provide a clean implementation-independent oracle for grouped range outputs.

### Compare Against an External Prometheus Instance

Rejected for the base suite. That would add substantial setup cost, make the
tests slower and more brittle, and still would not provide the direct
generator-derived invariants we want for debugging tile and bucket failures.

### Fully Reimplement PromQL Semantics in the Oracle

Rejected. The goal is not a second PromQL engine. The workload and query set
should stay narrow enough that the correct answer is algebraic.

## Open Questions

- Should the non-ignored regression assert full raw labelset equality at probe
  timestamps, or should it keep raw-selector checks to exact cardinality only?
- Should the final one or two buckets remain unflushed by default, or should
  mixed persisted plus head/delta coverage be a second explicit scenario?
- Once the subquery reservation issue is fixed, should this RFC's harness grow
  a fifth query covering `sum_over_time(...[30m:1m])`?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-04-18 | Initial draft |

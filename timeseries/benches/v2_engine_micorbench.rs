//! Criterion benchmarks comparing the v1 (row-oriented) and v2 (columnar)
//! PromQL engines on the same workloads.
//!
//! Mirrors `evaluator_micorbench.rs`'s warm-range setup: both engines
//! share the pre-parsed `Expr` held by [`WarmRangeQueryHarness`], so the
//! numbers reflect engine cost — not parser, not ingest, not fixture
//! construction. Preparation happens once before `b.iter(...)`; only the
//! inner loop is timed.
//!
//! Invoke with
//! `cargo bench -p opendata-timeseries --features bench-internals,promql-v2 --bench v2_engine_micorbench`.

use criterion::{Criterion, criterion_group, criterion_main};
use timeseries::WarmRangeQueryHarness;

mod common;

/// Shared benchmark geometry: 100 series × 10 labels over a 6 h range at
/// 30 s step (720 steps). Matches the existing `evaluator_micorbench`
/// dimensions so the two files are directly comparable.
const NUM_SERIES: usize = 100;
const NUM_LABELS: usize = 10;
const RANGE_SECS: u64 = 6 * 3600;
const STEP_SECS: u64 = 30;
const LOOKBACK_DELTA_MS: i64 = 300_000;

// ---------------------------------------------------------------------------
// Vector selector — plain `bench_metric`
// ---------------------------------------------------------------------------

fn bench_vector_selector_v1(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let harness = WarmRangeQueryHarness::vector_selector(
        NUM_SERIES,
        NUM_LABELS,
        RANGE_SECS,
        STEP_SECS,
        LOOKBACK_DELTA_MS,
    );
    runtime
        .block_on(harness.run_once())
        .expect("v1 warmup failed");

    c.bench_function("v2_engine/vector_selector/v1", |b| {
        b.iter(|| {
            runtime
                .block_on(harness.run_once())
                .expect("v1 iteration failed");
        })
    });
}

fn bench_vector_selector_v2(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let harness = WarmRangeQueryHarness::vector_selector(
        NUM_SERIES,
        NUM_LABELS,
        RANGE_SECS,
        STEP_SECS,
        LOOKBACK_DELTA_MS,
    );
    runtime
        .block_on(harness.run_once_v2())
        .expect("v2 warmup failed");

    c.bench_function("v2_engine/vector_selector/v2", |b| {
        b.iter(|| {
            runtime
                .block_on(harness.run_once_v2())
                .expect("v2 iteration failed");
        })
    });
}

// ---------------------------------------------------------------------------
// Aggregation — `sum by (label_0) (bench_metric)`
// ---------------------------------------------------------------------------

fn bench_aggregation_v1(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let harness = WarmRangeQueryHarness::aggregation(
        NUM_SERIES,
        NUM_LABELS,
        RANGE_SECS,
        STEP_SECS,
        LOOKBACK_DELTA_MS,
    );
    runtime
        .block_on(harness.run_once())
        .expect("v1 warmup failed");

    c.bench_function("v2_engine/aggregation/v1", |b| {
        b.iter(|| {
            runtime
                .block_on(harness.run_once())
                .expect("v1 iteration failed");
        })
    });
}

fn bench_aggregation_v2(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let harness = WarmRangeQueryHarness::aggregation(
        NUM_SERIES,
        NUM_LABELS,
        RANGE_SECS,
        STEP_SECS,
        LOOKBACK_DELTA_MS,
    );
    runtime
        .block_on(harness.run_once_v2())
        .expect("v2 warmup failed");

    c.bench_function("v2_engine/aggregation/v2", |b| {
        b.iter(|| {
            runtime
                .block_on(harness.run_once_v2())
                .expect("v2 iteration failed");
        })
    });
}

criterion_group! {
    name = benches;
    config = common::default_criterion();
    targets =
        bench_vector_selector_v1,
        bench_vector_selector_v2,
        bench_aggregation_v1,
        bench_aggregation_v2,
}
criterion_main!(benches);

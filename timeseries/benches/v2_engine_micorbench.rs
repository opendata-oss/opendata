//! Criterion benchmarks for the v2 (columnar) PromQL engine on warm
//! range-query workloads.
//!
//! The pre-parsed `Expr` held by [`WarmRangeQueryHarness`] is reused
//! across iterations so the numbers reflect planner + execution cost —
//! not parser, not ingest, not fixture construction. Preparation
//! happens once before `b.iter(...)`; only the inner loop is timed.
//!
//! Invoke with
//! `cargo bench -p opendata-timeseries --features bench-internals --bench v2_engine_micorbench`.

use criterion::{Criterion, criterion_group, criterion_main};
use timeseries::WarmRangeQueryHarness;

mod common;

/// Shared benchmark geometry: 100 series × 10 labels over a 6 h range at
/// 30 s step (720 steps).
const NUM_SERIES: usize = 100;
const NUM_LABELS: usize = 10;
const RANGE_SECS: u64 = 6 * 3600;
const STEP_SECS: u64 = 30;
const LOOKBACK_DELTA_MS: i64 = 300_000;

// ---------------------------------------------------------------------------
// Vector selector — plain `bench_metric`
// ---------------------------------------------------------------------------

fn bench_vector_selector(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let harness = WarmRangeQueryHarness::vector_selector(
        NUM_SERIES,
        NUM_LABELS,
        RANGE_SECS,
        STEP_SECS,
        LOOKBACK_DELTA_MS,
    );
    runtime.block_on(harness.run_once()).expect("warmup failed");

    c.bench_function("v2_engine/vector_selector", |b| {
        b.iter(|| {
            runtime
                .block_on(harness.run_once())
                .expect("iteration failed");
        })
    });
}

// ---------------------------------------------------------------------------
// Aggregation — `sum by (label_0) (bench_metric)`
// ---------------------------------------------------------------------------

fn bench_aggregation(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let harness = WarmRangeQueryHarness::aggregation(
        NUM_SERIES,
        NUM_LABELS,
        RANGE_SECS,
        STEP_SECS,
        LOOKBACK_DELTA_MS,
    );
    runtime.block_on(harness.run_once()).expect("warmup failed");

    c.bench_function("v2_engine/aggregation", |b| {
        b.iter(|| {
            runtime
                .block_on(harness.run_once())
                .expect("iteration failed");
        })
    });
}

criterion_group! {
    name = benches;
    config = common::default_criterion();
    targets = bench_vector_selector, bench_aggregation,
}
criterion_main!(benches);

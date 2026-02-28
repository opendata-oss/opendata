//! PromQL engine microbenchmarks.

use criterion::{Criterion, criterion_group, criterion_main};
use timeseries::SubqueryLabelCacheHarness;

mod common;

#[derive(Clone, Copy)]
struct WorkloadConfig {
    num_series: usize,
    num_labels: usize,
    num_steps: usize,
    lookback_delta_ms: i64,
    iterations_per_sample: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            num_series: 1000,
            num_labels: 10,
            num_steps: 120,
            lookback_delta_ms: 300_000,
            iterations_per_sample: 50,
        }
    }
}

fn bench_subquery_label_cache(c: &mut Criterion) {
    let config = WorkloadConfig::default();
    // Test preparation split:
    // Build synthetic data + query statement once before timing starts.
    // This benchmark does NOT time ingest/setup; it times evaluator execution only.
    // Keep all preparation outside `b.iter(...)`.
    let harness = SubqueryLabelCacheHarness::new(
        config.num_series,
        config.num_labels,
        config.num_steps,
        config.lookback_delta_ms,
    );
    let runtime = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");

    // Warmup executes evaluator logic only on the prebuilt harness.
    runtime
        .block_on(harness.run_iterations(10))
        .expect("warmup failed");

    c.bench_function("evaluator_micorbench/subquery_label_cache", |b| {
        b.iter(|| {
            // Measured section: evaluator execution only.
            runtime
                .block_on(harness.run_iterations(config.iterations_per_sample))
                .expect("benchmark iteration failed");
        })
    });
}

criterion_group! {
    name = benches;
    config = common::default_criterion();
    targets = bench_subquery_label_cache
}
criterion_main!(benches);

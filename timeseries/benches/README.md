# Timeseries Criterion Benchmarks

This folder contains Criterion benchmarks for evaluator-focused performance work.

## Current Structure

- `evaluator_micorbench.rs`: Primary Criterion entrypoint for evaluator microbenchmarks.
- `common.rs`: Shared Criterion configuration (sample size and defaults).
- `src/bench_api.rs`: Feature-gated internal runner functions used by bench targets.
- `Cargo.toml`:
  - `feature = "bench-internals"` enables internal benchmark runners.
  - `[[bench]]` registers the single `evaluator_micorbench` Criterion executable.

## Why This Design

- Evaluator internals are not public (`pub(crate)`/private), and Criterion bench targets in `benches/` compile as external crates.
- To benchmark internal evaluator paths without making production internals public, we expose only minimal, feature-gated benchmark helpers via `bench-internals`.
- Bench files stay focused on Criterion configuration and scenario definitions, while internal setup/execution logic stays in `src/bench_api.rs`.

## Run Benchmarks

Run the primary benchmark target:

```bash
cargo bench -p opendata-timeseries --bench evaluator_micorbench --features bench-internals
```

Run a single benchmark function in that target:

```bash
cargo bench -p opendata-timeseries --bench evaluator_micorbench --features bench-internals -- subquery_label_cache
```

Compile-only check:

```bash
cargo bench -p opendata-timeseries --bench evaluator_micorbench --features bench-internals --no-run
```

## Recommended Design

- Prefer one bench target (`evaluator_micorbench`) with multiple benchmark functions.
- Add new scenarios as additional `bench_*` functions in `evaluator_micorbench.rs`.
- This minimizes maintenance by avoiding a new bench file and `[[bench]]` entry for every scenario.

## Add a New Scenario (Aggregation, Binary Expr, etc.)

1. Add a new runner function in `timeseries/src/bench_api.rs`.
   - Keep it focused on one scenario (for example: aggregate, binary expression, selector path).
   - Build dataset/setup outside the measured hot loop when possible.
   - Return `Result<(), String>` for clear failure reporting from benches.

2. Add a new benchmark function in `timeseries/benches/evaluator_micorbench.rs`.
   - Example: `fn bench_aggregation_group_by(c: &mut Criterion)`.
   - Call the corresponding runner function from `bench_api`.
   - Define scenario-specific workload config (series count, labels, steps, iterations).

3. Add the function to `criterion_group!` targets in `evaluator_micorbench.rs`.
   - No `Cargo.toml` change needed when reusing the same target.

4. Run formatting and compile checks.

```bash
cargo fmt
cargo bench -p opendata-timeseries --bench evaluator_micorbench --features bench-internals --no-run
```

## Benchmark Design Guidance

- Keep each benchmark scoped to one evaluator behavior to make regressions diagnosable.
- Keep workload constants stable when comparing branches/commits.
- Record per-sample iteration count in the bench file so interpretation is explicit.
- Prefer grouped scenarios in `evaluator_micorbench.rs` unless isolation is required.
- Use end-to-end `timeseries/bench` benchmarks for system-level latency/throughput; use Criterion here for evaluator microbenchmarks.

## Interpreting Results

If a Criterion sample runs multiple internal iterations, divide reported time by iterations per sample for rough per-iteration cost.

Example:
- Reported median: `133 ms`
- Iterations/sample: `50`
- Approx per iteration: `133 / 50 = 2.66 ms`

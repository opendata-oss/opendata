# RFC 0003: Common Benchmark Framework

**Status**: Implemented

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC proposes a minimal benchmark framework for OpenData that handles
environment concerns—storage initialization/cleanup and metrics export—while
leaving workload structure entirely to benchmark authors. The framework produces
machine-readable output suitable for CI execution and regression analysis.

## Motivation

OpenData's database systems (TSDB, Log, Vector, etc.) share a common storage substrate
and need consistent performance measurement. While each system has distinct
workloads worth benchmarking, they share environment concerns: initializing
storage, recording metrics, and exporting results for analysis. A minimal
framework that handles these concerns reduces boilerplate without constraining
how benchmarks are written.

## Goals

- **Handle environment, not workload**: The framework handles common environment
  concerns—initializing and cleaning up storage, collecting and exporting
  metrics. It does not dictate what a benchmark does or how it structures its
  workload.

- **CI and regression analysis ready**: Produce machine-readable output (CSV)
  tagged by git commit, suitable for periodic CI execution and consumption by
  regression analysis tools like [Apache Otava](https://otava.apache.org/).

## Non-Goals

- **Built-in regression analysis**: The framework produces data; analysis is
  handled externally (by Apache Otava or other tools). We don't build statistical
  analysis or alerting into the framework itself.

- **Replacing Criterion for micro-benchmarks**: Criterion remains appropriate
  for CPU-bound micro-benchmarks (like the existing `varint` benchmarks). This
  framework targets higher-level integration benchmarks involving I/O, storage,
  and realistic workloads.

- **Real-time dashboarding**: Visualization and dashboards are external
  concerns. The framework focuses on execution and data collection.

- **Production load testing**: This framework is for controlled benchmarks, not
  for load testing production systems or chaos engineering.

## Design

### Structure

The `bencher` crate provides the core framework, including:

- **Configuration** for storage and optional metrics reporting
- **Bencher** which runs `Benchmark` implementations and manages their lifecycle
- **Metrics collection** (counters, gauges, histograms) during benchmark runs
- **Summary output** printed to console after each benchmark completes

Each system defines its own benchmarks locally (e.g., `log/bench/`) and builds
a dedicated binary that registers those benchmarks with Bencher. This keeps
benchmark implementations close to the code they measure while sharing common
infrastructure.

### Output

Summary metrics are always printed to console. If a reporter is configured,
both ongoing and summary metrics are persisted to the configured object store
using the TimeSeries format.

Example console output:

```
Running benchmark: ingest
  [batch_size=100, value_size=256, num_keys=10]
    throughput_ops  526.47K
    elapsed_ms      1
```

### Example

See `log/bench/` for a complete example including:

- Benchmark implementation (`src/ingest.rs`)
- Binary entry point (`src/main.rs`)
- Usage documentation (`README.md`)

### Future Work

The output format enables regression analysis through tools like
[Apache Otava](https://otava.apache.org/), which performs statistical
change-point detection. CI integration and tooling for automated regression
detection will be addressed in subsequent work.

## Alternatives

*To be detailed in subsequent revisions.*

## Open Questions

*None at this time.*

## Updates

| Date       | Description                |
|------------|----------------------------|
| 2026-01-16 | Initial draft              |
| 2026-01-24 | Updated to reflect implementation |
| 2026-01-28 | Simplified to high-level structure, removed API details |

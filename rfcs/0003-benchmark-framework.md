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

### Crate Structure

A separate `bencher` crate in the workspace provides the core framework.
System-specific benchmarks live in each system's crate (e.g., `log/bench/`)
and depend on `bencher` for common infrastructure.

### Configuration

```rust
pub struct Config {
    /// Storage for benchmark data.
    pub data: DataConfig,
    /// Optional reporter for persisting metrics.
    pub reporter: Option<ReporterConfig>,
}

pub struct ReporterConfig {
    /// Object store for metrics storage.
    pub object_store: ObjectStoreConfig,
    /// Interval for periodic metrics reporting.
    pub interval: Duration,  // default: 10s
}
```

### Core API

```rust
pub struct Bencher { /* ... */ }

impl Bencher {
    /// Start a benchmark run with the given parameters.
    pub fn bench(&self, params: impl Into<Vec<Label>>) -> Bench;

    /// Access data storage configuration.
    pub fn data_config(&self) -> &DataConfig;
}

pub struct Bench { /* ... */ }

impl Bench {
    /// Ongoing metrics - updated during the benchmark.
    pub fn counter(&self, name: &'static str) -> Counter;
    pub fn gauge(&self, name: &'static str) -> Gauge;
    pub fn histogram(&self, name: &'static str) -> Histogram;

    /// Summary metrics - recorded at the end.
    pub async fn summarize(&self, summary: Summary) -> Result<()>;

    /// Mark the run as complete.
    pub async fn close(self) -> Result<()>;
}

pub struct Summary { /* ... */ }

impl Summary {
    pub fn new() -> Self;
    pub fn add(self, name: impl Into<String>, value: f64) -> Self;
}

/// Entry point for a benchmark. Implemented by benchmark authors.
#[async_trait]
pub trait Benchmark: Send + Sync {
    fn name(&self) -> &str;
    fn labels(&self) -> Vec<Label> { vec![] }
    async fn run(&self, bencher: &Bencher) -> Result<()>;
}
```

### Output

Summary metrics are always printed to console. If a reporter is configured,
both ongoing and summary metrics are persisted to the configured object store
using the TimeSeries format.

### Example

```rust
use bencher::{Benchmark, Bencher, Label, Summary};

pub struct IngestBenchmark {
    params: Vec<Params>,
}

#[async_trait]
impl Benchmark for IngestBenchmark {
    fn name(&self) -> &str { "ingest" }

    async fn run(&self, bencher: &Bencher) -> Result<()> {
        for params in &self.params {
            let bench = bencher.bench(params);

            // Ongoing metrics
            let records = bench.counter("records");
            let latency = bench.histogram("latency_us");

            // ... run workload, updating metrics ...

            // Summary metrics
            bench.summarize(
                Summary::new()
                    .add("throughput_ops", ops_per_sec)
                    .add("elapsed_ms", elapsed_ms)
            ).await?;

            bench.close().await?;
        }
        Ok(())
    }
}
```

Console output:

```
Running benchmark: ingest
  [batch_size=100, value_size=256, num_keys=10]
    throughput_ops  526.47K
    elapsed_ms      1
```

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

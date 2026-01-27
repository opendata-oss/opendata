# RFC 0003: Common Benchmark Framework

**Status**: Draft

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

A separate `bencher` crate in the `common` workspace provides the core framework.
System-specific benchmarks live in each system's crate (e.g., `log/benches/`,
`timeseries/benches/`) and depend on `bencher` for common infrastructure.

### Core API

The framework provides two structs—no traits required for benchmark authors:

```rust
/// Configuration for the bencher.
pub struct BencherConfig {
    /// Object store for both benchmark storage and metrics export.
    pub object_store: ObjectStoreConfig,
    /// Output format for metrics.
    pub output: OutputFormat,
}

/// Metrics output format.
pub enum OutputFormat {
    /// Write CSV files.
    Csv,
    /// Write to timeseries format.
    Timeseries,
}

/// Provided by the framework. Initializes storage and metrics reporting.
pub struct Bencher {
    config: BencherConfig,
}

impl Bencher {
    pub fn new(config: BencherConfig) -> Self;

    /// Start a new benchmark run with the given labels.
    pub fn bench(&self, labels: Vec<Label>) -> Bench;
}

/// Represents a single benchmark run.
pub struct Bench {
    // ...
}

impl Bench {
    /// Access the storage backend.
    pub fn storage(&self) -> &StorageConfig;

    /// Record a metric sample. Labels were set when the bench was created.
    pub fn record(&self, metric: &str, sample: Sample);

    /// Mark the run as complete and flush metrics.
    pub fn close(self);
}

/// Entry point for a benchmark. Implemented by benchmark authors.
pub trait Benchmark {
    /// Run the benchmark using the provided bencher.
    async fn run(&self, bencher: Bencher) -> Result<()>;
}
```

A harness (not defined here) is responsible for instantiating the `Bencher`
from configuration and invoking `Benchmark::run`. The benchmark author controls
iteration over parameter space, setup, and teardown.

### CSV Output Format

Results are written as CSV compatible with Apache Otava's expected format:

```csv
timestamp,benchmark,commit,ops_per_sec,bytes_per_sec,p50_us,p99_us,p999_us
2026-01-22T10:30:00Z,log_append_throughput,abc123,150000,78643200,45,120,450
2026-01-22T10:31:00Z,log_scan_throughput,abc123,50000,52428800,200,500,1200
```

The `commit` column serves as the attribute Otava uses to correlate change
points with git history.

### Results Storage

Benchmark results can be exported to:

- **CSV on S3**: For consumption by Apache Otava and other analysis tools.
- **Timeseries database**: For querying with PromQL and visualization in Grafana.

Using the timeseries data model for metrics enables both export paths without
conversion.

### Example Usage

```rust
use bencher::{Benchmark, Bencher, Bench};
use timeseries::{Label, Sample};

pub struct LogAppendBenchmark;

impl Benchmark for LogAppendBenchmark {
    async fn run(&self, bencher: Bencher) -> Result<()> {
        for record_size in [64, 256, 1024, 4096] {
            let labels = vec![
                Label::new("benchmark", "log_append"),
                Label::new("record_size", record_size.to_string()),
            ];

            let bench = bencher.bench(labels);
            let mut log = Log::open(bench.storage()).await?;
            let data = vec![0u8; record_size];

            for _ in 0..1000 {
                let start = Instant::now();
                log.append(&data).await?;
                bench.record("latency_us", Sample::now(start.elapsed().as_micros() as f64));
            }

            bench.close();
        }
        Ok(())
    }
}
```

### Future Work

The CSV output format enables regression analysis through tools like
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

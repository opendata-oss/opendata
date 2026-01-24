# RFC 0003: Common Benchmark Framework

**Status**: Implemented

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

A minimal benchmark framework for OpenData that handles storage initialization
and metrics reporting while leaving workload structure to benchmark authors.

## Design

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
```

### Benchmark Trait

```rust
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

## Updates

| Date       | Description                |
|------------|----------------------------|
| 2026-01-16 | Initial draft              |
| 2026-01-23 | Updated to reflect implementation |

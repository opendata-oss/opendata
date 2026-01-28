# bencher

Minimal benchmark framework for OpenData. Handles storage initialization and
metrics reporting while leaving workload structure to benchmark authors.

## Modules

- **lib.rs** - Core API: `Bencher`, `Benchmark` trait, `run()` entry point
- **bench.rs** - Runtime types: `Bench`, `BenchSpec`, `Runner`, `Summary`
- **params.rs** - Parameter collection: `Params`
- **config.rs** - Configuration types: `Config`, `DataConfig`, `ReporterConfig`
- **cli.rs** - CLI argument parsing: `Args`
- **metrics.rs** - Internal metrics collection using the `metrics` crate
- **reporter.rs** - `Reporter` trait and `TimeSeriesReporter` implementation

## Usage

```rust
use bencher::{Benchmark, Bencher, Label, Summary};

struct MyBenchmark;

#[async_trait::async_trait]
impl Benchmark for MyBenchmark {
    fn name(&self) -> &str { "my_benchmark" }

    async fn run(&self, bencher: &Bencher) -> anyhow::Result<()> {
        let params = vec![Label::new("size", "1024")];
        let bench = bencher.bench(params);

        // Ongoing metrics (updated during benchmark)
        let ops = bench.counter("ops");
        let latency = bench.histogram("latency_us");

        // Run workload
        for _ in 0..1000 {
            let start = std::time::Instant::now();
            // ... do work ...
            ops.increment(1);
            latency.record(start.elapsed().as_micros() as f64);
        }

        // Summary metrics (recorded at end)
        bench.summarize(
            Summary::new()
                .add("throughput", 1000.0 / elapsed_secs)
        ).await?;

        bench.close().await
    }
}
```

Run with `bencher::run`:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bencher::run(vec![Box::new(MyBenchmark)]).await
}
```

## Configuration

```toml
[data.storage]
type = "in_memory"

[reporter]
interval = "10s"

[reporter.object_store]
type = "local"
path = "/tmp/bench-results"
```

Summary metrics are always printed to console. If `reporter` is configured,
metrics are also persisted to the object store.

//! Benchmarks for the timeseries database.

mod ingest;

use bencher::Benchmark;

fn benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![Box::new(ingest::IngestBenchmark::new())]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bencher::run(benchmarks()).await
}

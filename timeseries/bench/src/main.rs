//! Benchmarks for the timeseries database.

use bencher::Benchmark;

fn benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bencher::run(benchmarks()).await
}

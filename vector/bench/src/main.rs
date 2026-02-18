//! Benchmarks for the vector database.

mod recall;

use bencher::Benchmark;
use tracing_subscriber::EnvFilter;

fn benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![Box::new(recall::RecallBenchmark::new())]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    bencher::run(benchmarks()).await
}

//! Benchmarks for the log database.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod ingest;
mod slatedb_metrics;

use bencher::Benchmark;

fn benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![Box::new(ingest::IngestBenchmark::new())]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Install before any LogDb opens so SlateDB metric registrations land in
    // our registry.
    slatedb_metrics::install();
    bencher::run(benchmarks()).await
}

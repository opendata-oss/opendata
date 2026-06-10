//! Benchmarks for the log database.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod ingest;
mod scan;

use bencher::Benchmark;

fn benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![
        Box::new(ingest::IngestBenchmark::new()),
        Box::new(scan::ScanBenchmark::new()),
    ]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bencher::run(benchmarks()).await
}

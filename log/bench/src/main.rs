//! Benchmarks for the log database.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod burst_read;
mod follow;
mod ingest;
mod keyscale;
mod read;
mod workload;

use bencher::Benchmark;

fn benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![
        Box::new(ingest::IngestBenchmark::new()),
        Box::new(follow::FollowBenchmark::new()),
        Box::new(burst_read::BurstReadBenchmark::new()),
        Box::new(keyscale::KeyScaleBenchmark::new()),
    ]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bencher::run(benchmarks()).await
}

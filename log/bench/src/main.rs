//! Benchmarks for the log database.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod ingest;
mod slatedb_metrics;

use bencher::Benchmark;
use tracing_subscriber::EnvFilter;

fn benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![Box::new(ingest::IngestBenchmark::new())]
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Honor RUST_LOG (e.g. `RUST_LOG=slatedb=info,log=debug`). Falls back to
    // `warn` if unset, so we don't dump anything noisy by default.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    // Install before any LogDb opens so SlateDB metric registrations land in
    // our registry.
    slatedb_metrics::install();
    bencher::run(benchmarks()).await
}

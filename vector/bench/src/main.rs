//! Benchmarks for the vector database.

mod recall;

use bencher::Benchmark;
use common::tracing::{TracingConfig, TracingGuard, init as init_tracing};

/// Environment variable: when set, trace JSON files are written under this
/// directory. When unset, only console logs are emitted. Use
/// `OPENDATA_TRACE=<filter>` alongside to scope what lands in the file.
const TRACE_DIR_ENV: &str = "OPENDATA_TRACE_DIR";

fn benchmarks() -> Vec<Box<dyn Benchmark>> {
    vec![Box::new(recall::RecallBenchmark::new())]
}

fn install_tracing() -> TracingGuard {
    let output_dir = std::env::var(TRACE_DIR_ENV).ok().map(Into::into);
    let always_on = output_dir.is_some();
    let guard = init_tracing(TracingConfig {
        output_dir,
        always_on,
        ..Default::default()
    });
    if let Some(path) = guard.output_path() {
        eprintln!("tracing: chrome trace file → {}", path.display());
    }
    guard
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _trace_guard = install_tracing();
    bencher::run(benchmarks()).await
}

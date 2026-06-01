//! Generate ground-truth files for the datasets in a bench config.
//!
//! Reads the same TOML config the benchmark runner uses, and for every
//! `[[params.recall]]` dataset computes exact top-k neighbours (filtered
//! when the dataset configures a `filter_spec`) and writes the
//! `ground_truth_file` in the dataset's format (`ivecs` for fvecs/bvecs,
//! Parquet for parquet).
//!
//! Usage:
//!   cargo run -p vector-bench --release --bin gen_groundtruth -- --config bench.toml

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::path::PathBuf;

use bencher::Config;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config_path = parse_config_path()?;
    let contents = std::fs::read_to_string(&config_path)
        .map_err(|e| anyhow::anyhow!("failed to read {}: {}", config_path.display(), e))?;
    let config: Config = toml::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("failed to parse {}: {}", config_path.display(), e))?;

    vector_bench::recall::generate_ground_truth(&config).await
}

fn parse_config_path() -> anyhow::Result<PathBuf> {
    let mut config = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                config =
                    Some(PathBuf::from(args.next().ok_or_else(|| {
                        anyhow::anyhow!("--config requires a path")
                    })?));
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: gen_groundtruth --config <bench.toml>\n\n\
                     Generates ground-truth files for every [[params.recall]] dataset \
                     in the config."
                );
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }
    config.ok_or_else(|| anyhow::anyhow!("missing required --config <path>"))
}

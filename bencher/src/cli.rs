//! Command-line interface for the bencher.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::config::{Config, DataConfig};

/// CLI arguments for the bencher.
#[derive(Parser, Debug)]
#[command(about = "OpenData benchmark runner")]
pub struct Args {
    /// Run only the benchmark with this name.
    #[arg(value_name = "BENCHMARK", conflicts_with = "benchmark")]
    pub benchmark_name: Option<String>,

    /// Path to config file (TOML).
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Run only the benchmark with this name.
    #[arg(short, long)]
    pub benchmark: Option<String>,

    /// Duration for each benchmark run in seconds.
    #[arg(short, long, default_value = "5")]
    pub duration: u64,

    /// Skip cleanup of benchmark data after completion.
    #[arg(long)]
    pub no_cleanup: bool,
}

impl Args {
    /// Parse CLI arguments.
    pub fn parse_args() -> Self {
        Self::parse()
    }

    /// Get the selected benchmark name.
    pub fn benchmark(&self) -> Option<&str> {
        self.benchmark.as_deref().or(self.benchmark_name.as_deref())
    }

    /// Get the benchmark duration.
    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.duration)
    }

    /// Load configuration from file or use defaults.
    pub fn load_config(&self) -> anyhow::Result<Config> {
        match &self.config {
            Some(path) => {
                let contents = std::fs::read_to_string(path)?;
                let config: Config = toml::from_str(&contents)?;
                Ok(config)
            }
            None => Ok(Config::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Args;
    use clap::Parser;

    #[test]
    fn should_parse_benchmark_from_flag() {
        // given/when
        let args = Args::parse_from(["vector-bench", "--benchmark", "recall"]);

        // then
        assert_eq!(args.benchmark(), Some("recall"));
    }

    #[test]
    fn should_parse_benchmark_from_positional_argument() {
        // given/when
        let args = Args::parse_from(["vector-bench", "usearch"]);

        // then
        assert_eq!(args.benchmark(), Some("usearch"));
    }
}

impl Config {
    /// Default configuration for development/testing.
    fn default() -> Self {
        use common::StorageConfig;

        Self {
            data: DataConfig {
                storage: StorageConfig::InMemory,
            },
            reporter: None,
            params: Default::default(),
        }
    }
}

//! Command-line interface for the bencher.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::config::{Config, DataConfig};

/// CLI arguments for the bencher.
#[derive(Parser, Debug)]
#[command(about = "OpenData benchmark runner")]
pub struct Args {
    /// Path to config file (TOML).
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Run only the benchmark with this name.
    #[arg(short, long)]
    pub benchmark: Option<String>,

    /// Duration for each benchmark run in seconds.
    #[arg(short, long, default_value = "5")]
    pub duration: u64,
}

impl Args {
    /// Parse CLI arguments.
    pub fn parse_args() -> Self {
        Self::parse()
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

impl Config {
    /// Default configuration for development/testing.
    fn default() -> Self {
        use common::StorageConfig;

        Self {
            data: DataConfig {
                storage: StorageConfig::InMemory,
            },
            reporter: None,
        }
    }
}

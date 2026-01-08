//! Prometheus-compatible configuration for scraping targets.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use clap::Parser;
use common::storage::config::StorageConfig;
use serde::Deserialize;

use crate::util::Result;

/// CLI arguments for the server.
#[derive(Parser, Debug)]
#[command(name = "timeseries")]
#[command(about = "Prometheus-compatible time series database")]
pub struct CliArgs {
    /// Path to the prometheus.yaml configuration file
    #[arg(short, long, env = "PROMETHEUS_CONFIG_FILE")]
    pub config: Option<String>,

    /// Port to listen on
    #[arg(short, long, default_value = "9090", env = "OPEN_TSDB_PORT")]
    pub port: u16,
}

/// Root configuration matching prometheus.yaml structure.
#[derive(Debug, Clone, Deserialize)]
pub struct PrometheusConfig {
    #[serde(default)]
    pub global: GlobalConfig,
    #[serde(default)]
    pub scrape_configs: Vec<ScrapeConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
    /// Flush interval in seconds for persisting data to storage.
    /// Defaults to 5 seconds.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
}

fn default_flush_interval_secs() -> u64 {
    5
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            global: GlobalConfig::default(),
            scrape_configs: Vec::new(),
            storage: StorageConfig::default(),
            flush_interval_secs: default_flush_interval_secs(),
        }
    }
}

/// Global configuration defaults.
#[derive(Debug, Clone, Deserialize)]
pub struct GlobalConfig {
    #[serde(default = "default_scrape_interval")]
    pub scrape_interval: String,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            scrape_interval: default_scrape_interval(),
        }
    }
}

fn default_scrape_interval() -> String {
    "15s".to_string()
}

/// Configuration for a single scrape job.
#[derive(Debug, Clone, Deserialize)]
pub struct ScrapeConfig {
    pub job_name: String,
    #[serde(default)]
    pub scrape_interval: Option<String>,
    #[serde(default)]
    pub static_configs: Vec<StaticConfig>,
}

impl ScrapeConfig {
    /// Get the effective scrape interval for this job.
    pub fn effective_interval(&self, global: &GlobalConfig) -> Duration {
        let interval_str = self
            .scrape_interval
            .as_deref()
            .unwrap_or(&global.scrape_interval);
        parse_duration(interval_str).unwrap_or(Duration::from_secs(15))
    }
}

/// Static target configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct StaticConfig {
    pub targets: Vec<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Load Prometheus configuration from a YAML file.
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<PrometheusConfig> {
    let contents = std::fs::read_to_string(path.as_ref()).map_err(|e| {
        crate::error::Error::InvalidInput(format!("Failed to read config file: {}", e))
    })?;

    serde_yaml::from_str(&contents).map_err(|e| {
        crate::error::Error::InvalidInput(format!("Failed to parse config file: {}", e))
    })
}

/// Parse a Prometheus-style duration string (e.g., "15s", "1m", "2h").
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err("Empty duration string".into());
    }

    // Find where the numeric part ends
    let num_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());

    if num_end == 0 {
        return Err("Duration must start with a number".into());
    }

    let value: f64 = s[..num_end]
        .parse()
        .map_err(|_| "Invalid duration number")?;
    let unit = &s[num_end..];

    let multiplier = match unit {
        "ms" => 0.001,
        "s" | "" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        "d" => 86400.0,
        _ => {
            return Err(crate::error::Error::InvalidInput(format!(
                "Unknown duration unit: {}",
                unit
            )));
        }
    };

    Ok(Duration::from_secs_f64(value * multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::seconds("15s", 15)]
    #[case::minutes("1m", 60)]
    #[case::hours("2h", 7200)]
    #[case::days("1d", 86400)]
    #[case::milliseconds("500ms", 0)]
    #[case::no_unit("30", 30)]
    #[case::fractional_seconds("1.5s", 1)]
    fn should_parse_duration(#[case] input: &str, #[case] expected_secs: u64) {
        // when
        let result = parse_duration(input).unwrap();

        // then
        assert_eq!(result.as_secs(), expected_secs);
    }

    #[rstest]
    #[case::empty("")]
    #[case::invalid_unit("15x")]
    #[case::no_number("s")]
    fn should_fail_to_parse_invalid_duration(#[case] input: &str) {
        // when
        let result = parse_duration(input);

        // then
        assert!(result.is_err());
    }

    #[test]
    fn should_parse_prometheus_config() {
        // given
        let yaml = r#"
global:
  scrape_interval: 30s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets:
          - localhost:9090

  - job_name: node
    scrape_interval: 1m
    static_configs:
      - targets:
          - localhost:9100
          - localhost:9101
        labels:
          env: production
"#;

        // when
        let config: PrometheusConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        assert_eq!(config.global.scrape_interval, "30s");
        assert_eq!(config.scrape_configs.len(), 2);

        let prometheus_job = &config.scrape_configs[0];
        assert_eq!(prometheus_job.job_name, "prometheus");
        assert!(prometheus_job.scrape_interval.is_none());
        assert_eq!(prometheus_job.static_configs[0].targets.len(), 1);

        let node_job = &config.scrape_configs[1];
        assert_eq!(node_job.job_name, "node");
        assert_eq!(node_job.scrape_interval, Some("1m".to_string()));
        assert_eq!(node_job.static_configs[0].targets.len(), 2);
        assert_eq!(
            node_job.static_configs[0].labels.get("env"),
            Some(&"production".to_string())
        );
    }

    #[test]
    fn should_use_default_scrape_interval() {
        // given
        let yaml = r#"
scrape_configs:
  - job_name: test
    static_configs:
      - targets: ['localhost:9090']
"#;

        // when
        let config: PrometheusConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        assert_eq!(config.global.scrape_interval, "15s");
    }

    #[test]
    fn should_compute_effective_interval() {
        // given
        let global = GlobalConfig {
            scrape_interval: "30s".to_string(),
        };
        let job_with_override = ScrapeConfig {
            job_name: "test".to_string(),
            scrape_interval: Some("1m".to_string()),
            static_configs: vec![],
        };
        let job_without_override = ScrapeConfig {
            job_name: "test2".to_string(),
            scrape_interval: None,
            static_configs: vec![],
        };

        // when/then
        assert_eq!(
            job_with_override.effective_interval(&global),
            Duration::from_secs(60)
        );
        assert_eq!(
            job_without_override.effective_interval(&global),
            Duration::from_secs(30)
        );
    }
}

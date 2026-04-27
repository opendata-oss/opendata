//! Prometheus-compatible configuration for scraping targets.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use crate::util::Result;
#[cfg(feature = "otel")]
use common::ObjectStoreConfig;
use common::storage::config::StorageConfig;
use serde::{Deserialize, Deserializer};
use slatedb::config::DbReaderOptions;
use uuid::Uuid;

#[cfg(feature = "http-server")]
use clap::Parser;

/// CLI arguments for the server.
#[cfg(feature = "http-server")]
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
#[derive(Clone, Deserialize)]
pub struct PrometheusConfig {
    #[serde(default)]
    pub global: GlobalConfig,
    #[serde(default)]
    pub scrape_configs: Vec<ScrapeConfig>,
    #[serde(default)]
    pub otel: OtelServerConfig,
    #[cfg(feature = "otel")]
    #[serde(default)]
    pub ingest_consumer: Option<IngestConsumerConfig>,
    #[serde(default)]
    pub storage: StorageConfig,
    /// Flush interval in seconds for persisting data to storage.
    /// Defaults to 5 seconds.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    /// Run in read-only mode (no writes, no scraping, no fencing).
    #[serde(default)]
    pub read_only: bool,
    /// SlateDB reader configuration used when `read_only` is true.
    #[serde(
        default = "default_reader_options",
        deserialize_with = "deserialize_reader_options"
    )]
    pub reader: DbReaderOptions,
    /// Pin a read-only instance to a specific SlateDB checkpoint. Only
    /// honored when `read_only` is true. When unset, the reader follows the
    /// latest manifest as governed by `reader.manifest_poll_interval`.
    #[serde(default)]
    pub checkpoint_id: Option<Uuid>,
    /// Maximum number of bucket readers to cache in memory.
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: u64,
    /// Startup cache warmer configuration. Scans recent buckets on startup
    /// to pre-populate the block cache. Enabled by default (24h, with samples).
    #[serde(default = "default_cache_warmer")]
    pub cache_warmer: Option<CacheWarmerConfig>,
    /// Per-query tracing configuration. Controls whether each query collects
    /// per-phase / per-operator timing and returns it in the response.
    #[serde(default)]
    pub tracing: TracingConfig,
}

/// Controls per-query PromQL tracing.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TracingConfig {
    /// When true, every query is traced and its trace is returned inline
    /// in the response. When false, tracing is opt-in per query via
    /// `?trace=true`.
    pub enabled: bool,
}

fn default_flush_interval_secs() -> u64 {
    5
}

fn default_reader_options() -> DbReaderOptions {
    DbReaderOptions {
        skip_wal_replay: true,
        ..DbReaderOptions::default()
    }
}

fn deserialize_reader_options<'de, D>(
    deserializer: D,
) -> std::result::Result<DbReaderOptions, D::Error>
where
    D: Deserializer<'de>,
{
    let overrides = serde_yaml::Value::deserialize(deserializer)?;
    let mut defaults =
        serde_yaml::to_value(default_reader_options()).map_err(serde::de::Error::custom)?;
    merge_yaml_value(&mut defaults, overrides);
    serde_yaml::from_value(defaults).map_err(serde::de::Error::custom)
}

fn merge_yaml_value(base: &mut serde_yaml::Value, overrides: serde_yaml::Value) {
    match (base, overrides) {
        (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(overrides_map)) => {
            for (key, value) in overrides_map {
                match base_map.get_mut(&key) {
                    Some(existing) => merge_yaml_value(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_slot, override_value) => *base_slot = override_value,
    }
}

fn default_cache_capacity() -> u64 {
    crate::reader::DEFAULT_CACHE_CAPACITY
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            global: GlobalConfig::default(),
            scrape_configs: Vec::new(),
            otel: OtelServerConfig::default(),
            #[cfg(feature = "otel")]
            ingest_consumer: None,
            storage: StorageConfig::default(),
            flush_interval_secs: default_flush_interval_secs(),
            read_only: false,
            reader: default_reader_options(),
            checkpoint_id: None,
            cache_capacity: default_cache_capacity(),
            cache_warmer: default_cache_warmer(),
            tracing: TracingConfig::default(),
        }
    }
}

/// OTEL ingest configuration for the HTTP server.
#[derive(Debug, Clone, Deserialize)]
pub struct OtelServerConfig {
    #[serde(default = "default_true")]
    pub include_resource_attrs: bool,
    #[serde(default = "default_true")]
    pub include_scope_attrs: bool,
}

impl Default for OtelServerConfig {
    fn default() -> Self {
        Self {
            include_resource_attrs: true,
            include_scope_attrs: true,
        }
    }
}

fn default_true() -> bool {
    true
}

/// Configuration for the ingest consumer background task.
#[cfg(feature = "otel")]
#[derive(Debug, Clone, Deserialize)]
pub struct IngestConsumerConfig {
    /// Object store where the ingest queue lives.
    pub object_store: ObjectStoreConfig,

    /// Manifest path matching the ingestor's `manifest_path`.
    #[serde(default = "default_ingest_manifest_path")]
    pub manifest_path: String,

    /// Poll interval when the queue is empty (e.g. "100ms", "1s").
    #[serde(
        default = "default_poll_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub poll_interval: Duration,

    /// Path prefix for data batch objects in object storage.
    #[serde(default = "default_data_path_prefix")]
    pub data_path_prefix: String,

    /// How often the garbage collector runs (e.g. "5m", "300s").
    #[serde(
        default = "default_gc_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub gc_interval: Duration,

    /// Minimum age before an unreferenced batch file is deleted (e.g. "10m", "600s").
    #[serde(
        default = "default_gc_grace_period",
        deserialize_with = "deserialize_duration"
    )]
    pub gc_grace_period: Duration,
}

#[cfg(feature = "otel")]
fn default_ingest_manifest_path() -> String {
    "ingest/manifest".to_string()
}

#[cfg(feature = "otel")]
fn default_poll_interval() -> Duration {
    Duration::from_secs(1)
}

#[cfg(feature = "otel")]
fn default_data_path_prefix() -> String {
    "ingest".to_string()
}

#[cfg(feature = "otel")]
fn default_gc_interval() -> Duration {
    Duration::from_secs(300)
}

#[cfg(feature = "otel")]
fn default_gc_grace_period() -> Duration {
    Duration::from_secs(600)
}

fn deserialize_duration<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_duration(&s).map_err(serde::de::Error::custom)
}

/// Configuration for the startup cache warmer.
///
/// When present, the server scans recent time bucket key ranges on startup
/// to pre-populate the block cache. This is a temporary workaround until
/// SlateDB's CacheManager is available.
#[derive(Debug, Clone, Deserialize)]
pub struct CacheWarmerConfig {
    /// How far back to warm (e.g., "6h", "24h"). Defaults to 24h.
    #[serde(
        default = "default_warm_range",
        deserialize_with = "deserialize_duration"
    )]
    pub warm_range: Duration,

    /// Whether to warm timeseries sample data in addition to metadata.
    /// Defaults to true.
    #[serde(default = "default_true")]
    pub include_samples: bool,
}

fn default_warm_range() -> Duration {
    Duration::from_hours(24)
}

fn default_cache_warmer() -> Option<CacheWarmerConfig> {
    Some(CacheWarmerConfig {
        warm_range: default_warm_range(),
        include_samples: true,
    })
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
        assert!(config.otel.include_resource_attrs);
        assert!(config.otel.include_scope_attrs);

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

    #[test]
    fn should_parse_otel_config() {
        // given
        let yaml = r#"
otel:
  include_resource_attrs: false
  include_scope_attrs: true
"#;

        // when
        let config: PrometheusConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        assert!(!config.otel.include_resource_attrs);
        assert!(config.otel.include_scope_attrs);
    }

    #[test]
    fn should_use_default_otel_config() {
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
        assert!(config.otel.include_resource_attrs);
        assert!(config.otel.include_scope_attrs);
    }

    #[test]
    fn should_parse_reader_config() {
        // given
        let yaml = r#"
storage:
  type: InMemory
read_only: true
cache_capacity: 200
reader:
  manifest_poll_interval:
    secs: 86400
    nanos: 0
  skip_wal_replay: false
"#;

        // when
        let config: PrometheusConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        assert!(config.read_only);
        assert_eq!(
            config.reader.manifest_poll_interval,
            Duration::from_secs(86400)
        );
        assert_eq!(config.cache_capacity, 200);
        assert!(!config.reader.skip_wal_replay);
    }

    #[cfg(feature = "otel")]
    #[test]
    fn should_parse_ingest_consumer_poll_interval() {
        // given
        let yaml = r#"
ingest_consumer:
  object_store:
    type: InMemory
  manifest_path: ingest/manifest
  poll_interval: 250ms
"#;

        // when
        let config: PrometheusConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        let consumer = config.ingest_consumer.unwrap();
        assert_eq!(consumer.poll_interval, Duration::from_millis(250));
        assert_eq!(consumer.manifest_path, "ingest/manifest");
    }

    #[cfg(feature = "otel")]
    #[test]
    fn should_use_default_ingest_consumer_poll_interval() {
        // given
        let yaml = r#"
ingest_consumer:
  object_store:
    type: InMemory
"#;

        // when
        let config: PrometheusConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        let consumer = config.ingest_consumer.unwrap();
        assert_eq!(consumer.poll_interval, Duration::from_secs(1));
    }
}

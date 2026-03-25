//! Configuration options for OpenData TimeSeries operations.
//!
//! This module defines the configuration and options structs that control
//! the behavior of the time series database, including storage setup and
//! write operation parameters.

use std::time::Duration;

use common::StorageConfig;
use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};

/// Configuration for opening a [`TimeSeriesDb`](crate::TimeSeriesDb) database.
///
/// This struct holds all the settings needed to initialize a time series
/// instance, including storage backend configuration and operational parameters.
///
/// # Example
///
/// ```no_run
/// use timeseries::Config;
/// use common::StorageConfig;
/// use std::time::Duration;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config {
///     storage: StorageConfig::default(),
///     flush_interval: Duration::from_secs(30),
///     retention: Some(Duration::from_secs(86400 * 7)), // 7 days
/// };
/// let ts = timeseries::TimeSeriesDb::open(config).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage backend configuration.
    ///
    /// Determines where and how time series data is persisted. See [`StorageConfig`]
    /// for available options including in-memory and SlateDB backends.
    pub storage: StorageConfig,

    /// How often to flush data to durable storage.
    ///
    /// Data is buffered in memory and periodically flushed to the storage backend.
    /// Lower values provide better durability at the cost of write performance.
    pub flush_interval: Duration,

    /// Maximum age of data to retain.
    ///
    /// Data older than this duration may be automatically deleted during
    /// compaction. Set to `None` to retain data indefinitely.
    pub retention: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            flush_interval: Duration::from_secs(60),
            retention: None,
        }
    }
}

/// Configuration for opening a [`TimeSeriesDbReader`](crate::TimeSeriesDbReader).
///
/// This struct holds settings for read-only time series access, including storage
/// backend configuration and automatic refresh settings.
///
/// # Example
///
/// ```ignore
/// use timeseries::ReaderConfig;
/// use common::StorageConfig;
/// use std::time::Duration;
///
/// let config = ReaderConfig {
///     storage: StorageConfig::default(),
///     refresh_interval: Duration::from_secs(1),
///     ..Default::default()
/// };
/// let reader = TimeSeriesDbReader::open(config).await?;
/// ```
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderConfig {
    /// Storage backend configuration.
    ///
    /// Determines where and how time series data is read. See [`StorageConfig`]
    /// for available options including in-memory and SlateDB backends.
    pub storage: StorageConfig,

    /// Interval for discovering new time series data.
    ///
    /// The reader periodically polls the SlateDB manifest at this interval
    /// to discover new data written by other processes.
    ///
    /// Defaults to 1 second.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval: Duration,

    /// Maximum number of bucket readers to cache in memory.
    ///
    /// Each bucket reader holds open references to the underlying storage for
    /// a single time bucket. Increasing this value trades memory for reduced
    /// storage lookups on repeated queries.
    ///
    /// Defaults to 50.
    #[serde(default = "default_cache_capacity")]
    pub cache_capacity: u64,
}

fn default_refresh_interval() -> Duration {
    Duration::from_secs(1)
}

/// Default number of bucket readers cached in memory.
pub(crate) const DEFAULT_CACHE_CAPACITY: u64 = 50;

fn default_cache_capacity() -> u64 {
    DEFAULT_CACHE_CAPACITY
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            refresh_interval: default_refresh_interval(),
            cache_capacity: default_cache_capacity(),
        }
    }
}

// ── Runtime config (parsed from env at startup) ──────────────────────

/// Configuration for read-path load budgeting (semaphore permits).
#[derive(Debug, Clone)]
pub(crate) struct ReadLoadConfig {
    pub sample_permits: usize,
    pub metadata_permits: usize,
}

impl ReadLoadConfig {
    const DEFAULT_SAMPLE_PERMITS: usize = 32;
    const DEFAULT_METADATA_PERMITS: usize = 16;

    pub(crate) fn from_env() -> Self {
        let sample = std::env::var("TSDB_SAMPLE_PERMITS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(Self::DEFAULT_SAMPLE_PERMITS);
        let metadata = std::env::var("TSDB_METADATA_PERMITS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(Self::DEFAULT_METADATA_PERMITS);
        Self {
            sample_permits: sample,
            metadata_permits: metadata,
        }
    }
}

impl Default for ReadLoadConfig {
    fn default() -> Self {
        Self {
            sample_permits: Self::DEFAULT_SAMPLE_PERMITS,
            metadata_permits: Self::DEFAULT_METADATA_PERMITS,
        }
    }
}

/// Configuration for the background metadata warmer.
#[derive(Debug, Clone)]
pub(crate) struct MetadataWarmConfig {
    pub enabled: bool,
    pub preload_bytes: u64,
    pub refresh_interval: Duration,
    pub rewarm_interval: Duration,
    pub max_concurrent_buckets: usize,
}

impl MetadataWarmConfig {
    pub(crate) fn from_env() -> Self {
        let preload_bytes = std::env::var("TSDB_METADATA_PRELOAD_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0u64);
        let refresh_secs = std::env::var("TSDB_METADATA_REFRESH_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60u64);
        let rewarm_secs = std::env::var("TSDB_METADATA_REWARM_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300u64);
        let concurrency = std::env::var("TSDB_METADATA_PRELOAD_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4usize);
        Self {
            enabled: preload_bytes > 0,
            preload_bytes,
            refresh_interval: Duration::from_secs(refresh_secs),
            rewarm_interval: Duration::from_secs(rewarm_secs),
            max_concurrent_buckets: concurrency,
        }
    }
}

impl Default for MetadataWarmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            preload_bytes: 0,
            refresh_interval: Duration::from_secs(60),
            rewarm_interval: Duration::from_secs(300),
            max_concurrent_buckets: 4,
        }
    }
}

/// Physical layout for sample records in storage.
///
/// Controls how sample keys are structured:
/// - `LegacySeriesId`: keys are `<bucket, series_id>` (current default)
/// - `MetricPrefixed`: keys are `<bucket, metric_name, series_id>` (experimental)
///
/// The metric-prefixed layout groups all series for one metric together in the
/// keyspace, which may reduce read amplification for single-metric queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SampleStorageLayout {
    #[default]
    LegacySeriesId,
    MetricPrefixed,
}

impl SampleStorageLayout {
    pub(crate) fn from_env() -> Self {
        Self::parse(std::env::var("TSDB_SAMPLE_STORAGE_LAYOUT").ok().as_deref())
    }

    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("metric_prefixed") => Self::MetricPrefixed,
            None | Some("legacy") | Some("") => Self::LegacySeriesId,
            Some(other) => {
                tracing::warn!(
                    value = other,
                    "Unknown TSDB_SAMPLE_STORAGE_LAYOUT value, falling back to legacy. \
                     Valid values: 'legacy', 'metric_prefixed'"
                );
                Self::LegacySeriesId
            }
        }
    }
}

/// Configuration for sample-loading experiments in B3.
#[derive(Debug, Clone)]
pub(crate) struct SampleReadExperimentConfig {
    pub enable_range_scan_batches: bool,
    pub range_scan_min_series: usize,
    pub range_scan_min_density: f64,
    pub range_scan_max_ranges: usize,
    pub range_scan_merge_gap_series: u32,
    pub per_query_sample_scan_concurrency: usize,
}

impl SampleReadExperimentConfig {
    const DEFAULT_MIN_SERIES: usize = 16;
    const DEFAULT_MIN_DENSITY: f64 = 0.25;
    const DEFAULT_MAX_RANGES: usize = 4;
    const DEFAULT_MERGE_GAP: u32 = 8;
    const DEFAULT_CONCURRENCY: usize = 4;

    pub(crate) fn from_env() -> Self {
        Self::parse(
            std::env::var("TSDB_SAMPLE_RANGE_SCAN_BATCHES").ok().as_deref(),
            std::env::var("TSDB_SAMPLE_RANGE_SCAN_MIN_SERIES").ok().as_deref(),
            std::env::var("TSDB_SAMPLE_RANGE_SCAN_MIN_DENSITY").ok().as_deref(),
            std::env::var("TSDB_SAMPLE_RANGE_SCAN_MAX_RANGES").ok().as_deref(),
            std::env::var("TSDB_SAMPLE_RANGE_SCAN_MERGE_GAP").ok().as_deref(),
            std::env::var("TSDB_SAMPLE_SCAN_CONCURRENCY").ok().as_deref(),
        )
    }

    fn parse(
        enable: Option<&str>,
        min_series: Option<&str>,
        min_density: Option<&str>,
        max_ranges: Option<&str>,
        merge_gap: Option<&str>,
        concurrency: Option<&str>,
    ) -> Self {
        let enable_range_scan_batches = matches!(enable, Some("true") | Some("1"));
        Self {
            enable_range_scan_batches,
            range_scan_min_series: min_series
                .and_then(|v| v.parse().ok())
                .unwrap_or(Self::DEFAULT_MIN_SERIES),
            range_scan_min_density: min_density
                .and_then(|v| v.parse().ok())
                .unwrap_or(Self::DEFAULT_MIN_DENSITY),
            range_scan_max_ranges: max_ranges
                .and_then(|v| v.parse().ok())
                .unwrap_or(Self::DEFAULT_MAX_RANGES),
            range_scan_merge_gap_series: merge_gap
                .and_then(|v| v.parse().ok())
                .unwrap_or(Self::DEFAULT_MERGE_GAP),
            per_query_sample_scan_concurrency: concurrency
                .and_then(|v| v.parse().ok())
                .unwrap_or(Self::DEFAULT_CONCURRENCY),
        }
    }
}

impl Default for SampleReadExperimentConfig {
    fn default() -> Self {
        Self {
            enable_range_scan_batches: false,
            range_scan_min_series: Self::DEFAULT_MIN_SERIES,
            range_scan_min_density: Self::DEFAULT_MIN_DENSITY,
            range_scan_max_ranges: Self::DEFAULT_MAX_RANGES,
            range_scan_merge_gap_series: Self::DEFAULT_MERGE_GAP,
            per_query_sample_scan_concurrency: Self::DEFAULT_CONCURRENCY,
        }
    }
}

/// Top-level runtime config for Tsdb, aggregating all subsystem configs.
#[derive(Debug, Clone, Default)]
pub(crate) struct TsdbRuntimeConfig {
    pub read_load: ReadLoadConfig,
    pub metadata_warm: MetadataWarmConfig,
    pub sample_storage_layout: SampleStorageLayout,
    pub sample_read_experiment: SampleReadExperimentConfig,
}

impl TsdbRuntimeConfig {
    pub(crate) fn from_env() -> Self {
        Self {
            read_load: ReadLoadConfig::from_env(),
            metadata_warm: MetadataWarmConfig::from_env(),
            sample_storage_layout: SampleStorageLayout::from_env(),
            sample_read_experiment: SampleReadExperimentConfig::from_env(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_load_config_defaults() {
        let config = ReadLoadConfig::default();
        assert_eq!(config.sample_permits, 32);
        assert_eq!(config.metadata_permits, 16);
    }

    #[test]
    fn metadata_warm_config_defaults() {
        let config = MetadataWarmConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.preload_bytes, 0);
        assert_eq!(config.refresh_interval, Duration::from_secs(60));
        assert_eq!(config.rewarm_interval, Duration::from_secs(300));
        assert_eq!(config.max_concurrent_buckets, 4);
    }

    #[test]
    fn metadata_warm_config_enabled_when_preload_bytes_positive() {
        let mut config = MetadataWarmConfig::default();
        // Default is disabled (preload_bytes = 0).
        assert!(!config.enabled);
        // Simulate from_env with positive bytes.
        config.enabled = config.preload_bytes > 0;
        assert!(!config.enabled);
        config.preload_bytes = 1024;
        config.enabled = config.preload_bytes > 0;
        assert!(config.enabled);
    }

    #[test]
    fn tsdb_runtime_config_defaults() {
        let config = TsdbRuntimeConfig::default();
        assert_eq!(config.read_load.sample_permits, 32);
        assert!(!config.metadata_warm.enabled);
    }

    #[test]
    fn should_parse_legacy_sample_storage_layout_by_default() {
        assert_eq!(
            SampleStorageLayout::parse(None),
            SampleStorageLayout::LegacySeriesId
        );
    }

    #[test]
    fn should_parse_metric_prefixed_sample_storage_layout() {
        assert_eq!(
            SampleStorageLayout::parse(Some("metric_prefixed")),
            SampleStorageLayout::MetricPrefixed
        );
    }

    #[test]
    fn should_parse_legacy_sample_storage_layout_explicitly() {
        assert_eq!(
            SampleStorageLayout::parse(Some("legacy")),
            SampleStorageLayout::LegacySeriesId
        );
    }

    #[test]
    fn should_warn_and_fallback_to_legacy_for_unknown_layout_value() {
        // Unknown values fall back to legacy (with a tracing::warn)
        assert_eq!(
            SampleStorageLayout::parse(Some("unknown_value")),
            SampleStorageLayout::LegacySeriesId
        );
    }

    #[test]
    fn should_parse_empty_string_as_legacy() {
        assert_eq!(
            SampleStorageLayout::parse(Some("")),
            SampleStorageLayout::LegacySeriesId
        );
    }

    #[test]
    fn sample_read_experiment_defaults() {
        let config = SampleReadExperimentConfig::default();
        assert!(!config.enable_range_scan_batches);
        assert_eq!(config.range_scan_min_series, 16);
        assert!((config.range_scan_min_density - 0.25).abs() < f64::EPSILON);
        assert_eq!(config.range_scan_max_ranges, 4);
        assert_eq!(config.range_scan_merge_gap_series, 8);
        assert_eq!(config.per_query_sample_scan_concurrency, 4);
    }

    #[test]
    fn sample_read_experiment_parse_defaults() {
        let config =
            SampleReadExperimentConfig::parse(None, None, None, None, None, None);
        assert!(!config.enable_range_scan_batches);
        assert_eq!(config.range_scan_min_series, 16);
        assert!((config.range_scan_min_density - 0.25).abs() < f64::EPSILON);
        assert_eq!(config.range_scan_max_ranges, 4);
        assert_eq!(config.range_scan_merge_gap_series, 8);
        assert_eq!(config.per_query_sample_scan_concurrency, 4);
    }

    #[test]
    fn sample_read_experiment_parse_enabled() {
        let config = SampleReadExperimentConfig::parse(
            Some("true"),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(config.enable_range_scan_batches);

        let config = SampleReadExperimentConfig::parse(
            Some("1"),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(config.enable_range_scan_batches);

        let config = SampleReadExperimentConfig::parse(
            Some("false"),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(!config.enable_range_scan_batches);
    }

    #[test]
    fn sample_read_experiment_parse_custom_values() {
        let config = SampleReadExperimentConfig::parse(
            Some("true"),
            Some("32"),
            Some("0.5"),
            Some("8"),
            Some("16"),
            Some("8"),
        );
        assert!(config.enable_range_scan_batches);
        assert_eq!(config.range_scan_min_series, 32);
        assert!((config.range_scan_min_density - 0.5).abs() < f64::EPSILON);
        assert_eq!(config.range_scan_max_ranges, 8);
        assert_eq!(config.range_scan_merge_gap_series, 16);
        assert_eq!(config.per_query_sample_scan_concurrency, 8);
    }
}

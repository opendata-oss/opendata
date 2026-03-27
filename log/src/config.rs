//! Configuration options for OpenData Log operations.
//!
//! This module defines the configuration and options structs that control
//! the behavior of the log, including storage setup and operation parameters.

use std::time::Duration;

use common::StorageConfig;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};

/// Configuration for opening a [`LogDb`](crate::LogDb).
///
/// This struct holds all the settings needed to initialize a log instance,
/// including storage backend configuration.
///
/// # Example
///
/// ```no_run
/// use log::{Config, SegmentConfig};
/// use common::StorageConfig;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config {
///     storage: StorageConfig::default(),
///     segmentation: SegmentConfig::default(),
///     ..Default::default()
/// };
/// let log = log::LogDb::open(config).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Config {
    /// Storage backend configuration.
    ///
    /// Determines where and how log data is persisted. See [`StorageConfig`]
    /// for available options including in-memory and SlateDB backends.
    pub storage: StorageConfig,

    /// Segmentation configuration.
    ///
    /// Controls how the log is partitioned into segments for efficient
    /// time-based queries and retention management.
    #[serde(default)]
    pub segmentation: SegmentConfig,

    /// Read visibility level.
    ///
    /// `Memory` (default) exposes writes as soon as they are visible in memory.
    /// `Remote` only exposes writes after remote/object-store durability is
    /// confirmed by the storage engine.
    ///
    /// Backward-compatibility: legacy boolean `read_durable` values are accepted
    /// and mapped as `false => Memory`, `true => Remote`.
    #[serde(
        default,
        alias = "read_durable",
        deserialize_with = "deserialize_read_visibility"
    )]
    pub read_visibility: ReadVisibility,

    /// Compaction scheduling strategy.
    #[serde(default)]
    pub compaction: CompactionConfig,
}

/// Controls how LSM compaction is scheduled.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum CompactionConfig {
    /// Use SlateDB's built-in compaction scheduler.
    #[default]
    Default,
    /// Only compact L0 SSTs, never merge sorted runs.
    L0Only(L0OnlyCompactionConfig),
}

/// Configuration for the L0-only compaction scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L0OnlyCompactionConfig {
    /// Minimum number of L0 SSTs required to trigger a compaction.
    #[serde(default = "default_min_l0_sources")]
    pub min_compaction_sources: usize,
    /// Maximum number of L0 SSTs included in a single compaction.
    #[serde(default = "default_max_l0_sources")]
    pub max_compaction_sources: usize,
}

fn default_min_l0_sources() -> usize {
    2
}
fn default_max_l0_sources() -> usize {
    8
}

impl Default for L0OnlyCompactionConfig {
    fn default() -> Self {
        Self {
            min_compaction_sources: default_min_l0_sources(),
            max_compaction_sources: default_max_l0_sources(),
        }
    }
}

/// Read visibility levels.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadVisibility {
    /// Return reads once data is visible in memory (fast, not crash-safe).
    #[default]
    Memory,
    /// Return reads only after remote/object-store durability is confirmed.
    Remote,
}

impl ReadVisibility {
    pub fn is_remote(self) -> bool {
        matches!(self, Self::Remote)
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ReadVisibilityCompat {
    Bool(bool),
    Level(ReadVisibility),
}

fn deserialize_read_visibility<'de, D>(deserializer: D) -> Result<ReadVisibility, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<ReadVisibilityCompat>::deserialize(deserializer)?;
    Ok(match value {
        None => ReadVisibility::default(),
        Some(ReadVisibilityCompat::Bool(false)) => ReadVisibility::Memory,
        Some(ReadVisibilityCompat::Bool(true)) => ReadVisibility::Remote,
        Some(ReadVisibilityCompat::Level(level)) => level,
    })
}

/// Configuration for log segmentation.
///
/// Segments partition the log into time-based chunks, enabling efficient
/// range queries and retention management. See RFC 0002 for details.
#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SegmentConfig {
    /// Interval for automatic segment sealing based on wall-clock time.
    ///
    /// When set, a new segment is created after the specified duration has
    /// elapsed since the current segment was created. This enables time-based
    /// partitioning for efficient queries and retention.
    ///
    /// When `None` (the default), automatic sealing is disabled and all
    /// entries are written to segment 0 indefinitely.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    /// use log::SegmentConfig;
    ///
    /// // Create a new segment every hour
    /// let config = SegmentConfig {
    ///     seal_interval: Some(Duration::from_secs(3600)),
    /// };
    /// ```
    #[serde_as(as = "Option<DurationMilliSeconds<u64>>")]
    #[serde(default)]
    pub seal_interval: Option<Duration>,
}

/// Options for scan operations.
///
/// Controls the behavior of [`LogRead::scan`](crate::LogRead::scan) and
/// [`LogRead::scan_with_options`](crate::LogRead::scan_with_options).
/// Additional options may be added in future versions.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    // Reserved for future options such as:
    // - read_level: control consistency vs performance tradeoff
    // - cache_policy: control block cache behavior
}

/// Options for count operations.
///
/// Controls the behavior of [`LogRead::count`](crate::LogRead::count) and
/// [`LogRead::count_with_options`](crate::LogRead::count_with_options).
#[derive(Debug, Clone, Default)]
pub struct CountOptions {
    /// Whether to return an approximate count.
    ///
    /// When `true`, the count may be computed from index metadata without
    /// reading individual entries, providing faster results at the cost
    /// of accuracy. Useful for progress indicators and lag estimation.
    ///
    /// When `false` (the default), an exact count is computed by scanning
    /// the relevant index entries.
    pub approximate: bool,
}

/// Configuration for opening a [`LogDbReader`](crate::LogDbReader).
///
/// This struct holds settings for read-only log access, including storage
/// backend configuration and automatic refresh settings.
///
/// # Example
///
/// ```no_run
/// use log::ReaderConfig;
/// use common::StorageConfig;
/// use std::time::Duration;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = ReaderConfig {
///     storage: StorageConfig::default(),
///     refresh_interval: Duration::from_secs(1),
/// };
/// let reader = log::LogDbReader::open(config).await?;
/// # Ok(())
/// # }
/// ```
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderConfig {
    /// Storage backend configuration.
    ///
    /// Determines where and how log data is read. See [`StorageConfig`]
    /// for available options including in-memory and SlateDB backends.
    pub storage: StorageConfig,

    /// Interval for discovering new log data.
    ///
    /// The reader periodically checks for new data written by other processes
    /// at this interval. This enables readers to see new log entries without
    /// manual refresh calls.
    ///
    /// Defaults to 1 second.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval: Duration,
}

fn default_refresh_interval() -> Duration {
    Duration::from_secs(1)
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            refresh_interval: default_refresh_interval(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct DurabilityOnly {
        #[serde(
            default,
            alias = "read_durable",
            deserialize_with = "deserialize_read_visibility"
        )]
        read_visibility: ReadVisibility,
    }

    #[test]
    fn should_default_read_visibility_to_memory_when_missing() {
        // given
        let json = "{}";

        // when
        let cfg: DurabilityOnly = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(cfg.read_visibility, ReadVisibility::Memory);
    }

    #[test]
    fn should_deserialize_legacy_bool_true_as_remote() {
        // given
        let json = r#"{"read_durable": true}"#;

        // when
        let cfg: DurabilityOnly = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(cfg.read_visibility, ReadVisibility::Remote);
    }

    #[test]
    fn should_deserialize_enum_remote() {
        // given
        let json = r#"{"read_visibility": "remote"}"#;

        // when
        let cfg: DurabilityOnly = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(cfg.read_visibility, ReadVisibility::Remote);
    }
}

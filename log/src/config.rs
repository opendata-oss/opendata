//! Configuration options for OpenData Log operations.
//!
//! This module defines the configuration and options structs that control
//! the behavior of the log, including storage setup and operation parameters.

use std::time::Duration;

use common::StorageConfig;
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};
use slatedb::SstBlockSize;

use crate::error::Error;

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

    /// Retention policy; see [`RetentionConfig`] and RFC 0005.
    #[serde(default)]
    pub retention: RetentionConfig,

    /// Compaction policy; see [`LogCompactionOptions`] and RFC 0005.
    #[serde(default)]
    pub compaction: LogCompactionOptions,

    /// SlateDB SST block size.
    ///
    /// A no-op for the in-memory backend. `None` (the default) leaves SlateDB's
    /// own default of 4 KiB ([`SstBlockSize::Block4Kib`]).
    ///
    /// This is applied at `DbBuilder` time;
    /// [`LogDbBuilder::with_sst_block_size`](crate::LogDbBuilder::with_sst_block_size)
    /// overrides it when set.
    #[serde(default)]
    pub sst_block_size: Option<SstBlockSize>,
}

impl Config {
    /// Validates the retention/segmentation relationship per RFC 0005.
    ///
    /// Returns `Err(Error::InvalidInput)` if `retention` is set without a
    /// `seal_interval`, or if `retention` is smaller than `seal_interval`.
    pub fn validate_retention(&self) -> crate::error::Result<()> {
        let Some(retention) = self.retention.retention else {
            return Ok(());
        };
        let Some(seal_interval) = self.segmentation.seal_interval else {
            return Err(Error::InvalidInput(
                "retention requires segmentation.seal_interval to be set.".into(),
            ));
        };
        if retention < seal_interval {
            return Err(Error::InvalidInput(format!(
                "retention ({retention:?}) must be at least as large as \
                 segmentation.seal_interval ({seal_interval:?}).",
            )));
        }
        Ok(())
    }

    /// Validates that compaction thresholds are internally consistent.
    ///
    /// Returns `Err(Error::InvalidInput)` if `min_l0_per_compaction` exceeds
    /// `max_l0_per_compaction` — a config that would silently disable L0
    /// compactions on active segments.
    pub fn validate_compaction(&self) -> crate::error::Result<()> {
        let min = self.compaction.min_l0_per_compaction;
        let max = self.compaction.max_l0_per_compaction;
        if min > max {
            return Err(Error::InvalidInput(format!(
                "compaction.min_l0_per_compaction ({min}) must be \
                 <= compaction.max_l0_per_compaction ({max}).",
            )));
        }
        Ok(())
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

/// Retention policy configuration.
///
/// Retention is enforced at segment granularity: a sealed segment becomes
/// eligible for reclamation when its end time (its successor's
/// `start_time_ms`) is older than `now() - retention`. See RFC 0005 for the
/// full protocol.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Segment retention duration.
    ///
    /// Segments whose end time is older than `now() - retention` are eligible
    /// for deletion. `None` disables retention (segments live forever).
    ///
    /// When set, [`SegmentConfig::seal_interval`] must also be set and must
    /// be `<= retention`; otherwise [`Config::validate_retention`] fails.
    #[serde_as(as = "Option<DurationMilliSeconds<u64>>")]
    #[serde(default)]
    pub retention: Option<Duration>,

    /// How often the writer's retention task re-evaluates segment expiry and
    /// deletes any `SegmentMeta` records past the retention bound.
    ///
    /// Smaller values reduce the lag between expiry and the segment becoming
    /// invisible to readers. Defaults to 60 seconds.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_retention_check_interval")]
    pub check_interval: Duration,
}

fn default_retention_check_interval() -> Duration {
    Duration::from_secs(60)
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            retention: None,
            check_interval: default_retention_check_interval(),
        }
    }
}

/// Compaction policy configuration.
///
/// Tunes the compaction scheduler that operates on LogDb's per-segment
/// SlateDB layout. See RFC 0005 for the policy this configures.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogCompactionOptions {
    /// Minimum number of L0 SSTs required to trigger an active-segment
    /// compaction.
    ///
    /// Should be a fraction of SlateDB's `l0_max_ssts` so the scheduler
    /// triggers before backpressure hits. Defaults to 4 (half of SlateDB's
    /// default `l0_max_ssts` of 8).
    #[serde(default = "default_min_l0_per_compaction")]
    pub min_l0_per_compaction: usize,

    /// Maximum number of L0 SSTs to roll into one fresh SR per active-segment
    /// compaction.
    ///
    /// Defaults to 8.
    #[serde(default = "default_max_l0_per_compaction")]
    pub max_l0_per_compaction: usize,

    /// When set, skip the one-shot final consolidation that merges a sealed
    /// segment's L0s and sorted runs into a single SR, saving one rewrite per
    /// record at the cost of leaving sealed segments as multiple SRs. L0
    /// relief still runs, so L0 counts stay bounded; the system segment
    /// (id 0) and orphan drains are unaffected.
    ///
    /// The log is append-only, so unlike general SlateDB use there are no
    /// updates or deletes to reconcile across L0s and SRs — the merge buys no
    /// correctness, only read-side key locality within each SST. When writes
    /// target a small subset of logs (low key cardinality), a single relief
    /// pass already yields good per-SST locality, so consolidation adds
    /// little and `l0_only` is a clear win.
    #[serde(default)]
    pub l0_only: bool,
}

fn default_min_l0_per_compaction() -> usize {
    4
}

fn default_max_l0_per_compaction() -> usize {
    8
}

impl Default for LogCompactionOptions {
    fn default() -> Self {
        Self {
            min_l0_per_compaction: default_min_l0_per_compaction(),
            max_l0_per_compaction: default_max_l0_per_compaction(),
            l0_only: false,
        }
    }
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

    fn config_with(retention: Option<Duration>, seal_interval: Option<Duration>) -> Config {
        Config {
            segmentation: SegmentConfig { seal_interval },
            retention: RetentionConfig {
                retention,
                ..RetentionConfig::default()
            },
            ..Config::default()
        }
    }

    #[test]
    fn should_accept_retention_disabled() {
        // given
        let cfg = config_with(None, None);

        // when / then
        cfg.validate_retention().unwrap();
    }

    #[test]
    fn should_accept_retention_disabled_with_seal_interval() {
        // given
        let cfg = config_with(None, Some(Duration::from_secs(60)));

        // when / then
        cfg.validate_retention().unwrap();
    }

    #[test]
    fn should_accept_retention_equal_to_seal_interval() {
        // given
        let cfg = config_with(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));

        // when / then
        cfg.validate_retention().unwrap();
    }

    #[test]
    fn should_reject_retention_without_seal_interval() {
        // given
        let cfg = config_with(Some(Duration::from_secs(60)), None);

        // when
        let err = cfg.validate_retention().unwrap_err();

        // then
        assert!(
            matches!(&err, Error::InvalidInput(msg) if msg.contains("seal_interval")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn should_reject_retention_smaller_than_seal_interval() {
        // given
        let cfg = config_with(Some(Duration::from_secs(30)), Some(Duration::from_secs(60)));

        // when
        let err = cfg.validate_retention().unwrap_err();

        // then
        assert!(
            matches!(&err, Error::InvalidInput(msg) if msg.contains("at least as large")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn should_reject_min_l0_above_max_l0() {
        // given
        let cfg = Config {
            compaction: LogCompactionOptions {
                min_l0_per_compaction: 10,
                max_l0_per_compaction: 5,
                ..LogCompactionOptions::default()
            },
            ..Config::default()
        };

        // when
        let err = cfg.validate_compaction().unwrap_err();

        // then
        assert!(
            matches!(&err, Error::InvalidInput(msg)
                if msg.contains("min_l0_per_compaction") && msg.contains("max_l0_per_compaction")),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn should_accept_min_l0_equal_to_max_l0() {
        let cfg = Config {
            compaction: LogCompactionOptions {
                min_l0_per_compaction: 4,
                max_l0_per_compaction: 4,
                ..LogCompactionOptions::default()
            },
            ..Config::default()
        };
        cfg.validate_compaction().unwrap();
    }

    #[test]
    fn should_use_documented_defaults_for_retention_config() {
        let cfg = RetentionConfig::default();
        assert_eq!(cfg.retention, None);
        assert_eq!(cfg.check_interval, Duration::from_secs(60));
    }

    #[test]
    fn should_use_documented_defaults_for_compaction_options() {
        let cfg = LogCompactionOptions::default();
        assert_eq!(cfg.min_l0_per_compaction, 4);
        assert_eq!(cfg.max_l0_per_compaction, 8);
    }

    #[test]
    fn should_deserialize_retention_config_with_defaults() {
        // given
        let json = "{}";

        // when
        let cfg: RetentionConfig = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(cfg.retention, None);
        assert_eq!(cfg.check_interval, Duration::from_secs(60));
    }

    #[test]
    fn should_deserialize_retention_config_with_durations_in_ms() {
        // given
        let json = r#"{"retention": 3600000, "check_interval": 5000}"#;

        // when
        let cfg: RetentionConfig = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(cfg.retention, Some(Duration::from_secs(3600)));
        assert_eq!(cfg.check_interval, Duration::from_secs(5));
    }

    #[test]
    fn should_default_sst_block_size_to_none() {
        // given
        let json = r#"{"storage": {"type": "InMemory"}}"#;

        // when
        let cfg: Config = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(cfg.sst_block_size, None);
    }

    #[test]
    fn should_deserialize_sst_block_size() {
        // given
        let json = r#"{"storage": {"type": "InMemory"}, "sst_block_size": "Block16Kib"}"#;

        // when
        let cfg: Config = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(cfg.sst_block_size, Some(SstBlockSize::Block16Kib));
    }

    #[test]
    fn should_deserialize_compaction_options_with_defaults() {
        // given
        let json = "{}";

        // when
        let cfg: LogCompactionOptions = serde_json::from_str(json).unwrap();

        // then
        assert_eq!(cfg.min_l0_per_compaction, 4);
        assert_eq!(cfg.max_l0_per_compaction, 8);
    }
}

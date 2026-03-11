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
///     block_cache: None,
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

    /// Block cache configuration.
    ///
    /// When set, enables a hybrid (in-memory + on-disk) block cache for SST
    /// block lookups. This is used by both the writer and reader paths.
    pub block_cache: Option<BlockCacheConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            flush_interval: Duration::from_secs(60),
            retention: None,
            block_cache: None,
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

    /// Block cache configuration.
    ///
    /// When set, enables a hybrid (in-memory + on-disk) block cache for SST
    /// block lookups. This dramatically reduces S3 reads by caching hot blocks
    /// in RAM and warm blocks on local disk (ideally NVMe).
    #[serde(default)]
    pub block_cache: Option<BlockCacheConfig>,
}

/// Configuration for the hybrid block cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockCacheConfig {
    /// In-memory cache capacity in bytes.
    ///
    /// Hot SST blocks are served directly from memory. Set this to a
    /// significant fraction of available RAM (e.g., 8-12 GiB on a 32 GiB node).
    pub memory_capacity: u64,

    /// On-disk cache capacity in bytes.
    ///
    /// Evicted blocks spill to local disk. Best performance with NVMe SSDs.
    /// Set to the usable capacity of the local disk (e.g., 140 GiB on a
    /// 150 GiB NVMe drive).
    pub disk_capacity: u64,

    /// Path for the on-disk cache directory.
    ///
    /// Should point to a fast local disk, ideally an NVMe instance store
    /// volume (e.g., `/mnt/nvme/block-cache`).
    pub disk_path: String,
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
            block_cache: None,
        }
    }
}

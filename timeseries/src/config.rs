//! Configuration options for OpenData TimeSeries operations.
//!
//! This module defines the configuration and options structs that control
//! the behavior of the time series database, including storage setup and
//! write operation parameters.

use std::time::Duration;

use common::StorageConfig;

/// Configuration for opening a [`TimeSeries`](crate::TimeSeries) database.
///
/// This struct holds all the settings needed to initialize a time series
/// instance, including storage backend configuration and operational parameters.
///
/// # Example
///
/// ```ignore
/// use timeseries::Config;
/// use common::StorageConfig;
/// use std::time::Duration;
///
/// let config = Config {
///     storage: StorageConfig::default(),
///     flush_interval: Duration::from_secs(30),
///     retention: Some(Duration::from_secs(86400 * 7)), // 7 days
/// };
/// let ts = TimeSeries::open(config).await?;
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

/// Options for write operations.
///
/// Controls the durability and behavior of [`TimeSeries::write`](crate::TimeSeries::write)
/// and [`TimeSeries::write_with_options`](crate::TimeSeries::write_with_options).
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Whether to wait for data to be flushed to durable storage before returning.
    ///
    /// When `true`, the write operation will not return until the data has
    /// been persisted to durable storage.
    ///
    /// When `false` (the default), the operation returns as soon as the data
    /// is accepted for ingestion, providing lower latency but with data
    /// potentially only in memory.
    pub await_durable: bool,
}

//! Configuration options for OpenData Log operations.
//!
//! This module defines the configuration and options structs that control
//! the behavior of the log, including storage setup and operation parameters.

use std::time::Duration;

use common::StorageConfig;

/// Configuration for opening a [`Log`](crate::Log).
///
/// This struct holds all the settings needed to initialize a log instance,
/// including storage backend configuration.
///
/// # Example
///
/// ```ignore
/// use log::Config;
/// use common::StorageConfig;
///
/// let config = Config {
///     storage: StorageConfig::default(),
///     segmentation: SegmentConfig::default(),
/// };
/// let log = Log::open(config).await?;
/// ```
#[derive(Debug, Clone, Default)]
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
    pub segmentation: SegmentConfig,
}

/// Configuration for log segmentation.
///
/// Segments partition the log into time-based chunks, enabling efficient
/// range queries and retention management. See RFC 0002 for details.
#[derive(Debug, Clone, Default)]
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
    /// ```ignore
    /// use std::time::Duration;
    /// use log::SegmentConfig;
    ///
    /// // Create a new segment every hour
    /// let config = SegmentConfig {
    ///     seal_interval: Some(Duration::from_secs(3600)),
    /// };
    /// ```
    pub seal_interval: Option<Duration>,
}

/// Options for write operations.
///
/// Controls the durability and behavior of [`Log::append`](crate::Log::append)
/// and [`Log::append_with_options`](crate::Log::append_with_options).
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Whether to wait for the write to be durable before returning.
    ///
    /// When `true`, the append operation will not return until the data has
    /// been persisted to durable storage (e.g., flushed to the WAL and
    /// acknowledged by the object store).
    ///
    /// When `false` (the default), the operation returns as soon as the data
    /// is in memory, providing lower latency but risking data loss on crash.
    pub await_durable: bool,
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

/// Options for list operations.
///
/// Controls the behavior of [`LogRead::list`](crate::LogRead::list) and
/// [`LogRead::list_with_options`](crate::LogRead::list_with_options).
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    // Reserved for future options such as:
    // - prefix: filter keys by prefix
}

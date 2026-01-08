//! Configuration options for OpenData Log operations.
//!
//! This module defines the configuration and options structs that control
//! the behavior of the log, including storage setup and operation parameters.

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
/// Controls the behavior of [`Log::scan`](crate::Log::scan) and
/// [`Log::scan_with_options`](crate::Log::scan_with_options).
/// Additional options may be added in future versions.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    // Reserved for future options such as:
    // - read_level: control consistency vs performance tradeoff
    // - cache_policy: control block cache behavior
}

/// Options for count operations.
///
/// Controls the behavior of [`Log::count`](crate::Log::count) and
/// [`Log::count_with_options`](crate::Log::count_with_options).
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

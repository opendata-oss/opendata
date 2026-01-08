//! Read-only log access and the [`LogRead`] trait.
//!
//! This module provides:
//! - [`LogRead`]: The trait defining read operations on the log.
//! - [`LogReader`]: A read-only view of the log that implements `LogRead`.

use std::ops::RangeBounds;

use async_trait::async_trait;
use bytes::Bytes;

use std::sync::Arc;

use common::StorageRead;
use common::storage::factory::create_storage;

use crate::config::{Config, CountOptions, ScanOptions};
use crate::error::{Error, Result};
use crate::log::LogIterator;
use crate::serde::LogEntryKey;

/// Trait for read operations on the log.
///
/// This trait defines the common read interface shared by [`Log`](crate::Log)
/// and [`LogReader`]. It provides methods for scanning entries and counting
/// records within a key's log.
///
/// # Implementors
///
/// - [`Log`](crate::Log): The main log interface with both read and write access.
/// - [`LogReader`]: A read-only view of the log.
///
/// # Example
///
/// ```ignore
/// use log::LogRead;
/// use bytes::Bytes;
///
/// async fn process_log(reader: &impl LogRead) -> Result<()> {
///     // Works with both Log and LogReader
///     let mut iter = reader.scan(Bytes::from("orders"), ..);
///     while let Some(entry) = iter.next().await? {
///         println!("seq={}: {:?}", entry.sequence, entry.value);
///     }
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait LogRead {
    /// Scans entries for a key within a sequence number range.
    ///
    /// Returns an iterator that yields entries in sequence number order.
    /// The range is specified using Rust's standard range syntax.
    ///
    /// This method uses default scan options. Use [`scan_with_options`] for
    /// custom read behavior.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to scan.
    /// * `seq_range` - The sequence number range to scan. Supports all Rust
    ///   range types (`..`, `start..`, `..end`, `start..end`, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails due to storage issues.
    ///
    /// [`scan_with_options`]: LogRead::scan_with_options
    async fn scan(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
    ) -> Result<LogIterator> {
        self.scan_with_options(key, seq_range, ScanOptions::default())
            .await
    }

    /// Scans entries for a key within a sequence number range with custom options.
    ///
    /// Returns an iterator that yields entries in sequence number order.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to scan.
    /// * `seq_range` - The sequence number range to scan.
    /// * `options` - Scan options controlling read behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the scan fails due to storage issues.
    async fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        options: ScanOptions,
    ) -> Result<LogIterator>;

    /// Counts entries for a key within a sequence number range.
    ///
    /// Returns the number of entries in the specified range. This is useful
    /// for computing lag (how far behind a consumer is) or progress metrics.
    ///
    /// This method uses default count options (exact count). Use
    /// [`count_with_options`] for approximate counts.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to count.
    /// * `seq_range` - The sequence number range to count.
    ///
    /// # Errors
    ///
    /// Returns an error if the count fails due to storage issues.
    ///
    /// [`count_with_options`]: LogRead::count_with_options
    async fn count(&self, key: Bytes, seq_range: impl RangeBounds<u64> + Send) -> Result<u64> {
        self.count_with_options(key, seq_range, CountOptions::default())
            .await
    }

    /// Counts entries for a key within a sequence number range with custom options.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the log stream to count.
    /// * `seq_range` - The sequence number range to count.
    /// * `options` - Count options, including whether to return an approximate count.
    ///
    /// # Errors
    ///
    /// Returns an error if the count fails due to storage issues.
    async fn count_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        options: CountOptions,
    ) -> Result<u64>;
}

/// A read-only view of the log.
///
/// `LogReader` provides access to all read operations via the [`LogRead`]
/// trait, but not write operations. This is useful for:
///
/// - Consumers that should not have write access
/// - Sharing read access across multiple components
/// - Separating read and write concerns in your application
///
/// # Obtaining a LogReader
///
/// A `LogReader` is created by calling [`LogReader::open`]:
///
/// ```ignore
/// let reader = LogReader::open(config).await?;
/// ```
///
/// # Thread Safety
///
/// `LogReader` is designed to be cloned and shared across threads.
/// All methods take `&self` and are safe to call concurrently.
///
/// # Example
///
/// ```ignore
/// use log::{LogReader, LogRead};
/// use bytes::Bytes;
///
/// async fn consume_events(reader: LogReader, key: Bytes) -> Result<()> {
///     let mut checkpoint: u64 = 0;
///
///     loop {
///         let mut iter = reader.scan(key.clone(), checkpoint..);
///         while let Some(entry) = iter.next().await? {
///             process_entry(&entry);
///             checkpoint = entry.sequence + 1;
///         }
///
///         // Check how far behind we are
///         let lag = reader.count(key.clone(), checkpoint..).await?;
///         if lag == 0 {
///             // Caught up, wait for new entries
///             tokio::time::sleep(Duration::from_millis(100)).await;
///         }
///     }
/// }
/// ```
#[derive(Clone)]
pub struct LogReader {
    storage: Arc<dyn StorageRead>,
}

impl LogReader {
    /// Opens a read-only view of the log with the given configuration.
    ///
    /// This creates a `LogReader` that can scan and count entries but cannot
    /// append new records. Use this when you only need read access to the log.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration specifying storage backend and settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the storage backend cannot be initialized.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use log::{LogReader, LogRead, Config};
    /// use bytes::Bytes;
    ///
    /// let reader = LogReader::open(config).await?;
    /// let mut iter = reader.scan(Bytes::from("orders"), ..).await?;
    /// while let Some(entry) = iter.next().await? {
    ///     println!("seq={}: {:?}", entry.sequence, entry.value);
    /// }
    /// ```
    pub async fn open(config: Config) -> Result<Self> {
        let storage = create_storage(&config.storage, None)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { storage })
    }

    /// Creates a LogReader from an existing storage implementation.
    #[cfg(test)]
    pub(crate) fn new(storage: Arc<dyn StorageRead>) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl LogRead for LogReader {
    async fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        _options: ScanOptions,
    ) -> Result<LogIterator> {
        let range = LogEntryKey::scan_range(&key, seq_range);
        LogIterator::open(Arc::clone(&self.storage), range).await
    }

    async fn count_with_options(
        &self,
        _key: Bytes,
        _seq_range: impl RangeBounds<u64> + Send,
        _options: CountOptions,
    ) -> Result<u64> {
        todo!()
    }
}

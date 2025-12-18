//! Read-only log access.
//!
//! This module provides [`LogReader`], a read-only view of the log that
//! exposes scan and count operations without write access.

use std::ops::RangeBounds;

use bytes::Bytes;

use crate::config::{CountOptions, ScanOptions};
use crate::error::Result;
use crate::log::ScanIterator;

/// A read-only view of the log.
///
/// `LogReader` provides access to all read operations ([`scan`], [`count`])
/// but not write operations. This is useful for:
///
/// - Consumers that should not have write access
/// - Sharing read access across multiple components
/// - Separating read and write concerns in your application
///
/// # Obtaining a LogReader
///
/// A `LogReader` is obtained by calling [`Log::reader`](crate::Log::reader):
///
/// ```ignore
/// let log = Log::open(path, options).await?;
/// let reader = log.reader();
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
/// use open_log::LogReader;
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
    // Implementation details will be added later
    _private: (),
}

impl LogReader {
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
    /// # Example
    ///
    /// ```ignore
    /// // Scan all entries for a key
    /// let iter = reader.scan(Bytes::from("orders"), ..);
    ///
    /// // Scan from sequence 100 onwards (common for consumers)
    /// let iter = reader.scan(Bytes::from("orders"), checkpoint..);
    ///
    /// // Scan a specific range
    /// let iter = reader.scan(Bytes::from("orders"), 100..200);
    /// ```
    ///
    /// [`scan_with_options`]: LogReader::scan_with_options
    pub fn scan(&self, key: Bytes, seq_range: impl RangeBounds<u64>) -> ScanIterator {
        self.scan_with_options(key, seq_range, ScanOptions::default())
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
    /// # Example
    ///
    /// ```ignore
    /// let options = ScanOptions::default();
    /// let iter = reader.scan_with_options(Bytes::from("orders"), 100.., options);
    /// ```
    pub fn scan_with_options(
        &self,
        _key: Bytes,
        _seq_range: impl RangeBounds<u64>,
        _options: ScanOptions,
    ) -> ScanIterator {
        todo!()
    }

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
    /// # Example
    ///
    /// ```ignore
    /// // Count all entries
    /// let total = reader.count(Bytes::from("orders"), ..).await?;
    ///
    /// // Compute lag from a checkpoint
    /// let checkpoint: u64 = 1000;
    /// let lag = reader.count(Bytes::from("orders"), checkpoint..).await?;
    /// println!("Consumer is {} entries behind", lag);
    /// ```
    ///
    /// [`count_with_options`]: LogReader::count_with_options
    pub async fn count(&self, key: Bytes, seq_range: impl RangeBounds<u64>) -> Result<u64> {
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
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Get an approximate count for faster results
    /// let options = CountOptions { approximate: true };
    /// let approx_lag = reader.count_with_options(
    ///     Bytes::from("orders"),
    ///     checkpoint..,
    ///     options
    /// ).await?;
    /// ```
    pub async fn count_with_options(
        &self,
        _key: Bytes,
        _seq_range: impl RangeBounds<u64>,
        _options: CountOptions,
    ) -> Result<u64> {
        todo!()
    }
}

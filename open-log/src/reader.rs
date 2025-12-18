//! Read-only log access and the [`LogRead`] trait.
//!
//! This module provides:
//! - [`LogRead`]: The trait defining read operations on the log.
//! - [`LogReader`]: A read-only view of the log that implements `LogRead`.

use std::future::Future;
use std::ops::RangeBounds;

use bytes::Bytes;

use crate::config::{CountOptions, ScanOptions};
use crate::error::Result;
use crate::log::LogIterator;

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
/// use open_log::LogRead;
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
    /// [`scan_with_options`]: LogRead::scan_with_options
    fn scan(&self, key: Bytes, seq_range: impl RangeBounds<u64> + Send) -> LogIterator {
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
    fn scan_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        options: ScanOptions,
    ) -> LogIterator;

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
    fn count(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
    ) -> impl Future<Output = Result<u64>> + Send {
        self.count_with_options(key, seq_range, CountOptions::default())
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
    fn count_with_options(
        &self,
        key: Bytes,
        seq_range: impl RangeBounds<u64> + Send,
        options: CountOptions,
    ) -> impl Future<Output = Result<u64>> + Send;
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
/// use open_log::{LogReader, LogRead};
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

impl LogRead for LogReader {
    fn scan_with_options(
        &self,
        _key: Bytes,
        _seq_range: impl RangeBounds<u64> + Send,
        _options: ScanOptions,
    ) -> LogIterator {
        todo!()
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

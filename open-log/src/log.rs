//! Core Log implementation with read and write APIs.
//!
//! This module provides the [`Log`] struct, the primary entry point for
//! interacting with OpenData Log. It exposes both write operations ([`append`])
//! and read operations ([`scan`], [`count`]).

use std::ops::RangeBounds;

use bytes::Bytes;

use crate::config::{CountOptions, ScanOptions, WriteOptions};
use crate::error::Result;
use crate::model::{LogEntry, Record};
use crate::reader::LogReader;

/// An iterator over log entries for a specific key.
///
/// Created by [`Log::scan`] or [`Log::scan_with_options`]. Yields entries
/// in sequence number order within the specified range.
///
/// # Streaming Behavior
///
/// The iterator fetches entries lazily as they are consumed. Large scans
/// do not load all entries into memory at once.
///
/// # Example
///
/// ```ignore
/// let mut iter = log.scan(Bytes::from("orders"), 100..);
/// while let Some(entry) = iter.next().await? {
///     process_entry(entry);
/// }
/// ```
pub struct ScanIterator {
    // Implementation details will be added later
    _private: (),
}

impl ScanIterator {
    /// Advances the iterator and returns the next log entry.
    ///
    /// Returns `Ok(Some(entry))` if there is another entry in the range,
    /// `Ok(None)` if the iteration is complete, or `Err` if an error occurred.
    ///
    /// # Errors
    ///
    /// Returns an error if there is a storage failure while reading entries.
    pub async fn next(&mut self) -> Result<Option<LogEntry>> {
        todo!()
    }
}

/// The main log interface providing read and write operations.
///
/// `Log` is the primary entry point for interacting with OpenData Log.
/// It provides methods to append records, scan entries, and count records
/// within a key's log.
///
/// # Thread Safety
///
/// `Log` is designed to be shared across threads. All methods take `&self`
/// and internal synchronization is handled automatically.
///
/// # Writer Semantics
///
/// Currently, each log supports a single writer. Multi-writer support may
/// be added in the future, but would require each key to have a single
/// writer to maintain monotonic ordering within that key's log.
///
/// # Example
///
/// ```ignore
/// use open_log::{Log, Record, WriteOptions};
/// use bytes::Bytes;
///
/// // Open a log (implementation details TBD)
/// let log = Log::open(path, options).await?;
///
/// // Append records
/// let records = vec![
///     Record { key: Bytes::from("user:123"), value: Bytes::from("event-a") },
///     Record { key: Bytes::from("user:456"), value: Bytes::from("event-b") },
/// ];
/// log.append(records).await?;
///
/// // Scan entries for a specific key
/// let mut iter = log.scan(Bytes::from("user:123"), ..);
/// while let Some(entry) = iter.next().await? {
///     println!("seq={}: {:?}", entry.sequence, entry.value);
/// }
///
/// // Get a read-only view
/// let reader = log.reader();
/// ```
pub struct Log {
    // Implementation details will be added later
    _private: (),
}

impl Log {
    // ==================== Write API ====================

    /// Appends records to the log.
    ///
    /// Records are assigned sequence numbers in the order they appear in the
    /// input vector. All records in a single append call are written atomically.
    ///
    /// This method uses default write options. Use [`append_with_options`] for
    /// custom durability settings.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to append. Each record specifies its target
    ///   key and value.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails due to storage issues.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let records = vec![
    ///     Record { key: Bytes::from("events"), value: Bytes::from("event-1") },
    ///     Record { key: Bytes::from("events"), value: Bytes::from("event-2") },
    /// ];
    /// log.append(records).await?;
    /// ```
    ///
    /// [`append_with_options`]: Log::append_with_options
    pub async fn append(&self, records: Vec<Record>) -> Result<()> {
        self.append_with_options(records, WriteOptions::default())
            .await
    }

    /// Appends records to the log with custom options.
    ///
    /// Records are assigned sequence numbers in the order they appear in the
    /// input vector. All records in a single append call are written atomically.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to append. Each record specifies its target
    ///   key and value.
    /// * `options` - Write options controlling durability behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails due to storage issues.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let records = vec![
    ///     Record { key: Bytes::from("events"), value: Bytes::from("critical-event") },
    /// ];
    /// let options = WriteOptions { await_durable: true };
    /// log.append_with_options(records, options).await?;
    /// ```
    pub async fn append_with_options(
        &self,
        _records: Vec<Record>,
        _options: WriteOptions,
    ) -> Result<()> {
        todo!()
    }

    // ==================== Read API ====================

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
    /// let iter = log.scan(Bytes::from("orders"), ..);
    ///
    /// // Scan from sequence 100 onwards
    /// let iter = log.scan(Bytes::from("orders"), 100..);
    ///
    /// // Scan a specific range
    /// let iter = log.scan(Bytes::from("orders"), 100..200);
    /// ```
    ///
    /// [`scan_with_options`]: Log::scan_with_options
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
    /// let iter = log.scan_with_options(Bytes::from("orders"), 100.., options);
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
    /// let total = log.count(Bytes::from("orders"), ..).await?;
    ///
    /// // Compute lag from a checkpoint
    /// let checkpoint: u64 = 1000;
    /// let lag = log.count(Bytes::from("orders"), checkpoint..).await?;
    /// println!("Consumer is {} entries behind", lag);
    /// ```
    ///
    /// [`count_with_options`]: Log::count_with_options
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
    /// let approx_count = log.count_with_options(
    ///     Bytes::from("orders"),
    ///     ..,
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

    // ==================== Reader API ====================

    /// Creates a read-only view of the log.
    ///
    /// The returned [`LogReader`] provides access to all read operations
    /// ([`scan`], [`count`]) but not write operations. This is useful for
    /// consumers that should not have write access to the log.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let reader = log.reader();
    ///
    /// // Reader can scan and count
    /// let iter = reader.scan(Bytes::from("orders"), ..);
    /// let count = reader.count(Bytes::from("orders"), ..).await?;
    ///
    /// // But cannot append (this would be a compile error):
    /// // reader.append(records).await?; // ERROR: no method `append`
    /// ```
    ///
    /// [`scan`]: LogReader::scan
    /// [`count`]: LogReader::count
    pub fn reader(&self) -> LogReader {
        todo!()
    }
}

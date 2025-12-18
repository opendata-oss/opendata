//! Core Log implementation with read and write APIs.
//!
//! This module provides the [`Log`] struct, the primary entry point for
//! interacting with OpenData Log. It exposes both write operations ([`append`])
//! and read operations ([`scan`], [`count`]) via the [`LogRead`] trait.

use std::ops::RangeBounds;

use bytes::Bytes;

use crate::config::{CountOptions, ScanOptions, WriteOptions};
use crate::error::Result;
use crate::model::{LogEntry, Record};
use crate::reader::{LogRead, LogReader};

/// An iterator over log entries for a specific key.
///
/// Created by [`LogRead::scan`] or [`LogRead::scan_with_options`]. Yields
/// entries in sequence number order within the specified range.
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
pub struct LogIterator {
    // Implementation details will be added later
    _private: (),
}

impl LogIterator {
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
/// # Read Operations
///
/// Read operations are provided via the [`LogRead`] trait, which `Log`
/// implements. This allows generic code to work with either `Log` or
/// [`LogReader`].
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
/// use open_log::{Log, LogRead, Record, WriteOptions};
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

    /// Creates a read-only view of the log.
    ///
    /// The returned [`LogReader`] provides access to all read operations
    /// ([`scan`](LogRead::scan), [`count`](LogRead::count)) but not write
    /// operations. This is useful for consumers that should not have write
    /// access to the log.
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
    pub fn reader(&self) -> LogReader {
        todo!()
    }
}

impl LogRead for Log {
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

//! Core data types for OpenData Log.
//!
//! This module defines the fundamental data structures used throughout the
//! log API, including records for writing and entries for reading.

use bytes::Bytes;

/// A record to be appended to the log.
///
/// Records are the unit of data written to the log. Each record consists of
/// a key identifying the log stream and a value containing the payload.
///
/// # Key Selection
///
/// Keys determine how data is organized in the log. Each unique key represents
/// an independent log stream with its own sequence of entries. Choose keys based
/// on your access patterns:
///
/// - Use a single key for a simple append-only log
/// - Use entity IDs as keys for per-entity event streams
/// - Use composite keys (e.g., `tenant:entity`) for multi-tenant scenarios
///
/// # Example
///
/// ```
/// use bytes::Bytes;
/// use log::Record;
///
/// let record = Record {
///     key: Bytes::from("orders"),
///     value: Bytes::from(r#"{"id": "123", "amount": 99.99}"#),
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The key identifying the log stream.
    ///
    /// All records with the same key form a single ordered log. Keys can be
    /// any byte sequence but are typically human-readable identifiers.
    pub key: Bytes,

    /// The record payload.
    ///
    /// Values can contain any byte sequence. The log does not interpret
    /// or validate the contents.
    pub value: Bytes,
}

/// An entry read from the log.
///
/// Log entries are returned by [`LogIterator`](crate::LogIterator) and contain
/// the original record data along with metadata assigned at append time.
///
/// # Sequence Numbers
///
/// Each entry has a globally unique sequence number assigned when it was
/// appended. Within a single key's log, entries are ordered by sequence
/// number, but the numbers are not contiguous—other keys' appends are
/// interleaved in the global sequence.
///
/// # Example
///
/// ```ignore
/// let mut iter = log.scan(key, ..);
/// while let Some(entry) = iter.next().await? {
///     println!(
///         "key={:?}, seq={}, value={:?}",
///         entry.key, entry.sequence, entry.value
///     );
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The key of the log stream this entry belongs to.
    pub key: Bytes,

    /// The sequence number assigned to this entry.
    ///
    /// Sequence numbers are monotonically increasing within a key's log
    /// and globally unique across all keys.
    pub sequence: u64,

    /// The record value.
    pub value: Bytes,
}

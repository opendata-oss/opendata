//! Core data types for OpenData Log.
//!
//! This module defines the fundamental data structures used throughout the
//! log API, including records for writing and entries for reading.

use bytes::Bytes;

/// Unique identifier for a segment.
///
/// Segment IDs are monotonically increasing integers assigned when segments
/// are created. Use with [`LogRead::list_keys`](crate::LogRead::list_keys)
/// to query keys within specific segments.
pub type SegmentId = u32;

/// Global sequence number for log entries.
///
/// Sequence numbers are monotonically increasing integers assigned to each
/// entry at append time. They provide a total ordering across all keys in
/// the log.
pub type Sequence = u64;

/// A segment of the log.
///
/// Segments partition the log into coarse-grained chunks based on time or
/// other policies. Each segment has a unique identifier and tracks the
/// starting sequence number for entries it contains.
///
/// Segments are the natural boundary for attaching metadata such as key
/// listings. See [`LogRead::list_segments`](crate::LogRead::list_segments)
/// for querying segments.
///
/// # Example
///
/// ```no_run
/// # use log::{LogDb, LogRead, Config};
/// # use common::StorageConfig;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
/// # let log = LogDb::open(config).await?;
/// let segments = log.list_segments(..).await?;
/// for segment in segments {
///     println!(
///         "segment {}: start_seq={}, created at {}",
///         segment.id, segment.start_seq, segment.start_time_ms
///     );
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    /// Unique segment identifier (monotonically increasing).
    pub id: SegmentId,
    /// First sequence number in this segment.
    pub start_seq: Sequence,
    /// Wall-clock time when this segment was created (ms since epoch).
    pub start_time_ms: i64,
}

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

/// Output of an append operation.
///
/// Contains metadata about the appended records, including the starting
/// sequence number assigned to the first record in the batch.
///
/// # Example
///
/// ```no_run
/// # use log::{LogDb, Config, Record};
/// # use bytes::Bytes;
/// # use common::StorageConfig;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
/// # let log = LogDb::open(config).await?;
/// # let records = vec![Record { key: Bytes::from("k"), value: Bytes::from("v") }];
/// let result = log.try_append(records).await?;
/// println!("Appended starting at sequence {}", result.start_sequence);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOutput {
    /// Sequence number assigned to the first record in the batch.
    pub start_sequence: Sequence,
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
/// ```no_run
/// # use log::{LogDb, LogRead, Config};
/// # use bytes::Bytes;
/// # use common::StorageConfig;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let config = Config { storage: StorageConfig::InMemory, ..Default::default() };
/// # let log = LogDb::open(config).await?;
/// # let key = Bytes::from("orders");
/// let mut iter = log.scan(key, ..).await?;
/// while let Some(entry) = iter.next().await? {
///     println!(
///         "key={:?}, seq={}, value={:?}",
///         entry.key, entry.sequence, entry.value
///     );
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The key of the log stream this entry belongs to.
    pub key: Bytes,

    /// The sequence number assigned to this entry.
    ///
    /// Sequence numbers are monotonically increasing within a key's log
    /// and globally unique across all keys.
    pub sequence: Sequence,

    /// The record value.
    pub value: Bytes,
}

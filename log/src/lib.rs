//! OpenData Log - A key-oriented log system built on SlateDB.
//!
//! OpenData Log provides a simple log abstraction where each key represents an
//! independent log stream. Unlike traditional messaging systems with partitions,
//! users write directly to keys and can create new keys as access patterns evolve.
//!
//! # Architecture
//!
//! The log is built on SlateDB's LSM tree. Writes append entries to the WAL and
//! memtable, then flush to sorted string tables (SSTs). LSM compaction naturally
//! organizes data for log locality, grouping entries by key prefix over time.
//!
//! # Key Concepts
//!
//! - **LogDb**: The main entry point providing both read and write operations.
//! - **LogDbReader**: A read-only view of the log, useful for consumers that should
//!   not have write access.
//! - **Sequence Numbers**: Each entry is assigned a global sequence number at
//!   append time. Sequence numbers are monotonically increasing within a key's
//!   log but not contiguous (other keys' appends are interleaved).
//!
//! # Example
//!
//! ```ignore
//! use log::{LogDb, Config, Record};
//! use bytes::Bytes;
//!
//! // Open a log
//! let log = LogDb::open(Config::default()).await?;
//!
//! // Append records
//! let records = vec![
//!     Record { key: Bytes::from("orders"), value: Bytes::from("order-123") },
//! ];
//! log.append(records).await?;
//!
//! // Scan a key's log
//! let mut iter = log.scan(Bytes::from("orders"), ..);
//! while let Some(entry) = iter.next().await? {
//!     println!("seq={}, value={:?}", entry.sequence, entry.value);
//! }
//! ```

mod config;
mod error;
mod listing;
mod log;
mod model;
mod range;
mod reader;
mod segment;
mod sequence;
mod serde;
#[cfg(feature = "http-server")]
pub mod server;
mod storage;

pub use config::{Config, CountOptions, ScanOptions, SegmentConfig, WriteOptions};
pub use error::{Error, Result};
pub use listing::{LogKey, LogKeyIterator};
pub use log::LogDb;
pub use model::{AppendResult, LogEntry, Record, Segment, SegmentId, Sequence};
pub use reader::{LogDbReader, LogIterator, LogRead};

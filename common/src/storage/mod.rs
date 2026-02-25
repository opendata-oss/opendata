pub mod config;
pub mod factory;
pub mod in_memory;
pub mod loader;
pub mod slate;
pub mod util;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::BytesRange;

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub enum Ttl {
    #[default]
    Default,
    NoExpiry,
    ExpireAfter(u64),
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct PutOptions {
    pub ttl: Ttl,
}

/// Encapsulates a record being put along with options specific to the put.
#[derive(Clone, Debug)]
pub struct PutRecordOp {
    pub record: Record,
    pub options: PutOptions,
}

impl PutRecordOp {
    pub fn new(record: Record) -> Self {
        Self {
            record,
            options: PutOptions::default(),
        }
    }

    pub fn new_with_options(record: Record, options: PutOptions) -> Self {
        Self { record, options }
    }

    pub fn with_options(self, options: PutOptions) -> Self {
        Self {
            record: self.record,
            options,
        }
    }
}

/// Converts a Record to a PutRecordOp with default options
impl From<Record> for PutRecordOp {
    fn from(record: Record) -> Self {
        Self::new(record)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeOptions {
    pub ttl: Ttl,
}

/// Encapsulates a record written as part of a merge op along with options specific to the merge.
#[derive(Clone, Debug)]
pub struct MergeRecordOp {
    pub record: Record,
    pub options: MergeOptions,
}

impl MergeRecordOp {
    pub fn new(record: Record) -> Self {
        Self {
            record,
            options: MergeOptions::default(),
        }
    }

    pub fn new_with_ttl(record: Record, options: MergeOptions) -> Self {
        Self { record, options }
    }
}

/// Converts a Record to a PutRecordOp with default options
impl From<Record> for MergeRecordOp {
    fn from(record: Record) -> Self {
        Self::new(record)
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    pub key: Bytes,
    pub value: Bytes,
}

impl Record {
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self { key, value }
    }

    pub fn empty(key: Bytes) -> Self {
        Self::new(key, Bytes::new())
    }
}

#[derive(Clone, Debug)]
pub enum RecordOp {
    Put(PutRecordOp),
    Merge(MergeRecordOp),
    Delete(Bytes),
}

/// Options for write operations.
///
/// Controls the durability behavior of write operations like [`Storage::put`]
/// and [`Storage::put_with_options`].
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Whether to wait for the write to be durable before returning.
    ///
    /// When `true`, the operation will not return until the data has been
    /// persisted to durable storage (e.g., flushed to the WAL and acknowledged
    /// by the object store).
    ///
    /// When `false` (the default), the operation returns as soon as the data
    /// is in memory, providing lower latency but risking data loss on crash.
    pub await_durable: bool,
}

/// Error type for storage operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// Storage-related errors
    Storage(String),
    /// Internal errors
    Internal(String),
}

impl std::error::Error for StorageError {}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            StorageError::Storage(msg) => write!(f, "Storage error: {}", msg),
            StorageError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl StorageError {
    /// Converts a storage error to StorageError::Storage.
    pub fn from_storage(e: impl std::fmt::Display) -> Self {
        StorageError::Storage(e.to_string())
    }
}

/// Result type alias for storage operations
pub type StorageResult<T> = std::result::Result<T, StorageError>;

/// Trait for merging existing values with new values.
///
/// Merge operators must be associative: `merge(merge(a, b), c) == merge(a, merge(b, c))`.
/// This ensures consistent merging behavior regardless of the order of operations.
pub trait MergeOperator: Send + Sync {
    /// Merges an existing value with a new value to produce a merged result.
    ///
    /// # Arguments
    /// * `key` - The key associated with the values being merged
    /// * `existing_value` - The current value stored in the database (if any)
    /// * `new_value` - The new value to merge with the existing value
    ///
    /// # Returns
    /// The merged value.
    fn merge(&self, key: &Bytes, existing_value: Option<Bytes>, new_value: Bytes) -> Bytes;
}

/// Iterator over storage records.
#[async_trait]
pub trait StorageIterator {
    async fn next(&mut self) -> StorageResult<Option<Record>>;
}

/// Common read operations supported by both Storage and StorageSnapshot.
///
/// This trait provides the core read methods that are shared between full storage
/// access and point-in-time snapshots. By extracting these common operations,
/// we can write code that works with both storage types.
#[async_trait]
pub trait StorageRead: Send + Sync {
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>>;

    /// Returns an iterator over records in the given range.
    ///
    /// The returned iterator is owned and does not borrow from the storage,
    /// allowing it to be stored in structs or passed across await points.
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>>;

    /// Collects all records in the range into a Vec.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan(&self, range: BytesRange) -> StorageResult<Vec<Record>> {
        let mut iter = self.scan_iter(range).await?;
        let mut records = Vec::new();
        while let Some(record) = iter.next().await? {
            records.push(record);
        }
        Ok(records)
    }
}

/// A point-in-time snapshot of the storage layer.
///
/// Snapshots provide a consistent read-only view of the database at the time
/// the snapshot was created. Reads from a snapshot will not see any subsequent
/// writes to the underlying storage.
#[async_trait]
pub trait StorageSnapshot: StorageRead {}

/// The storage type encapsulates access to the underlying storage (e.g. SlateDB).
#[async_trait]
pub trait Storage: StorageRead {
    /// Applies a batch of mixed operations (puts, merges, and deletes) atomically.
    ///
    /// All operations in the batch are written together in a single `WriteBatch`,
    /// so either all succeed or none are visible. This is the primary write method
    /// used by flushers that produce a mix of operation types from a frozen delta.
    async fn apply(&self, ops: Vec<RecordOp>) -> StorageResult<()>;

    async fn put(&self, records: Vec<PutRecordOp>) -> StorageResult<()>;

    /// Writes records to storage with custom options.
    ///
    /// This method allows control over durability behavior. Use this when you
    /// need to specify whether to wait for writes to be durable.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to write
    /// * `options` - Write options controlling durability behavior
    async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        options: WriteOptions,
    ) -> StorageResult<()>;

    /// Merges values for the given keys using the configured merge operator.
    ///
    /// This method requires the underlying storage engine to be configured with
    /// a merge operator. If no merge operator is configured, this method will
    /// return a `StorageError::Storage` error.
    ///
    /// The merge operation is atomic - all merges in the batch are applied
    /// together or not at all.
    async fn merge(&self, records: Vec<MergeRecordOp>) -> StorageResult<()>;

    /// Creates a point-in-time snapshot of the storage.
    ///
    /// The snapshot provides a consistent read-only view of the database at the time
    /// the snapshot was created. Reads from the snapshot will not see any subsequent
    /// writes to the underlying storage.
    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>>;

    /// Flushes all pending writes to durable storage.
    ///
    /// This ensures that all writes that have been acknowledged are persisted
    /// to durable storage. For SlateDB, this flushes the memtable to the WAL
    /// and object store.
    async fn flush(&self) -> StorageResult<()>;

    /// Closes the storage, releasing any resources.
    ///
    /// This method should be called before dropping the storage to ensure
    /// proper cleanup. For SlateDB, this releases the database fence.
    async fn close(&self) -> StorageResult<()>;

    /// Registers storage engine metrics into the given Prometheus registry.
    ///
    /// The default implementation is a no-op. Storage backends that expose
    /// internal metrics (e.g., SlateDB) override this to register gauges
    /// that read live values on each scrape.
    #[cfg(feature = "metrics")]
    fn register_metrics(&self, _registry: &mut prometheus_client::registry::Registry) {}
}

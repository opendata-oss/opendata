//! Configuration options for KeyValue operations.

use common::StorageConfig;

/// Configuration for opening a [`KeyValueDb`](crate::KeyValueDb) or
/// [`KeyValueDbReader`](crate::KeyValueDbReader).
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Storage backend configuration.
    pub storage: StorageConfig,
}

/// Options for write operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// If true, waits for the write to be durable before returning.
    /// Default: false (returns after write is applied to memtable).
    pub await_durable: bool,
}

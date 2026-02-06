//! Data types for KeyValue operations.

use bytes::Bytes;

/// A key-value entry returned by iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueEntry {
    /// The user key.
    pub key: Bytes,
    /// The value.
    pub value: Bytes,
}

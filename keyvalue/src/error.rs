//! Error types for KeyValue operations.

use common::StorageError;

/// Error type for KeyValue operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Storage-related errors from the underlying SlateDB layer.
    Storage(String),

    /// Encoding or decoding errors.
    Encoding(String),

    /// Invalid input or parameter errors.
    InvalidInput(String),

    /// Internal errors indicating bugs or invariant violations.
    Internal(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Storage(msg) => write!(f, "Storage error: {}", msg),
            Error::Encoding(msg) => write!(f, "Encoding error: {}", msg),
            Error::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::Storage(msg) => Error::Storage(msg),
            StorageError::Internal(msg) => Error::Internal(msg),
        }
    }
}

/// Result type alias for KeyValue operations.
pub type Result<T> = std::result::Result<T, Error>;

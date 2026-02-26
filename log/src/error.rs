//! Error types for OpenData Log operations.
//!
//! This module defines [`Error`], the primary error type for all log
//! operations, along with a convenient [`Result`] type alias.

use common::{SequenceError, StorageError};

/// Error type for OpenData Log operations.
///
/// This enum captures all possible error conditions that can occur when
/// interacting with the log, including storage failures, encoding issues,
/// and invalid input.
///
/// # Error Categories
///
/// - [`Storage`](Error::Storage): Errors from the underlying SlateDB storage layer,
///   such as I/O failures or corruption.
/// - [`Encoding`](Error::Encoding): Errors during serialization or deserialization
///   of log entries.
/// - [`InvalidInput`](Error::InvalidInput): Errors caused by invalid parameters or
///   arguments provided by the caller.
/// - [`Internal`](Error::Internal): Unexpected internal errors that indicate bugs
///   or invariant violations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Storage-related errors from the underlying SlateDB layer.
    ///
    /// These errors typically indicate I/O failures, corruption, or
    /// issues with the object store backend.
    Storage(String),

    /// Encoding or decoding errors.
    ///
    /// These errors occur when serializing records for storage or
    /// deserializing entries during reads.
    Encoding(String),

    /// Invalid input or parameter errors.
    ///
    /// These errors indicate that the caller provided invalid arguments,
    /// such as empty keys or malformed sequence ranges.
    InvalidInput(String),

    /// Internal errors indicating bugs or invariant violations.
    ///
    /// These errors should not occur during normal operation and
    /// typically indicate a bug in the implementation.
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

impl From<SequenceError> for Error {
    fn from(err: SequenceError) -> Self {
        match err {
            SequenceError::Storage(storage_err) => Error::from(storage_err),
            SequenceError::Deserialize(de_err) => Error::Encoding(de_err.message),
        }
    }
}

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Error::InvalidInput(msg.to_string())
    }
}

/// Result type alias for OpenData Log operations.
///
/// This is a convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

use crate::model::Record;

/// Error type for append operations that preserves the batch for retry.
///
/// Unlike [`Error`], this type carries the original `Vec<Record>` inside
/// retryable variants (`QueueFull`, `Timeout`) so callers can retry without
/// cloning the batch up front.
#[derive(Debug)]
pub enum AppendError {
    /// The write queue is full (non-blocking send failed). Contains the batch.
    QueueFull(Vec<Record>),
    /// Timed out waiting for queue space. Contains the batch.
    Timeout(Vec<Record>),
    /// The writer has shut down.
    Shutdown,
    /// A storage-level error occurred while processing the write.
    Storage(String),
}

impl AppendError {
    /// Returns the batch of records if this is a retryable error, or `None`
    /// for terminal errors.
    pub fn into_inner(self) -> Option<Vec<Record>> {
        match self {
            AppendError::QueueFull(records) => Some(records),
            AppendError::Timeout(records) => Some(records),
            AppendError::Shutdown => None,
            AppendError::Storage(_) => None,
        }
    }
}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::QueueFull(_) => write!(f, "write queue full"),
            AppendError::Timeout(_) => write!(f, "write queue timeout"),
            AppendError::Shutdown => write!(f, "writer shut down"),
            AppendError::Storage(msg) => write!(f, "storage error: {}", msg),
        }
    }
}

impl std::error::Error for AppendError {}

impl From<AppendError> for Error {
    fn from(err: AppendError) -> Self {
        match err {
            AppendError::QueueFull(_) => Error::Internal("write queue full".into()),
            AppendError::Timeout(_) => Error::Internal("write queue timeout".into()),
            AppendError::Shutdown => Error::Internal("writer shut down".into()),
            AppendError::Storage(msg) => Error::Internal(msg),
        }
    }
}

/// Result type alias for append operations.
///
/// This is a convenience alias for `std::result::Result<T, AppendError>`.
pub type AppendResult<T> = std::result::Result<T, AppendError>;

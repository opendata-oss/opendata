//! Error types for OpenData TimeSeries operations.
//!
//! This module defines [`Error`], the primary error type for all time series
//! operations, along with a convenient [`Result`] type alias.

use common::StorageError;

/// Error type for OpenData TimeSeries operations.
///
/// This enum captures all possible error conditions that can occur when
/// interacting with the time series database, including storage failures,
/// encoding issues, and invalid input.
///
/// # Error Categories
///
/// - [`Storage`](Error::Storage): Errors from the underlying storage layer,
///   such as I/O failures or corruption.
/// - [`Encoding`](Error::Encoding): Errors during serialization or deserialization
///   of time series data.
/// - [`InvalidInput`](Error::InvalidInput): Errors caused by invalid parameters or
///   arguments provided by the caller.
/// - [`Internal`](Error::Internal): Unexpected internal errors that indicate bugs
///   or invariant violations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Storage-related errors from the underlying storage layer.
    ///
    /// These errors typically indicate I/O failures, corruption, or
    /// issues with the storage backend.
    Storage(String),

    /// Encoding or decoding errors.
    ///
    /// These errors occur when serializing data for storage or
    /// deserializing during reads.
    Encoding(String),

    /// Invalid input or parameter errors.
    ///
    /// These errors indicate that the caller provided invalid arguments,
    /// such as series without a metric name or malformed labels.
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

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Error::InvalidInput(msg.to_string())
    }
}

impl From<std::time::SystemTimeError> for Error {
    fn from(err: std::time::SystemTimeError) -> Self {
        Error::InvalidInput(format!("Invalid timestamp: {}", err))
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Self {
        Error::InvalidInput(format!("Integer conversion error: {}", err))
    }
}

impl From<crate::serde::EncodingError> for Error {
    fn from(err: crate::serde::EncodingError) -> Self {
        Error::Encoding(err.message)
    }
}

/// Result type alias for OpenData TimeSeries operations.
///
/// This is a convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;

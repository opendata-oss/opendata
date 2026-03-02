use common::serde::DeserializeError;
use common::storage::StorageError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Storage(String),
    Encoding(String),
    InvalidInput(String),
    Query(String),
    Internal(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Storage(msg) => write!(f, "Storage error: {msg}"),
            Error::Encoding(msg) => write!(f, "Encoding error: {msg}"),
            Error::InvalidInput(msg) => write!(f, "Invalid input: {msg}"),
            Error::Query(msg) => write!(f, "Query error: {msg}"),
            Error::Internal(msg) => write!(f, "Internal error: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<StorageError> for Error {
    fn from(e: StorageError) -> Self {
        Error::Storage(e.to_string())
    }
}

impl From<DeserializeError> for Error {
    fn from(e: DeserializeError) -> Self {
        Error::Encoding(e.to_string())
    }
}

impl From<common::SequenceError> for Error {
    fn from(e: common::SequenceError) -> Self {
        Error::Storage(e.to_string())
    }
}

impl From<grafeo_common::utils::error::Error> for Error {
    fn from(e: grafeo_common::utils::error::Error) -> Self {
        Error::Query(e.to_string())
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::Internal(s.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

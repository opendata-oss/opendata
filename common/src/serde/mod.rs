//! Serialization utilities for OpenData.

pub mod terminated_bytes;

/// Error type for deserialization failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeserializeError {
    pub message: String,
}

impl std::error::Error for DeserializeError {}

impl std::fmt::Display for DeserializeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

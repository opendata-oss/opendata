//! Error types for the write coordinator.

/// Errors that can occur during write coordination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteCoordinatorError {
    /// An error occurred in the underlying storage layer.
    Storage(String),
    /// An error occurred while applying writes to a delta.
    Apply(String),
    /// The coordinator has been shut down.
    Shutdown,
    /// The write queue is full and backpressure is being applied.
    Backpressure,
    /// An internal error occurred.
    Internal(String),
}

impl std::fmt::Display for WriteCoordinatorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteCoordinatorError::Storage(msg) => write!(f, "Storage error: {}", msg),
            WriteCoordinatorError::Apply(msg) => write!(f, "Apply error: {}", msg),
            WriteCoordinatorError::Shutdown => write!(f, "Coordinator has been shut down"),
            WriteCoordinatorError::Backpressure => write!(f, "Backpressure: write queue is full"),
            WriteCoordinatorError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for WriteCoordinatorError {}

impl From<crate::StorageError> for WriteCoordinatorError {
    fn from(err: crate::StorageError) -> Self {
        WriteCoordinatorError::Storage(err.to_string())
    }
}

/// Result type alias for write coordinator operations.
pub type Result<T> = std::result::Result<T, WriteCoordinatorError>;

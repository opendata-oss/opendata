/// Errors that can occur during write coordination.
#[derive(Debug, Clone)]
pub enum WriteError {
    /// The write queue is full and backpressure is being applied
    Backpressure,
    /// The coordinator has been dropped/shutdown
    Shutdown,
    /// Error applying the write to the delta
    ApplyError(u64, String),
    /// Error flushing the delta to storage
    FlushError(String),
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Backpressure => write!(f, "write queue is full, backpressure applied"),
            WriteError::Shutdown => write!(f, "coordinator has been dropped/shutdown"),
            WriteError::ApplyError(epoch, msg) => {
                write!(f, "error applying write @{}: {}", epoch, msg)
            }
            WriteError::FlushError(msg) => write!(f, "error flushing delta: {}", msg),
            WriteError::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for WriteError {}

/// Result type for write operations.
pub type WriteResult<T> = std::result::Result<T, WriteError>;

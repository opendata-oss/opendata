#[derive(Debug, Clone)]
pub enum Error {
    Storage(String),
    Serialization(String),
    BackpressureLimitExceeded { incoming_size: usize, limit: usize },
    Fenced,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Storage(msg) => write!(f, "Storage error: {}", msg),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::BackpressureLimitExceeded {
                incoming_size,
                limit,
            } => write!(
                f,
                "Incoming size {} exceeds backpressure limit {}",
                incoming_size, limit
            ),
            Error::Fenced => write!(f, "consumer fenced: epoch mismatch"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

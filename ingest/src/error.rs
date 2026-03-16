#[derive(Debug, Clone)]
pub enum Error {
    Storage(String),
    Serialization(String),
    InvalidInput(String),
    Fenced,
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Storage(msg) => write!(f, "Storage error: {}", msg),
            Error::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            Error::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            Error::Fenced => write!(f, "Consumer fenced: epoch mismatch"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

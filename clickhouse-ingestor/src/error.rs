//! Top-level error type for the ingestor pipeline.
//!
//! Layer-specific error types (envelope, signal decoder, adapter, writer)
//! all flow into [`IngestorError`] so the runtime loop can dispatch on a
//! single error shape.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestorError {
    #[error("buffer error: {0}")]
    Buffer(#[from] buffer::Error),

    #[error("metadata envelope: {0}")]
    Envelope(#[from] crate::envelope::EnvelopeError),

    #[error("signal decoder: {0}")]
    SignalDecode(String),

    #[error("adapter: {0}")]
    Adapter(String),

    #[error("clickhouse writer: {0}")]
    Writer(#[from] crate::writer::WriterError),

    #[error("config: {0}")]
    Config(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type IngestorResult<T> = Result<T, IngestorError>;

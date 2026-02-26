mod config;
mod error;
mod ingestor;
mod model;
mod queue;

pub use config::IngestorConfig;
pub use error::{Error, Result};
pub use ingestor::{Ingestor, WriteWatcher};

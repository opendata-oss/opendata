//! HTTP server for OpenData Vector.
//!
//! This module provides an HTTP API for interacting with the vector database,
//! exposing upsert, search, and get operations via REST endpoints.

mod config;
mod error;
mod handlers;
mod middleware;
mod proto;
mod request;
mod response;
mod vector;
mod vector_reader;

pub use config::{VectorServerConfig, load_reader_config, load_vector_config};
pub use vector::VectorServer;
pub use vector_reader::VectorReaderServer;

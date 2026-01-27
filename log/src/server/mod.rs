//! HTTP server for OpenData Log.
//!
//! This module provides an HTTP API for interacting with the log database,
//! exposing append, scan, list, and count operations via REST endpoints.

mod config;
mod error;
pub mod handlers;
mod http;
pub mod metrics;
mod middleware;
pub mod proto;
mod request;
mod response;

pub use config::{CliArgs, LogServerConfig};
pub use http::LogServer;

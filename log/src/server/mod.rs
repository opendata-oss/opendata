//! HTTP server for OpenData Log.
//!
//! This module provides an HTTP API for interacting with the log database,
//! exposing append, scan, list, and count operations via REST endpoints.

mod config;
mod error;
mod handlers;
mod metrics;
mod middleware;
mod request;
mod response;
mod server;

pub use config::{CliArgs, LogServerConfig};
pub use server::LogServer;

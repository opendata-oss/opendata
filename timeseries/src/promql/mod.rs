#![allow(dead_code)]

pub(crate) mod config;
pub(crate) mod evaluator;
mod functions;
pub(crate) mod metrics;
mod middleware;
pub(crate) mod openmetrics;
mod parser;
pub mod query_parser;
#[cfg(feature = "remote-write")]
pub(crate) mod remote_write;
mod request;
mod response;
mod router;
pub(crate) mod scraper;
pub(crate) mod selector;
pub(crate) mod server;
mod tsdb_router;

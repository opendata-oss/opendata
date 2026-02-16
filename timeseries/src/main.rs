#![allow(dead_code)]

mod delta;
mod error;
mod flusher;
mod index;
mod minitsdb;
mod model;
mod promql;
mod query;
mod serde;
mod storage;
#[cfg(test)]
mod test_utils;
mod tsdb;
mod util;

use std::sync::Arc;

use clap::Parser;
use common::storage::factory::create_storage;
use common::{StorageRuntime, StorageSemantics};

use promql::config::{CliArgs, PrometheusConfig, load_config};
use promql::server::{PromqlServer, ServerConfig};
use storage::merge_operator::OpenTsdbMergeOperator;
use tsdb::Tsdb;

#[tokio::main]
async fn main() {
    // Initialize tracing with configurable log level via RUST_LOG environment variable
    // Default to "info" if RUST_LOG is not set
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE) // Only exit events with timing
        .with_target(true)
        .with_line_number(true)
        .init();

    // Parse CLI arguments
    let args = CliArgs::parse();

    // Load Prometheus configuration if provided
    let prometheus_config = if let Some(config_path) = &args.config {
        match load_config(config_path) {
            Ok(config) => {
                tracing::info!("Loaded configuration from {}", config_path);
                config
            }
            Err(e) => {
                tracing::error!("Failed to load configuration: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        tracing::info!("No configuration file provided, using defaults");
        PrometheusConfig::default()
    };

    // Create storage based on configuration
    tracing::info!(
        "Creating storage with config: {:?}",
        prometheus_config.storage
    );
    let merge_operator = Arc::new(OpenTsdbMergeOperator);
    let storage = create_storage(
        &prometheus_config.storage,
        StorageRuntime::new(),
        StorageSemantics::new().with_merge_operator(merge_operator),
    )
    .await
    .unwrap_or_else(|e| {
        tracing::error!("Failed to create storage: {}", e);
        std::process::exit(1);
    });
    tracing::info!("Storage created successfully");

    // Create Tsdb
    let tsdb = Arc::new(Tsdb::new(storage.clone()));

    // Create server configuration
    let config = ServerConfig {
        port: args.port,
        prometheus_config,
    };

    // Create and run server
    let server = PromqlServer::new(tsdb, config, storage);

    tracing::info!(
        "Starting timeseries Prometheus-compatible server on port {}...",
        args.port
    );
    server.run().await;
}

#![allow(dead_code)]

mod delta;
mod error;
mod head;
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

use promql::config::{CliArgs, PrometheusConfig, load_config};
use promql::server::{PromqlServer, ServerConfig};
use storage::merge_operator::OpenTsdbMergeOperator;
use tsdb::Tsdb;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

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
    let storage = create_storage(&prometheus_config.storage, Some(merge_operator))
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to create storage: {}", e);
            std::process::exit(1);
        });
    tracing::info!("Storage created successfully");

    // Create Tsdb
    let tsdb = Arc::new(Tsdb::new(storage));

    // Create server configuration
    let config = ServerConfig {
        port: args.port,
        prometheus_config,
    };

    // Create and run server
    let server = PromqlServer::new(tsdb, config);

    tracing::info!(
        "Starting timeseries Prometheus-compatible server on port {}...",
        args.port
    );
    server.run().await;
}

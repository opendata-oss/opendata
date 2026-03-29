#![allow(dead_code)]

mod config;
mod delta;
mod error;
mod flusher;
mod index;
mod minitsdb;
mod model;
#[cfg(feature = "otel")]
mod otel;
mod promql;
mod query;
mod reader;
mod serde;
mod server;
mod storage;
#[cfg(test)]
mod test_utils;
mod timeseries;
mod tsdb;
mod util;

use std::sync::Arc;

use clap::Parser;
use common::{StorageBuilder, StorageSemantics};

use promql::config::{CliArgs, PrometheusConfig, load_config};
use reader::TimeSeriesDbReader;
use server::{ServerConfig, TimeSeriesHttpServer};
use storage::merge_operator::OpenTsdbMergeOperator;
use tracing_subscriber::EnvFilter;
use tsdb::{Tsdb, TsdbEngine};

#[tokio::main]
async fn main() {
    // Initialize tracing with configurable log level via RUST_LOG environment variable
    // Default to "info" if RUST_LOG is not set
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
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

    let read_only = prometheus_config.read_only;
    let (tsdb, storage) = if read_only {
        // Read-only mode: open a non-fencing reader.
        let reader = TimeSeriesDbReader::open(
            prometheus_config.storage.clone(),
            prometheus_config.reader.clone(),
            prometheus_config.cache_capacity,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to open read-only storage: {}", e);
            std::process::exit(1);
        });
        let engine: Arc<TsdbEngine> = Arc::new(Arc::new(reader).into());
        tracing::info!("Opened storage in read-only mode");
        (engine, None)
    } else {
        // Read-write mode: open full storage + Tsdb
        let merge_operator = Arc::new(OpenTsdbMergeOperator);
        let storage = StorageBuilder::new(&prometheus_config.storage)
            .await
            .unwrap_or_else(|e| {
                tracing::error!("Failed to create storage: {}", e);
                std::process::exit(1);
            })
            .with_semantics(StorageSemantics::new().with_merge_operator(merge_operator))
            .build()
            .await
            .unwrap_or_else(|e| {
                tracing::error!("Failed to create storage: {}", e);
                std::process::exit(1);
            });
        tracing::info!("Storage created successfully");
        let engine: Arc<TsdbEngine> = Arc::new(Arc::new(Tsdb::new(storage.clone())).into());
        (engine, Some(storage))
    };

    // Create server configuration
    let config = ServerConfig {
        port: args.port,
        prometheus_config,
    };

    // Create and run server
    let server = TimeSeriesHttpServer::new(tsdb, config, storage);

    tracing::info!(
        "Starting timeseries {} server on port {}...",
        if read_only { "read-only" } else { "read-write" },
        args.port
    );
    server.run().await;
}

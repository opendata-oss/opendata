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
mod tsdb;
mod util;

use std::sync::Arc;

use clap::Parser;
use common::storage::factory::create_storage;
use common::{StorageRuntime, StorageSemantics};

use promql::config::{CliArgs, PrometheusConfig, load_config};
use server::{ServerConfig, TimeSeriesHttpServer};
use storage::merge_operator::OpenTsdbMergeOperator;
use tracing_subscriber::EnvFilter;
use tsdb::Tsdb;

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

    let config = ServerConfig {
        port: args.port,
        prometheus_config: prometheus_config.clone(),
    };

    if args.reader {
        tracing::info!("Starting in reader mode (no writer fencing)");
        let reader_config = crate::config::ReaderConfig {
            storage: prometheus_config.storage,
            ..Default::default()
        };
        let reader = reader::TimeSeriesDbReader::open(reader_config)
            .await
            .unwrap_or_else(|e| {
                tracing::error!("Failed to open reader: {}", e);
                std::process::exit(1);
            });
        let server = TimeSeriesHttpServer::new_reader(Arc::new(reader), config);
        tracing::info!("Starting timeseries reader server on port {}...", args.port);
        server.run().await;
    } else {
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

        let tsdb = Arc::new(Tsdb::new(storage.clone()));
        let server = TimeSeriesHttpServer::new(tsdb, config, storage);
        tracing::info!(
            "Starting timeseries Prometheus-compatible server on port {}...",
            args.port
        );
        server.run().await;
    }
}

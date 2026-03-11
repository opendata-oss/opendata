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

    // Create storage based on configuration
    tracing::info!(
        "Creating storage with config: {:?}",
        prometheus_config.storage
    );
    let merge_operator = Arc::new(OpenTsdbMergeOperator);
    let mut runtime = StorageRuntime::new();
    if let Some(ref cache_config) = prometheus_config.block_cache {
        if matches!(prometheus_config.storage, common::StorageConfig::SlateDb(_)) {
            let hybrid_config = common::HybridCacheConfig {
                memory_capacity: cache_config.memory_capacity,
                disk_capacity: cache_config.disk_capacity,
                disk_path: cache_config.disk_path.clone(),
            };
            let cache = common::create_hybrid_cache(&hybrid_config)
                .await
                .unwrap_or_else(|e| {
                    tracing::error!("Failed to create hybrid block cache: {}", e);
                    std::process::exit(1);
                });
            runtime = runtime.with_block_cache(Arc::new(cache));
            tracing::info!(
                memory_mb = cache_config.memory_capacity / (1024 * 1024),
                disk_mb = cache_config.disk_capacity / (1024 * 1024),
                disk_path = %cache_config.disk_path,
                "hybrid block cache enabled"
            );
        } else {
            tracing::warn!("block_cache config ignored for non-SlateDB storage");
        }
    }
    let storage = create_storage(
        &prometheus_config.storage,
        runtime,
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
    let server = TimeSeriesHttpServer::new(tsdb, config, storage);

    tracing::info!(
        "Starting timeseries Prometheus-compatible server on port {}...",
        args.port
    );
    server.run().await;
}

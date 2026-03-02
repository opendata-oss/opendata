//! OpenData Graph HTTP Server binary entry point.

use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use graph::db::GraphDb;
use graph::server::{CliArgs, GraphServer, GraphServerConfig};

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Parse CLI arguments
    let args = CliArgs::parse();

    // Create graph configuration
    let graph_config = args.to_graph_config();
    let server_config = GraphServerConfig::from(&args);

    tracing::info!("Opening graph database with config: {:?}", graph_config);

    // Open the graph database (creates storage with correct merge operator)
    let db = GraphDb::open_with_config(&graph_config)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to open graph database: {}", e);
            std::process::exit(1);
        });

    // Create and run the server
    let server = GraphServer::new(Arc::new(db), server_config);
    server.run().await;
}

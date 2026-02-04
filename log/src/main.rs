//! OpenData Log HTTP Server binary entry point.

use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use log::LogDb;
use log::server::{CliArgs, LogServer, LogServerConfig};

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

    // Create log configuration
    let log_config = args.to_log_config();
    let server_config = LogServerConfig::from(&args);

    tracing::info!("Opening log with config: {:?}", log_config);

    // Open the log
    let log = LogDb::open(log_config).await.expect("Failed to open log");

    // Create and run the server
    let server = LogServer::new(Arc::new(log), server_config);
    server.run().await;
}

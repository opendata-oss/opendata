//! OpenData Log HTTP Server binary entry point.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use log::server::{CliArgs, LogServer, LogServerConfig};
use log::{LogDb, LogDbReader};

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

    let server_config = LogServerConfig::from(&args);

    // Install the metrics-rs recorder early so that slatedb metrics registered
    // during open are captured by the prometheus exporter.
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let metrics_handle = recorder.handle();
    let _ = metrics::set_global_recorder(recorder);

    // Open a read-only gateway or a full read-write server depending on the flag.
    let server = if args.read_only {
        let reader_config = args.to_reader_config();
        tracing::info!("Opening log reader with config: {:?}", reader_config);
        let reader = LogDbReader::open(reader_config)
            .await
            .expect("Failed to open log reader");
        LogServer::new_read_only(Arc::new(reader), server_config, metrics_handle)
    } else {
        let log_config = args.to_log_config();
        tracing::info!("Opening log with config: {:?}", log_config);
        let log = LogDb::open(log_config).await.expect("Failed to open log");
        LogServer::new(Arc::new(log), server_config, metrics_handle)
    };

    server.run().await;
}

//! OpenData Vector HTTP Server binary entry point.

use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use vector::VectorDb;
use vector::VectorDbReader;
use vector::server::{
    MetricsState, VectorReaderServer, VectorServer, VectorServerConfig, load_reader_config,
    load_vector_config,
};

/// CLI arguments for the vector server.
#[derive(Debug, Parser)]
#[command(name = "opendata-vector")]
#[command(about = "OpenData Vector HTTP Server")]
struct CliArgs {
    /// HTTP server port.
    #[arg(long, default_value = "8080")]
    port: u16,

    #[command(subcommand)]
    command: Command,
}

/// Server mode subcommands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Run the read-write vector server.
    Vector {
        /// Path to the vector database configuration file (YAML).
        #[arg(long)]
        config: String,
    },
    /// Run the read-only vector reader server.
    Reader {
        /// Path to the reader configuration file (YAML).
        #[arg(long)]
        config: String,
    },
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Install the metrics-rs Prometheus recorder before any subsystem
    // (including slatedb) emits metrics. The resulting handle is rendered by
    // the `/metrics` route.
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let metrics_state = MetricsState {
        handle: recorder.handle(),
    };
    metrics::set_global_recorder(recorder).expect("global metrics recorder already installed");

    // Parse CLI arguments
    let args = CliArgs::parse();
    let server_config = VectorServerConfig { port: args.port };

    match args.command {
        Command::Vector { config } => {
            let vector_config = load_vector_config(&config);
            tracing::info!("Opening vector database with config: {:?}", vector_config);
            let metadata_fields = vector_config.metadata_fields.clone();
            #[cfg(feature = "buffer")]
            let buffer_consumer_config = vector_config.buffer_consumer.clone();

            let db = Arc::new(
                VectorDb::open(vector_config)
                    .await
                    .expect("Failed to open vector database"),
            );

            let server = VectorServer::new(db, server_config, metadata_fields, metrics_state);

            #[cfg(feature = "buffer")]
            let server = match buffer_consumer_config {
                Some(buffer_config) => server.with_buffer_consumer(buffer_config),
                None => server,
            };

            server.run().await;
        }
        Command::Reader { config } => {
            let reader_config = load_reader_config(&config);
            tracing::info!(
                "Opening vector database reader with config: {:?}",
                reader_config
            );
            let metadata_fields = reader_config.metadata_fields.clone();

            let reader = VectorDbReader::open(reader_config)
                .await
                .expect("Failed to open vector database reader");

            let server = VectorReaderServer::new(
                Arc::new(reader),
                server_config,
                metadata_fields,
                metrics_state,
            );
            server.run().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_server_config_from_cli_args() {
        // given
        let args = CliArgs {
            port: 9090,
            command: Command::Vector {
                config: "unused.yaml".to_string(),
            },
        };

        // when
        let server_config = VectorServerConfig { port: args.port };

        // then
        assert_eq!(server_config.port, 9090);
    }
}

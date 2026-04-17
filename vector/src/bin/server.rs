//! OpenData Vector HTTP Server binary entry point.

use std::sync::Arc;

use clap::{Parser, Subcommand};

use common::tracing::{TracingGuard, init as init_tracing};
use vector::VectorDb;
use vector::VectorDbReader;
use vector::server::{
    VectorReaderServer, VectorServer, VectorServerConfig, load_reader_config, load_vector_config,
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
    // Parse CLI arguments first so we can read tracing config out of the YAML.
    let args = CliArgs::parse();
    let server_config = VectorServerConfig { port: args.port };

    match args.command {
        Command::Vector { config } => {
            let vector_config = load_vector_config(&config);
            let _trace_guard = init_and_log_tracing(vector_config.tracing.clone());
            tracing::info!("Opening vector database with config: {:?}", vector_config);
            let metadata_fields = vector_config.metadata_fields.clone();

            let db = VectorDb::open(vector_config)
                .await
                .expect("Failed to open vector database");

            let server = VectorServer::new(Arc::new(db), server_config, metadata_fields);
            server.run().await;
        }
        Command::Reader { config } => {
            let reader_config = load_reader_config(&config);
            let _trace_guard = init_and_log_tracing(reader_config.tracing.clone());
            tracing::info!(
                "Opening vector database reader with config: {:?}",
                reader_config
            );
            let metadata_fields = reader_config.metadata_fields.clone();

            let reader = VectorDbReader::open(reader_config)
                .await
                .expect("Failed to open vector database reader");

            let server = VectorReaderServer::new(Arc::new(reader), server_config, metadata_fields);
            server.run().await;
        }
    }
}

/// Initialize tracing and, if trace capture is enabled, log the output path so
/// operators can find the file.
fn init_and_log_tracing(config: common::TracingConfig) -> TracingGuard {
    let guard = init_tracing(config);
    if let Some(path) = guard.output_path() {
        tracing::info!(path = %path.display(), "tracing: chrome trace output enabled");
    }
    guard
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

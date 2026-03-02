//! Configuration for the graph HTTP server.

use clap::Parser;
use common::StorageConfig;
use common::storage::config::{
    AwsObjectStoreConfig, LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
};

use crate::Config;

/// CLI arguments for the graph server.
#[derive(Debug, Parser)]
#[command(name = "opendata-graph")]
#[command(about = "OpenData Graph HTTP Server")]
pub struct CliArgs {
    /// HTTP server port.
    #[arg(long, default_value = "8080")]
    pub port: u16,

    /// Storage data directory path (for local storage).
    #[arg(long, default_value = ".data")]
    pub data_dir: String,

    /// Use in-memory storage (for testing).
    #[arg(long, default_value = "false")]
    pub in_memory: bool,

    /// S3 bucket name (enables S3 storage when set).
    #[arg(long)]
    pub s3_bucket: Option<String>,

    /// AWS region for S3 storage.
    #[arg(long, default_value = "us-east-1")]
    pub s3_region: String,
}

impl CliArgs {
    /// Convert CLI args to graph configuration.
    pub fn to_graph_config(&self) -> Config {
        let storage = if self.in_memory {
            StorageConfig::InMemory
        } else if let Some(bucket) = &self.s3_bucket {
            StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "data".to_string(),
                object_store: ObjectStoreConfig::Aws(AwsObjectStoreConfig {
                    region: self.s3_region.clone(),
                    bucket: bucket.clone(),
                }),
                settings_path: None,
            })
        } else {
            StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "data".to_string(),
                object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                    path: self.data_dir.clone(),
                }),
                settings_path: None,
            })
        };

        Config {
            storage,
            ..Default::default()
        }
    }
}

/// Configuration for the graph HTTP server.
#[derive(Debug, Clone)]
pub struct GraphServerConfig {
    /// HTTP server port.
    pub port: u16,
}

impl Default for GraphServerConfig {
    fn default() -> Self {
        Self { port: 8080 }
    }
}

impl From<&CliArgs> for GraphServerConfig {
    fn from(args: &CliArgs) -> Self {
        Self { port: args.port }
    }
}

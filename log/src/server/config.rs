//! Configuration for the log HTTP server.

use clap::Parser;
use common::storage::config::{
    AwsObjectStoreConfig, LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
};
use common::StorageConfig;

use crate::Config;

/// CLI arguments for the log server.
#[derive(Debug, Parser)]
#[command(name = "log")]
#[command(about = "OpenData Log HTTP Server")]
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
    /// Convert CLI args to log configuration.
    pub fn to_log_config(&self) -> Config {
        let storage = if self.in_memory {
            StorageConfig::InMemory
        } else if let Some(bucket) = &self.s3_bucket {
            // S3 storage
            StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "data".to_string(),
                object_store: ObjectStoreConfig::Aws(AwsObjectStoreConfig {
                    region: self.s3_region.clone(),
                    bucket: bucket.clone(),
                }),
                settings_path: None,
            })
        } else {
            // Local filesystem storage
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

/// Configuration for the log HTTP server.
#[derive(Debug, Clone)]
pub struct LogServerConfig {
    /// HTTP server port.
    pub port: u16,
}

impl Default for LogServerConfig {
    fn default() -> Self {
        Self { port: 8080 }
    }
}

impl From<&CliArgs> for LogServerConfig {
    fn from(args: &CliArgs) -> Self {
        Self { port: args.port }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_in_memory_config() {
        // given
        let args = CliArgs {
            port: 9090,
            data_dir: ".data".to_string(),
            in_memory: true,
            s3_bucket: None,
            s3_region: "us-east-1".to_string(),
        };

        // when
        let config = args.to_log_config();

        // then
        assert!(matches!(config.storage, StorageConfig::InMemory));
    }

    #[test]
    fn should_create_local_slatedb_config() {
        // given
        let args = CliArgs {
            port: 9090,
            data_dir: "/tmp/log-data".to_string(),
            in_memory: false,
            s3_bucket: None,
            s3_region: "us-east-1".to_string(),
        };

        // when
        let config = args.to_log_config();

        // then
        match config.storage {
            StorageConfig::SlateDb(slate_config) => {
                match slate_config.object_store {
                    ObjectStoreConfig::Local(local_config) => {
                        assert_eq!(local_config.path, "/tmp/log-data");
                    }
                    _ => panic!("Expected Local object store"),
                }
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_create_s3_slatedb_config() {
        // given
        let args = CliArgs {
            port: 9090,
            data_dir: ".data".to_string(),
            in_memory: false,
            s3_bucket: Some("my-bucket".to_string()),
            s3_region: "us-west-2".to_string(),
        };

        // when
        let config = args.to_log_config();

        // then
        match config.storage {
            StorageConfig::SlateDb(slate_config) => {
                match slate_config.object_store {
                    ObjectStoreConfig::Aws(aws_config) => {
                        assert_eq!(aws_config.bucket, "my-bucket");
                        assert_eq!(aws_config.region, "us-west-2");
                    }
                    _ => panic!("Expected Aws object store"),
                }
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_create_server_config_from_cli_args() {
        // given
        let args = CliArgs {
            port: 9090,
            data_dir: ".data".to_string(),
            in_memory: true,
            s3_bucket: None,
            s3_region: "us-east-1".to_string(),
        };

        // when
        let server_config = LogServerConfig::from(&args);

        // then
        assert_eq!(server_config.port, 9090);
    }
}

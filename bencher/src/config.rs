//! Configuration types for the bencher.

use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig};
use common::StorageConfig;
use serde::{Deserialize, Serialize};

/// Configuration for the bencher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Configuration for benchmark data storage.
    pub data: DataConfig,
    /// Configuration for benchmark results export.
    pub results: ResultsConfig,
}

/// Configuration for benchmark data storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataConfig {
    /// Storage configuration for benchmark data.
    pub storage: StorageConfig,
}

/// Configuration for benchmark results export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultsConfig {
    /// Object store for results output. If None, CSV output goes to stdout.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_store: Option<ObjectStoreConfig>,
    /// Output format for metrics.
    pub format: OutputFormat,
}

impl ResultsConfig {
    /// Convert the results config to a StorageConfig for TimeSeries.
    pub fn to_storage_config(&self) -> StorageConfig {
        match &self.object_store {
            Some(obj_config) => StorageConfig::SlateDb(SlateDbStorageConfig {
                path: "bench-results".to_string(),
                object_store: obj_config.clone(),
                settings_path: None,
            }),
            None => StorageConfig::InMemory,
        }
    }
}

/// Metrics output format.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum OutputFormat {
    /// Write CSV files.
    #[default]
    Csv,
    /// Write to timeseries format.
    Timeseries,
}

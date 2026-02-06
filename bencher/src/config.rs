//! Configuration types for the bencher.

use std::time::Duration;

use common::StorageConfig;
use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig};
use serde::{Deserialize, Serialize};

/// Configuration for the bencher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Configuration for benchmark data storage.
    pub data: DataConfig,
    /// Optional reporter for persisting metrics.
    ///
    /// If configured, ongoing metrics are written periodically and summary
    /// metrics are written at the end of each benchmark run.
    /// Summary metrics are always printed to console regardless of this setting.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reporter: Option<ReporterConfig>,
}

/// Configuration for benchmark data storage.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataConfig {
    /// Storage configuration for benchmark data.
    #[serde(default)]
    pub storage: StorageConfig,
}

/// Configuration for metrics reporting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReporterConfig {
    /// Object store for metrics storage.
    pub object_store: ObjectStoreConfig,
    /// Interval for periodic metrics reporting.
    #[serde(with = "humantime_serde", default = "default_interval")]
    pub interval: Duration,
}

fn default_interval() -> Duration {
    Duration::from_secs(10)
}

impl ReporterConfig {
    /// Convert to a StorageConfig for TimeSeries.
    pub fn to_storage_config(&self) -> StorageConfig {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "bench-results".to_string(),
            object_store: self.object_store.clone(),
            settings_path: None,
        })
    }
}

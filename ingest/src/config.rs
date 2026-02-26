use std::time::Duration;

use common::StorageConfig;
use common::storage::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};

/// Configuration for an [`Ingestor`](crate::Ingestor).
///
/// Controls where data batches and the queue manifest are stored, how often
/// batches are flushed, and when backpressure is applied.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorConfig {
    /// Determines where and how ingest data is persisted. See [`StorageConfig`].
    pub storage: StorageConfig,

    /// Path prefix for data batch objects in object storage.
    ///
    /// Defaults to `"ingest"`.
    #[serde(default = "default_data_path_prefix")]
    pub data_path_prefix: String,

    /// Path to the queue manifest in object storage.
    ///
    /// Defaults to `"ingest/manifest"`.
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,

    /// Time interval that triggers the flush of the current batch to object storage when elapsed.
    ///
    /// Defaults to 100 ms.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_flush_interval")]
    pub flush_interval: Duration,

    /// Batch size in bytes (entries and metadata) that triggers a flush when exceeded.
    ///
    /// Defaults to 64 MiB.
    #[serde(default = "default_flush_size_bytes")]
    pub flush_size_bytes: usize,

    /// Maximum number of input entries vectors that can be buffered for the background
    /// batch writer before backpressure is applied.
    ///
    /// Defaults to 1000.
    #[serde(default = "default_max_buffered_inputs")]
    pub max_buffered_inputs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorConfig {
    pub object_store_config: ObjectStoreConfig,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
    #[serde(default = "default_heartbeat_timeout", with = "duration_millis")]
    pub heartbeat_timeout: Duration,
    #[serde(default = "default_done_cleanup_threshold")]
    pub done_cleanup_threshold: usize,
}

fn default_data_path_prefix() -> String {
    "ingest".to_string()
}

fn default_manifest_path() -> String {
    "ingest/manifest".to_string()
}

fn default_flush_interval() -> Duration {
    Duration::from_millis(100)
}

fn default_flush_size_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_max_buffered_inputs() -> usize {
    1000
}

fn default_heartbeat_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_done_cleanup_threshold() -> usize {
    100
}

mod duration_millis {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(duration.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

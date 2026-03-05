use std::time::Duration;

use common::storage::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorConfig {
    pub object_store_config: ObjectStoreConfig,
    #[serde(default = "default_data_path_prefix")]
    pub data_path_prefix: String,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
    #[serde(default = "default_flush_interval", with = "duration_millis")]
    pub flush_interval: Duration,
    #[serde(default = "default_flush_size_bytes")]
    pub flush_size_bytes: usize,
    #[serde(default = "default_max_unflushed_bytes")]
    pub max_unflushed_bytes: usize,
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
    "ingest/manifest.json".to_string()
}

fn default_flush_interval() -> Duration {
    Duration::from_millis(100)
}

fn default_flush_size_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_max_unflushed_bytes() -> usize {
    usize::MAX
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

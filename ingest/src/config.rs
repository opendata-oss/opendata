use common::storage::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorConfig {
    pub object_store: ObjectStoreConfig,
    #[serde(default = "default_path_prefix")]
    pub path_prefix: String,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
    #[serde(default = "default_batch_interval_ms")]
    pub batch_interval_ms: u64,
    #[serde(default = "default_batch_max_bytes")]
    pub batch_max_bytes: usize,
    #[serde(default)]
    pub backpressure_at_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectorConfig {
    pub object_store: ObjectStoreConfig,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
    #[serde(default = "default_heartbeat_timeout_ms")]
    pub heartbeat_timeout_ms: i64,
    #[serde(default = "default_done_cleanup_threshold")]
    pub done_cleanup_threshold: usize,
}

fn default_path_prefix() -> String {
    "ingest".to_string()
}

fn default_manifest_path() -> String {
    "ingest/manifest.json".to_string()
}

fn default_batch_interval_ms() -> u64 {
    100
}

fn default_batch_max_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_heartbeat_timeout_ms() -> i64 {
    30_000
}

fn default_done_cleanup_threshold() -> usize {
    100
}

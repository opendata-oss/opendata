use common::storage::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerConfig {
    pub object_store: ObjectStoreConfig,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    pub object_store: ObjectStoreConfig,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
    #[serde(default = "default_heartbeat_timeout_ms")]
    pub heartbeat_timeout_ms: i64,
    #[serde(default = "default_done_cleanup_threshold")]
    pub done_cleanup_threshold: usize,
}

fn default_manifest_path() -> String {
    "ingest/manifest.json".to_string()
}

fn default_heartbeat_timeout_ms() -> i64 {
    30_000
}

fn default_done_cleanup_threshold() -> usize {
    100
}

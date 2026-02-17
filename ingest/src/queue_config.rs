use common::storage::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub object_store: ObjectStoreConfig,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
    #[serde(default = "default_heartbeat_timeout_ms")]
    pub heartbeat_timeout_ms: i64,
}

fn default_manifest_path() -> String {
    "ingest/manifest.json".to_string()
}

fn default_heartbeat_timeout_ms() -> i64 {
    30_000
}

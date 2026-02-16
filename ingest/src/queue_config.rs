use common::storage::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueConfig {
    pub object_store: ObjectStoreConfig,
    #[serde(default = "default_manifest_path")]
    pub manifest_path: String,
}

fn default_manifest_path() -> String {
    "ingest/manifest.json".to_string()
}

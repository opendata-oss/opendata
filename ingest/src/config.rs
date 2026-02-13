use common::storage::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub object_store: ObjectStoreConfig,
    pub endpoint: String,
    #[serde(default = "default_path_prefix")]
    pub path_prefix: String,
}

fn default_path_prefix() -> String {
    "ingest".to_string()
}

use common::storage::config::ObjectStoreConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestorConfig {
    pub object_store: ObjectStoreConfig,
    #[serde(default = "default_path_prefix")]
    pub path_prefix: String,
    #[serde(default = "default_batch_interval_ms")]
    pub batch_interval_ms: u64,
    #[serde(default = "default_batch_max_bytes")]
    pub batch_max_bytes: usize,
    #[serde(default)]
    pub backpressure_at_bytes: Option<usize>,
}

fn default_path_prefix() -> String {
    "ingest".to_string()
}

fn default_batch_interval_ms() -> u64 {
    100
}

fn default_batch_max_bytes() -> usize {
    64 * 1024 * 1024
}

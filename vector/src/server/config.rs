//! Configuration for the vector HTTP server.

use std::path::Path;

use crate::Config;
use crate::model::ReaderConfig;

/// Load a vector database configuration from a YAML file.
pub fn load_vector_config(path: &str) -> Config {
    let contents = std::fs::read_to_string(Path::new(path))
        .unwrap_or_else(|e| panic!("Failed to read config file '{}': {}", path, e));
    serde_yaml::from_str(&contents)
        .unwrap_or_else(|e| panic!("Failed to parse config file '{}': {}", path, e))
}

/// Load a reader configuration from a YAML file.
pub fn load_reader_config(path: &str) -> ReaderConfig {
    let contents = std::fs::read_to_string(Path::new(path))
        .unwrap_or_else(|e| panic!("Failed to read config file '{}': {}", path, e));
    serde_yaml::from_str(&contents)
        .unwrap_or_else(|e| panic!("Failed to parse config file '{}': {}", path, e))
}

/// Configuration for the vector HTTP server.
#[derive(Debug, Clone)]
pub struct VectorServerConfig {
    /// HTTP server port.
    pub port: u16,
}

impl Default for VectorServerConfig {
    fn default() -> Self {
        Self { port: 8080 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::StorageConfig;

    #[test]
    fn should_load_vector_config_from_yaml() {
        // given
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
storage:
  type: InMemory
dimensions: 384
distance_metric: L2
flush_interval: 60
split_threshold_vectors: 2000
merge_threshold_vectors: 500
split_search_neighbourhood: 16
max_pending_and_running_rebalance_tasks: 16
rebalance_backpressure_resume_threshold: 8
max_rebalance_tasks: 8
chunk_target: 4096
metadata_fields: []
"#,
        )
        .unwrap();

        // when
        let config = load_vector_config(config_path.to_str().unwrap());

        // then
        assert!(matches!(config.storage, StorageConfig::InMemory));
        assert_eq!(config.dimensions, 384);
    }

    #[test]
    fn should_load_reader_config_from_yaml() {
        // given
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("reader.yaml");
        std::fs::write(
            &config_path,
            r#"
storage:
  type: InMemory
dimensions: 384
distance_metric: L2
metadata_fields: []
"#,
        )
        .unwrap();

        // when
        let config = load_reader_config(config_path.to_str().unwrap());

        // then
        assert!(matches!(config.storage, StorageConfig::InMemory));
        assert_eq!(config.dimensions, 384);
    }
}

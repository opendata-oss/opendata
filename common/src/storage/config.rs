//! Storage configuration types.
//!
//! This module provides configuration structures for different storage backends,
//! allowing services to configure storage type (InMemory or SlateDB) via config files
//! or environment variables.

use serde::{Deserialize, Serialize};

/// Top-level storage configuration.
///
/// Defaults to `SlateDb` with a local `/tmp/opendata-storage` directory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum StorageConfig {
    InMemory,
    SlateDb(SlateDbStorageConfig),
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: ".data".to_string(),
            }),
            settings_path: None,
        })
    }
}

/// SlateDB-specific configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlateDbStorageConfig {
    /// Path prefix for SlateDB data in the object store.
    pub path: String,

    /// Object store provider configuration.
    pub object_store: ObjectStoreConfig,

    /// Optional path to SlateDB settings file (TOML/YAML/JSON).
    ///
    /// If not provided, uses SlateDB's `Settings::load()` which checks for
    /// `SlateDb.toml`, `SlateDb.json`, `SlateDb.yaml` in the working directory
    /// and merges any `SLATEDB_` prefixed environment variables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings_path: Option<String>,
}

impl Default for SlateDbStorageConfig {
    fn default() -> Self {
        Self {
            path: "data".to_string(),
            object_store: ObjectStoreConfig::default(),
            settings_path: None,
        }
    }
}

/// Object store provider configuration for SlateDB.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    /// In-memory object store (useful for testing and development).
    #[default]
    InMemory,

    /// AWS S3 object store.
    Aws(AwsObjectStoreConfig),

    /// Local filesystem object store.
    Local(LocalObjectStoreConfig),
}

/// AWS S3 object store configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AwsObjectStoreConfig {
    /// AWS region (e.g., "us-west-2").
    pub region: String,

    /// S3 bucket name.
    pub bucket: String,
}

/// Local filesystem object store configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalObjectStoreConfig {
    /// Path to the local directory for storage.
    pub path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_default_to_slatedb_with_local_data_dir() {
        // given/when
        let config = StorageConfig::default();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Local(LocalObjectStoreConfig {
                        path: ".data".to_string()
                    })
                );
            }
            _ => panic!("Expected SlateDb config as default"),
        }
    }

    #[test]
    fn should_deserialize_in_memory_config() {
        // given
        let yaml = r#"type: InMemory"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        assert_eq!(config, StorageConfig::InMemory);
    }

    #[test]
    fn should_deserialize_slatedb_config_with_local_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: my-data
object_store:
  type: Local
  path: /tmp/slatedb
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "my-data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Local(LocalObjectStoreConfig {
                        path: "/tmp/slatedb".to_string()
                    })
                );
                assert!(slate_config.settings_path.is_none());
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_slatedb_config_with_aws_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: my-data
object_store:
  type: Aws
  region: us-west-2
  bucket: my-bucket
settings_path: slatedb.toml
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "my-data");
                assert_eq!(
                    slate_config.object_store,
                    ObjectStoreConfig::Aws(AwsObjectStoreConfig {
                        region: "us-west-2".to_string(),
                        bucket: "my-bucket".to_string()
                    })
                );
                assert_eq!(slate_config.settings_path, Some("slatedb.toml".to_string()));
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_deserialize_slatedb_config_with_in_memory_object_store() {
        // given
        let yaml = r#"
type: SlateDb
path: test-data
object_store:
  type: InMemory
"#;

        // when
        let config: StorageConfig = serde_yaml::from_str(yaml).unwrap();

        // then
        match config {
            StorageConfig::SlateDb(slate_config) => {
                assert_eq!(slate_config.path, "test-data");
                assert_eq!(slate_config.object_store, ObjectStoreConfig::InMemory);
            }
            _ => panic!("Expected SlateDb config"),
        }
    }

    #[test]
    fn should_serialize_slatedb_config() {
        // given
        let config = StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "my-data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: "/tmp/slatedb".to_string(),
            }),
            settings_path: None,
        });

        // when
        let yaml = serde_yaml::to_string(&config).unwrap();

        // then
        assert!(yaml.contains("type: SlateDb"));
        assert!(yaml.contains("path: my-data"));
        assert!(yaml.contains("type: Local"));
        // settings_path should be omitted when None
        assert!(!yaml.contains("settings_path"));
    }
}

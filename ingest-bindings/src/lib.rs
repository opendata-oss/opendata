use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::StorageConfig;
use common::clock::SystemClock;
use common::storage::config::{AwsObjectStoreConfig, LocalObjectStoreConfig, ObjectStoreConfig};
use ingest::BatchCompression;

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum IngestBindingError {
    #[error("Storage error: {message}")]
    Storage { message: String },
    #[error("Invalid input: {message}")]
    InvalidInput { message: String },
}

impl From<ingest::Error> for IngestBindingError {
    fn from(e: ingest::Error) -> Self {
        match e {
            ingest::Error::InvalidInput(msg) => IngestBindingError::InvalidInput { message: msg },
            other => IngestBindingError::Storage {
                message: other.to_string(),
            },
        }
    }
}

#[derive(uniffi::Enum)]
pub enum Compression {
    None,
    Zstd,
}

#[derive(uniffi::Enum)]
pub enum StorageBackend {
    InMemory,
    Local { path: String },
    Aws { bucket: String, region: String },
}

#[derive(uniffi::Record)]
pub struct IngestorConfig {
    pub storage_backend: StorageBackend,
    pub data_path_prefix: String,
    pub manifest_path: String,
    pub flush_interval_ms: u64,
    pub flush_size_bytes: u64,
    pub max_buffered_inputs: u64,
    pub compression: Compression,
}

#[derive(uniffi::Object)]
pub struct Ingestor {
    inner: Mutex<Option<ingest::Ingestor>>,
    runtime: tokio::runtime::Runtime,
}

impl Ingestor {
    fn with_inner<F, T>(&self, f: F) -> Result<T, IngestBindingError>
    where
        F: FnOnce(&ingest::Ingestor) -> Result<T, IngestBindingError>,
    {
        let guard = self.inner.lock().unwrap();
        let ingestor = guard
            .as_ref()
            .ok_or_else(|| IngestBindingError::InvalidInput {
                message: "ingestor is closed".to_string(),
            })?;
        f(ingestor)
    }
}

#[uniffi::export]
impl Ingestor {
    #[uniffi::constructor]
    pub fn new(config: IngestorConfig) -> Result<Arc<Self>, IngestBindingError> {
        let storage = match config.storage_backend {
            StorageBackend::InMemory => StorageConfig::InMemory,
            StorageBackend::Local { path } => {
                StorageConfig::SlateDb(common::storage::config::SlateDbStorageConfig {
                    path: "ingest".to_string(),
                    object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig { path }),
                    settings_path: None,
                    block_cache: None,
                })
            }
            StorageBackend::Aws { bucket, region } => {
                StorageConfig::SlateDb(common::storage::config::SlateDbStorageConfig {
                    path: "ingest".to_string(),
                    object_store: ObjectStoreConfig::Aws(AwsObjectStoreConfig { bucket, region }),
                    settings_path: None,
                    block_cache: None,
                })
            }
        };

        let batch_compression = match config.compression {
            Compression::None => BatchCompression::None,
            Compression::Zstd => BatchCompression::Zstd,
        };

        let ingestor_config = ingest::IngestorConfig {
            storage,
            data_path_prefix: config.data_path_prefix,
            manifest_path: config.manifest_path,
            flush_interval: Duration::from_millis(config.flush_interval_ms),
            flush_size_bytes: config.flush_size_bytes as usize,
            max_buffered_inputs: config.max_buffered_inputs as usize,
            batch_compression,
        };

        let runtime = tokio::runtime::Runtime::new().map_err(|e| IngestBindingError::Storage {
            message: format!("failed to create tokio runtime: {e}"),
        })?;

        let _guard = runtime.enter();
        let inner = ingest::Ingestor::new(ingestor_config, Arc::new(SystemClock))
            .map_err(IngestBindingError::from)?;

        Ok(Arc::new(Self {
            inner: Mutex::new(Some(inner)),
            runtime,
        }))
    }

    pub fn ingest(&self, payload: Vec<u8>, metadata: Vec<u8>) -> Result<(), IngestBindingError> {
        self.with_inner(|ingestor| {
            self.runtime.block_on(async {
                ingestor
                    .ingest(vec![Bytes::from(payload)], Bytes::from(metadata))
                    .await
                    .map_err(IngestBindingError::from)?
                    .watcher
                    .await_durable()
                    .await
                    .map_err(IngestBindingError::from)
            })
        })
    }

    pub fn ingest_many(
        &self,
        payloads: Vec<Vec<u8>>,
        metadata: Vec<u8>,
    ) -> Result<(), IngestBindingError> {
        let entries: Vec<Bytes> = payloads.into_iter().map(Bytes::from).collect();
        self.with_inner(|ingestor| {
            self.runtime.block_on(async {
                ingestor
                    .ingest(entries, Bytes::from(metadata))
                    .await
                    .map_err(IngestBindingError::from)?
                    .watcher
                    .await_durable()
                    .await
                    .map_err(IngestBindingError::from)
            })
        })
    }

    pub fn flush(&self) -> Result<(), IngestBindingError> {
        self.with_inner(|ingestor| {
            self.runtime
                .block_on(ingestor.flush())
                .map_err(IngestBindingError::from)
        })
    }

    pub fn close(&self) -> Result<(), IngestBindingError> {
        let ingestor =
            self.inner
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| IngestBindingError::InvalidInput {
                    message: "ingestor already closed".to_string(),
                })?;
        self.runtime
            .block_on(ingestor.close())
            .map_err(IngestBindingError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> IngestorConfig {
        IngestorConfig {
            storage_backend: StorageBackend::InMemory,
            data_path_prefix: "test-bindings".to_string(),
            manifest_path: "test-bindings/manifest".to_string(),
            flush_interval_ms: 50,
            flush_size_bytes: 64 * 1024 * 1024,
            max_buffered_inputs: 1000,
            compression: Compression::None,
        }
    }

    #[test]
    fn should_ingest_and_flush_single_payload() {
        let ingestor = Ingestor::new(test_config()).unwrap();
        ingestor
            .ingest(b"hello".to_vec(), vec![1, 1, 1, 0])
            .unwrap();
    }

    #[test]
    fn should_ingest_many_payloads() {
        let ingestor = Ingestor::new(test_config()).unwrap();
        ingestor
            .ingest_many(
                vec![b"entry1".to_vec(), b"entry2".to_vec()],
                vec![1, 1, 1, 0],
            )
            .unwrap();
    }

    #[test]
    fn should_flush_explicitly() {
        let ingestor = Ingestor::new(test_config()).unwrap();
        ingestor.ingest(b"data".to_vec(), vec![]).unwrap();
        ingestor.flush().unwrap();
    }

    #[test]
    fn should_create_with_zstd_compression() {
        let mut config = test_config();
        config.compression = Compression::Zstd;
        let ingestor = Ingestor::new(config).unwrap();
        ingestor.ingest(b"compressed".to_vec(), vec![]).unwrap();
    }

    #[test]
    fn should_close_cleanly() {
        let ingestor = Ingestor::new(test_config()).unwrap();
        ingestor.ingest(b"data".to_vec(), vec![]).unwrap();
        ingestor.close().unwrap();
    }

    #[test]
    fn should_reject_operations_after_close() {
        let ingestor = Ingestor::new(test_config()).unwrap();
        ingestor.close().unwrap();
        let result = ingestor.ingest(b"data".to_vec(), vec![]);
        assert!(matches!(
            result,
            Err(IngestBindingError::InvalidInput { .. })
        ));
    }

    #[test]
    fn should_reject_double_close() {
        let ingestor = Ingestor::new(test_config()).unwrap();
        ingestor.close().unwrap();
        let result = ingestor.close();
        assert!(matches!(
            result,
            Err(IngestBindingError::InvalidInput { .. })
        ));
    }
}

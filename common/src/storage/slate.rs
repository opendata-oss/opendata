use std::sync::Arc;

use crate::{
    BytesRange, Record, StorageError, StorageIterator, StorageRead, StorageResult,
    storage::{MergeOperator, RecordOp, Storage, StorageSnapshot, WriteOptions},
};
use async_trait::async_trait;
use bytes::Bytes;
use slatedb::config::ScanOptions;
use slatedb::{
    Db, DbIterator, DbReader, DbSnapshot, MergeOperator as SlateDbMergeOperator,
    MergeOperatorError, WriteBatch, config::WriteOptions as SlateDbWriteOptions,
};

/// Thin wrapper that exposes a SlateDB [`ReadableStat`] as a Prometheus gauge.
///
/// Instead of snapshotting stats into an intermediate representation and
/// refreshing gauges, this reads the live atomic value on each encode/scrape.
#[cfg(feature = "metrics")]
#[derive(Debug)]
struct ReadableStatGauge(std::sync::Arc<dyn slatedb::stats::ReadableStat>);

#[cfg(feature = "metrics")]
impl prometheus_client::encoding::EncodeMetric for ReadableStatGauge {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::MetricEncoder,
    ) -> Result<(), std::fmt::Error> {
        encoder.encode_gauge(&self.0.get())
    }

    fn metric_type(&self) -> prometheus_client::metrics::MetricType {
        prometheus_client::metrics::MetricType::Gauge
    }
}

/// Adapter that wraps our `MergeOperator` trait to implement SlateDB's `MergeOperator` trait.
///
/// This allows using our common merge operator interface with SlateDB's merge functionality.
pub struct SlateDbMergeOperatorAdapter {
    operator: Arc<dyn MergeOperator>,
}

impl SlateDbMergeOperatorAdapter {
    fn new(operator: Arc<dyn MergeOperator>) -> Self {
        Self { operator }
    }
}

impl SlateDbMergeOperator for SlateDbMergeOperatorAdapter {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        Ok(self.operator.merge(key, existing_value, value))
    }
}

/// Returns the default scan options used for storage scans.
fn default_scan_options() -> ScanOptions {
    ScanOptions {
        durability_filter: Default::default(),
        dirty: false,
        read_ahead_bytes: 1024 * 1024,
        cache_blocks: true,
        max_fetch_tasks: 4,
    }
}

/// SlateDB-backed implementation of the Storage trait.
///
/// SlateDB is an embedded key-value store built on object storage, providing
/// LSM-tree semantics with cloud-native durability.
pub struct SlateDbStorage {
    pub(super) db: Arc<Db>,
}

impl SlateDbStorage {
    /// Creates a new SlateDbStorage instance wrapping the given SlateDB database.
    pub fn new(db: Arc<Db>) -> Self {
        Self { db }
    }

    /// Creates a SlateDB `MergeOperator` from our common `MergeOperator` trait.
    ///
    /// This adapter can be used when constructing a SlateDB database with a merge operator:
    /// ```rust,ignore
    /// use common::storage::MergeOperator;
    /// use slatedb::{DbBuilder, object_store::ObjectStore};
    ///
    /// let my_merge_op: Arc<dyn MergeOperator> = Arc::new(MyMergeOperator);
    /// let slate_merge_op = SlateDbStorage::merge_operator_adapter(my_merge_op);
    ///
    /// let db = DbBuilder::new("path", object_store)
    ///     .with_merge_operator(Arc::new(slate_merge_op))
    ///     .build()
    ///     .await?;
    /// ```
    pub fn merge_operator_adapter(operator: Arc<dyn MergeOperator>) -> SlateDbMergeOperatorAdapter {
        SlateDbMergeOperatorAdapter::new(operator)
    }
}

#[async_trait]
impl StorageRead for SlateDbStorage {
    /// Retrieves a single record by key from SlateDB.
    ///
    /// Returns `None` if the key does not exist.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .db
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .db
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

pub(super) struct SlateDbIterator {
    iter: DbIterator,
}

#[async_trait]
impl StorageIterator for SlateDbIterator {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        match self.iter.next().await.map_err(StorageError::from_storage)? {
            Some(entry) => Ok(Some(Record::new(entry.key, entry.value))),
            None => Ok(None),
        }
    }
}

/// SlateDB snapshot wrapper that implements StorageSnapshot.
///
/// Provides a consistent read-only view of the database at the time the snapshot was created.
pub struct SlateDbStorageSnapshot {
    snapshot: Arc<DbSnapshot>,
}

#[async_trait]
impl StorageRead for SlateDbStorageSnapshot {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .snapshot
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .snapshot
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

#[async_trait]
impl StorageSnapshot for SlateDbStorageSnapshot {}

#[async_trait]
impl Storage for SlateDbStorage {
    async fn apply(&self, records: Vec<RecordOp>) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        for op in records {
            match op {
                RecordOp::Put(record) => batch.put(record.key, record.value),
                RecordOp::Merge(record) => batch.merge(record.key, record.value),
                RecordOp::Delete(key) => batch.delete(key),
            }
        }
        self.db
            .write(batch)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(())
    }
    async fn put(&self, records: Vec<Record>) -> StorageResult<()> {
        self.put_with_options(records, WriteOptions::default())
            .await
    }

    async fn put_with_options(
        &self,
        records: Vec<Record>,
        options: WriteOptions,
    ) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        for record in records {
            batch.put(record.key, record.value);
        }
        let slate_options = SlateDbWriteOptions {
            await_durable: options.await_durable,
        };
        self.db
            .write_with_options(batch, &slate_options)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(())
    }

    /// Merges values for the given keys using SlateDB's merge operator.
    ///
    /// This method requires the database to be configured with a merge operator
    /// during construction. If no merge operator is configured, this will return
    /// a `StorageError::Storage` error.
    async fn merge(&self, records: Vec<Record>) -> StorageResult<()> {
        let mut batch = WriteBatch::new();
        for record in records {
            batch.merge(record.key, record.value);
        }
        self.db.write(batch).await.map_err(|e| {
            let error_msg = e.to_string();
            // Check if the error indicates merge operator is not configured
            if error_msg.contains("merge operator") || error_msg.contains("not configured") {
                StorageError::Storage("Merge operator not configured for this database".to_string())
            } else {
                StorageError::from_storage(e)
            }
        })?;
        Ok(())
    }

    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
        let snapshot = self
            .db
            .snapshot()
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Arc::new(SlateDbStorageSnapshot { snapshot }))
    }

    async fn flush(&self) -> StorageResult<()> {
        self.db.flush().await.map_err(StorageError::from_storage)?;
        Ok(())
    }

    async fn close(&self) -> StorageResult<()> {
        self.db.close().await.map_err(StorageError::from_storage)?;
        Ok(())
    }

    #[cfg(feature = "metrics")]
    fn register_metrics(&self, registry: &mut prometheus_client::registry::Registry) {
        let stat_registry = self.db.metrics();
        let mut seen = std::collections::HashSet::new();
        for name in stat_registry.names() {
            if let Some(stat) = stat_registry.lookup(name) {
                let sanitized: String = name
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '_' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect();
                let prom_name = format!("slatedb_{sanitized}");
                if !seen.insert(prom_name.clone()) {
                    tracing::warn!(
                        "Duplicate metric name after sanitization: {prom_name:?} (from {name:?}, skipped)"
                    );
                    continue;
                }
                registry.register(
                    &prom_name,
                    format!("SlateDB {name}"),
                    ReadableStatGauge(stat),
                );
            }
        }
    }
}

/// Read-only SlateDB storage using `DbReader`.
///
/// This struct provides read-only access to a SlateDB database without fencing,
/// allowing multiple readers to coexist with a single writer.
pub struct SlateDbStorageReader {
    reader: Arc<DbReader>,
}

impl SlateDbStorageReader {
    /// Creates a new SlateDbStorageReader wrapping the given DbReader.
    pub fn new(reader: Arc<DbReader>) -> Self {
        Self { reader }
    }
}

#[async_trait]
impl StorageRead for SlateDbStorageReader {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let value = self
            .reader
            .get(&key)
            .await
            .map_err(StorageError::from_storage)?;

        match value {
            Some(v) => Ok(Some(Record::new(key, v))),
            None => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let iter = self
            .reader
            .scan_with_options(range, &default_scan_options())
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BytesRange;
    use slatedb::DbBuilder;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn should_read_data_written_by_storage_via_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        // Create writer and write data
        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")),
                Record::new(Bytes::from("key2"), Bytes::from("value2")),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        // Create reader and verify data
        let reader = DbReader::open(path, object_store, None, Default::default())
            .await
            .unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

        let record = storage_reader.get(Bytes::from("key1")).await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, Bytes::from("value1"));

        let record = storage_reader.get(Bytes::from("key2")).await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, Bytes::from("value2"));

        let record = storage_reader.get(Bytes::from("key3")).await.unwrap();
        assert!(record.is_none());

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_scan_data_written_by_storage_via_reader() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        // Create writer and write data
        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        storage
            .put(vec![
                Record::new(Bytes::from("a"), Bytes::from("1")),
                Record::new(Bytes::from("b"), Bytes::from("2")),
                Record::new(Bytes::from("c"), Bytes::from("3")),
            ])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        // Create reader and scan data
        let reader = DbReader::open(path, object_store, None, Default::default())
            .await
            .unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

        let mut iter = storage_reader
            .scan_iter(BytesRange::unbounded())
            .await
            .unwrap();
        let mut results = Vec::new();
        while let Some(record) = iter.next().await.unwrap() {
            results.push((record.key, record.value));
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (Bytes::from("a"), Bytes::from("1")));
        assert_eq!(results[1], (Bytes::from("b"), Bytes::from("2")));
        assert_eq!(results[2], (Bytes::from("c"), Bytes::from("3")));

        storage.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_coexist_writer_and_reader_without_fencing_error() {
        let object_store = Arc::new(InMemory::new());
        let path = "/test/db";

        // Create writer
        let db = DbBuilder::new(path, object_store.clone())
            .build()
            .await
            .unwrap();
        let storage = SlateDbStorage::new(Arc::new(db));

        // Write initial data
        storage
            .put(vec![Record::new(
                Bytes::from("key1"),
                Bytes::from("value1"),
            )])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        // Create reader while writer is still open - this should NOT cause fencing error
        let reader = DbReader::open(path, object_store, None, Default::default())
            .await
            .unwrap();
        let storage_reader = SlateDbStorageReader::new(Arc::new(reader));

        // Reader can read the data
        let record = storage_reader.get(Bytes::from("key1")).await.unwrap();
        assert!(record.is_some());
        assert_eq!(record.unwrap().value, Bytes::from("value1"));

        // Writer can still write more data
        storage
            .put(vec![Record::new(
                Bytes::from("key2"),
                Bytes::from("value2"),
            )])
            .await
            .unwrap();
        storage.flush().await.unwrap();

        storage.close().await.unwrap();
    }
}

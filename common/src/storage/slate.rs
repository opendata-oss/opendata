use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use slatedb::{
    Db, DbIterator, DbSnapshot, MergeOperator as SlateDbMergeOperator, MergeOperatorError,
    WriteBatch, config::WriteOptions as SlateDbWriteOptions,
};

use crate::{
    BytesRange, Record, StorageError, StorageIterator, StorageRead, StorageResult,
    storage::{MergeOperator, RecordOp, Storage, StorageSnapshot, WriteOptions},
};

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
            .scan(range)
            .await
            .map_err(StorageError::from_storage)?;
        Ok(Box::new(SlateDbIterator { iter }))
    }
}

struct SlateDbIterator {
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
            .scan(range)
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
}

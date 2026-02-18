use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;

use super::{MergeOperator, MergeRecordOp, PutRecordOp, Storage, StorageSnapshot, WriteOptions};
use crate::storage::RecordOp;
use crate::{BytesRange, Record, StorageError, StorageIterator, StorageRead, StorageResult, Ttl};

/// Trait for providing the current time.
pub trait Clock: Send + Sync {
    /// Returns the current time as milliseconds since the Unix epoch.
    fn now(&self) -> i64;
}

/// Clock implementation that returns the real system time.
pub struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_millis() as i64
    }
}

/// Internal wrapper that stores a value alongside its expiration timestamp.
#[derive(Clone, Debug)]
struct StoredValue {
    value: Bytes,
    /// `None` means the value never expires.
    expire_ts: Option<i64>,
}

impl StoredValue {
    fn is_expired(&self, now: i64) -> bool {
        self.expire_ts.is_some_and(|ts| now >= ts)
    }
}

/// Computes the absolute expiration timestamp from TTL options.
fn compute_expire_ts(now: i64, ttl: Ttl, default_ttl: Option<u64>) -> Option<i64> {
    let duration = match ttl {
        Ttl::Default => default_ttl,
        Ttl::NoExpiry => None,
        Ttl::ExpireAfter(ms) => Some(ms),
    };
    duration.map(|ms| now + ms as i64)
}

/// In-memory implementation of the Storage trait using a BTreeMap.
///
/// This implementation stores all data in memory and is useful for testing
/// or scenarios where durability is not required. Supports TTL-based
/// expiration via a configurable [`Clock`].
pub struct InMemoryStorage {
    data: Arc<RwLock<BTreeMap<Bytes, StoredValue>>>,
    merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>,
    clock: Arc<dyn Clock>,
    default_ttl: Option<u64>,
}

impl InMemoryStorage {
    /// Creates a new InMemoryStorage instance with an empty store.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            merge_operator: None,
            clock: Arc::new(WallClock),
            default_ttl: None,
        }
    }

    /// Creates a new InMemoryStorage instance with an optional merge operator.
    ///
    /// If a merge operator is provided, the `merge` method will use it to combine
    /// existing values with new values. If no merge operator is provided, the
    /// `merge` method will return an error.
    pub fn with_merge_operator(merge_operator: Arc<dyn MergeOperator + Send + Sync>) -> Self {
        Self {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            merge_operator: Some(merge_operator),
            clock: Arc::new(WallClock),
            default_ttl: None,
        }
    }

    /// Sets a custom clock for TTL expiration checks.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Sets the default TTL (in milliseconds) for records written with [`Ttl::Default`].
    pub fn with_default_ttl(mut self, ttl: u64) -> Self {
        self.default_ttl = Some(ttl);
        self
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageRead for InMemoryStorage {
    /// Retrieves a single record by key from the in-memory store.
    ///
    /// Returns `None` if the key does not exist or has expired.
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        match data.get(&key) {
            Some(stored) if !stored.is_expired(self.clock.now()) => {
                Ok(Some(Record::new(key, stored.value.clone())))
            }
            _ => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        let now = self.clock.now();
        let records: Vec<Record> = data
            .range((range.start_bound().cloned(), range.end_bound().cloned()))
            .filter(|(_, stored)| !stored.is_expired(now))
            .map(|(k, stored)| Record::new(k.clone(), stored.value.clone()))
            .collect();

        Ok(Box::new(InMemoryIterator { records, index: 0 }))
    }
}

struct InMemoryIterator {
    records: Vec<Record>,
    index: usize,
}

#[async_trait]
impl StorageIterator for InMemoryIterator {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn next(&mut self) -> StorageResult<Option<Record>> {
        if self.index >= self.records.len() {
            Ok(None)
        } else {
            let record = self.records[self.index].clone();
            self.index += 1;
            Ok(Some(record))
        }
    }
}

/// In-memory snapshot that holds a copy of the data at the time of snapshot creation.
///
/// Provides a consistent read-only view of the database at the time the snapshot was created.
/// Expired entries are filtered out at read time using the shared clock.
pub struct InMemoryStorageSnapshot {
    data: Arc<BTreeMap<Bytes, StoredValue>>,
    clock: Arc<dyn Clock>,
}

#[async_trait]
impl StorageRead for InMemoryStorageSnapshot {
    #[tracing::instrument(level = "trace", skip_all)]
    async fn get(&self, key: Bytes) -> StorageResult<Option<Record>> {
        match self.data.get(&key) {
            Some(stored) if !stored.is_expired(self.clock.now()) => {
                Ok(Some(Record::new(key, stored.value.clone())))
            }
            _ => Ok(None),
        }
    }

    #[tracing::instrument(level = "trace", skip_all)]
    async fn scan_iter(
        &self,
        range: BytesRange,
    ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
        let now = self.clock.now();
        let records: Vec<Record> = self
            .data
            .range((range.start_bound().cloned(), range.end_bound().cloned()))
            .filter(|(_, stored)| !stored.is_expired(now))
            .map(|(k, stored)| Record::new(k.clone(), stored.value.clone()))
            .collect();

        Ok(Box::new(InMemoryIterator { records, index: 0 }))
    }
}

#[async_trait]
impl StorageSnapshot for InMemoryStorageSnapshot {}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn apply(&self, records: Vec<RecordOp>) -> StorageResult<()> {
        let mut data = self
            .data
            .write()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire write lock: {}", e)))?;

        let now = self.clock.now();
        for record in records {
            match record {
                RecordOp::Put(op) => {
                    let expire_ts = compute_expire_ts(now, op.options.ttl, self.default_ttl);
                    data.insert(
                        op.record.key,
                        StoredValue {
                            value: op.record.value,
                            expire_ts,
                        },
                    );
                }
                RecordOp::Merge(op) => {
                    let existing_value = data
                        .get(&op.record.key)
                        .filter(|s| !s.is_expired(now))
                        .map(|s| s.value.clone());
                    let merged_value = self.merge_operator.as_ref().unwrap().merge(
                        &op.record.key,
                        existing_value,
                        op.record.value.clone(),
                    );
                    let expire_ts = compute_expire_ts(now, op.options.ttl, self.default_ttl);
                    data.insert(
                        op.record.key,
                        StoredValue {
                            value: merged_value,
                            expire_ts,
                        },
                    );
                }
                RecordOp::Delete(key) => {
                    data.remove(&key);
                }
            }
        }

        Ok(())
    }

    /// Writes a batch of records to the in-memory store with default options.
    ///
    /// Delegates to [`put_with_options`](Self::put_with_options) with default options.
    async fn put(&self, records: Vec<PutRecordOp>) -> StorageResult<()> {
        self.put_with_options(records, WriteOptions::default())
            .await
    }

    /// Writes a batch of records to the in-memory store.
    ///
    /// All records are written atomically within a single write lock acquisition.
    /// For in-memory storage, write options are ignored since there is no
    /// durable storage to await.
    async fn put_with_options(
        &self,
        records: Vec<PutRecordOp>,
        _options: WriteOptions,
    ) -> StorageResult<()> {
        let mut data = self
            .data
            .write()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire write lock: {}", e)))?;

        let now = self.clock.now();
        for op in records {
            let expire_ts = compute_expire_ts(now, op.options.ttl, self.default_ttl);
            data.insert(
                op.record.key,
                StoredValue {
                    value: op.record.value,
                    expire_ts,
                },
            );
        }

        Ok(())
    }

    /// Merges values for the given keys using the configured merge operator.
    ///
    /// This method requires a merge operator to be configured during construction.
    /// For each record, it will:
    /// 1. Get the existing value (if any), excluding expired entries
    /// 2. Call the merge operator to combine existing and new values
    /// 3. Put the merged result back with the new TTL
    ///
    /// If no merge operator is configured, this method will return a
    /// `StorageError::Storage` error.
    async fn merge(&self, records: Vec<MergeRecordOp>) -> StorageResult<()> {
        let merge_op = self
            .merge_operator
            .as_ref()
            .ok_or_else(|| {
                StorageError::Storage(
                    "Merge operator not configured: in-memory storage requires a merge operator to be set during construction".to_string(),
                )
            })?;

        let mut data = self
            .data
            .write()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire write lock: {}", e)))?;

        let now = self.clock.now();
        for op in records {
            let existing_value = data
                .get(&op.record.key)
                .filter(|s| !s.is_expired(now))
                .map(|s| s.value.clone());
            let merged_value =
                merge_op.merge(&op.record.key, existing_value, op.record.value.clone());
            let expire_ts = compute_expire_ts(now, op.options.ttl, self.default_ttl);
            data.insert(
                op.record.key,
                StoredValue {
                    value: merged_value,
                    expire_ts,
                },
            );
        }

        Ok(())
    }

    /// Creates a point-in-time snapshot of the in-memory storage.
    ///
    /// The snapshot provides a consistent read-only view of the database at the time
    /// the snapshot was created. Reads from the snapshot will not see any subsequent
    /// writes to the underlying storage. Expired entries are filtered at read time.
    async fn snapshot(&self) -> StorageResult<Arc<dyn StorageSnapshot>> {
        let data = self
            .data
            .read()
            .map_err(|e| StorageError::Internal(format!("Failed to acquire read lock: {}", e)))?;

        let snapshot_data = Arc::new(data.clone());

        Ok(Arc::new(InMemoryStorageSnapshot {
            data: snapshot_data,
            clock: self.clock.clone(),
        }))
    }

    async fn flush(&self) -> StorageResult<()> {
        // No-op for in-memory storage - all writes are immediately visible
        Ok(())
    }

    async fn close(&self) -> StorageResult<()> {
        // No-op for in-memory storage
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use std::ops::Bound;

    /// Test merge operator that appends new value to existing value with a separator.
    struct AppendMergeOperator;

    impl MergeOperator for AppendMergeOperator {
        fn merge(&self, _key: &Bytes, existing_value: Option<Bytes>, new_value: Bytes) -> Bytes {
            match existing_value {
                Some(existing) => {
                    let mut result = BytesMut::from(existing);
                    result.extend_from_slice(b",");
                    result.extend_from_slice(&new_value);
                    result.freeze()
                }
                None => new_value,
            }
        }
    }

    #[tokio::test]
    async fn should_return_none_when_key_not_found() {
        // given
        let storage = InMemoryStorage::new();

        // when
        let result = storage.get(Bytes::from("missing_key")).await;

        // then
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn should_store_and_retrieve_record() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        // when
        storage
            .put(vec![Record::new(key.clone(), value.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        let record = result.unwrap();
        assert_eq!(record.key, Bytes::from("test_key"));
        assert_eq!(record.value, value);
    }

    #[tokio::test]
    async fn should_overwrite_existing_key() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("test_key");
        let initial_value = Bytes::from("initial_value");
        let updated_value = Bytes::from("updated_value");

        // when
        storage
            .put(vec![Record::new(key.clone(), initial_value).into()])
            .await
            .unwrap();
        storage
            .put(vec![Record::new(key.clone(), updated_value.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, updated_value);
    }

    #[tokio::test]
    async fn should_store_multiple_records() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")),
            Record::new(Bytes::from("key2"), Bytes::from("value2")),
            Record::new(Bytes::from("key3"), Bytes::from("value3")),
        ];

        // when
        storage
            .put(records.iter().cloned().map(PutRecordOp::new).collect())
            .await
            .unwrap();

        // then
        for record in records {
            let retrieved = storage.get(record.key.clone()).await.unwrap();
            assert!(retrieved.is_some());
            assert_eq!(retrieved.unwrap().value, record.value);
        }
    }

    #[tokio::test]
    async fn should_scan_all_records_when_unbounded() {
        // given
        let storage = InMemoryStorage::new();
        let records = [
            Record::new(Bytes::from("a"), Bytes::from("value_a")),
            Record::new(Bytes::from("b"), Bytes::from("value_b")),
            Record::new(Bytes::from("c"), Bytes::from("value_c")),
        ];
        storage
            .put(records.iter().cloned().map(PutRecordOp::new).collect())
            .await
            .unwrap();

        // when
        let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();

        // then
        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[0].key, Bytes::from("a"));
        assert_eq!(scanned[1].key, Bytes::from("b"));
        assert_eq!(scanned[2].key, Bytes::from("c"));
    }

    #[tokio::test]
    async fn should_scan_records_with_prefix() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("prefix_a"), Bytes::from("value1")),
            Record::new(Bytes::from("prefix_b"), Bytes::from("value2")),
            Record::new(Bytes::from("other_c"), Bytes::from("value3")),
        ];
        storage
            .put(records.into_iter().map(PutRecordOp::new).collect())
            .await
            .unwrap();

        // when
        let scanned = storage
            .scan(BytesRange::prefix(Bytes::from("prefix_")))
            .await
            .unwrap();

        // then
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].key, Bytes::from("prefix_a"));
        assert_eq!(scanned[1].key, Bytes::from("prefix_b"));
    }

    #[tokio::test]
    async fn should_scan_records_in_bounded_range() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("a"), Bytes::from("value_a")),
            Record::new(Bytes::from("b"), Bytes::from("value_b")),
            Record::new(Bytes::from("c"), Bytes::from("value_c")),
            Record::new(Bytes::from("d"), Bytes::from("value_d")),
        ];
        storage
            .put(records.into_iter().map(PutRecordOp::new).collect())
            .await
            .unwrap();

        // when
        let range = BytesRange::new(
            Bound::Included(Bytes::from("b")),
            Bound::Excluded(Bytes::from("d")),
        );
        let scanned = storage.scan(range).await.unwrap();

        // then
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].key, Bytes::from("b"));
        assert_eq!(scanned[1].key, Bytes::from("c"));
    }

    #[tokio::test]
    async fn should_return_empty_vec_when_scanning_empty_storage() {
        // given
        let storage = InMemoryStorage::new();

        // when
        let scanned = storage.scan(BytesRange::unbounded()).await.unwrap();

        // then
        assert!(scanned.is_empty());
    }

    #[tokio::test]
    async fn should_iterate_over_records() {
        // given
        let storage = InMemoryStorage::new();
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
        ];
        storage.put(records).await.unwrap();

        // when
        let mut iter = storage.scan_iter(BytesRange::unbounded()).await.unwrap();
        let first = iter.next().await.unwrap();
        let second = iter.next().await.unwrap();
        let third = iter.next().await.unwrap();

        // then
        assert!(first.is_some());
        assert_eq!(first.unwrap().key, Bytes::from("key1"));
        assert!(second.is_some());
        assert_eq!(second.unwrap().key, Bytes::from("key2"));
        assert!(third.is_none());
    }

    #[tokio::test]
    async fn should_create_snapshot_with_current_data() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            ])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();

        // then
        let result = snapshot.get(Bytes::from("key1")).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("value1"));
    }

    #[tokio::test]
    async fn should_not_see_writes_after_snapshot() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            ])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();
        storage
            .put(vec![
                Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
            ])
            .await
            .unwrap();

        // then
        let snapshot_result = snapshot.get(Bytes::from("key2")).await.unwrap();
        assert!(snapshot_result.is_none());

        let storage_result = storage.get(Bytes::from("key2")).await.unwrap();
        assert!(storage_result.is_some());
    }

    #[tokio::test]
    async fn should_scan_snapshot_independently() {
        // given
        let storage = InMemoryStorage::new();
        storage
            .put(vec![
                Record::new(Bytes::from("a"), Bytes::from("value_a")).into(),
            ])
            .await
            .unwrap();

        // when
        let snapshot = storage.snapshot().await.unwrap();
        storage
            .put(vec![
                Record::new(Bytes::from("b"), Bytes::from("value_b")).into(),
            ])
            .await
            .unwrap();

        // then
        let snapshot_records = snapshot.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(snapshot_records.len(), 1);
        assert_eq!(snapshot_records[0].key, Bytes::from("a"));

        let storage_records = storage.scan(BytesRange::unbounded()).await.unwrap();
        assert_eq!(storage_records.len(), 2);
    }

    #[tokio::test]
    async fn should_handle_empty_record() {
        // given
        let storage = InMemoryStorage::new();
        let key = Bytes::from("empty_key");

        // when
        storage
            .put(vec![Record::empty(key.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::new());
    }

    #[tokio::test]
    async fn should_return_error_when_merge_operator_not_configured() {
        // given
        let storage = InMemoryStorage::new();
        let record = Record::new(Bytes::from("key1"), Bytes::from("value1"));

        // when
        let result = storage.merge(vec![record.into()]).await;

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Merge operator not configured")
        );
    }

    #[tokio::test]
    async fn should_merge_when_key_does_not_exist() {
        // given
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);
        let key = Bytes::from("new_key");
        let value = Bytes::from("value1");

        // when
        storage
            .merge(vec![Record::new(key.clone(), value.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, value);
    }

    #[tokio::test]
    async fn should_merge_when_key_exists() {
        // given
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);
        let key = Bytes::from("key1");
        let initial_value = Bytes::from("value1");
        let new_value = Bytes::from("value2");

        storage
            .put(vec![Record::new(key.clone(), initial_value).into()])
            .await
            .unwrap();

        // when
        storage
            .merge(vec![Record::new(key.clone(), new_value).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("value1,value2"));
    }

    #[tokio::test]
    async fn should_merge_multiple_keys() {
        // given
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);
        let records = vec![
            Record::new(Bytes::from("key1"), Bytes::from("value1")).into(),
            Record::new(Bytes::from("key2"), Bytes::from("value2")).into(),
        ];
        storage.put(records).await.unwrap();

        // when
        storage
            .merge(vec![
                Record::new(Bytes::from("key1"), Bytes::from("value1a")).into(),
                Record::new(Bytes::from("key2"), Bytes::from("value2a")).into(),
            ])
            .await
            .unwrap();

        // then
        let result1 = storage.get(Bytes::from("key1")).await.unwrap();
        assert_eq!(result1.unwrap().value, Bytes::from("value1,value1a"));

        let result2 = storage.get(Bytes::from("key2")).await.unwrap();
        assert_eq!(result2.unwrap().value, Bytes::from("value2,value2a"));
    }

    #[tokio::test]
    async fn should_merge_empty_values() {
        // given
        let merge_op = Arc::new(AppendMergeOperator);
        let storage = InMemoryStorage::with_merge_operator(merge_op);
        let key = Bytes::from("key1");

        // when
        storage
            .merge(vec![Record::empty(key.clone()).into()])
            .await
            .unwrap();
        let result = storage.get(key).await.unwrap();

        // then
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::new());
    }
}

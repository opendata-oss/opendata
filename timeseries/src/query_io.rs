//! Per-query I/O statistics collector.
//!
//! Uses a task-local collector to record physical bytes returned by
//! storage get/scan calls, attributed to the logical read kind
//! (bucket list, inverted index, forward index, sample point get, etc.).
//!
//! Install a collector at query entry with [`run_with_collector`],
//! then call [`record_get`] / [`record_scan`] at each storage return site.

use std::sync::Arc;
use std::sync::Mutex;

/// Physical I/O stats attributed by read kind.
/// All byte counts are `key.len() + value.len()` as returned by storage.
#[derive(Debug, Default, Clone)]
pub(crate) struct QueryIoStats {
    // Bucket list
    pub(crate) bucket_list_gets: u64,
    pub(crate) bucket_list_records: u64,
    pub(crate) bucket_list_bytes: u64,

    // Inverted index (partial term loads)
    pub(crate) inverted_index_gets: u64,
    pub(crate) inverted_index_scans: u64,
    pub(crate) inverted_index_records: u64,
    pub(crate) inverted_index_bytes: u64,

    // Forward index (partial series loads)
    pub(crate) forward_index_gets: u64,
    pub(crate) forward_index_scans: u64,
    pub(crate) forward_index_records: u64,
    pub(crate) forward_index_bytes: u64,

    // Sample point gets (legacy per-series key layout)
    pub(crate) sample_get_calls: u64,
    pub(crate) sample_get_records: u64,
    pub(crate) sample_get_bytes: u64,

    // Sample metric-prefixed gets
    pub(crate) sample_metric_get_calls: u64,
    pub(crate) sample_metric_get_records: u64,
    pub(crate) sample_metric_get_bytes: u64,
}

impl QueryIoStats {
    /// Total physical bytes returned by storage across all read kinds.
    pub(crate) fn physical_bytes_total(&self) -> u64 {
        self.bucket_list_bytes
            + self.inverted_index_bytes
            + self.forward_index_bytes
            + self.sample_get_bytes
            + self.sample_metric_get_bytes
    }

    /// Total metadata bytes (bucket list + inverted index + forward index).
    pub(crate) fn metadata_bytes(&self) -> u64 {
        self.bucket_list_bytes + self.inverted_index_bytes + self.forward_index_bytes
    }

    /// Total sample bytes (point gets + metric-prefixed gets).
    pub(crate) fn sample_bytes(&self) -> u64 {
        self.sample_get_bytes + self.sample_metric_get_bytes
    }
}

/// Which logical read kind the current storage call belongs to.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ReadKind {
    BucketList,
    InvertedIndex,
    ForwardIndex,
    SampleGet,
    SampleMetricGet,
}

/// Shared collector handle. Cloned into the task-local; the caller
/// keeps a handle to read stats back after the scope exits.
#[derive(Clone)]
pub(crate) struct QueryIoCollector {
    inner: Arc<Mutex<QueryIoStats>>,
}

tokio::task_local! {
    static QUERY_IO_COLLECTOR: QueryIoCollector;
}

impl QueryIoCollector {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(QueryIoStats::default())),
        }
    }

    /// Take a snapshot of the current stats.
    pub(crate) fn snapshot(&self) -> QueryIoStats {
        self.inner.lock().unwrap().clone()
    }

    fn record_get(&self, kind: ReadKind, record_bytes: u64) {
        let mut stats = self.inner.lock().unwrap();
        match kind {
            ReadKind::BucketList => {
                stats.bucket_list_gets += 1;
                if record_bytes > 0 {
                    stats.bucket_list_records += 1;
                }
                stats.bucket_list_bytes += record_bytes;
            }
            ReadKind::InvertedIndex => {
                stats.inverted_index_gets += 1;
                if record_bytes > 0 {
                    stats.inverted_index_records += 1;
                }
                stats.inverted_index_bytes += record_bytes;
            }
            ReadKind::ForwardIndex => {
                stats.forward_index_gets += 1;
                if record_bytes > 0 {
                    stats.forward_index_records += 1;
                }
                stats.forward_index_bytes += record_bytes;
            }
            ReadKind::SampleGet => {
                stats.sample_get_calls += 1;
                if record_bytes > 0 {
                    stats.sample_get_records += 1;
                }
                stats.sample_get_bytes += record_bytes;
            }
            ReadKind::SampleMetricGet => {
                stats.sample_metric_get_calls += 1;
                if record_bytes > 0 {
                    stats.sample_metric_get_records += 1;
                }
                stats.sample_metric_get_bytes += record_bytes;
            }
        }
    }

    fn record_scan(&self, kind: ReadKind, record_count: u64, total_bytes: u64) {
        let mut stats = self.inner.lock().unwrap();
        match kind {
            ReadKind::InvertedIndex => {
                stats.inverted_index_scans += 1;
                stats.inverted_index_records += record_count;
                stats.inverted_index_bytes += total_bytes;
            }
            ReadKind::ForwardIndex => {
                stats.forward_index_scans += 1;
                stats.forward_index_records += record_count;
                stats.forward_index_bytes += total_bytes;
            }
            _ => {
                // Other read kinds don't use scan on the current query path.
            }
        }
    }
}

/// Record a storage `get()` call into the active per-query collector (if any).
pub(crate) fn record_get(kind: ReadKind, record: &Option<common::Record>) {
    let _ = QUERY_IO_COLLECTOR.try_with(|collector| {
        let bytes = record
            .as_ref()
            .map(|r| (r.key.len() + r.value.len()) as u64)
            .unwrap_or(0);
        collector.record_get(kind, bytes);
    });
}

/// Record a storage `scan()` call into the active per-query collector (if any).
pub(crate) fn record_scan(kind: ReadKind, records: &[common::Record]) {
    let _ = QUERY_IO_COLLECTOR.try_with(|collector| {
        let total_bytes: u64 = records
            .iter()
            .map(|r| (r.key.len() + r.value.len()) as u64)
            .sum();
        collector.record_scan(kind, records.len() as u64, total_bytes);
    });
}

/// Run an async block with the given collector installed as a task-local.
pub(crate) async fn run_with_collector<F, T>(collector: &QueryIoCollector, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    QUERY_IO_COLLECTOR.scope(collector.clone(), f).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use common::Record;

    #[tokio::test]
    async fn should_record_get_bytes_into_collector() {
        let collector = QueryIoCollector::new();
        run_with_collector(&collector, async {
            let record = Some(Record::new(
                Bytes::from_static(b"key123"),
                Bytes::from_static(b"value-payload"),
            ));
            record_get(ReadKind::ForwardIndex, &record);
            record_get(ReadKind::ForwardIndex, &None);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.forward_index_gets, 2);
        assert_eq!(stats.forward_index_records, 1);
        assert_eq!(stats.forward_index_bytes, 6 + 13);
    }

    #[tokio::test]
    async fn should_record_scan_bytes_and_record_count() {
        let collector = QueryIoCollector::new();
        run_with_collector(&collector, async {
            let records = vec![
                Record::new(Bytes::from_static(b"k1"), Bytes::from_static(b"v1")),
                Record::new(Bytes::from_static(b"k2"), Bytes::from_static(b"v22")),
            ];
            record_scan(ReadKind::InvertedIndex, &records);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.inverted_index_scans, 1);
        assert_eq!(stats.inverted_index_records, 2);
        assert_eq!(stats.inverted_index_bytes, 2 + 2 + 2 + 3);
    }

    #[tokio::test]
    async fn should_not_panic_without_collector() {
        let record = Some(Record::new(
            Bytes::from_static(b"k"),
            Bytes::from_static(b"v"),
        ));
        record_get(ReadKind::BucketList, &record);
        record_scan(ReadKind::ForwardIndex, &[]);
    }

    #[tokio::test]
    async fn should_compute_derived_totals() {
        let collector = QueryIoCollector::new();
        run_with_collector(&collector, async {
            record_get(
                ReadKind::BucketList,
                &Some(Record::new(
                    Bytes::from_static(b"bl"),
                    Bytes::from(vec![0u8; 100]),
                )),
            );
            record_get(
                ReadKind::InvertedIndex,
                &Some(Record::new(
                    Bytes::from_static(b"ii"),
                    Bytes::from(vec![0u8; 200]),
                )),
            );
            record_get(
                ReadKind::ForwardIndex,
                &Some(Record::new(
                    Bytes::from_static(b"fi"),
                    Bytes::from(vec![0u8; 300]),
                )),
            );
            record_get(
                ReadKind::SampleGet,
                &Some(Record::new(
                    Bytes::from_static(b"sg"),
                    Bytes::from(vec![0u8; 1000]),
                )),
            );
            record_get(
                ReadKind::SampleMetricGet,
                &Some(Record::new(
                    Bytes::from_static(b"sm"),
                    Bytes::from(vec![0u8; 2000]),
                )),
            );
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.metadata_bytes(), 102 + 202 + 302);
        assert_eq!(stats.sample_bytes(), 1002 + 2002);
        assert_eq!(stats.physical_bytes_total(), 102 + 202 + 302 + 1002 + 2002);
    }
}

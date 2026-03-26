//! Per-query I/O statistics collector.
//!
//! Uses a task-local collector to record physical bytes returned by
//! storage get/scan calls, attributed to the logical read kind
//! (bucket list, inverted index, forward index, sample point get, etc.).
//!
//! Also records object-store level stats (get/head/list calls and bytes)
//! to enable comparison between storage-returned bytes and actual
//! object-store traffic.
//!
//! Install a collector at query entry with [`run_with_collector`],
//! then call [`record_get`] / [`record_scan`] at each storage return site.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::Record;

// ─── Storage-layer stats ────────────────────────────────────────────────────

/// Physical I/O stats attributed by read kind.
/// All byte counts are `key.len() + value.len()` as returned by storage.
#[derive(Debug, Default, Clone)]
pub struct QueryIoStats {
    // Bucket list
    pub bucket_list_gets: u64,
    pub bucket_list_records: u64,
    pub bucket_list_bytes: u64,

    // Inverted index (partial term loads)
    pub inverted_index_gets: u64,
    pub inverted_index_scans: u64,
    pub inverted_index_records: u64,
    pub inverted_index_bytes: u64,

    // Forward index (partial series loads)
    pub forward_index_gets: u64,
    pub forward_index_scans: u64,
    pub forward_index_records: u64,
    pub forward_index_bytes: u64,

    // Sample point gets (legacy per-series key layout)
    pub sample_get_calls: u64,
    pub sample_get_records: u64,
    pub sample_get_bytes: u64,

    // Sample metric-prefixed gets
    pub sample_metric_get_calls: u64,
    pub sample_metric_get_records: u64,
    pub sample_metric_get_bytes: u64,

    // ─── Object-store stats ─────────────────────────────────────────────

    // get_opts with no range (full object reads)
    pub os_get_calls: u64,
    pub os_get_bytes: u64,
    pub os_get_errors: u64,

    // get_opts with range / get_range
    pub os_get_range_calls: u64,
    pub os_get_range_bytes: u64,
    pub os_get_range_errors: u64,

    // get_ranges (multi-range batch)
    pub os_get_ranges_calls: u64,
    pub os_get_ranges_bytes: u64,
    pub os_get_ranges_errors: u64,

    // head
    pub os_head_calls: u64,
    pub os_head_errors: u64,

    // list
    pub os_list_calls: u64,
    pub os_list_entries: u64,
    pub os_list_errors: u64,

    // list_with_delimiter
    pub os_list_with_delimiter_calls: u64,
    pub os_list_with_delimiter_entries: u64,
    pub os_list_with_delimiter_errors: u64,

    // Repeated-read signature tracking
    pub os_distinct_read_signatures: u64,
    pub os_repeated_read_calls: u64,
    pub os_max_repeats_single_signature: u64,
}

impl QueryIoStats {
    /// Total physical bytes returned by storage across all read kinds.
    pub fn physical_bytes_total(&self) -> u64 {
        self.bucket_list_bytes
            + self.inverted_index_bytes
            + self.forward_index_bytes
            + self.sample_get_bytes
            + self.sample_metric_get_bytes
    }

    /// Total metadata bytes (bucket list + inverted index + forward index).
    pub fn metadata_bytes(&self) -> u64 {
        self.bucket_list_bytes + self.inverted_index_bytes + self.forward_index_bytes
    }

    /// Total sample bytes (point gets + metric-prefixed gets).
    pub fn sample_bytes(&self) -> u64 {
        self.sample_get_bytes + self.sample_metric_get_bytes
    }

    /// Total bytes read from the object store across all operation types.
    pub fn os_read_bytes_total(&self) -> u64 {
        self.os_get_bytes + self.os_get_range_bytes + self.os_get_ranges_bytes
    }
}

/// Which logical read kind the current storage call belongs to.
#[derive(Debug, Clone, Copy)]
pub enum ReadKind {
    BucketList,
    InvertedIndex,
    ForwardIndex,
    SampleGet,
    SampleMetricGet,
}

// ─── Read signature for repeated-read tracking ──────────────────────────────

/// Identifies a unique object-store read by path and optional byte range.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectReadSignature {
    pub path: Arc<str>,
    pub range_start: Option<u64>,
    pub range_end: Option<u64>,
}

// ─── Collector ──────────────────────────────────────────────────────────────

/// Shared collector handle. Cloned into the task-local; the caller
/// keeps a handle to read stats back after the scope exits.
#[derive(Clone)]
pub struct QueryIoCollector {
    inner: Arc<Mutex<CollectorInner>>,
}

#[derive(Debug, Default)]
struct CollectorInner {
    stats: QueryIoStats,
    /// Counts per read signature for repeated-read detection.
    read_signatures: HashMap<ObjectReadSignature, u64>,
}

tokio::task_local! {
    static QUERY_IO_COLLECTOR: QueryIoCollector;
}

impl Default for QueryIoCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryIoCollector {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CollectorInner::default())),
        }
    }

    /// Take a snapshot of the current stats, including computed signature fields.
    pub fn snapshot(&self) -> QueryIoStats {
        let inner = self.inner.lock().unwrap();
        let mut stats = inner.stats.clone();

        // Compute signature-derived fields from the map.
        let distinct = inner.read_signatures.len() as u64;
        let total_calls: u64 = inner.read_signatures.values().sum();
        let max_repeats = inner.read_signatures.values().copied().max().unwrap_or(0);

        stats.os_distinct_read_signatures = distinct;
        stats.os_repeated_read_calls = total_calls.saturating_sub(distinct);
        stats.os_max_repeats_single_signature = max_repeats;

        stats
    }

    // ─── Storage-layer recording ────────────────────────────────────────

    fn record_get(&self, kind: ReadKind, record_bytes: u64) {
        let mut inner = self.inner.lock().unwrap();
        let stats = &mut inner.stats;
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
        let mut inner = self.inner.lock().unwrap();
        let stats = &mut inner.stats;
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

    // ─── Object-store recording ─────────────────────────────────────────

    /// Record an object-store read (get or get_range) with its signature.
    pub fn record_os_read(&self, signature: ObjectReadSignature, bytes: u64, is_range: bool) {
        let mut inner = self.inner.lock().unwrap();
        let stats = &mut inner.stats;
        if is_range {
            stats.os_get_range_calls += 1;
            stats.os_get_range_bytes += bytes;
        } else {
            stats.os_get_calls += 1;
            stats.os_get_bytes += bytes;
        }
        *inner.read_signatures.entry(signature).or_insert(0) += 1;
    }

    /// Record an object-store read error.
    pub fn record_os_read_error(&self, is_range: bool) {
        let mut inner = self.inner.lock().unwrap();
        let stats = &mut inner.stats;
        if is_range {
            stats.os_get_range_errors += 1;
        } else {
            stats.os_get_errors += 1;
        }
    }

    /// Record a get_ranges call (multi-range batch).
    pub fn record_os_get_ranges(&self, path: Arc<str>, ranges_bytes: &[u64]) {
        let mut inner = self.inner.lock().unwrap();
        let stats = &mut inner.stats;
        stats.os_get_ranges_calls += 1;
        let total: u64 = ranges_bytes.iter().sum();
        stats.os_get_ranges_bytes += total;
        // Each sub-range is a distinct signature.
        // But we track the batch call as one per path for simplicity.
        let sig = ObjectReadSignature {
            path,
            range_start: None,
            range_end: None,
        };
        *inner.read_signatures.entry(sig).or_insert(0) += 1;
    }

    /// Record a get_ranges error.
    pub fn record_os_get_ranges_error(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.stats.os_get_ranges_errors += 1;
    }

    /// Record an object-store head call.
    pub fn record_os_head(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.stats.os_head_calls += 1;
    }

    /// Record an object-store head error.
    pub fn record_os_head_error(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.stats.os_head_errors += 1;
    }

    /// Record an object-store list call and number of entries yielded.
    pub fn record_os_list(&self, entries: u64) {
        let mut inner = self.inner.lock().unwrap();
        let stats = &mut inner.stats;
        stats.os_list_calls += 1;
        stats.os_list_entries += entries;
    }

    /// Record an object-store list error.
    pub fn record_os_list_error(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.stats.os_list_errors += 1;
    }

    /// Record an object-store list_with_delimiter call.
    pub fn record_os_list_with_delimiter(&self, entries: u64) {
        let mut inner = self.inner.lock().unwrap();
        let stats = &mut inner.stats;
        stats.os_list_with_delimiter_calls += 1;
        stats.os_list_with_delimiter_entries += entries;
    }

    /// Record an object-store list_with_delimiter error.
    pub fn record_os_list_with_delimiter_error(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.stats.os_list_with_delimiter_errors += 1;
    }
}

// ─── Public free functions (storage-layer convenience) ──────────────────────

/// Record a storage `get()` call into the active per-query collector (if any).
pub fn record_get(kind: ReadKind, record: &Option<Record>) {
    let _ = QUERY_IO_COLLECTOR.try_with(|collector| {
        let bytes = record
            .as_ref()
            .map(|r| (r.key.len() + r.value.len()) as u64)
            .unwrap_or(0);
        collector.record_get(kind, bytes);
    });
}

/// Record a storage `scan()` call into the active per-query collector (if any).
pub fn record_scan(kind: ReadKind, records: &[Record]) {
    let _ = QUERY_IO_COLLECTOR.try_with(|collector| {
        let total_bytes: u64 = records
            .iter()
            .map(|r| (r.key.len() + r.value.len()) as u64)
            .sum();
        collector.record_scan(kind, records.len() as u64, total_bytes);
    });
}

/// Run an async block with the given collector installed as a task-local.
pub async fn run_with_collector<F, T>(collector: &QueryIoCollector, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    QUERY_IO_COLLECTOR.scope(collector.clone(), f).await
}

/// Snapshot the current task-local collector's stats, or return defaults if none is active.
pub fn snapshot_current() -> QueryIoStats {
    QUERY_IO_COLLECTOR
        .try_with(|c| c.snapshot())
        .unwrap_or_default()
}

/// Get a clone of the current task-local collector, if one is active.
/// Used by the ObservedObjectStore wrapper to record into the same collector.
pub fn try_get_collector() -> Option<QueryIoCollector> {
    QUERY_IO_COLLECTOR.try_with(|c| c.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

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

    #[tokio::test]
    async fn should_default_object_store_stats_to_zero() {
        let collector = QueryIoCollector::new();
        let stats = collector.snapshot();
        assert_eq!(stats.os_get_calls, 0);
        assert_eq!(stats.os_get_bytes, 0);
        assert_eq!(stats.os_get_range_calls, 0);
        assert_eq!(stats.os_get_range_bytes, 0);
        assert_eq!(stats.os_head_calls, 0);
        assert_eq!(stats.os_list_calls, 0);
        assert_eq!(stats.os_read_bytes_total(), 0);
        assert_eq!(stats.os_distinct_read_signatures, 0);
        assert_eq!(stats.os_repeated_read_calls, 0);
        assert_eq!(stats.os_max_repeats_single_signature, 0);
    }

    #[tokio::test]
    async fn should_track_read_signatures() {
        let collector = QueryIoCollector::new();
        run_with_collector(&collector, async {
            let c = try_get_collector().unwrap();
            let sig1 = ObjectReadSignature {
                path: Arc::from("obj/a.sst"),
                range_start: Some(0),
                range_end: Some(100),
            };
            let sig2 = ObjectReadSignature {
                path: Arc::from("obj/b.sst"),
                range_start: Some(0),
                range_end: Some(200),
            };
            // Read sig1 three times, sig2 once.
            c.record_os_read(sig1.clone(), 100, true);
            c.record_os_read(sig1.clone(), 100, true);
            c.record_os_read(sig1.clone(), 100, true);
            c.record_os_read(sig2, 200, true);
        })
        .await;

        let stats = collector.snapshot();
        assert_eq!(stats.os_get_range_calls, 4);
        assert_eq!(stats.os_get_range_bytes, 500);
        assert_eq!(stats.os_distinct_read_signatures, 2);
        assert_eq!(stats.os_repeated_read_calls, 2); // 4 total - 2 distinct
        assert_eq!(stats.os_max_repeats_single_signature, 3);
    }
}

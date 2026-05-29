//! Bench-side capture of SlateDB internal metrics.
//!
//! SlateDB exports its internals through the `metrics` crate via the
//! `MetricsRsRecorder` bridge in `opendata-common`. The bench binary installs a
//! global `Recorder` here so those metrics land in a registry we can query.
//!
//! The registered metric names follow the `slatedb.db.*` namespace — see
//! `slatedb::db_stats` for the canonical set.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, SharedString, Unit};
use metrics_util::Summary;
use metrics_util::registry::{AtomicStorage, Registry};

/// Global recorder that owns the registry SlateDB metrics flow into.
struct GlobalRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl metrics::Recorder for GlobalRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.registry
            .get_or_create_counter(key, |c| Counter::from_arc(c.clone()))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.registry
            .get_or_create_gauge(key, |g| Gauge::from_arc(g.clone()))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.registry
            .get_or_create_histogram(key, |h| Histogram::from_arc(h.clone()))
    }
}

/// Read-side handle on the global metrics registry. Cheap to clone.
#[derive(Clone)]
pub struct Handle {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl Handle {
    /// Sums counter values across every labeled variant sharing `name`.
    pub fn counter_value(&self, name: &str) -> u64 {
        let mut total = 0u64;
        self.registry.visit_counters(|key, counter| {
            if key.name() == name {
                total = total.saturating_add(counter.load(Ordering::Relaxed));
            }
        });
        total
    }

    /// Last-set gauge value. Sums across labeled variants when several exist;
    /// in practice the gauges we care about are unlabeled, so this is just the
    /// current value.
    pub fn gauge_value(&self, name: &str) -> f64 {
        let mut total = 0.0;
        self.registry.visit_gauges(|key, gauge| {
            if key.name() == name {
                total += f64::from_bits(gauge.load(Ordering::Relaxed));
            }
        });
        total
    }

    /// Like [`counter_value`](Self::counter_value), but only counts variants
    /// whose labels contain every `(name, value)` pair in `required`.
    pub fn counter_value_filtered(&self, name: &str, required: &[(&str, &str)]) -> u64 {
        let mut total = 0u64;
        self.registry.visit_counters(|key, counter| {
            if key.name() == name && key_matches_labels(key, required) {
                total = total.saturating_add(counter.load(Ordering::Relaxed));
            }
        });
        total
    }

    /// Drains every histogram matching `name` and the `required` labels into
    /// a single [`Summary`]. Drains the underlying buckets — values are gone
    /// after the call. Returns `None` if no matching histogram recorded
    /// anything (quantile reads would be meaningless).
    ///
    /// Multiple labeled variants matching the filter are merged into one
    /// summary, which is what you want for "p99 of every PUT regardless of
    /// which API path it took."
    pub fn histogram_summary(&self, name: &str, required: &[(&str, &str)]) -> Option<Summary> {
        let mut summary = Summary::with_defaults();
        self.registry.visit_histograms(|key, bucket| {
            if key.name() == name && key_matches_labels(key, required) {
                bucket.clear_with(|values| {
                    for &v in values {
                        summary.add(v);
                    }
                });
            }
        });
        if summary.count() == 0 {
            None
        } else {
            Some(summary)
        }
    }
}

fn key_matches_labels(key: &Key, required: &[(&str, &str)]) -> bool {
    required.iter().all(|(rk, rv)| {
        key.labels()
            .any(|lbl| lbl.key() == *rk && lbl.value() == *rv)
    })
}

static HANDLE: OnceLock<Handle> = OnceLock::new();

/// Installs the global recorder. Idempotent — repeat calls are no-ops.
///
/// Must be called before any `metrics::counter!()` / `metrics::histogram!()`
/// calls (in particular, before opening any SlateDB-backed storage).
pub fn install() {
    let registry = Arc::new(Registry::new(AtomicStorage));
    let handle = Handle {
        registry: Arc::clone(&registry),
    };
    if HANDLE.set(handle).is_err() {
        return;
    }
    let recorder = GlobalRecorder { registry };
    let _ = metrics::set_global_recorder(recorder);
}

/// Returns the installed handle, or `None` if `install()` hasn't been called.
pub fn handle() -> Option<Handle> {
    HANDLE.get().cloned()
}

/// Subset of `slatedb.*` metrics useful for ingest-bench interpretation.
///
/// **Capture semantics differ by field kind:**
/// - **Counters** are cumulative and read non-destructively. Use
///   [`delta_since`](Self::delta_since) to get the per-run delta.
/// - **Gauges** are read non-destructively; the snapshot holds the value at
///   capture time.
/// - **Histogram quantiles** (`*_p50_ms`, `*_p99_ms`) are computed at capture
///   time *by draining* the underlying buckets. Calling
///   [`capture`](Self::capture) at the start of a run discards any pre-run
///   accumulation; the end-of-run capture then sees only this run's samples.
#[derive(Debug, Clone, Copy, Default)]
pub struct SlatedbSnapshot {
    // ---- DB counters ----
    pub backpressure_count: u64,
    pub write_ops: u64,
    pub write_batch_count: u64,
    pub immutable_memtable_flushes: u64,
    pub wal_buffer_flushes: u64,
    pub wal_buffer_flush_requests: u64,
    pub l0_flush_bytes: u64,

    // ---- DB gauges (end-of-run) ----
    pub l0_sst_count: f64,
    pub segment_max_l0_sst_count: f64,
    pub total_mem_size_bytes: f64,
    pub wal_buffer_estimated_bytes: f64,

    // ---- Compactor counters ----
    pub compactor_bytes_compacted: u64,

    // ---- Compactor gauges (end-of-run) ----
    pub compactor_running_compactions: f64,
    pub compactor_throughput_bps: f64,
    pub compactor_total_bytes_being_compacted: f64,
    pub compactor_last_completion_ts_sec: f64,
    // NOTE: slatedb defines `component=compactor` on its instrumented object
    // store, but routes the compactor through the same `component=db` store
    // when built via `DbBuilder::with_compactor_builder` (the path LogDb
    // uses). So per-component object-store request_count/duration metrics
    // collapse to a single `db` bucket and can't be split here. Fix is
    // upstream — see `slatedb::db::builder::build_handler`.

    // ---- Object-store PUT/GET latency (component=db slice, ms) ----
    //
    // PUTs cover L0 memtable flushes plus compactor SST writes (slatedb
    // routes both through the same instrumented store). GETs cover compactor
    // SST reads + any cold path reads. Filtered by `op` only; merges across
    // the `api` variants (plain put, multipart_init/part/complete for PUT;
    // get/get_range/head/list for GET) into one summary per direction.
    pub put_count: u64,
    pub put_p50_ms: f64,
    pub put_p99_ms: f64,
    pub get_count: u64,
    pub get_p50_ms: f64,
    pub get_p99_ms: f64,
}

impl SlatedbSnapshot {
    pub fn capture(handle: &Handle) -> Self {
        let put_labels = [("op", "put")];
        let get_labels = [("op", "get")];
        let put_summary =
            handle.histogram_summary("slatedb.object_store.request_duration_seconds", &put_labels);
        let get_summary =
            handle.histogram_summary("slatedb.object_store.request_duration_seconds", &get_labels);
        let q = |s: &Option<Summary>, q: f64| -> f64 {
            s.as_ref()
                .and_then(|s| s.quantile(q))
                .map(|v| v * 1000.0)
                .unwrap_or(0.0)
        };

        Self {
            backpressure_count: handle.counter_value("slatedb.db.backpressure_count"),
            write_ops: handle.counter_value("slatedb.db.write_ops"),
            write_batch_count: handle.counter_value("slatedb.db.write_batch_count"),
            immutable_memtable_flushes: handle
                .counter_value("slatedb.db.immutable_memtable_flushes"),
            wal_buffer_flushes: handle.counter_value("slatedb.db.wal_buffer_flushes"),
            wal_buffer_flush_requests: handle.counter_value("slatedb.db.wal_buffer_flush_requests"),
            l0_flush_bytes: handle.counter_value("slatedb.db.l0_flush_bytes"),
            l0_sst_count: handle.gauge_value("slatedb.db.l0_sst_count"),
            segment_max_l0_sst_count: handle.gauge_value("slatedb.db.segment_max_l0_sst_count"),
            total_mem_size_bytes: handle.gauge_value("slatedb.db.total_mem_size_bytes"),
            wal_buffer_estimated_bytes: handle.gauge_value("slatedb.db.wal_buffer_estimated_bytes"),

            compactor_bytes_compacted: handle.counter_value("slatedb.compactor.bytes_compacted"),
            compactor_running_compactions: handle
                .gauge_value("slatedb.compactor.running_compactions"),
            compactor_throughput_bps: handle
                .gauge_value("slatedb.compactor.total_throughput_bytes_per_sec"),
            compactor_total_bytes_being_compacted: handle
                .gauge_value("slatedb.compactor.total_bytes_being_compacted"),
            compactor_last_completion_ts_sec: handle
                .gauge_value("slatedb.compactor.last_compaction_timestamp_sec"),

            put_count: handle
                .counter_value_filtered("slatedb.object_store.request_count", &put_labels),
            put_p50_ms: q(&put_summary, 0.5),
            put_p99_ms: q(&put_summary, 0.99),
            get_count: handle
                .counter_value_filtered("slatedb.object_store.request_count", &get_labels),
            get_p50_ms: q(&get_summary, 0.5),
            get_p99_ms: q(&get_summary, 0.99),
        }
    }

    /// Per-run delta: counters subtract, gauges take the end-of-run value.
    pub fn delta_since(&self, before: &Self) -> Self {
        Self {
            backpressure_count: self
                .backpressure_count
                .saturating_sub(before.backpressure_count),
            write_ops: self.write_ops.saturating_sub(before.write_ops),
            write_batch_count: self
                .write_batch_count
                .saturating_sub(before.write_batch_count),
            immutable_memtable_flushes: self
                .immutable_memtable_flushes
                .saturating_sub(before.immutable_memtable_flushes),
            wal_buffer_flushes: self
                .wal_buffer_flushes
                .saturating_sub(before.wal_buffer_flushes),
            wal_buffer_flush_requests: self
                .wal_buffer_flush_requests
                .saturating_sub(before.wal_buffer_flush_requests),
            l0_flush_bytes: self.l0_flush_bytes.saturating_sub(before.l0_flush_bytes),

            // Gauges: report the end-of-run absolute value, not a delta.
            l0_sst_count: self.l0_sst_count,
            segment_max_l0_sst_count: self.segment_max_l0_sst_count,
            total_mem_size_bytes: self.total_mem_size_bytes,
            wal_buffer_estimated_bytes: self.wal_buffer_estimated_bytes,

            compactor_bytes_compacted: self
                .compactor_bytes_compacted
                .saturating_sub(before.compactor_bytes_compacted),

            compactor_running_compactions: self.compactor_running_compactions,
            compactor_throughput_bps: self.compactor_throughput_bps,
            compactor_total_bytes_being_compacted: self.compactor_total_bytes_being_compacted,
            compactor_last_completion_ts_sec: self.compactor_last_completion_ts_sec,

            put_count: self.put_count.saturating_sub(before.put_count),
            put_p50_ms: self.put_p50_ms,
            put_p99_ms: self.put_p99_ms,
            get_count: self.get_count.saturating_sub(before.get_count),
            get_p50_ms: self.get_p50_ms,
            get_p99_ms: self.get_p99_ms,
        }
    }
}

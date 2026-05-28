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

    /// Debug helper: dump every counter the registry currently knows about
    /// along with its non-zero value. Intended for troubleshooting metric
    /// name mismatches.
    pub fn dump_counters(&self) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        self.registry.visit_counters(|key, counter| {
            let v = counter.load(Ordering::Relaxed);
            out.push((key.name().to_string(), v));
        });
        out.sort();
        out
    }

    pub fn dump_gauges(&self) -> Vec<(String, f64)> {
        let mut out = Vec::new();
        self.registry.visit_gauges(|key, gauge| {
            out.push((
                key.name().to_string(),
                f64::from_bits(gauge.load(Ordering::Relaxed)),
            ));
        });
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
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

/// Subset of `slatedb.db.*` metrics that are useful for ingest-bench
/// interpretation. Counters are cumulative; subtract before/after snapshots to
/// get a per-run delta.
#[derive(Debug, Clone, Copy, Default)]
pub struct SlatedbSnapshot {
    pub backpressure_count: u64,
    pub write_ops: u64,
    pub write_batch_count: u64,
    pub immutable_memtable_flushes: u64,
    pub wal_buffer_flushes: u64,
    pub wal_buffer_flush_requests: u64,
    pub l0_flush_bytes: u64,
    pub l0_sst_count: f64,
    pub total_mem_size_bytes: f64,
    pub wal_buffer_estimated_bytes: f64,
}

impl SlatedbSnapshot {
    pub fn capture(handle: &Handle) -> Self {
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
            total_mem_size_bytes: handle.gauge_value("slatedb.db.total_mem_size_bytes"),
            wal_buffer_estimated_bytes: handle.gauge_value("slatedb.db.wal_buffer_estimated_bytes"),
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
            total_mem_size_bytes: self.total_mem_size_bytes,
            wal_buffer_estimated_bytes: self.wal_buffer_estimated_bytes,
        }
    }
}

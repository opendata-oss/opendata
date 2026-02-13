//! Bridge between storage engine statistics and prometheus-client.
//!
//! This module reads backend-agnostic [`StorageStats`] snapshots and
//! registers them as Prometheus gauges so they appear on the `/metrics`
//! endpoint of any service using a storage backend.
//!
//! All stats are registered as Prometheus gauges — even semantically
//! monotonic ones like `write_ops` — because `StorageStats::snapshot()`
//! returns absolute values and prometheus-client's `Counter` only supports
//! `inc()`/`inc_by()`, not `set()`. Prometheus `rate()` still works on
//! gauges for computing per-second rates.

use std::collections::HashSet;
use std::sync::Arc;

use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

use crate::storage::stats::StorageStats;

/// A registered gauge and the metric name it maps to (used for refresh lookups).
struct StatGauge {
    gauge: Gauge<i64>,
    name: String,
}

/// Bridges a [`StorageStats`] implementation to a prometheus-client `Registry`.
///
/// On each call to [`refresh()`](StorageMetrics::refresh), takes a fresh
/// snapshot from the underlying stats source and updates the corresponding
/// Prometheus gauges. Call `refresh()` before encoding the registry.
pub struct StorageMetrics {
    stats: Arc<dyn StorageStats>,
    gauges: Vec<StatGauge>,
}

impl StorageMetrics {
    /// Register all metrics discovered via `StorageStats::snapshot()` into
    /// the given prometheus registry under a `slatedb` sub-registry prefix.
    pub fn register(stats: Arc<dyn StorageStats>, registry: &mut Registry) -> Self {
        let sub = registry.sub_registry_with_prefix("slatedb");
        let mut gauges = Vec::new();
        let mut seen = HashSet::new();

        for (name, _value) in stats.snapshot() {
            if !seen.insert(name.clone()) {
                tracing::warn!(
                    "Storage stat name collision after normalization: \
                     {name:?} (skipped)"
                );
                continue;
            }
            let gauge = Gauge::<i64, _>::default();
            sub.register(&name, format!("Storage {name}"), gauge.clone());
            gauges.push(StatGauge {
                gauge,
                name: name.clone(),
            });
        }

        Self { stats, gauges }
    }

    /// Refresh all gauge values from the stats source. Call before encoding.
    pub fn refresh(&self) {
        let snapshot: std::collections::HashMap<String, i64> =
            self.stats.snapshot().into_iter().collect();
        for sg in &self.gauges {
            if let Some(&value) = snapshot.get(&sg.name) {
                sg.gauge.set(value);
            }
        }
    }
}

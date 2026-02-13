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
    ///
    /// Uses first-wins semantics for duplicate names, matching the collision
    /// policy in [`register()`](StorageMetrics::register).
    pub fn refresh(&self) {
        let mut snapshot = std::collections::HashMap::new();
        for (name, value) in self.stats.snapshot() {
            snapshot.entry(name).or_insert(value);
        }
        for sg in &self.gauges {
            if let Some(&value) = snapshot.get(&sg.name) {
                sg.gauge.set(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake StorageStats for testing.
    struct FakeStats {
        data: Vec<(String, i64)>,
    }

    impl StorageStats for FakeStats {
        fn snapshot(&self) -> Vec<(String, i64)> {
            self.data.clone()
        }
    }

    #[test]
    fn should_register_and_refresh_metrics() {
        let stats = Arc::new(FakeStats {
            data: vec![
                ("db_write_ops".to_string(), 42),
                ("db_read_ops".to_string(), 10),
            ],
        });

        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(stats, &mut registry);

        // Refresh should populate gauges
        metrics.refresh();

        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        assert!(buf.contains("slatedb_db_write_ops 42"));
        assert!(buf.contains("slatedb_db_read_ops 10"));
    }

    #[test]
    fn should_skip_duplicate_names_and_use_first_value() {
        // Two stats with the same normalized name — register should keep the
        // first and skip the second, refresh should use first-wins too.
        let stats = Arc::new(FakeStats {
            data: vec![
                ("dup_name".to_string(), 100),
                ("dup_name".to_string(), 999),
            ],
        });

        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(stats, &mut registry);

        // Only one gauge should be registered
        assert_eq!(metrics.gauges.len(), 1);

        metrics.refresh();

        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        // Should show 100 (first-wins), not 999
        assert!(
            buf.contains("slatedb_dup_name 100"),
            "Expected first-wins value 100, got:\n{}",
            buf
        );
    }

    #[test]
    fn should_handle_empty_stats() {
        let stats = Arc::new(FakeStats { data: vec![] });

        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(stats, &mut registry);

        assert!(metrics.gauges.is_empty());
        metrics.refresh(); // should not panic
    }
}

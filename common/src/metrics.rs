//! Bridge between SlateDB's internal metrics and prometheus-client.
//!
//! SlateDB exposes database stats via `Db::metrics() -> Arc<StatRegistry>`.
//! This module registers those stats as Prometheus gauges so they appear
//! on the `/metrics` endpoint of any service using SlateDB.

use std::sync::Arc;

use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use slatedb::stats::{ReadableStat, StatRegistry};

/// A single gauge paired with the SlateDB stat it reads from.
struct StatGauge {
    gauge: Gauge<i64>,
    stat: Arc<dyn ReadableStat>,
}

/// Bridges SlateDB's `StatRegistry` to a prometheus-client `Registry`.
///
/// On each call to [`refresh()`](SlateDbMetrics::refresh), reads current
/// values from SlateDB's atomic stats and updates the corresponding
/// Prometheus gauges. Call `refresh()` before encoding the registry.
pub struct SlateDbMetrics {
    gauges: Vec<StatGauge>,
}

impl SlateDbMetrics {
    /// Register all known SlateDB metrics into the given prometheus registry
    /// under a `slatedb` sub-registry prefix.
    pub fn register(stat_registry: Arc<StatRegistry>, registry: &mut Registry) -> Self {
        let sub = registry.sub_registry_with_prefix("slatedb");
        let mut gauges = Vec::new();

        for name in stat_registry.names() {
            if let Some(stat) = stat_registry.lookup(name) {
                // Convert "db/l0_sst_count" -> "db_l0_sst_count"
                let prom_name = name.replace('/', "_");
                let gauge = Gauge::<i64, _>::default();
                sub.register(&prom_name, format!("SlateDB {name}"), gauge.clone());
                gauges.push(StatGauge { gauge, stat });
            }
        }

        Self { gauges }
    }

    /// Refresh all gauge values from the StatRegistry. Call before encoding.
    pub fn refresh(&self) {
        for sg in &self.gauges {
            sg.gauge.set(sg.stat.get());
        }
    }
}

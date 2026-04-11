//! Bridge from slatedb's [`MetricsRecorder`] trait to the `metrics` crate.
//!
//! Slatedb calls `register_*` during DB construction; each call is forwarded
//! to the `metrics` global recorder so that slatedb metrics appear alongside
//! application metrics on the Prometheus `/metrics` endpoint.

use slatedb_common::metrics::{CounterFn, GaugeFn, HistogramFn, MetricsRecorder, UpDownCounterFn};
use std::sync::Arc;

/// [`MetricsRecorder`] implementation that forwards to the `metrics` crate.
///
/// Metric names are forwarded as-is (dot-separated, e.g.
/// `slatedb.db.write_ops`). Labels become `metrics::Label`s. Descriptions
/// are registered via `metrics::describe_*`.
pub struct MetricsRsRecorder;

impl MetricsRecorder for MetricsRsRecorder {
    fn register_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn CounterFn> {
        let name = name.to_string();
        let labels = to_labels(labels);
        metrics::describe_counter!(name.clone(), description.to_string());
        Arc::new(CounterHandle(metrics::counter!(name, labels)))
    }

    fn register_gauge(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn GaugeFn> {
        let name = name.to_string();
        let labels = to_labels(labels);
        metrics::describe_gauge!(name.clone(), description.to_string());
        Arc::new(GaugeHandle(metrics::gauge!(name, labels)))
    }

    fn register_up_down_counter(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
    ) -> Arc<dyn UpDownCounterFn> {
        // metrics-rs has no up-down counter; map to a gauge with increment.
        let name = name.to_string();
        let labels = to_labels(labels);
        metrics::describe_gauge!(name.clone(), description.to_string());
        Arc::new(UpDownCounterHandle(metrics::gauge!(name, labels)))
    }

    fn register_histogram(
        &self,
        name: &str,
        description: &str,
        labels: &[(&str, &str)],
        _boundaries: &[f64],
    ) -> Arc<dyn HistogramFn> {
        // Bucket boundaries are configured on the PrometheusBuilder, not here.
        let name = name.to_string();
        let labels = to_labels(labels);
        metrics::describe_histogram!(name.clone(), description.to_string());
        Arc::new(HistogramHandle(metrics::histogram!(name, labels)))
    }
}

fn to_labels(labels: &[(&str, &str)]) -> Vec<metrics::Label> {
    labels
        .iter()
        .map(|(k, v)| metrics::Label::new(k.to_string(), v.to_string()))
        .collect()
}

// -- Handle wrappers --

struct CounterHandle(metrics::Counter);

impl CounterFn for CounterHandle {
    fn increment(&self, value: u64) {
        self.0.increment(value);
    }
}

struct GaugeHandle(metrics::Gauge);

impl GaugeFn for GaugeHandle {
    fn set(&self, value: i64) {
        self.0.set(value as f64);
    }
}

struct UpDownCounterHandle(metrics::Gauge);

impl UpDownCounterFn for UpDownCounterHandle {
    fn increment(&self, value: i64) {
        self.0.increment(value as f64);
    }
}

struct HistogramHandle(metrics::Histogram);

impl HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        self.0.record(value);
    }
}

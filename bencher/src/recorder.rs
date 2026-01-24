//! Metrics recorder that collects into our Series model.

use std::sync::atomic::Ordering;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::Summary;
use metrics_util::registry::{AtomicStorage, Registry};
use timeseries::{Label, MetricType, Sample, Series, Temporality};

/// Standard quantiles to export for histograms.
const QUANTILES: &[f64] = &[0.5, 0.9, 0.95, 0.99];

/// A recorder that collects metrics for later export as Series.
pub struct BenchRecorder {
    registry: Registry<Key, AtomicStorage>,
}

impl BenchRecorder {
    /// Create a new recorder.
    pub fn new() -> Self {
        Self {
            registry: Registry::new(AtomicStorage),
        }
    }

    /// Snapshot all metrics as Series.
    ///
    /// `base_labels` are added to every series (e.g., benchmark name, params).
    pub fn snapshot(&self, base_labels: &[Label]) -> Vec<Series> {
        let mut series = Vec::new();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Snapshot counters
        self.registry.visit_counters(|key, counter| {
            let value = counter.load(Ordering::Relaxed) as f64;
            series.push(Self::key_to_series(
                key,
                base_labels,
                value,
                now_ms,
                MetricType::Sum {
                    monotonic: true,
                    temporality: Temporality::Cumulative,
                },
            ));
        });

        // Snapshot gauges
        self.registry.visit_gauges(|key, gauge| {
            let bits = gauge.load(Ordering::Relaxed);
            let value = f64::from_bits(bits);
            series.push(Self::key_to_series(
                key,
                base_labels,
                value,
                now_ms,
                MetricType::Gauge,
            ));
        });

        // Snapshot histograms - drain values into Summary and extract quantiles
        self.registry.visit_histograms(|key, bucket| {
            let mut summary = Summary::with_defaults();
            bucket.clear_with(|values| {
                for &v in values {
                    summary.add(v);
                }
            });

            if summary.count() > 0 {
                // Export each quantile as a separate series
                for &q in QUANTILES {
                    if let Some(value) = summary.quantile(q) {
                        series.push(Self::histogram_quantile_series(
                            key,
                            base_labels,
                            q,
                            value,
                            now_ms,
                        ));
                    }
                }

                // Also export min and max
                series.push(Self::histogram_quantile_series(
                    key,
                    base_labels,
                    0.0,
                    summary.min(),
                    now_ms,
                ));
                series.push(Self::histogram_quantile_series(
                    key,
                    base_labels,
                    1.0,
                    summary.max(),
                    now_ms,
                ));
                series.push(Self::histogram_count_series(
                    key,
                    base_labels,
                    summary.count(),
                    now_ms,
                ));
            }
        });

        series
    }

    fn key_to_series(
        key: &Key,
        base_labels: &[Label],
        value: f64,
        timestamp_ms: i64,
        metric_type: MetricType,
    ) -> Series {
        let mut labels = vec![Label::metric_name(key.name().to_string())];
        labels.extend(base_labels.iter().cloned());
        for label in key.labels() {
            labels.push(Label::new(
                label.key().to_string(),
                label.value().to_string(),
            ));
        }

        Series {
            labels,
            metric_type: Some(metric_type),
            unit: None,
            description: None,
            samples: vec![Sample::new(timestamp_ms, value)],
        }
    }

    fn histogram_quantile_series(
        key: &Key,
        base_labels: &[Label],
        quantile: f64,
        value: f64,
        timestamp_ms: i64,
    ) -> Series {
        let mut labels = vec![Label::metric_name(key.name().to_string())];
        labels.extend(base_labels.iter().cloned());
        labels.push(Label::new("quantile", quantile.to_string()));
        for label in key.labels() {
            labels.push(Label::new(
                label.key().to_string(),
                label.value().to_string(),
            ));
        }

        Series {
            labels,
            metric_type: Some(MetricType::Summary),
            unit: None,
            description: None,
            samples: vec![Sample::new(timestamp_ms, value)],
        }
    }

    fn histogram_count_series(
        key: &Key,
        base_labels: &[Label],
        count: usize,
        timestamp_ms: i64,
    ) -> Series {
        // Use a suffix for the count metric
        let count_name = format!("{}_count", key.name());
        let mut labels = vec![Label::metric_name(count_name)];
        labels.extend(base_labels.iter().cloned());
        for label in key.labels() {
            labels.push(Label::new(
                label.key().to_string(),
                label.value().to_string(),
            ));
        }

        Series {
            labels,
            metric_type: Some(MetricType::Sum {
                monotonic: true,
                temporality: Temporality::Cumulative,
            }),
            unit: None,
            description: None,
            samples: vec![Sample::new(timestamp_ms, count as f64)],
        }
    }
}

impl Default for BenchRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder for BenchRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.registry
            .get_or_create_counter(key, |c| Counter::from_arc(c.clone()))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.registry
            .get_or_create_gauge(key, |g| Gauge::from_arc(g.clone()))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.registry
            .get_or_create_histogram(key, |h| Histogram::from_arc(h.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metadata() -> Metadata<'static> {
        Metadata::new("test", metrics::Level::INFO, None)
    }

    #[test]
    fn should_snapshot_counter_with_accumulated_value() {
        // given
        let recorder = BenchRecorder::new();
        let counter = recorder.register_counter(&Key::from_name("requests"), &test_metadata());
        counter.increment(10);
        counter.increment(5);

        // when
        let base_labels = vec![Label::new("benchmark", "test")];
        let series = recorder.snapshot(&base_labels);

        // then
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].name(), "requests");
        assert_eq!(series[0].samples.len(), 1);
        assert_eq!(series[0].samples[0].value, 15.0);
    }

    #[test]
    fn should_snapshot_gauge_with_current_value() {
        // given
        let recorder = BenchRecorder::new();
        let gauge = recorder.register_gauge(&Key::from_name("temperature"), &test_metadata());
        gauge.set(98.6);

        // when
        let series = recorder.snapshot(&[]);

        // then
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].name(), "temperature");
        assert_eq!(series[0].samples[0].value, 98.6);
    }

    #[test]
    fn should_include_metric_labels_in_series() {
        // given
        let recorder = BenchRecorder::new();
        let key = Key::from_parts(
            "http_requests",
            vec![
                metrics::Label::new("method", "GET"),
                metrics::Label::new("status", "200"),
            ],
        );
        let counter = recorder.register_counter(&key, &test_metadata());
        counter.increment(42);

        // when
        let series = recorder.snapshot(&[]);

        // then
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].name(), "http_requests");
        let label_names: Vec<_> = series[0].labels.iter().map(|l| l.name.as_str()).collect();
        assert!(label_names.contains(&"method"));
        assert!(label_names.contains(&"status"));
    }

    #[test]
    fn should_snapshot_histogram_with_quantiles() {
        // given
        let recorder = BenchRecorder::new();
        let histogram =
            recorder.register_histogram(&Key::from_name("latency_us"), &test_metadata());

        // Record some latency values
        for v in [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0] {
            histogram.record(v);
        }

        // when
        let series = recorder.snapshot(&[]);

        // then - should have p50, p90, p95, p99, min, max, and count = 7 series
        assert_eq!(series.len(), 7);

        // Find the count series
        let count_series = series
            .iter()
            .find(|s| s.name() == "latency_us_count")
            .expect("should have count series");
        assert_eq!(count_series.samples[0].value, 10.0);

        // Find the p50 series
        let p50_series = series
            .iter()
            .find(|s| {
                s.name() == "latency_us"
                    && s.labels
                        .iter()
                        .any(|l| l.name == "quantile" && l.value == "0.5")
            })
            .expect("should have p50 series");
        // p50 should be around 50-55 for uniform distribution
        assert!(
            p50_series.samples[0].value >= 40.0 && p50_series.samples[0].value <= 60.0,
            "p50 was {}",
            p50_series.samples[0].value
        );
    }

    #[test]
    fn should_drain_histogram_values_on_snapshot() {
        // given
        let recorder = BenchRecorder::new();
        let histogram =
            recorder.register_histogram(&Key::from_name("latency_us"), &test_metadata());
        histogram.record(100.0);

        // when - first snapshot drains values
        let series1 = recorder.snapshot(&[]);

        // then - first snapshot has data
        assert!(!series1.is_empty());

        // when - second snapshot with no new data
        let series2 = recorder.snapshot(&[]);

        // then - second snapshot is empty (values were drained)
        let histogram_series: Vec<_> = series2
            .iter()
            .filter(|s| s.name() == "latency_us" || s.name() == "latency_us_count")
            .collect();
        assert!(
            histogram_series.is_empty(),
            "histogram should be drained after snapshot"
        );
    }
}

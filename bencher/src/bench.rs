//! Benchmark runtime types.
//!
//! This module contains the types used during benchmark execution:
//! - [`Bench`] - Runtime for recording metrics
//! - [`BenchSpec`] - Specification for a benchmark run
//! - [`Runner`] - Timing and deadline tracking
//! - [`Summary`] - Summary metrics collection

use std::sync::Arc;
use std::time::Instant;

use common::display::format_number;
use metrics::Recorder;
use timeseries::{Label, MetricType, Sample, Series};

use crate::config::DataConfig;
use crate::metrics::BenchRecorder;
use crate::params::Params;
use crate::reporter::Reporter;

/// Specification for a benchmark run.
///
/// Contains the parameters, data configuration, and labels for a benchmark.
pub struct BenchSpec {
    params: Params,
    data: DataConfig,
    labels: Vec<Label>,
    duration: std::time::Duration,
}

impl BenchSpec {
    pub(crate) fn new(
        params: Params,
        data: DataConfig,
        labels: Vec<Label>,
        duration: std::time::Duration,
    ) -> Self {
        Self {
            params,
            data,
            labels,
            duration,
        }
    }

    /// Access the benchmark parameters.
    pub fn params(&self) -> &Params {
        &self.params
    }

    /// Access the data storage configuration.
    pub fn data(&self) -> &DataConfig {
        &self.data
    }

    /// Get all labels (static labels + parameter labels).
    pub(crate) fn all_labels(&self) -> Vec<Label> {
        let mut labels = self.labels.clone();
        let param_labels: Vec<Label> = self.params.clone().into();
        labels.extend(param_labels);
        labels
    }
}

/// Timing and deadline tracking for a benchmark run.
pub struct Runner {
    start: Instant,
    deadline: Instant,
}

impl Runner {
    pub(crate) fn new(start: Instant, deadline: Instant) -> Self {
        Self { start, deadline }
    }

    /// Returns true if the benchmark should continue running.
    pub fn keep_running(&self) -> bool {
        Instant::now() < self.deadline
    }

    /// Get the elapsed time since the benchmark started.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }
}

/// Runtime for recording benchmark metrics.
///
/// Created by the framework for each parameter combination.
/// Provides access to the spec and metrics recording.
pub struct Bench {
    spec: BenchSpec,
    recorder: Arc<BenchRecorder>,
    reporter: Option<Arc<dyn Reporter>>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    report_task: Option<tokio::task::JoinHandle<()>>,
}

impl Bench {
    pub(crate) fn open(
        spec: BenchSpec,
        reporter: Option<Arc<dyn Reporter>>,
        interval: Option<std::time::Duration>,
    ) -> Self {
        let recorder = Arc::new(BenchRecorder::new());

        // Start background reporting if a reporter and interval are configured
        let (shutdown_tx, report_task) = match (&reporter, interval) {
            (Some(rep), Some(interval_duration)) => {
                let (tx, mut rx) = tokio::sync::oneshot::channel();
                let task_recorder = Arc::clone(&recorder);
                let task_reporter = Arc::clone(rep);
                let task_labels = spec.all_labels();

                let task = tokio::spawn(async move {
                    let mut interval = tokio::time::interval(interval_duration);
                    interval.tick().await; // First tick completes immediately

                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                let series = task_recorder.snapshot(&task_labels);
                                if !series.is_empty() {
                                    let _ = task_reporter.write(series).await;
                                }
                            }
                            _ = &mut rx => {
                                break;
                            }
                        }
                    }
                });
                (Some(tx), Some(task))
            }
            _ => (None, None),
        };

        Self {
            spec,
            recorder,
            reporter,
            shutdown_tx,
            report_task,
        }
    }

    /// Start the benchmark timer and return a runner for tracking progress.
    pub fn start(&self) -> Runner {
        let start = Instant::now();
        let deadline = start + self.spec.duration;
        Runner::new(start, deadline)
    }

    /// Access the benchmark specification.
    pub fn spec(&self) -> &BenchSpec {
        &self.spec
    }

    /// Get a counter handle for recording incremental values.
    ///
    /// The counter is identified by name and will include the bench's labels
    /// when exported.
    pub fn counter(&self, name: &'static str) -> metrics::Counter {
        self.recorder.register_counter(
            &metrics::Key::from_name(name),
            &metrics::Metadata::new("bench", metrics::Level::INFO, None),
        )
    }

    /// Get a gauge handle for recording point-in-time values.
    ///
    /// The gauge is identified by name and will include the bench's labels
    /// when exported.
    pub fn gauge(&self, name: &'static str) -> metrics::Gauge {
        self.recorder.register_gauge(
            &metrics::Key::from_name(name),
            &metrics::Metadata::new("bench", metrics::Level::INFO, None),
        )
    }

    /// Get a histogram handle for recording distributions (e.g., latencies).
    ///
    /// The histogram is identified by name and will include the bench's labels
    /// when exported. Quantiles (p50, p90, p95, p99) are computed at snapshot time.
    pub fn histogram(&self, name: &'static str) -> metrics::Histogram {
        self.recorder.register_histogram(
            &metrics::Key::from_name(name),
            &metrics::Metadata::new("bench", metrics::Level::INFO, None),
        )
    }

    /// Write summary metrics.
    ///
    /// Use this for final results or computed aggregates at the end of a benchmark
    /// run, such as total throughput or elapsed time.
    ///
    /// For metrics updated continuously during the benchmark, use [`counter()`](Bench::counter),
    /// [`gauge()`](Bench::gauge), or [`histogram()`](Bench::histogram) instead.
    ///
    /// # Example
    ///
    /// ```ignore
    /// bench.summarize(
    ///     Summary::new()
    ///         .add("throughput_ops", ops_per_sec)
    ///         .add("elapsed_ms", elapsed_ms)
    /// ).await?;
    /// ```
    pub async fn summarize(&self, summary: Summary) -> anyhow::Result<()> {
        // Always print to console
        self.print_summary(&summary);

        // Optionally report to configured reporter
        if let Some(ref reporter) = self.reporter {
            let series: Vec<Series> = summary
                .metrics
                .into_iter()
                .map(|(name, value)| {
                    let mut labels = vec![Label::metric_name(name)];
                    labels.extend(self.spec.all_labels());
                    Series {
                        labels,
                        metric_type: Some(MetricType::Gauge),
                        unit: None,
                        description: None,
                        samples: vec![Sample::now(value)],
                    }
                })
                .collect();
            reporter.summarize(series).await?;
        }
        Ok(())
    }

    /// Print summary to console with nice formatting.
    fn print_summary(&self, summary: &Summary) {
        // Print labels header
        let all_labels = self.spec.all_labels();
        if !all_labels.is_empty() {
            let labels_str: Vec<_> = all_labels
                .iter()
                .map(|l| format!("{}={}", l.name, l.value))
                .collect();
            println!("  [{}]", labels_str.join(", "));
        }

        // Find max metric name length for alignment
        let max_name_len = summary
            .metrics
            .iter()
            .map(|(name, _)| name.len())
            .max()
            .unwrap_or(0);

        // Print metrics with aligned formatting
        for (name, value) in &summary.metrics {
            println!(
                "    {:<width$}  {}",
                name,
                format_number(*value),
                width = max_name_len
            );
        }
    }

    /// Mark the run as complete.
    ///
    /// This stops background reporting, takes a final snapshot, and exports all metrics.
    pub async fn close(mut self) -> anyhow::Result<()> {
        // Signal shutdown to background task
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Wait for background task to finish
        if let Some(task) = self.report_task.take() {
            let _ = task.await;
        }

        // Final snapshot and report (only if reporter configured)
        if let Some(ref reporter) = self.reporter {
            let series = self.recorder.snapshot(&self.spec.all_labels());
            if !series.is_empty() {
                reporter.write(series).await?;
            }
            reporter.flush().await?;
        }
        Ok(())
    }
}

impl Drop for Bench {
    fn drop(&mut self) {
        // If close() wasn't called, abort the background task
        if let Some(task) = self.report_task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
impl Bench {
    /// Create a Bench with a custom reporter for testing.
    pub(crate) fn with_reporter(
        labels: Vec<Label>,
        reporter: Arc<dyn Reporter>,
        interval: std::time::Duration,
    ) -> Self {
        let spec = BenchSpec::new(
            Params::new(),
            DataConfig::default(),
            labels,
            std::time::Duration::from_secs(10),
        );
        Self::open(spec, Some(reporter), Some(interval))
    }
}

/// A collection of summary metrics to record.
///
/// Written via [`Bench::summarize()`].
pub struct Summary {
    pub(crate) metrics: Vec<(String, f64)>,
}

impl Summary {
    /// Create a new empty summary.
    pub fn new() -> Self {
        Self {
            metrics: Vec::new(),
        }
    }

    /// Add a metric to the summary.
    pub fn add(mut self, name: impl Into<String>, value: f64) -> Self {
        self.metrics.push((name.into(), value));
        self
    }
}

impl Default for Summary {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn runner_should_keep_running_before_deadline() {
        let start = Instant::now();
        let deadline = start + Duration::from_secs(60);
        let runner = Runner::new(start, deadline);

        assert!(runner.keep_running());
    }

    #[test]
    fn runner_should_stop_after_deadline() {
        let start = Instant::now() - Duration::from_secs(10);
        let deadline = start + Duration::from_secs(5); // Deadline is in the past
        let runner = Runner::new(start, deadline);

        assert!(!runner.keep_running());
    }

    #[test]
    fn runner_should_track_elapsed_time() {
        let start = Instant::now() - Duration::from_millis(100);
        let deadline = start + Duration::from_secs(60);
        let runner = Runner::new(start, deadline);

        assert!(runner.elapsed() >= Duration::from_millis(100));
    }

    #[test]
    fn bench_spec_should_combine_static_and_param_labels() {
        let mut params = Params::new();
        params.insert("batch_size", "100");
        params.insert("value_size", "256");

        let static_labels = vec![Label::new("benchmark", "ingest"), Label::new("env", "test")];

        let spec = BenchSpec::new(
            params,
            DataConfig::default(),
            static_labels,
            Duration::from_secs(10),
        );

        let all_labels = spec.all_labels();

        // Should have both static labels
        assert!(
            all_labels
                .iter()
                .any(|l| l.name == "benchmark" && l.value == "ingest")
        );
        assert!(
            all_labels
                .iter()
                .any(|l| l.name == "env" && l.value == "test")
        );

        // Should have param labels
        assert!(
            all_labels
                .iter()
                .any(|l| l.name == "batch_size" && l.value == "100")
        );
        assert!(
            all_labels
                .iter()
                .any(|l| l.name == "value_size" && l.value == "256")
        );
    }

    #[test]
    fn bench_spec_should_expose_params() {
        let mut params = Params::new();
        params.insert("key", "value");

        let spec = BenchSpec::new(
            params,
            DataConfig::default(),
            vec![],
            Duration::from_secs(10),
        );

        assert_eq!(spec.params().get("key"), Some("value"));
    }

    #[test]
    fn summary_should_build_with_metrics() {
        let summary = Summary::new()
            .add("throughput", 1000.0)
            .add("latency_p99", 42.5);

        assert_eq!(summary.metrics.len(), 2);
        assert_eq!(summary.metrics[0], ("throughput".to_string(), 1000.0));
        assert_eq!(summary.metrics[1], ("latency_p99".to_string(), 42.5));
    }

    #[test]
    fn summary_should_be_empty_by_default() {
        let summary = Summary::new();
        assert!(summary.metrics.is_empty());
    }
}

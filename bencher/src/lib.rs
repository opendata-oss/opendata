//! Minimal benchmark framework for OpenData.
//!
//! This crate provides infrastructure for running integration benchmarks:
//! - Storage initialization and cleanup
//! - Metrics collection and export
//! - Machine-readable output (CSV) for regression analysis
//!
//! # Recording Metrics
//!
//! There are two ways to record metrics:
//!
//! 1. **Ongoing metrics** via [`Bench::counter()`], [`Bench::gauge()`], [`Bench::histogram()`]:
//!    Updated during the benchmark and automatically snapshotted at regular intervals.
//!    Use for measurements like request counts, latencies, or queue depths.
//!
//! 2. **Summary metrics** via [`Bench::summary()`]:
//!    One-off samples recorded immediately. Use for final results or computed
//!    aggregates at the end of a benchmark run.
//!
//! # Example
//!
//! ```ignore
//! use bencher::{Bench, Benchmark, Params, Summary};
//!
//! fn make_params(record_size: usize) -> Params {
//!     let mut p = Params::new();
//!     p.insert("record_size", record_size.to_string());
//!     p
//! }
//!
//! struct LogAppendBenchmark;
//!
//! #[async_trait::async_trait]
//! impl Benchmark for LogAppendBenchmark {
//!     fn name(&self) -> &str { "log_append" }
//!
//!     fn default_params(&self) -> Vec<Params> {
//!         vec![make_params(64), make_params(256), make_params(1024)]
//!     }
//!
//!     async fn run(&self, bench: Bench) -> anyhow::Result<()> {
//!         let record_size: usize = bench.spec().params().get_parse("record_size")?;
//!
//!         // Ongoing metrics - updated during benchmark, snapshotted periodically
//!         let ops = bench.counter("operations");
//!         let latency = bench.histogram("latency_us");
//!
//!         // Run until the framework signals to stop
//!         while bench.keep_running() {
//!             // ... do work ...
//!             ops.increment(1);
//!             latency.record(42.0);
//!         }
//!
//!         bench.close().await?;
//!         Ok(())
//!     }
//! }
//! ```

mod cli;
mod config;
mod recorder;
mod reporter;

#[cfg(test)]
use reporter::MemoryReporter;
use reporter::{Reporter, TimeSeriesReporter};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use metrics::Recorder;
use recorder::BenchRecorder;

// Re-export metrics types for benchmark authors
pub use metrics::{Counter, Gauge, Histogram};

pub use cli::Args;
pub use config::{Config, DataConfig, ReporterConfig};

// Re-export timeseries model types
pub use timeseries::{Label, MetricType, Sample, Series, SeriesBuilder};

/// A collection of benchmark parameters with efficient key lookup.
///
/// Provides convenient methods for extracting and parsing parameter values.
/// Parameters are stored as string key-value pairs and can be converted
/// to/from `Vec<Label>` for use with the metrics system.
#[derive(Debug, Clone, Default)]
pub struct Params {
    inner: HashMap<String, String>,
}

impl Params {
    /// Create an empty Params collection.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Get a label value by name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.inner.get(name).map(|s| s.as_str())
    }

    /// Get and parse a label value.
    pub fn get_parse<T>(&self, name: &str) -> anyhow::Result<T>
    where
        T: std::str::FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        let value = self
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("missing label: {}", name))?;
        value
            .parse()
            .map_err(|e: T::Err| anyhow::anyhow!("failed to parse label '{}': {}", name, e))
    }

    /// Insert a label.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.inner.insert(name.into(), value.into());
    }
}

impl From<Vec<Label>> for Params {
    fn from(labels: Vec<Label>) -> Self {
        Self {
            inner: labels.into_iter().map(|l| (l.name, l.value)).collect(),
        }
    }
}

impl From<&[Label]> for Params {
    fn from(labels: &[Label]) -> Self {
        Self {
            inner: labels
                .iter()
                .map(|l| (l.name.clone(), l.value.clone()))
                .collect(),
        }
    }
}

impl From<Params> for Vec<Label> {
    fn from(labels: Params) -> Self {
        labels
            .inner
            .into_iter()
            .map(|(name, value)| Label::new(name, value))
            .collect()
    }
}

/// Main bencher struct. Handles storage initialization and metrics reporting.
pub struct Bencher {
    config: Config,
    reporter: Option<Arc<dyn Reporter>>,
    labels: Vec<Label>,
}

impl Bencher {
    /// Create a new Bencher with the given configuration, benchmark name, and static labels.
    async fn new(config: Config, name: &str, labels: Vec<Label>) -> anyhow::Result<Self> {
        let reporter: Option<Arc<dyn Reporter>> = match &config.reporter {
            Some(reporter_config) => {
                let ts_config = timeseries::Config {
                    storage: reporter_config.to_storage_config(),
                    ..Default::default()
                };
                let ts = timeseries::TimeSeries::open(ts_config).await?;
                Some(Arc::new(TimeSeriesReporter::new(ts)))
            }
            None => None,
        };

        let mut all_labels = vec![Label::new("benchmark", name)];
        all_labels.extend(Self::env_labels());
        all_labels.extend(labels);

        Ok(Self {
            config,
            reporter,
            labels: all_labels,
        })
    }

    /// Collect labels from the environment (git info, etc.).
    fn env_labels() -> Vec<Label> {
        let mut labels = Vec::new();

        // Git commit
        if let Ok(output) = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            && output.status.success()
            && let Ok(commit) = String::from_utf8(output.stdout)
        {
            labels.push(Label::new("commit", commit.trim()));
        }

        // Git branch
        if let Ok(output) = std::process::Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .output()
            && output.status.success()
            && let Ok(branch) = String::from_utf8(output.stdout)
        {
            labels.push(Label::new("branch", branch.trim()));
        }

        labels
    }

    /// Access the data storage configuration.
    pub fn data(&self) -> &DataConfig {
        &self.config.data
    }

    /// Start a new benchmark run with the given parameters.
    fn bench(&self, params: Params, duration: std::time::Duration) -> Bench {
        let spec = BenchSpec::new(
            params,
            self.config.data.clone(),
            self.labels.clone(),
            duration,
        );
        let interval = self.config.reporter.as_ref().map(|r| r.interval);
        Bench::open(spec, self.reporter.clone(), interval)
    }
}

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
    fn new(
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
    fn all_labels(&self) -> Vec<Label> {
        let mut labels = self.labels.clone();
        let param_labels: Vec<Label> = self.params.clone().into();
        labels.extend(param_labels);
        labels
    }
}

pub struct Runner {
    start: Instant,
    deadline: Instant,
}

impl Runner {
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
    fn open(
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

    pub fn start(&self) -> Runner {
        let start = Instant::now();
        let deadline = start + self.spec.duration;
        Runner { start, deadline }
    }

    /// Access the benchmark specification.
    pub fn spec(&self) -> &BenchSpec {
        &self.spec
    }

    /// Get a counter handle for recording incremental values.
    ///
    /// The counter is identified by name and will include the bench's labels
    /// when exported.
    pub fn counter(&self, name: &'static str) -> Counter {
        self.recorder.register_counter(
            &metrics::Key::from_name(name),
            &metrics::Metadata::new("bench", metrics::Level::INFO, None),
        )
    }

    /// Get a gauge handle for recording point-in-time values.
    ///
    /// The gauge is identified by name and will include the bench's labels
    /// when exported.
    pub fn gauge(&self, name: &'static str) -> Gauge {
        self.recorder.register_gauge(
            &metrics::Key::from_name(name),
            &metrics::Metadata::new("bench", metrics::Level::INFO, None),
        )
    }

    /// Get a histogram handle for recording distributions (e.g., latencies).
    ///
    /// The histogram is identified by name and will include the bench's labels
    /// when exported. Quantiles (p50, p90, p95, p99) are computed at snapshot time.
    pub fn histogram(&self, name: &'static str) -> Histogram {
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

/// A collection of summary metrics to record.
///
/// Written via [`Bench::summarize()`].
pub struct Summary {
    metrics: Vec<(String, f64)>,
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

/// Format a number for display with thousands separators and appropriate precision.
fn format_number(value: f64) -> String {
    let abs = value.abs();

    // Determine appropriate precision based on magnitude
    let (formatted, suffix) = if abs >= 1_000_000_000.0 {
        (value / 1_000_000_000.0, "B")
    } else if abs >= 1_000_000.0 {
        (value / 1_000_000.0, "M")
    } else if abs >= 1_000.0 {
        (value / 1_000.0, "K")
    } else {
        (value, "")
    };

    // Format with appropriate decimal places
    if suffix.is_empty() {
        if abs < 0.01 && abs > 0.0 {
            format!("{:.4}", formatted)
        } else if abs < 1.0 {
            format!("{:.2}", formatted)
        } else if formatted.fract() == 0.0 {
            format!("{:.0}", formatted)
        } else {
            format!("{:.2}", formatted)
        }
    } else {
        format!("{:.2}{}", formatted, suffix)
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

/// Entry point for a benchmark. Implemented by benchmark authors.
#[async_trait::async_trait]
pub trait Benchmark: Send + Sync {
    /// Name of the benchmark.
    fn name(&self) -> &str;

    /// Static labels for this benchmark (constant across all runs).
    fn labels(&self) -> Vec<Label> {
        vec![]
    }

    /// Default parameterizations for this benchmark.
    ///
    /// The framework will use these if no external parameterizations are provided.
    /// Each `Params` represents one parameter combination to run.
    fn default_params(&self) -> Vec<Params> {
        vec![]
    }

    /// Run a single benchmark iteration with the given bench context.
    ///
    /// The framework calls this once for each parameter combination.
    /// Access parameters via `bench.params()` and storage config via `bench.data()`.
    async fn run(&self, bench: Bench) -> anyhow::Result<()>;
}

/// Run a set of benchmarks.
pub async fn run(benchmarks: Vec<Box<dyn Benchmark>>) -> anyhow::Result<()> {
    use common::StorageConfig;
    use common::storage::util::delete;

    let args = Args::parse_args();
    let config = args.load_config()?;

    let benchmarks: Vec<_> = benchmarks
        .into_iter()
        .filter(|b| args.benchmark.as_ref().is_none_or(|name| b.name() == name))
        .collect();

    let duration = args.duration();

    // Track all storage configs used for deferred cleanup
    let mut storage_configs: Vec<StorageConfig> = Vec::new();
    let mut bench_counter = 0;

    for benchmark in benchmarks {
        println!("Running benchmark: {}", benchmark.name());

        // TODO: Allow params to be provided externally (config file, CLI)
        let params = benchmark.default_params();
        for p in params {
            // Create a unique storage path for this benchmark run
            let bench_storage = config
                .data
                .storage
                .with_path_suffix(&bench_counter.to_string());
            storage_configs.push(bench_storage.clone());

            let bench_config = Config {
                data: DataConfig {
                    storage: bench_storage,
                },
                reporter: config.reporter.clone(),
            };

            let bencher = Bencher::new(bench_config, benchmark.name(), benchmark.labels()).await?;
            benchmark.run(bencher.bench(p, duration)).await?;

            bench_counter += 1;
        }
    }

    // Clean up all benchmark data at the end
    if !args.no_cleanup {
        for storage_config in &storage_configs {
            delete(storage_config).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn create_test_bench(reporter: Arc<MemoryReporter>) -> Bench {
        let labels = vec![Label::new("benchmark", "test"), Label::new("env", "ci")];
        // Use a long interval so background reporting doesn't interfere
        Bench::with_reporter(labels, reporter, Duration::from_secs(3600))
    }

    #[tokio::test]
    async fn should_report_counter_on_close() {
        // given
        let reporter = Arc::new(MemoryReporter::new());
        let bench = create_test_bench(Arc::clone(&reporter));
        let counter = bench.counter("requests");

        // when
        counter.increment(10);
        counter.increment(5);
        bench.close().await.unwrap();

        // then
        let series = reporter.series();
        let request_series = series
            .iter()
            .find(|s| s.name() == "requests")
            .expect("should have requests series");

        assert_eq!(request_series.samples[0].value, 15.0);
        assert!(
            request_series
                .labels
                .iter()
                .any(|l| l.name == "benchmark" && l.value == "test")
        );
    }

    #[tokio::test]
    async fn should_report_gauge_on_close() {
        // given
        let reporter = Arc::new(MemoryReporter::new());
        let bench = create_test_bench(Arc::clone(&reporter));
        let gauge = bench.gauge("queue_depth");

        // when
        gauge.set(42.0);
        bench.close().await.unwrap();

        // then
        let series = reporter.series();
        let gauge_series = series
            .iter()
            .find(|s| s.name() == "queue_depth")
            .expect("should have queue_depth series");

        assert_eq!(gauge_series.samples[0].value, 42.0);
    }

    #[tokio::test]
    async fn should_report_histogram_quantiles_on_close() {
        // given
        let reporter = Arc::new(MemoryReporter::new());
        let bench = create_test_bench(Arc::clone(&reporter));
        let histogram = bench.histogram("latency_us");

        // when - record some latency values
        for v in [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0] {
            histogram.record(v);
        }
        bench.close().await.unwrap();

        // then - should have quantile series
        let series = reporter.series();

        // Check for p50 quantile
        let p50 = series
            .iter()
            .find(|s| {
                s.name() == "latency_us"
                    && s.labels
                        .iter()
                        .any(|l| l.name == "quantile" && l.value == "0.5")
            })
            .expect("should have p50 series");
        assert!(p50.samples[0].value >= 40.0 && p50.samples[0].value <= 60.0);

        // Check for count
        let count = series
            .iter()
            .find(|s| s.name() == "latency_us_count")
            .expect("should have count series");
        assert_eq!(count.samples[0].value, 10.0);
    }

    #[tokio::test]
    async fn should_report_summary_metrics() {
        // given
        let reporter = Arc::new(MemoryReporter::new());
        let bench = create_test_bench(Arc::clone(&reporter));

        // when
        bench
            .summarize(
                Summary::new()
                    .add("throughput_mb_s", 123.4)
                    .add("total_bytes", 1_000_000.0),
            )
            .await
            .unwrap();
        bench.close().await.unwrap();

        // then
        let series = reporter.series();

        let throughput = series
            .iter()
            .find(|s| s.name() == "throughput_mb_s")
            .expect("should have throughput series");
        assert_eq!(throughput.samples[0].value, 123.4);
        assert_eq!(throughput.metric_type, Some(MetricType::Gauge));

        let total = series
            .iter()
            .find(|s| s.name() == "total_bytes")
            .expect("should have total_bytes series");
        assert_eq!(total.samples[0].value, 1_000_000.0);
    }

    #[tokio::test]
    async fn should_include_labels_in_all_reported_series() {
        // given
        let reporter = Arc::new(MemoryReporter::new());
        let bench = create_test_bench(Arc::clone(&reporter));

        // when
        bench.counter("ops").increment(1);
        bench.gauge("temp").set(98.6);
        bench
            .summarize(Summary::new().add("result", 42.0))
            .await
            .unwrap();
        bench.close().await.unwrap();

        // then - all series should have the benchmark labels
        let series = reporter.series();
        for s in &series {
            let has_benchmark_label = s
                .labels
                .iter()
                .any(|l| l.name == "benchmark" && l.value == "test");
            let has_env_label = s.labels.iter().any(|l| l.name == "env" && l.value == "ci");
            assert!(
                has_benchmark_label,
                "series {} missing benchmark label",
                s.name()
            );
            assert!(has_env_label, "series {} missing env label", s.name());
        }
    }
}

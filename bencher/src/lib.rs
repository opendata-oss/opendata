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

mod bench;
mod cli;
mod config;
mod metrics;
mod params;
mod reporter;

use std::sync::Arc;

#[cfg(test)]
use reporter::MemoryReporter;
use reporter::{Reporter, TimeSeriesReporter};

use bench::BenchSpec;

// Re-export public types
pub use bench::{Bench, Runner, Summary};
pub use cli::Args;
pub use config::{Config, DataConfig, ReporterConfig};
pub use metrics::BenchRecorder;
pub use params::Params;

// Re-export metrics types for benchmark authors
pub use ::metrics::{Counter, Gauge, Histogram};

// Re-export timeseries model types
pub use timeseries::{Label, MetricType, Sample, Series, SeriesBuilder};

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
                let ts = timeseries::TimeSeriesDb::open(ts_config).await?;
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

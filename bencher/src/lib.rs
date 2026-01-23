//! Minimal benchmark framework for OpenData.
//!
//! This crate provides infrastructure for running integration benchmarks:
//! - Storage initialization and cleanup
//! - Metrics collection and export
//! - Machine-readable output (CSV) for regression analysis
//!
//! # Example
//!
//! ```ignore
//! use bencher::{Benchmark, Bencher, Label, Metric, MetricType};
//!
//! struct Params { record_size: usize }
//!
//! impl From<&Params> for Vec<Label> {
//!     fn from(p: &Params) -> Vec<Label> {
//!         vec![Label::new("record_size", p.record_size.to_string())]
//!     }
//! }
//!
//! struct LogAppendBenchmark;
//!
//! #[async_trait::async_trait]
//! impl Benchmark for LogAppendBenchmark {
//!     fn name(&self) -> &str { "log_append" }
//!
//!     async fn run(&self, bencher: &Bencher) -> anyhow::Result<()> {
//!         for record_size in [64, 256, 1024] {
//!             let bench = bencher.bench(&Params { record_size });
//!             // ... run workload ...
//!             bench.record([
//!                 Metric::new("latency_us", 42.0)
//!                     .with_type(MetricType::Gauge)
//!                     .with_unit("us"),
//!             ]).await?;
//!             bench.close().await?;
//!         }
//!         Ok(())
//!     }
//! }
//! ```

mod cli;
mod config;
mod exporter;

use std::sync::Arc;

use exporter::{Exporter, StubExporter, TimeSeriesExporter};

pub use cli::Args;
pub use config::{Config, DataConfig, OutputFormat, ResultsConfig};

// Re-export timeseries model types
pub use timeseries::{Label, MetricType, Sample, Series, SeriesBuilder};

/// A metric to record.
pub struct Metric {
    /// Name of the metric.
    pub name: String,
    /// Value of the metric.
    pub value: f64,
    /// Type of the metric (gauge, counter, etc.).
    pub metric_type: Option<MetricType>,
    /// Unit of measurement (e.g., "bytes", "seconds").
    pub unit: Option<String>,
}

impl Metric {
    /// Create a new metric with just a name and value.
    pub fn new(name: impl Into<String>, value: f64) -> Self {
        Self {
            name: name.into(),
            value,
            metric_type: None,
            unit: None,
        }
    }

    /// Set the metric type.
    pub fn with_type(mut self, metric_type: MetricType) -> Self {
        self.metric_type = Some(metric_type);
        self
    }

    /// Set the unit of measurement.
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }
}

/// Main bencher struct. Handles storage initialization and metrics export.
pub struct Bencher {
    config: Config,
    exporter: Arc<dyn Exporter>,
    labels: Vec<Label>,
}

impl Bencher {
    /// Create a new Bencher with the given configuration, benchmark name, and static labels.
    async fn new(config: Config, name: &str, labels: Vec<Label>) -> anyhow::Result<Self> {
        let exporter: Arc<dyn Exporter> = match config.results.format {
            OutputFormat::Csv => {
                // TODO: Implement CSV exporter
                Arc::new(StubExporter)
            }
            OutputFormat::Timeseries => {
                let ts_config = timeseries::Config {
                    storage: config.results.to_storage_config(),
                    ..Default::default()
                };
                let ts = timeseries::TimeSeries::open(ts_config).await?;
                Arc::new(TimeSeriesExporter::new(ts))
            }
        };

        let mut all_labels = vec![Label::new("benchmark", name)];
        all_labels.extend(Self::env_labels());
        all_labels.extend(labels);

        Ok(Self {
            config,
            exporter,
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
        {
            if output.status.success() {
                if let Ok(commit) = String::from_utf8(output.stdout) {
                    labels.push(Label::new("commit", commit.trim()));
                }
            }
        }

        // Git branch
        if let Ok(output) = std::process::Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .output()
        {
            if output.status.success() {
                if let Ok(branch) = String::from_utf8(output.stdout) {
                    labels.push(Label::new("branch", branch.trim()));
                }
            }
        }

        labels
    }

    /// Access the data storage configuration.
    pub fn data_config(&self) -> &DataConfig {
        &self.config.data
    }

    /// Start a new benchmark run with the given parameters.
    ///
    /// Parameters are converted to labels and combined with the Bencher's
    /// static labels.
    pub fn bench(&self, params: impl Into<Vec<Label>>) -> Bench {
        let mut labels = self.labels.clone();
        labels.extend(params.into());
        Bench::new(labels, Arc::clone(&self.exporter))
    }
}

/// Represents a single benchmark run with pre-set labels.
///
/// Created via [`Bencher::bench()`]. Records samples to the exporter
/// and finalizes on [`close()`](Bench::close).
pub struct Bench {
    labels: Vec<Label>,
    exporter: Arc<dyn Exporter>,
}

impl Bench {
    fn new(labels: Vec<Label>, exporter: Arc<dyn Exporter>) -> Self {
        Self { labels, exporter }
    }

    /// Record metric samples. Labels were set when the bench was created.
    ///
    /// All samples are recorded with the current timestamp.
    pub async fn record(&self, metrics: impl IntoIterator<Item = Metric>) -> anyhow::Result<()> {
        let series: Vec<Series> = metrics
            .into_iter()
            .map(|m| {
                let mut labels = vec![Label::metric_name(m.name)];
                labels.extend(self.labels.clone());
                Series {
                    labels,
                    metric_type: m.metric_type,
                    unit: m.unit,
                    description: None,
                    samples: vec![Sample::now(m.value)],
                }
            })
            .collect();
        self.exporter.write(series).await
    }

    /// Mark the run as complete.
    pub async fn close(self) -> anyhow::Result<()> {
        self.exporter.flush().await
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

    /// Run the benchmark using the provided bencher.
    async fn run(&self, bencher: &Bencher) -> anyhow::Result<()>;
}

/// Run a set of benchmarks.
pub async fn run(benchmarks: Vec<Box<dyn Benchmark>>) -> anyhow::Result<()> {
    let args = Args::parse_args();
    let config = args.load_config()?;

    let benchmarks: Vec<_> = benchmarks
        .into_iter()
        .filter(|b| {
            args.benchmark
                .as_ref()
                .is_none_or(|name| b.name() == name)
        })
        .collect();

    for benchmark in benchmarks {
        println!("Running benchmark: {}", benchmark.name());
        let bencher = Bencher::new(config.clone(), benchmark.name(), benchmark.labels()).await?;
        benchmark.run(&bencher).await?;
    }

    Ok(())
}

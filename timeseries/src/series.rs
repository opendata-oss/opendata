//! Core data types for OpenData TimeSeries.
//!
//! This module defines the fundamental data structures used in the public API,
//! including labels for series identification, samples for data points, and
//! series for batched ingestion.

use std::time::{SystemTime, UNIX_EPOCH};

/// A label is a key-value pair that identifies a time series.
///
/// # Naming
///
/// - The metric name is stored with key `__name__`
/// - Label names and values can be any valid UTF-8 string
/// - Labels starting with `__` are reserved for internal use
///
/// # Prometheus Compatibility
///
/// For Prometheus compatibility, label names should match `[a-zA-Z_][a-zA-Z0-9_]*`,
/// but this is not enforced by the API.
///
/// # Example
///
/// ```
/// use timeseries::Label;
///
/// let label = Label::new("env", "production");
/// let name = Label::metric_name("http_requests_total");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Label {
    /// The label name (key).
    pub name: String,
    /// The label value.
    pub value: String,
}

impl Label {
    /// Creates a new label with the given name and value.
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    /// Creates a metric name label (`__name__`).
    ///
    /// This is a convenience method for creating the special label that
    /// identifies the metric name.
    pub fn metric_name(name: impl Into<String>) -> Self {
        Self::new("__name__", name)
    }
}

/// A single data point in a time series.
///
/// Samples represent individual measurements at specific points in time.
/// The timestamp is in milliseconds since the Unix epoch, and the value
/// is a 64-bit floating point number.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Timestamp in milliseconds since Unix epoch.
    ///
    /// Uses `i64` (following chrono/protobuf conventions) to support pre-1970 dates.
    pub timestamp_ms: i64,

    /// The sample value.
    ///
    /// May be NaN or ±Inf for special cases.
    pub value: f64,
}

impl Sample {
    /// Creates a new sample with the given timestamp and value.
    pub fn new(timestamp_ms: i64, value: f64) -> Self {
        Self { timestamp_ms, value }
    }

    /// Creates a sample with the current timestamp.
    ///
    /// # Panics
    ///
    /// Panics if the system time is before the Unix epoch.
    pub fn now(value: f64) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before Unix epoch")
            .as_millis() as i64;
        Self::new(timestamp_ms, value)
    }
}

/// Optional metadata about a series.
///
/// Metadata provides additional context about a time series that is not
/// part of its identity. This information is useful for documentation,
/// visualization, and type-aware processing.
#[derive(Debug, Clone, Default)]
pub struct SeriesMetadata {
    /// The type of metric (gauge or counter).
    pub metric_type: Option<MetricType>,

    /// Human-readable description of the metric.
    pub description: Option<String>,

    /// Unit of measurement (e.g., "bytes", "seconds").
    pub unit: Option<String>,
}

/// The type of a metric.
///
/// This enum represents the two fundamental metric types in time series data:
///
/// - **Gauge**: A value that can go up or down (e.g., temperature, memory usage)
/// - **Counter**: A monotonically increasing value (e.g., request count, bytes sent)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    /// A metric that can increase or decrease.
    Gauge,
    /// A monotonically increasing metric.
    Counter,
}

/// A time series with its identifying labels and data points.
///
/// A series represents a single stream of timestamped values identified
/// by a unique set of labels. The labels must include `__name__` to
/// identify the metric.
///
/// # Example
///
/// ```
/// use timeseries::{Series, Label, Sample};
///
/// let series = Series::new(
///     vec![
///         Label::metric_name("http_requests_total"),
///         Label::new("method", "GET"),
///     ],
///     vec![Sample::new(1700000000000, 1.0)],
/// );
///
/// // Or use the builder:
/// let series = Series::builder("http_requests_total")
///     .label("method", "GET")
///     .sample(1700000000000, 1.0)
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct Series {
    /// Labels identifying this series (must include `__name__`).
    pub labels: Vec<Label>,

    /// One or more samples to write.
    pub samples: Vec<Sample>,

    /// Optional metadata about this series.
    pub metadata: Option<SeriesMetadata>,
}

impl Series {
    /// Creates a new series with the given labels and samples.
    pub fn new(labels: Vec<Label>, samples: Vec<Sample>) -> Self {
        Self {
            labels,
            samples,
            metadata: None,
        }
    }

    /// Creates a builder for constructing a series.
    ///
    /// The builder provides a fluent API for creating series with
    /// labels, samples, and optional metadata.
    ///
    /// # Arguments
    ///
    /// * `metric_name` - The metric name (will be stored as `__name__` label).
    pub fn builder(metric_name: impl Into<String>) -> SeriesBuilder {
        SeriesBuilder::new(metric_name)
    }
}

/// Builder for constructing [`Series`] instances.
///
/// Provides a fluent API for creating series with labels, samples,
/// and optional metadata.
#[derive(Debug, Clone)]
pub struct SeriesBuilder {
    labels: Vec<Label>,
    samples: Vec<Sample>,
    metadata: Option<SeriesMetadata>,
}

impl SeriesBuilder {
    fn new(metric_name: impl Into<String>) -> Self {
        Self {
            labels: vec![Label::metric_name(metric_name)],
            samples: Vec::new(),
            metadata: None,
        }
    }

    /// Adds a label to the series.
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push(Label::new(name, value));
        self
    }

    /// Adds a sample with the given timestamp and value.
    pub fn sample(mut self, timestamp_ms: i64, value: f64) -> Self {
        self.samples.push(Sample::new(timestamp_ms, value));
        self
    }

    /// Adds a sample with the current timestamp.
    pub fn sample_now(mut self, value: f64) -> Self {
        self.samples.push(Sample::now(value));
        self
    }

    /// Sets the metadata for the series.
    pub fn metadata(mut self, metadata: SeriesMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Builds the series.
    pub fn build(self) -> Series {
        Series {
            labels: self.labels,
            samples: self.samples,
            metadata: self.metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_label() {
        let label = Label::new("env", "prod");
        assert_eq!(label.name, "env");
        assert_eq!(label.value, "prod");
    }

    #[test]
    fn should_create_metric_name_label() {
        let label = Label::metric_name("http_requests");
        assert_eq!(label.name, "__name__");
        assert_eq!(label.value, "http_requests");
    }

    #[test]
    fn should_create_sample() {
        let sample = Sample::new(1700000000000, 42.5);
        assert_eq!(sample.timestamp_ms, 1700000000000);
        assert_eq!(sample.value, 42.5);
    }

    #[test]
    fn should_create_sample_now() {
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let sample = Sample::now(100.0);
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        assert!(sample.timestamp_ms >= before);
        assert!(sample.timestamp_ms <= after);
        assert_eq!(sample.value, 100.0);
    }

    #[test]
    fn should_build_series_with_builder() {
        let series = Series::builder("cpu_usage")
            .label("host", "server1")
            .sample(1000, 0.5)
            .sample(2000, 0.6)
            .build();

        assert_eq!(series.labels.len(), 2);
        assert_eq!(series.labels[0], Label::metric_name("cpu_usage"));
        assert_eq!(series.labels[1], Label::new("host", "server1"));
        assert_eq!(series.samples.len(), 2);
        assert_eq!(series.samples[0].value, 0.5);
        assert_eq!(series.samples[1].value, 0.6);
    }
}

//! Core data types for OpenData TimeSeries.
//!
//! This module defines the fundamental data structures used in the public API,
//! including labels for series identification, samples for data points, and
//! series for batched ingestion.

use crate::util::hour_bucket_in_epoch_minutes;
use std::time::{SystemTime, UNIX_EPOCH};

/// Series ID (unique within a time bucket)
pub(crate) type SeriesId = u32;
/// Series fingerprint (hash of label set)
pub(crate) type SeriesFingerprint = u128;
/// Time bucket (minutes since UNIX epoch)
pub(crate) type BucketStart = u32;
/// Time bucket size (1-15, exponential: 1=1h, 2=2h, 3=4h, 4=8h, etc. = 2^(n-1) hours)
pub(crate) type BucketSize = u8;

// Re-export common RecordTag for internal use
pub(crate) use common::serde::key_prefix::RecordTag;

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
//TODO(rohan): make this impl PartialOrd/Ord so it can be sorted easily
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
        Self {
            timestamp_ms,
            value,
        }
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Temporality {
    Cumulative,
    Delta,
    Unspecified,
}

/// The type of a metric.
///
/// This enum represents the two fundamental metric types in time series data:
///
/// - **Gauge**: A value that can go up or down (e.g., temperature, memory usage)
/// - **Sum**: A monotonically increasing value (e.g., request count, bytes sent)
/// - **Histogram**: A value that can go up or down (e.g., temperature, memory usage)
/// - **ExponentialHistogram**: A value that can go up or down (e.g., temperature, memory usage)
/// - **Summary**: A value that can go up or down (e.g., temperature, memory usage)
#[derive(Clone, Copy, Debug)]
pub enum MetricType {
    Gauge,
    Sum {
        monotonic: bool,
        temporality: Temporality,
    },
    Histogram {
        temporality: Temporality,
    },
    ExponentialHistogram {
        temporality: Temporality,
    },
    Summary,
}

/// A time series with its identifying labels and data points.
///
/// A series represents a single stream of timestamped values.
///
/// # Identity and Metadata
///
/// A series is uniquely identified by its labels, which include the metric name
/// stored as `__name__`. The `metric_type`, `unit`, and `description` fields are
/// metadata with last-write-wins semantics.
///
/// # Example
///
/// ```
/// use timeseries::{Series, Label, Sample};
///
/// let series = Series::new(
///     "http_requests_total",
///     vec![Label::new("method", "GET")],
///     vec![Sample::new(1700000000000, 1.0)],
/// );
///
/// // Or use the builder:
/// let series = Series::builder("http_requests_total")
///     .label("method", "GET")
///     .sample(1700000000000, 1.0)
///     .build();
///
/// assert_eq!(series.name(), "http_requests_total");
/// ```
#[derive(Debug, Clone)]
pub struct Series {
    /// Labels identifying this series, including `__name__` for the metric name.
    pub labels: Vec<Label>,

    // --- Metadata (last-write-wins) ---
    /// The type of metric (gauge or counter).
    pub metric_type: Option<MetricType>,

    /// Unit of measurement (e.g., "bytes", "seconds").
    pub unit: Option<String>,

    /// Human-readable description of the metric.
    pub description: Option<String>,

    // --- Data ---
    /// One or more samples to write.
    pub samples: Vec<Sample>,
}

impl Series {
    /// Creates a new series with the given name, labels, and samples.
    ///
    /// The metric name is stored as a `__name__` label and prepended to the
    /// provided labels.
    ///
    /// # Panics
    ///
    /// Panics if `labels` contains a `__name__` label. The metric name should
    /// only be provided via the `name` parameter.
    pub fn new(name: impl Into<String>, labels: Vec<Label>, samples: Vec<Sample>) -> Self {
        assert!(
            !labels.iter().any(|l| l.name == "__name__"),
            "labels must not contain __name__; use the name parameter instead"
        );
        let mut all_labels = Vec::with_capacity(labels.len() + 1);
        all_labels.push(Label::metric_name(name));
        all_labels.extend(labels);
        Self {
            labels: all_labels,
            metric_type: None,
            unit: None,
            description: None,
            samples,
        }
    }

    /// Returns the metric name (value of the `__name__` label).
    ///
    /// # Panics
    ///
    /// Panics if the series was constructed without a `__name__` label.
    /// This should never happen when using the provided constructors.
    pub fn name(&self) -> &str {
        self.labels
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| l.value.as_str())
            .expect("Series must have a __name__ label")
    }

    /// Creates a builder for constructing a series.
    ///
    /// The builder provides a fluent API for creating series with
    /// labels, samples, and metadata fields.
    ///
    /// # Arguments
    ///
    /// * `name` - The metric name.
    pub fn builder(name: impl Into<String>) -> SeriesBuilder {
        SeriesBuilder::new(name)
    }
}

/// Builder for constructing [`Series`] instances.
///
/// Provides a fluent API for creating series with labels, samples,
/// and metadata fields.
#[derive(Debug, Clone)]
pub struct SeriesBuilder {
    labels: Vec<Label>,
    metric_type: Option<MetricType>,
    unit: Option<String>,
    description: Option<String>,
    samples: Vec<Sample>,
}

impl SeriesBuilder {
    fn new(name: impl Into<String>) -> Self {
        Self {
            labels: vec![Label::metric_name(name)],
            metric_type: None,
            unit: None,
            description: None,
            samples: Vec::new(),
        }
    }

    /// Adds a label to the series.
    ///
    /// # Panics
    ///
    /// Panics if `name` is `__name__`. The metric name should only be provided
    /// via [`Series::builder()`].
    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        assert!(
            name != "__name__",
            "cannot add __name__ label; use Series::builder(name) instead"
        );
        self.labels.push(Label::new(name, value));
        self
    }

    /// Sets the metric type.
    pub fn metric_type(mut self, metric_type: MetricType) -> Self {
        self.metric_type = Some(metric_type);
        self
    }

    /// Sets the unit of measurement.
    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// Sets the description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
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

    /// Builds the series.
    pub fn build(self) -> Series {
        Series {
            labels: self.labels,
            metric_type: self.metric_type,
            unit: self.unit,
            description: self.description,
            samples: self.samples,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TimeBucket {
    pub(crate) start: BucketStart,
    pub(crate) size: BucketSize,
}

impl TimeBucket {
    pub(crate) fn hour(start: BucketStart) -> Self {
        Self { start, size: 1 }
    }

    pub(crate) fn round_to_hour(time: SystemTime) -> crate::error::Result<Self> {
        let bucket = hour_bucket_in_epoch_minutes(time)?;
        Ok(Self::hour(bucket))
    }

    pub(crate) fn size_in_mins(&self) -> u32 {
        (self.size.pow(2) * 60) as u32
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

        assert_eq!(series.name(), "cpu_usage");
        // labels includes __name__ + host
        assert_eq!(series.labels.len(), 2);
        assert_eq!(series.labels[0], Label::metric_name("cpu_usage"));
        assert_eq!(series.labels[1], Label::new("host", "server1"));
        assert_eq!(series.samples.len(), 2);
        assert_eq!(series.samples[0].value, 0.5);
        assert_eq!(series.samples[1].value, 0.6);
    }

    #[test]
    fn should_create_series_with_new() {
        let series = Series::new(
            "http_requests",
            vec![Label::new("method", "GET")],
            vec![Sample::new(1000, 1.0)],
        );

        assert_eq!(series.name(), "http_requests");
        // labels includes __name__ + method
        assert_eq!(series.labels.len(), 2);
        assert_eq!(series.labels[0], Label::metric_name("http_requests"));
        assert_eq!(series.labels[1], Label::new("method", "GET"));
    }

    #[test]
    #[should_panic(expected = "labels must not contain __name__")]
    fn should_panic_when_new_labels_contain_name() {
        Series::new(
            "http_requests",
            vec![Label::metric_name("other_name")],
            vec![],
        );
    }

    #[test]
    #[should_panic(expected = "cannot add __name__ label")]
    fn should_panic_when_builder_adds_name_label() {
        Series::builder("http_requests")
            .label("__name__", "other_name")
            .build();
    }
}

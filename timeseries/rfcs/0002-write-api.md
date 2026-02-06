# RFC 0002: TimeSeriesDb Write API

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC proposes a public write API for OpenData-TimeSeries, which is modeled after the `LogDb` API in the log data system, and `Db` in SlateDb. The `TimeSeriesDb` struct provides a clean, high-level interface with a simple model for time series data.

## Motivation

It would be useful to expose a low-level API to write time series data and enable ingest flexibility. Although OTEL is likely to be the primary data model for ingestion, it has a fairly complicated representation with support for higher-level metric types such as histograms and summaries. OpenData-TimeSeries aims to be simple in its core. We can isolate higher-level types in a separate OTEL module without compromising a simpler internal representation (similar to Prometheus). In principle, this allows OpenData-TimeSeries to accommodate other representations of time series data including those closer to user applications.    

## Goals

- Provide a simple, Prometheus-like data model for time series ingestion
- Expose a single batched write API with atomic semantics
- Abstract over internal bucketing and storage details
- Follow patterns established by the `LogDb` API and SlateDb's `Db`
- Support a separate OTEL mapping module (with optional Prometheus-like label validation)

## Non-Goals

- **Read API** - A `TimeSeriesDbReader` interface will be defined in a follow-up RFC
- **HTTP endpoints** - Remote Write and OTLP endpoints are out of scope
- **PromQL** - Query language support is out of scope
- **Strict Prometheus validation** - We intentionally allow lenient label naming

## Design

### Data Model

The public data model closely follows Prometheus conventions.

#### Labels

Labels are key-value pairs that, along with the metric name, identify a time series.

```rust
/// A label is a key-value pair that identifies a time series.
///
/// # Naming
/// - The metric name is stored with key `__name__`
/// - Label names and values can be any valid UTF-8 string
/// - Labels starting with `__` are reserved for internal use
///
/// # Prometheus Compatibility
/// For Prometheus compatibility, label names should match `[a-zA-Z_][a-zA-Z0-9_]*`,
/// but this is not enforced by the API.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self;

    /// Creates a metric name label (`__name__`).
    pub fn metric_name(name: impl Into<String>) -> Self;
}
```

#### Samples

A sample is a single data point in a time series.

```rust
/// A single data point in a time series.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Timestamp in milliseconds since Unix epoch.
    /// Uses i64 (following chrono/protobuf conventions) to support pre-1970 dates.
    pub timestamp_ms: i64,

    /// The value (typically f64, but may be NaN or ±Inf)
    pub value: f64,
}

impl Sample {
    pub fn new(timestamp_ms: i64, value: f64) -> Self;

    /// Create a sample with the current timestamp
    pub fn now(value: f64) -> Self;
}
```

#### Series

A series combines a metric name, labels, and samples. This is the primary unit of ingestion.

```rust
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
/// # Metric Name
///
/// The metric name is required and stored as the `__name__` label. All constructors
/// require a name parameter which is automatically prepended to the labels.
/// Use `name()` to retrieve the metric name.
#[derive(Debug, Clone)]
pub struct Series {
    /// Labels identifying this series, including `__name__` for the metric name.
    pub labels: Vec<Label>,

    // --- Metadata (last-write-wins) ---
    /// The type of metric (gauge or counter)
    pub metric_type: Option<MetricType>,

    /// Unit of measurement (e.g., "bytes", "seconds")
    pub unit: Option<String>,

    /// Human-readable description of the metric
    pub description: Option<String>,

    // --- Data ---
    /// One or more samples to write
    pub samples: Vec<Sample>,
}

impl Series {
    /// Creates a new series. The name is stored as a `__name__` label.
    ///
    /// # Panics
    /// Panics if `labels` contains a `__name__` label. The metric name should
    /// only be provided via the `name` parameter.
    pub fn new(name: impl Into<String>, labels: Vec<Label>, samples: Vec<Sample>) -> Self;

    /// Returns the metric name (value of the `__name__` label).
    pub fn name(&self) -> &str;

    /// Builder-style construction
    pub fn builder(name: impl Into<String>) -> SeriesBuilder;
}

pub struct SeriesBuilder { ... }

impl SeriesBuilder {
    pub fn label(self, name: impl Into<String>, value: impl Into<String>) -> Self;
    pub fn metric_type(self, metric_type: MetricType) -> Self;
    pub fn unit(self, unit: impl Into<String>) -> Self;
    pub fn description(self, description: impl Into<String>) -> Self;
    pub fn sample(self, timestamp_ms: i64, value: f64) -> Self;
    pub fn sample_now(self, value: f64) -> Self;
    pub fn build(self) -> Series;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Counter,
}
```

#### Design Philosophy: Minimal Core

OpenData emphasizes minimalism and composition. The core data model is the smallest useful primitive: a `Series` is simply labels (including a `__name__` label for the metric name) and timestamped float values. There is no native representation of histograms, summaries, or other complex metric types.

Richer abstractions are composed on top. For example, an OTEL mapping module would decompose histograms into multiple simple series (`_bucket`, `_sum`, `_count`) at ingestion time. Prometheus takes a similar approach for the same reasons.

In a similar vein, Prometheus adds alphanumeric validation on label names in order to keep PromQL simple. However, nothing in our underlying timeseries data system actually relies on this assumption, and we can therefore layer it on top. Future systems may allow more localization even in the core APIs, so we aim to keep the internal representation as permissive as possible.  

This design has several benefits:

1. **Minimal** - The storage layer handles one data type; fewer edge cases, simpler implementation
2. **Composable** - Higher-level abstractions can be built on top without constraining the core
3. **Permissive** - The low-level API accepts data without strict validation; stricter policies (like Prometheus label naming) are applied at higher layers

Users who need histogram semantics can either:
- Use the OTEL module, which decomposes histograms into series following Prometheus conventions
- Implement their own decomposition for custom histogram formats

### TimeSeriesDb API

#### Construction

```rust
/// A time series database for storing and querying metrics.
///
/// `TimeSeriesDb` provides a high-level API for ingesting Prometheus-style
/// metrics. It handles internal details like time bucketing, series
/// deduplication, and storage management automatically.
///
/// # Example
///
/// ```rust
/// use opendata_timeseries::{TimeSeriesDb, Config, Series};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let ts = TimeSeriesDb::open(Config::default()).await?;
///
///     let series = Series::builder("http_requests_total")
///         .label("method", "GET")
///         .label("status", "200")
///         .sample_now(1.0)
///         .build();
///
///     ts.write(vec![series]).await?;
///     Ok(())
/// }
/// ```
pub struct TimeSeriesDb {
    // Internal Tsdb - not exposed
}

impl TimeSeriesDb {
    /// Open or create a time series database with the given configuration.
    pub async fn open(config: Config) -> Result<Self>;
}
```

#### Configuration

```rust
/// Configuration for a TimeSeriesDb instance.
#[derive(Debug, Clone)]
pub struct Config {
    /// Storage backend configuration
    pub storage: StorageConfig,

    /// How often to flush data to durable storage
    pub flush_interval: Duration,

    /// Maximum age of data to retain
    pub retention: Option<Duration>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig::default(),
            flush_interval: Duration::from_secs(60),
            retention: None,
        }
    }
}
```

#### Write API

The write API consists of a single batched method with atomic semantics:

```rust
impl TimeSeriesDb {
    /// Write one or more time series.
    ///
    /// This is the primary write method. It accepts a batch of series,
    /// each containing labels and one or more samples. The method returns
    /// when the data has been accepted for ingestion (but not necessarily
    /// flushed to durable storage).
    ///
    /// # Atomicity
    ///
    /// This operation is atomic: either all series in the batch are accepted,
    /// or none are. This matches the behavior of `LogDb::append()`.
    ///
    /// # Series Identification
    ///
    /// A series is uniquely identified by its labels, which include the metric
    /// name stored as `__name__`. Metadata fields (`metric_type`, `unit`,
    /// `description`) use last-write-wins semantics.
    ///
    /// # Ordering
    ///
    /// Samples within a series should be in timestamp order, but out-of-order
    /// samples are accepted. Duplicate timestamps for the same series will
    /// overwrite previous values.
    ///
    /// # Example
    ///
    /// ```rust
    /// let series = vec![
    ///     Series::builder("cpu_usage")
    ///         .label("host", "server1")
    ///         .sample(1700000000000, 0.75)
    ///         .sample(1700000001000, 0.82)
    ///         .build(),
    ///     Series::builder("cpu_usage")
    ///         .label("host", "server2")
    ///         .sample(1700000000000, 0.45)
    ///         .build(),
    /// ];
    ///
    /// ts.write(series).await?;
    /// ```
    pub async fn write(&self, series: Vec<Series>) -> Result<()>;

    /// Write with custom options.
    ///
    /// Allows control over durability guarantees and other write behaviors.
    pub async fn write_with_options(
        &self,
        series: Vec<Series>,
        options: WriteOptions,
    ) -> Result<()>;

    /// Force flush all pending data to durable storage.
    ///
    /// Normally data is flushed according to `flush_interval`, but this
    /// method can be used to ensure durability immediately.
    pub async fn flush(&self) -> Result<()>;
}

/// Options for the write operation.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Wait for data to be flushed to durable storage before returning.
    /// Default: false (return immediately after buffering)
    pub await_durable: bool,
}
```

#### Reader Access (Placeholder)

```rust
impl TimeSeriesDb {
    /// Get a read-only view of the time series database.
    ///
    /// The reader provides query access without write capabilities.
    /// See RFC-XXXX for the TimeSeriesDbReader API.
    pub fn reader(&self) -> TimeSeriesDbReader;
}

/// Read-only view of a time series database.
///
/// API to be defined in a future RFC.
pub struct TimeSeriesDbReader {
    // ...
}
```

### OTEL Mapping Module (Future work)

A separate module handles conversion from OpenTelemetry data models to the TimeSeriesDb data model, following the approach used by Prometheus. This module is responsible for:

1. **Type decomposition** - Converting complex OTEL types (histograms, summaries) into multiple simple series
2. **Label sanitization** - Ensuring label names conform to Prometheus conventions
3. **Attribute mapping** - Converting OTEL resource/scope/metric attributes to labels

```rust
/// Hypothetical Module for converting OpenTelemetry metrics to TimeSeries format.
#[cfg(feature = "otel")]
pub mod otel {
    use super::{Series, Label, Sample, MetricType};

    /// Convert OTEL metrics to Series for ingestion.
    ///
    /// # Type Decomposition
    ///
    /// Complex OTEL metric types are decomposed into simple gauge/counter series:
    ///
    /// - OTEL `Gauge` → Single series (MetricType::Gauge)
    /// - OTEL `Sum` (monotonic) → Single series (MetricType::Counter)
    /// - OTEL `Sum` (non-monotonic) → Single series (MetricType::Gauge)
    /// - OTEL `Histogram` → Multiple series: `_bucket` (per le), `_sum`, `_count`
    /// - OTEL `ExponentialHistogram` → Multiple series following native histogram conventions
    /// - OTEL `Summary` → Multiple series: `{quantile="..."}`, `_sum`, `_count`
    ///
    /// # Label Sanitization
    ///
    /// OTEL attribute names are sanitized to conform to Prometheus conventions:
    /// - Non-alphanumeric characters (except `_`) are replaced with `_`
    /// - Names starting with a digit are prefixed with `_`
    ///
    /// # Attribute Mapping
    ///
    /// - OTEL resource attributes → labels with `resource_` prefix (if enabled)
    /// - OTEL scope attributes → labels with `scope_` prefix (if enabled)
    /// - OTEL metric attributes → labels directly
    /// - Metric name → `__name__` label
    pub fn from_metrics_request(
        request: &ExportMetricsServiceRequest,
        config: &OtelConfig,
    ) -> Result<Vec<Series>>;

    /// Convert a single OTEL metric to Series.
    pub fn from_metric(
        metric: &Metric,
        resource_labels: &[Label],
        scope_labels: &[Label],
        config: &OtelConfig,
    ) -> Result<Vec<Series>>;

    /// Configuration for OTEL to Prometheus mapping.
    #[derive(Debug, Clone, Default)]
    pub struct OtelConfig {
        /// Whether to include resource attributes as labels
        pub include_resource_attrs: bool,

        /// Whether to include scope attributes as labels
        pub include_scope_attrs: bool,

        /// Custom label mappings (OTEL attr name → Prometheus label name)
        pub label_mappings: HashMap<String, String>,
    }
}
```

### Comparison with LogDb API

| Aspect | LogDb | TimeSeriesDb |
|--------|-----|------------|
| Configuration | `Config` | `Config` |
| Primary write method | `append(Vec<Record>)` | `write(Vec<Series>)` |
| Options variant | `append_with_options()` | `write_with_options()` |
| Data unit | `Record` (key + value bytes) | `Series` (labels + samples) |
| Identification | `key: Bytes` | `labels` (including `__name__`) |
| Sequencing | Global sequence number | Timestamp (ms) |
| Durability control | `WriteOptions::await_durable` | `WriteOptions::await_durable` |
| Reader access | `fn reader() -> LogDbReader` | `fn reader() -> TimeSeriesDbReader` |

## Alternatives

### Strict Prometheus Label Validation

**Alternative**: Enforce Prometheus naming conventions (`[a-zA-Z_][a-zA-Z0-9_]*`) for labels at the API level.

**Rationale for rejection**: Strict validation reduces flexibility without significant benefit. Users ingesting from non-Prometheus sources may have legitimate label names that don't conform. The OTEL module can sanitize labels when Prometheus compatibility is required.

**Trade-offs**: Lenient validation may allow data that causes issues when queried via PromQL or exported to Prometheus-compatible systems.

### Partial Write Semantics

**Alternative**: Accept valid series from a batch even if some fail validation.

```rust
pub struct WriteResult {
    pub accepted: usize,
    pub rejected: Vec<(usize, Error)>,
}
```

**Rationale for rejection**: Atomic semantics are simpler to reason about and match existing APIs (`LogDb::append()`, SlateDB batch writes). Partial writes complicate error handling.

**Future consideration**: A separate `write_partial()` method could be introduced if needed.

### Native Histogram and Summary Types

**Alternative**: Include `Histogram` and `Summary` variants in `MetricType` and support native histogram data structures in `Sample`.

```rust
pub enum MetricType {
    Gauge,
    Counter,
    Histogram,  // Native support
    Summary,    // Native support
}

pub enum SampleValue {
    Float(f64),
    Histogram { buckets: Vec<(f64, u64)>, sum: f64, count: u64 },
    Summary { quantiles: Vec<(f64, f64)>, sum: f64, count: u64 },
}
```

**Rationale for rejection**: OpenData emphasizes minimalism and composition. The core API should be the smallest useful primitive; richer abstractions are composed on top. A series of labeled float values is the minimal building block for time series data.

Complex types like histograms are decomposed into simple series at higher layers (the OTEL module). This keeps the core:

- **Minimal** - One data type, one storage format, fewer edge cases
- **Composable** - Higher layers can implement any histogram scheme without core changes
- **Permissive** - No opinions about bucket boundaries, quantiles, or naming conventions

**Trade-offs**: Users must use the OTEL module (or implement their own decomposition) to ingest histogram/summary data.

## Updates

| Date       | Description |
|------------|-------------|
| 2025-12-19 | Initial draft |
| 2025-12-29 | Separate `name` field on Series; flatten SeriesMetadata into Series; clarify identity fields |
| 2025-12-29 | Rename `TimeSeriesConfig` to `Config` for consistency with Rust conventions |
| 2026-01-06 | Store metric name as `__name__` label instead of separate field; add `name()` accessor method |

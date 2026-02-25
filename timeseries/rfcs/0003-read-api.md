# RFC 0003: TimeSeriesDb Read API

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC defines the read API for `TimeSeriesDb`, using PromQL as the query language. The API provides embedded query methods that return typed Rust results. This complements the write API defined in RFC 0002 to make `TimeSeriesDb` a self-contained embedded time series database.

## Motivation

RFC 0002 establishes `TimeSeriesDb` as the central write entry point with a Prometheus-style data model. The read side needs a corresponding embedded API. PromQL is the natural choice — it is the standard query language for Prometheus-style metrics, and the existing implementation already evaluates PromQL expressions against the storage layer.

Today the PromQL evaluation is coupled to the HTTP server through the `PromqlRouter` trait, which uses service-oriented request/response types (string status fields, JSON-style containers). Lifting the query interface onto `TimeSeriesDb` itself — with idiomatic Rust types and `Result` returns — makes it usable as a standard embedded database without any transport layer.

### Why an Embedded API

Following the pattern established by `LogDb` and SlateDb, `TimeSeriesDb` is designed as an embedded database — a library you link into your process, not a service you connect to. This is a deliberate architectural choice in OpenData. The embedded API is the foundation; the HTTP service layer (RFC 0004) is optional transport built on top.

An embedded read API enables use cases that are awkward or impossible with a service-only model:

- **Ad hoc analysis of production data.** A script or tool can open a `TimeSeriesDb` directly against production storage and run PromQL queries without standing up a server, configuring ports, or routing traffic. This is useful for one-off debugging, incident investigation, or exploring historical trends — you get full query capabilities in a few lines of code with no infrastructure overhead.

- **Embedded alerting and automation.** An agent running alongside the database can evaluate PromQL expressions in-process to detect anomalies or trigger actions. There is no network hop, no serialization cost, and no dependency on a running HTTP server.

- **Testing.** Unit and integration tests can write series and query them back in a single process without starting a server. This makes tests faster, more deterministic, and easier to write.

- **Custom tooling.** CLI tools, data pipelines, and export jobs can read directly from storage. A compaction tool might scan series metadata; a migration script might query a time range and write results elsewhere. These are natural library consumers, not HTTP clients.

## Goals

- Expose PromQL query capabilities as methods on `TimeSeriesDb`
- Use idiomatic Rust types: plain arguments, domain result types, `Result<T, E>` error handling
- Support instant queries, range queries, series discovery, label discovery, and metadata lookup
- Keep the API independent of any transport layer (HTTP, gRPC, etc.)

## Non-Goals

- **HTTP endpoints** — See RFC 0004 (the service layer translates to Prometheus HTTP API conventions)
- **PromQL language extensions** — Standard PromQL only
- **Write API** — See RFC 0002
- **Raw sample iteration** — Low-level access to stored samples may be useful but is out of scope for this RFC

## Design

### Instant Query

```rust
impl TimeSeriesDb {
    /// Evaluate a PromQL expression at a single point in time.
    ///
    /// Returns the matching series with their values at the evaluation time.
    /// If `time` is `None`, evaluates at the current time.
    ///
    /// # Example
    ///
    /// ```rust
    /// let results = tsdb.query("rate(http_requests_total[5m])", None).await?;
    /// for sample in &results {
    ///     println!("{}: {} = {}", sample.metric_name(), sample.timestamp_ms, sample.value);
    /// }
    /// ```
    pub async fn query(
        &self,
        query: &str,
        time: Option<SystemTime>,
    ) -> Result<Vec<InstantSample>>;
}
```

### Range Query

```rust
impl TimeSeriesDb {
    /// Evaluate a PromQL expression over a time range.
    ///
    /// Returns the matching series with values at each step interval
    /// from `start` to `end`.
    ///
    /// # Example
    ///
    /// ```rust
    /// let results = tsdb.query_range(
    ///     "rate(http_requests_total[5m])",
    ///     start,
    ///     end,
    ///     Duration::from_secs(15),
    /// ).await?;
    ///
    /// for series in &results {
    ///     println!("{}:", series.metric_name());
    ///     for (ts, val) in &series.samples {
    ///         println!("  {} = {}", ts, val);
    ///     }
    /// }
    /// ```
    pub async fn query_range(
        &self,
        query: &str,
        start: SystemTime,
        end: SystemTime,
        step: Duration,
    ) -> Result<Vec<RangeSample>>;
}
```

### Series Discovery

```rust
impl TimeSeriesDb {
    /// Find all series matching the given label matchers.
    ///
    /// Each matcher follows PromQL selector syntax (e.g., `{job="prometheus"}`).
    /// At least one matcher is required. Returns the label set for each
    /// matching series.
    ///
    /// # Example
    ///
    /// ```rust
    /// let series = tsdb.series(
    ///     &[r#"{__name__=~"http_.*"}"#],
    ///     None,
    ///     None,
    /// ).await?;
    /// ```
    pub async fn series(
        &self,
        matchers: &[&str],
        start: Option<SystemTime>,
        end: Option<SystemTime>,
    ) -> Result<Vec<Labels>>;

    /// List all label names.
    ///
    /// Optionally filtered to series matching the given matchers
    /// within the given time range.
    pub async fn labels(
        &self,
        matchers: Option<&[&str]>,
        start: Option<SystemTime>,
        end: Option<SystemTime>,
    ) -> Result<Vec<String>>;

    /// List all values for a given label name.
    ///
    /// Optionally filtered to series matching the given matchers
    /// within the given time range.
    pub async fn label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        start: Option<SystemTime>,
        end: Option<SystemTime>,
    ) -> Result<Vec<String>>;
}
```

### Metadata

```rust
impl TimeSeriesDb {
    /// Return metric metadata for all or a specific metric.
    ///
    /// Metadata is populated from the `metric_type`, `unit`, and
    /// `description` fields of `Series` during writes (RFC 0002).
    ///
    /// # Example
    ///
    /// ```rust
    /// let all_metadata = tsdb.metadata(None).await?;
    /// let specific = tsdb.metadata(Some("http_requests_total")).await?;
    /// ```
    pub async fn metadata(
        &self,
        metric: Option<&str>,
    ) -> Result<Vec<MetricMetadata>>;
}
```

### Result Types

```rust
/// A single series value at a point in time.
#[derive(Debug, Clone)]
pub struct InstantSample {
    /// Labels identifying the series, including `__name__`.
    pub labels: Labels,

    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: i64,

    /// The value.
    pub value: f64,
}

impl InstantSample {
    /// Returns the metric name (value of the `__name__` label).
    pub fn metric_name(&self) -> &str;
}

/// A series with values over a time range.
#[derive(Debug, Clone)]
pub struct RangeSample {
    /// Labels identifying the series, including `__name__`.
    pub labels: Labels,

    /// Timestamp-value pairs, ordered by time.
    pub samples: Vec<(i64, f64)>,
}

impl RangeSample {
    /// Returns the metric name (value of the `__name__` label).
    pub fn metric_name(&self) -> &str;
}

/// An ordered set of labels identifying a series.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Labels(Vec<Label>);

impl Labels {
    /// Get the value of a label by name.
    pub fn get(&self, name: &str) -> Option<&str>;

    /// Returns the metric name (value of the `__name__` label).
    pub fn metric_name(&self) -> &str;

    /// Iterate over all labels.
    pub fn iter(&self) -> impl Iterator<Item = &Label>;
}

/// Metadata for a metric.
#[derive(Debug, Clone)]
pub struct MetricMetadata {
    /// The metric name.
    pub metric_name: String,

    /// The type of metric (gauge, counter).
    pub metric_type: Option<MetricType>,

    /// Human-readable description.
    pub description: Option<String>,

    /// Unit of measurement (e.g., "bytes", "seconds").
    pub unit: Option<String>,
}
```

### Error Handling

```rust
/// Errors returned by read operations.
#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    /// The PromQL expression could not be parsed.
    #[error("invalid query: {0}")]
    InvalidQuery(String),

    /// The query timed out.
    #[error("query timed out")]
    Timeout,

    /// An internal error occurred during evaluation.
    #[error("execution error: {0}")]
    Execution(String),
}
```

The service layer (RFC 0004) maps these to Prometheus-compatible HTTP responses:

| Variant | HTTP Status | Prometheus `error_type` |
|---|---|---|
| `InvalidQuery` | 400 | `bad_data` |
| `Timeout` | 422 | `timeout` |
| `Execution` | 422 | `execution` |

### Relationship to Existing Implementation

The existing `PromqlRouter` trait defines service-oriented operations with request/response types. The implementation on `Tsdb` today:

1. Parses PromQL expressions using the `promql_parser` crate
2. Resolves time ranges to internal storage buckets
3. Uses an `Evaluator` with a `CachedQueryReader` to evaluate expressions
4. Supports vector selectors, matrix selectors, binary operations, aggregations, and function calls (`rate`, `sum`, `avg`, etc.)

This RFC promotes those operations to the public `TimeSeriesDb` API with idiomatic Rust signatures. The `PromqlRouter` trait and its request/response types become internal to the service layer (RFC 0004), which translates between HTTP conventions and this embedded API.

### Comparison with LogDb

| Aspect | LogDb | TimeSeriesDb |
|--------|-------|--------------|
| Primary read | `reader.read(offset)` → `Result<Record>` | `tsdb.query(expr, time)` → `Result<Vec<InstantSample>>` |
| Range read | `reader.scan(start..end)` → `Result<Vec<Record>>` | `tsdb.query_range(expr, start, end, step)` → `Result<Vec<RangeSample>>` |
| Discovery | `reader.scan()` | `tsdb.series()`, `tsdb.labels()`, `tsdb.label_values()` |
| Query language | None (key/offset based) | PromQL |
| Error handling | `Result<T, SlateDbError>` | `Result<T, QueryError>` |

## Alternatives Considered

### Service-oriented request/response types

An earlier draft used `QueryRequest`/`QueryResponse` structs with string status fields and optional error messages, mirroring the Prometheus HTTP API format. This is appropriate for a service boundary but awkward for an embedded API — callers shouldn't have to check `response.status == "success"` in Rust. Plain method signatures with `Result` are more natural and let the service layer handle HTTP conventions.

### Separate TimeSeriesDbReader type

Following `LogDb`'s pattern, reads could go through a separate `TimeSeriesDbReader` obtained via `tsdb.reader()`. However, `LogDb` uses a reader because it provides a consistent snapshot view with a fixed offset. PromQL queries are inherently point-in-time (the `time` parameter determines the evaluation point), so a separate reader handle adds indirection without benefit. Direct methods on `TimeSeriesDb` are simpler.

### Raw series iteration instead of PromQL

An alternative is to expose low-level iterators over stored series and samples, letting callers build their own query logic. This is useful for some use cases (export, compaction) but insufficient as the primary read API. PromQL provides filtering, aggregation, rate calculation, and arithmetic — capabilities that virtually all time series consumers need. Raw iteration could be added later as a complementary API.

## Updates

| Date | Description |
|---|---|
| 2026-02-25 | Initial draft |

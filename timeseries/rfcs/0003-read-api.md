# RFC 0003: TimeSeriesDb Read API

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC defines the read API for `TimeSeriesDb`, using PromQL as the query language. The API provides embedded query methods that return typed Rust results. This complements the write API defined in RFC 0002 to make `TimeSeriesDb` a self-contained embedded time series database.

## Motivation

RFC 0002 establishes `TimeSeriesDb` as the central write entry point with a Prometheus-style data model. The read side needs a corresponding embedded API. PromQL is the natural choice — it is the standard query language for Prometheus-style metrics, and the existing implementation already evaluates PromQL expressions against the storage layer internally.

Today the PromQL evaluation is coupled to the HTTP server through the `PromqlRouter` trait, which uses service-oriented request/response types (string status fields, JSON-style containers). This RFC promotes those operations to the public `TimeSeriesDb` API with idiomatic Rust signatures. The service layer (RFC 0004) becomes a thin HTTP transport on top.

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
- Keep the API independent of any transport layer

## Non-Goals

- **HTTP endpoints** — See RFC 0004
- **PromQL language extensions** — Standard PromQL only
- **Write API** — See RFC 0002
- **Raw sample iteration** — Low-level access to stored samples may be added later

## Design

### Query API

```rust
impl TimeSeriesDb {
    /// Evaluate a PromQL expression at a single point in time.
    ///
    /// If `time` is `None`, evaluates at the current time.
    ///
    /// ```rust
    /// let results = tsdb.query("rate(http_requests_total[5m])", None).await?;
    /// for sample in &results {
    ///     println!("{}: {}", sample.metric_name(), sample.value);
    /// }
    /// ```
    pub async fn query(
        &self,
        query: &str,
        time: Option<SystemTime>,
    ) -> Result<Vec<InstantSample>, QueryError>;

    /// Evaluate a PromQL expression over a time range.
    ///
    /// Returns the matching series with values at each step interval
    /// from `start` to `end`.
    ///
    /// ```rust
    /// let results = tsdb.query_range(
    ///     "rate(http_requests_total[5m])",
    ///     start, end, Duration::from_secs(15),
    /// ).await?;
    /// ```
    pub async fn query_range(
        &self,
        query: &str,
        start: SystemTime,
        end: SystemTime,
        step: Duration,
    ) -> Result<Vec<RangeSample>, QueryError>;
}
```

### Discovery API

```rust
impl TimeSeriesDb {
    /// Find all series matching the given label matchers.
    ///
    /// Each matcher follows PromQL selector syntax (e.g., `{job="prometheus"}`).
    /// At least one matcher is required. The `range` parameter filters by time
    /// — use `..` for all time.
    pub async fn series(
        &self,
        matchers: &[&str],
        range: impl RangeBounds<SystemTime>,
    ) -> Result<Vec<Labels>, QueryError>;

    /// List all label names, optionally filtered by matchers and time range.
    /// Use `..` for all time.
    pub async fn labels(
        &self,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> Result<Vec<String>, QueryError>;

    /// List all values for a given label name, optionally filtered by matchers
    /// and time range. Use `..` for all time.
    pub async fn label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        range: impl RangeBounds<SystemTime>,
    ) -> Result<Vec<String>, QueryError>;

    /// Return metric metadata for all or a specific metric.
    ///
    /// Metadata is populated from the `metric_type`, `unit`, and
    /// `description` fields of `Series` during writes (RFC 0002).
    pub async fn metadata(
        &self,
        metric: Option<&str>,
    ) -> Result<Vec<MetricMetadata>, QueryError>;
}
```

### Result Types

```rust
/// A single series value at a point in time.
#[derive(Debug, Clone)]
pub struct InstantSample {
    pub labels: Labels,
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A series with values over a time range.
#[derive(Debug, Clone)]
pub struct RangeSample {
    pub labels: Labels,
    /// Timestamp-value pairs, ordered by time.
    pub samples: Vec<(i64, f64)>,
}

/// An ordered set of labels identifying a series.
///
/// Wraps `Vec<Label>` (from RFC 0002) with accessors for common operations.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Labels(Vec<Label>);

impl Labels {
    pub fn get(&self, name: &str) -> Option<&str>;
    pub fn metric_name(&self) -> &str;
    pub fn iter(&self) -> impl Iterator<Item = &Label>;
}

/// Metadata for a metric.
#[derive(Debug, Clone)]
pub struct MetricMetadata {
    pub metric_name: String,
    pub metric_type: Option<MetricType>,
    pub description: Option<String>,
    pub unit: Option<String>,
}
```

### Error Handling

```rust
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

The result types defined here (`InstantSample`, `RangeSample`, `Labels`, `MetricMetadata`) replace the internal evaluator types and become the canonical query result representation. The existing `PromqlRouter` trait reduces to a thin adapter that serializes these types into the Prometheus JSON wire format (status/error envelopes, string-encoded values) for HTTP transport.

### Separate TimeSeriesDbReader type

Following `LogDb`'s pattern, reads could go through a separate `TimeSeriesDbReader` obtained via `tsdb.reader()`. However, `LogDb` uses a reader because it provides a consistent snapshot view with a fixed offset. PromQL queries are inherently point-in-time (the `time` parameter determines the evaluation point), so a separate reader handle adds indirection without benefit.

### Raw series iteration instead of PromQL

An alternative is to expose low-level iterators over stored series and samples, letting callers build their own query logic. This is useful for some use cases (export, compaction) but insufficient as the primary read API. PromQL provides filtering, aggregation, rate calculation, and arithmetic — capabilities that virtually all time series consumers need. Raw iteration could be added later as a complementary API.

## Updates

| Date | Description |
|---|---|
| 2026-02-25 | Initial draft |

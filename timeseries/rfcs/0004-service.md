# RFC 0004: TimeSeries Service

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC defines the TimeSeries Service — an HTTP server that exposes `TimeSeriesDb` (RFCs 0002 and 0003) over the network. It covers Prometheus-compatible query endpoints, remote-write ingestion, and OpenTelemetry metrics ingestion. Each protocol is independently feature-gated. The service is a thin transport layer: HTTP handlers deserialize requests, delegate to `TimeSeriesDb`, and serialize responses.

## Motivation

RFCs 0002 and 0003 define `TimeSeriesDb` as a self-contained embedded database with write and PromQL query APIs. To be useful in a deployed system, these need to be accessible over the network. The service layer adds HTTP transport without introducing new data model or query concepts — it maps protocol-specific formats to and from the core API.

The service supports three protocols:

- **PromQL HTTP API** — Prometheus-compatible query endpoints, enabling Grafana and other standard tools
- **Remote Write** — Prometheus remote-write protocol for receiving metrics from Prometheus servers and compatible agents
- **OTLP/HTTP** — OpenTelemetry metrics ingestion, converting rich OTEL types to the Prometheus-style series model via `OtelSeriesBuilder`

Each is feature-gated so deployments only pull in the dependencies they need.

## Goals

- Expose `TimeSeriesDb` query and write APIs over HTTP
- Provide Prometheus-compatible query endpoints
- Support Prometheus remote-write ingestion
- Support OTLP/HTTP metrics ingestion with `OtelSeriesBuilder` for type decomposition
- Feature-gate each protocol independently

## Non-Goals

- **OTLP gRPC transport** — HTTP is simpler, runs on the same port, and is well-supported by OTEL SDKs. gRPC can be added later.
- **Traces and logs** — Only metrics are in scope
- **New query or storage semantics** — The service is a transport layer only

## Design

### Architecture Overview

```
  Grafana / PromQL clients          Prometheus / agents           OTEL SDK / Collector
          │                                │                              │
   GET /api/v1/query               POST /api/v1/write              POST /v1/metrics
   GET /api/v1/query_range         (snappy + protobuf)            (protobuf)
   GET /api/v1/series                      │                              │
   GET /api/v1/labels                      │                              │
          │                                │                              │
          └────────────────┬───────────────┴──────────────┬───────────────┘
                           │                              │
                           ▼                              ▼
                  ┌─────────────────┐           ┌──────────────────┐
                  │  Axum HTTP      │           │ OtelSeriesBuilder │
                  │  Server         │           │ (feature: otel)   │
                  │                 │           └────────┬─────────┘
                  └────────┬────────┘                    │
                           │                    Vec<Series>
                           ▼                              │
                  ┌─────────────────────────────┐         │
                  │       TimeSeriesDb          │◄────────┘
                  │  write() / query() / ...    │
                  └─────────────────────────────┘
```

### Feature Flags

```toml
[features]
http-server = ["dep:axum", "dep:tokio", "dep:tower", ...]
remote-write = ["http-server", "dep:prost", "dep:snap"]
otel = ["dep:opentelemetry-proto", "dep:prost"]
```

| Feature | What it enables | Dependencies |
|---|---|---|
| `http-server` | PromQL query endpoints, server lifecycle, metrics endpoint | axum, tokio, tower |
| `remote-write` | `POST /api/v1/write` endpoint | prost, snap (implies `http-server`) |
| `otel` | `OtelSeriesBuilder` + `POST /v1/metrics` endpoint (when `http-server` also enabled) | opentelemetry-proto, prost |

The `otel` feature is usable without `http-server` — the `OtelSeriesBuilder` can be used standalone for programmatic conversion (e.g., replaying OTEL data from `LogDb`). The HTTP endpoint is only registered when both `otel` and `http-server` are active.

### HTTP Server

The server uses Axum and follows the existing implementation pattern.

#### Server Lifecycle

```rust
#[cfg(feature = "http-server")]
pub struct TimeSeriesServer {
    tsdb: TimeSeriesDb,
    config: ServerConfig,
}

pub struct ServerConfig {
    /// HTTP listen port. Default: 9090.
    pub port: u16,
}

impl TimeSeriesServer {
    pub fn new(tsdb: TimeSeriesDb, config: ServerConfig) -> Self;

    /// Run the server until SIGINT or SIGTERM.
    ///
    /// Flushes TimeSeriesDb on shutdown.
    pub async fn run(self) -> Result<()>;
}
```

#### Route Registration

```rust
fn build_router(tsdb: Arc<Tsdb>, metrics: Arc<Metrics>) -> Router {
    let app = Router::new()
        // PromQL query endpoints
        .route("/api/v1/query", get(handle_query).post(handle_query))
        .route("/api/v1/query_range", get(handle_query_range).post(handle_query_range))
        .route("/api/v1/series", get(handle_series).post(handle_series))
        .route("/api/v1/labels", get(handle_labels))
        .route("/api/v1/label/{name}/values", get(handle_label_values))
        .route("/api/v1/metadata", get(handle_metadata))
        // Health and metrics
        .route("/-/healthy", get(handle_healthy))
        .route("/-/ready", get(handle_ready))
        .route("/metrics", get(handle_metrics));

    // Remote-write endpoint
    #[cfg(feature = "remote-write")]
    let app = app.route("/api/v1/write", post(handle_remote_write));

    // OTEL endpoint
    #[cfg(feature = "otel")]
    let app = app.route("/v1/metrics", post(handle_otel_metrics));

    app
}
```

### PromQL HTTP Endpoints

These endpoints are thin wrappers that deserialize HTTP parameters into the request types from RFC 0003 and serialize the responses as JSON.

| Endpoint | Method | Maps to |
|---|---|---|
| `/api/v1/query` | GET, POST | `tsdb.query(QueryRequest)` |
| `/api/v1/query_range` | GET, POST | `tsdb.query_range(QueryRangeRequest)` |
| `/api/v1/series` | GET, POST | `tsdb.series(SeriesRequest)` |
| `/api/v1/labels` | GET | `tsdb.labels(LabelsRequest)` |
| `/api/v1/label/{name}/values` | GET | `tsdb.label_values(LabelValuesRequest)` |
| `/api/v1/metadata` | GET | `tsdb.metadata(MetadataRequest)` |

All responses follow the Prometheus HTTP API format:

```json
{
  "status": "success",
  "data": { ... }
}
```

### Remote Write Endpoint

The remote-write handler accepts Prometheus remote-write protocol requests. Gated behind the `remote-write` feature flag.

| Aspect | Value |
|---|---|
| Path | `/api/v1/write` |
| Method | POST |
| Content-Type | `application/x-protobuf` |
| Content-Encoding | `snappy` |
| Request body | Snappy-compressed protobuf `WriteRequest` |
| Success status | `204 No Content` |

The handler decompresses, decodes the protobuf `WriteRequest`, extracts `Vec<Series>` from the flat label/sample pairs, and calls `tsdb.write()`.

### OTEL Metrics Ingest

OTEL support has two parts: `OtelSeriesBuilder` (the conversion layer) and the HTTP endpoint.

#### OtelSeriesBuilder

```rust
/// Decomposes OpenTelemetry metrics into Prometheus-style Series.
///
/// # Example
///
/// ```rust
/// let builder = OtelSeriesBuilder::new(OtelConfig::default());
/// let series = builder.build(&otel_request)?;
/// tsdb.write(series).await?;
/// ```
#[cfg(feature = "otel")]
pub struct OtelSeriesBuilder {
    config: OtelConfig,
}

impl OtelSeriesBuilder {
    pub fn new(config: OtelConfig) -> Self;

    /// Decompose an OTLP export request into Series.
    pub fn build(
        &self,
        request: &ExportMetricsServiceRequest,
    ) -> Result<Vec<Series>>;

    /// Decompose a batch of ResourceMetrics directly.
    pub fn build_from_resource_metrics(
        &self,
        resource_metrics: &[ResourceMetrics],
    ) -> Result<Vec<Series>>;
}

#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// Include resource attributes as labels. Default: true.
    pub include_resource_attrs: bool,

    /// Include scope attributes as labels. Default: true.
    pub include_scope_attrs: bool,
}
```

#### Type Decomposition

All OTEL metric types are decomposed into flat `Series` with `MetricType::Gauge` or `MetricType::Counter` as defined in RFC 0002. This follows the same approach used by Prometheus.

The conversion walks the OTLP hierarchy: `ResourceMetrics` → `ScopeMetrics` → `Metric` → data points

At each level, attributes are collected as labels:

| OTEL level | Label treatment |
|---|---|
| Resource attributes | Included as labels (e.g., `service_name`, `host_name`) |
| Scope attributes | Included as labels (e.g., `otel_scope_name`) |
| Metric attributes | Included as labels directly |
| Metric name | Stored as `__name__` label |

**Gauge** → Single series, `MetricType::Gauge`

**Sum (monotonic, cumulative)** → Single series with `_total` suffix, `MetricType::Counter`

**Sum (non-monotonic)** → Single series, `MetricType::Gauge`

**Sum (delta)** → Dropped with warning

**Histogram** → Multiple series: `_bucket` (per `le`), `_sum`, `_count` — all `MetricType::Counter`

**Exponential Histogram** → Converted to explicit boundaries, then same as Histogram

**Summary** → Per-quantile series (`MetricType::Gauge`) + `_sum`, `_count` (`MetricType::Counter`)

#### OTLP HTTP Endpoint

The endpoint follows the [OTLP/HTTP specification](https://opentelemetry.io/docs/specs/otlp/#otlphttp). Registered when both `otel` and `http-server` features are enabled.

```rust
#[cfg(feature = "otel")]
pub(crate) async fn handle_otel_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, StatusCode> {
    // 1. Validate Content-Type
    // 2. Decode protobuf ExportMetricsServiceRequest
    // 3. builder.build(&request) → Vec<Series>
    // 4. tsdb.write(series)
    // 5. Return ExportMetricsServiceResponse
}
```

| Aspect | Value |
|---|---|
| Path | `/v1/metrics` |
| Method | POST |
| Content-Type | `application/x-protobuf` |
| Request body | Protobuf-encoded `ExportMetricsServiceRequest` |
| Response body | Protobuf-encoded `ExportMetricsServiceResponse` |
| Success status | `200 OK` |

#### Dependencies

```toml
[dependencies]
opentelemetry-proto = { version = "0.28", optional = true, features = ["metrics", "gen-prost"] }
prost = { version = "0.13", optional = true }
```

The `opentelemetry-proto` crate provides pre-generated prost types for all OTLP protobuf messages. We use `gen-prost` (not `gen-tonic`) since we only need the message types, not gRPC service definitions.

### Comparison of Ingest Protocols

| Aspect | Remote Write | OTLP/HTTP |
|---|---|---|
| Feature flag | `remote-write` | `otel` |
| Endpoint | `POST /api/v1/write` | `POST /v1/metrics` |
| Encoding | Snappy + protobuf | Protobuf |
| Source data | Flat labels + samples | Rich hierarchy (Resource → Scope → Metric → DataPoints) |
| Type info | None (default to Gauge) | Full (Gauge, Sum, Histogram, etc.) |
| Decomposition | 1:1 mapping | Complex (histograms → N series) |
| Write path | Parse → `tsdb.write()` | Parse → `builder.build()` → `tsdb.write()` |

## Alternatives Considered

### OTLP gRPC instead of HTTP

An earlier draft used gRPC via tonic as the primary OTLP transport. This requires a separate port (4317), a tonic dependency, and HTTP/2 support. OTLP/HTTP is simpler — it runs on the existing Axum server, shares the same port, and requires no new networking dependencies. Most OTEL SDKs and collectors support both transports. gRPC can be added later as a separate feature flag if needed.

### OtelTimeSeriesDb wrapper instead of OtelSeriesBuilder

An earlier draft wrapped `TimeSeriesDb` in an `OtelTimeSeriesDb` that accepted OTEL requests and wrote decomposed series internally. This couples the conversion and storage steps. The builder approach keeps them explicit — `build()` produces series, then the caller writes them — and composes more naturally with the existing `TimeSeriesDb::write()` API.

### Separate server per protocol

Instead of adding routes to a single Axum server, each protocol could run its own server on a different port. This adds operational complexity (multiple ports to configure, monitor, and load-balance) with no clear benefit. A single server with feature-gated routes is simpler.

## Open Questions

1. **Unit suffix handling** — Should the OTEL `unit` field be appended to the metric name (e.g., `request_duration_seconds`) or stored only as metadata? Prometheus convention appends it.

2. **Scope name as label** — Should `otel_scope_name` and `otel_scope_version` be included as labels by default, or only when explicitly configured?

## Updates

| Date | Description |
|---|---|
| 2026-02-24 | Initial draft (as RFC 0003: OTLP Metrics Ingest) |
| 2026-02-25 | Restructured as TimeSeries Service RFC covering HTTP server, remote-write, and OTEL ingest |

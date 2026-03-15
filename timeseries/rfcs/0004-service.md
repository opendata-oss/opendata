# RFC 0004: TimeSeries Service

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC defines the TimeSeries Service — an HTTP server that exposes `TimeSeriesDb` (RFCs 0002 and 0003) over the network. It covers Prometheus-compatible query endpoints, remote-write ingestion, and OpenTelemetry metrics ingestion. Each protocol is independently feature-gated. The service is a thin transport layer: HTTP handlers deserialize requests, delegate to `TimeSeriesDb`, and serialize responses.

## Motivation

RFCs 0002 and 0003 define `TimeSeriesDb` as a self-contained embedded database with write and PromQL query APIs. To be useful in a deployed system, these need to be accessible over the network. The service layer adds HTTP transport without introducing new data model or query concepts — it maps protocol-specific formats to and from the core API.

## Goals

- Expose `TimeSeriesDb` query and write APIs over HTTP
- Provide Prometheus-compatible query and remote-write endpoints
- Support OTLP/HTTP metrics ingestion with `OtelConverter` for type decomposition
- Feature-gate each protocol independently

## Non-Goals

- **OTLP gRPC transport** — HTTP is simpler, runs on the same port, and is well-supported by OTEL SDKs. gRPC can be added later.
- **Traces and logs** — Only metrics are in scope
- **New query or storage semantics** — The service is a transport layer only

## Design

### Architecture Overview

```
  Grafana / PromQL         Prometheus / agents        OTEL SDK / Collector
          │                       │                           │
   GET /api/v1/query       POST /api/v1/write          POST /v1/metrics
   GET /api/v1/query_range  (snappy + protobuf)         (protobuf)
   GET /api/v1/series             │                           │
   ...                            │                     ┌─────┴──────┐
          │                       │                     │    Otel-   │
          │                       │                     │  Converter │
          │                       │                     └─────┬──────┘
          │                       │                     Vec<Series>
          └───────────┬───────────┴───────────┬───────────────┘
                      │                       │
                      ▼                       ▼
             tsdb.query()              tsdb.write()
                      │                       │
                      └───────────┬───────────┘
                                  ▼
                          ┌──────────────┐
                          │ TimeSeriesDb │
                          └──────────────┘
```

### Feature Flags

```toml
[features]
http-server = ["dep:axum", "dep:tokio", "dep:tower", ...]
remote-write = ["http-server", "dep:prost", "dep:snap"]
otel = ["dep:opentelemetry-proto", "dep:prost"]
```

| Feature | What it enables | Key dependencies |
|---|---|---|
| `http-server` | PromQL endpoints, server lifecycle | axum, tokio, tower |
| `remote-write` | `POST /api/v1/write` (implies `http-server`) | prost, snap |
| `otel` | `OtelConverter`; `POST /v1/metrics` when `http-server` also enabled | opentelemetry-proto, prost |

The `otel` feature is usable without `http-server` — the converter can be used standalone for programmatic conversion (e.g., replaying OTEL data from `LogDb`).

### HTTP Server

The service uses Axum, listens on a single port (default 9090), and handles graceful shutdown (SIGINT/SIGTERM) with a TSDB flush.

#### Endpoints

**PromQL query endpoints** (always available with `http-server`):

| Endpoint | Method | Maps to |
|---|---|---|
| `/api/v1/query` | GET, POST | `tsdb.query(expr, time)` |
| `/api/v1/query_range` | GET, POST | `tsdb.query_range(expr, start..=end, step)` |
| `/api/v1/series` | GET, POST | `tsdb.series(matchers, start..end)` |
| `/api/v1/labels` | GET | `tsdb.labels(matchers, start..end)` |
| `/api/v1/label/{name}/values` | GET | `tsdb.label_values(name, matchers, start..end)` |
| `/api/v1/metadata` | GET | `tsdb.metadata(metric)` |

Each handler deserializes HTTP parameters, calls the corresponding `TimeSeriesDb` method (RFC 0003), and wraps the result in the Prometheus JSON format (`{"status": "success", "data": ...}`). `QueryError` variants map to HTTP status codes:

| `QueryError` | HTTP Status | Prometheus `error_type` |
|---|---|---|
| `InvalidQuery` | 400 | `bad_data` |
| `Timeout` | 422 | `timeout` |
| `Execution` | 422 | `execution` |

**Remote-write endpoint** (feature: `remote-write`):

| Aspect | Value |
|---|---|
| Path | `POST /api/v1/write` |
| Encoding | Snappy-compressed protobuf `WriteRequest` |
| Success | `204 No Content` |

The handler decompresses, decodes the `WriteRequest` (a flat list of label/sample pairs), converts to `Vec<Series>`, and calls `tsdb.write()`.

**OTLP endpoint** (feature: `otel` + `http-server`):

| Aspect | Value |
|---|---|
| Path | `POST /v1/metrics` |
| Encoding | OTLP/HTTP protobuf (`application/x-protobuf`) `ExportMetricsServiceRequest` |
| Success | `200 OK` with protobuf `ExportMetricsServiceResponse` (including `partial_success` when applicable) |

The handler decodes the request, calls `OtelConverter::convert()` to decompose OTEL metrics into `Vec<Series>`, and calls `tsdb.write()`.

OTLP/HTTP response semantics follow the OTLP protocol spec:
- On success, return `200 OK` with `ExportMetricsServiceResponse`.
- On partial acceptance, still return `200 OK` and populate `partial_success`.
- On failures (`4xx`/`5xx`), return OTLP protobuf error details (`google.rpc.Status`) with the same content type as the request.

### OtelConverter

The converter decomposes OpenTelemetry metrics into `Vec<Series>` suitable for `TimeSeriesDb::write()`. It is gated behind the `otel` feature and has no dependency on the HTTP server.

```rust
#[cfg(feature = "otel")]
pub struct OtelConverter {
    config: OtelConfig,
}

impl OtelConverter {
    pub fn new(config: OtelConfig) -> Self;

    /// Convert an OTLP export request into Series.
    pub fn convert(
        &self,
        request: &ExportMetricsServiceRequest,
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

The converter walks the OTLP hierarchy (`ResourceMetrics` → `ScopeMetrics` → `Metric` → data points) and collects attributes as labels at each level. The mapping follows the [OTLP Prometheus compatibility spec](https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/) — including unit suffix normalization and scope labels (`otel_scope_name`, `otel_scope_version`, `otel_scope_schema_url`). OTEL metric types map to Prometheus-style series as follows:

| OTEL type | Decomposition | MetricType |
|---|---|---|
| Gauge | Single series | Gauge |
| Sum (monotonic, cumulative) | Single series with `_total` suffix | Counter |
| Sum (non-monotonic) | Single series | Gauge |
| Sum (delta, monotonic) | SHOULD be converted to cumulative counter series; if not feasible, MAY be dropped | Counter |
| Sum (delta, non-monotonic) | MUST be dropped | — |
| Histogram (cumulative) | `_bucket` (per `le`), `_sum`, `_count` | Counter |
| Histogram (delta) | SHOULD be aggregated to cumulative; otherwise MUST be dropped | Counter |
| Exponential Histogram (cumulative) | SHOULD map to Native Histogram; MAY be converted to fixed buckets | Counter |
| Exponential Histogram (delta) | MUST be dropped | — |
| Summary | Per-quantile series + `_sum`, `_count` | Gauge / Counter |

#### Dependencies

```toml
[dependencies]
opentelemetry-proto = { version = "0.28", optional = true, features = ["metrics", "gen-tonic-messages"] }
prost = { version = "0.13", optional = true }
```

We use `gen-tonic-messages` (not `gen-tonic`) — only message types are needed, not gRPC transport. The `gen-tonic-messages` feature pulls in `tonic` and `prost` for message definitions without enabling `tonic/transport`.

## Alternatives Considered

### OTLP gRPC instead of HTTP

An earlier draft used gRPC via tonic as the primary OTLP transport. This requires a separate port (4317), a tonic dependency, and HTTP/2 support. OTLP/HTTP is simpler — it runs on the existing Axum server, shares the same port, and requires no new networking dependencies. gRPC can be added later as a separate feature flag.

### OtelTimeSeriesDb wrapper instead of OtelConverter

An earlier draft wrapped `TimeSeriesDb` in an `OtelTimeSeriesDb` that accepted OTEL requests and wrote decomposed series internally. This couples the conversion and storage steps. The converter approach keeps them explicit — `convert()` produces series, then the caller writes them — and composes naturally with `TimeSeriesDb::write()`.

### Separate server per protocol

Each protocol could run its own server on a different port. This adds operational complexity with no clear benefit. A single server with feature-gated routes is simpler.

## Updates

| Date | Description |
|---|---|
| 2026-02-24 | Initial draft (as RFC 0003: OTLP Metrics Ingest) |
| 2026-02-25 | Restructured as TimeSeries Service RFC covering HTTP server, remote-write, and OTEL ingest |
| 2026-03-05 | Aligned OTLP endpoint and OTLP-to-Prometheus mapping language with OTLP and compatibility specs; added references |

## References

- OTLP protocol: <https://opentelemetry.io/docs/specs/otlp/>
- OTLP exporter endpoint defaults (`/v1/metrics` for OTLP/HTTP): <https://opentelemetry.io/docs/specs/otel/protocol/exporter/>
- OTLP-to-Prometheus/OpenMetrics compatibility mapping: <https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/>
- Prometheus remote-write 1.0 spec: <https://prometheus.io/docs/specs/prw/remote_write_spec/>

# RFC 0003: Service Layer

**Status**: Draft

**Authors**:
- OpenData Contributors

## Summary

This RFC defines the Service Layer for OpenData—a framework that enables OpenData databases (Log, TSDB, Vector) to be consumed in various deployment configurations, from fully embedded to fully service-oriented. The design introduces role abstractions (Writer, Reader, Compactor), composition patterns for different topologies, a shared ServiceRuntime for HTTP infrastructure, protocol bindings for network APIs, and a catalog abstraction for future sharding support.

## Motivation

OpenData databases today exist as Rust libraries with embedded APIs. While this works well for Rust applications, it creates barriers for broader adoption:

1. **Language barriers**: Python, Go, Java, and other language users cannot consume OpenData databases without Rust bindings or FFI.

2. **Deployment flexibility**: Users cannot easily split writers and readers for independent scaling, run compaction in separate processes, or deploy in serverless environments.

3. **Operational overhead**: Each database has its own server implementation (e.g., PromQL server for TSDB), leading to inconsistent operational patterns.

4. **Progressive enhancement path**: There's no clear path from "embed in my Rust app" to "run as a distributed service" without significant rearchitecture.

The Service Layer addresses these gaps by providing a common framework that enables any OpenData database to be consumed via network APIs while preserving the embedded mode as a first-class option.

## Goals

- Define role abstractions (Writer, Reader, Compactor) that decompose each database into composable pieces
- Define composition patterns for the primary deployment configurations (Embedded, Single-Server, Scaled Readers, Serverless)
- Define a ServiceRuntime that provides shared HTTP infrastructure across all databases
- Define how protocol bindings register database-specific endpoints with the ServiceRuntime
- Sketch a Catalog abstraction for future sharding support
- Define a configuration model that supports both programmatic and declarative configuration

## Non-Goals (left for future RFCs)

- Detailed sharding implementation (key ranges, rebalancing, split/merge)
- Specific protocol implementations (Kafka wire protocol, gRPC bindings)
- Authentication and authorization framework
- Multi-tenancy isolation
- Distributed transactions across databases

## Design

### Overview

The Service Layer introduces a three-tier architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                      Protocol Bindings                          │
│  ┌──────────┐  ┌──────────────┐  ┌────────────────────────┐    │
│  │   Log    │  │    PromQL    │  │       Vector           │    │
│  │   HTTP   │  │    HTTP      │  │       HTTP/gRPC        │    │
│  └────┬─────┘  └──────┬───────┘  └───────────┬────────────┘    │
└───────┼───────────────┼──────────────────────┼──────────────────┘
        │               │                      │
┌───────┼───────────────┼──────────────────────┼──────────────────┐
│       │         ServiceRuntime               │                  │
│  ┌────┴─────────────────────────────────────┴────┐              │
│  │  HTTP Server │ Health │ Metrics │ Middleware  │              │
│  └────┬─────────────────────────────────────┬────┘              │
└───────┼─────────────────────────────────────┼───────────────────┘
        │                                     │
┌───────┼─────────────────────────────────────┼───────────────────┐
│       │           Role Abstractions         │                   │
│  ┌────┴────┐    ┌──────────┐    ┌──────────┴──┐                 │
│  │ Writer  │    │  Reader  │    │  Compactor  │                 │
│  └────┬────┘    └────┬─────┘    └──────┬──────┘                 │
└───────┼──────────────┼─────────────────┼────────────────────────┘
        │              │                 │
┌───────┼──────────────┼─────────────────┼────────────────────────┐
│       └──────────────┼─────────────────┘                        │
│                      │                                          │
│              Database Core (Log, Tsdb, VectorDb)                │
│                      │                                          │
│              ┌───────┴───────┐                                  │
│              │    Storage    │                                  │
│              │   (SlateDB)   │                                  │
│              └───────┬───────┘                                  │
│                      │                                          │
│              ┌───────┴───────┐                                  │
│              │ Object Store  │                                  │
│              │     (S3)      │                                  │
│              └───────────────┘                                  │
└─────────────────────────────────────────────────────────────────┘
```

Each layer has a specific responsibility:

- **Role Abstractions**: Decompose database operations into Writer, Reader, and Compactor roles
- **ServiceRuntime**: Provides shared HTTP server infrastructure, health checks, metrics
- **Protocol Bindings**: Database-specific HTTP/gRPC endpoints that translate network requests to role operations

### Role Abstractions

Each OpenData database can be decomposed into three logical roles that can be instantiated independently or together:

- **Writer**: Receives writes, buffers in memory, flushes to storage
- **Reader**: Serves read queries from storage snapshots
- **Compactor**: Background process that optimizes data layout

#### Core Role Traits

```rust
use async_trait::async_trait;

/// Marker trait for all database roles
pub trait DatabaseRole: Send + Sync + 'static {
    /// Human-readable role name for logging and metrics
    fn role_name(&self) -> &'static str;

    /// Health check for the role
    fn is_healthy(&self) -> bool { true }
}

/// Writer role - receives and buffers writes
#[async_trait]
pub trait Writer: DatabaseRole {
    /// Database-specific write request type
    type WriteRequest: Send;

    /// Database-specific write response type
    type WriteResponse: Send;

    /// Process a write request
    async fn write(&self, request: Self::WriteRequest) -> Result<Self::WriteResponse, WriteError>;

    /// Flush buffered data to storage
    async fn flush(&self) -> Result<(), WriteError>;

    /// Get current buffer statistics
    fn buffer_stats(&self) -> BufferStats;
}

/// Reader role - serves read queries
#[async_trait]
pub trait Reader: DatabaseRole {
    /// Database-specific query request type
    type QueryRequest: Send;

    /// Database-specific query response type
    type QueryResponse: Send;

    /// Execute a read query
    async fn query(&self, request: Self::QueryRequest) -> Result<Self::QueryResponse, ReadError>;

    /// Refresh view of underlying storage
    async fn refresh(&self) -> Result<(), ReadError>;
}

/// Compactor role - optimizes storage layout
#[async_trait]
pub trait Compactor: DatabaseRole {
    /// Trigger compaction
    async fn compact(&self) -> Result<CompactionResult, CompactionError>;

    /// Get compaction statistics
    fn compaction_stats(&self) -> CompactionStats;

    /// Check if compaction is needed
    fn needs_compaction(&self) -> bool;
}
```

#### Database-Specific Implementations

Each database provides concrete implementations of these traits:

**Log Database**:

```rust
// log/src/roles.rs

/// Writer role for the Log database
pub struct LogWriter {
    inner: Log,
}

impl LogWriter {
    pub async fn open(config: Config) -> Result<Self, Error> {
        let inner = Log::open(config).await?;
        Ok(Self { inner })
    }

    /// Direct access to the underlying Log for embedded use
    pub fn as_log(&self) -> &Log {
        &self.inner
    }
}

impl DatabaseRole for LogWriter {
    fn role_name(&self) -> &'static str { "log-writer" }
}

#[async_trait]
impl Writer for LogWriter {
    type WriteRequest = LogWriteRequest;
    type WriteResponse = LogWriteResponse;

    async fn write(&self, request: Self::WriteRequest) -> Result<Self::WriteResponse, WriteError> {
        let count = request.records.len();
        self.inner.append(request.records).await?;
        Ok(LogWriteResponse { records_written: count })
    }

    async fn flush(&self) -> Result<(), WriteError> {
        Ok(()) // Log flushes through storage layer
    }

    fn buffer_stats(&self) -> BufferStats {
        BufferStats::default()
    }
}

/// Reader role for the Log database
pub struct LogReaderRole {
    inner: LogReader,
}

impl LogReaderRole {
    pub async fn open(config: Config) -> Result<Self, Error> {
        let inner = LogReader::open(config).await?;
        Ok(Self { inner })
    }

    pub fn as_reader(&self) -> &LogReader {
        &self.inner
    }
}

impl DatabaseRole for LogReaderRole {
    fn role_name(&self) -> &'static str { "log-reader" }
}

#[async_trait]
impl Reader for LogReaderRole {
    type QueryRequest = LogQueryRequest;
    type QueryResponse = LogQueryResponse;

    async fn query(&self, request: Self::QueryRequest) -> Result<Self::QueryResponse, ReadError> {
        let seq_range = request.start_sequence.unwrap_or(0)..request.end_sequence.unwrap_or(u64::MAX);
        let iter = self.inner.scan(request.key, seq_range);
        // Collect results...
        Ok(LogQueryResponse { entries, has_more })
    }

    async fn refresh(&self) -> Result<(), ReadError> {
        Ok(())
    }
}
```

**TimeSeries Database**:

```rust
// timeseries/src/roles.rs

pub struct TsdbWriter {
    tsdb: Arc<Tsdb>,
    flush_interval_secs: u64,
}

impl TsdbWriter {
    pub fn new(tsdb: Arc<Tsdb>, flush_interval_secs: u64) -> Self {
        Self { tsdb, flush_interval_secs }
    }
}

impl DatabaseRole for TsdbWriter {
    fn role_name(&self) -> &'static str { "tsdb-writer" }
}

#[async_trait]
impl Writer for TsdbWriter {
    type WriteRequest = TsdbWriteRequest;
    type WriteResponse = TsdbWriteResponse;

    async fn write(&self, request: Self::WriteRequest) -> Result<Self::WriteResponse, WriteError> {
        let samples: u64 = request.series.iter().map(|s| s.samples.len() as u64).sum();
        self.tsdb.ingest_samples(request.series, self.flush_interval_secs).await?;
        Ok(TsdbWriteResponse { samples_written: samples })
    }

    async fn flush(&self) -> Result<(), WriteError> {
        self.tsdb.flush(self.flush_interval_secs).await?;
        Ok(())
    }

    fn buffer_stats(&self) -> BufferStats {
        BufferStats::default()
    }
}

pub struct TsdbReaderRole {
    tsdb: Arc<Tsdb>,
}

impl DatabaseRole for TsdbReaderRole {
    fn role_name(&self) -> &'static str { "tsdb-reader" }
}
```

**Vector Database**:

```rust
// vector/src/roles.rs

pub struct VectorWriter {
    db: VectorDb,
}

impl VectorWriter {
    pub async fn open(config: Config) -> Result<Self, Error> {
        let db = VectorDb::open(config).await?;
        Ok(Self { db })
    }
}

impl DatabaseRole for VectorWriter {
    fn role_name(&self) -> &'static str { "vector-writer" }
}

#[async_trait]
impl Writer for VectorWriter {
    type WriteRequest = VectorWriteRequest;
    type WriteResponse = VectorWriteResponse;

    async fn write(&self, request: Self::WriteRequest) -> Result<Self::WriteResponse, WriteError> {
        let count = request.vectors.len();
        self.db.write(request.vectors).await?;
        Ok(VectorWriteResponse { vectors_written: count })
    }

    async fn flush(&self) -> Result<(), WriteError> {
        self.db.flush().await?;
        Ok(())
    }

    fn buffer_stats(&self) -> BufferStats {
        BufferStats::default()
    }
}
```

#### Storage Sharing

Roles can share the underlying storage via `Arc<dyn Storage>`, enabling different topologies:

```rust
// Create shared storage
let storage = create_storage(&config.storage, None).await?;

// Writer and Reader share the same storage
let writer = LogWriter::with_storage(Arc::clone(&storage));
let reader = LogReaderRole::with_storage(Arc::clone(&storage));

// Both coordinate via SlateDB manifest in S3
```

### Composition Patterns

The role abstractions enable five primary deployment configurations:

#### A. Embedded (Library Mode)

All roles in one struct with direct Rust API. No network overhead.

```rust
/// All-in-one embedded deployment
pub struct EmbeddedLog {
    writer: LogWriter,
    reader: LogReaderRole,
    compactor: LogCompactor,
}

impl EmbeddedLog {
    pub async fn open(config: Config) -> Result<Self, Error> {
        let storage = create_storage(&config.storage, None).await?;

        Ok(Self {
            writer: LogWriter::with_storage(Arc::clone(&storage)),
            reader: LogReaderRole::with_storage(Arc::clone(&storage)),
            compactor: LogCompactor::with_storage(storage),
        })
    }

    // Direct API access
    pub async fn append(&self, records: Vec<Record>) -> Result<(), Error> {
        self.writer.as_log().append(records).await
    }

    pub fn scan(&self, key: Bytes, seq_range: impl RangeBounds<u64>) -> LogIterator {
        self.reader.as_reader().scan(key, seq_range)
    }
}

// Usage:
let log = EmbeddedLog::open(config).await?;
log.append(records).await?;
let entries = log.scan(key, ..).collect().await?;
```

#### B. Embedded with Background Compactor

Writer and Reader in application process; Compactor runs as background task.

```rust
pub struct EmbeddedWithBackgroundCompaction {
    writer: LogWriter,
    reader: LogReaderRole,
    compactor_handle: JoinHandle<()>,
}

impl EmbeddedWithBackgroundCompaction {
    pub async fn open(config: Config) -> Result<Self, Error> {
        let storage = create_storage(&config.storage, None).await?;

        let writer = LogWriter::with_storage(Arc::clone(&storage));
        let reader = LogReaderRole::with_storage(Arc::clone(&storage));
        let compactor = LogCompactor::with_storage(storage);

        // Run compactor in background
        let compactor_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if compactor.needs_compaction() {
                    if let Err(e) = compactor.compact().await {
                        tracing::error!("Compaction failed: {}", e);
                    }
                }
            }
        });

        Ok(Self { writer, reader, compactor_handle })
    }
}
```

#### C. Single-Process Server

All roles in one server process with HTTP APIs exposed.

```rust
pub struct SingleServerLog {
    writer: Arc<LogWriter>,
    reader: Arc<LogReaderRole>,
    compactor: Arc<LogCompactor>,
    runtime: ServiceRuntime,
}

impl SingleServerLog {
    pub async fn open(config: LogServiceConfig) -> Result<Self, Error> {
        let storage = create_storage(&config.storage, None).await?;

        let writer = Arc::new(LogWriter::with_storage(Arc::clone(&storage)));
        let reader = Arc::new(LogReaderRole::with_storage(Arc::clone(&storage)));
        let compactor = Arc::new(LogCompactor::with_storage(storage));

        let runtime = ServiceRuntime::builder()
            .with_port(config.port)
            .build();

        Ok(Self { writer, reader, compactor, runtime })
    }

    pub async fn run(self) -> Result<(), Error> {
        let router = self.runtime.create_router();
        let router = LogProtocol::register(router, LogState {
            writer: Some(self.writer),
            reader: self.reader,
        });

        self.runtime.serve(router).await
    }
}

// Usage:
let server = SingleServerLog::open(config).await?;
server.run().await?;
```

#### D. Scaled Readers

One writer process, multiple reader processes. All share storage via S3 + manifest.

```rust
// Writer server (ingestor)
pub struct LogWriterServer {
    writer: Arc<LogWriter>,
    runtime: ServiceRuntime,
}

impl LogWriterServer {
    pub async fn open(config: LogServiceConfig) -> Result<Self, Error> {
        let storage = create_storage(&config.storage, None).await?;
        let writer = Arc::new(LogWriter::with_storage(storage));

        let runtime = ServiceRuntime::builder()
            .with_port(config.ingest_port)
            .build();

        Ok(Self { writer, runtime })
    }

    pub async fn run(self) -> Result<(), Error> {
        let router = LogProtocol::register_write_only(
            self.runtime.create_router(),
            self.writer,
        );
        self.runtime.serve(router).await
    }
}

// Reader server (query executor)
pub struct LogReaderServer {
    reader: Arc<LogReaderRole>,
    runtime: ServiceRuntime,
}

impl LogReaderServer {
    pub async fn open(config: LogServiceConfig) -> Result<Self, Error> {
        // Reader connects to same S3 location as writer
        let storage = create_storage(&config.storage, None).await?;
        let reader = Arc::new(LogReaderRole::with_storage(storage));

        let runtime = ServiceRuntime::builder()
            .with_port(config.query_port)
            .build();

        Ok(Self { reader, runtime })
    }

    pub async fn run(self) -> Result<(), Error> {
        let router = LogProtocol::register_read_only(
            self.runtime.create_router(),
            self.reader,
        );
        self.runtime.serve(router).await
    }
}
```

#### E. Serverless

Ephemeral writer and reader instances. Maximum elasticity.

```rust
/// Serverless handler for Lambda/Cloud Functions
pub struct ServerlessLogReader {
    reader: LogReaderRole,
}

impl ServerlessLogReader {
    pub async fn from_env() -> Result<Self, Error> {
        let config = LogServiceConfig::from_env()?;
        let storage = create_storage(&config.storage, None).await?;
        let reader = LogReaderRole::with_storage(storage);
        Ok(Self { reader })
    }

    pub async fn handle(&self, request: LogQueryRequest) -> Result<LogQueryResponse, Error> {
        self.reader.query(request).await.map_err(Into::into)
    }
}

// Lambda handler example:
use lambda_http::{run, service_fn, Body, Request, Response};
use std::sync::OnceLock;

static HANDLER: OnceLock<ServerlessLogReader> = OnceLock::new();

async fn handler(event: Request) -> Result<Response<Body>, lambda_http::Error> {
    let handler = HANDLER.get().expect("handler not initialized");
    let request: LogQueryRequest = serde_json::from_slice(event.body())?;
    let response = handler.handle(request).await?;
    Ok(Response::builder()
        .status(200)
        .body(serde_json::to_vec(&response)?.into())?)
}

#[tokio::main]
async fn main() -> Result<(), lambda_http::Error> {
    let reader = ServerlessLogReader::from_env().await?;
    HANDLER.set(reader).expect("handler already set");
    run(service_fn(handler)).await
}
```

### ServiceRuntime

The ServiceRuntime provides shared HTTP infrastructure that all database servers use:

```rust
// service/src/runtime.rs

use axum::{Router, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Common HTTP service infrastructure
pub struct ServiceRuntime {
    config: RuntimeConfig,
    metrics: Arc<RuntimeMetrics>,
}

#[derive(Clone)]
pub struct RuntimeConfig {
    pub bind_addr: SocketAddr,
    pub enable_metrics: bool,
    pub enable_health: bool,
    pub request_timeout: Duration,
    pub max_request_body_size: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            enable_metrics: true,
            enable_health: true,
            request_timeout: Duration::from_secs(30),
            max_request_body_size: 10 * 1024 * 1024,
        }
    }
}

impl ServiceRuntime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder::default()
    }

    /// Create base router with common endpoints
    pub fn create_router(&self) -> Router {
        let mut router = Router::new();

        if self.config.enable_health {
            router = router
                .route("/health", get(|| async { "OK" }))
                .route("/ready", get(|| async { "READY" }));
        }

        if self.config.enable_metrics {
            let metrics = Arc::clone(&self.metrics);
            router = router.route("/metrics", get(move || {
                let m = Arc::clone(&metrics);
                async move { m.encode() }
            }));
        }

        router
    }

    /// Serve the router with graceful shutdown
    pub async fn serve(self, router: Router) -> Result<(), Error> {
        let listener = tokio::net::TcpListener::bind(self.config.bind_addr).await?;
        tracing::info!("Starting server on {}", self.config.bind_addr);

        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await?;

        Ok(())
    }

    /// Get metrics registry for protocol bindings
    pub fn metrics(&self) -> &Arc<RuntimeMetrics> {
        &self.metrics
    }
}

/// Builder for ServiceRuntime
#[derive(Default)]
pub struct RuntimeBuilder {
    config: RuntimeConfig,
}

impl RuntimeBuilder {
    pub fn with_port(mut self, port: u16) -> Self {
        self.config.bind_addr.set_port(port);
        self
    }

    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.config.bind_addr = addr;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    pub fn disable_metrics(mut self) -> Self {
        self.config.enable_metrics = false;
        self
    }

    pub fn build(self) -> ServiceRuntime {
        ServiceRuntime {
            config: self.config,
            metrics: Arc::new(RuntimeMetrics::new()),
        }
    }
}
```

#### RuntimeMetrics

```rust
/// Shared metrics for the runtime
pub struct RuntimeMetrics {
    registry: prometheus::Registry,
    requests_total: prometheus::IntCounterVec,
    request_duration_seconds: prometheus::HistogramVec,
    active_connections: prometheus::IntGauge,
}

impl RuntimeMetrics {
    pub fn new() -> Self {
        let registry = prometheus::Registry::new();

        let requests_total = prometheus::IntCounterVec::new(
            prometheus::Opts::new("http_requests_total", "Total HTTP requests"),
            &["method", "path", "status"],
        ).unwrap();

        let request_duration_seconds = prometheus::HistogramVec::new(
            prometheus::HistogramOpts::new(
                "http_request_duration_seconds",
                "HTTP request duration"
            ),
            &["method", "path"],
        ).unwrap();

        let active_connections = prometheus::IntGauge::new(
            "http_active_connections",
            "Number of active HTTP connections",
        ).unwrap();

        registry.register(Box::new(requests_total.clone())).unwrap();
        registry.register(Box::new(request_duration_seconds.clone())).unwrap();
        registry.register(Box::new(active_connections.clone())).unwrap();

        Self { registry, requests_total, request_duration_seconds, active_connections }
    }

    /// Encode metrics in Prometheus text format
    pub fn encode(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// Register additional metrics from protocol bindings
    pub fn register(&self, collector: Box<dyn prometheus::core::Collector>) -> Result<(), Error> {
        self.registry.register(collector).map_err(|e| Error::Internal(e.to_string()))
    }
}
```

### Protocol Bindings

Protocol bindings translate network requests to role operations. Each database provides its own binding.

#### ProtocolBinding Trait

```rust
// service/src/protocol.rs

use axum::Router;

/// Trait for protocol bindings that add routes to a router
pub trait ProtocolBinding {
    /// The state type required by this protocol
    type State: Clone + Send + Sync + 'static;

    /// Register routes with the router
    fn register(router: Router, state: Self::State) -> Router;

    /// Protocol name for logging/metrics
    fn protocol_name() -> &'static str;
}
```

#### Log Protocol Binding

```rust
// log/src/protocol.rs

use axum::{Router, routing::{get, post}, extract::{State, Path, Query}, Json};

pub struct LogProtocol;

#[derive(Clone)]
pub struct LogState<R, W> {
    pub reader: Arc<R>,
    pub writer: Option<Arc<W>>,
}

impl<R: Reader + 'static, W: Writer + 'static> ProtocolBinding for LogProtocol {
    type State = LogState<R, W>;

    fn register(router: Router, state: Self::State) -> Router {
        let mut router = router
            .route("/v1/log/:key/scan", get(scan_handler::<R>))
            .route("/v1/log/:key/count", get(count_handler::<R>));

        if state.writer.is_some() {
            router = router.route("/v1/log/:key/append", post(append_handler::<W>));
        }

        router.with_state(state)
    }

    fn protocol_name() -> &'static str { "log" }
}

impl LogProtocol {
    pub fn register_read_only<R: Reader + 'static>(
        router: Router,
        reader: Arc<R>,
    ) -> Router {
        let state = LogState::<R, NoopWriter> { reader, writer: None };
        Self::register(router, state)
    }

    pub fn register_write_only<W: Writer + 'static>(
        router: Router,
        writer: Arc<W>,
    ) -> Router {
        // Write-only doesn't need reader for append
        todo!()
    }
}

async fn scan_handler<R: Reader>(
    State(state): State<LogState<R, impl Writer>>,
    Path(key): Path<String>,
    Query(params): Query<ScanParams>,
) -> Result<Json<ScanResponse>, ApiError> {
    let request = LogQueryRequest {
        key: Bytes::from(key),
        start_sequence: params.start,
        end_sequence: params.end,
        limit: params.limit,
    };
    let response = state.reader.query(request).await?;
    Ok(Json(response.into()))
}

async fn append_handler<W: Writer>(
    State(state): State<LogState<impl Reader, W>>,
    Path(key): Path<String>,
    Json(body): Json<AppendRequest>,
) -> Result<Json<AppendResponse>, ApiError> {
    let writer = state.writer.as_ref().ok_or(ApiError::WriteNotSupported)?;
    let request = LogWriteRequest {
        records: body.records.into_iter().map(|r| Record {
            key: Bytes::from(key.clone()),
            value: Bytes::from(r.value),
        }).collect(),
        await_durable: body.await_durable.unwrap_or(false),
    };
    let response = writer.write(request).await?;
    Ok(Json(response.into()))
}
```

#### PromQL Protocol Binding (TSDB)

The existing PromQL server pattern extends naturally to the protocol binding model:

```rust
// timeseries/src/protocol.rs

pub struct PromqlProtocol;

#[derive(Clone)]
pub struct PromqlState<R, W> {
    pub reader: Arc<R>,
    pub writer: Option<Arc<W>>,
    pub flush_interval_secs: u64,
}

impl<R: Reader + 'static, W: Writer + 'static> ProtocolBinding for PromqlProtocol {
    type State = PromqlState<R, W>;

    fn register(router: Router, state: Self::State) -> Router {
        let mut router = router
            .route("/api/v1/query", get(query_handler::<R>).post(query_handler::<R>))
            .route("/api/v1/query_range", get(query_range_handler::<R>).post(query_range_handler::<R>))
            .route("/api/v1/series", get(series_handler::<R>).post(series_handler::<R>))
            .route("/api/v1/labels", get(labels_handler::<R>))
            .route("/api/v1/label/:name/values", get(label_values_handler::<R>));

        if state.writer.is_some() {
            router = router.route("/api/v1/write", post(remote_write_handler::<W>));
        }

        router.with_state(state)
    }

    fn protocol_name() -> &'static str { "promql" }
}
```

#### Vector Protocol Binding

```rust
// vector/src/protocol.rs

pub struct VectorProtocol;

#[derive(Clone)]
pub struct VectorState<R, W> {
    pub reader: Arc<R>,
    pub writer: Option<Arc<W>>,
}

impl<R: Reader + 'static, W: Writer + 'static> ProtocolBinding for VectorProtocol {
    type State = VectorState<R, W>;

    fn register(router: Router, state: Self::State) -> Router {
        let mut router = router
            .route("/v1/vectors/search", post(search_handler::<R>))
            .route("/v1/vectors/:id", get(get_handler::<R>));

        if state.writer.is_some() {
            router = router
                .route("/v1/vectors", post(upsert_handler::<W>))
                .route("/v1/vectors/:id", axum::routing::delete(delete_handler::<W>));
        }

        router.with_state(state)
    }

    fn protocol_name() -> &'static str { "vector" }
}
```

### Catalog Abstraction

The Catalog abstraction provides a foundation for future sharding support. Initially, most deployments use a single shard, but the abstraction allows transparent scaling.

#### Catalog Trait

```rust
// service/src/catalog.rs

use async_trait::async_trait;

/// Unique identifier for a shard
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ShardId(pub String);

/// Metadata about a shard
#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub id: ShardId,
    pub storage_path: String,
    pub status: ShardStatus,
    pub key_range: Option<KeyRange>,
    pub time_range: Option<TimeRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStatus {
    Active,
    ReadOnly,
    Migrating,
    Offline,
}

#[derive(Debug, Clone)]
pub struct KeyRange {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct TimeRange {
    pub start_secs: Option<i64>,
    pub end_secs: Option<i64>,
}

/// Catalog trait for shard discovery and routing
#[async_trait]
pub trait Catalog: Send + Sync {
    /// List all known shards
    async fn list_shards(&self) -> Result<Vec<ShardInfo>, CatalogError>;

    /// Get information about a specific shard
    async fn get_shard(&self, id: &ShardId) -> Result<Option<ShardInfo>, CatalogError>;

    /// Register a new shard
    async fn register_shard(&self, info: ShardInfo) -> Result<(), CatalogError>;

    /// Update shard status
    async fn update_status(&self, id: &ShardId, status: ShardStatus) -> Result<(), CatalogError>;

    /// Route a key to shards that may contain it
    async fn route_key(&self, key: &[u8]) -> Result<Vec<ShardId>, CatalogError>;

    /// Route a time range to shards that may contain it
    async fn route_time_range(&self, start: i64, end: i64) -> Result<Vec<ShardId>, CatalogError>;
}
```

#### LocalCatalog (Single-Shard)

```rust
/// Simple in-memory catalog for single-node deployments
pub struct LocalCatalog {
    shards: tokio::sync::RwLock<HashMap<ShardId, ShardInfo>>,
}

impl LocalCatalog {
    /// Create a single-shard catalog (most common case)
    pub fn single_shard(storage_path: String) -> Self {
        let catalog = Self { shards: Default::default() };
        let shard = ShardInfo {
            id: ShardId("default".to_string()),
            storage_path,
            status: ShardStatus::Active,
            key_range: None,
            time_range: None,
        };
        catalog.shards.blocking_write().insert(shard.id.clone(), shard);
        catalog
    }
}

#[async_trait]
impl Catalog for LocalCatalog {
    async fn list_shards(&self) -> Result<Vec<ShardInfo>, CatalogError> {
        Ok(self.shards.read().await.values().cloned().collect())
    }

    async fn route_key(&self, _key: &[u8]) -> Result<Vec<ShardId>, CatalogError> {
        // Single-shard routes everything to the default shard
        Ok(self.shards.read().await.keys().cloned().collect())
    }

    async fn route_time_range(&self, _start: i64, _end: i64) -> Result<Vec<ShardId>, CatalogError> {
        Ok(self.shards.read().await.keys().cloned().collect())
    }

    // ... other methods
}
```

#### S3Catalog (Multi-Shard)

> **Note**: The catalog implementation sketched here is directional. There are many approaches. For instance the catalog could be stored in a special config-kv store that's backed by SlateDB itself, this could use a single-writer/many-readers pattern, with reader instances embedded alongside database writers and readers to quickly resolve key → shard → storage config. This architecture would scale well and is worth exploring in a follow-up RFC.

For distributed deployments, the catalog is stored in S3 alongside the data:

```
s3://bucket/
├── catalog.json          # Shard directory (CAS-protected)
├── shard-0/manifest/...  # SlateDB instance 0
├── shard-1/manifest/...  # SlateDB instance 1
└── shard-2/manifest/...  # SlateDB instance 2
```

```rust
/// S3-backed catalog for distributed deployments
pub struct S3Catalog {
    object_store: Arc<dyn object_store::ObjectStore>,
    catalog_path: object_store::path::Path,
    local_cache: tokio::sync::RwLock<CatalogSnapshot>,
}

impl S3Catalog {
    pub fn new(
        object_store: Arc<dyn object_store::ObjectStore>,
        catalog_path: &str,
    ) -> Self {
        Self {
            object_store,
            catalog_path: object_store::path::Path::from(catalog_path),
            local_cache: Default::default(),
        }
    }

    async fn refresh(&self) -> Result<(), CatalogError> {
        let bytes = self.object_store.get(&self.catalog_path).await?.bytes().await?;
        let snapshot: CatalogSnapshot = serde_json::from_slice(&bytes)?;
        *self.local_cache.write().await = snapshot;
        Ok(())
    }

    async fn update(&self, f: impl FnOnce(&mut CatalogSnapshot)) -> Result<(), CatalogError> {
        // Read-modify-write with CAS
        let bytes = self.object_store.get(&self.catalog_path).await?;
        let e_tag = bytes.meta.e_tag.clone();
        let mut snapshot: CatalogSnapshot = serde_json::from_slice(&bytes.bytes().await?)?;

        f(&mut snapshot);

        let new_bytes = serde_json::to_vec(&snapshot)?;
        self.object_store
            .put_opts(
                &self.catalog_path,
                new_bytes.into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Update(object_store::UpdateVersion {
                        e_tag,
                        version: None,
                    }),
                    ..Default::default()
                },
            )
            .await?;

        *self.local_cache.write().await = snapshot;
        Ok(())
    }
}
```

### Configuration Model

The configuration model supports both programmatic (builder pattern) and declarative (TOML/YAML) approaches.

#### Configuration Structures

```rust
// service/src/config.rs

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Base configuration shared across all database services
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub metrics: MetricsConfig,

    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_bind")]
    pub bind: String,

    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,
}

fn default_port() -> u16 { 8080 }
fn default_bind() -> String { "0.0.0.0".to_string() }
fn default_timeout() -> Duration { Duration::from_secs(30) }

/// Log database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogServiceConfig {
    #[serde(flatten)]
    pub base: ServiceConfig,

    #[serde(default)]
    pub segmentation: LogSegmentConfig,
}

/// TimeSeries database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsdbServiceConfig {
    #[serde(flatten)]
    pub base: ServiceConfig,

    #[serde(default = "default_flush_interval", with = "humantime_serde")]
    pub flush_interval: Duration,

    #[serde(default)]
    pub scrape_configs: Vec<ScrapeConfig>,
}

/// Vector database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorServiceConfig {
    #[serde(flatten)]
    pub base: ServiceConfig,

    pub dimensions: u16,

    #[serde(default)]
    pub distance_metric: DistanceMetric,

    #[serde(default = "default_flush_interval", with = "humantime_serde")]
    pub flush_interval: Duration,
}
```

#### Configuration Builder

```rust
/// Builder for configurations with file loading and env overrides
pub struct ConfigBuilder<T> {
    config: T,
}

impl<T: Default + serde::de::DeserializeOwned> ConfigBuilder<T> {
    pub fn new() -> Self {
        Self { config: T::default() }
    }

    /// Load from file (TOML, YAML, or JSON)
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)?;

        let config = match path.extension().and_then(|e| e.to_str()) {
            Some("toml") => toml::from_str(&contents)?,
            Some("yaml") | Some("yml") => serde_yaml::from_str(&contents)?,
            Some("json") => serde_json::from_str(&contents)?,
            _ => return Err(ConfigError::UnsupportedFormat),
        };

        Ok(Self { config })
    }

    pub fn build(self) -> T {
        self.config
    }
}

// Fluent builder methods
impl ConfigBuilder<TsdbServiceConfig> {
    pub fn with_port(mut self, port: u16) -> Self {
        self.config.base.server.port = port;
        self
    }

    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.config.flush_interval = interval;
        self
    }

    pub fn with_storage(mut self, storage: StorageConfig) -> Self {
        self.config.base.storage = storage;
        self
    }
}
```

#### Example Configuration Files

**TimeSeries (YAML)**:

```yaml
# timeseries.yaml
server:
  port: 9090
  bind: "0.0.0.0"
  request_timeout: 30s

storage:
  type: SlateDb
  path: data/tsdb
  object_store:
    type: Aws
    region: us-west-2
    bucket: my-tsdb-bucket

flush_interval: 30s

scrape_configs:
  - job_name: prometheus
    scrape_interval: 15s
    static_configs:
      - targets: ["localhost:9090"]

metrics:
  enabled: true
  path: /metrics

logging:
  level: info
  format: json
```

**Log (TOML)**:

```toml
# log.toml
[server]
port = 8081
bind = "0.0.0.0"

[storage]
type = "SlateDb"
path = "data/log"

[storage.object_store]
type = "Local"
path = "/var/lib/opendata/log"

[segmentation]
seal_interval = "1h"

[logging]
level = "debug"
```

**Vector (YAML)**:

```yaml
# vector.yaml
server:
  port: 8082

storage:
  type: SlateDb
  path: data/vector
  object_store:
    type: InMemory

dimensions: 384
distance_metric: cosine
flush_interval: 10s
```

## Example Configurations

### Complete Single-Server TSDB

```rust
use service::{ServiceRuntime, ConfigBuilder};
use timeseries::{Tsdb, TsdbWriter, TsdbReaderRole};
use timeseries::protocol::{PromqlProtocol, PromqlState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = ConfigBuilder::<TsdbServiceConfig>::from_file("timeseries.yaml")?
        .build();

    // Initialize storage
    let storage = create_storage(&config.base.storage, Some(merge_operator())).await?;

    // Create shared TSDB
    let tsdb = Arc::new(Tsdb::new(storage.clone()));

    // Create roles
    let writer = Arc::new(TsdbWriter::new(Arc::clone(&tsdb), config.flush_interval.as_secs()));
    let reader = Arc::new(TsdbReaderRole::new(Arc::clone(&tsdb)));

    // Build runtime
    let runtime = ServiceRuntime::builder()
        .with_port(config.base.server.port)
        .with_request_timeout(config.base.server.request_timeout)
        .build();

    // Register protocol
    let router = runtime.create_router();
    let router = PromqlProtocol::register(router, PromqlState {
        reader,
        writer: Some(writer),
        flush_interval_secs: config.flush_interval.as_secs(),
    });

    // Start flush timer
    let tsdb_flush = Arc::clone(&tsdb);
    let interval = config.flush_interval;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(e) = tsdb_flush.flush(interval.as_secs()).await {
                tracing::error!("Flush failed: {}", e);
            }
        }
    });

    // Serve
    runtime.serve(router).await?;
    Ok(())
}
```

### Scaled Readers Deployment

```rust
// ingestor.rs - Writer process
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ConfigBuilder::<TsdbServiceConfig>::from_file("ingestor.yaml")?.build();
    let storage = create_storage(&config.base.storage, Some(merge_operator())).await?;
    let tsdb = Arc::new(Tsdb::new(storage));
    let writer = Arc::new(TsdbWriter::new(tsdb, config.flush_interval.as_secs()));

    let runtime = ServiceRuntime::builder().with_port(config.base.server.port).build();
    let router = RemoteWriteProtocol::register(runtime.create_router(), writer);

    runtime.serve(router).await?;
    Ok(())
}

// query-executor.rs - Reader process (can run multiple instances)
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ConfigBuilder::<TsdbServiceConfig>::from_file("query-executor.yaml")?.build();

    // Connect to same S3 bucket as writer
    let storage = create_storage(&config.base.storage, None).await?;
    let tsdb = Arc::new(Tsdb::new(storage));
    let reader = Arc::new(TsdbReaderRole::new(tsdb));

    let runtime = ServiceRuntime::builder().with_port(config.base.server.port).build();
    let router = PromqlProtocol::register_read_only(runtime.create_router(), reader);

    runtime.serve(router).await?;
    Ok(())
}
```

## Future Work

The following items are explicitly deferred to future RFCs:

1. **Detailed Sharding Implementation**: Key range partitioning, rebalancing, split/merge operations, consistent hashing
2. **Kafka Wire Protocol**: Native Kafka protocol support for Log database
3. **gRPC Bindings**: gRPC protocol bindings for Vector database
4. **Authentication/Authorization**: Token-based auth, RBAC, tenant isolation
5. **Distributed Transactions**: Cross-database consistency guarantees
6. **Query Federation**: Querying across multiple databases in a single request

## Alternatives Considered

### Monolithic Server vs Role Composition

**Alternative**: Single server binary per database with all functionality built-in.

**Rejected because**:
1. Cannot scale readers independently of writers
2. Cannot run compaction in separate processes for resource isolation
3. No path from embedded to service mode
4. Duplicates HTTP infrastructure across databases

### gRPC-First vs HTTP-First

**Alternative**: Use gRPC as the primary protocol with HTTP as a gateway.

**Rejected because**:
1. HTTP is more accessible (curl, browsers, simpler clients)
2. Prometheus ecosystem expects HTTP
3. gRPC can still be added as a protocol binding later
4. Axum already used in existing code

### Abstract Database Trait vs Concrete Role Traits

**Alternative**: Single `Database` trait with methods for all operations.

**Rejected because**:
1. Not all deployments need all operations (readers don't write)
2. Associated types become unwieldy with multiple request/response types
3. Role separation enables better resource accounting and scaling
4. Matches the conceptual architecture (Ingestors, Compactors, Query Executors)

### Configuration: Code vs Config Files

**Alternative**: Pure programmatic configuration via Rust builders.

**Rejected because**:
1. Non-Rust users need declarative config
2. Kubernetes/container deployments expect config files
3. Environment variable overrides are standard practice
4. Both can coexist (builder + config loading)

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-15 | Initial draft |

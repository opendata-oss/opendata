use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::Request;
use axum::extract::{FromRequest, Path, State};
use axum::http::{Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_extra::extract::{Form, Query};
use rust_embed::Embed;
use tokio::signal;

use super::metrics::Metrics;
use super::middleware::{MetricsLayer, TracingLayer};
use crate::error::Error;
use crate::model::{QueryOptions, QueryValue};
#[cfg(feature = "otel")]
use crate::otel::{OtelConfig, OtelConverter};
use crate::promql::config::{OtelServerConfig, PrometheusConfig};
use crate::promql::request::{
    FederateParams, LabelValuesParams, LabelsParams, MetadataParams, QueryParams, QueryRangeParams,
    SeriesParams,
};
use crate::promql::response::{
    self, FederateResponse, LabelValuesResponse, LabelsResponse, MetadataResponse,
    QueryRangeResponse, QueryResponse, SeriesResponse,
};
use crate::promql::scraper::Scraper;
use crate::tsdb::TsdbEngine;

use crate::util::{parse_duration, parse_timestamp, parse_timestamp_to_seconds};

#[derive(Embed)]
#[folder = "ui/"]
struct UiAssets;

/// Shared application state.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) tsdb: Arc<TsdbEngine>,
    pub(crate) metrics: Arc<Metrics>,
    #[cfg(feature = "otel")]
    pub(crate) otel_converter: Arc<OtelConverter>,
}

/// Server configuration
pub struct ServerConfig {
    pub port: u16,
    pub prometheus_config: PrometheusConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            prometheus_config: PrometheusConfig::default(),
        }
    }
}

/// Build the production Axum router with all routes, middleware, and state.
///
/// Used by `TimeSeriesHttpServer::run()` and the `testing` module for integration tests.
pub(crate) fn build_router(
    tsdb: Arc<TsdbEngine>,
    metrics: Arc<Metrics>,
    _otel_config: OtelServerConfig,
) -> Router {
    let state = AppState {
        tsdb,
        metrics: metrics.clone(),
        #[cfg(feature = "otel")]
        otel_converter: Arc::new(OtelConverter::new(OtelConfig {
            include_resource_attrs: _otel_config.include_resource_attrs,
            include_scope_attrs: _otel_config.include_scope_attrs,
        })),
    };

    let app = Router::new()
        .route("/api/v1/query", get(handle_query).post(handle_query))
        .route(
            "/api/v1/query_range",
            get(handle_query_range).post(handle_query_range),
        )
        .route("/api/v1/series", get(handle_series).post(handle_series))
        .route("/api/v1/labels", get(handle_labels))
        .route("/api/v1/label/{name}/values", get(handle_label_values))
        .route("/api/v1/metadata", get(handle_metadata))
        .route("/federate", get(handle_federate))
        .route("/metrics", get(handle_metrics))
        .route("/-/healthy", get(handle_healthy))
        .route("/-/ready", get(handle_ready))
        .route("/-/flush", post(handle_flush));

    #[cfg(feature = "remote-write")]
    let app = app.route(
        "/api/v1/write",
        post(super::remote_write::handle_remote_write),
    );
    #[cfg(feature = "otel")]
    let app = app.route("/v1/metrics", post(super::otel::handle_otel_metrics));

    app.route("/", get(handle_ui_redirect))
        .route("/query", get(handle_ui_index))
        .route("/{*path}", get(handle_ui))
        .layer(TracingLayer::new())
        .layer(MetricsLayer::new())
        .with_state(state)
}

/// Prometheus-compatible HTTP server
pub(crate) struct TimeSeriesHttpServer {
    tsdb: Arc<TsdbEngine>,
    config: ServerConfig,
    storage: Option<Arc<dyn common::Storage>>,
}

impl TimeSeriesHttpServer {
    pub(crate) fn new(
        tsdb: Arc<TsdbEngine>,
        config: ServerConfig,
        storage: Option<Arc<dyn common::Storage>>,
    ) -> Self {
        Self {
            tsdb,
            config,
            storage,
        }
    }

    /// Run the HTTP server
    pub(crate) async fn run(self) {
        // Install the metrics-rs recorder and create the rendering handle
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        // Install globally — if a recorder is already installed (e.g. tests), this is a no-op
        let _ = metrics::set_global_recorder(recorder);

        // Create metrics container with optional storage engine metrics
        let mut metrics = Metrics::new(handle);
        if let Some(storage) = &self.storage {
            storage.register_metrics(metrics.storage_registry_mut());
        }
        let metrics = Arc::new(metrics);

        // Start the scraper if there are scrape configs (requires read-write mode)
        if !self.config.prometheus_config.scrape_configs.is_empty() {
            if let Some(tsdb) = self.tsdb.as_tsdb() {
                let scraper = Arc::new(Scraper::new(tsdb, self.config.prometheus_config.clone()));
                scraper.run();
                tracing::info!(
                    "Started scraper with {} job(s)",
                    self.config.prometheus_config.scrape_configs.len()
                );
            } else {
                tracing::warn!("Scrape configs present but ignored in read-only mode");
            }
        } else {
            tracing::info!("No scrape configs found, scraper not started");
        }

        // Start the ingest consumer if configured (requires read-write mode + otel)
        #[cfg(feature = "otel")]
        let consumer_handle = {
            let mut handle = None;
            if let Some(ingest_config) = &self.config.prometheus_config.ingest_consumer {
                if let Some(tsdb) = self.tsdb.as_tsdb() {
                    let converter =
                        Arc::new(crate::otel::OtelConverter::new(crate::otel::OtelConfig {
                            include_resource_attrs: self
                                .config
                                .prometheus_config
                                .otel
                                .include_resource_attrs,
                            include_scope_attrs: self
                                .config
                                .prometheus_config
                                .otel
                                .include_scope_attrs,
                        }));
                    let consumer = Arc::new(super::ingest_consumer::IngestConsumer::new(
                        tsdb,
                        converter,
                        ingest_config.clone(),
                    ));
                    match consumer.run().await {
                        Ok(h) => handle = Some(h),
                        Err(e) => {
                            tracing::error!("Failed to start ingest consumer: {e}");
                            std::process::exit(1);
                        }
                    }
                } else {
                    tracing::warn!("ingest_consumer config present but ignored in read-only mode");
                }
            }
            handle
        };

        // Build router with metrics middleware
        let app = build_router(
            self.tsdb.clone(),
            metrics,
            self.config.prometheus_config.otel.clone(),
        );

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        tracing::info!("Starting Prometheus-compatible server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();

        // Stop the ingest consumer and flush pending acks before flushing TSDB
        #[cfg(feature = "otel")]
        if let Some(handle) = consumer_handle {
            tracing::info!("Shutting down ingest consumer...");
            handle.shutdown().await;
        }

        // Flush TSDB on shutdown to persist any buffered data
        tracing::info!("Flushing TSDB before shutdown...");
        if let Err(e) = self.tsdb.flush().await {
            tracing::error!("Failed to flush TSDB on shutdown: {}", e);
        }

        tracing::info!("Server shut down gracefully");
    }
}

/// Error response wrapper for converting TimeseriesError to HTTP responses
struct ApiError(Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            Error::InvalidInput(_) => (StatusCode::BAD_REQUEST, "bad_data"),
            Error::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            Error::Encoding(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            Error::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            Error::Backpressure => (StatusCode::SERVICE_UNAVAILABLE, "unavailable"),
        };

        let body = serde_json::json!({
            "status": "error",
            "errorType": error_type,
            "error": self.0.to_string()
        });

        (status, Json(body)).into_response()
    }
}

impl From<Error> for ApiError {
    fn from(err: Error) -> Self {
        ApiError(err)
    }
}

impl IntoResponse for FederateResponse {
    fn into_response(self) -> Response {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, self.content_type)],
            self.body,
        )
            .into_response()
    }
}

/// Extract params from GET (query string) or POST (form body).
///
/// Note: `AppState` must implement `Clone` because axum's `FromRequest`
/// trait requires `S: Clone` for extractors like `Query` and `Form`.
async fn extract_params<T: serde::de::DeserializeOwned>(
    request: Request,
    state: &AppState,
) -> Result<T, ApiError> {
    let method = request.method().clone();
    match method {
        Method::GET => {
            let Query(params) = Query::<T>::from_request(request, state)
                .await
                .map_err(|e| {
                    Error::InvalidInput(format!("Failed to parse query parameters: {}", e))
                })?;
            Ok(params)
        }
        Method::POST => {
            let Form(params) = Form::<T>::from_request(request, state)
                .await
                .map_err(|e| Error::InvalidInput(format!("Failed to parse form body: {}", e)))?;
            Ok(params)
        }
        _ => Err(ApiError(Error::InvalidInput(
            "Only GET and POST methods are supported".to_string(),
        ))),
    }
}

/// Handle /api/v1/query
async fn handle_query(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<QueryResponse>, ApiError> {
    let params: QueryParams = extract_params(request, &state).await?;
    let time = params.time.map(|s| parse_timestamp(&s)).transpose()?;
    let result = state
        .tsdb
        .eval_query(&params.query, time, &QueryOptions::default())
        .await;
    Ok(Json(response::query_value_to_response(result)))
}

/// Handle /api/v1/query_range
async fn handle_query_range(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<QueryRangeResponse>, ApiError> {
    let params: QueryRangeParams = extract_params(request, &state).await?;
    let start = parse_timestamp(&params.start)?;
    let end = parse_timestamp(&params.end)?;
    let step = parse_duration(&params.step)?;
    let result = state
        .tsdb
        .eval_query_range(&params.query, start..=end, step, &QueryOptions::default())
        .await;
    Ok(Json(response::range_result_to_response(result)))
}

/// Handle /api/v1/series
async fn handle_series(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<SeriesResponse>, ApiError> {
    let params: SeriesParams = extract_params(request, &state).await?;
    let start = params
        .start
        .map(|s| parse_timestamp_to_seconds(&s))
        .transpose()?
        .unwrap_or(0);
    let end = params
        .end
        .map(|s| parse_timestamp_to_seconds(&s))
        .transpose()?
        .unwrap_or(i64::MAX);
    let matchers: Vec<&str> = params.matches.iter().map(|s| s.as_str()).collect();
    let result = state.tsdb.find_series(&matchers, start, end).await;
    Ok(Json(response::series_to_response(result, params.limit)))
}

/// Handle /api/v1/labels
async fn handle_labels(
    State(state): State<AppState>,
    Query(params): Query<LabelsParams>,
) -> Result<Json<LabelsResponse>, ApiError> {
    let start = params
        .start
        .map(|s| parse_timestamp_to_seconds(&s))
        .transpose()?
        .unwrap_or(0);
    let end = params
        .end
        .map(|s| parse_timestamp_to_seconds(&s))
        .transpose()?
        .unwrap_or(i64::MAX);
    let matchers: Option<Vec<&str>> = if params.matches.is_empty() {
        None
    } else {
        Some(params.matches.iter().map(|s| s.as_str()).collect())
    };
    let result = state
        .tsdb
        .find_labels(matchers.as_deref(), start, end)
        .await;
    Ok(Json(response::labels_to_response(result, params.limit)))
}

/// Handle /api/v1/label/{name}/values
async fn handle_label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<LabelValuesParams>,
) -> Result<Json<LabelValuesResponse>, ApiError> {
    let start = params
        .start
        .map(|s| parse_timestamp_to_seconds(&s))
        .transpose()?
        .unwrap_or(0);
    let end = params
        .end
        .map(|s| parse_timestamp_to_seconds(&s))
        .transpose()?
        .unwrap_or(i64::MAX);
    let matchers: Option<Vec<&str>> = if params.matches.is_empty() {
        None
    } else {
        Some(params.matches.iter().map(|s| s.as_str()).collect())
    };
    let result = state
        .tsdb
        .find_label_values(&name, matchers.as_deref(), start, end)
        .await;
    Ok(Json(response::label_values_to_response(
        result,
        params.limit,
    )))
}

/// Handle /api/v1/metadata
async fn handle_metadata(
    State(state): State<AppState>,
    Query(params): Query<MetadataParams>,
) -> Result<Json<MetadataResponse>, ApiError> {
    let result = state.tsdb.find_metadata(params.metric.as_deref()).await;
    Ok(Json(response::metadata_to_response(
        result,
        params.limit,
        params.limit_per_metric,
    )))
}

/// Handle /federate by returning recent samples matching the query in text format
/// <https://prometheus.io/docs/prometheus/latest/federation/>
/// <https://prometheus.io/docs/instrumenting/exposition_formats/>
async fn handle_federate(
    State(state): State<AppState>,
    Query(params): Query<FederateParams>,
) -> Result<FederateResponse, ApiError> {
    use std::collections::HashSet;

    if params.matches.is_empty() {
        return Err(
            Error::InvalidInput("at least one match[] parameter is required".to_string()).into(),
        );
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut body = String::new();

    for selector in &params.matches {
        let result = state
            .tsdb
            .eval_query(selector, None, &QueryOptions::default())
            .await
            .map_err(|e| match e {
                crate::error::QueryError::InvalidQuery(msg) => Error::InvalidInput(msg),
                crate::error::QueryError::Execution(msg) => Error::Internal(msg),
                crate::error::QueryError::Timeout => Error::Internal("query timed out".to_string()),
            })?;

        let samples = match result {
            QueryValue::Vector(s) => s,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "match[] must be a vector selector, got: {selector}"
                ))
                .into());
            }
        };

        for sample in samples {
            // Build dedup key from the full label set
            let key: String = sample
                .labels
                .iter()
                .map(|l| format!("{}={}", l.name, l.value))
                .collect::<Vec<_>>()
                .join("\x00");
            if !seen.insert(key) {
                continue;
            }

            let metric_name = sample.labels.metric_name();
            let extra_labels: Vec<_> = sample
                .labels
                .iter()
                .filter(|l| l.name != "__name__")
                .collect();

            body.push_str(metric_name);
            if !extra_labels.is_empty() {
                body.push('{');
                for (i, l) in extra_labels.iter().enumerate() {
                    if i > 0 {
                        body.push(',');
                    }
                    body.push_str(&l.name);
                    body.push_str("=\"");
                    push_escaped_label_value(&mut body, &l.value);
                    body.push('"');
                }
                body.push('}');
            }
            body.push(' ');
            if sample.value.is_nan() {
                body.push_str("NaN");
            } else if sample.value == f64::INFINITY {
                body.push_str("+Inf");
            } else if sample.value == f64::NEG_INFINITY {
                body.push_str("-Inf");
            } else {
                body.push_str(&sample.value.to_string());
            }
            body.push(' ');
            body.push_str(&sample.timestamp_ms.to_string());
            body.push('\n');
        }
    }

    Ok(FederateResponse {
        content_type: "text/plain; version=0.0.4; charset=utf-8".to_string(),
        body: body.into_bytes(),
    })
}

fn push_escaped_label_value(buf: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '\\' => buf.push_str("\\\\"),
            '"' => buf.push_str("\\\""),
            '\n' => buf.push_str("\\n"),
            _ => buf.push(c),
        }
    }
}

/// Handle /metrics endpoint - returns Prometheus text format
async fn handle_metrics(State(state): State<AppState>) -> String {
    state.metrics.encode()
}

/// Handle /-/healthy endpoint - returns 200 OK if service is running
async fn handle_healthy() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}

/// Handle /-/ready endpoint - returns 200 OK if service is ready to serve requests
async fn handle_ready(State(_state): State<AppState>) -> (StatusCode, &'static str) {
    // Service is ready if it's running (TSDB is initialized in AppState)
    (StatusCode::OK, "OK")
}

/// Handle /-/flush endpoint - flushes buffered data to durable storage
async fn handle_flush(
    State(state): State<AppState>,
) -> Result<(StatusCode, &'static str), ApiError> {
    state.tsdb.flush().await?;
    Ok((StatusCode::OK, "OK"))
}

/// Redirect `/` to `/query`.
async fn handle_ui_redirect(uri: Uri) -> Redirect {
    // Preserve any query string from the original request
    match uri.query() {
        Some(q) => Redirect::permanent(&format!("/query?{}", q)),
        None => Redirect::permanent("/query"),
    }
}

/// Serve the UI index page at `/query`.
async fn handle_ui_index() -> impl IntoResponse {
    match UiAssets::get("index.html") {
        Some(file) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            file.data.to_vec(),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "UI not found").into_response(),
    }
}

/// Serve UI static assets at `/{*path}`.
async fn handle_ui(Path(path): Path<String>) -> impl IntoResponse {
    match UiAssets::get(&path) {
        Some(file) => {
            let mime = mime_guess::from_path(&path)
                .first_or_octet_stream()
                .to_string();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, mime)],
                file.data.to_vec(),
            )
                .into_response()
        }
        None => match UiAssets::get("404.html") {
            Some(page) => (
                StatusCode::NOT_FOUND,
                [(header::CONTENT_TYPE, "text/html; charset=utf-8".to_string())],
                page.data.to_vec(),
            )
                .into_response(),
            None => (StatusCode::NOT_FOUND, "Not found").into_response(),
        },
    }
}

/// Listen for SIGTERM (K8s pod termination) and SIGINT (Ctrl+C).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received SIGINT, starting graceful shutdown"),
        _ = terminate => tracing::info!("Received SIGTERM, starting graceful shutdown"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    /// Build a minimal router with just the UI and health routes (no AppState needed).
    fn ui_router() -> Router {
        Router::new()
            .route("/-/healthy", get(handle_healthy))
            .route("/api/v1/labels", get(|| async { "labels-api" }))
            .route("/", get(handle_ui_redirect))
            .route("/query", get(handle_ui_index))
            .route("/{*path}", get(handle_ui))
    }

    #[tokio::test]
    async fn should_redirect_root_to_query() {
        // given
        let app = ui_router();
        let req = Request::builder().uri("/").body(Body::empty()).unwrap();

        // when
        let resp = app.oneshot(req).await.unwrap();

        // then
        assert_eq!(resp.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(resp.headers().get("location").unwrap(), "/query");
    }

    #[tokio::test]
    async fn should_redirect_root_preserving_query_string() {
        // given
        let app = ui_router();
        let req = Request::builder()
            .uri("/?expr=up&tab=graph")
            .body(Body::empty())
            .unwrap();

        // when
        let resp = app.oneshot(req).await.unwrap();

        // then
        assert_eq!(resp.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            resp.headers().get("location").unwrap(),
            "/query?expr=up&tab=graph"
        );
    }

    #[tokio::test]
    async fn should_serve_index_html_at_query() {
        // given
        let app = ui_router();
        let req = Request::builder()
            .uri("/query")
            .body(Body::empty())
            .unwrap();

        // when
        let resp = app.oneshot(req).await.unwrap();

        // then
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/html; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn should_serve_static_assets_with_correct_mime() {
        // given
        let app = ui_router();
        let req = Request::builder()
            .uri("/style.css")
            .body(Body::empty())
            .unwrap();

        // when
        let resp = app.oneshot(req).await.unwrap();

        // then
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(
            resp.headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("css")
        );
    }

    #[tokio::test]
    async fn should_return_404_for_missing_assets() {
        // given
        let app = ui_router();
        let req = Request::builder()
            .uri("/does-not-exist.js")
            .body(Body::empty())
            .unwrap();

        // when
        let resp = app.oneshot(req).await.unwrap();

        // then
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn should_not_intercept_api_routes() {
        // given
        let app = ui_router();
        let req = Request::builder()
            .uri("/api/v1/labels")
            .body(Body::empty())
            .unwrap();

        // when
        let resp = app.oneshot(req).await.unwrap();

        // then — should hit the stub API handler, not the UI wildcard
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, "labels-api");
    }

    #[tokio::test]
    async fn should_not_intercept_health_routes() {
        // given
        let app = ui_router();
        let req = Request::builder()
            .uri("/-/healthy")
            .body(Body::empty())
            .unwrap();

        // when
        let resp = app.oneshot(req).await.unwrap();

        // then
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body, "OK");
    }
}

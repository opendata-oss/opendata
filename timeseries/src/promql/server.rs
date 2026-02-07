use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{FromRequest, Path, Query, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
#[cfg(feature = "remote-write")]
use axum::routing::post;
use axum::{Form, extract::Request};
use axum::{Json, Router};
use tokio::signal;
use tokio::time::interval;

use super::config::PrometheusConfig;
use super::metrics::Metrics;
use super::middleware::{MetricsLayer, TracingLayer};
use super::request::{
    LabelValuesParams, LabelsParams, LabelsRequest, QueryParams, QueryRangeParams,
    QueryRangeRequest, QueryRequest, SeriesParams, SeriesRequest,
};
use super::response::{
    LabelValuesResponse, LabelsResponse, QueryRangeResponse, QueryResponse, SeriesResponse,
};
use super::router::PromqlRouter;
use super::scraper::Scraper;
use crate::error::Error;
use crate::tsdb::Tsdb;

/// Shared application state.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) tsdb: Arc<Tsdb>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) flush_interval_secs: u64,
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

/// Prometheus-compatible HTTP server
pub(crate) struct PromqlServer {
    tsdb: Arc<Tsdb>,
    config: ServerConfig,
}

impl PromqlServer {
    pub(crate) fn new(tsdb: Arc<Tsdb>, config: ServerConfig) -> Self {
        Self { tsdb, config }
    }

    /// Run the HTTP server
    pub(crate) async fn run(self) {
        // Create metrics registry
        let metrics = Arc::new(Metrics::new());

        // Create app state
        let state = AppState {
            tsdb: self.tsdb.clone(),
            metrics: metrics.clone(),
            flush_interval_secs: self.config.prometheus_config.flush_interval_secs,
        };

        // Start the scraper if there are scrape configs
        if !self.config.prometheus_config.scrape_configs.is_empty() {
            let scraper = Arc::new(Scraper::new(
                self.tsdb.clone(),
                self.config.prometheus_config.clone(),
                metrics.clone(),
            ));
            scraper.run();
            tracing::info!(
                "Started scraper with {} job(s)",
                self.config.prometheus_config.scrape_configs.len()
            );
        } else {
            tracing::info!("No scrape configs found, scraper not started");
        }

        // Start the flush timer
        let flush_interval_secs = self.config.prometheus_config.flush_interval_secs;
        let tsdb_for_flush = self.tsdb.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(flush_interval_secs));
            tracing::info!(
                "Starting flush timer with {}s interval",
                flush_interval_secs
            );
            loop {
                ticker.tick().await;
                if let Err(e) = tsdb_for_flush.flush(flush_interval_secs).await {
                    tracing::error!("Failed to flush TSDB: {}", e);
                } else {
                    tracing::debug!("Flushed TSDB");
                }
            }
        });

        // Build router with metrics middleware
        let app = Router::new()
            .route("/api/v1/query", get(handle_query).post(handle_query))
            .route(
                "/api/v1/query_range",
                get(handle_query_range).post(handle_query_range),
            )
            .route("/api/v1/series", get(handle_series).post(handle_series))
            .route("/api/v1/labels", get(handle_labels))
            .route("/api/v1/label/{name}/values", get(handle_label_values))
            .route("/metrics", get(handle_metrics))
            .route("/-/healthy", get(handle_healthy))
            .route("/-/ready", get(handle_ready));

        #[cfg(feature = "remote-write")]
        let app = app.route(
            "/api/v1/write",
            post(super::remote_write::handle_remote_write),
        );

        let app = app
            .layer(TracingLayer::new())
            .layer(MetricsLayer::new(metrics))
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        tracing::info!("Starting Prometheus-compatible server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();

        // Flush TSDB on shutdown to persist any buffered data
        let flush_interval_secs = self.config.prometheus_config.flush_interval_secs;
        tracing::info!("Flushing TSDB before shutdown...");
        if let Err(e) = self.tsdb.flush(flush_interval_secs).await {
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

/// Handle /api/v1/query
async fn handle_query(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<QueryResponse>, ApiError> {
    let method = request.method().clone();

    let query_request: QueryRequest = match method {
        Method::GET => {
            // For GET requests, extract from query parameters
            let Query(params) = Query::<QueryParams>::from_request(request, &state)
                .await
                .map_err(|e| {
                    Error::InvalidInput(format!("Failed to parse query parameters: {}", e))
                })?;
            params.try_into()?
        }
        Method::POST => {
            // For POST requests, extract from form body
            let Form(params) = Form::<QueryParams>::from_request(request, &state)
                .await
                .map_err(|e| Error::InvalidInput(format!("Failed to parse form body: {}", e)))?;
            params.try_into()?
        }
        _ => {
            return Err(ApiError(Error::InvalidInput(
                "Only GET and POST methods are supported".to_string(),
            )));
        }
    };

    Ok(Json(state.tsdb.query(query_request).await))
}

/// Handle /api/v1/query_range
async fn handle_query_range(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<QueryRangeResponse>, ApiError> {
    let method = request.method().clone();

    let query_request: QueryRangeRequest = match method {
        Method::GET => {
            // For GET requests, extract from query parameters
            let Query(params) = Query::<QueryRangeParams>::from_request(request, &state)
                .await
                .map_err(|e| {
                    Error::InvalidInput(format!("Failed to parse query parameters: {}", e))
                })?;
            params.try_into()?
        }
        Method::POST => {
            // For POST requests, extract from form body
            let Form(params) = Form::<QueryRangeParams>::from_request(request, &state)
                .await
                .map_err(|e| Error::InvalidInput(format!("Failed to parse form body: {}", e)))?;
            params.try_into()?
        }
        _ => {
            return Err(ApiError(Error::InvalidInput(
                "Only GET and POST methods are supported".to_string(),
            )));
        }
    };

    Ok(Json(state.tsdb.query_range(query_request).await))
}

/// Handle /api/v1/series
async fn handle_series(
    State(state): State<AppState>,
    request: Request,
) -> Result<Json<SeriesResponse>, ApiError> {
    let method = request.method().clone();

    let series_request: SeriesRequest = match method {
        Method::GET => {
            // For GET requests, extract from query parameters
            let Query(params) = Query::<SeriesParams>::from_request(request, &state)
                .await
                .map_err(|e| {
                    Error::InvalidInput(format!("Failed to parse query parameters: {}", e))
                })?;
            params.try_into()?
        }
        Method::POST => {
            // For POST requests, extract from form body
            let Form(params) = Form::<SeriesParams>::from_request(request, &state)
                .await
                .map_err(|e| Error::InvalidInput(format!("Failed to parse form body: {}", e)))?;
            params.try_into()?
        }
        _ => {
            return Err(ApiError(Error::InvalidInput(
                "Only GET and POST methods are supported".to_string(),
            )));
        }
    };

    Ok(Json(state.tsdb.series(series_request).await))
}

/// Handle /api/v1/labels
async fn handle_labels(
    State(state): State<AppState>,
    Query(params): Query<LabelsParams>,
) -> Result<Json<LabelsResponse>, ApiError> {
    let request: LabelsRequest = params.try_into()?;
    Ok(Json(state.tsdb.labels(request).await))
}

/// Handle /api/v1/label/{name}/values
async fn handle_label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<LabelValuesParams>,
) -> Result<Json<LabelValuesResponse>, ApiError> {
    let request = params.into_request(name)?;
    Ok(Json(state.tsdb.label_values(request).await))
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

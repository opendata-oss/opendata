use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
#[cfg(feature = "remote-write")]
use axum::routing::post;
use axum::{Json, Router};
use tokio::time::interval;

use super::config::PrometheusConfig;
use super::metrics::Metrics;
use super::middleware::MetricsLayer;
use super::request::{
    LabelValuesParams, LabelsParams, LabelsRequest, QueryParams, QueryRangeParams,
    QueryRangeRequest, QueryRequest, SeriesParams, SeriesRequest,
};
use super::response::{
    LabelValuesResponse, LabelsResponse, QueryRangeResponse, QueryResponse, SeriesResponse,
};
use super::router::PromqlRouter;
use super::scraper::Scraper;
use crate::tsdb::Tsdb;
use crate::util::OpenTsdbError;

/// Shared application state.
#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) tsdb: Arc<Tsdb>,
    pub(crate) metrics: Arc<Metrics>,
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
                if let Err(e) = tsdb_for_flush.flush().await {
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
            .route("/metrics", get(handle_metrics));

        #[cfg(feature = "remote-write")]
        let app = app.route(
            "/api/v1/write",
            post(super::remote_write::handle_remote_write),
        );

        let app = app.layer(MetricsLayer::new(metrics)).with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        tracing::info!("Starting Prometheus-compatible server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}

/// Error response wrapper for converting OpenTsdbError to HTTP responses
struct ApiError(OpenTsdbError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type) = match &self.0 {
            OpenTsdbError::InvalidInput(_) => (StatusCode::BAD_REQUEST, "bad_data"),
            OpenTsdbError::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            OpenTsdbError::Encoding(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
            OpenTsdbError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };

        let body = serde_json::json!({
            "status": "error",
            "errorType": error_type,
            "error": self.0.to_string()
        });

        (status, Json(body)).into_response()
    }
}

impl From<OpenTsdbError> for ApiError {
    fn from(err: OpenTsdbError) -> Self {
        ApiError(err)
    }
}

/// Handle /api/v1/query
async fn handle_query(
    State(state): State<AppState>,
    Query(params): Query<QueryParams>,
) -> Result<Json<QueryResponse>, ApiError> {
    let request: QueryRequest = params.try_into()?;
    Ok(Json(state.tsdb.query(request).await))
}

/// Handle /api/v1/query_range
async fn handle_query_range(
    State(state): State<AppState>,
    Query(params): Query<QueryRangeParams>,
) -> Result<Json<QueryRangeResponse>, ApiError> {
    let request: QueryRangeRequest = params.try_into()?;
    Ok(Json(state.tsdb.query_range(request).await))
}

/// Handle /api/v1/series
async fn handle_series(
    State(state): State<AppState>,
    Query(params): Query<SeriesParams>,
) -> Result<Json<SeriesResponse>, ApiError> {
    let request: SeriesRequest = params.try_into()?;
    Ok(Json(state.tsdb.series(request).await))
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

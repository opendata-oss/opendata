use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{FromRequest, Path, Query, State};
use axum::http::{Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
#[cfg(feature = "remote-write")]
use axum::routing::post;
use axum::{Form, extract::Request};
use axum::{Json, Router};
use rust_embed::Embed;
use tokio::signal;

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

#[derive(Embed)]
#[folder = "ui/"]
struct UiAssets;

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
    storage: Arc<dyn common::Storage>,
}

impl PromqlServer {
    pub(crate) fn new(
        tsdb: Arc<Tsdb>,
        config: ServerConfig,
        storage: Arc<dyn common::Storage>,
    ) -> Self {
        Self {
            tsdb,
            config,
            storage,
        }
    }

    /// Run the HTTP server
    pub(crate) async fn run(self) {
        // Create metrics registry and register storage engine metrics
        let mut metrics = Metrics::new();
        self.storage.register_metrics(metrics.registry_mut());
        let metrics = Arc::new(metrics);

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
            .route("/", get(handle_ui_redirect))
            .route("/query", get(handle_ui_index))
            .route("/{*path}", get(handle_ui));

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

use super::{
    config::PrometheusConfig,
    metrics::Metrics,
    middleware::{MetricsLayer, TracingLayer},
    query_parser::parse_query_with_repeated,
    request::{
        LabelValuesParams, LabelsParams, LabelsRequest, QueryParams, QueryRangeParams,
        QueryRangeRequest, QueryRequest, SeriesParams, SeriesRequest,
    },
    response::{
        LabelValuesResponse, LabelsResponse, QueryRangeResponse, QueryResponse, SeriesResponse,
    },
    router::PromqlRouter,
    scraper::Scraper,
};
use crate::{error::Error, tsdb::Tsdb};

#[cfg(feature = "remote-write")]
use axum::routing::post;
use axum::{
    Form, Json, Router,
    extract::{FromRequest, Path, Query, RawQuery, Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

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
            .route("/metrics", get(handle_metrics));

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
        axum::serve(listener, app).await.unwrap();
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
///
/// Axum’s `Query` extractor does not support repeated `match[]` parameters.
/// This handler uses a shared query parser to preserve Prometheus-compatible
/// semantics when handling label value requests.
async fn handle_label_values(
    State(state): State<AppState>,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<LabelValuesResponse>, ApiError> {
    let raw_query = raw_query.unwrap_or_default();

    let (matches, mut params): (Vec<String>, LabelValuesParams) =
        parse_query_with_repeated(&raw_query, "match[]")
            .map_err(|e| Error::InvalidInput(format!("Failed to parse query: {}", e)))?;
    // tracing::info!("label_values matches = {:?}", matches);
    // tracing::info!("label_values start = {:?}", params.start);
    // tracing::info!("label_values end = {:?}", params.end);
    // tracing::info!("label_values limit = {:?}", params.limit);

    params.matches = matches;
    let request = params.into_request(name)?;

    Ok(Json(state.tsdb.label_values(request).await))
}

/// Handle /metrics endpoint - returns Prometheus text format
async fn handle_metrics(State(state): State<AppState>) -> String {
    state.metrics.encode()
}

#[cfg(test)]
mod tests {
    use super::{LabelValuesParams, parse_query_with_repeated};

    #[test]
    fn parses_single_match_param() {
        let raw = "match%5B%5D=%7Binstance%3D%22host-1%22%7D";

        let (matches, params): (Vec<String>, LabelValuesParams) =
            parse_query_with_repeated::<LabelValuesParams>(raw, "match[]").unwrap();

        assert_eq!(matches, vec![r#"{instance="host-1"}"#.to_string()]);
        assert!(params.start.is_none());
        assert!(params.end.is_none());
        assert!(params.limit.is_none());
    }

    #[test]
    fn parses_multiple_match_params() {
        let raw = concat!(
            "match%5B%5D=%7Binstance%3D%22host-1%22%7D&",
            "match%5B%5D=%7Bjob%3D%22node%22%7D"
        );

        let (matches, params): (Vec<String>, LabelValuesParams) =
            parse_query_with_repeated::<LabelValuesParams>(raw, "match[]").unwrap();

        assert_eq!(
            matches,
            vec![
                r#"{instance="host-1"}"#.to_string(),
                r#"{job="node"}"#.to_string(),
            ]
        );
        assert!(params.start.is_none());
        assert!(params.end.is_none());
        assert!(params.limit.is_none());
    }

    #[test]
    fn parses_optional_params_without_match() {
        let raw = "start=100&end=200&limit=10";

        let (matches, params): (Vec<String>, LabelValuesParams) =
            parse_query_with_repeated::<LabelValuesParams>(raw, "match[]").unwrap();

        assert!(matches.is_empty());
        assert_eq!(params.start.as_deref(), Some("100"));
        assert_eq!(params.end.as_deref(), Some("200"));
        assert_eq!(params.limit, Some(10));
    }

    #[test]
    fn parses_match_and_optional_params_together() {
        let raw = concat!("match%5B%5D=%7Bjob%3D%22api%22%7D&", "start=123&limit=5");

        let (matches, params): (Vec<String>, LabelValuesParams) =
            parse_query_with_repeated::<LabelValuesParams>(raw, "match[]").unwrap();

        assert_eq!(matches, vec![r#"{job="api"}"#.to_string()]);
        assert_eq!(params.start.as_deref(), Some("123"));
        assert!(params.end.is_none());
        assert_eq!(params.limit, Some(5));
    }

    #[test]
    fn handles_empty_query() {
        let (matches, params): (Vec<String>, LabelValuesParams) =
            parse_query_with_repeated::<LabelValuesParams>("", "match[]").unwrap();

        assert!(matches.is_empty());
        assert!(params.start.is_none());
        assert!(params.end.is_none());
        assert!(params.limit.is_none());
    }
}

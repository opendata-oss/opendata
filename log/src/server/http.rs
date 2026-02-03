//! HTTP server implementation for OpenData Log.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};

use super::config::LogServerConfig;
use super::handlers::{
    AppState, handle_append, handle_count, handle_list_keys, handle_list_segments, handle_metrics,
    handle_scan,
};
use super::metrics::Metrics;
use super::middleware::{MetricsLayer, TracingLayer};
use crate::LogDb;

/// HTTP server for the log service.
pub struct LogServer {
    log: Arc<LogDb>,
    config: LogServerConfig,
}

impl LogServer {
    /// Create a new log server.
    pub fn new(log: Arc<LogDb>, config: LogServerConfig) -> Self {
        Self { log, config }
    }

    /// Run the HTTP server.
    pub async fn run(self) {
        // Create metrics registry
        let metrics = Arc::new(Metrics::new());

        // Create app state
        let state = AppState {
            log: self.log,
            metrics: metrics.clone(),
        };

        // Build router with routes and middleware
        let app = Router::new()
            .route("/api/v1/log/append", post(handle_append))
            .route("/api/v1/log/scan", get(handle_scan))
            .route("/api/v1/log/keys", get(handle_list_keys))
            .route("/api/v1/log/segments", get(handle_list_segments))
            .route("/api/v1/log/count", get(handle_count))
            .route("/metrics", get(handle_metrics))
            .layer(TracingLayer::new())
            .layer(MetricsLayer::new(metrics))
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        tracing::info!("Starting Log HTTP server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    }
}

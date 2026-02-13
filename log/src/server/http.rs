//! HTTP server implementation for OpenData Log.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use tokio::signal;

use super::config::LogServerConfig;
use super::handlers::{
    AppState, handle_append, handle_count, handle_healthy, handle_list_keys, handle_list_segments,
    handle_metrics, handle_ready, handle_scan,
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
        // Create metrics registry and register storage engine metrics
        let mut metrics = Metrics::new();
        self.log.register_metrics(metrics.registry_mut());
        let metrics = Arc::new(metrics);

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
            .route("/-/healthy", get(handle_healthy))
            .route("/-/ready", get(handle_ready))
            .layer(TracingLayer::new())
            .layer(MetricsLayer::new(metrics))
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        tracing::info!("Starting Log HTTP server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();

        tracing::info!("Server shut down gracefully");
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

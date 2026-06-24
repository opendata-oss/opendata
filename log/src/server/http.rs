//! HTTP server implementation for OpenData Log.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use tokio::signal;

use super::config::LogServerConfig;
use super::handlers::{
    AppState, LogBackend, handle_append, handle_count, handle_healthy, handle_list_keys,
    handle_list_segments, handle_metrics, handle_ready, handle_scan,
};
use super::metrics::Metrics;
use super::middleware::{MetricsLayer, TracingLayer};

/// HTTP server for the log service.
pub struct LogServer {
    backend: LogBackend,
    config: LogServerConfig,
    metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
}

impl LogServer {
    /// Create a new log server.
    ///
    /// Pass a [`LogBackend::ReadWrite`] for a full log service, or a
    /// [`LogBackend::ReadOnly`] for a read-only gateway. In read-only mode the
    /// append route is not registered.
    pub fn new(
        backend: LogBackend,
        config: LogServerConfig,
        metrics_handle: metrics_exporter_prometheus::PrometheusHandle,
    ) -> Self {
        Self {
            backend,
            config,
            metrics_handle,
        }
    }

    /// Run the HTTP server.
    pub async fn run(self) {
        let metrics = Arc::new(Metrics::with_metrics_rs_handle(Some(self.metrics_handle)));

        let read_only = matches!(self.backend, LogBackend::ReadOnly(_));

        // Create app state
        let state = AppState {
            log: self.backend,
            metrics: metrics.clone(),
        };

        // Build router with read routes shared by both modes.
        let mut app = Router::new()
            .route("/api/v1/log/scan", get(handle_scan))
            .route("/api/v1/log/keys", get(handle_list_keys))
            .route("/api/v1/log/segments", get(handle_list_segments))
            .route("/api/v1/log/count", get(handle_count))
            .route("/metrics", get(handle_metrics))
            .route("/-/healthy", get(handle_healthy))
            .route("/-/ready", get(handle_ready));

        // The append route exists only when the server fronts a writable log.
        if !read_only {
            app = app.route("/api/v1/log/append", post(handle_append));
        }

        let app = app
            .layer(TracingLayer::new())
            .layer(MetricsLayer::new(metrics))
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        let mode = if read_only { "read-only" } else { "read-write" };
        tracing::info!("Starting Log HTTP server on {} ({} mode)", addr, mode);

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

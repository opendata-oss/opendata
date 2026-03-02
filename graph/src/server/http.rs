//! HTTP server implementation for OpenData Graph.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use tokio::signal;

use super::config::GraphServerConfig;
use super::handlers::{AppState, handle_healthy, handle_ready};
use crate::db::GraphDb;

/// HTTP server for the graph service.
pub struct GraphServer {
    db: Arc<GraphDb>,
    config: GraphServerConfig,
}

impl GraphServer {
    /// Create a new graph server.
    pub fn new(db: Arc<GraphDb>, config: GraphServerConfig) -> Self {
        Self { db, config }
    }

    /// Run the HTTP server.
    pub async fn run(self) {
        let state = AppState {
            db: self.db,
        };

        let app = build_router(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.port));
        tracing::info!("Starting Graph HTTP server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();

        tracing::info!("Server shut down gracefully");
    }
}

/// Build an axum Router for the graph service.
///
/// Useful for integration tests via `oneshot()`.
pub fn build_app(db: Arc<GraphDb>) -> Router {
    let state = AppState { db };
    build_router(state)
}

fn build_router(state: AppState) -> Router {
    let api = Router::new();

    #[cfg(feature = "gql")]
    let api = api.route("/api/v1/graph/query", post(super::handlers::handle_query));

    api.route("/-/healthy", get(handle_healthy))
        .route("/-/ready", get(handle_ready))
        .with_state(state)
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

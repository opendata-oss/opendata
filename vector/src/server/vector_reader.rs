//! HTTP server implementation for OpenData Vector (read-only).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use tokio::signal;

use super::config::VectorServerConfig;
use super::handlers::{AppState, handle_get_vector, handle_healthy, handle_ready, handle_search};
use super::middleware::TracingLayer;
use crate::{FieldType, MetadataFieldSpec, VectorDbReader};

/// HTTP server for the vector reader service (read-only).
pub struct VectorReaderServer {
    db: Arc<VectorDbReader>,
    config: VectorServerConfig,
    metadata_fields: Vec<MetadataFieldSpec>,
}

impl VectorReaderServer {
    /// Create a new vector reader server.
    pub fn new(
        db: Arc<VectorDbReader>,
        config: VectorServerConfig,
        metadata_fields: Vec<MetadataFieldSpec>,
    ) -> Self {
        Self {
            db,
            config,
            metadata_fields,
        }
    }

    /// Run the HTTP server.
    pub async fn run(self) {
        let Self {
            db,
            config,
            metadata_fields,
        } = self;
        let state = AppState {
            db,
            metadata_fields: metadata_fields_by_name(&metadata_fields),
        };

        let app = Router::new()
            .route(
                "/api/v1/vector/search",
                post(handle_search::<VectorDbReader>),
            )
            .route(
                "/api/v1/vector/vectors/{id}",
                get(handle_get_vector::<VectorDbReader>),
            )
            .route("/-/healthy", get(handle_healthy))
            .route("/-/ready", get(handle_ready))
            .layer(TracingLayer::new())
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
        tracing::info!("Starting Vector Reader HTTP server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();

        tracing::info!("Server shut down gracefully");
    }
}

fn metadata_fields_by_name(metadata_fields: &[MetadataFieldSpec]) -> HashMap<String, FieldType> {
    metadata_fields
        .iter()
        .map(|field| (field.name.clone(), field.field_type))
        .collect()
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

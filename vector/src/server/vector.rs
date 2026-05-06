//! HTTP server implementation for OpenData Vector (read-write).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use tokio::signal;

#[cfg(feature = "buffer")]
use super::buffer_consumer::{BufferConsumer, ConsumerHandle};
use super::config::VectorServerConfig;
use super::handlers::{
    AppState, handle_get_vector, handle_healthy, handle_ready, handle_search, handle_write,
};
use super::middleware::TracingLayer;
#[cfg(feature = "buffer")]
use crate::model::BufferConsumerConfig;
use crate::{FieldType, MetadataFieldSpec, VectorDb};

/// HTTP server for the vector service (read-write).
pub struct VectorServer {
    db: Arc<VectorDb>,
    config: VectorServerConfig,
    metadata_fields: Vec<MetadataFieldSpec>,
    #[cfg(feature = "buffer")]
    buffer_consumer: Option<Arc<BufferConsumer>>,
}

impl VectorServer {
    /// Create a new vector server.
    pub fn new(
        db: Arc<VectorDb>,
        config: VectorServerConfig,
        metadata_fields: Vec<MetadataFieldSpec>,
    ) -> Self {
        Self {
            db,
            config,
            metadata_fields,
            #[cfg(feature = "buffer")]
            buffer_consumer: None,
        }
    }

    /// Attach a buffer consumer that will be started alongside the HTTP server
    /// and stopped before the server shuts down.
    #[cfg(feature = "buffer")]
    pub fn with_buffer_consumer(mut self, config: BufferConsumerConfig) -> Self {
        self.buffer_consumer = Some(Arc::new(BufferConsumer::new(self.db.clone(), config)));
        self
    }

    /// Run the HTTP server.
    pub async fn run(self) {
        let Self {
            db,
            config,
            metadata_fields,
            #[cfg(feature = "buffer")]
            buffer_consumer,
        } = self;
        let state = AppState {
            db,
            metadata_fields: metadata_fields_by_name(&metadata_fields),
        };

        #[cfg(feature = "buffer")]
        let consumer_handle: Option<ConsumerHandle> = match buffer_consumer {
            Some(consumer) => match consumer.run().await {
                Ok(handle) => Some(handle),
                Err(e) => {
                    tracing::error!("Failed to start vector buffer consumer: {e}");
                    std::process::exit(1);
                }
            },
            None => None,
        };

        let app = Router::new()
            .route("/api/v1/vector/write", post(handle_write))
            .route("/api/v1/vector/search", post(handle_search::<VectorDb>))
            .route(
                "/api/v1/vector/vectors/{id}",
                get(handle_get_vector::<VectorDb>),
            )
            .route("/-/healthy", get(handle_healthy))
            .route("/-/ready", get(handle_ready))
            .layer(TracingLayer::new())
            .with_state(state);

        let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
        tracing::info!("Starting Vector HTTP server on {}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .unwrap();

        #[cfg(feature = "buffer")]
        if let Some(handle) = consumer_handle {
            handle.shutdown().await;
        }

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

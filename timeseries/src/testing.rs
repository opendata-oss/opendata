//! Testing utilities for the timeseries HTTP server.
//!
//! Provides helpers for integration tests that exercise HTTP endpoints
//! using Axum's `oneshot()` infrastructure with a real SlateDB-backed TSDB.

use std::sync::Arc;

use axum::Router;
use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig};
use common::{StorageConfig, StorageRuntime, StorageSemantics, create_storage};

use crate::promql::metrics::Metrics;
use crate::promql::server::build_router;
use crate::storage::merge_operator::OpenTsdbMergeOperator;

// Re-export production types so integration tests use the real types.
pub use crate::promql::response::{
    ErrorResponse, LabelValuesResponse, LabelsResponse, MatrixSeries, QueryRangeResponse,
    QueryRangeResult, QueryResponse, QueryResult, SeriesResponse, VectorSeries,
};
pub use crate::tsdb::Tsdb;

/// Create a [`Tsdb`] backed by SlateDB with an in-memory object store.
///
/// This exercises the full storage path including SlateDB merge operations.
pub async fn create_test_tsdb() -> Arc<Tsdb> {
    let config = StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "test-data".to_string(),
        object_store: ObjectStoreConfig::InMemory,
        settings_path: None,
    });
    let storage = create_storage(
        &config,
        StorageRuntime::new(),
        StorageSemantics::new().with_merge_operator(Arc::new(OpenTsdbMergeOperator)),
    )
    .await
    .unwrap();
    Arc::new(Tsdb::new(storage))
}

/// Build the production Axum router — same routes, middleware, and state
/// as [`crate::promql::server::PromqlServer::run()`] but without binding
/// to a TCP port.
pub fn build_app(tsdb: Arc<Tsdb>) -> Router {
    build_router(tsdb, Arc::new(Metrics::new()))
}

//! Testing utilities for the timeseries HTTP server.
//!
//! Provides helpers for integration tests that exercise HTTP endpoints
//! using Axum's `oneshot()` infrastructure with a real SlateDB-backed TSDB.

use std::sync::Arc;

use axum::Router;
use common::storage::config::{ObjectStoreConfig, SlateDbStorageConfig};
use common::{StorageConfig, StorageRuntime, StorageSemantics, create_storage};

use common::Storage;

use crate::model::Series;
use crate::promql::metrics::Metrics;
use crate::promql::server::build_router;
use crate::storage::merge_operator::OpenTsdbMergeOperator;
use crate::tsdb::Tsdb;

// Re-export production types so integration tests use the real types.
pub use crate::promql::response::{
    ErrorResponse, LabelValuesResponse, LabelsResponse, MatrixSeries, QueryRangeResponse,
    QueryRangeResult, QueryResponse, QueryResult, SeriesResponse, VectorSeries,
};

/// Opaque handle to a test TSDB instance.
///
/// Wraps the internal `Tsdb` so that integration tests can ingest data
/// without the crate needing to expose `Tsdb` as a public type.
pub struct TestTsdb {
    inner: Arc<Tsdb>,
    storage: Arc<dyn Storage>,
}

impl TestTsdb {
    /// Ingest series into the TSDB (production ingestion path).
    pub async fn ingest_samples(&self, series: Vec<Series>) {
        self.inner.ingest_samples(series).await.unwrap();
    }

    /// Flush all dirty buckets to storage.
    pub async fn flush(&self) {
        self.inner.flush().await.unwrap();
    }
}

/// Create a [`TestTsdb`] backed by SlateDB with an in-memory object store.
///
/// This exercises the full storage path including SlateDB merge operations.
pub async fn create_test_tsdb() -> TestTsdb {
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
    TestTsdb {
        inner: Arc::new(Tsdb::new(storage.clone())),
        storage,
    }
}

/// Build the production Axum router — same routes, middleware, and state
/// as [`crate::promql::server::PromqlServer::run()`] but without binding
/// to a TCP port.
pub fn build_app(tsdb: &TestTsdb) -> Router {
    let mut metrics = Metrics::new();
    tsdb.storage.register_metrics(metrics.registry_mut());
    build_router(tsdb.inner.clone(), Arc::new(metrics))
}

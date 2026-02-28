//! Testing utilities for the timeseries HTTP server.
//!
//! Provides helpers for integration tests that exercise HTTP endpoints
//! using Axum's `oneshot()` infrastructure with a real SlateDB-backed TSDB.

use std::sync::Arc;

use axum::Router;
use common::storage::config::SlateDbStorageConfig;
use common::{StorageConfig, StorageRuntime, StorageSemantics, create_storage};

use common::Storage;

use crate::model::Series;
use crate::promql::metrics::Metrics;
use crate::promql::server::build_router;
use crate::storage::merge_operator::OpenTsdbMergeOperator;
use crate::tsdb::Tsdb;

// Re-export storage config types so benchmarks and integration tests
// can construct object store configs without depending on `common` directly.
pub use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig};

// Re-export production types so integration tests use the real types.
pub use crate::promql::response::{
    ErrorResponse, LabelValuesResponse, LabelsResponse, MatrixSeries, MetadataResponse,
    QueryRangeResponse, QueryRangeResult, QueryResponse, QueryResult, QueryResultValue,
    SeriesResponse, VectorSeries,
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
    create_test_tsdb_with_config(ObjectStoreConfig::InMemory).await
}

/// Create a [`TestTsdb`] with a caller-provided object store config.
///
/// This allows benchmarks to use production-like storage backends
/// such as local filesystem or S3 instead of in-memory.
pub async fn create_test_tsdb_with_config(object_store: ObjectStoreConfig) -> TestTsdb {
    let config = StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "bench-data".to_string(),
        object_store,
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

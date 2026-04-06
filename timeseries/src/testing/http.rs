//! HTTP-specific testing helpers.
//!
//! Provides the production Axum router wired to a [`TestTsdb`] for
//! integration tests that exercise HTTP endpoints via `oneshot()`.

use std::sync::{Arc, OnceLock};

use axum::Router;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::promql::config::OtelServerConfig;
use crate::server::{Metrics, build_router};
use crate::tsdb::TsdbEngine;

use super::TestTsdb;

/// Install a global metrics-rs recorder once for the entire test process.
/// Returns a handle that can render the recorded metrics.
fn global_prometheus_handle() -> PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            let recorder = PrometheusBuilder::new().build_recorder();
            let handle = recorder.handle();
            let _ = metrics::set_global_recorder(recorder);
            handle
        })
        .clone()
}

/// Build the production Axum router — same routes, middleware, and state
/// as `crate::server::TimeSeriesHttpServer::run()` but without binding
/// to a TCP port.
pub fn build_app(tsdb: &TestTsdb) -> Router {
    let handle = global_prometheus_handle();
    let mut metrics = Metrics::new(handle);
    tsdb.storage
        .register_metrics(metrics.storage_registry_mut());
    let engine: Arc<TsdbEngine> = Arc::new(tsdb.inner.clone().into());
    build_router(engine, Arc::new(metrics), OtelServerConfig::default())
}

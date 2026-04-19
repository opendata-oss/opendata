//! HTTP-specific testing helpers.
//!
//! Provides the production Axum router wired to a [`TestTsdb`] for
//! integration tests that exercise HTTP endpoints via `oneshot()`.

use std::sync::Arc;

use axum::Router;

use crate::promql::config::{OtelServerConfig, TracingConfig};
use crate::server::{Metrics, build_router};
use crate::tsdb::TsdbEngine;

use super::TestTsdb;

/// Build the production Axum router — same routes, middleware, and state
/// as `crate::server::TimeSeriesHttpServer::run()` but without binding
/// to a TCP port.
pub fn build_app(tsdb: &TestTsdb) -> Router {
    let handle = super::ensure_metrics_recorder();
    let metrics = Metrics::new(handle);
    let engine: Arc<TsdbEngine> = Arc::new(tsdb.inner.clone().into());
    build_router(
        engine,
        Arc::new(metrics),
        OtelServerConfig::default(),
        TracingConfig::default(),
    )
}

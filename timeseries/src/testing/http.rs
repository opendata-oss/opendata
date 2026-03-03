//! HTTP-specific testing helpers.
//!
//! Provides the production Axum router wired to a [`TestTsdb`] for
//! integration tests that exercise HTTP endpoints via `oneshot()`.

use std::sync::Arc;

use axum::Router;

use crate::server::{Metrics, build_router};

use super::TestTsdb;

/// Build the production Axum router — same routes, middleware, and state
/// as [`crate::server::TimeSeriesHttpServer::run()`] but without binding
/// to a TCP port.
pub fn build_app(tsdb: &TestTsdb) -> Router {
    let mut metrics = Metrics::new();
    tsdb.storage.register_metrics(metrics.registry_mut());
    build_router(tsdb.inner.clone(), Arc::new(metrics))
}

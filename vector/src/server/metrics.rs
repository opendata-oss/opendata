//! Prometheus metrics for the vector server.
//!
//! Vector instrumentation flows through the `metrics-rs` facade. The server
//! binary installs a `PrometheusBuilder` recorder at startup, which captures
//! both vector-server counters emitted via [`metrics::counter!`] and metrics
//! emitted by slatedb through the same facade. The `/metrics` route renders
//! the recorder's accumulated state.

use axum::extract::State;
use metrics_exporter_prometheus::PrometheusHandle;

/// Counter name for vector API requests, labeled by `endpoint`.
pub const API_REQUESTS_TOTAL: &str = "vector_api_requests_total";

/// Endpoint label values for [`API_REQUESTS_TOTAL`].
pub const ENDPOINT_WRITE: &str = "write";
pub const ENDPOINT_DELETE: &str = "delete";
pub const ENDPOINT_SEARCH: &str = "search";

/// Handle to the global Prometheus recorder, shared with the `/metrics` route.
#[derive(Clone)]
pub struct MetricsState {
    pub handle: PrometheusHandle,
}

/// Handle GET /metrics
pub(crate) async fn handle_metrics(State(state): State<MetricsState>) -> String {
    state.handle.render()
}

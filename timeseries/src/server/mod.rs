mod http;
pub(crate) mod metrics;
mod middleware;
#[cfg(feature = "remote-write")]
pub(crate) mod remote_write;

// Re-exports serve different targets (lib vs bin), so not all are used in each.
#[allow(unused_imports)]
pub(crate) use http::{AppState, ServerConfig, TimeSeriesHttpServer, build_router};
#[allow(unused_imports)]
pub(crate) use metrics::Metrics;

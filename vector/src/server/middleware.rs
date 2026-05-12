//! HTTP middleware for Axum.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, Response};
use tower::{Layer, Service};

use super::metrics::{API_REQUESTS_TOTAL, ENDPOINT_DELETE, ENDPOINT_SEARCH, ENDPOINT_WRITE};

/// Layer that wraps services with request tracing.
#[derive(Clone)]
pub struct TracingLayer;

impl TracingLayer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TracingLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for TracingLayer {
    type Service = TracingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingService { inner }
    }
}

/// Service that logs HTTP requests and responses at debug level.
#[derive(Clone)]
pub struct TracingService<S> {
    inner: S,
}

impl<S, ResBody> Service<Request<Body>> for TracingService<S>
where
    S: Service<Request<Body>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ResBody: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let http_method = request.method().clone();
        let uri = request.uri().clone();
        let user_agent = request
            .headers()
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");
        let content_length = request
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("-");

        tracing::debug!(
            method = %http_method,
            uri = %uri,
            user_agent = %user_agent,
            content_length = %content_length,
            "HTTP request received"
        );

        let start_time = Instant::now();
        let future = self.inner.call(request);

        Box::pin(async move {
            let response = future.await?;
            let status = response.status().as_u16();
            let elapsed = start_time.elapsed();

            tracing::debug!(
                method = %http_method,
                uri = %uri,
                status = %status,
                duration_ms = %elapsed.as_millis(),
                "HTTP request completed"
            );

            Ok(response)
        })
    }
}

/// Layer that records per-endpoint API call counters via the metrics-rs facade.
#[derive(Clone, Default)]
pub struct MetricsLayer;

impl MetricsLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService { inner }
    }
}

/// Service that increments `vector_api_requests_total` for write/delete/search calls.
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
}

impl<S, ResBody> Service<Request<Body>> for MetricsService<S>
where
    S: Service<Request<Body>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ResBody: Send,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        if let Some(endpoint) = endpoint_label(request.uri().path()) {
            metrics::counter!(API_REQUESTS_TOTAL, "endpoint" => endpoint).increment(1);
        }
        let future = self.inner.call(request);
        Box::pin(future)
    }
}

/// Map a request path to the endpoint label, or `None` if the path is not
/// one of the counted API calls.
fn endpoint_label(path: &str) -> Option<&'static str> {
    match path {
        "/api/v1/vector/write" => Some(ENDPOINT_WRITE),
        "/api/v1/vector/delete" => Some(ENDPOINT_DELETE),
        "/api/v1/vector/search" => Some(ENDPOINT_SEARCH),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_map_known_endpoints_to_labels() {
        // given/when/then
        assert_eq!(endpoint_label("/api/v1/vector/write"), Some(ENDPOINT_WRITE));
        assert_eq!(
            endpoint_label("/api/v1/vector/delete"),
            Some(ENDPOINT_DELETE)
        );
        assert_eq!(
            endpoint_label("/api/v1/vector/search"),
            Some(ENDPOINT_SEARCH)
        );
    }

    #[test]
    fn should_return_none_for_unmetered_paths() {
        // given/when/then
        assert_eq!(endpoint_label("/-/healthy"), None);
        assert_eq!(endpoint_label("/-/ready"), None);
        assert_eq!(endpoint_label("/metrics"), None);
        assert_eq!(endpoint_label("/api/v1/vector/vectors/abc"), None);
    }
}

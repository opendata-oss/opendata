//! HTTP middleware for Axum.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, Response};
use tower::{Layer, Service};

use super::metrics::{HttpLabelsWithStatus, HttpMethod, Metrics};

/// Layer that wraps services with metrics collection.
#[derive(Clone)]
pub struct MetricsLayer {
    metrics: Arc<Metrics>,
}

impl MetricsLayer {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: self.metrics.clone(),
        }
    }
}

/// Service that collects HTTP metrics.
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics: Arc<Metrics>,
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
        let method = HttpMethod::from(request.method());
        let endpoint = normalize_endpoint(request.uri().path());
        let metrics = self.metrics.clone();

        let future = self.inner.call(request);

        Box::pin(async move {
            let response = future.await?;
            let status = response.status().as_u16();

            // Record request count
            metrics
                .http_requests_total
                .get_or_create(&HttpLabelsWithStatus {
                    method,
                    endpoint,
                    status,
                })
                .inc();

            Ok(response)
        })
    }
}

/// Normalize endpoint paths to avoid high cardinality.
/// Replaces path parameters with placeholders.
fn normalize_endpoint(path: &str) -> String {
    // Handle /api/v1/label/{name}/values -> /api/v1/label/:name/values
    if path.starts_with("/api/v1/label/") && path.ends_with("/values") {
        return "/api/v1/label/:name/values".to_string();
    }
    path.to_string()
}

/// Layer that wraps services with request tracing.
#[derive(Clone)]
pub struct TracingLayer;

impl TracingLayer {
    pub fn new() -> Self {
        Self
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
        // Extract request details for logging
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

        // Log request details
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

            // Log response details
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn should_normalize_label_values_endpoint() {
        // given
        let path = "/api/v1/label/some_label/values";

        // when
        let normalized = normalize_endpoint(path);

        // then
        assert_eq!(normalized, "/api/v1/label/:name/values");
    }

    #[test]
    fn should_preserve_other_endpoints() {
        // given
        let path = "/api/v1/query";

        // when
        let normalized = normalize_endpoint(path);

        // then
        assert_eq!(normalized, "/api/v1/query");
    }

    #[test]
    fn should_preserve_metrics_endpoint() {
        // given
        let path = "/metrics";

        // when
        let normalized = normalize_endpoint(path);

        // then
        assert_eq!(normalized, "/metrics");
    }

    #[tokio::test]
    async fn should_log_request_and_response_with_tracing_middleware() {
        use tower::service_fn;

        // Create a simple test service that returns 200 OK
        let test_service = service_fn(|_req: Request<Body>| async {
            Ok::<_, std::convert::Infallible>(
                Response::builder().status(200).body(Body::empty()).unwrap(),
            )
        });

        // Wrap with tracing middleware
        let mut service = TracingService {
            inner: test_service,
        };

        // Create test request
        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/query?query=up")
            .header("user-agent", "test-client")
            .header("content-length", "0")
            .body(Body::empty())
            .unwrap();

        // Call the service - this should log the request and response
        let response = service.call(request).await.unwrap();

        // Verify response
        assert_eq!(response.status().as_u16(), 200);

        // Note: In a real test environment, you'd capture the log output
        // to verify the tracing messages were emitted correctly
    }
}

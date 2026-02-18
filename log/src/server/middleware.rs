//! HTTP middleware for Axum.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, Response};
use tower::{Layer, Service};

use super::metrics::{HttpLabels, HttpLabelsWithStatus, HttpMethod, Metrics};

/// RAII guard that decrements the in-flight gauge on drop, ensuring the gauge
/// is always decremented even if the request future is cancelled or errors.
struct InFlightGuard {
    metrics: Arc<Metrics>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.metrics.http_requests_in_flight.dec();
    }
}

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

        metrics.http_requests_in_flight.inc();
        let _guard = InFlightGuard {
            metrics: metrics.clone(),
        };
        let start = Instant::now();
        let future = self.inner.call(request);

        Box::pin(async move {
            let _guard = _guard; // move guard into the future so it drops when the future completes
            let response = future.await?;
            let status = response.status().as_u16();
            let duration = start.elapsed().as_secs_f64();

            // Record request count
            metrics
                .http_requests_total
                .get_or_create(&HttpLabelsWithStatus {
                    method: method.clone(),
                    endpoint: endpoint.clone(),
                    status,
                })
                .inc();

            // Record request latency
            metrics
                .http_request_duration_seconds
                .get_or_create(&HttpLabels { method, endpoint })
                .observe(duration);

            Ok(response)
        })
    }
}

/// Normalize endpoint paths to avoid high cardinality.
fn normalize_endpoint(path: &str) -> String {
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
    use tower::service_fn;

    fn test_request(uri: &str) -> Request<Body> {
        Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn should_decrement_in_flight_gauge_after_successful_request() {
        // given
        let metrics = Arc::new(Metrics::new());
        let mut service = MetricsService {
            inner: service_fn(|_req: Request<Body>| async {
                Ok::<_, std::convert::Infallible>(
                    Response::builder().status(200).body(Body::empty()).unwrap(),
                )
            }),
            metrics: metrics.clone(),
        };

        assert_eq!(metrics.http_requests_in_flight.get(), 0);

        // when
        let future = service.call(test_request("/test"));
        assert_eq!(metrics.http_requests_in_flight.get(), 1);
        let response = future.await.unwrap();

        // then
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(metrics.http_requests_in_flight.get(), 0);
    }

    #[tokio::test]
    async fn should_decrement_in_flight_gauge_when_inner_service_errors() {
        // given
        let metrics = Arc::new(Metrics::new());
        let mut service = MetricsService {
            inner: service_fn(|_req: Request<Body>| async {
                Err::<Response<Body>, _>(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection reset",
                ))
            }),
            metrics: metrics.clone(),
        };

        assert_eq!(metrics.http_requests_in_flight.get(), 0);

        // when
        let future = service.call(test_request("/test"));
        assert_eq!(metrics.http_requests_in_flight.get(), 1);
        let result = future.await;

        // then
        assert!(result.is_err());
        assert_eq!(metrics.http_requests_in_flight.get(), 0);
    }

    #[tokio::test]
    async fn should_decrement_in_flight_gauge_when_future_is_cancelled() {
        // given — a service that never completes (simulates a long-poll)
        let metrics = Arc::new(Metrics::new());
        let mut service = MetricsService {
            inner: service_fn(|_req: Request<Body>| {
                std::future::pending::<Result<Response<Body>, std::convert::Infallible>>()
            }),
            metrics: metrics.clone(),
        };

        assert_eq!(metrics.http_requests_in_flight.get(), 0);

        // when — call returns a future (gauge incremented), then drop it
        let future = service.call(test_request("/api/v1/log/scan"));
        assert_eq!(metrics.http_requests_in_flight.get(), 1);

        drop(future);

        // then — gauge must return to zero despite the future never completing
        assert_eq!(metrics.http_requests_in_flight.get(), 0);
    }

    #[test]
    fn should_preserve_api_endpoints() {
        // given
        let path = "/api/v1/log/scan";

        // when
        let normalized = normalize_endpoint(path);

        // then
        assert_eq!(normalized, "/api/v1/log/scan");
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
            .uri("/api/v1/log/scan?key=test")
            .header("user-agent", "test-client")
            .header("content-length", "0")
            .body(Body::empty())
            .unwrap();

        // Call the service - this should log the request and response
        let response = service.call(request).await.unwrap();

        // Verify response
        assert_eq!(response.status().as_u16(), 200);
    }
}

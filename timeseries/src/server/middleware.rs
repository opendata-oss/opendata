//! HTTP middleware for Axum.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use axum::http::{Method, Request, Response};
use tower::{Layer, Service};

use super::metrics::{HTTP_REQUEST_DURATION_SECONDS, HTTP_REQUESTS_IN_FLIGHT, HTTP_REQUESTS_TOTAL};

/// RAII guard that decrements the in-flight gauge on drop, ensuring the gauge
/// is always decremented even if the request future is cancelled or errors.
struct InFlightGuard {
    in_flight: Arc<AtomicI64>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let prev = self.in_flight.fetch_sub(1, Ordering::Relaxed);
        metrics::gauge!(HTTP_REQUESTS_IN_FLIGHT).set((prev - 1) as f64);
    }
}

/// Layer that wraps services with metrics collection.
#[derive(Clone)]
pub struct MetricsLayer {
    in_flight: Arc<AtomicI64>,
}

impl MetricsLayer {
    pub fn new() -> Self {
        Self {
            in_flight: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Return the current in-flight request count (for testing).
    #[cfg(test)]
    pub fn in_flight(&self) -> i64 {
        self.in_flight.load(Ordering::Relaxed)
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            in_flight: self.in_flight.clone(),
        }
    }
}

/// Service that collects HTTP metrics.
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    in_flight: Arc<AtomicI64>,
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
        let method = method_label(request.method());
        let endpoint = normalize_endpoint(request.uri().path());
        let in_flight = self.in_flight.clone();

        let val = in_flight.fetch_add(1, Ordering::Relaxed);
        metrics::gauge!(HTTP_REQUESTS_IN_FLIGHT).set((val + 1) as f64);
        let _guard = InFlightGuard {
            in_flight: in_flight.clone(),
        };
        let start = Instant::now();
        let future = self.inner.call(request);

        Box::pin(async move {
            let _guard = _guard; // move guard into the future so it drops when the future completes
            let response = future.await?;
            let status = response.status().as_u16().to_string();
            let duration = start.elapsed().as_secs_f64();

            metrics::counter!(
                HTTP_REQUESTS_TOTAL,
                "method" => method.clone(),
                "endpoint" => endpoint.clone(),
                "status" => status,
            )
            .increment(1);

            metrics::histogram!(
                HTTP_REQUEST_DURATION_SECONDS,
                "method" => method,
                "endpoint" => endpoint,
            )
            .record(duration);

            Ok(response)
        })
    }
}

fn method_label(method: &Method) -> String {
    match *method {
        Method::GET => "GET".to_string(),
        Method::POST => "POST".to_string(),
        Method::PUT => "PUT".to_string(),
        Method::DELETE => "DELETE".to_string(),
        Method::PATCH => "PATCH".to_string(),
        Method::HEAD => "HEAD".to_string(),
        Method::OPTIONS => "OPTIONS".to_string(),
        _ => "OTHER".to_string(),
    }
}

/// Normalize endpoint paths to avoid high cardinality.
/// Replaces path parameters with placeholders.
fn normalize_endpoint(path: &str) -> String {
    // Handle /api/v1/label/{name}/values -> /api/v1/label/:name/values
    if path.starts_with("/api/v1/label/") && path.ends_with("/values") {
        return "/api/v1/label/:name/values".to_string();
    }
    // Group UI asset requests to prevent high-cardinality metrics
    if !path.starts_with("/api/") && !path.starts_with("/-/") && path != "/metrics" {
        return "/ui".to_string();
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
        let layer = MetricsLayer::new();
        let mut service = layer.layer(service_fn(|_req: Request<Body>| async {
            Ok::<_, std::convert::Infallible>(
                Response::builder().status(200).body(Body::empty()).unwrap(),
            )
        }));

        assert_eq!(layer.in_flight(), 0);

        // when
        let future = service.call(test_request("/test"));
        assert_eq!(layer.in_flight(), 1);
        let response = future.await.unwrap();

        // then
        assert_eq!(response.status().as_u16(), 200);
        assert_eq!(layer.in_flight(), 0);
    }

    #[tokio::test]
    async fn should_decrement_in_flight_gauge_when_inner_service_errors() {
        // given
        let layer = MetricsLayer::new();
        let mut service = layer.layer(service_fn(|_req: Request<Body>| async {
            Err::<Response<Body>, _>(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection reset",
            ))
        }));

        assert_eq!(layer.in_flight(), 0);

        // when
        let future = service.call(test_request("/test"));
        assert_eq!(layer.in_flight(), 1);
        let result = future.await;

        // then
        assert!(result.is_err());
        assert_eq!(layer.in_flight(), 0);
    }

    #[tokio::test]
    async fn should_decrement_in_flight_gauge_when_future_is_cancelled() {
        // given — a service that never completes (simulates a long-poll)
        let layer = MetricsLayer::new();
        let mut service = layer.layer(service_fn(|_req: Request<Body>| {
            std::future::pending::<Result<Response<Body>, std::convert::Infallible>>()
        }));

        assert_eq!(layer.in_flight(), 0);

        // when — call returns a future (gauge incremented), then drop it
        let future = service.call(test_request("/api/v1/query"));
        assert_eq!(layer.in_flight(), 1);

        drop(future);

        // then — gauge must return to zero despite the future never completing
        assert_eq!(layer.in_flight(), 0);
    }

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

    #[test]
    fn should_normalize_ui_paths_to_ui() {
        // given / when / then
        assert_eq!(normalize_endpoint("/"), "/ui");
        assert_eq!(normalize_endpoint("/style.css"), "/ui");
        assert_eq!(normalize_endpoint("/app.js"), "/ui");
        assert_eq!(normalize_endpoint("/index.html"), "/ui");
    }

    #[test]
    fn should_preserve_health_endpoints() {
        // given / when / then
        assert_eq!(normalize_endpoint("/-/healthy"), "/-/healthy");
        assert_eq!(normalize_endpoint("/-/ready"), "/-/ready");
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

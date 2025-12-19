//! HTTP metrics middleware for Axum.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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

#[cfg(test)]
mod tests {
    use super::*;

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
}

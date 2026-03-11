//! HTTP middleware for Axum.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, Response};
use tower::{Layer, Service};

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

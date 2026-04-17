//! HTTP middleware for Axum.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::body::Body;
use axum::http::{HeaderValue, Request, Response};
use common::tracing::{request_trace_span, request_trace_span_with_filter};
use tower::{Layer, Service};
use tracing::Instrument;

/// HTTP header that opts a single request into query tracing. When present
/// with a truthy value (`true`, `1`, `yes`), the handler runs under a
/// [`request_trace_span`] so all instrumented sub-operations land in the
/// Chrome trace file.
pub const TRACE_OPT_IN_HEADER: &str = "x-opendata-trace";
/// Optional HTTP header with an `EnvFilter` directive string applied as the
/// per-request level filter (e.g. `vector::query_engine=debug`). When
/// present, replaces the process-wide `OPENDATA_TRACE` for this request's
/// descendant spans. Implies tracing opt-in.
pub const TRACE_FILTER_HEADER: &str = "x-opendata-trace-filter";
/// Response header that carries the assigned trace ID back to the client.
pub const TRACE_ID_HEADER: &str = "x-opendata-trace-id";
/// Query parameter that opts into tracing (alternative to the header — easier
/// to set from a browser / curl one-liner).
pub const TRACE_OPT_IN_QUERY_PARAM: &str = "trace";
/// Query parameter form of [`TRACE_FILTER_HEADER`].
pub const TRACE_FILTER_QUERY_PARAM: &str = "trace_filter";

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

        let filter_directive = trace_filter_directive(&request);
        let (trace_id, trace_span) = if trace_opt_in(&request) || filter_directive.is_some() {
            let id = ulid::Ulid::new().to_string();
            let span = match &filter_directive {
                Some(filter) => request_trace_span_with_filter(&id, filter),
                None => request_trace_span(&id),
            };
            (Some(id), Some(span))
        } else {
            (None, None)
        };

        let start_time = Instant::now();
        let future = self.inner.call(request);

        Box::pin(async move {
            let mut response = match trace_span {
                Some(span) => future.instrument(span).await?,
                None => future.await?,
            };
            let status = response.status().as_u16();
            let elapsed = start_time.elapsed();

            tracing::debug!(
                method = %http_method,
                uri = %uri,
                status = %status,
                duration_ms = %elapsed.as_millis(),
                "HTTP request completed"
            );

            if let Some(id) = trace_id.as_deref()
                && let Ok(value) = HeaderValue::from_str(id)
            {
                response.headers_mut().insert(TRACE_ID_HEADER, value);
            }

            Ok(response)
        })
    }
}

/// Returns true if the request opts into tracing via the
/// `X-Opendata-Trace` header or `?trace=1` query parameter.
fn trace_opt_in(request: &Request<Body>) -> bool {
    if request
        .headers()
        .get(TRACE_OPT_IN_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(is_truthy)
        .unwrap_or(false)
    {
        return true;
    }
    query_param_value(request, TRACE_OPT_IN_QUERY_PARAM)
        .map(|v| is_truthy(&v))
        .unwrap_or(false)
}

/// Returns the `EnvFilter` directive string opted in via the
/// `X-Opendata-Trace-Filter` header or `?trace_filter=...` query parameter,
/// if any.
fn trace_filter_directive(request: &Request<Body>) -> Option<String> {
    if let Some(value) = request
        .headers()
        .get(TRACE_FILTER_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }
    query_param_value(request, TRACE_FILTER_QUERY_PARAM).and_then(|raw| {
        // URL-decode common ASCII escapes so curl users can pass
        // `?trace_filter=vector%3A%3Aquery_engine=debug`.
        let decoded = percent_decode(&raw);
        let trimmed = decoded.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn query_param_value(request: &Request<Body>, key: &str) -> Option<String> {
    let query = request.uri().query()?;
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        let k = parts.next().unwrap_or("");
        let v = parts.next().unwrap_or("");
        if k.eq_ignore_ascii_case(key) {
            return Some(v.to_string());
        }
    }
    None
}

/// Minimal percent-decode for the handful of escapes a `trace_filter`
/// directive actually needs (`%3A` for `:`, `%3D` for `=`, etc.).
/// Non-ASCII / malformed escapes are preserved as-is.
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2]))
        {
            out.push((h << 4) | l);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;

    #[test]
    fn should_detect_trace_opt_in_via_header() {
        // given
        let req = Request::builder()
            .uri("/api/v1/vector/search")
            .header(TRACE_OPT_IN_HEADER, "true")
            .body(Body::empty())
            .unwrap();

        // when / then
        assert!(trace_opt_in(&req));
    }

    #[test]
    fn should_detect_trace_opt_in_via_query_param() {
        // given
        let req = Request::builder()
            .uri("/api/v1/vector/search?trace=1&other=x")
            .body(Body::empty())
            .unwrap();

        // when / then
        assert!(trace_opt_in(&req));
    }

    #[test]
    fn should_reject_trace_opt_in_when_absent() {
        // given
        let req = Request::builder()
            .uri("/api/v1/vector/search")
            .body(Body::empty())
            .unwrap();

        // when / then
        assert!(!trace_opt_in(&req));
    }

    #[test]
    fn should_reject_trace_opt_in_when_header_falsy() {
        // given
        let req = Request::builder()
            .uri("/api/v1/vector/search")
            .header(TRACE_OPT_IN_HEADER, "false")
            .body(Body::empty())
            .unwrap();

        // when / then
        assert!(!trace_opt_in(&req));
    }

    #[test]
    fn should_extract_trace_filter_directive_from_header() {
        // given
        let req = Request::builder()
            .uri("/api/v1/vector/search")
            .header(TRACE_FILTER_HEADER, "vector::query_engine=debug")
            .body(Body::empty())
            .unwrap();

        // when / then
        assert_eq!(
            trace_filter_directive(&req).as_deref(),
            Some("vector::query_engine=debug")
        );
    }

    #[test]
    fn should_percent_decode_trace_filter_query_param() {
        // given - URL-encoded colons in the module path
        let req = Request::builder()
            .uri("/api/v1/vector/search?trace_filter=vector%3A%3Aquery_engine%3Ddebug")
            .body(Body::empty())
            .unwrap();

        // when / then
        assert_eq!(
            trace_filter_directive(&req).as_deref(),
            Some("vector::query_engine=debug")
        );
    }

    #[test]
    fn should_return_none_when_trace_filter_header_is_empty() {
        // given
        let req = Request::builder()
            .uri("/api/v1/vector/search")
            .header(TRACE_FILTER_HEADER, "   ")
            .body(Body::empty())
            .unwrap();

        // when / then
        assert!(trace_filter_directive(&req).is_none());
    }
}

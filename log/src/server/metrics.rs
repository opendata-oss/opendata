//! Prometheus metrics for the log server.

use axum::http::Method;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

/// Labels for HTTP request metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct HttpLabelsWithStatus {
    pub method: HttpMethod,
    pub endpoint: String,
    pub status: u16,
}

/// HTTP method label value.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
    Other,
}

impl From<&Method> for HttpMethod {
    fn from(method: &Method) -> Self {
        match *method {
            Method::GET => HttpMethod::Get,
            Method::POST => HttpMethod::Post,
            Method::PUT => HttpMethod::Put,
            Method::DELETE => HttpMethod::Delete,
            Method::PATCH => HttpMethod::Patch,
            Method::HEAD => HttpMethod::Head,
            Method::OPTIONS => HttpMethod::Options,
            _ => HttpMethod::Other,
        }
    }
}

/// Labels for HTTP request latency histogram (without status, since status is unknown at start).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct HttpLabels {
    pub method: HttpMethod,
    pub endpoint: String,
}

/// Container for all Prometheus metrics.
pub struct Metrics {
    registry: Registry,

    /// Counter of records successfully appended.
    pub log_append_records_total: Counter,

    /// Counter of bytes written
    pub log_append_bytes_total: Counter,

    /// Counter of records scanned
    pub log_records_scanned_total: Counter,

    /// Counter of bytes scanned
    pub log_bytes_scanned_total: Counter,

    /// Counter of HTTP requests.
    pub http_requests_total: Family<HttpLabelsWithStatus, Counter>,

    /// Histogram of HTTP request latency in seconds.
    pub http_request_duration_seconds: Family<HttpLabels, Histogram>,

    /// Gauge of currently in-flight requests.
    pub http_requests_in_flight: Gauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics registry with all metrics registered.
    pub fn new() -> Self {
        let mut registry = Registry::default();

        // Log append records counter
        let log_append_records_total = Counter::default();
        registry.register(
            "log_append_records_total",
            "Total number of records appended to the log",
            log_append_records_total.clone(),
        );

        // Log append bytes counter
        let log_append_bytes_total = Counter::default();
        registry.register(
            "log_append_bytes_total",
            "Total number of bytes written to the log",
            log_append_bytes_total.clone(),
        );

        // Log scan records counter
        let log_records_scanned_total = Counter::default();
        registry.register(
            "log_records_scanned_total",
            "Total number of records scanned in the log",
            log_records_scanned_total.clone(),
        );

        // Log scan bytes counter
        let log_bytes_scanned_total = Counter::default();
        registry.register(
            "log_bytes_scanned_total",
            "Total number of bytes scanned in the log",
            log_bytes_scanned_total.clone(),
        );

        // HTTP requests total counter
        let http_requests_total = Family::<HttpLabelsWithStatus, Counter>::default();
        registry.register(
            "http_requests_total",
            "Total number of HTTP requests",
            http_requests_total.clone(),
        );

        // HTTP request duration histogram (buckets from 1ms to ~8s)
        let http_request_duration_seconds =
            Family::<HttpLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.001, 2.0, 14))
            });
        registry.register(
            "http_request_duration_seconds",
            "HTTP request latency in seconds",
            http_request_duration_seconds.clone(),
        );

        // In-flight requests gauge
        let http_requests_in_flight = Gauge::default();
        registry.register(
            "http_requests_in_flight",
            "Number of HTTP requests currently being processed",
            http_requests_in_flight.clone(),
        );

        Self {
            registry,
            log_append_records_total,
            log_append_bytes_total,
            log_records_scanned_total,
            log_bytes_scanned_total,
            http_requests_total,
            http_request_duration_seconds,
            http_requests_in_flight,
        }
    }

    /// Returns a mutable reference to the underlying Prometheus registry.
    ///
    /// Use this to register additional metrics (e.g. storage engine metrics)
    /// before wrapping `Metrics` in an `Arc`.
    pub fn registry_mut(&mut self) -> &mut Registry {
        &mut self.registry
    }

    /// Encode all metrics to Prometheus text format.
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        prometheus_client::encoding::text::encode(&mut buffer, &self.registry)
            .expect("encoding metrics should not fail");
        buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_default_metrics() {
        // given/when
        let metrics = Metrics::new();

        // then
        let encoded = metrics.encode();
        assert!(encoded.contains("# HELP log_append_records_total"));
        assert!(encoded.contains("# HELP log_append_bytes_total"));
        assert!(encoded.contains("# HELP log_records_scanned_total"));
        assert!(encoded.contains("# HELP log_bytes_scanned_total"));
        assert!(encoded.contains("# HELP http_requests_total"));
        assert!(encoded.contains("# HELP http_request_duration_seconds"));
        assert!(encoded.contains("# HELP http_requests_in_flight"));
    }

    #[test]
    fn should_convert_http_method_to_label() {
        // given
        let method = Method::GET;

        // when
        let label = HttpMethod::from(&method);

        // then
        assert!(matches!(label, HttpMethod::Get));
    }
}

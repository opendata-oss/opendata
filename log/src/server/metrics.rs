//! Prometheus metrics for the log server.

use axum::http::Method;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
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

        Self {
            registry,
            log_append_records_total,
            log_append_bytes_total,
            log_records_scanned_total,
            log_bytes_scanned_total,
            http_requests_total,
        }
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
    }

    #[test]
    fn should_increment_log_metrics() {
        // given
        let metrics = Metrics::new();

        // when
        metrics.log_append_bytes_total.inc_by(100);
        metrics.log_records_scanned_total.inc_by(5);
        metrics.log_bytes_scanned_total.inc_by(500);

        // then
        let encoded = metrics.encode();
        assert!(encoded.contains("log_append_bytes_total_total 100"));
        assert!(encoded.contains("log_records_scanned_total_total 5"));
        assert!(encoded.contains("log_bytes_scanned_total_total 500"));
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

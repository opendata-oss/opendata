//! Prometheus metrics for the log server.

use axum::http::Method;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;

/// Labels for append operation metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct AppendLabels {
    pub status: OperationStatus,
}

/// Labels for scan operation metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ScanLabels {
    pub status: OperationStatus,
}

/// Operation status for metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum OperationStatus {
    Success,
    Error,
}

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

    /// Counter of append requests by status.
    pub log_append_requests_total: Family<AppendLabels, Counter>,

    /// Counter of scan requests by status.
    pub log_scan_requests_total: Family<ScanLabels, Counter>,

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

        // Log append requests counter
        let log_append_requests_total = Family::<AppendLabels, Counter>::default();
        registry.register(
            "log_append_requests_total",
            "Total number of append requests by status",
            log_append_requests_total.clone(),
        );

        // Log scan requests counter
        let log_scan_requests_total = Family::<ScanLabels, Counter>::default();
        registry.register(
            "log_scan_requests_total",
            "Total number of scan requests by status",
            log_scan_requests_total.clone(),
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
            log_append_requests_total,
            log_scan_requests_total,
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
        assert!(encoded.contains("# HELP log_append_requests_total"));
        assert!(encoded.contains("# HELP log_scan_requests_total"));
        assert!(encoded.contains("# HELP http_requests_total"));
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

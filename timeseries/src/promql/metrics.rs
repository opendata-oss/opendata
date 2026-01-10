//! Prometheus metrics for the timeseries server.

use axum::http::Method;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::registry::Registry;

/// Labels for scrape metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ScrapeLabels {
    pub job: String,
    pub instance: String,
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

    /// Counter of samples successfully scraped.
    pub scrape_samples_scraped: Family<ScrapeLabels, Counter>,

    /// Counter of failed samples.
    pub scrape_samples_failed: Family<ScrapeLabels, Counter>,

    /// Counter of HTTP requests.
    pub http_requests_total: Family<HttpLabelsWithStatus, Counter>,

    /// Counter of samples successfully ingested via remote write.
    pub remote_write_samples_ingested: Counter,

    /// Counter of samples that failed to ingest via remote write.
    pub remote_write_samples_failed: Counter,
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

        // Scrape samples scraped counter
        let scrape_samples_scraped = Family::<ScrapeLabels, Counter>::default();
        registry.register(
            "scrape_samples_scraped",
            "Number of samples scraped per target",
            scrape_samples_scraped.clone(),
        );

        // Scrape samples failed counter
        let scrape_samples_failed = Family::<ScrapeLabels, Counter>::default();
        registry.register(
            "scrape_samples_failed",
            "Number of failed samples per target",
            scrape_samples_failed.clone(),
        );

        // HTTP requests total counter
        let http_requests_total = Family::<HttpLabelsWithStatus, Counter>::default();
        registry.register(
            "http_requests_total",
            "Total number of HTTP requests",
            http_requests_total.clone(),
        );

        // Remote write samples ingested counter
        let remote_write_samples_ingested = Counter::default();
        registry.register(
            "remote_write_samples_ingested_total",
            "Total number of samples successfully ingested via remote write",
            remote_write_samples_ingested.clone(),
        );

        // Remote write samples failed counter
        let remote_write_samples_failed = Counter::default();
        registry.register(
            "remote_write_samples_failed_total",
            "Total number of samples that failed to ingest via remote write",
            remote_write_samples_failed.clone(),
        );

        Self {
            registry,
            scrape_samples_scraped,
            scrape_samples_failed,
            http_requests_total,
            remote_write_samples_ingested,
            remote_write_samples_failed,
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
        assert!(encoded.contains("# HELP scrape_samples_scraped"));
        assert!(encoded.contains("# HELP http_requests_total"));
        assert!(encoded.contains("# HELP remote_write_samples_ingested_total"));
        assert!(encoded.contains("# HELP remote_write_samples_failed_total"));
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

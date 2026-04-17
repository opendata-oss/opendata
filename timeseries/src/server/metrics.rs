//! Prometheus metrics for the timeseries server.

use metrics_exporter_prometheus::PrometheusHandle;

// ── Metric name constants ──

pub(crate) const HTTP_REQUESTS_TOTAL: &str = "http_requests_total";
pub(crate) const HTTP_REQUEST_DURATION_SECONDS: &str = "http_request_duration_seconds";
pub(crate) const HTTP_REQUESTS_IN_FLIGHT: &str = "http_requests_in_flight";
pub(crate) const SCRAPE_SAMPLES_SCRAPED: &str = "scrape_samples_scraped";
pub(crate) const SCRAPE_SAMPLES_FAILED: &str = "scrape_samples_failed";
pub(crate) const REMOTE_WRITE_SAMPLES_INGESTED: &str = "remote_write_samples_ingested_total";
pub(crate) const REMOTE_WRITE_SAMPLES_FAILED: &str = "remote_write_samples_failed_total";

fn describe_metrics() {
    metrics::describe_counter!(HTTP_REQUESTS_TOTAL, "Total number of HTTP requests");
    metrics::describe_histogram!(
        HTTP_REQUEST_DURATION_SECONDS,
        "HTTP request latency in seconds"
    );
    metrics::describe_gauge!(
        HTTP_REQUESTS_IN_FLIGHT,
        "Number of HTTP requests currently being processed"
    );
    metrics::describe_counter!(
        SCRAPE_SAMPLES_SCRAPED,
        "Number of samples scraped per target"
    );
    metrics::describe_counter!(SCRAPE_SAMPLES_FAILED, "Number of failed samples per target");
    metrics::describe_counter!(
        REMOTE_WRITE_SAMPLES_INGESTED,
        "Total number of samples successfully ingested via remote write"
    );
    metrics::describe_counter!(
        REMOTE_WRITE_SAMPLES_FAILED,
        "Total number of samples that failed to ingest via remote write"
    );
    crate::tsdb_metrics::describe_engine_metrics();
}

/// Container for metrics rendering.
///
/// Uses `metrics-rs` for recording (callers use `metrics::counter!()` etc.)
/// and `metrics-exporter-prometheus` for rendering. SlateDB metrics are
/// also routed through `metrics-rs` via the `MetricsRsRecorder`.
pub struct Metrics {
    handle: PrometheusHandle,
}

impl Metrics {
    /// Create a new metrics instance and install the global recorder.
    ///
    /// Only the first call per process installs the recorder; subsequent calls
    /// (e.g. in tests) reuse the existing one but get their own handle.
    pub fn new(handle: PrometheusHandle) -> Self {
        describe_metrics();
        Self { handle }
    }

    /// Encode all metrics to Prometheus text format.
    pub fn encode(&self) -> String {
        self.handle.render()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_exporter_prometheus::PrometheusBuilder;

    #[test]
    fn should_create_metrics_and_encode() {
        // given
        let builder = PrometheusBuilder::new();
        let recorder = builder.build_recorder();
        let handle = recorder.handle();

        // when
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("test_counter").increment(1);
        });
        let metrics = Metrics::new(handle);
        let encoded = metrics.encode();

        // then
        assert!(encoded.contains("test_counter"));
    }
}

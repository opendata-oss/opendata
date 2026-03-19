//! Prometheus metrics for the timeseries server.

use axum::http::Method;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;

// ── Label types ──────────────────────────────────────────────────────

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

/// Labels for HTTP request latency histogram (without status, since status is unknown at start).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct HttpLabels {
    pub method: HttpMethod,
    pub endpoint: String,
}

// ── Query metrics labels ─────────────────────────────────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct QueryLabels {
    pub operation: QueryOperation,
    pub status: QueryStatus,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub(crate) enum QueryOperation {
    QueryRange,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub(crate) enum QueryStatus {
    Ok,
    Error,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct QueryPhaseLabels {
    pub operation: QueryOperation,
    pub phase: QueryPhase,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub(crate) enum QueryPhase {
    Preload,
    StepLoop,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct QueryWorkLabels {
    pub operation: QueryOperation,
    pub kind: QueryWorkKind,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub(crate) enum QueryWorkKind {
    MetadataQueueWait,
    MetadataLoad,
    SampleQueueWait,
    SampleLoad,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct QueryOperationLabels {
    pub operation: QueryOperation,
}

// ── Warmer metrics labels ────────────────────────────────────────────

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct WarmerCycleLabels {
    pub status: QueryStatus,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct WarmerBucketWarmLabels {
    pub unit: WarmerUnit,
    pub reason: WarmerReason,
    pub status: QueryStatus,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct WarmerBytesLabels {
    pub unit: WarmerUnit,
    pub reason: WarmerReason,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub(crate) enum WarmerUnit {
    BucketList,
    ForwardIndex,
    InvertedIndex,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub(crate) enum WarmerReason {
    Initial,
    NewBucket,
    Rewarm,
}

// ── Grouped metric families ──────────────────────────────────────────

pub struct HttpMetrics {
    pub requests_total: Family<HttpLabelsWithStatus, Counter>,
    pub request_duration_seconds: Family<HttpLabels, Histogram>,
    pub requests_in_flight: Gauge,
}

pub struct ScrapeMetrics {
    pub samples_scraped: Family<ScrapeLabels, Counter>,
    pub samples_failed: Family<ScrapeLabels, Counter>,
}

pub struct RemoteWriteMetrics {
    pub samples_ingested: Counter,
    pub samples_failed: Counter,
}

pub(crate) struct QueryMetrics {
    pub requests_total: Family<QueryLabels, Counter>,
    pub duration_seconds: Family<QueryLabels, Histogram>,
    pub phase_wall_duration_seconds: Family<QueryPhaseLabels, Histogram>,
    pub phase_work_duration_seconds: Family<QueryWorkLabels, Histogram>,
    pub samples_loaded_total: Family<QueryOperationLabels, Counter>,
    pub forward_index_series_loaded_total: Family<QueryOperationLabels, Counter>,
}

pub(crate) struct MetadataWarmerMetrics {
    pub enabled: Gauge,
    pub target_bytes: Gauge,
    pub estimated_target_bytes: Gauge,
    pub warmed_buckets: Gauge,
    pub oldest_target_bucket_start_minutes: Gauge,
    pub inflight_buckets: Gauge,
    pub cycles_total: Family<WarmerCycleLabels, Counter>,
    pub bucket_warms_total: Family<WarmerBucketWarmLabels, Counter>,
    pub bytes_read_total: Family<WarmerBytesLabels, Counter>,
    pub cycle_duration_seconds: Family<WarmerCycleLabels, Histogram>,
    pub bucket_warm_duration_seconds: Family<WarmerBucketWarmLabels, Histogram>,
    pub cycle_buckets_selected: Gauge,
    pub cycle_buckets_warmed: Gauge,
    pub cycle_new_buckets: Gauge,
    pub cycle_rewarmed_buckets: Gauge,
    pub cycle_skipped_fresh_buckets: Gauge,
    pub consecutive_failures: Gauge,
}

// ── Metrics container ────────────────────────────────────────────────

/// Container for all Prometheus metrics, grouped by subsystem.
pub struct Metrics {
    registry: Registry,
    pub http: HttpMetrics,
    pub scrape: ScrapeMetrics,
    pub remote_write: RemoteWriteMetrics,
    pub(crate) query: QueryMetrics,
    pub(crate) warmer: MetadataWarmerMetrics,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Histogram buckets for query durations: 10ms to ~40s (exponential).
fn query_duration_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.01, 2.0, 12)
}

/// Histogram buckets for warmer durations: 1ms to ~8s (exponential).
fn warmer_duration_buckets() -> impl Iterator<Item = f64> {
    exponential_buckets(0.001, 2.0, 14)
}

impl Metrics {
    /// Create a new metrics registry with all metrics registered.
    pub fn new() -> Self {
        let mut registry = Registry::default();

        // ── HTTP metrics ─────────────────────────────────────────
        let http_requests_total = Family::<HttpLabelsWithStatus, Counter>::default();
        registry.register(
            "http_requests_total",
            "Total number of HTTP requests",
            http_requests_total.clone(),
        );

        let http_request_duration_seconds =
            Family::<HttpLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.001, 2.0, 14))
            });
        registry.register(
            "http_request_duration_seconds",
            "HTTP request latency in seconds",
            http_request_duration_seconds.clone(),
        );

        let http_requests_in_flight = Gauge::default();
        registry.register(
            "http_requests_in_flight",
            "Number of HTTP requests currently being processed",
            http_requests_in_flight.clone(),
        );

        // ── Scrape metrics ───────────────────────────────────────
        let scrape_samples_scraped = Family::<ScrapeLabels, Counter>::default();
        registry.register(
            "scrape_samples_scraped",
            "Number of samples scraped per target",
            scrape_samples_scraped.clone(),
        );

        let scrape_samples_failed = Family::<ScrapeLabels, Counter>::default();
        registry.register(
            "scrape_samples_failed",
            "Number of failed samples per target",
            scrape_samples_failed.clone(),
        );

        // ── Remote write metrics ─────────────────────────────────
        let remote_write_samples_ingested = Counter::default();
        registry.register(
            "remote_write_samples_ingested_total",
            "Total number of samples successfully ingested via remote write",
            remote_write_samples_ingested.clone(),
        );

        let remote_write_samples_failed = Counter::default();
        registry.register(
            "remote_write_samples_failed_total",
            "Total number of samples that failed to ingest via remote write",
            remote_write_samples_failed.clone(),
        );

        // ── Query metrics ────────────────────────────────────────
        let query_requests_total = Family::<QueryLabels, Counter>::default();
        registry.register(
            "tsdb_query_requests_total",
            "Total number of TSDB queries",
            query_requests_total.clone(),
        );

        let query_duration_seconds = Family::<QueryLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(query_duration_buckets())
        });
        registry.register(
            "tsdb_query_duration_seconds",
            "Total query duration in seconds",
            query_duration_seconds.clone(),
        );

        let query_phase_wall = Family::<QueryPhaseLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(query_duration_buckets())
        });
        registry.register(
            "tsdb_query_phase_wall_duration_seconds",
            "Wall-clock duration of query phases in seconds",
            query_phase_wall.clone(),
        );

        let query_phase_work = Family::<QueryWorkLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(query_duration_buckets())
        });
        registry.register(
            "tsdb_query_phase_work_duration_seconds",
            "Aggregated work duration of query phases in seconds",
            query_phase_work.clone(),
        );

        let query_samples_loaded = Family::<QueryOperationLabels, Counter>::default();
        registry.register(
            "tsdb_query_samples_loaded_total",
            "Total samples loaded across all queries",
            query_samples_loaded.clone(),
        );

        let query_fi_series_loaded = Family::<QueryOperationLabels, Counter>::default();
        registry.register(
            "tsdb_query_forward_index_series_loaded_total",
            "Total forward index series loaded across all queries",
            query_fi_series_loaded.clone(),
        );

        // ── Metadata warmer metrics ──────────────────────────────
        let warmer_enabled = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_enabled",
            "Whether the metadata warmer is enabled (1) or disabled (0)",
            warmer_enabled.clone(),
        );

        let warmer_target_bytes = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_target_bytes",
            "Configured target byte budget for metadata warming",
            warmer_target_bytes.clone(),
        );

        let warmer_estimated_target_bytes = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_estimated_target_bytes",
            "Estimated bytes of metadata in the current target set",
            warmer_estimated_target_bytes.clone(),
        );

        let warmer_warmed_buckets = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_warmed_buckets",
            "Number of buckets in the current warm target set",
            warmer_warmed_buckets.clone(),
        );

        let warmer_oldest_start = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_oldest_target_bucket_start_minutes",
            "Start time (epoch minutes) of the oldest bucket in the target set",
            warmer_oldest_start.clone(),
        );

        let warmer_inflight = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_inflight_buckets",
            "Number of buckets currently being warmed",
            warmer_inflight.clone(),
        );

        let warmer_cycles_total = Family::<WarmerCycleLabels, Counter>::default();
        registry.register(
            "tsdb_metadata_warmer_cycles_total",
            "Total number of warmer cycles",
            warmer_cycles_total.clone(),
        );

        let warmer_bucket_warms = Family::<WarmerBucketWarmLabels, Counter>::default();
        registry.register(
            "tsdb_metadata_warmer_bucket_warms_total",
            "Total number of bucket warm operations",
            warmer_bucket_warms.clone(),
        );

        let warmer_bytes_read = Family::<WarmerBytesLabels, Counter>::default();
        registry.register(
            "tsdb_metadata_warmer_bytes_read_total",
            "Total bytes read by the metadata warmer",
            warmer_bytes_read.clone(),
        );

        let warmer_cycle_duration =
            Family::<WarmerCycleLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(warmer_duration_buckets())
            });
        registry.register(
            "tsdb_metadata_warmer_cycle_duration_seconds",
            "Duration of warmer cycles in seconds",
            warmer_cycle_duration.clone(),
        );

        let warmer_bucket_warm_duration =
            Family::<WarmerBucketWarmLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(warmer_duration_buckets())
            });
        registry.register(
            "tsdb_metadata_warmer_bucket_warm_duration_seconds",
            "Duration of individual bucket warm operations in seconds",
            warmer_bucket_warm_duration.clone(),
        );

        let warmer_cycle_buckets_selected = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_cycle_buckets_selected",
            "Number of buckets selected in the last cycle",
            warmer_cycle_buckets_selected.clone(),
        );

        let warmer_cycle_buckets_warmed = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_cycle_buckets_warmed",
            "Number of buckets warmed in the last cycle",
            warmer_cycle_buckets_warmed.clone(),
        );

        let warmer_cycle_new = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_cycle_new_buckets",
            "Number of new buckets warmed in the last cycle",
            warmer_cycle_new.clone(),
        );

        let warmer_cycle_rewarmed = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_cycle_rewarmed_buckets",
            "Number of stale buckets rewarmed in the last cycle",
            warmer_cycle_rewarmed.clone(),
        );

        let warmer_cycle_skipped = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_cycle_skipped_fresh_buckets",
            "Number of fresh buckets skipped in the last cycle",
            warmer_cycle_skipped.clone(),
        );

        let warmer_consecutive_failures = Gauge::default();
        registry.register(
            "tsdb_metadata_warmer_consecutive_failures",
            "Number of consecutive warmer cycle failures",
            warmer_consecutive_failures.clone(),
        );

        Self {
            registry,
            http: HttpMetrics {
                requests_total: http_requests_total,
                request_duration_seconds: http_request_duration_seconds,
                requests_in_flight: http_requests_in_flight,
            },
            scrape: ScrapeMetrics {
                samples_scraped: scrape_samples_scraped,
                samples_failed: scrape_samples_failed,
            },
            remote_write: RemoteWriteMetrics {
                samples_ingested: remote_write_samples_ingested,
                samples_failed: remote_write_samples_failed,
            },
            query: QueryMetrics {
                requests_total: query_requests_total,
                duration_seconds: query_duration_seconds,
                phase_wall_duration_seconds: query_phase_wall,
                phase_work_duration_seconds: query_phase_work,
                samples_loaded_total: query_samples_loaded,
                forward_index_series_loaded_total: query_fi_series_loaded,
            },
            warmer: MetadataWarmerMetrics {
                enabled: warmer_enabled,
                target_bytes: warmer_target_bytes,
                estimated_target_bytes: warmer_estimated_target_bytes,
                warmed_buckets: warmer_warmed_buckets,
                oldest_target_bucket_start_minutes: warmer_oldest_start,
                inflight_buckets: warmer_inflight,
                cycles_total: warmer_cycles_total,
                bucket_warms_total: warmer_bucket_warms,
                bytes_read_total: warmer_bytes_read,
                cycle_duration_seconds: warmer_cycle_duration,
                bucket_warm_duration_seconds: warmer_bucket_warm_duration,
                cycle_buckets_selected: warmer_cycle_buckets_selected,
                cycle_buckets_warmed: warmer_cycle_buckets_warmed,
                cycle_new_buckets: warmer_cycle_new,
                cycle_rewarmed_buckets: warmer_cycle_rewarmed,
                cycle_skipped_fresh_buckets: warmer_cycle_skipped,
                consecutive_failures: warmer_consecutive_failures,
            },
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
        assert!(encoded.contains("# HELP scrape_samples_scraped"));
        assert!(encoded.contains("# HELP http_requests_total"));
        assert!(encoded.contains("# HELP http_request_duration_seconds"));
        assert!(encoded.contains("# HELP http_requests_in_flight"));
        assert!(encoded.contains("# HELP remote_write_samples_ingested_total"));
        assert!(encoded.contains("# HELP remote_write_samples_failed_total"));
    }

    #[test]
    fn should_register_query_metrics() {
        // given/when
        let metrics = Metrics::new();

        // then
        let encoded = metrics.encode();
        assert!(encoded.contains("# HELP tsdb_query_requests_total"));
        assert!(encoded.contains("# HELP tsdb_query_duration_seconds"));
        assert!(encoded.contains("# HELP tsdb_query_phase_wall_duration_seconds"));
        assert!(encoded.contains("# HELP tsdb_query_phase_work_duration_seconds"));
        assert!(encoded.contains("# HELP tsdb_query_samples_loaded_total"));
        assert!(encoded.contains("# HELP tsdb_query_forward_index_series_loaded_total"));
    }

    #[test]
    fn should_register_metadata_warmer_metrics() {
        // given/when
        let metrics = Metrics::new();

        // then
        let encoded = metrics.encode();
        assert!(encoded.contains("# HELP tsdb_metadata_warmer_enabled"));
        assert!(encoded.contains("# HELP tsdb_metadata_warmer_target_bytes"));
        assert!(encoded.contains("# HELP tsdb_metadata_warmer_cycles_total"));
        assert!(encoded.contains("# HELP tsdb_metadata_warmer_bucket_warms_total"));
        assert!(encoded.contains("# HELP tsdb_metadata_warmer_bytes_read_total"));
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

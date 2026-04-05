//! Engine-level metrics for the timeseries database.
//!
//! These metrics are always available (the `metrics` crate is a required
//! dependency). Calls are no-ops when no global recorder is installed.

// ── Write path ──

pub(crate) const TSDB_SAMPLES_INGESTED: &str = "tsdb_samples_ingested_total";
pub(crate) const TSDB_SERIES_CREATED: &str = "tsdb_series_created_total";
pub(crate) const TSDB_FLUSH_DURATION_SECONDS: &str = "tsdb_flush_duration_seconds";
pub(crate) const TSDB_FLUSH_TOTAL: &str = "tsdb_flush_total";
pub(crate) const TSDB_BACKPRESSURE: &str = "tsdb_backpressure_total";

// ── Read path ──

pub(crate) const TSDB_QUERIES: &str = "tsdb_queries_total";
pub(crate) const TSDB_QUERY_DURATION_SECONDS: &str = "tsdb_query_duration_seconds";

// ── Ingest consumer ──

pub(crate) const TSDB_INGEST_ENTRIES_SKIPPED: &str = "tsdb_ingest_entries_skipped_total";

/// Describe all engine-level metrics on the global recorder.
///
/// Called by the HTTP server's metric initialization so that the
/// Prometheus `/metrics` endpoint includes help text for each metric.
pub(crate) fn describe_engine_metrics() {
    metrics::describe_counter!(
        TSDB_SAMPLES_INGESTED,
        "Total samples ingested into the TSDB"
    );
    metrics::describe_counter!(
        TSDB_SERIES_CREATED,
        "New series created per time bucket (same label set in different buckets counts separately)"
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_DURATION_SECONDS,
        "Duration of delta flush operations in seconds"
    );
    metrics::describe_counter!(TSDB_FLUSH_TOTAL, "Total delta flush operations");
    metrics::describe_counter!(
        TSDB_BACKPRESSURE,
        "Total writes rejected due to backpressure"
    );
    metrics::describe_counter!(TSDB_QUERIES, "Total PromQL queries evaluated");
    metrics::describe_histogram!(
        TSDB_QUERY_DURATION_SECONDS,
        "PromQL query evaluation duration in seconds"
    );
    metrics::describe_counter!(
        TSDB_INGEST_ENTRIES_SKIPPED,
        "Ingest consumer entries skipped due to errors"
    );
}

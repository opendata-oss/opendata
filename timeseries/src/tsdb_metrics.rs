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
pub(crate) const TSDB_ACTIVE_SERIES: &str = "tsdb_active_series";

// ── Flusher phases ──
//
// Existing `tsdb_flush_duration_seconds` covers the whole flush; the
// constants below split it into its phases so a slow flush can be attributed
// to a specific phase (op-building vs. SlateDB apply vs. snapshot refresh).

pub(crate) const TSDB_FLUSH_BUCKET_LIST_LOOKUP_DURATION_SECONDS: &str =
    "tsdb_flush_bucket_list_lookup_duration_seconds";
pub(crate) const TSDB_FLUSH_BUILD_OPS_DURATION_SECONDS: &str =
    "tsdb_flush_build_ops_duration_seconds";
pub(crate) const TSDB_FLUSH_STORAGE_APPLY_DURATION_SECONDS: &str =
    "tsdb_flush_storage_apply_duration_seconds";
pub(crate) const TSDB_FLUSH_STORAGE_SNAPSHOT_DURATION_SECONDS: &str =
    "tsdb_flush_storage_snapshot_duration_seconds";

pub(crate) const TSDB_FLUSH_OPS: &str = "tsdb_flush_ops";
pub(crate) const TSDB_FLUSH_ESTIMATED_BYTES: &str = "tsdb_flush_estimated_bytes";
pub(crate) const TSDB_FLUSH_NEW_SERIES: &str = "tsdb_flush_new_series";
pub(crate) const TSDB_FLUSH_SAMPLES: &str = "tsdb_flush_samples";

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
    metrics::describe_gauge!(
        TSDB_ACTIVE_SERIES,
        "Estimated number of unique series that have received data in the last ~15 minutes \
         (HyperLogLog-based, ~1.6% standard error)"
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_BUCKET_LIST_LOOKUP_DURATION_SECONDS,
        "Time spent looking up the BucketList during a delta flush (seconds)"
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_BUILD_OPS_DURATION_SECONDS,
        "Time spent building storage ops for a delta flush (seconds)"
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_STORAGE_APPLY_DURATION_SECONDS,
        "Time spent in Storage::apply during a delta flush (seconds). Indicates SlateDB write backpressure."
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_STORAGE_SNAPSHOT_DURATION_SECONDS,
        "Time spent refreshing the post-apply storage snapshot (seconds)"
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_OPS,
        "Number of storage ops produced by a single delta flush"
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_ESTIMATED_BYTES,
        "Estimated key+value bytes produced by a single delta flush"
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_NEW_SERIES,
        "Number of new series registered by a single delta flush"
    );
    metrics::describe_histogram!(
        TSDB_FLUSH_SAMPLES,
        "Number of samples written by a single delta flush"
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

    common::coordinator::describe_coordinator_metrics();
}

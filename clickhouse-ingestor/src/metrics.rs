//! Prometheus metric names + describe/record helpers.
//!
//! Mirrors the metric set documented in RFC 0003. Names are chosen to
//! match the existing OpenData observability conventions (snake_case,
//! `_seconds` for histograms, `_total` for counters).
//!
//! `last_decoded_sequence` and `last_acked_sequence` are tracked
//! separately so dry-run is observable: in dry-run the decoded gauge
//! advances and the acked gauge stays still by design.

use std::time::Instant;

pub const BUFFER_BATCHES_READ_TOTAL: &str = "ingestor_buffer_batches_read_total";
pub const COMMIT_GROUPS_TOTAL: &str = "ingestor_commit_groups_total";
pub const ROWS_PLANNED_TOTAL: &str = "ingestor_rows_planned_total";
pub const ROWS_INSERTED_TOTAL: &str = "ingestor_rows_inserted_total";
pub const INSERT_CHUNKS_TOTAL: &str = "ingestor_insert_chunks_total";
pub const INSERT_LATENCY_SECONDS: &str = "ingestor_insert_latency_seconds";
pub const COMMIT_GROUP_SIZE_ROWS: &str = "ingestor_commit_group_size_rows";
pub const COMMIT_GROUP_SIZE_BYTES: &str = "ingestor_commit_group_size_bytes";
pub const ACK_FLUSH_LATENCY_SECONDS: &str = "ingestor_ack_flush_latency_seconds";
pub const RETRY_COUNT_TOTAL: &str = "ingestor_retry_count_total";
pub const LAST_DECODED_SEQUENCE: &str = "ingestor_last_decoded_sequence";
pub const LAST_ACKED_SEQUENCE: &str = "ingestor_last_acked_sequence";
pub const TIME_SINCE_LAST_SUCCESSFUL_ACK_SECONDS: &str =
    "ingestor_time_since_last_successful_ack_seconds";
// `current_lag_sequences` (head_sequence - last_decoded_sequence) is
// deferred to a follow-up: the runtime does not currently observe the
// manifest's head sequence, so the gauge would always read zero. We
// will wire it up once the buffer crate exposes a peek API; for now
// leave the metric off the registry rather than emit a misleading
// value.
pub const DECODE_FAILURES_TOTAL: &str = "ingestor_decode_failures_total";
pub const CLICKHOUSE_FAILURES_TOTAL: &str = "ingestor_clickhouse_failures_total";

/// Register descriptions with the global metrics recorder. Safe to call
/// multiple times; the macros are idempotent for known names.
pub fn describe() {
    use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

    describe_counter!(
        BUFFER_BATCHES_READ_TOTAL,
        "Number of Buffer batches pulled by the ingestor."
    );
    describe_counter!(
        COMMIT_GROUPS_TOTAL,
        "Number of commit groups flushed downstream."
    );
    describe_counter!(
        ROWS_PLANNED_TOTAL,
        "Number of rows produced by the adapter."
    );
    describe_counter!(
        ROWS_INSERTED_TOTAL,
        "Number of rows acknowledged by ClickHouse."
    );
    describe_counter!(
        INSERT_CHUNKS_TOTAL,
        "ClickHouse insert chunks attempted, labeled by result."
    );
    describe_histogram!(
        INSERT_LATENCY_SECONDS,
        Unit::Seconds,
        "Per-chunk ClickHouse insert latency."
    );
    describe_histogram!(COMMIT_GROUP_SIZE_ROWS, "Rows in a flushed commit group.");
    describe_histogram!(
        COMMIT_GROUP_SIZE_BYTES,
        Unit::Bytes,
        "Approximate bytes in a flushed commit group."
    );
    describe_histogram!(
        ACK_FLUSH_LATENCY_SECONDS,
        Unit::Seconds,
        "Time from a commit group's first ack to flush completion."
    );
    describe_counter!(
        RETRY_COUNT_TOTAL,
        "Per-chunk retry attempts, labeled by reason."
    );
    describe_gauge!(
        LAST_DECODED_SEQUENCE,
        "Highest Buffer sequence the runtime has decoded; advances even in dry-run."
    );
    describe_gauge!(
        LAST_ACKED_SEQUENCE,
        "Highest Buffer sequence the runtime has acked; advances only when not in dry-run."
    );
    describe_gauge!(
        TIME_SINCE_LAST_SUCCESSFUL_ACK_SECONDS,
        Unit::Seconds,
        "Wall-clock seconds since the most recent successful ack+flush. Primary alerting signal in non-dry-run mode."
    );
    describe_counter!(
        DECODE_FAILURES_TOTAL,
        "Decode failures, labeled by stage (envelope/signal/adapter)."
    );
    describe_counter!(
        CLICKHOUSE_FAILURES_TOTAL,
        "ClickHouse insert failures, labeled by classification."
    );
}

/// Snapshot the recorder-side counters that the runtime updates as it
/// runs. Tests use this via the `metrics-exporter-prometheus` recorder
/// to assert metric emission.
#[derive(Debug, Default, Clone)]
pub struct RuntimeMetrics {
    pub last_successful_ack_at: Option<Instant>,
}

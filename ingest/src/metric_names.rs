// Metric names for the ingest crate.
//
// ## Ingestor (write path)
//
// | Name                          | Type      | Labels | Description                              |
// |-------------------------------|-----------|--------|------------------------------------------|
// | ingest.batches_flushed        | counter   |        | Batches written to object store          |
// | ingest.entries_flushed        | counter   |        | Entries across all flushed batches        |
// | ingest.bytes_flushed          | counter   |        | Raw bytes flushed (pre-compression)      |
// | ingest.bytes_written          | counter   |        | Bytes written to object store (post-compression) |
// | ingest.flush_duration_seconds | histogram |        | Flush latency in seconds (write+enqueue) |
// | ingest.manifest_writes        | counter   | role   | Manifest write attempts                  |
// | ingest.manifest_conflicts     | counter   | role   | Manifest CAS conflicts                   |
//
// ## Collector (read path)
//
// | Name                            | Type      | Labels | Description                              |
// |---------------------------------|-----------|--------|------------------------------------------|
// | ingest.batches_collected        | counter   |        | Batches fetched from object store        |
// | ingest.entries_collected         | counter   |        | Entries across collected batches          |
// | ingest.bytes_collected           | counter   |        | Bytes read from object store             |
// | ingest.collector_lag_seconds     | gauge     |        | Wall clock - last batch ingestion time   |
// | ingest.queue_length              | gauge     |        | Entries in the manifest queue             |
// | ingest.acks                      | counter   |        | Batch acks processed                     |
// | ingest.gc_files_deleted          | counter   |        | Batch files cleaned by GC                |
// | ingest.gc_files_failed           | counter   |        | Failed GC file deletions                 |
// | ingest.gc_duration_seconds       | histogram |        | GC cycle wall time in seconds            |
// | ingest.fetch_duration_seconds    | histogram |        | Batch fetch latency from object store    |
// | ingest.manifest_writes           | counter   | role   | Manifest write attempts                  |
// | ingest.manifest_conflicts        | counter   | role   | Manifest CAS conflicts                   |

pub(crate) const BATCHES_FLUSHED: &str = "ingest.batches_flushed";
pub(crate) const ENTRIES_FLUSHED: &str = "ingest.entries_flushed";
pub(crate) const BYTES_FLUSHED: &str = "ingest.bytes_flushed";
pub(crate) const BYTES_WRITTEN: &str = "ingest.bytes_written";
pub(crate) const FLUSH_DURATION_SECONDS: &str = "ingest.flush_duration_seconds";

pub(crate) const BATCHES_COLLECTED: &str = "ingest.batches_collected";
pub(crate) const ENTRIES_COLLECTED: &str = "ingest.entries_collected";
pub(crate) const BYTES_COLLECTED: &str = "ingest.bytes_collected";
pub(crate) const COLLECTOR_LAG_SECONDS: &str = "ingest.collector_lag_seconds";
pub(crate) const QUEUE_LENGTH: &str = "ingest.queue_length";
pub(crate) const ACKS: &str = "ingest.acks";
pub(crate) const GC_FILES_DELETED: &str = "ingest.gc_files_deleted";
pub(crate) const GC_FILES_FAILED: &str = "ingest.gc_files_failed";
pub(crate) const GC_DURATION_SECONDS: &str = "ingest.gc_duration_seconds";
pub(crate) const FETCH_DURATION_SECONDS: &str = "ingest.fetch_duration_seconds";

pub(crate) const MANIFEST_WRITES: &str = "ingest.manifest_writes";
pub(crate) const MANIFEST_CONFLICTS: &str = "ingest.manifest_conflicts";

pub(crate) fn describe_ingestor_metrics() {
    metrics::describe_counter!(BATCHES_FLUSHED, "Batches written to object store");
    metrics::describe_counter!(ENTRIES_FLUSHED, "Entries across all flushed batches");
    metrics::describe_counter!(BYTES_FLUSHED, "Raw bytes flushed (pre-compression)");
    metrics::describe_counter!(
        BYTES_WRITTEN,
        "Bytes written to object store (post-compression)"
    );
    metrics::describe_histogram!(
        FLUSH_DURATION_SECONDS,
        "Flush latency in seconds (write + enqueue)"
    );
    metrics::describe_counter!(MANIFEST_WRITES, "Producer manifest write attempts");
    metrics::describe_counter!(MANIFEST_CONFLICTS, "Producer manifest CAS conflicts");
}

pub(crate) fn describe_collector_metrics() {
    metrics::describe_counter!(BATCHES_COLLECTED, "Batches fetched from object store");
    metrics::describe_counter!(ENTRIES_COLLECTED, "Entries across collected batches");
    metrics::describe_counter!(BYTES_COLLECTED, "Bytes read from object store");
    metrics::describe_gauge!(
        COLLECTOR_LAG_SECONDS,
        "Wall clock minus last batch ingestion time in seconds"
    );
    metrics::describe_gauge!(QUEUE_LENGTH, "Entries in the manifest queue");
    metrics::describe_counter!(ACKS, "Batch acks processed");
    metrics::describe_counter!(GC_FILES_DELETED, "Batch files cleaned by GC");
    metrics::describe_counter!(GC_FILES_FAILED, "Failed GC file deletions");
    metrics::describe_histogram!(GC_DURATION_SECONDS, "GC cycle wall time in seconds");
    metrics::describe_histogram!(
        FETCH_DURATION_SECONDS,
        "Batch fetch latency from object store in seconds"
    );
    metrics::describe_counter!(MANIFEST_WRITES, "Consumer manifest write attempts");
    metrics::describe_counter!(MANIFEST_CONFLICTS, "Consumer manifest CAS conflicts");
}

// Metric names for the buffer crate.
//
// ## Buffer (write path)
//
// | Name                          | Type      | Labels | Description                              |
// |-------------------------------|-----------|--------|------------------------------------------|
// | buffer.batches_flushed        | counter   |        | Batches written to object store          |
// | buffer.entries_flushed        | counter   |        | Entries across all flushed batches        |
// | buffer.bytes_flushed          | counter   |        | Raw bytes flushed (pre-compression)      |
// | buffer.bytes_written          | counter   |        | Bytes written to object store (post-compression) |
// | buffer.flush_duration_seconds | histogram |        | Flush latency in seconds (write+enqueue) |
// | buffer.manifest_writes        | counter   | role   | Manifest write attempts                  |
// | buffer.manifest_conflicts     | counter   | role   | Manifest CAS conflicts                   |
//
// ## Consumer (read path)
//
// | Name                            | Type      | Labels | Description                              |
// |---------------------------------|-----------|--------|------------------------------------------|
// | buffer.batches_collected        | counter   |        | Batches fetched from object store        |
// | buffer.entries_collected         | counter   |        | Entries across collected batches          |
// | buffer.bytes_collected           | counter   |        | Bytes read from object store             |
// | buffer.consumer_lag_seconds      | gauge     |        | Wall clock - last batch ingestion time   |
// | buffer.queue_length              | gauge     |        | Entries in the manifest queue             |
// | buffer.acks                      | counter   |        | Batch acks processed                     |
// | buffer.gc_files_deleted          | counter   |        | Batch files cleaned by GC                |
// | buffer.gc_files_failed           | counter   |        | Failed GC file deletions                 |
// | buffer.gc_duration_seconds       | histogram |        | GC cycle wall time in seconds            |
// | buffer.fetch_duration_seconds    | histogram |        | Batch fetch latency from object store    |
// | buffer.manifest_writes           | counter   | role   | Manifest write attempts                  |
// | buffer.manifest_conflicts        | counter   | role   | Manifest CAS conflicts                   |

pub(crate) const BATCHES_FLUSHED: &str = "buffer.batches_flushed";
pub(crate) const ENTRIES_FLUSHED: &str = "buffer.entries_flushed";
pub(crate) const BYTES_FLUSHED: &str = "buffer.bytes_flushed";
pub(crate) const BYTES_WRITTEN: &str = "buffer.bytes_written";
pub(crate) const FLUSH_DURATION_SECONDS: &str = "buffer.flush_duration_seconds";

pub(crate) const BATCHES_COLLECTED: &str = "buffer.batches_collected";
pub(crate) const ENTRIES_COLLECTED: &str = "buffer.entries_collected";
pub(crate) const BYTES_COLLECTED: &str = "buffer.bytes_collected";
pub(crate) const CONSUMER_LAG_SECONDS: &str = "buffer.consumer_lag_seconds";
pub(crate) const QUEUE_LENGTH: &str = "buffer.queue_length";
pub(crate) const ACKS: &str = "buffer.acks";
pub(crate) const GC_FILES_DELETED: &str = "buffer.gc_files_deleted";
pub(crate) const GC_FILES_FAILED: &str = "buffer.gc_files_failed";
pub(crate) const GC_DURATION_SECONDS: &str = "buffer.gc_duration_seconds";
pub(crate) const FETCH_DURATION_SECONDS: &str = "buffer.fetch_duration_seconds";

pub(crate) const MANIFEST_WRITES: &str = "buffer.manifest_writes";
pub(crate) const MANIFEST_CONFLICTS: &str = "buffer.manifest_conflicts";

pub(crate) fn describe_buffer_metrics() {
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

pub(crate) fn describe_consumer_metrics() {
    metrics::describe_counter!(BATCHES_COLLECTED, "Batches fetched from object store");
    metrics::describe_counter!(ENTRIES_COLLECTED, "Entries across collected batches");
    metrics::describe_counter!(BYTES_COLLECTED, "Bytes read from object store");
    metrics::describe_gauge!(
        CONSUMER_LAG_SECONDS,
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

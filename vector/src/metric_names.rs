//! Metric name constants for the vector crate.
//!
//! All instrumentation flows through the `metrics-rs` facade. Emitted counters
//! are no-ops when no global recorder is installed, so embedded users opt in by
//! installing their own recorder. The vector server binary installs a
//! `PrometheusBuilder` recorder; see [`crate::server`] for the scrape endpoint.

/// Counter for upserted vectors observed by `VectorDb::write`.
pub const VECTOR_UPSERTS_TOTAL: &str = "vector_upserts_total";

/// Counter for deleted vector ids observed by `VectorDb::delete`.
pub const VECTOR_DELETES_TOTAL: &str = "vector_deletes_total";

/// Counter for writes (upserts and deletes) seen by the indexer at the start of
/// each `WriteVectors` op.
pub const INDEXER_WRITES_TOTAL: &str = "vector_indexer_writes_total";

/// Counter for centroids split by the indexer. Labeled by tree `level`
/// (255 for the root level).
pub const INDEXER_ANN_SPLITS_TOTAL: &str = "vector_indexer_ann_splits_total";

/// Counter for centroids merged by the indexer. Labeled by tree `level`.
pub const INDEXER_ANN_MERGES_TOTAL: &str = "vector_indexer_ann_merges_total";

/// Counter for vectors reassigned by the indexer. Labeled by tree `level`.
pub const INDEXER_ANN_REASSIGNS_TOTAL: &str = "vector_indexer_ann_reassigns_total";

/// Counter for the total number of candidate vectors scored during query
/// execution (sum of posting-list lengths over loaded centroids).
pub const QUERY_VECTORS_SCORED_TOTAL: &str = "vector_query_vectors_scored_total";

/// Histogram of end-to-end `search_with_options` latency, in seconds.
pub const QUERY_SEARCH_DURATION_SECONDS: &str = "vector_query_search_duration_seconds";

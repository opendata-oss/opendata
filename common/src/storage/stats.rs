/// Backend-agnostic interface for exposing storage engine statistics.
///
/// Implementations translate engine-specific stat registries into a flat
/// list of `(name, value)` pairs suitable for Prometheus gauge registration.
///
/// Names returned by [`snapshot()`](StorageStats::snapshot) are registered
/// directly into the service's Prometheus registry. To avoid collisions with
/// service-level metrics, implementations **must** prefix every name with a
/// backend-specific namespace (e.g. `slatedb_`).
pub trait StorageStats: Send + Sync {
    /// Returns a snapshot of all current stat values.
    ///
    /// Names must be fully-qualified Prometheus metric names with a
    /// backend-specific prefix (e.g. `slatedb_db_write_ops`). Use
    /// underscores, not slashes.
    fn snapshot(&self) -> Vec<(String, i64)>;
}

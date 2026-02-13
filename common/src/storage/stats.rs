/// Backend-agnostic interface for exposing storage engine statistics.
///
/// Implementations translate engine-specific stat registries into a flat
/// list of `(name, value)` pairs suitable for Prometheus gauge registration.
pub trait StorageStats: Send + Sync {
    /// Returns a snapshot of all current stat values.
    ///
    /// Names should use underscores (no slashes) and be suitable for use
    /// as Prometheus metric name suffixes.
    fn snapshot(&self) -> Vec<(String, i64)>;
}

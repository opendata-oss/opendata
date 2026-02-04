use crate::StorageRead;
use async_trait::async_trait;
use std::ops::Range;
use std::sync::Arc;

/// The level of durability for a write.
///
/// Durability levels form an ordered progression: `Applied < Flushed < Durable`.
/// Each level provides stronger guarantees about write persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Durability {
    Applied,
    Flushed,
    Durable,
}

/// Event emitted when a flush occurs.
pub struct FlushEvent<D: Delta> {
    pub delta: D::Frozen,
    /// The range of epochs contained in this flush (exclusive end).
    /// Start is the first epoch in the flush, end is one past the last epoch.
    pub epoch_range: Range<u64>,
}

/// A delta accumulates writes and can produce a snapshot image.
pub trait Delta: Default + Sized + Send + Sync + 'static {
    type Image: Send + Sync + 'static;
    type Write: Send + 'static;
    type Frozen: Clone + Send + Sync + 'static;

    /// Create a new delta initialized from a snapshot image.
    fn init(&mut self, image: &Self::Image);

    /// Apply a write to the delta.
    fn apply(&mut self, write: Self::Write) -> Result<(), String>;

    /// Estimate the size of the delta in bytes.
    fn estimate_size(&self) -> usize;

    /// Freezes the current delta, creating an image with the delta
    /// applied.
    ///
    /// Implementations should ensure this operation is efficient (e.g., via
    /// copy-on-write or reference counting) since it blocks writes. After this
    /// is complete, the [`Flusher::flush`] happens on a background thread.
    fn freeze(self, image: &Self::Image) -> (Self::Frozen, Self::Image);
}

/// A flusher persists flush events to durable storage.
#[async_trait]
pub trait Flusher<D: Delta>: Send + Sync + 'static {
    /// Flush the given event to durable storage and returns a storage
    /// snapshot for readers to use
    async fn flush(&self, event: &FlushEvent<D>) -> Result<Arc<dyn StorageRead>, String>;
}

use crate::StorageRead;
use async_trait::async_trait;
use std::ops::Range;
use std::sync::Arc;

/// Result of a flush operation, broadcast to subscribers.
pub struct FlushResult<D: Delta> {
    /// The new snapshot reflecting the flushed state
    pub snapshot: Arc<dyn StorageRead>,
    /// The delta that was flushed (wrapped in Arc for cheap cloning)
    pub delta: Arc<D::Frozen>,
    /// Epoch range covered by this flush (exclusive end)
    pub epoch_range: Range<u64>,
}

impl<D: Delta> Clone for FlushResult<D> {
    fn clone(&self) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            delta: self.delta.clone(),
            epoch_range: self.epoch_range.clone(),
        }
    }
}

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
pub trait Delta: Sized + Send + Sync + 'static {
    /// The Context is data owned only while the Delta is mutable. After
    /// freezing the delta the context is returned to the write coordinator
    type Context: Send + Sync + 'static;
    type Write: Send + 'static;
    type Frozen: Clone + Send + Sync + 'static;

    /// Create a new delta initialized from a snapshot context.
    /// The delta takes ownership of the context while it is mutable.
    fn init(context: Self::Context) -> Self;

    /// Apply a write to the delta.
    fn apply(&mut self, write: Self::Write) -> Result<(), String>;

    /// Estimate the size of the delta in bytes.
    fn estimate_size(&self) -> usize;

    /// Freezes the current delta, creating an image with the delta
    /// applied.
    ///
    /// Returns the frozen delta and the context (which was owned by the delta).
    /// Implementations should ensure this operation is efficient (e.g., via
    /// copy-on-write or reference counting) since it blocks writes. After this
    /// is complete, the [`Flusher::flush`] happens on a background thread.
    fn freeze(self) -> (Self::Frozen, Self::Context);
}

/// A flusher persists flush events to durable storage.
#[async_trait]
pub trait Flusher<D: Delta>: Send + Sync + 'static {
    /// Flush the given event to durable storage and returns a storage
    /// snapshot for readers to use
    async fn flush(&self, event: &FlushEvent<D>) -> Result<Arc<dyn StorageRead>, String>;
}

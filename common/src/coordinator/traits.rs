use crate::StorageRead;
use async_trait::async_trait;
use std::ops::Range;
use std::sync::Arc;

/// Result of a flush operation, broadcast to subscribers.
pub struct FlushResult<D: Delta> {
    /// The flushed delta with snapshot and broadcast payload.
    pub delta: BroadcastDelta<D>,
    /// Epoch range covered by this flush (exclusive end)
    pub epoch_range: Range<u64>,
}

impl<D: Delta> Clone for FlushResult<D> {
    fn clone(&self) -> Self {
        Self {
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

/// A delta accumulates writes and can produce a snapshot image.
pub trait Delta: Sized + Send + Sync + 'static {
    /// The Context is data owned only while the Delta is mutable. After
    /// freezing the delta the context is returned to the write coordinator
    type Context: Send + Sync + 'static;
    type Write: Send + 'static;
    type Frozen: Clone + Send + Sync + 'static;
    /// The payload broadcast to subscribers after a flush.
    type Broadcast: Clone + Send + Sync + 'static;
    /// Metadata returned from [`Delta::apply`], delivered to the caller
    /// through [`WriteHandle::result`](super::WriteHandle::result).
    type ApplyResult: Clone + Send + 'static;

    /// Create a new delta initialized from a snapshot context.
    /// The delta takes ownership of the context while it is mutable.
    fn init(context: Self::Context) -> Self;

    /// Apply a write to the delta and return a result for the caller.
    fn apply(&mut self, write: Self::Write) -> Result<Self::ApplyResult, String>;

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

/// The result of flushing a frozen delta, broadcast to subscribers.
pub struct BroadcastDelta<D: Delta> {
    /// The new snapshot reflecting the flushed state.
    pub snapshot: Arc<dyn StorageRead>,
    /// The broadcast payload for subscribers.
    pub broadcast: D::Broadcast,
}

impl<D: Delta> Clone for BroadcastDelta<D> {
    fn clone(&self) -> Self {
        Self {
            snapshot: self.snapshot.clone(),
            broadcast: self.broadcast.clone(),
        }
    }
}

/// A flusher persists frozen deltas and ensures storage durability.
#[async_trait]
pub trait Flusher<D: Delta>: Send + Sync + 'static {
    /// Flush a frozen delta to storage.
    ///
    /// Consumes the frozen delta by value and returns a snapshot for readers
    /// along with a broadcast payload for subscribers.
    async fn flush_delta(
        &self,
        frozen: D::Frozen,
        epoch_range: &Range<u64>,
    ) -> Result<BroadcastDelta<D>, String>;

    /// Ensure storage durability (e.g. call storage.flush()).
    async fn flush_storage(&self) -> Result<(), String>;
}

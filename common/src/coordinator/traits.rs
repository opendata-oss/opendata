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

/// A delta accumulates writes and can produce a frozen snapshot for flushing.
///
/// The write coordinator manages a pipeline of three stages, each represented
/// by a different type:
///
/// - **`Delta`** (this trait) — the mutable, in-progress batch. Writes are
///   applied here until the delta is frozen.
/// - **`Frozen`** — an immutable snapshot of the delta, sent to the
///   [`Flusher`] to be persisted to storage.
/// - **`Broadcast`** — a minimal representation of the flushed state
///   that readers need to update their read image.
pub trait Delta: Sized + Send + Sync + 'static {
    /// Mutable state owned by the delta while it accumulates writes.
    /// Returned to the write coordinator on [`freeze`](Delta::freeze) so the
    /// next delta can continue where this one left off.
    type Context: Send + Sync + 'static;
    /// A single write operation applied via [`apply`](Delta::apply).
    type Write: Send + 'static;
    /// Immutable snapshot produced by [`freeze`](Delta::freeze), consumed by
    /// the [`Flusher`] to persist the batch to storage.
    type Frozen: Send + Sync + 'static;
    /// Minimal representation of flushed state, broadcast to subscribers
    /// so they can update their read image.
    type Broadcast: Clone + Send + Sync + 'static;
    /// Metadata returned from [`apply`](Delta::apply), delivered to the caller
    /// through [`WriteHandle::wait`](super::WriteHandle::wait).
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

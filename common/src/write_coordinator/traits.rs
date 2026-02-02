//! Traits for implementing write coordination.

use std::sync::Arc;

use async_trait::async_trait;

use super::error::Result;
use crate::StorageSnapshot;

/// A delta represents the in-memory state accumulated from writes.
///
/// Deltas are initialized from an image (a point-in-time snapshot of state),
/// accumulate writes, and can be forked back into images for the next delta.
pub trait Delta: Send + Clone + 'static {
    /// The image type that deltas are initialized from and forked into.
    type Image: Send + Sync + 'static;

    /// The write type that can be applied to this delta.
    type Write: Send + 'static;

    /// Initialize this delta from an image.
    ///
    /// This is called when a new delta is created after a flush.
    fn init(&mut self, image: &Self::Image);

    /// Apply a batch of writes to this delta.
    ///
    /// Returns an error if any write cannot be applied.
    fn apply(&mut self, writes: Vec<Self::Write>) -> Result<()>;

    /// Estimate the memory size of this delta in bytes.
    ///
    /// Used to determine when to trigger a flush based on memory pressure.
    fn estimate_size(&self) -> usize;

    /// Fork this delta into a new image.
    ///
    /// The returned image captures the current state and can be used
    /// to initialize the next delta after a flush.
    fn fork_image(&self) -> Self::Image;
}

/// A flusher is responsible for persisting deltas to storage.
#[async_trait]
pub trait Flusher: Send + Sync + 'static {
    /// The delta type this flusher can flush.
    type Delta: Delta;

    /// Flush a delta to storage and return a snapshot.
    ///
    /// The returned snapshot reflects the state after the delta has been applied.
    async fn flush(&self, delta: Self::Delta) -> Result<Arc<dyn StorageSnapshot>>;
}

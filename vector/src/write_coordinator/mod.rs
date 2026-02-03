use anyhow::{Error, Result};
use std::sync::Arc;
use async_trait::async_trait;
use futures::future::Shared;
use tokio::sync::{oneshot, watch};
use common::StorageRead;

/// Event broadcast to subscribers after each flush.
pub struct FlushEvent<D: Delta> {
    /// The new snapshot reflecting the flushed state.
    pub snapshot: Arc<dyn StorageRead>,
    /// Clone of the delta that was flushed (pre-flush state).
    pub delta: D,
    /// Epoch range covered by this flush: (previous_max, new_max].
    /// A subscriber whose cache is at `epoch_range.0` can apply `delta`
    /// to update their state to `epoch_range.1`.
    pub epoch_range: (u64, u64),
}

pub trait Delta: Send + Clone + 'static {
    type Image: Send + Sync + 'static;
    type Write: Send + 'static;

    /// Initialize the delta with state from the image.
    fn init(&mut self, image: &Self::Image);
    /// Apply writes to this delta.
    fn apply(&mut self, writes: Vec<Self::Write>) -> Result<()>;
    /// Estimate the memory size of this delta for backpressure.
    fn estimate_size(&self) -> usize;
    /// Extract state needed for the next delta's initialization.
    /// Called before flush to enable non-blocking writes.
    fn fork_image(&self) -> Self::Image;
}

#[async_trait]
pub trait Flusher: Send + Sync + 'static {
    type Delta: Delta;

    /// Flush a delta to storage and return the new snapshot.
    async fn flush(&self, delta: Self::Delta) -> Result<Arc<dyn StorageRead>>;
}

/// Durability levels for write acknowledgment.
pub enum Durability {
    /// Write has been applied to an in-memory delta.
    Applied,
    /// Write has been flushed to SlateDB memtable.
    Flushed,
    /// Write has been persisted to object storage.
    Durable,
}

/// Watchers for durability watermarks. Created per-handle via `sender.subscribe()` to ensure
/// each handle has independent cursor state.
struct FlushWatchers {
    applied: watch::Receiver<u64>,
    flushed: watch::Receiver<u64>,
    durable: watch::Receiver<u64>,
}

impl FlushWatchers {
    async fn wait(&self, epoch: u64, durability: Durability) -> Result<()> {
        let mut rx = match durability {
            Durability::Applied => self.applied.clone(),
            Durability::Flushed => self.flushed.clone(),
            Durability::Durable => self.durable.clone(),
        };
        rx.wait_for(|v| *v <= epoch).await.map(|_| ()).map_err(Error::from)
    }
}

/// Handle returned from a write operation for tracking durability.
pub struct WriteHandle {
    epoch: Shared<oneshot::Receiver<u64>>,
    watchers: FlushWatchers,
}

impl WriteHandle {
    /// Returns the epoch assigned to this write. Epochs are assigned when the
    /// coordinator dequeues the write, so this method blocks until sequencing.
    /// Epochs are monotonically increasing and reflect the actual write order.
    pub async fn epoch(&self) -> Result<u64> {
        Ok(self.epoch.clone().await.map_err(|_| Error::msg("coordinator dropped"))?)
    }
    /// Wait until the write reaches the specified durability level.
    pub async fn wait(&self, durability: Durability) -> Result<()> {
        let epoch = self.epoch().await?;
        self.watchers.wait(epoch, durability).await
    }
}

/// Handle for interacting with the write coordinator.
pub struct WriteCoordinatorHandle<D: Delta> {
    current_delta: D,
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    /// Submit a write and receive a handle to track its durability.
    pub async fn write(&self, event: D::Write) -> Result<WriteHandle> {
        todo!();
    }

    /// Request a flush up to the specified epoch (or all pending if None).
    pub async fn flush(&self, epoch: Option<u64>) -> Result<()> {
        todo!()
    }

    /// Subscribe to flush events.
    ///
    /// Each event contains the new snapshot, a clone of the flushed delta, and the
    /// epoch range covered. Subscribers can use this for incremental cache updates
    /// if their local state is at `epoch_range.0`.
    ///
    /// **Note**: Uses a `watch` channel — if flushes occur faster than the subscriber
    /// processes them, intermediate events are dropped. Subscribers that miss events
    /// must rebuild their cache from the snapshot.
    pub fn subscribe(&self) -> watch::Receiver<FlushEvent<D>> {
        todo!()
    }
}


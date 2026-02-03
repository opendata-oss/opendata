mod coordinator;

use anyhow::{Error, Result};
use async_trait::async_trait;
use futures::FutureExt;
use futures::future::Shared;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};

use common::StorageRead;

use crate::write_coordinator::coordinator::{WriteCoordinatorCtlMsg, WriteCoordinatorWriteMsg};
pub use coordinator::WriteCoordinator;

pub trait Delta: Send + Clone + 'static {
    type Image: Send + Sync + 'static;
    type Write: Send + 'static;
    type ImmutableDelta: Send + Sync + 'static;

    /// Initialize the delta with state from the image.
    fn init(image: Self::Image) -> Self;
    /// Apply writes to this delta.
    fn apply(&mut self, writes: Vec<Self::Write>) -> Result<(), Arc<Error>>;
    /// Estimate the memory size of this delta for backpressure.
    #[allow(dead_code)]
    fn estimate_size(&self) -> usize;
    /// Extract state needed for the next delta's initialization.
    /// Called before flush to enable non-blocking writes.
    fn freeze(&self) -> (Self::ImmutableDelta, Self::Image);
}

#[async_trait]
pub trait Flusher<D: Delta>: Send + Sync + 'static {
    /// Flush a delta to storage and return the new snapshot.
    async fn flush(&self, delta: &D::ImmutableDelta) -> Result<Arc<dyn StorageRead>>;
}

/// Durability levels for write acknowledgment.
#[allow(dead_code)]
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
#[derive(Clone)]
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
        rx.wait_for(|v| *v <= epoch)
            .await
            .map(|_| ())
            .map_err(Error::from)
    }
}

/// Handle returned from a write operation for tracking durability.
pub struct WriteHandle {
    epoch: Shared<oneshot::Receiver<Result<u64, Arc<Error>>>>,
    #[allow(dead_code)]
    watchers: FlushWatchers,
}

impl WriteHandle {
    /// Returns the epoch assigned to this write. Epochs are assigned when the
    /// coordinator dequeues the write, so this method blocks until sequencing.
    /// Epochs are monotonically increasing and reflect the actual write order.
    pub async fn epoch(&self) -> Result<u64, Arc<Error>> {
        self.epoch
            .clone()
            .await
            .map_err(|_| Error::msg("coordinator dropped"))?
    }

    /// Wait until the write reaches the specified durability level.
    #[allow(dead_code)]
    pub async fn wait(&self, durability: Durability) -> Result<(), Arc<Error>> {
        let epoch = self.epoch().await?;
        self.watchers
            .wait(epoch, durability)
            .await
            .map_err(Arc::new)
    }
}

/// Handle for interacting with the write coordinator.
#[derive(Clone)]
pub struct WriteCoordinatorHandle<D: Delta> {
    write_tx: tokio::sync::mpsc::Sender<WriteCoordinatorWriteMsg<D>>,
    ctl_tx: tokio::sync::mpsc::UnboundedSender<WriteCoordinatorCtlMsg>,
    flush_watchers: FlushWatchers,
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    /// Submit a write and receive a handle to track its durability.
    pub async fn write(&self, write: D::Write) -> Result<WriteHandle> {
        let (msg, rx) = WriteCoordinatorWriteMsg::new(write);
        self.write_tx
            .send(msg)
            .await
            .map_err(|_| Error::msg("coordinator dropped"))?;
        Ok(WriteHandle {
            epoch: rx.shared(),
            watchers: self.flush_watchers.clone(),
        })
    }

    /// Request a flush of all pending writes.
    ///
    /// Blocks until the flush is complete.
    pub async fn flush(&self, epoch: Option<u64>) -> Result<()> {
        let (result_tx, result_rx) = oneshot::channel();
        self.ctl_tx
            .send(WriteCoordinatorCtlMsg::Flush { epoch, result_tx })
            .map_err(|_| Error::msg("coordinator dropped"))?;
        let Some(epoch) = result_rx
            .await
            .map_err(|_| Error::msg("coordinator dropped"))?
        else {
            return Err(Error::msg("flush already in progress"));
        };
        self.flush_watchers.wait(epoch, Durability::Durable).await?;
        Ok(())
    }
}

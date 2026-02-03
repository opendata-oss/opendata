use super::{Delta, Durability, FlushEvent, WriteError, WriteResult};
use crate::coordinator::coordinator::WriteCommand;
use futures::FutureExt;
use futures::future::Shared;
use tokio::sync::{mpsc, oneshot, watch};

/// Receivers for durability watermark updates.
///
/// Each receiver tracks the highest epoch that has reached the corresponding
/// [`Durability`] level. See [`Durability`] for details on each level.
#[derive(Clone)]
pub(crate) struct EpochWatcher {
    pub applied_rx: watch::Receiver<u64>,
    pub flushed_rx: watch::Receiver<u64>,
    pub durable_rx: watch::Receiver<u64>,
}

/// Handle returned from a write operation.
///
/// Provides the epoch assigned to the write and allows waiting
/// for the write to reach a desired durability level.
pub struct WriteHandle {
    epoch: Shared<oneshot::Receiver<u64>>,
    watchers: EpochWatcher,
}

impl WriteHandle {
    pub(crate) fn new(epoch: oneshot::Receiver<u64>, watchers: EpochWatcher) -> Self {
        Self {
            epoch: epoch.shared(),
            watchers,
        }
    }

    /// Returns the epoch assigned to this write.
    ///
    /// Epochs are assigned when the coordinator dequeues the write, so this
    /// method blocks until sequencing completes. Epochs are monotonically
    /// increasing and reflect the actual write order.
    pub async fn epoch(&self) -> WriteResult<u64> {
        self.epoch.clone().await.map_err(|_| WriteError::Shutdown)
    }

    /// Wait for the write to reach the specified durability level.
    pub async fn wait(&mut self, durability: Durability) -> WriteResult<()> {
        let epoch = self.epoch().await?;

        let recv = match durability {
            Durability::Applied => &mut self.watchers.applied_rx,
            Durability::Flushed => &mut self.watchers.flushed_rx,
            Durability::Durable => &mut self.watchers.durable_rx,
        };

        while *recv.borrow() < epoch {
            match recv.changed().await {
                Ok(epoch) => {}
                Err(_) => return Err(WriteError::Shutdown),
            }
        }

        Ok(())
    }
}

/// Handle for submitting writes to the coordinator.
///
/// This is the main interface for interacting with the write coordinator.
/// It can be cloned and shared across tasks.
pub struct WriteCoordinatorHandle<D: Delta> {
    cmd_tx: mpsc::Sender<WriteCommand<D>>,
    watchers: EpochWatcher,
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    pub(crate) fn new(cmd_tx: mpsc::Sender<WriteCommand<D>>, watchers: EpochWatcher) -> Self {
        Self { cmd_tx, watchers }
    }
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    /// Submit a write to the coordinator.
    ///
    /// Returns a handle that can be used to wait for the write to
    /// reach a desired durability level.
    pub async fn write(&self, write: D::Write) -> WriteResult<WriteHandle> {
        let (epoch_tx, epoch_rx) = oneshot::channel();
        self.cmd_tx
            .try_send(WriteCommand::Write {
                write,
                epoch: epoch_tx,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => WriteError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => WriteError::Shutdown,
            })?;

        Ok(WriteHandle::new(epoch_rx, self.watchers.clone()))
    }

    /// Request a flush of the current delta.
    ///
    /// This will trigger a flush even if the flush threshold has not been reached.
    pub async fn flush(&self) -> WriteResult<()> {
        self.cmd_tx
            .try_send(WriteCommand::Flush)
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => WriteError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => WriteError::Shutdown,
            })?;

        Ok(())
    }
}

impl<D: Delta> Clone for WriteCoordinatorHandle<D> {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            watchers: self.watchers.clone(),
        }
    }
}

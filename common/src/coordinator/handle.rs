use super::WriteCommand;
use super::{Delta, Durability, WriteError, WriteResult};
use crate::StorageRead;
use crate::coordinator::traits::EpochStamped;
use crate::storage::StorageSnapshot;
use futures::FutureExt;
use futures::future::Shared;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

/// The current view of all applied writes. The view is made up a reader for
/// the current delta, all frozen deltas awaiting flush, and the current
/// storage snapshot. Readers use the view to query the written data.
pub struct View<D: Delta> {
    pub current: D::DeltaView,
    pub frozen: Vec<EpochStamped<D::FrozenView>>,
    pub snapshot: Arc<dyn StorageSnapshot>,
    pub last_flushed_delta: Option<EpochStamped<D::FrozenView>>,
}

impl<D: Delta> Clone for View<D> {
    fn clone(&self) -> Self {
        Self {
            current: self.current.clone(),
            frozen: self.frozen.clone(),
            snapshot: self.snapshot.clone(),
            last_flushed_delta: self.last_flushed_delta.clone(),
        }
    }
}

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

/// Successful write application with its assigned epoch.
#[derive(Clone, Debug)]
pub(crate) struct WriteApplied<M> {
    pub epoch: u64,
    pub result: M,
}

/// Failed write application with its assigned epoch.
#[derive(Clone, Debug)]
pub(crate) struct WriteFailed {
    pub epoch: u64,
    pub error: String,
}

/// Result payload sent through the oneshot channel for a write or flush.
pub(crate) type EpochResult<M> = Result<WriteApplied<M>, WriteFailed>;

/// Handle returned from a write or flush operation.
///
/// Provides the epoch assigned to the operation, the apply result (for writes),
/// and allows waiting for the operation to reach a desired durability level.
pub struct WriteHandle<M: Clone + Send + 'static = ()> {
    inner: Shared<oneshot::Receiver<EpochResult<M>>>,
    watchers: EpochWatcher,
}

impl<M: Clone + Send + 'static> WriteHandle<M> {
    pub(crate) fn new(rx: oneshot::Receiver<EpochResult<M>>, watchers: EpochWatcher) -> Self {
        Self {
            inner: rx.shared(),
            watchers,
        }
    }

    async fn recv(&self) -> WriteResult<WriteApplied<M>> {
        self.inner
            .clone()
            .await
            .map_err(|_| WriteError::Shutdown)?
            .map_err(|e| WriteError::ApplyError(e.epoch, e.error))
    }

    /// Returns the epoch assigned to this write.
    ///
    /// Epochs are assigned when the coordinator dequeues the write, so this
    /// method blocks until sequencing completes. Epochs are monotonically
    /// increasing and reflect the actual write order.
    #[cfg(test)]
    pub async fn epoch(&self) -> WriteResult<u64> {
        Ok(self.recv().await?.epoch)
    }

    /// Wait for the write to reach the specified durability level.
    ///
    /// Returns the apply result produced by [`Delta::apply`] once the
    /// requested durability level has been reached.
    pub async fn wait(&mut self, durability: Durability) -> WriteResult<M> {
        let WriteApplied { epoch, result } = self.recv().await?;

        let recv = match durability {
            Durability::Applied => &mut self.watchers.applied_rx,
            Durability::Flushed => &mut self.watchers.flushed_rx,
            Durability::Durable => &mut self.watchers.durable_rx,
        };

        recv.wait_for(|curr| *curr >= epoch)
            .await
            .map_err(|_| WriteError::Shutdown)?;
        Ok(result)
    }
}

/// Handle for submitting writes to the coordinator.
///
/// This is the main interface for interacting with the write coordinator.
/// It can be cloned and shared across tasks.
pub struct WriteCoordinatorHandle<D: Delta> {
    write_tx: mpsc::Sender<WriteCommand<D>>,
    watchers: EpochWatcher,
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    pub(crate) fn new(write_tx: mpsc::Sender<WriteCommand<D>>, watchers: EpochWatcher) -> Self {
        Self { write_tx, watchers }
    }
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    /// Submit a write to the coordinator.
    ///
    /// Returns a handle that can be used to retrieve the apply result
    /// and wait for the write to reach a desired durability level.
    pub async fn write(&self, write: D::Write) -> WriteResult<WriteHandle<D::ApplyResult>> {
        let (tx, rx) = oneshot::channel();
        self.write_tx
            .try_send(WriteCommand::Write {
                write,
                result_tx: tx,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => WriteError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => WriteError::Shutdown,
            })?;

        Ok(WriteHandle::new(rx, self.watchers.clone()))
    }

    /// Request a flush of the current delta.
    ///
    /// This will trigger a flush even if the flush threshold has not been reached.
    /// When `flush_storage` is true, the flush will also call `storage.flush()`
    /// to guarantee durability, and the durable watermark will be advanced.
    /// Returns a handle that can be used to wait for the flush to complete.
    pub async fn flush(&self, flush_storage: bool) -> WriteResult<WriteHandle> {
        let (tx, rx) = oneshot::channel();
        self.write_tx
            .try_send(WriteCommand::Flush {
                epoch_tx: tx,
                flush_storage,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => WriteError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => WriteError::Shutdown,
            })?;

        Ok(WriteHandle::new(rx, self.watchers.clone()))
    }
}

impl<D: Delta> Clone for WriteCoordinatorHandle<D> {
    fn clone(&self) -> Self {
        Self {
            write_tx: self.write_tx.clone(),
            watchers: self.watchers.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::watch;

    fn create_watchers(
        applied: watch::Receiver<u64>,
        flushed: watch::Receiver<u64>,
        durable: watch::Receiver<u64>,
    ) -> EpochWatcher {
        EpochWatcher {
            applied_rx: applied,
            flushed_rx: flushed,
            durable_rx: durable,
        }
    }

    #[tokio::test]
    async fn should_return_epoch_when_assigned() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));

        // when
        tx.send(Ok(WriteApplied {
            epoch: 42,
            result: (),
        }))
        .unwrap();
        let result = handle.epoch().await;

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn should_allow_multiple_epoch_calls() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));
        tx.send(Ok(WriteApplied {
            epoch: 42,
            result: (),
        }))
        .unwrap();

        // when
        let result1 = handle.epoch().await;
        let result2 = handle.epoch().await;
        let result3 = handle.epoch().await;

        // then
        assert_eq!(result1.unwrap(), 42);
        assert_eq!(result2.unwrap(), 42);
        assert_eq!(result3.unwrap(), 42);
    }

    #[tokio::test]
    async fn should_return_apply_result_from_wait() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(100u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle: WriteHandle<String> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));

        // when
        tx.send(Ok(WriteApplied {
            epoch: 1,
            result: "hello".to_string(),
        }))
        .unwrap();

        // then
        assert_eq!(handle.wait(Durability::Applied).await.unwrap(), "hello");
    }

    #[tokio::test]
    async fn should_return_immediately_when_watermark_already_reached() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(100u64); // watermark already at 100
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));
        tx.send(Ok(WriteApplied {
            epoch: 50,
            result: (),
        }))
        .unwrap(); // epoch is 50, watermark is 100

        // when
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_wait_until_watermark_reaches_epoch() {
        // given
        let (tx, rx) = oneshot::channel();
        let (applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));
        tx.send(Ok(WriteApplied {
            epoch: 10,
            result: (),
        }))
        .unwrap();

        // when - spawn a task to update the watermark after a delay
        let wait_task = tokio::spawn(async move { handle.wait(Durability::Applied).await });

        tokio::task::yield_now().await;
        applied_tx.send(5).unwrap(); // still below epoch
        tokio::task::yield_now().await;
        applied_tx.send(10).unwrap(); // reaches epoch

        let result = wait_task.await.unwrap();

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_wait_for_correct_durability_level() {
        // given - set up watchers with different values
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(100u64);
        let (_flushed_tx, flushed_rx) = watch::channel(50u64);
        let (durable_tx, durable_rx) = watch::channel(10u64);
        let mut handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));
        tx.send(Ok(WriteApplied {
            epoch: 25,
            result: (),
        }))
        .unwrap();

        // when - wait for Durable (watermark is 10, epoch is 25)
        let wait_task = tokio::spawn(async move { handle.wait(Durability::Durable).await });

        tokio::task::yield_now().await;
        durable_tx.send(25).unwrap(); // update durable watermark

        let result = wait_task.await.unwrap();

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_propagate_epoch_error_in_wait() {
        // given
        let (tx, rx) = oneshot::channel::<EpochResult<()>>();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle = WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));

        // when - drop the sender without sending
        drop(tx);
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(matches!(result, Err(WriteError::Shutdown)));
    }

    #[tokio::test]
    async fn should_propagate_apply_error_in_wait() {
        // given
        let (tx, rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle: WriteHandle<()> =
            WriteHandle::new(rx, create_watchers(applied_rx, flushed_rx, durable_rx));

        // when - send an error
        tx.send(Err(WriteFailed {
            epoch: 1,
            error: "apply error".into(),
        }))
        .unwrap();
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(
            matches!(result, Err(WriteError::ApplyError(epoch, msg)) if epoch == 1 && msg == "apply error")
        );
    }
}

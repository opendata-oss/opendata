use super::WriteCommand;
use super::{Delta, Durability, FlushEvent, FlushResult, WriteError, WriteResult};
use futures::FutureExt;
use futures::future::Shared;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

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
    epoch: Shared<oneshot::Receiver<Result<u64, (u64, String)>>>,
    watchers: EpochWatcher,
}

impl WriteHandle {
    pub(crate) fn new(
        epoch: oneshot::Receiver<Result<u64, (u64, String)>>,
        watchers: EpochWatcher,
    ) -> Self {
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
        self.epoch
            .clone()
            .await
            .map_err(|_| WriteError::Shutdown)?
            .map_err(|(epoch, msg)| WriteError::ApplyError(epoch, msg))
    }

    /// Wait for the write to reach the specified durability level.
    pub async fn wait(&mut self, durability: Durability) -> WriteResult<()> {
        let epoch = self.epoch().await?;

        let recv = match durability {
            Durability::Applied => &mut self.watchers.applied_rx,
            Durability::Flushed => &mut self.watchers.flushed_rx,
            Durability::Durable => &mut self.watchers.durable_rx,
        };

        recv.wait_for(|curr| *curr >= epoch)
            .await
            .map_err(|_| WriteError::Shutdown);
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
    flush_result_tx: broadcast::Sender<FlushResult<D>>,
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    pub(crate) fn new(
        cmd_tx: mpsc::Sender<WriteCommand<D>>,
        watchers: EpochWatcher,
        flush_result_tx: broadcast::Sender<FlushResult<D>>,
    ) -> Self {
        Self {
            cmd_tx,
            watchers,
            flush_result_tx,
        }
    }

    /// Subscribe to flush result notifications.
    ///
    /// Returns a receiver that yields flush results as they occur.
    /// New subscribers only receive flushes that happen after subscribing.
    pub fn subscribe(&self) -> broadcast::Receiver<FlushResult<D>> {
        self.flush_result_tx.subscribe()
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
    /// Returns a handle that can be used to wait for the flush to complete.
    pub async fn flush(&self) -> WriteResult<WriteHandle> {
        let (epoch_tx, epoch_rx) = oneshot::channel();
        self.cmd_tx
            .try_send(WriteCommand::Flush { epoch: epoch_tx })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => WriteError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => WriteError::Shutdown,
            })?;

        Ok(WriteHandle::new(epoch_rx, self.watchers.clone()))
    }
}

impl<D: Delta> Clone for WriteCoordinatorHandle<D> {
    fn clone(&self) -> Self {
        Self {
            cmd_tx: self.cmd_tx.clone(),
            watchers: self.watchers.clone(),
            flush_result_tx: self.flush_result_tx.clone(),
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
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let handle = WriteHandle::new(
            epoch_rx,
            create_watchers(applied_rx, flushed_rx, durable_rx),
        );

        // when
        epoch_tx.send(Ok(42)).unwrap();
        let result = handle.epoch().await;

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn should_allow_multiple_epoch_calls() {
        // given
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let handle = WriteHandle::new(
            epoch_rx,
            create_watchers(applied_rx, flushed_rx, durable_rx),
        );
        epoch_tx.send(Ok(42)).unwrap();

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
    async fn should_return_immediately_when_watermark_already_reached() {
        // given
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(100u64); // watermark already at 100
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle = WriteHandle::new(
            epoch_rx,
            create_watchers(applied_rx, flushed_rx, durable_rx),
        );
        epoch_tx.send(Ok(50)).unwrap(); // epoch is 50, watermark is 100

        // when
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_wait_until_watermark_reaches_epoch() {
        // given
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle = WriteHandle::new(
            epoch_rx,
            create_watchers(applied_rx, flushed_rx, durable_rx),
        );
        epoch_tx.send(Ok(10)).unwrap();

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
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(100u64);
        let (_flushed_tx, flushed_rx) = watch::channel(50u64);
        let (durable_tx, durable_rx) = watch::channel(10u64);
        let mut handle = WriteHandle::new(
            epoch_rx,
            create_watchers(applied_rx, flushed_rx, durable_rx),
        );
        epoch_tx.send(Ok(25)).unwrap();

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
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle = WriteHandle::new(
            epoch_rx,
            create_watchers(applied_rx, flushed_rx, durable_rx),
        );

        // when - drop the sender without sending
        drop(epoch_tx);
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(matches!(result, Err(WriteError::Shutdown)));
    }

    #[tokio::test]
    async fn should_propagate_apply_error_in_wait() {
        // given
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (_applied_tx, applied_rx) = watch::channel(0u64);
        let (_flushed_tx, flushed_rx) = watch::channel(0u64);
        let (_durable_tx, durable_rx) = watch::channel(0u64);
        let mut handle = WriteHandle::new(
            epoch_rx,
            create_watchers(applied_rx, flushed_rx, durable_rx),
        );

        // when - drop the sender without sending
        epoch_tx.send(Err((1, "apply error".into()))).unwrap();
        let result = handle.wait(Durability::Applied).await;

        // then
        assert!(
            matches!(result, Err(WriteError::ApplyError(epoch, msg)) if epoch == 1 && msg == "apply error")
        );
    }
}

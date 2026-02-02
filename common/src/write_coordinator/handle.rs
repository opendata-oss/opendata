//! Write handle for tracking individual write operations.

use futures::future::Shared;
use tokio::sync::oneshot;

use super::error::{Result, WriteCoordinatorError};
use super::flush::FlushWatchers;
use super::types::{Durability, Epoch};

/// A handle returned from a write operation.
///
/// This handle can be used to:
/// - Get the epoch assigned to the write
/// - Wait for the write to reach a specific durability level
#[derive(Debug)]
pub struct WriteHandle {
    /// Receiver for the epoch assigned to this write.
    /// Shared so that epoch() can be called multiple times - Shared caches the result.
    epoch_rx: Shared<oneshot::Receiver<Epoch>>,

    /// Watchers for tracking durability progress.
    watchers: FlushWatchers,
}

impl WriteHandle {
    /// Create a new write handle.
    pub(crate) fn new(epoch_rx: oneshot::Receiver<Epoch>, watchers: FlushWatchers) -> Self {
        use futures::FutureExt;
        Self {
            epoch_rx: epoch_rx.shared(),
            watchers,
        }
    }

    /// Get the epoch assigned to this write.
    ///
    /// This method can be called multiple times and will return the same epoch.
    /// It will wait for the epoch to be assigned if not yet available.
    ///
    /// Note: This method takes `&mut self` to prevent concurrent calls.
    /// The `Shared` future caches the result, so subsequent calls return immediately.
    pub async fn epoch(&mut self) -> Result<Epoch> {
        self.epoch_rx
            .clone()
            .await
            .map_err(|_| WriteCoordinatorError::Shutdown)
    }

    /// Wait for this write to reach the specified durability level.
    ///
    /// This will first wait for the epoch to be assigned, then wait for
    /// the coordinator to signal that the epoch has reached the requested
    /// durability level.
    pub async fn wait(&mut self, durability: Durability) -> Result<()> {
        let epoch = self.epoch().await?;
        self.watchers.wait_for_epoch(epoch, durability).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::flush::FlushWatcherSenders;
    use super::*;

    #[tokio::test]
    async fn should_return_same_epoch_on_multiple_calls() {
        // given
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (_, watchers) = FlushWatcherSenders::new();
        let mut handle = WriteHandle::new(epoch_rx, watchers);
        epoch_tx.send(42).unwrap();

        // when
        let epoch1 = handle.epoch().await.unwrap();
        let epoch2 = handle.epoch().await.unwrap();

        // then
        assert_eq!(epoch1, 42);
        assert_eq!(epoch2, 42);
    }

    #[tokio::test]
    async fn should_wait_for_durability() {
        // given
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (senders, watchers) = FlushWatcherSenders::new();
        let mut handle = WriteHandle::new(epoch_rx, watchers);
        epoch_tx.send(5).unwrap();
        senders.set_applied_epoch(5);

        // when
        let result =
            tokio::time::timeout(Duration::from_millis(100), handle.wait(Durability::Applied))
                .await;

        // then
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn should_return_shutdown_when_sender_dropped() {
        // given
        let (epoch_tx, epoch_rx) = oneshot::channel();
        let (senders, watchers) = FlushWatcherSenders::new();
        let mut handle = WriteHandle::new(epoch_rx, watchers);
        epoch_tx.send(100).unwrap();

        // when
        let wait_handle = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(100), handle.wait(Durability::Durable)).await
        });
        tokio::task::yield_now().await;
        drop(senders);

        // then
        let result = wait_handle.await.unwrap();
        assert_eq!(result.unwrap(), Err(WriteCoordinatorError::Shutdown));
    }
}

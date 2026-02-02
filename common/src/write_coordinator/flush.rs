//! Flush event types and watchers for tracking flush progress.

use std::ops::Range;
use std::sync::Arc;

use tokio::sync::watch;

use super::error::{Result, WriteCoordinatorError};
use super::traits::Delta;
use super::types::Epoch;
use crate::StorageSnapshot;

/// An event emitted when a flush completes.
#[derive(Clone)]
pub struct FlushEvent<D: Delta> {
    /// The snapshot created by the flush.
    pub snapshot: Arc<dyn StorageSnapshot>,
    /// The delta that was flushed.
    pub delta: D,
    /// The range of epochs included in this flush (start inclusive, end exclusive).
    pub epoch_range: Range<Epoch>,
}

/// Watchers for tracking flush progress at different durability levels.
///
/// These are held by WriteHandle to wait for writes to reach specific durability.
#[derive(Debug)]
pub(crate) struct FlushWatchers {
    pub(crate) applied: watch::Receiver<Epoch>,
    pub(crate) flushed: watch::Receiver<Epoch>,
    pub(crate) durable: watch::Receiver<Epoch>,
}

impl FlushWatchers {
    /// Wait for the given epoch to reach the specified durability level.
    pub(crate) async fn wait_for_epoch(
        &mut self,
        epoch: Epoch,
        durability: super::types::Durability,
    ) -> Result<()> {
        use super::types::Durability;

        let receiver = match durability {
            Durability::Applied => &mut self.applied,
            Durability::Flushed => &mut self.flushed,
            Durability::Durable => &mut self.durable,
        };

        // Check if already reached. Note: watch::Receiver::changed() will return
        // immediately if a value was sent after the last borrow(), so there's no
        // race condition between borrow() and changed().
        if *receiver.borrow() >= epoch {
            return Ok(());
        }

        loop {
            match receiver.changed().await {
                Ok(()) => {
                    if *receiver.borrow() >= epoch {
                        return Ok(());
                    }
                }
                Err(_) => {
                    return Err(WriteCoordinatorError::Shutdown);
                }
            }
        }
    }
}

impl Clone for FlushWatchers {
    fn clone(&self) -> Self {
        Self {
            applied: self.applied.clone(),
            flushed: self.flushed.clone(),
            durable: self.durable.clone(),
        }
    }
}

/// Senders for updating flush progress at different durability levels.
///
/// These are held by the coordinator to notify waiters of progress.
pub(crate) struct FlushWatcherSenders {
    pub(crate) applied: watch::Sender<Epoch>,
    pub(crate) flushed: watch::Sender<Epoch>,
    pub(crate) durable: watch::Sender<Epoch>,
}

impl FlushWatcherSenders {
    /// Create a new pair of senders and watchers.
    pub(crate) fn new() -> (Self, FlushWatchers) {
        let (applied_tx, applied_rx) = watch::channel(0);
        let (flushed_tx, flushed_rx) = watch::channel(0);
        let (durable_tx, durable_rx) = watch::channel(0);

        (
            Self {
                applied: applied_tx,
                flushed: flushed_tx,
                durable: durable_tx,
            },
            FlushWatchers {
                applied: applied_rx,
                flushed: flushed_rx,
                durable: durable_rx,
            },
        )
    }

    /// Update the applied epoch.
    pub(crate) fn set_applied_epoch(&self, epoch: Epoch) {
        let _ = self.applied.send(epoch);
    }

    /// Update the flushed epoch.
    pub(crate) fn set_flushed_epoch(&self, epoch: Epoch) {
        let _ = self.flushed.send(epoch);
    }

    /// Update the durable epoch.
    pub(crate) fn set_durable_epoch(&self, epoch: Epoch) {
        let _ = self.durable.send(epoch);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::types::Durability;
    use super::*;

    #[tokio::test]
    async fn should_return_immediately_when_epoch_already_reached() {
        // given
        let (senders, mut watchers) = FlushWatcherSenders::new();
        senders.set_applied_epoch(10);

        // when
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            watchers.wait_for_epoch(5, Durability::Applied),
        )
        .await;

        // then
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn should_wait_until_epoch_is_reached() {
        // given
        let (senders, mut watchers) = FlushWatcherSenders::new();

        // when
        let wait_handle = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                watchers.wait_for_epoch(5, Durability::Flushed),
            )
            .await
        });
        tokio::task::yield_now().await;
        senders.set_flushed_epoch(5);

        // then
        let result = wait_handle.await.unwrap();
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn should_return_shutdown_when_sender_dropped() {
        // given
        let (senders, mut watchers) = FlushWatcherSenders::new();

        // when
        let wait_handle = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(100),
                watchers.wait_for_epoch(100, Durability::Durable),
            )
            .await
        });
        tokio::task::yield_now().await;
        drop(senders);

        // then
        let result = wait_handle.await.unwrap();
        assert_eq!(result.unwrap(), Err(WriteCoordinatorError::Shutdown));
    }
}

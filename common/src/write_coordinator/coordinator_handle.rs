//! Handle for interacting with the write coordinator.

use tokio::sync::{mpsc, oneshot, watch};

use super::error::{Result, WriteCoordinatorError};
use super::flush::{FlushEvent, FlushWatchers};
use super::handle::WriteHandle;
use super::traits::Delta;
use super::types::Epoch;

/// Commands sent to the coordinator.
pub(crate) enum CoordinatorCommand<D: Delta> {
    /// Submit a write to be applied.
    Write {
        write: D::Write,
        epoch_tx: oneshot::Sender<Epoch>,
    },
    /// Request a flush up to a specific epoch.
    Flush { up_to_epoch: Option<Epoch> },
}

/// Handle for submitting writes and requesting flushes.
///
/// This handle can be cloned and shared across tasks.
pub struct WriteCoordinatorHandle<D: Delta> {
    /// Channel for sending commands to the coordinator.
    command_tx: mpsc::Sender<CoordinatorCommand<D>>,

    /// Watchers for tracking durability progress.
    watchers: FlushWatchers,

    /// Receiver for flush events.
    flush_events: watch::Receiver<Option<FlushEvent<D>>>,
}

impl<D: Delta> WriteCoordinatorHandle<D> {
    /// Create a new coordinator handle.
    pub(crate) fn new(
        command_tx: mpsc::Sender<CoordinatorCommand<D>>,
        watchers: FlushWatchers,
        flush_events: watch::Receiver<Option<FlushEvent<D>>>,
    ) -> Self {
        Self {
            command_tx,
            watchers,
            flush_events,
        }
    }

    /// Submit a write to the coordinator.
    ///
    /// Returns a handle that can be used to track the write's progress
    /// and wait for it to reach specific durability levels.
    ///
    /// # Errors
    ///
    /// - `Backpressure` if the write queue is full
    /// - `Shutdown` if the coordinator has been shut down
    pub fn write(&self, write: D::Write) -> Result<WriteHandle> {
        let (epoch_tx, epoch_rx) = oneshot::channel();

        self.command_tx
            .try_send(CoordinatorCommand::Write { write, epoch_tx })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => WriteCoordinatorError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => WriteCoordinatorError::Shutdown,
            })?;

        Ok(WriteHandle::new(epoch_rx, self.watchers.clone()))
    }

    /// Request a flush up to the specified epoch.
    ///
    /// If `up_to_epoch` is `None`, flushes all pending writes.
    /// This method returns immediately after the flush request is sent;
    /// use `WriteHandle::wait` to wait for the flush to complete.
    pub async fn flush(&self, up_to_epoch: Option<Epoch>) -> Result<()> {
        self.command_tx
            .send(CoordinatorCommand::Flush { up_to_epoch })
            .await
            .map_err(|_| WriteCoordinatorError::Shutdown)?;

        Ok(())
    }

    /// Subscribe to flush events.
    ///
    /// Returns a watch receiver that will receive flush events as they occur.
    pub fn subscribe(&self) -> watch::Receiver<Option<FlushEvent<D>>> {
        self.flush_events.clone()
    }
}

impl<D: Delta> Clone for WriteCoordinatorHandle<D> {
    fn clone(&self) -> Self {
        Self {
            command_tx: self.command_tx.clone(),
            watchers: self.watchers.clone(),
            flush_events: self.flush_events.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::flush::FlushWatcherSenders;
    use super::*;

    #[derive(Clone, Default)]
    struct TestDelta {
        values: Vec<i32>,
    }

    impl Delta for TestDelta {
        type Image = Vec<i32>;
        type Write = i32;

        fn init(&mut self, image: &Self::Image) {
            self.values = image.clone();
        }

        fn apply(&mut self, writes: Vec<Self::Write>) -> Result<()> {
            self.values.extend(writes);
            Ok(())
        }

        fn estimate_size(&self) -> usize {
            self.values.len() * std::mem::size_of::<i32>()
        }

        fn fork_image(&self) -> Self::Image {
            self.values.clone()
        }
    }

    #[test]
    fn should_return_backpressure_when_queue_full() {
        // given
        let (command_tx, _command_rx) = mpsc::channel::<CoordinatorCommand<TestDelta>>(2);
        let (_, watchers) = FlushWatcherSenders::new();
        let (_flush_tx, flush_rx) = watch::channel(None);
        let handle = WriteCoordinatorHandle::new(command_tx, watchers, flush_rx);

        // when
        handle.write(1).unwrap();
        handle.write(2).unwrap();
        let result = handle.write(3);

        // then
        assert_eq!(result.unwrap_err(), WriteCoordinatorError::Backpressure);
    }

    #[test]
    fn should_return_shutdown_when_receiver_dropped() {
        // given
        let (command_tx, command_rx) = mpsc::channel::<CoordinatorCommand<TestDelta>>(10);
        let (_, watchers) = FlushWatcherSenders::new();
        let (_flush_tx, flush_rx) = watch::channel(None);
        let handle = WriteCoordinatorHandle::new(command_tx, watchers, flush_rx);
        drop(command_rx);

        // when
        let result = handle.write(42);

        // then
        assert_eq!(result.unwrap_err(), WriteCoordinatorError::Shutdown);
    }
}

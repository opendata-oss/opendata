//! The main write coordinator implementation.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use super::coordinator_handle::{CoordinatorCommand, WriteCoordinatorHandle};
use super::error::Result;
use super::flush::{FlushEvent, FlushWatcherSenders};
use super::traits::{Delta, Flusher};
use crate::Storage;

/// Configuration for the write coordinator.
#[derive(Debug, Clone)]
pub struct WriteCoordinatorConfig {
    /// The capacity of the write command queue.
    ///
    /// When the queue is full, writes will be rejected with backpressure.
    pub queue_capacity: usize,

    /// The interval at which to automatically flush pending writes.
    pub flush_interval: Duration,

    /// The maximum size of a delta in bytes before triggering a flush.
    ///
    /// When the estimated size of the current delta exceeds this value,
    /// a flush will be triggered.
    pub max_delta_size_bytes: usize,
}

impl Default for WriteCoordinatorConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            flush_interval: Duration::from_secs(1),
            max_delta_size_bytes: 64 * 1024 * 1024, // 64 MB
        }
    }
}

/// The write coordinator manages write operations and flushes.
///
/// It receives writes through a channel, applies them to an in-memory delta,
/// and periodically flushes the delta to storage.
pub struct WriteCoordinator<D: Delta, F: Flusher<Delta = D>> {
    config: WriteCoordinatorConfig,
    storage: Arc<dyn Storage>,
    flusher: F,
    delta: D,
    current_image: D::Image,
    command_rx: mpsc::Receiver<CoordinatorCommand<D>>,
    watcher_senders: FlushWatcherSenders,
    flush_event_tx: watch::Sender<Option<FlushEvent<D>>>,
}

impl<D: Delta + Default, F: Flusher<Delta = D>> WriteCoordinator<D, F> {
    /// Create a new write coordinator.
    ///
    /// Returns the coordinator and a handle for interacting with it.
    pub fn new(
        config: WriteCoordinatorConfig,
        storage: Arc<dyn Storage>,
        flusher: F,
        initial_image: D::Image,
    ) -> (Self, WriteCoordinatorHandle<D>) {
        let (command_tx, command_rx) = mpsc::channel(config.queue_capacity);
        let (watcher_senders, watchers) = FlushWatcherSenders::new();
        let (flush_event_tx, flush_event_rx) = watch::channel(None);

        let mut delta = D::default();
        delta.init(&initial_image);

        let coordinator = Self {
            config,
            storage,
            flusher,
            delta,
            current_image: initial_image,
            command_rx,
            watcher_senders,
            flush_event_tx,
        };

        let handle = WriteCoordinatorHandle::new(command_tx, watchers, flush_event_rx);

        (coordinator, handle)
    }

    /// Run the coordinator loop.
    ///
    /// This method processes incoming commands and manages flushes.
    /// It runs until the command channel is closed (all handles are dropped).
    pub async fn run(self) -> Result<()> {
        todo!("Implement coordinator loop")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::in_memory::InMemoryStorage;

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

    struct TestFlusher;

    #[async_trait::async_trait]
    impl Flusher for TestFlusher {
        type Delta = TestDelta;

        async fn flush(&self, _delta: Self::Delta) -> Result<Arc<dyn crate::StorageSnapshot>> {
            todo!("Test flusher not implemented")
        }
    }

    #[test]
    fn should_create_coordinator_with_config() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher;
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_millis(500),
            max_delta_size_bytes: 1024 * 1024,
        };

        // when
        let (coordinator, _handle) =
            WriteCoordinator::new(config.clone(), storage, flusher, vec![1, 2, 3]);

        // then
        assert_eq!(coordinator.config.queue_capacity, 100);
        assert_eq!(
            coordinator.config.flush_interval,
            Duration::from_millis(500)
        );
        assert_eq!(coordinator.config.max_delta_size_bytes, 1024 * 1024);
    }
}

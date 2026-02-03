//! The main write coordinator implementation.

#![allow(dead_code)]

use std::ops::Range;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use super::coordinator_handle::{CoordinatorCommand, WriteCoordinatorHandle};
use super::error::Result;
use super::flush::{FlushEvent, FlushWatcherSenders};
use super::traits::{Delta, Flusher};
use super::types::Epoch;
use crate::{Storage, StorageSnapshot};

/// Request sent to the background flush worker.
struct FlushRequest<D: Delta> {
    epoch_range: Range<Epoch>,
    delta: D,
}

/// Message sent from background flush task to coordinator when flush completes.
struct FlushCompletion<D: Delta> {
    epoch_range: Range<Epoch>,
    delta: D,
    result: Result<Arc<dyn StorageSnapshot>>,
}

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
///
/// Flushes are performed by a single background worker task to avoid blocking write
/// processing. Only the image forking is synchronized; the actual storage flush happens
/// in the background. Flush requests are queued and processed sequentially.
pub struct WriteCoordinator<D: Delta, F: Flusher<Delta = D>> {
    config: WriteCoordinatorConfig,
    #[allow(dead_code)]
    storage: Arc<dyn Storage>,
    flusher: Arc<F>,
    delta: D,
    current_image: D::Image,
    command_rx: mpsc::Receiver<CoordinatorCommand<D>>,
    watcher_senders: FlushWatcherSenders,
    flush_event_tx: watch::Sender<Option<FlushEvent<D>>>,
    /// Channel for sending flush requests to the background worker.
    flush_request_tx: mpsc::Sender<FlushRequest<D>>,
    /// Channel for receiving flush completion notifications from background worker.
    flush_completion_rx: mpsc::Receiver<FlushCompletion<D>>,
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

        // Channels for flush worker communication
        // TODO: Consider backpressure for flush_request channel. Currently unbounded-ish
        // since backed up flushes are uncommon, but may need limits if flushes fall behind.
        let (flush_request_tx, flush_request_rx) = mpsc::channel::<FlushRequest<D>>(16);
        let (flush_completion_tx, flush_completion_rx) = mpsc::channel(16);

        let flusher = Arc::new(flusher);

        // Spawn the background flush worker
        tokio::spawn(Self::flush_worker(
            flusher.clone(),
            flush_request_rx,
            flush_completion_tx,
        ));

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
            flush_request_tx,
            flush_completion_rx,
        };

        let handle = WriteCoordinatorHandle::new(command_tx, watchers, flush_event_rx);

        (coordinator, handle)
    }

    /// Run the coordinator loop.
    ///
    /// This method processes incoming commands and manages flushes.
    /// It runs until the command channel is closed (all handles are dropped).
    pub async fn run(mut self) -> Result<()> {
        let mut current_epoch = 0;
        let mut flush_start_epoch: Epoch = 1;

        let mut flush_deadline = Instant::now() + self.config.flush_interval;
        let mut flush_timer = pin!(tokio::time::sleep_until(flush_deadline));

        loop {
            tokio::select! {
                biased;

                // Handle flush completions first to update watchers promptly
                Some(completion) = self.flush_completion_rx.recv() => {
                    self.handle_flush_completion(completion)?;
                }

                command = self.command_rx.recv() => {
                    match command {
                        Some(CoordinatorCommand::Write { write, epoch_tx }) => {
                            current_epoch += 1;
                            let epoch = current_epoch;

                            fail::fail_point!("write_coordinator::before_apply");

                            self.delta.apply(vec![write])?;

                            fail::fail_point!("write_coordinator::after_apply");

                            let _ = epoch_tx.send(epoch);
                            self.watcher_senders.set_applied_epoch(epoch);

                            if self.delta.estimate_size() >= self.config.max_delta_size_bytes {
                                flush_start_epoch =
                                    self.enqueue_flush(flush_start_epoch, current_epoch);
                                flush_deadline = Instant::now() + self.config.flush_interval;
                                flush_timer.as_mut().reset(flush_deadline);
                            }
                        }
                        Some(CoordinatorCommand::Flush { up_to_epoch: _ }) => {
                            fail::fail_point!("write_coordinator::before_flush_command");

                            if flush_start_epoch <= current_epoch {
                                flush_start_epoch =
                                    self.enqueue_flush(flush_start_epoch, current_epoch);
                                flush_deadline = Instant::now() + self.config.flush_interval;
                                flush_timer.as_mut().reset(flush_deadline);
                            }
                        }
                        None => {
                            // Channel closed; dropping flush_request_tx will cause
                            // the flush worker to drain and exit
                            break;
                        }
                    }
                }

                _ = &mut flush_timer => {
                    if flush_start_epoch <= current_epoch {
                        flush_start_epoch = self.enqueue_flush(flush_start_epoch, current_epoch);
                    }
                    flush_deadline = Instant::now() + self.config.flush_interval;
                    flush_timer.as_mut().reset(flush_deadline);
                }
            }
        }

        Ok(())
    }

    /// Background worker that processes flush requests sequentially.
    async fn flush_worker(
        flusher: Arc<F>,
        mut request_rx: mpsc::Receiver<FlushRequest<D>>,
        completion_tx: mpsc::Sender<FlushCompletion<D>>,
    ) {
        while let Some(request) = request_rx.recv().await {
            // Failpoint after fork but before flush - allows testing concurrent writes
            fail::fail_point!("write_coordinator::after_fork_image");

            let result = flusher.flush(request.delta.clone()).await;

            fail::fail_point!("write_coordinator::after_flush");

            let _ = completion_tx
                .send(FlushCompletion {
                    epoch_range: request.epoch_range,
                    delta: request.delta,
                    result,
                })
                .await;
        }
    }

    /// Enqueue a flush operation, returning the next flush_start_epoch.
    ///
    /// This method forks the image synchronously (which is fast) and sends the
    /// delta to the background flush worker. This allows writes to continue while
    /// the flush is in progress.
    fn enqueue_flush(&mut self, flush_start_epoch: Epoch, end_epoch: Epoch) -> Epoch {
        fail::fail_point!("write_coordinator::flush_start");

        // Clone the delta and fork the image - this is the synchronized part
        let delta_to_flush = self.delta.clone();
        self.current_image = self.delta.fork_image();
        self.delta = D::default();
        self.delta.init(&self.current_image);

        let epoch_range = flush_start_epoch..(end_epoch + 1);

        // Send to background worker (this won't block since channel has capacity)
        // If the channel is full, try_send would fail - for now we just use send
        // which will block. See TODO in new() about backpressure.
        let _ = self.flush_request_tx.try_send(FlushRequest {
            epoch_range,
            delta: delta_to_flush,
        });

        end_epoch + 1
    }

    /// Handle completion of a background flush.
    fn handle_flush_completion(&mut self, completion: FlushCompletion<D>) -> Result<()> {
        let snapshot = completion.result?;
        let end_epoch = completion.epoch_range.end - 1;

        self.watcher_senders.set_flushed_epoch(end_epoch);
        self.watcher_senders.set_durable_epoch(end_epoch);

        let _ = self.flush_event_tx.send(Some(FlushEvent {
            snapshot,
            delta: completion.delta,
            epoch_range: completion.epoch_range,
        }));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::super::types::Durability;
    use super::*;
    use crate::storage::in_memory::InMemoryStorage;

    #[derive(Clone, Default)]
    struct TestDelta {
        // Initial state from the image
        base_values: Vec<i32>,
        // New writes since last flush
        new_values: Vec<i32>,
    }

    impl TestDelta {
        fn all_values(&self) -> Vec<i32> {
            let mut result = self.base_values.clone();
            result.extend(&self.new_values);
            result
        }
    }

    impl Delta for TestDelta {
        type Image = Vec<i32>;
        type Write = i32;

        fn init(&mut self, image: &Self::Image) {
            self.base_values = image.clone();
            self.new_values.clear();
        }

        fn apply(&mut self, writes: Vec<Self::Write>) -> Result<()> {
            self.new_values.extend(writes);
            Ok(())
        }

        fn estimate_size(&self) -> usize {
            self.new_values.len() * std::mem::size_of::<i32>()
        }

        fn fork_image(&self) -> Self::Image {
            self.all_values()
        }
    }

    #[derive(Clone)]
    struct TestFlusher {
        storage: Arc<dyn crate::Storage>,
        flush_count: Arc<Mutex<usize>>,
    }

    impl TestFlusher {
        fn new(storage: Arc<dyn crate::Storage>) -> Self {
            Self {
                storage,
                flush_count: Arc::new(Mutex::new(0)),
            }
        }

        fn get_flush_count(&self) -> usize {
            *self.flush_count.lock().unwrap()
        }
    }

    #[async_trait::async_trait]
    impl Flusher for TestFlusher {
        type Delta = TestDelta;

        async fn flush(&self, _delta: Self::Delta) -> Result<Arc<dyn crate::StorageSnapshot>> {
            *self.flush_count.lock().unwrap() += 1;
            Ok(self.storage.snapshot().await?)
        }
    }

    #[tokio::test]
    async fn should_create_coordinator_with_config() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher::new(storage.clone());
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

    #[tokio::test]
    async fn should_assign_epochs_and_apply_writes() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 10,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) = WriteCoordinator::new(config, storage, flusher, vec![]);
        tokio::spawn(coordinator.run());

        // when
        let mut h1 = handle.write(1).unwrap();
        let mut h2 = handle.write(2).unwrap();
        let mut h3 = handle.write(3).unwrap();

        // then
        assert_eq!(h1.epoch().await.unwrap(), 1);
        assert_eq!(h2.epoch().await.unwrap(), 2);
        assert_eq!(h3.epoch().await.unwrap(), 3);

        h1.wait(Durability::Applied).await.unwrap();
        h2.wait(Durability::Applied).await.unwrap();
        h3.wait(Durability::Applied).await.unwrap();
    }

    #[tokio::test]
    async fn should_flush_when_size_threshold_exceeded() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 10,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 8,
        };

        let (coordinator, handle) = WriteCoordinator::new(config, storage, flusher.clone(), vec![]);
        tokio::spawn(coordinator.run());

        // when - write 2 i32s (8 bytes) which meets threshold
        let mut h1 = handle.write(1).unwrap();
        let mut h2 = handle.write(2).unwrap();

        // then - h2 triggers flush
        h2.wait(Durability::Flushed).await.unwrap();
        assert_eq!(flusher.get_flush_count(), 1);

        // and h1 is also flushed
        h1.wait(Durability::Flushed).await.unwrap();
    }

    #[tokio::test]
    async fn should_flush_on_timer() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 10,
            flush_interval: Duration::from_millis(50),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) = WriteCoordinator::new(config, storage, flusher.clone(), vec![]);
        tokio::spawn(coordinator.run());

        // when
        let mut h1 = handle.write(1).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // then
        h1.wait(Durability::Flushed).await.unwrap();
        assert!(flusher.get_flush_count() >= 1);
    }

    #[tokio::test]
    async fn should_flush_on_explicit_command() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 10,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) = WriteCoordinator::new(config, storage, flusher.clone(), vec![]);
        tokio::spawn(coordinator.run());

        // when
        let mut h1 = handle.write(1).unwrap();
        handle.flush(None).await.unwrap();

        // then
        h1.wait(Durability::Flushed).await.unwrap();
        assert_eq!(flusher.get_flush_count(), 1);
    }

    #[tokio::test]
    async fn should_not_flush_when_no_pending_writes() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 10,
            flush_interval: Duration::from_millis(50),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) = WriteCoordinator::new(config, storage, flusher.clone(), vec![]);
        tokio::spawn(coordinator.run());

        // when - wait for timer without writing anything
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.flush(None).await.unwrap();

        // then - no flushes should occur
        assert_eq!(flusher.get_flush_count(), 0);
    }

    #[tokio::test]
    async fn should_receive_flush_events() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 10,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) = WriteCoordinator::new(config, storage, flusher, vec![]);
        tokio::spawn(coordinator.run());

        let mut events = handle.subscribe();

        // when
        let _h1 = handle.write(1).unwrap();
        let _h2 = handle.write(2).unwrap();
        handle.flush(None).await.unwrap();

        // then
        events.changed().await.unwrap();
        let event = events.borrow().clone().unwrap();
        assert_eq!(event.epoch_range, 1..3);
        assert_eq!(event.delta.all_values(), vec![1, 2]);
    }

    #[tokio::test]
    async fn should_preserve_image_across_flushes() {
        // given
        let storage = Arc::new(InMemoryStorage::new());
        let flusher = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 10,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) = WriteCoordinator::new(config, storage, flusher, vec![100, 200]);
        tokio::spawn(coordinator.run());

        let mut events = handle.subscribe();

        // when - write and flush
        handle.write(1).unwrap();
        handle.flush(None).await.unwrap();

        // then - first flush includes initial image + write
        events.changed().await.unwrap();
        let event1 = events.borrow().clone().unwrap();
        assert_eq!(event1.delta.all_values(), vec![100, 200, 1]);

        // when - write and flush again
        handle.write(2).unwrap();
        handle.flush(None).await.unwrap();

        // then - second flush preserves forked image
        events.changed().await.unwrap();
        let event2 = events.borrow().clone().unwrap();
        assert_eq!(event2.delta.all_values(), vec![100, 200, 1, 2]);
    }
}

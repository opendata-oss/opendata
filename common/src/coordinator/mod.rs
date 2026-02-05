#![allow(unused)]

mod error;
mod handle;
mod traits;

pub use error::{WriteError, WriteResult};
pub use handle::{WriteCoordinatorHandle, WriteHandle};
use std::ops::{Deref, DerefMut};
pub use traits::{Delta, Durability, FlushEvent, FlushResult, Flusher};

// Internal use only
pub(crate) use handle::EpochWatcher;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Instant, Interval, interval_at};

/// Configuration for the write coordinator.
#[derive(Debug, Clone)]
pub struct WriteCoordinatorConfig {
    /// Maximum number of pending writes in the queue.
    pub queue_capacity: usize,
    /// Interval at which to trigger automatic flushes.
    pub flush_interval: Duration,
    /// Delta size threshold at which to trigger a flush.
    pub flush_size_threshold: usize,
}

impl Default for WriteCoordinatorConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            flush_interval: Duration::from_secs(10),
            flush_size_threshold: 64 * 1024 * 1024, // 64 MB
        }
    }
}

pub(crate) enum WriteCommand<D: Delta> {
    Write {
        write: D::Write,
        epoch: oneshot::Sender<u64>,
    },
    Flush {
        epoch: oneshot::Sender<u64>,
    },
}

/// The write coordinator manages write ordering, batching, and durability.
///
/// It accepts writes through `WriteCoordinatorHandle`, applies them to a `Delta`,
/// and coordinates flushing through a `Flusher`.
pub struct WriteCoordinator<D: Delta, F: Flusher<D>> {
    config: WriteCoordinatorConfig,
    delta: CurrentDelta<D>,
    flush_task: Option<FlushTask<D, F>>,
    flush_tx: mpsc::Sender<FlushEvent<D>>,
    cmd_rx: mpsc::Receiver<WriteCommand<D>>,
    applied_tx: watch::Sender<u64>,
    #[allow(dead_code)]
    durable_tx: watch::Sender<u64>,
    epoch: u64,
    last_flush_epoch: u64,
    flush_interval: Interval,
}

impl<D: Delta, F: Flusher<D>> WriteCoordinator<D, F> {
    /// Create a new write coordinator with the given flusher.
    ///
    /// This is useful for testing with mock flushers.
    pub fn new(
        config: WriteCoordinatorConfig,
        initial_context: D::Context,
        flusher: F,
    ) -> (Self, WriteCoordinatorHandle<D>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(config.queue_capacity);

        let (applied_tx, applied_rx) = watch::channel(0);
        let (flushed_tx, flushed_rx) = watch::channel(0);
        let (durable_tx, durable_rx) = watch::channel(0);

        // this is the channel that sends FlushEvents to be flushed
        // by a background task so that the process of converting deltas
        // to storage operations is non-blocking. for now, we apply no
        // backpressure on this channel, so writes will block if more than
        // one flush is pending
        let (flush_tx, flush_rx) = mpsc::channel(1);

        // Broadcast channel for flush results (buffer size 16 should be plenty)
        let (flush_result_tx, _) = broadcast::channel(16);

        let watcher = EpochWatcher {
            applied_rx,
            flushed_rx,
            durable_rx,
        };

        let mut delta = D::init(initial_context);

        let flush_task = FlushTask {
            flusher,
            flush_rx,
            flushed_tx,
            flush_result_tx: flush_result_tx.clone(),
        };
        let flush_interval = interval_at(
            Instant::now() + config.flush_interval,
            config.flush_interval,
        );
        let coordinator = Self {
            config,
            delta: CurrentDelta::new(delta),
            cmd_rx,
            flush_tx,
            flush_task: Some(flush_task),
            applied_tx,
            durable_tx,
            // Epochs start at 1 because watch channels initialize to 0 (meaning "nothing
            // processed yet"). If the first write had epoch 0, wait() would return
            // immediately since the condition `watermark < epoch` would be `0 < 0` = false.
            epoch: 1,
            last_flush_epoch: 1,
            flush_interval,
        };

        (
            coordinator,
            WriteCoordinatorHandle::new(cmd_tx, watcher, flush_result_tx),
        )
    }

    /// Run the coordinator event loop.
    pub async fn run(mut self) -> Result<(), String> {
        // Start the flush task
        let flush_task = self
            .flush_task
            .take()
            .expect("flush_task should be Some at start of run");
        let flush_task_handle = flush_task.run();

        // Reset the interval to start fresh from when run() is called
        self.flush_interval.reset();

        loop {
            tokio::select! {
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(WriteCommand::Write {write, epoch: epoch_tx}) => {
                            self.handle_write(write, epoch_tx).await?;
                        }
                        Some(WriteCommand::Flush { epoch: epoch_tx }) => {
                            // Send back the epoch of the last processed write
                            let _ = epoch_tx.send(self.epoch.saturating_sub(1));
                            self.handle_flush().await;
                        }
                        None => {
                            break;
                        }
                    }
                }

                _ = self.flush_interval.tick() => {
                    self.handle_flush().await;
                }
            }
        }

        // Flush any remaining pending writes before shutdown
        self.handle_flush().await;

        // Signal the flush task to stop by dropping the sender
        drop(self.flush_tx);

        // Wait for the flush task to complete and propagate any errors
        flush_task_handle
            .await
            .map_err(|e| format!("flush task panicked: {}", e))?
            .map_err(|e| format!("flush task error: {}", e))
    }

    async fn handle_write(
        &mut self,
        write: D::Write,
        epoch_tx: oneshot::Sender<u64>,
    ) -> Result<(), String> {
        let write_epoch = self.epoch;
        self.epoch += 1;

        // Ignore error if receiver was dropped (fire-and-forget write)
        let _ = epoch_tx.send(write_epoch);

        self.delta.apply(write)?;
        // Ignore error if no watchers are listening - this is non-fatal
        let _ = self.applied_tx.send(write_epoch);

        if self.delta.estimate_size() >= self.config.flush_size_threshold {
            self.handle_flush().await;
        }

        Ok(())
    }

    async fn handle_flush(&mut self) {
        if self.epoch == self.last_flush_epoch {
            return;
        }

        let epoch_range = self.last_flush_epoch..self.epoch;
        self.last_flush_epoch = self.epoch;
        self.flush_interval.reset();

        // this is the blocking section of the flush, new writes will not be accepted
        // until the event is sent to the FlushTask
        let frozen = self.delta.freeze_and_init();

        // Block until the flush task can accept the event
        let _ = self
            .flush_tx
            .send(FlushEvent {
                delta: frozen,
                epoch_range,
            })
            .await;
    }
}

struct FlushTask<D: Delta, F: Flusher<D>> {
    flusher: F,
    flush_rx: mpsc::Receiver<FlushEvent<D>>,
    flushed_tx: watch::Sender<u64>,
    flush_result_tx: broadcast::Sender<FlushResult<D>>,
}

impl<D: Delta, F: Flusher<D>> FlushTask<D, F> {
    fn run(mut self) -> tokio::task::JoinHandle<WriteResult<()>> {
        tokio::spawn(async move {
            while let Some(event) = self.flush_rx.recv().await {
                let snapshot = self
                    .flusher
                    .flush(&event)
                    .await
                    .map_err(|e| WriteError::FlushError(e.to_string()))?;

                let flushed_epoch = event.epoch_range.end - 1;
                self.flushed_tx
                    .send(flushed_epoch)
                    .map_err(|_| WriteError::Shutdown)?;

                // Broadcast flush result to subscribers (ignore if no receivers)
                let result = FlushResult {
                    snapshot,
                    delta: Arc::new(event.delta),
                    epoch_range: event.epoch_range,
                };
                let _ = self.flush_result_tx.send(result);
            }

            Ok(())
        })
    }
}

struct CurrentDelta<D: Delta> {
    delta: Option<D>,
}

impl<D: Delta> Deref for CurrentDelta<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        match &self.delta {
            Some(d) => d,
            None => panic!("current delta not initialized"),
        }
    }
}

impl<D: Delta> DerefMut for CurrentDelta<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.delta {
            Some(d) => d,
            None => panic!("current delta not initialized"),
        }
    }
}

impl<D: Delta> CurrentDelta<D> {
    fn new(delta: D) -> Self {
        Self { delta: Some(delta) }
    }

    fn freeze_and_init(&mut self) -> D::Frozen {
        let Some(delta) = self.delta.take() else {
            panic!("delta not initialized");
        };
        let (imm_delta, context) = delta.freeze();
        self.delta = Some(D::init(context));
        imm_delta
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StorageRead;
    use crate::coordinator::Durability;
    use crate::storage::in_memory::InMemoryStorage;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Mutex;
    // ============================================================================
    // Test Infrastructure
    // ============================================================================

    #[derive(Clone, Debug)]
    struct TestWrite {
        key: String,
        value: u64,
        size: usize,
    }

    /// Context carries state that must persist across deltas (like series dictionary)
    #[derive(Clone, Debug, Default)]
    struct TestContext {
        key_to_id: HashMap<String, u64>,
        next_id: u64,
    }

    /// Delta accumulates writes and can allocate new IDs for unknown keys.
    /// Stores the context directly and updates it in place.
    #[derive(Clone, Debug, Default)]
    struct TestDelta {
        context: TestContext,
        writes: HashMap<u64, Vec<u64>>,
        total_size: usize,
    }

    impl Delta for TestDelta {
        type Context = TestContext;
        type Write = TestWrite;
        type Frozen = TestDelta;

        fn init(context: Self::Context) -> Self {
            Self {
                context,
                writes: HashMap::default(),
                total_size: 0,
            }
        }

        fn apply(&mut self, write: Self::Write) -> Result<(), String> {
            let id = *self.context.key_to_id.entry(write.key).or_insert_with(|| {
                let id = self.context.next_id;
                self.context.next_id += 1;
                id
            });

            self.writes.entry(id).or_default().push(write.value);
            self.total_size += write.size;
            Ok(())
        }

        fn estimate_size(&self) -> usize {
            self.total_size
        }

        fn freeze(self) -> (Self::Frozen, Self::Context) {
            let context = self.context;
            let frozen = TestDelta {
                context: context.clone(),
                writes: self.writes,
                total_size: self.total_size,
            };
            (frozen, context)
        }
    }

    /// Shared state for TestFlusher - allows test to inspect and control behavior
    #[derive(Default)]
    struct TestFlusherState {
        flushed_events: Vec<(TestDelta, Range<u64>)>,
        /// Signals when a flush starts (before blocking)
        flush_started_tx: Option<oneshot::Sender<()>>,
        /// Blocks flush until signaled
        unblock_rx: Option<mpsc::Receiver<()>>,
    }

    #[derive(Clone, Default)]
    struct TestFlusher {
        state: Arc<Mutex<TestFlusherState>>,
    }

    impl TestFlusher {
        /// Create a flusher that blocks until signaled, with a notification when flush starts.
        /// Returns (flusher, flush_started_rx, unblock_tx).
        fn with_flush_control() -> (Self, oneshot::Receiver<()>, mpsc::Sender<()>) {
            let (started_tx, started_rx) = oneshot::channel();
            let (unblock_tx, unblock_rx) = mpsc::channel(1);
            let flusher = Self {
                state: Arc::new(Mutex::new(TestFlusherState {
                    flushed_events: Vec::new(),
                    flush_started_tx: Some(started_tx),
                    unblock_rx: Some(unblock_rx),
                })),
            };
            (flusher, started_rx, unblock_tx)
        }

        fn flushed_events(&self) -> Vec<(TestDelta, Range<u64>)> {
            self.state.lock().unwrap().flushed_events.clone()
        }
    }

    #[async_trait]
    impl Flusher<TestDelta> for TestFlusher {
        async fn flush(
            &self,
            event: &FlushEvent<TestDelta>,
        ) -> Result<Arc<dyn StorageRead>, String> {
            // Signal that flush has started
            let flush_started_tx = {
                let mut state = self.state.lock().unwrap();
                state.flush_started_tx.take()
            };
            if let Some(tx) = flush_started_tx {
                let _ = tx.send(());
            }

            // Block if test wants to control timing
            let unblock_rx = {
                let mut state = self.state.lock().unwrap();
                state.unblock_rx.take()
            };
            if let Some(mut rx) = unblock_rx {
                rx.recv().await;
            }

            // Record the flush
            {
                let mut state = self.state.lock().unwrap();
                state
                    .flushed_events
                    .push((event.delta.clone(), event.epoch_range.clone()));
            }

            // not used in the tests
            Ok(Arc::new(InMemoryStorage::default()))
        }
    }

    fn test_config() -> WriteCoordinatorConfig {
        WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600), // Long interval to avoid timer flushes
            flush_size_threshold: usize::MAX,
        }
    }

    // ============================================================================
    // Basic Write Flow Tests
    // ============================================================================

    #[tokio::test]
    async fn should_assign_monotonic_epochs() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        let write1 = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let write2 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let write3 = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        let epoch1 = write1.epoch().await.unwrap();
        let epoch2 = write2.epoch().await.unwrap();
        let epoch3 = write3.epoch().await.unwrap();

        // then
        assert!(epoch1 < epoch2);
        assert!(epoch2 < epoch3);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_apply_writes_in_order() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let mut last_write = handle
            .write(TestWrite {
                key: "a".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        handle.flush().await.unwrap();
        // Wait for flush to complete via watermark
        last_write.wait(Durability::Flushed).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let (delta, _) = &events[0];
        // All writes to key "a" should be under the same ID in order
        let ctx = &delta.context;
        let id = ctx.key_to_id.get("a").unwrap();
        let values = delta.writes.get(id).unwrap();
        assert_eq!(values, &[1, 2, 3]);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn should_update_applied_watermark_after_each_write() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        let mut write_handle = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();

        // then - wait should succeed immediately after write is applied
        let result = write_handle.wait(Durability::Applied).await;
        assert!(result.is_ok());

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    // ============================================================================
    // Manual Flush Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_flush_on_command() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        let mut write = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write.wait(Durability::Flushed).await.unwrap();

        // then
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_wait_on_flush_handle() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let mut flush_handle = handle.flush().await.unwrap();

        // then - can wait directly on the flush handle
        flush_handle.wait(Durability::Flushed).await.unwrap();
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_return_correct_epoch_from_flush_handle() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        let write1 = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let write2 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let flush_handle = handle.flush().await.unwrap();

        // then - flush handle epoch should be the last write's epoch
        let flush_epoch = flush_handle.epoch().await.unwrap();
        let write2_epoch = write2.epoch().await.unwrap();
        assert_eq!(flush_epoch, write2_epoch);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_all_pending_writes_in_flush() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let mut last_write = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        handle.flush().await.unwrap();
        last_write.wait(Durability::Flushed).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let (delta, _) = &events[0];
        assert_eq!(delta.context.key_to_id.len(), 3);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_skip_flush_when_no_new_writes() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        let mut write = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write.wait(Durability::Flushed).await.unwrap();

        // Second flush with no new writes
        handle.flush().await.unwrap();

        // Synchronization: write and wait for applied to ensure the flush command
        // has been processed (commands are processed in order)
        let sync_write = handle
            .write(TestWrite {
                key: "sync".into(),
                value: 0,
                size: 1,
            })
            .await
            .unwrap();
        sync_write.epoch().await.unwrap();

        // then - only one flush should have occurred (the second flush was a no-op)
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_update_flushed_watermark_after_flush() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        let mut write_handle = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();

        handle.flush().await.unwrap();

        // then - wait for Flushed should succeed after flush completes
        let result = write_handle.wait(Durability::Flushed).await;
        assert!(result.is_ok());

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    // ============================================================================
    // Timer-Based Flush Tests
    // ============================================================================

    #[tokio::test(start_paused = true)]
    async fn should_start_flush_interval_when_run_is_called() {
        // given - create coordinator with short flush interval
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_millis(100),
            flush_size_threshold: usize::MAX,
        };
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            config,
            TestContext::default(),
            flusher.clone(),
        );

        // Advance time past the flush interval BEFORE calling run()
        tokio::time::advance(Duration::from_millis(200)).await;

        // when - start coordinator and write something
        let coordinator_task = tokio::spawn(coordinator.run());
        tokio::task::yield_now().await;

        let mut write = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        write.wait(Durability::Applied).await.unwrap();

        // then - no flush should have happened yet (interval was reset in run())
        assert_eq!(flusher.flushed_events().len(), 0);

        // when - advance time past the flush interval from when run() was called
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        // then - flush should have happened
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    // ============================================================================
    // Size-Threshold Flush Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_flush_when_size_threshold_exceeded() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: 100, // Low threshold for testing
        };
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            config,
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when - write that exceeds threshold
        let mut write = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 150,
            })
            .await
            .unwrap();
        write.wait(Durability::Flushed).await.unwrap();

        // then
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_accumulate_until_threshold() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: 100,
        };
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            config,
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when - small writes that accumulate
        for i in 0..5 {
            let mut w = handle
                .write(TestWrite {
                    key: format!("key{}", i),
                    value: i,
                    size: 15,
                })
                .await
                .unwrap();
            w.wait(Durability::Applied).await.unwrap();
        }

        // then - no flush yet (75 bytes < 100 threshold)
        assert_eq!(flusher.flushed_events().len(), 0);

        // when - write that pushes over threshold
        let mut final_write = handle
            .write(TestWrite {
                key: "final".into(),
                value: 999,
                size: 30,
            })
            .await
            .unwrap();
        final_write.wait(Durability::Flushed).await.unwrap();

        // then - should have flushed (105 bytes > 100 threshold)
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    // ============================================================================
    // Non-Blocking Flush (Concurrency) Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_accept_writes_during_flush() {
        // given
        let (flusher, flush_started_rx, unblock_tx) = TestFlusher::with_flush_control();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when: trigger a flush and wait for it to start (proving it's in progress)
        let write1 = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        flush_started_rx.await.unwrap(); // wait until flush is actually in progress

        // then: writes during blocked flush still succeed
        let write2 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        assert!(write2.epoch().await.unwrap() > write1.epoch().await.unwrap());

        // cleanup
        unblock_tx.send(()).await.unwrap();
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_assign_new_epochs_during_flush() {
        // given
        let (flusher, flush_started_rx, unblock_tx) = TestFlusher::with_flush_control();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when: write, flush, then write more during blocked flush
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        flush_started_rx.await.unwrap(); // wait until flush is actually in progress

        // Writes during blocked flush get new epochs
        let w1 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let w2 = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        // then: epochs continue incrementing
        let e1 = w1.epoch().await.unwrap();
        let e2 = w2.epoch().await.unwrap();
        assert!(e1 < e2);

        // cleanup
        unblock_tx.send(()).await.unwrap();
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    // ============================================================================
    // Backpressure Tests
    // ============================================================================

    #[tokio::test]
    async fn should_return_backpressure_when_queue_full() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: usize::MAX,
        };
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            config,
            TestContext::default(),
            flusher,
        );
        // Don't start coordinator - queue will fill

        // when - fill the queue
        let _ = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;
        let _ = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await;

        // Third write should fail with backpressure
        let result = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await;

        // then
        assert!(matches!(result, Err(WriteError::Backpressure)));

        drop(handle);
        drop(coordinator);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_accept_writes_after_queue_drains() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_secs(3600),
            flush_size_threshold: usize::MAX,
        };
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            config,
            TestContext::default(),
            flusher,
        );

        // Fill queue without processing
        let _ = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;
        let mut write_b = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();

        // when - start coordinator to drain queue and wait for it to process writes
        let coordinator_task = tokio::spawn(coordinator.run());
        write_b.wait(Durability::Applied).await.unwrap();

        // then - writes should succeed now
        let result = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await;
        assert!(result.is_ok());

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    // ============================================================================
    // Shutdown Tests
    // ============================================================================

    #[tokio::test]
    async fn should_shutdown_cleanly_when_handles_dropped() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        drop(handle);

        // then - coordinator should return Ok
        let result = coordinator_task.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_flush_pending_writes_on_shutdown() {
        // given
        let flusher = TestFlusher::default();
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600), // Long interval - won't trigger
            flush_size_threshold: usize::MAX,          // High threshold - won't trigger
        };
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            config,
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when - write without explicit flush, then shutdown
        let write = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let epoch = write.epoch().await.unwrap();

        // Drop handle to trigger shutdown
        drop(handle);
        coordinator_task.await.unwrap().unwrap();

        // then - pending writes should have been flushed
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let (_, epoch_range) = &events[0];
        assert!(epoch_range.contains(&epoch));
    }

    #[tokio::test]
    async fn should_return_shutdown_error_after_coordinator_stops() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );

        // Stop coordinator by dropping it
        drop(coordinator);

        // when
        let result = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await;

        // then
        assert!(matches!(result, Err(WriteError::Shutdown)));
    }

    // ============================================================================
    // Epoch Range Tracking Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_track_epoch_range_in_flush_event() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let mut last_write = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();

        handle.flush().await.unwrap();
        last_write.wait(Durability::Flushed).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let (_, epoch_range) = &events[0];
        assert_eq!(epoch_range.start, 1);
        assert_eq!(epoch_range.end, 4); // exclusive: one past the last epoch (3)

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_have_contiguous_epoch_ranges() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when - first batch
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let mut write2 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write2.wait(Durability::Flushed).await.unwrap();

        // when - second batch
        let mut write3 = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write3.wait(Durability::Flushed).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 2);

        let (_, range1) = &events[0];
        let (_, range2) = &events[1];

        // Ranges should be contiguous (end of first == start of second)
        assert_eq!(range1.end, range2.start);
        assert_eq!(range1, &(1..3));
        assert_eq!(range2, &(3..4));

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_exact_epochs_in_range() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when - write and capture the assigned epochs
        let write1 = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let epoch1 = write1.epoch().await.unwrap();

        let mut write2 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        let epoch2 = write2.epoch().await.unwrap();

        handle.flush().await.unwrap();
        write2.wait(Durability::Flushed).await.unwrap();

        // then - the epoch_range should contain exactly the epochs assigned to writes
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 1);
        let (_, epoch_range) = &events[0];

        // The range should start at the first write's epoch
        assert_eq!(epoch_range.start, epoch1);
        // The range end should be one past the last write's epoch (exclusive)
        assert_eq!(epoch_range.end, epoch2 + 1);
        // Both epochs should be contained in the range
        assert!(epoch_range.contains(&epoch1));
        assert!(epoch_range.contains(&epoch2));

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    // ============================================================================
    // State Carryover (ID Allocation) Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_preserve_key_to_id_mapping_across_flushes() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when - write key "a" in first batch
        let mut write1 = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write1.wait(Durability::Flushed).await.unwrap();

        // Write to key "a" again in second batch
        let mut write2 = handle
            .write(TestWrite {
                key: "a".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write2.wait(Durability::Flushed).await.unwrap();

        // then
        let events = flusher.flushed_events();
        assert_eq!(events.len(), 2);

        let (delta1, _) = &events[0];
        let (delta2, _) = &events[1];

        // Same key should get the same ID across flushes
        let id1 = delta1.context.key_to_id.get("a").unwrap();
        let id2 = delta2.context.key_to_id.get("a").unwrap();
        assert_eq!(id1, id2);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_continue_id_sequence_across_flushes() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when - write keys in first batch
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let mut write2 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write2.wait(Durability::Flushed).await.unwrap();

        // New key in second batch
        let mut write3 = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write3.wait(Durability::Flushed).await.unwrap();

        // then
        let events = flusher.flushed_events();
        let (delta1, _) = &events[0];
        let (delta2, _) = &events[1];

        // First batch: a=0, b=1
        let ctx1 = &delta1.context;
        let id_a = ctx1.key_to_id.get("a").unwrap();
        let id_b = ctx1.key_to_id.get("b").unwrap();

        // Second batch: c should get ID 2 (continuing sequence)
        let ctx2 = &delta2.context;
        let id_c = ctx2.key_to_id.get("c").unwrap();

        // IDs should be unique and sequential
        assert_ne!(id_a, id_b);
        assert_ne!(id_b, id_c);
        assert_ne!(id_a, id_c);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_complete_mapping_in_flush_event() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when - write keys in first batch
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let mut write2 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write2.wait(Durability::Flushed).await.unwrap();

        // Add new key c in second batch
        let mut write3 = handle
            .write(TestWrite {
                key: "c".into(),
                value: 3,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        write3.wait(Durability::Flushed).await.unwrap();

        // then - second delta should contain mappings for a, b, c
        let events = flusher.flushed_events();
        let (delta2, _) = &events[1];
        let ctx2 = &delta2.context;

        // Delta should have inherited a and b from context, plus new c
        assert!(ctx2.key_to_id.contains_key("a"));
        assert!(ctx2.key_to_id.contains_key("b"));
        assert!(ctx2.key_to_id.contains_key("c"));

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    // ============================================================================
    // Subscribe Tests
    // ============================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_receive_flush_result_on_subscribe() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());
        let mut subscriber = handle.subscribe();

        // when
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();

        // then
        let result = subscriber.recv().await;
        assert!(result.is_ok());

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_snapshot_in_flush_result() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());
        let mut subscriber = handle.subscribe();

        // when
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        let result = subscriber.recv().await.unwrap();

        // then - snapshot should be the Arc<dyn StorageRead> returned by the flusher
        // We can verify it exists and is usable (InMemoryStorage in tests)
        assert!(Arc::strong_count(&result.snapshot) >= 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_delta_in_flush_result() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());
        let mut subscriber = handle.subscribe();

        // when
        handle
            .write(TestWrite {
                key: "a".into(),
                value: 42,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        let result = subscriber.recv().await.unwrap();

        // then - delta should contain the write we made
        let ctx = &result.delta.context;
        assert!(ctx.key_to_id.contains_key("a"));
        let id = ctx.key_to_id.get("a").unwrap();
        let values = result.delta.writes.get(id).unwrap();
        assert_eq!(values, &[42]);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_include_epoch_range_in_flush_result() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::new(
            test_config(),
            TestContext::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());
        let mut subscriber = handle.subscribe();

        // when
        let write1 = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        let write2 = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        let result = subscriber.recv().await.unwrap();

        // then - epoch range should contain the epochs of the writes
        let epoch1 = write1.epoch().await.unwrap();
        let epoch2 = write2.epoch().await.unwrap();
        assert!(result.epoch_range.contains(&epoch1));
        assert!(result.epoch_range.contains(&epoch2));
        assert_eq!(result.epoch_range.start, epoch1);
        assert_eq!(result.epoch_range.end, epoch2 + 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }
}

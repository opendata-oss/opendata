use super::{Delta, FlushEvent, Flusher, WriteCoordinatorHandle, WriteError, WriteResult};
use crate::coordinator::handle::EpochWatcher;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
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
    Flush,
}

/// The write coordinator manages write ordering, batching, and durability.
///
/// It accepts writes through `WriteCoordinatorHandle`, applies them to a `Delta`,
/// and coordinates flushing through a `Flusher`.
pub struct WriteCoordinator<D: Delta, F: Flusher<D>> {
    config: WriteCoordinatorConfig,
    image: D::Image,
    delta: D,
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
    /// Create a new write coordinator.
    ///
    /// Returns the coordinator and a handle for submitting writes.
    pub fn new(
        config: WriteCoordinatorConfig,
        initial_image: D::Image,
    ) -> (Self, WriteCoordinatorHandle<D>) {
        Self::with_flusher(config, initial_image, F::default())
    }

    /// Create a new write coordinator with the given flusher.
    ///
    /// This is useful for testing with mock flushers.
    pub fn with_flusher(
        config: WriteCoordinatorConfig,
        initial_image: D::Image,
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

        let watcher = EpochWatcher {
            applied_rx,
            flushed_rx,
            durable_rx,
        };

        let flusher = Arc::new(flusher);
        let delta = D::default();

        let flush_task = FlushTask {
            flusher,
            flush_rx,
            flushed_tx,
        };
        // Start the interval in the future to avoid immediate first tick
        let flush_interval = interval_at(
            Instant::now() + config.flush_interval,
            config.flush_interval,
        );
        let coordinator = Self {
            config,
            image: initial_image,
            delta,
            cmd_rx,
            flush_tx,
            flush_task: Some(flush_task),
            applied_tx,
            durable_tx,
            epoch: 0,
            last_flush_epoch: 0,
            flush_interval,
        };

        (coordinator, WriteCoordinatorHandle::new(cmd_tx, watcher))
    }

    /// Run the coordinator event loop.
    pub async fn run(mut self) -> Result<(), String> {
        self.delta.init(&self.image);

        // Start the flush task
        let flush_task = self
            .flush_task
            .take()
            .expect("flush_task should be Some at start of run");
        let _flush_task_handle = flush_task.run();

        loop {
            tokio::select! {
                biased;

                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(WriteCommand::Write {write, epoch: epoch_tx}) => {
                            self.handle_write(write, epoch_tx).await?;
                        }
                        Some(WriteCommand::Flush) => {
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

        Ok(())
    }

    async fn handle_write(
        &mut self,
        write: D::Write,
        epoch_tx: oneshot::Sender<u64>,
    ) -> Result<(), String> {
        self.epoch += 1;
        // Ignore error if receiver was dropped (fire-and-forget write)
        let _ = epoch_tx.send(self.epoch);

        self.delta.apply(write)?;
        self.applied_tx
            .send(self.epoch)
            .map_err(|_| "failed to send epoch")?;

        if self.delta.estimate_size() >= self.config.flush_size_threshold {
            self.handle_flush().await;
        }

        Ok(())
    }

    async fn handle_flush(&mut self) {
        if self.epoch <= self.last_flush_epoch {
            return;
        }

        let epoch_range = self.last_flush_epoch..self.epoch;
        self.last_flush_epoch = self.epoch;
        self.flush_interval.reset();

        // this is the blocking section of the flush, new writes will not be accepted
        // until the event is sent to the FlushTask
        let delta = self.delta.clone();
        let image = std::mem::replace(&mut self.image, self.delta.fork_image());

        self.delta = D::default();
        self.delta.init(&self.image);

        // Block until flush task can accept the event
        // (This provides backpressure: if flushing is slow, writes pause)
        let _ = self
            .flush_tx
            .send(FlushEvent {
                snapshot: image,
                delta,
                epoch_range,
            })
            .await;
    }
}

struct FlushTask<D: Delta, F: Flusher<D>> {
    flusher: Arc<F>,
    flush_rx: mpsc::Receiver<FlushEvent<D>>,
    flushed_tx: watch::Sender<u64>,
}

impl<D: Delta, F: Flusher<D>> FlushTask<D, F> {
    fn run(mut self) -> tokio::task::JoinHandle<WriteResult<()>> {
        tokio::spawn(async move {
            while let Some(event) = self.flush_rx.recv().await {
                let flushed_epoch = event.epoch_range.end;

                self.flusher
                    .flush(event)
                    .await
                    .map_err(|e| WriteError::FlushError(e.to_string()))?;
                self.flushed_tx
                    .send(flushed_epoch)
                    .map_err(|_| WriteError::Shutdown)?;
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::Durability;
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

    /// Image carries state that must persist across deltas (like series dictionary)
    #[derive(Clone, Debug, Default)]
    struct TestImage {
        key_to_id: HashMap<String, u64>,
        next_id: u64,
    }

    /// Delta accumulates writes and can allocate new IDs for unknown keys
    #[derive(Clone, Debug, Default)]
    struct TestDelta {
        key_to_id: HashMap<String, u64>,
        next_id: u64,
        writes: HashMap<u64, Vec<u64>>,
        total_size: usize,
    }

    impl Delta for TestDelta {
        type Image = TestImage;
        type Write = TestWrite;

        fn init(&mut self, image: &Self::Image) -> Self {
            self.key_to_id = image.key_to_id.clone();
            self.next_id = image.next_id;
            self.writes = HashMap::new();
            self.total_size = 0;
            self.clone()
        }

        fn apply(&mut self, write: Self::Write) -> Result<(), String> {
            let id = *self.key_to_id.entry(write.key).or_insert_with(|| {
                let id = self.next_id;
                self.next_id += 1;
                id
            });

            self.writes.entry(id).or_default().push(write.value);
            self.total_size += write.size;
            Ok(())
        }

        fn estimate_size(&self) -> usize {
            self.total_size
        }

        fn fork_image(&self) -> Self::Image {
            TestImage {
                key_to_id: self.key_to_id.clone(),
                next_id: self.next_id,
            }
        }
    }

    /// Shared state for TestFlusher - allows test to inspect and control behavior
    #[derive(Default)]
    struct TestFlusherState {
        flushed_events: Vec<(TestDelta, Range<u64>)>,
        unblock_rx: Option<mpsc::Receiver<()>>,
    }

    #[derive(Clone, Default)]
    struct TestFlusher {
        state: Arc<Mutex<TestFlusherState>>,
    }

    impl TestFlusher {
        fn with_blocker() -> (Self, mpsc::Sender<()>) {
            let (tx, rx) = mpsc::channel(1);
            let flusher = Self {
                state: Arc::new(Mutex::new(TestFlusherState {
                    flushed_events: Vec::new(),
                    unblock_rx: Some(rx),
                })),
            };
            (flusher, tx)
        }

        fn flushed_events(&self) -> Vec<(TestDelta, Range<u64>)> {
            self.state.lock().unwrap().flushed_events.clone()
        }
    }

    #[async_trait]
    impl Flusher<TestDelta> for TestFlusher {
        async fn flush(&self, event: FlushEvent<TestDelta>) -> Result<Arc<TestImage>, String> {
            // Block if test wants to control timing
            let unblock_rx = {
                let mut state = self.state.lock().unwrap();
                state.unblock_rx.take()
            };
            if let Some(mut rx) = unblock_rx {
                rx.recv().await;
            }

            // Record the flush
            let image = event.delta.fork_image();
            {
                let mut state = self.state.lock().unwrap();
                state.flushed_events.push((event.delta, event.epoch_range));
            }
            Ok(Arc::new(image))
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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        let id = delta.key_to_id.get("a").unwrap();
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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
    async fn should_include_all_pending_writes_in_flush() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        assert_eq!(delta.key_to_id.len(), 3);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_skip_flush_when_no_new_writes() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        // Give time for second flush to process (it's a no-op)
        tokio::task::yield_now().await;

        // then - only one flush should have occurred
        assert_eq!(flusher.flushed_events().len(), 1);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_update_flushed_watermark_after_flush() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
    // Note: Timer-based tests require tokio's time::pause() which only works with
    // the current_thread runtime, but we need multi_thread for proper async
    // coordination. These tests are skipped for now and would need to use a
    // different approach (e.g., injecting a mock clock) to be testable.

    // TODO: Add timer-based flush tests with injectable clock

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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            config,
            TestImage::default(),
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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            config,
            TestImage::default(),
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
        let (flusher, unblock_tx) = TestFlusher::with_blocker();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
            flusher.clone(),
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when: trigger a flush and block it
        let write1 = handle
            .write(TestWrite {
                key: "a".into(),
                value: 1,
                size: 10,
            })
            .await
            .unwrap();
        handle.flush().await.unwrap();
        tokio::task::yield_now().await;

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
        let (flusher, unblock_tx) = TestFlusher::with_blocker();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        tokio::task::yield_now().await;

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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            config,
            TestImage::default(),
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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            config,
            TestImage::default(),
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
        let _ = handle
            .write(TestWrite {
                key: "b".into(),
                value: 2,
                size: 10,
            })
            .await;

        // when - start coordinator to drain queue
        let coordinator_task = tokio::spawn(coordinator.run());
        // Give time for queue to drain
        tokio::time::sleep(Duration::from_millis(10)).await;

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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
            flusher,
        );
        let coordinator_task = tokio::spawn(coordinator.run());

        // when
        drop(handle);

        // then - coordinator should return Ok
        let result = coordinator_task.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_return_shutdown_error_after_coordinator_stops() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        assert_eq!(epoch_range.start, 0);
        assert_eq!(epoch_range.end, 3);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_have_contiguous_epoch_ranges() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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

        // Ranges should be contiguous
        assert_eq!(range1.end, range2.start);
        assert_eq!(range1, &(0..2));
        assert_eq!(range2, &(2..3));

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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        let id1 = delta1.key_to_id.get("a").unwrap();
        let id2 = delta2.key_to_id.get("a").unwrap();
        assert_eq!(id1, id2);

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_continue_id_sequence_across_flushes() {
        // given
        let flusher = TestFlusher::default();
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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
        let id_a = delta1.key_to_id.get("a").unwrap();
        let id_b = delta1.key_to_id.get("b").unwrap();

        // Second batch: c should get ID 2 (continuing sequence)
        let id_c = delta2.key_to_id.get("c").unwrap();

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
        let (coordinator, handle) = WriteCoordinator::<TestDelta, TestFlusher>::with_flusher(
            test_config(),
            TestImage::default(),
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

        // Delta should have inherited a and b from image, plus new c
        assert!(delta2.key_to_id.contains_key("a"));
        assert!(delta2.key_to_id.contains_key("b"));
        assert!(delta2.key_to_id.contains_key("c"));

        // cleanup
        drop(handle);
        coordinator_task.await.unwrap().unwrap();
    }
}

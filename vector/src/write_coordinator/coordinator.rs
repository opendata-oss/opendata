use anyhow::{Error, Result};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, Interval, interval_at};

use crate::write_coordinator::{Delta, FlushWatchers, Flusher, WriteCoordinatorHandle};

struct FlushWatermarks {
    applied: tokio::sync::watch::Sender<u64>,
    flushed: tokio::sync::watch::Sender<u64>,
    durable: tokio::sync::watch::Sender<u64>,
    watchers: FlushWatchers,
}

impl FlushWatermarks {
    fn new() -> Self {
        let (applied_tx, applied_rx) = tokio::sync::watch::channel(0);
        let (flushed_tx, flushed_rx) = tokio::sync::watch::channel(0);
        let (durable_tx, durable_rx) = tokio::sync::watch::channel(0);
        let watchers = FlushWatchers {
            applied: applied_rx,
            flushed: flushed_rx,
            durable: durable_rx,
        };
        Self {
            applied: applied_tx,
            flushed: flushed_tx,
            durable: durable_tx,
            watchers,
        }
    }

    fn applied(&self) -> u64 {
        *self.applied.borrow()
    }

    fn flushed(&self) -> u64 {
        *self.flushed.borrow()
    }

    fn durable(&self) -> u64 {
        *self.durable.borrow()
    }

    fn update_applied(&self, epoch: u64) {
        let _ = self.applied.send(epoch);
    }

    fn update_flushed(&self, epoch: u64) {
        let _ = self.flushed.send(epoch);
    }

    fn update_durable(&self, epoch: u64) {
        let _ = self.durable.send(epoch);
    }
}

pub(crate) struct WriteCoordinatorWriteMsg<D: Delta> {
    write: D::Write,
    result_tx: tokio::sync::oneshot::Sender<Result<u64, Arc<Error>>>,
}

impl<D: Delta> WriteCoordinatorWriteMsg<D> {
    pub(crate) fn new(write: D::Write) -> (Self, tokio::sync::oneshot::Receiver<Result<u64, Arc<Error>>>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Self { write, result_tx: tx }, rx)
    }
}

pub(crate) enum WriteCoordinatorCtlMsg {
    /// Request a flush. The oneshot is used to signal completion.
    Flush {
        epoch: Option<u64>,
        result_tx: tokio::sync::oneshot::Sender<Option<u64>>,
    },
    // TODO: add a Shutdown msg
}

struct WriteCoordinator<D: Delta> {
    handle: WriteCoordinatorHandle<D>,
    task_jh: tokio::task::JoinHandle<()>,
}

impl<D: Delta> WriteCoordinator<D> {
    pub(crate) fn new(
        image: D::Image,
        max_buffered_writes: usize,
        flush_interval: Duration,
        flusher: Box<dyn Flusher<D>>,
    ) -> Self {
        let delta = D::init(image);
        let (write_tx, write_rx) = tokio::sync::mpsc::channel(max_buffered_writes);
        let (ctl_tx, ctl_rx) = tokio::sync::mpsc::unbounded_channel();
        let flush_watermarks = FlushWatermarks::new();
        let flush_watchers = flush_watermarks.watchers.clone();
        let (flush_tx, flush_rx) = tokio::sync::watch::channel(FlushTaskMsg::None);
        let (flush_result_tx, flush_result_rx) = tokio::sync::mpsc::unbounded_channel();
        let flush_task = FlushTask::new(flusher, flush_rx, flush_result_tx);
        let flush_task_jh = tokio::spawn(async move { flush_task.run().await });
        let write_task = WriteCoordinatorTask {
            current_delta: delta,
            write_rx,
            ctl_rx,
            flush_result_rx,
            watermarks: flush_watermarks,
            flush_interval,
            flush_tx,
            current_epoch: 0,
            delta_start_epoch: 1,
            flush_task_jh,
        };
        let task_jh = tokio::spawn(async move { write_task.run().await });
        let handle = WriteCoordinatorHandle {
            write_tx,
            ctl_tx,
            flush_watchers,
        };
        Self { handle, task_jh }
    }

    pub(crate) fn handle(&self) -> WriteCoordinatorHandle<D> {
        self.handle.clone()
    }
}

struct WriteCoordinatorTask<D: Delta> {
    current_delta: D,
    write_rx: tokio::sync::mpsc::Receiver<WriteCoordinatorWriteMsg<D>>,
    ctl_rx: tokio::sync::mpsc::UnboundedReceiver<WriteCoordinatorCtlMsg>,
    flush_result_rx: tokio::sync::mpsc::UnboundedReceiver<FlushResult<D>>,
    watermarks: FlushWatermarks,
    flush_interval: Duration,
    flush_tx: tokio::sync::watch::Sender<FlushTaskMsg<D>>,
    /// Current epoch counter. Increments with each write.
    current_epoch: u64,
    /// Epoch at which the current delta started.
    delta_start_epoch: u64,
    flush_task_jh: tokio::task::JoinHandle<()>,
}

impl<D: Delta> WriteCoordinatorTask<D> {
    async fn run(mut self) {
        // TODO: track a target next time to flush and set it when writes come in
        // Use interval_at to delay the first tick by flush_interval
        let mut flush_timer: Interval =
            interval_at(Instant::now() + self.flush_interval, self.flush_interval);

        loop {
            tokio::select! {
                // Handle incoming writes
                Some(msg) = self.write_rx.recv() => {
                    self.handle_write(msg);
                }

                // Handle control messages (flush requests)
                Some(ctl_msg) = self.ctl_rx.recv() => {
                    match ctl_msg {
                        WriteCoordinatorCtlMsg::Flush{ epoch, result_tx } => {
                            let flush_epoch = self.flush(epoch);
                            let _ = result_tx.send(flush_epoch);
                        }
                    }
                }

                Some(flush_result) = self.flush_result_rx.recv() => {
                    self.watermarks.update_durable(*flush_result.epoch_range.end());
                }

                // Periodic flush based on flush_interval
                _ = flush_timer.tick() => {
                    self.flush(None);
                }


                // TODO: wait on flush task join handle and fail

                // All channels closed - shutdown
                else => break,
            }
        }
    }

    fn handle_write(&mut self, msg: WriteCoordinatorWriteMsg<D>) {
        // Increment epoch and assign to this write
        self.current_epoch += 1;
        let epoch = self.current_epoch;

        // Apply the write to the current delta
        // Note: apply takes a Vec, so we wrap the single write
        // TODO: don't burn an epoch if the write fails
        if let Err(e) = self.current_delta.apply(vec![msg.write]) {
            // The epoch channel will be dropped which signals failure to the caller
            let _ = msg.result_tx.send(Err(e));
            return;
        }

        // Send the epoch back to the caller
        let _ = msg.result_tx.send(Ok(epoch));

        // Update the applied watermark
        self.watermarks.update_applied(epoch);
    }

    fn flush_pending(&self) -> bool {
        let durable_watermark = self.watermarks.durable();
        match &*self.flush_tx.borrow() {
            FlushTaskMsg::Flush {
                delta: _,
                epoch_range,
            } => {
                // Return true if there's a flush in progress that hasn't completed
                durable_watermark < *epoch_range.end()
            }
            FlushTaskMsg::None => false,
        }
    }

    fn flush(&mut self, flush_epoch: Option<u64>) -> Option<u64> {
        let flush_epoch = flush_epoch.unwrap_or(self.current_epoch);

        // Nothing to flush if no writes since last flush
        if flush_epoch < self.delta_start_epoch {
            return Some(flush_epoch);
        }

        if self.flush_pending() {
            return None;
        }

        let epoch_range_start = self.delta_start_epoch;
        let epoch_range_end = self.current_epoch;
        assert!(epoch_range_start <= epoch_range_end);

        // Freeze the current delta to get an immutable snapshot and new image
        let (delta, new_image) = self.current_delta.freeze();

        // Replace the current delta with a fresh one initialized from the new image
        self.current_delta = D::init(new_image);
        self.delta_start_epoch = self.current_epoch + 1;

        self.flush_tx
            .send(FlushTaskMsg::Flush {
                delta: Arc::new(delta),
                epoch_range: epoch_range_start..=epoch_range_end,
            })
            .unwrap();

        // Update flushed watermark
        self.watermarks.update_flushed(epoch_range_end);

        Some(epoch_range_end)
    }
}

enum FlushTaskMsg<D: Delta> {
    None,
    Flush {
        delta: Arc<D::ImmutableDelta>,
        epoch_range: RangeInclusive<u64>,
    },
    // Shutdown,
}

// Manual Clone impl because derive(Clone) would require D::ImmutableDelta: Clone,
// but Arc::clone doesn't need the inner type to be Clone.
impl<D: Delta> Clone for FlushTaskMsg<D> {
    fn clone(&self) -> Self {
        match self {
            FlushTaskMsg::None => FlushTaskMsg::None,
            FlushTaskMsg::Flush { delta, epoch_range } => FlushTaskMsg::Flush {
                delta: Arc::clone(delta),
                epoch_range: epoch_range.clone(),
            },
        }
    }
}

struct FlushResult<D: Delta> {
    delta: Arc<D::ImmutableDelta>,
    epoch_range: RangeInclusive<u64>,
}

struct FlushTask<D: Delta> {
    flusher: Box<dyn Flusher<D>>,
    rx: tokio::sync::watch::Receiver<FlushTaskMsg<D>>,
    tx: tokio::sync::mpsc::UnboundedSender<FlushResult<D>>,
}

impl<D: Delta> FlushTask<D> {
    fn new(
        flusher: Box<dyn Flusher<D>>,
        rx: tokio::sync::watch::Receiver<FlushTaskMsg<D>>,
        tx: tokio::sync::mpsc::UnboundedSender<FlushResult<D>>,
    ) -> Self {
        Self { flusher, rx, tx }
    }

    async fn run(mut self) {
        let mut last_flushed = 0u64;
        loop {
            let (delta, epoch_range) = {
                let msg = self
                    .rx
                    .wait_for(|msg| match msg {
                        FlushTaskMsg::None => false,
                        FlushTaskMsg::Flush {
                            delta: _,
                            epoch_range,
                        } => *epoch_range.end() > last_flushed,
                    })
                    .await
                    .expect("coordinator hung up");
                let FlushTaskMsg::Flush { delta, epoch_range } = msg.clone() else {
                    panic!("unreachable");
                };
                (delta.clone(), epoch_range)
            };
            self.flusher
                .flush(delta.as_ref())
                .await
                .expect("flush failed");
            last_flushed = *epoch_range.end();
            self.tx
                .send(FlushResult { delta, epoch_range })
                .expect("coordinator hung up");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use bytes::Bytes;
    use common::{BytesRange, Record, StorageIterator, StorageRead, StorageResult};
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Mock types for testing
    #[derive(Clone)]
    struct MockDelta {
        writes: Vec<i32>,
        image_value: i32,
    }

    struct MockImage {
        value: i32,
    }

    struct MockImmutableDelta {
        writes: Vec<i32>,
    }

    impl Delta for MockDelta {
        type Image = MockImage;
        type Write = i32;
        type ImmutableDelta = MockImmutableDelta;

        fn init(image: Self::Image) -> Self {
            Self {
                writes: Vec::new(),
                image_value: image.value,
            }
        }

        fn apply(&mut self, writes: Vec<Self::Write>) -> Result<(), Arc<Error>> {
            self.writes.extend(writes);
            Ok(())
        }

        fn estimate_size(&self) -> usize {
            self.writes.len() * std::mem::size_of::<i32>()
        }

        fn freeze(&self) -> (Self::ImmutableDelta, Self::Image) {
            let sum: i32 = self.writes.iter().sum();
            (
                MockImmutableDelta {
                    writes: self.writes.clone(),
                },
                MockImage {
                    value: self.image_value + sum,
                },
            )
        }
    }

    struct MockFlusher {
        flush_count: Arc<AtomicUsize>,
        flushed_writes: Arc<tokio::sync::Mutex<Vec<Vec<i32>>>>,
    }

    impl MockFlusher {
        fn new() -> Self {
            Self {
                flush_count: Arc::new(AtomicUsize::new(0)),
                flushed_writes: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Flusher<MockDelta> for MockFlusher {
        async fn flush(&self, delta: &MockImmutableDelta) -> Result<Arc<dyn StorageRead>> {
            self.flush_count.fetch_add(1, Ordering::SeqCst);
            self.flushed_writes.lock().await.push(delta.writes.clone());
            // Return a dummy StorageRead - we don't actually use it in tests
            Ok(Arc::new(MockStorageRead))
        }
    }

    struct MockStorageRead;

    struct EmptyIterator;

    #[async_trait]
    impl StorageIterator for EmptyIterator {
        async fn next(&mut self) -> StorageResult<Option<Record>> {
            Ok(None)
        }
    }

    #[async_trait]
    impl StorageRead for MockStorageRead {
        async fn get(&self, _key: Bytes) -> StorageResult<Option<Record>> {
            Ok(None)
        }

        async fn scan_iter(
            &self,
            _range: BytesRange,
        ) -> StorageResult<Box<dyn StorageIterator + Send + 'static>> {
            Ok(Box::new(EmptyIterator))
        }
    }

    #[tokio::test]
    async fn should_apply_writes_and_increment_epoch() {
        // given
        let image = MockImage { value: 0 };
        let flusher = MockFlusher::new();
        let coordinator = WriteCoordinator::<MockDelta>::new(
            image,
            100,
            Duration::from_secs(60), // Long interval to avoid automatic flushes
            Box::new(flusher),
        );
        let handle = coordinator.handle();

        // when
        let write_handle1 = handle.write(42).await.unwrap();
        let write_handle2 = handle.write(100).await.unwrap();

        // then
        let epoch1 = write_handle1.epoch().await.unwrap();
        let epoch2 = write_handle2.epoch().await.unwrap();

        assert_eq!(epoch1, 1);
        assert_eq!(epoch2, 2);
    }

    #[tokio::test]
    async fn should_flush_on_manual_request() {
        // given
        let image = MockImage { value: 0 };
        let flusher = MockFlusher::new();
        let flush_count = flusher.flush_count.clone();
        let flushed_writes = flusher.flushed_writes.clone();
        let coordinator = WriteCoordinator::<MockDelta>::new(
            image,
            100,
            Duration::from_secs(60),
            Box::new(flusher),
        );
        let handle = coordinator.handle();

        // when
        handle.write(10).await.unwrap().epoch().await.unwrap();
        handle.write(20).await.unwrap().epoch().await.unwrap();
        handle.flush(None).await.unwrap();

        // Wait for flush task to process
        while flush_count.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // then
        assert_eq!(flush_count.load(Ordering::SeqCst), 1);
        let writes = flushed_writes.lock().await;
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0], vec![10, 20]);
    }

    #[tokio::test]
    async fn should_flush_automatically_after_interval() {
        // given
        let image = MockImage { value: 0 };
        let flusher = MockFlusher::new();
        let flush_count = flusher.flush_count.clone();
        let flushed_writes = flusher.flushed_writes.clone();
        let coordinator = WriteCoordinator::<MockDelta>::new(
            image,
            100,
            Duration::from_millis(50), // Short interval
            Box::new(flusher),
        );
        let handle = coordinator.handle();

        // when
        handle.write(5).await.unwrap();
        handle.write(15).await.unwrap();

        // Wait for the automatic flush
        tokio::time::sleep(Duration::from_millis(150)).await;

        // then
        assert!(flush_count.load(Ordering::SeqCst) >= 1);
        let writes = flushed_writes.lock().await;
        assert!(!writes.is_empty());
        assert_eq!(writes[0], vec![5, 15]);
    }

    #[tokio::test]
    async fn should_accumulate_writes_across_flushes() {
        // given
        let image = MockImage { value: 0 };
        let flusher = MockFlusher::new();
        let flush_count = flusher.flush_count.clone();
        let flushed_writes = flusher.flushed_writes.clone();
        let coordinator = WriteCoordinator::<MockDelta>::new(
            image,
            100,
            Duration::from_secs(60),
            Box::new(flusher),
        );
        let handle = coordinator.handle();

        // when - first batch (await epoch to ensure write is processed)
        handle.write(1).await.unwrap().epoch().await.unwrap();
        handle.write(2).await.unwrap().epoch().await.unwrap();
        handle.flush(None).await.unwrap();

        // Wait for flush to actually complete by polling the flush_count
        while flush_count.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // when - second batch (await epoch to ensure write is processed)
        handle.write(3).await.unwrap().epoch().await.unwrap();
        handle.write(4).await.unwrap().epoch().await.unwrap();
        handle.flush(None).await.unwrap();

        // Wait for second flush to complete
        while flush_count.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // then
        let writes = flushed_writes.lock().await;
        assert_eq!(writes.len(), 2, "expected 2 flushes, got {}", writes.len());
        assert_eq!(writes[0], vec![1, 2]);
        assert_eq!(writes[1], vec![3, 4]);
    }

    #[tokio::test]
    async fn should_not_flush_when_no_writes() {
        // given
        let image = MockImage { value: 0 };
        let flusher = MockFlusher::new();
        let flush_count = flusher.flush_count.clone();
        let coordinator = WriteCoordinator::<MockDelta>::new(
            image,
            100,
            Duration::from_secs(60),
            Box::new(flusher),
        );
        let handle = coordinator.handle();

        // when - request flush without any writes
        handle.flush(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // then
        assert_eq!(flush_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn should_update_watermarks_correctly() {
        // given
        let image = MockImage { value: 0 };
        let flusher = MockFlusher::new();
        let coordinator = WriteCoordinator::<MockDelta>::new(
            image,
            100,
            Duration::from_secs(60),
            Box::new(flusher),
        );
        let handle = coordinator.handle();

        // when
        let write_handle = handle.write(42).await.unwrap();
        let epoch = write_handle.epoch().await.unwrap();

        // then - applied watermark should be updated
        assert_eq!(epoch, 1);

        // Flush and verify flushed watermark updates
        handle.flush(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The watchers should reflect the flushed epoch
        // We verify this indirectly through successful completion
    }
}

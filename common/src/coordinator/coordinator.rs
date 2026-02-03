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

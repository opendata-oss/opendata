use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch::Ref;
use tokio::time::{Interval, interval};

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
    epoch: tokio::sync::oneshot::Sender<u64>,
}

impl<D: Delta> WriteCoordinatorWriteMsg<D> {
    pub(crate) fn new(write: D::Write) -> (Self, tokio::sync::oneshot::Receiver<u64>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Self { write, epoch: tx }, rx)
    }
}

pub(crate) enum WriteCoordinatorCtlMsg {
    /// Request a flush. The oneshot is used to signal completion.
    Flush(tokio::sync::oneshot::Sender<()>),
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
        let flush_task = FlushTask::new(
            flusher,
            flush_rx,
            flush_result_tx,
        );
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
            delta_start_epoch: 0,
            flush_task_jh,
        };
        let task_jh = tokio::spawn(async move { write_task.run().await });
        let handle = WriteCoordinatorHandle {
            write_tx,
            ctl_tx,
            flush_watchers,
        };
        Self { handle, task_jh, }
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

impl <D: Delta> WriteCoordinatorTask<D> {
    async fn run(mut self) {
        // TODO: track a target next time to flush and set it when writes come in
        let mut flush_timer: Interval = interval(self.flush_interval);

        loop {
            tokio::select! {
                // Handle incoming writes
                Some(msg) = self.write_rx.recv() => {
                    self.handle_write(msg);
                }

                // Handle control messages (flush requests)
                Some(ctl_msg) = self.ctl_rx.recv() => {
                    match ctl_msg {
                        WriteCoordinatorCtlMsg::Flush(completion) => {
                            self.flush();
                            let _ = completion.send(());
                        }
                    }
                }

                Some(flush_result) = self.flush_result_rx.recv() => {
                    self.watermarks.update_durable(*flush_result.epoch_range.end());
                }

                // Periodic flush based on flush_interval
                _ = flush_timer.tick() => {
                    self.flush();
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
        if let Err(_e) = self.current_delta.apply(vec![msg.write]) {
            // The epoch channel will be dropped which signals failure to the caller
            return;
        }

        // Send the epoch back to the caller
        let _ = msg.epoch.send(epoch);

        // Update the applied watermark
        self.watermarks.update_applied(epoch);
    }

    fn flush_pending(&self) -> bool {
        let durable_watermark = self.watermarks.durable();
        match &*self.flush_tx.borrow() {
            FlushTaskMsg::Flush {delta, epoch_range} => {
                durable_watermark >= *epoch_range.end()
            },
            FlushTaskMsg::None => false,
        }
    }

    fn flush(&mut self) {
        // Nothing to flush if no writes since last flush
        if self.current_epoch == self.delta_start_epoch {
            return;
        }

        if self.flush_pending() {
            return;
        }

        let epoch_range_start = self.delta_start_epoch;
        let epoch_range_end = self.current_epoch;

        // Freeze the current delta to get an immutable snapshot and new image
        let (delta, new_image) = self.current_delta.freeze();

        // Replace the current delta with a fresh one initialized from the new image
        self.current_delta = D::init(new_image);
        self.delta_start_epoch = epoch_range_end;

        self.flush_tx.send(
            FlushTaskMsg::Flush {
                delta: Arc::new(delta),
                epoch_range: epoch_range_start..=epoch_range_end,
            }
        ).unwrap();

        // Update flushed watermark
        self.watermarks.update_flushed(epoch_range_end);
    }
}

#[derive(Clone)]
enum FlushTaskMsg<D: Delta> {
    None,
    Flush{
        delta: Arc<D::ImmutableDelta>,
        epoch_range: RangeInclusive<u64>,
    },
    // Shutdown,
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

impl <D: Delta> FlushTask<D> {
    fn new(
        flusher: Box<dyn Flusher<D>>,
        rx: tokio::sync::watch::Receiver<FlushTaskMsg<D>>,
        tx: tokio::sync::mpsc::UnboundedSender<FlushResult<D>>,
    ) -> Self {
        Self { flusher, rx, tx }
    }

    async fn run(mut self) {
        let last_flushed = 0u64;
        loop {
            let msg = self.rx.wait_for(|msg| {
                match msg {
                    FlushTaskMsg::None => false,
                    FlushTaskMsg::Flush { delta, epoch_range } => {
                        return *epoch_range.end() > last_flushed;
                    }
                }
            }).await.expect("coordinator hung up");
            let FlushTaskMsg::Flush { delta, epoch_range } = msg.clone() else {
                panic!("unreachable");
            };
            drop(msg);
            let delta = delta.clone();
            self.flusher.flush(delta.as_ref()).await.expect("flush failed");
            self.tx.send(FlushResult {
                delta,
                epoch_range,
            }).expect("coordinator hung up");
        }
    }
}
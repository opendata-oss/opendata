use std::time::Duration;
use crate::write_coordinator::{Delta, FlushEvent, FlushWatchers, Flusher, WriteCoordinatorHandle};

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
            watchers
        }
    }
}

pub (crate) struct WriteCoordinatorWriteMsg<D: Delta> {
    write: D::Write,
    epoch: tokio::sync::oneshot::Sender<u64>,
}

impl <D: Delta> WriteCoordinatorWriteMsg<D> {
    pub(crate) fn new(write: D::Write) -> (Self, tokio::sync::oneshot::Receiver<u64>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Self { write, epoch: tx }, rx)
    }
}

pub(crate) enum WriteCoordinatorCtlMsg<D: Delta> {
    Flush(FlushEvent<D>)
}

struct WriteCoordinator<D: Delta> {
    current_delta: D,
    write_rx: tokio::sync::mpsc::Receiver<WriteCoordinatorWriteMsg<D>>,
    ctl_rx: tokio::sync::mpsc::UnboundedReceiver<WriteCoordinatorCtlMsg<D>>,
    flusher: Box<dyn Flusher<D>>,
    flush_watermarks: FlushWatermarks,
    flush_interval: Duration,
}

impl <D: Delta> WriteCoordinator<D> {
    fn new(
        image: D::Image,
        max_buffered_writes: usize,
        flush_interval: Duration,
        flusher: Box<dyn Flusher<D>>,
    ) -> (Self, WriteCoordinatorHandle<D>) {
        let delta = D::init(image);
        let (write_tx, write_rx) = tokio::sync::mpsc::channel(max_buffered_writes);
        let (ctl_tx, ctl_rx) = tokio::sync::mpsc::unbounded_channel();
        let flush_watermarks = FlushWatermarks::new();
        let flush_watchers = flush_watermarks.watchers.clone();
        (
            Self {
                current_delta: delta,
                write_rx,
                ctl_rx,
                flush_watermarks,
                flush_interval,
                flusher
            },
            WriteCoordinatorHandle{ write_tx, ctl_tx, flush_watchers }
        )
    }

    async fn run(&mut self) {
        // todo: run main select! loop that listens on write_rx, flush_rx, and a timer on flush_interval
        // whenever a write msg is received, pass it to the delta. whenever a flush needs to happen,
        // call Delta#freeze on the current delta to consume it and replace it with a new Delta that uses
        // the image returned by freeze, and then calls flusher.flush
    }
}
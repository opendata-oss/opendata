use super::Delta;
use tokio::sync::oneshot;

pub(crate) enum WriteCommand<D: Delta> {
    Write {
        write: D::Write,
        epoch: oneshot::Sender<u64>,
    },
    Flush,
}

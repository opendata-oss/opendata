use futures::future::BoxFuture;
use futures::stream::{self, StreamExt};

/// Maximum number of concurrent I/O futures to poll at once.
const DEFAULT_CONCURRENCY: usize = 256;

/// Driver for i/o heavy batches of tasks
pub(crate) struct AsyncBatchDriver {}

impl AsyncBatchDriver {
    pub(crate) async fn execute<T: Send + 'static>(batch: Vec<BoxFuture<'static, T>>) -> Vec<T> {
        stream::iter(batch)
            .buffer_unordered(DEFAULT_CONCURRENCY)
            .collect()
            .await
    }
}

use futures::future::BoxFuture;
use tokio::task::JoinSet;

/// Maximum number of concurrent I/O futures to poll at once.
const DEFAULT_CONCURRENCY: usize = 256;

/// Driver for i/o heavy batches of tasks
pub(crate) struct AsyncBatchDriver {}

impl AsyncBatchDriver {
    pub(crate) async fn execute<T: Send + 'static>(batch: Vec<BoxFuture<'static, T>>) -> Vec<T> {
        let mut results = Vec::with_capacity(batch.len());
        let mut pending = batch.into_iter();
        let mut tasks = JoinSet::new();

        for fut in pending.by_ref().take(DEFAULT_CONCURRENCY) {
            tasks.spawn(fut);
        }

        while let Some(result) = tasks.join_next().await {
            results.push(result.expect("async batch task failed"));
            if let Some(fut) = pending.next() {
                tasks.spawn(fut);
            }
        }

        results
    }
}

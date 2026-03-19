use futures::future::{join_all, BoxFuture};
use tokio::task::JoinSet;

/// Driver for i/o heavy batches of tasks
pub(crate) struct AsyncBatchDriver {
}

impl AsyncBatchDriver {
    pub(crate) async fn execute<T: Send + 'static>(batch: Vec<BoxFuture<'static, T>>) -> Vec<T> {
        // TODO: do something better here that limits concurrent tasks/ios
        //join_all(batch.into_iter()).await
        let mut s = JoinSet::new();
        for b in batch {
            s.spawn(b);
        }
        s.join_all().await
    }
}

/// Driver for compute-heavy batches of tasks
pub(crate) struct ComputeBatchDriver {
}

impl ComputeBatchDriver {
}
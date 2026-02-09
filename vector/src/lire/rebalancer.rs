#![allow(unused)]

use crate::delta::VectorDbWriteDelta;
use crate::hnsw::CentroidGraph;
use common::coordinator::WriteCoordinatorHandle;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Specifies individual rebalance operations. Sent by [`VectorDbWriteDelta`] to [`IndexRebalancer`]
pub(crate) enum IndexRebalanceOp {
    /// Split a centroid into 2 new centroids
    ExecuteSplit { centroid: u32 },
    /// Merge a centroid into a neighbouring centroid
    ExecuteMerge { centroid: u32 },
}

/// The index rebalancer executes index rebalance operations to maintain centroids
/// with limited and balanced sizes of postings.
pub(crate) struct IndexRebalancer {
    task_jh: JoinHandle<Result<(), String>>,
    stop_tok: CancellationToken,
}

impl IndexRebalancer {
    pub(crate) fn start(
        centroid_graph: Arc<dyn CentroidGraph>,
        coordinator_handle: WriteCoordinatorHandle<VectorDbWriteDelta>,
        rx: tokio::sync::mpsc::UnboundedReceiver<IndexRebalanceOp>,
    ) -> Self {
        let stop_tok = CancellationToken::new();
        let task = IndexRebalancerTask {
            centroid_graph,
            coordinator_handle,
            rx,
            stop_tok: stop_tok.clone(),
        };
        let task_jh = tokio::spawn(task.run());
        Self { task_jh, stop_tok }
    }

    async fn stop(self) -> Result<(), String> {
        self.stop_tok.cancel();
        self.task_jh.await.map_err(|err| err.to_string())?
    }
}

struct IndexRebalancerTask {
    centroid_graph: Arc<dyn CentroidGraph>,
    coordinator_handle: WriteCoordinatorHandle<VectorDbWriteDelta>,
    rx: tokio::sync::mpsc::UnboundedReceiver<IndexRebalanceOp>,
    stop_tok: CancellationToken,
}

impl IndexRebalancerTask {
    async fn run(mut self) -> Result<(), String> {
        loop {
            tokio::select! {
                Some(op) = self.rx.recv() => {
                    self.handle_rebalance_op(op).await?
                },
                _ = self.stop_tok.cancelled() => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_rebalance_op(&mut self, op: IndexRebalanceOp) -> Result<(), String> {
        match op {
            IndexRebalanceOp::ExecuteSplit { centroid } => self.handle_split(centroid).await,
            IndexRebalanceOp::ExecuteMerge { centroid } => self.handle_merge(centroid).await,
        }
    }

    async fn handle_split(&mut self, _centroid: u32) -> Result<(), String> {
        todo!()
    }

    async fn handle_merge(&mut self, _centroid: u32) -> Result<(), String> {
        todo!()
    }
}

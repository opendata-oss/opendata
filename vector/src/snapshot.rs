use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, AtomicU64};
use uuid::Uuid;
use common::cell::AtomicCell;
use common::coordinator::View;
use crate::delta::VectorDbWriteDelta;
use crate::hnsw::CentroidGraph;

pub(crate) struct CentroidGraphViewBroker {
    inner: Mutex<CentroidGraphViewBrokerInner>,
    centroid_graph: Arc<dyn CentroidGraph>,
}

impl CentroidGraphViewBroker {
    pub(crate) fn new(centroid_graph: Arc<dyn CentroidGraph>) -> Self {
        Self {
            inner: Mutex::new(
                CentroidGraphViewBrokerInner {
                    snapshots: HashMap::new(),
                    retention_watermark: None,
                }
            ),
            centroid_graph,
        }
    }
}

impl CentroidGraphViewBroker {
    pub(crate) fn view(self: &Arc<Self>, epoch: u64) -> CentroidGraphView {
        let id = uuid::Uuid::new_v4();
        let mut inner = self.inner.lock().expect("lock poisoned");
        inner.hold(id, epoch);
        CentroidGraphView {
            id,
            epoch,
            broker: Arc::clone(self),
            centroid_graph: self.centroid_graph.clone(),
        }
    }

    fn release(&self, id: Uuid) {
        let retention_watermark = self.inner.lock().expect("lock poisoned").release(id);
        // self.centroid_graph.update_retention_watermark(retention_watermark)
    }
}

struct CentroidGraphViewBrokerInner {
    snapshots: HashMap<Uuid, u64>,
    retention_watermark: Option<u64>,
}

impl CentroidGraphViewBrokerInner {
    fn hold(&mut self, id: Uuid, epoch: u64) {
        if let Some(current_wm) = self.retention_watermark {
            assert!(epoch >= current_wm);
        }
        self.snapshots.insert(id, epoch);
    }

    fn release(&mut self, id: Uuid) -> Option<u64> {
        self.snapshots.remove(&id);
        self.update_watermark();
        self.retention_watermark
    }

    fn update_watermark(&mut self) {
        self.retention_watermark = self.snapshots.values().cloned().min();
    }
}

pub(crate) struct CentroidGraphView {
    id: Uuid,
    epoch: u64,
    broker: Arc<CentroidGraphViewBroker>,
    centroid_graph: Arc<dyn CentroidGraph>,
}

impl CentroidGraphView {
    pub(crate) fn search(&self, query: &[f32], k: usize) -> Vec<u64> {
        self.centroid_graph.search_at_epoch(query, k, self.epoch)
    }
}

impl Drop for CentroidGraphView {
    fn drop(&mut self) {
        self.broker.release(self.id);
    }
}
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicI64, AtomicU64};
use uuid::Uuid;
use common::cell::AtomicCell;
use common::coordinator::View;
use crate::delta::VectorDbWriteDelta;

pub(crate) struct SnapshotBroker {
    inner: Mutex<SnapshotBrokerInner>,
}

impl SnapshotBroker {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(
                SnapshotBrokerInner {
                    snapshots: HashMap::new(),
                    retention_watermark: Arc::new(AtomicCell::new(None)),
                }
            )
        }
    }
}

impl SnapshotBroker {
    pub(crate) fn reserve_snapshot(self: &Arc<Self>, epoch: u64) -> SnapshotToken {
        let id = uuid::Uuid::new_v4();
        let mut inner = self.inner.lock().expect("lock poisoned");
        inner.acquire_snapshot(id, epoch);
        SnapshotToken {
            id,
            epoch,
            broker: Arc::clone(self),
        }
    }

    fn release_snapshot(&self, id: Uuid) {
        self.inner.lock().expect("lock poisoned").release_snapshot(id);
    }
}

struct SnapshotBrokerInner {
    snapshots: HashMap<Uuid, u64>,
    retention_watermark: Arc<AtomicCell<Option<u64>>>,
}

impl SnapshotBrokerInner {
    fn acquire_snapshot(&mut self, id: Uuid, epoch: u64) {
        let current_wm = self.retention_watermark.get();
        if let Some(current_wm) = current_wm {
            assert!(epoch >= current_wm);
        }
        self.snapshots.insert(id, epoch);
    }

    fn release_snapshot(&mut self, id: Uuid) {
        self.snapshots.remove(&id);
        self.update_watermark();
    }

    fn update_watermark(&mut self) {
        self.retention_watermark.set(
            self.snapshots.values().min().cloned()
        );
    }
}

pub(crate) struct SnapshotToken {
    id: Uuid,
    epoch: u64,
    broker: Arc<SnapshotBroker>,
}

impl SnapshotToken {
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Drop for SnapshotToken {
    fn drop(&mut self) {
        self.broker.release_snapshot(self.id);
    }
}
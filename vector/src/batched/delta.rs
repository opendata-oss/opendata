use crate::delta::{VectorDbWrite, VectorWrite};
use common::coordinator::Delta;
use std::any::Any;
use std::sync::Arc;
use tracing::{debug, info};

/// Configuration options for the delta.
pub(crate) struct VectorDbDeltaOpts {
    /// Vector dimensions for encoding.
    pub(crate) dimensions: usize,
}

/// Image containing shared state for the delta.
///
/// This is passed to `Delta::init()` when creating a fresh delta. The image
/// contains references to shared in-memory structures that persist across
/// delta lifecycles.
pub(crate) struct VectorDbDeltaContext {
    /// Configuration options.
    pub(crate) opts: VectorDbDeltaOpts,
}

/// Mutable delta that accumulates writes and builds RecordOps.
///
/// Implements the `Delta` trait for use with WriteCoordinator.
pub(crate) struct VectorDbWriteDelta {
    context: VectorDbDeltaContext,
    /// Shared view of the delta's current state, readable by concurrent readers.
    view: Arc<std::sync::RwLock<VectorDbDeltaView>>,
}

impl Delta for VectorDbWriteDelta {
    type Context = VectorDbDeltaContext;
    type Write = VectorDbWrite;
    type Frozen = Arc<VectorDbDeltaView>;
    type FrozenView = Arc<VectorDbDeltaView>;
    type ApplyResult = Arc<dyn Any + Send + Sync + 'static>;
    type DeltaView = Arc<std::sync::RwLock<VectorDbDeltaView>>;

    fn init(context: VectorDbDeltaContext) -> Self {
        Self {
            context,
            view: Arc::new(std::sync::RwLock::new(VectorDbDeltaView::new())),
        }
    }

    fn apply(
        &mut self,
        write: Self::Write,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        let result = match write {
            VectorDbWrite::Write(writes) => self.apply_write(writes),
            VectorDbWrite::Rebalance(_cmd) => {
                panic!("no rebalance commands in batched write")
            }
        };
        result
    }

    fn estimate_size(&self) -> usize {
        let view = self.view.read().expect("lock poisoned");
        // view.writes.len() * self.context.opts.dimensions * 4
        view.writes.len()
    }

    fn freeze(self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        let frozen = Arc::new(VectorDbDeltaView::clone(
            &self.view.read().expect("lock poisoned"),
        ));
        debug!("freezing delta view with {} writes", frozen.writes.len());
        let frozen_view = frozen.clone();
        (frozen, frozen_view, self.context)
    }

    fn reader(&self) -> Self::DeltaView {
        self.view.clone()
    }
}

impl VectorDbWriteDelta {
    fn apply_write(
        &mut self,
        vector_writes: Vec<VectorWrite>,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        let mut view = self.view.write().expect("lock poisoned");
        view.apply(vector_writes);
        drop(view);
        Ok(Arc::new(()))
    }
}

#[derive(Clone)]
pub(crate) struct VectorDbDeltaView {
    pub(crate) writes: Vec<VectorWrite>,
}

impl VectorDbDeltaView {
    fn new() -> Self {
        Self { writes: Vec::new() }
    }

    fn apply(&mut self, writes: Vec<VectorWrite>) {
        self.writes.extend(writes);
    }
}

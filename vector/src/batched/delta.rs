use crate::delta::{VectorDbWrite, VectorWrite};
use common::coordinator::Delta;
use std::any::Any;
use std::sync::Arc;
use tracing::debug;

/// Mutable delta that accumulates writes and builds RecordOps.
///
/// Implements the `Delta` trait for use with WriteCoordinator.
pub(crate) struct VectorDbWriteDelta {
    /// Shared view of the delta's current state, readable by concurrent readers.
    view: Arc<std::sync::RwLock<VectorDbDeltaView>>,
}

impl Delta for VectorDbWriteDelta {
    type Context = ();
    type Write = VectorDbWrite;
    type Frozen = Arc<VectorDbDeltaView>;
    type FrozenView = Arc<VectorDbDeltaView>;
    type ApplyResult = Arc<dyn Any + Send + Sync + 'static>;
    type DeltaView = Arc<std::sync::RwLock<VectorDbDeltaView>>;

    fn init(_context: Self::Context) -> Self {
        Self {
            view: Arc::new(std::sync::RwLock::new(VectorDbDeltaView::new())),
        }
    }

    fn apply(
        &mut self,
        write: Self::Write,
    ) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        match write {
            VectorDbWrite::Write(writes) => self.apply_write(writes),
            VectorDbWrite::Rebalance(_cmd) => {
                panic!("no rebalance commands in batched write")
            }
        }
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
        (frozen, frozen_view, ())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AttributeValue;

    fn make_write(id: &str, values: Vec<f32>) -> VectorWrite {
        VectorWrite {
            external_id: id.to_string(),
            values: values.clone(),
            attributes: vec![("vector".to_string(), AttributeValue::Vector(values))],
        }
    }

    fn assert_write(write: &VectorWrite, expected_id: &str, expected_values: &[f32]) {
        assert_eq!(write.external_id, expected_id);
        assert_eq!(write.values, expected_values);
        // The vector attribute should match the values
        let vector_attr = write
            .attributes
            .iter()
            .find(|(name, _)| name == "vector")
            .expect("missing vector attribute");
        match &vector_attr.1 {
            AttributeValue::Vector(v) => assert_eq!(v.as_slice(), expected_values),
            other => panic!("expected Vector attribute, got {:?}", other),
        }
    }

    #[test]
    fn reader_should_see_applied_writes() {
        let mut delta = VectorDbWriteDelta::init(());
        let reader = delta.reader();

        delta
            .apply(VectorDbWrite::Write(vec![
                make_write("v1", vec![1.0, 0.0, 0.0]),
                make_write("v2", vec![0.0, 1.0, 0.0]),
            ]))
            .unwrap();
        delta
            .apply(VectorDbWrite::Write(vec![make_write(
                "v3",
                vec![0.0, 0.0, 1.0],
            )]))
            .unwrap();

        let view = reader.read().expect("lock poisoned");
        assert_eq!(view.writes.len(), 3);
        assert_write(&view.writes[0], "v1", &[1.0, 0.0, 0.0]);
        assert_write(&view.writes[1], "v2", &[0.0, 1.0, 0.0]);
        assert_write(&view.writes[2], "v3", &[0.0, 0.0, 1.0]);
    }

    #[test]
    fn frozen_view_should_contain_applied_writes() {
        let mut delta = VectorDbWriteDelta::init(());

        delta
            .apply(VectorDbWrite::Write(vec![
                make_write("v1", vec![1.0, 0.0, 0.0]),
                make_write("v2", vec![0.0, 1.0, 0.0]),
            ]))
            .unwrap();

        let (_frozen, frozen_view, _ctx) = delta.freeze();
        assert_eq!(frozen_view.writes.len(), 2);
        assert_write(&frozen_view.writes[0], "v1", &[1.0, 0.0, 0.0]);
        assert_write(&frozen_view.writes[1], "v2", &[0.0, 1.0, 0.0]);
    }

    #[test]
    fn estimate_size_should_return_number_of_vectors() {
        let mut delta = VectorDbWriteDelta::init(());
        assert_eq!(delta.estimate_size(), 0);

        delta
            .apply(VectorDbWrite::Write(vec![make_write(
                "v1",
                vec![1.0, 0.0, 0.0],
            )]))
            .unwrap();
        assert_eq!(delta.estimate_size(), 1);

        delta
            .apply(VectorDbWrite::Write(vec![
                make_write("v2", vec![0.0, 1.0, 0.0]),
                make_write("v3", vec![0.0, 0.0, 1.0]),
            ]))
            .unwrap();
        assert_eq!(delta.estimate_size(), 3);
    }
}

use crate::AttributeValue;
use common::coordinator::Delta;
use std::any::Any;
use std::sync::Arc;
use tracing::debug;

#[derive(Clone, Debug)]
pub(crate) enum VectorDbOp {
    Write(Vec<VectorWrite>),
    Delete(Vec<String>),
}

impl VectorDbOp {
    pub(crate) fn len(&self) -> usize {
        match self {
            VectorDbOp::Write(v) => v.len(),
            VectorDbOp::Delete(v) => v.len(),
        }
    }
}

/// A vector write ready for the coordinator.
///
/// The write path validates and enqueues this struct.
/// The delta handles ID allocation, dictionary lookup, centroid assignment, and updates.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorWrite {
    /// User-provided external ID.
    pub(crate) external_id: String,
    /// Vector embedding values.
    pub(crate) values: Vec<f32>,
    /// All attributes including the vector field.
    pub(crate) attributes: Vec<(String, AttributeValue)>,
}

/// Mutable delta that accumulates writes as well as deletes and builds RecordOps.
///
/// Implements the `Delta` trait for use with WriteCoordinator.
pub(crate) struct VectorDbOpDelta {
    /// Shared view of the delta's current state, readable by concurrent readers.
    view: Arc<std::sync::RwLock<VectorDbDeltaView>>,
}

impl Delta for VectorDbOpDelta {
    type Context = ();
    type Op = VectorDbOp;
    type Frozen = Arc<VectorDbDeltaView>;
    type FrozenView = Arc<VectorDbDeltaView>;
    type ApplyResult = Arc<dyn Any + Send + Sync + 'static>;
    type DeltaView = Arc<std::sync::RwLock<VectorDbDeltaView>>;

    fn init(_context: Self::Context) -> Self {
        Self {
            view: Arc::new(std::sync::RwLock::new(VectorDbDeltaView::new())),
        }
    }

    fn apply(&mut self, op: Self::Op) -> Result<Arc<dyn Any + Send + Sync + 'static>, String> {
        let mut view = self.view.write().expect("lock poisoned");
        view.push(op);
        drop(view);
        Ok(Arc::new(()))
    }

    fn estimate_size(&self) -> usize {
        let view = self.view.read().expect("lock poisoned");
        view.ops.iter().map(VectorDbOp::len).sum()
    }

    fn freeze(self) -> (Self::Frozen, Self::FrozenView, Self::Context) {
        let frozen = Arc::new(VectorDbDeltaView::clone(
            &self.view.read().expect("lock poisoned"),
        ));
        debug!("freezing delta view with {} ops", frozen.ops.len());
        let frozen_view = frozen.clone();
        (frozen, frozen_view, ())
    }

    fn reader(&self) -> Self::DeltaView {
        self.view.clone()
    }
}

#[derive(Clone)]
pub(crate) struct VectorDbDeltaView {
    pub(crate) ops: Vec<VectorDbOp>,
}

impl VectorDbDeltaView {
    fn new() -> Self {
        Self { ops: Vec::new() }
    }

    fn push(&mut self, op: VectorDbOp) {
        self.ops.push(op);
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

    /// Flatten the view's per-call ops into a list of every individual
    /// `VectorWrite`, preserving order. Test-only helper.
    fn flatten_writes(view: &VectorDbDeltaView) -> Vec<&VectorWrite> {
        view.ops
            .iter()
            .flat_map(|op| match op {
                VectorDbOp::Write(w) => w.iter().collect::<Vec<_>>(),
                VectorDbOp::Delete(_) => Vec::new(),
            })
            .collect()
    }

    /// Flatten the view's per-call ops into a list of every individual
    /// delete external ID, preserving order. Test-only helper.
    fn flatten_deletes(view: &VectorDbDeltaView) -> Vec<&str> {
        view.ops
            .iter()
            .flat_map(|op| match op {
                VectorDbOp::Delete(ids) => ids.iter().map(String::as_str).collect::<Vec<_>>(),
                VectorDbOp::Write(_) => Vec::new(),
            })
            .collect()
    }

    #[test]
    fn reader_should_see_applied_writes() {
        let mut delta = VectorDbOpDelta::init(());
        let reader = delta.reader();

        delta
            .apply(VectorDbOp::Write(vec![
                make_write("v1", vec![1.0, 0.0, 0.0]),
                make_write("v2", vec![0.0, 1.0, 0.0]),
            ]))
            .unwrap();
        delta
            .apply(VectorDbOp::Write(vec![make_write(
                "v3",
                vec![0.0, 0.0, 1.0],
            )]))
            .unwrap();

        let view = reader.read().expect("lock poisoned");

        let writes = flatten_writes(&view);
        let deletes = flatten_deletes(&view);
        assert_eq!(writes.len(), 3);
        assert_eq!(deletes.len(), 0);
        assert_write(writes[0], "v1", &[1.0, 0.0, 0.0]);
        assert_write(writes[1], "v2", &[0.0, 1.0, 0.0]);
        assert_write(writes[2], "v3", &[0.0, 0.0, 1.0]);
    }

    #[test]
    fn frozen_view_should_contain_applied_writes() {
        let mut delta = VectorDbOpDelta::init(());

        delta
            .apply(VectorDbOp::Write(vec![
                make_write("v1", vec![1.0, 0.0, 0.0]),
                make_write("v2", vec![0.0, 1.0, 0.0]),
            ]))
            .unwrap();

        let (_frozen, frozen_view, _ctx) = delta.freeze();
        let writes = flatten_writes(&frozen_view);
        let deletes = flatten_deletes(&frozen_view);
        assert_eq!(writes.len(), 2);
        assert_eq!(deletes.len(), 0);
        assert_write(writes[0], "v1", &[1.0, 0.0, 0.0]);
        assert_write(writes[1], "v2", &[0.0, 1.0, 0.0]);
    }

    #[test]
    fn estimate_size_should_return_number_of_vectors() {
        let mut delta = VectorDbOpDelta::init(());
        assert_eq!(delta.estimate_size(), 0);

        delta
            .apply(VectorDbOp::Write(vec![make_write(
                "v1",
                vec![1.0, 0.0, 0.0],
            )]))
            .unwrap();
        assert_eq!(delta.estimate_size(), 1);

        delta
            .apply(VectorDbOp::Write(vec![
                make_write("v2", vec![0.0, 1.0, 0.0]),
                make_write("v3", vec![0.0, 0.0, 1.0]),
            ]))
            .unwrap();
        assert_eq!(delta.estimate_size(), 3);
    }

    #[test]
    fn delta_should_record_deletes_in_frozen_view() {
        let mut delta = VectorDbOpDelta::init(());

        delta
            .apply(VectorDbOp::Delete(vec!["v1".to_string(), "v2".to_string()]))
            .unwrap();
        delta
            .apply(VectorDbOp::Delete(vec!["v3".to_string()]))
            .unwrap();

        let (_frozen, frozen_view, _ctx) = delta.freeze();
        assert!(flatten_writes(&frozen_view).is_empty());
        assert_eq!(flatten_deletes(&frozen_view), vec!["v1", "v2", "v3"]);
    }

    #[test]
    fn delta_should_record_writes_and_deletes_mixed() {
        let mut delta = VectorDbOpDelta::init(());

        // Interleave writes and deletes; the frozen view should preserve each
        // collection's insertion order, regardless of how the calls are mixed.
        delta
            .apply(VectorDbOp::Write(vec![
                make_write("v1", vec![1.0, 0.0, 0.0]),
                make_write("v2", vec![0.0, 1.0, 0.0]),
            ]))
            .unwrap();
        delta
            .apply(VectorDbOp::Delete(vec!["d1".to_string()]))
            .unwrap();
        delta
            .apply(VectorDbOp::Write(vec![make_write(
                "v3",
                vec![0.0, 0.0, 1.0],
            )]))
            .unwrap();
        delta
            .apply(VectorDbOp::Delete(vec!["d2".to_string(), "d3".to_string()]))
            .unwrap();

        let (_frozen, frozen_view, _ctx) = delta.freeze();
        let writes = flatten_writes(&frozen_view);
        assert_eq!(writes.len(), 3);
        assert_write(writes[0], "v1", &[1.0, 0.0, 0.0]);
        assert_write(writes[1], "v2", &[0.0, 1.0, 0.0]);
        assert_write(writes[2], "v3", &[0.0, 0.0, 1.0]);
        assert_eq!(flatten_deletes(&frozen_view), vec!["d1", "d2", "d3"]);

        // verify order-preservation
        assert_eq!(frozen_view.ops.len(), 4);
        assert!(matches!(frozen_view.ops[0], VectorDbOp::Write(_)));
        assert!(matches!(frozen_view.ops[1], VectorDbOp::Delete(_)));
        assert!(matches!(frozen_view.ops[2], VectorDbOp::Write(_)));
        assert!(matches!(frozen_view.ops[3], VectorDbOp::Delete(_)));
    }

    #[test]
    fn delta_estimate_size_should_include_deletes() {
        let mut delta = VectorDbOpDelta::init(());
        assert_eq!(delta.estimate_size(), 0);

        delta
            .apply(VectorDbOp::Delete(vec!["d1".to_string()]))
            .unwrap();
        assert_eq!(delta.estimate_size(), 1);

        delta
            .apply(VectorDbOp::Write(vec![make_write(
                "v1",
                vec![1.0, 0.0, 0.0],
            )]))
            .unwrap();
        assert_eq!(delta.estimate_size(), 2);

        delta
            .apply(VectorDbOp::Delete(vec!["d2".to_string(), "d3".to_string()]))
            .unwrap();
        assert_eq!(delta.estimate_size(), 4);
    }
}

//! Label-manipulation breaker for `label_replace` / `label_join`.
//!
//! These functions rewrite output labelsets rather than sample values. The
//! planner precomputes the deduplicated output roster plus an
//! `input_series -> output_series` map from the child schema and the call's
//! string arguments. Runtime still must reject overlapping rewrites that
//! collapse distinct input series onto the same output labels at the same
//! step, so this operator drains its child and merges cells onto the output
//! roster, erroring on same-step collisions.

use std::sync::Arc;
use std::task::{Context, Poll};

use regex::Regex;

use crate::model::{Label, Labels};
use crate::promql::v2::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use crate::promql::v2::memory::{MemoryReservation, QueryError};
use crate::promql::v2::operator::{Operator, OperatorSchema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelManipKind {
    Replace {
        dst_label: String,
        replacement: String,
        src_label: String,
        regex: String,
    },
    Join {
        dst_label: String,
        separator: String,
        src_labels: Arc<[String]>,
    },
}

impl LabelManipKind {
    pub fn apply_to_labels(&self, labels: &Labels) -> Result<Labels, QueryError> {
        match self {
            Self::Replace {
                dst_label,
                replacement,
                src_label,
                regex,
            } => apply_label_replace(labels, dst_label, replacement, src_label, regex),
            Self::Join {
                dst_label,
                separator,
                src_labels,
            } => Ok(apply_label_join(labels, dst_label, separator, src_labels)),
        }
    }
}

fn apply_label_replace(
    labels: &Labels,
    dst_label: &str,
    replacement: &str,
    src_label: &str,
    regex_src: &str,
) -> Result<Labels, QueryError> {
    let regex = Regex::new(&format!("^(?s:{regex_src})$"))
        .map_err(|err| QueryError::Internal(err.to_string()))?;
    let src_value = labels.get(src_label).unwrap_or_default();
    let Some(captures) = regex.captures(src_value) else {
        return Ok(labels.clone());
    };

    let mut replaced = String::new();
    captures.expand(replacement, &mut replaced);
    Ok(rewrite_label(
        labels,
        dst_label,
        (!replaced.is_empty()).then_some(replaced),
    ))
}

fn apply_label_join(
    labels: &Labels,
    dst_label: &str,
    separator: &str,
    src_labels: &[String],
) -> Labels {
    let mut joined = String::new();
    for (index, src_label) in src_labels.iter().enumerate() {
        if index > 0 {
            joined.push_str(separator);
        }
        if let Some(value) = labels.get(src_label) {
            joined.push_str(value);
        }
    }

    rewrite_label(labels, dst_label, (!joined.is_empty()).then_some(joined))
}

fn rewrite_label(labels: &Labels, dst_label: &str, replacement: Option<String>) -> Labels {
    let mut out: Vec<Label> = labels
        .iter()
        .filter(|label| label.name != dst_label)
        .cloned()
        .collect();
    if let Some(value) = replacement {
        out.push(Label::new(dst_label.to_string(), value));
    }
    out.sort();
    Labels::new(out)
}

#[inline]
fn out_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

struct OutBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
    validity: BitSet,
}

impl OutBuffers {
    fn allocate(reservation: &MemoryReservation, cells: usize) -> Result<Self, QueryError> {
        let bytes = out_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            values: vec![0.0; cells],
            validity: BitSet::with_len(cells),
        })
    }

    fn finish(mut self) -> (Vec<f64>, BitSet) {
        let values = std::mem::take(&mut self.values);
        let validity = std::mem::replace(&mut self.validity, BitSet::with_len(0));
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (values, validity)
    }
}

impl Drop for OutBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

pub struct LabelManipOp<C: Operator> {
    child: Option<C>,
    input_to_output: Arc<[u32]>,
    schema: OperatorSchema,
    reservation: MemoryReservation,
    done: bool,
    errored: bool,
}

impl<C: Operator> LabelManipOp<C> {
    pub fn new(
        child: C,
        input_to_output: Arc<[u32]>,
        output_schema: Arc<SeriesSchema>,
        reservation: MemoryReservation,
    ) -> Self {
        let step_grid = child.schema().step_grid;
        debug_assert_eq!(
            input_to_output.len(),
            child
                .schema()
                .series
                .as_static()
                .map(|schema| schema.len())
                .unwrap_or(0),
            "label-manip input_to_output must cover the full child schema",
        );

        Self {
            child: Some(child),
            input_to_output,
            schema: OperatorSchema::new(SchemaRef::Static(output_schema), step_grid),
            reservation,
            done: false,
            errored: false,
        }
    }

    fn drain_child(&mut self, cx: &mut Context<'_>) -> Result<Option<Vec<StepBatch>>, QueryError> {
        let child = match self.child.as_mut() {
            Some(child) => child,
            None => return Ok(Some(Vec::new())),
        };

        let mut batches = Vec::new();
        loop {
            match child.next(cx) {
                Poll::Pending => return Ok(None),
                Poll::Ready(None) => {
                    self.child = None;
                    return Ok(Some(batches));
                }
                Poll::Ready(Some(Err(err))) => return Err(err),
                Poll::Ready(Some(Ok(batch))) => batches.push(batch),
            }
        }
    }

    fn merge_batches(&self, batches: Vec<StepBatch>) -> Result<StepBatch, QueryError> {
        let step_grid = self.schema.step_grid;
        let step_count = step_grid.step_count;
        let output_schema = self
            .schema
            .series
            .as_static()
            .expect("label manip publishes a static schema")
            .clone();
        let out_series_count = output_schema.len();
        let cells = step_count.saturating_mul(out_series_count);
        let mut out = OutBuffers::allocate(&self.reservation, cells)?;

        for batch in &batches {
            let in_series_count = batch.series_count();
            for step_off in 0..batch.step_count() {
                let global_step = batch.step_range.start + step_off;
                if global_step >= step_count {
                    return Err(QueryError::Internal(format!(
                        "label manipulation step index {global_step} exceeds output step_count {step_count}"
                    )));
                }
                let step_base = step_off * in_series_count;
                for series_off in 0..in_series_count {
                    let cell = step_base + series_off;
                    if !batch.validity.get(cell) {
                        continue;
                    }

                    let global_series = batch.series_range.start + series_off;
                    let out_series = *self.input_to_output.get(global_series).ok_or_else(|| {
                        QueryError::Internal(format!(
                            "label manipulation input series {global_series} exceeds mapping len {}",
                            self.input_to_output.len()
                        ))
                    })? as usize;
                    let out_cell = global_step * out_series_count + out_series;
                    if out.validity.get(out_cell) {
                        return Err(QueryError::Internal(
                            "vector cannot contain metrics with the same labelset".to_string(),
                        ));
                    }

                    out.values[out_cell] = batch.values[cell];
                    out.validity.set(out_cell);
                }
            }
        }

        let step_timestamps = batches
            .first()
            .map(|batch| batch.step_timestamps.clone())
            .unwrap_or_else(|| {
                Arc::from(
                    (0..step_count)
                        .map(|step| step_grid.start_ms + (step as i64) * step_grid.step_ms)
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                )
            });
        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(output_schema),
            0..out_series_count,
            values,
            validity,
        ))
    }
}

impl<C: Operator> Operator for LabelManipOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }

        match self.drain_child(cx) {
            Ok(None) => Poll::Pending,
            Ok(Some(batches)) => match self.merge_batches(batches) {
                Ok(batch) => {
                    self.done = true;
                    Poll::Ready(Some(Ok(batch)))
                }
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            },
            Err(err) => {
                self.errored = true;
                Poll::Ready(Some(Err(err)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::v2::operator::StepGrid;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    struct MockOp {
        schema: OperatorSchema,
        queue: Vec<Result<StepBatch, QueryError>>,
    }

    impl MockOp {
        fn new(schema: Arc<SeriesSchema>, grid: StepGrid, batches: Vec<StepBatch>) -> Self {
            Self {
                schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
                queue: batches.into_iter().map(Ok).collect(),
            }
        }
    }

    impl Operator for MockOp {
        fn schema(&self) -> &OperatorSchema {
            &self.schema
        }

        fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
            if self.queue.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(self.queue.remove(0)))
            }
        }
    }

    fn mk_labels(pairs: &[(&str, &str)]) -> Labels {
        Labels::new(
            pairs
                .iter()
                .map(|(name, value)| Label::new((*name).to_string(), (*value).to_string()))
                .collect(),
        )
    }

    fn mk_schema(entries: Vec<Labels>) -> Arc<SeriesSchema> {
        let fps: Vec<u128> = (0..entries.len() as u128).collect();
        Arc::new(SeriesSchema::new(Arc::from(entries), Arc::from(fps)))
    }

    fn mk_grid(step_count: usize) -> StepGrid {
        StepGrid {
            start_ms: 0,
            end_ms: ((step_count as i64) - 1).max(0) * 10,
            step_ms: 10,
            step_count,
        }
    }

    fn mk_batch(
        schema: Arc<SeriesSchema>,
        step_count: usize,
        series_count: usize,
        values: Vec<f64>,
        validity: Vec<bool>,
    ) -> StepBatch {
        let mut bits = BitSet::with_len(values.len());
        for (index, valid) in validity.into_iter().enumerate() {
            if valid {
                bits.set(index);
            }
        }
        let step_timestamps: Arc<[i64]> = Arc::from(
            (0..step_count)
                .map(|step| (step as i64) * 10)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(schema),
            0..series_count,
            values,
            bits,
        )
    }

    fn drive<C: Operator>(op: &mut LabelManipOp<C>) -> Vec<Result<StepBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(result)) => out.push(result),
                Poll::Pending => panic!("unexpected Pending from sync mock"),
            }
        }
    }

    #[test]
    fn should_merge_non_overlapping_duplicate_output_labelsets() {
        // given
        let input_schema = mk_schema(vec![
            mk_labels(&[("__name__", "m"), ("src", "a")]),
            mk_labels(&[("__name__", "m"), ("src", "b")]),
        ]);
        let output_schema = mk_schema(vec![mk_labels(&[("__name__", "m"), ("dst", "same")])]);
        let grid = mk_grid(2);
        let batch = mk_batch(
            input_schema.clone(),
            2,
            2,
            vec![1.0, 0.0, 0.0, 2.0],
            vec![true, false, false, true],
        );
        let child = MockOp::new(input_schema, grid, vec![batch]);
        let mut op = LabelManipOp::new(
            child,
            Arc::from(vec![0u32, 0u32]),
            output_schema,
            MemoryReservation::new(1 << 20),
        );

        // when
        let out = drive(&mut op);

        // then
        let batches: Vec<StepBatch> = out.into_iter().map(|result| result.unwrap()).collect();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.series_count(), 1);
        assert_eq!(batch.get(0, 0), Some(1.0));
        assert_eq!(batch.get(1, 0), Some(2.0));
    }

    #[test]
    fn should_error_when_duplicate_output_labelsets_overlap_same_step() {
        // given
        let input_schema = mk_schema(vec![
            mk_labels(&[("__name__", "m"), ("src", "a")]),
            mk_labels(&[("__name__", "m"), ("src", "b")]),
        ]);
        let output_schema = mk_schema(vec![mk_labels(&[("__name__", "m"), ("dst", "same")])]);
        let grid = mk_grid(1);
        let batch = mk_batch(input_schema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
        let child = MockOp::new(input_schema, grid, vec![batch]);
        let mut op = LabelManipOp::new(
            child,
            Arc::from(vec![0u32, 0u32]),
            output_schema,
            MemoryReservation::new(1 << 20),
        );

        // when
        let out = drive(&mut op);

        // then
        assert_eq!(out.len(), 1);
        let err = out.into_iter().next().unwrap().unwrap_err();
        assert!(
            err.to_string().contains("same labelset"),
            "unexpected error: {err}"
        );
    }
}

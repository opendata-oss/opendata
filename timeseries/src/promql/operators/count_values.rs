//! `CountValuesOp` implements PromQL `count_values` — the one aggregation
//! whose output labelset depends on the sample values it sees, not just
//! the query's matchers. It buckets input series by their per-step value
//! and emits one output series per distinct observed value, labelled
//! `{<label>="<value>"}` plus any `by` / `without` grouping labels.
//!
//! Because the output roster isn't known until the samples have been
//! read, this is a pipeline breaker (buffers its child fully before
//! emitting): it drains the child, discovers the distinct values,
//! finalises an output [`SeriesSchema`], and only then emits batches.
//! [`Operator::schema`] publishes [`SchemaRef::Deferred`] for the
//! operator's whole life; downstream consumers read the concrete schema
//! off each emitted batch (internally memoised and exposed via
//! [`CountValuesOp::finalized_schema`]).
//!
//! Value-to-label formatting matches Prometheus' Go
//! `FormatFloat(v, 'f', -1, 64)`: NaN → `"NaN"`, ±Inf → `"±Inf"`, `-0.0`
//! → `"-0"`, finite values use the shortest round-trip decimal. Buckets
//! key on `f64::to_bits()` internally so `+0.0` / `-0.0` and individual
//! NaN bit patterns don't collide during hashing; label formatting then
//! collapses all NaN patterns to `"NaN"` so the user-facing roster stays
//! consistent.
//!
//! The optional [`GroupMap`] partitions inputs into `(group, value)`
//! buckets; absent a group map, all inputs feed one synthetic group. The
//! planner passes one `Labels` per group for composing output rows.

use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::model::{Label, Labels};

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};
use super::aggregate::GroupMap;

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

/// Matches Prometheus' `FormatFloat(v, 'f', -1, 64)` for the cases Rust's
/// `to_string` diverges on: NaN, ±Inf, and `-0.0`.
pub fn format_value_label(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Inf".to_string()
        } else {
            "+Inf".to_string()
        };
    }
    if value == 0.0 && value.is_sign_negative() {
        return "-0".to_string();
    }
    // Rust's `f64::to_string` emits the shortest round-trip decimal
    // representation (e.g. `6` for `6.0`, `0.5` for `0.5`), matching Go's
    // `FormatFloat(-1, 64)` for finite non-zero values. Bitwise-equal
    // integers that exceed `f64`'s integer precision still round-trip
    // through the shortest representation, which is what Prometheus
    // emits too.
    value.to_string()
}

// ---------------------------------------------------------------------------
// Intermediate bucket state
// ---------------------------------------------------------------------------

/// Key into the `(group, value-bits)` intermediate map.
///
/// `value_bits = f64::to_bits(v)`: distinguishes `+0.0` from `-0.0` and
/// every NaN bit pattern. Label formatting later collapses all NaNs to
/// the literal `"NaN"`.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct BucketKey {
    group: u32,
    value_bits: u64,
}

/// Per-bucket counts array — one entry per step in the output grid.
#[derive(Debug)]
struct BucketCounts {
    /// Length = `step_count`. Each entry is the number of input series
    /// in `(group)` that observed this value at that step.
    per_step: Vec<u32>,
}

// ---------------------------------------------------------------------------
// Memory accounting
// ---------------------------------------------------------------------------

/// Conservative per-bucket byte cost: one `HashMap` entry (~48 B on
/// 64-bit for `(BucketKey, BucketCounts)` with hasher overhead) + the
/// `Vec<u32>` per-step counter (`4 * step_count` bytes). Overshoots
/// slightly to stay safely on the upper-bound side of the reservation.
#[inline]
fn bucket_bytes(step_count: usize) -> usize {
    const ENTRY_OVERHEAD: usize = 64;
    ENTRY_OVERHEAD.saturating_add(step_count.saturating_mul(std::mem::size_of::<u32>()))
}

#[inline]
fn schema_bytes(series_count: usize, avg_labels_per_series: usize) -> usize {
    // `Arc<[Labels]>` (`Vec<Label>` per series) + `Arc<[u128]>`
    // fingerprints. Per-series cost: one `Vec<Label>` header (24 B) +
    // `avg_labels_per_series * (sizeof(Label) + avg label string bytes)`.
    // We conservatively assume 32 bytes per label string tail on top of
    // the 48-byte `Label` struct. Plus 16 B for the fingerprint.
    const VEC_HDR: usize = 24;
    const LABEL_STRUCT: usize = std::mem::size_of::<Label>();
    const LABEL_STRING_TAIL: usize = 32;
    let per_label = LABEL_STRUCT.saturating_add(LABEL_STRING_TAIL);
    let per_series = VEC_HDR
        .saturating_add(avg_labels_per_series.saturating_mul(per_label))
        .saturating_add(std::mem::size_of::<u128>());
    series_count.saturating_mul(per_series)
}

#[inline]
fn out_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

/// RAII guard for the per-batch output `Vec<f64>` + `BitSet`. Mirrors the
/// pattern in `aggregate.rs` / `rollup.rs`.
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

// ---------------------------------------------------------------------------
// CountValuesOp — the operator
// ---------------------------------------------------------------------------

/// Implements PromQL's `count_values`. The one operator whose output
/// series roster depends on sample values — so it publishes
/// [`SchemaRef::Deferred`] from [`Self::schema`] and only binds a concrete
/// [`SeriesSchema`] after it has drained its child.
///
/// Life cycle:
/// 1. Constructor stores the child and grouping configuration.
///    [`Self::schema`] returns [`SchemaRef::Deferred`].
/// 2. The first call to [`Self::next`] drains the entire child, builds
///    the intermediate `(group, value)` counts table, finalises the
///    output [`SeriesSchema`] (which [`Self::finalized_schema`] then
///    exposes), and emits one [`StepBatch`] covering the whole grid.
/// 3. Subsequent calls to [`Self::next`] return `Poll::Ready(None)`.
pub struct CountValuesOp<C: Operator> {
    child: Option<C>,
    label_name: String,
    /// Optional grouping. `None` ⇒ all inputs fall into a single
    /// synthetic group (group_count = 1, group labels empty).
    group_map: Option<GroupMap>,
    /// Per-group label sets (planner-supplied). Length equals
    /// `group_count` when grouping is used; length = 1 with empty labels
    /// when grouping is absent.
    group_labels: Arc<[Labels]>,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    /// Finalised output schema, populated once the child has been drained
    /// and the `(group, value)` roster has been computed. Exposed via
    /// [`Self::finalized_schema`] after draining.
    finalized: Option<Arc<SeriesSchema>>,
    /// Bytes reserved for the intermediate buckets + finalised schema;
    /// released on `Drop`.
    scratch_bytes: usize,
    done: bool,
    errored: bool,
}

impl<C: Operator> CountValuesOp<C> {
    /// Construct a `count_values` operator.
    ///
    /// * `child` — upstream operator producing the value stream.
    /// * `label_name` — label to attach to output series (e.g. `"version"`).
    ///   The planner is responsible for validating the label name is a
    ///   legal Prometheus label identifier.
    /// * `group_map` — optional `by`/`without` grouping. `None` ⇒ every
    ///   input is aggregated into one group (no group labels).
    /// * `group_labels` — per-group label sets. Required to have
    ///   `group_map.group_count` entries when `group_map` is `Some`; must
    ///   be `[Labels::empty()]` when `group_map` is `None`.
    /// * `reservation` — per-query reservation.
    /// * `step_grid` — outer step grid the emitted batch lands on.
    pub fn new(
        child: C,
        label_name: impl Into<String>,
        group_map: Option<GroupMap>,
        group_labels: Arc<[Labels]>,
        reservation: MemoryReservation,
    ) -> Self {
        let step_grid = child.schema().step_grid;

        debug_assert!(
            match &group_map {
                Some(g) => group_labels.len() == g.group_count,
                None => group_labels.len() == 1,
            },
            "group_labels length must match group_count (or be 1 when ungrouped)",
        );

        Self {
            child: Some(child),
            label_name: label_name.into(),
            group_map,
            group_labels,
            reservation,
            schema: OperatorSchema::new(SchemaRef::Deferred, step_grid),
            finalized: None,
            scratch_bytes: 0,
            done: false,
            errored: false,
        }
    }

    /// Returns the finalised output schema once the child has been drained
    /// and the first emit has happened. Before that, returns `None`.
    ///
    /// Downstream operators that need the concrete roster (a planner
    /// concern) should call this after polling their deferred child to
    /// completion of its first batch.
    pub fn finalized_schema(&self) -> Option<&Arc<SeriesSchema>> {
        self.finalized.as_ref()
    }

    /// Drain the child synchronously under the supplied context. Returns
    /// `Ok(Some(batches))` on full drain, `Ok(None)` on pending, or
    /// `Err` on upstream error.
    #[allow(clippy::type_complexity)]
    fn drain_child(&mut self, cx: &mut Context<'_>) -> Result<Option<Vec<StepBatch>>, QueryError> {
        let child = match self.child.as_mut() {
            Some(c) => c,
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

    /// After the child has been fully drained, build the counts table,
    /// finalise the schema, and emit the full-grid `StepBatch`.
    fn finalise(&mut self, batches: Vec<StepBatch>) -> Result<StepBatch, QueryError> {
        let step_count = self.schema.step_grid.step_count;

        // The query grid's `step_timestamps: Arc<[i64]>`. Shared by every
        // input batch; take the first one and reuse it. If the child
        // produced no batches we synthesise a fresh `Arc<[i64]>` sized to
        // the grid — output is an empty-roster batch (no series), which
        // the downstream treats as "no series emitted".
        let step_timestamps: Arc<[i64]> = batches
            .first()
            .map(|b| b.step_timestamps.clone())
            .unwrap_or_else(|| Arc::from(vec![0i64; step_count].into_boxed_slice()));

        // Bucket every `(group, value_bits)` occurrence per step.
        let mut buckets: HashMap<BucketKey, BucketCounts> = HashMap::new();
        // Stable emission order: insertion order of bucket keys. Keeps
        // tests deterministic and matches "first-seen wins" semantics
        // found elsewhere in the engine.
        let mut bucket_order: Vec<BucketKey> = Vec::new();

        for batch in &batches {
            let step_start = batch.step_range.start;
            let step_count_in = batch.step_count();
            let in_series_count = batch.series_count();
            let series_start = batch.series_range.start;

            for step_off in 0..step_count_in {
                let global_step = step_start + step_off;
                let step_base = step_off * in_series_count;
                for s in 0..in_series_count {
                    let cell = step_base + s;
                    if !batch.validity.get(cell) {
                        continue;
                    }
                    let global_series = series_start + s;
                    let group = match &self.group_map {
                        Some(gm) => match gm.input_to_group.get(global_series) {
                            Some(Some(g)) => *g,
                            Some(None) => continue,
                            // Out-of-range series index ⇒ planner bug.
                            // Drop defensively (debug_assert catches tests).
                            None => {
                                debug_assert!(
                                    false,
                                    "series index {global_series} out of group_map bounds"
                                );
                                continue;
                            }
                        },
                        None => 0u32,
                    };
                    let v = batch.values[cell];
                    let key = BucketKey {
                        group,
                        value_bits: v.to_bits(),
                    };
                    // Reserve one bucket worth of bytes before inserting a
                    // fresh entry.
                    let counts = match buckets.get_mut(&key) {
                        Some(c) => c,
                        None => {
                            let bytes = bucket_bytes(step_count);
                            self.reservation.try_grow(bytes)?;
                            self.scratch_bytes = self.scratch_bytes.saturating_add(bytes);
                            bucket_order.push(key);
                            buckets.insert(
                                key,
                                BucketCounts {
                                    per_step: vec![0u32; step_count],
                                },
                            );
                            buckets
                                .get_mut(&key)
                                .expect("just inserted bucket must exist")
                        }
                    };
                    // Guard against mismatched child grid — `global_step`
                    // must land inside the outer grid. Planner bug
                    // otherwise.
                    if global_step < counts.per_step.len() {
                        counts.per_step[global_step] =
                            counts.per_step[global_step].saturating_add(1);
                    } else {
                        debug_assert!(
                            false,
                            "global_step {global_step} exceeds outer grid step_count {step_count}"
                        );
                    }
                }
            }
        }

        // Build the output roster in insertion order.
        let out_series_count = bucket_order.len();
        let mut labels_vec: Vec<Labels> = Vec::with_capacity(out_series_count);
        let mut fps_vec: Vec<u128> = Vec::with_capacity(out_series_count);
        let avg_labels_per_series = self
            .group_labels
            .first()
            .map(|l| l.len())
            .unwrap_or(0)
            .saturating_add(1);
        let schema_budget = schema_bytes(out_series_count, avg_labels_per_series);
        self.reservation.try_grow(schema_budget)?;
        self.scratch_bytes = self.scratch_bytes.saturating_add(schema_budget);

        for (fp, key) in bucket_order.iter().enumerate() {
            let value = f64::from_bits(key.value_bits);
            let group_idx = key.group as usize;
            let base_labels = self
                .group_labels
                .get(group_idx)
                .cloned()
                .unwrap_or_else(Labels::empty);
            let mut label_vec: Vec<Label> = base_labels
                .iter()
                .filter(|l| l.name != self.label_name)
                .cloned()
                .collect();
            label_vec.push(Label {
                name: self.label_name.clone(),
                value: format_value_label(value),
            });
            label_vec.sort();
            labels_vec.push(Labels::new(label_vec));
            // Simple stable fingerprint — deterministic but not
            // cross-query comparable. The planner's fingerprint function
            // can replace this in Phase 4; for now insertion order is
            // sufficient for downstream identity checks.
            fps_vec.push(fp as u128);
        }

        let output_schema = Arc::new(SeriesSchema::new(
            Arc::from(labels_vec.into_boxed_slice()),
            Arc::from(fps_vec.into_boxed_slice()),
        ));
        self.finalized = Some(output_schema.clone());

        // Emit a single StepBatch spanning the full grid × all output
        // series. Values are the counts; validity=1 iff any input
        // contributed for that (step, output-series).
        let cells = step_count.saturating_mul(out_series_count);
        let mut out = OutBuffers::allocate(&self.reservation, cells)?;
        for (out_idx, key) in bucket_order.iter().enumerate() {
            let counts = buckets
                .get(key)
                .expect("bucket present in order vec must be in map");
            for step in 0..step_count {
                let n = counts.per_step[step];
                if n == 0 {
                    continue;
                }
                let cell = step * out_series_count + out_idx;
                out.values[cell] = n as f64;
                out.validity.set(cell);
            }
        }

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

impl<C: Operator> Operator for CountValuesOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }
        if self.finalized.is_some() {
            // Already emitted the one batch; transition to done.
            self.done = true;
            return Poll::Ready(None);
        }
        match self.drain_child(cx) {
            Ok(None) => Poll::Pending,
            Ok(Some(batches)) => match self.finalise(batches) {
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

impl<C: Operator> Drop for CountValuesOp<C> {
    fn drop(&mut self) {
        if self.scratch_bytes > 0 {
            self.reservation.release(self.scratch_bytes);
            self.scratch_bytes = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels};
    use crate::promql::batch::SeriesSchema;
    use crate::promql::operator::StepGrid;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    // ---- waker + mock child -------------------------------------------------

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
        fn with_queue(
            schema: Arc<SeriesSchema>,
            grid: StepGrid,
            queue: Vec<Result<StepBatch, QueryError>>,
        ) -> Self {
            Self {
                schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
                queue,
            }
        }
        fn new(schema: Arc<SeriesSchema>, grid: StepGrid, batches: Vec<StepBatch>) -> Self {
            Self::with_queue(schema, grid, batches.into_iter().map(Ok).collect())
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

    // ---- fixtures -----------------------------------------------------------

    fn mk_labels(pairs: &[(&str, &str)]) -> Labels {
        Labels::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: (*n).to_string(),
                    value: (*v).to_string(),
                })
                .collect(),
        )
    }

    fn mk_input_schema(count: usize) -> Arc<SeriesSchema> {
        let labels: Vec<Labels> = (0..count)
            .map(|i| mk_labels(&[("__name__", "version"), ("inst", &i.to_string())]))
            .collect();
        let fps: Vec<u128> = (0..count as u128).collect();
        Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
    }

    fn mk_grid(step_count: usize) -> StepGrid {
        StepGrid {
            start_ms: 0,
            end_ms: 10 * ((step_count as i64) - 1).max(0),
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
        assert_eq!(values.len(), step_count * series_count);
        assert_eq!(validity.len(), step_count * series_count);
        let ts: Arc<[i64]> =
            Arc::from((0..step_count).map(|i| (i as i64) * 10).collect::<Vec<_>>());
        let mut bits = BitSet::with_len(step_count * series_count);
        for (i, &b) in validity.iter().enumerate() {
            if b {
                bits.set(i);
            }
        }
        StepBatch::new(
            ts,
            0..step_count,
            SchemaRef::Static(schema),
            0..series_count,
            values,
            bits,
        )
    }

    fn drive<C: Operator>(op: &mut CountValuesOp<C>) -> Vec<Result<StepBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(r)) => out.push(r),
                Poll::Pending => panic!("unexpected Pending from sync mock"),
            }
        }
    }

    /// Collect the `__name__`/custom-label pairs from every output series
    /// as `(metric, value_label)` tuples to make assertions compact.
    fn value_labels(schema: &Arc<SeriesSchema>, label_name: &str) -> Vec<String> {
        schema
            .labels_slice()
            .iter()
            .map(|l| l.get(label_name).unwrap_or("").to_string())
            .collect()
    }

    // ========================================================================
    // tests
    // ========================================================================

    #[test]
    fn should_produce_one_series_per_distinct_value() {
        // given: 4 input series, 1 step. Values [6, 6, 7, 8] ⇒ three
        // distinct buckets.
        let in_schema = mk_input_schema(4);
        let grid = mk_grid(1);
        let batch = mk_batch(
            in_schema.clone(),
            1,
            4,
            vec![6.0, 6.0, 7.0, 8.0],
            vec![true; 4],
        );
        let child = MockOp::new(in_schema, grid, vec![batch]);

        // when
        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one batch, three output series
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.series_count(), 3);
        let finalised = op.finalized_schema().expect("finalised after drain");
        let labels = value_labels(finalised, "version");
        let mut sorted = labels.clone();
        sorted.sort();
        assert_eq!(sorted, vec!["6", "7", "8"]);
    }

    #[test]
    fn should_count_series_per_value_per_step() {
        // given: 3 input series, 2 steps.
        //   step 0: [6, 6, 7] ⇒ {6:2, 7:1}
        //   step 1: [7, 7, 8] ⇒ {7:2, 8:1}
        let in_schema = mk_input_schema(3);
        let grid = mk_grid(2);
        let values = vec![6.0, 6.0, 7.0, 7.0, 7.0, 8.0];
        let valid = vec![true; 6];
        let batch = mk_batch(in_schema.clone(), 2, 3, values, valid);
        let child = MockOp::new(in_schema, grid, vec![batch]);

        // when
        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let b = &outs[0];
        assert_eq!(b.step_count(), 2);
        assert_eq!(b.series_count(), 3);
        let finalised = op.finalized_schema().unwrap();
        let labels = value_labels(finalised, "version");
        // find each series's column by its label
        let col_for = |needle: &str| -> usize {
            labels
                .iter()
                .position(|s| s == needle)
                .unwrap_or_else(|| panic!("no column for {needle}, labels = {labels:?}"))
        };
        let c6 = col_for("6");
        let c7 = col_for("7");
        let c8 = col_for("8");

        assert_eq!(b.get(0, c6), Some(2.0));
        assert_eq!(b.get(0, c7), Some(1.0));
        assert_eq!(b.get(0, c8), None); // no inputs hit 8 at step 0
        assert_eq!(b.get(1, c6), None);
        assert_eq!(b.get(1, c7), Some(2.0));
        assert_eq!(b.get(1, c8), Some(1.0));
    }

    #[test]
    fn should_respect_by_grouping() {
        // given: 4 inputs split into two groups.
        //   group 0: series 0,1 values [6, 7]
        //   group 1: series 2,3 values [6, 6]
        // Expected output series:
        //   {group="a", version="6"} step0 = 1
        //   {group="a", version="7"} step0 = 1
        //   {group="b", version="6"} step0 = 2
        let in_schema = mk_input_schema(4);
        let grid = mk_grid(1);
        let batch = mk_batch(
            in_schema.clone(),
            1,
            4,
            vec![6.0, 7.0, 6.0, 6.0],
            vec![true; 4],
        );
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(0), Some(1), Some(1)], 2);
        let group_labels: Arc<[Labels]> = Arc::from(
            vec![mk_labels(&[("group", "a")]), mk_labels(&[("group", "b")])].into_boxed_slice(),
        );

        let mut op = CountValuesOp::new(
            child,
            "version",
            Some(gmap),
            group_labels,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.series_count(), 3);
        let finalised = op.finalized_schema().unwrap();

        // Locate each expected series by its full label set.
        let find = |group: &str, version: &str| -> usize {
            finalised
                .labels_slice()
                .iter()
                .position(|l| l.get("group") == Some(group) && l.get("version") == Some(version))
                .unwrap_or_else(|| {
                    panic!(
                        "no series with group={group} version={version}: {:?}",
                        finalised
                    )
                })
        };
        let a6 = find("a", "6");
        let a7 = find("a", "7");
        let b6 = find("b", "6");
        assert_eq!(b.get(0, a6), Some(1.0));
        assert_eq!(b.get(0, a7), Some(1.0));
        assert_eq!(b.get(0, b6), Some(2.0));
    }

    #[test]
    fn should_respect_without_grouping() {
        // given: 3 inputs, each in its own group (mimics `without`).
        //   group 0 (inst=0): value 6
        //   group 1 (inst=1): value 6
        //   group 2 (inst=2): value 7
        // Output: three serieses all with different `{inst=..., version=...}`.
        let in_schema = mk_input_schema(3);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 3, vec![6.0, 6.0, 7.0], vec![true; 3]);
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(1), Some(2)], 3);
        let group_labels: Arc<[Labels]> = Arc::from(
            vec![
                mk_labels(&[("inst", "0")]),
                mk_labels(&[("inst", "1")]),
                mk_labels(&[("inst", "2")]),
            ]
            .into_boxed_slice(),
        );

        let mut op = CountValuesOp::new(
            child,
            "version",
            Some(gmap),
            group_labels,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.series_count(), 3);
        let finalised = op.finalized_schema().unwrap();
        for l in finalised.labels_slice() {
            assert!(l.get("inst").is_some());
            assert!(l.get("version").is_some());
        }
        // every output cell should equal 1 (each input is alone in its group)
        for s in 0..3 {
            assert_eq!(b.get(0, s), Some(1.0));
        }
    }

    #[test]
    fn should_expose_deferred_schema_before_drain() {
        // given: fresh operator
        let in_schema = mk_input_schema(1);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 1, vec![6.0], vec![true]);
        let child = MockOp::new(in_schema, grid, vec![batch]);

        let op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );

        // when / then: schema is Deferred even before polling
        assert!(op.schema().series.is_deferred());
        assert!(op.finalized_schema().is_none());
    }

    #[test]
    fn should_expose_static_schema_after_drain() {
        // given
        let in_schema = mk_input_schema(2);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 2, vec![6.0, 7.0], vec![true, true]);
        let child = MockOp::new(in_schema, grid, vec![batch]);

        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );

        // when: drive to completion
        let _ = drive(&mut op);

        // then: schema() still reports Deferred, but finalized_schema() is Some
        assert!(op.schema().series.is_deferred());
        let finalised = op.finalized_schema().expect("finalised after drain");
        assert_eq!(finalised.len(), 2);
    }

    #[test]
    fn should_distinguish_nan_and_value_by_bit_pattern() {
        // given: two inputs with NaN values that have *different* bit
        // patterns — they should end up in two separate buckets in the
        // intermediate map even though both render to "NaN".
        // To keep the test deterministic and user-facing, we check that a
        // NaN-valued input produces a `{version="NaN"}` output series and
        // the count is correct for repeated NaN inputs.
        let in_schema = mk_input_schema(3);
        let grid = mk_grid(1);
        let nan_a = f64::NAN;
        // Construct a different NaN bit pattern.
        let nan_b = f64::from_bits(f64::NAN.to_bits() ^ 0x1);
        assert!(nan_b.is_nan());
        assert_ne!(nan_a.to_bits(), nan_b.to_bits());
        let batch = mk_batch(
            in_schema.clone(),
            1,
            3,
            vec![nan_a, nan_b, 6.0],
            vec![true; 3],
        );
        let child = MockOp::new(in_schema, grid, vec![batch]);

        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        let finalised = op.finalized_schema().unwrap();
        // Three distinct buckets by value_bits: two NaN variants + 6.
        assert_eq!(b.series_count(), 3);
        // All NaN buckets render with label `"NaN"` even though the bit
        // patterns differ.
        let labels = value_labels(finalised, "version");
        let nan_count = labels.iter().filter(|s| *s == "NaN").count();
        assert_eq!(nan_count, 2);
        // The sixth bucket is present.
        assert!(labels.iter().any(|s| s == "6"));
        // Every output cell at step 0 equals 1 (each bucket saw exactly
        // one input).
        for s in 0..3 {
            assert_eq!(b.get(0, s), Some(1.0));
        }
    }

    #[test]
    fn should_propagate_error_from_upstream() {
        // given: child queue has an error before EOS
        let in_schema = mk_input_schema(1);
        let grid = mk_grid(1);
        let child = MockOp::with_queue(
            in_schema,
            grid,
            vec![Err(QueryError::Internal("boom".into()))],
        );

        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );

        // when
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let first = op.next(&mut cx);

        // then
        match first {
            Poll::Ready(Some(Err(QueryError::Internal(msg)))) => assert!(msg.contains("boom")),
            other => panic!("expected upstream error, got {other:?}"),
        }
        // subsequent polls return None (errored)
        assert!(matches!(op.next(&mut cx), Poll::Ready(None)));
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: a tiny cap that cannot fit even one bucket
        let in_schema = mk_input_schema(1);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 1, vec![6.0], vec![true]);
        let child = MockOp::new(in_schema, grid, vec![batch]);

        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(8), // 8-byte cap, far below any reasonable bucket
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let r = op.next(&mut cx);
        match r {
            Poll::Ready(Some(Err(QueryError::MemoryLimit { .. }))) => {}
            other => panic!("expected MemoryLimit error, got {other:?}"),
        }
    }

    #[test]
    fn should_handle_child_end_of_stream() {
        // given: child produces no batches at all
        let in_schema = mk_input_schema(0);
        let grid = mk_grid(2);
        let child = MockOp::new(in_schema, grid, vec![]);

        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one batch with zero series, finalised schema is empty.
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].series_count(), 0);
        let finalised = op.finalized_schema().unwrap();
        assert_eq!(finalised.len(), 0);
    }

    #[test]
    fn should_format_value_label_matching_existing_engine() {
        // Reference behaviour cited from Prometheus' strconv.FormatFloat(-1, 64):
        //   6.0 -> "6", 6.5 -> "6.5", -0.0 -> "-0",
        //   NaN -> "NaN", +Inf -> "+Inf", -Inf -> "-Inf"
        // Additional finite cases agree with Rust's f64::to_string.
        assert_eq!(format_value_label(6.0), "6");
        assert_eq!(format_value_label(6.5), "6.5");
        assert_eq!(format_value_label(0.0), "0");
        assert_eq!(format_value_label(-0.0), "-0");
        assert_eq!(format_value_label(f64::NAN), "NaN");
        assert_eq!(format_value_label(f64::INFINITY), "+Inf");
        assert_eq!(format_value_label(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_value_label(-3.25), "-3.25");
        assert_eq!(format_value_label(1e20), "100000000000000000000");
    }

    #[test]
    fn should_return_none_on_subsequent_polls_after_emission() {
        // given: valid input; drive once to collect the emit, then poll
        // a second time to confirm idempotent EOS.
        let in_schema = mk_input_schema(2);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 2, vec![6.0, 7.0], vec![true; 2]);
        let child = MockOp::new(in_schema, grid, vec![batch]);

        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        // first: batch
        let first = op.next(&mut cx);
        assert!(matches!(first, Poll::Ready(Some(Ok(_)))));
        // second: none
        assert!(matches!(op.next(&mut cx), Poll::Ready(None)));
        // third: still none
        assert!(matches!(op.next(&mut cx), Poll::Ready(None)));
    }

    #[test]
    fn should_drop_inputs_with_none_group_assignment() {
        // given: 3 inputs, second one unassigned (group = None).
        let in_schema = mk_input_schema(3);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 3, vec![6.0, 7.0, 6.0], vec![true; 3]);
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), None, Some(0)], 1);
        let group_labels: Arc<[Labels]> =
            Arc::from(vec![mk_labels(&[("group", "a")])].into_boxed_slice());

        let mut op = CountValuesOp::new(
            child,
            "version",
            Some(gmap),
            group_labels,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: only value 6 emerges (the 7 was dropped).
        let b = &outs[0];
        assert_eq!(b.series_count(), 1);
        let finalised = op.finalized_schema().unwrap();
        assert_eq!(value_labels(finalised, "version"), vec!["6"]);
        assert_eq!(b.get(0, 0), Some(2.0));
    }

    fn mk_tile_batch(
        schema: Arc<SeriesSchema>,
        step_count: usize,
        series_range: std::ops::Range<usize>,
        values: Vec<f64>,
        validity: Vec<bool>,
    ) -> StepBatch {
        let sc = series_range.len();
        assert_eq!(values.len(), step_count * sc);
        assert_eq!(validity.len(), values.len());
        let ts: Arc<[i64]> =
            Arc::from((0..step_count).map(|i| (i as i64) * 10).collect::<Vec<_>>());
        let mut bits = BitSet::with_len(values.len());
        for (i, &b) in validity.iter().enumerate() {
            if b {
                bits.set(i);
            }
        }
        StepBatch::new(
            ts,
            0..step_count,
            SchemaRef::Static(schema),
            series_range,
            values,
            bits,
        )
    }

    #[test]
    fn should_count_values_across_multi_series_tile_batches_over_512_series() {
        // given: 1024 input series emitted as two series-tile batches
        // mirroring `VectorSelectorOp`'s default `series_chunk=512`
        // emission. Values follow a round-robin: input `i` takes value
        // `i % 3` (three distinct output buckets, each with ~341 or ~342
        // contributors). CountValuesOp must bucket across tiles so the
        // per-step counts sum over both tiles.
        const INPUTS: usize = 1024;
        const TILE: usize = 512;
        const STEPS: usize = 1;

        let in_schema = mk_input_schema(INPUTS);
        let grid = mk_grid(STEPS);

        let build_tile = |series_range: std::ops::Range<usize>| -> StepBatch {
            let sc = series_range.len();
            let mut values = Vec::with_capacity(STEPS * sc);
            for _step in 0..STEPS {
                for s in series_range.clone() {
                    values.push((s % 3) as f64);
                }
            }
            mk_tile_batch(
                in_schema.clone(),
                STEPS,
                series_range,
                values,
                vec![true; STEPS * sc],
            )
        };
        let batch_a = build_tile(0..TILE);
        let batch_b = build_tile(TILE..INPUTS);
        let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);

        // when
        let mut op = CountValuesOp::new(
            child,
            "version",
            None,
            Arc::from(vec![Labels::empty()].into_boxed_slice()),
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one output batch with three series (values 0, 1, 2).
        // Counts: i in 0..1024 with i % 3 == 0 → 342; == 1 → 341; == 2
        // → 341. Verify via finalised schema + per-bucket count.
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.series_count(), 3);
        let finalised = op.finalized_schema().unwrap();
        let labels = value_labels(finalised, "version");
        let col_for = |v: &str| -> usize {
            labels
                .iter()
                .position(|s| s == v)
                .unwrap_or_else(|| panic!("no column for {v}, labels = {labels:?}"))
        };
        let count_for =
            |modulus: usize| -> f64 { (0..INPUTS).filter(|i| i % 3 == modulus).count() as f64 };
        assert_eq!(b.get(0, col_for("0")), Some(count_for(0)));
        assert_eq!(b.get(0, col_for("1")), Some(count_for(1)));
        assert_eq!(b.get(0, col_for("2")), Some(count_for(2)));
    }
}

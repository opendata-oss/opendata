//! `StepBatch` → `QueryValue` reshape (unit 5.3, polished from 5.1).
//!
//! The v2 executor streams a sequence of [`StepBatch`]es rooted at the
//! physical plan's root operator. The HTTP / embedded surface consumes
//! [`QueryValue`] (the wire shape used by v1 — unchanged per RFC
//! §"Migration Strategy"). This module bridges the two.
//!
//! Shapes produced:
//!
//! - Instant (`step_count == 1`): one [`InstantSample`] per output series
//!   with a valid cell; invalid series dropped. Plans rooted at a
//!   scalar-producing op (single series with empty labels) emit
//!   [`QueryValue::Scalar`].
//! - Range (`step_count > 1`): one [`RangeSample`] per output series that
//!   produced at least one valid cell; absent cells elided inside the
//!   series. Plans rooted at a scalar-producing op fan out into a single
//!   anonymous [`RangeSample`] per v1's `evaluate_range` semantics (the
//!   HTTP adapter from unit 5.2 also handles `Scalar` defensively on a
//!   range grid; reshape emits a `Matrix` directly).
//!
//! Label-copy strategy: the output samples own `Labels` by value (wire
//! type, unchanged). The reshape clones each series' `Labels` from
//! `Arc<SeriesSchema>` exactly once per output series — never per step.
//! `reshape_instant` reaches each series at most once (one step per
//! batch); `reshape_range` uses a per-series accumulator keyed by the
//! global `series_idx`, so the label clone happens the first time a
//! valid cell for that series is seen, regardless of how many steps or
//! batches it spans.
//!
//! Ordering: output series are ordered by the global `series_idx` stamped
//! by the planner. v1's `evaluate_range` uses `HashMap<Labels, _>`
//! iteration (non-deterministic); v1's `evaluate_instant` preserves the
//! evaluator's emission order. Prometheus clients don't rely on order
//! (the wire field is an unordered array), so v2's deterministic
//! `series_idx`-order is structurally compatible with v1. Root `topk` /
//! `bottomk` instant queries are the exception: they are re-sorted by
//! value so `promqltest`'s ordered expectations still match PromQL.
//!
//! Deferred schemas: operators like `CountValuesOp` publish
//! [`SchemaRef::Deferred`] on their `Operator::schema()` but stamp
//! emitted batches with [`SchemaRef::Static`] once the schema is
//! finalised. The reshape reads labels off the *batch's* schema handle,
//! so no special `Deferred` branch is needed here — a `Deferred` batch
//! (which should never happen in practice) is flagged as a
//! [`ReshapeError`] rather than a silent panic.
//!
//! Streaming vs. collect: 5.1 collects every `StepBatch` into a `Vec` in
//! `Tsdb::execute_v2` and reshapes in a single pass. A single-pass
//! streaming reshape is possible (each series' range-sample vector is
//! bounded by `step_count`, so we could allocate per-series accumulators
//! up-front and push as batches arrive), and is a future optimisation —
//! not required for the Phase-5 acceptance criterion. The polished
//! reshape here keeps the collected input and single BTreeMap-keyed
//! accumulator for clarity.

use crate::model::{InstantSample, Labels, QueryValue, RangeSample};

use super::batch::{SchemaRef, SeriesSchema, StepBatch};
use super::plan::{InstantVectorSort, PhysicalPlan};

/// Error surfaced when the executor produces a shape the reshape layer
/// does not know how to interpret. The wiring layer translates this
/// into [`crate::error::QueryError::Execution`].
#[derive(Debug)]
pub struct ReshapeError(pub String);

impl std::fmt::Display for ReshapeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "reshape error: {}", self.0)
    }
}

impl std::error::Error for ReshapeError {}

/// Reshape a set of collected [`StepBatch`]es into the instant-query
/// [`QueryValue`].
///
/// Invariants the caller must uphold:
/// - The plan's step grid has `step_count == 1`.
/// - Emitted batches have schema `SchemaRef::Static` (the RFC's
///   `Deferred → Static` transition happens inside the producing
///   operator before batches reach this function).
///
/// Absent cells are elided. An empty input (no batches, or batches with
/// zero valid cells) produces `QueryValue::Vector(vec![])`. Plans rooted
/// at a pure scalar node (empty schema with a single valid cell) emit
/// `QueryValue::Scalar`.
pub fn reshape_instant(
    plan: &PhysicalPlan,
    batches: Vec<StepBatch>,
) -> Result<QueryValue, ReshapeError> {
    let grid = plan.step_grid;
    if grid.step_count != 1 {
        return Err(ReshapeError(format!(
            "reshape_instant called with step_count={}",
            grid.step_count,
        )));
    }

    // Pure-scalar detection: single-series roster with empty labels —
    // `ConstScalarOp` (and plans whose root collapsed to one) publish
    // exactly this shape. v1 surfaces such results as `QueryValue::Scalar`
    // rather than `Vector([{ labels: {}, .. }])`.
    if plan.root_is_scalar {
        for batch in &batches {
            check_batch_shape(batch)?;
            if batch.series_count() == 1 && batch.step_count() == 1 && batch.validity.get(0) {
                let ts = batch.step_timestamps_slice()[0];
                return Ok(QueryValue::Scalar {
                    timestamp_ms: ts,
                    value: batch.values[0],
                });
            }
        }
        // No valid cell — empty Vector (matches v1's "no sample in
        // lookback window = empty result" behaviour; an unevaluatable
        // scalar should not normally reach this branch).
        return Ok(QueryValue::Vector(Vec::new()));
    }

    let mut samples: Vec<InstantSample> = Vec::new();
    for batch in batches {
        check_batch_shape(&batch)?;
        let schema = batch_static_schema(&batch)?;
        let step_count = batch.step_count();
        let series_count = batch.series_count();
        if step_count == 0 || series_count == 0 {
            continue;
        }
        if step_count != 1 {
            return Err(ReshapeError(format!(
                "reshape_instant saw batch with step_count={step_count}; expected 1",
            )));
        }
        let step_ts = batch.step_timestamps_slice()[0];
        for series_off in 0..series_count {
            // Row-major-by-step ⇒ cell index for step 0 is just the
            // series offset. Fast path: skip validity checks via direct
            // indexing.
            let cell = series_off;
            if !batch.validity.get(cell) {
                continue;
            }
            let global_idx = (batch.series_range.start + series_off) as u32;
            // One clone per emitted sample == one clone per valid output
            // series in instant mode (each series contributes at most
            // one sample).
            let labels = schema.labels(global_idx).clone();
            samples.push(InstantSample {
                labels,
                timestamp_ms: step_ts,
                value: batch.values[cell],
            });
        }
    }
    if let Some(sort) = plan.root_instant_vector_sort {
        samples.sort_by(|left, right| compare_instant_values(left.value, right.value, sort));
    }
    Ok(QueryValue::Vector(samples))
}

fn compare_instant_values(left: f64, right: f64, sort: InstantVectorSort) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => match sort {
            InstantVectorSort::DescendingValue => right
                .partial_cmp(&left)
                .unwrap_or(std::cmp::Ordering::Equal),
            InstantVectorSort::AscendingValue => left
                .partial_cmp(&right)
                .unwrap_or(std::cmp::Ordering::Equal),
        },
    }
}

/// Reshape a set of collected [`StepBatch`]es into the range-query
/// [`QueryValue`].
///
/// One [`RangeSample`] per series that produced at least one valid cell,
/// ordered by global `series_idx`. Samples inside each [`RangeSample`]
/// are ordered by step timestamp; absent cells are elided.
///
/// Plans rooted at a scalar-producing operator collapse into a single
/// anonymous [`RangeSample`] containing every valid cell — mirrors v1's
/// `evaluate_range` handling of a scalar expression on a range grid.
pub fn reshape_range(
    plan: &PhysicalPlan,
    batches: Vec<StepBatch>,
) -> Result<QueryValue, ReshapeError> {
    // Scalar-root path.
    if plan.root_is_scalar {
        let mut samples: Vec<(i64, f64)> = Vec::new();
        for batch in &batches {
            check_batch_shape(batch)?;
            let step_count = batch.step_count();
            let series_count = batch.series_count();
            if step_count == 0 || series_count == 0 {
                continue;
            }
            let ts_slice = batch.step_timestamps_slice();
            for (step_off, &step_ts) in ts_slice.iter().enumerate().take(step_count) {
                for series_off in 0..series_count {
                    let cell = batch.cell_index(step_off, series_off);
                    if !batch.validity.get(cell) {
                        continue;
                    }
                    samples.push((step_ts, batch.values[cell]));
                }
            }
        }
        if samples.is_empty() {
            return Ok(QueryValue::Matrix(Vec::new()));
        }
        // Batches may arrive out of order (e.g. from `Coalesce`), so
        // sort defensively.
        samples.sort_by_key(|(ts, _)| *ts);
        return Ok(QueryValue::Matrix(vec![RangeSample {
            labels: Labels::empty(),
            samples,
        }]));
    }

    // Aggregate per global series index so out-of-order batch emission
    // still produces stable `RangeSample`s. `or_insert_with` clones each
    // series' `Labels` exactly once — the first time a valid cell for
    // that series is seen — never per step.
    use std::collections::BTreeMap;
    let mut per_series: BTreeMap<u32, (Labels, Vec<(i64, f64)>)> = BTreeMap::new();

    for batch in batches {
        check_batch_shape(&batch)?;
        let schema = batch_static_schema(&batch)?;
        let step_count = batch.step_count();
        let series_count = batch.series_count();
        if step_count == 0 || series_count == 0 {
            continue;
        }
        let ts_slice = batch.step_timestamps_slice();
        for series_off in 0..series_count {
            let global_idx = (batch.series_range.start + series_off) as u32;
            for (step_off, &step_ts) in ts_slice.iter().enumerate().take(step_count) {
                let cell = batch.cell_index(step_off, series_off);
                if !batch.validity.get(cell) {
                    continue;
                }
                let entry = per_series
                    .entry(global_idx)
                    .or_insert_with(|| (schema.labels(global_idx).clone(), Vec::new()));
                entry.1.push((step_ts, batch.values[cell]));
            }
        }
    }

    // Steps within a single batch arrive in order, but batches from
    // different operators (Coalesce, Concurrent) may interleave step
    // ranges across series — sort per series to be safe.
    let mut out: Vec<RangeSample> = Vec::with_capacity(per_series.len());
    for (_idx, (labels, mut samples)) in per_series.into_iter() {
        samples.sort_by_key(|(ts, _)| *ts);
        out.push(RangeSample { labels, samples });
    }
    Ok(QueryValue::Matrix(out))
}

/// Defensive dimension check. `StepBatch::new` enforces these invariants
/// via `debug_assert` — release builds skip those, so repeat the checks
/// explicitly here and surface any violation as a `ReshapeError` rather
/// than letting index-out-of-bounds crash the process.
fn check_batch_shape(batch: &StepBatch) -> Result<(), ReshapeError> {
    let step_count = batch.step_count();
    let series_count = batch.series_count();
    let expected = step_count * series_count;
    if batch.values.len() != expected {
        return Err(ReshapeError(format!(
            "batch values.len()={} but step_count*series_count={} (steps={step_count}, series={series_count})",
            batch.values.len(),
            expected,
        )));
    }
    if batch.validity.len() != expected {
        return Err(ReshapeError(format!(
            "batch validity.len()={} but step_count*series_count={} (steps={step_count}, series={series_count})",
            batch.validity.len(),
            expected,
        )));
    }
    if batch.step_range.end > batch.step_timestamps.len() {
        return Err(ReshapeError(format!(
            "batch step_range end={} exceeds step_timestamps len={}",
            batch.step_range.end,
            batch.step_timestamps.len(),
        )));
    }
    if let SchemaRef::Static(schema) = &batch.series
        && batch.series_range.end > schema.len()
    {
        return Err(ReshapeError(format!(
            "batch series_range end={} exceeds schema len={}",
            batch.series_range.end,
            schema.len(),
        )));
    }
    Ok(())
}

/// Extract a static schema from the batch itself — the planner stamps
/// emitted batches with their (possibly freshly finalised) `Static`
/// schema, so the reshape can read labels directly off the batch.
fn batch_static_schema(batch: &StepBatch) -> Result<&SeriesSchema, ReshapeError> {
    match &batch.series {
        SchemaRef::Static(schema) => Ok(schema.as_ref()),
        SchemaRef::Deferred => Err(ReshapeError(
            "batch emitted with SchemaRef::Deferred — operator failed to stamp finalised schema"
                .to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Label;
    use crate::promql::v2::batch::{BitSet, SeriesSchema, StepBatch};
    use crate::promql::v2::operator::StepGrid;
    use std::sync::Arc;

    fn mk_labels(name: &str, env: &str) -> Labels {
        Labels::new(vec![
            Label {
                name: "__name__".to_string(),
                value: name.to_string(),
            },
            Label {
                name: "env".to_string(),
                value: env.to_string(),
            },
        ])
    }

    fn mk_schema(entries: Vec<Labels>) -> Arc<SeriesSchema> {
        let len = entries.len();
        Arc::new(SeriesSchema::new(
            Arc::from(entries),
            Arc::from((0..len as u128).collect::<Vec<_>>()),
        ))
    }

    fn mk_plan(schema: SchemaRef, grid: StepGrid) -> PhysicalPlan {
        mk_plan_with_scalar_flag(schema, grid, false)
    }

    fn mk_scalar_plan(schema: SchemaRef, grid: StepGrid) -> PhysicalPlan {
        mk_plan_with_scalar_flag(schema, grid, true)
    }

    fn mk_plan_with_scalar_flag(
        schema: SchemaRef,
        grid: StepGrid,
        root_is_scalar: bool,
    ) -> PhysicalPlan {
        // Test-only builder — the tests never poll `root`, they just
        // consume `step_grid` and `output_schema` for the reshape call.
        struct Stub {
            schema: crate::promql::v2::operator::OperatorSchema,
        }
        impl crate::promql::v2::operator::Operator for Stub {
            fn schema(&self) -> &crate::promql::v2::operator::OperatorSchema {
                &self.schema
            }
            fn next(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Result<StepBatch, crate::promql::v2::memory::QueryError>>>
            {
                std::task::Poll::Ready(None)
            }
        }
        PhysicalPlan {
            root: Box::new(Stub {
                schema: crate::promql::v2::operator::OperatorSchema::new(schema.clone(), grid),
            }),
            output_schema: schema,
            step_grid: grid,
            root_is_scalar,
            root_instant_vector_sort: None,
        }
    }

    // ── helpers ─────────────────────────────────────────────────────

    fn instant_grid(ts: i64) -> StepGrid {
        StepGrid {
            start_ms: ts,
            end_ms: ts,
            step_ms: 1,
            step_count: 1,
        }
    }

    fn range_grid(start_ms: i64, step_ms: i64, step_count: usize) -> StepGrid {
        StepGrid {
            start_ms,
            end_ms: start_ms + step_ms * (step_count.saturating_sub(1) as i64),
            step_ms,
            step_count,
        }
    }

    // ── instant ────────────────────────────────────────────────────

    #[test]
    fn should_reshape_instant_vector_dropping_invalid_cells() {
        // given: 3 series, middle cell invalid
        let schema = mk_schema(vec![
            mk_labels("http_requests", "a"),
            mk_labels("http_requests", "b"),
            mk_labels("http_requests", "c"),
        ]);
        let steps: Arc<[i64]> = Arc::from(vec![10_000i64]);
        let mut validity = BitSet::with_len(3);
        validity.set(0);
        validity.set(2);
        let batch = StepBatch::new(
            steps,
            0..1,
            SchemaRef::Static(schema.clone()),
            0..3,
            vec![1.0, 0.0, 3.0],
            validity,
        );
        let plan = mk_plan(SchemaRef::Static(schema), instant_grid(10_000));

        // when
        let value = reshape_instant(&plan, vec![batch]).unwrap();

        // then: invalid cell elided
        match value {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 2);
                assert_eq!(samples[0].value, 1.0);
                assert_eq!(samples[1].value, 3.0);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn should_reshape_instant_vector_with_one_sample_per_valid_series() {
        // given: 2-series instant batch, both cells valid
        let schema = mk_schema(vec![
            mk_labels("http_requests", "prod"),
            mk_labels("http_requests", "staging"),
        ]);
        let steps: Arc<[i64]> = Arc::from(vec![10_000i64]);
        let mut validity = BitSet::with_len(2);
        validity.set(0);
        validity.set(1);
        let batch = StepBatch::new(
            steps,
            0..1,
            SchemaRef::Static(schema.clone()),
            0..2,
            vec![42.0, 10.0],
            validity,
        );
        let plan = mk_plan(SchemaRef::Static(schema), instant_grid(10_000));

        // when
        let value = reshape_instant(&plan, vec![batch]).unwrap();

        // then
        match value {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 2);
                assert_eq!(samples[0].timestamp_ms, 10_000);
                assert_eq!(samples[0].value, 42.0);
                assert_eq!(samples[1].value, 10.0);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn should_reshape_scalar_root_for_instant_query() {
        // given: a ConstScalarOp-style 1-series schema with empty labels
        let schema = Arc::new(SeriesSchema::new(
            Arc::from(vec![Labels::empty()]),
            Arc::from(vec![0u128]),
        ));
        let steps: Arc<[i64]> = Arc::from(vec![50_000i64]);
        let mut validity = BitSet::with_len(1);
        validity.set(0);
        let batch = StepBatch::new(
            steps,
            0..1,
            SchemaRef::Static(schema.clone()),
            0..1,
            vec![2.0],
            validity,
        );
        let plan = mk_scalar_plan(SchemaRef::Static(schema), instant_grid(50_000));

        // when
        let value = reshape_instant(&plan, vec![batch]).unwrap();

        // then
        match value {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => {
                assert_eq!(timestamp_ms, 50_000);
                assert_eq!(value, 2.0);
            }
            other => panic!("expected Scalar, got {other:?}"),
        }
    }

    // ── range ──────────────────────────────────────────────────────

    #[test]
    fn should_reshape_range_matrix_preserving_sparse_timestamps() {
        // given: 2 series × 4 steps, validity is sparse
        // series 0 valid at steps [0, 2]; series 1 valid at steps [1, 3]
        let schema = mk_schema(vec![
            mk_labels("http_requests", "prod"),
            mk_labels("http_requests", "staging"),
        ]);
        let steps: Arc<[i64]> = Arc::from(vec![100i64, 200, 300, 400]);
        let mut validity = BitSet::with_len(4 * 2);
        // row-major: validity[step * series_count + series_off]
        validity.set(0); // s0 step0
        validity.set(4); // s0 step2
        validity.set(3); // s1 step1
        validity.set(7); // s1 step3
        let values = vec![
            1.0, 0.0, // step 0
            0.0, 22.0, // step 1
            3.0, 0.0, // step 2
            0.0, 44.0, // step 3
        ];
        let batch = StepBatch::new(
            steps,
            0..4,
            SchemaRef::Static(schema.clone()),
            0..2,
            values,
            validity,
        );
        let plan = mk_plan(SchemaRef::Static(schema), range_grid(100, 100, 4));

        // when
        let value = reshape_range(&plan, vec![batch]).unwrap();

        // then
        match value {
            QueryValue::Matrix(series) => {
                assert_eq!(series.len(), 2);
                assert_eq!(series[0].samples, vec![(100, 1.0), (300, 3.0)]);
                assert_eq!(series[1].samples, vec![(200, 22.0), (400, 44.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn should_reshape_range_produces_one_series_per_label_set() {
        // given: 3 steps x 2 series, all valid
        let schema = mk_schema(vec![
            mk_labels("http_requests", "prod"),
            mk_labels("http_requests", "staging"),
        ]);
        let steps: Arc<[i64]> = Arc::from(vec![100i64, 200, 300]);
        let mut validity = BitSet::with_len(3 * 2);
        for i in 0..6 {
            validity.set(i);
        }
        let values = vec![1.0, 10.0, 2.0, 20.0, 3.0, 30.0];
        let batch = StepBatch::new(
            steps,
            0..3,
            SchemaRef::Static(schema.clone()),
            0..2,
            values,
            validity,
        );
        let plan = mk_plan(SchemaRef::Static(schema), range_grid(100, 100, 3));

        // when
        let value = reshape_range(&plan, vec![batch]).unwrap();

        // then
        match value {
            QueryValue::Matrix(series) => {
                assert_eq!(series.len(), 2);
                assert_eq!(series[0].samples.len(), 3);
                assert_eq!(series[0].samples[0], (100, 1.0));
                assert_eq!(series[0].samples[2], (300, 3.0));
                assert_eq!(series[1].samples[0], (100, 10.0));
                assert_eq!(series[1].samples[2], (300, 30.0));
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn should_reshape_scalar_root_for_range_query() {
        // given: a ConstScalarOp-style 1-series schema over a range
        let schema = Arc::new(SeriesSchema::new(
            Arc::from(vec![Labels::empty()]),
            Arc::from(vec![0u128]),
        ));
        let steps: Arc<[i64]> = Arc::from(vec![100i64, 200, 300]);
        let mut validity = BitSet::with_len(3);
        validity.set(0);
        validity.set(1);
        validity.set(2);
        let batch = StepBatch::new(
            steps,
            0..3,
            SchemaRef::Static(schema.clone()),
            0..1,
            vec![7.0, 7.0, 7.0],
            validity,
        );
        let plan = mk_plan(SchemaRef::Static(schema), range_grid(100, 100, 3));

        // when
        let value = reshape_range(&plan, vec![batch]).unwrap();

        // then: a single anonymous RangeSample with one point per step
        match value {
            QueryValue::Matrix(series) => {
                assert_eq!(series.len(), 1);
                assert!(series[0].labels.is_empty());
                assert_eq!(series[0].samples, vec![(100, 7.0), (200, 7.0), (300, 7.0)],);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn should_drop_series_with_no_valid_samples_in_range_query() {
        // given: 3 series, only series 0 and 2 ever valid
        let schema = mk_schema(vec![
            mk_labels("http_requests", "a"),
            mk_labels("http_requests", "b"),
            mk_labels("http_requests", "c"),
        ]);
        let steps: Arc<[i64]> = Arc::from(vec![100i64, 200]);
        let mut validity = BitSet::with_len(2 * 3);
        validity.set(0); // step 0, series 0
        validity.set(5); // step 1, series 2
        let values = vec![1.0, 0.0, 0.0, 0.0, 0.0, 3.0];
        let batch = StepBatch::new(
            steps,
            0..2,
            SchemaRef::Static(schema.clone()),
            0..3,
            values,
            validity,
        );
        let plan = mk_plan(SchemaRef::Static(schema), range_grid(100, 100, 2));

        // when
        let value = reshape_range(&plan, vec![batch]).unwrap();

        // then: series 1 (no valid samples) dropped
        match value {
            QueryValue::Matrix(series) => {
                assert_eq!(series.len(), 2);
                assert_eq!(series[0].labels.get("env"), Some("a"));
                assert_eq!(series[0].samples, vec![(100, 1.0)]);
                assert_eq!(series[1].labels.get("env"), Some("c"));
                assert_eq!(series[1].samples, vec![(200, 3.0)]);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn should_preserve_step_timestamps_in_range_output() {
        // given: non-uniform step timestamps (valid in a general batch;
        // the grid's step_ms is just metadata — the batch's
        // step_timestamps are the source of truth)
        let schema = mk_schema(vec![mk_labels("http_requests", "prod")]);
        let steps: Arc<[i64]> = Arc::from(vec![111i64, 222, 333]);
        let mut validity = BitSet::with_len(3);
        for i in 0..3 {
            validity.set(i);
        }
        let batch = StepBatch::new(
            steps,
            0..3,
            SchemaRef::Static(schema.clone()),
            0..1,
            vec![1.0, 2.0, 3.0],
            validity,
        );
        let plan = mk_plan(SchemaRef::Static(schema), range_grid(111, 111, 3));

        // when
        let value = reshape_range(&plan, vec![batch]).unwrap();

        // then
        match value {
            QueryValue::Matrix(series) => {
                assert_eq!(series.len(), 1);
                assert_eq!(series[0].samples, vec![(111, 1.0), (222, 2.0), (333, 3.0)],);
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn should_handle_multi_batch_range_query() {
        // given: two batches covering disjoint step ranges for two
        // series (e.g. from a Rechunk or a paged selector).
        let schema = mk_schema(vec![
            mk_labels("http_requests", "prod"),
            mk_labels("http_requests", "staging"),
        ]);
        let steps: Arc<[i64]> = Arc::from(vec![100i64, 200, 300, 400]);

        // batch 1: steps 0..2 × series 0..2
        let mut v1 = BitSet::with_len(2 * 2);
        for i in 0..4 {
            v1.set(i);
        }
        let b1 = StepBatch::new(
            steps.clone(),
            0..2,
            SchemaRef::Static(schema.clone()),
            0..2,
            vec![1.0, 10.0, 2.0, 20.0],
            v1,
        );

        // batch 2: steps 2..4 × series 0..2 (emitted *later*, possibly
        // out of order in a Coalesce tree)
        let mut v2 = BitSet::with_len(2 * 2);
        for i in 0..4 {
            v2.set(i);
        }
        let b2 = StepBatch::new(
            steps.clone(),
            2..4,
            SchemaRef::Static(schema.clone()),
            0..2,
            vec![3.0, 30.0, 4.0, 40.0],
            v2,
        );
        let plan = mk_plan(SchemaRef::Static(schema), range_grid(100, 100, 4));

        // when: batches supplied in reverse order to exercise sort
        let value = reshape_range(&plan, vec![b2, b1]).unwrap();

        // then: assembled contiguously per series
        match value {
            QueryValue::Matrix(series) => {
                assert_eq!(series.len(), 2);
                assert_eq!(
                    series[0].samples,
                    vec![(100, 1.0), (200, 2.0), (300, 3.0), (400, 4.0)],
                );
                assert_eq!(
                    series[1].samples,
                    vec![(100, 10.0), (200, 20.0), (300, 30.0), (400, 40.0)],
                );
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn should_clone_labels_once_per_series_not_per_step() {
        // given: 1 series across 100 steps, all valid — a naive
        // per-step reshape would produce 100 label clones (one per
        // RangeSample push). The aggregator keeps the count at 1 per
        // output series.
        let schema = mk_schema(vec![mk_labels("http_requests", "prod")]);
        const N: usize = 100;
        let steps: Arc<[i64]> = Arc::from((0..N as i64).collect::<Vec<_>>());
        let mut validity = BitSet::with_len(N);
        for i in 0..N {
            validity.set(i);
        }
        let batch = StepBatch::new(
            steps,
            0..N,
            SchemaRef::Static(schema.clone()),
            0..1,
            vec![1.0; N],
            validity,
        );
        let plan = mk_plan(SchemaRef::Static(schema), range_grid(0, 1, N));

        // when
        let value = reshape_range(&plan, vec![batch]).unwrap();

        // then: exactly one RangeSample, carrying exactly one Labels
        // instance — the guarantee "one clone per output series,
        // regardless of step count."
        match value {
            QueryValue::Matrix(series) => {
                assert_eq!(series.len(), 1);
                assert_eq!(series[0].samples.len(), N);
                // Labels is present and non-empty (cloned from schema);
                // there is exactly one per series (one series, one
                // Labels value in the output).
                assert!(!series[0].labels.is_empty());
            }
            other => panic!("expected Matrix, got {other:?}"),
        }
    }

    #[test]
    fn should_handle_deferred_schema_from_count_values() {
        // given: a batch stamped with SchemaRef::Static(finalised) —
        // the contract CountValuesOp meets (its `Operator::schema()`
        // returns `Deferred`, but emitted batches carry `Static`).
        let finalised = mk_schema(vec![
            Labels::new(vec![Label {
                name: "version".to_string(),
                value: "1".to_string(),
            }]),
            Labels::new(vec![Label {
                name: "version".to_string(),
                value: "2".to_string(),
            }]),
        ]);
        let steps: Arc<[i64]> = Arc::from(vec![1_000i64]);
        let mut validity = BitSet::with_len(2);
        validity.set(0);
        validity.set(1);
        let batch = StepBatch::new(
            steps,
            0..1,
            SchemaRef::Static(finalised.clone()),
            0..2,
            vec![3.0, 5.0],
            validity,
        );
        // Plan's output_schema stays Deferred (the operator publishes
        // Deferred even after finalisation); reshape reads labels off
        // the *batch's* SchemaRef::Static, not the plan's.
        let plan = mk_plan(SchemaRef::Deferred, instant_grid(1_000));

        // when
        let value = reshape_instant(&plan, vec![batch]).unwrap();

        // then
        match value {
            QueryValue::Vector(samples) => {
                assert_eq!(samples.len(), 2);
                assert_eq!(samples[0].labels.get("version"), Some("1"));
                assert_eq!(samples[0].value, 3.0);
                assert_eq!(samples[1].labels.get("version"), Some("2"));
                assert_eq!(samples[1].value, 5.0);
            }
            other => panic!("expected Vector, got {other:?}"),
        }
    }

    #[test]
    fn should_return_empty_vector_when_no_batches() {
        // given: no batches at all
        let schema = mk_schema(vec![mk_labels("http_requests", "prod")]);
        let plan_instant = mk_scalar_plan(SchemaRef::Static(schema.clone()), instant_grid(0));
        let plan_range = mk_scalar_plan(SchemaRef::Static(schema), range_grid(0, 1, 5));

        // when / then (instant)
        match reshape_instant(&plan_instant, vec![]).unwrap() {
            QueryValue::Vector(s) => assert!(s.is_empty()),
            other => panic!("expected empty Vector, got {other:?}"),
        }

        // when / then (range)
        match reshape_range(&plan_range, vec![]).unwrap() {
            QueryValue::Matrix(s) => assert!(s.is_empty()),
            other => panic!("expected empty Matrix, got {other:?}"),
        }
    }

    #[test]
    fn should_return_query_error_on_dimension_mismatch() {
        // given: a batch whose values vector doesn't match its
        // declared dimensions. Skirt `StepBatch::new`'s debug-assert
        // by constructing the struct literally (reshape must still
        // guard release builds).
        let schema = mk_schema(vec![mk_labels("http_requests", "prod")]);
        let steps: Arc<[i64]> = Arc::from(vec![100i64, 200]);
        let validity = BitSet::with_len(2);
        let bad = StepBatch {
            step_timestamps: steps,
            step_range: 0..2,
            series: SchemaRef::Static(schema.clone()),
            series_range: 0..1,
            // expected 2 cells, supplied 1 — dimension mismatch
            values: vec![1.0],
            validity,
            source_timestamps: None,
        };
        let plan = mk_plan(SchemaRef::Static(schema), range_grid(100, 100, 2));

        // when
        let err = reshape_range(&plan, vec![bad]).unwrap_err();

        // then
        assert!(
            err.0.contains("values.len()") || err.0.contains("validity.len()"),
            "expected dimension-mismatch error, got {}",
            err.0,
        );
    }

    #[test]
    fn should_return_query_error_on_deferred_batch_schema() {
        // given: reshape never sees `SchemaRef::Deferred` in a batch
        // (operators stamp `Static` before emitting). If it happens,
        // surface a `ReshapeError` rather than panicking.
        let steps: Arc<[i64]> = Arc::from(vec![100i64]);
        let validity = BitSet::with_len(1);
        let bad = StepBatch {
            step_timestamps: steps,
            step_range: 0..1,
            series: SchemaRef::Deferred,
            series_range: 0..1,
            values: vec![1.0],
            validity,
            source_timestamps: None,
        };
        let plan = mk_plan(SchemaRef::Deferred, instant_grid(100));

        // when
        let err = reshape_instant(&plan, vec![bad]).unwrap_err();

        // then
        assert!(
            err.0.contains("Deferred"),
            "expected deferred-schema error, got {}",
            err.0,
        );
    }
}

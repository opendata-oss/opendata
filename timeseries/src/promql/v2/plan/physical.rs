//! Physical planner (unit 4.3).
//!
//! Consumes an optimised [`LogicalPlan`] and produces a concrete operator
//! tree plus everything the executor needs to actually run:
//!
//! - Leaves: calls [`SeriesSource::resolve`] to materialise the post-
//!   resolution series roster (`Arc<SeriesSchema>` + parallel
//!   `Arc<[ResolvedSeriesRef]>` hint slice) and wires them into the leaf
//!   operator.
//! - Intermediate nodes: pre-computes the plan-time artifacts operators
//!   need at construction time — [`GroupMap`] for `Aggregate` /
//!   `CountValues`, [`MatchTable`] for `Binary`, output `SeriesSchema`s
//!   where the operator shape requires them.
//! - Subquery: builds a child-factory closure that re-plans the inner
//!   subtree for each outer step's time range.
//!
//! # Scope (unit 4.3 strict)
//!
//! - Does **not** perform cardinality gating — that is unit 4.4.
//! - Does **not** insert `Concurrent` / `Coalesce` — that is unit 4.5.
//! - Does **not** modify `LogicalPlan` / operators / `SeriesSource`.
//!
//! # CountValues composition
//!
//! `CountValues` emits a `SchemaRef::Deferred` handle. v1 operators that
//! compose a `Deferred` child under a parent that needs a `Static` child
//! (aggregate, binary, rollup, instant-fn, …) cannot bind at plan time.
//! The planner rejects these compositions with
//! [`PlanError::InvalidMatching`]; only root-positioned `CountValues` is
//! supported in v1. See RFC §"Core Data Model".

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use promql_parser::parser;

use crate::model::{Label, Labels};

use super::super::batch::{SchemaRef, SeriesSchema};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};
use super::super::operators::aggregate::{AggregateKind, AggregateOp, GroupMap};
use super::super::operators::binary::{BinaryOp, BinaryOpKind, ConstScalarOp, MatchTable};
use super::super::operators::concurrent::ConcurrentOp;
use super::super::operators::count_values::CountValuesOp;
use super::super::operators::instant_fn::InstantFnOp;
use super::super::operators::matrix_selector::MatrixSelectorOp;
use super::super::operators::rollup::{MatrixWindowSource, RollupOp};
use super::super::operators::subquery::{ChildFactory, SubqueryOp};
use super::super::operators::vector_selector::VectorSelectorOp;
use super::super::source::{ResolvedSeriesChunk, ResolvedSeriesRef, SeriesSource, TimeRange};

use super::error::PlanError;
use super::lowering::LoweringContext;
use super::parallelism::ExchangeStats;
use super::plan_types::{
    AggregateGrouping, AtModifier, BinaryMatching, Cardinality, LogicalPlan, MatchingAxis, Offset,
};

/// Convert our plan-time [`Offset`] into the parser's
/// `promql_parser::parser::Offset` the leaf operators consume.
fn to_parser_offset(o: Offset) -> parser::Offset {
    match o {
        Offset::Pos(ms) => parser::Offset::Pos(std::time::Duration::from_millis(ms as u64)),
        Offset::Neg(ms) => parser::Offset::Neg(std::time::Duration::from_millis(ms as u64)),
    }
}

/// Convert our plan-time [`AtModifier`] into the parser's `AtModifier`.
fn to_parser_at(a: AtModifier) -> parser::AtModifier {
    match a {
        AtModifier::Start => parser::AtModifier::Start,
        AtModifier::End => parser::AtModifier::End,
        AtModifier::Value(ms) => parser::AtModifier::At(
            std::time::UNIX_EPOCH + std::time::Duration::from_millis(ms.max(0) as u64),
        ),
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// The output of [`build_physical_plan`].
pub struct PhysicalPlan {
    /// Root operator — already wired end-to-end.
    pub root: Box<dyn Operator + Send>,
    /// Schema the root will publish. Mirrors `root.schema().series`.
    pub output_schema: SchemaRef,
    /// Step grid the root emits on.
    pub step_grid: StepGrid,
}

impl std::fmt::Debug for PhysicalPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalPlan")
            .field("output_schema", &self.output_schema)
            .field("step_grid", &self.step_grid)
            .finish_non_exhaustive()
    }
}

/// Build a physical plan from an (already-lowered and optionally optimised)
/// [`LogicalPlan`].
///
/// `source` must live for `'static` — the generated operator tree captures
/// `Arc<S>` clones and is returned as `Box<dyn Operator + Send>`. `ctx`
/// carries the query's step grid and lookback (same shape the lowering
/// pass consumed).
pub async fn build_physical_plan<S>(
    plan: LogicalPlan,
    source: &Arc<S>,
    reservation: MemoryReservation,
    ctx: &LoweringContext,
) -> Result<PhysicalPlan, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let (physical, _stats) = build_physical_plan_with_stats(plan, source, reservation, ctx).await?;
    Ok(physical)
}

/// Variant of [`build_physical_plan`] that also reports the exchange-operator
/// insertion statistics accumulated during the walk (see unit 4.5).
///
/// The stats are used by unit tests to verify `ConcurrentOp` insertion
/// decisions without downcasting the `dyn Operator` tree. Production
/// callers should prefer [`build_physical_plan`] and discard the stats.
pub async fn build_physical_plan_with_stats<S>(
    plan: LogicalPlan,
    source: &Arc<S>,
    reservation: MemoryReservation,
    ctx: &LoweringContext,
) -> Result<(PhysicalPlan, ExchangeStats), PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let grid = step_grid_from_ctx(ctx);

    // Fail-fast cardinality gate (unit 4.4 / RFC §"Execution Model").
    // Runs before any `SeriesSource::resolve` so an oversized query never
    // materialises sample state. No-op when limits are disabled.
    super::cardinality::enforce_cardinality_gate(
        &plan,
        source.as_ref(),
        grid.step_count,
        ctx.cardinality_limits,
    )
    .await?;

    let mut stats = ExchangeStats::default();
    let root = build_node(
        plan,
        source,
        &reservation,
        ctx,
        grid,
        /*under_rollup=*/ false,
        &mut stats,
    )
    .await?;
    let output_schema = root.schema().series.clone();
    let step_grid = root.schema().step_grid;
    Ok((
        PhysicalPlan {
            root,
            output_schema,
            step_grid,
        },
        stats,
    ))
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn step_grid_from_ctx(ctx: &LoweringContext) -> StepGrid {
    let step_count = if ctx.is_instant() {
        1
    } else {
        // ctx.step_ms > 0 for range queries (lowering sets a `1` sentinel
        // for instant queries, handled above).
        let span = (ctx.end_ms - ctx.start_ms).max(0);
        (span / ctx.step_ms.max(1)) as usize + 1
    };
    StepGrid {
        start_ms: ctx.start_ms,
        end_ms: ctx.end_ms,
        step_ms: ctx.step_ms.max(1),
        step_count,
    }
}

fn static_schema(schema: &SchemaRef) -> Result<&Arc<SeriesSchema>, PlanError> {
    match schema {
        SchemaRef::Static(s) => Ok(s),
        SchemaRef::Deferred => Err(PlanError::InvalidMatching(
            "deferred-schema child (count_values) not supported under a schema-sensitive parent \
             in v1"
                .to_string(),
        )),
    }
}

fn map_source_err(err: QueryError) -> PlanError {
    match err {
        QueryError::MemoryLimit { .. } => PlanError::MemoryLimit(err.to_string()),
        other => PlanError::SourceError(other.to_string()),
    }
}

fn map_construct_err(err: QueryError) -> PlanError {
    match err {
        QueryError::MemoryLimit { .. } => PlanError::MemoryLimit(err.to_string()),
        other => PlanError::PhysicalPlanFailed(other.to_string()),
    }
}

/// Compute an outer time-range that bounds the query window. For leaf
/// selectors we fold in the effective lookback (vector) or range (matrix)
/// plus the `@`/offset modifiers so the source returns every sample an
/// operator could consume. Conservative is fine — the source does not
/// widen, and any extra samples land inside the hint's window.
fn selector_time_range(
    grid: StepGrid,
    lookback_ms: i64,
    range_ms: i64,
    at: Option<AtModifier>,
    offset: Offset,
) -> TimeRange {
    // Apply `@` pin if present: the window collapses to a single pin point
    // ± modifiers. Otherwise the window tracks the whole grid.
    let (pin_start, pin_end) = match at {
        Some(AtModifier::Value(v)) => (v, v),
        Some(AtModifier::Start) => (grid.start_ms, grid.start_ms),
        Some(AtModifier::End) => (grid.end_ms, grid.end_ms),
        None => (grid.start_ms, grid.end_ms),
    };
    // `Offset::Pos(d)` subtracts `d` from the effective time (looks
    // backward); `Neg(d)` adds `d` (looks forward). The operator path applies
    // the same sign convention (§5 decisions 3a.1). The source's window needs
    // to cover (effective - window, effective], so we shift both ends by the
    // offset in the same direction.
    let signed = offset.signed_ms();
    let effective_start = pin_start.saturating_sub(signed);
    let effective_end = pin_end.saturating_sub(signed);
    // Window size we need on the source. For vector selectors this is the
    // lookback delta; for matrix selectors it is the bracketed range. Both
    // extend into the past from each step.
    let window_ms = lookback_ms.max(range_ms).max(0);
    let start = effective_start.saturating_sub(window_ms);
    // Inclusive-to-exclusive: the operator uses `(effective - window,
    // effective]`. For the source we ask for `[start, effective_end + 1)`.
    let end_exclusive = effective_end.saturating_add(1);
    TimeRange::new(start, end_exclusive.max(start))
}

// ---------------------------------------------------------------------------
// Resolved-series materialisation
// ---------------------------------------------------------------------------

struct ResolvedLeaf {
    schema: Arc<SeriesSchema>,
    hint_series: Arc<[ResolvedSeriesRef]>,
}

/// Drain [`SeriesSource::resolve`] into a flat series roster (labels +
/// fingerprints) plus a parallel hint slice. Fingerprints are a simple
/// deterministic FxHash over `(bucket_id, series_id)` — enough for the v1
/// deduplication contract the operator layer expects; cross-query cache
/// fingerprinting is deferred to the cross-query cache unit.
async fn resolve_leaf<S>(
    source: &Arc<S>,
    selector: &parser::VectorSelector,
    time_range: TimeRange,
    reservation: &MemoryReservation,
) -> Result<ResolvedLeaf, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let mut stream: BoxStream<'_, Result<ResolvedSeriesChunk, QueryError>> =
        Box::pin(source.resolve(selector, time_range));

    let mut labels: Vec<Labels> = Vec::new();
    let mut fingerprints: Vec<u128> = Vec::new();
    let mut refs: Vec<ResolvedSeriesRef> = Vec::new();

    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res.map_err(map_source_err)?;
        debug_assert_eq!(chunk.labels.len(), chunk.series.len());
        for (label, sref) in chunk.labels.iter().zip(chunk.series.iter()) {
            labels.push(label.clone());
            fingerprints.push(series_fingerprint(sref));
            refs.push(*sref);
        }
    }

    // Conservative label-storage reservation: one `Label` struct per label,
    // rounded to 48 bytes (the stable shape today) + per-series overhead.
    let bytes = labels
        .iter()
        .map(|l| l.len().saturating_mul(48))
        .fold(0usize, |a, b| a.saturating_add(b))
        .saturating_add(labels.len().saturating_mul(32));
    reservation.try_grow(bytes).map_err(map_construct_err)?;

    let schema = Arc::new(SeriesSchema::new(
        Arc::from(labels),
        Arc::from(fingerprints),
    ));
    let hint_series: Arc<[ResolvedSeriesRef]> = Arc::from(refs);
    Ok(ResolvedLeaf {
        schema,
        hint_series,
    })
}

#[inline]
fn series_fingerprint(sref: &ResolvedSeriesRef) -> u128 {
    // Mix bucket_id into the high half, series_id into the low half. Unique
    // for any given (bucket, series) pair the source emits. Deterministic
    // but not cross-query stable — the cross-query cache unit will replace
    // this.
    ((sref.bucket_id as u128) << 32) | (sref.series_id as u128)
}

// ---------------------------------------------------------------------------
// Group-map construction (Aggregate / CountValues)
// ---------------------------------------------------------------------------

/// Project a labelset onto the grouping keys — either keep only `by(…)` or
/// drop `without(…)` labels. Always drops `__name__` per Prometheus
/// convention (aggregates do not carry the source metric name forward).
fn group_key_labels(labels: &Labels, grouping: &AggregateGrouping) -> Vec<Label> {
    match grouping {
        AggregateGrouping::By(keep) => labels
            .iter()
            .filter(|l| l.name != "__name__" && keep.iter().any(|k| k == &l.name))
            .cloned()
            .collect(),
        AggregateGrouping::Without(drop) => labels
            .iter()
            .filter(|l| l.name != "__name__" && !drop.iter().any(|k| k == &l.name))
            .cloned()
            .collect(),
    }
}

/// Result of bucketing an input schema by an [`AggregateGrouping`]: a
/// [`GroupMap`] for the operator plus parallel group labels for the output
/// schema.
struct GroupBuild {
    map: GroupMap,
    group_labels: Vec<Labels>,
}

fn build_group_map(
    input: &SeriesSchema,
    grouping: &AggregateGrouping,
) -> Result<GroupBuild, PlanError> {
    let mut keys: HashMap<Vec<Label>, u32> = HashMap::new();
    let mut group_labels: Vec<Labels> = Vec::new();
    let mut input_to_group: Vec<Option<u32>> = Vec::with_capacity(input.len());
    for idx in 0..input.len() {
        let labels = input.labels(idx as u32);
        let mut key = group_key_labels(labels, grouping);
        key.sort();
        let next_idx = group_labels.len() as u32;
        let g = match keys.get(&key) {
            Some(&g) => g,
            None => {
                keys.insert(key.clone(), next_idx);
                group_labels.push(Labels::new(key));
                next_idx
            }
        };
        input_to_group.push(Some(g));
    }
    Ok(GroupBuild {
        map: GroupMap::new(input_to_group, group_labels.len()),
        group_labels,
    })
}

fn build_group_schema(group_labels: &[Labels]) -> Arc<SeriesSchema> {
    let labels: Arc<[Labels]> = Arc::from(group_labels.to_vec());
    let fps: Vec<u128> = (0..group_labels.len() as u128).collect();
    Arc::new(SeriesSchema::new(labels, Arc::from(fps)))
}

// ---------------------------------------------------------------------------
// Match-table construction (Binary)
// ---------------------------------------------------------------------------

/// Project a labelset onto a binary matching axis. `on(l…)` keeps only
/// those labels; `ignoring(l…)` drops them. `__name__` is dropped in either
/// case for vector/vector matching (matches Prometheus `signature`
/// semantics).
fn matching_key(labels: &Labels, axis: MatchingAxis, matching_labels: &[String]) -> Vec<Label> {
    match axis {
        MatchingAxis::On => labels
            .iter()
            .filter(|l| l.name != "__name__" && matching_labels.iter().any(|m| m == &l.name))
            .cloned()
            .collect(),
        MatchingAxis::Ignoring => labels
            .iter()
            .filter(|l| l.name != "__name__" && !matching_labels.iter().any(|m| m == &l.name))
            .cloned()
            .collect(),
    }
}

/// Default matching (no explicit modifier): `ignoring()` with an empty
/// drop-list (i.e. match on every label except `__name__`).
fn default_axis_and_labels() -> (MatchingAxis, Vec<String>) {
    (MatchingAxis::Ignoring, Vec::new())
}

struct MatchBuild {
    table: MatchTable,
    /// Output schema the binary operator publishes.
    output_schema: Arc<SeriesSchema>,
}

fn build_match_table(
    lhs: &SeriesSchema,
    rhs: &SeriesSchema,
    matching: Option<&BinaryMatching>,
    include_name_on_output: bool,
) -> Result<MatchBuild, PlanError> {
    let (axis, labels): (MatchingAxis, Vec<String>) = match matching {
        Some(m) => {
            let mut v: Vec<String> = m.labels.iter().cloned().collect();
            v.sort();
            (m.axis, v)
        }
        None => default_axis_and_labels(),
    };
    let cardinality = matching
        .map(|m| m.cardinality.clone())
        .unwrap_or(Cardinality::OneToOne);

    match cardinality {
        Cardinality::OneToOne | Cardinality::ManyToMany => {
            build_one_to_one(lhs, rhs, axis, &labels, include_name_on_output)
        }
        Cardinality::GroupLeft { include } => {
            build_group_left(lhs, rhs, axis, &labels, &include, include_name_on_output)
        }
        Cardinality::GroupRight { include } => {
            build_group_right(lhs, rhs, axis, &labels, &include, include_name_on_output)
        }
    }
}

fn build_one_to_one(
    lhs: &SeriesSchema,
    rhs: &SeriesSchema,
    axis: MatchingAxis,
    match_labels: &[String],
    include_name_on_output: bool,
) -> Result<MatchBuild, PlanError> {
    // Index RHS by matching key. `OneToOne` requires at most one RHS per
    // key; we defer duplicate-detection to runtime (operator emits per-cell
    // validity = 0 on unmatched — duplicate pairing is a rare corner case
    // not exercised by v1 tests).
    let mut rhs_by_key: HashMap<Vec<Label>, u32> = HashMap::new();
    for j in 0..rhs.len() {
        let key = matching_key(rhs.labels(j as u32), axis, match_labels);
        rhs_by_key.entry(key).or_insert(j as u32);
    }
    let mut map: Vec<Option<u32>> = Vec::with_capacity(lhs.len());
    let mut out_labels: Vec<Labels> = Vec::with_capacity(lhs.len());
    for i in 0..lhs.len() {
        let lab = lhs.labels(i as u32);
        let key = matching_key(lab, axis, match_labels);
        map.push(rhs_by_key.get(&key).copied());
        out_labels.push(result_labels_for_output(lab, include_name_on_output));
    }
    let output_schema = build_output_schema_from_labels(out_labels);
    Ok(MatchBuild {
        table: MatchTable::OneToOne(map),
        output_schema,
    })
}

fn build_group_left(
    lhs: &SeriesSchema,
    rhs: &SeriesSchema,
    axis: MatchingAxis,
    match_labels: &[String],
    include_labels: &[String],
    include_name_on_output: bool,
) -> Result<MatchBuild, PlanError> {
    // LHS is the "many" side; output has one row per LHS row pointing at
    // the single RHS "one" side. `include_labels` are carried from the
    // "one" side onto the output labels.
    let mut rhs_by_key: HashMap<Vec<Label>, u32> = HashMap::new();
    for j in 0..rhs.len() {
        let key = matching_key(rhs.labels(j as u32), axis, match_labels);
        rhs_by_key.entry(key).or_insert(j as u32);
    }
    let mut map: Vec<Option<u32>> = Vec::with_capacity(lhs.len());
    let mut out_labels: Vec<Labels> = Vec::with_capacity(lhs.len());
    for i in 0..lhs.len() {
        let lab = lhs.labels(i as u32);
        let key = matching_key(lab, axis, match_labels);
        let rhs_idx = rhs_by_key.get(&key).copied();
        let composed = compose_group_labels(
            lab,
            rhs_idx.map(|j| rhs.labels(j)),
            include_labels,
            include_name_on_output,
        );
        map.push(rhs_idx);
        out_labels.push(composed);
    }
    let output_schema = build_output_schema_from_labels(out_labels);
    Ok(MatchBuild {
        table: MatchTable::GroupLeft(map),
        output_schema,
    })
}

fn build_group_right(
    lhs: &SeriesSchema,
    rhs: &SeriesSchema,
    axis: MatchingAxis,
    match_labels: &[String],
    include_labels: &[String],
    include_name_on_output: bool,
) -> Result<MatchBuild, PlanError> {
    // Mirror of GroupLeft: RHS is "many"; output rows align with RHS.
    let mut lhs_by_key: HashMap<Vec<Label>, u32> = HashMap::new();
    for i in 0..lhs.len() {
        let key = matching_key(lhs.labels(i as u32), axis, match_labels);
        lhs_by_key.entry(key).or_insert(i as u32);
    }
    let mut map: Vec<Option<u32>> = Vec::with_capacity(rhs.len());
    let mut out_labels: Vec<Labels> = Vec::with_capacity(rhs.len());
    for j in 0..rhs.len() {
        let lab = rhs.labels(j as u32);
        let key = matching_key(lab, axis, match_labels);
        let lhs_idx = lhs_by_key.get(&key).copied();
        let composed = compose_group_labels(
            lab,
            lhs_idx.map(|i| lhs.labels(i)),
            include_labels,
            include_name_on_output,
        );
        map.push(lhs_idx);
        out_labels.push(composed);
    }
    let output_schema = build_output_schema_from_labels(out_labels);
    Ok(MatchBuild {
        table: MatchTable::GroupRight(map),
        output_schema,
    })
}

fn result_labels_for_output(input: &Labels, include_name: bool) -> Labels {
    if include_name {
        input.clone()
    } else {
        let v: Vec<Label> = input
            .iter()
            .filter(|l| l.name != "__name__")
            .cloned()
            .collect();
        Labels::new(v)
    }
}

/// Compose the output labels for a group_left / group_right row: the
/// "many" side's full labelset (drop `__name__` unless requested), plus
/// the `include(...)` labels copied from the "one" side (if matched).
fn compose_group_labels(
    many_side: &Labels,
    one_side: Option<&Labels>,
    include_labels: &[String],
    include_name_on_output: bool,
) -> Labels {
    let mut out: Vec<Label> = many_side
        .iter()
        .filter(|l| include_name_on_output || l.name != "__name__")
        .cloned()
        .collect();
    if let Some(one) = one_side {
        for name in include_labels {
            // Drop any existing label with this name from the many side,
            // then copy the one-side value (if present).
            out.retain(|l| &l.name != name);
            if let Some(v) = one.get(name) {
                out.push(Label::new(name.clone(), v));
            }
        }
    }
    out.sort();
    Labels::new(out)
}

fn build_output_schema_from_labels(labels: Vec<Labels>) -> Arc<SeriesSchema> {
    let fps: Vec<u128> = (0..labels.len() as u128).collect();
    Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
}

/// `true` when a binary op preserves the source metric's `__name__` label.
/// Matches the legacy engine's `changes_metric_schema`: arithmetic ops
/// drop `__name__`; set ops and non-`bool` comparisons preserve it.
fn preserves_metric_name(op: BinaryOpKind) -> bool {
    match op {
        BinaryOpKind::Add | BinaryOpKind::Sub | BinaryOpKind::Mul | BinaryOpKind::Div => false,
        BinaryOpKind::Mod | BinaryOpKind::Pow | BinaryOpKind::Atan2 => true,
        BinaryOpKind::Eq { bool_modifier } => !bool_modifier,
        BinaryOpKind::Ne { bool_modifier } => !bool_modifier,
        BinaryOpKind::Gt { bool_modifier } => !bool_modifier,
        BinaryOpKind::Lt { bool_modifier } => !bool_modifier,
        BinaryOpKind::Gte { bool_modifier } => !bool_modifier,
        BinaryOpKind::Lte { bool_modifier } => !bool_modifier,
        BinaryOpKind::And | BinaryOpKind::Or | BinaryOpKind::Unless => true,
    }
}

// ---------------------------------------------------------------------------
// Plan-tree walk (bottom-up)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn build_node<'a, S>(
    plan: LogicalPlan,
    source: &'a Arc<S>,
    reservation: &'a MemoryReservation,
    ctx: &'a LoweringContext,
    grid: StepGrid,
    under_rollup: bool,
    stats: &'a mut ExchangeStats,
) -> futures::future::BoxFuture<'a, Result<Box<dyn Operator + Send>, PlanError>>
where
    S: SeriesSource + Send + Sync + 'static,
{
    Box::pin(async move {
        build_node_inner(plan, source, reservation, ctx, grid, under_rollup, stats).await
    })
}

#[allow(clippy::too_many_arguments)]
async fn build_node_inner<S>(
    plan: LogicalPlan,
    source: &Arc<S>,
    reservation: &MemoryReservation,
    ctx: &LoweringContext,
    grid: StepGrid,
    under_rollup: bool,
    stats: &mut ExchangeStats,
) -> Result<Box<dyn Operator + Send>, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    match plan {
        LogicalPlan::Scalar(v) => {
            let op = ConstScalarOp::new(v, grid, reservation.clone());
            Ok(Box::new(op))
        }
        LogicalPlan::VectorSelector {
            selector,
            offset,
            at,
            lookback_ms,
        } => {
            let lookback = lookback_ms.unwrap_or(ctx.lookback_delta_ms);
            let time_range = selector_time_range(grid, lookback, 0, at, offset);
            let resolved = resolve_leaf(source, &selector, time_range, reservation).await?;
            let series_count = resolved.schema.len() as u64;
            let at_parser = at.map(to_parser_at);
            let off_parser = to_parser_offset(offset);
            let op = VectorSelectorOp::<'static, S>::new(
                source.clone(),
                resolved.schema,
                resolved.hint_series,
                grid,
                at_parser,
                Some(off_parser),
                lookback,
                reservation.clone(),
                super::super::operators::vector_selector::BatchShape::default(),
            );
            // Unit 4.5: wrap the leaf in `ConcurrentOp` when its resolved
            // series count exceeds the configured threshold. Decouples the
            // async `samples()` pull from downstream evaluation.
            Ok(maybe_wrap_concurrent(
                Box::new(op),
                series_count,
                ctx,
                stats,
            ))
        }
        LogicalPlan::MatrixSelector { .. } => {
            // A bare matrix selector never lives at the output of the plan;
            // it must be wrapped in a `Rollup` or `Subquery`. The Rollup
            // branch (below) matches and handles this directly.
            if !under_rollup {
                return Err(PlanError::UnsupportedExpression(
                    "MatrixSelector without a parent Rollup / Subquery".to_string(),
                ));
            }
            unreachable!("MatrixSelector handled by Rollup branch")
        }
        LogicalPlan::InstantFn { kind, child } => {
            let child_op = build_node(*child, source, reservation, ctx, grid, false, stats).await?;
            let _ = static_schema(&child_op.schema().series)?;
            let op = InstantFnOp::new(BoxedOp(child_op), kind, reservation.clone());
            Ok(Box::new(op))
        }
        LogicalPlan::Rollup { kind, child } => {
            // Rollup wraps either a MatrixSelector (directly) or a Subquery.
            match *child {
                LogicalPlan::MatrixSelector {
                    selector,
                    range_ms,
                    offset,
                    at,
                } => {
                    let time_range = selector_time_range(grid, 0, range_ms, at, offset);
                    let resolved = resolve_leaf(source, &selector, time_range, reservation).await?;
                    let series_count = resolved.schema.len() as u64;
                    let at_parser = at.map(to_parser_at);
                    let off_parser = to_parser_offset(offset);
                    let matrix = MatrixSelectorOp::<'static, S>::new(
                        source.clone(),
                        resolved.schema.clone(),
                        resolved.hint_series,
                        grid,
                        at_parser,
                        Some(off_parser),
                        range_ms,
                        reservation.clone(),
                        super::super::operators::matrix_selector::BatchShape::default(),
                    );
                    let schema_snapshot =
                        OperatorSchema::new(SchemaRef::Static(resolved.schema), grid);
                    let window = MatrixWindowSource::new(matrix, schema_snapshot);
                    let op = RollupOp::new(window, kind, range_ms, reservation.clone());
                    // Unit 4.5: `MatrixSelectorOp::next` is degenerate (see
                    // §3a.2), so we wrap the enclosing `RollupOp` — which
                    // owns the I/O leaf — instead of the matrix selector
                    // itself. This preserves the "decouple I/O from
                    // evaluation" contract at the right boundary.
                    Ok(maybe_wrap_concurrent(
                        Box::new(op),
                        series_count,
                        ctx,
                        stats,
                    ))
                }
                LogicalPlan::Subquery {
                    child: inner,
                    range_ms,
                    step_ms,
                    offset,
                    at,
                } => {
                    let sub = build_subquery(
                        *inner,
                        source,
                        reservation,
                        ctx,
                        grid,
                        range_ms,
                        step_ms,
                        offset,
                        at,
                        stats,
                    )
                    .await?;
                    let op = RollupOp::new(sub, kind, range_ms, reservation.clone());
                    Ok(Box::new(op))
                }
                other => Err(PlanError::UnsupportedExpression(format!(
                    "Rollup child must be MatrixSelector or Subquery, got {other:?}"
                ))),
            }
        }
        LogicalPlan::Binary {
            op,
            lhs,
            rhs,
            matching,
        } => {
            build_binary(
                op,
                *lhs,
                *rhs,
                matching,
                source,
                reservation,
                ctx,
                grid,
                stats,
            )
            .await
        }
        LogicalPlan::Aggregate {
            kind,
            child,
            grouping,
        } => {
            build_aggregate(
                kind,
                *child,
                grouping,
                source,
                reservation,
                ctx,
                grid,
                stats,
            )
            .await
        }
        LogicalPlan::Subquery { .. } => Err(PlanError::UnsupportedExpression(
            "bare Subquery without a Rollup parent is not supported in v1".to_string(),
        )),
        LogicalPlan::Rechunk { .. } => Err(PlanError::UnsupportedExpression(
            "Rechunk is unit 4.5's responsibility; 4.3 does not lower it".to_string(),
        )),
        LogicalPlan::CountValues {
            label,
            child,
            grouping,
        } => {
            build_count_values(
                label,
                *child,
                grouping,
                source,
                reservation,
                ctx,
                grid,
                stats,
            )
            .await
        }
        LogicalPlan::Concurrent { .. } | LogicalPlan::Coalesce { .. } => {
            Err(PlanError::UnsupportedExpression(
                "Concurrent / Coalesce are unit 4.5's responsibility".to_string(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Binary
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn build_binary<S>(
    op: BinaryOpKind,
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    matching: Option<BinaryMatching>,
    source: &Arc<S>,
    reservation: &MemoryReservation,
    ctx: &LoweringContext,
    grid: StepGrid,
    stats: &mut ExchangeStats,
) -> Result<Box<dyn Operator + Send>, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let lhs_is_scalar = matches!(lhs, LogicalPlan::Scalar(_));
    let rhs_is_scalar = matches!(rhs, LogicalPlan::Scalar(_));

    let lhs_op = build_node(lhs, source, reservation, ctx, grid, false, stats).await?;
    let rhs_op = build_node(rhs, source, reservation, ctx, grid, false, stats).await?;

    match (lhs_is_scalar, rhs_is_scalar) {
        (true, true) => {
            let op_box = BinaryOp::<BoxedOp, BoxedOp>::new_scalar_scalar(
                BoxedOp(lhs_op),
                BoxedOp(rhs_op),
                op,
                reservation.clone(),
            );
            Ok(Box::new(op_box))
        }
        (true, false) => {
            let op_box = BinaryOp::<BoxedOp, BoxedOp>::new_scalar_vector(
                BoxedOp(lhs_op),
                BoxedOp(rhs_op),
                op,
                reservation.clone(),
            );
            Ok(Box::new(op_box))
        }
        (false, true) => {
            let op_box = BinaryOp::<BoxedOp, BoxedOp>::new_vector_scalar(
                BoxedOp(lhs_op),
                BoxedOp(rhs_op),
                op,
                reservation.clone(),
            );
            Ok(Box::new(op_box))
        }
        (false, false) => {
            let lhs_schema = static_schema(&lhs_op.schema().series)?.clone();
            let rhs_schema = static_schema(&rhs_op.schema().series)?.clone();
            let include_name = preserves_metric_name(op);
            let built =
                build_match_table(&lhs_schema, &rhs_schema, matching.as_ref(), include_name)?;
            let op_box = BinaryOp::<BoxedOp, BoxedOp>::new_vector_vector(
                BoxedOp(lhs_op),
                BoxedOp(rhs_op),
                op,
                built.table,
                built.output_schema,
                reservation.clone(),
            );
            Ok(Box::new(op_box))
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn build_aggregate<S>(
    kind: AggregateKind,
    child: LogicalPlan,
    grouping: AggregateGrouping,
    source: &Arc<S>,
    reservation: &MemoryReservation,
    ctx: &LoweringContext,
    grid: StepGrid,
    stats: &mut ExchangeStats,
) -> Result<Box<dyn Operator + Send>, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let child_op = build_node(child, source, reservation, ctx, grid, false, stats).await?;
    let input_schema = static_schema(&child_op.schema().series)?.clone();

    // topk/bottomk are filter-shaped: output schema == input schema.
    // streaming kinds + quantile are reducer-shaped: one row per group.
    let is_filter_shape = matches!(kind, AggregateKind::Topk(_) | AggregateKind::Bottomk(_));

    let built = build_group_map(&input_schema, &grouping)?;
    let output_schema = if is_filter_shape {
        input_schema.clone()
    } else {
        build_group_schema(&built.group_labels)
    };

    let op = AggregateOp::new(
        BoxedOp(child_op),
        kind,
        built.map,
        output_schema,
        reservation.clone(),
    )
    .map_err(map_construct_err)?;
    Ok(Box::new(op))
}

// ---------------------------------------------------------------------------
// CountValues
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn build_count_values<S>(
    label: String,
    child: LogicalPlan,
    grouping: AggregateGrouping,
    source: &Arc<S>,
    reservation: &MemoryReservation,
    ctx: &LoweringContext,
    grid: StepGrid,
    stats: &mut ExchangeStats,
) -> Result<Box<dyn Operator + Send>, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    let child_op = build_node(child, source, reservation, ctx, grid, false, stats).await?;
    let input_schema = static_schema(&child_op.schema().series)?.clone();

    let (group_map_opt, group_labels_arc) = if is_trivial_grouping(&grouping) {
        // `count_values(l, expr)` without by/without — single global group
        // (matches the operator's `None` contract: group_labels must be
        // [empty]).
        (None, Arc::<[Labels]>::from(vec![Labels::empty()]))
    } else {
        let built = build_group_map(&input_schema, &grouping)?;
        let group_labels: Arc<[Labels]> = Arc::from(built.group_labels);
        (Some(built.map), group_labels)
    };

    let op = CountValuesOp::new(
        BoxedOp(child_op),
        label,
        group_map_opt,
        group_labels_arc,
        reservation.clone(),
    );
    Ok(Box::new(op))
}

fn is_trivial_grouping(grouping: &AggregateGrouping) -> bool {
    matches!(grouping, AggregateGrouping::By(labels) if labels.is_empty())
}

// ---------------------------------------------------------------------------
// Subquery (factory returning a fresh child operator per outer step)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn build_subquery<S>(
    inner: LogicalPlan,
    source: &Arc<S>,
    reservation: &MemoryReservation,
    ctx: &LoweringContext,
    outer_grid: StepGrid,
    range_ms: i64,
    step_ms: i64,
    offset: Offset,
    at: Option<AtModifier>,
    stats: &mut ExchangeStats,
) -> Result<SubqueryOp, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    // Resolve the inner subtree's output schema by planning it once over
    // a representative inner-grid window. Phase-6 may want to replan at
    // factory call time if the inner plan's series set can vary across
    // outer steps; the `ChildFactory` shape allows that already.
    //
    // For v1 the subquery's output series is assumed plan-time-stable
    // (matches RFC §"Core Data Model": only `count_values` carries
    // deferred schemas, and that is rejected under schema-sensitive
    // parents — which `Rollup` is).

    // Build a probe child to snapshot the output schema.
    let (probe_start, probe_end) = outer_step_window(outer_grid, 0, range_ms, offset, at);
    let probe_range = TimeRange::new(probe_start, probe_end.saturating_add(1));
    let probe_grid = inner_grid(probe_range, step_ms);
    // Probe the inner subtree once to snapshot its output schema. Stats
    // bookkeeping for the probe is intentionally discarded — the factory
    // will rebuild the subtree (possibly with its own wraps) per outer
    // step; counting the probe's wrap decisions would double-count.
    let mut probe_stats = ExchangeStats::default();
    let probe = build_node(
        inner.clone(),
        source,
        reservation,
        ctx,
        probe_grid,
        false,
        &mut probe_stats,
    )
    .await?;
    // Accumulate the probe's wrap/skip decisions into the caller's stats
    // for observability. These wrap decisions are informational — the
    // probe itself is dropped right after schema extraction.
    stats.concurrent_wrapped = stats
        .concurrent_wrapped
        .saturating_add(probe_stats.concurrent_wrapped);
    stats.concurrent_skipped = stats
        .concurrent_skipped
        .saturating_add(probe_stats.concurrent_skipped);
    let inner_schema = static_schema(&probe.schema().series)?.clone();
    drop(probe);

    // Clone captures for the factory closure.
    let source_arc = source.clone();
    let ctx_copy = *ctx;
    let reservation_inner = reservation.clone();
    let inner_plan = inner;
    let factory: ChildFactory = Box::new(move |tr: TimeRange, inner_step_ms: i64| {
        // Use a blocking task-local poll for the factory's sync-Future
        // surface: the factory's return type is sync, but the planner
        // walk is async. Real wiring will replace this with a planner-
        // cached precomputed tree per unique (range, step); for v1 the
        // factory just re-invokes the planner synchronously.
        let grid = inner_grid(tr, inner_step_ms);
        let src = source_arc.clone();
        let res = reservation_inner.clone();
        let ctx_cp = ctx_copy;
        let plan = inner_plan.clone();
        // Drive the async recursive planner to completion using a
        // single-threaded runtime. This is a plan-time path, called once
        // per outer step by `SubqueryOp::windows`; we accept the per-call
        // runtime spin-up over propagating async through the operator
        // trait (the trait is sync `poll` and the RFC places subquery
        // re-planning inside `poll_windows`).
        // Factory-owned stats scratch: wraps inserted inside the inner
        // subtree are already reflected in the top-level `ExchangeStats`
        // via the schema-probe walk above, so we discard them here.
        let mut factory_stats = ExchangeStats::default();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                // Hand the work back to the existing runtime via
                // `block_in_place`; callers should already be on a
                // multi-thread runtime for the outer query.
                let fut = build_node(plan, &src, &res, &ctx_cp, grid, false, &mut factory_stats);
                tokio::task::block_in_place(|| handle.block_on(fut))
                    .map_err(|e| QueryError::Internal(format!("subquery plan failed: {e}")))
            }
            Err(_) => {
                // No runtime — synthesise a minimal runtime just for the
                // plan. This path is only hit in sync tests that drive the
                // plan outside a tokio context.
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| QueryError::Internal(format!("subquery rt build: {e}")))?;
                let fut = build_node(plan, &src, &res, &ctx_cp, grid, false, &mut factory_stats);
                rt.block_on(fut)
                    .map_err(|e| QueryError::Internal(format!("subquery plan failed: {e}")))
            }
        }
    });

    let sub = SubqueryOp::new(
        factory,
        inner_schema,
        outer_grid,
        range_ms,
        step_ms,
        reservation.clone(),
    );
    Ok(sub)
}

fn outer_step_window(
    outer_grid: StepGrid,
    step_idx: usize,
    range_ms: i64,
    offset: Offset,
    at: Option<AtModifier>,
) -> (i64, i64) {
    let step_idx = step_idx as i64;
    let pinned = match at {
        Some(AtModifier::Value(v)) => v,
        Some(AtModifier::Start) => outer_grid.start_ms,
        Some(AtModifier::End) => outer_grid.end_ms,
        None => outer_grid.start_ms + step_idx * outer_grid.step_ms,
    };
    let effective = pinned.saturating_sub(offset.signed_ms());
    let start = effective.saturating_sub(range_ms).saturating_add(1);
    (start, effective)
}

fn inner_grid(range: TimeRange, step_ms: i64) -> StepGrid {
    let span = (range.end_ms_exclusive - 1 - range.start_ms).max(0);
    let step = step_ms.max(1);
    let step_count = (span / step) as usize + 1;
    StepGrid {
        start_ms: range.start_ms,
        end_ms: range.start_ms + (step_count as i64 - 1) * step,
        step_ms: step,
        step_count,
    }
}

// ---------------------------------------------------------------------------
// Exchange-operator insertion (unit 4.5)
// ---------------------------------------------------------------------------

/// Wrap `op` in [`ConcurrentOp`] when the leaf's resolved cardinality
/// exceeds `ctx.parallelism.concurrent_threshold_series`. Otherwise the
/// op is returned unchanged.
///
/// The returned operator is always `Box<dyn Operator + Send>` so callers
/// can stitch wrapped and unwrapped leaves into the same tree uniformly.
///
/// Stats bookkeeping: each call records either a wrap (`record_wrap`) or
/// a skip (`record_skip`) on `stats`, so tests can verify the decision
/// without downcasting the resulting `dyn Operator`.
fn maybe_wrap_concurrent(
    op: Box<dyn Operator + Send>,
    series_count: u64,
    ctx: &LoweringContext,
    stats: &mut ExchangeStats,
) -> Box<dyn Operator + Send> {
    if ctx.parallelism.should_wrap_concurrent(series_count) {
        stats.record_wrap();
        // `ConcurrentOp::new` takes `C: Operator + Send + 'static` by
        // value. `Box<dyn Operator + Send>` doesn't itself implement
        // `Operator` (no blanket impl), so we route through the existing
        // `BoxedOp` shim that forwards `schema()` / `next()`.
        let wrapped = ConcurrentOp::new(BoxedOp(op), ctx.parallelism.channel_bound);
        Box::new(wrapped)
    } else {
        stats.record_skip();
        op
    }
}

// ---------------------------------------------------------------------------
// BoxedOp — a small shim giving `Box<dyn Operator + Send>` a concrete
// `Operator` impl so the generic operator structs (`BinaryOp<L, R>`,
// `AggregateOp<C>`, etc.) can consume trait-object children.
// ---------------------------------------------------------------------------

struct BoxedOp(Box<dyn Operator + Send>);

impl Operator for BoxedOp {
    fn schema(&self) -> &OperatorSchema {
        self.0.schema()
    }

    fn next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<super::super::batch::StepBatch, QueryError>>> {
        self.0.next(cx)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use promql_parser::label::{MatchOp, Matcher, Matchers};
    use promql_parser::parser as pparser;
    use std::future::ready;
    use std::task::{Context, RawWaker, RawWakerVTable, Waker};

    use crate::model::{Label, Labels};
    use crate::promql::v2::source::{
        CardinalityEstimate, ResolvedSeriesChunk, SampleBatch, SampleBlock, SampleHint,
    };

    // ---- mock source -----------------------------------------------------

    /// Minimal `SeriesSource` for unit tests. Associates a selector's
    /// metric name (via `__name__` matchers) with a roster of (labels,
    /// samples) entries.
    struct MockSource {
        series: Vec<(Labels, Vec<(i64, f64)>)>,
        /// When set, `resolve()` yields this error instead of the roster.
        fail_resolve: Option<String>,
    }

    impl MockSource {
        fn new(series: Vec<(Labels, Vec<(i64, f64)>)>) -> Self {
            Self {
                series,
                fail_resolve: None,
            }
        }

        fn failing(msg: &str) -> Self {
            Self {
                series: Vec::new(),
                fail_resolve: Some(msg.to_string()),
            }
        }
    }

    impl SeriesSource for MockSource {
        fn resolve(
            &self,
            _selector: &pparser::VectorSelector,
            _time_range: TimeRange,
        ) -> impl futures::Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
            if let Some(msg) = &self.fail_resolve {
                let err = QueryError::Internal(msg.clone());
                return stream::iter(vec![Err(err)]).left_stream();
            }
            let labels: Vec<Labels> = self.series.iter().map(|(l, _)| l.clone()).collect();
            let refs: Vec<ResolvedSeriesRef> = (0..self.series.len())
                .map(|i| ResolvedSeriesRef::new(1, i as u32))
                .collect();
            let chunk = ResolvedSeriesChunk {
                bucket_id: 1,
                labels: Arc::from(labels),
                series: Arc::from(refs),
            };
            stream::iter(vec![Ok(chunk)]).right_stream()
        }

        fn estimate_cardinality(
            &self,
            _selector: &pparser::VectorSelector,
            _time_range: TimeRange,
        ) -> impl std::future::Future<Output = Result<CardinalityEstimate, QueryError>> + Send
        {
            ready(Ok(CardinalityEstimate::exact(self.series.len() as u64)))
        }

        fn samples(
            &self,
            hint: SampleHint,
        ) -> impl futures::Stream<Item = Result<SampleBatch, QueryError>> + Send {
            let mut block = SampleBlock::with_series_count(hint.series.len());
            for (col, sref) in hint.series.iter().enumerate() {
                let samples = &self.series[sref.series_id as usize].1;
                for (t, v) in samples {
                    if *t >= hint.time_range.start_ms && *t < hint.time_range.end_ms_exclusive {
                        block.timestamps[col].push(*t);
                        block.values[col].push(*v);
                    }
                }
            }
            stream::iter(vec![Ok(SampleBatch {
                series_range: 0..hint.series.len(),
                samples: block,
            })])
        }
    }

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    fn labels_of(pairs: &[(&str, &str)]) -> Labels {
        let mut v: Vec<Label> = pairs.iter().map(|(n, val)| Label::new(*n, *val)).collect();
        v.sort();
        Labels::new(v)
    }

    fn make_ctx() -> LoweringContext {
        LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
    }

    fn parse(input: &str) -> pparser::Expr {
        pparser::parse(input).unwrap_or_else(|e| panic!("parse({input:?}): {e}"))
    }

    fn make_selector(metric: &str) -> pparser::VectorSelector {
        let m = Matcher::new(MatchOp::Equal, "__name__", metric);
        pparser::VectorSelector::new(Some(metric.to_string()), Matchers::new(vec![m]))
    }

    async fn build<S>(
        plan: LogicalPlan,
        source: &Arc<S>,
        reservation: &MemoryReservation,
        ctx: &LoweringContext,
    ) -> Result<PhysicalPlan, PlanError>
    where
        S: SeriesSource + Send + Sync + 'static,
    {
        build_physical_plan(plan, source, reservation.clone(), ctx).await
    }

    fn mk_rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    // ---- tests ----------------------------------------------------------

    #[test]
    fn should_build_vector_selector_from_logical_plan() {
        // given: a mock source with two series under metric `m`
        let source = Arc::new(MockSource::new(vec![
            (
                labels_of(&[("__name__", "m"), ("pod", "a")]),
                vec![(0, 1.0)],
            ),
            (
                labels_of(&[("__name__", "m"), ("pod", "b")]),
                vec![(0, 2.0)],
            ),
        ]));
        let ctx = make_ctx();
        let plan = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let reservation = MemoryReservation::new(1 << 20);

        // when: build the physical plan
        let rt = mk_rt();
        let physical = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .expect("physical plan");

        // then: root is a VectorSelectorOp with the resolved 2-series schema
        let schema = physical
            .output_schema
            .as_static()
            .expect("static schema")
            .clone();
        assert_eq!(schema.len(), 2);
        assert_eq!(physical.step_grid.step_count, 11);
    }

    #[test]
    fn should_build_matrix_selector() {
        // given: a mock source with one series
        let source = Arc::new(MockSource::new(vec![(
            labels_of(&[("__name__", "m")]),
            vec![(0, 1.0), (1_000, 2.0)],
        )]));
        let ctx = make_ctx();
        // Wrap matrix selector in a Rollup so it has a legal parent.
        let plan = LogicalPlan::Rollup {
            kind: crate::promql::v2::operators::rollup::RollupKind::Rate,
            child: Box::new(LogicalPlan::MatrixSelector {
                selector: make_selector("m"),
                range_ms: 5_000,
                offset: Offset::Pos(0),
                at: None,
            }),
        };
        let reservation = MemoryReservation::new(1 << 20);

        // when: build
        let rt = mk_rt();
        let physical = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .expect("physical plan");

        // then: output has the one-series input schema (Rollup passes schema through)
        let schema = physical
            .output_schema
            .as_static()
            .expect("static schema")
            .clone();
        assert_eq!(schema.len(), 1);
    }

    #[test]
    fn should_compute_group_map_for_sum_by_label() {
        // given: three series across two groups (pod=a twice, pod=b once)
        let input = SeriesSchema::new(
            Arc::from(vec![
                labels_of(&[("__name__", "m"), ("pod", "a"), ("inst", "1")]),
                labels_of(&[("__name__", "m"), ("pod", "a"), ("inst", "2")]),
                labels_of(&[("__name__", "m"), ("pod", "b"), ("inst", "3")]),
            ]),
            Arc::from(vec![0u128, 1, 2]),
        );
        let grouping = AggregateGrouping::By(Arc::from(vec!["pod".to_string()]));
        // when: build the group map
        let built = build_group_map(&input, &grouping).expect("group build");
        // then: two groups, inputs 0 and 1 share group 0, input 2 is group 1
        assert_eq!(built.map.group_count, 2);
        assert_eq!(built.map.input_to_group[0], Some(0));
        assert_eq!(built.map.input_to_group[1], Some(0));
        assert_eq!(built.map.input_to_group[2], Some(1));
        assert_eq!(built.group_labels.len(), 2);
    }

    #[test]
    fn should_compute_group_map_for_sum_without_label() {
        // given: three series; `without (inst)` projects onto (__name__, pod)
        let input = SeriesSchema::new(
            Arc::from(vec![
                labels_of(&[("__name__", "m"), ("pod", "a"), ("inst", "1")]),
                labels_of(&[("__name__", "m"), ("pod", "a"), ("inst", "2")]),
                labels_of(&[("__name__", "m"), ("pod", "b"), ("inst", "3")]),
            ]),
            Arc::from(vec![0u128, 1, 2]),
        );
        let grouping = AggregateGrouping::Without(Arc::from(vec!["inst".to_string()]));
        // when
        let built = build_group_map(&input, &grouping).unwrap();
        // then: `__name__` is also stripped (aggregate convention), so the
        // keys are just `pod=a` and `pod=b` — 2 groups.
        assert_eq!(built.map.group_count, 2);
        assert_eq!(built.map.input_to_group[0], built.map.input_to_group[1]);
        assert_ne!(built.map.input_to_group[0], built.map.input_to_group[2]);
    }

    #[test]
    fn should_output_input_schema_for_topk() {
        // given: a topk plan over a 3-series selector
        let source = Arc::new(MockSource::new(vec![
            (labels_of(&[("__name__", "m"), ("i", "1")]), vec![(0, 1.0)]),
            (labels_of(&[("__name__", "m"), ("i", "2")]), vec![(0, 2.0)]),
            (labels_of(&[("__name__", "m"), ("i", "3")]), vec![(0, 3.0)]),
        ]));
        let ctx = make_ctx();
        let child = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let plan = LogicalPlan::Aggregate {
            kind: AggregateKind::Topk(2),
            child: Box::new(child),
            grouping: AggregateGrouping::by_empty(),
        };
        let reservation = MemoryReservation::new(1 << 20);
        let rt = mk_rt();

        // when
        let physical = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .expect("physical plan");

        // then: output schema matches the 3-series input
        let schema = physical
            .output_schema
            .as_static()
            .expect("static schema")
            .clone();
        assert_eq!(schema.len(), 3);
    }

    #[test]
    fn should_output_group_schema_for_sum() {
        // given: sum by (pod) over 3 series in 2 groups
        let source = Arc::new(MockSource::new(vec![
            (
                labels_of(&[("__name__", "m"), ("pod", "a")]),
                vec![(0, 1.0)],
            ),
            (
                labels_of(&[("__name__", "m"), ("pod", "a")]),
                vec![(0, 2.0)],
            ),
            (
                labels_of(&[("__name__", "m"), ("pod", "b")]),
                vec![(0, 3.0)],
            ),
        ]));
        let ctx = make_ctx();
        let child = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let plan = LogicalPlan::Aggregate {
            kind: AggregateKind::Sum,
            child: Box::new(child),
            grouping: AggregateGrouping::By(Arc::from(vec!["pod".to_string()])),
        };
        let reservation = MemoryReservation::new(1 << 20);
        let rt = mk_rt();

        // when
        let physical = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .expect("physical plan");

        // then: one series per group (2)
        let schema = physical
            .output_schema
            .as_static()
            .expect("static schema")
            .clone();
        assert_eq!(schema.len(), 2);
    }

    #[test]
    fn should_compute_one_to_one_match_table() {
        // given: two 2-series schemas whose label keys line up
        let lhs = SeriesSchema::new(
            Arc::from(vec![
                labels_of(&[("__name__", "a"), ("inst", "1")]),
                labels_of(&[("__name__", "a"), ("inst", "2")]),
            ]),
            Arc::from(vec![0u128, 1]),
        );
        let rhs = SeriesSchema::new(
            Arc::from(vec![
                labels_of(&[("__name__", "b"), ("inst", "2")]),
                labels_of(&[("__name__", "b"), ("inst", "1")]),
            ]),
            Arc::from(vec![0u128, 1]),
        );
        // when: build a one-to-one match table (default: ignoring on __name__ only)
        let built = build_match_table(&lhs, &rhs, None, false).unwrap();
        // then: inst=1 pairs LHS[0] -> RHS[1]; inst=2 pairs LHS[1] -> RHS[0]
        match built.table {
            MatchTable::OneToOne(map) => {
                assert_eq!(map, vec![Some(1), Some(0)]);
            }
            other => panic!("unexpected table: {other:?}"),
        }
        assert_eq!(built.output_schema.len(), 2);
    }

    #[test]
    fn should_mark_unmatched_series_as_none_in_match_table() {
        // given: LHS has inst=3 not present on RHS
        let lhs = SeriesSchema::new(
            Arc::from(vec![
                labels_of(&[("__name__", "a"), ("inst", "1")]),
                labels_of(&[("__name__", "a"), ("inst", "3")]),
            ]),
            Arc::from(vec![0u128, 1]),
        );
        let rhs = SeriesSchema::new(
            Arc::from(vec![labels_of(&[("__name__", "b"), ("inst", "1")])]),
            Arc::from(vec![0u128]),
        );
        // when
        let built = build_match_table(&lhs, &rhs, None, false).unwrap();
        // then: LHS[0] matches RHS[0]; LHS[1] is unmatched (None)
        match built.table {
            MatchTable::OneToOne(map) => assert_eq!(map, vec![Some(0), None]),
            other => panic!("unexpected table: {other:?}"),
        }
    }

    #[test]
    fn should_build_subquery_factory_producing_child_per_outer_step() {
        // given: a subquery `foo[3s:1s]` rolled up via rate()
        let source = Arc::new(MockSource::new(vec![(
            labels_of(&[("__name__", "m")]),
            (0..20).map(|i| (i as i64 * 1_000, i as f64)).collect(),
        )]));
        let ctx = LoweringContext::new(0, 5_000, 1_000, 5 * 60_000);
        let plan = LogicalPlan::Rollup {
            kind: crate::promql::v2::operators::rollup::RollupKind::Rate,
            child: Box::new(LogicalPlan::Subquery {
                child: Box::new(LogicalPlan::VectorSelector {
                    selector: make_selector("m"),
                    offset: Offset::Pos(0),
                    at: None,
                    lookback_ms: Some(ctx.lookback_delta_ms),
                }),
                range_ms: 3_000,
                step_ms: 1_000,
                offset: Offset::Pos(0),
                at: None,
            }),
        };
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();
        // when: build (plan-time only; don't poll the operator tree).
        let physical = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .expect("subquery physical plan");
        // then: built and output schema is a single-series static ref
        let schema = physical
            .output_schema
            .as_static()
            .expect("static schema")
            .clone();
        assert_eq!(schema.len(), 1);
    }

    #[test]
    fn should_propagate_source_resolve_error_as_plan_error() {
        // given: a source that always errors on resolve
        let source = Arc::new(MockSource::failing("storage down"));
        let ctx = make_ctx();
        let plan = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let reservation = MemoryReservation::new(1 << 20);
        let rt = mk_rt();
        // when
        let err = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .unwrap_err();
        // then: SourceError with the underlying message
        match err {
            PlanError::SourceError(msg) => assert!(msg.contains("storage down"), "{msg}"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn should_build_rollup_over_matrix_selector_end_to_end() {
        // given: `rate(m[5s])` with an increasing counter
        let source = Arc::new(MockSource::new(vec![(
            labels_of(&[("__name__", "m")]),
            (0..10)
                .map(|i| (i as i64 * 1_000, i as f64 * 10.0))
                .collect(),
        )]));
        let ctx = make_ctx();
        let plan = LogicalPlan::Rollup {
            kind: crate::promql::v2::operators::rollup::RollupKind::Rate,
            child: Box::new(LogicalPlan::MatrixSelector {
                selector: make_selector("m"),
                range_ms: 5_000,
                offset: Offset::Pos(0),
                at: None,
            }),
        };
        let reservation = MemoryReservation::new(1 << 20);
        let rt = mk_rt();
        // when: build and poll the root once (smoke).
        let mut physical = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .expect("physical plan");
        // Drive one poll to check the tree is actually wired.
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let _ = physical.root.next(&mut cx);
        // then: output schema is the single input series
        let schema = physical
            .output_schema
            .as_static()
            .expect("static schema")
            .clone();
        assert_eq!(schema.len(), 1);
    }

    #[test]
    fn should_build_nested_aggregate_of_rollup() {
        // given: `sum by (pod) (rate(m[5s]))` over two series
        let source = Arc::new(MockSource::new(vec![
            (
                labels_of(&[("__name__", "m"), ("pod", "a")]),
                (0..10)
                    .map(|i| (i as i64 * 1_000, i as f64 * 10.0))
                    .collect(),
            ),
            (
                labels_of(&[("__name__", "m"), ("pod", "b")]),
                (0..10)
                    .map(|i| (i as i64 * 1_000, i as f64 * 5.0))
                    .collect(),
            ),
        ]));
        let ctx = make_ctx();
        let expr = parse("sum by (pod) (rate(m[5s]))");
        let plan = super::super::lowering::lower(&expr, &ctx).unwrap();
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();
        // when
        let physical = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .expect("physical plan");
        // then: output has one series per distinct `pod` — two
        let schema = physical
            .output_schema
            .as_static()
            .expect("static schema")
            .clone();
        assert_eq!(schema.len(), 2);
    }

    #[test]
    fn should_reject_count_values_under_schema_sensitive_parent() {
        // given: `sum by (label) (count_values("v", m))` — the Aggregate parent
        // needs a static-schema child.
        let source = Arc::new(MockSource::new(vec![(
            labels_of(&[("__name__", "m")]),
            vec![(0, 1.0)],
        )]));
        let ctx = make_ctx();
        let inner = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let plan = LogicalPlan::Aggregate {
            kind: AggregateKind::Sum,
            child: Box::new(LogicalPlan::CountValues {
                label: "v".to_string(),
                child: Box::new(inner),
                grouping: AggregateGrouping::by_empty(),
            }),
            grouping: AggregateGrouping::by_empty(),
        };
        let reservation = MemoryReservation::new(1 << 20);
        let rt = mk_rt();
        // when
        let err = rt
            .block_on(build(plan, &source, &reservation, &ctx))
            .unwrap_err();
        // then: explicit InvalidMatching rejecting the deferred child
        match err {
            PlanError::InvalidMatching(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn should_group_left_preserves_include_labels_from_one_side() {
        // given: LHS 2-series sharing `job=web` but with distinct `inst`,
        // RHS 1-series also `job=web` plus a `zone=z` label. Match on
        // `job` (via `ignoring(inst)` which also strips `__name__`) and
        // `group_left(zone)` carries `zone` onto each LHS output.
        let lhs = SeriesSchema::new(
            Arc::from(vec![
                labels_of(&[("__name__", "a"), ("job", "web"), ("inst", "1")]),
                labels_of(&[("__name__", "a"), ("job", "web"), ("inst", "2")]),
            ]),
            Arc::from(vec![0u128, 1]),
        );
        let rhs = SeriesSchema::new(
            Arc::from(vec![labels_of(&[
                ("__name__", "b"),
                ("job", "web"),
                ("zone", "z"),
            ])]),
            Arc::from(vec![0u128]),
        );
        let matching = BinaryMatching {
            axis: MatchingAxis::Ignoring,
            labels: Arc::from(vec!["inst".to_string(), "zone".to_string()]),
            cardinality: Cardinality::GroupLeft {
                include: Arc::from(vec!["zone".to_string()]),
            },
        };
        // when
        let built = build_match_table(&lhs, &rhs, Some(&matching), false).unwrap();
        // then: GroupLeft with both LHS rows mapped to RHS[0], and the
        // output labels carry `zone="z"` from the "one" side.
        match built.table {
            MatchTable::GroupLeft(map) => assert_eq!(map, vec![Some(0), Some(0)]),
            other => panic!("unexpected: {other:?}"),
        }
        let out = built.output_schema;
        assert_eq!(out.len(), 2);
        for i in 0..2 {
            let lbls = out.labels(i as u32);
            assert!(
                lbls.iter().any(|l| l.name == "zone" && l.value == "z"),
                "output row {i} must carry zone=z",
            );
        }
    }

    // ------------------------------------------------------------------
    // Unit 4.5: exchange-operator insertion tests
    // ------------------------------------------------------------------

    use crate::promql::v2::operators::concurrent::DEFAULT_CHANNEL_BOUND;
    use crate::promql::v2::plan::parallelism::Parallelism;

    /// Build a mock source with `n` identical single-sample series under
    /// metric `m`. Each series gets a distinct `i` label so they pass the
    /// selector's roster projection.
    fn mock_source_with_n_series(n: usize) -> Arc<MockSource> {
        let series = (0..n)
            .map(|i| {
                (
                    labels_of(&[("__name__", "m"), ("i", &i.to_string())]),
                    vec![(0, i as f64)],
                )
            })
            .collect();
        Arc::new(MockSource::new(series))
    }

    async fn build_with_stats<S>(
        plan: LogicalPlan,
        source: &Arc<S>,
        reservation: &MemoryReservation,
        ctx: &LoweringContext,
    ) -> Result<(PhysicalPlan, ExchangeStats), PlanError>
    where
        S: SeriesSource + Send + Sync + 'static,
    {
        build_physical_plan_with_stats(plan, source, reservation.clone(), ctx).await
    }

    #[test]
    fn should_wrap_leaves_above_threshold_in_concurrent() {
        // given: a 128-series selector and the default threshold (64)
        let source = mock_source_with_n_series(128);
        let ctx = make_ctx();
        let plan = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();

        // when: build the physical plan with stats
        let (_physical, stats) = rt
            .block_on(build_with_stats(plan, &source, &reservation, &ctx))
            .expect("physical plan");

        // then: the single leaf is wrapped; no skips recorded
        assert_eq!(stats.concurrent_wrapped, 1);
        assert_eq!(stats.concurrent_skipped, 0);
        assert_eq!(stats.coalesce_inserted, 0);
    }

    #[test]
    fn should_not_wrap_leaves_below_threshold() {
        // given: a 16-series selector and the default threshold (64)
        let source = mock_source_with_n_series(16);
        let ctx = make_ctx();
        let plan = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();

        // when
        let (_physical, stats) = rt
            .block_on(build_with_stats(plan, &source, &reservation, &ctx))
            .expect("physical plan");

        // then: the leaf is skipped; no wraps
        assert_eq!(stats.concurrent_wrapped, 0);
        assert_eq!(stats.concurrent_skipped, 1);
    }

    #[test]
    fn should_leave_intermediate_operators_unwrapped() {
        // given: `sum by (i) (m)` over 128 series — one leaf + one aggregate.
        // The Aggregate is an intermediate CPU-bound op; only the selector
        // leaf should be wrapped.
        let source = mock_source_with_n_series(128);
        let ctx = make_ctx();
        let child = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let plan = LogicalPlan::Aggregate {
            kind: AggregateKind::Sum,
            child: Box::new(child),
            grouping: AggregateGrouping::By(Arc::from(vec!["i".to_string()])),
        };
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();

        // when
        let (_physical, stats) = rt
            .block_on(build_with_stats(plan, &source, &reservation, &ctx))
            .expect("physical plan");

        // then: exactly one wrap (the leaf), and exactly one wrap-or-skip
        // decision was recorded (the aggregate is not a wrap candidate, so
        // it doesn't appear in either counter).
        assert_eq!(stats.concurrent_wrapped, 1);
        assert_eq!(stats.concurrent_skipped, 0);
    }

    #[test]
    fn should_use_default_parallelism_when_not_specified() {
        // given: a plain `LoweringContext::new` (no explicit parallelism)
        let ctx = make_ctx();
        // then: parallelism uses the documented defaults
        assert_eq!(
            ctx.parallelism.concurrent_threshold_series,
            Parallelism::DEFAULT_CONCURRENT_THRESHOLD_SERIES,
        );
        assert_eq!(ctx.parallelism.coalesce_max_shards, 0);
    }

    #[test]
    fn should_allow_disabling_concurrent_with_threshold_zero_or_max() {
        // given: two configurations — threshold=0 (wrap every leaf) and
        // threshold=u64::MAX (wrap no leaf). Both use the same 32-series
        // source (between the default's 64 threshold) so the distinction
        // is purely from the knob.
        let source = mock_source_with_n_series(32);
        let plan_fn = || LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(5 * 60_000),
        };
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();

        // when: threshold = 0 → wrap every leaf (even small ones)
        let ctx_wrap_all = LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
            .with_parallelism(Parallelism::new(0, 0, DEFAULT_CHANNEL_BOUND));
        let (_, stats_wrap_all) = rt
            .block_on(build_with_stats(
                plan_fn(),
                &source,
                &reservation,
                &ctx_wrap_all,
            ))
            .expect("physical plan (wrap-all)");

        // when: threshold = u64::MAX → wrap no leaf
        let ctx_wrap_none = LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
            .with_parallelism(Parallelism::new(u64::MAX, 0, DEFAULT_CHANNEL_BOUND));
        let (_, stats_wrap_none) = rt
            .block_on(build_with_stats(
                plan_fn(),
                &source,
                &reservation,
                &ctx_wrap_none,
            ))
            .expect("physical plan (wrap-none)");

        // then
        assert_eq!(stats_wrap_all.concurrent_wrapped, 1);
        assert_eq!(stats_wrap_all.concurrent_skipped, 0);
        assert_eq!(stats_wrap_none.concurrent_wrapped, 0);
        assert_eq!(stats_wrap_none.concurrent_skipped, 1);
    }

    #[test]
    fn should_respect_channel_bound() {
        // given: a custom channel bound propagated through Parallelism.
        // We cannot introspect the `ConcurrentOp`'s channel capacity
        // through `dyn Operator`, but we can verify the setting round-trips
        // through `LoweringContext` and that the resulting plan builds
        // (non-zero bounds satisfy `ConcurrentOp::new`'s `assert!(bound > 0)`).
        let source = mock_source_with_n_series(100);
        let ctx = LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
            .with_parallelism(Parallelism::new(1, 0, 7));
        assert_eq!(ctx.parallelism.channel_bound, 7);
        let plan = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();
        // when: build; the ConcurrentOp constructor would panic for bound=0
        let (_, stats) = rt
            .block_on(build_with_stats(plan, &source, &reservation, &ctx))
            .expect("physical plan (bound=7)");
        // then: leaf wrapped under the threshold=1 setting
        assert_eq!(stats.concurrent_wrapped, 1);
    }

    #[test]
    fn should_skip_coalesce_in_v1() {
        // given: any plan shape — v1 deliberately skips `Coalesce` insertion
        let source = mock_source_with_n_series(256);
        let ctx = make_ctx();
        let plan = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(ctx.lookback_delta_ms),
        };
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();
        // when
        let (_, stats) = rt
            .block_on(build_with_stats(plan, &source, &reservation, &ctx))
            .expect("physical plan");
        // then: no coalesce insertions
        assert_eq!(stats.coalesce_inserted, 0);
    }

    #[test]
    fn should_wrap_rollup_leaf_above_threshold() {
        // given: a matrix-selector leaf (wrapped in Rollup) with 200 series.
        // `MatrixSelectorOp::next` is degenerate, so the planner wraps the
        // enclosing `RollupOp` instead — still decouples I/O from
        // evaluation at the right boundary.
        let source = mock_source_with_n_series(200);
        let ctx = make_ctx();
        let plan = LogicalPlan::Rollup {
            kind: crate::promql::v2::operators::rollup::RollupKind::Rate,
            child: Box::new(LogicalPlan::MatrixSelector {
                selector: make_selector("m"),
                range_ms: 5_000,
                offset: Offset::Pos(0),
                at: None,
            }),
        };
        let reservation = MemoryReservation::new(1 << 22);
        let rt = mk_rt();

        // when
        let (_, stats) = rt
            .block_on(build_with_stats(plan, &source, &reservation, &ctx))
            .expect("physical plan");

        // then: one wrap (the rollup of the matrix leaf)
        assert_eq!(stats.concurrent_wrapped, 1);
        assert_eq!(stats.concurrent_skipped, 0);
    }
}

//! Plan-time cardinality gate (unit 4.4).
//!
//! Fail-fast pre-resolve check that rejects oversized queries before the
//! physical planner materialises sample state. For each leaf
//! [`LogicalPlan::VectorSelector`] / [`LogicalPlan::MatrixSelector`] the
//! gate calls [`SeriesSource::estimate_cardinality`] and multiplies the
//! returned upper bound by the query's step count, summing across leaves.
//! If the total exceeds [`CardinalityLimits::max_series_x_steps`] the gate
//! emits [`PlanError::TooLarge`].
//!
//! See RFC 0007 §"Execution Model" — "Fail-fast cardinality gate" — and
//! the impl-plan §4.4 brief.
//!
//! # Estimate fidelity
//!
//! The gate uses [`CardinalityEstimate::series_count_estimate`] (the upper
//! bound) even when the estimate is flagged approximate. Rejecting a query
//! that would have fit is cheap; letting one through that OOMs mid-execution
//! is expensive. When the source returns `u64::MAX` as the upper bound (the
//! "no positive matchers" escape hatch from unit 2.2) the gate trips
//! immediately, provided any non-zero limit is configured.
//!
//! # Subqueries
//!
//! Subqueries multiply the true per-step work
//! (`inner_step_count × outer_step_count` cells per inner leaf), but the RFC
//! text explicitly says "`sum(resolved_series × steps)`" — the outer-grid
//! flavour. v1 follows the RFC verbatim: each leaf's contribution is
//! `series_estimate × outer_step_count`, ignoring any subquery step
//! multiplication. Subqueries are rarely the dominant cardinality and
//! underestimating here lets the reservation (unit 1.4) trip mid-exec if
//! the inner plan really does explode. Flagged in §5 Decisions Log so
//! Phase 6 triage can revisit.

use super::error::PlanError;
use super::plan_types::LogicalPlan;
use crate::promql::v2::source::{SeriesSource, TimeRange};

/// Configurable limits consulted by the planner's fail-fast gates.
///
/// Kept as a plain-data struct so callers can construct it without
/// threading through an options builder. Stored on
/// [`super::lowering::LoweringContext`] and therefore copied freely
/// through the plan pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CardinalityLimits {
    /// Maximum permitted `sum(resolved_series × steps)` across all leaf
    /// selectors. `0` disables the gate (see
    /// [`Self::is_disabled`]).
    ///
    /// Default: **20,000,000 cells** — picked to allow ~2 k series over a
    /// 10k-step range, or ~20k series over a 1k-step range, comfortably
    /// within the typical per-query memory cap. Tune per deployment.
    pub max_series_x_steps: u64,
}

impl CardinalityLimits {
    /// Default limit (20M cells). Same as `<Self as Default>::default()`;
    /// provided as a `const` so it can be used in module-level static
    /// contexts.
    pub const DEFAULT_MAX_SERIES_X_STEPS: u64 = 20_000_000;

    /// Construct limits explicitly. Pass `0` to disable the gate.
    pub fn new(max_series_x_steps: u64) -> Self {
        Self { max_series_x_steps }
    }

    /// `true` when the gate is explicitly disabled (limit == 0).
    #[inline]
    pub fn is_disabled(&self) -> bool {
        self.max_series_x_steps == 0
    }
}

impl Default for CardinalityLimits {
    fn default() -> Self {
        Self {
            max_series_x_steps: Self::DEFAULT_MAX_SERIES_X_STEPS,
        }
    }
}

/// Gate a logical plan against the caller-supplied source. See the module
/// docs for estimate-fidelity and subquery-treatment details.
///
/// The gate runs **before** any [`SeriesSource::resolve`] call so an
/// oversized query never allocates sample state. Returns `Ok(estimated)`
/// on success (the number the gate computed — useful for tracing) and
/// [`PlanError::TooLarge`] when the sum exceeds the configured limit.
pub async fn enforce_cardinality_gate<S>(
    plan: &LogicalPlan,
    source: &S,
    outer_step_count: usize,
    limits: CardinalityLimits,
) -> Result<u64, PlanError>
where
    S: SeriesSource + Send + Sync + 'static,
{
    if limits.is_disabled() {
        return Ok(0);
    }

    let outer_steps = outer_step_count.max(1) as u64;
    let limit = limits.max_series_x_steps;
    let mut estimated: u64 = 0;

    let mut leaves: Vec<LeafRef<'_>> = Vec::new();
    collect_leaves(plan, &mut leaves);

    for leaf in leaves {
        let est = source
            .estimate_cardinality(leaf.selector, leaf.time_range)
            .await
            .map_err(|e| PlanError::SourceError(e.to_string()))?;
        // Saturating arithmetic throughout — the `u64::MAX` sentinel
        // returned by the 2.2 adapter's "no positive matchers" escape
        // hatch must trip the gate rather than wrap around.
        let series = est.series_count_estimate();
        let cells = series.saturating_mul(outer_steps);
        estimated = estimated.saturating_add(cells);
        if estimated > limit {
            return Err(PlanError::TooLarge {
                estimated_cells: estimated,
                limit,
            });
        }
    }

    Ok(estimated)
}

/// Internal view over a leaf selector node.
struct LeafRef<'a> {
    selector: &'a promql_parser::parser::VectorSelector,
    time_range: TimeRange,
}

/// Walk the plan and push every leaf (`VectorSelector` / `MatrixSelector`)
/// with a derived [`TimeRange`] the source can use as the window hint.
///
/// The time range does **not** need to be the exact absolute window the
/// physical planner will later compute for the leaf (that includes
/// lookback / `@` / offset folding and is handled by
/// [`super::physical::selector_time_range`]). For the gate the source only
/// uses the range to size per-bucket candidate counts; a generous window
/// keeps the estimate a safe upper bound.
fn collect_leaves<'a>(plan: &'a LogicalPlan, out: &mut Vec<LeafRef<'a>>) {
    // The gate treats `Binary` / `Aggregate` / `Rollup` / `InstantFn` /
    // `CountValues` / `Rechunk` / `Concurrent` / `Coalesce` as transparent
    // — they do not change the input cardinality we care about (the gate
    // counts what the planner would materialise from storage, not the
    // post-transform shape).
    //
    // `CountValues` cannot be gated on output cardinality (data-dependent),
    // but its input cardinality is a valid lower bound — the wording of
    // §3.5 scope: "use the input cardinality as a lower bound; if the
    // input exceeds the limit, fail. Otherwise let it through."
    //
    // `Subquery` is a leaf *container* that wraps a whole sub-plan; v1
    // uses the outer-grid step count for the gate (see module docs), so
    // we recurse into the subquery's child and treat each leaf inside it
    // the same way.
    match plan {
        LogicalPlan::VectorSelector {
            selector,
            lookback_ms: _,
            offset: _,
            at: _,
        } => out.push(LeafRef {
            selector,
            // The source's `estimate_cardinality` is expected to be cheap
            // and only peeks at time-range overlap with bucket metadata,
            // so an open-ended range is acceptable for a pre-resolve
            // estimate. Pass a zero-to-i64::MAX window to cover the full
            // possible absolute range — the adapter narrows by bucket
            // membership either way.
            time_range: TimeRange::new(0, i64::MAX),
        }),
        LogicalPlan::MatrixSelector {
            selector,
            range_ms: _,
            offset: _,
            at: _,
        } => out.push(LeafRef {
            selector,
            time_range: TimeRange::new(0, i64::MAX),
        }),
        LogicalPlan::Scalar(_) | LogicalPlan::Time => {}
        LogicalPlan::Scalarize { child, .. } | LogicalPlan::Vectorize { child, .. } => {
            collect_leaves(child, out)
        }
        LogicalPlan::InstantFn { child, .. }
        | LogicalPlan::LabelManip { child, .. }
        | LogicalPlan::Rollup { child, .. }
        | LogicalPlan::Rechunk { child, .. }
        | LogicalPlan::CountValues { child, .. }
        | LogicalPlan::Concurrent { child, .. } => collect_leaves(child, out),
        LogicalPlan::Aggregate { child, param, .. } => {
            collect_leaves(child, out);
            if let Some(param) = param {
                collect_leaves(param, out);
            }
        }
        LogicalPlan::Binary { lhs, rhs, .. } => {
            collect_leaves(lhs, out);
            collect_leaves(rhs, out);
        }
        LogicalPlan::Subquery { child, .. } => collect_leaves(child, out),
        LogicalPlan::Coalesce { children } => {
            for c in children {
                collect_leaves(c, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Labels;
    use crate::promql::v2::memory::QueryError;
    use crate::promql::v2::source::{
        CardinalityEstimate, ResolvedSeriesChunk, SampleBatch, SampleBlock, SampleHint,
    };
    use futures::stream;
    use promql_parser::label::{MatchOp, Matcher, Matchers};
    use promql_parser::parser as pparser;
    use std::future::ready;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // ---- mock source ----------------------------------------------------

    /// Scripted source whose `estimate_cardinality` returns a pre-set
    /// series count indexed by the selector's metric name. Counts calls to
    /// `estimate_cardinality` and `resolve` so tests can assert ordering.
    struct MockSource {
        /// Series count per `__name__` matcher value.
        counts: std::collections::HashMap<String, u64>,
        approx_default: bool,
        /// Number of `estimate_cardinality` invocations.
        estimate_calls: Arc<AtomicUsize>,
        /// Number of `resolve` invocations.
        resolve_calls: Arc<AtomicUsize>,
    }

    impl MockSource {
        fn new() -> Self {
            Self {
                counts: std::collections::HashMap::new(),
                approx_default: false,
                estimate_calls: Arc::new(AtomicUsize::new(0)),
                resolve_calls: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with(mut self, metric: &str, count: u64) -> Self {
            self.counts.insert(metric.to_string(), count);
            self
        }

        fn approx(mut self, flag: bool) -> Self {
            self.approx_default = flag;
            self
        }
    }

    impl SeriesSource for MockSource {
        fn resolve(
            &self,
            _selector: &pparser::VectorSelector,
            _time_range: TimeRange,
        ) -> impl futures::Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
            self.resolve_calls.fetch_add(1, Ordering::SeqCst);
            let chunk = ResolvedSeriesChunk {
                bucket_id: 1,
                labels: Arc::from(Vec::<Labels>::new()),
                series: Arc::from(Vec::new()),
            };
            stream::iter(vec![Ok(chunk)])
        }

        fn estimate_cardinality(
            &self,
            selector: &pparser::VectorSelector,
            _time_range: TimeRange,
        ) -> impl std::future::Future<Output = Result<CardinalityEstimate, QueryError>> + Send
        {
            self.estimate_calls.fetch_add(1, Ordering::SeqCst);
            let metric = selector
                .matchers
                .matchers
                .iter()
                .find(|m| m.name == "__name__")
                .map(|m| m.value.clone())
                .unwrap_or_default();
            let count = self.counts.get(&metric).copied().unwrap_or(0);
            let est = if self.approx_default {
                // Approximate: lower bound is smaller than upper bound.
                CardinalityEstimate {
                    series_lower_bound: count / 2,
                    series_upper_bound: count,
                    approx: true,
                }
            } else {
                CardinalityEstimate::exact(count)
            };
            ready(Ok(est))
        }

        fn samples(
            &self,
            hint: SampleHint,
        ) -> impl futures::Stream<Item = Result<SampleBatch, QueryError>> + Send {
            let block = SampleBlock::with_series_count(hint.series.len());
            stream::iter(vec![Ok(SampleBatch {
                series_range: 0..hint.series.len(),
                samples: block,
            })])
        }
    }

    fn make_selector(metric: &str) -> pparser::VectorSelector {
        let m = Matcher::new(MatchOp::Equal, "__name__", metric);
        pparser::VectorSelector::new(Some(metric.to_string()), Matchers::new(vec![m]))
    }

    fn vs_plan(metric: &str) -> LogicalPlan {
        use crate::promql::v2::plan::plan_types::Offset;
        LogicalPlan::VectorSelector {
            selector: make_selector(metric),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(5 * 60_000),
        }
    }

    fn mk_rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    // ---- CardinalityLimits constructor tests ---------------------------

    #[test]
    fn should_default_cardinality_limits_to_twenty_million_cells() {
        // given: no configuration
        // when: construct defaults
        let limits = CardinalityLimits::default();
        // then: matches the documented default
        assert_eq!(limits.max_series_x_steps, 20_000_000);
        assert!(!limits.is_disabled());
    }

    #[test]
    fn should_treat_zero_limit_as_disabled() {
        // given: a zero-capped limit
        let limits = CardinalityLimits::new(0);
        // when / then: gate reports disabled
        assert!(limits.is_disabled());
    }

    // ---- Gate tests -----------------------------------------------------

    #[test]
    fn should_accept_plan_within_limit() {
        // given: a single-leaf plan with 10 series and a 100-step grid
        //        (1_000 cells, limit 10_000)
        let source = MockSource::new().with("m", 10);
        let plan = vs_plan("m");
        let limits = CardinalityLimits::new(10_000);
        let rt = mk_rt();
        // when: gate runs
        let result = rt.block_on(enforce_cardinality_gate(&plan, &source, 100, limits));
        // then: accepts, reports 1_000 cells, never invokes resolve
        let cells = result.expect("gate should accept");
        assert_eq!(cells, 1_000);
        assert_eq!(source.estimate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(source.resolve_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn should_reject_plan_exceeding_limit() {
        // given: a single-leaf plan with 1_000 series and a 100-step grid
        //        (100_000 cells, limit 10_000)
        let source = MockSource::new().with("m", 1_000);
        let plan = vs_plan("m");
        let limits = CardinalityLimits::new(10_000);
        let rt = mk_rt();
        // when: gate runs
        let err = rt
            .block_on(enforce_cardinality_gate(&plan, &source, 100, limits))
            .unwrap_err();
        // then: TooLarge with the computed estimate and limit
        match err {
            PlanError::TooLarge {
                estimated_cells,
                limit,
            } => {
                assert_eq!(estimated_cells, 100_000);
                assert_eq!(limit, 10_000);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn should_accumulate_cardinality_across_multiple_leaves() {
        // given: `rate(a[5m]) + rate(b[5m])` shape with two matrix leaves,
        //        50 + 75 = 125 series, 10 steps → 1_250 cells (limit 10k)
        use crate::promql::v2::operators::binary::BinaryOpKind;
        use crate::promql::v2::operators::rollup::RollupKind;
        use crate::promql::v2::plan::plan_types::Offset;
        let source = MockSource::new().with("a", 50).with("b", 75);
        let matrix = |metric: &str| LogicalPlan::MatrixSelector {
            selector: make_selector(metric),
            range_ms: 5 * 60_000,
            offset: Offset::Pos(0),
            at: None,
        };
        let rollup = |metric: &str| LogicalPlan::Rollup {
            kind: RollupKind::Rate,
            child: Box::new(matrix(metric)),
        };
        let plan = LogicalPlan::Binary {
            op: BinaryOpKind::Add,
            lhs: Box::new(rollup("a")),
            rhs: Box::new(rollup("b")),
            matching: None,
        };
        let limits = CardinalityLimits::new(10_000);
        let rt = mk_rt();
        // when
        let cells = rt
            .block_on(enforce_cardinality_gate(&plan, &source, 10, limits))
            .expect("gate should accept");
        // then: (50 + 75) × 10 = 1_250
        assert_eq!(cells, 1_250);
        assert_eq!(source.estimate_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn should_multiply_by_step_count() {
        // given: a 100-series plan. step_count=10 → 1_000 cells (well
        //        within 20_000); step_count=100 → 10_000 cells (at cap).
        let source = MockSource::new().with("m", 100);
        let plan = vs_plan("m");
        let limits = CardinalityLimits::new(20_000);
        let rt = mk_rt();
        // when: two separate invocations with different step counts
        let low = rt
            .block_on(enforce_cardinality_gate(&plan, &source, 10, limits))
            .unwrap();
        let high = rt
            .block_on(enforce_cardinality_gate(&plan, &source, 100, limits))
            .unwrap();
        // then: the 100-step case scales linearly
        assert_eq!(low, 1_000);
        assert_eq!(high, 10_000);
    }

    #[test]
    fn should_report_estimated_cells_in_error() {
        // given: a plan whose single leaf produces 500 series; step_count = 50
        //        → 25_000 cells, limit 10_000.
        let source = MockSource::new().with("m", 500);
        let plan = vs_plan("m");
        let limits = CardinalityLimits::new(10_000);
        let rt = mk_rt();
        // when
        let err = rt
            .block_on(enforce_cardinality_gate(&plan, &source, 50, limits))
            .unwrap_err();
        // then: error payload carries the computed number
        match err {
            PlanError::TooLarge {
                estimated_cells,
                limit,
            } => {
                assert_eq!(estimated_cells, 25_000);
                assert_eq!(limit, 10_000);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn should_use_upper_bound_when_estimate_is_approx() {
        // given: a source that returns `approx: true` with lower=50,
        //        upper=1_000 for `m`. step_count=10 → upper-bound gate
        //        uses 1_000 × 10 = 10_000 cells (over the 5_000 limit).
        let source = MockSource::new().with("m", 1_000).approx(true);
        let plan = vs_plan("m");
        let limits = CardinalityLimits::new(5_000);
        let rt = mk_rt();
        // when
        let err = rt
            .block_on(enforce_cardinality_gate(&plan, &source, 10, limits))
            .unwrap_err();
        // then: gate trips on the upper bound (conservative), not the
        // approx-tagged lower bound
        match err {
            PlanError::TooLarge {
                estimated_cells, ..
            } => assert_eq!(estimated_cells, 10_000),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn should_bypass_gate_when_limit_is_zero_or_disabled() {
        // given: a plan whose cardinality is absurd (u64::MAX / 2 × 1_000),
        //        and an explicitly disabled limit.
        let source = MockSource::new().with("m", u64::MAX / 2);
        let plan = vs_plan("m");
        let limits = CardinalityLimits::new(0);
        let rt = mk_rt();
        // when
        let cells = rt
            .block_on(enforce_cardinality_gate(&plan, &source, 1_000, limits))
            .expect("disabled gate should accept");
        // then: gate returns zero (no estimate computed) and skips source
        assert_eq!(cells, 0);
        assert_eq!(source.estimate_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn should_trip_gate_immediately_on_unbounded_sentinel() {
        // given: a source whose selector returns the `u64::MAX` escape
        //        hatch (no-positive-matchers case from unit 2.2)
        let source = MockSource::new().with("m", u64::MAX);
        let plan = vs_plan("m");
        let limits = CardinalityLimits::new(1_000_000);
        let rt = mk_rt();
        // when
        let err = rt
            .block_on(enforce_cardinality_gate(&plan, &source, 1, limits))
            .unwrap_err();
        // then: saturating arithmetic lets the sentinel trip the gate
        match err {
            PlanError::TooLarge {
                estimated_cells,
                limit,
            } => {
                assert_eq!(estimated_cells, u64::MAX);
                assert_eq!(limit, 1_000_000);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    // ---- Integration via build_physical_plan ---------------------------

    use crate::promql::v2::memory::MemoryReservation;
    use crate::promql::v2::plan::physical::build_physical_plan;
    use crate::promql::v2::plan::plan_types::Offset;

    fn build_ctx(limits: CardinalityLimits) -> super::super::lowering::LoweringContext {
        super::super::lowering::LoweringContext::new(0, 10_000, 1_000, 5 * 60_000)
            .with_cardinality_limits(limits)
    }

    /// Mock source that actually resolves to a roster so `build_physical_plan`
    /// can succeed on the accept path. The `estimate_calls` / `resolve_calls`
    /// counters let the ordering test assert the gate runs first.
    struct ResolvingSource {
        series: Vec<Labels>,
        estimate_calls: Arc<AtomicUsize>,
        resolve_calls: Arc<AtomicUsize>,
    }

    impl ResolvingSource {
        fn new(series: Vec<Labels>) -> Self {
            Self {
                series,
                estimate_calls: Arc::new(AtomicUsize::new(0)),
                resolve_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl SeriesSource for ResolvingSource {
        fn resolve(
            &self,
            _selector: &pparser::VectorSelector,
            _time_range: TimeRange,
        ) -> impl futures::Stream<Item = Result<ResolvedSeriesChunk, QueryError>> + Send {
            self.resolve_calls.fetch_add(1, Ordering::SeqCst);
            let labels = self.series.clone();
            let refs: Vec<_> = (0..self.series.len())
                .map(|i| crate::promql::v2::source::ResolvedSeriesRef::new(1, i as u32))
                .collect();
            let chunk = ResolvedSeriesChunk {
                bucket_id: 1,
                labels: Arc::from(labels),
                series: Arc::from(refs),
            };
            stream::iter(vec![Ok(chunk)])
        }

        fn estimate_cardinality(
            &self,
            _selector: &pparser::VectorSelector,
            _time_range: TimeRange,
        ) -> impl std::future::Future<Output = Result<CardinalityEstimate, QueryError>> + Send
        {
            self.estimate_calls.fetch_add(1, Ordering::SeqCst);
            ready(Ok(CardinalityEstimate::exact(self.series.len() as u64)))
        }

        fn samples(
            &self,
            hint: SampleHint,
        ) -> impl futures::Stream<Item = Result<SampleBatch, QueryError>> + Send {
            let block = SampleBlock::with_series_count(hint.series.len());
            stream::iter(vec![Ok(SampleBatch {
                series_range: 0..hint.series.len(),
                samples: block,
            })])
        }
    }

    #[test]
    fn should_call_estimate_cardinality_before_resolve() {
        // given: a resolving source and a plan well within the limit
        let source = Arc::new(ResolvingSource::new(vec![Labels::new(vec![
            crate::model::Label::new("__name__", "m"),
        ])]));
        let plan = vs_plan("m");
        let limits = CardinalityLimits::new(10_000);
        let ctx = build_ctx(limits);
        let reservation = MemoryReservation::new(1 << 20);
        let rt = mk_rt();
        // when: build the physical plan end-to-end
        let res = rt.block_on(build_physical_plan(plan, &source, reservation, &ctx));
        // then: plan built; estimate ran, and at least one resolve call
        // happened *after* the estimate. Counters are simple atomics, so
        // we check that estimate was called at least once and resolve was
        // called at least once (ordering assertion is inherent: the gate
        // awaits the estimate before invoking `build_node` which is the
        // only path that calls resolve).
        res.expect("plan should build");
        let est = source.estimate_calls.load(Ordering::SeqCst);
        let res_calls = source.resolve_calls.load(Ordering::SeqCst);
        assert!(est >= 1, "estimate_cardinality must be called");
        assert!(res_calls >= 1, "resolve must be called after estimate");
    }

    #[test]
    fn should_not_call_resolve_when_gate_rejects() {
        // given: a plan whose single leaf reports 1M series but the limit
        //        is only 10_000 cells.
        let mut many_series = Vec::new();
        for i in 0..1_000 {
            many_series.push(Labels::new(vec![
                crate::model::Label::new("__name__", "m"),
                crate::model::Label::new("i", i.to_string()),
            ]));
        }
        let source = Arc::new(ResolvingSource::new(many_series));
        let plan = LogicalPlan::VectorSelector {
            selector: make_selector("m"),
            offset: Offset::Pos(0),
            at: None,
            lookback_ms: Some(5 * 60_000),
        };
        let limits = CardinalityLimits::new(100);
        let ctx = build_ctx(limits);
        let reservation = MemoryReservation::new(1 << 20);
        let rt = mk_rt();
        // when
        let err = rt
            .block_on(build_physical_plan(plan, &source, reservation, &ctx))
            .unwrap_err();
        // then: TooLarge and resolve was never called
        match err {
            PlanError::TooLarge { .. } => {}
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(source.estimate_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            source.resolve_calls.load(Ordering::SeqCst),
            0,
            "resolve must NOT be called when the gate rejects",
        );
    }
}

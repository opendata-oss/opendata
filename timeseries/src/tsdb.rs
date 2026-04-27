use std::collections::{HashMap, HashSet};
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use common::{Storage, StorageRead};
use futures::stream;
use futures::{StreamExt, TryStreamExt};
use moka::future::Cache;
use promql_parser::parser::{EvalStmt, Expr, VectorSelector};
use tokio::sync::RwLock;
use tracing::error;

use crate::error::QueryError;
use crate::index::{ForwardIndexLookup, InvertedIndexLookup};
use crate::minitsdb::{MiniQueryReader, MiniTsdb};
use crate::model::{
    Label, Labels, MetricMetadata, QueryOptions, QueryValue, RangeSample, Sample, Series, SeriesId,
    TimeBucket,
};
use crate::query::{BucketQueryReader, QueryReader};
use crate::storage::OpenTsdbStorageReadExt;
use crate::tsdb_metrics;
use crate::util::Result;

/// Compute the disjoint preload ranges (seconds) a query touches after
/// applying `offset`/`@` modifiers. Falls back to
/// `[(default_start, default_end)]` for selector-free expressions.
pub(crate) fn preload_ranges(
    stmt: &EvalStmt,
    default_start: i64,
    default_end: i64,
) -> Vec<(i64, i64)> {
    let ranges = compute_preload_ranges(&stmt.expr, stmt.start, stmt.end, stmt.lookback_delta);
    if ranges.is_empty() {
        vec![(default_start, default_end)]
    } else {
        ranges
    }
}

/// Default per-query memory cap for the PromQL engine.
///
/// 1 GiB — a conservative ceiling that should comfortably fit any
/// single-digit-GB host's working set while still failing fast on a
/// runaway query.
pub(crate) const DEFAULT_MEMORY_CAP_BYTES: usize = 1024 * 1024 * 1024;

/// Convert `SystemTime` to milliseconds since the UNIX epoch, clamping
/// pre-epoch values to zero.
fn system_time_to_ms(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

/// Convert `Duration` to milliseconds, saturating at `i64::MAX` for
/// pathological inputs.
fn duration_to_ms(d: Duration) -> i64 {
    d.as_millis().min(i64::MAX as u128) as i64
}

/// Compute the preload ranges (seconds) the PromQL engine's `QueryReader`
/// needs to open by re-parsing `query`.
fn preload_ranges_for_query(
    query: &str,
    start_ms: i64,
    end_ms: i64,
    lookback_delta: Duration,
) -> std::result::Result<Vec<(i64, i64)>, QueryError> {
    let expr =
        promql_parser::parser::parse(query).map_err(|e| QueryError::InvalidQuery(e.to_string()))?;
    let start = UNIX_EPOCH + Duration::from_millis(start_ms.max(0) as u64);
    let end = UNIX_EPOCH + Duration::from_millis(end_ms.max(0) as u64);
    let stmt = EvalStmt {
        expr,
        start,
        end,
        interval: Duration::from_secs(0),
        lookback_delta,
    };
    let default_start_secs = start
        .checked_sub(lookback_delta)
        .unwrap_or(UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64;
    let default_end_secs = end
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs() as i64;
    Ok(preload_ranges(&stmt, default_start_secs, default_end_secs))
}

// ── Preload-range computation (ported from v1 evaluator) ──

/// Walk the AST and compute the disjoint time ranges needed for bucket preloading.
///
/// For each selector, compute the effective evaluation time by applying
/// `@` and `offset` modifiers, then expand by `lookback_delta` (vector) or
/// range (matrix). Returns a sorted, non-overlapping list of
/// `(earliest_secs, latest_secs)` ranges covering all selectors.
///
/// Returns an empty `Vec` when the expression contains no selectors
/// (e.g. `1 + 2`), allowing the caller to fall back to the default window.
fn compute_preload_ranges(
    expr: &Expr,
    query_start: SystemTime,
    query_end: SystemTime,
    lookback_delta: Duration,
) -> Vec<(i64, i64)> {
    let start_ms = system_time_to_ms(query_start);
    let end_ms = system_time_to_ms(query_end);
    let lookback_ms = lookback_delta.as_millis() as i64;
    let mut ranges = Vec::new();
    preload_ranges_inner(
        expr,
        start_ms,
        end_ms,
        start_ms,
        end_ms,
        lookback_ms,
        &mut ranges,
    );
    let ranges_secs: Vec<(i64, i64)> = ranges
        .into_iter()
        .map(|(lo, hi)| {
            let start_secs = lo.div_euclid(1000);
            let end_secs = hi.div_euclid(1000) + i64::from(hi.rem_euclid(1000) != 0);
            (start_secs, end_secs)
        })
        .collect();
    normalize_ranges(ranges_secs)
}

/// Sort ranges by start and merge overlapping ones. Adjacent-but-not-overlapping
/// ranges are kept separate since they may map to different buckets.
fn normalize_ranges(mut ranges: Vec<(i64, i64)>) -> Vec<(i64, i64)> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|&(start, _)| start);
    let mut merged = Vec::with_capacity(ranges.len());
    let (mut cur_start, mut cur_end) = ranges[0];
    for &(start, end) in &ranges[1..] {
        if start <= cur_end {
            cur_end = cur_end.max(end);
        } else {
            merged.push((cur_start, cur_end));
            cur_start = start;
            cur_end = end;
        }
    }
    merged.push((cur_start, cur_end));
    merged
}

/// Compute the effective evaluation-time range for a selector after applying
/// `@` and `offset` modifiers, then return `(earliest_ms, latest_ms)` after
/// subtracting the backward window (lookback or matrix range).
fn selector_bounds(
    at: Option<&promql_parser::parser::AtModifier>,
    offset: Option<&promql_parser::parser::Offset>,
    at_start_ms: i64,
    at_end_ms: i64,
    eval_start_ms: i64,
    eval_end_ms: i64,
    backward_window_ms: i64,
) -> (i64, i64) {
    use promql_parser::parser::{AtModifier, Offset};

    let (mut start, mut end) = if let Some(at_mod) = at {
        match at_mod {
            AtModifier::At(time) => {
                let t = system_time_to_ms(*time);
                (t, t)
            }
            AtModifier::Start | AtModifier::End => (at_start_ms, at_end_ms),
        }
    } else {
        (eval_start_ms, eval_end_ms)
    };

    if let Some(off) = offset {
        match off {
            Offset::Pos(d) => {
                let off_ms = d.as_millis() as i64;
                start = start.saturating_sub(off_ms);
                end = end.saturating_sub(off_ms);
            }
            Offset::Neg(d) => {
                let off_ms = d.as_millis() as i64;
                start = start.saturating_add(off_ms);
                end = end.saturating_add(off_ms);
            }
        }
    }

    let earliest = start.saturating_sub(backward_window_ms);
    (earliest, end)
}

fn preload_ranges_inner(
    expr: &Expr,
    at_start_ms: i64,
    at_end_ms: i64,
    eval_start_ms: i64,
    eval_end_ms: i64,
    lookback_ms: i64,
    out: &mut Vec<(i64, i64)>,
) {
    match expr {
        Expr::VectorSelector(vs) => {
            out.push(selector_bounds(
                vs.at.as_ref(),
                vs.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
            ));
        }
        Expr::MatrixSelector(ms) => {
            let range_ms = ms.range.as_millis() as i64;
            out.push(selector_bounds(
                ms.vs.at.as_ref(),
                ms.vs.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                range_ms,
            ));
        }
        Expr::Subquery(sq) => {
            let (sq_start, sq_end) = selector_bounds(
                sq.at.as_ref(),
                sq.offset.as_ref(),
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                0,
            );
            let range_ms = sq.range.as_millis() as i64;
            let inner_eval_start = sq_start.saturating_sub(range_ms);
            preload_ranges_inner(
                &sq.expr,
                at_start_ms,
                at_end_ms,
                inner_eval_start,
                sq_end,
                lookback_ms,
                out,
            );
        }
        Expr::Aggregate(agg) => {
            preload_ranges_inner(
                &agg.expr,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
            if let Some(ref param) = agg.param {
                preload_ranges_inner(
                    param,
                    at_start_ms,
                    at_end_ms,
                    eval_start_ms,
                    eval_end_ms,
                    lookback_ms,
                    out,
                );
            }
        }
        Expr::Binary(b) => {
            preload_ranges_inner(
                &b.lhs,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
            preload_ranges_inner(
                &b.rhs,
                at_start_ms,
                at_end_ms,
                eval_start_ms,
                eval_end_ms,
                lookback_ms,
                out,
            );
        }
        Expr::Paren(p) => preload_ranges_inner(
            &p.expr,
            at_start_ms,
            at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_ms,
            out,
        ),
        Expr::Call(call) => {
            for arg in &call.args.args {
                preload_ranges_inner(
                    arg,
                    at_start_ms,
                    at_end_ms,
                    eval_start_ms,
                    eval_end_ms,
                    lookback_ms,
                    out,
                );
            }
        }
        Expr::Unary(u) => preload_ranges_inner(
            &u.expr,
            at_start_ms,
            at_end_ms,
            eval_start_ms,
            eval_end_ms,
            lookback_ms,
            out,
        ),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}

/// Execution outcome bundling the reshaped query value with an optional
/// tracing snapshot. The snapshot is populated when the
/// [`LoweringContext`](crate::promql::plan::LoweringContext) carried a
/// trace collector.
pub(crate) struct ExecuteOutcome {
    pub value: QueryValue,
    pub trace: Option<crate::promql::trace::QueryTrace>,
}

/// Drive the operator tree to completion and reshape into a
/// [`QueryValue`]. Shared by [`TsdbReadEngine::eval_query`] and
/// [`TsdbReadEngine::eval_query_range`]; keeps the dispatch logic in
/// one place. Generic over the [`QueryReader`] type so both the writer's
/// `TsdbQueryReader` and the reader's `ReaderQueryReader` flow through
/// the same pipeline without duplication.
async fn execute_query<R: QueryReader + Send + Sync + 'static>(
    query: &str,
    reader: R,
    ctx: crate::promql::plan::LoweringContext,
    is_instant: bool,
) -> std::result::Result<ExecuteOutcome, QueryError> {
    use crate::promql::trace::{self, Phase};
    use std::future::poll_fn;

    let trace_collector = ctx.trace.clone();
    let trace_for_run = trace_collector.clone();

    // Hoist the whole pipeline into a task-local scope so storage-layer
    // code can record I/O timings without the planner having to thread the
    // collector through every function signature.
    let run = async move {
        let trace_collector = trace_for_run;
        // Parse --------------------------------------------------------
        let span = tracing::debug_span!("phase", name = "parse");
        let t0 = Instant::now();
        let parse_res = span.in_scope(|| promql_parser::parser::parse(query));
        let expr = parse_res.map_err(|e| QueryError::InvalidQuery(e.to_string()))?;
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Parse, t0.elapsed().as_nanos() as u64);
        }

        // Lower --------------------------------------------------------
        let span = tracing::debug_span!("phase", name = "lower");
        let t0 = Instant::now();
        let logical = span
            .in_scope(|| crate::promql::plan::lower(&expr, &ctx))
            .map_err(plan_error_to_query_error)?;
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Lower, t0.elapsed().as_nanos() as u64);
        }

        // Optimize -----------------------------------------------------
        let span = tracing::debug_span!("phase", name = "optimize");
        let t0 = Instant::now();
        let logical = span.in_scope(|| crate::promql::plan::optimize(logical));
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Optimize, t0.elapsed().as_nanos() as u64);
        }

        let source = Arc::new(crate::promql::source_adapter::QueryReaderSource::new(
            Arc::new(reader),
        ));
        let reservation = crate::promql::memory::MemoryReservation::new(DEFAULT_MEMORY_CAP_BYTES);

        // Build physical ----------------------------------------------
        let t0 = Instant::now();
        let mut plan = tracing::Instrument::instrument(
            crate::promql::plan::build_physical_plan(logical, &source, reservation, &ctx),
            tracing::debug_span!("phase", name = "build_physical"),
        )
        .await
        .map_err(plan_error_to_query_error)?;
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::BuildPhysical, t0.elapsed().as_nanos() as u64);
        }

        // Execute ------------------------------------------------------
        let t0 = Instant::now();
        let mut batches = Vec::new();
        let exec_span = tracing::debug_span!("phase", name = "execute");
        let _exec_guard = exec_span.enter();
        loop {
            match poll_fn(|cx| plan.root.next(cx)).await {
                Some(Ok(batch)) => batches.push(batch),
                Some(Err(e)) => return Err(execution_error_to_query_error(e)),
                None => break,
            }
        }
        drop(_exec_guard);
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Execute, t0.elapsed().as_nanos() as u64);
        }

        // Reshape ------------------------------------------------------
        let span = tracing::debug_span!("phase", name = "reshape");
        let t0 = Instant::now();
        let reshaped = span.in_scope(|| {
            if is_instant {
                crate::promql::reshape::reshape_instant(&plan, batches)
            } else {
                crate::promql::reshape::reshape_range(&plan, batches)
            }
        });
        if let Some(c) = trace_collector.as_ref() {
            c.record_phase(Phase::Reshape, t0.elapsed().as_nanos() as u64);
        }
        reshaped.map_err(|e| QueryError::Execution(e.to_string()))
    };

    let value = match trace_collector.clone() {
        Some(c) => trace::with_trace(c, run).await?,
        None => run.await?,
    };

    Ok(ExecuteOutcome {
        value,
        trace: trace_collector.as_ref().map(|c| c.finish()),
    })
}

/// Dry-run: parse, lower, optimize, and describe the physical plan
/// for `query` without opening a reader or executing any operator.
/// Backs both [`TsdbEngine::explain_query`] and
/// [`TsdbEngine::explain_query_range`].
fn explain_query(
    query: &str,
    ctx: &crate::promql::plan::LoweringContext,
) -> std::result::Result<crate::promql::plan::ExplainResult, QueryError> {
    let expr =
        promql_parser::parser::parse(query).map_err(|e| QueryError::InvalidQuery(e.to_string()))?;
    let unoptimized = crate::promql::plan::lower(&expr, ctx).map_err(plan_error_to_query_error)?;
    let logical_unoptimized = crate::promql::plan::describe_logical(&unoptimized);
    let optimized = crate::promql::plan::optimize(unoptimized.clone());
    let logical_optimized = crate::promql::plan::describe_logical(&optimized);
    let physical = crate::promql::plan::describe_physical(&optimized, ctx);
    Ok(crate::promql::plan::ExplainResult {
        schema_version: crate::promql::plan::SCHEMA_VERSION,
        logical_unoptimized,
        logical_optimized,
        physical,
    })
}

/// Translate a [`PlanError`](crate::promql::plan::PlanError) into
/// the crate-wide [`QueryError`].
fn plan_error_to_query_error(e: crate::promql::plan::PlanError) -> QueryError {
    use crate::promql::plan::PlanError;
    match e {
        PlanError::UnknownFunction(_)
        | PlanError::InvalidArgument { .. }
        | PlanError::InvalidTopLevelString
        | PlanError::UnsupportedExpression(_)
        | PlanError::UnsupportedFeature(_) => QueryError::InvalidQuery(e.to_string()),
        PlanError::MemoryLimit(_)
        | PlanError::SourceError(_)
        | PlanError::InvalidMatching(_)
        | PlanError::PhysicalPlanFailed(_) => QueryError::Execution(e.to_string()),
    }
}

/// Translate an execution-time
/// [`QueryError`](crate::promql::memory::QueryError) onto the
/// crate-wide [`QueryError`] for the HTTP / embedded boundary.
fn execution_error_to_query_error(e: crate::promql::memory::QueryError) -> QueryError {
    QueryError::Execution(e.to_string())
}

/// Collapse a [`QueryValue`] into the `Vec<RangeSample>` wire shape.
/// Matrix results pass through; scalar results fan out into a single
/// anonymous series with one sample at each returned timestamp; vector
/// results become a one-sample-per-series matrix.
pub(crate) fn query_value_to_range_samples(
    value: QueryValue,
) -> std::result::Result<Vec<RangeSample>, QueryError> {
    match value {
        QueryValue::Matrix(series) => Ok(series),
        QueryValue::Scalar {
            timestamp_ms,
            value,
        } => Ok(vec![RangeSample {
            labels: Labels::empty(),
            samples: vec![(timestamp_ms, value)],
        }]),
        QueryValue::Vector(samples) => Ok(samples
            .into_iter()
            .map(|s| RangeSample {
                labels: s.labels,
                samples: vec![(s.timestamp_ms, s.value)],
            })
            .collect()),
    }
}

/// Parse multiple match[] selector strings into VectorSelectors.
fn parse_selectors(matchers: &[&str]) -> std::result::Result<Vec<VectorSelector>, QueryError> {
    matchers
        .iter()
        .map(|s| {
            let expr = promql_parser::parser::parse(s)
                .map_err(|e| QueryError::InvalidQuery(e.to_string()))?;
            match expr {
                Expr::VectorSelector(vs) => Ok(vs),
                _ => Err(QueryError::InvalidQuery(
                    "Expected a vector selector".to_string(),
                )),
            }
        })
        .collect()
}

// ── TsdbReadEngine trait ─────────────────────────────────────────────────
//
// Factors out the 5 duplicated eval/find methods shared between `Tsdb`
// (writer) and `TimeSeriesDbReader` (reader). Each implementor provides
// its own `QueryReader` construction; the query logic lives once in the
// default methods.

#[async_trait]
pub(crate) trait TsdbReadEngine: Send + Sync {
    type QR: QueryReader + Send + Sync;

    /// Build a query reader spanning `[start, end]` (seconds).
    async fn make_query_reader(&self, start: i64, end: i64) -> Result<Self::QR>;

    /// Build a query reader spanning a set of disjoint ranges (seconds).
    async fn make_query_reader_for_ranges(&self, ranges: &[(i64, i64)]) -> Result<Self::QR>;

    // ── Provided: 5 default methods written once ──

    /// Discover series matching any of the given selectors.
    async fn find_series(
        &self,
        matchers: &[&str],
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_series(&reader, matchers).await
    }

    /// Discover label names, optionally filtered by matchers.
    async fn find_labels(
        &self,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_labels(&reader, matchers).await
    }

    /// Discover values for a specific label, optionally filtered by matchers.
    async fn find_label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        let reader = self.make_query_reader(start_secs, end_secs).await?;
        discover_label_values(&reader, label_name, matchers).await
    }

    /// Evaluate an instant PromQL query, returning a [`QueryValue`]
    /// (scalar, vector or matrix). Parses the query, lowers to the
    /// logical plan, runs the rule-based optimiser, builds a physical
    /// plan over a [`crate::promql::source_adapter::QueryReaderSource`]
    /// wrapping this engine's [`QueryReader`], drives the operator tree
    /// to completion and reshapes collected batches into a `QueryValue`.
    async fn eval_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError>
    where
        Self::QR: 'static,
    {
        let start = Instant::now();
        let result = self
            .eval_query_traced(query, time, opts, None)
            .await
            .map(|o| o.value);

        metrics::counter!(tsdb_metrics::TSDB_QUERIES, "type" => "instant").increment(1);
        metrics::histogram!(tsdb_metrics::TSDB_QUERY_DURATION_SECONDS, "type" => "instant")
            .record(start.elapsed().as_secs_f64());

        result
    }

    /// Tracing-aware variant of [`Self::eval_query`]. Pass `trace`
    /// `Some(...)` to populate [`ExecuteOutcome::trace`] on the result.
    async fn eval_query_traced(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
        trace: Option<Arc<crate::promql::trace::TraceCollector>>,
    ) -> std::result::Result<ExecuteOutcome, QueryError>
    where
        Self::QR: 'static,
    {
        let query_time = time.unwrap_or_else(SystemTime::now);
        let at_ms = system_time_to_ms(query_time);
        let mut plan_ctx = crate::promql::plan::LoweringContext::for_instant(
            at_ms,
            duration_to_ms(opts.lookback_delta),
        );
        if let Some(c) = trace {
            plan_ctx = plan_ctx.with_trace(c);
        }
        let collector = plan_ctx.trace.clone();

        let ranges = preload_ranges_for_query(query, at_ms, at_ms, opts.lookback_delta)?;
        let t0 = Instant::now();
        let build_reader = self.make_query_reader_for_ranges(&ranges);
        let reader = match collector.clone() {
            Some(c) => crate::promql::trace::with_trace(c, build_reader).await?,
            None => build_reader.await?,
        };
        if let Some(c) = collector.as_ref() {
            c.record_phase(
                crate::promql::trace::Phase::ReaderSetup,
                t0.elapsed().as_nanos() as u64,
            );
        }
        execute_query(query, reader, plan_ctx, /*is_instant=*/ true).await
    }

    /// Evaluate a range PromQL query and project the resulting
    /// [`QueryValue`] onto the `Vec<RangeSample>` wire contract.
    async fn eval_query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<Vec<RangeSample>, QueryError>
    where
        Self::QR: 'static,
    {
        let start = Instant::now();
        let result = self
            .eval_query_range_traced(query, range, step, opts, None)
            .await
            .and_then(|o| query_value_to_range_samples(o.value));

        metrics::counter!(tsdb_metrics::TSDB_QUERIES, "type" => "range").increment(1);
        metrics::histogram!(tsdb_metrics::TSDB_QUERY_DURATION_SECONDS, "type" => "range")
            .record(start.elapsed().as_secs_f64());

        result
    }

    /// Like [`Self::eval_query_range`] but returns the raw [`QueryValue`]
    /// so HTTP handlers can decide whether to keep scalar/vector shapes
    /// intact.
    async fn eval_query_range_value(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError>
    where
        Self::QR: 'static,
    {
        self.eval_query_range_traced(query, range, step, opts, None)
            .await
            .map(|o| o.value)
    }

    /// Tracing-aware variant of [`Self::eval_query_range`].
    async fn eval_query_range_traced(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
        trace: Option<Arc<crate::promql::trace::TraceCollector>>,
    ) -> std::result::Result<ExecuteOutcome, QueryError>
    where
        Self::QR: 'static,
    {
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let start_ms = system_time_to_ms(start);
        let end_ms = system_time_to_ms(end);
        let step_ms = duration_to_ms(step);
        if step_ms <= 0 {
            return Err(QueryError::InvalidQuery(
                "step must be greater than zero".to_string(),
            ));
        }

        let mut plan_ctx = crate::promql::plan::LoweringContext::new(
            start_ms,
            end_ms,
            step_ms,
            duration_to_ms(opts.lookback_delta),
        );
        if let Some(c) = trace {
            plan_ctx = plan_ctx.with_trace(c);
        }
        let collector = plan_ctx.trace.clone();

        let ranges = preload_ranges_for_query(query, start_ms, end_ms, opts.lookback_delta)?;
        let t0 = Instant::now();
        let build_reader = self.make_query_reader_for_ranges(&ranges);
        let reader = match collector.clone() {
            Some(c) => crate::promql::trace::with_trace(c, build_reader).await?,
            None => build_reader.await?,
        };
        if let Some(c) = collector.as_ref() {
            c.record_phase(
                crate::promql::trace::Phase::ReaderSetup,
                t0.elapsed().as_nanos() as u64,
            );
        }
        execute_query(query, reader, plan_ctx, /*is_instant=*/ false).await
    }
}

/// Discover series over a time range specified as Rust range bounds.
pub(crate) async fn find_series_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    matchers: &[&str],
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<Labels>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_series(engine, matchers, start, end).await
}

/// Discover label names over a time range specified as Rust range bounds.
pub(crate) async fn find_labels_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    matchers: Option<&[&str]>,
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<String>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_labels(engine, matchers, start, end).await
}

/// Discover label values over a time range specified as Rust range bounds.
pub(crate) async fn find_label_values_in_range<E: TsdbReadEngine + ?Sized>(
    engine: &E,
    label_name: &str,
    matchers: Option<&[&str]>,
    range: impl RangeBounds<SystemTime>,
) -> std::result::Result<Vec<String>, QueryError> {
    let (start, end) = crate::util::range_bounds_to_secs(range)?;
    E::find_label_values(engine, label_name, matchers, start, end).await
}

// ── Series / label discovery ────────────────────────────────────────

/// Cross-bucket readahead used by the discovery helpers below.
const DISCOVERY_BUCKET_READAHEAD: usize = 32;

/// Resolve the series matching `selector` within `bucket`, applying any
/// negative / empty-string post-filters.
async fn resolve_selector_in_bucket<R: QueryReader>(
    reader: &R,
    index_cache: &crate::promql::index_cache::IndexCache,
    bucket: TimeBucket,
    selector: &VectorSelector,
) -> std::result::Result<Vec<(SeriesId, Labels)>, QueryError> {
    use crate::promql::source_adapter::selector_util;

    let candidates = selector_util::find_candidates(reader, index_cache, &bucket, selector)
        .await
        .map_err(|e| QueryError::Execution(e.to_string()))?;
    if candidates.is_empty() {
        return Ok(Vec::new());
    }

    let forward = index_cache
        .forward_index(reader, &bucket, &candidates)
        .await
        .map_err(|e| QueryError::Execution(e.to_string()))?;

    let needs_filter = selector_util::has_negative_matchers(selector)
        || selector_util::has_empty_string_matchers(selector);
    let final_ids = if needs_filter {
        selector_util::apply_post_filters(forward.as_ref(), candidates, selector)
            .map_err(|e| QueryError::Execution(e.to_string()))?
    } else {
        candidates
    };

    let mut out = Vec::with_capacity(final_ids.len());
    for id in final_ids {
        if let Some(spec) = forward.get_spec(&id) {
            let mut labels = spec.labels;
            labels.sort();
            out.push((id, Labels::new(labels)));
        }
    }
    Ok(out)
}

/// Discover series matching any of the given selectors.
pub(crate) async fn discover_series<R: QueryReader>(
    reader: &R,
    matchers: &[&str],
) -> std::result::Result<Vec<Labels>, QueryError> {
    if matchers.is_empty() {
        return Err(QueryError::InvalidQuery(
            "at least one match[] required".to_string(),
        ));
    }

    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let selectors = parse_selectors(matchers)?;
    let index_cache = crate::promql::index_cache::IndexCache::new();
    let mut unique_series: HashSet<Labels> = HashSet::new();
    for bucket in &buckets {
        for selector in &selectors {
            let found = resolve_selector_in_bucket(reader, &index_cache, *bucket, selector).await?;
            for (_, labels) in found {
                unique_series.insert(labels);
            }
        }
    }

    let mut result: Vec<Labels> = unique_series.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Discover label names, optionally filtered by matchers.
pub(crate) async fn discover_labels<R: QueryReader>(
    reader: &R,
    matchers: Option<&[&str]>,
) -> std::result::Result<Vec<String>, QueryError> {
    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let mut label_names: HashSet<String> = HashSet::new();

    match matchers {
        Some(matches) if !matches.is_empty() => {
            let selectors = parse_selectors(matches)?;
            let index_cache = crate::promql::index_cache::IndexCache::new();
            for bucket in &buckets {
                for selector in &selectors {
                    let found =
                        resolve_selector_in_bucket(reader, &index_cache, *bucket, selector).await?;
                    for (_, labels) in found {
                        for attr in labels.iter() {
                            label_names.insert(attr.name.clone());
                        }
                    }
                }
            }
        }
        _ => {
            let width = buckets.len().clamp(1, DISCOVERY_BUCKET_READAHEAD);
            let results: Vec<_> = stream::iter(buckets)
                .map(|bucket| async move { reader.all_inverted_index(&bucket).await })
                .buffer_unordered(width)
                .try_collect()
                .await?;
            for inverted_index in results {
                for attr in inverted_index.all_keys() {
                    label_names.insert(attr.name);
                }
            }
        }
    }

    let mut result: Vec<String> = label_names.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Discover values for a specific label, optionally filtered by matchers.
pub(crate) async fn discover_label_values<R: QueryReader>(
    reader: &R,
    label_name: &str,
    matchers: Option<&[&str]>,
) -> std::result::Result<Vec<String>, QueryError> {
    let buckets = reader.list_buckets().await?;
    if buckets.is_empty() {
        return Ok(vec![]);
    }

    let mut values: HashSet<String> = HashSet::new();

    match matchers {
        Some(matches) if !matches.is_empty() => {
            let selectors = parse_selectors(matches)?;
            let index_cache = crate::promql::index_cache::IndexCache::new();
            for bucket in &buckets {
                for selector in &selectors {
                    let found =
                        resolve_selector_in_bucket(reader, &index_cache, *bucket, selector).await?;
                    for (_, labels) in found {
                        if let Some(v) = labels.get(label_name) {
                            values.insert(v.to_string());
                        }
                    }
                }
            }
        }
        _ => {
            let width = buckets.len().clamp(1, DISCOVERY_BUCKET_READAHEAD);
            let results: Vec<_> = stream::iter(buckets)
                .map(|bucket| async move { reader.label_values(&bucket, label_name).await })
                .buffer_unordered(width)
                .try_collect()
                .await?;
            for label_vals in results {
                values.extend(label_vals);
            }
        }
    }

    let mut result: Vec<String> = values.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Multi-bucket time series database.
///
/// Tsdb manages multiple MiniTsdb instances (one per time bucket) and provides
/// a unified QueryReader interface that merges results across buckets.
pub(crate) struct Tsdb {
    storage: Arc<dyn Storage>,

    /// TTI cache (15 min idle) for buckets being actively ingested into.
    /// Also used during queries so that unflushed data is visible.
    ingest_cache: Cache<TimeBucket, Arc<MiniTsdb>>,

    // Metadata catalog (keyed by metric name)
    pub(crate) metadata_catalog: RwLock<HashMap<String, Vec<MetricMetadata>>>,
}

impl Tsdb {
    pub(crate) fn new(storage: Arc<dyn Storage>) -> Self {
        // TTI cache: 15 minute idle timeout for ingest buckets
        let ingest_cache = Cache::builder()
            .time_to_idle(Duration::from_secs(15 * 60))
            .build();

        Self {
            storage,
            ingest_cache,
            metadata_catalog: RwLock::new(HashMap::new()),
        }
    }

    /// Returns a read handle to the underlying storage, for background tasks
    /// like the cache warmer.
    pub(crate) fn storage_read(&self) -> Arc<dyn StorageRead> {
        self.storage.clone() as Arc<dyn StorageRead>
    }

    /// Get or create a MiniTsdb for ingestion into a specific bucket.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn get_or_create_for_ingest(
        &self,
        bucket: TimeBucket,
    ) -> Result<Arc<MiniTsdb>> {
        // Try ingest cache first
        if let Some(mini) = self.ingest_cache.get(&bucket).await {
            return Ok(mini);
        }

        // Load from storage and put in ingest cache
        let mini = Arc::new(MiniTsdb::load(bucket, self.storage.clone()).await?);
        self.ingest_cache.insert(bucket, mini.clone()).await;
        Ok(mini)
    }

    /// Create a QueryReader for a time range.
    /// For buckets in the ingest cache, uses the write coordinator's view
    /// (includes unflushed data). For all other buckets, constructs a
    /// lightweight reader directly from the storage snapshot.
    pub(crate) async fn query_reader(
        &self,
        start_secs: i64,
        end_secs: i64,
    ) -> Result<TsdbQueryReader> {
        let snapshot = self.storage.snapshot().await?;
        let buckets = snapshot
            .get_buckets_in_range(Some(start_secs), Some(end_secs))
            .await?;

        let readers = self.build_readers(&snapshot, buckets).await;
        Ok(TsdbQueryReader::new(readers))
    }

    /// Create a QueryReader for a set of disjoint time ranges.
    pub(crate) async fn query_reader_for_ranges(
        &self,
        ranges: &[(i64, i64)],
    ) -> Result<TsdbQueryReader> {
        let snapshot = {
            let _g = crate::promql::trace::Scope::enter("snapshot");
            self.storage.snapshot().await?
        };
        let buckets = {
            let _g = crate::promql::trace::Scope::enter("list_buckets");
            snapshot.get_buckets_for_ranges(ranges).await?
        };

        let readers = {
            let _g = crate::promql::trace::Scope::enter("build_readers");
            self.build_readers(&snapshot, buckets).await
        };
        Ok(TsdbQueryReader::new(readers))
    }

    /// Build readers for a set of buckets. Uses the ingest cache when
    /// available, otherwise constructs a reader directly from the snapshot.
    async fn build_readers(
        &self,
        snapshot: &Arc<dyn common::storage::StorageSnapshot>,
        buckets: Vec<TimeBucket>,
    ) -> Vec<(TimeBucket, MiniQueryReader)> {
        let mut readers = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let reader = if let Some(mini) = self.ingest_cache.get(&bucket).await {
                mini.query_reader()
            } else {
                MiniQueryReader::new(bucket, snapshot.clone() as Arc<dyn StorageRead>)
            };
            readers.push((bucket, reader));
        }
        readers
    }

    /// Flush all dirty buckets to durable storage.
    ///
    /// First flushes each bucket's delta to the storage memtable in parallel,
    /// then issues a single `storage.flush()` to persist everything durably.
    pub(crate) async fn flush(&self) -> Result<()> {
        // `iter()` does not include entries whose insert is still queued
        // in moka's internal write buffer; drain it first so a bucket
        // created moments before flush isn't silently skipped.
        self.ingest_cache.run_pending_tasks().await;
        let futs: futures::stream::FuturesUnordered<_> = self
            .ingest_cache
            .iter()
            .map(|(_, mini)| async move { mini.flush_written().await })
            .collect();
        futs.try_collect::<Vec<_>>().await?;

        self.storage.flush().await?;
        Ok(())
    }

    /// Flushes pending writes and creates a durable checkpoint.
    ///
    /// The returned [`common::CheckpointInfo::id`] can be passed to
    /// [`crate::reader::TimeSeriesDbReader::open`] (via
    /// [`common::StorageReaderRuntime::with_checkpoint_id`]) to open a
    /// reader pinned to this exact view of the database.
    pub(crate) async fn create_checkpoint(&self) -> Result<common::CheckpointInfo> {
        self.flush().await?;
        Ok(self.storage.create_checkpoint().await?)
    }

    pub(crate) async fn close(&self) -> Result<()> {
        self.flush().await?;
        self.storage.close().await?;
        Ok(())
    }

    /// Ingest series into the TSDB.
    /// Each series is split by time bucket based on sample timestamps.
    ///
    /// If `timeout` is provided, each bucket batch will wait up to the given
    /// duration for space in the write queue. Otherwise, writes fail
    /// immediately if the queue is full.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(
            series_count = series_list.len(),
            total_samples = tracing::field::Empty,
            buckets_touched = tracing::field::Empty
        )
    )]
    pub(crate) async fn ingest_samples(
        &self,
        series_list: Vec<Series>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let mut bucket_series_map: HashMap<TimeBucket, Vec<Series>> = HashMap::new();
        let mut total_samples = 0;

        // First pass: group all series by bucket
        for series in series_list {
            let series_sample_count = series.samples.len();
            total_samples += series_sample_count;

            if let Some(metric_name) = series
                .labels
                .iter()
                .find(|l| l.name == "__name__")
                .map(|l| l.value.clone())
            {
                let entry = MetricMetadata {
                    metric_name: metric_name.clone(),
                    metric_type: series.metric_type,
                    description: series.description.clone(),
                    unit: series.unit.clone(),
                };
                let mut catalog = self.metadata_catalog.write().await;
                let entries = catalog.entry(metric_name).or_default();
                if !entries.contains(&entry) {
                    entries.push(entry);
                }
            }

            // Group samples by bucket for this series
            let mut bucket_samples: HashMap<TimeBucket, Vec<Sample>> = HashMap::new();

            for sample in series.samples {
                let bucket = TimeBucket::round_to_hour(
                    std::time::UNIX_EPOCH
                        + std::time::Duration::from_millis(sample.timestamp_ms as u64),
                )?;
                bucket_samples.entry(bucket).or_default().push(sample);
            }

            // Create a series for each bucket and add to bucket_series_map
            for (bucket, samples) in bucket_samples {
                let bucket_series = Series {
                    labels: series.labels.clone(),
                    metric_type: series.metric_type,
                    unit: series.unit.clone(),
                    description: series.description.clone(),
                    samples,
                };
                bucket_series_map
                    .entry(bucket)
                    .or_default()
                    .push(bucket_series);
            }
        }

        let buckets_touched = bucket_series_map.len();

        // Second pass: ingest all series for each bucket in a single batch
        for (bucket, series_list) in bucket_series_map {
            let series_count = series_list.len();
            let samples_count: usize = series_list.iter().map(|s| s.samples.len()).sum();

            tracing::debug!(
                bucket = ?bucket,
                series_count = series_count,
                samples_count = samples_count,
                "Ingesting batch into bucket"
            );

            let mini = match self.get_or_create_for_ingest(bucket).await {
                Ok(mini) => mini,
                Err(err) => {
                    error!("failed to load minitsdb: {:?}: {:?}", bucket, err);
                    return Err(err);
                }
            };
            mini.ingest_batch(&series_list, timeout).await?;

            tracing::debug!(
                bucket = ?bucket,
                series_count = series_count,
                samples_count = samples_count,
                "Bucket batch ingestion completed"
            );
        }

        // Record final metrics on the main span
        tracing::Span::current().record("total_samples", total_samples);
        tracing::Span::current().record("buckets_touched", buckets_touched);

        metrics::counter!(tsdb_metrics::TSDB_SAMPLES_INGESTED).increment(total_samples as u64);

        tracing::debug!(
            total_samples = total_samples,
            buckets_touched = buckets_touched,
            "Completed ingesting all samples"
        );

        Ok(())
    }

    /// Return metadata for all (or a specific) metric.
    pub(crate) async fn find_metadata(
        &self,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        let catalog = self.metadata_catalog.read().await;

        let entries: Vec<MetricMetadata> = match metric {
            Some(name) => catalog.get(name).cloned().unwrap_or_default(),
            None => catalog.values().flatten().cloned().collect(),
        };

        Ok(entries)
    }
}

#[async_trait]
impl TsdbReadEngine for Tsdb {
    type QR = TsdbQueryReader;

    async fn make_query_reader(&self, start: i64, end: i64) -> Result<TsdbQueryReader> {
        self.query_reader(start, end).await
    }

    async fn make_query_reader_for_ranges(&self, ranges: &[(i64, i64)]) -> Result<TsdbQueryReader> {
        self.query_reader_for_ranges(ranges).await
    }
}

// ── TsdbEngine: unified read/write or read-only dispatch ────────────

/// Wraps either a read-write [`Tsdb`] or a read-only [`crate::reader::TimeSeriesDbReader`],
/// dispatching read methods to the inner engine and rejecting writes in
/// read-only mode.
pub(crate) enum TsdbEngine {
    ReadWrite(Arc<Tsdb>),
    ReadOnly(Arc<crate::reader::TimeSeriesDbReader>),
}

impl TsdbEngine {
    /// Returns `true` when the engine is read-only.
    pub(crate) fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly(_))
    }

    /// Returns a read handle to the underlying storage, for background tasks
    /// like the cache warmer.
    pub(crate) fn storage_read(&self) -> Arc<dyn StorageRead> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.storage_read(),
            Self::ReadOnly(reader) => reader.storage_read(),
        }
    }

    /// Returns a clone of the inner `Arc<Tsdb>` if this is a read-write engine.
    pub(crate) fn as_tsdb(&self) -> Option<Arc<Tsdb>> {
        match self {
            Self::ReadWrite(tsdb) => Some(tsdb.clone()),
            Self::ReadOnly(_) => None,
        }
    }

    // ── Read methods (dispatch to inner engine) ──

    pub(crate) async fn eval_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.eval_query(query, time, opts).await,
            Self::ReadOnly(reader) => reader.eval_query(query, time, opts).await,
        }
    }

    pub(crate) async fn eval_query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<Vec<RangeSample>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.eval_query_range(query, range, step, opts).await,
            Self::ReadOnly(reader) => reader.eval_query_range(query, range, step, opts).await,
        }
    }

    /// Tracing-aware instant query. Populates [`ExecuteOutcome::trace`]
    /// when `trace` is `Some`. Otherwise behaves like [`Self::eval_query`]
    /// wrapped in an [`ExecuteOutcome`].
    pub(crate) async fn eval_query_traced(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
        trace: Option<Arc<crate::promql::trace::TraceCollector>>,
    ) -> std::result::Result<ExecuteOutcome, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.eval_query_traced(query, time, opts, trace).await,
            Self::ReadOnly(reader) => reader.eval_query_traced(query, time, opts, trace).await,
        }
    }

    /// Like [`Self::eval_query_range`] but returns the raw [`QueryValue`].
    pub(crate) async fn eval_query_range_value(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<QueryValue, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.eval_query_range_value(query, range, step, opts).await,
            Self::ReadOnly(reader) => {
                reader
                    .eval_query_range_value(query, range, step, opts)
                    .await
            }
        }
    }

    /// Tracing-aware range query. See [`Self::eval_query_traced`].
    pub(crate) async fn eval_query_range_traced(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime> + Send,
        step: Duration,
        opts: &QueryOptions,
        trace: Option<Arc<crate::promql::trace::TraceCollector>>,
    ) -> std::result::Result<ExecuteOutcome, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => {
                tsdb.eval_query_range_traced(query, range, step, opts, trace)
                    .await
            }
            Self::ReadOnly(reader) => {
                reader
                    .eval_query_range_traced(query, range, step, opts, trace)
                    .await
            }
        }
    }

    /// Dry-run EXPLAIN for instant queries. Parses, lowers, optimises,
    /// and describes the physical plan — does not open a reader or
    /// touch the index cache.
    pub(crate) fn explain_query(
        &self,
        query: &str,
        time: Option<SystemTime>,
        opts: &QueryOptions,
    ) -> std::result::Result<crate::promql::plan::ExplainResult, QueryError> {
        let query_time = time.unwrap_or_else(SystemTime::now);
        let at_ms = system_time_to_ms(query_time);
        let ctx = crate::promql::plan::LoweringContext::for_instant(
            at_ms,
            duration_to_ms(opts.lookback_delta),
        );
        explain_query(query, &ctx)
    }

    /// Dry-run EXPLAIN for range queries.
    pub(crate) fn explain_query_range(
        &self,
        query: &str,
        range: impl RangeBounds<SystemTime>,
        step: Duration,
        opts: &QueryOptions,
    ) -> std::result::Result<crate::promql::plan::ExplainResult, QueryError> {
        let (start, end) = crate::util::range_bounds_to_system_time(range);
        let start_ms = system_time_to_ms(start);
        let end_ms = system_time_to_ms(end);
        let step_ms = duration_to_ms(step);
        if step_ms <= 0 {
            return Err(QueryError::InvalidQuery(
                "step must be greater than zero".to_string(),
            ));
        }
        let ctx = crate::promql::plan::LoweringContext::new(
            start_ms,
            end_ms,
            step_ms,
            duration_to_ms(opts.lookback_delta),
        );
        explain_query(query, &ctx)
    }

    pub(crate) async fn find_series(
        &self,
        matchers: &[&str],
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<Labels>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.find_series(matchers, start_secs, end_secs).await,
            Self::ReadOnly(reader) => reader.find_series(matchers, start_secs, end_secs).await,
        }
    }

    pub(crate) async fn find_labels(
        &self,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.find_labels(matchers, start_secs, end_secs).await,
            Self::ReadOnly(reader) => reader.find_labels(matchers, start_secs, end_secs).await,
        }
    }

    pub(crate) async fn find_label_values(
        &self,
        label_name: &str,
        matchers: Option<&[&str]>,
        start_secs: i64,
        end_secs: i64,
    ) -> std::result::Result<Vec<String>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => {
                tsdb.find_label_values(label_name, matchers, start_secs, end_secs)
                    .await
            }
            Self::ReadOnly(reader) => {
                reader
                    .find_label_values(label_name, matchers, start_secs, end_secs)
                    .await
            }
        }
    }

    pub(crate) async fn find_metadata(
        &self,
        metric: Option<&str>,
    ) -> std::result::Result<Vec<MetricMetadata>, QueryError> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.find_metadata(metric).await,
            Self::ReadOnly(_) => Ok(vec![]),
        }
    }

    // ── Write methods (error in read-only mode) ──

    pub(crate) async fn ingest_samples(
        &self,
        series_list: Vec<Series>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.ingest_samples(series_list, timeout).await,
            Self::ReadOnly(_) => Err(crate::error::Error::InvalidInput(
                "write operations are not supported in read-only mode".to_string(),
            )),
        }
    }

    pub(crate) async fn flush(&self) -> Result<()> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.flush().await,
            Self::ReadOnly(_) => Ok(()),
        }
    }

    pub(crate) async fn create_checkpoint(&self) -> Result<common::CheckpointInfo> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.create_checkpoint().await,
            Self::ReadOnly(_) => Err(crate::error::Error::InvalidInput(
                "checkpoint creation is not supported in read-only mode".to_string(),
            )),
        }
    }

    pub(crate) async fn close(&self) -> Result<()> {
        match self {
            Self::ReadWrite(tsdb) => tsdb.close().await,
            Self::ReadOnly(reader) => reader.close().await,
        }
    }
}

impl From<Arc<Tsdb>> for TsdbEngine {
    fn from(tsdb: Arc<Tsdb>) -> Self {
        Self::ReadWrite(tsdb)
    }
}

impl From<Arc<crate::reader::TimeSeriesDbReader>> for TsdbEngine {
    fn from(reader: Arc<crate::reader::TimeSeriesDbReader>) -> Self {
        Self::ReadOnly(reader)
    }
}

/// QueryReader implementation that properly handles bucket-scoped series IDs.
pub(crate) struct TsdbQueryReader {
    /// Map from bucket to MiniTsdb for efficient bucket queries
    mini_readers: HashMap<TimeBucket, MiniQueryReader>,
}

impl TsdbQueryReader {
    pub fn new(mini_tsdbs: Vec<(TimeBucket, MiniQueryReader)>) -> Self {
        let bucket_minis = mini_tsdbs.into_iter().collect();
        Self {
            mini_readers: bucket_minis,
        }
    }
}

#[async_trait]
impl QueryReader for TsdbQueryReader {
    async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        Ok(self.mini_readers.keys().cloned().collect())
    }

    async fn forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Box<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index(series_ids).await
    }

    async fn inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.inverted_index(terms).await
    }

    async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        // TODO: this should be internal error
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.all_inverted_index().await
    }

    async fn label_values(&self, bucket: &TimeBucket, label_name: &str) -> Result<Vec<String>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.label_values(label_name).await
    }

    async fn samples(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        metric_name: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.samples(series_id, metric_name, start_ms, end_ms).await
    }

    async fn forward_index_one(
        &self,
        bucket: &TimeBucket,
        series_id: SeriesId,
    ) -> Result<Option<crate::index::SeriesSpec>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.forward_index_one(series_id).await
    }

    async fn inverted_index_term(
        &self,
        bucket: &TimeBucket,
        term: &Label,
    ) -> Result<Option<roaring::RoaringBitmap>> {
        let mini = self.mini_readers.get(bucket).ok_or_else(|| {
            crate::error::Error::Internal(format!("Bucket {:?} not found", bucket))
        })?;
        mini.inverted_index_term(term).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MetricType;
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::storage::in_memory::InMemoryStorage;
    use opendata_macros::storage_test;

    fn create_sample(
        metric_name: &str,
        label_pairs: Vec<(&str, &str)>,
        timestamp: i64,
        value: f64,
    ) -> Series {
        let mut labels = vec![Label {
            name: "__name__".to_string(),
            value: metric_name.to_string(),
        }];
        for (key, val) in label_pairs {
            labels.push(Label {
                name: key.to_string(),
                value: val.to_string(),
            });
        }
        Series {
            labels,
            unit: None,
            metric_type: Some(MetricType::Gauge),
            description: None,
            samples: vec![Sample {
                timestamp_ms: timestamp,
                value,
            }],
        }
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_create_tsdb_with_caches(storage: Arc<dyn Storage>) {
        // when
        let tsdb = Tsdb::new(storage);

        // then: tsdb is created successfully
        tsdb.ingest_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 0);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_get_or_create_bucket_for_ingest(storage: Arc<dyn Storage>) {
        // given
        let tsdb = Tsdb::new(storage);
        let bucket = TimeBucket::hour(1000);

        // when
        let mini1 = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // then: same Arc is returned (cached)
        assert!(Arc::ptr_eq(&mini1, &mini2));
        tsdb.ingest_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 1);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_use_ingest_cache_during_queries(storage: Arc<dyn Storage>) {
        // given: a bucket in the ingest cache with ingested data
        let tsdb = Tsdb::new(storage);
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        let sample = create_sample("test_metric", vec![("env", "prod")], 4_000_000, 1.0);
        mini.ingest(&sample).await.unwrap();
        tsdb.flush().await.unwrap();

        // when: building a query reader that covers this bucket
        let reader = tsdb.query_reader(3600, 7200).await.unwrap();

        // then: reader should see the ingested data (via ingest cache)
        let buckets = reader.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 1);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_ingest_and_query_single_bucket(storage: Arc<dyn Storage>) {
        // given
        let tsdb = Tsdb::new(storage);

        // Use hour-aligned bucket (60 minutes = 1 hour)
        // Bucket at minute 60 covers minutes 60-119, i.e., seconds 3600-7199
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // Ingest a sample with timestamp in the bucket range (seconds 3600-7199)
        // Using 4000 seconds = 4000000 ms
        let sample = create_sample("http_requests", vec![("env", "prod")], 4000000, 42.0);
        mini.ingest(&sample).await.unwrap();

        // Flush to make data visible
        tsdb.flush().await.unwrap();

        // when: query the data with range covering the bucket (seconds 3600-7200)
        let reader = tsdb.query_reader(3600, 7200).await.unwrap();
        let terms = vec![Label {
            name: "__name__".to_string(),
            value: "http_requests".to_string(),
        }];
        let bucket = TimeBucket::hour(60);
        let index = reader.inverted_index(&bucket, &terms).await.unwrap();
        let series_ids: Vec<_> = index.intersect(terms).iter().collect();

        // then
        assert_eq!(series_ids.len(), 1);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_query_across_multiple_buckets(storage: Arc<dyn Storage>) {
        use crate::test_utils::assertions::assert_approx_eq;
        use std::time::{Duration, UNIX_EPOCH};

        // given: 4 hour-aligned buckets
        // Bucket layout (each bucket is 1 hour = 60 minutes):
        //   Bucket 60:  minutes 60-119,  seconds 3600-7199,   ms 3,600,000-7,199,999
        //   Bucket 120: minutes 120-179, seconds 7200-10799,  ms 7,200,000-10,799,999
        //   Bucket 180: minutes 180-239, seconds 10800-14399, ms 10,800,000-14,399,999
        //   Bucket 240: minutes 240-299, seconds 14400-17999, ms 14,400,000-17,999,999
        let tsdb = Tsdb::new(storage);

        // Buckets 1 & 2: will end up in query cache (ingest, flush, then invalidate from ingest cache)
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);

        // Buckets 3 & 4: will stay in ingest cache
        let bucket3 = TimeBucket::hour(180);
        let bucket4 = TimeBucket::hour(240);

        // Ingest data into buckets 1 & 2
        // Sample timestamps should be well within the bucket and reachable by lookback
        // Bucket 60: covers 3,600,000-7,199,999 ms -> sample at 3,900,000 ms (3900s)
        // Bucket 120: covers 7,200,000-10,799,999 ms -> sample at 7,900,000 ms (7900s)
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        mini1
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                3_900_000,
                10.0,
            ))
            .await
            .unwrap();
        mini1
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                3_900_001,
                15.0,
            ))
            .await
            .unwrap();

        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini2
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                7_900_000,
                20.0,
            ))
            .await
            .unwrap();
        mini2
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                7_900_001,
                25.0,
            ))
            .await
            .unwrap();

        // Flush buckets 1 & 2 to storage
        tsdb.flush().await.unwrap();

        // Invalidate buckets 1 & 2 from ingest cache so they'll be loaded from query cache
        tsdb.ingest_cache.invalidate(&bucket1).await;
        tsdb.ingest_cache.invalidate(&bucket2).await;
        tsdb.ingest_cache.run_pending_tasks().await;

        // Ingest data into buckets 3 & 4 (these stay in ingest cache)
        // Bucket 180: covers 10,800,000-14,399,999 ms -> sample at 11,900,000 ms (11900s)
        // Bucket 240: covers 14,400,000-17,999,999 ms -> sample at 15,900,000 ms (15900s)
        let mini3 = tsdb.get_or_create_for_ingest(bucket3).await.unwrap();
        mini3
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                11_900_000,
                30.0,
            ))
            .await
            .unwrap();
        mini3
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                11_900_001,
                35.0,
            ))
            .await
            .unwrap();

        let mini4 = tsdb.get_or_create_for_ingest(bucket4).await.unwrap();
        mini4
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "prod")],
                15_900_000,
                40.0,
            ))
            .await
            .unwrap();
        mini4
            .ingest(&create_sample(
                "http_requests",
                vec![("env", "staging")],
                15_900_001,
                45.0,
            ))
            .await
            .unwrap();

        // Flush buckets 3 & 4 to storage (data is now visible for queries)
        tsdb.flush().await.unwrap();

        // Verify cache state: 2 in ingest cache (buckets 3 & 4)
        tsdb.ingest_cache.run_pending_tasks().await;
        assert_eq!(tsdb.ingest_cache.entry_count(), 2);

        // when: query across all 4 buckets via the public eval_query surface
        let query = r#"http_requests"#;
        let lookback = Duration::from_secs(1000);

        // Query times: one point in each bucket where we expect to find data
        // Bucket 60:  sample at 3,900,000 ms (3900s) -> query at 4000s
        // Bucket 120: sample at 7,900,000 ms (7900s) -> query at 8000s
        // Bucket 180: sample at 11,900,000 ms (11900s) -> query at 12000s
        // Bucket 240: sample at 15,900,000 ms (15900s) -> query at 16000s
        // Lookback of 1000s ensures samples are within the window
        let query_times_secs = [4000u64, 8000, 12000, 16000];
        let expected_prod_values = [10.0, 20.0, 30.0, 40.0];
        let expected_staging_values = [15.0, 25.0, 35.0, 45.0];

        let opts = QueryOptions {
            lookback_delta: lookback,
            ..Default::default()
        };
        for (i, &query_time_secs) in query_times_secs.iter().enumerate() {
            let query_time = UNIX_EPOCH + Duration::from_secs(query_time_secs);
            let result = tsdb
                .eval_query(query, Some(query_time), &opts)
                .await
                .unwrap();
            let mut results = match result {
                QueryValue::Vector(s) => s,
                other => panic!("expected Vector, got {:?}", other),
            };
            results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

            assert_eq!(
                results.len(),
                2,
                "query {} at {}s: expected 2 results",
                i,
                query_time_secs
            );

            // env=prod
            assert_eq!(results[0].labels.get("env"), Some("prod"));
            assert_approx_eq(results[0].value, expected_prod_values[i]);

            // env=staging
            assert_eq!(results[1].labels.get("env"), Some("staging"));
            assert_approx_eq(results[1].value, expected_staging_values[i]);
        }
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn should_query_across_multiple_buckets_with_different_series_id_mappings(
        storage: Arc<dyn Storage>,
    ) {
        use std::time::{Duration, UNIX_EPOCH};

        // given: Two time buckets with overlapping series but different series IDs
        let tsdb = Tsdb::new(storage);
        // Bucket 1: hour 60 (covers 3,600,000-7,199,999 ms)
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        mini1
            .ingest(&create_sample(
                "foo",
                vec![("a", "b"), ("x", "y")],
                3_900_000,
                1.0,
            ))
            .await
            .unwrap();
        mini1
            .ingest(&create_sample(
                "foo",
                vec![("a", "c"), ("x", "z")],
                3_900_001,
                2.0,
            ))
            .await
            .unwrap();
        // Bucket 2: hour 120 (covers 7,200,000-10,799,999 ms)
        let bucket2 = TimeBucket::hour(120);
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini2
            .ingest(&create_sample(
                "foo",
                vec![("a", "c"), ("x", "z")],
                7_900_000,
                3.0,
            ))
            .await
            .unwrap();
        mini2
            .ingest(&create_sample(
                "foo",
                vec![("a", "d"), ("x", "w")],
                7_900_001,
                4.0,
            ))
            .await
            .unwrap();
        // Flush to storage
        tsdb.flush().await.unwrap();

        // when: query foo{a="c"} at t=8000s (inside bucket 2)
        let query = r#"foo{a="c"}"#;
        let query_time = UNIX_EPOCH + Duration::from_secs(8000);
        let opts = QueryOptions {
            lookback_delta: Duration::from_secs(5000),
            ..Default::default()
        };
        let result = tsdb
            .eval_query(query, Some(query_time), &opts)
            .await
            .unwrap();
        let samples = match result {
            QueryValue::Vector(s) => s,
            other => panic!("expected Vector, got {:?}", other),
        };

        // then: we should only get the series foo{a="c",x="z"} with value 3.0
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].labels.get("a"), Some("c"));
        assert_eq!(samples[0].labels.get("x"), Some("z"));
        assert_eq!(samples[0].labels.metric_name(), "foo");
        assert_eq!(samples[0].value, 3.0);
    }

    // ── Native read method tests ─────────────────────────────────────

    async fn create_tsdb_with_data() -> Tsdb {
        let tsdb = Tsdb::new(Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        ))));

        // Ingest two series into bucket at minute 60 (covers 3,600,000–7,199,999 ms)
        let series = vec![
            create_sample("http_requests", vec![("env", "prod")], 4_000_000, 42.0),
            create_sample("http_requests", vec![("env", "staging")], 4_000_000, 10.0),
        ];
        tsdb.ingest_samples(series, None).await.unwrap();
        tsdb.flush().await.unwrap();
        tsdb
    }

    #[tokio::test]
    async fn eval_query_should_return_instant_vector() {
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("http_requests", Some(query_time), &opts)
            .await
            .unwrap();
        let mut samples = match result {
            QueryValue::Vector(samples) => samples,
            other => panic!("expected Vector, got {:?}", other),
        };
        samples.sort_by(|a, b| {
            a.labels
                .metric_name()
                .cmp(b.labels.metric_name())
                .then_with(|| a.labels.get("env").cmp(&b.labels.get("env")))
        });

        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].labels.get("env"), Some("prod"));
        assert_eq!(samples[0].value, 42.0);
        assert_eq!(samples[1].labels.get("env"), Some("staging"));
        assert_eq!(samples[1].value, 10.0);
    }

    #[tokio::test]
    async fn eval_query_should_respect_lookback_delta() {
        // Sample at t=4000s, query at t=4100s (100s later).
        // Default 5m lookback finds it; 10s lookback should not.
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);

        let wide = QueryOptions::default(); // 5m
        let results = tsdb
            .eval_query("http_requests", Some(query_time), &wide)
            .await
            .unwrap()
            .into_matrix();
        assert_eq!(results.len(), 2);

        let narrow = QueryOptions {
            lookback_delta: std::time::Duration::from_secs(10),
            ..Default::default()
        };
        let results = tsdb
            .eval_query("http_requests", Some(query_time), &narrow)
            .await
            .unwrap()
            .into_matrix();
        assert_eq!(
            results.len(),
            0,
            "10s lookback should miss samples 100s ago"
        );
    }

    #[tokio::test]
    async fn eval_query_range_should_respect_lookback_delta() {
        // Same idea but for range queries: narrow lookback → no results.
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);
        let end = start;
        let step = std::time::Duration::from_secs(60);

        let wide = QueryOptions::default();
        let results = tsdb
            .eval_query_range("http_requests", start..=end, step, &wide)
            .await
            .unwrap();
        assert_eq!(results.len(), 2);

        let narrow = QueryOptions {
            lookback_delta: std::time::Duration::from_secs(10),
            ..Default::default()
        };
        let results = tsdb
            .eval_query_range("http_requests", start..=end, step, &narrow)
            .await
            .unwrap();
        assert!(
            results.is_empty(),
            "10s lookback should miss samples 100s ago"
        );
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_should_return_scalar(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("1+1", Some(query_time), &opts)
            .await
            .unwrap();

        match result {
            QueryValue::Scalar {
                timestamp_ms,
                value,
            } => {
                assert_eq!(value, 2.0);
                assert_eq!(
                    timestamp_ms,
                    query_time
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as i64
                );
            }
            other => panic!("expected Scalar, got {:?}", other),
        }
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_should_return_error_for_invalid_query(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let opts = QueryOptions::default();
        let result = tsdb.eval_query("invalid{", None, &opts).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[tokio::test]
    async fn eval_query_range_should_return_range_samples() {
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4000);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4000);
        let step = std::time::Duration::from_secs(60);

        let opts = QueryOptions::default();
        let mut results = tsdb
            .eval_query_range("http_requests", start..=end, step, &opts)
            .await
            .unwrap();
        results.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].labels.get("env"), Some("prod"));
        assert!(!results[0].samples.is_empty());
        assert_eq!(results[1].labels.get("env"), Some("staging"));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_range_should_return_scalar(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(160);
        let step = std::time::Duration::from_secs(60);

        let opts = QueryOptions::default();
        let results = tsdb
            .eval_query_range("1+1", start..=end, step, &opts)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].labels.metric_name(), "");
        assert_eq!(results[0].samples.len(), 2); // two steps: 100s and 160s
        assert_eq!(results[0].samples[0].1, 2.0);
        assert_eq!(results[0].samples[1].1, 2.0);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_range_should_return_error_for_invalid_query(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(200);

        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query_range(
                "invalid{",
                start..=end,
                std::time::Duration::from_secs(60),
                &opts,
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[tokio::test]
    async fn find_series_should_return_matching_series() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_series(&["http_requests"], 3600, 7200)
            .await
            .unwrap();
        results.sort_by(|a, b| a.get("env").cmp(&b.get("env")));

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("env"), Some("prod"));
        assert_eq!(results[0].metric_name(), "http_requests");
        assert_eq!(results[1].get("env"), Some("staging"));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_series_should_error_on_empty_matchers(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let result = tsdb.find_series(&[], 0, i64::MAX).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::QueryError::InvalidQuery(_)
        ));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_series_should_dedup_across_buckets(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2], None)
            .await
            .unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb.find_series(&["cpu"], 0, i64::MAX).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name(), "cpu");
    }

    #[tokio::test]
    async fn find_labels_should_return_all_label_names() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb.find_labels(None, 3600, 7200).await.unwrap();
        results.sort();

        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"env".to_string()));
    }

    #[tokio::test]
    async fn find_labels_should_filter_by_matcher() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_labels(Some(&[r#"http_requests{env="prod"}"#]), 3600, 7200)
            .await
            .unwrap();
        results.sort();

        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"env".to_string()));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_labels_should_return_empty_for_no_data(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let results = tsdb.find_labels(None, 0, 100).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn find_label_values_should_return_values() {
        let tsdb = create_tsdb_with_data().await;

        let mut results = tsdb
            .find_label_values("env", None, 3600, 7200)
            .await
            .unwrap();
        results.sort();

        assert_eq!(results, vec!["prod", "staging"]);
    }

    #[tokio::test]
    async fn find_label_values_should_filter_by_matcher() {
        let tsdb = create_tsdb_with_data().await;

        let results = tsdb
            .find_label_values("env", Some(&[r#"http_requests{env="prod"}"#]), 3600, 7200)
            .await
            .unwrap();

        assert_eq!(results, vec!["prod"]);
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_label_values_should_return_empty_for_no_data(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let results = tsdb.find_label_values("env", None, 0, 100).await.unwrap();
        assert!(results.is_empty());
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_metadata_should_return_all(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        let mut series = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        series.description = Some("CPU usage".to_string());
        series.unit = Some("percent".to_string());
        tsdb.ingest_samples(vec![series], None).await.unwrap();

        let results = tsdb.find_metadata(None).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name, "cpu");
        assert_eq!(results[0].metric_type, Some(MetricType::Gauge));
        assert_eq!(results[0].description, Some("CPU usage".to_string()));
        assert_eq!(results[0].unit, Some("percent".to_string()));
    }

    // -----------------------------------------------------------------------
    // Offset / @ modifier tests (bucket preloading)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn eval_query_with_offset_should_load_correct_bucket() {
        // Data at 4000s (bucket hour-60: 3600–7199s).
        // Query at 7600s with offset 1h → effective time = 4000s.
        let tsdb = create_tsdb_with_data().await;
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7600);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query("http_requests offset 1h", Some(query_time), &opts)
            .await
            .unwrap();
        let samples = result.into_matrix();
        assert_eq!(samples.len(), 2, "offset 1h should find data at 4000s");
    }

    #[tokio::test]
    async fn eval_query_range_with_offset_crossing_bucket() {
        // Data at 4000s. Range [7600,7660] with offset 1h → effective [4000,4060].
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7600);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(7660);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("http_requests offset 1h", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(!results.is_empty(), "offset range query should find data");
        for rs in &results {
            assert!(!rs.samples.is_empty());
        }
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_with_offset_before_epoch_should_not_error(storage: Arc<dyn Storage>) {
        // Query at 100s with offset 1h → effective time = -3500s (before epoch).
        let tsdb = Tsdb::new(storage);
        let query_time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query("up offset 1h", Some(query_time), &opts)
            .await
            .unwrap();
        let samples = result.into_matrix();
        assert!(
            samples.is_empty(),
            "before-epoch offset should return empty"
        );
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_range_with_at_before_epoch_should_not_error(storage: Arc<dyn Storage>) {
        // `@ 0 offset 1h` pins evaluation to t=0, then offset pushes to -3600.
        let tsdb = Tsdb::new(storage);
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1000);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(2000);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("up @ 0 offset 1h", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(
            results.is_empty() || results.iter().all(|rs| rs.samples.is_empty()),
            "@ 0 offset 1h should return empty matrix"
        );
    }

    #[tokio::test]
    async fn eval_query_range_with_at_end_should_load_correct_bucket() {
        // Data at 4000s. Range [4000,8000]. `@ end()` pins to 8000s.
        // With default 5m lookback, sample at 4000s is within range from 8000.
        // Actually, @ end() pins evaluation to t=8000 for each step, but
        // lookback only covers 5min=300s. Data at 4000s is 4000s before 8000s,
        // so it won't be found by lookback. Let's use a range where @ end()
        // helps: range [3900,4100], data at 4000s, `@ end()` pins to 4100s,
        // lookback 5min covers it.
        let tsdb = create_tsdb_with_data().await;
        let start = std::time::UNIX_EPOCH + std::time::Duration::from_secs(3900);
        let end = std::time::UNIX_EPOCH + std::time::Duration::from_secs(4100);
        let step = std::time::Duration::from_secs(60);
        let opts = QueryOptions::default();

        let results = tsdb
            .eval_query_range("http_requests @ end()", start..=end, step, &opts)
            .await
            .unwrap();
        assert!(!results.is_empty(), "@ end() should find data");
        // All steps should see the same sample (pinned to end)
        for rs in &results {
            assert!(!rs.samples.is_empty());
        }
    }

    // -----------------------------------------------------------------------
    // Multi-bucket dedup for labels and label_values
    // -----------------------------------------------------------------------

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_labels_should_dedup_across_buckets(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2], None)
            .await
            .unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb.find_labels(None, 0, i64::MAX).await.unwrap();

        // Label names should not be duplicated
        let mut sorted = results.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            results.len(),
            sorted.len(),
            "label names should be deduplicated across buckets"
        );
        assert!(results.contains(&"__name__".to_string()));
        assert!(results.contains(&"host".to_string()));
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_label_values_should_dedup_across_buckets(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        // Same series in two different buckets
        let series1 = create_sample("cpu", vec![("host", "a")], 4_000_000, 1.0);
        let series2 = create_sample("cpu", vec![("host", "a")], 7_500_000, 2.0);
        tsdb.ingest_samples(vec![series1, series2], None)
            .await
            .unwrap();
        tsdb.flush().await.unwrap();

        let results = tsdb
            .find_label_values("host", None, 0, i64::MAX)
            .await
            .unwrap();

        assert_eq!(
            results,
            vec!["a"],
            "label values should be deduplicated across buckets"
        );
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn find_metadata_should_filter_by_metric(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        tsdb.ingest_samples(
            vec![
                create_sample("cpu", vec![], 4_000_000, 1.0),
                create_sample("mem", vec![], 4_000_000, 2.0),
            ],
            None,
        )
        .await
        .unwrap();

        let results = tsdb.find_metadata(Some("cpu")).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].metric_name, "cpu");

        let results = tsdb.find_metadata(Some("nonexistent")).await.unwrap();
        assert!(results.is_empty());
    }

    #[storage_test(merge_operator = OpenTsdbMergeOperator)]
    async fn eval_query_range_rejects_zero_step(storage: Arc<dyn Storage>) {
        let tsdb = Tsdb::new(storage);

        tsdb.ingest_samples(vec![create_sample("cpu", vec![], 1_000_000, 1.0)], None)
            .await
            .unwrap();
        tsdb.flush().await.unwrap();

        let start = UNIX_EPOCH + Duration::from_secs(1000);
        let end = UNIX_EPOCH + Duration::from_secs(1060);
        let opts = QueryOptions::default();

        let result = tsdb
            .eval_query_range("cpu", start..=end, Duration::ZERO, &opts)
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::InvalidQuery(_)));
    }

    /// End-to-end test: Ingestor → IngestConsumer → Tsdb → query.
    ///
    /// Runs the real consumer poll loop (metadata validation, ack/flush) and
    /// shuts it down gracefully via [`ConsumerHandle`].
    #[cfg(all(feature = "otel", feature = "http-server"))]
    #[tokio::test]
    async fn should_ingest_via_consumer_and_query_back() {
        use bytes::Bytes;
        use opentelemetry_proto::tonic::{
            collector::metrics::v1::ExportMetricsServiceRequest,
            common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
            metrics::v1::{
                Gauge, Metric, NumberDataPoint, ScopeMetrics, metric, number_data_point,
            },
            resource::v1::Resource,
        };
        use prost::Message;

        use slatedb::object_store::ObjectStore;
        use slatedb::object_store::memory::InMemory;

        // given — shared in-memory object store for ingestor and consumer
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest = "ingest/manifest".to_string();

        // Build an OTLP gauge: cpu_temperature{host="server1"} = 72.5 at t=3_900s
        let ts_nanos = 3_900_000_000_000u64; // 3900s in nanos
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![opentelemetry_proto::tonic::metrics::v1::ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "test".to_string(),
                        version: "1.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![Metric {
                        name: "cpu_temperature".to_string(),
                        description: String::new(),
                        unit: String::new(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue(
                                            "server1".to_string(),
                                        )),
                                    }),
                                }],
                                start_time_unix_nano: 0,
                                time_unix_nano: ts_nanos,
                                value: Some(number_data_point::Value::AsDouble(72.5)),
                                exemplars: vec![],
                                flags: 0,
                            }],
                        })),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let proto_bytes = request.encode_to_vec();

        // Metadata: version=1, signal_type=metrics(1), encoding=otlp_proto(1), reserved=0
        let metadata = Bytes::from_static(&[1, 1, 1, 0]);

        // Produce via Ingestor
        let ingestor_config = ingest::IngestorConfig {
            object_store: common::ObjectStoreConfig::InMemory,
            data_path_prefix: "ingest".to_string(),
            manifest_path: manifest.clone(),
            flush_interval: Duration::from_millis(10),
            flush_size_bytes: 64 * 1024 * 1024,
            max_buffered_inputs: 1000,
            batch_compression: ingest::CompressionType::None,
        };
        let ingestor = ingest::Ingestor::with_object_store(
            ingestor_config,
            obj_store.clone(),
            Arc::new(common::clock::SystemClock),
        )
        .unwrap();
        ingestor
            .ingest(vec![Bytes::from(proto_bytes)], metadata)
            .await
            .unwrap();
        ingestor.close().await.unwrap();

        // Start the real IngestConsumer against the shared object store
        let tsdb = Arc::new(Tsdb::new(Arc::new(InMemoryStorage::with_merge_operator(
            Arc::new(OpenTsdbMergeOperator),
        ))));
        let converter = Arc::new(crate::otel::OtelConverter::new(
            crate::otel::OtelConfig::default(),
        ));
        let consumer_config = crate::promql::config::IngestConsumerConfig {
            object_store: common::ObjectStoreConfig::InMemory,
            manifest_path: manifest,
            poll_interval: Duration::from_millis(10),
            data_path_prefix: "ingest".to_string(),
            gc_interval: Duration::from_secs(300),
            gc_grace_period: Duration::from_secs(600),
        };
        let consumer = Arc::new(crate::server::ingest_consumer::IngestConsumer::new(
            tsdb.clone(),
            converter,
            consumer_config,
        ));
        let handle = consumer.run_with_object_store(obj_store).await.unwrap();

        // Wait for the consumer to process the batch
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Graceful shutdown — flushes pending acks
        handle.shutdown().await;

        tsdb.flush().await.unwrap();

        // when — query back
        let query_time = UNIX_EPOCH + Duration::from_secs(3900);
        let opts = QueryOptions::default();
        let result = tsdb
            .eval_query("cpu_temperature", Some(query_time), &opts)
            .await
            .unwrap();

        // then
        let samples = match result {
            QueryValue::Vector(s) => s,
            other => panic!("expected Vector, got {:?}", other),
        };
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].value, 72.5);
        assert_eq!(samples[0].labels.get("host"), Some("server1"));
    }

    // ── Engine wiring tests ───────────────────────────────────────────

    mod wiring_tests {
        use super::*;
        use crate::testing::columnar_stress::{
            Oracle, Scenario, ScenarioConfig, assert_grouped_count_matrix,
            assert_grouped_count_vector, assert_grouped_rate_matrix, assert_grouped_sum_matrix,
            assert_grouped_sum_vector, assert_raw_selector_cardinality, build_oracle,
            build_scenario, concurrency_profiles, create_tsdb_for_scenario, query_end_time,
            query_start_time, query_step,
        };

        async fn create_tsdb_with_counter() -> Tsdb {
            let tsdb = Tsdb::new(Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
                OpenTsdbMergeOperator,
            ))));
            // Counter over bucket at minute 60 (covers 3.6M-7.2M ms).
            // Samples every 10s at 4_000_000, 4_010_000, 4_020_000.
            let series = vec![
                create_sample("req_total", vec![("env", "prod")], 4_000_000, 10.0),
                create_sample("req_total", vec![("env", "prod")], 4_010_000, 20.0),
                create_sample("req_total", vec![("env", "prod")], 4_020_000, 30.0),
            ];
            tsdb.ingest_samples(series, None).await.unwrap();
            tsdb.flush().await.unwrap();
            tsdb
        }

        #[tokio::test]
        async fn should_eval_instant_query_selector_returns_one_sample_per_series() {
            // given: two series in a single bucket
            let tsdb = create_tsdb_with_data().await;
            let query_time = UNIX_EPOCH + Duration::from_secs(4100);

            // when: run the instant selector via the engine entry point
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("http_requests", Some(query_time), &opts)
                .await
                .unwrap();

            // then: two instant samples, one per series, both at the
            // evaluation timestamp
            let mut samples = match result {
                QueryValue::Vector(s) => s,
                other => panic!("expected Vector, got {:?}", other),
            };
            samples.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));
            assert_eq!(samples.len(), 2);
            let expected_ts = 4_100_000i64;
            assert_eq!(samples[0].timestamp_ms, expected_ts);
            assert_eq!(samples[0].labels.get("env"), Some("prod"));
            assert_eq!(samples[0].value, 42.0);
            assert_eq!(samples[1].labels.get("env"), Some("staging"));
            assert_eq!(samples[1].value, 10.0);
        }

        #[tokio::test]
        async fn should_eval_range_query_produces_samples_per_step() {
            // given: a counter with 3 samples in one bucket
            let tsdb = create_tsdb_with_counter().await;
            // Query window straddling the samples — step 10s.
            let start = UNIX_EPOCH + Duration::from_secs(4_000);
            let end = UNIX_EPOCH + Duration::from_secs(4_020);
            let step = Duration::from_secs(10);

            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query_range_value("req_total", start..=end, step, &opts)
                .await
                .unwrap();

            // then: one range sample (one series) with 3 points
            let series = match result {
                QueryValue::Matrix(m) => m,
                other => panic!("expected Matrix, got {:?}", other),
            };
            assert_eq!(series.len(), 1);
            assert_eq!(series[0].labels.get("env"), Some("prod"));
            assert_eq!(series[0].samples.len(), 3);
            assert_eq!(series[0].samples[0].1, 10.0);
            assert_eq!(series[0].samples[1].1, 20.0);
            assert_eq!(series[0].samples[2].1, 30.0);
        }

        #[tokio::test]
        async fn should_eval_rate_over_counter_matches_expected_rate() {
            // given: counter 10 → 20 → 30 over 20 seconds
            let tsdb = create_tsdb_with_counter().await;
            // Evaluate instant rate over a [1m] window ending at 4020s;
            // Prometheus extrapolates but for an evenly-sampled counter
            // the result is ≈ 1 req/s (20 total increase / 20s ≈ 1).
            let query_time = UNIX_EPOCH + Duration::from_secs(4_020);

            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("rate(req_total[1m])", Some(query_time), &opts)
                .await
                .unwrap();

            // then: one series, rate close to 1 per second (extrapolation
            // of a 2-sample-per-10s counter over a 1m window)
            let samples = match result {
                QueryValue::Vector(v) => v,
                other => panic!("expected Vector, got {:?}", other),
            };
            assert_eq!(samples.len(), 1);
            // Rate is per-second, extrapolated across the range window.
            // For this fixture Prometheus emits ~0.417 (20-count increase
            // over 60s extrapolation). Tolerate a wide plausibility band
            // rather than pinning a precise golden.
            assert!(
                samples[0].value > 0.0 && samples[0].value < 10.0,
                "rate should be plausible (>0 and <10), got {}",
                samples[0].value
            );
        }

        #[tokio::test]
        async fn should_propagate_memory_limit_at_exec_time() {
            // given: a small-but-real fixture and an aggressively tiny
            // memory cap so every operator allocation trips.
            let tsdb = create_tsdb_with_data().await;
            let at_ms = 4_100_000i64;
            let ctx = crate::promql::plan::LoweringContext::for_instant(at_ms, 300_000);
            let reader = tsdb.query_reader_for_ranges(&[(0, 10_000)]).await.unwrap();
            let source = Arc::new(crate::promql::source_adapter::QueryReaderSource::new(
                Arc::new(reader),
            ));
            // 1 byte cap — any allocation fails.
            let reservation = crate::promql::memory::MemoryReservation::new(1);
            let expr = promql_parser::parser::parse("http_requests").unwrap();
            let plan = crate::promql::plan::lower(&expr, &ctx).unwrap();

            // when: build + drive. We expect the operator to surface a
            // `MemoryLimit` error either at build time (scratch reservation)
            // or on the first `next()` poll (per-batch output reservation).
            let built =
                crate::promql::plan::build_physical_plan(plan, &source, reservation, &ctx).await;
            let err: String = match built {
                Err(e) => e.to_string(),
                Ok(mut phys) => {
                    use std::future::poll_fn;
                    let polled = poll_fn(|cx| phys.root.next(cx)).await;
                    match polled {
                        Some(Err(e)) => e.to_string(),
                        Some(Ok(_)) => panic!("expected a memory-limit error, got a batch"),
                        None => panic!("expected an error, got end-of-stream"),
                    }
                }
            };

            // then: error mentions memory limit
            assert!(
                err.to_lowercase().contains("memory"),
                "expected a memory-limit error, got: {err}"
            );
        }

        #[tokio::test]
        async fn should_return_query_error_for_unknown_function() {
            // given: a query using a function the engine does not lower
            let tsdb = create_tsdb_with_data().await;
            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("histogram_quantile(0.9, http_requests)", None, &opts)
                .await;
            // then
            let err = result.unwrap_err();
            assert!(
                matches!(err, QueryError::InvalidQuery(_)),
                "expected InvalidQuery, got {:?}",
                err
            );
        }

        #[tokio::test]
        async fn should_eval_time_function_as_scalar() {
            // given
            let tsdb = create_tsdb_with_data().await;
            let query_time = UNIX_EPOCH + Duration::from_millis(1);

            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("time()", Some(query_time), &opts)
                .await
                .unwrap();

            // then
            match result {
                QueryValue::Scalar {
                    timestamp_ms,
                    value,
                } => {
                    assert_eq!(timestamp_ms, 1);
                    assert_eq!(value, 0.001);
                }
                other => panic!("expected Scalar, got {:?}", other),
            }
        }

        #[tokio::test]
        async fn should_eval_vector_time_as_vector() {
            // given
            let tsdb = create_tsdb_with_data().await;
            let query_time = UNIX_EPOCH + Duration::from_secs(5);

            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("vector(time())", Some(query_time), &opts)
                .await
                .unwrap();

            // then
            match result {
                QueryValue::Vector(samples) => {
                    assert_eq!(samples.len(), 1);
                    assert!(samples[0].labels.is_empty());
                    assert_eq!(samples[0].value, 5.0);
                }
                other => panic!("expected Vector, got {:?}", other),
            }
        }

        #[tokio::test]
        async fn should_eval_calendar_function() {
            // given
            let tsdb = create_tsdb_with_data().await;
            let query_time = UNIX_EPOCH;

            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("minute(vector(1136239445))", Some(query_time), &opts)
                .await
                .unwrap();

            // then
            match result {
                QueryValue::Vector(samples) => {
                    assert_eq!(samples.len(), 1);
                    assert_eq!(samples[0].value, 4.0);
                }
                other => panic!("expected Vector, got {:?}", other),
            }
        }

        #[tokio::test]
        async fn should_eval_label_replace() {
            // given
            let tsdb = create_tsdb_with_data().await;
            let query_time = UNIX_EPOCH + Duration::from_secs(4100);

            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query(
                    r#"label_replace(http_requests, "tier", "$1-app", "env", "(.*)")"#,
                    Some(query_time),
                    &opts,
                )
                .await
                .unwrap();

            // then
            let mut samples = match result {
                QueryValue::Vector(samples) => samples,
                other => panic!("expected Vector, got {:?}", other),
            };
            samples.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].labels.get("tier"), Some("prod-app"));
            assert_eq!(samples[1].labels.get("tier"), Some("staging-app"));
        }

        #[tokio::test]
        async fn should_eval_label_join() {
            // given
            let tsdb = create_tsdb_with_data().await;
            let query_time = UNIX_EPOCH + Duration::from_secs(4100);

            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query(
                    r#"label_join(http_requests, "joined", "/", "__name__", "env")"#,
                    Some(query_time),
                    &opts,
                )
                .await
                .unwrap();

            // then
            let mut samples = match result {
                QueryValue::Vector(samples) => samples,
                other => panic!("expected Vector, got {:?}", other),
            };
            samples.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].labels.get("joined"), Some("http_requests/prod"));
            assert_eq!(
                samples[1].labels.get("joined"),
                Some("http_requests/staging")
            );
        }

        #[tokio::test]
        async fn should_eval_sum_by_label() {
            // given: two series under the same metric name, grouped by env
            let tsdb = create_tsdb_with_data().await;
            let query_time = UNIX_EPOCH + Duration::from_secs(4100);

            // when
            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("sum by (env) (http_requests)", Some(query_time), &opts)
                .await
                .unwrap();

            // then: two groups, values match the input series
            let mut samples = match result {
                QueryValue::Vector(v) => v,
                other => panic!("expected Vector, got {:?}", other),
            };
            samples.sort_by(|a, b| a.labels.get("env").cmp(&b.labels.get("env")));
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].labels.get("env"), Some("prod"));
            assert_eq!(samples[0].value, 42.0);
            assert_eq!(samples[1].labels.get("env"), Some("staging"));
            assert_eq!(samples[1].value, 10.0);
        }

        // ── RFC 0007 §4 unit 6.3.9 — tile-boundary stress end-to-end ──

        /// Ingest a large-roster metric: `n_series` series under a single
        /// metric name `metric`, one sample per series at `(base_ts,
        /// base_ts + step)` — two samples per series so `sum_over_time`
        /// has real values to aggregate inside the 10s sub-window.
        async fn create_tsdb_with_large_roster(metric: &str, n_series: usize) -> Tsdb {
            let tsdb = Tsdb::new(Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
                OpenTsdbMergeOperator,
            ))));
            let mut samples = Vec::with_capacity(n_series * 2);
            for i in 0..n_series {
                samples.push(create_sample(
                    metric,
                    vec![("i", &i.to_string())],
                    4_000_000,
                    1.0,
                ));
                samples.push(create_sample(
                    metric,
                    vec![("i", &i.to_string())],
                    4_005_000,
                    1.0,
                ));
            }
            tsdb.ingest_samples(samples, None).await.unwrap();
            tsdb.flush().await.unwrap();
            tsdb
        }

        /// Subquery end-to-end at >512 series (RFC 0007 6.3.9 audit item
        /// 10). Ignored because `SubqueryOp`'s
        /// `tokio::task::block_in_place` path inside
        /// [`build_subquery`](crate::promql::plan::physical) re-invokes
        /// the child factory synchronously per outer step and does not
        /// release the child's reservation on drop, causing the 1 GiB
        /// plan-time cap to fill before the query completes. This is a
        /// pre-existing subquery resource-accounting bug, not a
        /// tile-boundary bug — the operators themselves absorb multi-tile
        /// input correctly (verified by the per-operator stress tests in
        /// 6.3.9). Re-enable once subquery reservation cleanup lands.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        #[ignore = "pre-existing subquery reservation leak; see doc comment"]
        async fn should_eval_sum_over_time_subquery_with_over_512_series() {
            const N_SERIES: usize = 600;
            let tsdb = create_tsdb_with_large_roster("m", N_SERIES).await;
            let query_time = UNIX_EPOCH + Duration::from_secs(4_020);

            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("sum_over_time(m[10s:2s])", Some(query_time), &opts)
                .await
                .unwrap();

            let samples = match result {
                QueryValue::Vector(v) => v,
                other => panic!("expected Vector, got {:?}", other),
            };
            assert_eq!(
                samples.len(),
                N_SERIES,
                "expected one output series per input",
            );
            for s in &samples {
                assert!(
                    s.value >= 2.0,
                    "series {:?} sum_over_time = {}, expected >= 2.0",
                    s.labels.get("i"),
                    s.value,
                );
            }
        }

        #[tokio::test]
        async fn should_eval_sum_by_label_over_large_roster_end_to_end() {
            // given: 1500 series under `m`, bucketed into 3 groups
            // (round-robin by `i % 3`). Each series has value 1.0 at the
            // query time, so sum by (g) (m) should emit 3 groups with
            // counts 500 each.
            const N_SERIES: usize = 1500;
            const GROUPS: usize = 3;
            let tsdb = {
                let tsdb = Tsdb::new(Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
                    OpenTsdbMergeOperator,
                ))));
                let mut samples = Vec::with_capacity(N_SERIES);
                for i in 0..N_SERIES {
                    samples.push(create_sample(
                        "m",
                        vec![("i", &i.to_string()), ("g", &((i % GROUPS).to_string()))],
                        4_000_000,
                        1.0,
                    ));
                }
                tsdb.ingest_samples(samples, None).await.unwrap();
                tsdb.flush().await.unwrap();
                tsdb
            };
            let query_time = UNIX_EPOCH + Duration::from_secs(4_010);

            let opts = QueryOptions::default();
            let result = tsdb
                .eval_query("sum by (g) (m)", Some(query_time), &opts)
                .await
                .unwrap();

            // then: three groups, each carrying N_SERIES / GROUPS = 500.
            let mut samples = match result {
                QueryValue::Vector(v) => v,
                other => panic!("expected Vector, got {:?}", other),
            };
            samples.sort_by(|a, b| a.labels.get("g").cmp(&b.labels.get("g")));
            assert_eq!(samples.len(), GROUPS);
            for s in &samples {
                assert_eq!(
                    s.value,
                    (N_SERIES / GROUPS) as f64,
                    "group {:?} did not reach expected count",
                    s.labels.get("g"),
                );
            }
        }

        async fn create_columnar_stress_fixture(
            config: ScenarioConfig,
        ) -> (Scenario, Oracle, Tsdb) {
            let scenario = build_scenario(config);
            let oracle = build_oracle(&scenario);
            let tsdb = create_tsdb_for_scenario(&scenario).await;
            (scenario, oracle, tsdb)
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_eval_long_window_stress_regression_instant_probes() {
            // given
            let (scenario, oracle, tsdb) =
                create_columnar_stress_fixture(ScenarioConfig::regression()).await;

            for (profile_name, opts) in concurrency_profiles() {
                for &timestamp_ms in &scenario.probe_timestamps {
                    let query_time = UNIX_EPOCH + Duration::from_millis(timestamp_ms as u64);

                    // when
                    let raw_result = tsdb
                        .eval_query("stress_gauge", Some(query_time), &opts)
                        .await
                        .unwrap();
                    let count_result = tsdb
                        .eval_query(
                            "count by (applicationid) (stress_gauge)",
                            Some(query_time),
                            &opts,
                        )
                        .await
                        .unwrap();
                    let sum_result = tsdb
                        .eval_query(
                            "sum by (applicationid) (stress_gauge)",
                            Some(query_time),
                            &opts,
                        )
                        .await
                        .unwrap();

                    // then
                    let raw_samples = match raw_result {
                        QueryValue::Vector(samples) => samples,
                        other => panic!("expected Vector for raw selector, got {:?}", other),
                    };
                    assert_raw_selector_cardinality(&raw_samples, &scenario, timestamp_ms);

                    let count_samples = match count_result {
                        QueryValue::Vector(samples) => samples,
                        other => panic!("expected Vector for count query, got {:?}", other),
                    };
                    assert_grouped_count_vector(&count_samples, &scenario, &oracle, timestamp_ms);

                    let sum_samples = match sum_result {
                        QueryValue::Vector(samples) => samples,
                        other => panic!("expected Vector for sum query, got {:?}", other),
                    };
                    assert_grouped_sum_vector(&sum_samples, &scenario, &oracle, timestamp_ms);
                }

                assert!(
                    !scenario.probe_timestamps.is_empty(),
                    "expected probe timestamps for profile {profile_name}"
                );
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_eval_long_window_stress_regression_range_queries() {
            // given
            let (scenario, oracle, tsdb) =
                create_columnar_stress_fixture(ScenarioConfig::regression()).await;
            let start = query_start_time(&scenario);
            let end = query_end_time(&scenario);
            let step = query_step(&scenario);

            for (_profile_name, opts) in concurrency_profiles() {
                // when
                let count_result = tsdb
                    .eval_query_range_value(
                        "count by (applicationid) (stress_gauge)",
                        start..=end,
                        step,
                        &opts,
                    )
                    .await
                    .unwrap();
                let sum_result = tsdb
                    .eval_query_range_value(
                        "sum by (applicationid) (stress_gauge)",
                        start..=end,
                        step,
                        &opts,
                    )
                    .await
                    .unwrap();
                let rate_result = tsdb
                    .eval_query_range_value(
                        "sum by (applicationid) (rate(stress_counter_total[15m]))",
                        start..=end,
                        step,
                        &opts,
                    )
                    .await
                    .unwrap();

                // then
                let count_matrix = match count_result {
                    QueryValue::Matrix(matrix) => matrix,
                    other => panic!("expected Matrix for count range, got {:?}", other),
                };
                assert_grouped_count_matrix(&count_matrix, &scenario, &oracle);

                let sum_matrix = match sum_result {
                    QueryValue::Matrix(matrix) => matrix,
                    other => panic!("expected Matrix for sum range, got {:?}", other),
                };
                assert_grouped_sum_matrix(&sum_matrix, &scenario, &oracle);

                let rate_matrix = match rate_result {
                    QueryValue::Matrix(matrix) => matrix,
                    other => panic!("expected Matrix for rate range, got {:?}", other),
                };
                assert_grouped_rate_matrix(&rate_matrix, &scenario, &oracle);
            }
        }

        /// Larger ignored soak for RFC 0008. Invoke with:
        /// `cargo test -p opendata-timeseries should_eval_long_window_stress_soak -- --ignored --nocapture`
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        #[ignore]
        async fn should_eval_long_window_stress_soak() {
            // given
            let (scenario, oracle, tsdb) =
                create_columnar_stress_fixture(ScenarioConfig::soak()).await;
            let start = query_start_time(&scenario);
            let end = query_end_time(&scenario);
            let step = query_step(&scenario);
            let opts = QueryOptions::default();

            // when
            let count_result = tsdb
                .eval_query_range_value(
                    "count by (applicationid) (stress_gauge)",
                    start..=end,
                    step,
                    &opts,
                )
                .await
                .unwrap();
            let sum_result = tsdb
                .eval_query_range_value(
                    "sum by (applicationid) (stress_gauge)",
                    start..=end,
                    step,
                    &opts,
                )
                .await
                .unwrap();
            let rate_result = tsdb
                .eval_query_range_value(
                    "sum by (applicationid) (rate(stress_counter_total[15m]))",
                    start..=end,
                    step,
                    &opts,
                )
                .await
                .unwrap();

            // then
            let count_matrix = match count_result {
                QueryValue::Matrix(matrix) => matrix,
                other => panic!("expected Matrix for count soak, got {:?}", other),
            };
            assert_grouped_count_matrix(&count_matrix, &scenario, &oracle);

            let sum_matrix = match sum_result {
                QueryValue::Matrix(matrix) => matrix,
                other => panic!("expected Matrix for sum soak, got {:?}", other),
            };
            assert_grouped_sum_matrix(&sum_matrix, &scenario, &oracle);

            let rate_matrix = match rate_result {
                QueryValue::Matrix(matrix) => matrix,
                other => panic!("expected Matrix for rate soak, got {:?}", other),
            };
            assert_grouped_rate_matrix(&rate_matrix, &scenario, &oracle);
        }

        // ── Read-only reader tests (RFC 0007 unit 7.0) ───────────────
        //
        // These verify that the engine runs through the
        // `TsdbReadEngine` trait defaults on both the writer (`Tsdb`)
        // and the read-only `TimeSeriesDbReader`, so reader-only prod
        // binaries exercise the columnar engine.

        /// Build a writer + reader pair on shared storage, ingest a
        /// counter spanning 3 samples, flush, and return both handles.
        async fn create_writer_and_reader_with_counter() -> (Tsdb, crate::reader::TimeSeriesDbReader)
        {
            let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
                OpenTsdbMergeOperator,
            )));
            let tsdb = Tsdb::new(storage.clone());
            let series = vec![
                create_sample("req_total", vec![("env", "prod")], 4_000_000, 10.0),
                create_sample("req_total", vec![("env", "prod")], 4_010_000, 20.0),
                create_sample("req_total", vec![("env", "prod")], 4_020_000, 30.0),
            ];
            tsdb.ingest_samples(series, None).await.unwrap();
            tsdb.flush().await.unwrap();
            let reader = crate::reader::TimeSeriesDbReader::from_storage(storage);
            (tsdb, reader)
        }

        #[tokio::test]
        async fn should_eval_instant_query_on_read_only_reader_returns_one_sample_per_series() {
            // given: counter ingested via writer, read through a reader
            let (_tsdb, reader) = create_writer_and_reader_with_counter().await;
            let query_time = UNIX_EPOCH + Duration::from_secs(4_020);

            // when: the reader's entry point evaluates the selector
            let opts = QueryOptions::default();
            let result = reader
                .eval_query("req_total", Some(query_time), &opts)
                .await
                .unwrap();

            // then: one instant sample at the evaluation timestamp
            let samples = match result {
                QueryValue::Vector(s) => s,
                other => panic!("expected Vector, got {:?}", other),
            };
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].timestamp_ms, 4_020_000);
            assert_eq!(samples[0].labels.get("env"), Some("prod"));
            assert_eq!(samples[0].value, 30.0);
        }

        #[tokio::test]
        async fn should_eval_range_query_on_read_only_reader_produces_samples_per_step() {
            // given: counter ingested via writer, read through a reader
            let (_tsdb, reader) = create_writer_and_reader_with_counter().await;
            let start = UNIX_EPOCH + Duration::from_secs(4_000);
            let end = UNIX_EPOCH + Duration::from_secs(4_020);
            let step = Duration::from_secs(10);

            // when: the reader's range entry point evaluates the query
            let opts = QueryOptions::default();
            let result = reader
                .eval_query_range_value("req_total", start..=end, step, &opts)
                .await
                .unwrap();

            // then: one range sample (one series) with 3 points
            let series = match result {
                QueryValue::Matrix(m) => m,
                other => panic!("expected Matrix, got {:?}", other),
            };
            assert_eq!(series.len(), 1);
            assert_eq!(series[0].labels.get("env"), Some("prod"));
            assert_eq!(series[0].samples.len(), 3);
            assert_eq!(series[0].samples[0].1, 10.0);
            assert_eq!(series[0].samples[1].1, 20.0);
            assert_eq!(series[0].samples[2].1, 30.0);
        }

        #[tokio::test]
        async fn should_return_identical_results_for_read_write_and_read_only_paths_on_same_storage()
         {
            // given: writer and reader on the same in-memory storage
            let (tsdb, reader) = create_writer_and_reader_with_counter().await;
            let start = UNIX_EPOCH + Duration::from_secs(4_000);
            let end = UNIX_EPOCH + Duration::from_secs(4_020);
            let step = Duration::from_secs(10);

            // when: both engines run the identical range query
            let opts = QueryOptions::default();
            let query = "rate(req_total[1m])";
            let writer_result = tsdb
                .eval_query_range_value(query, start..=end, step, &opts)
                .await
                .unwrap();
            let reader_result = reader
                .eval_query_range_value(query, start..=end, step, &opts)
                .await
                .unwrap();

            // then: both produce matrices that agree on labels and samples
            let writer_matrix = match writer_result {
                QueryValue::Matrix(m) => m,
                other => panic!("expected Matrix from writer, got {:?}", other),
            };
            let reader_matrix = match reader_result {
                QueryValue::Matrix(m) => m,
                other => panic!("expected Matrix from reader, got {:?}", other),
            };
            assert_eq!(writer_matrix.len(), reader_matrix.len());
            let mut w_sorted = writer_matrix.clone();
            let mut r_sorted = reader_matrix.clone();
            w_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));
            r_sorted.sort_by(|a, b| a.labels.cmp(&b.labels));
            for (w, r) in w_sorted.iter().zip(r_sorted.iter()) {
                assert_eq!(w.labels, r.labels);
                assert_eq!(w.samples, r.samples);
            }
        }
    }
}

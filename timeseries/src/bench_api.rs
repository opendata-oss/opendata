use std::future::poll_fn;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use promql_parser::parser::Expr;

use crate::model::{Label, MetricType, Sample, TimeBucket};
use crate::promql;
use crate::query::test_utils::MockMultiBucketQueryReaderBuilder;

// ---------------------------------------------------------------------------
// Warm range-query benchmark harness
// ---------------------------------------------------------------------------

/// Benchmark harness for warm outer range queries.
///
/// Constructs a multi-bucket synthetic dataset and repeatedly evaluates a
/// range query through the columnar pipeline. The query expression is
/// parsed once at construction time so the measured loop only times
/// planner + execution cost.
pub struct WarmRangeQueryHarness {
    reader: Arc<crate::query::test_utils::MockQueryReader>,
    /// Pre-parsed expression, cloned into each run.
    expr: Expr,
    start: SystemTime,
    end: SystemTime,
    step: Duration,
    lookback_delta: Duration,
}

/// Number of groups for the aggregation benchmark. Using a small fixed
/// number produces a realistic grouped workload rather than a no-op
/// aggregation over per-series-unique values.
const AGG_GROUP_COUNT: usize = 10;

impl WarmRangeQueryHarness {
    /// Build a harness for a plain vector selector range query.
    pub fn vector_selector(
        num_series: usize,
        num_labels: usize,
        range_secs: u64,
        step_secs: u64,
        lookback_delta_ms: i64,
    ) -> Self {
        let reader = build_multi_bucket_dataset(
            num_series,
            num_labels,
            range_secs,
            step_secs,
            |series_idx, label_idx| format!("value_{series_idx}_{label_idx}"),
        );
        let expr =
            promql_parser::parser::parse("bench_metric").expect("failed to parse bench query");

        Self {
            reader: Arc::new(reader),
            expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(range_secs),
            step: Duration::from_secs(step_secs),
            lookback_delta: Duration::from_millis(lookback_delta_ms as u64),
        }
    }

    /// Build a harness for a `sum by (group) (metric)` aggregation range query.
    pub fn aggregation(
        num_series: usize,
        num_labels: usize,
        range_secs: u64,
        step_secs: u64,
        lookback_delta_ms: i64,
    ) -> Self {
        let reader = build_multi_bucket_dataset(
            num_series,
            num_labels,
            range_secs,
            step_secs,
            |series_idx, label_idx| {
                if label_idx == 0 {
                    format!("group_{}", series_idx % AGG_GROUP_COUNT)
                } else {
                    format!("value_{series_idx}_{label_idx}")
                }
            },
        );
        let expr = promql_parser::parser::parse("sum by (label_0) (bench_metric)")
            .expect("failed to parse bench query");

        Self {
            reader: Arc::new(reader),
            expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(range_secs),
            step: Duration::from_secs(step_secs),
            lookback_delta: Duration::from_millis(lookback_delta_ms as u64),
        }
    }

    /// Run the range query once through the columnar pipeline: lower →
    /// optimize → build physical plan → poll root to completion.
    pub async fn run_once(&self) -> Result<usize, String> {
        let start_ms = self
            .start
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis() as i64;
        let end_ms = self
            .end
            .duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis() as i64;
        let step_ms = self.step.as_millis() as i64;
        let lookback_ms = self.lookback_delta.as_millis() as i64;

        let ctx = promql::plan::LoweringContext::new(start_ms, end_ms, step_ms, lookback_ms);
        let logical = promql::plan::lower(&self.expr, &ctx).map_err(|e| e.to_string())?;
        let logical = promql::plan::optimize(logical);

        let source = Arc::new(promql::source_adapter::QueryReaderSource::new(
            self.reader.clone(),
        ));
        // 1 GiB reservation — mirrors the production `DEFAULT_MEMORY_CAP_BYTES`.
        let reservation = promql::memory::MemoryReservation::new(1024 * 1024 * 1024);

        let mut plan = promql::plan::build_physical_plan(logical, &source, reservation, &ctx)
            .await
            .map_err(|e| e.to_string())?;

        let mut cell_count = 0usize;
        loop {
            match poll_fn(|cx| plan.root.next(cx)).await {
                Some(Ok(batch)) => {
                    cell_count = cell_count.saturating_add(batch.values.len());
                }
                Some(Err(e)) => return Err(e.to_string()),
                None => break,
            }
        }
        Ok(cell_count)
    }

    /// Run the range query `iterations` times, black-boxing each result.
    pub async fn run_iterations(&self, iterations: usize) -> Result<(), String> {
        for _ in 0..iterations {
            let result = self.run_once().await?;
            black_box(result);
        }
        Ok(())
    }
}

/// Build a multi-bucket mock dataset spanning `range_secs`.
///
/// Data is distributed across 1-hour buckets so that range queries must
/// resolve metadata and load samples from multiple buckets.
fn build_multi_bucket_dataset(
    num_series: usize,
    num_labels: usize,
    range_secs: u64,
    step_secs: u64,
    label_value_fn: impl Fn(usize, usize) -> String,
) -> crate::query::test_utils::MockQueryReader {
    let mut builder = MockMultiBucketQueryReaderBuilder::new();
    let num_steps = (range_secs / step_secs) as usize;

    for series_idx in 0..num_series {
        let mut labels = vec![Label::metric_name("bench_metric")];
        for label_idx in 0..num_labels {
            labels.push(Label::new(
                format!("label_{label_idx}"),
                label_value_fn(series_idx, label_idx),
            ));
        }
        for step_idx in 0..=num_steps {
            let ts_ms = (step_idx as i64) * (step_secs as i64) * 1000;
            let ts_secs = ts_ms / 1000;
            let bucket_start_mins = (ts_secs / 3600) * 60; // hour-aligned
            let bucket = TimeBucket::hour(bucket_start_mins as u32);
            builder.add_sample(
                bucket,
                labels.clone(),
                MetricType::Gauge,
                Sample {
                    timestamp_ms: ts_ms,
                    value: step_idx as f64,
                },
            );
        }
    }
    builder.build()
}

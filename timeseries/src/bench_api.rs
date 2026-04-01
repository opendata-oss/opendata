use std::hint::black_box;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::model::{Label, MetricType, Sample, TimeBucket};
use crate::promql::evaluator::Evaluator;
use crate::query::test_utils::{MockMultiBucketQueryReaderBuilder, MockQueryReaderBuilder};
use promql_parser::parser::{EvalStmt, Expr, SubqueryExpr, VectorSelector};

// ---------------------------------------------------------------------------
// Warm range-query benchmark harness
// ---------------------------------------------------------------------------

/// Benchmark harness for warm outer range queries.
///
/// Constructs a multi-bucket synthetic dataset and repeatedly evaluates a
/// range query (stepping from `start` to `end` with a given step) that
/// reuses the same Evaluator across all steps — matching the real
/// `evaluate_range` path in `tsdb.rs`.
///
/// The query expression is parsed once at construction time so the measured
/// loop only times evaluator execution.
pub struct WarmRangeQueryHarness {
    reader: crate::query::test_utils::MockQueryReader,
    /// Pre-parsed expression, cloned into each `EvalStmt`.
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
    ///
    /// Creates `num_series` series across multiple 1-hour buckets,
    /// each with `num_labels` extra labels, producing one sample every
    /// `step` from `start` to `end`.
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
            reader,
            expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(range_secs),
            step: Duration::from_secs(step_secs),
            lookback_delta: Duration::from_millis(lookback_delta_ms as u64),
        }
    }

    /// Build a harness for a `sum by (group) (metric)` aggregation range query.
    ///
    /// Uses a low-cardinality `group` label so the aggregation actually
    /// merges series, producing a realistic grouped workload.
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
                    // Low-cardinality grouping label
                    format!("group_{}", series_idx % AGG_GROUP_COUNT)
                } else {
                    format!("value_{series_idx}_{label_idx}")
                }
            },
        );
        let expr = promql_parser::parser::parse("sum by (label_0) (bench_metric)")
            .expect("failed to parse bench query");

        Self {
            reader,
            expr,
            start: UNIX_EPOCH,
            end: UNIX_EPOCH + Duration::from_secs(range_secs),
            step: Duration::from_secs(step_secs),
            lookback_delta: Duration::from_millis(lookback_delta_ms as u64),
        }
    }

    /// Run the range query once (all steps), returning the result count.
    pub async fn run_once(&self) -> Result<usize, String> {
        use crate::promql::pipeline::PipelineConcurrency;

        let stmt = EvalStmt {
            expr: self.expr.clone(),
            start: self.start,
            end: self.end,
            interval: self.step,
            lookback_delta: self.lookback_delta,
        };

        let concurrency = PipelineConcurrency::default();
        let result = crate::tsdb::evaluate_range(&self.reader, stmt, concurrency)
            .await
            .map_err(|e| e.to_string())?;
        Ok(result.len())
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
            // Determine which hour bucket this sample belongs to
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

/// Reusable benchmark harness for subquery label cache evaluation.
pub struct SubqueryLabelCacheHarness {
    reader: crate::query::test_utils::MockQueryReader,
    stmt: EvalStmt,
}

impl SubqueryLabelCacheHarness {
    pub fn new(
        num_series: usize,
        num_labels: usize,
        num_steps: usize,
        lookback_delta_ms: i64,
    ) -> Self {
        let bucket = TimeBucket::hour(0);
        let mut builder = MockQueryReaderBuilder::new(bucket);

        for series_idx in 0..num_series {
            let mut labels = vec![Label::metric_name("metric")];

            for label_idx in 0..num_labels {
                labels.push(Label {
                    name: format!("label_{label_idx}"),
                    value: format!("value_{series_idx}"),
                });
            }

            for step_idx in 0..num_steps {
                builder.add_sample(
                    labels.clone(),
                    MetricType::Gauge,
                    Sample {
                        timestamp_ms: step_idx as i64 * 1000,
                        value: step_idx as f64,
                    },
                );
            }
        }

        let subquery = SubqueryExpr {
            expr: Box::new(Expr::VectorSelector(VectorSelector {
                name: Some("metric".to_string()),
                matchers: promql_parser::label::Matchers::empty(),
                offset: None,
                at: None,
            })),
            range: Duration::from_secs(num_steps as u64),
            step: Some(Duration::from_secs(1)),
            offset: None,
            at: None,
        };

        let eval_time: SystemTime = UNIX_EPOCH + Duration::from_secs(num_steps as u64);
        let stmt = EvalStmt {
            expr: Expr::Subquery(subquery),
            start: eval_time,
            end: eval_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_millis(lookback_delta_ms as u64),
        };

        Self {
            reader: builder.build(),
            stmt,
        }
    }

    pub async fn run_iterations(&self, iterations: usize) -> Result<(), String> {
        let mut evaluator = Evaluator::new(&self.reader);

        for _ in 0..iterations {
            let result = evaluator
                .evaluate(self.stmt.clone())
                .await
                .map_err(|e| e.to_string())?;
            black_box(result);
        }

        Ok(())
    }
}

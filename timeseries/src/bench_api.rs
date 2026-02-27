use std::hint::black_box;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::model::{Label, MetricType, Sample, TimeBucket};
use crate::promql::evaluator::Evaluator;
use crate::query::test_utils::MockQueryReaderBuilder;
use promql_parser::parser::{EvalStmt, Expr, SubqueryExpr, VectorSelector};

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

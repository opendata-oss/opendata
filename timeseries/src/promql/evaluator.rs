use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use promql_parser::parser::token::*;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, EvalStmt, Expr, LabelModifier, MatrixSelector,
    VectorMatchCardinality, VectorSelector,
};

use crate::index::ForwardIndexLookup;
use crate::model::Label;
use crate::model::SeriesFingerprint;
use crate::promql::functions::FunctionRegistry;
use crate::query::QueryReader;

#[derive(Debug)]
pub enum EvaluationError {
    StorageError(String),
    InternalError(String),
}

impl Display for EvaluationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EvaluationError::StorageError(err) => write!(f, "PromQL evaluation error: {err}"),
            EvaluationError::InternalError(err) => write!(f, "PromQL internal error: {err}"),
        }
    }
}

impl std::error::Error for EvaluationError {}

impl From<crate::error::Error> for EvaluationError {
    fn from(err: crate::error::Error) -> Self {
        EvaluationError::StorageError(err.to_string())
    }
}

pub(crate) type EvalResult<T> = std::result::Result<T, EvaluationError>;

// ToDo(cadonna): Add histogram samples
#[derive(Debug, Clone)]
pub struct EvalSample {
    pub(crate) timestamp_ms: i64,
    pub(crate) value: f64,
    pub(crate) labels: HashMap<String, String>,
}

pub(crate) struct Evaluator<'a, R: QueryReader> {
    reader: &'a R,
}

enum ExprResult {
    Scalar(f64),
    InstantVector(Vec<EvalSample>),
}

impl<'a, R: QueryReader> Evaluator<'a, R> {
    pub(crate) fn new(reader: &'a R) -> Self {
        Self { reader }
    }

    pub(crate) async fn evaluate(&self, stmt: EvalStmt) -> EvalResult<Vec<EvalSample>> {
        let result = self
            .evaluate_expr(
                &stmt.expr,
                stmt.start,
                stmt.end,
                stmt.interval,
                stmt.lookback_delta,
            )
            .await?;
        match result {
            ExprResult::InstantVector(vector) => Ok(vector),
            ExprResult::Scalar(_) => Err(EvaluationError::InternalError(
                "unsupported result type: scalar".to_string(),
            )),
        }
    }

    // this call recurses to evaluate sub-expressions, so it needs to return a boxed future
    // so that the return type is sized (so can be stack-allocated)
    fn evaluate_expr<'b>(
        &'b self,
        expr: &'b Expr,
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        lookback_delta: Duration,
    ) -> Pin<Box<dyn Future<Output = EvalResult<ExprResult>> + Send + 'b>> {
        match expr {
            Expr::Aggregate(aggregate) => {
                let fut = self.evaluate_aggregate(aggregate, start, end, interval, lookback_delta);
                Box::pin(fut)
            }
            Expr::Unary(_u) => {
                todo!()
            }
            Expr::Binary(b) => {
                let fut = self.evaluate_binary_expr(b, start, end, interval, lookback_delta);
                Box::pin(fut)
            }
            Expr::Paren(_p) => {
                todo!()
            }
            Expr::Subquery(_q) => {
                todo!()
            }
            Expr::NumberLiteral(l) => {
                let val = l.val;
                Box::pin(async move { Ok(ExprResult::Scalar(val)) })
            }
            Expr::StringLiteral(_l) => {
                todo!()
            }
            Expr::VectorSelector(vector_selector) => {
                let fut = self.evaluate_vector_selector(vector_selector, end, lookback_delta);
                Box::pin(fut)
            }
            Expr::MatrixSelector(matrix_selector) => {
                let fut = self.evaluate_matrix_selector(matrix_selector.clone());
                Box::pin(fut)
            }
            Expr::Call(call) => {
                let fut = self.evaluate_call(call, start, end, interval, lookback_delta);
                Box::pin(fut)
            }
            Expr::Extension(_) => {
                todo!()
            }
        }
    }

    async fn evaluate_matrix_selector(
        &self,
        matrix_selector: MatrixSelector,
    ) -> EvalResult<ExprResult> {
        let _vector_selector = matrix_selector.vs;
        todo!()
        // let labels = self.evaluate_matchers(vector_selector);
        // self.storage.get(&labels).await
    }

    async fn evaluate_vector_selector(
        &self,
        vector_selector: &VectorSelector,
        end: SystemTime,
        lookback_delta: Duration,
    ) -> EvalResult<ExprResult> {
        let end_ms = end
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let start_ms = end_ms - (lookback_delta.as_millis() as i64);

        // Use the selector module to find matching series
        let candidates =
            crate::promql::selector::evaluate_selector_with_reader(self.reader, vector_selector)
                .await
                .map_err(|e| EvaluationError::InternalError(e.to_string()))?;

        // Batch load forward index for all candidates upfront
        let candidates_vec: Vec<_> = candidates.into_iter().collect();
        let forward_index_view = self.reader.forward_index(&candidates_vec).await?;

        let mut series_with_results: HashSet<SeriesFingerprint> = HashSet::new();
        let mut samples = Vec::new();

        for series_id in candidates_vec {
            // Get series spec from forward index view (batched lookup)
            let series_spec = match forward_index_view.get_spec(&series_id) {
                Some(spec) => spec,
                None => {
                    return Err(EvaluationError::InternalError(format!(
                        "Series {} not found in any layer",
                        series_id
                    )));
                }
            };
            let fingerprint = self.compute_fingerprint(&series_spec.labels);

            if series_with_results.contains(&fingerprint) {
                continue;
            }

            // Read and merge timeseries data from all layers
            let sample_data = self.reader.samples(series_id, start_ms, end_ms).await?;

            // Find the best (latest) point in the time range
            if let Some(best_sample) = sample_data.last() {
                // Convert attributes to labels HashMap
                let labels = self.labels_to_hashmap(&series_spec.labels);

                samples.push(EvalSample {
                    timestamp_ms: best_sample.timestamp_ms,
                    value: best_sample.value,
                    labels,
                });

                series_with_results.insert(fingerprint);
            }
        }

        Ok(ExprResult::InstantVector(samples))
    }

    /// Convert labels to HashMap
    fn labels_to_hashmap(&self, labels: &[Label]) -> HashMap<String, String> {
        labels
            .iter()
            .map(|label| (label.name.clone(), label.value.clone()))
            .collect()
    }

    /// Compute fingerprint from labels (simple hash for deduplication)
    fn compute_fingerprint(&self, labels: &[Label]) -> SeriesFingerprint {
        // Use a simple hash of sorted labels
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let mut sorted_labels: Vec<_> = labels.iter().collect();
        sorted_labels.sort_by(|a, b| a.name.cmp(&b.name));
        for label in sorted_labels {
            label.name.hash(&mut hasher);
            label.value.hash(&mut hasher);
        }
        hasher.finish() as SeriesFingerprint
    }

    async fn evaluate_call(
        &self,
        call: &Call,
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        lookback_delta: Duration,
    ) -> EvalResult<ExprResult> {
        // Evaluate the argument first (before creating registry to avoid Send issues)
        let arg_samples = self
            .eval_single_argument(call, start, end, interval, lookback_delta)
            .await?;

        // Get the function from the registry and apply it
        let registry = FunctionRegistry::new();
        let func = registry.get(call.func.name).ok_or_else(|| {
            EvaluationError::InternalError(format!("Unknown function: {}", call.func.name))
        })?;

        // Calculate evaluation timestamp in milliseconds
        let eval_timestamp_ms = end
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Apply the function with the evaluation timestamp
        let result = func.apply(arg_samples, eval_timestamp_ms)?;
        Ok(ExprResult::InstantVector(result))
    }

    async fn eval_single_argument(
        &self,
        call: &Call,
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        lookback_delta: Duration,
    ) -> EvalResult<Vec<EvalSample>> {
        if call.args.args.len() != 1 {
            return Err(EvaluationError::InternalError(format!(
                "{} function requires exactly one argument",
                call.func.name
            )));
        }

        // Evaluate the argument (can be any expression that returns an instant vector)
        match self
            .evaluate_expr(
                call.args.args[0].as_ref(),
                start,
                end,
                interval,
                lookback_delta,
            )
            .await?
        {
            // TODO: support functions with scalar args
            ExprResult::Scalar(_) => Err(EvaluationError::InternalError(
                "unsupported scalar result".to_string(),
            )),
            ExprResult::InstantVector(v) => Ok(v),
        }
    }

    async fn evaluate_binary_expr(
        &self,
        expr: &BinaryExpr,
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        lookback_delta: Duration,
    ) -> EvalResult<ExprResult> {
        let lhs = expr.lhs.as_ref();
        let rhs = expr.rhs.as_ref();
        let op = expr.op;
        // Evaluate left and right expressions
        let left_result = self
            .evaluate_expr(lhs, start, end, interval, lookback_delta)
            .await?;
        let right_result = self
            .evaluate_expr(rhs, start, end, interval, lookback_delta)
            .await?;

        // Check if this is a comparison operation (filters results in PromQL)
        let is_comparison = matches!(op.id(), T_NEQ | T_LSS | T_GTR | T_LTE | T_GTE | T_EQLC);

        match (left_result, right_result) {
            // Vector-Scalar operations: apply scalar to each vector element
            (ExprResult::InstantVector(vector), ExprResult::Scalar(scalar)) => {
                let result: Vec<_> = vector
                    .into_iter()
                    .filter_map(|mut sample| {
                        match self.apply_binary_op(op, sample.value, scalar) {
                            Ok(value) => {
                                // For comparison operations, filter out false results (0.0)
                                if is_comparison && value == 0.0 {
                                    None
                                } else {
                                    sample.value = value;
                                    Some(sample)
                                }
                            }
                            Err(_) => None,
                        }
                    })
                    .collect();
                Ok(ExprResult::InstantVector(result))
            }
            // Scalar-Vector operations: apply scalar to each vector element
            (ExprResult::Scalar(scalar), ExprResult::InstantVector(vector)) => {
                let result: Vec<_> = vector
                    .into_iter()
                    .filter_map(|mut sample| {
                        match self.apply_binary_op(op, scalar, sample.value) {
                            Ok(value) => {
                                // For comparison operations, filter out false results (0.0)
                                if is_comparison && value == 0.0 {
                                    None
                                } else {
                                    sample.value = value;
                                    Some(sample)
                                }
                            }
                            Err(_) => None,
                        }
                    })
                    .collect();
                Ok(ExprResult::InstantVector(result))
            }
            // Vector-Vector operations: one-to-one matching only
            (ExprResult::InstantVector(left_vector), ExprResult::InstantVector(right_vector)) => {
                if let Some(modifier) = &expr.modifier {
                    if !matches!(modifier.card, VectorMatchCardinality::OneToOne) {
                        return Err(EvaluationError::InternalError(
                            "only one-to-one cardinality supported".to_string(),
                        ));
                        // TODO: support many-to-one/one-to-many cardinality
                    }
                    if modifier.matching.is_some() {
                        return Err(EvaluationError::InternalError(
                            "label matching not yet supported".to_string(),
                        ));
                        // TODO: support label matching between vectors
                    }
                }

                // For one-to-one matching, we need vectors of the same size with matching labels
                if left_vector.len() != right_vector.len() {
                    return Err(EvaluationError::InternalError(
                        "Vector-vector operations require one-to-one matching cardinality"
                            .to_string(),
                    ));
                }

                let mut result = Vec::new();

                // Simple implementation: match by position (assumes same ordering)
                // TODO: Implement proper label matching
                for (left_sample, right_sample) in
                    left_vector.into_iter().zip(right_vector.into_iter())
                {
                    if left_sample.labels != right_sample.labels {
                        return Err(EvaluationError::InternalError(
                            "Vector-vector operations require matching labels".to_string(),
                        ));
                    }

                    let mut sample = left_sample;
                    match self.apply_binary_op(op, sample.value, right_sample.value) {
                        Ok(value) => {
                            // For comparison operations, filter out false results (0.0)
                            if is_comparison && value == 0.0 {
                                continue;
                            }
                            sample.value = value;
                            result.push(sample);
                        }
                        Err(e) => return Err(e),
                    }
                }

                Ok(ExprResult::InstantVector(result))
            }
            // Scalar-Scalar operations
            (ExprResult::Scalar(left), ExprResult::Scalar(right)) => {
                let result_value = self.apply_binary_op(op, left, right)?;
                Ok(ExprResult::Scalar(result_value))
            }
        }
    }

    fn apply_binary_op(&self, op: TokenType, left: f64, right: f64) -> EvalResult<f64> {
        // Use the token constants with TokenType::new() for clean comparison
        match op.id() {
            T_ADD => Ok(left + right),
            T_SUB => Ok(left - right),
            T_MUL => Ok(left * right),
            T_DIV => {
                if right == 0.0 {
                    Ok(f64::NAN) // Division by zero results in NaN in PromQL
                } else {
                    Ok(left / right)
                }
            }
            T_NEQ => Ok(if left != right { 1.0 } else { 0.0 }),
            T_LSS => Ok(if left < right { 1.0 } else { 0.0 }),
            T_GTR => Ok(if left > right { 1.0 } else { 0.0 }),
            T_LTE => Ok(if left <= right { 1.0 } else { 0.0 }),
            T_GTE => Ok(if left >= right { 1.0 } else { 0.0 }),
            T_EQLC => Ok(if left == right { 1.0 } else { 0.0 }),
            _ => Err(EvaluationError::InternalError(format!(
                "Binary operator not yet implemented: {:?}",
                op
            ))),
        }
    }

    fn compute_grouping_labels(
        mut labels: HashMap<String, String>,
        modifier: Option<&LabelModifier>,
    ) -> HashMap<String, String> {
        match modifier {
            None => HashMap::new(), // No grouping, return empty labels
            Some(LabelModifier::Include(label_list)) => {
                // Keep only specified labels
                labels.retain(|k, _| label_list.labels.contains(k));
                labels
            }
            Some(LabelModifier::Exclude(label_list)) => {
                // Remove specified labels
                labels.retain(|k, _| !label_list.labels.contains(k));
                labels
            }
        }
    }

    fn labels_to_grouping_key(labels: HashMap<String, String>) -> Vec<(String, String)> {
        let mut key_vec: Vec<_> = labels.into_iter().collect();
        key_vec.sort();
        key_vec
    }

    async fn evaluate_aggregate(
        &self,
        aggregate: &AggregateExpr,
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        lookback_delta: Duration,
    ) -> EvalResult<ExprResult> {
        // Evaluate the inner expression to get all samples
        let result = self
            .evaluate_expr(&aggregate.expr, start, end, interval, lookback_delta)
            .await?;

        // Extract samples from the result
        let samples = match result {
            ExprResult::InstantVector(samples) => samples,
            ExprResult::Scalar(_) => {
                return Err(EvaluationError::InternalError(
                    "Cannot aggregate scalar values".to_string(),
                ));
            }
        };

        // If there are no samples, return empty result
        if samples.is_empty() {
            return Ok(ExprResult::InstantVector(vec![]));
        }

        // Group samples by their grouping key (which consumes the filtered labels)
        let mut groups: HashMap<Vec<(String, String)>, Vec<f64>> = HashMap::new();
        for sample in samples {
            // Compute the grouping labels by taking ownership and filtering
            let group_labels =
                Self::compute_grouping_labels(sample.labels, aggregate.modifier.as_ref());

            // Convert labels to sorted key, consuming the labels
            let group_key = Self::labels_to_grouping_key(group_labels);

            groups.entry(group_key).or_default().push(sample.value);
        }

        // Use the end time as the timestamp for the aggregated result
        let timestamp_ms = end
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Aggregate each group
        let mut result_samples = Vec::new();
        for (group_key, values) in groups {
            // Apply the aggregation function to this group
            let aggregated_value = match aggregate.op.id() {
                T_SUM => values.iter().sum(),
                T_AVG => values.iter().sum::<f64>() / values.len() as f64,
                T_MIN => values.iter().fold(f64::INFINITY, |a, &b| f64::min(a, b)),
                T_MAX => values
                    .iter()
                    .fold(f64::NEG_INFINITY, |a, &b| f64::max(a, b)),
                T_COUNT => values.len() as f64,
                _ => {
                    return Err(EvaluationError::InternalError(format!(
                        "Unsupported aggregation operator: {:?}",
                        aggregate.op
                    )));
                }
            };

            // Reconstruct the labels HashMap from the group key
            let result_labels: HashMap<String, String> = group_key.into_iter().collect();

            result_samples.push(EvalSample {
                timestamp_ms,
                value: aggregated_value,
                labels: result_labels,
            });
        }

        Ok(ExprResult::InstantVector(result_samples))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TimeBucket;
    use crate::model::{Label, MetricType, Sample};
    use crate::query::test_utils::MockQueryReaderBuilder;
    use crate::test_utils::assertions::approx_eq;
    use promql_parser::label::METRIC_NAME;
    use promql_parser::parser::EvalStmt;
    use rstest::rstest;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Type alias for test data: (metric_name, labels, timestamp_offset_ms, value)
    type TestSampleData = Vec<(&'static str, Vec<(&'static str, &'static str)>, i64, f64)>;

    /// Helper to parse a PromQL query and evaluate it
    async fn parse_and_evaluate<R: QueryReader>(
        evaluator: &Evaluator<'_, R>,
        query: &str,
        end_time: SystemTime,
        lookback_delta: Duration,
    ) -> EvalResult<Vec<EvalSample>> {
        let expr = promql_parser::parser::parse(query)
            .map_err(|e| EvaluationError::InternalError(format!("Parse error: {}", e)))?;

        let stmt = EvalStmt {
            expr,
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };

        evaluator.evaluate(stmt).await
    }

    /// Helper to convert label vec to HashMap for comparison
    fn labels_to_map(labels: &[(&str, &str)]) -> HashMap<String, String> {
        labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Sort samples by labels (for deterministic comparison)
    fn sort_samples_by_labels(samples: &mut [EvalSample]) {
        samples.sort_by(|a, b| {
            let mut a_labels: Vec<_> = a.labels.iter().collect();
            let mut b_labels: Vec<_> = b.labels.iter().collect();
            a_labels.sort();
            b_labels.sort();
            a_labels.cmp(&b_labels)
        });
    }

    /// Compare actual results with expected results
    fn assert_results_match(actual: &[EvalSample], expected: &[(f64, Vec<(&str, &str)>)]) {
        assert_eq!(
            actual.len(),
            expected.len(),
            "Result count mismatch: got {}, expected {}",
            actual.len(),
            expected.len()
        );

        let mut actual_sorted: Vec<_> = actual.to_vec();
        sort_samples_by_labels(&mut actual_sorted);

        let mut expected_sorted: Vec<_> = expected.to_vec();
        expected_sorted.sort_by(|a, b| {
            let a_labels = labels_to_map(&a.1);
            let b_labels = labels_to_map(&b.1);
            let mut a_vec: Vec<_> = a_labels.iter().collect();
            let mut b_vec: Vec<_> = b_labels.iter().collect();
            a_vec.sort();
            b_vec.sort();
            a_vec.cmp(&b_vec)
        });

        for (i, (actual_sample, (expected_value, expected_labels))) in
            actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
        {
            assert!(
                approx_eq(actual_sample.value, *expected_value),
                "Sample {} value mismatch: got {}, expected {}",
                i,
                actual_sample.value,
                expected_value
            );

            let expected_labels_map = labels_to_map(expected_labels);
            assert_eq!(
                actual_sample.labels, expected_labels_map,
                "Sample {} labels mismatch: got {:?}, expected {:?}",
                i, actual_sample.labels, expected_labels_map
            );
        }
    }

    /// Helper to create labels from metric name and label pairs
    fn create_labels(metric_name: &str, label_pairs: Vec<(&str, &str)>) -> Vec<Label> {
        let mut labels = vec![Label {
            name: METRIC_NAME.to_string(),
            value: metric_name.to_string(),
        }];
        for (key, val) in label_pairs {
            labels.push(Label {
                name: key.to_string(),
                value: val.to_string(),
            });
        }
        labels
    }

    /// Setup helper: Creates a MockQueryReader with test data
    /// data: Vec of (metric_name, labels, timestamp_offset_ms, value)
    /// Returns (MockQueryReader, end_time) where end_time is suitable for querying
    fn setup_mock_reader(
        data: TestSampleData,
    ) -> (crate::query::test_utils::MockQueryReader, SystemTime) {
        let bucket = TimeBucket::hour(1000);
        let mut builder = MockQueryReaderBuilder::new(bucket);

        // Base timestamp: 300001ms (ensures samples are > start_ms with 5min lookback)
        // Query time will be calculated to be well after all samples
        let base_timestamp = 300001i64;

        // Find max offset before consuming data
        let max_offset = data
            .iter()
            .map(|(_, _, offset_ms, _)| *offset_ms)
            .max()
            .unwrap_or(0);

        for (metric_name, labels, offset_ms, value) in data {
            let attributes = create_labels(metric_name, labels);
            let sample = Sample {
                timestamp_ms: base_timestamp + offset_ms,
                value,
            };
            builder.add_sample(attributes, MetricType::Gauge, sample);
        }

        // Query time: base_timestamp + max_offset + 1ms (just after all samples)
        // Lookback window: (start_ms, query_time] where start_ms = query_time - 300000
        // Since lookback uses exclusive start (timestamp > start_ms), we need:
        //   start_ms < base_timestamp (to include all samples)
        //   => query_time - 300000 < base_timestamp
        //   => query_time < base_timestamp + 300000
        // We set query_time = base_timestamp + max_offset + 1, which works as long as max_offset < 300000
        // This ensures start_ms = base_timestamp + max_offset + 1 - 300000 < base_timestamp
        // So all samples at base_timestamp + offset (where offset <= max_offset) are included
        let query_timestamp = base_timestamp + max_offset + 1;
        let end_time = UNIX_EPOCH + Duration::from_millis(query_timestamp as u64);

        (builder.build(), end_time)
    }

    #[rstest]
    // Vector Selectors
    #[case(
        "vector_selector_all_series",
        "http_requests_total",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_with_single_equality_matcher",
        r#"http_requests_total{env="prod"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_with_different_label_matcher",
        r#"http_requests_total{method="GET"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
        ]
    )]
    #[case(
        "vector_selector_with_multiple_equality_matchers",
        r#"http_requests_total{env="prod",method="GET"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
        ]
    )]
    #[case(
        "vector_selector_with_not_equal_matcher",
        r#"http_requests_total{env!="staging"}"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
        ]
    )]
    #[case(
        "vector_selector_different_metric",
        "cpu_usage",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "vector_selector_single_series_metric",
        "memory_bytes",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0, vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    // Function Calls - Unary Math
    #[case(
        "function_abs",
        "abs(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (30.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "function_sqrt",
        "sqrt(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![("__name__", "memory_bytes"), ("env", "prod")]), // sqrt(100) = 10
        ]
    )]
    #[case(
        "function_ceil",
        "ceil(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_floor",
        "floor(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_round",
        "round(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0, vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    // Function Calls - Trigonometry
    #[case(
        "function_sin",
        "sin(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0_f64.sin(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0_f64.sin(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    #[case(
        "function_cos",
        "cos(cpu_usage)",
        vec![
            ("cpu_usage", vec![("env", "prod"), ("instance", "i1")], 0, 50.0),
            ("cpu_usage", vec![("env", "prod"), ("instance", "i2")], 1, 60.0),
        ],
        vec![
            (50.0_f64.cos(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i1")]),
            (60.0_f64.cos(), vec![("__name__", "cpu_usage"), ("env", "prod"), ("instance", "i2")]),
        ]
    )]
    // Function Calls - Logarithms
    #[case(
        "function_ln",
        "ln(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.ln(), vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    #[case(
        "function_log10",
        "log10(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.log10(), vec![("__name__", "memory_bytes"), ("env", "prod")]), // log10(100) = 2
        ]
    )]
    #[case(
        "function_log2",
        "log2(memory_bytes)",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (100.0_f64.log2(), vec![("__name__", "memory_bytes"), ("env", "prod")]),
        ]
    )]
    // Function Calls - Special
    #[case(
        "function_absent_with_existing_metric",
        "absent(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
        ],
        vec![] // Should return empty since http_requests_total exists
    )]
    #[case(
        "function_absent_with_nonexistent_metric",
        "absent(nonexistent_metric)",
        vec![
            ("other_metric", vec![("env", "prod")], 0, 5.0),
        ],
        vec![
            (1.0, vec![]), // Should return 1.0 when metric doesn't exist
        ]
    )]
    // Binary Operations - Arithmetic
    #[case(
        "binary_add_vector_scalar",
        "http_requests_total + 5",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (15.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (25.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (35.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (45.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "binary_multiply_vector_scalar",
        "http_requests_total * 2",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (20.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]),
            (40.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]),
            (60.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]),
            (80.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]),
        ]
    )]
    #[case(
        "binary_divide_vector_scalar",
        "memory_bytes / 10",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![("__name__", "memory_bytes"), ("env", "prod")]), // 100 / 10 = 10
        ]
    )]
    // Binary Operations - Comparison
    #[case(
        "binary_greater_than_filter",
        "http_requests_total > 15",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 > 15
            (1.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "GET")]), // 30 > 15
            (1.0, vec![("__name__", "http_requests_total"), ("env", "staging"), ("method", "POST")]), // 40 > 15
        ]
    )]
    #[case(
        "binary_less_than_filter",
        "http_requests_total < 25",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "GET")]), // 10 < 25
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 < 25
        ]
    )]
    #[case(
        "binary_equal_filter",
        "http_requests_total == 20",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (1.0, vec![("__name__", "http_requests_total"), ("env", "prod"), ("method", "POST")]), // 20 == 20
        ]
    )]
    // Aggregations
    #[case(
        "aggregation_sum",
        "sum(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (100.0, vec![]), // 10 + 20 + 30 + 40 = 100
        ]
    )]
    #[case(
        "aggregation_avg",
        "avg(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (25.0, vec![]), // (10 + 20 + 30 + 40) / 4 = 25
        ]
    )]
    #[case(
        "aggregation_min",
        "min(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (10.0, vec![]), // min(10, 20, 30, 40) = 10
        ]
    )]
    #[case(
        "aggregation_max",
        "max(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (40.0, vec![]), // max(10, 20, 30, 40) = 40
        ]
    )]
    #[case(
        "aggregation_count",
        "count(http_requests_total)",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (4.0, vec![]), // 4 series
        ]
    )]
    // Aggregations with grouping
    #[case(
        "aggregation_sum_by_env",
        r#"sum by (env) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (30.0, vec![("env", "prod")]),    // 10 + 20 = 30
            (70.0, vec![("env", "staging")]), // 30 + 40 = 70
        ]
    )]
    #[case(
        "aggregation_avg_by_env",
        r#"avg by (env) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (15.0, vec![("env", "prod")]),    // (10 + 20) / 2 = 15
            (35.0, vec![("env", "staging")]), // (30 + 40) / 2 = 35
        ]
    )]
    #[case(
        "aggregation_sum_by_method",
        r#"sum by (method) (http_requests_total)"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (40.0, vec![("method", "GET")]),  // 10 + 30 = 40
            (60.0, vec![("method", "POST")]), // 20 + 40 = 60
        ]
    )]
    // Complex Expressions
    #[case(
        "nested_function_abs_sum",
        "abs(sum(http_requests_total))",
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "POST")], 3, 40.0),
        ],
        vec![
            (100.0, vec![]), // abs(sum(10, 20, 30, 40)) = abs(100) = 100
        ]
    )]
    #[case(
        "nested_function_sqrt_sum",
        "sqrt(sum(memory_bytes))",
        vec![
            ("memory_bytes", vec![("env", "prod")], 0, 100.0),
        ],
        vec![
            (10.0, vec![]), // sqrt(sum(100)) = sqrt(100) = 10
        ]
    )]
    #[case(
        "aggregation_with_selector",
        r#"sum(http_requests_total{env="prod"})"#,
        vec![
            ("http_requests_total", vec![("env", "prod"), ("method", "GET")], 0, 10.0),
            ("http_requests_total", vec![("env", "prod"), ("method", "POST")], 1, 20.0),
            ("http_requests_total", vec![("env", "staging"), ("method", "GET")], 2, 30.0),
        ],
        vec![
            (30.0, vec![]), // sum(10, 20) = 30
        ]
    )]
    #[rstest]
    #[tokio::test]
    async fn should_evaluate_queries(
        #[case] _name: &str,
        #[case] query: &str,
        #[case] test_data: TestSampleData,
        #[case] expected_samples: Vec<(f64, Vec<(&str, &str)>)>,
    ) {
        let (reader, end_time) = setup_mock_reader(test_data);
        let evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300); // 5 minutes

        let result = parse_and_evaluate(&evaluator, query, end_time, lookback_delta)
            .await
            .expect("Query should evaluate successfully");

        assert_results_match(&result, &expected_samples);
    }

    #[tokio::test]
    async fn should_evaluate_number_literal() {
        // given: create an empty mock reader
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let evaluator = Evaluator::new(&reader);

        // when: evaluate a number literal (should return scalar, which is unsupported)
        let end_time = UNIX_EPOCH + Duration::from_secs(2000);
        let stmt = EvalStmt {
            expr: promql_parser::parser::Expr::NumberLiteral(
                promql_parser::parser::NumberLiteral { val: 42.0 },
            ),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt).await;

        // then: should return error for scalar result
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unsupported result type: scalar")
        );
    }
}

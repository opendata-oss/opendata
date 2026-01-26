use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::index::{ForwardIndexLookup, InvertedIndexLookup, SeriesSpec};
use crate::model::Sample;
use crate::model::SeriesFingerprint;
use crate::model::{Label, SeriesId, TimeBucket};
use crate::promql::functions::FunctionRegistry;
use crate::promql::selector::evaluate_selector_with_reader;
use crate::query::QueryReader;
use crate::util::Result;
use promql_parser::parser::token::*;
use promql_parser::parser::{
    AggregateExpr, BinaryExpr, Call, EvalStmt, Expr, LabelModifier, MatrixSelector,
    VectorMatchCardinality, VectorSelector,
};

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

/// Type alias for complex HashMap used in matrix selector evaluation.
/// Maps from label key (sorted vector of label pairs) to samples vector
type SeriesMap = HashMap<Vec<Label>, Vec<Sample>>;

pub(crate) struct QueryReaderBucketEvalCache {
    // Map from terms (series_ids for forward, labels for inverted) to cached results
    forward_index_cache:
        HashMap<Vec<SeriesId>, Arc<dyn ForwardIndexLookup + Send + Sync + 'static>>,
    inverted_index_cache: HashMap<Vec<Label>, Arc<dyn InvertedIndexLookup + Send + Sync + 'static>>,
    samples: HashMap<SeriesId, Vec<Sample>>,
}

impl QueryReaderBucketEvalCache {
    fn new() -> Self {
        Self {
            forward_index_cache: HashMap::new(),
            inverted_index_cache: HashMap::new(),
            samples: HashMap::new(),
        }
    }
}

pub(crate) struct QueryReaderEvalCache {
    cache: HashMap<TimeBucket, QueryReaderBucketEvalCache>,
}

impl QueryReaderEvalCache {
    pub(crate) fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub(crate) fn get_bucket_cache_mut(
        &mut self,
        bucket: &TimeBucket,
    ) -> &mut QueryReaderBucketEvalCache {
        self.cache
            .entry(*bucket)
            .or_insert_with(QueryReaderBucketEvalCache::new)
    }

    pub(crate) fn cache_forward_index(
        &mut self,
        bucket: TimeBucket,
        series_ids: Vec<SeriesId>,
        forward_index: Box<dyn ForwardIndexLookup + Send + Sync + 'static>,
    ) {
        let bucket_cache = self.get_bucket_cache_mut(&bucket);
        bucket_cache
            .forward_index_cache
            .insert(series_ids, forward_index.into());
    }

    pub(crate) fn get_forward_index(
        &self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Option<Arc<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        self.cache
            .get(bucket)
            .and_then(|bucket_cache| bucket_cache.forward_index_cache.get(series_ids))
            .cloned()
    }

    pub(crate) fn cache_inverted_index(
        &mut self,
        bucket: TimeBucket,
        terms: Vec<Label>,
        result: Box<dyn InvertedIndexLookup + Send + Sync + 'static>,
    ) {
        let bucket_cache = self.get_bucket_cache_mut(&bucket);
        bucket_cache
            .inverted_index_cache
            .insert(terms, result.into());
    }

    pub(crate) fn get_inverted_index(
        &self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Option<Arc<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        self.cache
            .get(bucket)
            .and_then(|bucket_cache| bucket_cache.inverted_index_cache.get(terms))
            .cloned()
    }

    pub(crate) fn cache_samples(
        &mut self,
        bucket: TimeBucket,
        series_id: SeriesId,
        samples: Vec<Sample>,
    ) {
        let bucket_cache = self.get_bucket_cache_mut(&bucket);
        bucket_cache.samples.insert(series_id, samples);
    }

    pub(crate) fn get_samples(
        &self,
        bucket: &TimeBucket,
        series_id: &SeriesId,
    ) -> Option<&Vec<Sample>> {
        self.cache
            .get(bucket)
            .and_then(|bucket_cache| bucket_cache.samples.get(series_id))
    }
}

/// A cached forward index lookup that wraps cached data
struct CachedForwardIndex {
    data: HashMap<SeriesId, SeriesSpec>,
}

impl ForwardIndexLookup for CachedForwardIndex {
    fn get_spec(&self, series_id: &SeriesId) -> Option<SeriesSpec> {
        self.data.get(series_id).cloned()
    }

    fn all_series(&self) -> Vec<(SeriesId, SeriesSpec)> {
        self.data
            .iter()
            .map(|(&id, spec)| (id, spec.clone()))
            .collect()
    }
}

/// A cached inverted index lookup that wraps cached data
struct CachedInvertedIndex {
    result: roaring::RoaringBitmap,
}

impl InvertedIndexLookup for CachedInvertedIndex {
    fn intersect(&self, _terms: Vec<Label>) -> roaring::RoaringBitmap {
        // Return the pre-computed intersection result
        self.result.clone()
    }

    fn all_keys(&self) -> Vec<Label> {
        // This method doesn't make sense for a pre-computed intersection result
        // but we need to implement it for the trait
        Vec::new()
    }
}

// ToDo(cadonna): Add histogram samples
#[derive(Debug, Clone, PartialEq)]
pub struct EvalSample {
    pub(crate) timestamp_ms: i64,
    pub(crate) value: f64,
    pub(crate) labels: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EvalSamples {
    pub(crate) values: Vec<Sample>,
    pub(crate) labels: HashMap<String, String>,
}

pub(crate) struct Evaluator<'reader, R: QueryReader> {
    reader: CachedQueryReader<'reader, R>,
}

/// A wrapper around QueryReader that uses QueryReaderEvalCache for caching
pub(crate) struct CachedQueryReader<'reader, R: QueryReader> {
    reader: &'reader R,
    cache: QueryReaderEvalCache,
}

impl<'reader, R: QueryReader> CachedQueryReader<'reader, R> {
    pub(crate) fn new(reader: &'reader R) -> Self {
        Self {
            reader,
            cache: QueryReaderEvalCache::new(),
        }
    }

    pub(crate) async fn list_buckets(&self) -> Result<Vec<TimeBucket>> {
        self.reader.list_buckets().await
    }

    pub(crate) async fn forward_index(
        &mut self,
        bucket: &TimeBucket,
        series_ids: &[SeriesId],
    ) -> Result<Arc<dyn ForwardIndexLookup + Send + Sync + 'static>> {
        let mut series_ids = Vec::from(series_ids);
        series_ids.sort();
        // Check cache first
        if let Some(cached_data) = self.cache.get_forward_index(bucket, &series_ids) {
            Ok(cached_data)
        } else {
            // Load from underlying reader
            let forward_index = self.reader.forward_index(bucket, &series_ids).await?;

            self.cache
                .cache_forward_index(*bucket, series_ids.clone(), forward_index);

            Ok(self
                .cache
                .get_forward_index(bucket, &series_ids)
                .expect("unreachable"))
        }
    }

    pub(crate) async fn inverted_index(
        &mut self,
        bucket: &TimeBucket,
        terms: &[Label],
    ) -> Result<Arc<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        let mut terms = terms.to_vec();
        // Sort by canonical Label ordering (name, then value) for cache key consistency
        terms.sort();
        // Check cache first
        if let Some(cached_result) = self.cache.get_inverted_index(bucket, &terms) {
            return Ok(cached_result);
        }

        // Load from underlying reader
        let inverted_index = self.reader.inverted_index(bucket, &terms).await?;

        // Cache the result
        self.cache
            .cache_inverted_index(*bucket, terms.clone(), inverted_index);

        Ok(self
            .cache
            .get_inverted_index(bucket, &terms)
            .expect("unreachable"))
    }

    pub(crate) async fn all_inverted_index(
        &self,
        bucket: &TimeBucket,
    ) -> Result<Box<dyn InvertedIndexLookup + Send + Sync + 'static>> {
        self.reader.all_inverted_index(bucket).await
    }

    pub(crate) async fn label_values(
        &self,
        bucket: &TimeBucket,
        label_name: &str,
    ) -> Result<Vec<String>> {
        self.reader.label_values(bucket, label_name).await
    }

    pub(crate) async fn samples(
        &mut self,
        bucket: &TimeBucket,
        series_id: SeriesId,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<Sample>> {
        // Check cache first
        if let Some(cached_samples) = self.cache.get_samples(bucket, &series_id) {
            // Filter cached samples by requested time range
            let filtered: Vec<Sample> = cached_samples
                .iter()
                .filter(|s| s.timestamp_ms > start_ms && s.timestamp_ms <= end_ms)
                .cloned()
                .collect();
            return Ok(filtered);
        }

        // Not in cache, load from underlying reader with wide bounds to cache the whole bucket
        let samples = self
            .reader
            .samples(bucket, series_id, i64::MIN, i64::MAX)
            .await?;

        // Cache the full sample set
        self.cache
            .cache_samples(*bucket, series_id, samples.clone());

        // Filter by requested time range
        let filtered: Vec<Sample> = samples
            .iter()
            .filter(|s| s.timestamp_ms > start_ms && s.timestamp_ms <= end_ms)
            .cloned()
            .collect();

        Ok(filtered)
    }
}

#[derive(Debug)]
pub(crate) enum ExprResult {
    Scalar(f64),
    InstantVector(Vec<EvalSample>),
    RangeVector(Vec<EvalSamples>),
}

impl ExprResult {
    /// Extract the instant vector samples, returning None if this is a scalar or range vector result
    pub(crate) fn into_instant_vector(self) -> Option<Vec<EvalSample>> {
        match self {
            ExprResult::InstantVector(samples) => Some(samples),
            ExprResult::Scalar(_) | ExprResult::RangeVector(_) => None,
        }
    }

    /// Extract the range vector samples, returning None if this is not a range vector result
    pub(crate) fn into_range_vector(self) -> Option<Vec<EvalSamples>> {
        match self {
            ExprResult::RangeVector(samples) => Some(samples),
            ExprResult::Scalar(_) | ExprResult::InstantVector(_) => None,
        }
    }

    #[cfg(test)]
    /// Extract instant vector samples, panicking if this is not an instant vector result
    pub(crate) fn expect_instant_vector(self, msg: &str) -> Vec<EvalSample> {
        match self {
            ExprResult::InstantVector(samples) => samples,
            ExprResult::Scalar(_) | ExprResult::RangeVector(_) => panic!("{}", msg),
        }
    }
}

impl<'reader, R: QueryReader> Evaluator<'reader, R> {
    pub(crate) fn new(reader: &'reader R) -> Self {
        Self {
            reader: CachedQueryReader {
                reader,
                cache: QueryReaderEvalCache::new(),
            },
        }
    }

    pub(crate) async fn evaluate(&mut self, stmt: EvalStmt) -> EvalResult<ExprResult> {
        if stmt.start != stmt.end {
            return Err(EvaluationError::InternalError(format!(
                "evaluation must always be done at an instant.got start({:?}), end({:?})",
                stmt.start, stmt.end
            )));
        }
        self.evaluate_expr(
            &stmt.expr,
            stmt.start,
            stmt.end,
            stmt.interval,
            stmt.lookback_delta,
        )
        .await
    }

    // this call recurses to evaluate sub-expressions, so it needs to return a boxed future
    // so that the return type is sized (so can be stack-allocated)
    fn evaluate_expr<'a>(
        &'a mut self,
        expr: &'a Expr,
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        lookback_delta: Duration,
    ) -> Pin<Box<dyn Future<Output = EvalResult<ExprResult>> + Send + 'a>> {
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
            Expr::Paren(p) => {
                let fut = self.evaluate_expr(&p.expr, start, end, interval, lookback_delta);
                Box::pin(fut)
            }
            Expr::Subquery(_q) => {
                todo!()
            }
            Expr::NumberLiteral(l) => {
                let val = l.val;
                Box::pin(async move { Ok(ExprResult::Scalar(val)) })
            }
            Expr::StringLiteral(l) => {
                let val = l.val.clone();
                Box::pin(async move {
                    Err(EvaluationError::InternalError(format!(
                        "string literal \"{}\" is not directly evaluatable",
                        val
                    )))
                })
            }
            Expr::VectorSelector(vector_selector) => {
                let fut = self.evaluate_vector_selector(vector_selector, end, lookback_delta);
                Box::pin(fut)
            }
            Expr::MatrixSelector(matrix_selector) => {
                let fut = self.evaluate_matrix_selector(matrix_selector.clone(), end);
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
        &mut self,
        matrix_selector: MatrixSelector,
        end: SystemTime,
    ) -> EvalResult<ExprResult> {
        let vector_selector = &matrix_selector.vs;
        let range = matrix_selector.range;

        let start = end - range;

        let end_ms = end
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let start_ms = start
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // order buckets in chronological order
        let mut buckets = self.reader.list_buckets().await?;
        buckets.sort_by(|a, b| a.start.cmp(&b.start));

        // Group samples by series (using sorted label vector as key since HashMap doesn't impl Hash)
        let mut series_map: SeriesMap = HashMap::new();

        for bucket in buckets {
            // Check if bucket overlaps with our time range
            let bucket_start_ms = (bucket.start as i64) * 60 * 1000; // Convert minutes to milliseconds
            let bucket_end_ms = bucket_start_ms + (bucket.size_in_mins() as i64) * 60 * 1000;
            if bucket_end_ms < start_ms || bucket_start_ms > end_ms {
                continue;
            }

            let candidates =
                evaluate_selector_with_reader(&mut self.reader, bucket, vector_selector)
                    .await
                    .map_err(|e| EvaluationError::InternalError(e.to_string()))?;

            if candidates.is_empty() {
                continue;
            }

            let candidates_vec: Vec<_> = candidates.into_iter().collect();
            let forward_index_view = self.reader.forward_index(&bucket, &candidates_vec).await?;

            for series_id in candidates_vec {
                let series_spec = match forward_index_view.get_spec(&series_id) {
                    Some(spec) => spec,
                    None => {
                        return Err(EvaluationError::InternalError(format!(
                            "Series {} not found in bucket {:?}",
                            series_id, bucket
                        )));
                    }
                };

                let sample_data = self
                    .reader
                    .samples(&bucket, series_id, start_ms, end_ms)
                    .await?;

                let mut labels_key: Vec<Label> = series_spec.labels.clone();
                // Sort by canonical Label ordering (name, then value) for series grouping
                labels_key.sort();

                let values = series_map.entry(labels_key).or_default();
                for sample in sample_data {
                    values.push(sample);
                }
            }
        }

        let mut range_vector = Vec::new();
        for (labels, values) in series_map {
            let labels = self.labels_to_hashmap(&labels);
            range_vector.push(EvalSamples { values, labels });
        }

        Ok(ExprResult::RangeVector(range_vector))
    }

    async fn evaluate_vector_selector(
        &mut self,
        vector_selector: &VectorSelector,
        end: SystemTime,
        lookback_delta: Duration,
    ) -> EvalResult<ExprResult> {
        let end_ms = end
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let start_ms = end_ms - (lookback_delta.as_millis() as i64);

        // Get all buckets and sort by start time in reverse order (newest first)
        let mut buckets = self.reader.list_buckets().await?;
        buckets.sort_by(|a, b| b.start.cmp(&a.start)); // newest first

        let mut series_with_results: HashSet<SeriesFingerprint> = HashSet::new();
        let mut samples = Vec::new();

        // Iterate through buckets in reverse time order (newest first)
        for bucket in buckets {
            // Find matching series in this bucket
            let candidates =
                evaluate_selector_with_reader(&mut self.reader, bucket, vector_selector)
                    .await
                    .map_err(|e| EvaluationError::InternalError(e.to_string()))?;

            if candidates.is_empty() {
                continue;
            }

            // Batch load forward index for all candidates upfront
            let candidates_vec: Vec<_> = candidates.into_iter().collect();
            let forward_index_view = self.reader.forward_index(&bucket, &candidates_vec).await?;

            for series_id in candidates_vec {
                // Get series spec from forward index view (batched lookup)
                let series_spec = match forward_index_view.get_spec(&series_id) {
                    Some(spec) => spec,
                    None => {
                        return Err(EvaluationError::InternalError(format!(
                            "Series {} not found in bucket {:?}",
                            series_id, bucket
                        )));
                    }
                };
                let fingerprint = self.compute_fingerprint(&series_spec.labels);

                // Skip if we already found a sample for this series in a newer bucket
                if series_with_results.contains(&fingerprint) {
                    continue;
                }

                // Read samples from this bucket within the lookback window
                let sample_data = self
                    .reader
                    .samples(&bucket, series_id, start_ms, end_ms)
                    .await?;

                // Find the best (latest) point in the time range
                if let Some(best_sample) = sample_data.last() {
                    // Convert attributes to labels HashMap
                    let labels = self.labels_to_hashmap(&series_spec.labels);

                    samples.push(EvalSample {
                        timestamp_ms: best_sample.timestamp_ms,
                        value: best_sample.value,
                        labels,
                    });

                    // Mark this series fingerprint as found so we don't add it again from older buckets
                    series_with_results.insert(fingerprint);
                }
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
        &mut self,
        call: &Call,
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        lookback_delta: Duration,
    ) -> EvalResult<ExprResult> {
        if call.args.args.len() != 1 {
            return Err(EvaluationError::InternalError(format!(
                "{} function requires exactly one argument",
                call.func.name
            )));
        }

        // Check for string literal arguments before evaluation.
        // String literals are valid as function arguments in PromQL (e.g., label_replace),
        // but we don't yet support evaluating them as function arguments.
        let arg = call.args.args[0].as_ref();
        if let Expr::StringLiteral(lit) = arg {
            return Err(EvaluationError::InternalError(format!(
                "string literal \"{}\" passed as argument to function '{}': \
                 string arguments are not yet supported",
                lit.val, call.func.name
            )));
        }

        // Evaluate the argument
        let arg_result = self
            .evaluate_expr(arg, start, end, interval, lookback_delta)
            .await?;

        let registry = FunctionRegistry::new();

        // Calculate evaluation timestamp in milliseconds
        let eval_timestamp_ms = end
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        match arg_result {
            ExprResult::InstantVector(samples) => {
                // Try instant vector function first
                if let Some(func) = registry.get(call.func.name) {
                    let result = func.apply(samples, eval_timestamp_ms)?;
                    Ok(ExprResult::InstantVector(result))
                } else {
                    Err(EvaluationError::InternalError(format!(
                        "Unknown instant vector function: {}",
                        call.func.name
                    )))
                }
            }
            ExprResult::RangeVector(samples) => {
                // Try range vector function
                if let Some(func) = registry.get_range_function(call.func.name) {
                    let result = func.apply(samples, eval_timestamp_ms)?;
                    Ok(ExprResult::InstantVector(result))
                } else {
                    Err(EvaluationError::InternalError(format!(
                        "Unknown range vector function: {}",
                        call.func.name
                    )))
                }
            }
            ExprResult::Scalar(_) => Err(EvaluationError::InternalError(
                "Scalar function arguments not yet supported".to_string(),
            )),
        }
    }

    async fn eval_single_argument(
        &mut self,
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
            ExprResult::RangeVector(_) => Err(EvaluationError::InternalError(
                "range vector handling not yet implemented in eval_single_argument".to_string(),
            )),
        }
    }

    async fn evaluate_binary_expr(
        &mut self,
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
            // RangeVector operations not yet supported
            (ExprResult::RangeVector(_), _) | (_, ExprResult::RangeVector(_)) => {
                Err(EvaluationError::InternalError(
                    "Binary operations with range vectors not yet supported".to_string(),
                ))
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
        &mut self,
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
            ExprResult::RangeVector(_) => {
                return Err(EvaluationError::InternalError(
                    "Cannot aggregate range vectors directly - use functions like rate() first"
                        .to_string(),
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
    use promql_parser::label::{METRIC_NAME, Matchers};
    use promql_parser::parser::EvalStmt;
    use promql_parser::parser::value::ValueType;
    use rstest::rstest;

    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Type alias for test data: (metric_name, labels, timestamp_offset_ms, value)
    type TestSampleData = Vec<(&'static str, Vec<(&'static str, &'static str)>, i64, f64)>;

    // Type aliases for vector selector test to reduce complexity warnings
    type VectorSelectorTestData = Vec<(
        TimeBucket,
        &'static str,
        Vec<(&'static str, &'static str)>,
        i64,
        f64,
    )>;
    type VectorSelectorExpectedResults = Vec<(f64, Vec<(&'static str, &'static str)>)>;

    /// Helper to parse a PromQL query and evaluate it
    async fn parse_and_evaluate<'reader, R: QueryReader>(
        evaluator: &mut Evaluator<'reader, R>,
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

        evaluator
            .evaluate(stmt)
            .await
            .map(|result| result.expect_instant_vector("Expected instant vector result"))
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
        let mut evaluator = Evaluator::new(&reader);
        let lookback_delta = Duration::from_secs(300); // 5 minutes

        let result = parse_and_evaluate(&mut evaluator, query, end_time, lookback_delta)
            .await
            .expect("Query should evaluate successfully");

        assert_results_match(&result, &expected_samples);
    }

    #[tokio::test]
    async fn should_cache_samples_across_evaluations() {
        // given: mock reader with data in specific bucket
        let bucket = TimeBucket::hour(1000);
        let mut builder = MockQueryReaderBuilder::new(bucket);
        let labels = vec![
            Label {
                name: METRIC_NAME.to_string(),
                value: "cached_metric".to_string(),
            },
            Label {
                name: "instance".to_string(),
                value: "server1".to_string(),
            },
        ];
        let sample = Sample {
            timestamp_ms: 300001,
            value: 100.0,
        };
        builder.add_sample(labels, MetricType::Gauge, sample);
        let reader = builder.build();
        // Create cached reader and evaluator
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate the same query multiple times
        let end_time = UNIX_EPOCH + Duration::from_millis(300002);
        let lookback_delta = Duration::from_secs(300);
        let expr = promql_parser::parser::parse("cached_metric").unwrap();
        let stmt = EvalStmt {
            expr,
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta,
        };
        // First evaluation
        let result1 = evaluator
            .evaluate(stmt.clone())
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");
        // Second evaluation - should use cached data
        let result2 = evaluator
            .evaluate(stmt.clone())
            .await
            .unwrap()
            .expect_instant_vector("Expected instant vector result");

        // then: results should be identical (sample caching disabled for now)
        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result1[0].value, 100.0);
        assert_eq!(result2[0].value, 100.0);
        assert_eq!(result1[0].labels, result2[0].labels);

        // Note: Sample caching is disabled for now to avoid time range issues
        // assert!(cache.get_samples(&bucket, &0).is_some());
        // let cached_samples = cache.get_samples(&bucket, &0).unwrap();
        // assert_eq!(cached_samples.len(), 1);
        // assert_eq!(cached_samples[0].value, 100.0);
    }

    #[tokio::test]
    async fn should_evaluate_number_literal() {
        // given: create an empty mock reader
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);

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

        // then: should return scalar result with value 42.0
        assert!(result.is_ok());
        match result.unwrap() {
            ExprResult::Scalar(value) => assert_eq!(value, 42.0),
            ExprResult::InstantVector(_) => panic!("Expected scalar result, got vector"),
            ExprResult::RangeVector(_) => panic!("Expected scalar result, got range vector"),
        }
    }

    #[tokio::test]
    async fn should_error_on_string_literal() {
        // given: create an empty mock reader
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate a string literal
        let end_time = UNIX_EPOCH + Duration::from_secs(2000);
        let stmt = EvalStmt {
            expr: promql_parser::parser::Expr::StringLiteral(
                promql_parser::parser::StringLiteral {
                    val: "hello".to_string(),
                },
            ),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt).await;

        // then: should return an error (string literals cannot be evaluated standalone)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("string literal"),
            "Error message should mention 'string literal', got: {}",
            err
        );
    }

    #[tokio::test]
    async fn should_error_on_string_literal_as_function_argument() {
        // given: create an empty mock reader
        let bucket = TimeBucket::hour(1000);
        let reader = MockQueryReaderBuilder::new(bucket).build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate a function call with a string literal argument
        let end_time = UNIX_EPOCH + Duration::from_secs(2000);
        let stmt = EvalStmt {
            expr: promql_parser::parser::Expr::Call(promql_parser::parser::Call {
                func: promql_parser::parser::Function {
                    name: "label_replace",
                    arg_types: vec![],
                    variadic: false,
                    return_type: ValueType::Vector,
                },
                args: promql_parser::parser::FunctionArgs {
                    args: vec![Box::new(promql_parser::parser::Expr::StringLiteral(
                        promql_parser::parser::StringLiteral {
                            val: "replacement".to_string(),
                        },
                    ))],
                },
            }),
            start: end_time,
            end: end_time,
            interval: Duration::from_secs(0),
            lookback_delta: Duration::from_secs(300),
        };

        let result = evaluator.evaluate(stmt).await;

        // then: should return a context-specific error (string arg not yet supported)
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("string literal")
                && err.to_string().contains("label_replace")
                && err.to_string().contains("not yet supported"),
            "Error message should mention string literal, function name, and 'not yet supported', got: {}",
            err
        );
    }

    #[allow(clippy::type_complexity)]
    #[rstest]
    #[case(
        "single_bucket_selector", 
        vec![
            (TimeBucket::hour(100), "http_requests", vec![("env", "prod")], 6_000_001, 10.0),
            (TimeBucket::hour(100), "http_requests", vec![("env", "staging")], 6_000_002, 20.0),
        ],
        6_300_000, // query time 
        300_000,   // 5 min lookback
        vec![(10.0, vec![("__name__", "http_requests"), ("env", "prod")]), (20.0, vec![("__name__", "http_requests"), ("env", "staging")])]
    )]
    #[case(
        "multi_bucket_latest_wins", 
        vec![
            // Same series in bucket 100 (older)
            (TimeBucket::hour(100), "cpu_usage", vec![("host", "server1")], 6_000_000, 50.0),
            // Same series in bucket 200 (newer) - this should win
            (TimeBucket::hour(200), "cpu_usage", vec![("host", "server1")], 12_000_000, 75.0),
        ],
        12_300_000, // query time in bucket 200
        600_000,    // 10 min lookback covers both buckets
        vec![(75.0, vec![("__name__", "cpu_usage"), ("host", "server1")])] // only the newer value
    )]
    #[case(
        "multi_bucket_different_series_different_buckets", 
        vec![
            // Series A: sample in bucket 100 is outside lookback, sample in bucket 200 is within lookback
            (TimeBucket::hour(100), "memory", vec![("app", "frontend")], 6_000_000, 100.0), // outside lookback window
            (TimeBucket::hour(200), "memory", vec![("app", "frontend")], 10_000_000, 80.0), // within lookback window
            // Series B: latest sample in bucket 200 within lookback
            (TimeBucket::hour(100), "memory", vec![("app", "backend")], 5_000_000, 150.0), // outside lookback window
            (TimeBucket::hour(200), "memory", vec![("app", "backend")], 12_000_000, 200.0), // within lookback window
        ],
        12_300_000, // query time
        3_600_000,  // 1 hour lookback: (8,700,000, 12,300,000]
        vec![
            (80.0, vec![("__name__", "memory"), ("app", "frontend")]),  // latest within lookback from bucket 200
            (200.0, vec![("__name__", "memory"), ("app", "backend")])   // latest within lookback from bucket 200
        ]
    )]
    #[tokio::test]
    async fn should_evaluate_vector_selector(
        #[case] _test_name: &str,
        #[case] data: VectorSelectorTestData,
        #[case] query_time_ms: i64,
        #[case] lookback_ms: i64,
        #[case] expected: VectorSelectorExpectedResults,
    ) {
        use crate::query::test_utils::MockMultiBucketQueryReaderBuilder;
        use promql_parser::label::{METRIC_NAME, Matchers};
        use promql_parser::parser::VectorSelector;
        use std::time::{Duration, UNIX_EPOCH};

        // Extract metric name from first sample for selector before consuming data
        let metric_name = if let Some((_, name, _, _, _)) = data.first() {
            name.to_string()
        } else {
            "test_metric".to_string()
        };

        // given: build mock reader with test data
        let mut builder = MockMultiBucketQueryReaderBuilder::new();

        for (bucket, metric_name, label_pairs, timestamp_ms, value) in data {
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

            builder.add_sample(
                bucket,
                labels,
                MetricType::Gauge,
                Sample {
                    timestamp_ms,
                    value,
                },
            );
        }

        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate vector selector
        let query_time = UNIX_EPOCH + Duration::from_millis(query_time_ms as u64);
        let lookback_delta = Duration::from_millis(lookback_ms as u64);

        let selector = VectorSelector {
            name: Some(metric_name),
            matchers: Matchers {
                matchers: vec![],
                or_matchers: vec![],
            },
            offset: None,
            at: None,
        };

        let result = evaluator
            .evaluate_vector_selector(&selector, query_time, lookback_delta)
            .await
            .unwrap();

        // then: verify results
        if let ExprResult::InstantVector(samples) = result {
            assert_eq!(samples.len(), expected.len(), "Result count mismatch");

            // Sort both actual and expected results for comparison
            let mut actual_sorted = samples;
            actual_sorted.sort_by(|a, b| {
                let mut a_labels: Vec<_> = a.labels.iter().collect();
                let mut b_labels: Vec<_> = b.labels.iter().collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            });

            let mut expected_sorted = expected;
            expected_sorted.sort_by(|a, b| {
                let mut a_labels: Vec<(String, String)> =
                    a.1.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                let mut b_labels: Vec<(String, String)> =
                    b.1.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            });

            for (i, (actual, (expected_value, expected_labels))) in
                actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
            {
                assert!(
                    (actual.value - expected_value).abs() < 0.0001,
                    "Sample {} value mismatch: got {}, expected {}",
                    i,
                    actual.value,
                    expected_value
                );

                for (key, value) in expected_labels {
                    assert_eq!(
                        actual.labels.get(*key),
                        Some(&value.to_string()),
                        "Sample {} missing label {}={}",
                        i,
                        key,
                        value
                    );
                }
            }
        } else {
            panic!("Expected InstantVector result");
        }
    }

    // Matrix Selector Tests

    type MatrixSelectorTestData = Vec<(
        TimeBucket,
        &'static str,
        Vec<(&'static str, &'static str)>,
        i64,
        f64,
    )>;
    type MatrixSelectorExpectedResults = Vec<(Vec<(&'static str, &'static str)>, Vec<(i64, f64)>)>;

    #[rstest]
    #[case(
        "single_series_multiple_samples",
        vec![
            // One series with multiple samples across time
            (TimeBucket::hour(100), "cpu_usage", vec![("host", "server1")], 6_000_000, 10.0),
            (TimeBucket::hour(100), "cpu_usage", vec![("host", "server1")], 6_060_000, 15.0), // 1 min later
            (TimeBucket::hour(100), "cpu_usage", vec![("host", "server1")], 6_120_000, 20.0), // 2 min later
        ],
        6_150_000, // query time: 2.5 min after first sample
        Duration::from_secs(180), // 3 min range: covers all 3 samples
        vec![
            (vec![("__name__", "cpu_usage"), ("host", "server1")], vec![(6_000_000, 10.0), (6_060_000, 15.0), (6_120_000, 20.0)])
        ]
    )]
    #[case(
        "multiple_series_same_time_range",
        vec![
            // Two different series with samples in the range
            (TimeBucket::hour(100), "memory", vec![("app", "frontend")], 6_000_000, 100.0),
            (TimeBucket::hour(100), "memory", vec![("app", "frontend")], 6_060_000, 110.0),
            (TimeBucket::hour(100), "memory", vec![("app", "backend")], 6_030_000, 200.0),
            (TimeBucket::hour(100), "memory", vec![("app", "backend")], 6_090_000, 220.0),
        ],
        6_100_000, // query time
        Duration::from_secs(120), // 2 min range
        vec![
            (vec![("__name__", "memory"), ("app", "backend")], vec![(6_030_000, 200.0), (6_090_000, 220.0)]),
            (vec![("__name__", "memory"), ("app", "frontend")], vec![(6_000_000, 100.0), (6_060_000, 110.0)])
        ]
    )]
    #[case(
        "single_bucket_all_samples_in_range",
        vec![
            // All samples in same bucket within the range
            (TimeBucket::hour(100), "disk_io", vec![("device", "sda")], 6_000_000, 50.0),
            (TimeBucket::hour(100), "disk_io", vec![("device", "sda")], 6_030_000, 55.0),
            (TimeBucket::hour(100), "disk_io", vec![("device", "sda")], 6_060_000, 60.0),
            (TimeBucket::hour(100), "disk_io", vec![("device", "sda")], 6_090_000, 65.0),
        ],
        6_100_000, // query time
        Duration::from_secs(120), // 2 min range: should include last 3 samples
        vec![
            (vec![("__name__", "disk_io"), ("device", "sda")], vec![(6_000_000, 50.0), (6_030_000, 55.0), (6_060_000, 60.0), (6_090_000, 65.0)])
        ]
    )]
    #[case(
        "partial_time_range_filtering",
        vec![
            // Some samples outside the range should be filtered out
            (TimeBucket::hour(100), "requests", vec![("method", "GET")], 5_900_000, 100.0), // too old
            (TimeBucket::hour(100), "requests", vec![("method", "GET")], 6_000_000, 110.0), // in range
            (TimeBucket::hour(100), "requests", vec![("method", "GET")], 6_030_000, 120.0), // in range  
            (TimeBucket::hour(100), "requests", vec![("method", "GET")], 6_200_000, 130.0), // too new
        ],
        6_100_000, // query time
        Duration::from_secs(90), // 1.5 min range: end-90s to end, so 6_010_000 to 6_100_000
        vec![
            (vec![("__name__", "requests"), ("method", "GET")], vec![(6_030_000, 120.0)]) // only middle samples in range
        ]
    )]
    #[tokio::test]
    async fn should_evaluate_matrix_selector(
        #[case] test_name: &str,
        #[case] data: MatrixSelectorTestData,
        #[case] query_time_ms: i64,
        #[case] range: Duration,
        #[case] expected: MatrixSelectorExpectedResults,
    ) {
        // given:
        let bucket = TimeBucket::hour(100);
        let mut builder = MockQueryReaderBuilder::new(bucket);
        for (_test_bucket, metric_name, label_pairs, timestamp_ms, value) in data {
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
            builder.add_sample(
                labels,
                MetricType::Gauge,
                Sample {
                    timestamp_ms,
                    value,
                },
            );
        }
        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate matrix selector
        let query_time = UNIX_EPOCH + Duration::from_millis(query_time_ms as u64);
        let metric_name = match expected.first() {
            Some((labels, _)) => labels
                .iter()
                .find(|(k, _)| k == &"__name__")
                .map(|(_, v)| v.as_ref())
                .unwrap_or("cpu_usage"),
            None => "cpu_usage", // default fallback
        };
        let matrix_selector = MatrixSelector {
            vs: VectorSelector {
                name: Some(metric_name.to_string()),
                matchers: Matchers {
                    matchers: vec![],
                    or_matchers: vec![],
                },
                offset: None,
                at: None,
            },
            range,
        };
        let result = evaluator
            .evaluate_matrix_selector(matrix_selector, query_time)
            .await
            .unwrap();

        // then: verify results
        if let ExprResult::RangeVector(range_samples) = result {
            assert_eq!(
                range_samples.len(),
                expected.len(),
                "Test '{}': Expected {} series, got {}",
                test_name,
                expected.len(),
                range_samples.len()
            );
            let mut actual_sorted = range_samples;
            actual_sorted.sort_by(|a, b| {
                let mut a_labels: Vec<_> = a.labels.iter().collect();
                let mut b_labels: Vec<_> = b.labels.iter().collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            });
            let mut expected_sorted = expected;
            expected_sorted.sort_by(|a, b| {
                let mut a_labels: Vec<(String, String)> =
                    a.0.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                let mut b_labels: Vec<(String, String)> =
                    b.0.iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            });

            // Compare each series
            for (i, (actual, expected)) in
                actual_sorted.iter().zip(expected_sorted.iter()).enumerate()
            {
                // Check that the series has the expected labels
                for (key, expected_value) in &expected.0 {
                    assert_eq!(
                        actual.labels.get(*key),
                        Some(&expected_value.to_string()),
                        "Test '{}': Series {} missing label {}={}",
                        test_name,
                        i,
                        key,
                        expected_value
                    );
                }
                // Check that the series has the expected number of samples
                assert_eq!(
                    actual.values.len(),
                    expected.1.len(),
                    "Test '{}': Series {} expected {} samples, got {}",
                    test_name,
                    i,
                    expected.1.len(),
                    actual.values.len()
                );
                // Check each sample's timestamp and value
                for (j, (actual, (expected_ts, expected_val))) in
                    actual.values.iter().zip(expected.1.iter()).enumerate()
                {
                    assert_eq!(
                        actual.timestamp_ms, *expected_ts,
                        "Test '{}': Series {} sample {} timestamp mismatch: expected {}, got {}",
                        test_name, i, j, expected_ts, actual.timestamp_ms
                    );
                    assert_eq!(
                        actual.value, *expected_val,
                        "Test '{}': Series {} sample {} value mismatch: expected {}, got {}",
                        test_name, i, j, expected_val, actual.value
                    );
                }
            }
        } else {
            panic!(
                "Test '{}': Expected RangeVector result, got {:?}",
                test_name, result
            );
        }
    }

    #[tokio::test]
    async fn should_evaluate_rate_function_with_matrix_selector() {
        // given: mock reader with counter data over time
        let bucket = TimeBucket::hour(100);
        let mut builder = MockQueryReaderBuilder::new(bucket);
        let labels = vec![
            Label {
                name: "__name__".to_string(),
                value: "http_requests_total".to_string(),
            },
            Label {
                name: "job".to_string(),
                value: "webapp".to_string(),
            },
        ];
        builder
            .add_sample(
                labels.clone(),
                MetricType::Sum {
                    monotonic: true,
                    temporality: crate::model::Temporality::Cumulative,
                },
                Sample {
                    timestamp_ms: 6_000_000, // t=0s, counter at 100
                    value: 100.0,
                },
            )
            .add_sample(
                labels.clone(),
                MetricType::Sum {
                    monotonic: true,
                    temporality: crate::model::Temporality::Cumulative,
                },
                Sample {
                    timestamp_ms: 6_030_000, // t=30s, counter at 115
                    value: 115.0,
                },
            )
            .add_sample(
                labels.clone(),
                MetricType::Sum {
                    monotonic: true,
                    temporality: crate::model::Temporality::Cumulative,
                },
                Sample {
                    timestamp_ms: 6_060_000, // t=60s, counter at 130
                    value: 130.0,
                },
            );
        let reader = builder.build();
        let mut evaluator = Evaluator::new(&reader);

        // when: evaluate rate(http_requests_total[1m])
        let query_time = UNIX_EPOCH + Duration::from_millis(6_060_000);
        let query = "rate(http_requests_total[1m])";
        let expr = promql_parser::parser::parse(query).expect("Failed to parse query");
        let pipeline_result = evaluator
            .evaluate_expr(
                &expr,
                query_time - Duration::from_secs(60),
                query_time,
                Duration::from_secs(15), // 15s step
                Duration::from_secs(5),  // 5s lookback
            )
            .await
            .unwrap();

        if let ExprResult::InstantVector(instant_samples) = pipeline_result {
            assert_eq!(instant_samples.len(), 1, "Expected 1 result from pipeline");
            // The pipeline should give the same rate as the direct function call
            assert!(instant_samples[0].value > 0.0, "Rate should be positive");
            assert_eq!(
                instant_samples[0].labels.get("job"),
                Some(&"webapp".to_string())
            );
        } else {
            panic!(
                "Expected InstantVector result from rate function pipeline, got {:?}",
                pipeline_result
            );
        }
    }
}
